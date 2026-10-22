//! SQLite search over the distinct texts of entries: the trigram index
//! narrows the candidate texts, the shared matcher verifies each of them,
//! and references expand the matching texts into hits of events.

mod index;
#[cfg(test)]
mod tests;

use std::collections::HashMap;

use diesel::{
    Connection, ExpressionMethods, OptionalExtension, QueryDsl, QueryableByName, RunQueryDsl,
    SqliteConnection,
    connection::DefaultLoadingMode,
    query_builder::{BoxedSqlQuery, SqlQuery},
    sql_query,
    sql_types::{BigInt, Integer, Nullable, Text},
    sqlite::Sqlite,
};
use uuid::Uuid;

use crate::{
    error::AppError,
    search::{
        InSessionHit, InSessionSearchQuery, InSessionSearchResponse, PieceField,
        SessionSearchQuery, SessionSearchResponse, ValidatedSearchQuery,
        matching::{
            fragment::fragment,
            in_session::MAX_IN_SESSION_HITS,
            pattern::{Pattern, utf16_ranges},
            ranking::{MatchedEvent, SearchHit, SessionDetails, is_new, rank_sessions},
        },
    },
    usage::{UsageEventType, streams::event_segments},
};

use super::{
    models::{parse_uuid, timestamp_from_micros},
    repository::{stream_events, timestamp_to_micros},
    schema::{agent_sessions, devices, llm_usage_events, search_texts},
};

pub(super) use self::index::{backfill, index_event};

pub(super) fn session_search(
    connection: &mut SqliteConnection,
    user_id: Uuid,
    query: SessionSearchQuery,
) -> Result<SessionSearchResponse, AppError> {
    let query = query.validate()?;
    let pattern = Pattern::new(&query.text);
    let owner = user_id.to_string();
    let offset = usize::try_from(query.offset)
        .map_err(|error| AppError::internal(format!("search offset: {error}")))?;
    let page_size = usize::try_from(query.page_size)
        .map_err(|error| AppError::internal(format!("search page size: {error}")))?;
    // One read transaction keeps the hits, the session details, and the
    // fragments on the same snapshot.
    connection.transaction(|connection| {
        let mut matching = Vec::new();
        for text in candidates(&owner, &pattern, None)
            .load_iter::<TextRow, DefaultLoadingMode>(connection)?
        {
            let text = text?;
            if pattern.matches(&text.text) {
                matching.push(text.id);
            }
        }
        let hits = session_hits(connection, &owner, &matching, &query)?;
        let page = rank_sessions(hits, offset, page_size);
        let details = session_details(connection, &owner, &page.session_pks())?;
        let events = matched_events(connection, &owner, &page.event_pks())?;
        let fragments = search_texts::table
            .filter(search_texts::user_id.eq(&owner))
            .filter(search_texts::id.eq_any(page.fragment_texts()))
            .select((search_texts::id, search_texts::text))
            .load::<(i64, String)>(connection)?
            .into_iter()
            .filter_map(|(id, text)| Some((id, fragment(&text, &pattern.occurrences(&text))?)))
            .collect();
        Ok(page.into_response(query, details, &events, &fragments))
    })
}

/// Every hit in one session, each with the head of the segment holding its
/// event, derived from the session's stream facts as its streams are.
pub(super) fn search_in_session(
    connection: &mut SqliteConnection,
    user_id: Uuid,
    session_pk: Uuid,
    query: &InSessionSearchQuery,
) -> Result<InSessionSearchResponse, AppError> {
    let text = query.validate()?;
    let pattern = Pattern::new(&text);
    let owner = user_id.to_string();
    let session = session_pk.to_string();
    connection.transaction(|connection| {
        let exists = agent_sessions::table
            .filter(agent_sessions::user_id.eq(&owner))
            .filter(agent_sessions::id.eq(&session))
            .select(agent_sessions::id)
            .first::<String>(connection)
            .optional()?
            .is_some();
        if !exists {
            return Err(AppError::not_found(format!(
                "session not found: {session_pk}"
            )));
        }
        let mut ranges = HashMap::new();
        for candidate in candidates(&owner, &pattern, Some(&session))
            .load_iter::<TextRow, DefaultLoadingMode>(connection)?
        {
            let candidate = candidate?;
            let occurrences = pattern.occurrences(&candidate.text);
            if !occurrences.is_empty() {
                ranges.insert(candidate.id, utf16_ranges(&candidate.text, &occurrences));
            }
        }
        let matching = ranges.keys().copied().collect::<Vec<_>>();
        let mut hits = hits_in_session(connection, &owner, &session, &matching)?;
        if !hits.is_empty() {
            let segments = event_segments(stream_events(connection, user_id, session_pk)?);
            for hit in &mut hits {
                hit.segment = segments.get(&hit.event_pk).cloned();
            }
        }
        Ok(InSessionSearchResponse::from_hits(text, hits, ranges))
    })
}

#[derive(Clone)]
enum Bind {
    Text(String),
    BigInt(i64),
}

fn bound_query(sql: String, binds: Vec<Bind>) -> BoxedSqlQuery<'static, Sqlite, SqlQuery> {
    let mut query = sql_query(sql).into_boxed::<Sqlite>();
    for bind in binds {
        query = match bind {
            Bind::Text(value) => query.bind::<Text, _>(value),
            Bind::BigInt(value) => query.bind::<BigInt, _>(value),
        };
    }
    query
}

#[derive(QueryableByName)]
struct TextRow {
    #[diesel(sql_type = BigInt)]
    id: i64,
    #[diesel(sql_type = Text)]
    text: String,
}

/// Every text of the owner `?`.
const OWNER_TEXTS_SQL: &str =
    "SELECT t.id AS id, t.text AS text FROM search_texts AS t WHERE t.user_id = ?";

/// The texts matching the trigram expression `?1` of the owner `?2`.
const TRIGRAM_TEXTS_SQL: &str = "\
    SELECT t.id AS id, t.text AS text \
    FROM search_texts_trigram AS f \
    CROSS JOIN search_texts AS t ON t.id = f.rowid \
    WHERE search_texts_trigram MATCH ? AND t.user_id = ?";

/// The owner's texts that may hold the query: for three characters or
/// more, those whose folded form has every trigram of the folded query,
/// else all of them. With a session key, only texts its events refer to.
fn candidates(
    owner: &str,
    pattern: &Pattern,
    session_pk: Option<&str>,
) -> BoxedSqlQuery<'static, Sqlite, SqlQuery> {
    // The trigram index, when it applies, drives the lookup; the owner
    // filter narrows little, since most texts belong to one owner.
    let (mut sql, mut binds) = pattern.trigram_expression().map_or_else(
        || {
            (
                String::from(OWNER_TEXTS_SQL),
                vec![Bind::Text(owner.to_owned())],
            )
        },
        |expression| {
            (
                String::from(TRIGRAM_TEXTS_SQL),
                vec![Bind::Text(expression), Bind::Text(owner.to_owned())],
            )
        },
    );
    if let Some(session_pk) = session_pk {
        sql.push_str(
            " AND t.id IN (SELECT r.text_id FROM llm_usage_events AS e \
             CROSS JOIN search_refs AS r ON r.event_pk = e.id \
             WHERE e.user_id = ? AND e.session_pk = ?)",
        );
        binds.push(Bind::Text(owner.to_owned()));
        binds.push(Bind::Text(session_pk.to_owned()));
    }
    bound_query(sql, binds)
}

#[derive(QueryableByName)]
struct HitRow {
    #[diesel(sql_type = Text)]
    event_pk: String,
    #[diesel(sql_type = Text)]
    session_pk: String,
    #[diesel(sql_type = Text)]
    session_id: String,
    #[diesel(sql_type = BigInt)]
    observed_at: i64,
    #[diesel(sql_type = Text)]
    event_type: String,
    #[diesel(sql_type = Nullable<Integer>)]
    new_from: Option<i32>,
    #[diesel(sql_type = Integer)]
    entry_index: i32,
    #[diesel(sql_type = Text)]
    field: String,
    #[diesel(sql_type = BigInt)]
    text_id: i64,
}

/// The pieces of the owner's events that refer to `text_ids`, within the
/// query's filters. The ids travel as one JSON array. A request's `new_from`
/// is in its stream facts; an event stored before they were read has none.
fn session_hits(
    connection: &mut SqliteConnection,
    owner: &str,
    text_ids: &[i64],
    query: &ValidatedSearchQuery,
) -> Result<Vec<SearchHit>, AppError> {
    if text_ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut sql = String::from(
        "SELECT r.event_pk AS event_pk, e.session_pk AS session_pk, \
             e.session_id AS session_id, e.observed_at AS observed_at, \
             e.event_type AS event_type, f.new_from AS new_from, \
             r.entry_index AS entry_index, r.field AS field, r.text_id AS text_id \
         FROM json_each(?) AS m \
         CROSS JOIN search_refs AS r ON r.text_id = m.value \
         CROSS JOIN llm_usage_events AS e ON e.id = r.event_pk \
         LEFT JOIN llm_usage_event_stream_facts AS f ON f.event_pk = e.id \
         WHERE e.user_id = ?",
    );
    let mut binds = vec![
        Bind::Text(json_ids(text_ids)?),
        Bind::Text(owner.to_owned()),
    ];
    if let Some(from) = query.from {
        sql.push_str(" AND e.observed_at >= ?");
        binds.push(Bind::BigInt(timestamp_to_micros(from)));
    }
    if let Some(to) = query.to {
        sql.push_str(" AND e.observed_at < ?");
        binds.push(Bind::BigInt(timestamp_to_micros(to)));
    }
    push_any_filter(&mut sql, &mut binds, "e.agent_name", &query.agent_names);
    push_any_filter(&mut sql, &mut binds, "e.llm_provider", &query.llm_providers);
    push_any_filter(&mut sql, &mut binds, "e.llm_model", &query.llm_models);
    if let Some(event_type) = &query.event_type {
        sql.push_str(" AND e.event_type = ?");
        binds.push(Bind::Text(event_type.clone()));
    }
    bound_query(sql, binds)
        .load::<HitRow>(connection)?
        .into_iter()
        .map(|row| {
            let entry_index = entry_index(row.entry_index)?;
            Ok(SearchHit {
                new: is_new(
                    event_type(&row.event_type)?,
                    new_from(row.new_from)?,
                    entry_index,
                ),
                event_pk: parse_uuid(&row.event_pk, "search event id")?,
                session_pk: parse_uuid(&row.session_pk, "search session id")?,
                session_id: row.session_id,
                observed_at: timestamp_from_micros(row.observed_at)?,
                entry_index,
                field: field(&row.field)?,
                text_id: row.text_id,
            })
        })
        .collect()
}

/// Matches any of `values`; an empty list adds no filter.
fn push_any_filter(sql: &mut String, binds: &mut Vec<Bind>, column: &str, values: &[String]) {
    if values.is_empty() {
        return;
    }
    sql.push_str(" AND ");
    sql.push_str(column);
    sql.push_str(" IN (");
    sql.push_str(&vec!["?"; values.len()].join(", "));
    sql.push(')');
    binds.extend(values.iter().cloned().map(Bind::Text));
}

#[derive(QueryableByName)]
struct SessionHitRow {
    #[diesel(sql_type = Text)]
    event_pk: String,
    #[diesel(sql_type = Text)]
    event_type: String,
    #[diesel(sql_type = Nullable<Integer>)]
    new_from: Option<i32>,
    #[diesel(sql_type = Integer)]
    entry_index: i32,
    #[diesel(sql_type = Text)]
    field: String,
    #[diesel(sql_type = BigInt)]
    text_id: i64,
}

/// The pieces of one session's events that refer to `text_ids`, by
/// observation, event, entry, and `name` before `text`; one more than the
/// cap, so the caller can tell the hits were cut. Segments are filled in
/// afterwards.
fn hits_in_session(
    connection: &mut SqliteConnection,
    owner: &str,
    session_pk: &str,
    text_ids: &[i64],
) -> Result<Vec<InSessionHit>, AppError> {
    if text_ids.is_empty() {
        return Ok(Vec::new());
    }
    let limit = i64::try_from(MAX_IN_SESSION_HITS.saturating_add(1))
        .map_err(|error| AppError::internal(format!("hit limit: {error}")))?;
    // The session's references are read by event, and the unary `+` keeps
    // the text ids a filter on them, instead of a lookup of every text id
    // for every event of the session.
    sql_query(
        "WITH events AS MATERIALIZED ( \
             SELECT id, observed_at, event_type FROM llm_usage_events \
             WHERE user_id = ?1 AND session_pk = ?2 \
         ) \
         SELECT r.event_pk AS event_pk, e.event_type AS event_type, f.new_from AS new_from, \
             r.entry_index AS entry_index, r.field AS field, r.text_id AS text_id \
         FROM events AS e \
         CROSS JOIN search_refs AS r ON r.event_pk = e.id \
         LEFT JOIN llm_usage_event_stream_facts AS f ON f.event_pk = e.id \
         WHERE +r.text_id IN (SELECT value FROM json_each(?3)) \
         ORDER BY e.observed_at, e.id, r.entry_index, r.field <> 'name' \
         LIMIT ?4",
    )
    .bind::<Text, _>(owner)
    .bind::<Text, _>(session_pk)
    .bind::<Text, _>(json_ids(text_ids)?)
    .bind::<BigInt, _>(limit)
    .load::<SessionHitRow>(connection)?
    .into_iter()
    .map(|row| {
        let entry_index = entry_index(row.entry_index)?;
        Ok(InSessionHit {
            new: is_new(
                event_type(&row.event_type)?,
                new_from(row.new_from)?,
                entry_index,
            ),
            event_pk: parse_uuid(&row.event_pk, "search event id")?,
            entry_index,
            field: field(&row.field)?,
            text_id: row.text_id,
            segment: None,
        })
    })
    .collect()
}

/// Event columns of the shown matches.
fn matched_events(
    connection: &mut SqliteConnection,
    owner: &str,
    event_pks: &[Uuid],
) -> Result<HashMap<Uuid, MatchedEvent>, AppError> {
    llm_usage_events::table
        .filter(llm_usage_events::user_id.eq(owner))
        .filter(llm_usage_events::id.eq_any(event_pks.iter().map(Uuid::to_string)))
        .select((
            llm_usage_events::id,
            llm_usage_events::turn_pk,
            llm_usage_events::turn_index,
            llm_usage_events::event_type,
            llm_usage_events::llm_provider,
            llm_usage_events::llm_model,
        ))
        .load::<(String, String, i32, String, String, String)>(connection)?
        .into_iter()
        .map(
            |(id, turn_pk, turn_index, event_type, llm_provider, llm_model)| {
                Ok((
                    parse_uuid(&id, "search event id")?,
                    MatchedEvent {
                        turn_pk: parse_uuid(&turn_pk, "search turn id")?,
                        turn_index,
                        event_type,
                        llm_provider,
                        llm_model,
                    },
                ))
            },
        )
        .collect()
}

/// Session and device columns of the page's sessions.
fn session_details(
    connection: &mut SqliteConnection,
    owner: &str,
    session_pks: &[Uuid],
) -> Result<HashMap<Uuid, SessionDetails>, AppError> {
    if session_pks.is_empty() {
        return Ok(HashMap::new());
    }
    agent_sessions::table
        .inner_join(devices::table)
        .filter(agent_sessions::user_id.eq(owner))
        .filter(agent_sessions::id.eq_any(session_pks.iter().map(Uuid::to_string)))
        .select((
            agent_sessions::id,
            agent_sessions::session_id,
            agent_sessions::agent_name,
            agent_sessions::agent_version,
            devices::host_name,
            devices::platform,
            agent_sessions::started_at,
            agent_sessions::ended_at,
        ))
        .load::<SessionDetailColumns>(connection)?
        .into_iter()
        .map(
            |(
                session_pk,
                session_id,
                agent_name,
                agent_version,
                host_name,
                platform,
                started_at,
                ended_at,
            )| {
                Ok((
                    parse_uuid(&session_pk, "session id")?,
                    SessionDetails {
                        session_id,
                        agent_name,
                        agent_version,
                        host_name,
                        platform,
                        started_at: timestamp_from_micros(started_at)?,
                        ended_at: ended_at.map(timestamp_from_micros).transpose()?,
                    },
                ))
            },
        )
        .collect()
}

/// Columns of one session row and its device.
type SessionDetailColumns = (
    String,
    String,
    String,
    Option<String>,
    String,
    String,
    i64,
    Option<i64>,
);

fn json_ids(ids: &[i64]) -> Result<String, AppError> {
    serde_json::to_string(ids)
        .map_err(|error| AppError::internal(format!("serialize text ids: {error}")))
}

fn event_type(value: &str) -> Result<UsageEventType, AppError> {
    match value {
        "request" => Ok(UsageEventType::Request),
        "response" => Ok(UsageEventType::Response),
        _ => Err(AppError::internal(format!(
            "stored event type is invalid: {value}"
        ))),
    }
}

fn entry_index(value: i32) -> Result<u32, AppError> {
    u32::try_from(value)
        .map_err(|error| AppError::internal(format!("stored entry index is invalid: {error}")))
}

fn new_from(value: Option<i32>) -> Result<Option<u32>, AppError> {
    value
        .map(u32::try_from)
        .transpose()
        .map_err(|error| AppError::internal(format!("stored new_from is invalid: {error}")))
}

fn field(value: &str) -> Result<PieceField, AppError> {
    PieceField::parse(value)
        .ok_or_else(|| AppError::internal(format!("stored search field is invalid: {value}")))
}
