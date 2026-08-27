//! SQLite FTS5 raw queries with Diesel ORM hydration of relational session data.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use diesel::{
    ExpressionMethods, QueryDsl, QueryableByName, RunQueryDsl, SqliteConnection,
    query_builder::{BoxedSqlQuery, SqlQuery},
    sql_query,
    sql_types::{BigInt, Integer, Nullable, Text},
    sqlite::Sqlite,
};
use uuid::Uuid;

use crate::{
    error::AppError,
    search::{
        HIGHLIGHT_END, HIGHLIGHT_START, SearchFragment, SessionSearchMatch, SessionSearchQuery,
        SessionSearchResponse, SessionSearchResult, ValidatedSearchQuery,
    },
    usage::persistence::canonical_agent_name,
};

use super::{
    models::{parse_uuid, timestamp_from_micros},
    repository::timestamp_to_micros,
    schema::{agent_sessions, devices},
};

pub(super) fn session_search(
    connection: &mut SqliteConnection,
    user_id: Uuid,
    query: SessionSearchQuery,
) -> Result<SessionSearchResponse, AppError> {
    let query = query.validate()?;
    let source = MatchSource::new(&query, user_id);
    let total_sessions = count_sessions(connection, &source)?;
    let session_ids = load_session_page(connection, &query, &source)?;
    let matches = load_matches(connection, &session_ids, &source)?;
    let mut details = load_session_details(connection, user_id, &session_ids)?;

    let items = session_ids
        .into_iter()
        .filter_map(|session_pk| {
            let details = details.remove(&session_pk)?;
            let session_matches = matches.get(&session_pk).cloned().unwrap_or_default();
            let match_count = session_matches
                .first()
                .map_or(0, |matched| matched.match_count);
            let mut providers = session_matches
                .iter()
                .map(|matched| matched.llm_provider.clone())
                .collect::<HashSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            providers.sort();
            let mut models = session_matches
                .iter()
                .map(|matched| matched.llm_model.clone())
                .collect::<HashSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            models.sort();
            Some(SessionSearchResult {
                session_pk,
                session_id: details.session_id,
                agent_name: canonical_agent_name(&details.agent_name),
                agent_version: details.agent_version,
                host_name: details.host_name,
                platform: details.platform,
                started_at: details.started_at,
                ended_at: details.ended_at,
                providers,
                models,
                match_count,
                matches: session_matches
                    .into_iter()
                    .map(MatchedEvent::into_response)
                    .collect(),
            })
        })
        .collect();
    let consumed = u64::from(query.offset).saturating_add(u64::from(query.page_size));
    Ok(SessionSearchResponse {
        query: query.text,
        total_sessions,
        page: query.page,
        page_size: query.page_size,
        has_more: consumed < total_sessions,
        items,
    })
}

#[derive(Clone)]
enum FtsBind {
    Text(String),
    BigInt(i64),
}

struct MatchSource {
    sql: String,
    binds: Vec<FtsBind>,
}

impl MatchSource {
    fn new(query: &ValidatedSearchQuery, user_id: Uuid) -> Self {
        let mut sql = String::from(
            "FROM usage_events_fts
             INNER JOIN llm_usage_events e ON e.id = usage_events_fts.event_pk
             WHERE usage_events_fts MATCH ? AND e.user_id = ?",
        );
        let mut binds = vec![
            FtsBind::Text(fts_query(&query.text)),
            FtsBind::Text(user_id.to_string()),
        ];
        if let Some(from) = query.from {
            sql.push_str(" AND e.observed_at >= ?");
            binds.push(FtsBind::BigInt(timestamp_to_micros(from)));
        }
        if let Some(to) = query.to {
            sql.push_str(" AND e.observed_at < ?");
            binds.push(FtsBind::BigInt(timestamp_to_micros(to)));
        }
        push_exact_filter(
            &mut sql,
            &mut binds,
            "e.agent_name",
            query.agent_name.as_deref(),
        );
        push_exact_filter(
            &mut sql,
            &mut binds,
            "e.llm_provider",
            query.llm_provider.as_deref(),
        );
        push_exact_filter(
            &mut sql,
            &mut binds,
            "e.llm_model",
            query.llm_model.as_deref(),
        );
        push_exact_filter(
            &mut sql,
            &mut binds,
            "e.event_type",
            query.event_type.as_deref(),
        );
        Self { sql, binds }
    }
}

fn push_exact_filter(
    sql: &mut String,
    binds: &mut Vec<FtsBind>,
    column: &str,
    value: Option<&str>,
) {
    if let Some(value) = value {
        sql.push_str(" AND ");
        sql.push_str(column);
        sql.push_str(" = ?");
        binds.push(FtsBind::Text(value.to_owned()));
    }
}

fn bound_query(sql: String, binds: Vec<FtsBind>) -> BoxedSqlQuery<'static, Sqlite, SqlQuery> {
    let mut query = sql_query(sql).into_boxed::<Sqlite>();
    for bind in binds {
        query = match bind {
            FtsBind::Text(value) => query.bind::<Text, _>(value),
            FtsBind::BigInt(value) => query.bind::<BigInt, _>(value),
        };
    }
    query
}

#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    count: i64,
}

fn count_sessions(
    connection: &mut SqliteConnection,
    source: &MatchSource,
) -> Result<u64, AppError> {
    let sql = format!(
        "SELECT COUNT(DISTINCT e.session_pk) AS count {}",
        source.sql
    );
    let count = bound_query(sql, source.binds.clone())
        .get_result::<CountRow>(connection)?
        .count;
    u64::try_from(count.max(0))
        .map_err(|error| AppError::internal(format!("invalid FTS session count: {error}")))
}

#[derive(QueryableByName)]
struct SessionIdRow {
    #[diesel(sql_type = Text)]
    session_pk: String,
}

fn load_session_page(
    connection: &mut SqliteConnection,
    query: &ValidatedSearchQuery,
    source: &MatchSource,
) -> Result<Vec<Uuid>, AppError> {
    let sql = format!(
        "SELECT e.session_pk AS session_pk
         {}
         ORDER BY bm25(usage_events_fts) ASC, e.observed_at DESC, e.session_pk ASC",
        source.sql
    );
    let rows = bound_query(sql, source.binds.clone()).load::<SessionIdRow>(connection)?;
    let page_start = usize::try_from(query.offset)
        .map_err(|error| AppError::internal(format!("invalid FTS page offset: {error}")))?;
    let page_size = usize::try_from(query.page_size)
        .map_err(|error| AppError::internal(format!("invalid FTS page size: {error}")))?;
    let page_end = page_start.saturating_add(page_size);
    let mut seen = HashSet::new();
    let mut sessions = Vec::with_capacity(page_size);
    for row in rows {
        let session_pk = parse_uuid(&row.session_pk, "FTS session id")?;
        if !seen.insert(session_pk) {
            continue;
        }
        let position = seen.len().saturating_sub(1);
        if position >= page_start {
            sessions.push(session_pk);
        }
        if seen.len() >= page_end {
            break;
        }
    }
    Ok(sessions)
}

fn load_matches(
    connection: &mut SqliteConnection,
    session_ids: &[Uuid],
    source: &MatchSource,
) -> Result<HashMap<Uuid, Vec<MatchedEvent>>, AppError> {
    if session_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let selected_placeholders = std::iter::repeat_n("?", session_ids.len())
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "SELECT
             e.id AS event_pk,
             e.session_pk AS session_pk,
             e.turn_pk AS turn_pk,
             e.turn_index AS turn_index,
             e.event_type AS event_type,
             e.llm_provider AS llm_provider,
             e.llm_model AS llm_model,
             e.observed_at AS observed_at,
             snippet(usage_events_fts, 4, '{HIGHLIGHT_START}', '{HIGHLIGHT_END}', '…', 32)
                 AS content_fragment,
             snippet(usage_events_fts, 7, '{HIGHLIGHT_START}', '{HIGHLIGHT_END}', '…', 32)
                 AS command_fragment,
             snippet(usage_events_fts, 8, '{HIGHLIGHT_START}', '{HIGHLIGHT_END}', '…', 32)
                 AS path_fragment,
             snippet(usage_events_fts, 5, '{HIGHLIGHT_START}', '{HIGHLIGHT_END}', '…', 32)
                 AS tool_name_fragment,
             snippet(usage_events_fts, 6, '{HIGHLIGHT_START}', '{HIGHLIGHT_END}', '…', 32)
                 AS tool_content_fragment
         {}
           AND e.session_pk IN ({selected_placeholders})
         ORDER BY bm25(usage_events_fts) ASC, e.observed_at DESC, e.id ASC",
        source.sql
    );
    let mut binds = source.binds.clone();
    binds.extend(
        session_ids
            .iter()
            .map(|session_pk| FtsBind::Text(session_pk.to_string())),
    );
    let rows = bound_query(sql, binds).load::<MatchedEventRow>(connection)?;
    let mut by_session: HashMap<Uuid, (u64, Vec<MatchedEvent>)> = HashMap::new();
    for row in rows {
        let matched = row.into_matched()?;
        let entry = by_session
            .entry(matched.session_pk)
            .or_insert_with(|| (0, Vec::new()));
        entry.0 = entry.0.saturating_add(1);
        if entry.1.len() < 3 {
            entry.1.push(matched);
        }
    }
    Ok(by_session
        .into_iter()
        .map(|(session_pk, (match_count, mut matches))| {
            for matched in &mut matches {
                matched.match_count = match_count;
            }
            (session_pk, matches)
        })
        .collect())
}

#[derive(QueryableByName)]
struct MatchedEventRow {
    #[diesel(sql_type = Text)]
    event_pk: String,
    #[diesel(sql_type = Text)]
    session_pk: String,
    #[diesel(sql_type = Text)]
    turn_pk: String,
    #[diesel(sql_type = Integer)]
    turn_index: i32,
    #[diesel(sql_type = Text)]
    event_type: String,
    #[diesel(sql_type = Text)]
    llm_provider: String,
    #[diesel(sql_type = Text)]
    llm_model: String,
    #[diesel(sql_type = BigInt)]
    observed_at: i64,
    #[diesel(sql_type = Nullable<Text>)]
    content_fragment: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    command_fragment: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    path_fragment: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    tool_name_fragment: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    tool_content_fragment: Option<String>,
}

impl MatchedEventRow {
    fn into_matched(self) -> Result<MatchedEvent, AppError> {
        let fragments = [
            self.content_fragment,
            self.command_fragment,
            self.path_fragment,
            self.tool_name_fragment,
            self.tool_content_fragment,
        ]
        .into_iter()
        .flatten()
        .filter(|fragment| fragment.contains(HIGHLIGHT_START))
        .take(2)
        .collect();
        Ok(MatchedEvent {
            event_pk: parse_uuid(&self.event_pk, "FTS event id")?,
            session_pk: parse_uuid(&self.session_pk, "FTS session id")?,
            turn_pk: parse_uuid(&self.turn_pk, "FTS turn id")?,
            turn_index: self.turn_index,
            event_type: self.event_type,
            llm_provider: self.llm_provider,
            llm_model: self.llm_model,
            observed_at: timestamp_from_micros(self.observed_at)?,
            match_count: 0,
            fragments,
        })
    }
}

#[derive(Clone)]
struct MatchedEvent {
    event_pk: Uuid,
    session_pk: Uuid,
    turn_pk: Uuid,
    turn_index: i32,
    event_type: String,
    llm_provider: String,
    llm_model: String,
    observed_at: DateTime<Utc>,
    match_count: u64,
    fragments: Vec<String>,
}

impl MatchedEvent {
    fn into_response(self) -> SessionSearchMatch {
        SessionSearchMatch {
            event_pk: self.event_pk,
            turn_pk: self.turn_pk,
            turn_index: self.turn_index,
            event_type: self.event_type,
            llm_provider: self.llm_provider,
            llm_model: self.llm_model,
            observed_at: self.observed_at,
            fragments: self
                .fragments
                .into_iter()
                .map(|fragment| SearchFragment::parse(&fragment))
                .collect(),
        }
    }
}

struct SessionDetails {
    session_id: String,
    agent_name: String,
    agent_version: Option<String>,
    host_name: String,
    platform: String,
    started_at: DateTime<Utc>,
    ended_at: Option<DateTime<Utc>>,
}

fn load_session_details(
    connection: &mut SqliteConnection,
    user_id: Uuid,
    session_ids: &[Uuid],
) -> Result<HashMap<Uuid, SessionDetails>, AppError> {
    if session_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let ids = session_ids.iter().map(Uuid::to_string).collect::<Vec<_>>();
    let rows = agent_sessions::table
        .inner_join(devices::table)
        .filter(agent_sessions::user_id.eq(user_id.to_string()))
        .filter(agent_sessions::id.eq_any(ids))
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
        .load::<(
            String,
            String,
            String,
            Option<String>,
            String,
            String,
            i64,
            Option<i64>,
        )>(connection)?;
    rows.into_iter()
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

fn fts_query(value: &str) -> String {
    value
        .split_whitespace()
        .map(|term| format!("\"{}\"", term.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(" AND ")
}

#[cfg(test)]
mod tests {
    use super::fts_query;

    #[test]
    fn fts_query_uses_literal_and_terms() {
        assert_eq!(
            fts_query("blackbox response"),
            r#""blackbox" AND "response""#
        );
        assert_eq!(fts_query(r#"quoted"value"#), r#""quoted""value""#);
    }
}
