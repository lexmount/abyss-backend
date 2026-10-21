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
    let hits = count_hits(connection, &query)?;
    search(connection, user_id, query, hits)
}

/// Runs a validated search; `hits` only chooses how observation times are read.
fn search(
    connection: &mut SqliteConnection,
    user_id: Uuid,
    query: ValidatedSearchQuery,
    hits: i64,
) -> Result<SessionSearchResponse, AppError> {
    let source = MatchSource::new(&query, user_id, hits);
    let (page, total_sessions) = load_session_page(connection, &query, &source)?;
    let session_ids = page
        .iter()
        .map(|(session_pk, _)| *session_pk)
        .collect::<Vec<_>>();
    let mut matches = load_matches(connection, &session_ids, &source)?;
    let mut details = load_session_details(connection, user_id, &session_ids)?;

    let items = page
        .into_iter()
        .filter_map(|(session_pk, match_count)| {
            let details = details.remove(&session_pk)?;
            let session_matches = matches.remove(&session_pk).unwrap_or_default();
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
                agent_name: details.agent_name,
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

/// Matches the strongest events rank first: the lowest bm25, then the latest
/// observation, then the event key, so results are deterministic.
const MATCHES_PER_SESSION: i64 = 3;

#[derive(Clone)]
enum FtsBind {
    Text(String),
    BigInt(i64),
}

/// Above this many FTS hits, observation times come from the owner-time
/// covering index for all of the owner's events at once; below it, each hit
/// reads its own event row, which is cheaper than building that index.
const ROW_LOOKUP_MAX_HITS: i64 = 256;

/// Where a page's session list is inserted into the shared expressions.
const SESSION_FILTER: &str = "/* session filter */";

/// Common table expressions that list every matching event of the owner with
/// its rank, observation time, and position and count within its session.
///
/// Every step avoids reading wide rows: `hits` takes only rowids and bm25 from
/// the FTS index; event and session keys come from FTS5's content table rather
/// than through the virtual table, which would load each matching document.
/// Observation times sit after the event text in its row, so for many hits
/// they come from the owner-time covering index instead, and the time window
/// is applied after the join so SQLite keeps an index on that list. Event rows
/// are otherwise joined only for filters whose columns precede the text.
struct MatchSource {
    sql: String,
    binds: Vec<FtsBind>,
}

impl MatchSource {
    fn new(query: &ValidatedSearchQuery, user_id: Uuid, hits: i64) -> Self {
        let mut binds = vec![FtsBind::Text(fts_query(&query.text))];
        let read_rows = hits <= ROW_LOOKUP_MAX_HITS;
        let (events_cte, joins, time_column) = if read_rows {
            (
                "",
                "INNER JOIN llm_usage_events e ON e.id = c.c0",
                "e.observed_at",
            )
        } else {
            binds.push(FtsBind::Text(user_id.to_string()));
            (
                ",
             events AS MATERIALIZED (
                 SELECT id, observed_at FROM llm_usage_events WHERE user_id = ?
             )",
                "INNER JOIN events ev ON ev.id = c.c0",
                "ev.observed_at",
            )
        };
        let mut conditions = String::from("1 = 1");
        if read_rows {
            conditions.push_str(" AND e.user_id = ?");
            binds.push(FtsBind::Text(user_id.to_string()));
        }
        if let Some(from) = query.from {
            conditions.push_str(" AND ");
            conditions.push_str(time_column);
            conditions.push_str(" >= ?");
            binds.push(FtsBind::BigInt(timestamp_to_micros(from)));
        }
        if let Some(to) = query.to {
            conditions.push_str(" AND ");
            conditions.push_str(time_column);
            conditions.push_str(" < ?");
            binds.push(FtsBind::BigInt(timestamp_to_micros(to)));
        }
        let mut filters = String::new();
        push_any_filter(&mut filters, &mut binds, "e.agent_name", &query.agent_names);
        push_any_filter(
            &mut filters,
            &mut binds,
            "e.llm_provider",
            &query.llm_providers,
        );
        push_any_filter(&mut filters, &mut binds, "e.llm_model", &query.llm_models);
        push_exact_filter(
            &mut filters,
            &mut binds,
            "e.event_type",
            query.event_type.as_deref(),
        );
        let event_join = if read_rows || filters.is_empty() {
            ""
        } else {
            "INNER JOIN llm_usage_events e ON e.id = c.c0"
        };
        let sql = format!(
            "WITH hits AS MATERIALIZED (
                 SELECT rowid AS fts_rowid, bm25(usage_events_fts) AS rank
                 FROM usage_events_fts
                 WHERE usage_events_fts MATCH ?
             ){events_cte},
             matched AS MATERIALIZED (
                 SELECT c.c0 AS event_pk, c.c2 AS session_pk, h.fts_rowid, h.rank,
                        {time_column} AS observed_at
                 FROM hits h
                 INNER JOIN usage_events_fts_content c ON c.id = h.fts_rowid
                 {joins}
                 {event_join}
                 WHERE {conditions}{filters} {SESSION_FILTER}
             ),
             ranked AS (
                 SELECT matched.*,
                        ROW_NUMBER() OVER (
                            PARTITION BY session_pk
                            ORDER BY rank ASC, observed_at DESC, event_pk ASC
                        ) AS position,
                        COUNT(*) OVER (PARTITION BY session_pk) AS match_count
                 FROM matched
             )"
        );
        Self { sql, binds }
    }

    /// The expressions with matches limited to `session_ids`, or all sessions.
    fn sql_for(&self, session_ids: &[Uuid]) -> (String, Vec<FtsBind>) {
        let mut binds = self.binds.clone();
        let session_filter = if session_ids.is_empty() {
            String::new()
        } else {
            binds.extend(
                session_ids
                    .iter()
                    .map(|session_pk| FtsBind::Text(session_pk.to_string())),
            );
            format!("AND c.c2 IN ({})", vec!["?"; session_ids.len()].join(", "))
        };
        (self.sql.replace(SESSION_FILTER, &session_filter), binds)
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

/// Matches any of `values`; an empty list adds no filter.
fn push_any_filter(sql: &mut String, binds: &mut Vec<FtsBind>, column: &str, values: &[String]) {
    if values.is_empty() {
        return;
    }
    sql.push_str(" AND ");
    sql.push_str(column);
    sql.push_str(" IN (");
    sql.push_str(&vec!["?"; values.len()].join(", "));
    sql.push(')');
    binds.extend(values.iter().cloned().map(FtsBind::Text));
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

/// FTS hits across all owners, before any filter; reads only the index.
fn count_hits(
    connection: &mut SqliteConnection,
    query: &ValidatedSearchQuery,
) -> Result<i64, AppError> {
    Ok(bound_query(
        "SELECT COUNT(*) AS count FROM usage_events_fts WHERE usage_events_fts MATCH ?".to_owned(),
        vec![FtsBind::Text(fts_query(&query.text))],
    )
    .get_result::<CountRow>(connection)?
    .count)
}

#[derive(QueryableByName)]
struct SessionPageRow {
    #[diesel(sql_type = Text)]
    session_pk: String,
    #[diesel(sql_type = BigInt)]
    match_count: i64,
    #[diesel(sql_type = BigInt)]
    total_sessions: i64,
}

/// One page of sessions with their match counts, ordered by each session's
/// strongest event, and the number of matching sessions.
fn load_session_page(
    connection: &mut SqliteConnection,
    query: &ValidatedSearchQuery,
    source: &MatchSource,
) -> Result<(Vec<(Uuid, u64)>, u64), AppError> {
    let (sql, mut binds) = source.sql_for(&[]);
    let page_sql = format!(
        "{sql}
         SELECT session_pk, match_count, COUNT(*) OVER () AS total_sessions
         FROM ranked
         WHERE position = 1
         ORDER BY rank ASC, observed_at DESC, session_pk ASC
         LIMIT ? OFFSET ?"
    );
    binds.push(FtsBind::BigInt(i64::from(query.page_size)));
    binds.push(FtsBind::BigInt(i64::from(query.offset)));
    let rows = bound_query(page_sql, binds).load::<SessionPageRow>(connection)?;
    let total_sessions = match rows.first() {
        Some(row) => row.total_sessions,
        // A page past the end has no rows to carry the total.
        None if query.offset > 0 => {
            let (sql, binds) = source.sql_for(&[]);
            bound_query(
                format!("{sql} SELECT COUNT(*) AS count FROM ranked WHERE position = 1"),
                binds,
            )
            .get_result::<CountRow>(connection)?
            .count
        }
        None => 0,
    };
    let page = rows
        .into_iter()
        .map(|row| {
            Ok((
                parse_uuid(&row.session_pk, "FTS session id")?,
                u64::try_from(row.match_count.max(0)).map_err(|error| {
                    AppError::internal(format!("invalid FTS match count: {error}"))
                })?,
            ))
        })
        .collect::<Result<Vec<_>, AppError>>()?;
    let total_sessions = u64::try_from(total_sessions.max(0))
        .map_err(|error| AppError::internal(format!("invalid FTS session count: {error}")))?;
    Ok((page, total_sessions))
}

/// The strongest matches of each session, with highlighted fragments built
/// only for the events returned.
fn load_matches(
    connection: &mut SqliteConnection,
    session_ids: &[Uuid],
    source: &MatchSource,
) -> Result<HashMap<Uuid, Vec<MatchedEvent>>, AppError> {
    if session_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let (sql, mut binds) = source.sql_for(session_ids);
    let matches_sql = format!(
        "{sql},
         top AS MATERIALIZED (
             SELECT * FROM ranked WHERE position <= {MATCHES_PER_SESSION}
         )
         SELECT
             top.event_pk AS event_pk,
             top.session_pk AS session_pk,
             e.turn_pk AS turn_pk,
             e.turn_index AS turn_index,
             e.event_type AS event_type,
             e.llm_provider AS llm_provider,
             e.llm_model AS llm_model,
             top.observed_at AS observed_at,
             snippet(usage_events_fts, 4, '{HIGHLIGHT_START}', '{HIGHLIGHT_END}', '…', 32)
                 AS content_fragment,
             snippet(usage_events_fts, 5, '{HIGHLIGHT_START}', '{HIGHLIGHT_END}', '…', 32)
                 AS tool_name_fragment,
             snippet(usage_events_fts, 6, '{HIGHLIGHT_START}', '{HIGHLIGHT_END}', '…', 32)
                 AS tool_content_fragment
         FROM top
         INNER JOIN usage_events_fts ON usage_events_fts.rowid = top.fts_rowid
         INNER JOIN llm_usage_events e ON e.id = top.event_pk
         WHERE usage_events_fts MATCH ?
         ORDER BY top.session_pk, top.position"
    );
    binds.push(source.binds[0].clone());
    let rows = bound_query(matches_sql, binds).load::<MatchedEventRow>(connection)?;
    let mut by_session: HashMap<Uuid, Vec<MatchedEvent>> = HashMap::new();
    for row in rows {
        let matched = row.into_matched()?;
        by_session
            .entry(matched.session_pk)
            .or_default()
            .push(matched);
    }
    Ok(by_session)
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
    tool_name_fragment: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    tool_content_fragment: Option<String>,
}

impl MatchedEventRow {
    fn into_matched(self) -> Result<MatchedEvent, AppError> {
        let fragments = [
            self.content_fragment,
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
            fragments,
        })
    }
}

struct MatchedEvent {
    event_pk: Uuid,
    session_pk: Uuid,
    turn_pk: Uuid,
    turn_index: i32,
    event_type: String,
    llm_provider: String,
    llm_model: String,
    observed_at: DateTime<Utc>,
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
    use diesel::{Connection, SqliteConnection};
    use serde_json::{Value, json};
    use uuid::Uuid;

    use crate::{
        search::{SessionSearchQuery, SessionSearchResponse},
        usage::IngestEventsRequest,
    };

    use super::{
        super::{migrations, repository::ingest_events},
        ROW_LOOKUP_MAX_HITS, fts_query, search,
    };

    const OWNER_ID: Uuid = Uuid::from_u128(1);

    #[test]
    fn fts_query_uses_literal_and_terms() {
        assert_eq!(
            fts_query("blackbox response"),
            r#""blackbox" AND "response""#
        );
        assert_eq!(fts_query(r#"quoted"value"#), r#""quoted""value""#);
    }

    fn event(id: &str, session: &str, observed_at: &str, text: &str) -> Value {
        json!({
            "event_id": id,
            "observed_at": observed_at,
            "device": {"host_name": "alice-mbp", "platform": "macos"},
            "agent": {"name": "codex"},
            "session_id": session,
            "turn_index": 1,
            "llm": {"provider": "openai", "model": "gpt-5"},
            "event_type": "response",
            "token_usage": {"input_tokens": 0_i64, "output_tokens": 1_i64, "cache_read_tokens": 0_i64, "cache_write_tokens": 0_i64, "reasoning_tokens": 0_i64, "total_tokens": 1_i64},
            "metadata": {"conversation": {
                "entries": [{"role": "assistant", "text": text}],
                "completeness": "complete"
            }}
        })
    }

    /// Three sessions: `alpha` has five equal hits, `beta` one strong hit, and
    /// `gamma` one hit diluted by other words.
    fn seeded() -> SqliteConnection {
        let mut database = SqliteConnection::establish(":memory:").expect("SQLite should open");
        migrations::run(&mut database).expect("migration should succeed");
        let mut events = (1_u32..=5_u32)
            .map(|minute| {
                event(
                    &format!("a{minute}"),
                    "alpha",
                    &format!("2026-09-01T00:0{minute}:00Z"),
                    "needle note",
                )
            })
            .collect::<Vec<_>>();
        events.push(event("b1", "beta", "2026-09-02T00:00:00Z", "needle"));
        events.push(event(
            "g1",
            "gamma",
            "2026-09-03T00:00:00Z",
            "needle among many other unrelated words in a much longer reply",
        ));
        events.push(event("z1", "zeta", "2026-09-04T00:00:00Z", "nothing here"));
        let request: IngestEventsRequest =
            serde_json::from_value(json!({ "events": events })).expect("request should parse");
        ingest_events(&mut database, &request, OWNER_ID, 100).expect("ingest should succeed");
        database
    }

    fn run(database: &mut SqliteConnection, query: Value, hits: i64) -> SessionSearchResponse {
        let query: SessionSearchQuery =
            serde_json::from_value(query).expect("query should deserialize");
        search(
            database,
            OWNER_ID,
            query.validate().expect("query should validate"),
            hits,
        )
        .expect("search should succeed")
    }

    fn summary(response: &SessionSearchResponse) -> Vec<(String, u64, usize)> {
        response
            .items
            .iter()
            .map(|item| {
                (
                    item.session_id.clone(),
                    item.match_count,
                    item.matches.len(),
                )
            })
            .collect()
    }

    #[test]
    fn sessions_rank_by_their_strongest_event_and_page_in_order() {
        let mut database = seeded();

        let first = run(&mut database, json!({"q": "needle", "page_size": 2_u32}), 0);
        let second = run(
            &mut database,
            json!({"q": "needle", "page": 2_u32, "page_size": 2_u32}),
            0,
        );
        let past_end = run(
            &mut database,
            json!({"q": "needle", "page": 5_u32, "page_size": 2_u32}),
            0,
        );

        assert_eq!(first.total_sessions, 3);
        assert!(first.has_more);
        assert_eq!(
            summary(&first),
            [("beta".to_owned(), 1, 1), ("alpha".to_owned(), 5, 3)]
        );
        assert_eq!(summary(&second), [("gamma".to_owned(), 1, 1)]);
        assert!(!second.has_more);
        assert_eq!(past_end.total_sessions, 3);
        assert!(past_end.items.is_empty());
    }

    #[test]
    fn a_session_returns_its_three_latest_equal_matches_and_counts_all() {
        let mut database = seeded();

        let response = run(&mut database, json!({"q": "needle note"}), 0);

        assert_eq!(summary(&response), [("alpha".to_owned(), 5, 3)]);
        let minutes = response.items[0]
            .matches
            .iter()
            .map(|matched| matched.observed_at.format("%M").to_string())
            .collect::<Vec<_>>();
        // Equal scores fall back to the latest observation first.
        assert_eq!(minutes, ["05", "04", "03"]);
        assert!(
            response.items[0].matches[0].fragments[0]
                .segments
                .iter()
                .any(|segment| segment.highlighted)
        );
    }

    #[test]
    fn both_time_lookups_return_the_same_results_with_filters() {
        let mut database = seeded();
        for query in [
            json!({"q": "needle"}),
            json!({"q": "needle", "from": "2026-09-01T00:03:00Z", "to": "2026-09-02T12:00:00Z"}),
            json!({"q": "needle", "agent_name": ["codex"], "event_type": "response", "page_size": 1_u32}),
            json!({"q": "needle", "llm_model": ["absent"]}),
        ] {
            let by_rows = run(&mut database, query.clone(), 0);
            let by_index = run(&mut database, query.clone(), ROW_LOOKUP_MAX_HITS + 1);
            assert_eq!(
                serde_json::to_value(&by_rows).expect("serializes"),
                serde_json::to_value(&by_index).expect("serializes"),
                "{query}"
            );
        }
        let windowed = run(
            &mut database,
            json!({"q": "needle", "from": "2026-09-01T00:03:00Z", "to": "2026-09-02T12:00:00Z"}),
            ROW_LOOKUP_MAX_HITS + 1,
        );
        assert_eq!(
            summary(&windowed),
            [("beta".to_owned(), 1, 1), ("alpha".to_owned(), 3, 3)]
        );
    }
}
