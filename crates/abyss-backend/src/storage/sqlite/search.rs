//! Owner-scoped SQLite FTS5 session search and relational result hydration.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use rusqlite::{Connection, Row, params_from_iter, types::Value as SqlValue};
use uuid::Uuid;

use crate::{
    error::AppError,
    search::{
        HIGHLIGHT_END, HIGHLIGHT_START, SearchFragment, SessionSearchMatch, SessionSearchQuery,
        SessionSearchResponse, SessionSearchResult, ValidatedSearchQuery,
    },
    usage::persistence::canonical_agent_name,
};

use super::repository::{parse_uuid_value, timestamp_from_micros, timestamp_to_micros};

pub(super) fn session_search(
    connection: &Connection,
    user_id: Uuid,
    query: SessionSearchQuery,
) -> Result<SessionSearchResponse, AppError> {
    let query = query.validate()?;
    let (match_sql, match_values) = match_source(&query, user_id);
    let total_sessions = count_sessions(connection, &match_sql, &match_values)?;
    let session_ids = load_session_page(connection, &query, &match_sql, &match_values)?;
    let matches = load_matches(connection, &session_ids, &match_sql, &match_values)?;
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

fn match_source(query: &ValidatedSearchQuery, user_id: Uuid) -> (String, Vec<SqlValue>) {
    let mut sql = String::from(
        "FROM usage_events_fts
         INNER JOIN llm_usage_events e ON e.id = usage_events_fts.event_pk
         WHERE usage_events_fts MATCH ? AND e.user_id = ?",
    );
    let mut values = vec![
        SqlValue::Text(fts_query(&query.text)),
        SqlValue::Text(user_id.to_string()),
    ];
    if let Some(from) = query.from {
        sql.push_str(" AND e.observed_at >= ?");
        values.push(SqlValue::Integer(timestamp_to_micros(from)));
    }
    if let Some(to) = query.to {
        sql.push_str(" AND e.observed_at < ?");
        values.push(SqlValue::Integer(timestamp_to_micros(to)));
    }
    push_exact_filter(
        &mut sql,
        &mut values,
        "e.agent_name",
        query.agent_name.as_deref(),
    );
    push_exact_filter(
        &mut sql,
        &mut values,
        "e.llm_provider",
        query.llm_provider.as_deref(),
    );
    push_exact_filter(
        &mut sql,
        &mut values,
        "e.llm_model",
        query.llm_model.as_deref(),
    );
    push_exact_filter(
        &mut sql,
        &mut values,
        "e.event_type",
        query.event_type.as_deref(),
    );
    (sql, values)
}

fn push_exact_filter(
    sql: &mut String,
    values: &mut Vec<SqlValue>,
    column: &str,
    value: Option<&str>,
) {
    if let Some(value) = value {
        sql.push_str(" AND ");
        sql.push_str(column);
        sql.push_str(" = ?");
        values.push(SqlValue::Text(value.to_owned()));
    }
}

fn count_sessions(
    connection: &Connection,
    match_sql: &str,
    values: &[SqlValue],
) -> Result<u64, AppError> {
    let sql = format!("SELECT COUNT(DISTINCT e.session_pk) {match_sql}");
    let count = connection.query_row(&sql, params_from_iter(values.iter()), |row| {
        row.get::<_, i64>(0)
    })?;
    u64::try_from(count.max(0))
        .map_err(|error| AppError::internal(format!("invalid FTS session count: {error}")))
}

fn load_session_page(
    connection: &Connection,
    query: &ValidatedSearchQuery,
    match_sql: &str,
    values: &[SqlValue],
) -> Result<Vec<Uuid>, AppError> {
    let sql = format!(
        "SELECT e.session_pk
         {match_sql}
         ORDER BY bm25(usage_events_fts) ASC, e.observed_at DESC, e.session_pk ASC"
    );
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map(params_from_iter(values.iter()), |row| {
        let value = row.get::<_, String>(0)?;
        parse_uuid_value(&value, 0)
    })?;
    let page_start = usize::try_from(query.offset)
        .map_err(|error| AppError::internal(format!("invalid FTS page offset: {error}")))?;
    let page_size = usize::try_from(query.page_size)
        .map_err(|error| AppError::internal(format!("invalid FTS page size: {error}")))?;
    let page_end = page_start.saturating_add(page_size);
    let mut seen = HashSet::new();
    let mut sessions = Vec::with_capacity(page_size);
    for row in rows {
        let session_pk = row?;
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
    connection: &Connection,
    session_ids: &[Uuid],
    match_sql: &str,
    values: &[SqlValue],
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
             e.session_pk,
             e.turn_pk,
             e.turn_index,
             e.event_type,
             e.llm_provider,
             e.llm_model,
             e.observed_at,
             snippet(usage_events_fts, 4, '{HIGHLIGHT_START}', '{HIGHLIGHT_END}', '…', 32),
             snippet(usage_events_fts, 7, '{HIGHLIGHT_START}', '{HIGHLIGHT_END}', '…', 32),
             snippet(usage_events_fts, 8, '{HIGHLIGHT_START}', '{HIGHLIGHT_END}', '…', 32),
             snippet(usage_events_fts, 5, '{HIGHLIGHT_START}', '{HIGHLIGHT_END}', '…', 32),
             snippet(usage_events_fts, 6, '{HIGHLIGHT_START}', '{HIGHLIGHT_END}', '…', 32)
         {match_sql}
           AND e.session_pk IN ({selected_placeholders})
         ORDER BY bm25(usage_events_fts) ASC, e.observed_at DESC, e.id ASC"
    );

    let mut parameters = values.to_vec();
    parameters.extend(
        session_ids
            .iter()
            .map(|session_pk| SqlValue::Text(session_pk.to_string())),
    );
    let mut statement = connection.prepare(&sql)?;
    let rows = statement
        .query_map(params_from_iter(parameters.iter()), map_match)?
        .collect::<Result<Vec<_>, _>>()?;
    let mut by_session: HashMap<Uuid, (u64, Vec<MatchedEvent>)> = HashMap::new();
    for matched in rows {
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

fn map_match(row: &Row<'_>) -> rusqlite::Result<MatchedEvent> {
    let mut fragments = Vec::new();
    for index in 8..13 {
        if let Some(fragment) = row.get::<_, Option<String>>(index)?
            && fragment.contains(HIGHLIGHT_START)
            && fragments.len() < 2
        {
            fragments.push(fragment);
        }
    }
    Ok(MatchedEvent {
        event_pk: parse_uuid_value(&row.get::<_, String>(0)?, 0)?,
        session_pk: parse_uuid_value(&row.get::<_, String>(1)?, 1)?,
        turn_pk: parse_uuid_value(&row.get::<_, String>(2)?, 2)?,
        turn_index: row.get(3)?,
        event_type: row.get(4)?,
        llm_provider: row.get(5)?,
        llm_model: row.get(6)?,
        observed_at: timestamp_from_micros(row.get(7)?, 7)?,
        match_count: 0,
        fragments,
    })
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
    connection: &Connection,
    user_id: Uuid,
    session_ids: &[Uuid],
) -> Result<HashMap<Uuid, SessionDetails>, AppError> {
    if session_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let placeholders = std::iter::repeat_n("?", session_ids.len())
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "SELECT s.id, s.session_id, s.agent_name, s.agent_version,
                d.host_name, d.platform, s.started_at, s.ended_at
         FROM agent_sessions s
         INNER JOIN devices d ON d.id = s.device_context_id
         WHERE s.user_id = ? AND s.id IN ({placeholders})"
    );
    let mut values = vec![SqlValue::Text(user_id.to_string())];
    values.extend(
        session_ids
            .iter()
            .map(|session_pk| SqlValue::Text(session_pk.to_string())),
    );
    let mut statement = connection.prepare(&sql)?;
    let rows = statement
        .query_map(params_from_iter(values.iter()), |row| {
            let session_pk = parse_uuid_value(&row.get::<_, String>(0)?, 0)?;
            let ended_at = row
                .get::<_, Option<i64>>(7)?
                .map(|value| timestamp_from_micros(value, 7))
                .transpose()?;
            Ok((
                session_pk,
                SessionDetails {
                    session_id: row.get(1)?,
                    agent_name: row.get(2)?,
                    agent_version: row.get(3)?,
                    host_name: row.get(4)?,
                    platform: row.get(5)?,
                    started_at: timestamp_from_micros(row.get(6)?, 6)?,
                    ended_at,
                },
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows.into_iter().collect())
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
