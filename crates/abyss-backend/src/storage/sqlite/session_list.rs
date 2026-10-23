//! SQLite session index ordered by latest activity.

use diesel::{
    QueryableByName, RunQueryDsl, SqliteConnection, sql_query,
    sql_types::{BigInt, Bool, Integer, Nullable, Text},
};
use uuid::Uuid;

use crate::{
    error::AppError,
    usage::{
        session_list::{SessionListItem, SessionListResponse, ValidatedSessionListQuery},
        streams::EntryLabel,
    },
};

use super::{
    models::{parse_uuid, timestamp_from_micros},
    repository::timestamp_to_micros,
};

/// Per-session activity with the filters that apply before paging.
///
/// Bind order: `?1` owner, `?2`/`?3` agent filter flag and JSON name list,
/// `?4`/`?5` lower activity bound, `?6`/`?7` upper start bound, `?8`/`?9`
/// JSON provider list and `?10`/`?11` JSON model list a session must have used
/// at least one of.
const ACTIVITY_CTE: &str = "\
    WITH activity AS ( \
        SELECT \
            s.id AS session_pk, \
            s.session_id AS session_id, \
            s.agent_name AS agent_name, \
            s.agent_version AS agent_version, \
            d.host_name AS host_name, \
            d.platform AS platform, \
            s.started_at AS started_at, \
            s.ended_at AS ended_at, \
            COALESCE(MAX(e.observed_at), s.started_at) AS last_activity_at, \
            (SELECT CASE WHEN COUNT(tf.turn_id) > 0 THEN COUNT(DISTINCT tf.turn_id) \
                    ELSE COUNT(*) FILTER (WHERE tf.starts_turn = 1) END \
             FROM llm_usage_events te \
             JOIN llm_usage_event_stream_facts tf ON tf.event_pk = te.id \
             WHERE te.session_pk = s.id) AS turns, \
            COUNT(e.session_pk) AS events, \
            COALESCE(SUM(e.total_tokens), 0) AS total_tokens, \
            title_event.id AS title_event_pk, \
            title_facts.label_index AS title_index, \
            title_facts.label_text AS title_text, \
            title_facts.label_length AS title_length, \
            st.purpose AS purpose \
        FROM agent_sessions s \
        JOIN devices d ON d.id = s.device_context_id \
        LEFT JOIN session_titles st ON st.session_pk = s.id AND st.user_id = s.user_id \
        LEFT JOIN llm_usage_events title_event ON title_event.id = st.title_event_pk \
            AND title_event.session_pk = s.id AND title_event.event_type = 'request' \
        LEFT JOIN llm_usage_event_stream_facts title_facts ON title_facts.event_pk = title_event.id \
        LEFT JOIN llm_usage_events e ON e.session_pk = s.id \
        WHERE s.user_id = ?1 \
            AND (?2 = 0 OR s.agent_name IN (SELECT value FROM json_each(?3))) \
            AND (?8 = 0 OR EXISTS ( \
                SELECT 1 FROM llm_usage_events p \
                WHERE p.session_pk = s.id \
                    AND p.llm_provider IN (SELECT value FROM json_each(?9)))) \
            AND (?10 = 0 OR EXISTS ( \
                SELECT 1 FROM llm_usage_events m \
                WHERE m.session_pk = s.id \
                    AND m.llm_model IN (SELECT value FROM json_each(?11)))) \
        GROUP BY s.id \
    ), \
    filtered AS ( \
        SELECT * FROM activity \
        WHERE (?4 = 0 OR last_activity_at >= ?5) \
            AND (?6 = 0 OR started_at < ?7) \
    ) ";

#[derive(QueryableByName)]
struct SessionListRow {
    #[diesel(sql_type = Text)]
    session_pk: String,
    #[diesel(sql_type = Text)]
    session_id: String,
    #[diesel(sql_type = Text)]
    agent_name: String,
    #[diesel(sql_type = Nullable<Text>)]
    agent_version: Option<String>,
    #[diesel(sql_type = Text)]
    host_name: String,
    #[diesel(sql_type = Text)]
    platform: String,
    #[diesel(sql_type = BigInt)]
    started_at: i64,
    #[diesel(sql_type = Nullable<BigInt>)]
    ended_at: Option<i64>,
    #[diesel(sql_type = BigInt)]
    last_activity_at: i64,
    #[diesel(sql_type = BigInt)]
    turns: i64,
    #[diesel(sql_type = BigInt)]
    events: i64,
    #[diesel(sql_type = BigInt)]
    total_tokens: i64,
    #[diesel(sql_type = Nullable<Text>)]
    title_event_pk: Option<String>,
    #[diesel(sql_type = Nullable<Integer>)]
    title_index: Option<i32>,
    #[diesel(sql_type = Nullable<Text>)]
    title_text: Option<String>,
    #[diesel(sql_type = Nullable<Integer>)]
    title_length: Option<i32>,
    #[diesel(sql_type = Nullable<Text>)]
    purpose: Option<String>,
}

#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    total: i64,
}

pub(super) fn list_sessions(
    connection: &mut SqliteConnection,
    user_id: Uuid,
    query: &ValidatedSessionListQuery,
) -> Result<SessionListResponse, AppError> {
    let agent_names = json_list(&query.agent_names, "agent")?;
    let providers = json_list(&query.llm_providers, "provider")?;
    let models = json_list(&query.llm_models, "model")?;
    let has_agents = !query.agent_names.is_empty();
    let from = query.from.map_or(0, timestamp_to_micros);
    let to = query.to.map_or(0, timestamp_to_micros);

    let total = sql_query(format!(
        "{ACTIVITY_CTE} SELECT COUNT(*) AS total FROM filtered"
    ))
    .bind::<Text, _>(user_id.to_string())
    .bind::<Bool, _>(has_agents)
    .bind::<Text, _>(&agent_names)
    .bind::<Bool, _>(query.from.is_some())
    .bind::<BigInt, _>(from)
    .bind::<Bool, _>(query.to.is_some())
    .bind::<BigInt, _>(to)
    .bind::<Bool, _>(!query.llm_providers.is_empty())
    .bind::<Text, _>(&providers)
    .bind::<Bool, _>(!query.llm_models.is_empty())
    .bind::<Text, _>(&models)
    .get_result::<CountRow>(connection)?
    .total;

    let rows = sql_query(format!(
        "{ACTIVITY_CTE} SELECT * FROM filtered \
         ORDER BY last_activity_at DESC, session_pk DESC LIMIT ?12 OFFSET ?13"
    ))
    .bind::<Text, _>(user_id.to_string())
    .bind::<Bool, _>(has_agents)
    .bind::<Text, _>(&agent_names)
    .bind::<Bool, _>(query.from.is_some())
    .bind::<BigInt, _>(from)
    .bind::<Bool, _>(query.to.is_some())
    .bind::<BigInt, _>(to)
    .bind::<Bool, _>(!query.llm_providers.is_empty())
    .bind::<Text, _>(&providers)
    .bind::<Bool, _>(!query.llm_models.is_empty())
    .bind::<Text, _>(&models)
    .bind::<BigInt, _>(i64::from(query.page_size))
    .bind::<BigInt, _>(query.offset)
    .load::<SessionListRow>(connection)?;

    let items = rows
        .into_iter()
        .map(|row| {
            Ok(SessionListItem {
                session_pk: parse_uuid(&row.session_pk, "session id")?,
                session_id: row.session_id,
                agent_name: row.agent_name,
                agent_version: row.agent_version,
                host_name: row.host_name,
                platform: row.platform,
                started_at: timestamp_from_micros(row.started_at)?,
                ended_at: row.ended_at.map(timestamp_from_micros).transpose()?,
                last_activity_at: timestamp_from_micros(row.last_activity_at)?,
                turns: row.turns,
                events: row.events,
                total_tokens: row.total_tokens,
                title: EntryLabel::from_columns(
                    row.title_event_pk
                        .as_deref()
                        .map(|pk| parse_uuid(pk, "title event id"))
                        .transpose()?,
                    row.title_index,
                    row.title_text,
                    row.title_length,
                ),
                purpose: row.purpose,
            })
        })
        .collect::<Result<Vec<_>, AppError>>()?;
    Ok(SessionListResponse::new(query, total, items))
}

fn json_list(values: &[String], filter: &str) -> Result<String, AppError> {
    serde_json::to_string(values)
        .map_err(|error| AppError::internal(format!("serialize {filter} filter: {error}")))
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use diesel::{Connection, SqliteConnection};
    use serde_json::{Value, json};
    use uuid::Uuid;

    use crate::usage::{IngestEventsRequest, session_list::SessionListQuery};

    use super::{super::migrations, super::repository::ingest_events, list_sessions};

    const OWNER_ID: Uuid = Uuid::from_u128(1);

    fn database() -> SqliteConnection {
        let mut database = SqliteConnection::establish(":memory:").expect("SQLite should open");
        migrations::run(&mut database).expect("migration should succeed");
        database
    }

    fn event(id: &str, session: &str, agent: &str, observed_at: &str, turn: i32) -> Value {
        event_with_model(id, session, agent, observed_at, turn, "gpt-5")
    }

    fn event_with_model(
        id: &str,
        session: &str,
        agent: &str,
        observed_at: &str,
        turn: i32,
        model: &str,
    ) -> Value {
        json!({
            "event_id": id,
            "observed_at": observed_at,
            "device": {"host_name": "alice-mbp", "platform": "macos"},
            "agent": {"name": agent},
            "session_id": session,
            "llm": {"provider": "OpenAI", "model": model},
            "event_type": "request",
            "token_usage": {"input_tokens": 0_i64, "output_tokens": 10_i64, "cache_read_tokens": 0_i64, "cache_write_tokens": 0_i64, "reasoning_tokens": 0_i64, "total_tokens": 10_i64},
            // Each event asks in words, so each opens a turn.
            "metadata": {"conversation": {
                "entries": [{"role": "user", "text": format!("ask {turn}")}],
                "completeness": "complete"
            }}
        })
    }

    fn seeded() -> SqliteConnection {
        let mut database = database();
        let request: IngestEventsRequest = serde_json::from_value(json!({
            "events": [
                event("a1", "alpha", "codex", "2026-09-01T00:00:00Z", 1),
                event_with_model("a2", "alpha", "codex", "2026-09-03T00:00:00Z", 2, "gpt-5-mini"),
                event("b1", "beta", "claude-desktop", "2026-09-02T00:00:00Z", 1),
                event("c1", "gamma", "codex", "2026-08-01T00:00:00Z", 1),
            ]
        }))
        .expect("request should deserialize");
        ingest_events(&mut database, &request, OWNER_ID, 100).expect("ingest should succeed");
        database
    }

    fn list(database: &mut SqliteConnection, query: SessionListQuery) -> Vec<(String, i64, i64)> {
        let query = query.validate().expect("query should validate");
        list_sessions(database, OWNER_ID, &query)
            .expect("sessions should list")
            .items
            .into_iter()
            .map(|item| (item.session_id, item.turns, item.events))
            .collect()
    }

    #[test]
    fn lists_sessions_by_latest_activity_with_counts() {
        let mut database = seeded();
        let query = SessionListQuery::default().validate().expect("valid");
        let response = list_sessions(&mut database, OWNER_ID, &query).expect("should list");

        assert_eq!(response.total_sessions, 3);
        assert!(!response.has_more);
        let alpha = &response.items[0];
        assert_eq!(alpha.session_id, "alpha");
        assert_eq!((alpha.turns, alpha.events, alpha.total_tokens), (2, 2, 20));
        assert_eq!(alpha.host_name, "alice-mbp");
        assert_eq!(
            alpha.last_activity_at,
            Utc.with_ymd_and_hms(2026, 9, 3, 0, 0, 0).unwrap()
        );
        let order: Vec<_> = response
            .items
            .iter()
            .map(|item| item.session_id.as_str())
            .collect();
        assert_eq!(order, ["alpha", "beta", "gamma"]);
    }

    #[test]
    fn filters_by_exact_agent_and_activity_window() {
        let mut database = seeded();
        let desktop = SessionListQuery {
            agent_name: vec!["claude-desktop".to_owned()],
            ..SessionListQuery::default()
        };
        assert_eq!(list(&mut database, desktop), [("beta".to_owned(), 1, 1)]);
        // Claude Desktop is not an alias of Claude Code, and case is not folded.
        for agent in ["claude-code", "Codex"] {
            let other = SessionListQuery {
                agent_name: vec![agent.to_owned()],
                ..SessionListQuery::default()
            };
            assert_eq!(list(&mut database, other), Vec::new(), "{agent}");
        }
        let either = SessionListQuery {
            agent_name: vec!["claude-desktop".to_owned(), "codex".to_owned()],
            ..SessionListQuery::default()
        };
        assert_eq!(list(&mut database, either).len(), 3);

        // gamma went quiet before the window; alpha is still active inside it.
        let window = SessionListQuery {
            from: Some(Utc.with_ymd_and_hms(2026, 8, 15, 0, 0, 0).unwrap()),
            to: Some(Utc.with_ymd_and_hms(2026, 9, 2, 12, 0, 0).unwrap()),
            ..SessionListQuery::default()
        };
        let ids: Vec<_> = list(&mut database, window)
            .into_iter()
            .map(|row| row.0)
            .collect();
        assert_eq!(ids, ["alpha", "beta"]);
    }

    #[test]
    fn filters_by_models_or_providers_the_session_used() {
        let mut database = seeded();
        let mini = SessionListQuery {
            llm_model: vec!["gpt-5-mini".to_owned(), "unused-model".to_owned()],
            ..SessionListQuery::default()
        };
        let ids: Vec<_> = list(&mut database, mini)
            .into_iter()
            .map(|row| row.0)
            .collect();
        assert_eq!(ids, ["alpha"]);

        let openai = SessionListQuery {
            llm_provider: vec!["anthropic".to_owned(), "OpenAI".to_owned()],
            ..SessionListQuery::default()
        };
        assert_eq!(list(&mut database, openai).len(), 3);

        let other = SessionListQuery {
            llm_provider: vec!["anthropic".to_owned()],
            ..SessionListQuery::default()
        };
        assert_eq!(list(&mut database, other), Vec::new());
    }

    #[test]
    fn pages_through_sessions() {
        let mut database = seeded();
        let first = SessionListQuery {
            page_size: Some(2),
            ..SessionListQuery::default()
        }
        .validate()
        .expect("valid");
        let page = list_sessions(&mut database, OWNER_ID, &first).expect("should list");
        assert_eq!(
            (page.items.len(), page.total_sessions, page.has_more),
            (2, 3, true)
        );

        let second = SessionListQuery {
            page: Some(2),
            page_size: Some(2),
            ..SessionListQuery::default()
        }
        .validate()
        .expect("valid");
        let page = list_sessions(&mut database, OWNER_ID, &second).expect("should list");
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].session_id, "gamma");
        assert!(!page.has_more);
    }
}
