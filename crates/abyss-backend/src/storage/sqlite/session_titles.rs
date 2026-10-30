//! SQLite registration of each session's earliest request and its stored label.

use std::collections::HashMap;

use chrono::Utc;
use diesel::{
    ExpressionMethods, OptionalExtension, QueryDsl, QueryableByName, RunQueryDsl, SqliteConnection,
    sql_query,
    sql_types::{Integer, Nullable, Text},
};
use uuid::Uuid;

use super::{models::parse_uuid, schema::session_titles};
use crate::{error::AppError, usage::streams::EntryLabel};

/// First-request label, purpose, and matched task session.
type StoredTitle = (Option<EntryLabel>, Option<String>, Option<Uuid>);

/// One stored event considered for the first-request registration.
pub(super) struct CapturedEvent<'a> {
    pub(super) user_id: Uuid,
    pub(super) session_pk: Uuid,
    pub(super) event_pk: Uuid,
    pub(super) event_type: &'a str,
    pub(super) observed_at: i64,
}

/// Keeps the earliest request even when it has no label, including late delivery.
/// Returns whether the registered request changed.
pub(super) fn capture(
    connection: &mut SqliteConnection,
    event: &CapturedEvent<'_>,
) -> Result<bool, AppError> {
    if event.event_type != "request" {
        return Ok(false);
    }
    let session = event.session_pk.to_string();
    let event_pk = event.event_pk.to_string();
    let stored = session_titles::table
        .find(&session)
        .select((
            session_titles::title_observed_at,
            session_titles::title_event_pk,
        ))
        .first::<(Option<i64>, Option<String>)>(connection)
        .optional()?
        .and_then(|(at, pk)| at.zip(pk));
    if stored.is_some_and(|(at, pk)| (at, pk) <= (event.observed_at, event_pk.clone())) {
        return Ok(false);
    }
    let now = Utc::now().timestamp_micros();
    diesel::insert_into(session_titles::table)
        .values((
            session_titles::session_pk.eq(&session),
            session_titles::user_id.eq(event.user_id.to_string()),
            session_titles::title.eq(None::<String>),
            session_titles::title_event_pk.eq(&event_pk),
            session_titles::title_observed_at.eq(event.observed_at),
            session_titles::updated_at.eq(now),
        ))
        .on_conflict(session_titles::session_pk)
        .do_update()
        .set((
            session_titles::title.eq(None::<String>),
            session_titles::title_event_pk.eq(&event_pk),
            session_titles::title_observed_at.eq(event.observed_at),
            session_titles::updated_at.eq(now),
        ))
        .execute(connection)?;
    Ok(true)
}

const TITLE_SELECT: &str = "\
    SELECT s.id AS session_pk, e.id AS event_pk, \
        f.label_index, f.label_text, f.label_length, st.purpose, st.parent_session_pk \
    FROM session_titles st \
    JOIN agent_sessions s ON s.id = st.session_pk AND s.user_id = st.user_id \
    LEFT JOIN llm_usage_events e ON e.id = st.title_event_pk \
        AND e.session_pk = s.id AND e.event_type = 'request' \
    LEFT JOIN llm_usage_event_stream_facts f ON f.event_pk = e.id";

#[derive(QueryableByName)]
struct TitleRow {
    #[diesel(sql_type = Text)]
    session_pk: String,
    #[diesel(sql_type = Nullable<Text>)]
    event_pk: Option<String>,
    #[diesel(sql_type = Nullable<Integer>)]
    label_index: Option<i32>,
    #[diesel(sql_type = Nullable<Text>)]
    label_text: Option<String>,
    #[diesel(sql_type = Nullable<Integer>)]
    label_length: Option<i32>,
    #[diesel(sql_type = Nullable<Text>)]
    purpose: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    parent_session_pk: Option<String>,
}

impl TitleRow {
    fn into_title(self) -> Result<StoredTitle, AppError> {
        let event_pk = self
            .event_pk
            .as_deref()
            .map(|pk| parse_uuid(pk, "title event id"))
            .transpose()?;
        let label = EntryLabel::from_columns(
            event_pk,
            self.label_index,
            self.label_text,
            self.label_length,
        );
        let parent = self
            .parent_session_pk
            .as_deref()
            .map(|pk| parse_uuid(pk, "parent session id"))
            .transpose()?;
        Ok((label, self.purpose, parent))
    }
}

/// Labels of an authorized result page, fetched in one query.
pub(super) fn titles(
    connection: &mut SqliteConnection,
    owner: Uuid,
    sessions: &[Uuid],
) -> Result<HashMap<Uuid, EntryLabel>, AppError> {
    if sessions.is_empty() {
        return Ok(HashMap::new());
    }
    let sessions = serde_json::to_string(sessions)
        .map_err(|error| AppError::internal(format!("serialize title session ids: {error}")))?;
    let rows = sql_query(format!(
        "{TITLE_SELECT} WHERE s.user_id = ? AND s.id IN (SELECT value FROM json_each(?))"
    ))
    .bind::<Text, _>(owner.to_string())
    .bind::<Text, _>(sessions)
    .load::<TitleRow>(connection)?;
    let mut titles = HashMap::new();
    for row in rows {
        let session = parse_uuid(&row.session_pk, "session id")?;
        if let Some(label) = row.into_title()?.0 {
            titles.insert(session, label);
        }
    }
    Ok(titles)
}

/// The registered request's label, shared with Turn labels.
pub(super) fn title(
    connection: &mut SqliteConnection,
    session_pk: Uuid,
) -> Result<Option<EntryLabel>, AppError> {
    Ok(load(connection, session_pk)?.0)
}

fn load(connection: &mut SqliteConnection, session_pk: Uuid) -> Result<StoredTitle, AppError> {
    sql_query(format!("{TITLE_SELECT} WHERE s.id = ?"))
        .bind::<Text, _>(session_pk.to_string())
        .get_result::<TitleRow>(connection)
        .optional()?
        .map_or(Ok((None, None, None)), TitleRow::into_title)
}
#[cfg(test)]
mod tests {
    use diesel::{Connection, ExpressionMethods, QueryDsl, RunQueryDsl, SqliteConnection};
    use serde_json::{Value, json};
    use uuid::Uuid;

    use super::{
        super::{migrations, repository::ingest_events, schema::agent_sessions},
        load,
    };
    use crate::usage::IngestEventsRequest;

    const OWNER_ID: Uuid = Uuid::from_u128(1);

    fn database() -> SqliteConnection {
        let mut database = SqliteConnection::establish(":memory:").expect("SQLite should open");
        migrations::run(&mut database).expect("migration should succeed");
        database
    }

    /// A request whose collector conversation holds one user entry.
    fn request(agent: &str, session: &str, id: &str, at: &str, input: &str) -> Value {
        let mut request = event(agent, session, id, at, "request", json!({}));
        request["conversation"] = json!({
            "entries": [{"role": "user", "text": input}],
            "completeness": "complete"
        });
        request
    }

    /// The response of a naming call, carrying the collector's title.
    fn titled_response(session: &str, id: &str, at: &str, title: &str) -> Value {
        event(
            "claude-code",
            session,
            id,
            at,
            "response",
            json!({"purpose": "session_title", "session_title": title}),
        )
    }

    fn event(agent: &str, session: &str, id: &str, at: &str, kind: &str, metadata: Value) -> Value {
        let mut event = json!({
            "event_id": id,
            "observed_at": at,
            "device": {"host_name": "local", "platform": "macos"},
            "agent": {"name": agent},
            "session_id": session,
            "llm": {"provider": "anthropic", "model": "claude-haiku"},
            "event_type": kind,
            "token_usage": {"input_tokens": 0_i64, "output_tokens": 1_i64, "cache_read_tokens": 0_i64, "cache_write_tokens": 0_i64, "reasoning_tokens": 0_i64, "total_tokens": 1_i64}
        });
        event["metadata"] = metadata;
        event
    }

    fn ingest(database: &mut SqliteConnection, events: &[Value]) {
        let request: IngestEventsRequest =
            serde_json::from_value(json!({ "events": events })).expect("request should parse");
        ingest_events(database, &request, OWNER_ID, 100).expect("ingest should succeed");
    }

    fn session_title(
        database: &mut SqliteConnection,
        session: &str,
    ) -> (Option<String>, Option<String>) {
        let pk = agent_sessions::table
            .filter(agent_sessions::session_id.eq(session))
            .select(agent_sessions::id)
            .first::<String>(database)
            .expect("session should exist");
        let (title, purpose, _) =
            load(database, Uuid::parse_str(&pk).expect("uuid")).expect("title should load");
        (title.map(|label| label.text), purpose)
    }

    #[test]
    fn names_a_session_from_user_input_and_ignores_collector_titles() {
        let mut database = database();
        let mut naming = request(
            "claude-code",
            "cc",
            "r2",
            "2026-09-30T00:00:02Z",
            "<session>fix the build</session>",
        );
        naming["metadata"]["purpose"] = json!("session_title");
        ingest(
            &mut database,
            &[
                request(
                    "claude-code",
                    "cc",
                    "r1",
                    "2026-09-30T00:00:00Z",
                    "fix the build",
                ),
                naming,
                titled_response("cc", "s2", "2026-09-30T00:00:03Z", "Build fix"),
            ],
        );

        assert_eq!(
            session_title(&mut database, "cc"),
            (Some("fix the build".to_owned()), None)
        );
    }

    #[test]
    fn collector_titles_are_ignored_in_any_arrival_order() {
        for title_first in [false, true] {
            let mut database = database();
            let mut events = vec![
                request(
                    "claude-code",
                    "cc",
                    "r1",
                    "2026-09-30T00:00:00Z",
                    "fix the build",
                ),
                titled_response("cc", "s1", "2026-09-30T00:00:03Z", "Build fix"),
            ];
            // The user input remains authoritative even when a title response arrives first.
            if title_first {
                events.reverse();
            }
            for value in events {
                ingest(&mut database, &[value]);
            }
            assert_eq!(
                session_title(&mut database, "cc").0.as_deref(),
                Some("fix the build"),
                "title first: {title_first}"
            );
        }
    }

    #[test]
    fn responses_without_user_requests_have_no_title() {
        let mut database = database();
        ingest(
            &mut database,
            &[titled_response(
                "cc",
                "s1",
                "2026-09-30T01:00:01Z",
                "Generated title",
            )],
        );
        assert_eq!(session_title(&mut database, "cc"), (None, None));
    }

    #[test]
    fn title_like_replies_without_the_collector_field_are_ordinary_output() {
        let mut database = database();
        let mut reply = event(
            "claude-code",
            "cc",
            "s1",
            "2026-09-30T00:00:01Z",
            "response",
            json!({}),
        );
        reply["conversation"] = json!({
            "entries": [{"role": "assistant", "text": "{\"title\":\"Quoted\"}"}],
            "completeness": "complete"
        });
        reply["text"] = json!("{\"title\":\"Quoted\"}");
        ingest(
            &mut database,
            &[
                request("claude-code", "cc", "r1", "2026-09-30T00:00:00Z", "Name it"),
                reply,
            ],
        );
        assert_eq!(
            session_title(&mut database, "cc").0.as_deref(),
            Some("Name it")
        );
    }

    #[test]
    fn titles_keep_the_earliest_request_even_when_delivered_late() {
        let mut database = database();
        for (id, at, kind, input, expected) in [
            (
                "r2",
                "2026-09-30T00:00:02Z",
                "request",
                "Second",
                Some("Second"),
            ),
            (
                "r3",
                "2026-09-30T00:00:03Z",
                "request",
                "Third",
                Some("Second"),
            ),
            (
                "r1",
                "2026-09-30T00:00:01Z",
                "request",
                "First",
                Some("First"),
            ),
            (
                "s0",
                "2026-09-30T00:00:00Z",
                "response",
                "Response",
                Some("First"),
            ),
            ("r0", "2026-09-30T00:00:00Z", "request", "", None),
            ("r4", "2026-09-30T00:00:04Z", "request", "Fourth", None),
        ] {
            let mut value = request("cursor", "default", id, at, input);
            value["event_type"] = json!(kind);
            ingest(&mut database, &[value]);
            assert_eq!(
                session_title(&mut database, "default").0.as_deref(),
                expected
            );
        }
    }

    #[test]
    fn old_naming_flags_do_not_change_the_first_request_label_rule() {
        let mut database = database();
        let mut naming = request(
            "codex",
            "title-thread",
            "n1",
            "2026-09-30T00:00:01Z",
            "Generate a concise, single-line task title.\nUser prompt:\n画一只鹈鹕",
        );
        naming["naming_thread"] = json!(true);
        let task = request(
            "codex",
            "task",
            "t1",
            "2026-09-30T00:00:00Z",
            "<environment_context>…",
        );
        ingest(
            &mut database,
            &[
                task,
                naming,
                event(
                    "codex",
                    "title-thread",
                    "n2",
                    "2026-09-30T00:00:02Z",
                    "response",
                    json!({}),
                ),
            ],
        );

        assert_eq!(
            session_title(&mut database, "task").0.as_deref(),
            Some("<environment_context>…")
        );
        assert_eq!(
            session_title(&mut database, "title-thread"),
            (
                Some("Generate a concise, single-line task title.\nUser p".to_owned()),
                None
            )
        );
    }
}
