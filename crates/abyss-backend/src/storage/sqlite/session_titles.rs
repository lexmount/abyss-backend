//! SQLite persistence of session names reported by the collector.

use chrono::Utc;
use diesel::{ExpressionMethods, OptionalExtension, QueryDsl, RunQueryDsl, SqliteConnection};
use serde_json::Value;
use uuid::Uuid;

use super::{
    models::parse_uuid,
    schema::{llm_usage_events, session_titles},
};
use crate::{
    error::AppError,
    usage::{
        SessionInfo,
        session_title::{TITLE_GENERATION_PURPOSE, TitleContribution, default_title},
    },
};

/// Stored title, purpose, and matched task session of one session.
type StoredTitle = (Option<String>, Option<String>, Option<Uuid>);

/// One stored event considered for title capture.
pub(super) struct CapturedEvent<'a> {
    pub(super) user_id: Uuid,
    pub(super) session_pk: Uuid,
    pub(super) event_pk: Uuid,
    pub(super) event_type: &'a str,
    pub(super) metadata: &'a Value,
    pub(super) naming_thread: bool,
    pub(super) observed_at: i64,
}

/// Records what a newly stored event says about its session's name.
///
/// An Agent title replaces a default title and an older Agent title. A default
/// title keeps the earliest user input, even after late delivery, and never
/// replaces an Agent title.
pub(super) fn capture(
    connection: &mut SqliteConnection,
    event: &CapturedEvent<'_>,
) -> Result<(), AppError> {
    let session = event.session_pk.to_string();
    let agent_title =
        match TitleContribution::of(event.event_type, event.metadata, event.naming_thread) {
            None => return Ok(()),
            Some(TitleContribution::TitleThread) => {
                return mark_title_thread(connection, &session, event.user_id);
            }
            Some(TitleContribution::Agent(title)) => Some(title),
            Some(TitleContribution::UserInput) => None,
        };
    let stored = session_titles::table
        .find(&session)
        .select((
            session_titles::title_event_pk,
            session_titles::title_observed_at,
        ))
        .first::<(Option<String>, Option<i64>)>(connection)
        .optional()?
        .and_then(|(event_pk, observed_at)| event_pk.zip(observed_at));
    let agent = agent_title.is_some();
    let replaces = match stored {
        None => true,
        Some((_, stored_at)) if agent && stored_at < event.observed_at => true,
        Some((_, stored_at)) if !agent && stored_at <= event.observed_at => false,
        // Otherwise only a stored default title gives way.
        Some((event_pk, _)) => !is_agent_title(connection, &event_pk)?,
    };
    if !replaces {
        return Ok(());
    }
    // Read the user input only once it could name the session.
    let Some(title) = agent_title
        .map(str::to_owned)
        .or_else(|| default_title(event.metadata))
    else {
        return Ok(());
    };
    let now = Utc::now().timestamp_micros();
    diesel::insert_into(session_titles::table)
        .values((
            session_titles::session_pk.eq(&session),
            session_titles::user_id.eq(event.user_id.to_string()),
            session_titles::title.eq(&title),
            session_titles::title_event_pk.eq(event.event_pk.to_string()),
            session_titles::title_observed_at.eq(event.observed_at),
            session_titles::updated_at.eq(now),
        ))
        .on_conflict(session_titles::session_pk)
        .do_update()
        .set((
            session_titles::title.eq(&title),
            session_titles::title_event_pk.eq(event.event_pk.to_string()),
            session_titles::title_observed_at.eq(event.observed_at),
            session_titles::updated_at.eq(now),
        ))
        .execute(connection)?;
    Ok(())
}

/// Agent titles are read from responses and default titles from requests, so
/// the side of the event that gave the stored title tells them apart.
fn is_agent_title(connection: &mut SqliteConnection, event_pk: &str) -> Result<bool, AppError> {
    Ok(llm_usage_events::table
        .find(event_pk)
        .select(llm_usage_events::event_type)
        .first::<String>(connection)
        .optional()?
        .is_some_and(|event_type| event_type == "response"))
}

/// Marks a session as a thread that names another session.
fn mark_title_thread(
    connection: &mut SqliteConnection,
    session: &str,
    user_id: Uuid,
) -> Result<(), AppError> {
    let now = Utc::now().timestamp_micros();
    diesel::insert_into(session_titles::table)
        .values((
            session_titles::session_pk.eq(session),
            session_titles::user_id.eq(user_id.to_string()),
            session_titles::purpose.eq(TITLE_GENERATION_PURPOSE),
            session_titles::updated_at.eq(now),
        ))
        .on_conflict(session_titles::session_pk)
        .do_update()
        .set((
            session_titles::purpose.eq(TITLE_GENERATION_PURPOSE),
            session_titles::updated_at.eq(now),
        ))
        .execute(connection)?;
    Ok(())
}

/// Adds the stored title, purpose, and matched task session to a session.
pub(super) fn fill(
    connection: &mut SqliteConnection,
    session: &mut SessionInfo,
) -> Result<(), AppError> {
    (session.title, session.purpose, session.parent_session_pk) =
        load(connection, session.session_pk)?;
    Ok(())
}

/// Returns the stored title and purpose of one session, and for a naming
/// thread the task session it was matched to.
fn load(connection: &mut SqliteConnection, session_pk: Uuid) -> Result<StoredTitle, AppError> {
    let Some((title, purpose, parent)) = session_titles::table
        .find(session_pk.to_string())
        .select((
            session_titles::title,
            session_titles::purpose,
            session_titles::parent_session_pk,
        ))
        .first::<(Option<String>, Option<String>, Option<String>)>(connection)
        .optional()?
    else {
        return Ok((None, None, None));
    };
    let parent = parent
        .map(|parent| parse_uuid(&parent, "parent session id"))
        .transpose()?;
    Ok((title, purpose, parent))
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
        event(
            agent,
            session,
            id,
            at,
            "request",
            json!({"conversation": {
                "entries": [{"role": "user", "text": input}],
                "completeness": "complete"
            }}),
        )
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
        (title, purpose)
    }

    #[test]
    fn names_a_session_from_the_collector_title() {
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
            (Some("Build fix".to_owned()), None)
        );
    }

    #[test]
    fn agent_titles_outrank_default_titles_in_any_arrival_order() {
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
            // An earlier user request delivered late does not replace the title.
            if title_first {
                events.reverse();
            }
            for value in events {
                ingest(&mut database, &[value]);
            }
            assert_eq!(
                session_title(&mut database, "cc").0.as_deref(),
                Some("Build fix"),
                "title first: {title_first}"
            );
        }
    }

    #[test]
    fn keeps_the_newest_agent_title() {
        let mut database = database();
        ingest(
            &mut database,
            &[
                titled_response("cc", "s2", "2026-09-30T02:00:01Z", "Newer"),
                titled_response("cc", "s1", "2026-09-30T01:00:01Z", "Older"),
            ],
        );
        assert_eq!(
            session_title(&mut database, "cc").0.as_deref(),
            Some("Newer")
        );
        ingest(
            &mut database,
            &[titled_response(
                "cc",
                "s3",
                "2026-09-30T03:00:01Z",
                "Newest",
            )],
        );
        assert_eq!(
            session_title(&mut database, "cc").0.as_deref(),
            Some("Newest")
        );
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
            json!({"conversation": {
                "entries": [{"role": "assistant", "text": "{\"title\":\"Quoted\"}"}],
                "completeness": "complete"
            }}),
        );
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
    fn default_titles_keep_the_earliest_user_request_even_when_delivered_late() {
        let mut database = database();
        for (id, at, kind, input, expected) in [
            ("r2", "2026-09-30T00:00:02Z", "request", "Second", "Second"),
            ("r3", "2026-09-30T00:00:03Z", "request", "Third", "Second"),
            ("r1", "2026-09-30T00:00:01Z", "request", "First", "First"),
            (
                "s0",
                "2026-09-30T00:00:00Z",
                "response",
                "Response",
                "First",
            ),
        ] {
            let mut value = request("cursor", "default", id, at, input);
            value["event_type"] = json!(kind);
            ingest(&mut database, &[value]);
            assert_eq!(
                session_title(&mut database, "default").0.as_deref(),
                Some(expected)
            );
        }
    }

    #[test]
    fn codex_task_sessions_keep_their_first_input_and_naming_threads_are_marked() {
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
            (None, Some("title_generation".to_owned()))
        );
    }
}
