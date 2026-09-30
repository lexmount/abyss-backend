//! SQLite persistence of session names read from title-generation calls.

use chrono::Utc;
use diesel::{
    BoolExpressionMethods, ExpressionMethods, OptionalExtension, QueryDsl, RunQueryDsl,
    SqliteConnection, TextExpressionMethods,
};
use uuid::Uuid;

use super::{
    models::parse_uuid,
    schema::{llm_usage_events, schema_backfills, session_titles},
};
use crate::{
    error::AppError,
    usage::session_title::{
        TITLE_GENERATION_PURPOSE, TitleTask, may_carry_title, title_from_response,
    },
};

/// Marks the one-time scan of events stored before titles were captured.
const BACKFILL_NAME: &str = "session_titles_v1";

/// One stored event considered for title capture.
pub(super) struct CapturedEvent<'a> {
    pub(super) user_id: Uuid,
    pub(super) session_pk: Uuid,
    pub(super) event_pk: Uuid,
    pub(super) turn_index: i32,
    pub(super) event_type: &'a str,
    pub(super) text: Option<&'a str>,
    pub(super) observed_at: i64,
}

/// A naming call recognized from one of its events.
struct NamingCall {
    task: TitleTask,
    /// Title with the response event and observation time that carried it.
    title: Option<(String, String, i64)>,
}

/// Records the session title when the event completes a naming call.
///
/// A naming request and its response arrive as separate events in either
/// order, so each side looks up the other within the same turn.
pub(super) fn capture(
    connection: &mut SqliteConnection,
    event: &CapturedEvent<'_>,
) -> Result<(), AppError> {
    naming_call(connection, event)?.map_or_else(|| Ok(()), |call| store(connection, event, call))
}

fn naming_call(
    connection: &mut SqliteConnection,
    event: &CapturedEvent<'_>,
) -> Result<Option<NamingCall>, AppError> {
    let Some(text) = event.text else {
        return Ok(None);
    };
    let session = event.session_pk.to_string();
    match event.event_type {
        "request" => {
            let Some(task) = TitleTask::from_request(text) else {
                return Ok(None);
            };
            let response = llm_usage_events::table
                .filter(llm_usage_events::session_pk.eq(&session))
                .filter(llm_usage_events::turn_index.eq(event.turn_index))
                .filter(llm_usage_events::event_type.eq("response"))
                .filter(llm_usage_events::observed_at.ge(event.observed_at))
                .order_by(llm_usage_events::observed_at.asc())
                .select((
                    llm_usage_events::id,
                    llm_usage_events::text,
                    llm_usage_events::observed_at,
                ))
                .first::<(String, Option<String>, i64)>(connection)
                .optional()?;
            let title = response.and_then(|(id, text, observed_at)| {
                text.as_deref()
                    .and_then(title_from_response)
                    .map(|title| (title, id, observed_at))
            });
            Ok(Some(NamingCall { task, title }))
        }
        "response" if may_carry_title(text) => {
            let Some(title) = title_from_response(text) else {
                return Ok(None);
            };
            let request_text = llm_usage_events::table
                .filter(llm_usage_events::session_pk.eq(&session))
                .filter(llm_usage_events::turn_index.eq(event.turn_index))
                .filter(llm_usage_events::event_type.eq("request"))
                .filter(llm_usage_events::observed_at.le(event.observed_at))
                .order_by(llm_usage_events::observed_at.desc())
                .select(llm_usage_events::text)
                .first::<Option<String>>(connection)
                .optional()?
                .flatten();
            Ok(request_text
                .as_deref()
                .and_then(TitleTask::from_request)
                .map(|task| NamingCall {
                    task,
                    title: Some((title, event.event_pk.to_string(), event.observed_at)),
                }))
        }
        _ => Ok(None),
    }
}

fn store(
    connection: &mut SqliteConnection,
    event: &CapturedEvent<'_>,
    call: NamingCall,
) -> Result<(), AppError> {
    let session = event.session_pk.to_string();
    let now = Utc::now().timestamp_micros();
    let purpose =
        (call.task == TitleTask::AuxiliarySession).then(|| TITLE_GENERATION_PURPOSE.to_owned());
    let (title, title_event_pk, title_observed_at) = match call.title {
        Some((title, event_pk, observed_at)) => (Some(title), Some(event_pk), Some(observed_at)),
        None => (None, None, None),
    };
    let existing = session_titles::table
        .find(&session)
        .select((session_titles::title_observed_at, session_titles::purpose))
        .first::<(Option<i64>, Option<String>)>(connection)
        .optional()?;
    let Some((stored_at, stored_purpose)) = existing else {
        diesel::insert_into(session_titles::table)
            .values((
                session_titles::session_pk.eq(&session),
                session_titles::user_id.eq(event.user_id.to_string()),
                session_titles::title.eq(title),
                session_titles::title_event_pk.eq(title_event_pk),
                session_titles::title_observed_at.eq(title_observed_at),
                session_titles::purpose.eq(purpose),
                session_titles::updated_at.eq(now),
            ))
            .execute(connection)?;
        return Ok(());
    };
    // A late or replayed naming call must not replace a newer title.
    let newer = title.is_some() && stored_at.is_none_or(|stored| title_observed_at > Some(stored));
    let target = session_titles::table.find(&session);
    if newer {
        diesel::update(target)
            .set((
                session_titles::title.eq(title),
                session_titles::title_event_pk.eq(title_event_pk),
                session_titles::title_observed_at.eq(title_observed_at),
                session_titles::purpose.eq(purpose.or(stored_purpose)),
                session_titles::updated_at.eq(now),
            ))
            .execute(connection)?;
    } else if stored_purpose.is_none() && purpose.is_some() {
        diesel::update(target)
            .set((
                session_titles::purpose.eq(purpose),
                session_titles::updated_at.eq(now),
            ))
            .execute(connection)?;
    }
    Ok(())
}

/// Captures titles from events stored before capture existed, once.
pub(super) fn backfill(connection: &mut SqliteConnection) -> Result<(), AppError> {
    let done = schema_backfills::table
        .find(BACKFILL_NAME)
        .select(schema_backfills::name)
        .first::<String>(connection)
        .optional()?
        .is_some();
    if done {
        return Ok(());
    }
    connection.immediate_transaction(|transaction| {
        let requests =
            llm_usage_events::table
                .filter(llm_usage_events::event_type.eq("request"))
                .filter(
                    llm_usage_events::text
                        .like("%You are naming a coding session%")
                        .or(llm_usage_events::text
                            .like("%Generate a concise, single-line task title%")),
                )
                .order_by(llm_usage_events::observed_at.asc())
                .select((
                    llm_usage_events::id,
                    llm_usage_events::user_id,
                    llm_usage_events::session_pk,
                    llm_usage_events::turn_index,
                    llm_usage_events::text,
                    llm_usage_events::observed_at,
                ))
                .load::<(String, String, String, i32, Option<String>, i64)>(transaction)?;
        for (event_pk, user_id, session_pk, turn_index, text, observed_at) in requests {
            capture(
                transaction,
                &CapturedEvent {
                    user_id: parse_uuid(&user_id, "event user id")?,
                    session_pk: parse_uuid(&session_pk, "event session id")?,
                    event_pk: parse_uuid(&event_pk, "event id")?,
                    turn_index,
                    event_type: "request",
                    text: text.as_deref(),
                    observed_at,
                },
            )?;
        }
        diesel::insert_into(schema_backfills::table)
            .values((
                schema_backfills::name.eq(BACKFILL_NAME),
                schema_backfills::completed_at.eq(Utc::now().timestamp_micros()),
            ))
            .execute(transaction)?;
        Ok::<_, AppError>(())
    })
}

/// Returns the stored title and purpose of one session.
pub(super) fn load(
    connection: &mut SqliteConnection,
    session_pk: Uuid,
) -> Result<(Option<String>, Option<String>), AppError> {
    Ok(session_titles::table
        .find(session_pk.to_string())
        .select((session_titles::title, session_titles::purpose))
        .first::<(Option<String>, Option<String>)>(connection)
        .optional()?
        .unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use diesel::{Connection, ExpressionMethods, QueryDsl, RunQueryDsl, SqliteConnection};
    use serde_json::{Value, json};
    use uuid::Uuid;

    use super::{
        super::{
            migrations,
            repository::ingest_events,
            schema::{agent_sessions, schema_backfills, session_titles},
        },
        backfill, load,
    };
    use crate::usage::IngestEventsRequest;

    const OWNER_ID: Uuid = Uuid::from_u128(1);
    const CLAUDE_NAMING: &str = "You are naming a coding session so the user can pick it out";
    const CODEX_NAMING: &str = "Generate a concise, single-line task title of at most 36";

    fn database() -> SqliteConnection {
        let mut database = SqliteConnection::establish(":memory:").expect("SQLite should open");
        migrations::run(&mut database).expect("migration should succeed");
        database
    }

    fn event(agent: &str, session: &str, id: &str, at: &str, kind: &str, text: &str) -> Value {
        json!({
            "event_id": id,
            "observed_at": at,
            "device": {"host_name": "local", "platform": "macos"},
            "agent": {"name": agent},
            "session_id": session,
            "turn_index": 2,
            "llm": {"provider": "anthropic", "model": "claude-haiku"},
            "event_type": kind,
            "text": text,
            "token_usage": {"output_tokens": 1},
            "metadata": {}
        })
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
        load(database, Uuid::parse_str(&pk).expect("uuid")).expect("title should load")
    }

    #[test]
    fn names_a_claude_code_session_from_its_own_naming_call() {
        let mut database = database();
        ingest(
            &mut database,
            &[
                event(
                    "claude-code",
                    "cc",
                    "r1",
                    "2026-09-30T00:00:00Z",
                    "request",
                    "fix the build",
                ),
                event(
                    "claude-code",
                    "cc",
                    "s1",
                    "2026-09-30T00:00:01Z",
                    "response",
                    "Done.",
                ),
                event(
                    "claude-code",
                    "cc",
                    "r2",
                    "2026-09-30T00:00:02Z",
                    "request",
                    CLAUDE_NAMING,
                ),
                event(
                    "claude-code",
                    "cc",
                    "s2",
                    "2026-09-30T00:00:03Z",
                    "response",
                    r#"{"title":"Build fix"}"#,
                ),
            ],
        );

        assert_eq!(
            session_title(&mut database, "cc"),
            (Some("Build fix".to_owned()), None)
        );
    }

    #[test]
    fn marks_codex_naming_threads_as_auxiliary_even_when_the_response_arrives_first() {
        let mut database = database();
        ingest(
            &mut database,
            &[event(
                "codex",
                "title-thread",
                "s1",
                "2026-09-30T00:00:01Z",
                "response",
                "{\"title\":\"Draw a pelican\"}\n\n{\"title\":\"Draw a pelican\"}",
            )],
        );
        ingest(
            &mut database,
            &[event(
                "codex",
                "title-thread",
                "r1",
                "2026-09-30T00:00:00Z",
                "request",
                CODEX_NAMING,
            )],
        );

        assert_eq!(
            session_title(&mut database, "title-thread"),
            (
                Some("Draw a pelican".to_owned()),
                Some("title_generation".to_owned())
            )
        );
    }

    #[test]
    fn keeps_the_newest_title_and_ignores_quoted_instructions() {
        let mut database = database();
        ingest(
            &mut database,
            &[
                event(
                    "claude-code",
                    "cc",
                    "r2",
                    "2026-09-30T02:00:00Z",
                    "request",
                    CLAUDE_NAMING,
                ),
                event(
                    "claude-code",
                    "cc",
                    "s2",
                    "2026-09-30T02:00:01Z",
                    "response",
                    r#"{"title":"Newer"}"#,
                ),
                event(
                    "claude-code",
                    "cc",
                    "r1",
                    "2026-09-30T01:00:00Z",
                    "request",
                    CLAUDE_NAMING,
                ),
                event(
                    "claude-code",
                    "cc",
                    "s1",
                    "2026-09-30T01:00:01Z",
                    "response",
                    r#"{"title":"Older"}"#,
                ),
                event(
                    "claude-code",
                    "cc",
                    "r3",
                    "2026-09-30T03:00:00Z",
                    "request",
                    CLAUDE_NAMING,
                ),
                event(
                    "claude-code",
                    "cc",
                    "s3",
                    "2026-09-30T03:00:01Z",
                    "response",
                    "That prompt names sessions.",
                ),
            ],
        );

        assert_eq!(
            session_title(&mut database, "cc").0.as_deref(),
            Some("Newer")
        );
    }

    #[test]
    fn backfills_stored_naming_calls_once() {
        let mut database = database();
        ingest(
            &mut database,
            &[
                event(
                    "claude-code",
                    "cc",
                    "r1",
                    "2026-09-30T00:00:00Z",
                    "request",
                    CLAUDE_NAMING,
                ),
                event(
                    "claude-code",
                    "cc",
                    "s1",
                    "2026-09-30T00:00:01Z",
                    "response",
                    r#"{"title":"Stored"}"#,
                ),
            ],
        );
        diesel::delete(session_titles::table)
            .execute(&mut database)
            .expect("clear titles");

        backfill(&mut database).expect("backfill should succeed");
        assert_eq!(
            session_title(&mut database, "cc").0.as_deref(),
            Some("Stored")
        );

        diesel::delete(session_titles::table)
            .execute(&mut database)
            .expect("clear titles");
        backfill(&mut database).expect("second backfill should succeed");
        assert_eq!(session_title(&mut database, "cc"), (None, None));
        assert_eq!(
            schema_backfills::table
                .count()
                .get_result::<i64>(&mut database)
                .expect("count"),
            1
        );
    }
}
