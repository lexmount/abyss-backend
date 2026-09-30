//! SQLite persistence of session names read from title-generation calls.

use std::collections::BTreeSet;

use chrono::Utc;
use diesel::{
    BoolExpressionMethods, ExpressionMethods, OptionalExtension, QueryDsl, RunQueryDsl,
    SqliteConnection, TextExpressionMethods,
    dsl::sql,
    sql_types::{Nullable, Text},
};
use serde_json::Value;
use uuid::Uuid;

use super::{
    models::parse_uuid,
    schema::{agent_sessions, llm_usage_events, schema_backfills, session_titles},
};
use crate::{
    error::AppError,
    usage::{
        SessionInfo,
        session_title::{
            TITLE_GENERATION_PURPOSE, TitleTask, codex_naming_prompt, codex_turn_id,
            codex_turn_id_range, may_carry_title, opens_with_prompt, title_from_response,
        },
    },
};

/// Marks the one-time scan of events stored before titles were captured.
const BACKFILL_NAME: &str = "session_titles_v1";
/// Marks the one-time matching of Codex naming threads captured before
/// matching existed.
const LINK_BACKFILL_NAME: &str = "codex_title_links_v1";
/// Widest delivery delay tolerated between a naming thread and its task when
/// looking for candidates, in microseconds.
const LINK_DELIVERY_WINDOW_MICROS: i64 = 24 * 60 * 60 * 1_000_000;

/// Stored title, purpose, and matched task session of one session.
type StoredTitle = (Option<String>, Option<String>, Option<Uuid>);

/// One stored event considered for title capture.
pub(super) struct CapturedEvent<'a> {
    pub(super) user_id: Uuid,
    pub(super) session_pk: Uuid,
    pub(super) event_pk: Uuid,
    pub(super) turn_index: i32,
    pub(super) agent_name: &'a str,
    pub(super) event_type: &'a str,
    pub(super) text: Option<&'a str>,
    pub(super) metadata: &'a Value,
    pub(super) observed_at: i64,
}

/// A naming call recognized from one of its events.
struct NamingCall {
    task: TitleTask,
    /// Title with the response event and observation time that carried it.
    title: Option<(String, String, i64)>,
    /// Task message a Codex naming request quotes.
    prompt: Option<String>,
}

/// A title to record for one session.
struct TitleUpdate<'a> {
    session_pk: &'a str,
    user_id: &'a str,
    title: Option<(String, String, i64)>,
    purpose: Option<&'a str>,
}

/// Records the session title when the event completes a naming call, and
/// matches Codex naming threads with the task sessions they name.
///
/// A naming request and its response arrive as separate events in either
/// order, so each side looks up the other within the same turn. Likewise a
/// naming thread and its task session may arrive in either order, so both
/// sides try the match.
pub(super) fn capture(
    connection: &mut SqliteConnection,
    event: &CapturedEvent<'_>,
) -> Result<(), AppError> {
    match naming_call(connection, event)? {
        Some(call) => store(connection, event, call),
        None => match_pending_naming_threads(connection, event),
    }
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
            let prompt = codex_naming_prompt(text).map(str::to_owned);
            Ok(Some(NamingCall {
                task,
                title,
                prompt,
            }))
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
            Ok(request_text.as_deref().and_then(|request_text| {
                TitleTask::from_request(request_text).map(|task| NamingCall {
                    task,
                    title: Some((title, event.event_pk.to_string(), event.observed_at)),
                    prompt: codex_naming_prompt(request_text).map(str::to_owned),
                })
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
    let auxiliary = call.task == TitleTask::AuxiliarySession;
    put_title(
        connection,
        TitleUpdate {
            session_pk: &session,
            user_id: &event.user_id.to_string(),
            title: call.title,
            purpose: auxiliary.then_some(TITLE_GENERATION_PURPOSE),
        },
    )?;
    if !auxiliary {
        return Ok(());
    }
    let target = session_titles::table.find(&session);
    if let Some(turn_id) = codex_turn_id(event.metadata) {
        diesel::update(target.filter(session_titles::naming_turn_id.is_null()))
            .set(session_titles::naming_turn_id.eq(turn_id))
            .execute(connection)?;
    }
    if let Some(prompt) = call.prompt {
        diesel::update(target.filter(session_titles::naming_prompt.is_null()))
            .set(session_titles::naming_prompt.eq(prompt))
            .execute(connection)?;
    }
    match_naming_thread(connection, &session)
}

/// Inserts or updates a session title; a late or replayed naming call never
/// replaces a newer title.
fn put_title(connection: &mut SqliteConnection, update: TitleUpdate<'_>) -> Result<(), AppError> {
    let now = Utc::now().timestamp_micros();
    let (title, title_event_pk, title_observed_at) = match update.title {
        Some((title, event_pk, observed_at)) => (Some(title), Some(event_pk), Some(observed_at)),
        None => (None, None, None),
    };
    let existing = session_titles::table
        .find(update.session_pk)
        .select((session_titles::title_observed_at, session_titles::purpose))
        .first::<(Option<i64>, Option<String>)>(connection)
        .optional()?;
    let Some((stored_at, stored_purpose)) = existing else {
        diesel::insert_into(session_titles::table)
            .values((
                session_titles::session_pk.eq(update.session_pk),
                session_titles::user_id.eq(update.user_id),
                session_titles::title.eq(title),
                session_titles::title_event_pk.eq(title_event_pk),
                session_titles::title_observed_at.eq(title_observed_at),
                session_titles::purpose.eq(update.purpose),
                session_titles::updated_at.eq(now),
            ))
            .execute(connection)?;
        return Ok(());
    };
    let newer = title.is_some() && stored_at.is_none_or(|stored| title_observed_at > Some(stored));
    let target = session_titles::table.find(update.session_pk);
    if newer {
        diesel::update(target)
            .set((
                session_titles::title.eq(title),
                session_titles::title_event_pk.eq(title_event_pk),
                session_titles::title_observed_at.eq(title_observed_at),
                session_titles::purpose.eq(update.purpose.map(str::to_owned).or(stored_purpose)),
                session_titles::updated_at.eq(now),
            ))
            .execute(connection)?;
    } else if stored_purpose.is_none() && update.purpose.is_some() {
        diesel::update(target)
            .set((
                session_titles::purpose.eq(update.purpose),
                session_titles::updated_at.eq(now),
            ))
            .execute(connection)?;
    }
    Ok(())
}

/// Re-examines Codex naming threads whose turn started near this task
/// request's turn: an unmatched one may now have its task, and a matched one
/// that quotes this request too no longer has a single candidate.
fn match_pending_naming_threads(
    connection: &mut SqliteConnection,
    event: &CapturedEvent<'_>,
) -> Result<(), AppError> {
    if event.event_type != "request" || event.agent_name != "codex" {
        return Ok(());
    }
    let Some((low, high)) = codex_turn_id(event.metadata).and_then(codex_turn_id_range) else {
        return Ok(());
    };
    let session = event.session_pk.to_string();
    let nearby = session_titles::table
        .filter(session_titles::user_id.eq(event.user_id.to_string()))
        .filter(session_titles::title.is_not_null())
        .filter(session_titles::naming_turn_id.between(low, high))
        .select((
            session_titles::session_pk,
            session_titles::naming_prompt,
            session_titles::parent_session_pk,
            session_titles::title_event_pk,
        ))
        .load::<(String, Option<String>, Option<String>, Option<String>)>(connection)?;
    for (naming_session, prompt, parent, title_event_pk) in nearby {
        if !prompt.is_some_and(|prompt| opens_with_prompt(event.metadata, &prompt)) {
            continue;
        }
        match parent {
            None => match_naming_thread(connection, &naming_session)?,
            Some(parent) if parent != session => {
                unmatch(connection, &naming_session, &parent, title_event_pk)?;
            }
            Some(_) => {}
        }
    }
    Ok(())
}

/// Withdraws a match and the title it gave, because a second task session
/// quotes the same message.
fn unmatch(
    connection: &mut SqliteConnection,
    naming_session: &str,
    task_session: &str,
    title_event_pk: Option<String>,
) -> Result<(), AppError> {
    diesel::update(session_titles::table.find(naming_session))
        .set(session_titles::parent_session_pk.eq(None::<String>))
        .execute(connection)?;
    diesel::update(
        session_titles::table
            .find(task_session)
            .filter(session_titles::title_event_pk.eq(title_event_pk)),
    )
    .set((
        session_titles::title.eq(None::<String>),
        session_titles::title_event_pk.eq(None::<String>),
        session_titles::title_observed_at.eq(None::<i64>),
        session_titles::updated_at.eq(Utc::now().timestamp_micros()),
    ))
    .execute(connection)?;
    Ok(())
}

/// Gives a Codex naming thread's title to the one task session whose turn
/// started near the naming turn with the quoted message. Zero or several
/// candidates leave the naming thread unmatched.
fn match_naming_thread(
    connection: &mut SqliteConnection,
    naming_session: &str,
) -> Result<(), AppError> {
    let Some((user_id, title, title_event_pk, title_observed_at, turn_id, prompt)) =
        session_titles::table
            .find(naming_session)
            .filter(session_titles::parent_session_pk.is_null())
            .select((
                session_titles::user_id,
                session_titles::title,
                session_titles::title_event_pk,
                session_titles::title_observed_at,
                session_titles::naming_turn_id,
                session_titles::naming_prompt,
            ))
            .first::<(
                String,
                Option<String>,
                Option<String>,
                Option<i64>,
                Option<String>,
                Option<String>,
            )>(connection)
            .optional()?
    else {
        return Ok(());
    };
    let (Some(title), Some(title_event_pk), Some(title_observed_at), Some(prompt)) =
        (title, title_event_pk, title_observed_at, prompt)
    else {
        return Ok(());
    };
    let Some((low, high)) = turn_id.as_deref().and_then(codex_turn_id_range) else {
        return Ok(());
    };
    let device = agent_sessions::table
        .find(naming_session)
        .select(agent_sessions::device_context_id)
        .first::<String>(connection)?;
    let candidates = llm_usage_events::table
        .filter(llm_usage_events::user_id.eq(&user_id))
        .filter(llm_usage_events::observed_at.between(
            title_observed_at.saturating_sub(LINK_DELIVERY_WINDOW_MICROS),
            title_observed_at.saturating_add(LINK_DELIVERY_WINDOW_MICROS),
        ))
        .filter(llm_usage_events::device_context_id.eq(&device))
        .filter(llm_usage_events::agent_name.eq("codex"))
        .filter(llm_usage_events::event_type.eq("request"))
        .filter(llm_usage_events::session_pk.ne(naming_session))
        .filter(
            sql::<Nullable<Text>>("json_extract(llm_usage_events.metadata, '$.codex_turn_id')")
                .between(low, high),
        )
        .select((llm_usage_events::session_pk, llm_usage_events::metadata))
        .load::<(String, String)>(connection)?;
    let sessions = candidates
        .into_iter()
        .filter(|(_, metadata)| {
            serde_json::from_str::<Value>(metadata)
                .is_ok_and(|metadata| opens_with_prompt(&metadata, &prompt))
        })
        .map(|(session, _)| session)
        .collect::<BTreeSet<_>>();
    let mut sessions = sessions.into_iter();
    let (Some(task_session), None) = (sessions.next(), sessions.next()) else {
        return Ok(());
    };
    diesel::update(session_titles::table.find(naming_session))
        .set(session_titles::parent_session_pk.eq(&task_session))
        .execute(connection)?;
    put_title(
        connection,
        TitleUpdate {
            session_pk: &task_session,
            user_id: &user_id,
            title: Some((title, title_event_pk, title_observed_at)),
            purpose: None,
        },
    )
}

/// Runs the one-time scans that bring events stored before capture existed up
/// to date.
pub(super) fn backfill(connection: &mut SqliteConnection) -> Result<(), AppError> {
    run_once(connection, BACKFILL_NAME, backfill_titles)?;
    run_once(connection, LINK_BACKFILL_NAME, backfill_links)
}

fn run_once(
    connection: &mut SqliteConnection,
    name: &str,
    scan: fn(&mut SqliteConnection) -> Result<(), AppError>,
) -> Result<(), AppError> {
    let done = schema_backfills::table
        .find(name)
        .select(schema_backfills::name)
        .first::<String>(connection)
        .optional()?
        .is_some();
    if done {
        return Ok(());
    }
    connection.immediate_transaction(|transaction| {
        scan(transaction)?;
        diesel::insert_into(schema_backfills::table)
            .values((
                schema_backfills::name.eq(name),
                schema_backfills::completed_at.eq(Utc::now().timestamp_micros()),
            ))
            .execute(transaction)?;
        Ok::<_, AppError>(())
    })
}

/// Captures titles from naming requests stored before capture existed.
fn backfill_titles(connection: &mut SqliteConnection) -> Result<(), AppError> {
    let requests = llm_usage_events::table
        .filter(llm_usage_events::event_type.eq("request"))
        .filter(
            llm_usage_events::text
                .like("%You are naming a coding session%")
                .or(llm_usage_events::text.like("%Generate a concise, single-line task title%")),
        )
        .order_by(llm_usage_events::observed_at.asc())
        .select((
            llm_usage_events::id,
            llm_usage_events::user_id,
            llm_usage_events::session_pk,
            llm_usage_events::turn_index,
            llm_usage_events::text,
            llm_usage_events::metadata,
            llm_usage_events::observed_at,
        ))
        .load::<(String, String, String, i32, Option<String>, String, i64)>(connection)?;
    for (event_pk, user_id, session_pk, turn_index, text, metadata, observed_at) in requests {
        let metadata = serde_json::from_str::<Value>(&metadata).unwrap_or(Value::Null);
        capture(
            connection,
            &CapturedEvent {
                user_id: parse_uuid(&user_id, "event user id")?,
                session_pk: parse_uuid(&session_pk, "event session id")?,
                event_pk: parse_uuid(&event_pk, "event id")?,
                turn_index,
                agent_name: "",
                event_type: "request",
                text: text.as_deref(),
                metadata: &metadata,
                observed_at,
            },
        )?;
    }
    Ok(())
}

/// Matches naming threads whose titles were captured before matching existed.
fn backfill_links(connection: &mut SqliteConnection) -> Result<(), AppError> {
    let naming_sessions = session_titles::table
        .filter(session_titles::purpose.eq(TITLE_GENERATION_PURPOSE))
        .filter(session_titles::naming_turn_id.is_null())
        .select(session_titles::session_pk)
        .load::<String>(connection)?;
    for naming_session in naming_sessions {
        let request = llm_usage_events::table
            .filter(llm_usage_events::session_pk.eq(&naming_session))
            .filter(llm_usage_events::event_type.eq("request"))
            .filter(llm_usage_events::text.like("%Generate a concise, single-line task title%"))
            .order_by(llm_usage_events::observed_at.desc())
            .select((llm_usage_events::text, llm_usage_events::metadata))
            .first::<(Option<String>, String)>(connection)
            .optional()?;
        let Some((text, metadata)) = request else {
            continue;
        };
        let metadata = serde_json::from_str::<Value>(&metadata).unwrap_or(Value::Null);
        diesel::update(session_titles::table.find(&naming_session))
            .set((
                session_titles::naming_turn_id.eq(codex_turn_id(&metadata)),
                session_titles::naming_prompt.eq(text.as_deref().and_then(codex_naming_prompt)),
            ))
            .execute(connection)?;
        match_naming_thread(connection, &naming_session)?;
    }
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
        let (title, purpose, _) =
            load(database, Uuid::parse_str(&pk).expect("uuid")).expect("title should load");
        (title, purpose)
    }

    fn session_pk(database: &mut SqliteConnection, session: &str) -> Uuid {
        let pk = agent_sessions::table
            .filter(agent_sessions::session_id.eq(session))
            .select(agent_sessions::id)
            .first::<String>(database)
            .expect("session should exist");
        Uuid::parse_str(&pk).expect("uuid")
    }

    fn parent_of(database: &mut SqliteConnection, session: &str) -> Option<Uuid> {
        let pk = session_pk(database, session);
        load(database, pk).expect("title should load").2
    }

    const NAMING_TURN: &str = "01a0f320-138e-7d21-99ea-03a7ae2a9b50";
    const TASK_TURN: &str = "01a0f320-1350-71c2-9c93-5fc5fd7a6bfb";
    const LATE_TURN: &str = "01a0f330-1350-71c2-9c93-5fc5fd7a6bfb";
    const TASK: &str = "画一只鹈鹕骑自行车，不要联网";

    fn codex_naming_thread(session: &str) -> [Value; 2] {
        let mut request = event(
            "codex",
            session,
            &format!("{session}-r"),
            "2026-09-30T00:00:01Z",
            "request",
            &format!("{CODEX_NAMING}\n\nUser prompt:\n画一只鹈鹕骑自行车"),
        );
        request["metadata"] = json!({"codex_turn_id": NAMING_TURN});
        let mut response = event(
            "codex",
            session,
            &format!("{session}-s"),
            "2026-09-30T00:00:02Z",
            "response",
            r#"{"title":"Draw a pelican"}"#,
        );
        response["metadata"] = json!({"codex_turn_id": NAMING_TURN});
        [request, response]
    }

    fn codex_task(session: &str, turn_id: &str, message: &str) -> Value {
        let mut request = event(
            "codex",
            session,
            &format!("{session}-{turn_id}"),
            "2026-09-30T00:03:00Z",
            "request",
            message,
        );
        request["metadata"] = json!({
            "codex_turn_id": turn_id,
            "conversation": {"messages": [
                {"role": "developer", "text": "You are Codex"},
                {"role": "user", "text": "<environment_context>…"},
                {"role": "user", "text": message},
            ]},
        });
        request
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
            2,
            "titles and Codex links are each scanned once"
        );
    }

    #[test]
    fn names_the_codex_task_its_naming_thread_quotes_in_either_order() {
        for naming_first in [true, false] {
            let mut database = database();
            let naming = codex_naming_thread("title-thread");
            let task = codex_task("task", TASK_TURN, TASK);
            if naming_first {
                ingest(&mut database, &naming);
                ingest(&mut database, &[task]);
            } else {
                ingest(&mut database, &[task]);
                ingest(&mut database, &naming);
            }

            assert_eq!(
                session_title(&mut database, "task"),
                (Some("Draw a pelican".to_owned()), None)
            );
            let task_pk = session_pk(&mut database, "task");
            assert_eq!(parent_of(&mut database, "title-thread"), Some(task_pk));
        }
    }

    #[test]
    fn leaves_codex_naming_threads_unmatched_without_one_clear_task() {
        let mut database = database();
        ingest(&mut database, &codex_naming_thread("title-thread"));
        ingest(
            &mut database,
            &[
                codex_task("other-message", TASK_TURN, "写一个排序函数"),
                codex_task("too-late", LATE_TURN, TASK),
            ],
        );
        assert_eq!(parent_of(&mut database, "title-thread"), None);
        assert_eq!(session_title(&mut database, "too-late"), (None, None));

        // The same message sent twice at once cannot be told apart.
        ingest(
            &mut database,
            &[
                codex_task("first", TASK_TURN, TASK),
                codex_task("second", TASK_TURN, TASK),
            ],
        );
        assert_eq!(parent_of(&mut database, "title-thread"), None);
        assert_eq!(session_title(&mut database, "second"), (None, None));
    }
}
