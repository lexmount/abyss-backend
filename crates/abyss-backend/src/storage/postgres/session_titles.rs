//! PostgreSQL persistence of session names read from title-generation calls.

use std::collections::BTreeSet;

use chrono::{DateTime, Duration, Utc};
use diesel::{
    BoolExpressionMethods, Connection, ExpressionMethods, OptionalExtension, PgConnection,
    QueryDsl, RunQueryDsl, TextExpressionMethods,
    dsl::sql,
    sql_types::{Nullable, Text},
};
use serde_json::Value;
use uuid::Uuid;

use crate::{
    db::schema::{agent_sessions, llm_usage_events, schema_backfills, session_titles},
    error::AppError,
    usage::{
        SessionInfo,
        session_title::{
            NamingRequest, TITLE_GENERATION_PURPOSE, TitleTask, codex_turn_id, codex_turn_id_range,
            may_carry_title, opens_with_prompt, title_from_response,
        },
    },
};

/// Marks the one-time scan of events stored before titles were captured.
const BACKFILL_NAME: &str = "session_titles_v1";
/// Marks the one-time matching of Codex naming threads captured before
/// matching existed.
const LINK_BACKFILL_NAME: &str = "codex_title_links_v1";
/// Rebuild links after switching to archived request bodies and scoped adapters.
const LINK_REPAIR_NAME: &str = "codex_title_links_v2";
/// Widest delivery delay tolerated between a naming thread and its task when
/// looking for candidates, in hours.
const LINK_DELIVERY_WINDOW_HOURS: i64 = 24;

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
    pub(super) observed_at: DateTime<Utc>,
}

/// A naming call recognized from one of its events.
struct NamingCall {
    task: TitleTask,
    /// Title with the response event and observation time that carried it.
    title: Option<(String, Uuid, DateTime<Utc>)>,
    /// Task message a Codex naming request quotes.
    prompt: Option<String>,
}

/// A title to record for one session.
struct TitleUpdate<'a> {
    session_pk: Uuid,
    user_id: Uuid,
    title: Option<(String, Uuid, DateTime<Utc>)>,
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
    connection: &mut PgConnection,
    event: &CapturedEvent<'_>,
) -> Result<(), AppError> {
    match naming_call(connection, event)? {
        Some(call) => store(connection, event, call),
        None => match_pending_naming_threads(connection, event),
    }
}

fn naming_call(
    connection: &mut PgConnection,
    event: &CapturedEvent<'_>,
) -> Result<Option<NamingCall>, AppError> {
    let session = event.session_pk;
    match event.event_type {
        "request" => {
            let Some(naming) = NamingRequest::parse(event.agent_name, event.text, event.metadata)
            else {
                return Ok(None);
            };
            let response = llm_usage_events::table
                .filter(llm_usage_events::session_pk.eq(session))
                .filter(llm_usage_events::turn_index.eq(event.turn_index))
                .filter(llm_usage_events::event_type.eq("response"))
                .filter(llm_usage_events::observed_at.ge(event.observed_at))
                .order_by(llm_usage_events::observed_at.asc())
                .select((
                    llm_usage_events::id,
                    llm_usage_events::text,
                    llm_usage_events::observed_at,
                ))
                .first::<(Uuid, Option<String>, DateTime<Utc>)>(connection)
                .optional()?;
            let title = response.and_then(|(id, text, observed_at)| {
                text.as_deref()
                    .and_then(title_from_response)
                    .map(|title| (title, id, observed_at))
            });
            Ok(Some(NamingCall {
                task: naming.task,
                title,
                prompt: naming.prompt,
            }))
        }
        "response" if event.text.is_some_and(may_carry_title) => {
            let Some(title) = event.text.and_then(title_from_response) else {
                return Ok(None);
            };
            let request = llm_usage_events::table
                .filter(llm_usage_events::session_pk.eq(session))
                .filter(llm_usage_events::turn_index.eq(event.turn_index))
                .filter(llm_usage_events::event_type.eq("request"))
                .filter(llm_usage_events::observed_at.le(event.observed_at))
                .order_by(llm_usage_events::observed_at.desc())
                .select((llm_usage_events::text, llm_usage_events::metadata))
                .first::<(Option<String>, Value)>(connection)
                .optional()?;
            Ok(request.and_then(|(text, metadata)| {
                NamingRequest::parse(event.agent_name, text.as_deref(), &metadata).map(|naming| {
                    NamingCall {
                        task: naming.task,
                        title: Some((title, event.event_pk, event.observed_at)),
                        prompt: naming.prompt,
                    }
                })
            }))
        }
        _ => Ok(None),
    }
}

fn store(
    connection: &mut PgConnection,
    event: &CapturedEvent<'_>,
    call: NamingCall,
) -> Result<(), AppError> {
    let session = event.session_pk;
    let auxiliary = call.task == TitleTask::AuxiliarySession;
    put_title(
        connection,
        TitleUpdate {
            session_pk: session,
            user_id: event.user_id,
            title: call.title,
            purpose: auxiliary.then_some(TITLE_GENERATION_PURPOSE),
        },
    )?;
    if !auxiliary {
        return Ok(());
    }
    let target = session_titles::table.find(session);
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
    match_naming_thread(connection, session)
}

/// Inserts or updates a session title; a late or replayed naming call never
/// replaces a newer title.
fn put_title(connection: &mut PgConnection, update: TitleUpdate<'_>) -> Result<(), AppError> {
    let now = Utc::now();
    let (title, title_event_pk, title_observed_at) = match update.title {
        Some((title, event_pk, observed_at)) => (Some(title), Some(event_pk), Some(observed_at)),
        None => (None, None, None),
    };
    let existing = session_titles::table
        .find(update.session_pk)
        .select((session_titles::title_observed_at, session_titles::purpose))
        .first::<(Option<DateTime<Utc>>, Option<String>)>(connection)
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
    connection: &mut PgConnection,
    event: &CapturedEvent<'_>,
) -> Result<(), AppError> {
    if event.event_type != "request" || event.agent_name != "codex" {
        return Ok(());
    }
    let Some((low, high)) = codex_turn_id(event.metadata).and_then(codex_turn_id_range) else {
        return Ok(());
    };
    let nearby = session_titles::table
        .filter(session_titles::user_id.eq(event.user_id))
        .filter(session_titles::purpose.eq(TITLE_GENERATION_PURPOSE))
        .filter(session_titles::title.is_not_null())
        .filter(
            session_titles::parent_session_pk
                .is_null()
                .or(session_titles::parent_session_pk.ne(event.session_pk)),
        )
        .filter(session_titles::naming_turn_id.between(low, high))
        .select((session_titles::session_pk, session_titles::naming_prompt))
        .load::<(Uuid, Option<String>)>(connection)?;
    if nearby.is_empty() {
        return Ok(());
    }
    for (naming_session, prompt) in nearby {
        if prompt.is_some_and(|prompt| opens_with_prompt(event.metadata, &prompt)) {
            // Recompute the complete candidate set before adding or withdrawing
            // a link. A request from a different device is not a conflict.
            match_naming_thread(connection, naming_session)?;
        }
    }
    Ok(())
}

/// Withdraws a match and the title it gave, because a second task session
/// quotes the same message.
fn unmatch(
    connection: &mut PgConnection,
    naming_session: Uuid,
    task_session: Uuid,
    title_event_pk: Option<Uuid>,
) -> Result<(), AppError> {
    diesel::update(session_titles::table.find(naming_session))
        .set(session_titles::parent_session_pk.eq(None::<Uuid>))
        .execute(connection)?;
    diesel::update(
        session_titles::table
            .find(task_session)
            .filter(session_titles::title_event_pk.eq(title_event_pk)),
    )
    .set((
        session_titles::title.eq(None::<String>),
        session_titles::title_event_pk.eq(None::<Uuid>),
        session_titles::title_observed_at.eq(None::<DateTime<Utc>>),
        session_titles::updated_at.eq(Utc::now()),
    ))
    .execute(connection)?;
    Ok(())
}

/// Gives a Codex naming thread's title to the one task session whose turn
/// started near the naming turn with the quoted message. Zero or several
/// candidates leave the naming thread unmatched.
fn match_naming_thread(
    connection: &mut PgConnection,
    naming_session: Uuid,
) -> Result<(), AppError> {
    let Some((user_id, title, title_event_pk, title_observed_at, turn_id, prompt, old_parent)) =
        session_titles::table
            .find(naming_session)
            .select((
                session_titles::user_id,
                session_titles::title,
                session_titles::title_event_pk,
                session_titles::title_observed_at,
                session_titles::naming_turn_id,
                session_titles::naming_prompt,
                session_titles::parent_session_pk,
            ))
            .first::<(
                Uuid,
                Option<String>,
                Option<Uuid>,
                Option<DateTime<Utc>>,
                Option<String>,
                Option<String>,
                Option<Uuid>,
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
    let window = Duration::hours(LINK_DELIVERY_WINDOW_HOURS);
    let (Some(earliest), Some(latest)) = (
        title_observed_at.checked_sub_signed(window),
        title_observed_at.checked_add_signed(window),
    ) else {
        return Ok(());
    };
    let device = agent_sessions::table
        .find(naming_session)
        .select(agent_sessions::device_context_id)
        .first::<Uuid>(connection)?;
    let candidates = llm_usage_events::table
        .filter(llm_usage_events::user_id.eq(user_id))
        .filter(llm_usage_events::observed_at.between(earliest, latest))
        .filter(llm_usage_events::device_context_id.eq(device))
        .filter(llm_usage_events::agent_name.eq("codex"))
        .filter(llm_usage_events::event_type.eq("request"))
        .filter(llm_usage_events::session_pk.ne(naming_session))
        .filter(
            sql::<Nullable<Text>>("llm_usage_events.metadata ->> 'codex_turn_id'")
                .between(low, high),
        )
        .select((llm_usage_events::session_pk, llm_usage_events::metadata))
        .load::<(Uuid, Value)>(connection)?;
    let sessions = candidates
        .into_iter()
        .filter(|(_, metadata)| opens_with_prompt(metadata, &prompt))
        .map(|(session, _)| session)
        .collect::<BTreeSet<_>>();
    let mut sessions = sessions.into_iter();
    let task_session = match (sessions.next(), sessions.next()) {
        (Some(session), None) => Some(session),
        _ => None,
    };
    if let Some(parent) = old_parent
        && Some(parent) != task_session
    {
        unmatch(connection, naming_session, parent, Some(title_event_pk))?;
    }
    let Some(task_session) = task_session else {
        return Ok(());
    };
    diesel::update(session_titles::table.find(naming_session))
        .set(session_titles::parent_session_pk.eq(task_session))
        .execute(connection)?;
    put_title(
        connection,
        TitleUpdate {
            session_pk: task_session,
            user_id,
            title: Some((title, title_event_pk, title_observed_at)),
            purpose: None,
        },
    )
}

/// Runs the one-time scans that bring events stored before capture existed up
/// to date.
pub(super) fn backfill(connection: &mut PgConnection) -> Result<(), AppError> {
    run_once(connection, BACKFILL_NAME, backfill_titles)?;
    run_once(connection, LINK_BACKFILL_NAME, backfill_links)?;
    run_once(connection, LINK_REPAIR_NAME, backfill_links)
}

fn run_once(
    connection: &mut PgConnection,
    name: &str,
    scan: fn(&mut PgConnection) -> Result<(), AppError>,
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
    connection.transaction(|transaction| {
        scan(transaction)?;
        diesel::insert_into(schema_backfills::table)
            .values((
                schema_backfills::name.eq(name),
                schema_backfills::completed_at.eq(Utc::now()),
            ))
            .execute(transaction)?;
        Ok::<_, AppError>(())
    })
}

/// Captures titles from naming requests stored before capture existed.
fn backfill_titles(connection: &mut PgConnection) -> Result<(), AppError> {
    let requests = llm_usage_events::table
        .filter(llm_usage_events::event_type.eq("request"))
        .filter(llm_usage_events::agent_name.eq_any(["codex", "claude-code"]))
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
            llm_usage_events::agent_name,
            llm_usage_events::text,
            llm_usage_events::metadata,
            llm_usage_events::observed_at,
        ))
        .load::<(
            Uuid,
            Uuid,
            Uuid,
            i32,
            String,
            Option<String>,
            Value,
            DateTime<Utc>,
        )>(connection)?;
    for (event_pk, user_id, session_pk, turn_index, agent_name, text, metadata, observed_at) in
        requests
    {
        capture(
            connection,
            &CapturedEvent {
                user_id,
                session_pk,
                event_pk,
                turn_index,
                agent_name: &agent_name,
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
fn backfill_links(connection: &mut PgConnection) -> Result<(), AppError> {
    let naming_sessions = session_titles::table
        .filter(session_titles::purpose.eq(TITLE_GENERATION_PURPOSE))
        .select(session_titles::session_pk)
        .load::<Uuid>(connection)?;
    for naming_session in naming_sessions {
        let request = llm_usage_events::table
            .filter(llm_usage_events::session_pk.eq(naming_session))
            .filter(llm_usage_events::event_type.eq("request"))
            .filter(llm_usage_events::agent_name.eq("codex"))
            .order_by(llm_usage_events::observed_at.desc())
            .select((llm_usage_events::text, llm_usage_events::metadata))
            .first::<(Option<String>, Value)>(connection)
            .optional()?;
        let Some((text, metadata)) = request else {
            continue;
        };
        let Some(naming) = NamingRequest::parse("codex", text.as_deref(), &metadata) else {
            continue;
        };
        diesel::update(session_titles::table.find(naming_session))
            .set((
                session_titles::naming_turn_id.eq(codex_turn_id(&metadata)),
                session_titles::naming_prompt.eq(naming.prompt),
            ))
            .execute(connection)?;
        match_naming_thread(connection, naming_session)?;
    }
    Ok(())
}

/// Adds the stored title, purpose, and matched task session to a session.
pub(super) fn fill(
    connection: &mut PgConnection,
    session: &mut SessionInfo,
) -> Result<(), AppError> {
    (session.title, session.purpose, session.parent_session_pk) =
        load(connection, session.session_pk)?;
    Ok(())
}

/// Returns the stored title and purpose of one session, and for a naming
/// thread the task session it was matched to.
fn load(connection: &mut PgConnection, session_pk: Uuid) -> Result<StoredTitle, AppError> {
    Ok(session_titles::table
        .find(session_pk)
        .select((
            session_titles::title,
            session_titles::purpose,
            session_titles::parent_session_pk,
        ))
        .first::<(Option<String>, Option<String>, Option<Uuid>)>(connection)
        .optional()?
        .unwrap_or((None, None, None)))
}
