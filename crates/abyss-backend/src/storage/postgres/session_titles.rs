//! PostgreSQL persistence of session names read from title-generation calls.

use chrono::{DateTime, Utc};
use diesel::{
    BoolExpressionMethods, Connection, ExpressionMethods, OptionalExtension, PgConnection,
    QueryDsl, RunQueryDsl, TextExpressionMethods,
};
use uuid::Uuid;

use crate::{
    db::schema::{llm_usage_events, schema_backfills, session_titles},
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
    pub(super) observed_at: DateTime<Utc>,
}

/// A naming call recognized from one of its events.
struct NamingCall {
    task: TitleTask,
    /// Title with the response event and observation time that carried it.
    title: Option<(String, Uuid, DateTime<Utc>)>,
}

/// Returns `true` when an event's text could belong to a naming call, so
/// ingestion copies text only for candidates.
pub(super) fn is_candidate(event_type: &str, text: &str) -> bool {
    match event_type {
        "request" => TitleTask::from_request(text).is_some(),
        "response" => may_carry_title(text),
        _ => false,
    }
}

/// Records the session title when the event completes a naming call.
///
/// A naming request and its response arrive as separate events in either
/// order, so each side looks up the other within the same turn.
pub(super) fn capture(
    connection: &mut PgConnection,
    event: &CapturedEvent<'_>,
) -> Result<(), AppError> {
    naming_call(connection, event)?.map_or_else(|| Ok(()), |call| store(connection, event, call))
}

fn naming_call(
    connection: &mut PgConnection,
    event: &CapturedEvent<'_>,
) -> Result<Option<NamingCall>, AppError> {
    let Some(text) = event.text else {
        return Ok(None);
    };
    match event.event_type {
        "request" => {
            let Some(task) = TitleTask::from_request(text) else {
                return Ok(None);
            };
            let response = llm_usage_events::table
                .filter(llm_usage_events::session_pk.eq(event.session_pk))
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
            Ok(Some(NamingCall { task, title }))
        }
        "response" if may_carry_title(text) => {
            let Some(title) = title_from_response(text) else {
                return Ok(None);
            };
            let request_text = llm_usage_events::table
                .filter(llm_usage_events::session_pk.eq(event.session_pk))
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
                    title: Some((title, event.event_pk, event.observed_at)),
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
    let now = Utc::now();
    let purpose =
        (call.task == TitleTask::AuxiliarySession).then(|| TITLE_GENERATION_PURPOSE.to_owned());
    let (title, title_event_pk, title_observed_at) = match call.title {
        Some((title, event_pk, observed_at)) => (Some(title), Some(event_pk), Some(observed_at)),
        None => (None, None, None),
    };
    let existing = session_titles::table
        .find(event.session_pk)
        .select((session_titles::title_observed_at, session_titles::purpose))
        .first::<(Option<DateTime<Utc>>, Option<String>)>(connection)
        .optional()?;
    let Some((stored_at, stored_purpose)) = existing else {
        diesel::insert_into(session_titles::table)
            .values((
                session_titles::session_pk.eq(event.session_pk),
                session_titles::user_id.eq(event.user_id),
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
    let target = session_titles::table.find(event.session_pk);
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
pub(super) fn backfill(connection: &mut PgConnection) -> Result<(), AppError> {
    connection.transaction(|transaction| {
        let done = schema_backfills::table
            .find(BACKFILL_NAME)
            .select(schema_backfills::name)
            .first::<String>(transaction)
            .optional()?
            .is_some();
        if done {
            return Ok(());
        }
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
                .load::<(Uuid, Uuid, Uuid, i32, Option<String>, DateTime<Utc>)>(transaction)?;
        for (event_pk, user_id, session_pk, turn_index, text, observed_at) in requests {
            capture(
                transaction,
                &CapturedEvent {
                    user_id,
                    session_pk,
                    event_pk,
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
                schema_backfills::completed_at.eq(Utc::now()),
            ))
            .execute(transaction)?;
        Ok::<_, AppError>(())
    })
}

/// Returns the stored title and purpose of one session.
pub(super) fn load(
    connection: &mut PgConnection,
    session_pk: Uuid,
) -> Result<(Option<String>, Option<String>), AppError> {
    Ok(session_titles::table
        .find(session_pk)
        .select((session_titles::title, session_titles::purpose))
        .first::<(Option<String>, Option<String>)>(connection)
        .optional()?
        .unwrap_or_default())
}
