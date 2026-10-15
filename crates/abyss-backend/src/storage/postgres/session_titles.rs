//! PostgreSQL persistence of session names reported by the collector.

use chrono::{DateTime, Utc};
use diesel::{ExpressionMethods, OptionalExtension, PgConnection, QueryDsl, RunQueryDsl};
use serde_json::Value;
use uuid::Uuid;

use crate::{
    db::schema::{llm_usage_events, session_titles},
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
    pub(super) observed_at: DateTime<Utc>,
}

/// Records what a newly stored event says about its session's name.
///
/// An Agent title replaces a default title and an older Agent title. A default
/// title keeps the earliest user input, even after late delivery, and never
/// replaces an Agent title.
pub(super) fn capture(
    connection: &mut PgConnection,
    event: &CapturedEvent<'_>,
) -> Result<(), AppError> {
    let session = event.session_pk;
    let agent_title =
        match TitleContribution::of(event.event_type, event.metadata, event.naming_thread) {
            None => return Ok(()),
            Some(TitleContribution::TitleThread) => {
                return mark_title_thread(connection, session, event.user_id);
            }
            Some(TitleContribution::Agent(title)) => Some(title),
            Some(TitleContribution::UserInput) => None,
        };
    let stored = session_titles::table
        .find(session)
        .select((
            session_titles::title_event_pk,
            session_titles::title_observed_at,
        ))
        .first::<(Option<Uuid>, Option<DateTime<Utc>>)>(connection)
        .optional()?
        .and_then(|(event_pk, observed_at)| event_pk.zip(observed_at));
    let agent = agent_title.is_some();
    let replaces = match stored {
        None => true,
        Some((_, stored_at)) if agent && stored_at < event.observed_at => true,
        Some((_, stored_at)) if !agent && stored_at <= event.observed_at => false,
        // Otherwise only a stored default title gives way.
        Some((event_pk, _)) => !is_agent_title(connection, event_pk)?,
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
    let now = Utc::now();
    diesel::insert_into(session_titles::table)
        .values((
            session_titles::session_pk.eq(session),
            session_titles::user_id.eq(event.user_id),
            session_titles::title.eq(&title),
            session_titles::title_event_pk.eq(event.event_pk),
            session_titles::title_observed_at.eq(event.observed_at),
            session_titles::updated_at.eq(now),
        ))
        .on_conflict(session_titles::session_pk)
        .do_update()
        .set((
            session_titles::title.eq(&title),
            session_titles::title_event_pk.eq(event.event_pk),
            session_titles::title_observed_at.eq(event.observed_at),
            session_titles::updated_at.eq(now),
        ))
        .execute(connection)?;
    Ok(())
}

/// Agent titles are read from responses and default titles from requests, so
/// the side of the event that gave the stored title tells them apart.
fn is_agent_title(connection: &mut PgConnection, event_pk: Uuid) -> Result<bool, AppError> {
    Ok(llm_usage_events::table
        .find(event_pk)
        .select(llm_usage_events::event_type)
        .first::<String>(connection)
        .optional()?
        .is_some_and(|event_type| event_type == "response"))
}

/// Marks a session as a thread that names another session.
fn mark_title_thread(
    connection: &mut PgConnection,
    session: Uuid,
    user_id: Uuid,
) -> Result<(), AppError> {
    let now = Utc::now();
    diesel::insert_into(session_titles::table)
        .values((
            session_titles::session_pk.eq(session),
            session_titles::user_id.eq(user_id),
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
