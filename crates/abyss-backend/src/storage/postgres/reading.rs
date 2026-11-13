//! Snapshot-scoped, owner-authorized directory and recorded-content reads.

use super::repository;
use crate::db::schema::llm_usage_events;
use crate::{
    error::AppError,
    usage::reading::{
        DirectoryPage, DirectoryQuery, ReadPage, ReadQuery, ReadingIndex, SourceEvent,
    },
};
use diesel::{ExpressionMethods, PgConnection, QueryDsl, RunQueryDsl};
use uuid::Uuid;

fn index(
    connection: &mut PgConnection,
    user_id: Uuid,
    session_pk: Uuid,
) -> Result<ReadingIndex, AppError> {
    let rows = llm_usage_events::table
        .filter(llm_usage_events::user_id.eq(user_id))
        .filter(llm_usage_events::session_pk.eq(session_pk))
        .order_by((
            llm_usage_events::observed_at.asc(),
            llm_usage_events::event_type.asc(),
            llm_usage_events::id.asc(),
        ))
        .select((
            llm_usage_events::id,
            llm_usage_events::event_id,
            llm_usage_events::observed_at,
            llm_usage_events::event_type,
        ))
        .load::<(Uuid, String, chrono::DateTime<chrono::Utc>, String)>(connection)?;
    let events = rows
        .into_iter()
        .map(
            |(event_pk, event_id, observed_at, event_type)| SourceEvent {
                event_pk,
                event_id,
                observed_at,
                event_type,
            },
        )
        .collect();
    ReadingIndex::new(session_pk, events)
}

/// Directory and revision are derived in the same database snapshot.
pub fn directory(
    connection: &mut PgConnection,
    user_id: Uuid,
    session_pk: Uuid,
    query: &DirectoryQuery,
) -> Result<DirectoryPage, AppError> {
    connection
        .build_transaction()
        .read_only()
        .repeatable_read()
        .run(|connection| index(connection, user_id, session_pk)?.directory(query))
}

/// Only the selected event body is loaded, after owner and cursor validation.
pub fn read(
    connection: &mut PgConnection,
    user_id: Uuid,
    session_pk: Uuid,
    query: &ReadQuery,
) -> Result<ReadPage, AppError> {
    connection
        .build_transaction()
        .read_only()
        .repeatable_read()
        .run(|connection| {
            let index = index(connection, user_id, session_pk)?;
            let position = index.position(query)?;
            let event = repository::usage_event(connection, user_id, index.event_pk(&position))?;
            let value = serde_json::to_value(event)
                .map_err(|error| AppError::internal(error.to_string()))?;
            index.read(position, &value)
        })
}
