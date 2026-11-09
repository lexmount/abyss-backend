//! Owner-scoped search of stored conversation entries in one PostgreSQL
//! snapshot. Event bodies stream row by row, independently of Elasticsearch.

use diesel::{
    ExpressionMethods, NullableExpressionMethods, OptionalExtension, PgConnection, QueryDsl,
    RunQueryDsl, pg::PgRowByRowLoadingMode,
};
use serde_json::Value;
use uuid::Uuid;

use super::repository::session_structure;
use crate::{
    db::schema::{agent_sessions, llm_usage_event_stream_facts, llm_usage_events},
    error::AppError,
    search::{InSessionSearchQuery, InSessionSearchResponse, matching::scan::SessionScan},
    usage::UsageEventType,
};

/// Matches only the authenticated owner's session, including events that
/// have not reached the asynchronous search index yet.
pub fn search_in_session(
    connection: &mut PgConnection,
    user_id: Uuid,
    session_pk: Uuid,
    query: &InSessionSearchQuery,
) -> Result<InSessionSearchResponse, AppError> {
    let query = query.validate()?;
    connection
        .build_transaction()
        .read_only()
        .repeatable_read()
        .run(|connection| {
            agent_sessions::table
                .filter(agent_sessions::user_id.eq(user_id))
                .filter(agent_sessions::id.eq(session_pk))
                .select(agent_sessions::id)
                .first::<Uuid>(connection)
                .optional()?
                .ok_or_else(|| AppError::not_found(format!("session not found: {session_pk}")))?;
            let mut response = scan_events(connection, user_id, session_pk, query)?;
            if !response.hits.is_empty() {
                let structure = session_structure(connection, user_id, session_pk)?;
                for hit in &mut response.hits {
                    hit.segment = structure.event_segment(hit.event_pk).map(str::to_owned);
                }
            }
            Ok(response)
        })
}

fn scan_events(
    connection: &mut PgConnection,
    user_id: Uuid,
    session_pk: Uuid,
    query: String,
) -> Result<InSessionSearchResponse, AppError> {
    let mut scan = SessionScan::new(query);
    let rows = llm_usage_events::table
        .left_join(llm_usage_event_stream_facts::table)
        .filter(llm_usage_events::user_id.eq(user_id))
        .filter(llm_usage_events::session_pk.eq(session_pk))
        .filter(llm_usage_events::conversation.is_not_null())
        .order((
            llm_usage_events::observed_at.asc(),
            llm_usage_events::id.asc(),
        ))
        .select((
            llm_usage_events::id,
            llm_usage_events::event_type,
            llm_usage_event_stream_facts::new_from.nullable(),
            llm_usage_events::conversation,
        ))
        .load_iter::<(Uuid, String, Option<i32>, Option<Value>), PgRowByRowLoadingMode>(
            connection,
        )?;
    for row in rows {
        let (event_pk, side, new_from, conversation) = row?;
        let event_type = match side.as_str() {
            "request" => UsageEventType::Request,
            "response" => UsageEventType::Response,
            _ => {
                return Err(AppError::internal(format!(
                    "stored event type is invalid: {side}"
                )));
            }
        };
        let new_from = new_from
            .map(u32::try_from)
            .transpose()
            .map_err(|error| AppError::internal(format!("stored new_from is invalid: {error}")))?;
        if scan.push(event_pk, event_type, new_from, conversation.as_ref())? {
            break;
        }
    }
    Ok(scan.finish())
}
