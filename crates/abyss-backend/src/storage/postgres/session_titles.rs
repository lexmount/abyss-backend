//! The earliest request anchors a session's title to its stored stream label.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use diesel::{
    PgConnection, QueryableByName, RunQueryDsl, sql_query,
    sql_types::{Array, Integer, Nullable, Text, Timestamptz, Uuid as SqlUuid},
};
use uuid::Uuid;

use crate::{error::AppError, usage::streams::EntryLabel};

/// One stored event considered for the session's first-request anchor.
pub(super) struct CapturedEvent<'a> {
    pub(super) user_id: Uuid,
    pub(super) session_pk: Uuid,
    pub(super) event_pk: Uuid,
    pub(super) event_type: &'a str,
    pub(super) observed_at: DateTime<Utc>,
}

/// Registers the earliest request even when that request has no label.
/// A request delivered later can replace the anchor if its order is earlier.
pub(super) fn capture(
    connection: &mut PgConnection,
    event: &CapturedEvent<'_>,
) -> Result<bool, AppError> {
    if event.event_type != "request" {
        return Ok(false);
    }
    let changed = sql_query(
        "INSERT INTO session_titles \
         (session_pk, user_id, title, title_event_pk, title_observed_at, updated_at) \
         VALUES ($1, $2, NULL, $3, $4, CURRENT_TIMESTAMP) \
         ON CONFLICT (session_pk) DO UPDATE SET \
         title = NULL, title_event_pk = EXCLUDED.title_event_pk, \
         title_observed_at = EXCLUDED.title_observed_at, updated_at = EXCLUDED.updated_at \
         WHERE session_titles.title_event_pk IS NULL \
         OR session_titles.title_observed_at IS NULL \
         OR (EXCLUDED.title_observed_at, EXCLUDED.title_event_pk) \
            < (session_titles.title_observed_at, session_titles.title_event_pk)",
    )
    .bind::<SqlUuid, _>(event.session_pk)
    .bind::<SqlUuid, _>(event.user_id)
    .bind::<SqlUuid, _>(event.event_pk)
    .bind::<Timestamptz, _>(event.observed_at)
    .execute(connection)?;
    Ok(changed == 1)
}

/// Rebuilds only first-request anchors within the caller's transaction. The
/// indexed request lookup stops at its first row regardless of label presence.
/// Existing labels remain facts of their events; old bodies are never decoded.
pub(super) fn rebuild(connection: &mut PgConnection) -> Result<(), AppError> {
    sql_query(
        "UPDATE session_titles SET title = NULL, title_event_pk = NULL, \
         title_observed_at = NULL, updated_at = CURRENT_TIMESTAMP",
    )
    .execute(connection)?;
    sql_query(
        "INSERT INTO session_titles \
         (session_pk, user_id, title, title_event_pk, title_observed_at, updated_at) \
         SELECT s.id, s.user_id, NULL, first_request.id, first_request.observed_at, CURRENT_TIMESTAMP \
         FROM agent_sessions s CROSS JOIN LATERAL ( \
             SELECT e.id, e.observed_at FROM llm_usage_events e \
             WHERE e.session_pk = s.id AND e.event_type = 'request' \
             ORDER BY e.observed_at, e.id LIMIT 1 \
         ) first_request \
         ON CONFLICT (session_pk) DO UPDATE SET \
         title = NULL, title_event_pk = EXCLUDED.title_event_pk, \
         title_observed_at = EXCLUDED.title_observed_at, updated_at = EXCLUDED.updated_at",
    )
    .execute(connection)?;
    Ok(())
}

#[derive(QueryableByName)]
struct StoredLabel {
    #[diesel(sql_type = SqlUuid)]
    session_pk: Uuid,
    #[diesel(sql_type = Nullable<SqlUuid>)]
    event_pk: Option<Uuid>,
    #[diesel(sql_type = Nullable<Integer>)]
    entry_index: Option<i32>,
    #[diesel(sql_type = Nullable<Text>)]
    text: Option<String>,
    #[diesel(sql_type = Nullable<Integer>)]
    length: Option<i32>,
}

/// Reads the anchor request's existing label for both the header and picker.
pub(super) fn title(
    connection: &mut PgConnection,
    session_pk: Uuid,
) -> Result<Option<EntryLabel>, AppError> {
    Ok(titles(connection, &[session_pk])?.remove(&session_pk))
}

/// Reads labels for an already authorized page of sessions in one query.
pub(super) fn titles(
    connection: &mut PgConnection,
    session_pks: &[Uuid],
) -> Result<HashMap<Uuid, EntryLabel>, AppError> {
    if session_pks.is_empty() {
        return Ok(HashMap::new());
    }
    Ok(sql_query(
        "SELECT st.session_pk, st.title_event_pk AS event_pk, f.label_index AS entry_index, \
         f.label_text AS text, f.label_length AS length \
         FROM session_titles st \
         LEFT JOIN llm_usage_event_stream_facts f ON f.event_pk = st.title_event_pk \
         WHERE st.session_pk = ANY($1)",
    )
    .bind::<Array<SqlUuid>, _>(session_pks)
    .load::<StoredLabel>(connection)?
    .into_iter()
    .filter_map(|row| {
        EntryLabel::from_columns(row.event_pk, row.entry_index, row.text, row.length)
            .map(|label| (row.session_pk, label))
    })
    .collect())
}
