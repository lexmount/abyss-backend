//! `PostgreSQL` outbox leasing, retry transitions, and session ownership hydration.

use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};
use diesel::{
    ExpressionMethods, PgConnection, QueryDsl, QueryableByName, RunQueryDsl, SelectableHelper,
    sql_query,
    sql_types::{BigInt, Bool, Integer, Text, Uuid as SqlUuid},
};
use uuid::Uuid;

use crate::{
    db::{
        models::{AgentSession, Device, UsageEvent},
        schema::{agent_sessions, devices, llm_usage_events, search_outbox},
    },
    error::AppError,
};

use super::{document::SearchDocument, elasticsearch::BulkOperation};

const OUTBOX_LEASE_SECONDS: i64 = 120;
const MAX_OUTBOX_ATTEMPTS: i32 = 10;
const MAX_RETRY_DELAY_SECONDS: i64 = 300;
const MAX_ERROR_CHARACTERS: usize = 2_000;

/// One leased outbox row paired with the Elasticsearch operation it requires.
pub struct PreparedOutboxTask {
    pub id: i64,
    pub attempt_count: i32,
    pub operation: BulkOperation,
}

/// Result of one Elasticsearch bulk item, used to advance durable outbox state.
pub struct OutboxTaskResult {
    pub id: i64,
    pub attempt_count: i32,
    pub result: Result<(), String>,
}

pub struct SearchSessionDetails {
    pub session: AgentSession,
    pub device: Device,
}

pub struct SearchOutboxRepository;

impl SearchOutboxRepository {
    /// Queues a bounded slice of source events that predate the search outbox.
    pub fn advance_backfill_batch(
        connection: &mut PgConnection,
        batch_size: i64,
    ) -> Result<bool, AppError> {
        let queued = sql_query(
            "INSERT INTO search_outbox (event_pk, user_id, operation, created_at) \
             SELECT source.id, source.user_id, 'upsert', source.created_at \
             FROM llm_usage_events AS source \
             WHERE NOT EXISTS (\
                 SELECT 1 \
                 FROM search_outbox AS queued \
                 WHERE queued.event_pk = source.id \
                   AND queued.operation = 'upsert'\
             ) \
             ORDER BY source.id \
             LIMIT $1 \
             ON CONFLICT (event_pk, operation) DO NOTHING",
        )
        .bind::<BigInt, _>(batch_size)
        .execute(connection)?;
        if i64::try_from(queued).unwrap_or(i64::MAX) >= batch_size {
            return Ok(false);
        }

        let state = sql_query(
            "SELECT NOT EXISTS (\
                 SELECT 1 \
                 FROM llm_usage_events AS source \
                 WHERE NOT EXISTS (\
                     SELECT 1 \
                     FROM search_outbox AS queued \
                     WHERE queued.event_pk = source.id \
                       AND queued.operation = 'upsert'\
                 )\
             ) AS complete",
        )
        .get_result::<RawBackfillState>(connection)?;
        Ok(state.complete)
    }

    /// Leases pending rows and resolves their source events into bulk operations.
    pub fn claim_and_prepare(
        connection: &mut PgConnection,
        worker_id: &str,
        batch_size: i64,
    ) -> Result<Vec<PreparedOutboxTask>, AppError> {
        let tasks = sql_query(
            "WITH claimable AS (\
                 SELECT id \
                 FROM search_outbox \
                 WHERE processed_at IS NULL \
                   AND dead_lettered_at IS NULL \
                   AND available_at <= now() \
                   AND (claimed_at IS NULL OR claimed_at < now() - ($3 * interval '1 second')) \
                 ORDER BY id \
                 LIMIT $1 \
                 FOR UPDATE SKIP LOCKED\
             ) \
             UPDATE search_outbox AS task \
             SET claimed_at = now(), claimed_by = $2 \
             FROM claimable \
             WHERE task.id = claimable.id \
             RETURNING task.id, task.event_pk, task.operation, task.attempt_count",
        )
        .bind::<BigInt, _>(batch_size)
        .bind::<Text, _>(worker_id)
        .bind::<BigInt, _>(OUTBOX_LEASE_SECONDS)
        .load::<RawClaimedOutboxTask>(connection)?
        .into_iter()
        .map(ClaimedOutboxTask::try_from)
        .collect::<Result<Vec<_>, AppError>>()?;

        if tasks.is_empty() {
            return Ok(Vec::new());
        }

        let event_ids = tasks
            .iter()
            .filter(|task| matches!(&task.operation, SearchOutboxOperation::Upsert))
            .map(|task| task.event_pk)
            .collect::<Vec<_>>();
        let events = llm_usage_events::table
            .filter(llm_usage_events::id.eq_any(event_ids))
            .select(UsageEvent::as_select())
            .load::<UsageEvent>(connection)?
            .into_iter()
            .map(|event| (event.id, event))
            .collect::<HashMap<_, _>>();

        Ok(tasks
            .into_iter()
            .map(|task| {
                let operation = match task.operation {
                    SearchOutboxOperation::Upsert => events
                        .get(&task.event_pk)
                        .cloned()
                        .map(SearchDocument::from_event)
                        .map(Box::new)
                        .map_or(BulkOperation::Delete(task.event_pk), BulkOperation::Index),
                    // A source row that disappeared after an upsert task was
                    // queued is healed by deleting any stale search document.
                    SearchOutboxOperation::Delete => BulkOperation::Delete(task.event_pk),
                };
                PreparedOutboxTask {
                    id: task.id,
                    attempt_count: task.attempt_count,
                    operation,
                }
            })
            .collect())
    }

    /// Marks successful items complete and reschedules or dead-letters failures.
    pub fn record_results(
        connection: &mut PgConnection,
        results: Vec<OutboxTaskResult>,
    ) -> Result<(), AppError> {
        let now = Utc::now();
        for task in results {
            match task.result {
                Ok(()) => {
                    diesel::update(search_outbox::table.find(task.id))
                        .set((
                            search_outbox::processed_at.eq(Some(now)),
                            search_outbox::claimed_at.eq::<Option<DateTime<Utc>>>(None),
                            search_outbox::claimed_by.eq::<Option<String>>(None),
                            search_outbox::last_error.eq::<Option<String>>(None),
                        ))
                        .execute(connection)?;
                }
                Err(error) => {
                    let attempt_count = task.attempt_count.saturating_add(1);
                    let error = truncate_error(error);
                    if attempt_count >= MAX_OUTBOX_ATTEMPTS {
                        diesel::update(search_outbox::table.find(task.id))
                            .set((
                                search_outbox::attempt_count.eq(attempt_count),
                                search_outbox::dead_lettered_at.eq(Some(now)),
                                search_outbox::claimed_at.eq::<Option<DateTime<Utc>>>(None),
                                search_outbox::claimed_by.eq::<Option<String>>(None),
                                search_outbox::last_error.eq(Some(error)),
                            ))
                            .execute(connection)?;
                        tracing::error!(
                            outbox_id = task.id,
                            attempt_count,
                            "session search outbox task moved to dead letter"
                        );
                    } else {
                        let available_at = now
                            .checked_add_signed(Duration::seconds(retry_delay(attempt_count)))
                            .ok_or_else(|| {
                                AppError::internal(
                                    "session search retry timestamp overflow".to_owned(),
                                )
                            })?;
                        diesel::update(search_outbox::table.find(task.id))
                            .set((
                                search_outbox::attempt_count.eq(attempt_count),
                                search_outbox::available_at.eq(available_at),
                                search_outbox::claimed_at.eq::<Option<DateTime<Utc>>>(None),
                                search_outbox::claimed_by.eq::<Option<String>>(None),
                                search_outbox::last_error.eq(Some(error)),
                            ))
                            .execute(connection)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Reloads session/device rows under the authenticated owner boundary.
    pub fn session_details(
        connection: &mut PgConnection,
        user_id: Uuid,
        session_ids: &[Uuid],
    ) -> Result<HashMap<Uuid, SearchSessionDetails>, AppError> {
        if session_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let rows = agent_sessions::table
            .inner_join(devices::table)
            .filter(agent_sessions::user_id.eq(user_id))
            .filter(agent_sessions::id.eq_any(session_ids))
            .select((AgentSession::as_select(), Device::as_select()))
            .load::<(AgentSession, Device)>(connection)?;
        Ok(rows
            .into_iter()
            .map(|(session, device)| (session.id, SearchSessionDetails { session, device }))
            .collect())
    }
}

struct ClaimedOutboxTask {
    id: i64,
    event_pk: Uuid,
    operation: SearchOutboxOperation,
    attempt_count: i32,
}

enum SearchOutboxOperation {
    Upsert,
    Delete,
}

impl TryFrom<RawClaimedOutboxTask> for ClaimedOutboxTask {
    type Error = AppError;

    fn try_from(task: RawClaimedOutboxTask) -> Result<Self, Self::Error> {
        let operation = match task.operation.as_str() {
            "upsert" => SearchOutboxOperation::Upsert,
            "delete" => SearchOutboxOperation::Delete,
            value => {
                return Err(AppError::internal(format!(
                    "unknown session search outbox operation: {value}"
                )));
            }
        };
        Ok(Self {
            id: task.id,
            event_pk: task.event_pk,
            operation,
            attempt_count: task.attempt_count,
        })
    }
}

#[derive(QueryableByName)]
struct RawClaimedOutboxTask {
    #[diesel(sql_type = BigInt)]
    id: i64,
    #[diesel(sql_type = SqlUuid)]
    event_pk: Uuid,
    #[diesel(sql_type = Text)]
    operation: String,
    #[diesel(sql_type = Integer)]
    attempt_count: i32,
}

#[derive(QueryableByName)]
struct RawBackfillState {
    #[diesel(sql_type = Bool)]
    complete: bool,
}

fn retry_delay(attempt_count: i32) -> i64 {
    let exponent = u32::try_from(attempt_count.clamp(1_i32, 8_i32)).unwrap_or(8_u32);
    2_i64.saturating_pow(exponent).min(MAX_RETRY_DELAY_SECONDS)
}

fn truncate_error(mut error: String) -> String {
    let Some((byte_index, _character)) = error.char_indices().nth(MAX_ERROR_CHARACTERS) else {
        return error;
    };
    error.truncate(byte_index);
    error
}

#[cfg(test)]
mod tests {
    use super::{MAX_ERROR_CHARACTERS, retry_delay, truncate_error};

    #[test]
    fn retry_delay_is_bounded() {
        assert_eq!(retry_delay(1), 2);
        assert_eq!(retry_delay(4), 16);
        assert_eq!(retry_delay(100), 256);
    }

    #[test]
    fn error_truncation_preserves_utf8() {
        let error = "错".repeat(MAX_ERROR_CHARACTERS + 1);
        assert_eq!(truncate_error(error).chars().count(), MAX_ERROR_CHARACTERS);
    }
}
