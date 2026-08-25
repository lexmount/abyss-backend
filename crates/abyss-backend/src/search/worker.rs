//! Background projection worker from the PostgreSQL outbox into Elasticsearch.
//!
//! The worker performs blocking database operations on Tokio's blocking pool,
//! batches Elasticsearch writes, and persists each bulk-item result separately.
//! It polls only while idle or unhealthy; full batches are drained immediately
//! to reduce projection lag without busy-looping an empty queue.

use std::time::Duration;

use tokio::{sync::watch, task::JoinHandle};
use uuid::Uuid;

use crate::{
    config::SearchConfig,
    db::DbPool,
    error::AppError,
    search::{
        elasticsearch::ElasticsearchClient,
        outbox::{OutboxTaskResult, SearchOutboxRepository},
    },
};

/// Factory for the detached search projection task.
pub struct SearchIndexer;

impl SearchIndexer {
    /// Spawns one indexer with a unique lease owner identifier.
    ///
    /// The returned handle must be joined or aborted during service shutdown.
    #[must_use]
    pub fn spawn(
        pool: DbPool,
        client: ElasticsearchClient,
        config: &SearchConfig,
        shutdown: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        let worker = SearchIndexerWorker {
            pool,
            client,
            worker_id: format!("abyss-backend-{}", Uuid::now_v7()),
            batch_size: config.batch_size,
            poll_interval: Duration::from_millis(config.poll_interval_milliseconds),
            shutdown,
        };
        tokio::spawn(worker.run())
    }
}

struct SearchIndexerWorker {
    pool: DbPool,
    client: ElasticsearchClient,
    worker_id: String,
    batch_size: i64,
    poll_interval: Duration,
    shutdown: watch::Receiver<bool>,
}

enum PollSchedule {
    /// Start another iteration without sleeping because work may remain.
    Immediately,
    /// Wait for the configured interval or shutdown notification.
    AfterInterval,
}

impl SearchIndexerWorker {
    async fn run(mut self) {
        let mut index_ready = false;
        let mut backfill_complete = false;
        tracing::info!(worker_id = %self.worker_id, "session search indexer started");
        loop {
            if *self.shutdown.borrow() {
                break;
            }
            if matches!(
                self.process_iteration(&mut index_ready, &mut backfill_complete)
                    .await,
                PollSchedule::AfterInterval
            ) && self.wait_for_next_poll().await
            {
                break;
            }
        }
        tracing::info!(worker_id = %self.worker_id, "session search indexer stopped");
    }

    async fn process_iteration(
        &self,
        index_ready: &mut bool,
        backfill_complete: &mut bool,
    ) -> PollSchedule {
        if !self.ensure_index_ready(index_ready).await {
            return PollSchedule::AfterInterval;
        }

        // Historical rows are queued incrementally so enabling search on an
        // existing installation does not require a separate migration job.
        if !*backfill_complete && let Err(error) = self.advance_backfill(backfill_complete).await {
            tracing::error!(%error, "queue session search backfill batch");
            return PollSchedule::AfterInterval;
        }

        let tasks = match self.claim_tasks().await {
            Ok(tasks) => tasks,
            Err(error) => {
                tracing::error!(%error, "claim session search outbox tasks");
                return PollSchedule::AfterInterval;
            }
        };
        if tasks.is_empty() {
            return PollSchedule::AfterInterval;
        }

        let task_count = self.apply_tasks(tasks, index_ready).await;
        if i64::try_from(task_count).unwrap_or(i64::MAX) < self.batch_size {
            PollSchedule::AfterInterval
        } else {
            PollSchedule::Immediately
        }
    }

    async fn ensure_index_ready(&self, ready: &mut bool) -> bool {
        if *ready {
            return true;
        }
        if let Err(error) = self.client.ensure_index().await {
            tracing::warn!(%error, "session search index is unavailable");
            return false;
        }
        *ready = true;
        true
    }

    async fn apply_tasks(
        &self,
        tasks: Vec<super::outbox::PreparedOutboxTask>,
        index_ready: &mut bool,
    ) -> usize {
        let task_count = tasks.len();
        // Keep durable state aligned by position with bulk operations. The ES
        // boundary guarantees one response result for every submitted item.
        let (task_states, operations): (Vec<_>, Vec<_>) = tasks
            .into_iter()
            .map(|task| ((task.id, task.attempt_count), task.operation))
            .unzip();
        let item_results = match self.client.apply_bulk(&operations).await {
            Ok(results) => {
                if results.iter().any(Result::is_err) {
                    *index_ready = false;
                }
                results
            }
            Err(error) => {
                // A request-level failure has no trustworthy per-item result;
                // retry every leased task and force the index check to rerun.
                *index_ready = false;
                let message = error.to_string();
                task_states
                    .iter()
                    .map(|_task| Err(message.clone()))
                    .collect()
            }
        };
        let results = task_states
            .into_iter()
            .zip(item_results)
            .map(|((id, attempt_count), result)| OutboxTaskResult {
                id,
                attempt_count,
                result,
            })
            .collect();
        if let Err(error) = self.record_results(results).await {
            tracing::error!(%error, "record session search outbox results");
        }
        task_count
    }

    async fn claim_tasks(&self) -> Result<Vec<super::outbox::PreparedOutboxTask>, AppError> {
        let pool = self.pool.clone();
        let worker_id = self.worker_id.clone();
        let batch_size = self.batch_size;
        tokio::task::spawn_blocking(move || {
            let mut connection = pool.get()?;
            SearchOutboxRepository::claim_and_prepare(&mut connection, &worker_id, batch_size)
        })
        .await
        .map_err(|error| AppError::internal(format!("session search indexer join: {error}")))?
    }

    async fn advance_backfill_batch(&self) -> Result<bool, AppError> {
        let pool = self.pool.clone();
        let batch_size = self.batch_size;
        tokio::task::spawn_blocking(move || {
            let mut connection = pool.get()?;
            SearchOutboxRepository::advance_backfill_batch(&mut connection, batch_size)
        })
        .await
        .map_err(|error| AppError::internal(format!("session search indexer join: {error}")))?
    }

    async fn advance_backfill(&self, complete: &mut bool) -> Result<(), AppError> {
        *complete = self.advance_backfill_batch().await?;
        if *complete {
            tracing::info!("session search historical backfill queued");
        }
        Ok(())
    }

    async fn record_results(&self, results: Vec<OutboxTaskResult>) -> Result<(), AppError> {
        let pool = self.pool.clone();
        tokio::task::spawn_blocking(move || {
            let mut connection = pool.get()?;
            SearchOutboxRepository::record_results(&mut connection, results)
        })
        .await
        .map_err(|error| AppError::internal(format!("session search indexer join: {error}")))?
    }

    /// Returns true when shutdown was requested while waiting.
    async fn wait_for_next_poll(&mut self) -> bool {
        match tokio::time::timeout(self.poll_interval, self.shutdown.changed()).await {
            Ok(Ok(())) => *self.shutdown.borrow(),
            Ok(Err(_closed)) => true,
            Err(_elapsed) => false,
        }
    }
}
