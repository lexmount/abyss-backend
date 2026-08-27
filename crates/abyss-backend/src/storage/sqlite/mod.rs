//! Self-contained SQLite source-of-truth with transactional FTS5 projection.

mod connection;
mod migrations;
mod models;
mod repository;
mod schema;
mod search;

use diesel::{QueryDsl, RunQueryDsl, SqliteConnection, dsl::count_star};
use uuid::Uuid;

use crate::{
    config::Config,
    error::AppError,
    search::{SessionSearchQuery, SessionSearchResponse},
    storage::{BoxedFuture, StorageBackend},
    usage::{
        IngestEventsRequest, IngestEventsResponse, RawEventsQuery, RawEventsResponse,
        SessionTimelineResponse, SummaryQuery, SummaryResponse, attachments::StoredImageAttachment,
    },
};

use self::connection::SqlitePool;

/// SQLite event store with an FTS5 projection committed beside each event.
pub(super) struct SqliteFtsBackend {
    pool: SqlitePool,
}

impl SqliteFtsBackend {
    pub(super) fn new(config: &Config) -> Result<Self, AppError> {
        let pool = connection::create_pool(&config.database_url, config.database_pool_size)?;
        {
            let mut database = pool.get()?;
            connection::configure_database(&mut database)?;
            if config.run_migrations {
                migrations::run(&mut database)?;
            }
        }
        Ok(Self { pool })
    }

    async fn run_db<T, F>(&self, task_fn: F) -> Result<T, AppError>
    where
        T: Send + 'static,
        F: FnOnce(&mut SqliteConnection) -> Result<T, AppError> + Send + 'static,
    {
        let pool = self.pool.clone();
        tokio::task::spawn_blocking(move || {
            let mut connection = pool.get()?;
            task_fn(&mut connection)
        })
        .await
        .map_err(|error| AppError::internal(format!("database task failed: {error}")))?
    }
}

impl StorageBackend for SqliteFtsBackend {
    fn ready(&self) -> BoxedFuture<'_, Result<(), AppError>> {
        Box::pin(async move {
            self.run_db(|connection| {
                schema::app_users::table
                    .select(count_star())
                    .first::<i64>(connection)?;
                Ok(())
            })
            .await
        })
    }

    fn ingest_events(
        &self,
        user_id: Uuid,
        request: IngestEventsRequest,
        max_batch_size: usize,
    ) -> BoxedFuture<'_, Result<IngestEventsResponse, AppError>> {
        Box::pin(async move {
            self.run_db(move |connection| {
                repository::ingest_events(connection, &request, user_id, max_batch_size)
            })
            .await
        })
    }

    fn raw_events(
        &self,
        user_id: Uuid,
        query: RawEventsQuery,
        default_page_size: i64,
    ) -> BoxedFuture<'_, Result<RawEventsResponse, AppError>> {
        Box::pin(async move {
            self.run_db(move |connection| {
                repository::raw_events(connection, &query, user_id, default_page_size)
            })
            .await
        })
    }

    fn usage_summary(
        &self,
        user_id: Uuid,
        query: SummaryQuery,
        summary_limit: i64,
    ) -> BoxedFuture<'_, Result<SummaryResponse, AppError>> {
        Box::pin(async move {
            self.run_db(move |connection| {
                repository::usage_summary(connection, &query, user_id, summary_limit)
            })
            .await
        })
    }

    fn session_timeline(
        &self,
        user_id: Uuid,
        session_pk: Uuid,
    ) -> BoxedFuture<'_, Result<SessionTimelineResponse, AppError>> {
        Box::pin(async move {
            self.run_db(move |connection| {
                repository::session_timeline(connection, user_id, session_pk)
            })
            .await
        })
    }

    fn image_attachment(
        &self,
        user_id: Uuid,
        attachment_id: Uuid,
    ) -> BoxedFuture<'_, Result<StoredImageAttachment, AppError>> {
        Box::pin(async move {
            self.run_db(move |connection| {
                repository::image_attachment(connection, user_id, attachment_id)
            })
            .await
        })
    }

    fn session_search(
        &self,
        user_id: Uuid,
        query: SessionSearchQuery,
    ) -> BoxedFuture<'_, Result<SessionSearchResponse, AppError>> {
        Box::pin(async move {
            self.run_db(move |connection| search::session_search(connection, user_id, query))
                .await
        })
    }

    fn shutdown(&self) -> BoxedFuture<'_, ()> {
        Box::pin(async {})
    }
}
