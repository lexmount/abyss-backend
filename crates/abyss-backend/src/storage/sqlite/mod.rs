//! Self-contained SQLite source-of-truth with transactional FTS5 projection.

mod connection;
mod migrations;
mod models;
mod repository;
mod search;

use async_trait::async_trait;
use rusqlite::Connection;
use uuid::Uuid;

use crate::{
    config::Config,
    error::AppError,
    search::{SessionSearchQuery, SessionSearchResponse},
    storage::StorageBackend,
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
            connection::configure_database(&database)?;
            if config.run_migrations {
                migrations::run(&mut database)?;
            }
        }
        Ok(Self { pool })
    }

    async fn run_db<T, F>(&self, task_fn: F) -> Result<T, AppError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T, AppError> + Send + 'static,
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

#[async_trait]
impl StorageBackend for SqliteFtsBackend {
    async fn ready(&self) -> Result<(), AppError> {
        self.run_db(|connection| {
            connection.query_row("SELECT 1", [], |_row| Ok(()))?;
            Ok(())
        })
        .await
    }

    async fn ingest_events(
        &self,
        user_id: Uuid,
        request: IngestEventsRequest,
        max_batch_size: usize,
    ) -> Result<IngestEventsResponse, AppError> {
        self.run_db(move |connection| {
            repository::ingest_events(connection, &request, user_id, max_batch_size)
        })
        .await
    }

    async fn raw_events(
        &self,
        user_id: Uuid,
        query: RawEventsQuery,
        default_page_size: i64,
    ) -> Result<RawEventsResponse, AppError> {
        self.run_db(move |connection| {
            repository::raw_events(connection, &query, user_id, default_page_size)
        })
        .await
    }

    async fn usage_summary(
        &self,
        user_id: Uuid,
        query: SummaryQuery,
        summary_limit: i64,
    ) -> Result<SummaryResponse, AppError> {
        self.run_db(move |connection| {
            repository::usage_summary(connection, &query, user_id, summary_limit)
        })
        .await
    }

    async fn session_timeline(
        &self,
        user_id: Uuid,
        session_pk: Uuid,
    ) -> Result<SessionTimelineResponse, AppError> {
        self.run_db(move |connection| repository::session_timeline(connection, user_id, session_pk))
            .await
    }

    async fn image_attachment(
        &self,
        user_id: Uuid,
        attachment_id: Uuid,
    ) -> Result<StoredImageAttachment, AppError> {
        self.run_db(move |connection| {
            repository::image_attachment(connection, user_id, attachment_id)
        })
        .await
    }

    async fn session_search(
        &self,
        user_id: Uuid,
        query: SessionSearchQuery,
    ) -> Result<SessionSearchResponse, AppError> {
        self.run_db(move |connection| search::session_search(connection, user_id, query))
            .await
    }

    async fn shutdown(&self) {}
}
