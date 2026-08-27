//! Compile-time-selected persistence and full-text search compatibility boundary.
//!
//! HTTP handlers depend only on [`StorageBackend`]. Exactly one concrete
//! implementation is compiled into a binary, while dynamic dispatch keeps
//! connection pools, SQL dialects, search projection, and worker lifecycle out
//! of the API layer.

#[cfg(feature = "postgres-es")]
mod postgres;
#[cfg(feature = "sqlite-fts")]
mod sqlite;

use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::{
    config::Config,
    error::AppError,
    search::{SessionSearchQuery, SessionSearchResponse},
    usage::{
        IngestEventsRequest, IngestEventsResponse, RawEventsQuery, RawEventsResponse,
        SessionTimelineResponse, SummaryQuery, SummaryResponse, attachments::StoredImageAttachment,
    },
};

/// Complete event-store contract consumed by the HTTP layer.
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Verifies that the authoritative database can serve requests.
    async fn ready(&self) -> Result<(), AppError>;

    /// Validates and atomically ingests one owner-scoped event batch.
    async fn ingest_events(
        &self,
        user_id: Uuid,
        request: IngestEventsRequest,
        max_batch_size: usize,
    ) -> Result<IngestEventsResponse, AppError>;

    /// Returns one newest-first page of owner-scoped raw events.
    async fn raw_events(
        &self,
        user_id: Uuid,
        query: RawEventsQuery,
        default_page_size: i64,
    ) -> Result<RawEventsResponse, AppError>;

    /// Aggregates owner-scoped event and token usage.
    async fn usage_summary(
        &self,
        user_id: Uuid,
        query: SummaryQuery,
        summary_limit: i64,
    ) -> Result<SummaryResponse, AppError>;

    /// Loads one owner-scoped session timeline.
    async fn session_timeline(
        &self,
        user_id: Uuid,
        session_pk: Uuid,
    ) -> Result<SessionTimelineResponse, AppError>;

    /// Loads authorized image bytes by attachment identifier.
    async fn image_attachment(
        &self,
        user_id: Uuid,
        attachment_id: Uuid,
    ) -> Result<StoredImageAttachment, AppError>;

    /// Executes one owner-scoped full-text session search.
    async fn session_search(
        &self,
        user_id: Uuid,
        query: SessionSearchQuery,
    ) -> Result<SessionSearchResponse, AppError>;

    /// Stops backend-owned background work and releases lifecycle resources.
    async fn shutdown(&self);
}

/// Builds the one storage implementation selected by Cargo features.
#[cfg(all(feature = "postgres-es", not(feature = "sqlite-fts")))]
pub fn build(config: &Config) -> Result<Arc<dyn StorageBackend>, AppError> {
    postgres::PostgresEsBackend::new(config)
        .map(|backend| -> Arc<dyn StorageBackend> { Arc::new(backend) })
}

/// Builds the one storage implementation selected by Cargo features.
#[cfg(all(feature = "sqlite-fts", not(feature = "postgres-es")))]
pub fn build(config: &Config) -> Result<Arc<dyn StorageBackend>, AppError> {
    sqlite::SqliteFtsBackend::new(config)
        .map(|backend| -> Arc<dyn StorageBackend> { Arc::new(backend) })
}

/// Reports an invalid storage feature selection after the compile-time error.
#[cfg(any(
    all(feature = "postgres-es", feature = "sqlite-fts"),
    not(any(feature = "postgres-es", feature = "sqlite-fts"))
))]
pub fn build(_config: &Config) -> Result<Arc<dyn StorageBackend>, AppError> {
    Err(AppError::config(
        "one storage backend feature must be selected".to_owned(),
    ))
}
