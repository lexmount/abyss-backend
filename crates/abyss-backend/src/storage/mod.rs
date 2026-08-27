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

use std::{future::Future, pin::Pin, sync::Arc};

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

/// Sendable heap-allocated future returned across the dynamic storage boundary.
pub type BoxedFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Complete object-safe event-store contract consumed by the HTTP layer.
pub trait StorageBackend: Send + Sync {
    /// Verifies that the authoritative database can serve requests.
    fn ready(&self) -> BoxedFuture<'_, Result<(), AppError>>;

    /// Validates and atomically ingests one owner-scoped event batch.
    fn ingest_events(
        &self,
        user_id: Uuid,
        request: IngestEventsRequest,
        max_batch_size: usize,
    ) -> BoxedFuture<'_, Result<IngestEventsResponse, AppError>>;

    /// Returns one newest-first page of owner-scoped raw events.
    fn raw_events(
        &self,
        user_id: Uuid,
        query: RawEventsQuery,
        default_page_size: i64,
    ) -> BoxedFuture<'_, Result<RawEventsResponse, AppError>>;

    /// Aggregates owner-scoped event and token usage.
    fn usage_summary(
        &self,
        user_id: Uuid,
        query: SummaryQuery,
        summary_limit: i64,
    ) -> BoxedFuture<'_, Result<SummaryResponse, AppError>>;

    /// Loads one owner-scoped session timeline.
    fn session_timeline(
        &self,
        user_id: Uuid,
        session_pk: Uuid,
    ) -> BoxedFuture<'_, Result<SessionTimelineResponse, AppError>>;

    /// Loads authorized image bytes by attachment identifier.
    fn image_attachment(
        &self,
        user_id: Uuid,
        attachment_id: Uuid,
    ) -> BoxedFuture<'_, Result<StoredImageAttachment, AppError>>;

    /// Executes one owner-scoped full-text session search.
    fn session_search(
        &self,
        user_id: Uuid,
        query: SessionSearchQuery,
    ) -> BoxedFuture<'_, Result<SessionSearchResponse, AppError>>;

    /// Stops backend-owned background work and releases lifecycle resources.
    fn shutdown(&self) -> BoxedFuture<'_, ()>;
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
