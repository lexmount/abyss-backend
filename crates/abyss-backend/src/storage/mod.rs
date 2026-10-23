//! Compile-time-selected persistence and search compatibility boundary.
//!
//! HTTP handlers depend only on [`StorageBackend`]. Exactly one concrete
//! implementation is compiled into a binary, while dynamic dispatch keeps
//! connection pools, SQL dialects, search projection, and worker lifecycle out
//! of the API layer.

mod event_locations;
#[cfg(feature = "postgres-es")]
mod postgres;
#[cfg(feature = "sqlite-fts")]
mod sqlite;
mod stream_cache;

use std::{future::Future, pin::Pin, sync::Arc};

use uuid::Uuid;

use crate::{
    config::Config,
    error::AppError,
    search::{
        InSessionSearchQuery, InSessionSearchResponse, SessionSearchQuery, SessionSearchResponse,
    },
    usage::{
        IngestEventsRequest, IngestEventsResponse, RawEventsQuery, RawEventsResponse, SummaryQuery,
        SummaryResponse, UsageEventResponse,
        attachments::StoredImageAttachment,
        preview::{ConversationPreview, Window},
        segments::{SegmentResponse, ValidatedSegmentQuery},
        session_header::SessionHeaderResponse,
        session_list::{SessionListResponse, ValidatedSessionListQuery},
        streams::{EventLocationResponse, SessionStreamsResponse, SessionTurnsResponse},
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

    /// Loads one owner-scoped request or response event with its full content.
    fn usage_event(
        &self,
        user_id: Uuid,
        event_pk: Uuid,
    ) -> BoxedFuture<'_, Result<UsageEventResponse, AppError>>;

    /// Resolves a stable event identifier to its current session segment and exchange.
    fn event_location(
        &self,
        user_id: Uuid,
        event_pk: Uuid,
    ) -> BoxedFuture<'_, Result<EventLocationResponse, AppError>>;

    /// Aggregates owner-scoped event and token usage.
    fn usage_summary(
        &self,
        user_id: Uuid,
        query: SummaryQuery,
        summary_limit: i64,
    ) -> BoxedFuture<'_, Result<SummaryResponse, AppError>>;

    /// The header of an owner-scoped session; not found when the owner has
    /// no event in it.
    fn session_header(
        &self,
        user_id: Uuid,
        session_pk: Uuid,
    ) -> BoxedFuture<'_, Result<SessionHeaderResponse, AppError>>;

    /// Derives the segments of an owner-scoped session; not found when the
    /// owner has no event in it.
    fn session_streams(
        &self,
        user_id: Uuid,
        session_pk: Uuid,
    ) -> BoxedFuture<'_, Result<SessionStreamsResponse, AppError>>;

    /// Derives the turn view of an owner-scoped session; not found when the
    /// owner has no event in it.
    fn session_turns(
        &self,
        user_id: Uuid,
        session_pk: Uuid,
    ) -> BoxedFuture<'_, Result<SessionTurnsResponse, AppError>>;

    /// The exchanges of the segment holding exchange `head` in
    /// an owner-scoped session, from `head` to the segment end; not found when the session
    /// has no such exchange.
    fn session_segment(
        &self,
        user_id: Uuid,
        session_pk: Uuid,
        head: String,
        query: ValidatedSegmentQuery,
    ) -> BoxedFuture<'_, Result<SegmentResponse, AppError>>;

    /// The conversation of one owner-scoped event through `window`.
    fn event_conversation(
        &self,
        user_id: Uuid,
        event_pk: Uuid,
        window: Window,
    ) -> BoxedFuture<'_, Result<ConversationPreview, AppError>>;

    /// Lists one page of owner-scoped sessions, most recently active first.
    fn list_sessions(
        &self,
        user_id: Uuid,
        query: ValidatedSessionListQuery,
    ) -> BoxedFuture<'_, Result<SessionListResponse, AppError>>;

    /// Loads authorized image bytes by attachment identifier.
    fn image_attachment(
        &self,
        user_id: Uuid,
        attachment_id: Uuid,
    ) -> BoxedFuture<'_, Result<StoredImageAttachment, AppError>>;

    /// Executes one owner-scoped session search.
    fn session_search(
        &self,
        user_id: Uuid,
        query: SessionSearchQuery,
    ) -> BoxedFuture<'_, Result<SessionSearchResponse, AppError>>;

    /// Finds every hit of a query in one owner-scoped session, with the
    /// segment of each; not found when the owner has no such session.
    fn search_in_session(
        &self,
        user_id: Uuid,
        session_pk: Uuid,
        query: InSessionSearchQuery,
    ) -> BoxedFuture<'_, Result<InSessionSearchResponse, AppError>>;

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
