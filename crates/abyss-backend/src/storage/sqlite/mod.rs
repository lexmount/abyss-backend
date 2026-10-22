//! Self-contained SQLite source-of-truth with its search index written in
//! the same transactions.

mod connection;
mod day_offsets;
mod migrations;
mod models;
mod repository;
mod schema;
mod search;
mod session_list;
mod session_titles;

use std::sync::Arc;

use diesel::{Connection, QueryDsl, RunQueryDsl, SqliteConnection, dsl::count_star};
use uuid::Uuid;

use crate::{
    config::Config,
    error::AppError,
    search::{
        InSessionSearchQuery, InSessionSearchResponse, SessionSearchQuery, SessionSearchResponse,
    },
    storage::{
        BoxedFuture, StorageBackend,
        event_locations::{locate_events, locate_search_matches},
        stream_cache::StreamCache,
    },
    usage::{
        IngestEventsRequest, IngestEventsResponse, RawEventsQuery, RawEventsResponse, SummaryQuery,
        SummaryResponse, UsageEventResponse,
        attachments::StoredImageAttachment,
        preview::{ConversationPreview, Window},
        segments::{SegmentResponse, ValidatedSegmentQuery},
        session_header::SessionHeaderResponse,
        session_list::{SessionListResponse, ValidatedSessionListQuery},
        streams::{EventLocationResponse, SessionStreamsResponse},
    },
};

use self::connection::SqlitePool;

/// SQLite event store with a search index committed beside each event.
pub(super) struct SqliteFtsBackend {
    pool: SqlitePool,
    structures: Arc<StreamCache>,
}

impl SqliteFtsBackend {
    pub(super) fn new(config: &Config) -> Result<Self, AppError> {
        let pool = connection::create_pool(&config.database_url, config.database_pool_size)?;
        {
            let mut database = pool.get()?;
            connection::configure_database(&mut database)?;
            if config.run_migrations {
                migrations::run(&mut database)?;
                search::backfill(&mut database)?;
            }
        }
        Ok(Self {
            pool,
            structures: Arc::new(StreamCache::default()),
        })
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
        let structures = Arc::clone(&self.structures);
        Box::pin(async move {
            self.run_db(move |connection| {
                if !query.locate {
                    return repository::raw_events(connection, &query, user_id, default_page_size);
                }
                connection.transaction(|connection| {
                    let mut response =
                        repository::raw_events(connection, &query, user_id, default_page_size)?;
                    locate_events(&mut response.events, |session_pk| {
                        repository::session_structure(connection, user_id, session_pk, &structures)
                    })?;
                    Ok(response)
                })
            })
            .await
        })
    }

    fn usage_event(
        &self,
        user_id: Uuid,
        event_pk: Uuid,
    ) -> BoxedFuture<'_, Result<UsageEventResponse, AppError>> {
        let structures = Arc::clone(&self.structures);
        Box::pin(async move {
            self.run_db(move |connection| {
                connection.transaction(|connection| {
                    let mut event = repository::usage_event(connection, user_id, event_pk)?;
                    locate_events(std::slice::from_mut(&mut event), |session_pk| {
                        repository::session_structure(connection, user_id, session_pk, &structures)
                    })?;
                    Ok(event)
                })
            })
            .await
        })
    }

    fn event_location(
        &self,
        user_id: Uuid,
        event_pk: Uuid,
    ) -> BoxedFuture<'_, Result<EventLocationResponse, AppError>> {
        let structures = Arc::clone(&self.structures);
        Box::pin(async move {
            self.run_db(move |connection| {
                connection.transaction(|connection| {
                    repository::event_location(connection, user_id, event_pk, &structures)
                })
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

    fn session_header(
        &self,
        user_id: Uuid,
        session_pk: Uuid,
    ) -> BoxedFuture<'_, Result<SessionHeaderResponse, AppError>> {
        let structures = Arc::clone(&self.structures);
        Box::pin(async move {
            self.run_db(move |connection| {
                connection.transaction(|connection| {
                    repository::session_header(connection, user_id, session_pk, &structures)
                })
            })
            .await
        })
    }

    fn session_streams(
        &self,
        user_id: Uuid,
        session_pk: Uuid,
    ) -> BoxedFuture<'_, Result<SessionStreamsResponse, AppError>> {
        let structures = Arc::clone(&self.structures);
        Box::pin(async move {
            self.run_db(move |connection| {
                connection.transaction(|connection| {
                    repository::session_streams(connection, user_id, session_pk, &structures)
                })
            })
            .await
        })
    }

    fn session_segment(
        &self,
        user_id: Uuid,
        session_pk: Uuid,
        head: String,
        query: ValidatedSegmentQuery,
    ) -> BoxedFuture<'_, Result<SegmentResponse, AppError>> {
        let structures = Arc::clone(&self.structures);
        Box::pin(async move {
            self.run_db(move |connection| {
                connection.transaction(|connection| {
                    repository::session_segment(
                        connection,
                        user_id,
                        session_pk,
                        &head,
                        &query,
                        &structures,
                    )
                })
            })
            .await
        })
    }

    fn event_conversation(
        &self,
        user_id: Uuid,
        event_pk: Uuid,
        window: Window,
    ) -> BoxedFuture<'_, Result<ConversationPreview, AppError>> {
        Box::pin(async move {
            self.run_db(move |connection| {
                repository::event_conversation(connection, user_id, event_pk, &window)
            })
            .await
        })
    }

    fn list_sessions(
        &self,
        user_id: Uuid,
        query: ValidatedSessionListQuery,
    ) -> BoxedFuture<'_, Result<SessionListResponse, AppError>> {
        Box::pin(async move {
            self.run_db(move |connection| session_list::list_sessions(connection, user_id, &query))
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
        let structures = Arc::clone(&self.structures);
        Box::pin(async move {
            self.run_db(move |connection| {
                connection.transaction(|connection| {
                    let mut response = search::session_search(connection, user_id, query)?;
                    locate_search_matches(&mut response, |session_pk| {
                        repository::session_structure(connection, user_id, session_pk, &structures)
                    })?;
                    Ok(response)
                })
            })
            .await
        })
    }

    fn search_in_session(
        &self,
        user_id: Uuid,
        session_pk: Uuid,
        query: InSessionSearchQuery,
    ) -> BoxedFuture<'_, Result<InSessionSearchResponse, AppError>> {
        let structures = Arc::clone(&self.structures);
        Box::pin(async move {
            self.run_db(move |connection| {
                search::search_in_session(connection, user_id, session_pk, &query, &structures)
            })
            .await
        })
    }

    fn shutdown(&self) -> BoxedFuture<'_, ()> {
        Box::pin(async {})
    }
}
