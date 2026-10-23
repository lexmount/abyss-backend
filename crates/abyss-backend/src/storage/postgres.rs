//! PostgreSQL source-of-truth and Elasticsearch projection implementation.

mod comparison;
mod migrations;
mod repository;
mod search;
mod session_list;
mod session_titles;

use std::{sync::Arc, time::Duration};

use diesel::PgConnection;
use tokio::{sync::Mutex, task::JoinHandle};
use uuid::Uuid;

use crate::{
    config::Config,
    db::{self, DbPool},
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
        comparison::{ComparisonQuery, ComparisonResponse},
        preview::{ConversationPreview, Window},
        segments::{SegmentResponse, ValidatedSegmentQuery},
        session_header::SessionHeaderResponse,
        session_list::{SessionListResponse, ValidatedSessionListQuery},
        streams::{EventLocationResponse, SessionStreamsResponse, SessionTurnsResponse},
    },
};

use self::search::{SearchIndexer, SearchOutboxRepository, SearchService};

struct SearchWorker {
    shutdown: tokio::sync::watch::Sender<bool>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

/// Existing PostgreSQL and optional Elasticsearch storage stack.
pub struct PostgresEsBackend {
    pool: DbPool,
    structures: Arc<StreamCache>,
    search: Option<SearchService>,
    search_worker: Option<SearchWorker>,
}

impl PostgresEsBackend {
    pub(super) fn new(config: &Config) -> Result<Self, AppError> {
        let pool = db::create_pool(config)?;
        if config.run_migrations {
            let mut connection = pool.get()?;
            migrations::run(&mut connection)?;
        }

        let search = config.search.as_ref().map(SearchService::new).transpose()?;
        let search_worker = match (&search, config.search.as_ref()) {
            (Some(search), Some(search_config)) => {
                let (shutdown, receiver) = tokio::sync::watch::channel(false);
                let handle =
                    SearchIndexer::spawn(pool.clone(), search.client(), search_config, receiver);
                Some(SearchWorker {
                    shutdown,
                    handle: Mutex::new(Some(handle)),
                })
            }
            _ => None,
        };

        Ok(Self {
            pool,
            structures: Arc::new(StreamCache::default()),
            search,
            search_worker,
        })
    }

    async fn run_db<T, F>(&self, task_fn: F) -> Result<T, AppError>
    where
        T: Send + 'static,
        F: FnOnce(&mut PgConnection) -> Result<T, AppError> + Send + 'static,
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

impl StorageBackend for PostgresEsBackend {
    fn ready(&self) -> BoxedFuture<'_, Result<(), AppError>> {
        Box::pin(async move { self.run_db(db::check_ready).await })
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
                connection
                    .build_transaction()
                    .read_only()
                    .repeatable_read()
                    .run(|connection| {
                        let mut response =
                            repository::raw_events(connection, &query, user_id, default_page_size)?;
                        locate_events(&mut response.events, |session_pk| {
                            repository::session_structure(
                                connection,
                                user_id,
                                session_pk,
                                &structures,
                            )
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
                connection
                    .build_transaction()
                    .read_only()
                    .repeatable_read()
                    .run(|connection| {
                        let mut event = repository::usage_event(connection, user_id, event_pk)?;
                        locate_events(std::slice::from_mut(&mut event), |session_pk| {
                            repository::session_structure(
                                connection,
                                user_id,
                                session_pk,
                                &structures,
                            )
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
                connection
                    .build_transaction()
                    .read_only()
                    .repeatable_read()
                    .run(|connection| {
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

    fn usage_comparison(
        &self,
        user_id: Uuid,
        query: ComparisonQuery,
    ) -> BoxedFuture<'_, Result<ComparisonResponse, AppError>> {
        Box::pin(async move {
            self.run_db(move |connection| comparison::compare(connection, &query, user_id))
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
                connection
                    .build_transaction()
                    .read_only()
                    .repeatable_read()
                    .run(|connection| {
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
                connection
                    .build_transaction()
                    .read_only()
                    .repeatable_read()
                    .run(|connection| {
                        repository::session_streams(connection, user_id, session_pk, &structures)
                    })
            })
            .await
        })
    }

    fn session_turns(
        &self,
        user_id: Uuid,
        session_pk: Uuid,
    ) -> BoxedFuture<'_, Result<SessionTurnsResponse, AppError>> {
        let structures = Arc::clone(&self.structures);
        Box::pin(async move {
            self.run_db(move |connection| {
                connection
                    .build_transaction()
                    .read_only()
                    .repeatable_read()
                    .run(|connection| {
                        repository::session_turns(connection, user_id, session_pk, &structures)
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
                connection
                    .build_transaction()
                    .read_only()
                    .repeatable_read()
                    .run(|connection| {
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
            let search = self.search.clone().ok_or_else(|| {
                AppError::unavailable("session search is not configured".to_owned())
            })?;
            let execution = search.search(user_id, query).await?;
            let session_ids = execution.session_ids();
            self.run_db(move |connection| {
                connection
                    .build_transaction()
                    .read_only()
                    .repeatable_read()
                    .run(|connection| {
                        let details = SearchOutboxRepository::session_details(
                            connection,
                            user_id,
                            &session_ids,
                        )?;
                        let mut response = execution.hydrate(details);
                        locate_search_matches(&mut response, |session_pk| {
                            repository::session_structure(
                                connection,
                                user_id,
                                session_pk,
                                &structures,
                            )
                        })?;
                        Ok(response)
                    })
            })
            .await
        })
    }

    /// Elasticsearch holds each event's text as one document without entry
    /// positions, so this profile cannot list the hits within a session.
    fn search_in_session(
        &self,
        _user_id: Uuid,
        _session_pk: Uuid,
        query: InSessionSearchQuery,
    ) -> BoxedFuture<'_, Result<InSessionSearchResponse, AppError>> {
        Box::pin(async move {
            query.validate()?;
            Err(AppError::unavailable(
                "search within a session is not supported by this storage backend".to_owned(),
            ))
        })
    }

    fn shutdown(&self) -> BoxedFuture<'_, ()> {
        Box::pin(async move {
            let Some(worker) = &self.search_worker else {
                return;
            };
            if worker.shutdown.send(true).is_err() {
                tracing::trace!("session search indexer already stopped");
            }
            let Some(mut handle) = worker.handle.lock().await.take() else {
                return;
            };
            match tokio::time::timeout(Duration::from_secs(15), &mut handle).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => tracing::warn!(%error, "session search indexer task failed"),
                Err(_elapsed) => {
                    tracing::warn!("session search indexer did not stop before timeout");
                    handle.abort();
                }
            }
        })
    }
}
