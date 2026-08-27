//! PostgreSQL source-of-truth and Elasticsearch projection implementation.

use std::time::Duration;

use async_trait::async_trait;
use diesel::PgConnection;
use tokio::{sync::Mutex, task::JoinHandle};
use uuid::Uuid;

use crate::{
    config::Config,
    db::{self, DbPool},
    error::AppError,
    search::{
        SearchService, SessionSearchQuery, SessionSearchResponse, outbox::SearchOutboxRepository,
    },
    storage::StorageBackend,
    usage::{
        IngestEventsRequest, IngestEventsResponse, RawEventsQuery, RawEventsResponse,
        SessionTimelineResponse, SummaryQuery, SummaryResponse, attachments::StoredImageAttachment,
        repository as usage_repository,
    },
};

struct SearchWorker {
    shutdown: tokio::sync::watch::Sender<bool>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

/// Existing PostgreSQL and optional Elasticsearch storage stack.
pub struct PostgresEsBackend {
    pool: DbPool,
    search: Option<SearchService>,
    search_worker: Option<SearchWorker>,
}

impl PostgresEsBackend {
    pub(super) fn new(config: &Config) -> Result<Self, AppError> {
        let pool = db::create_pool(config)?;
        if config.run_migrations {
            db::run_migrations(&pool).map_err(|error| {
                AppError::internal(format!("run PostgreSQL migrations: {error}"))
            })?;
        }

        let search = config.search.as_ref().map(SearchService::new).transpose()?;
        let search_worker = match (&search, config.search.as_ref()) {
            (Some(search), Some(search_config)) => {
                let (shutdown, receiver) = tokio::sync::watch::channel(false);
                let handle = crate::search::worker::SearchIndexer::spawn(
                    pool.clone(),
                    search.client(),
                    search_config,
                    receiver,
                );
                Some(SearchWorker {
                    shutdown,
                    handle: Mutex::new(Some(handle)),
                })
            }
            _ => None,
        };

        Ok(Self {
            pool,
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

#[async_trait]
impl StorageBackend for PostgresEsBackend {
    async fn ready(&self) -> Result<(), AppError> {
        self.run_db(db::check_ready).await
    }

    async fn ingest_events(
        &self,
        user_id: Uuid,
        request: IngestEventsRequest,
        max_batch_size: usize,
    ) -> Result<IngestEventsResponse, AppError> {
        self.run_db(move |connection| {
            usage_repository::ingest_events(connection, &request, user_id, max_batch_size)
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
            usage_repository::raw_events(connection, &query, user_id, default_page_size)
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
            usage_repository::usage_summary(connection, &query, user_id, summary_limit)
        })
        .await
    }

    async fn session_timeline(
        &self,
        user_id: Uuid,
        session_pk: Uuid,
    ) -> Result<SessionTimelineResponse, AppError> {
        self.run_db(move |connection| {
            usage_repository::session_timeline(connection, user_id, session_pk)
        })
        .await
    }

    async fn image_attachment(
        &self,
        user_id: Uuid,
        attachment_id: Uuid,
    ) -> Result<StoredImageAttachment, AppError> {
        self.run_db(move |connection| {
            usage_repository::image_attachment(connection, user_id, attachment_id)
        })
        .await
    }

    async fn session_search(
        &self,
        user_id: Uuid,
        query: SessionSearchQuery,
    ) -> Result<SessionSearchResponse, AppError> {
        let search = self
            .search
            .clone()
            .ok_or_else(|| AppError::unavailable("session search is not configured".to_owned()))?;
        let execution = search.search(user_id, query).await?;
        let session_ids = execution.session_ids();
        let details = self
            .run_db(move |connection| {
                SearchOutboxRepository::session_details(connection, user_id, &session_ids)
            })
            .await?;
        Ok(execution.hydrate(details))
    }

    async fn shutdown(&self) {
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
    }
}
