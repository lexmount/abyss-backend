//! PostgreSQL-owned Elasticsearch projection and session-search implementation.
//!
//! Search results are grouped by session in Elasticsearch, then hydrated with
//! authoritative PostgreSQL session and device rows. Elasticsearch never acts
//! as an authorization source: every query includes the authenticated owner and
//! stale hits without matching PostgreSQL details are omitted.

mod document;
mod elasticsearch;
mod outbox;
mod worker;

use std::collections::{HashMap, HashSet};

use uuid::Uuid;

use crate::{
    config::SearchConfig,
    error::AppError,
    search::{
        SearchFragment, SessionSearchMatch, SessionSearchQuery, SessionSearchResponse,
        SessionSearchResult, ValidatedSearchQuery,
    },
};

use self::{
    elasticsearch::{ElasticsearchClient, SearchMatchPage},
    outbox::SearchSessionDetails,
};

pub(super) use self::{outbox::SearchOutboxRepository, worker::SearchIndexer};

#[derive(Clone)]
/// Validating facade over the Elasticsearch HTTP client.
pub(super) struct SearchService {
    client: ElasticsearchClient,
}

impl SearchService {
    /// Creates a search service from startup-validated settings.
    pub(super) fn new(config: &SearchConfig) -> Result<Self, AppError> {
        Ok(Self {
            client: ElasticsearchClient::new(config)?,
        })
    }

    /// Returns a cloned client for the background indexer.
    #[must_use]
    pub(super) fn client(&self) -> ElasticsearchClient {
        self.client.clone()
    }

    /// Validates and executes one owner-scoped session search.
    pub(super) async fn search(
        &self,
        user_id: Uuid,
        query: SessionSearchQuery,
    ) -> Result<SearchExecution, AppError> {
        let query = query.validate()?;
        let page = self.client.search(user_id, &query).await.map_err(|error| {
            tracing::warn!(%error, %user_id, "session search request failed");
            AppError::unavailable("session search is temporarily unavailable".to_owned())
        })?;
        Ok(SearchExecution { query, page })
    }
}

/// Elasticsearch matches paired with the validated query that produced them.
pub(super) struct SearchExecution {
    query: ValidatedSearchQuery,
    page: SearchMatchPage,
}

impl SearchExecution {
    /// Returns session keys that must be hydrated from PostgreSQL.
    #[must_use]
    pub(super) fn session_ids(&self) -> Vec<Uuid> {
        self.page
            .sessions
            .iter()
            .map(|session| session.session_pk)
            .collect()
    }

    /// Combines search matches with authoritative session/device details.
    ///
    /// Stale index entries whose session no longer exists are omitted instead
    /// of returning partially authorized or incomplete data.
    #[must_use]
    pub(super) fn hydrate(
        self,
        mut details: HashMap<Uuid, SearchSessionDetails>,
    ) -> SessionSearchResponse {
        let items = self
            .page
            .sessions
            .into_iter()
            .filter_map(|matches| {
                let details = details.remove(&matches.session_pk)?;
                let mut models = matches
                    .events
                    .iter()
                    .map(|event| event.llm_model.clone())
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>();
                models.sort();
                let mut providers = matches
                    .events
                    .iter()
                    .map(|event| event.llm_provider.clone())
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>();
                providers.sort();
                Some(SessionSearchResult {
                    session_pk: details.session.id,
                    session_id: details.session.session_id,
                    agent_name: details.session.agent_name,
                    agent_version: details.session.agent_version,
                    host_name: details.device.host_name,
                    platform: details.device.platform,
                    started_at: details.session.started_at,
                    ended_at: details.session.ended_at,
                    providers,
                    models,
                    match_count: matches.match_count,
                    matches: matches
                        .events
                        .into_iter()
                        .map(|event| SessionSearchMatch {
                            event_pk: event.event_pk,
                            turn_pk: event.turn_pk,
                            turn_index: event.turn_index,
                            event_type: event.event_type,
                            llm_provider: event.llm_provider,
                            llm_model: event.llm_model,
                            observed_at: event.observed_at,
                            fragments: event
                                .fragments
                                .into_iter()
                                .map(|fragment| SearchFragment::parse(&fragment))
                                .collect(),
                        })
                        .collect(),
                })
            })
            .collect::<Vec<_>>();
        let consumed = u64::from(self.query.page).saturating_mul(u64::from(self.query.page_size));
        SessionSearchResponse {
            query: self.query.text,
            total_sessions: self.page.total_sessions,
            page: self.query.page,
            page_size: self.query.page_size,
            has_more: consumed < self.page.total_sessions,
            items,
        }
    }
}
