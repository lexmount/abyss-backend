//! Traditional full-text session search backed by a derived Elasticsearch index.

mod document;
mod elasticsearch;
pub mod outbox;
pub mod worker;

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{config::SearchConfig, error::AppError};

use self::{
    elasticsearch::{ElasticsearchClient, HIGHLIGHT_END, HIGHLIGHT_START, SearchMatchPage},
    outbox::SearchSessionDetails,
};

const DEFAULT_PAGE_SIZE: u32 = 20;
const MAX_PAGE_SIZE: u32 = 50;
const MAX_QUERY_CHARACTERS: usize = 256;
const MAX_FILTER_CHARACTERS: usize = 256;
const MAX_RESULT_WINDOW: u32 = 10_000;

#[derive(Clone)]
pub struct SearchService {
    client: ElasticsearchClient,
}

impl SearchService {
    pub fn new(config: &SearchConfig) -> Result<Self, AppError> {
        Ok(Self {
            client: ElasticsearchClient::new(config)?,
        })
    }

    #[must_use]
    pub fn client(&self) -> ElasticsearchClient {
        self.client.clone()
    }

    pub async fn search(
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

#[derive(Debug, Deserialize)]
pub struct SessionSearchQuery {
    pub q: String,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    pub agent_name: Option<String>,
    pub llm_provider: Option<String>,
    pub llm_model: Option<String>,
    pub event_type: Option<String>,
    pub page: Option<u32>,
    pub page_size: Option<u32>,
}

impl SessionSearchQuery {
    fn validate(self) -> Result<ValidatedSearchQuery, AppError> {
        let text = normalized_required(&self.q, "q", MAX_QUERY_CHARACTERS)?;
        if let (Some(from), Some(to)) = (self.from, self.to)
            && from >= to
        {
            return Err(AppError::validation(
                "from must be earlier than to".to_owned(),
            ));
        }
        let page = self.page.unwrap_or(1);
        if page == 0 {
            return Err(AppError::validation("page must be at least 1".to_owned()));
        }
        let page_size = self.page_size.unwrap_or(DEFAULT_PAGE_SIZE);
        if page_size == 0 || page_size > MAX_PAGE_SIZE {
            return Err(AppError::validation(format!(
                "page_size must be between 1 and {MAX_PAGE_SIZE}"
            )));
        }
        let offset = page
            .saturating_sub(1)
            .checked_mul(page_size)
            .ok_or_else(|| AppError::validation("search page is too large".to_owned()))?;
        let window_end = offset
            .checked_add(page_size)
            .ok_or_else(|| AppError::validation("search page is too large".to_owned()))?;
        if window_end > MAX_RESULT_WINDOW {
            return Err(AppError::validation(format!(
                "search results are limited to the first {MAX_RESULT_WINDOW} matches"
            )));
        }
        let event_type = normalized_optional(self.event_type.as_deref(), "event_type")?;
        if event_type
            .as_deref()
            .is_some_and(|value| !matches!(value, "request" | "response"))
        {
            return Err(AppError::validation(
                "event_type must be request or response".to_owned(),
            ));
        }
        Ok(ValidatedSearchQuery {
            text,
            from: self.from,
            to: self.to,
            agent_name: normalized_optional(self.agent_name.as_deref(), "agent_name")?
                .map(|value| canonical_agent_name(&value)),
            llm_provider: normalized_optional(self.llm_provider.as_deref(), "llm_provider")?
                .map(|value| normalize_slug(&value)),
            llm_model: normalized_optional(self.llm_model.as_deref(), "llm_model")?,
            event_type,
            page,
            page_size,
            offset,
        })
    }
}

pub struct ValidatedSearchQuery {
    pub text: String,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    pub agent_name: Option<String>,
    pub llm_provider: Option<String>,
    pub llm_model: Option<String>,
    pub event_type: Option<String>,
    pub page: u32,
    pub page_size: u32,
    pub offset: u32,
}

pub struct SearchExecution {
    query: ValidatedSearchQuery,
    page: SearchMatchPage,
}

impl SearchExecution {
    #[must_use]
    pub fn session_ids(&self) -> Vec<Uuid> {
        self.page
            .sessions
            .iter()
            .map(|session| session.session_pk)
            .collect()
    }

    #[must_use]
    pub fn hydrate(
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

#[derive(Serialize)]
pub struct SessionSearchResponse {
    pub query: String,
    pub total_sessions: u64,
    pub page: u32,
    pub page_size: u32,
    pub has_more: bool,
    pub items: Vec<SessionSearchResult>,
}

#[derive(Serialize)]
pub struct SessionSearchResult {
    pub session_pk: Uuid,
    pub session_id: String,
    pub agent_name: String,
    pub agent_version: Option<String>,
    pub host_name: String,
    pub platform: String,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub providers: Vec<String>,
    pub models: Vec<String>,
    pub match_count: u64,
    pub matches: Vec<SessionSearchMatch>,
}

#[derive(Serialize)]
pub struct SessionSearchMatch {
    pub event_pk: Uuid,
    pub turn_pk: Uuid,
    pub turn_index: i32,
    pub event_type: String,
    pub llm_provider: String,
    pub llm_model: String,
    pub observed_at: DateTime<Utc>,
    pub fragments: Vec<SearchFragment>,
}

#[derive(Serialize)]
pub struct SearchFragment {
    pub segments: Vec<SearchFragmentSegment>,
}

impl SearchFragment {
    fn parse(fragment: &str) -> Self {
        let mut segments = Vec::new();
        let mut remainder = fragment;
        while let Some(start) = remainder.find(HIGHLIGHT_START) {
            let (plain, tagged) = remainder.split_at(start);
            push_fragment_segment(&mut segments, plain, false);
            let highlighted = tagged
                .strip_prefix(HIGHLIGHT_START)
                .expect("the marker position came from find");
            let Some(end) = highlighted.find(HIGHLIGHT_END) else {
                push_fragment_segment(&mut segments, tagged, false);
                remainder = "";
                break;
            };
            let (matched, after_match) = highlighted.split_at(end);
            push_fragment_segment(&mut segments, matched, true);
            remainder = after_match
                .strip_prefix(HIGHLIGHT_END)
                .expect("the marker position came from find");
        }
        push_fragment_segment(&mut segments, remainder, false);
        Self { segments }
    }
}

#[derive(Serialize)]
pub struct SearchFragmentSegment {
    pub text: String,
    pub highlighted: bool,
}

fn push_fragment_segment(segments: &mut Vec<SearchFragmentSegment>, text: &str, highlighted: bool) {
    if text.is_empty() {
        return;
    }
    segments.push(SearchFragmentSegment {
        text: text.to_owned(),
        highlighted,
    });
}

fn normalized_required(value: &str, field: &str, maximum: usize) -> Result<String, AppError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(AppError::validation(format!("{field} must not be empty")));
    }
    if value.chars().count() > maximum {
        return Err(AppError::validation(format!(
            "{field} must not exceed {maximum} characters"
        )));
    }
    Ok(value.to_owned())
}

fn normalized_optional(value: Option<&str>, field: &str) -> Result<Option<String>, AppError> {
    value
        .map(|value| normalized_required(value, field, MAX_FILTER_CHARACTERS))
        .transpose()
}

fn normalize_slug(value: &str) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .chars()
        .map(|character| match character {
            '_' | ' ' => '-',
            other => other,
        })
        .collect()
}

fn canonical_agent_name(value: &str) -> String {
    let normalized = normalize_slug(value);
    match normalized.as_str() {
        "claude" | "claude-desktop" => "claude-code".to_owned(),
        _ => normalized,
    }
}

#[cfg(test)]
mod tests {
    use super::{HIGHLIGHT_END, HIGHLIGHT_START, SearchFragment, SessionSearchQuery};

    #[test]
    fn rejects_empty_and_invalid_pagination_queries() {
        let empty = query("  ").validate().err().expect("empty query must fail");
        assert!(empty.to_string().contains("q must not be empty"));

        let mut invalid_page = query("hello");
        invalid_page.page = Some(0);
        assert!(invalid_page.validate().is_err());

        let mut invalid_size = query("hello");
        invalid_size.page_size = Some(51);
        assert!(invalid_size.validate().is_err());

        let mut last_valid_page = query("hello");
        last_valid_page.page = Some(200);
        last_valid_page.page_size = Some(50);
        assert!(last_valid_page.validate().is_ok());

        let mut outside_result_window = query("hello");
        outside_result_window.page = Some(201);
        outside_result_window.page_size = Some(50);
        let error = outside_result_window
            .validate()
            .err()
            .expect("deep pagination must fail");
        assert!(error.to_string().contains("first 10000 matches"));
    }

    #[test]
    fn parses_highlights_without_rendering_html() {
        let fragment = SearchFragment::parse(&format!(
            "before {HIGHLIGHT_START}timeout{HIGHLIGHT_END} after"
        ));

        assert_eq!(fragment.segments.len(), 3);
        assert_eq!(fragment.segments[1].text, "timeout");
        assert!(fragment.segments[1].highlighted);
    }

    fn query(q: &str) -> SessionSearchQuery {
        SessionSearchQuery {
            q: q.to_owned(),
            from: None,
            to: None,
            agent_name: None,
            llm_provider: None,
            llm_model: None,
            event_type: None,
            page: None,
            page_size: None,
        }
    }
}
