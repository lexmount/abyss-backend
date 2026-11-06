//! Backend-independent search contracts and the literal text matching both
//! searches share.
//!
//! A search reads the pieces of each event's conversation entries, each
//! entry's `text` and its `name`, and matches the query as one literal
//! substring after per-character case folding. Storage backends find the
//! candidate texts and expand matching texts into events; folding, matching,
//! occurrence ranges, fragments, and result ordering live in [`matching`], so
//! every backend returns the same hits.

#[cfg_attr(
    all(feature = "postgres-es", not(test)),
    expect(
        dead_code,
        reason = "the PostgreSQL profile keeps its Elasticsearch search until its replacement is decided"
    )
)]
pub mod matching;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    error::AppError,
    usage::streams::{EntryLabel, EventLocation},
};

const DEFAULT_PAGE_SIZE: u32 = 20;
const MAX_PAGE_SIZE: u32 = 50;
const MAX_QUERY_CHARACTERS: usize = 256;
const MAX_FILTER_CHARACTERS: usize = 256;
const MAX_RESULT_WINDOW: u32 = 10_000;

#[derive(Debug, Deserialize)]
/// Query-string contract for session search.
pub struct SessionSearchQuery {
    /// Required query, matched as one literal substring.
    pub q: String,
    /// Inclusive lower observation-time bound.
    pub from: Option<DateTime<Utc>>,
    /// Exclusive upper observation-time bound.
    pub to: Option<DateTime<Utc>>,
    /// Optional Agent filter, matched exactly; repeat the key to match any of several.
    #[serde(default)]
    pub agent_name: Vec<String>,
    /// Optional LLM provider filter, matched exactly; repeat the key to match any of several.
    #[serde(default)]
    pub llm_provider: Vec<String>,
    /// Optional model filter; repeat the key to match any of several.
    #[serde(default)]
    pub llm_model: Vec<String>,
    /// Optional request or response filter.
    pub event_type: Option<String>,
    /// One-based result page.
    pub page: Option<u32>,
    /// Sessions returned per page.
    pub page_size: Option<u32>,
}

impl SessionSearchQuery {
    pub(crate) fn validate(self) -> Result<ValidatedSearchQuery, AppError> {
        let text = normalized_query(&self.q)?;
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
        // Both search implementations expose the same bounded result window.
        // Checked arithmetic makes oversized user input a validation error.
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
            agent_names: normalized_list(&self.agent_name, "agent_name")?,
            llm_providers: normalized_list(&self.llm_provider, "llm_provider")?,
            llm_models: normalized_list(&self.llm_model, "llm_model")?,
            event_type,
            page,
            page_size,
            offset,
        })
    }
}

/// Normalized query safe to translate into a backend-specific search request.
pub struct ValidatedSearchQuery {
    /// Trimmed query.
    pub text: String,
    /// Inclusive lower observation-time bound.
    pub from: Option<DateTime<Utc>>,
    /// Exclusive upper observation-time bound.
    pub to: Option<DateTime<Utc>>,
    /// Trimmed Agent names to match exactly, any of, or empty for every Agent.
    pub agent_names: Vec<String>,
    /// Trimmed providers to match any of, or empty for every provider.
    pub llm_providers: Vec<String>,
    /// Trimmed models to match any of, or empty for every model.
    pub llm_models: Vec<String>,
    /// Canonical request or response filter.
    pub event_type: Option<String>,
    /// One-based result page.
    pub page: u32,
    /// Sessions returned per page.
    pub page_size: u32,
    /// Zero-based search result offset.
    pub offset: u32,
}

#[derive(Serialize)]
/// Paginated session search response.
pub struct SessionSearchResponse {
    /// Normalized query.
    pub query: String,
    /// Sessions with at least one matching event.
    pub total_sessions: u64,
    /// One-based current page.
    pub page: u32,
    /// Requested sessions per page.
    pub page_size: u32,
    /// Whether the reported session count extends past this page.
    pub has_more: bool,
    /// Hydrated matching sessions.
    pub items: Vec<SessionSearchResult>,
}

#[derive(Serialize)]
/// One hydrated session with up to three of its matching events.
pub struct SessionSearchResult {
    /// Backend session primary key.
    pub session_pk: Uuid,
    /// Agent-native session identifier.
    pub session_id: String,
    /// The same earliest-request label returned by the session list and header.
    pub title: Option<EntryLabel>,
    /// Agent name as reported by the collector.
    pub agent_name: String,
    /// Most recently observed Agent version.
    pub agent_version: Option<String>,
    /// Authoritative device host name.
    pub host_name: String,
    /// Authoritative device platform.
    pub platform: String,
    /// Earliest event observation in the session.
    pub started_at: DateTime<Utc>,
    /// Latest event observation in the session.
    pub ended_at: Option<DateTime<Utc>>,
    /// Sorted providers represented by the returned matching events.
    pub providers: Vec<String>,
    /// Sorted models represented by the returned matching events.
    pub models: Vec<String>,
    /// Matching events in the session.
    pub match_count: u64,
    /// Matching events with at least one new hit.
    pub new_match_count: u64,
    /// Matching events with fragments: those with a new hit first, then
    /// those whose hits are all in the inherited context.
    pub matches: Vec<SessionSearchMatch>,
}

#[derive(Serialize)]
/// Search metadata and fragments for one matching usage event.
pub struct SessionSearchMatch {
    /// Backend event primary key.
    pub event_pk: Uuid,
    /// Current position in this session's stream view, when available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<EventLocation>,
    /// Request or response side.
    pub event_type: String,
    /// LLM provider as reported by the collector.
    pub llm_provider: String,
    /// Provider-specific model identifier.
    pub llm_model: String,
    /// Collector observation time.
    pub observed_at: DateTime<Utc>,
    /// Whether the event has a hit outside its inherited context.
    pub new: bool,
    /// Highlighted fragments of the event's matching pieces.
    pub fragments: Vec<SearchFragment>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
/// One search highlight fragment split into plain and matching text.
pub struct SearchFragment {
    /// Ordered text segments suitable for structured UI rendering.
    pub segments: Vec<SearchFragmentSegment>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
/// Plain or highlighted portion of a search fragment.
pub struct SearchFragmentSegment {
    /// Fragment text.
    pub text: String,
    /// Whether this segment is an occurrence of the query.
    pub highlighted: bool,
}

#[derive(Debug, Deserialize)]
/// Query-string contract for search within one session.
pub struct InSessionSearchQuery {
    /// Required query, matched as one literal substring.
    pub q: String,
}

impl InSessionSearchQuery {
    /// The trimmed query.
    pub(crate) fn validate(&self) -> Result<String, AppError> {
        normalized_query(&self.q)
    }
}

#[derive(Debug, Serialize)]
/// Every hit of the query in one session, with the occurrences in each
/// matching text.
pub struct InSessionSearchResponse {
    /// Normalized query.
    pub query: String,
    /// The texts the hits name, by id, with their occurrences.
    pub texts: Vec<MatchedText>,
    /// Hits in observation order, then by event, entry, and field, `name`
    /// before `text`.
    pub hits: Vec<InSessionHit>,
    /// Whether hits beyond the returned ones were left out.
    pub truncated: bool,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
/// One matching text and the occurrences of the query in it.
pub struct MatchedText {
    /// Text id the hits name.
    pub id: i64,
    /// Non-overlapping occurrences, left to right, as half-open ranges of the
    /// text's UTF-16 code units.
    pub ranges: Vec<[usize; 2]>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
/// One piece of one event whose text holds the query.
pub struct InSessionHit {
    /// Event the piece belongs to.
    pub event_pk: Uuid,
    /// Position of the entry in the event's `conversation.entries`.
    pub entry_index: u32,
    /// Which value of the entry the piece is.
    pub field: PieceField,
    /// Whether the hit is outside the event's inherited context.
    pub new: bool,
    /// Text of the piece, in [`InSessionSearchResponse::texts`].
    pub text_id: i64,
    /// Head of the segment holding the event's exchange, as the session's
    /// streams give it; null when the event is in no exchange.
    pub segment: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "lowercase")]
/// Which value of an entry a piece is. Within an entry, `name` orders
/// before `text`.
pub enum PieceField {
    /// The entry's `name`, the tool name of a tool call.
    Name,
    /// The entry's `text`.
    Text,
}

fn normalized_query(value: &str) -> Result<String, AppError> {
    normalized_required(value, "q", MAX_QUERY_CHARACTERS)
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

/// Validates and trims every value, then de-duplicates them.
fn normalized_list(values: &[String], field: &str) -> Result<Vec<String>, AppError> {
    let mut normalized = values
        .iter()
        .map(|value| normalized_required(value, field, MAX_FILTER_CHARACTERS))
        .collect::<Result<Vec<_>, _>>()?;
    normalized.sort_unstable();
    normalized.dedup();
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::{InSessionSearchQuery, SessionSearchQuery};

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
    fn matches_agent_names_exactly_as_reported() {
        let mut agents = query("hello");
        agents.agent_name = vec![
            " claude-desktop ".to_owned(),
            "claude-code".to_owned(),
            "claude-code".to_owned(),
        ];
        let validated = agents.validate().expect("agent filter is valid");
        assert_eq!(validated.agent_names, ["claude-code", "claude-desktop"]);
    }

    #[test]
    fn a_query_is_trimmed_and_bounded_by_characters() {
        let trimmed = query("  cargo test \n").validate().expect("query is valid");
        assert_eq!(
            trimmed.text, "cargo test",
            "inner spaces stay part of the query"
        );
        assert!(query(&"会".repeat(256)).validate().is_ok());
        assert!(query(&"会".repeat(257)).validate().is_err());

        let in_session = |q: &str| InSessionSearchQuery { q: q.to_owned() }.validate();
        assert_eq!(
            in_session(" STATUS.md ").expect("query is valid"),
            "STATUS.md"
        );
        assert!(in_session(" ").is_err());
        assert!(in_session(&"a".repeat(257)).is_err());
    }

    fn query(q: &str) -> SessionSearchQuery {
        SessionSearchQuery {
            q: q.to_owned(),
            from: None,
            to: None,
            agent_name: Vec::new(),
            llm_provider: Vec::new(),
            llm_model: Vec::new(),
            event_type: None,
            page: None,
            page_size: None,
        }
    }
}
