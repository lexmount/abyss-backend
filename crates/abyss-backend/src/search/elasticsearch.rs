//! Minimal Elasticsearch HTTP boundary for fixed-index search and bulk projection.
//!
//! This module owns every Elasticsearch-specific request and response shape so
//! the rest of the service depends on typed domain results. The index name and
//! strict mapping are fixed by the backend; operators configure only the base
//! endpoint, optional Basic Authentication, and request timeout.

use std::{collections::HashMap, time::Duration};

use chrono::{DateTime, Utc};
use reqwest::{Method, StatusCode, Url};
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{config::SearchConfig, error::AppError};

use super::{HIGHLIGHT_END, HIGHLIGHT_START, ValidatedSearchQuery, document::SearchDocument};

/// Fixed name of the derived usage-event index.
pub const SEARCH_INDEX: &str = "abyss_usage_events";
/// One idempotent operation submitted through Elasticsearch's Bulk API.
pub enum BulkOperation {
    /// Create or replace a document from current source-event state.
    Index(Box<SearchDocument>),
    /// Remove a document whose source event no longer exists.
    Delete(Uuid),
}

impl BulkOperation {
    const fn document_id(&self) -> Uuid {
        match self {
            Self::Index(document) => document.event_pk,
            Self::Delete(event_pk) => *event_pk,
        }
    }

    const fn accepts_not_found(&self) -> bool {
        matches!(self, Self::Delete(_))
    }
}

#[derive(Clone)]
/// Small HTTP client for the backend-owned Elasticsearch index.
pub struct ElasticsearchClient {
    client: reqwest::Client,
    endpoint: Url,
    username: Option<String>,
    password: Option<String>,
}

impl ElasticsearchClient {
    /// Builds a client after validating endpoint restrictions and credentials.
    pub fn new(config: &SearchConfig) -> Result<Self, AppError> {
        let endpoint = Url::parse(&config.endpoint).map_err(|error| {
            AppError::config(format!(
                "ABYSS_BACKEND_ELASTICSEARCH_URL must be a valid URL: {error}"
            ))
        })?;
        if !matches!(endpoint.scheme(), "http" | "https") {
            return Err(AppError::config(
                "ABYSS_BACKEND_ELASTICSEARCH_URL must use http or https".to_owned(),
            ));
        }
        if !endpoint.username().is_empty() || endpoint.password().is_some() {
            return Err(AppError::config(
                "configure Elasticsearch credentials with the dedicated username and password variables"
                    .to_owned(),
            ));
        }
        if endpoint.query().is_some() || endpoint.fragment().is_some() {
            return Err(AppError::config(
                "ABYSS_BACKEND_ELASTICSEARCH_URL must not contain a query or fragment".to_owned(),
            ));
        }
        // Ignore ambient HTTP proxy variables. A private Elasticsearch endpoint
        // and its credentials should not be routed through an unrelated proxy.
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.request_timeout_seconds))
            .no_proxy()
            .build()
            .map_err(|error| {
                AppError::config(format!("build Elasticsearch HTTP client: {error}"))
            })?;
        Ok(Self {
            client,
            endpoint,
            username: config.username.clone(),
            password: config.password.clone(),
        })
    }

    /// Creates the fixed search index when it does not yet exist.
    pub async fn ensure_index(&self) -> Result<(), SearchClientError> {
        let url = self.endpoint_url(SEARCH_INDEX);
        let response = self.request(Method::HEAD, url.clone()).send().await?;
        if response.status().is_success() {
            return Ok(());
        }
        if response.status() != StatusCode::NOT_FOUND {
            return Err(response_error("check Elasticsearch index", response).await);
        }

        let response = self
            .request(Method::PUT, url)
            .json(&index_definition())
            .send()
            .await?;
        if response.status().is_success() {
            return Ok(());
        }
        let status = response.status();
        let body = bounded_response_body(response).await;
        // Multiple backend replicas may observe the missing index together.
        if status == StatusCode::BAD_REQUEST && body.contains("resource_already_exists_exception") {
            return Ok(());
        }
        Err(SearchClientError::Response {
            operation: "create Elasticsearch index",
            status,
            body,
        })
    }

    /// Applies operations and returns one independent result per bulk item.
    pub async fn apply_bulk(
        &self,
        operations: &[BulkOperation],
    ) -> Result<Vec<Result<(), String>>, SearchClientError> {
        if operations.is_empty() {
            return Ok(Vec::new());
        }
        let mut body = String::new();
        for operation in operations {
            match operation {
                BulkOperation::Index(document) => {
                    append_json_line(
                        &mut body,
                        &json!({"index": {"_index": SEARCH_INDEX, "_id": document.event_pk}}),
                    )?;
                    append_json_line(&mut body, document)?;
                }
                BulkOperation::Delete(event_pk) => {
                    append_json_line(
                        &mut body,
                        &json!({"delete": {"_index": SEARCH_INDEX, "_id": event_pk}}),
                    )?;
                }
            }
        }

        let response = self
            .request(Method::POST, self.endpoint_url("_bulk"))
            .header("content-type", "application/x-ndjson")
            .body(body)
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(response_error("write Elasticsearch bulk request", response).await);
        }
        let response = response.json::<BulkResponse>().await?;
        // Positional correspondence is required to update the durable outbox.
        if response.items.len() != operations.len() {
            return Err(SearchClientError::Protocol(format!(
                "Elasticsearch bulk response returned {} items for {} operations",
                response.items.len(),
                operations.len()
            )));
        }

        Ok(response
            .items
            .into_iter()
            .zip(operations)
            .map(|(item, operation)| {
                item.result(operation.document_id(), operation.accepts_not_found())
            })
            .collect())
    }

    /// Executes an owner-scoped, session-collapsed full-text query.
    pub async fn search(
        &self,
        user_id: Uuid,
        query: &ValidatedSearchQuery,
    ) -> Result<SearchMatchPage, SearchClientError> {
        let response = self
            .request(
                Method::POST,
                self.endpoint_url(&format!("{SEARCH_INDEX}/_search")),
            )
            .json(&search_request(user_id, query))
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(response_error("search Elasticsearch index", response).await);
        }
        SearchMatchPage::try_from(response.json::<RawSearchResponse>().await?)
    }

    fn endpoint_url(&self, suffix: &str) -> Url {
        let mut url = self.endpoint.clone();
        let base_path = url.path().trim_end_matches('/');
        url.set_path(&format!("{base_path}/{}", suffix.trim_start_matches('/')));
        url
    }

    fn request(&self, method: Method, url: Url) -> reqwest::RequestBuilder {
        let request = self.client.request(method, url);
        match (&self.username, &self.password) {
            (Some(username), Some(password)) => request.basic_auth(username, Some(password)),
            _ => request,
        }
    }
}

/// Parsed Elasticsearch page before PostgreSQL hydration.
pub struct SearchMatchPage {
    /// Approximate number of distinct matching sessions.
    pub total_sessions: u64,
    /// Collapsed session hits in Elasticsearch relevance order.
    pub sessions: Vec<SessionMatches>,
}

/// Matching events collapsed under one session.
pub struct SessionMatches {
    /// Backend session primary key.
    pub session_pk: Uuid,
    /// Total events matching within the session.
    pub match_count: u64,
    /// Bounded strongest matching events.
    pub events: Vec<MatchedEvent>,
}

/// Parsed event metadata and raw highlight fragments from Elasticsearch.
pub struct MatchedEvent {
    /// Backend event primary key.
    pub event_pk: Uuid,
    /// Backend turn primary key.
    pub turn_pk: Uuid,
    /// Normalized turn number.
    pub turn_index: i32,
    /// Request or response side.
    pub event_type: String,
    /// Canonical LLM provider slug.
    pub llm_provider: String,
    /// Provider-specific model identifier.
    pub llm_model: String,
    /// Collector observation time.
    pub observed_at: DateTime<Utc>,
    /// Ordered raw fragments containing backend sentinel markers.
    pub fragments: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
/// Failures produced by the Elasticsearch protocol boundary.
pub enum SearchClientError {
    /// Transport, TLS, timeout, or response-decoding failure from reqwest.
    #[error("Elasticsearch request failed: {0}")]
    Request(#[from] reqwest::Error),
    /// Failure while constructing NDJSON or JSON request data.
    #[error("serialize Elasticsearch request: {0}")]
    Serialization(#[from] serde_json::Error),
    /// Non-success HTTP response with a bounded body for diagnostics.
    #[error("{operation} returned HTTP {status}: {body}")]
    Response {
        /// Backend operation that received the response.
        operation: &'static str,
        /// Elasticsearch HTTP status.
        status: StatusCode,
        /// Response body truncated to a safe diagnostic bound.
        body: String,
    },
    /// Structurally valid JSON that violates an expected ES response contract.
    #[error("invalid Elasticsearch response: {0}")]
    Protocol(String),
}

fn index_definition() -> Value {
    // Strict dynamic mapping rejects accidental expansion when a projection
    // field is added without an intentional mapping and review.
    json!({
        "mappings": {
            "dynamic": "strict",
            "properties": {
                "event_pk": {"type": "keyword"},
                "user_id": {"type": "keyword"},
                "session_pk": {"type": "keyword"},
                "session_id": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "turn_pk": {"type": "keyword"},
                "turn_index": {"type": "integer"},
                "agent_name": {"type": "keyword"},
                "llm_provider": {"type": "keyword"},
                "llm_model": {"type": "keyword"},
                "event_type": {"type": "keyword"},
                "observed_at": {"type": "date"},
                "content": {"type": "text"},
                "tool_names": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "tool_content": {"type": "text"},
                "commands": {"type": "text"},
                "file_paths": {"type": "text"}
            }
        }
    })
}

fn search_request(user_id: Uuid, query: &ValidatedSearchQuery) -> Value {
    // Authorization is encoded as a mandatory filter rather than a scoring
    // clause, guaranteeing that foreign documents cannot enter the hit set.
    let mut filters = vec![json!({"term": {"user_id": user_id}})];
    if query.from.is_some() || query.to.is_some() {
        let mut range = serde_json::Map::new();
        if let Some(from) = query.from {
            range.insert("gte".to_owned(), json!(from));
        }
        if let Some(to) = query.to {
            range.insert("lt".to_owned(), json!(to));
        }
        filters.push(json!({"range": {"observed_at": Value::Object(range)}}));
    }
    push_term_filter(&mut filters, "agent_name", query.agent_name.as_deref());
    push_term_filter(&mut filters, "llm_provider", query.llm_provider.as_deref());
    push_term_filter(&mut filters, "llm_model", query.llm_model.as_deref());
    push_term_filter(&mut filters, "event_type", query.event_type.as_deref());

    json!({
        "from": query.offset,
        "size": query.page_size,
        "track_total_hits": false,
        "_source": ["session_pk"],
        "query": {
            "bool": {
                "filter": filters,
                "must": [{
                    "multi_match": {
                        "query": query.text,
                        "fields": [
                            "content^4",
                            "session_id^3",
                            "commands^3",
                            "file_paths^3",
                            "tool_names^2",
                            "tool_content"
                        ],
                        "type": "best_fields",
                        "operator": "and"
                    }
                }]
            }
        },
        // Pagination applies to collapsed sessions, while inner_hits returns a
        // bounded sample of the strongest matching events for each session.
        "collapse": {
            "field": "session_pk",
            "inner_hits": {
                "name": "matches",
                "size": 3,
                "_source": [
                    "event_pk",
                    "turn_pk",
                    "turn_index",
                    "event_type",
                    "llm_provider",
                    "llm_model",
                    "observed_at"
                ],
                "highlight": {
                    "pre_tags": [HIGHLIGHT_START],
                    "post_tags": [HIGHLIGHT_END],
                    "fragment_size": 240,
                    "number_of_fragments": 2,
                    "fields": {
                        "content": {},
                        "commands": {},
                        "file_paths": {},
                        "tool_names": {},
                        "tool_content": {}
                    }
                }
            }
        },
        "aggs": {
            "session_count": {
                "cardinality": {
                    "field": "session_pk",
                    "precision_threshold": 40000
                }
            }
        }
    })
}

fn push_term_filter(filters: &mut Vec<Value>, field: &str, value: Option<&str>) {
    if let Some(value) = value {
        let mut term = serde_json::Map::new();
        term.insert(field.to_owned(), Value::String(value.to_owned()));
        filters.push(json!({"term": Value::Object(term)}));
    }
}

fn append_json_line<T: serde::Serialize>(
    body: &mut String,
    value: &T,
) -> Result<(), serde_json::Error> {
    body.push_str(&serde_json::to_string(value)?);
    body.push('\n');
    Ok(())
}

async fn response_error(operation: &'static str, response: reqwest::Response) -> SearchClientError {
    SearchClientError::Response {
        operation,
        status: response.status(),
        body: bounded_response_body(response).await,
    }
}

async fn bounded_response_body(response: reqwest::Response) -> String {
    // Elasticsearch errors can echo request data. Bound retained/logged text to
    // avoid turning a dependency failure into uncontrolled memory or log use.
    let body = response
        .text()
        .await
        .unwrap_or_else(|error| format!("unreadable response body: {error}"));
    body.chars().take(2_000).collect()
}

#[derive(Deserialize)]
struct BulkResponse {
    #[expect(
        dead_code,
        reason = "Per-item statuses are the authoritative bulk result."
    )]
    errors: bool,
    items: Vec<HashMap<String, BulkItem>>,
}

impl BulkResponseItem for HashMap<String, BulkItem> {
    fn result(self, document_id: Uuid, accepts_not_found: bool) -> Result<(), String> {
        let Some(item) = self.into_values().next() else {
            return Err(format!(
                "Elasticsearch omitted the bulk result for document {document_id}"
            ));
        };
        if (200..300).contains(&item.status) || (accepts_not_found && item.status == 404) {
            return Ok(());
        }
        Err(format!(
            "Elasticsearch bulk item for document {document_id} returned status {}: {}",
            item.status,
            item.error.unwrap_or(Value::Null)
        ))
    }
}

trait BulkResponseItem {
    fn result(self, document_id: Uuid, accepts_not_found: bool) -> Result<(), String>;
}

#[derive(Deserialize)]
struct BulkItem {
    status: u16,
    error: Option<Value>,
}

#[derive(Deserialize)]
struct RawSearchResponse {
    aggregations: Option<RawAggregations>,
    hits: RawOuterHits,
}

#[derive(Deserialize)]
struct RawAggregations {
    session_count: RawCardinality,
}

#[derive(Deserialize)]
struct RawCardinality {
    value: u64,
}

#[derive(Deserialize)]
struct RawOuterHits {
    hits: Vec<RawOuterHit>,
}

#[derive(Deserialize)]
struct RawOuterHit {
    #[serde(rename = "_source")]
    source: RawOuterSource,
    #[serde(default)]
    inner_hits: HashMap<String, RawInnerHits>,
}

#[derive(Deserialize)]
struct RawOuterSource {
    session_pk: Uuid,
}

#[derive(Deserialize)]
struct RawInnerHits {
    hits: RawMatchedHits,
}

#[derive(Deserialize)]
struct RawMatchedHits {
    total: RawTotalHits,
    hits: Vec<RawMatchedHit>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RawTotalHits {
    Number(u64),
    Object { value: u64 },
}

impl RawTotalHits {
    const fn value(&self) -> u64 {
        match self {
            Self::Number(value) | Self::Object { value } => *value,
        }
    }
}

#[derive(Deserialize)]
struct RawMatchedHit {
    #[serde(rename = "_source")]
    source: RawMatchedSource,
    #[serde(default)]
    highlight: HashMap<String, Vec<String>>,
}

#[derive(Deserialize)]
struct RawMatchedSource {
    event_pk: Uuid,
    turn_pk: Uuid,
    turn_index: i32,
    event_type: String,
    llm_provider: String,
    llm_model: String,
    observed_at: DateTime<Utc>,
}

impl TryFrom<RawSearchResponse> for SearchMatchPage {
    type Error = SearchClientError;

    fn try_from(response: RawSearchResponse) -> Result<Self, Self::Error> {
        let total_sessions = response
            .aggregations
            .map_or(0, |aggregations| aggregations.session_count.value);
        let sessions = response
            .hits
            .hits
            .into_iter()
            .map(|hit| {
                let matches = hit.inner_hits.get("matches").ok_or_else(|| {
                    SearchClientError::Protocol(
                        "collapsed search hit omitted matches inner_hits".to_owned(),
                    )
                })?;
                let match_count = matches.hits.total.value();
                let events = matches
                    .hits
                    .hits
                    .iter()
                    .map(|event| MatchedEvent {
                        event_pk: event.source.event_pk,
                        turn_pk: event.source.turn_pk,
                        turn_index: event.source.turn_index,
                        event_type: event.source.event_type.clone(),
                        llm_provider: event.source.llm_provider.clone(),
                        llm_model: event.source.llm_model.clone(),
                        observed_at: event.source.observed_at,
                        fragments: ordered_fragments(&event.highlight),
                    })
                    .collect();
                Ok(SessionMatches {
                    session_pk: hit.source.session_pk,
                    match_count,
                    events,
                })
            })
            .collect::<Result<Vec<_>, SearchClientError>>()?;
        Ok(Self {
            total_sessions,
            sessions,
        })
    }
}

fn ordered_fragments(highlights: &HashMap<String, Vec<String>>) -> Vec<String> {
    [
        "content",
        "commands",
        "file_paths",
        "tool_names",
        "tool_content",
    ]
    .into_iter()
    .flat_map(|field| highlights.get(field).into_iter().flatten().cloned())
    .take(2)
    .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::{TimeZone as _, Utc};
    use serde_json::json;
    use uuid::Uuid;

    use super::{BulkItem, BulkResponseItem, RawSearchResponse, SearchMatchPage, search_request};
    use crate::search::ValidatedSearchQuery;

    #[test]
    fn search_request_always_scopes_by_authenticated_user() {
        let user_id = Uuid::now_v7();
        let query = ValidatedSearchQuery {
            text: "timeout".to_owned(),
            from: Some(Utc.with_ymd_and_hms(2026, 8, 1, 0, 0, 0).unwrap()),
            to: None,
            agent_name: Some("claude-code".to_owned()),
            llm_provider: None,
            llm_model: None,
            event_type: None,
            page: 1,
            page_size: 20,
            offset: 0,
        };

        let request = search_request(user_id, &query);

        let filters = request["query"]["bool"]["filter"]
            .as_array()
            .expect("filters should be an array");
        assert!(filters.contains(&json!({"term": {"user_id": user_id}})));
        assert!(filters.contains(&json!({"term": {"agent_name": "claude-code"}})));
        assert_eq!(request["collapse"]["field"], "session_pk");
    }

    #[test]
    fn bulk_not_found_is_success_only_for_deletes() {
        let document_id = Uuid::now_v7();
        let index_result = HashMap::from([(
            "index".to_owned(),
            BulkItem {
                status: 404_u16,
                error: None,
            },
        )]);
        let delete_result = HashMap::from([(
            "delete".to_owned(),
            BulkItem {
                status: 404_u16,
                error: None,
            },
        )]);

        assert!(index_result.result(document_id, false).is_err());
        assert!(delete_result.result(document_id, true).is_ok());
    }

    #[test]
    fn parses_collapsed_hits_and_highlights() {
        let session_pk = Uuid::now_v7();
        let event_pk = Uuid::now_v7();
        let turn_pk = Uuid::now_v7();
        let response = serde_json::from_value::<RawSearchResponse>(json!({
            "aggregations": {"session_count": {"value": 1_u64}},
            "hits": {"hits": [{
                "_source": {"session_pk": session_pk},
                "inner_hits": {"matches": {"hits": {
                    "total": {"value": 2_u64, "relation": "eq"},
                    "hits": [{
                        "_source": {
                            "event_pk": event_pk,
                            "turn_pk": turn_pk,
                            "turn_index": 4_i32,
                            "event_type": "response",
                            "llm_provider": "openai",
                            "llm_model": "gpt-5",
                            "observed_at": "2026-08-05T10:00:00Z"
                        },
                        "highlight": {"content": ["a highlighted fragment"]}
                    }]
                }}}
            }]}
        }))
        .expect("fixture should decode");

        let parsed = SearchMatchPage::try_from(response).expect("response should parse");

        assert_eq!(parsed.total_sessions, 1);
        assert_eq!(parsed.sessions[0].session_pk, session_pk);
        assert_eq!(parsed.sessions[0].match_count, 2);
        assert_eq!(parsed.sessions[0].events[0].event_pk, event_pk);
        assert_eq!(
            parsed.sessions[0].events[0].fragments,
            ["a highlighted fragment"]
        );
    }
}
