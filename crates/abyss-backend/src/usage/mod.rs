//! API contracts and shared transformations for Agent usage collection.
//!
//! Ingest types preserve collector-provided observations, while response types
//! expose the normalized device/session/turn hierarchy stored by PostgreSQL.
//! Repository code owns persistence and validation that requires database state;
//! small deterministic metadata transformations remain in this module.

/// Image attachment contracts and validation.
pub mod attachments;
/// Opaque diagnostic-capture ingest contracts.
pub mod diagnostics;
pub mod event_order;
pub mod persistence;
/// PostgreSQL-backed ingest and query operations.
#[cfg(feature = "postgres-es")]
pub mod repository;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

const WORKING_DIRECTORY_METADATA_KEY: &str = "working_directory";

#[derive(Deserialize)]
/// Atomic batch accepted by the event-ingestion endpoint.
pub struct IngestEventsRequest {
    /// Usage observations to validate and persist.
    pub events: Vec<IngestUsageEvent>,
    /// Optional captures correlated with events from this same request.
    #[serde(default)]
    pub diagnostic_captures: Vec<diagnostics::IngestDiagnosticCapture>,
}

#[derive(Debug, Deserialize)]
/// One collector-observed LLM request or response.
pub struct IngestUsageEvent {
    /// Collector-generated global idempotency key.
    pub event_id: String,
    /// Time at which the collector observed the event.
    pub observed_at: DateTime<Utc>,
    /// Host context associated with the event.
    pub device: DevicePayload,
    /// Agent implementation that produced the event.
    pub agent: AgentPayload,
    /// Agent-native session identifier.
    pub session_id: String,
    /// Collector-side best-effort turn number. Ingest normalizes this when a
    /// stable logical-turn/provider identity shows that a restarted collector
    /// reused an earlier local sequence number.
    pub turn_index: i32,
    /// LLM provider and model labels.
    pub llm: LlmPayload,
    /// Whether the event represents the request or response side of a call.
    pub event_type: UsageEventType,
    /// Optional prompt or response text.
    pub text: Option<String>,
    /// Provider-reported token counters; missing counters default to zero.
    #[serde(default)]
    pub token_usage: TokenUsagePayload,
    /// Collector metadata retained with the raw event.
    #[serde(default = "empty_metadata")]
    pub metadata: Value,
    /// Optional image metadata and content ordered within the event.
    #[serde(default)]
    pub attachments: Vec<attachments::IngestImageAttachment>,
}

#[derive(Debug, Deserialize)]
/// Collector-supplied host identity used to group events by device context.
pub struct DevicePayload {
    /// User-visible host name; `hostname` remains an accepted JSON alias.
    #[serde(alias = "hostname")]
    pub host_name: String,
    /// Normalized operating-system or runtime platform label.
    pub platform: String,
    /// Optional operating-system version observed by the collector.
    pub os_version: Option<String>,
}

#[derive(Debug, Deserialize)]
/// Collector-supplied Agent identity.
pub struct AgentPayload {
    /// Agent name, normalized to a canonical slug during ingest.
    pub name: String,
    /// Optional Agent build or release version.
    pub version: Option<String>,
}

#[derive(Debug, Deserialize)]
/// Collector-supplied LLM target identity.
pub struct LlmPayload {
    /// Provider name, normalized to a lowercase slug during ingest.
    pub provider: String,
    /// Provider-specific model identifier.
    pub model: String,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
/// Side of the LLM exchange represented by an event.
pub enum UsageEventType {
    /// Prompt or tool request sent to the provider.
    Request,
    /// Provider response returned to the Agent.
    Response,
}

impl UsageEventType {
    /// Returns the canonical value stored in PostgreSQL and returned by APIs.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Request => "request",
            Self::Response => "response",
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[expect(
    clippy::struct_field_names,
    reason = "Public JSON token usage fields intentionally include the token suffix."
)]
pub struct TokenUsagePayload {
    /// Input tokens reported by the provider.
    pub input_tokens: Option<i64>,
    /// Output tokens reported by the provider.
    pub output_tokens: Option<i64>,
    /// Tokens served from a provider cache.
    pub cache_read_tokens: Option<i64>,
    /// Tokens written to a provider cache.
    pub cache_write_tokens: Option<i64>,
    /// Provider-reported reasoning tokens.
    pub reasoning_tokens: Option<i64>,
    /// Provider total, or a derived sum when omitted.
    pub total_tokens: Option<i64>,
}

#[derive(Debug, Serialize)]
/// Counts returned after an atomic ingest batch finishes.
pub struct IngestEventsResponse {
    /// Newly persisted usage events.
    pub accepted: usize,
    /// Events skipped because their `event_id` already exists.
    pub duplicates: usize,
    /// Per-event rejection count; currently always zero because batches are atomic.
    pub rejected: usize,
    /// Per-event errors; currently empty because validation fails the whole request.
    pub errors: Vec<String>,
    /// Newly persisted diagnostic captures.
    pub accepted_diagnostic_captures: usize,
    /// Captures skipped because their `(user_id, capture_id)` already exists.
    pub duplicate_diagnostic_captures: usize,
}

#[derive(Clone, Debug, Deserialize)]
/// Filters, dimensions, and bounds for token-usage aggregation.
pub struct SummaryQuery {
    /// Inclusive lower observation-time bound.
    pub from: Option<DateTime<Utc>>,
    /// Exclusive upper observation-time bound.
    pub to: Option<DateTime<Utc>>,
    /// Comma-separated grouping dimensions.
    pub group_by: Option<String>,
    /// Selects the full or token-only response shape.
    pub fields: Option<SummaryFields>,
    /// User UUID, name, or email filter retained for the summary contract.
    pub user_filter: Option<String>,
    /// Agent-name filter.
    pub agent_name: Option<String>,
    /// Agent-native session identifier filter.
    pub session_id: Option<String>,
    /// LLM provider filter.
    pub llm_provider: Option<String>,
    /// LLM model filter.
    pub llm_model: Option<String>,
    /// Event-side filter.
    pub event_type: Option<String>,
    /// Requested aggregate row limit.
    pub limit: Option<i64>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
/// Response projection requested from the summary endpoint.
pub enum SummaryFields {
    /// Return dimensions, event counts, and every token counter.
    Full,
    /// Return only day, input-token, and total-token fields.
    TokenUsage,
}

#[derive(Debug, Serialize)]
/// Full summary response grouped by the requested dimensions.
pub struct SummaryResponse {
    /// Inclusive lower bound echoed from the request.
    pub from: Option<DateTime<Utc>>,
    /// Exclusive upper bound echoed from the request.
    pub to: Option<DateTime<Utc>>,
    /// Normalized grouping dimensions actually applied.
    pub group_by: Vec<String>,
    /// Aggregate rows ordered by token usage and stable tie-breakers.
    pub rows: Vec<SummaryRow>,
    /// Reserved cursor field; summary queries are currently single-page.
    pub next_page_token: Option<String>,
}

#[derive(Debug, Serialize)]
/// One aggregate bucket from the full usage summary.
pub struct SummaryRow {
    /// UTC calendar day when grouped by day.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub day: Option<String>,
    /// Owner UUID when grouped by user.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<Uuid>,
    /// Owner display name when grouped by user.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_name: Option<String>,
    /// Owner email when grouped by user.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_email: Option<String>,
    /// Device host name when grouped by device.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_name: Option<String>,
    /// Device platform when grouped by device.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub platform: Option<String>,
    /// Device OS version when grouped by device.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub os_version: Option<String>,
    /// Canonical Agent name when grouped by Agent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_name: Option<String>,
    /// LLM provider when grouped by provider.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub llm_provider: Option<String>,
    /// LLM model when grouped by model.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub llm_model: Option<String>,
    /// Request or response value when grouped by event type.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_type: Option<String>,
    /// Distinct sessions in this bucket.
    pub sessions: usize,
    /// Distinct turns in this bucket.
    pub turns: usize,
    /// Request events in this bucket.
    pub requests: i64,
    /// Response events in this bucket.
    pub responses: i64,
    /// Summed input tokens.
    pub input_tokens: i64,
    /// Summed output tokens.
    pub output_tokens: i64,
    /// Summed cache-read tokens.
    pub cache_read_tokens: i64,
    /// Summed cache-write tokens.
    pub cache_write_tokens: i64,
    /// Summed reasoning tokens.
    pub reasoning_tokens: i64,
    /// Summed provider or derived total tokens.
    pub total_tokens: i64,
}

#[derive(Debug, Serialize)]
/// Reduced summary response used by token-usage-only consumers.
pub struct TokenUsageSummaryResponse {
    /// Inclusive lower bound echoed from the request.
    pub from: Option<DateTime<Utc>>,
    /// Exclusive upper bound echoed from the request.
    pub to: Option<DateTime<Utc>>,
    /// Normalized grouping dimensions actually applied.
    pub group_by: Vec<String>,
    /// Token-only aggregate rows.
    pub rows: Vec<TokenUsageSummaryRow>,
    /// Reserved cursor field; summary queries are currently single-page.
    pub next_page_token: Option<String>,
}

impl TokenUsageSummaryResponse {
    /// Projects a full summary into its stable token-only representation.
    pub fn from_summary(summary: SummaryResponse) -> Self {
        Self {
            from: summary.from,
            to: summary.to,
            group_by: summary.group_by,
            rows: summary
                .rows
                .into_iter()
                .map(TokenUsageSummaryRow::from)
                .collect(),
            next_page_token: summary.next_page_token,
        }
    }
}

#[derive(Debug, Serialize)]
/// Minimal token counters for one summary bucket.
pub struct TokenUsageSummaryRow {
    /// UTC calendar day when day grouping was requested.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub day: Option<String>,
    /// Summed input tokens.
    pub input_tokens: i64,
    /// Summed provider or derived total tokens.
    pub total_tokens: i64,
}

impl From<SummaryRow> for TokenUsageSummaryRow {
    fn from(row: SummaryRow) -> Self {
        Self {
            day: row.day,
            input_tokens: row.input_tokens,
            total_tokens: row.total_tokens,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
/// Filters and offset pagination for the raw-event endpoint.
pub struct RawEventsQuery {
    /// Inclusive lower observation-time bound.
    pub from: Option<DateTime<Utc>>,
    /// Exclusive upper observation-time bound.
    pub to: Option<DateTime<Utc>>,
    /// Agent-name filter.
    pub agent_name: Option<String>,
    /// Agent-native session identifier filter.
    pub session_id: Option<String>,
    /// Backend session primary-key filter.
    pub session_pk: Option<Uuid>,
    /// Normalized turn-number filter.
    pub turn_index: Option<i32>,
    /// LLM provider filter.
    pub llm_provider: Option<String>,
    /// LLM model filter.
    pub llm_model: Option<String>,
    /// Request or response filter.
    pub event_type: Option<String>,
    /// Requested page size.
    pub limit: Option<i64>,
    /// Number of newest matching events to skip.
    pub offset: Option<i64>,
}

#[derive(Debug, Serialize)]
/// One page of raw usage events.
pub struct RawEventsResponse {
    /// Events ordered from newest to oldest observation time.
    pub events: Vec<UsageEventResponse>,
    /// Decimal offset for the next page, or `None` at the end.
    pub next_page_token: Option<String>,
}

#[derive(Debug, Serialize)]
/// API representation of one persisted usage event.
pub struct UsageEventResponse {
    /// Backend-generated event primary key.
    pub id: Uuid,
    /// Collector-generated idempotency key.
    pub event_id: String,
    /// Authenticated owner identifier.
    pub user_id: Uuid,
    /// Backend device-context primary key.
    pub device_context_id: Uuid,
    /// Device host name, when the referenced device still exists.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_name: Option<String>,
    /// Device platform, when the referenced device still exists.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub platform: Option<String>,
    /// Backend session primary key.
    pub session_pk: Uuid,
    /// Backend turn primary key.
    pub turn_pk: Uuid,
    /// Canonical Agent name.
    pub agent_name: String,
    /// Agent version observed on this event.
    pub agent_version: Option<String>,
    /// Agent-native session identifier.
    pub session_id: String,
    /// Backend-normalized turn number.
    pub turn_index: i32,
    /// Canonical LLM provider slug.
    pub llm_provider: String,
    /// Provider-specific model identifier.
    pub llm_model: String,
    /// Canonical request or response value.
    pub event_type: String,
    /// Optional prompt or response text.
    pub text: Option<String>,
    /// Lowercase SHA-256 of text when text is present.
    pub text_sha256: Option<String>,
    /// Normalized input-token count.
    pub input_tokens: i64,
    /// Normalized output-token count.
    pub output_tokens: i64,
    /// Normalized cache-read-token count.
    pub cache_read_tokens: i64,
    /// Normalized cache-write-token count.
    pub cache_write_tokens: i64,
    /// Normalized reasoning-token count.
    pub reasoning_tokens: i64,
    /// Provider-reported or derived total-token count.
    pub total_tokens: i64,
    /// Collector observation timestamp.
    pub observed_at: DateTime<Utc>,
    /// Collector metadata retained without schema expansion.
    pub metadata: Value,
    /// Ordered attachment metadata associated with the event.
    pub attachments: Vec<attachments::UsageEventAttachmentResponse>,
}

#[derive(Debug, Serialize)]
/// Session metadata and its ordered turn timelines.
pub struct SessionTimelineResponse {
    /// Authoritative session information.
    pub session: SessionInfo,
    /// Turns ordered by normalized turn index.
    pub turns: Vec<TurnTimeline>,
}

#[derive(Debug, Serialize)]
/// API representation of an Agent session.
pub struct SessionInfo {
    /// Backend session primary key.
    pub session_pk: Uuid,
    /// Authenticated owner identifier.
    pub user_id: Uuid,
    /// Most recently associated device context.
    pub device_context_id: Uuid,
    /// Canonical Agent name.
    pub agent_name: String,
    /// Most recently known Agent version.
    pub agent_version: Option<String>,
    /// Agent-native session identifier.
    pub session_id: String,
    /// Earliest event observation time in the session.
    pub started_at: DateTime<Utc>,
    /// Latest event observation time in the session.
    pub ended_at: Option<DateTime<Utc>>,
    /// Stable, allowlisted session metadata.
    pub metadata: Value,
}

/// Extracts the endpoint working directory retained in session metadata.
#[must_use]
pub fn session_working_directory(metadata: &Value) -> Option<String> {
    metadata
        .get(WORKING_DIRECTORY_METADATA_KEY)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty() && value.len() <= 4_096)
        .map(str::to_owned)
}

/// Keeps only stable session-level metadata from an event.
#[must_use]
pub fn session_metadata_from_event(metadata: &Value) -> Value {
    session_working_directory(metadata).map_or_else(
        || json!({}),
        |working_directory| json!({WORKING_DIRECTORY_METADATA_KEY: working_directory}),
    )
}

#[derive(Debug, Serialize)]
/// One normalized turn and its deterministically ordered events.
pub struct TurnTimeline {
    /// Backend turn primary key.
    pub turn_pk: Uuid,
    /// Backend-normalized turn number.
    pub turn_index: i32,
    /// Earliest event observation time in the turn.
    pub started_at: DateTime<Utc>,
    /// Latest event observation time in the turn.
    pub ended_at: Option<DateTime<Utc>>,
    /// Request and response events in authoritative timeline order.
    pub events: Vec<UsageEventResponse>,
}

/// Returns the JSON object used when optional metadata is absent.
pub fn empty_metadata() -> Value {
    json!({})
}

#[cfg(test)]
mod tests {
    use super::{
        IngestEventsRequest, SummaryFields, SummaryQuery, SummaryResponse, SummaryRow,
        TokenUsageSummaryResponse, UsageEventType, session_metadata_from_event,
        session_working_directory,
    };
    use serde_json::json;

    #[test]
    fn ingest_event_accepts_device_context_without_client_user_id() {
        let request = serde_json::from_value::<IngestEventsRequest>(json!({
            "events": [
                {
                    "event_id": "evt-usage-001",
                    "observed_at": "2026-05-20T01:02:03Z",
                    "device": {
                        "host_name": "alice-mbp",
                        "platform": "macos",
                        "os_version": "15.5"
                    },
                    "agent": {
                        "name": "Codex",
                        "version": "1.2.3"
                    },
                    "session_id": "session-001",
                    "turn_index": 1_i32,
                    "llm": {
                        "provider": "OpenAI",
                        "model": "gpt-5.5"
                    },
                    "event_type": "request",
                    "text": "Explain this repository.",
                    "token_usage": {
                        "input_tokens": 10_i64,
                        "cache_read_tokens": 2_i64
                    },
                    "metadata": {
                        "source": "unit-test"
                    }
                }
            ]
        }))
        .expect("ingest payload should deserialize");

        let event = request
            .events
            .first()
            .expect("ingest request should contain one event");
        assert_eq!(
            event.device.host_name.as_str(),
            "alice-mbp",
            "device context should capture the host name without an external device id"
        );
        assert_eq!(
            event.event_type,
            UsageEventType::Request,
            "lowercase request event_type should deserialize"
        );
        assert_eq!(
            event.token_usage.input_tokens,
            Some(10_i64),
            "input token usage should be preserved"
        );
        assert_eq!(
            event.metadata,
            json!({ "source": "unit-test" }),
            "metadata should preserve arbitrary JSON object content"
        );
    }

    #[test]
    fn ingest_event_defaults_optional_token_usage_and_metadata() {
        let request = serde_json::from_value::<IngestEventsRequest>(json!({
            "events": [
                {
                    "event_id": "evt-usage-002",
                    "observed_at": "2026-05-20T01:03:03Z",
                    "device": {
                        "hostname": "alice-mbp",
                        "platform": "macos"
                    },
                    "agent": {
                        "name": "Claude Code"
                    },
                    "session_id": "session-002",
                    "turn_index": 2_i32,
                    "llm": {
                        "provider": "Anthropic",
                        "model": "claude-4"
                    },
                    "event_type": "response"
                }
            ]
        }))
        .expect("minimal ingest payload should deserialize");

        let event = request
            .events
            .first()
            .expect("ingest request should contain one event");
        assert_eq!(
            event.event_type,
            UsageEventType::Response,
            "lowercase response event_type should deserialize"
        );
        assert_eq!(
            event.token_usage.total_tokens, None,
            "omitted token_usage should default to empty token counters"
        );
        assert_eq!(
            event.metadata,
            json!({}),
            "omitted metadata should default to an empty object"
        );
    }

    #[test]
    fn session_metadata_keeps_only_a_bounded_working_directory() {
        let metadata = json!({
            "working_directory": "/Users/alice/repo",
            "request_hash": "sensitive-event-detail"
        });

        assert_eq!(
            session_working_directory(&metadata).as_deref(),
            Some("/Users/alice/repo")
        );
        assert_eq!(
            session_metadata_from_event(&metadata),
            json!({"working_directory": "/Users/alice/repo"})
        );
    }

    #[test]
    fn summary_query_accepts_token_usage_fields_mode() {
        let query = serde_json::from_value::<SummaryQuery>(json!({
            "fields": "token_usage"
        }))
        .expect("summary query should deserialize token usage field mode");

        assert_eq!(query.fields, Some(SummaryFields::TokenUsage));
    }

    #[test]
    fn token_usage_summary_response_omits_non_token_fields() {
        let response = TokenUsageSummaryResponse::from_summary(SummaryResponse {
            from: None,
            to: None,
            group_by: vec!["day".to_owned()],
            rows: vec![SummaryRow {
                day: Some("2026-07-01".to_owned()),
                user_id: None,
                user_name: Some("Alice".to_owned()),
                user_email: Some("alice@example.invalid".to_owned()),
                host_name: Some("alice-mbp".to_owned()),
                platform: Some("macos".to_owned()),
                os_version: Some("15.5".to_owned()),
                agent_name: Some("codex".to_owned()),
                llm_provider: Some("openai".to_owned()),
                llm_model: Some("gpt-5".to_owned()),
                event_type: Some("request".to_owned()),
                sessions: 9_usize,
                turns: 8_usize,
                requests: 7_i64,
                responses: 6_i64,
                input_tokens: 123_i64,
                output_tokens: 456_i64,
                cache_read_tokens: 11_i64,
                cache_write_tokens: 12_i64,
                reasoning_tokens: 13_i64,
                total_tokens: 615_i64,
            }],
            next_page_token: None,
        });

        let value = serde_json::to_value(response).expect("response should serialize");

        assert_eq!(
            value,
            json!({
                "from": null,
                "to": null,
                "group_by": ["day"],
                "rows": [
                    {
                        "day": "2026-07-01",
                        "input_tokens": 123_i64,
                        "total_tokens": 615_i64
                    }
                ],
                "next_page_token": null
            }),
            "token usage summary should only expose chart token fields"
        );
    }
}
