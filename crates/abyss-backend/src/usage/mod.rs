//! API contracts and shared transformations for Agent usage collection.
//!
//! Ingest types preserve collector-provided observations, while response types
//! expose the normalized device/session/turn hierarchy returned by every storage
//! backend. Deterministic validation and metadata transformations live here;
//! backend-specific persistence stays behind the storage compatibility layer.

/// Image attachment contracts and validation.
pub mod attachments;
/// Comparison chart contracts.
pub mod comparison;
mod conversation;
/// Opaque diagnostic-capture ingest contracts.
pub mod diagnostics;
pub mod event_order;
pub mod persistence;
/// Owner-scoped session index contracts.
pub mod session_list;
pub mod session_title;
pub mod trajectory;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

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
    /// The user turn this request works for, unique within its session;
    /// absent when the collector cannot identify a particular turn.
    pub turn_id: Option<String>,
    /// Whether the request comes from a thread the Agent opened only to name
    /// another session.
    #[serde(default)]
    pub naming_thread: bool,
    /// The thread that sent this exchange's request, as the Agent declares
    /// it; requests with the same value continue one conversation.
    pub stream_id: Option<String>,
    /// Provider identity of the response this exchange produced; both events
    /// of the exchange carry it. Used to group a fallback turn when `turn_id`
    /// is absent; one user turn can contain several such model calls.
    pub response_id: Option<String>,
    /// The response this exchange's request continues after; the request then
    /// carries only the messages added since.
    pub previous_response_id: Option<String>,
    /// LLM provider and model labels.
    pub llm: LlmPayload,
    /// Whether the event represents the request or response side of a call.
    pub event_type: UsageEventType,
    /// Optional prompt or response text.
    pub text: Option<String>,
    /// Token counters of the `AgentEvent`; absent counters are stored as zero.
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
    /// User-visible host name, stored exactly as received.
    pub host_name: String,
    /// Operating-system or runtime platform label, stored exactly as received.
    pub platform: String,
    /// Optional operating-system version observed by the collector.
    pub os_version: Option<String>,
}

#[derive(Debug, Deserialize)]
/// Collector-supplied Agent identity.
pub struct AgentPayload {
    /// Agent name identified by the collector, stored exactly as received.
    pub name: String,
    /// Optional Agent build or release version.
    pub version: Option<String>,
}

#[derive(Debug, Deserialize)]
/// Collector-supplied LLM target identity.
pub struct LlmPayload {
    /// Provider name, stored exactly as received.
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
    /// Returns the canonical value stored by backends and returned by APIs.
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
/// Token counters of the `AgentEvent`, stored as sent.
///
/// Input, cache read, cache write, and output counters are disjoint;
/// reasoning is a breakdown of output.
pub struct TokenUsagePayload {
    /// Input tokens neither read from nor written into a provider cache.
    pub input_tokens: Option<i64>,
    /// Output tokens, including reasoning.
    pub output_tokens: Option<i64>,
    /// Input tokens served from a provider cache.
    pub cache_read_tokens: Option<i64>,
    /// Input tokens written to a provider cache.
    pub cache_write_tokens: Option<i64>,
    /// Part of output tokens spent on reasoning.
    pub reasoning_tokens: Option<i64>,
    /// Sum of the four disjoint counters.
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
    /// Agent-name filter; repeat the key to match any of several Agents.
    #[serde(default)]
    pub agent_name: Vec<String>,
    /// Agent-native session identifier filter; repeat the key to match any of several.
    #[serde(default)]
    pub session_id: Vec<String>,
    /// LLM provider filter; repeat the key to match any of several providers.
    #[serde(default)]
    pub llm_provider: Vec<String>,
    /// LLM model filter; repeat the key to match any of several models.
    #[serde(default)]
    pub llm_model: Vec<String>,
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
    /// Return only the day and disjoint token counters used by charts.
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
    /// Agent name as reported by the collector, when grouped by Agent.
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
    /// Summed input tokens that were neither read from nor written to a cache.
    pub input_tokens: i64,
    /// Summed output tokens, including reasoning.
    pub output_tokens: i64,
    /// Summed input tokens read from a provider cache.
    pub cache_read_tokens: i64,
    /// Summed input tokens written into a provider cache.
    pub cache_write_tokens: i64,
    /// Summed total of the four disjoint counters.
    pub total_tokens: i64,
}

impl From<SummaryRow> for TokenUsageSummaryRow {
    fn from(row: SummaryRow) -> Self {
        Self {
            day: row.day,
            input_tokens: row.input_tokens,
            output_tokens: row.output_tokens,
            cache_read_tokens: row.cache_read_tokens,
            cache_write_tokens: row.cache_write_tokens,
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
    /// Agent-name filter; repeat the key to match any of several Agents.
    #[serde(default)]
    pub agent_name: Vec<String>,
    /// Agent-native session identifier filter; repeat the key to match any of several.
    #[serde(default)]
    pub session_id: Vec<String>,
    /// Backend session primary-key filter.
    pub session_pk: Option<Uuid>,
    /// Normalized turn-number filter.
    pub turn_index: Option<i32>,
    /// LLM provider filter; repeat the key to match any of several providers.
    #[serde(default)]
    pub llm_provider: Vec<String>,
    /// LLM model filter; repeat the key to match any of several models.
    #[serde(default)]
    pub llm_model: Vec<String>,
    /// Request or response filter.
    pub event_type: Option<String>,
    /// Requested page size.
    pub limit: Option<i64>,
    /// Number of newest matching events to skip.
    pub offset: Option<i64>,
    /// Selects complete events or the content-free list projection.
    pub fields: Option<RawEventFields>,
}

impl RawEventsQuery {
    /// Whether events carry their text, metadata, and attachments.
    pub fn includes_content(&self) -> bool {
        self.fields.unwrap_or_default() == RawEventFields::Full
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
/// Response projection requested from the raw-event endpoint.
pub enum RawEventFields {
    /// Return every event field, including text, metadata, and attachments.
    #[default]
    Full,
    /// Return what an event list shows. Text and metadata hold whole request
    /// bodies, so storage does not read them and attachments are not loaded.
    List,
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
/// One page of usage events without their content, for event lists.
pub struct RawEventListResponse {
    /// Events ordered from newest to oldest observation time.
    pub events: Vec<UsageEventListItem>,
    /// Decimal offset for the next page, or `None` at the end.
    pub next_page_token: Option<String>,
}

impl RawEventListResponse {
    /// Drops the content fields from a page loaded without them.
    pub fn from_events(response: RawEventsResponse) -> Self {
        Self {
            events: response
                .events
                .into_iter()
                .map(UsageEventListItem::from)
                .collect(),
            next_page_token: response.next_page_token,
        }
    }
}

#[derive(Debug, Serialize)]
/// One usage event as an event list shows it: identity, model, Agent, device,
/// and token counts, without text, metadata, or attachments.
pub struct UsageEventListItem {
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
    /// Agent name as reported by the collector.
    pub agent_name: String,
    /// Agent version observed on this event.
    pub agent_version: Option<String>,
    /// Agent-native session identifier.
    pub session_id: String,
    /// Backend-normalized turn number.
    pub turn_index: i32,
    /// LLM provider as reported by the collector.
    pub llm_provider: String,
    /// Provider-specific model identifier.
    pub llm_model: String,
    /// Canonical request or response value.
    pub event_type: String,
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
}

impl From<UsageEventResponse> for UsageEventListItem {
    fn from(event: UsageEventResponse) -> Self {
        Self {
            id: event.id,
            event_id: event.event_id,
            user_id: event.user_id,
            device_context_id: event.device_context_id,
            host_name: event.host_name,
            platform: event.platform,
            session_pk: event.session_pk,
            turn_pk: event.turn_pk,
            agent_name: event.agent_name,
            agent_version: event.agent_version,
            session_id: event.session_id,
            turn_index: event.turn_index,
            llm_provider: event.llm_provider,
            llm_model: event.llm_model,
            event_type: event.event_type,
            input_tokens: event.input_tokens,
            output_tokens: event.output_tokens,
            cache_read_tokens: event.cache_read_tokens,
            cache_write_tokens: event.cache_write_tokens,
            reasoning_tokens: event.reasoning_tokens,
            total_tokens: event.total_tokens,
            observed_at: event.observed_at,
        }
    }
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
    /// Agent name as reported by the collector.
    pub agent_name: String,
    /// Agent version observed on this event.
    pub agent_version: Option<String>,
    /// Agent-native session identifier.
    pub session_id: String,
    /// Backend-normalized turn number.
    pub turn_index: i32,
    /// LLM provider as reported by the collector.
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
    /// Agent-declared thread of the exchange.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_id: Option<String>,
    /// Provider identity of the response the exchange produced.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_id: Option<String>,
    /// Response whose conversation the exchange's request continues.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_response_id: Option<String>,
    /// Collector metadata retained without schema expansion.
    pub metadata: Value,
    /// Ordered attachment metadata associated with the event.
    pub attachments: Vec<attachments::UsageEventAttachmentResponse>,
}

#[derive(Debug, Serialize)]
/// Session metadata and its ordered turn timelines.
///
/// `E` is the per-event representation: complete events for the full
/// timeline, or [`UsageEventOutline`] for the content-free outline.
pub struct SessionTimelineResponse<E = UsageEventResponse> {
    /// Authoritative session information.
    pub session: SessionInfo,
    /// Turns ordered by normalized turn index.
    pub turns: Vec<TurnTimeline<E>>,
}

/// Session timeline whose events omit text and metadata bodies.
pub type SessionOutlineResponse = SessionTimelineResponse<UsageEventOutline>;

/// Maximum number of characters returned in an outline event preview.
pub const OUTLINE_PREVIEW_CHARS: i64 = 200;

#[derive(Debug, Serialize)]
/// Content-free representation of one event, sized independently of its text.
///
/// Full content is loaded separately by event primary key.
pub struct UsageEventOutline {
    /// Backend-generated event primary key.
    pub id: Uuid,
    /// Collector-generated idempotency key.
    pub event_id: String,
    /// Owning turn, used only to group events.
    #[serde(skip)]
    pub turn_pk: Uuid,
    /// Backend-normalized turn number.
    pub turn_index: i32,
    /// LLM provider as reported by the collector.
    pub llm_provider: String,
    /// Provider-specific model identifier.
    pub llm_model: String,
    /// Canonical request or response value.
    pub event_type: String,
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
    /// Text length in characters; zero when text is absent.
    pub text_chars: i64,
    /// Up to [`OUTLINE_PREVIEW_CHARS`] characters of text: the tail of a
    /// request, where the newest prompt is, or the head of a response.
    pub preview: Option<String>,
    /// Number of image attachments on the event.
    pub attachment_count: i64,
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
    /// Agent name as reported by the collector.
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
    /// Agent-generated title, or a first-user-input preview for other Agents.
    pub title: Option<String>,
    /// Why the session exists when it only serves the Agent, such as
    /// `title_generation`; `None` for ordinary sessions.
    pub purpose: Option<String>,
    /// For a naming thread, the task session its title was matched to.
    pub parent_session_pk: Option<Uuid>,
}

#[derive(Debug, Serialize)]
/// One normalized turn and its deterministically ordered events.
pub struct TurnTimeline<E = UsageEventResponse> {
    /// Backend turn primary key.
    pub turn_pk: Uuid,
    /// Backend-normalized turn number.
    pub turn_index: i32,
    /// Earliest event observation time in the turn.
    pub started_at: DateTime<Utc>,
    /// Latest event observation time in the turn.
    pub ended_at: Option<DateTime<Utc>>,
    /// Request and response events in authoritative timeline order.
    pub events: Vec<E>,
}

/// Returns the JSON object used when optional metadata is absent.
pub fn empty_metadata() -> Value {
    json!({})
}

#[cfg(test)]
mod tests {
    use super::{
        IngestEventsRequest, RawEventFields, RawEventsQuery, SummaryFields, SummaryQuery,
        SummaryResponse, SummaryRow, TokenUsageSummaryResponse, UsageEventType,
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
                    "token_usage": {"input_tokens": 10_i64, "output_tokens": 0_i64, "cache_read_tokens": 2_i64, "cache_write_tokens": 0_i64, "reasoning_tokens": 0_i64, "total_tokens": 12_i64},
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
                        "host_name": "alice-mbp",
                        "platform": "macos"
                    },
                    "agent": {
                        "name": "claude-code"
                    },
                    "session_id": "session-002",
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
            "omitted token_usage should leave every counter absent"
        );
        assert_eq!(event.turn_id, None, "an omitted turn names no turn");
        assert!(
            !event.naming_thread,
            "an omitted flag is not a naming thread"
        );
        assert_eq!(
            event.metadata,
            json!({}),
            "omitted metadata should default to an empty object"
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
    fn raw_events_query_selects_the_list_projection() {
        let list = serde_json::from_value::<RawEventsQuery>(json!({"fields": "list"}))
            .expect("list query should deserialize");
        let full = serde_json::from_value::<RawEventsQuery>(json!({}))
            .expect("default query should deserialize");

        assert_eq!(list.fields, Some(RawEventFields::List));
        assert!(!list.includes_content());
        assert!(full.includes_content());
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
                        "output_tokens": 456_i64,
                        "cache_read_tokens": 11_i64,
                        "cache_write_tokens": 12_i64,
                        "total_tokens": 615_i64
                    }
                ],
                "next_page_token": null
            }),
            "token usage summary should only expose chart token fields"
        );
    }
}
