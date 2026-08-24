//! API request and response types for Agent usage collection.

pub mod attachments;
pub mod diagnostics;
mod event_order;
pub mod repository;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

const WORKING_DIRECTORY_METADATA_KEY: &str = "working_directory";

#[derive(Deserialize)]
pub struct IngestEventsRequest {
    pub events: Vec<IngestUsageEvent>,
    #[serde(default)]
    pub diagnostic_captures: Vec<diagnostics::IngestDiagnosticCapture>,
}

#[derive(Debug, Deserialize)]
pub struct IngestUsageEvent {
    pub event_id: String,
    pub observed_at: DateTime<Utc>,
    pub device: DevicePayload,
    pub agent: AgentPayload,
    pub session_id: String,
    /// Collector-side best-effort turn number. Ingest normalizes this when a
    /// stable logical-turn/provider identity shows that a restarted collector
    /// reused an earlier local sequence number.
    pub turn_index: i32,
    pub llm: LlmPayload,
    pub event_type: UsageEventType,
    pub text: Option<String>,
    #[serde(default)]
    pub token_usage: TokenUsagePayload,
    #[serde(default = "empty_metadata")]
    pub metadata: Value,
    #[serde(default)]
    pub attachments: Vec<attachments::IngestImageAttachment>,
}

#[derive(Debug, Deserialize)]
pub struct DevicePayload {
    #[serde(alias = "hostname")]
    pub host_name: String,
    pub platform: String,
    pub os_version: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct AgentPayload {
    pub name: String,
    pub version: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct LlmPayload {
    pub provider: String,
    pub model: String,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum UsageEventType {
    Request,
    Response,
}

impl UsageEventType {
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
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cache_read_tokens: Option<i64>,
    pub cache_write_tokens: Option<i64>,
    pub reasoning_tokens: Option<i64>,
    pub total_tokens: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct IngestEventsResponse {
    pub accepted: usize,
    pub duplicates: usize,
    pub rejected: usize,
    pub errors: Vec<String>,
    pub accepted_diagnostic_captures: usize,
    pub duplicate_diagnostic_captures: usize,
}

#[derive(Clone, Debug, Deserialize)]
pub struct SummaryQuery {
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    pub group_by: Option<String>,
    pub fields: Option<SummaryFields>,
    pub user_filter: Option<String>,
    pub agent_name: Option<String>,
    pub session_id: Option<String>,
    pub llm_provider: Option<String>,
    pub llm_model: Option<String>,
    pub event_type: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SummaryFields {
    Full,
    TokenUsage,
}

#[derive(Debug, Serialize)]
pub struct SummaryResponse {
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    pub group_by: Vec<String>,
    pub rows: Vec<SummaryRow>,
    pub next_page_token: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SummaryRow {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub day: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_email: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub platform: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub os_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub llm_provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub llm_model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_type: Option<String>,
    pub sessions: usize,
    pub turns: usize,
    pub requests: i64,
    pub responses: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_write_tokens: i64,
    pub reasoning_tokens: i64,
    pub total_tokens: i64,
}

#[derive(Debug, Serialize)]
pub struct TokenUsageSummaryResponse {
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    pub group_by: Vec<String>,
    pub rows: Vec<TokenUsageSummaryRow>,
    pub next_page_token: Option<String>,
}

impl TokenUsageSummaryResponse {
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
pub struct TokenUsageSummaryRow {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub day: Option<String>,
    pub input_tokens: i64,
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
pub struct RawEventsQuery {
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    pub agent_name: Option<String>,
    pub session_id: Option<String>,
    pub session_pk: Option<Uuid>,
    pub turn_index: Option<i32>,
    pub llm_provider: Option<String>,
    pub llm_model: Option<String>,
    pub event_type: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct RawEventsResponse {
    pub events: Vec<UsageEventResponse>,
    pub next_page_token: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct UsageEventResponse {
    pub id: Uuid,
    pub event_id: String,
    pub user_id: Uuid,
    pub device_context_id: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub platform: Option<String>,
    pub session_pk: Uuid,
    pub turn_pk: Uuid,
    pub agent_name: String,
    pub agent_version: Option<String>,
    pub session_id: String,
    pub turn_index: i32,
    pub llm_provider: String,
    pub llm_model: String,
    pub event_type: String,
    pub text: Option<String>,
    pub text_sha256: Option<String>,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_write_tokens: i64,
    pub reasoning_tokens: i64,
    pub total_tokens: i64,
    pub observed_at: DateTime<Utc>,
    pub metadata: Value,
    pub attachments: Vec<attachments::UsageEventAttachmentResponse>,
}

#[derive(Debug, Serialize)]
pub struct SessionTimelineResponse {
    pub session: SessionInfo,
    pub turns: Vec<TurnTimeline>,
}

#[derive(Debug, Serialize)]
pub struct SessionInfo {
    pub session_pk: Uuid,
    pub user_id: Uuid,
    pub device_context_id: Uuid,
    pub agent_name: String,
    pub agent_version: Option<String>,
    pub session_id: String,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
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
pub struct TurnTimeline {
    pub turn_pk: Uuid,
    pub turn_index: i32,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub events: Vec<UsageEventResponse>,
}

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
