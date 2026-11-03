//! The stored event as the event API returns it, grouped the way the
//! collector's `AgentEvent` groups it. The collector uploads the event's
//! purpose, omitted categories, request path, and headers inside `metadata`;
//! they come back out as typed fields here.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use super::{attachments::UsageEventAttachmentResponse, streams::EventLocation};

#[derive(Debug, Serialize)]
/// API representation of one persisted usage event.
pub struct UsageEventResponse {
    /// Backend-generated event primary key.
    pub id: Uuid,
    /// Collector-generated idempotency key.
    pub event_id: String,
    /// Backend session primary key.
    pub session_pk: Uuid,
    /// Agent-native session identifier.
    pub session_id: String,
    /// Current stream position, when the request asks for event locations.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<EventLocation>,
    /// Collector observation timestamp.
    pub observed_at: DateTime<Utc>,
    /// Request or response.
    pub event_type: String,
    /// Agent that produced the event.
    pub agent: EventAgent,
    /// Device the event was observed on.
    pub device: EventDevice,
    /// Provider and model of the call.
    pub llm: EventLlm,
    /// Why the Agent made the call, when it says so.
    pub purpose: Option<String>,
    /// Ids the client declared for the exchange.
    pub declared: DeclaredIds,
    /// Provider identity of the response the exchange produced.
    pub response_id: Option<String>,
    /// Response whose conversation the exchange's request continues.
    pub previous_response_id: Option<String>,
    /// Request path and captured headers.
    pub http: EventHttp,
    /// Token counters as reported.
    pub token_usage: EventTokenUsage,
    /// Content categories the collector left out of the event.
    pub omitted: Vec<String>,
    /// Conversation of the event, as the collector reported it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conversation: Option<Value>,
    /// Ordered attachment metadata associated with the event.
    pub attachments: Vec<UsageEventAttachmentResponse>,
}

#[derive(Debug, Serialize)]
/// Agent identity of an event.
pub struct EventAgent {
    /// Agent name as reported by the collector.
    pub name: String,
    /// Agent version observed on this event.
    pub version: Option<String>,
}

#[derive(Debug, Default, Serialize)]
/// Device of an event; every label is null when the device row is missing.
pub struct EventDevice {
    /// Device host name.
    pub host_name: Option<String>,
    /// Device platform.
    pub platform: Option<String>,
    /// Operating-system version the collector last reported for the device.
    pub os_version: Option<String>,
}

#[derive(Debug, Serialize)]
/// LLM target of an event.
pub struct EventLlm {
    /// LLM provider as reported by the collector.
    pub provider: String,
    /// Provider-specific model identifier.
    pub model: String,
}

#[derive(Debug, Default, Serialize)]
#[expect(
    clippy::struct_field_names,
    reason = "The fields keep the names the client declares them under."
)]
/// Ids the client declared for the event's exchange, as stored with its
/// stream facts.
pub struct DeclaredIds {
    /// Sub-agent the exchange belongs to.
    pub subagent_id: Option<String>,
    /// Sub-agent that started that sub-agent.
    pub parent_subagent_id: Option<String>,
    /// Turn the exchange works for.
    pub turn_id: Option<String>,
}

#[derive(Debug, Default, Serialize)]
/// HTTP request path and headers of an event.
pub struct EventHttp {
    /// Request path, as captured.
    pub path: Option<String>,
    /// Captured headers in capture order.
    pub headers: Vec<HttpHeader>,
}

#[derive(Debug, Deserialize, Serialize)]
/// One captured header, in the collector's wire shape: a UTF-8 value as
/// `value`, other bytes base64-encoded as `value_base64`.
pub struct HttpHeader {
    /// Header name as captured.
    pub name: String,
    /// UTF-8 header value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    /// Base64 of a header value that is not UTF-8.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_base64: Option<String>,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[expect(
    clippy::struct_field_names,
    reason = "Public JSON token usage fields intentionally include the token suffix."
)]
/// Token counters of an event, as stored.
pub struct EventTokenUsage {
    /// Input-token count.
    pub input_tokens: i64,
    /// Output-token count.
    pub output_tokens: i64,
    /// Cache-read-token count.
    pub cache_read_tokens: i64,
    /// Cache-write-token count.
    pub cache_write_tokens: i64,
    /// Reasoning-token count.
    pub reasoning_tokens: i64,
    /// Total-token count.
    pub total_tokens: i64,
}

#[derive(Debug, Default)]
/// What the collector uploaded inside `metadata`.
pub struct RecordedMetadata {
    /// `metadata.purpose`.
    pub purpose: Option<String>,
    /// `metadata.omitted`.
    pub omitted: Vec<String>,
    /// `metadata.path` and `metadata.headers`.
    pub http: EventHttp,
}

impl RecordedMetadata {
    /// Reads the stored metadata; a key that is missing or has another shape
    /// reads as absent.
    pub fn read(metadata: &Value) -> Self {
        fn field<T: serde::de::DeserializeOwned + Default>(metadata: &Value, key: &str) -> T {
            metadata
                .get(key)
                .and_then(|value| T::deserialize(value).ok())
                .unwrap_or_default()
        }
        Self {
            purpose: field(metadata, "purpose"),
            omitted: field(metadata, "omitted"),
            http: EventHttp {
                path: field(metadata, "path"),
                headers: field(metadata, "headers"),
            },
        }
    }
}

#[derive(Debug, Serialize)]
/// One usage event as an event list shows it: identity, Agent, device, model,
/// and token counts, without content.
pub struct UsageEventListItem {
    /// Backend-generated event primary key.
    pub id: Uuid,
    /// Collector-generated idempotency key.
    pub event_id: String,
    /// Backend session primary key.
    pub session_pk: Uuid,
    /// Agent-native session identifier.
    pub session_id: String,
    /// Current stream position, when the request asks for event locations.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<EventLocation>,
    /// Collector observation timestamp.
    pub observed_at: DateTime<Utc>,
    /// Request or response.
    pub event_type: String,
    /// Agent that produced the event.
    pub agent: EventAgent,
    /// Device the event was observed on.
    pub device: EventDevice,
    /// Provider and model of the call.
    pub llm: EventLlm,
    /// Token counters as reported.
    pub token_usage: EventTokenUsage,
}

impl From<UsageEventResponse> for UsageEventListItem {
    fn from(event: UsageEventResponse) -> Self {
        Self {
            id: event.id,
            event_id: event.event_id,
            session_pk: event.session_pk,
            session_id: event.session_id,
            location: event.location,
            observed_at: event.observed_at,
            event_type: event.event_type,
            agent: event.agent,
            device: event.device,
            llm: event.llm,
            token_usage: event.token_usage,
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::RecordedMetadata;

    #[test]
    fn recorded_metadata_comes_out_as_typed_fields() {
        let recorded = RecordedMetadata::read(&json!({
            "purpose": "session_title",
            "omitted": ["images"],
            "path": "/v1/messages",
            "headers": [
                {"name": "content-type", "value": "application/json"},
                {"name": "x-raw", "value_base64": "/w=="}
            ],
            "archive": "ignored"
        }));
        assert_eq!(recorded.purpose.as_deref(), Some("session_title"));
        assert_eq!(recorded.omitted, ["images"]);
        assert_eq!(recorded.http.path.as_deref(), Some("/v1/messages"));
        assert_eq!(
            serde_json::to_value(&recorded.http.headers).expect("headers serialize"),
            json!([
                {"name": "content-type", "value": "application/json"},
                {"name": "x-raw", "value_base64": "/w=="}
            ])
        );
    }

    #[test]
    fn missing_or_misshapen_metadata_reads_as_absent() {
        let recorded = RecordedMetadata::read(&json!({"purpose": 3_i64, "headers": {}}));
        assert_eq!(recorded.purpose, None);
        assert_eq!(recorded.omitted, Vec::<String>::new());
        assert_eq!(recorded.http.path, None);
        assert_eq!(recorded.http.headers.len(), 0);
    }
}
