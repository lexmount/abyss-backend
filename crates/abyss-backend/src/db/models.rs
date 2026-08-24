//! Diesel row and insert models owned by the Agent event service.
//!
//! Query models mirror complete table rows because Diesel uses them for typed
//! selection. Insert models make server-assigned fields explicit and prevent
//! HTTP payloads from being written directly without normalization.

use chrono::{DateTime, Utc};
use diesel::{Insertable, Queryable, Selectable};
use serde_json::Value;
use uuid::Uuid;

use super::schema::{
    agent_diagnostic_capture_events, agent_diagnostic_captures, agent_sessions, agent_turns,
    devices, llm_usage_event_attachments, llm_usage_events, search_outbox,
};

#[derive(Clone, Debug, Queryable, Selectable)]
#[diesel(table_name = devices)]
#[diesel(check_for_backend(diesel::pg::Pg))]
/// Persisted device context keyed by owner, host name, and platform.
pub struct Device {
    /// Server-generated device primary key.
    pub id: Uuid,
    /// Owning user identifier.
    pub user_id: Uuid,
    /// User-visible host name.
    pub host_name: String,
    /// Normalized platform slug.
    pub platform: String,
    /// Most recently observed OS version.
    pub os_version: Option<String>,
    /// Earliest event observation associated with the device.
    pub first_seen_at: DateTime<Utc>,
    /// Latest event observation associated with the device.
    pub last_seen_at: DateTime<Utc>,
    /// Row creation time.
    pub created_at: DateTime<Utc>,
    /// Last metadata update time.
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = devices)]
/// Values used to create a device context.
pub struct NewDevice {
    /// Server-generated device primary key.
    pub id: Uuid,
    /// Owning user identifier.
    pub user_id: Uuid,
    /// User-visible host name.
    pub host_name: String,
    /// Normalized platform slug.
    pub platform: String,
    /// Optional observed OS version.
    pub os_version: Option<String>,
    /// Initial earliest observation time.
    pub first_seen_at: DateTime<Utc>,
    /// Initial latest observation time.
    pub last_seen_at: DateTime<Utc>,
    /// Row creation time.
    pub created_at: DateTime<Utc>,
    /// Initial metadata update time.
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Queryable, Selectable)]
#[diesel(table_name = agent_sessions)]
#[diesel(check_for_backend(diesel::pg::Pg))]
/// Persisted Agent session containing many normalized turns.
pub struct AgentSession {
    /// Server-generated session primary key.
    pub id: Uuid,
    /// Owning user identifier.
    pub user_id: Uuid,
    /// Most recently associated device-context key.
    pub device_context_id: Uuid,
    /// Canonical Agent name.
    pub agent_name: String,
    /// Most recently observed Agent version.
    pub agent_version: Option<String>,
    /// Agent-native session identifier.
    pub session_id: String,
    /// Earliest event observation in the session.
    pub started_at: DateTime<Utc>,
    /// Latest event observation in the session.
    pub ended_at: Option<DateTime<Utc>>,
    /// Stable allowlisted session metadata.
    pub metadata: Value,
    /// Row creation time.
    pub created_at: DateTime<Utc>,
    /// Last session metadata update time.
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = agent_sessions)]
/// Values used to create an Agent session.
pub struct NewAgentSession {
    /// Server-generated session primary key.
    pub id: Uuid,
    /// Owning user identifier.
    pub user_id: Uuid,
    /// Associated device-context key.
    pub device_context_id: Uuid,
    /// Canonical Agent name.
    pub agent_name: String,
    /// Optional Agent version.
    pub agent_version: Option<String>,
    /// Agent-native session identifier.
    pub session_id: String,
    /// Initial earliest observation time.
    pub started_at: DateTime<Utc>,
    /// Initial latest observation time.
    pub ended_at: Option<DateTime<Utc>>,
    /// Stable allowlisted session metadata.
    pub metadata: Value,
    /// Row creation time.
    pub created_at: DateTime<Utc>,
    /// Initial metadata update time.
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Queryable, Selectable)]
#[diesel(table_name = agent_turns)]
#[diesel(check_for_backend(diesel::pg::Pg))]
/// Persisted logical turn within one Agent session.
pub struct AgentTurn {
    /// Server-generated turn primary key.
    pub id: Uuid,
    /// Owning user identifier.
    pub user_id: Uuid,
    /// Parent session primary key.
    pub session_pk: Uuid,
    /// Positive turn number unique within the session.
    pub turn_index: i32,
    /// Earliest event observation in the turn.
    pub started_at: DateTime<Utc>,
    /// Latest event observation in the turn.
    pub ended_at: Option<DateTime<Utc>>,
    /// Row creation time.
    pub created_at: DateTime<Utc>,
    /// Last turn-boundary update time.
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = agent_turns)]
/// Values used to create a logical Agent turn.
pub struct NewAgentTurn {
    /// Server-generated turn primary key.
    pub id: Uuid,
    /// Owning user identifier.
    pub user_id: Uuid,
    /// Parent session primary key.
    pub session_pk: Uuid,
    /// Positive turn number unique within the session.
    pub turn_index: i32,
    /// Initial earliest observation time.
    pub started_at: DateTime<Utc>,
    /// Initial latest observation time.
    pub ended_at: Option<DateTime<Utc>>,
    /// Row creation time.
    pub created_at: DateTime<Utc>,
    /// Initial boundary update time.
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Queryable, Selectable)]
#[diesel(table_name = llm_usage_events)]
#[diesel(check_for_backend(diesel::pg::Pg))]
/// Source-of-truth row for one immutable LLM usage observation.
pub struct UsageEvent {
    /// Server-generated event primary key.
    pub id: Uuid,
    /// Owning user identifier.
    pub user_id: Uuid,
    /// Device context observed for the event.
    pub device_context_id: Uuid,
    /// Parent session primary key.
    pub session_pk: Uuid,
    /// Parent normalized-turn primary key.
    pub turn_pk: Uuid,
    /// Collector-generated idempotency key.
    pub event_id: String,
    /// Canonical Agent name.
    pub agent_name: String,
    /// Agent version observed for the event.
    pub agent_version: Option<String>,
    /// Agent-native session identifier copied for direct filtering.
    pub session_id: String,
    /// Normalized turn number copied for direct filtering.
    pub turn_index: i32,
    /// Canonical LLM provider slug.
    pub llm_provider: String,
    /// Provider-specific model identifier.
    pub llm_model: String,
    /// Canonical request or response value.
    pub event_type: String,
    /// Optional prompt or response text.
    pub text: Option<String>,
    /// Binary SHA-256 digest of text when present.
    pub text_sha256: Option<Vec<u8>>,
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
    /// Collector observation time.
    pub observed_at: DateTime<Utc>,
    /// Collector metadata retained with the event.
    pub metadata: Value,
    /// Server insertion time.
    pub created_at: DateTime<Utc>,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = llm_usage_event_attachments)]
#[diesel(check_for_backend(diesel::pg::Pg))]
/// Persisted metadata and optional bytes for one event image.
pub struct UsageEventAttachment {
    /// Server-generated attachment primary key.
    pub id: Uuid,
    /// Owning user identifier copied for direct authorization.
    pub user_id: Uuid,
    /// Parent usage-event primary key.
    pub event_pk: Uuid,
    /// Unique display position within the event.
    pub position: i32,
    /// Validated browser-safe MIME type.
    pub media_type: String,
    /// Declared decoded content length.
    pub byte_size: i64,
    /// Binary SHA-256 digest.
    pub sha256: Vec<u8>,
    /// Optional image bytes for metadata-only attachments.
    pub content: Option<Vec<u8>>,
    /// Server insertion time.
    pub created_at: DateTime<Utc>,
}

#[derive(Insertable)]
#[diesel(table_name = agent_diagnostic_captures)]
/// Values used to create an opaque diagnostic capture.
pub struct NewAgentDiagnosticCapture {
    /// Server-generated capture primary key.
    pub id: Uuid,
    /// Owning user identifier.
    pub user_id: Uuid,
    /// Correlated device-context key.
    pub device_context_id: Uuid,
    /// Correlated session key.
    pub session_pk: Uuid,
    /// Collector-generated idempotency key.
    pub capture_id: String,
    /// Collector flow identifier.
    pub flow_id: String,
    /// Collector observation time.
    pub captured_at: DateTime<Utc>,
    /// Collector version that produced the payload.
    pub collector_version: String,
    /// Opaque diagnostic JSON.
    pub payload: Value,
    /// Server insertion time.
    pub created_at: DateTime<Utc>,
}

#[derive(Insertable)]
#[diesel(table_name = agent_diagnostic_capture_events)]
/// Join row linking one diagnostic capture to one usage event.
pub struct NewAgentDiagnosticCaptureEvent {
    /// Diagnostic capture primary key.
    pub capture_pk: Uuid,
    /// Correlated usage-event primary key.
    pub event_pk: Uuid,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = llm_usage_events)]
/// Normalized values used to create one immutable usage event.
pub struct NewUsageEvent {
    /// Server-generated event primary key.
    pub id: Uuid,
    /// Owning user identifier.
    pub user_id: Uuid,
    /// Associated device-context key.
    pub device_context_id: Uuid,
    /// Parent session primary key.
    pub session_pk: Uuid,
    /// Parent normalized-turn primary key.
    pub turn_pk: Uuid,
    /// Collector-generated idempotency key.
    pub event_id: String,
    /// Canonical Agent name.
    pub agent_name: String,
    /// Optional Agent version.
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
    /// Optional binary SHA-256 of text.
    pub text_sha256: Option<Vec<u8>>,
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
    /// Collector observation time.
    pub observed_at: DateTime<Utc>,
    /// Collector metadata retained with the event.
    pub metadata: Value,
    /// Server insertion time.
    pub created_at: DateTime<Utc>,
}

#[derive(Insertable)]
#[diesel(table_name = llm_usage_event_attachments)]
/// Validated values used to create one event attachment.
pub struct NewUsageEventAttachment {
    /// Server-generated attachment primary key.
    pub id: Uuid,
    /// Owning user identifier.
    pub user_id: Uuid,
    /// Parent usage-event primary key.
    pub event_pk: Uuid,
    /// Unique display position within the event.
    pub position: i32,
    /// Validated browser-safe MIME type.
    pub media_type: String,
    /// Decoded content length.
    pub byte_size: i64,
    /// Binary SHA-256 digest.
    pub sha256: Vec<u8>,
    /// Optional decoded image bytes.
    pub content: Option<Vec<u8>>,
    /// Server insertion time.
    pub created_at: DateTime<Utc>,
}

#[derive(Insertable)]
#[diesel(table_name = search_outbox)]
/// Durable search-projection task created with a new usage event.
pub struct NewSearchOutboxTask {
    /// Source usage-event primary key and Elasticsearch document key.
    pub event_pk: Uuid,
    /// Owning user identifier retained for projection bookkeeping.
    pub user_id: Uuid,
    /// Source event insertion time.
    pub created_at: DateTime<Utc>,
}
