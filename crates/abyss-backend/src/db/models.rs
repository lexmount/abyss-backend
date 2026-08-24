//! Diesel row and insert models owned by the Agent event service.

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
pub struct Device {
    pub id: Uuid,
    pub user_id: Uuid,
    pub host_name: String,
    pub platform: String,
    pub os_version: Option<String>,
    pub first_seen_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = devices)]
pub struct NewDevice {
    pub id: Uuid,
    pub user_id: Uuid,
    pub host_name: String,
    pub platform: String,
    pub os_version: Option<String>,
    pub first_seen_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Queryable, Selectable)]
#[diesel(table_name = agent_sessions)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct AgentSession {
    pub id: Uuid,
    pub user_id: Uuid,
    pub device_context_id: Uuid,
    pub agent_name: String,
    pub agent_version: Option<String>,
    pub session_id: String,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub metadata: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = agent_sessions)]
pub struct NewAgentSession {
    pub id: Uuid,
    pub user_id: Uuid,
    pub device_context_id: Uuid,
    pub agent_name: String,
    pub agent_version: Option<String>,
    pub session_id: String,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub metadata: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Queryable, Selectable)]
#[diesel(table_name = agent_turns)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct AgentTurn {
    pub id: Uuid,
    pub user_id: Uuid,
    pub session_pk: Uuid,
    pub turn_index: i32,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = agent_turns)]
pub struct NewAgentTurn {
    pub id: Uuid,
    pub user_id: Uuid,
    pub session_pk: Uuid,
    pub turn_index: i32,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Queryable, Selectable)]
#[diesel(table_name = llm_usage_events)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct UsageEvent {
    pub id: Uuid,
    pub user_id: Uuid,
    pub device_context_id: Uuid,
    pub session_pk: Uuid,
    pub turn_pk: Uuid,
    pub event_id: String,
    pub agent_name: String,
    pub agent_version: Option<String>,
    pub session_id: String,
    pub turn_index: i32,
    pub llm_provider: String,
    pub llm_model: String,
    pub event_type: String,
    pub text: Option<String>,
    pub text_sha256: Option<Vec<u8>>,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_write_tokens: i64,
    pub reasoning_tokens: i64,
    pub total_tokens: i64,
    pub observed_at: DateTime<Utc>,
    pub metadata: Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = llm_usage_event_attachments)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct UsageEventAttachment {
    pub id: Uuid,
    pub user_id: Uuid,
    pub event_pk: Uuid,
    pub position: i32,
    pub media_type: String,
    pub byte_size: i64,
    pub sha256: Vec<u8>,
    pub content: Option<Vec<u8>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Insertable)]
#[diesel(table_name = agent_diagnostic_captures)]
pub struct NewAgentDiagnosticCapture {
    pub id: Uuid,
    pub user_id: Uuid,
    pub device_context_id: Uuid,
    pub session_pk: Uuid,
    pub capture_id: String,
    pub flow_id: String,
    pub captured_at: DateTime<Utc>,
    pub collector_version: String,
    pub payload: Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Insertable)]
#[diesel(table_name = agent_diagnostic_capture_events)]
pub struct NewAgentDiagnosticCaptureEvent {
    pub capture_pk: Uuid,
    pub event_pk: Uuid,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = llm_usage_events)]
pub struct NewUsageEvent {
    pub id: Uuid,
    pub user_id: Uuid,
    pub device_context_id: Uuid,
    pub session_pk: Uuid,
    pub turn_pk: Uuid,
    pub event_id: String,
    pub agent_name: String,
    pub agent_version: Option<String>,
    pub session_id: String,
    pub turn_index: i32,
    pub llm_provider: String,
    pub llm_model: String,
    pub event_type: String,
    pub text: Option<String>,
    pub text_sha256: Option<Vec<u8>>,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_write_tokens: i64,
    pub reasoning_tokens: i64,
    pub total_tokens: i64,
    pub observed_at: DateTime<Utc>,
    pub metadata: Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Insertable)]
#[diesel(table_name = llm_usage_event_attachments)]
pub struct NewUsageEventAttachment {
    pub id: Uuid,
    pub user_id: Uuid,
    pub event_pk: Uuid,
    pub position: i32,
    pub media_type: String,
    pub byte_size: i64,
    pub sha256: Vec<u8>,
    pub content: Option<Vec<u8>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Insertable)]
#[diesel(table_name = search_outbox)]
pub struct NewSearchOutboxTask {
    pub event_pk: Uuid,
    pub user_id: Uuid,
    pub created_at: DateTime<Utc>,
}
