//! Diesel row and insert models for SQLite relational tables.

use chrono::{DateTime, Utc};
use diesel::{Insertable, Queryable, Selectable};
use serde_json::Value;
use uuid::Uuid;

use crate::{error::AppError, usage::event_order::TimelineEvent};

use super::schema::{
    agent_diagnostic_capture_events, agent_diagnostic_captures, agent_sessions, agent_turns,
    devices, llm_usage_event_attachments, llm_usage_events,
};

#[derive(Queryable, Selectable)]
#[diesel(table_name = devices)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub(super) struct DeviceRow {
    pub(super) id: String,
    pub(super) user_id: String,
    pub(super) host_name: String,
    pub(super) platform: String,
    pub(super) os_version: Option<String>,
    pub(super) first_seen_at: i64,
    pub(super) last_seen_at: i64,
    pub(super) created_at: i64,
    pub(super) updated_at: i64,
}

#[derive(Insertable)]
#[diesel(table_name = devices)]
pub(super) struct NewDevice {
    pub(super) id: String,
    pub(super) user_id: String,
    pub(super) host_name: String,
    pub(super) platform: String,
    pub(super) os_version: Option<String>,
    pub(super) first_seen_at: i64,
    pub(super) last_seen_at: i64,
    pub(super) created_at: i64,
    pub(super) updated_at: i64,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = agent_sessions)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub(super) struct SessionRow {
    pub(super) id: String,
    pub(super) user_id: String,
    pub(super) device_context_id: String,
    pub(super) agent_name: String,
    pub(super) agent_version: Option<String>,
    pub(super) session_id: String,
    pub(super) started_at: i64,
    pub(super) ended_at: Option<i64>,
    pub(super) metadata: String,
    pub(super) created_at: i64,
    pub(super) updated_at: i64,
}

#[derive(Insertable)]
#[diesel(table_name = agent_sessions)]
pub(super) struct NewSession {
    pub(super) id: String,
    pub(super) user_id: String,
    pub(super) device_context_id: String,
    pub(super) agent_name: String,
    pub(super) agent_version: Option<String>,
    pub(super) session_id: String,
    pub(super) started_at: i64,
    pub(super) ended_at: Option<i64>,
    pub(super) metadata: String,
    pub(super) created_at: i64,
    pub(super) updated_at: i64,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = agent_turns)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub(super) struct TurnRow {
    pub(super) id: String,
    pub(super) user_id: String,
    pub(super) session_pk: String,
    pub(super) turn_index: i32,
    pub(super) started_at: i64,
    pub(super) ended_at: Option<i64>,
    pub(super) created_at: i64,
    pub(super) updated_at: i64,
}

#[derive(Insertable)]
#[diesel(table_name = agent_turns)]
pub(super) struct NewTurn {
    pub(super) id: String,
    pub(super) user_id: String,
    pub(super) session_pk: String,
    pub(super) turn_index: i32,
    pub(super) started_at: i64,
    pub(super) ended_at: Option<i64>,
    pub(super) created_at: i64,
    pub(super) updated_at: i64,
}

#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = llm_usage_events)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub(super) struct EventRow {
    pub(super) id: String,
    pub(super) user_id: String,
    pub(super) device_context_id: String,
    pub(super) session_pk: String,
    pub(super) turn_pk: String,
    pub(super) event_id: String,
    pub(super) agent_name: String,
    pub(super) agent_version: Option<String>,
    pub(super) session_id: String,
    pub(super) turn_index: i32,
    pub(super) llm_provider: String,
    pub(super) llm_model: String,
    pub(super) event_type: String,
    pub(super) text: Option<String>,
    pub(super) text_sha256: Option<Vec<u8>>,
    pub(super) input_tokens: i64,
    pub(super) output_tokens: i64,
    pub(super) cache_read_tokens: i64,
    pub(super) cache_write_tokens: i64,
    pub(super) reasoning_tokens: i64,
    pub(super) total_tokens: i64,
    pub(super) observed_at: i64,
    pub(super) metadata: String,
    pub(super) created_at: i64,
}

#[derive(Insertable)]
#[diesel(table_name = llm_usage_events)]
pub(super) struct NewEvent {
    pub(super) id: String,
    pub(super) user_id: String,
    pub(super) device_context_id: String,
    pub(super) session_pk: String,
    pub(super) turn_pk: String,
    pub(super) event_id: String,
    pub(super) agent_name: String,
    pub(super) agent_version: Option<String>,
    pub(super) session_id: String,
    pub(super) turn_index: i32,
    pub(super) llm_provider: String,
    pub(super) llm_model: String,
    pub(super) event_type: String,
    pub(super) text: Option<String>,
    pub(super) text_sha256: Option<Vec<u8>>,
    pub(super) input_tokens: i64,
    pub(super) output_tokens: i64,
    pub(super) cache_read_tokens: i64,
    pub(super) cache_write_tokens: i64,
    pub(super) reasoning_tokens: i64,
    pub(super) total_tokens: i64,
    pub(super) observed_at: i64,
    pub(super) metadata: String,
    pub(super) created_at: i64,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = llm_usage_event_attachments)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub(super) struct AttachmentRow {
    pub(super) id: String,
    pub(super) user_id: String,
    pub(super) event_pk: String,
    pub(super) position: i32,
    pub(super) media_type: String,
    pub(super) byte_size: i64,
    pub(super) sha256: Vec<u8>,
    pub(super) content: Option<Vec<u8>>,
    pub(super) created_at: i64,
}

#[derive(Insertable)]
#[diesel(table_name = llm_usage_event_attachments)]
pub(super) struct NewAttachment {
    pub(super) id: String,
    pub(super) user_id: String,
    pub(super) event_pk: String,
    pub(super) position: i32,
    pub(super) media_type: String,
    pub(super) byte_size: i64,
    pub(super) sha256: Vec<u8>,
    pub(super) content: Option<Vec<u8>>,
    pub(super) created_at: i64,
}

#[derive(Insertable)]
#[diesel(table_name = agent_diagnostic_captures)]
pub(super) struct NewDiagnosticCapture {
    pub(super) id: String,
    pub(super) user_id: String,
    pub(super) device_context_id: String,
    pub(super) session_pk: String,
    pub(super) capture_id: String,
    pub(super) flow_id: String,
    pub(super) captured_at: i64,
    pub(super) collector_version: String,
    pub(super) payload: String,
    pub(super) created_at: i64,
}

#[derive(Insertable)]
#[diesel(table_name = agent_diagnostic_capture_events)]
pub(super) struct NewDiagnosticCaptureEvent {
    pub(super) capture_pk: String,
    pub(super) event_pk: String,
}

pub(super) struct DeviceRecord {
    pub(super) id: Uuid,
    pub(super) host_name: String,
    pub(super) platform: String,
    pub(super) os_version: Option<String>,
}

pub(super) struct SessionRecord {
    pub(super) id: Uuid,
    pub(super) user_id: Uuid,
    pub(super) device_context_id: Uuid,
    pub(super) agent_name: String,
    pub(super) agent_version: Option<String>,
    pub(super) session_id: String,
    pub(super) started_at: DateTime<Utc>,
    pub(super) ended_at: Option<DateTime<Utc>>,
    pub(super) metadata: Value,
}

pub(super) struct TurnRecord {
    pub(super) id: Uuid,
    pub(super) turn_index: i32,
    pub(super) started_at: DateTime<Utc>,
    pub(super) ended_at: Option<DateTime<Utc>>,
}

#[derive(Clone)]
pub(super) struct EventRecord {
    pub(super) id: Uuid,
    pub(super) user_id: Uuid,
    pub(super) device_context_id: Uuid,
    pub(super) session_pk: Uuid,
    pub(super) turn_pk: Uuid,
    pub(super) event_id: String,
    pub(super) agent_name: String,
    pub(super) agent_version: Option<String>,
    pub(super) session_id: String,
    pub(super) turn_index: i32,
    pub(super) llm_provider: String,
    pub(super) llm_model: String,
    pub(super) event_type: String,
    pub(super) text: Option<String>,
    pub(super) text_sha256: Option<Vec<u8>>,
    pub(super) input_tokens: i64,
    pub(super) output_tokens: i64,
    pub(super) cache_read_tokens: i64,
    pub(super) cache_write_tokens: i64,
    pub(super) reasoning_tokens: i64,
    pub(super) total_tokens: i64,
    pub(super) observed_at: DateTime<Utc>,
    pub(super) metadata: Value,
}

impl TimelineEvent for EventRecord {
    fn turn_index(&self) -> i32 {
        self.turn_index
    }

    fn metadata(&self) -> &Value {
        &self.metadata
    }

    fn observed_at(&self) -> DateTime<Utc> {
        self.observed_at
    }

    fn event_type(&self) -> &str {
        &self.event_type
    }

    fn event_id(&self) -> &str {
        &self.event_id
    }
}

pub(super) struct AttachmentRecord {
    pub(super) id: Uuid,
    pub(super) event_pk: Uuid,
    pub(super) position: i32,
    pub(super) media_type: String,
    pub(super) byte_size: i64,
    pub(super) sha256: Vec<u8>,
    pub(super) content: Option<Vec<u8>>,
}

impl DeviceRow {
    pub(super) fn into_record(self) -> Result<DeviceRecord, AppError> {
        let _ = (&self.user_id, self.created_at, self.updated_at);
        Ok(DeviceRecord {
            id: parse_uuid(&self.id, "device id")?,
            host_name: self.host_name,
            platform: self.platform,
            os_version: self.os_version,
        })
    }
}

impl SessionRow {
    pub(super) fn into_record(self) -> Result<SessionRecord, AppError> {
        let _ = (self.created_at, self.updated_at);
        Ok(SessionRecord {
            id: parse_uuid(&self.id, "session id")?,
            user_id: parse_uuid(&self.user_id, "session user id")?,
            device_context_id: parse_uuid(&self.device_context_id, "session device id")?,
            agent_name: self.agent_name,
            agent_version: self.agent_version,
            session_id: self.session_id,
            started_at: timestamp_from_micros(self.started_at)?,
            ended_at: self.ended_at.map(timestamp_from_micros).transpose()?,
            metadata: parse_json(&self.metadata, "session metadata")?,
        })
    }
}

impl TurnRow {
    pub(super) fn into_record(self) -> Result<TurnRecord, AppError> {
        let _ = (
            &self.user_id,
            &self.session_pk,
            self.created_at,
            self.updated_at,
        );
        Ok(TurnRecord {
            id: parse_uuid(&self.id, "turn id")?,
            turn_index: self.turn_index,
            started_at: timestamp_from_micros(self.started_at)?,
            ended_at: self.ended_at.map(timestamp_from_micros).transpose()?,
        })
    }
}

impl EventRow {
    pub(super) fn into_record(self) -> Result<EventRecord, AppError> {
        let _ = self.created_at;
        Ok(EventRecord {
            id: parse_uuid(&self.id, "event id")?,
            user_id: parse_uuid(&self.user_id, "event user id")?,
            device_context_id: parse_uuid(&self.device_context_id, "event device id")?,
            session_pk: parse_uuid(&self.session_pk, "event session id")?,
            turn_pk: parse_uuid(&self.turn_pk, "event turn id")?,
            event_id: self.event_id,
            agent_name: self.agent_name,
            agent_version: self.agent_version,
            session_id: self.session_id,
            turn_index: self.turn_index,
            llm_provider: self.llm_provider,
            llm_model: self.llm_model,
            event_type: self.event_type,
            text: self.text,
            text_sha256: self.text_sha256,
            input_tokens: self.input_tokens,
            output_tokens: self.output_tokens,
            cache_read_tokens: self.cache_read_tokens,
            cache_write_tokens: self.cache_write_tokens,
            reasoning_tokens: self.reasoning_tokens,
            total_tokens: self.total_tokens,
            observed_at: timestamp_from_micros(self.observed_at)?,
            metadata: parse_json(&self.metadata, "event metadata")?,
        })
    }
}

impl AttachmentRow {
    pub(super) fn into_record(self) -> Result<AttachmentRecord, AppError> {
        let _ = (&self.user_id, self.created_at);
        Ok(AttachmentRecord {
            id: parse_uuid(&self.id, "attachment id")?,
            event_pk: parse_uuid(&self.event_pk, "attachment event id")?,
            position: self.position,
            media_type: self.media_type,
            byte_size: self.byte_size,
            sha256: self.sha256,
            content: self.content,
        })
    }
}

pub(super) fn parse_uuid(value: &str, field: &str) -> Result<Uuid, AppError> {
    Uuid::parse_str(value)
        .map_err(|error| AppError::internal(format!("stored {field} is not a UUID: {error}")))
}

pub(super) fn timestamp_from_micros(value: i64) -> Result<DateTime<Utc>, AppError> {
    DateTime::from_timestamp_micros(value).ok_or_else(|| {
        AppError::internal(format!(
            "stored timestamp microseconds are out of range: {value}"
        ))
    })
}

fn parse_json(value: &str, field: &str) -> Result<Value, AppError> {
    serde_json::from_str(value)
        .map_err(|error| AppError::internal(format!("stored {field} is invalid JSON: {error}")))
}
