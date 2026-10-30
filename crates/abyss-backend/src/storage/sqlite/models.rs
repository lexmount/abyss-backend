//! Diesel row and insert models for SQLite relational tables.

use chrono::{DateTime, Utc};
use diesel::{Insertable, Queryable, Selectable};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use uuid::Uuid;

use crate::{
    error::AppError,
    usage::{
        UsageEventType,
        streams::{EventFacts, Label, RequestFacts, ResponseFacts, StreamEvent},
    },
};

use super::schema::{
    agent_diagnostic_capture_events, agent_diagnostic_captures, agent_sessions, devices,
    llm_usage_event_attachments, llm_usage_event_stream_facts, llm_usage_events, search_refs,
    search_texts,
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
    pub(super) agent_version: Option<String>,
    pub(super) started_at: i64,
    pub(super) ended_at: Option<i64>,
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
    /// Changes whenever ingestion adds an event to the session.
    pub(super) structure_revision: i64,
}

#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = llm_usage_events)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub(super) struct EventRow {
    pub(super) id: String,
    pub(super) user_id: String,
    pub(super) device_context_id: String,
    pub(super) session_pk: String,
    pub(super) event_id: String,
    pub(super) agent_name: String,
    pub(super) agent_version: Option<String>,
    pub(super) session_id: String,
    pub(super) llm_provider: String,
    pub(super) llm_model: String,
    pub(super) event_type: String,
    pub(super) input_tokens: i64,
    pub(super) output_tokens: i64,
    pub(super) cache_read_tokens: i64,
    pub(super) cache_write_tokens: i64,
    pub(super) reasoning_tokens: i64,
    pub(super) total_tokens: i64,
    pub(super) observed_at: i64,
    pub(super) metadata: String,
    pub(super) created_at: i64,
    pub(super) stream_id: Option<String>,
    pub(super) response_id: Option<String>,
    pub(super) previous_response_id: Option<String>,
    pub(super) conversation: Option<String>,
}

#[derive(Insertable)]
#[diesel(table_name = llm_usage_events)]
pub(super) struct NewEvent {
    pub(super) id: String,
    pub(super) user_id: String,
    pub(super) device_context_id: String,
    pub(super) session_pk: String,
    pub(super) event_id: String,
    pub(super) agent_name: String,
    pub(super) agent_version: Option<String>,
    pub(super) session_id: String,
    pub(super) llm_provider: String,
    pub(super) llm_model: String,
    pub(super) event_type: String,
    pub(super) input_tokens: i64,
    pub(super) output_tokens: i64,
    pub(super) cache_read_tokens: i64,
    pub(super) cache_write_tokens: i64,
    pub(super) reasoning_tokens: i64,
    pub(super) total_tokens: i64,
    pub(super) observed_at: i64,
    pub(super) metadata: String,
    pub(super) created_at: i64,
    pub(super) stream_id: Option<String>,
    pub(super) response_id: Option<String>,
    pub(super) previous_response_id: Option<String>,
    pub(super) conversation: Option<String>,
}

/// Stream facts of a new event; the other side's columns stay NULL.
#[derive(Default, Insertable)]
#[diesel(table_name = llm_usage_event_stream_facts)]
pub(super) struct NewStreamFacts {
    pub(super) event_pk: String,
    pub(super) new_from: Option<i32>,
    pub(super) starts_turn: Option<bool>,
    pub(super) last_identity: Option<String>,
    pub(super) trailing: Option<String>,
    pub(super) result_ids: Option<String>,
    pub(super) emitted_ids: Option<String>,
    pub(super) output_skeleton: Option<String>,
    pub(super) label_index: Option<i32>,
    pub(super) label_text: Option<String>,
    pub(super) label_length: Option<i32>,
    pub(super) subagent_id: Option<String>,
    pub(super) parent_subagent_id: Option<String>,
    pub(super) turn_id: Option<String>,
    pub(super) continues_context: Option<bool>,
    pub(super) previous_identity: Option<String>,
}

impl NewStreamFacts {
    /// Columns of the facts read from an event's conversation, by its type,
    /// and whether the event names the response it continues; without the
    /// ids the client declared.
    pub(super) fn read(
        event_pk: String,
        event_type: &str,
        conversation: Option<&Value>,
        names_previous: bool,
    ) -> Result<Self, AppError> {
        if event_type == UsageEventType::Request.as_str() {
            Self::request(event_pk, &RequestFacts::read(conversation), names_previous)
        } else {
            Self::response(event_pk, &ResponseFacts::read(conversation))
        }
    }

    /// Columns of a request's facts.
    pub(super) fn request(
        event_pk: String,
        facts: &RequestFacts,
        names_previous: bool,
    ) -> Result<Self, AppError> {
        Ok(Self {
            event_pk,
            new_from: Some(facts.new_from),
            continues_context: Some(facts.new_from > 0_i32 || names_previous),
            starts_turn: Some(facts.starts_turn),
            last_identity: facts.last_identity.clone(),
            previous_identity: facts.previous_identity.clone(),
            trailing: Some(json_text(&facts.trailing, "trailing roles")?),
            result_ids: Some(json_text(&facts.result_ids, "result ids")?),
            label_index: facts.label.as_ref().map(|label| label.index),
            label_text: facts.label.as_ref().map(|label| label.text.clone()),
            label_length: facts.label.as_ref().map(|label| label.length),
            ..Self::default()
        })
    }

    /// Columns of a response's facts.
    pub(super) fn response(event_pk: String, facts: &ResponseFacts) -> Result<Self, AppError> {
        Ok(Self {
            event_pk,
            emitted_ids: Some(json_text(&facts.emitted_ids, "emitted ids")?),
            output_skeleton: Some(json_text(&facts.output_skeleton, "output skeleton")?),
            ..Self::default()
        })
    }
}

/// The columns of one session event that its streams read; the facts are
/// NULL for an event stored before they were read.
#[derive(Queryable)]
pub(super) struct StreamEventRow {
    pub(super) id: String,
    pub(super) event_id: String,
    pub(super) event_type: String,
    pub(super) observed_at: i64,
    pub(super) response_id: Option<String>,
    pub(super) previous_response_id: Option<String>,
    pub(super) new_from: Option<i32>,
    pub(super) starts_turn: Option<bool>,
    pub(super) last_identity: Option<String>,
    pub(super) previous_identity: Option<String>,
    pub(super) trailing: Option<String>,
    pub(super) result_ids: Option<String>,
    pub(super) emitted_ids: Option<String>,
    pub(super) output_skeleton: Option<String>,
    pub(super) label_index: Option<i32>,
    pub(super) label_text: Option<String>,
    pub(super) label_length: Option<i32>,
    pub(super) subagent_id: Option<String>,
    pub(super) parent_subagent_id: Option<String>,
    pub(super) turn_id: Option<String>,
}

impl StreamEventRow {
    pub(super) fn into_stream_event(self) -> Result<StreamEvent, AppError> {
        let facts = if self.event_type == "request" {
            EventFacts::Request(RequestFacts {
                new_from: self.new_from.unwrap_or_default(),
                starts_turn: self.starts_turn.unwrap_or_default(),
                last_identity: self.last_identity,
                previous_identity: self.previous_identity,
                trailing: json_list(self.trailing.as_deref(), "trailing roles")?,
                result_ids: json_list(self.result_ids.as_deref(), "result ids")?,
                label: Label::from_columns(self.label_index, self.label_text, self.label_length),
            })
        } else {
            EventFacts::Response(ResponseFacts {
                emitted_ids: json_list(self.emitted_ids.as_deref(), "emitted ids")?,
                output_skeleton: json_list(self.output_skeleton.as_deref(), "output skeleton")?,
            })
        };
        Ok(StreamEvent {
            pk: parse_uuid(&self.id, "event id")?,
            event_id: self.event_id,
            observed_at: timestamp_from_micros(self.observed_at)?,
            response_id: self.response_id,
            previous_response_id: self.previous_response_id,
            subagent_id: self.subagent_id,
            parent_subagent_id: self.parent_subagent_id,
            turn_id: self.turn_id,
            facts,
        })
    }
}

/// A stored JSON list; absent reads as empty.
fn json_list<T: DeserializeOwned + Default>(
    text: Option<&str>,
    field: &str,
) -> Result<T, AppError> {
    text.map_or_else(
        || Ok(T::default()),
        |text| {
            serde_json::from_str(text)
                .map_err(|error| AppError::internal(format!("stored {field} are invalid: {error}")))
        },
    )
}

fn json_text<T: Serialize>(value: &T, field: &str) -> Result<String, AppError> {
    serde_json::to_string(value)
        .map_err(|error| AppError::internal(format!("serialize {field}: {error}")))
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

/// A distinct text of one owner.
#[derive(Insertable)]
#[diesel(table_name = search_texts)]
pub(super) struct NewSearchText<'a> {
    pub(super) user_id: &'a str,
    pub(super) sha256: &'a [u8],
    pub(super) text: &'a str,
}

/// One piece of one event, referring to its text.
#[derive(Insertable)]
#[diesel(table_name = search_refs)]
pub(super) struct NewSearchRef<'a> {
    pub(super) event_pk: &'a str,
    pub(super) entry_index: i32,
    pub(super) field: &'static str,
    pub(super) text_id: i64,
}

pub(super) struct DeviceRecord {
    pub(super) id: Uuid,
    pub(super) host_name: String,
    pub(super) platform: String,
}

pub(super) struct EventRecord {
    pub(super) id: Uuid,
    pub(super) user_id: Uuid,
    pub(super) device_context_id: Uuid,
    pub(super) session_pk: Uuid,
    pub(super) event_id: String,
    pub(super) agent_name: String,
    pub(super) agent_version: Option<String>,
    pub(super) session_id: String,
    pub(super) llm_provider: String,
    pub(super) llm_model: String,
    pub(super) event_type: String,
    pub(super) input_tokens: i64,
    pub(super) output_tokens: i64,
    pub(super) cache_read_tokens: i64,
    pub(super) cache_write_tokens: i64,
    pub(super) reasoning_tokens: i64,
    pub(super) total_tokens: i64,
    pub(super) observed_at: DateTime<Utc>,
    pub(super) metadata: Value,
    pub(super) stream_id: Option<String>,
    pub(super) response_id: Option<String>,
    pub(super) previous_response_id: Option<String>,
    pub(super) conversation: Option<Value>,
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
        let _ = (
            &self.user_id,
            &self.os_version,
            self.created_at,
            self.updated_at,
        );
        Ok(DeviceRecord {
            id: parse_uuid(&self.id, "device id")?,
            host_name: self.host_name,
            platform: self.platform,
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
            event_id: self.event_id,
            agent_name: self.agent_name,
            agent_version: self.agent_version,
            session_id: self.session_id,
            llm_provider: self.llm_provider,
            llm_model: self.llm_model,
            event_type: self.event_type,
            input_tokens: self.input_tokens,
            output_tokens: self.output_tokens,
            cache_read_tokens: self.cache_read_tokens,
            cache_write_tokens: self.cache_write_tokens,
            reasoning_tokens: self.reasoning_tokens,
            total_tokens: self.total_tokens,
            observed_at: timestamp_from_micros(self.observed_at)?,
            metadata: parse_json(&self.metadata, "event metadata")?,
            stream_id: self.stream_id,
            response_id: self.response_id,
            previous_response_id: self.previous_response_id,
            conversation: self
                .conversation
                .as_deref()
                .map(|conversation| parse_json(conversation, "event conversation"))
                .transpose()?,
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
