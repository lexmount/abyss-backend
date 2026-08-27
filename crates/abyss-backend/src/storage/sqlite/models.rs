//! Backend-local row models decoded from SQLite's storage representations.

use chrono::{DateTime, Utc};
use serde_json::Value;
use uuid::Uuid;

use crate::usage::event_order::TimelineEvent;

pub(super) struct DeviceRecord {
    pub(super) id: Uuid,
    pub(super) host_name: String,
    pub(super) platform: String,
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
