//! Revision-checked, bounded reads of recorded session events, independent of storage.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::error::AppError;

/// Maximum raw UTF-8 bytes in a content chunk, before JSON transport escaping.
pub const MAX_CHUNK_BYTES: usize = 32 * 1024;

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
/// A directory page; subsequent pages should carry the first page's revision.
pub struct DirectoryQuery {
    pub offset: Option<usize>,
    pub limit: Option<usize>,
    pub revision: Option<String>,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
/// An initial event selection or an opaque continuation, never both.
pub struct ReadQuery {
    pub event_index: Option<usize>,
    pub cursor: Option<String>,
    pub revision: Option<String>,
    pub max_bytes: Option<usize>,
}

#[derive(Serialize)]
/// Body-free source identity in deterministic session order.
pub struct SourceEvent {
    pub event_pk: Uuid,
    pub event_id: String,
    pub observed_at: DateTime<Utc>,
    pub event_type: String,
}

#[derive(Serialize)]
/// A bounded directory page, using absolute zero-based event indexes.
pub struct DirectoryPage {
    pub session_pk: Uuid,
    pub revision: String,
    pub total_events: usize,
    pub offset: usize,
    pub events: Vec<SourceEvent>,
    pub next_offset: Option<usize>,
}

#[derive(Serialize)]
/// One lossless slice of serialized event JSON. Concatenate slices of an event
/// before parsing JSON; `session_complete` refers only to recorded scope.
pub struct ReadPage {
    pub session_pk: Uuid,
    pub revision: String,
    pub total_events: usize,
    pub event_index: usize,
    pub source: SourceEvent,
    pub content_sha256: String,
    pub content_bytes: usize,
    pub byte_offset: usize,
    pub next_byte_offset: usize,
    pub content_json: String,
    pub event_complete: bool,
    pub session_complete: bool,
    pub next_cursor: Option<String>,
    pub capture: CaptureStatus,
}

#[derive(Serialize)]
/// Collector claims, kept separate from whether the reader reached the end.
pub struct CaptureStatus {
    pub conversation_present: bool,
    pub completeness: Option<String>,
    pub omitted: Value,
    pub attachment_count: usize,
    pub scope: &'static str,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    version: u8,
    session_pk: Uuid,
    revision: String,
    event_index: usize,
    byte_offset: usize,
    content_sha256: Option<String>,
}

/// The event index belongs to one owner-authorized database read snapshot.
pub struct ReadingIndex {
    session_pk: Uuid,
    revision: String,
    events: Vec<SourceEvent>,
}

/// Validated position; the selected event must be loaded in the index snapshot.
pub struct ReadPosition {
    cursor: Cursor,
    max_bytes: usize,
}

impl ReadPosition {
    pub const fn event_index(&self) -> usize {
        self.cursor.event_index
    }
}

impl ReadingIndex {
    /// Rows must be sorted by `(observed_at, event_type, event_pk)` by storage.
    pub fn new(session_pk: Uuid, events: Vec<SourceEvent>) -> Result<Self, AppError> {
        if events.is_empty() {
            return Err(AppError::not_found(format!(
                "session not found: {session_pk}"
            )));
        }
        let mut digest = Sha256::new();
        digest.update(b"abyss-recorded-session-v1");
        digest.update(session_pk.as_bytes());
        for event in &events {
            digest.update(event.event_pk.as_bytes());
        }
        Ok(Self {
            session_pk,
            revision: hex::encode(digest.finalize()),
            events,
        })
    }

    fn check_revision(&self, revision: Option<&str>) -> Result<(), AppError> {
        if revision.is_some_and(|value| value != self.revision) {
            return Err(invalid(
                "session_changed: reload the directory and restart the read",
            ));
        }
        Ok(())
    }

    pub fn directory(mut self, query: &DirectoryQuery) -> Result<DirectoryPage, AppError> {
        self.check_revision(query.revision.as_deref())?;
        let limit = query.limit.unwrap_or(50);
        let offset = query.offset.unwrap_or(0);
        let total_events = self.events.len();
        if !(1..=100).contains(&limit) || offset > total_events {
            return Err(invalid(
                "limit must be 1..100 and offset must be within the directory",
            ));
        }
        let end = offset.saturating_add(limit).min(total_events);
        let events = self.events.drain(offset..end).collect();
        Ok(DirectoryPage {
            session_pk: self.session_pk,
            revision: self.revision,
            total_events,
            offset,
            events,
            next_offset: (end < total_events).then_some(end),
        })
    }

    pub fn position(&self, query: &ReadQuery) -> Result<ReadPosition, AppError> {
        self.check_revision(query.revision.as_deref())?;
        let max_bytes = query.max_bytes.unwrap_or(MAX_CHUNK_BYTES);
        if !(4..=MAX_CHUNK_BYTES).contains(&max_bytes) {
            return Err(invalid("max_bytes must be between 4 and 32768"));
        }
        let cursor = if let Some(encoded) = &query.cursor {
            if query.event_index.is_some() || encoded.len() > 2048 {
                return Err(invalid(
                    "cursor cannot be combined with event_index and must be bounded",
                ));
            }
            let bytes = URL_SAFE_NO_PAD
                .decode(encoded)
                .map_err(|_| invalid("invalid read cursor"))?;
            let cursor: Cursor =
                serde_json::from_slice(&bytes).map_err(|_| invalid("invalid read cursor"))?;
            if cursor.version != 1 || cursor.session_pk != self.session_pk {
                return Err(invalid("cursor belongs to a different session or version"));
            }
            self.check_revision(Some(&cursor.revision))?;
            if cursor.byte_offset > 0 && cursor.content_sha256.is_none() {
                return Err(invalid("continuation requires a content digest"));
            }
            cursor
        } else {
            Cursor {
                version: 1,
                session_pk: self.session_pk,
                revision: self.revision.clone(),
                event_index: query.event_index.unwrap_or(0),
                byte_offset: 0,
                content_sha256: None,
            }
        };
        if cursor.event_index >= self.events.len() {
            return Err(invalid("event_index is outside the recorded session"));
        }
        Ok(ReadPosition { cursor, max_bytes })
    }

    pub fn event_pk(&self, position: &ReadPosition) -> Uuid {
        self.events[position.event_index()].event_pk
    }

    /// Emits one chunk after the adapter has loaded the selected event.
    pub fn read(mut self, position: ReadPosition, event: &Value) -> Result<ReadPage, AppError> {
        let content =
            serde_json::to_string(event).map_err(|error| AppError::internal(error.to_string()))?;
        let content_sha256 = hex::encode(Sha256::digest(content.as_bytes()));
        let cursor = position.cursor;
        if cursor
            .content_sha256
            .as_ref()
            .is_some_and(|digest| digest != &content_sha256)
        {
            return Err(invalid(
                "content_changed: restart this event from its directory index",
            ));
        }
        let start = cursor.byte_offset;
        if start >= content.len() || !content.is_char_boundary(start) {
            return Err(invalid("cursor offset is not a valid UTF-8 position"));
        }
        let mut end = start.saturating_add(position.max_bytes).min(content.len());
        while !content.is_char_boundary(end) {
            end = end.saturating_sub(1);
        }
        let total_events = self.events.len();
        let event_complete = end == content.len();
        let next_event = cursor.event_index.saturating_add(1);
        let session_complete = event_complete && next_event == total_events;
        let next_cursor = if session_complete {
            None
        } else {
            let next = Cursor {
                version: 1,
                session_pk: self.session_pk,
                revision: self.revision.clone(),
                event_index: if event_complete {
                    next_event
                } else {
                    cursor.event_index
                },
                byte_offset: if event_complete { 0 } else { end },
                content_sha256: (!event_complete).then(|| content_sha256.clone()),
            };
            Some(URL_SAFE_NO_PAD.encode(
                serde_json::to_vec(&next).map_err(|error| AppError::internal(error.to_string()))?,
            ))
        };
        let conversation = event.get("conversation").filter(|value| !value.is_null());
        let capture = CaptureStatus {
            conversation_present: conversation.is_some(),
            completeness: conversation
                .and_then(|value| value.get("completeness"))
                .and_then(Value::as_str)
                .map(str::to_owned),
            omitted: event.get("omitted").cloned().unwrap_or(Value::Null),
            attachment_count: event
                .get("attachments")
                .and_then(Value::as_array)
                .map_or(0, Vec::len),
            scope: "recorded_events_only",
        };
        Ok(ReadPage {
            session_pk: self.session_pk,
            revision: self.revision,
            total_events,
            event_index: cursor.event_index,
            source: self.events.remove(cursor.event_index),
            content_sha256,
            content_bytes: content.len(),
            byte_offset: start,
            next_byte_offset: end,
            content_json: content[start..end].to_owned(),
            event_complete,
            session_complete,
            next_cursor,
            capture,
        })
    }
}

fn invalid(message: &str) -> AppError {
    AppError::validation(message.to_owned())
}

#[cfg(test)]
mod tests;
