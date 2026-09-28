//! Merged session trajectory built from accumulated request/response snapshots.
//!
//! Collectors record each provider request verbatim, so every request repeats
//! the conversation so far. The trajectory keeps only what each event adds:
//! new request paragraphs, first-seen tool results, and response text and tool
//! calls, with each tool call paired to its result by call identifier.

use std::{
    collections::{HashMap, HashSet, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
};

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

use super::{SessionInfo, SessionTimelineResponse, UsageEventResponse};

/// Collectors join request fragments with this separator.
const PARAGRAPH_SEPARATOR: &str = "\n\n";
/// Collectors prefix the system prompt fragment with this marker.
const SYSTEM_PREFIX: &str = "system: ";
/// Shared prefix length above which a changed paragraph counts as grown.
const GROWN_PARAGRAPH_MIN_PREFIX_BYTES: usize = 200;

#[derive(Debug, Serialize)]
/// Session information and its merged, de-duplicated trajectory items.
pub struct SessionTrajectoryResponse {
    /// Authoritative session information.
    pub session: SessionInfo,
    /// Items in authoritative event order.
    pub items: Vec<TrajectoryItem>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
/// Kind of one trajectory item.
pub enum TrajectoryItemKind {
    /// First request content of a stream, starting with its system prompt.
    Context,
    /// Request paragraphs not seen earlier in the session.
    User,
    /// Response text.
    Assistant,
    /// Response tool call, with its result once one arrives.
    ToolCall,
    /// Request tool result whose call was not observed.
    ToolResult,
}

#[derive(Debug, Serialize)]
/// One piece of new content contributed by one event.
pub struct TrajectoryItem {
    /// Item kind.
    pub kind: TrajectoryItemKind,
    /// Zero-based request stream, keyed by system prompt. Side requests such
    /// as classifiers or subagents form their own streams.
    pub stream: usize,
    /// Backend-normalized turn number.
    pub turn_index: i32,
    /// Event that contributed this item.
    pub event_pk: Uuid,
    /// Observation time of that event.
    pub observed_at: DateTime<Utc>,
    /// Model of that event.
    pub llm_model: String,
    /// Text of context, user, and assistant items.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// Whether `text` is only the new tail of a paragraph that grew.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub continued: bool,
    /// Tool name of a tool call.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Provider call identifier of a tool call or result.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub call_id: Option<String>,
    /// Tool call input.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input: Option<String>,
    /// Tool result output.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    /// Event that first carried the tool result.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_event_pk: Option<Uuid>,
}

impl TrajectoryItem {
    fn new(kind: TrajectoryItemKind, stream: usize, event: &UsageEventResponse) -> Self {
        Self {
            kind,
            stream,
            turn_index: event.turn_index,
            event_pk: event.id,
            observed_at: event.observed_at,
            llm_model: event.llm_model.clone(),
            text: None,
            continued: false,
            name: None,
            call_id: None,
            input: None,
            output: None,
            output_event_pk: None,
        }
    }
}

/// Merges an ordered session timeline into its trajectory.
pub fn build(timeline: SessionTimelineResponse) -> SessionTrajectoryResponse {
    let mut merger = Merger::default();
    for turn in &timeline.turns {
        for event in &turn.events {
            if event.event_type == "request" {
                merger.request(event);
            } else {
                merger.response(event);
            }
        }
    }
    SessionTrajectoryResponse {
        session: timeline.session,
        items: merger.items,
    }
}

#[derive(Default)]
struct Merger {
    items: Vec<TrajectoryItem>,
    seen_paragraphs: HashSet<u64>,
    seen_results: HashSet<String>,
    streams: HashMap<u64, usize>,
    last_paragraphs: HashMap<usize, Vec<String>>,
    /// Tool call item index by call identifier.
    calls: HashMap<String, usize>,
    /// Stream of the most recent request; responses belong to it.
    current_stream: usize,
}

impl Merger {
    fn request(&mut self, event: &UsageEventResponse) {
        let text = event.text.as_deref().unwrap_or_default();
        let paragraphs = text.split(PARAGRAPH_SEPARATOR).collect::<Vec<_>>();
        let first = paragraphs.first().copied().unwrap_or_default();
        let stream_key = if first.starts_with(SYSTEM_PREFIX) {
            paragraph_key(0, first)
        } else {
            0
        };
        let next_stream = self.streams.len();
        let stream = *self.streams.entry(stream_key).or_insert(next_stream);
        self.current_stream = stream;

        let previous = self.last_paragraphs.remove(&stream).unwrap_or_default();
        let mut added = Vec::new();
        let mut continued = false;
        for (index, paragraph) in paragraphs.iter().enumerate() {
            if paragraph.trim().is_empty()
                || !self.seen_paragraphs.insert(paragraph_key(index, paragraph))
            {
                continue;
            }
            // A paragraph that grew since the stream's previous request, such
            // as an embedded transcript, contributes only its new tail.
            let shared = previous
                .iter()
                .map(|earlier| common_prefix_len(paragraph, earlier))
                .max()
                .unwrap_or(0);
            if shared >= GROWN_PARAGRAPH_MIN_PREFIX_BYTES {
                added.push(&paragraph[shared..]);
                continued = true;
            } else {
                added.push(paragraph);
            }
        }
        if !added.is_empty() {
            let kind = if added[0].starts_with(SYSTEM_PREFIX) {
                TrajectoryItemKind::Context
            } else {
                TrajectoryItemKind::User
            };
            let mut item = TrajectoryItem::new(kind, stream, event);
            item.text = Some(added.join(PARAGRAPH_SEPARATOR));
            item.continued = continued;
            self.items.push(item);
        }
        self.last_paragraphs
            .insert(stream, paragraphs.into_iter().map(str::to_owned).collect());

        for segment in content_segments(&event.metadata) {
            if segment_str(segment, "type") != Some("tool_result") {
                continue;
            }
            let call_id = segment_str(segment, "call_id").map(str::to_owned);
            let output = segment_str(segment, "output").map(str::to_owned);
            let result_key = call_id.clone().unwrap_or_else(|| {
                format!(
                    "output:{:x}",
                    hash_str(output.as_deref().unwrap_or_default())
                )
            });
            if !self.seen_results.insert(result_key) {
                continue;
            }
            if let Some(&index) = call_id.as_ref().and_then(|id| self.calls.get(id)) {
                let call = &mut self.items[index];
                call.output = output;
                call.output_event_pk = Some(event.id);
            } else {
                let mut item = TrajectoryItem::new(TrajectoryItemKind::ToolResult, stream, event);
                item.call_id = call_id;
                item.output = output;
                item.output_event_pk = Some(event.id);
                self.items.push(item);
            }
        }
    }

    fn response(&mut self, event: &UsageEventResponse) {
        let stream = self.current_stream;
        if let Some(text) = event.text.as_deref().filter(|text| !text.trim().is_empty()) {
            let mut item = TrajectoryItem::new(TrajectoryItemKind::Assistant, stream, event);
            item.text = Some(text.to_owned());
            self.items.push(item);
        }
        for segment in content_segments(&event.metadata) {
            if segment_str(segment, "type") != Some("tool_call") {
                continue;
            }
            let mut item = TrajectoryItem::new(TrajectoryItemKind::ToolCall, stream, event);
            item.name = segment_str(segment, "name").map(str::to_owned);
            item.call_id = segment_str(segment, "call_id").map(str::to_owned);
            item.input = segment_str(segment, "input").map(str::to_owned);
            if let Some(call_id) = &item.call_id {
                self.calls.insert(call_id.clone(), self.items.len());
            }
            self.items.push(item);
        }
    }
}

/// Identity of a request paragraph. The system fragment's first line carries
/// per-request noise, such as a billing header, so it is excluded.
fn paragraph_key(index: usize, paragraph: &str) -> u64 {
    if index == 0 && paragraph.starts_with(SYSTEM_PREFIX) {
        return hash_str(paragraph.split_once('\n').map_or("", |(_, rest)| rest));
    }
    hash_str(paragraph)
}

fn hash_str(value: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

/// Length in bytes of the shared prefix, ending on a character boundary.
fn common_prefix_len(left: &str, right: &str) -> usize {
    let mut len = left
        .bytes()
        .zip(right.bytes())
        .take_while(|(a, b)| a == b)
        .count();
    while !left.is_char_boundary(len) {
        len = len.saturating_sub(1);
    }
    len
}

fn content_segments(metadata: &Value) -> &[Value] {
    metadata
        .get("content_segments")
        .and_then(Value::as_array)
        .map_or(&[], Vec::as_slice)
}

fn segment_str<'a>(segment: &'a Value, key: &str) -> Option<&'a str> {
    segment.get(key).and_then(Value::as_str)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::usage::TurnTimeline;

    #[test]
    fn keeps_only_new_content_and_pairs_tool_results() {
        let system = "system: header cch=1\nYou are an agent.";
        let events = vec![
            event("request", &format!("{system}\n\nfirst prompt"), json!({})),
            event(
                "response",
                "",
                json!({"content_segments": [
                    {"type": "tool_call", "call_id": "c1", "name": "Bash", "input": "ls"}
                ]}),
            ),
            event(
                "request",
                "system: header cch=2\nYou are an agent.\n\nfirst prompt",
                json!({"content_segments": [
                    {"type": "tool_result", "call_id": "c1", "output": "README.md"}
                ]}),
            ),
            event("response", "done", json!({})),
            event(
                "request",
                "system: header cch=3\nYou are an agent.\n\nfirst prompt\n\nsecond prompt",
                json!({"content_segments": [
                    {"type": "tool_result", "call_id": "c1", "output": "README.md"}
                ]}),
            ),
        ];

        let items = build(timeline(events)).items;

        let kinds = items.iter().map(|item| item.kind).collect::<Vec<_>>();
        assert_eq!(
            kinds,
            [
                TrajectoryItemKind::Context,
                TrajectoryItemKind::ToolCall,
                TrajectoryItemKind::Assistant,
                TrajectoryItemKind::User,
            ]
        );
        assert_eq!(items[1].output.as_deref(), Some("README.md"));
        assert_eq!(items[3].text.as_deref(), Some("second prompt"));
        assert!(items.iter().all(|item| item.stream == 0));
    }

    #[test]
    fn grown_paragraphs_keep_their_tail_and_side_requests_form_streams() {
        let transcript = "x".repeat(300);
        let events = vec![
            event("request", "system: main\nMain agent.\n\nhello", json!({})),
            event(
                "request",
                &format!("system: classifier\nJudge.\n\n<transcript>{transcript}"),
                json!({}),
            ),
            event(
                "request",
                &format!("system: classifier\nJudge.\n\n<transcript>{transcript} more"),
                json!({}),
            ),
        ];

        let items = build(timeline(events)).items;

        assert_eq!(items.len(), 3);
        assert_eq!(items[1].stream, 1);
        assert_eq!(items[2].stream, 1);
        assert_eq!(items[2].text.as_deref(), Some(" more"));
        assert!(items[2].continued);
    }

    #[test]
    fn common_prefix_stops_on_character_boundaries() {
        assert_eq!(common_prefix_len("中文a", "中字b"), 3);
    }

    fn timeline(events: Vec<UsageEventResponse>) -> SessionTimelineResponse {
        let now = Utc::now();
        SessionTimelineResponse {
            session: SessionInfo {
                session_pk: Uuid::nil(),
                user_id: Uuid::nil(),
                device_context_id: Uuid::nil(),
                agent_name: "codex".to_owned(),
                agent_version: None,
                session_id: "session".to_owned(),
                started_at: now,
                ended_at: None,
                metadata: json!({}),
            },
            turns: vec![TurnTimeline {
                turn_pk: Uuid::nil(),
                turn_index: 1,
                started_at: now,
                ended_at: None,
                events,
            }],
        }
    }

    fn event(event_type: &str, text: &str, metadata: Value) -> UsageEventResponse {
        UsageEventResponse {
            id: Uuid::now_v7(),
            event_id: Uuid::now_v7().to_string(),
            user_id: Uuid::nil(),
            device_context_id: Uuid::nil(),
            host_name: None,
            platform: None,
            session_pk: Uuid::nil(),
            turn_pk: Uuid::nil(),
            agent_name: "codex".to_owned(),
            agent_version: None,
            session_id: "session".to_owned(),
            turn_index: 1,
            llm_provider: "openai".to_owned(),
            llm_model: "gpt-5".to_owned(),
            event_type: event_type.to_owned(),
            text: (!text.is_empty()).then(|| text.to_owned()),
            text_sha256: None,
            input_tokens: 0,
            output_tokens: 0,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
            reasoning_tokens: 0,
            total_tokens: 0,
            observed_at: Utc::now(),
            metadata,
            attachments: Vec::new(),
        }
    }
}
