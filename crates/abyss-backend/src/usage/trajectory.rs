//! Merged session trajectory built from accumulated request/response snapshots.
//!
//! Collectors record each provider request verbatim, so every request repeats
//! the conversation so far. The trajectory keeps only what each event adds:
//! role-aware messages, first-seen tool results, and response text and tool
//! calls, with each tool call paired to its result by call identifier.

mod conversation;

use conversation::ConversationProjection;

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
/// Codex opens the request that asks the model for a context handoff summary
/// with this prompt when it compacts its context.
const COMPACTION_PROMPT: &str = "You are performing a CONTEXT CHECKPOINT COMPACTION.";
/// Codex opens the compacted history it sends afterwards with this sentence,
/// followed by the handoff summary.
const COMPACTION_HANDOFF: &str = "Another language model started to solve this problem and \
                                  produced a summary of its thinking process.";

#[derive(Debug, Serialize)]
/// Session information and its merged, de-duplicated trajectory items.
pub struct SessionTrajectoryResponse {
    /// Authoritative session information.
    pub session: SessionInfo,
    /// Items in authoritative event order.
    pub items: Vec<TrajectoryItem>,
    /// Every observed request and response, including events with no new content.
    pub events: Vec<TrajectoryEvent>,
}

#[derive(Debug, Serialize)]
/// Lightweight usage summary independent of content de-duplication.
pub struct TrajectoryEvent {
    /// Event that owns these counters.
    pub event_pk: Uuid,
    /// Backend-normalized turn number.
    pub turn_index: i32,
    /// Canonical request or response value.
    pub event_type: String,
    /// Observation time of the event.
    pub observed_at: DateTime<Utc>,
    /// Model requested or returned by the provider.
    pub llm_model: String,
    /// Uncached input tokens; cache counters are separate.
    pub input_tokens: i64,
    /// Output tokens, including reasoning.
    pub output_tokens: i64,
    /// Input tokens read from a provider cache.
    pub cache_read_tokens: i64,
    /// Input tokens written into a provider cache.
    pub cache_write_tokens: i64,
    /// Reasoning breakdown of output tokens.
    pub reasoning_tokens: i64,
    /// Authoritative total for this event, counted once.
    pub total_tokens: i64,
    /// Whether counters were reported or estimated rather than absent or hidden.
    pub token_usage_available: bool,
    /// Whether the collector estimated token usage.
    pub token_usage_estimated: bool,
}

impl From<&UsageEventResponse> for TrajectoryEvent {
    fn from(event: &UsageEventResponse) -> Self {
        let source = event
            .metadata
            .get("token_usage_source")
            .and_then(Value::as_str);
        let hidden =
            event.metadata.pointer("/content_policy/token_usage") == Some(&Value::Bool(false));
        let has_usage = event.input_tokens != 0
            || event.output_tokens != 0
            || event.cache_read_tokens != 0
            || event.cache_write_tokens != 0
            || event.reasoning_tokens != 0
            || event.total_tokens != 0;
        Self {
            event_pk: event.id,
            turn_index: event.turn_index,
            event_type: event.event_type.clone(),
            observed_at: event.observed_at,
            llm_model: event.llm_model.clone(),
            input_tokens: event.input_tokens,
            output_tokens: event.output_tokens,
            cache_read_tokens: event.cache_read_tokens,
            cache_write_tokens: event.cache_write_tokens,
            reasoning_tokens: event.reasoning_tokens,
            total_tokens: event.total_tokens,
            token_usage_available: !hidden
                && source != Some("absent")
                && (has_usage || source.is_some()),
            token_usage_estimated: event.metadata.get("token_usage_estimated")
                == Some(&Value::Bool(true)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
/// Kind of one trajectory item.
pub enum TrajectoryItemKind {
    /// Request content whose message role is unavailable.
    Request,
    /// Explicit system or developer message.
    Context,
    /// Explicit user message not seen earlier in the session.
    User,
    /// Response text.
    Assistant,
    /// Response tool call, with its result once one arrives.
    ToolCall,
    /// Request tool result whose call was not observed.
    ToolResult,
    /// Context compaction: the summary prompt as `input` and the handoff
    /// summary that replaced the earlier history as `output`.
    Compaction,
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

    /// An empty item of another kind contributed by the same event.
    fn sibling(&self, kind: TrajectoryItemKind) -> Self {
        Self {
            kind,
            stream: self.stream,
            turn_index: self.turn_index,
            event_pk: self.event_pk,
            observed_at: self.observed_at,
            llm_model: self.llm_model.clone(),
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
    let mut events = Vec::new();
    for turn in &timeline.turns {
        for event in &turn.events {
            events.push(TrajectoryEvent::from(event));
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
        events,
    }
}

#[derive(Default)]
struct Merger {
    conversation: ConversationProjection,
    items: Vec<TrajectoryItem>,
    seen_results: HashSet<String>,
    streams: HashMap<u64, usize>,
    /// Tool call item index by call identifier.
    calls: HashMap<String, usize>,
    /// Stream of the most recent request; responses belong to it.
    current_stream: usize,
    /// Compaction item whose summary the next response carries.
    pending_compaction: Option<usize>,
    /// Handoff summaries already shown, so their replay is not shown again.
    summaries: Vec<String>,
}

impl Merger {
    fn request(&mut self, event: &UsageEventResponse) {
        self.pending_compaction = None;
        let first_new = self.items.len();
        if let Some(items) =
            self.conversation
                .project(event, &mut self.current_stream, &mut self.streams)
        {
            self.items.extend(items);
        } else {
            self.unclassified_request(event);
        }
        self.mark_compaction(first_new);
        self.request_tools(event);
    }

    fn unclassified_request(&mut self, event: &UsageEventResponse) {
        if let Some(text) = event.text.as_ref().filter(|text| !text.is_empty()) {
            let mut item =
                TrajectoryItem::new(TrajectoryItemKind::Request, self.current_stream, event);
            item.text = Some(text.clone());
            self.items.push(item);
        }
    }

    fn request_tools(&mut self, event: &UsageEventResponse) {
        let stream = self.current_stream;

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

    /// Turns the compaction prompt into a compaction item and hides the
    /// handoff summary when the compacted history replays it.
    fn mark_compaction(&mut self, first_new: usize) {
        for mut item in self.items.split_off(first_new) {
            let Some(text) = item
                .text
                .clone()
                .filter(|_| item.kind == TrajectoryItemKind::User)
            else {
                self.items.push(item);
                continue;
            };
            // Legacy projections join all new paragraphs of a request, so the
            // prompt may follow other new input in the same item.
            if let Some(start) = paragraph_start(&text, COMPACTION_PROMPT) {
                let (before, prompt) = text.split_at(start);
                let mut compaction = item.sibling(TrajectoryItemKind::Compaction);
                compaction.input = Some(prompt.trim().to_owned());
                let before = before.trim_end();
                if !before.is_empty() {
                    item.text = Some(before.to_owned());
                    self.items.push(item);
                }
                self.pending_compaction = Some(self.items.len());
                self.items.push(compaction);
                continue;
            }
            let Some(start) = paragraph_start(&text, COMPACTION_HANDOFF) else {
                self.items.push(item);
                continue;
            };
            let (before, handoff) = text.split_at(start);
            let before = before.trim_end();
            let replayed = self
                .summaries
                .iter()
                .any(|summary| handoff.trim_end().ends_with(summary.as_str()));
            // Without the summary request, the handoff is the only record of
            // the compaction.
            let recovered = (!replayed).then(|| {
                let summary = handoff
                    .split_once('\n')
                    .map_or(handoff, |(_, rest)| rest)
                    .trim()
                    .to_owned();
                let mut compaction = item.sibling(TrajectoryItemKind::Compaction);
                compaction.output = Some(summary.clone());
                compaction.output_event_pk = Some(item.event_pk);
                self.summaries.push(summary);
                compaction
            });
            if !before.is_empty() {
                item.text = Some(before.to_owned());
                self.items.push(item);
            }
            self.items.extend(recovered);
        }
    }

    fn response(&mut self, event: &UsageEventResponse) {
        let stream = self.current_stream;
        let projected =
            self.conversation
                .project(event, &mut self.current_stream, &mut self.streams);
        if let Some(index) = self.pending_compaction.take() {
            // The response to the compaction prompt is the handoff summary.
            if let Some(summary) = event.text.as_deref().map(str::trim)
                && !summary.is_empty()
                && let Some(item) = self.items.get_mut(index)
            {
                item.output = Some(summary.to_owned());
                item.output_event_pk = Some(event.id);
                self.summaries.push(summary.to_owned());
            }
        } else if let Some(items) = projected {
            self.items.extend(items);
        } else if let Some(text) = event.text.as_deref().filter(|text| !text.trim().is_empty()) {
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

/// Byte offset of the paragraph that contains `marker`.
fn paragraph_start(text: &str, marker: &str) -> Option<usize> {
    let position = text.find(marker)?;
    Some(
        text[..position]
            .rfind(PARAGRAPH_SEPARATOR)
            .map_or(0, |separator| {
                separator.saturating_add(PARAGRAPH_SEPARATOR.len())
            }),
    )
}

fn hash_str(value: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
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
    #[test]
    fn usage_retains_events_without_new_content_and_counts_each_event_once() {
        let body = r#"{"messages":[{"id":null,"role":"user","text":"hello"}],"instructions_present":false,"previous_response_id":null}"#;
        let mut request = structured_event("request", body);
        request.input_tokens = 2;
        request.cache_read_tokens = 77_000;
        request.cache_write_tokens = 400;
        request.total_tokens = 77_402;
        let repeated = structured_event("request", body);
        let mut response = event("response", "", json!({"token_usage_estimated": true}));
        response.llm_model = "other-model".to_owned();
        response.output_tokens = 150;
        response.reasoning_tokens = 40;
        response.total_tokens = 150;
        let expected_ids = vec![request.id, repeated.id, response.id];

        let trajectory = build(timeline(vec![request, repeated, response]));
        assert_eq!(trajectory.items.len(), 1);
        assert_eq!(
            trajectory
                .events
                .iter()
                .map(|event| event.event_pk)
                .collect::<Vec<_>>(),
            expected_ids
        );
        assert_eq!(trajectory.events[0].total_tokens, 77_402);
        assert_eq!(trajectory.events[0].cache_read_tokens, 77_000);
        assert_eq!(trajectory.events[0].cache_write_tokens, 400);
        assert!(trajectory.events[0].token_usage_available);
        assert!(!trajectory.events[1].token_usage_available);
        assert_eq!(trajectory.events[2].llm_model, "other-model");
        assert_eq!(trajectory.events[2].output_tokens, 150);
        assert_eq!(trajectory.events[2].reasoning_tokens, 40);
        assert!(trajectory.events[2].token_usage_estimated);
        let serialized = serde_json::to_value(&trajectory.events).unwrap();
        assert!(serialized[0].get("text").is_none());
        assert!(serialized[0].get("metadata").is_none());
    }

    #[test]
    fn usage_distinguishes_reported_zero_from_absent_and_hidden_counts() {
        let reported = event(
            "response",
            "",
            json!({"token_usage_source": "provider_reported"}),
        );
        let absent = event("request", "", json!({"token_usage_source": "absent"}));
        let mut hidden = event(
            "request",
            "",
            json!({"content_policy": {"token_usage": false}}),
        );
        hidden.input_tokens = 10;
        hidden.total_tokens = 10;
        let trajectory = build(timeline(vec![reported, absent, hidden]));
        assert_eq!(trajectory.events.len(), 3);
        assert!(trajectory.events[0].token_usage_available);
        assert!(!trajectory.events[1].token_usage_available);
        assert!(!trajectory.events[2].token_usage_available);
    }

    use serde_json::json;

    use super::*;
    use crate::usage::TurnTimeline;

    #[test]
    fn unclassified_requests_keep_every_observation_and_pair_tool_results() {
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
                TrajectoryItemKind::Request,
                TrajectoryItemKind::ToolCall,
                TrajectoryItemKind::Request,
                TrajectoryItemKind::Assistant,
                TrajectoryItemKind::Request,
            ]
        );
        assert_eq!(items[1].output.as_deref(), Some("README.md"));
        assert_eq!(
            items[4].text.as_deref(),
            Some("system: header cch=3\nYou are an agent.\n\nfirst prompt\n\nsecond prompt")
        );
        assert!(items.iter().all(|item| item.stream == 0));
    }

    #[test]
    fn unclassified_requests_are_not_split_trimmed_or_grouped_by_text() {
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
        assert!(
            items
                .iter()
                .all(|i| i.kind == TrajectoryItemKind::Request && !i.continued)
        );
        assert_eq!(
            items[2].text,
            Some(format!(
                "system: classifier\nJudge.\n\n<transcript>{transcript} more"
            ))
        );
    }

    fn user_request(texts: &[&str]) -> UsageEventResponse {
        structured_event("request", &json!({"messages": texts.iter().map(|text| json!({"role":"user", "text":text})).collect::<Vec<_>>()}).to_string())
    }

    #[test]
    fn unavailable_roles_never_become_system_or_user_by_text_guessing() {
        let text = "system: instructions\n\n用户内容\n\n用户内容";
        for metadata in [
            json!({}),
            json!({"conversation":{"completeness":"partial","messages":[]}}),
            json!({"conversation":{"messages":"invalid"}}),
            json!({"conversation":{"messages":[],"completeness":"complete"}}),
            // Older collectors stored messages without completeness.
            json!({"conversation":{"messages":[{"role":"user","text":"用户内容"}]}}),
        ] {
            let items = build(timeline(vec![event("request", text, metadata)])).items;
            assert_eq!(items.len(), 1);
            assert_eq!(items[0].kind, TrajectoryItemKind::Request);
            assert_eq!(items[0].text.as_deref(), Some(text));
            assert!(!items[0].continued);
        }
        let items = build(timeline(vec![structured_event("request", r#"{"messages":[{"id":null,"role":"unknown","text":"system: unknown"},{"id":null,"role":"unrecognized","text":"hello"},{"id":null,"role":"user","text":"system: quoted text"}],"instructions_present":false,"previous_response_id":null}"#)])).items;
        assert_eq!(
            items.iter().map(|i| i.kind).collect::<Vec<_>>(),
            [
                TrajectoryItemKind::Request,
                TrajectoryItemKind::Request,
                TrajectoryItemKind::User
            ]
        );
    }

    /// A current collector always declares completeness; default it to complete.
    fn structured_event(side: &str, body: &str) -> UsageEventResponse {
        let mut conversation = serde_json::from_str::<Value>(body).unwrap();
        conversation
            .as_object_mut()
            .unwrap()
            .entry("completeness")
            .or_insert_with(|| json!("complete"));
        event(
            side,
            "legacy fallback",
            json!({"conversation": conversation}),
        )
    }

    #[test]
    fn structured_projection_keeps_roles_and_repeated_user_occurrences() {
        let first = r#"{"messages":[{"role":"developer","text":"system instructions"},{"id":null,"role":"user","text":"继续"}],"instructions_present":true,"previous_response_id":null}"#;
        let response = r#"{"messages":[{"id":"a1","role":"assistant","text":"working"}],"instructions_present":false,"previous_response_id":null}"#;
        let replay = r#"{"messages":[{"role":"developer","text":"system instructions"},{"id":null,"role":"user","text":"继续"},{"id":"a1","role":"assistant","text":"working"},{"id":null,"role":"user","text":"继续"}],"instructions_present":true,"previous_response_id":null}"#;
        let events = vec![
            structured_event("request", first),
            structured_event("response", response),
            structured_event("request", replay),
            structured_event("request", replay),
        ];
        let metadata_before = events
            .iter()
            .map(|e| e.metadata.clone())
            .collect::<Vec<_>>();
        let mut merger = Merger::default();
        for e in &events {
            if e.event_type == "request" {
                merger.request(e);
            } else {
                merger.response(e);
            }
        }
        assert_eq!(
            merger.items.iter().map(|i| i.kind).collect::<Vec<_>>(),
            [
                TrajectoryItemKind::Context,
                TrajectoryItemKind::User,
                TrajectoryItemKind::Assistant,
                TrajectoryItemKind::User
            ]
        );
        assert_eq!(merger.items[1].text.as_deref(), Some("继续"));
        assert_eq!(merger.items[3].text.as_deref(), Some("继续"));
        assert_eq!(
            metadata_before,
            events
                .iter()
                .map(|e| e.metadata.clone())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn streamed_anthropic_response_is_not_replayed_as_user_input() {
        let response = r#"{"messages":[{"role":"assistant","text":"hi"}]}"#;
        let items = build(timeline(vec![
            structured_event("request", r#"{"messages":[{"role":"system","text":"instructions"},{"id":null,"role":"user","text":"hello"}],"instructions_present":true,"previous_response_id":null}"#),
            structured_event("response", response),
            structured_event("request", r#"{"messages":[{"role":"system","text":"instructions"},{"id":null,"role":"user","text":"hello"},{"id":null,"role":"assistant","text":"hi"},{"id":null,"role":"user","text":"hello"}],"instructions_present":true,"previous_response_id":null}"#),
        ])).items;
        assert_eq!(
            items.iter().map(|i| i.kind).collect::<Vec<_>>(),
            [
                TrajectoryItemKind::Context,
                TrajectoryItemKind::User,
                TrajectoryItemKind::Assistant,
                TrajectoryItemKind::User
            ]
        );
        assert_eq!(items[2].text.as_deref(), Some("hi"));
    }

    #[test]
    fn anthropic_reasoning_is_shown_once_with_its_assistant_message() {
        let first = r#"{"messages":[{"id":null,"role":"system","text":"instructions"},{"id":null,"role":"user","text":"hello"}],"instructions_present":true,"previous_response_id":null,"completeness":"complete"}"#;
        let response = r#"{"messages":[{"id":"msg_1","role":"reasoning","text":"thinking it over"},{"id":"msg_1","role":"assistant","text":"hi"}],"instructions_present":false,"previous_response_id":null,"completeness":"complete"}"#;
        let replay = r#"{"messages":[{"id":null,"role":"system","text":"instructions"},{"id":null,"role":"user","text":"hello"},{"id":null,"role":"reasoning","text":"thinking it over"},{"id":null,"role":"assistant","text":"hi"},{"id":null,"role":"user","text":"next"}],"instructions_present":true,"previous_response_id":null,"completeness":"complete"}"#;
        let items = build(timeline(vec![
            structured_event("request", first),
            structured_event("response", response),
            structured_event("request", replay),
        ]))
        .items;
        assert_eq!(
            items
                .iter()
                .map(|i| (i.kind, i.text.as_deref().unwrap_or_default()))
                .collect::<Vec<_>>(),
            [
                (TrajectoryItemKind::Context, "instructions"),
                (TrajectoryItemKind::User, "hello"),
                (TrajectoryItemKind::Assistant, "thinking it over"),
                (TrajectoryItemKind::Assistant, "hi"),
                (TrajectoryItemKind::User, "next"),
            ]
        );
        assert!(items.iter().all(|i| i.stream == 0));
    }

    #[test]
    fn incremental_requests_keep_identical_new_user_messages() {
        let items = build(timeline(vec![
            structured_event(
                "request",
                r#"{"messages":[{"role":"developer","text":"instructions"},{"id":null,"role":"user","text":"continue"}],"instructions_present":true,"previous_response_id":null}"#,
            ),
            structured_event("response", r#"{"messages":[{"id":null,"role":"assistant","text":"working"}],"instructions_present":false,"previous_response_id":null}"#),
            structured_event(
                "request",
                r#"{"messages":[{"id":null,"role":"user","text":"continue"}],"instructions_present":false,"previous_response_id":"r1"}"#,
            ),
        ]))
        .items;
        assert_eq!(
            items.iter().map(|i| i.kind).collect::<Vec<_>>(),
            [
                TrajectoryItemKind::Context,
                TrajectoryItemKind::User,
                TrajectoryItemKind::Assistant,
                TrajectoryItemKind::User
            ]
        );
        assert!(items.iter().all(|i| i.stream == 0));
    }

    #[test]
    fn structured_request_retains_system_and_assistant_roles() {
        let body = r#"{"messages":[{"role":"system","text":"instructions"},{"id":null,"role":"user","text":"hello"},{"id":null,"role":"assistant","text":"hi"},{"id":null,"role":"user","text":"hello"}],"instructions_present":true,"previous_response_id":null}"#;
        let items = build(timeline(vec![
            structured_event("request", body),
            structured_event("request", body),
        ]))
        .items;
        assert_eq!(
            items.iter().map(|i| i.kind).collect::<Vec<_>>(),
            [
                TrajectoryItemKind::Context,
                TrajectoryItemKind::User,
                TrajectoryItemKind::Assistant,
                TrajectoryItemKind::User
            ]
        );
    }

    #[test]
    fn changing_billing_headers_and_token_budgets_keep_one_stream() {
        let request = |hash: &str, left: u32, messages: &str| {
            let mut messages: Vec<Value> = serde_json::from_str(messages).unwrap();
            messages.insert(0, json!({"role":"system", "text": format!("x-anthropic-billing-header: cc_version=2.1.288; cch={hash}; cc_prev_req=req_{hash};\ninstructions\n\n<total_tokens>{left} tokens left</total_tokens>")}));
            json!({"messages": messages, "instructions_present": true}).to_string()
        };
        let items = build(timeline(vec![
            structured_event(
                "request",
                &request("a1", 900, r#"[{"role":"user","text":"hello"}]"#),
            ),
            structured_event("response", r#"{"messages":[{"id":null,"role":"assistant","text":"hi"}],"instructions_present":false,"previous_response_id":null}"#),
            structured_event(
                "request",
                &request(
                    "b2",
                    850,
                    r#"[{"role":"user","text":"hello"},{"role":"assistant","text":"hi"},{"role":"user","text":"next"}]"#,
                ),
            ),
        ]))
        .items;

        assert_eq!(
            items
                .iter()
                .map(|i| (i.kind, i.text.as_deref().unwrap_or_default()))
                .collect::<Vec<_>>(),
            [
                (
                    TrajectoryItemKind::Context,
                    "x-anthropic-billing-header: cc_version=2.1.288; cch=a1; cc_prev_req=req_a1;\ninstructions\n\n<total_tokens>900 tokens left</total_tokens>"
                ),
                (TrajectoryItemKind::User, "hello"),
                (TrajectoryItemKind::Assistant, "hi"),
                (TrajectoryItemKind::User, "next"),
            ]
        );
        assert!(items.iter().all(|i| i.stream == 0));
    }

    /// Claude Code thread requests as the collector projects them: a
    /// `create` carries the whole system prompt and history, a `continue`
    /// only the billing header and the messages added since
    /// `previous_message_id`.
    fn claude_request(thread: Value, system: &[&str], messages: Value) -> UsageEventResponse {
        let system = std::iter::once(
            "x-anthropic-billing-header: cc_version=2.1.293.af3; cc_entrypoint=cli; cch=3ceb5;",
        )
        .chain(system.iter().copied())
        .collect::<Vec<_>>()
        .join("\n");
        let mut conversation = json!({
            "messages": [{"id": null, "role": "system", "text": system}],
            "instructions_present": true,
            "previous_response_id": null,
        });
        conversation["messages"]
            .as_array_mut()
            .unwrap()
            .extend(serde_json::from_value::<Vec<Value>>(messages).unwrap());
        let mut request = structured_event("request", &conversation.to_string());
        if !thread.is_null() {
            request.metadata["claude_thread"] = thread;
        }
        request
    }

    fn create(messages: Value) -> UsageEventResponse {
        claude_request(
            json!({"type": "create"}),
            &["You are Claude Code.", "Main loop instructions."],
            messages,
        )
    }

    fn continue_from(previous: &str, messages: Value) -> UsageEventResponse {
        claude_request(
            json!({"type": "continue", "previous_message_id": previous}),
            &[],
            messages,
        )
    }

    fn user(text: &str) -> Value {
        json!({"id": null, "role": "user", "text": text})
    }

    fn budget() -> Value {
        json!({"id": null, "role": "system", "text": "<total_tokens>15000000 tokens left</total_tokens>"})
    }

    fn tool_result() -> Value {
        json!({"id": null, "role": "tool", "text": ""})
    }

    /// The replayed form of a response: thinking is redacted.
    fn replayed(text: &str) -> [Value; 3] {
        [
            json!({"id": null, "role": "reasoning", "text": ""}),
            json!({"id": null, "role": "assistant", "text": text}),
            json!({"id": null, "role": "tool", "text": ""}),
        ]
    }

    fn streamed(id: &str, text: &str) -> UsageEventResponse {
        let conversation = json!({
            "messages": [
                {"id": id, "role": "reasoning", "text": ""},
                {"id": id, "role": "assistant", "text": text},
                {"id": null, "role": "tool", "text": ""},
            ],
            "instructions_present": false,
            "previous_response_id": null,
        });
        structured_event("response", &conversation.to_string())
    }

    /// A `thread_not_found` error carries no messages.
    fn thread_not_found() -> UsageEventResponse {
        let mut response = structured_event(
            "response",
            r#"{"messages":[],"instructions_present":false,"previous_response_id":null,"completeness":"unsupported"}"#,
        );
        response.text = None;
        response
    }

    fn texts(items: &[TrajectoryItem]) -> Vec<(TrajectoryItemKind, usize, &str)> {
        items
            .iter()
            .map(|i| (i.kind, i.stream, i.text.as_deref().unwrap_or_default()))
            .collect()
    }

    const MAIN_CONTEXT: &str = "x-anthropic-billing-header: cc_version=2.1.293.af3; \
                                cc_entrypoint=cli; cch=3ceb5;\nYou are Claude Code.\nMain loop \
                                instructions.";

    #[test]
    fn thread_continue_appends_new_messages_in_the_stream_it_continues() {
        let search = claude_request(
            Value::Null,
            &[
                "You are Claude Code.",
                "You are an assistant for performing a web search tool use",
            ],
            json!([user("search the web")]),
        );
        let events = vec![
            create(json!([user("hello"), budget()])),
            streamed("msg_1", "looking"),
            search,
            streamed("msg_side", "results"),
            continue_from("msg_1", json!([tool_result(), budget()])),
            streamed("msg_2", "found it"),
            continue_from("msg_2", json!([tool_result(), user("thanks"), budget()])),
        ];
        let items = build(timeline(events)).items;

        assert_eq!(
            texts(&items),
            [
                (TrajectoryItemKind::Context, 0, MAIN_CONTEXT),
                (TrajectoryItemKind::User, 0, "hello"),
                (
                    TrajectoryItemKind::Context,
                    0,
                    "<total_tokens>15000000 tokens left</total_tokens>"
                ),
                (TrajectoryItemKind::Assistant, 0, "looking"),
                (
                    TrajectoryItemKind::Context,
                    1,
                    "x-anthropic-billing-header: cc_version=2.1.293.af3; cc_entrypoint=cli; \
                     cch=3ceb5;\nYou are Claude Code.\nYou are an assistant for performing a web \
                     search tool use"
                ),
                (TrajectoryItemKind::User, 1, "search the web"),
                (TrajectoryItemKind::Assistant, 1, "results"),
                (TrajectoryItemKind::Assistant, 0, "found it"),
                (TrajectoryItemKind::User, 0, "thanks"),
            ]
        );
    }

    #[test]
    fn thread_replay_after_a_failed_continue_adds_no_duplicates() {
        let first = user("hello");
        let retry = user("try the wire");
        let [looking_thinking, looking, looking_tool] = replayed("looking");
        let [found_thinking, found, found_tool] = replayed("found it");
        let replay = json!([
            first,
            budget(),
            looking_thinking,
            looking,
            looking_tool,
            tool_result(),
            budget(),
            found_thinking,
            found,
            found_tool,
            retry,
            budget(),
        ]);
        let events = vec![
            create(json!([first, budget()])),
            streamed("msg_1", "looking"),
            continue_from("msg_1", json!([tool_result(), budget()])),
            streamed("msg_2", "found it"),
            continue_from("msg_2", json!([retry, budget()])),
            thread_not_found(),
            create(replay),
            streamed("msg_3", "no wire port"),
            continue_from("msg_3", json!([tool_result(), budget()])),
            streamed("msg_4", "compressed"),
        ];
        let trajectory = build(timeline(events));

        assert_eq!(
            texts(&trajectory.items),
            [
                (TrajectoryItemKind::Context, 0, MAIN_CONTEXT),
                (TrajectoryItemKind::User, 0, "hello"),
                (
                    TrajectoryItemKind::Context,
                    0,
                    "<total_tokens>15000000 tokens left</total_tokens>"
                ),
                (TrajectoryItemKind::Assistant, 0, "looking"),
                (TrajectoryItemKind::Assistant, 0, "found it"),
                (TrajectoryItemKind::User, 0, "try the wire"),
                (TrajectoryItemKind::Assistant, 0, "no wire port"),
                (TrajectoryItemKind::Assistant, 0, "compressed"),
            ]
        );
        assert_eq!(trajectory.events.len(), 10);
    }

    #[test]
    fn thread_continue_of_an_unknown_message_follows_the_latest_thread() {
        let events = vec![
            create(json!([user("hello"), budget()])),
            claude_request(
                Value::Null,
                &["You are a title generator."],
                json!([user("name it")]),
            ),
            continue_from("msg_uncaptured", json!([user("next"), budget()])),
        ];
        let items = build(timeline(events)).items;
        assert_eq!(
            items.last().map(|i| (i.kind, i.stream, i.text.as_deref())),
            Some((TrajectoryItemKind::User, 0, Some("next")))
        );

        // Without any earlier thread request the continue keeps its own stream.
        let items = build(timeline(vec![
            claude_request(
                Value::Null,
                &["You are a title generator."],
                json!([user("name it")]),
            ),
            continue_from("msg_uncaptured", json!([user("next")])),
        ]))
        .items;
        assert_eq!(
            texts(&items)[2..],
            [
                (
                    TrajectoryItemKind::Context,
                    1,
                    "x-anthropic-billing-header: cc_version=2.1.293.af3; cc_entrypoint=cli; \
                     cch=3ceb5;"
                ),
                (TrajectoryItemKind::User, 1, "next"),
            ]
        );
    }

    #[test]
    fn system_messages_inside_the_conversation_keep_one_stream() {
        let first = r#"{"messages":[{"role":"system","text":"instructions"},{"id":null,"role":"user","text":"hello"},{"id":null,"role":"system","text":"reminder one"}],"instructions_present":true,"previous_response_id":null}"#;
        let second = r#"{"messages":[{"role":"system","text":"instructions"},{"id":null,"role":"user","text":"hello"},{"id":null,"role":"system","text":"reminder one"},{"id":null,"role":"assistant","text":"hi"},{"id":null,"role":"system","text":"reminder two"},{"id":null,"role":"user","text":"next"}],"instructions_present":true,"previous_response_id":null}"#;
        let items = build(timeline(vec![
            structured_event("request", first),
            structured_event("request", second),
        ]))
        .items;

        assert_eq!(
            items
                .iter()
                .map(|i| (i.kind, i.text.as_deref().unwrap_or_default()))
                .collect::<Vec<_>>(),
            [
                (TrajectoryItemKind::Context, "instructions"),
                (TrajectoryItemKind::User, "hello"),
                (TrajectoryItemKind::Context, "reminder one"),
                (TrajectoryItemKind::Assistant, "hi"),
                (TrajectoryItemKind::Context, "reminder two"),
                (TrajectoryItemKind::User, "next"),
            ]
        );
        assert!(items.iter().all(|i| i.stream == 0));
    }

    #[test]
    fn structured_compaction_keeps_existing_summary_presentation() {
        let request = json!({"messages": [{"role": "user", "text": "write report"}, {"role": "user", "text": PROMPT}]}).to_string();
        let replay = json!({"messages": [{"role": "user", "text": "write report"}, {"role": "user", "text": handoff()}]}).to_string();
        let items = build(timeline(vec![
            structured_event("request", &request),
            event("response", SUMMARY, json!({})),
            structured_event("request", &replay),
        ]))
        .items;
        assert_eq!(
            items.iter().map(|i| i.kind).collect::<Vec<_>>(),
            [TrajectoryItemKind::User, TrajectoryItemKind::Compaction]
        );
        assert_eq!(items[1].output.as_deref(), Some(SUMMARY));
    }

    const PROMPT: &str = "You are performing a CONTEXT CHECKPOINT COMPACTION. Create a \
                          handoff summary for another LLM that will resume the task.";
    const SUMMARY: &str = "## Goal\n\nFinish the report.";

    fn handoff() -> String {
        format!(
            "Another language model started to solve this problem and produced a summary \
             of its thinking process. Here is the summary produced by the other language \
             model, use the information in this summary to assist with your own \
             analysis:\n{SUMMARY}"
        )
    }

    #[test]
    fn compaction_prompt_and_summary_form_one_item_and_the_replay_is_hidden() {
        let events = vec![
            user_request(&["write the report"]),
            event("response", "working", json!({})),
            user_request(&[
                "write the report",
                "<environment_context>date</environment_context>",
                PROMPT,
            ]),
            event("response", SUMMARY, json!({})),
            user_request(&["<environment_context>cwd</environment_context>", &handoff()]),
            event("response", "continuing", json!({})),
        ];

        let items = build(timeline(events)).items;

        let kinds = items.iter().map(|item| item.kind).collect::<Vec<_>>();
        assert_eq!(
            kinds,
            [
                TrajectoryItemKind::User,
                TrajectoryItemKind::Assistant,
                TrajectoryItemKind::User,
                TrajectoryItemKind::Compaction,
                TrajectoryItemKind::User,
                TrajectoryItemKind::Assistant,
            ]
        );
        // New input sent along with the prompt stays user input.
        assert_eq!(
            items[2].text.as_deref(),
            Some("<environment_context>date</environment_context>")
        );
        assert_eq!(items[3].input.as_deref(), Some(PROMPT));
        assert_eq!(items[3].output.as_deref(), Some(SUMMARY));
        // The summary comes from the response, not from the prompt's request.
        assert!(items[3].output_event_pk.is_some());
        assert_ne!(items[3].output_event_pk, Some(items[3].event_pk));
        assert_eq!(
            items[4].text.as_deref(),
            Some("<environment_context>cwd</environment_context>")
        );
    }

    #[test]
    fn unobserved_compaction_is_recovered_from_its_handoff() {
        let events = vec![
            user_request(&[&format!("resume\n\n{}", handoff())]),
            event("response", "continuing", json!({})),
        ];

        let items = build(timeline(events)).items;

        assert_eq!(items.len(), 3);
        assert_eq!(items[0].kind, TrajectoryItemKind::User);
        assert_eq!(items[0].text.as_deref(), Some("resume"));
        assert_eq!(items[1].kind, TrajectoryItemKind::Compaction);
        assert_eq!(items[1].input, None);
        assert_eq!(items[1].output.as_deref(), Some(SUMMARY));
        assert_eq!(items[2].kind, TrajectoryItemKind::Assistant);
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
                title: None,
                purpose: None,
                parent_session_pk: None,
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
