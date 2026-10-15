//! Merged session trajectory built from accumulated request/response snapshots.
//!
//! Collectors record each provider request verbatim, so every request repeats
//! the conversation so far. The trajectory keeps only what each event adds:
//! role-aware messages, first-seen tool results, and response text and tool
//! calls, with each tool call paired to its result by call identifier.

mod conversation;

use conversation::{ConversationProjection, Placement};

use std::{
    collections::{HashMap, HashSet, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
};

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

use super::{SessionInfo, SessionTimelineResponse, UsageEventResponse};

/// Purpose the collector declares on both events of a call that asks the
/// model to summarize the conversation so far.
const COMPACTION_PURPOSE: &str = "compaction";

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
    /// Whether any token counter is non-zero.
    pub token_usage_available: bool,
}

impl From<&UsageEventResponse> for TrajectoryEvent {
    fn from(event: &UsageEventResponse) -> Self {
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
            token_usage_available: has_usage,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
/// Kind of one trajectory item.
pub enum TrajectoryItemKind {
    /// A conversation message not seen earlier in its stream, with the role
    /// the collector reported.
    Message,
    /// Event text of an event whose conversation could not be read.
    Text,
    /// Response tool call, with its result once one arrives.
    ToolCall,
    /// Request tool result whose call was not observed.
    ToolResult,
    /// Context compaction: the new user input of the compaction request as
    /// `input` and the summary its response returned as `output`.
    Compaction,
}

#[derive(Debug, Serialize)]
/// One piece of new content contributed by one event.
pub struct TrajectoryItem {
    /// Item kind.
    pub kind: TrajectoryItemKind,
    /// Role of a message item, exactly as the collector reported it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    /// Zero-based stream, placed by the identities the collector declares.
    pub stream: usize,
    /// Backend-normalized turn number.
    pub turn_index: i32,
    /// Event that contributed this item.
    pub event_pk: Uuid,
    /// Observation time of that event.
    pub observed_at: DateTime<Utc>,
    /// Model of that event.
    pub llm_model: String,
    /// Text of message and text items.
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
            role: None,
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
    /// Tool call item index by call identifier.
    calls: HashMap<String, usize>,
    /// Compaction item index by the response id of its exchange, until the
    /// response carrying the summary arrives.
    compactions: HashMap<String, usize>,
}

impl Merger {
    fn request(&mut self, event: &UsageEventResponse) {
        let first_new = self.items.len();
        let placement = self.conversation.place(event);
        if let Some(items) = self.conversation.project(event, placement) {
            self.items.extend(items);
        } else {
            self.unclassified_request(event, placement);
        }
        if is_compaction(event) {
            self.compaction(event, placement.stream, first_new);
        }
        self.request_tools(event, placement.stream);
    }

    fn unclassified_request(&mut self, event: &UsageEventResponse, placement: Placement) {
        if let Some(text) = event.text.as_ref().filter(|text| !text.is_empty()) {
            let mut item = TrajectoryItem::new(TrajectoryItemKind::Text, placement.stream, event);
            item.text = Some(text.clone());
            self.items.push(item);
        }
    }

    fn request_tools(&mut self, event: &UsageEventResponse, stream: usize) {
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

    /// Merges the new user input of a compaction request into one compaction
    /// item, which the response of the same exchange completes with its
    /// summary.
    fn compaction(&mut self, event: &UsageEventResponse, stream: usize, first_new: usize) {
        let mut compaction = TrajectoryItem::new(TrajectoryItemKind::Compaction, stream, event);
        let mut index = None;
        let mut input = Vec::new();
        for item in self.items.split_off(first_new) {
            if item.role.as_deref() != Some("user") {
                self.items.push(item);
                continue;
            }
            index.get_or_insert(self.items.len());
            input.extend(item.text);
        }
        compaction.input = (!input.is_empty()).then(|| input.join("\n\n"));
        let index = index.unwrap_or(self.items.len());
        self.items.insert(index, compaction);
        if let Some(id) = &event.response_id {
            self.compactions.insert(id.clone(), index);
        }
    }

    fn response(&mut self, event: &UsageEventResponse) {
        let placement = self.conversation.place(event);
        let stream = placement.stream;
        let projected = self.conversation.project(event, placement);
        let compaction = event
            .response_id
            .as_ref()
            .and_then(|id| self.compactions.remove(id));
        if let Some(index) = compaction {
            // The response of a compaction exchange is the summary.
            if let Some(summary) = event.text.as_deref().map(str::trim)
                && !summary.is_empty()
                && let Some(item) = self.items.get_mut(index)
            {
                item.output = Some(summary.to_owned());
                item.output_event_pk = Some(event.id);
            }
        } else if let Some(items) = projected {
            self.items.extend(items);
        } else if let Some(text) = event.text.as_deref().filter(|text| !text.trim().is_empty()) {
            let mut item = TrajectoryItem::new(TrajectoryItemKind::Text, stream, event);
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

fn is_compaction(event: &UsageEventResponse) -> bool {
    event.metadata.get("purpose").and_then(Value::as_str) == Some(COMPACTION_PURPOSE)
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

    /// The role of a message item, else the item kind, as the API reports it.
    fn shown(item: &TrajectoryItem) -> &str {
        item.role.as_deref().unwrap_or(match item.kind {
            TrajectoryItemKind::Message => "message",
            TrajectoryItemKind::Text => "text",
            TrajectoryItemKind::ToolCall => "tool_call",
            TrajectoryItemKind::ToolResult => "tool_result",
            TrajectoryItemKind::Compaction => "compaction",
        })
    }

    #[test]
    fn usage_retains_events_without_new_content_and_counts_each_event_once() {
        let body = r#"{"messages":[{"id":null,"role":"user","text":"hello"}],"instructions_present":false}"#;
        let mut request = structured_event("request", body);
        request.input_tokens = 2;
        request.cache_read_tokens = 77_000;
        request.cache_write_tokens = 400;
        request.total_tokens = 77_402;
        let repeated = structured_event("request", body);
        let mut response = event("response", "", json!({}));
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
        assert!(trajectory.events[2].token_usage_available);
        let serialized = serde_json::to_value(&trajectory.events).unwrap();
        assert!(serialized[0].get("text").is_none());
        assert!(serialized[0].get("metadata").is_none());
    }

    #[test]
    fn usage_is_available_when_any_counter_is_non_zero() {
        let zero = event("response", "", json!({}));
        let mut cached = event("request", "", json!({}));
        cached.cache_read_tokens = 10;
        let trajectory = build(timeline(vec![zero, cached]));
        assert_eq!(trajectory.events.len(), 2);
        assert!(!trajectory.events[0].token_usage_available);
        assert!(trajectory.events[1].token_usage_available);
        let serialized = serde_json::to_value(&trajectory.events).unwrap();
        assert!(serialized[0].get("token_usage_estimated").is_none());
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

        let kinds = items.iter().map(shown).collect::<Vec<_>>();
        assert_eq!(kinds, ["text", "tool_call", "text", "text", "text",]);
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
        assert!(items.iter().all(|i| shown(i) == "text" && !i.continued));
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
            // Without completeness the conversation is outside the contract.
            json!({"conversation":{"messages":[{"role":"user","text":"用户内容"}]}}),
        ] {
            let items = build(timeline(vec![event("request", text, metadata)])).items;
            assert_eq!(items.len(), 1);
            assert_eq!(shown(&items[0]), "text");
            assert_eq!(items[0].text.as_deref(), Some(text));
            assert!(!items[0].continued);
        }
        let items = build(timeline(vec![structured_event("request", r#"{"messages":[{"id":null,"role":"unknown","text":"system: unknown"},{"id":null,"role":"unrecognized","text":"hello"},{"id":null,"role":"user","text":"system: quoted text"}],"instructions_present":false}"#)])).items;
        assert_eq!(
            items.iter().map(shown).collect::<Vec<_>>(),
            ["unknown", "unrecognized", "user"]
        );
    }

    /// Every conversation in the contract declares completeness; default it to complete.
    fn structured_event(side: &str, body: &str) -> UsageEventResponse {
        let mut conversation = serde_json::from_str::<Value>(body).unwrap();
        conversation
            .as_object_mut()
            .unwrap()
            .entry("completeness")
            .or_insert_with(|| json!("complete"));
        event(
            side,
            "flattened text",
            json!({"conversation": conversation}),
        )
    }

    #[test]
    fn structured_projection_keeps_roles_and_repeated_user_occurrences() {
        let first = r#"{"messages":[{"role":"developer","text":"system instructions"},{"id":null,"role":"user","text":"继续"}],"instructions_present":true}"#;
        let response = r#"{"messages":[{"id":"a1","role":"assistant","text":"working"}],"instructions_present":false}"#;
        let replay = r#"{"messages":[{"role":"developer","text":"system instructions"},{"id":null,"role":"user","text":"继续"},{"id":"a1","role":"assistant","text":"working"},{"id":null,"role":"user","text":"继续"}],"instructions_present":true}"#;
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
            merger.items.iter().map(shown).collect::<Vec<_>>(),
            ["developer", "user", "assistant", "user"]
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
            structured_event("request", r#"{"messages":[{"role":"system","text":"instructions"},{"id":null,"role":"user","text":"hello"}],"instructions_present":true}"#),
            structured_event("response", response),
            structured_event("request", r#"{"messages":[{"role":"system","text":"instructions"},{"id":null,"role":"user","text":"hello"},{"id":null,"role":"assistant","text":"hi"},{"id":null,"role":"user","text":"hello"}],"instructions_present":true}"#),
        ])).items;
        assert_eq!(
            items.iter().map(shown).collect::<Vec<_>>(),
            ["system", "user", "assistant", "user"]
        );
        assert_eq!(items[2].text.as_deref(), Some("hi"));
    }

    #[test]
    fn anthropic_reasoning_is_shown_once_with_its_assistant_message() {
        let first = r#"{"messages":[{"id":null,"role":"system","text":"instructions"},{"id":null,"role":"user","text":"hello"}],"instructions_present":true,"completeness":"complete"}"#;
        let response = r#"{"messages":[{"id":"msg_1","role":"reasoning","text":"thinking it over"},{"id":"msg_1","role":"assistant","text":"hi"}],"instructions_present":false,"completeness":"complete"}"#;
        let replay = r#"{"messages":[{"id":null,"role":"system","text":"instructions"},{"id":null,"role":"user","text":"hello"},{"id":null,"role":"reasoning","text":"thinking it over"},{"id":null,"role":"assistant","text":"hi"},{"id":null,"role":"user","text":"next"}],"instructions_present":true,"completeness":"complete"}"#;
        let items = build(timeline(vec![
            structured_event("request", first),
            structured_event("response", response),
            structured_event("request", replay),
        ]))
        .items;
        assert_eq!(
            items
                .iter()
                .map(|i| (shown(i), i.text.as_deref().unwrap_or_default()))
                .collect::<Vec<_>>(),
            [
                ("system", "instructions"),
                ("user", "hello"),
                ("reasoning", "thinking it over"),
                ("assistant", "hi"),
                ("user", "next"),
            ]
        );
        assert!(items.iter().all(|i| i.stream == 0));
    }

    #[test]
    fn incremental_requests_keep_identical_new_user_messages() {
        let mut first = structured_event(
            "request",
            r#"{"messages":[{"role":"developer","text":"instructions"},{"id":null,"role":"user","text":"continue"}]}"#,
        );
        first.response_id = Some("r1".to_owned());
        let mut response = structured_event(
            "response",
            r#"{"messages":[{"id":null,"role":"assistant","text":"working"}]}"#,
        );
        response.response_id = Some("r1".to_owned());
        let mut next = structured_event(
            "request",
            r#"{"messages":[{"id":null,"role":"user","text":"continue"}]}"#,
        );
        next.previous_response_id = Some("r1".to_owned());
        let items = build(timeline(vec![first, response, next])).items;
        assert_eq!(
            items.iter().map(shown).collect::<Vec<_>>(),
            ["developer", "user", "assistant", "user"]
        );
        assert!(items.iter().all(|i| i.stream == 0));
    }

    #[test]
    fn a_continuation_shows_new_developer_messages_but_not_its_instructions() {
        let mut first = structured_event(
            "request",
            r#"{"messages":[{"role":"developer","text":"instructions"},{"role":"user","text":"start"}],"instructions_present":true}"#,
        );
        first.response_id = Some("r1".to_owned());
        let mut response = structured_event(
            "response",
            r#"{"messages":[{"role":"assistant","text":"working"}]}"#,
        );
        response.response_id = Some("r1".to_owned());
        // The continuation repeats its instructions, then adds a developer
        // notice and the next user input.
        let mut next = structured_event(
            "request",
            r#"{"messages":[{"role":"developer","text":"instructions"},{"role":"developer","text":"permissions changed"},{"role":"user","text":"go on"}],"instructions_present":true}"#,
        );
        next.previous_response_id = Some("r1".to_owned());
        let items = build(timeline(vec![first, response, next])).items;
        assert_eq!(
            texts(&items),
            [
                ("developer", 0, "instructions"),
                ("user", 0, "start"),
                ("assistant", 0, "working"),
                ("developer", 0, "permissions changed"),
                ("user", 0, "go on"),
            ]
        );
    }

    #[test]
    fn structured_request_retains_system_and_assistant_roles() {
        let body = r#"{"messages":[{"role":"system","text":"instructions"},{"id":null,"role":"user","text":"hello"},{"id":null,"role":"assistant","text":"hi"},{"id":null,"role":"user","text":"hello"}],"instructions_present":true}"#;
        let items = build(timeline(vec![
            structured_event("request", body),
            structured_event("request", body),
        ]))
        .items;
        assert_eq!(
            items.iter().map(shown).collect::<Vec<_>>(),
            ["system", "user", "assistant", "user"]
        );
    }

    /// Claude Code requests as the collector projects them: a `create`
    /// carries the whole system prompt and history, a `continue` only the
    /// billing header and the messages added since the response it names.
    fn claude_request(
        stream: Option<&str>,
        previous: Option<&str>,
        response: Option<&str>,
        system: &[&str],
        messages: Value,
    ) -> UsageEventResponse {
        let system = std::iter::once(
            "x-anthropic-billing-header: cc_version=2.1.293.af3; cc_entrypoint=cli; cch=3ceb5;",
        )
        .chain(system.iter().copied())
        .collect::<Vec<_>>()
        .join("\n");
        let mut conversation = json!({
            "messages": [{"id": null, "role": "system", "text": system}],
            "instructions_present": true,
        });
        conversation["messages"]
            .as_array_mut()
            .unwrap()
            .extend(serde_json::from_value::<Vec<Value>>(messages).unwrap());
        let mut request = structured_event("request", &conversation.to_string());
        request.stream_id = stream.map(str::to_owned);
        request.previous_response_id = previous.map(str::to_owned);
        request.response_id = response.map(str::to_owned);
        request
    }

    /// A main-loop request with its whole history; `response` names the
    /// response its exchange produced.
    fn create(response: &str, messages: Value) -> UsageEventResponse {
        claude_request(
            Some("main"),
            None,
            Some(response),
            &["You are Claude Code.", "Main loop instructions."],
            messages,
        )
    }

    /// A main-loop request continuing after `previous`.
    fn continue_from(
        previous: &str,
        response: Option<&str>,
        messages: Value,
    ) -> UsageEventResponse {
        claude_request(Some("main"), Some(previous), response, &[], messages)
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

    /// A response found only by the response id its exchange declares.
    fn streamed(id: &str, text: &str) -> UsageEventResponse {
        let conversation = json!({
            "messages": [
                {"id": id, "role": "reasoning", "text": ""},
                {"id": id, "role": "assistant", "text": text},
                {"id": null, "role": "tool", "text": ""},
            ],
        });
        let mut response = structured_event("response", &conversation.to_string());
        response.stream_id = None;
        response.response_id = Some(id.to_owned());
        response
    }

    /// A `thread_not_found` error carries no messages and no response id.
    fn thread_not_found() -> UsageEventResponse {
        let mut response = structured_event(
            "response",
            r#"{"messages":[],"completeness":"unsupported"}"#,
        );
        response.stream_id = None;
        response.text = None;
        response
    }

    fn texts(items: &[TrajectoryItem]) -> Vec<(&str, usize, &str)> {
        items
            .iter()
            .map(|i| (shown(i), i.stream, i.text.as_deref().unwrap_or_default()))
            .collect()
    }

    const MAIN_CONTEXT: &str = "x-anthropic-billing-header: cc_version=2.1.293.af3; \
                                cc_entrypoint=cli; cch=3ceb5;\nYou are Claude Code.\nMain loop \
                                instructions.";

    #[test]
    fn a_continuation_appends_new_messages_in_the_stream_of_its_response() {
        let search = claude_request(
            None,
            None,
            Some("msg_side"),
            &[
                "You are Claude Code.",
                "You are an assistant for performing a web search tool use",
            ],
            json!([user("search the web")]),
        );
        let events = vec![
            create("msg_1", json!([user("hello"), budget()])),
            streamed("msg_1", "looking"),
            search,
            streamed("msg_side", "results"),
            continue_from("msg_1", Some("msg_2"), json!([tool_result(), budget()])),
            streamed("msg_2", "found it"),
            continue_from(
                "msg_2",
                Some("msg_3"),
                json!([tool_result(), user("thanks"), budget()]),
            ),
        ];
        let items = build(timeline(events)).items;

        assert_eq!(
            texts(&items),
            [
                ("system", 0, MAIN_CONTEXT),
                ("user", 0, "hello"),
                (
                    "system",
                    0,
                    "<total_tokens>15000000 tokens left</total_tokens>"
                ),
                ("assistant", 0, "looking"),
                (
                    "system",
                    1,
                    "x-anthropic-billing-header: cc_version=2.1.293.af3; cc_entrypoint=cli; \
                     cch=3ceb5;\nYou are Claude Code.\nYou are an assistant for performing a web \
                     search tool use"
                ),
                ("user", 1, "search the web"),
                ("assistant", 1, "results"),
                (
                    "system",
                    0,
                    "<total_tokens>15000000 tokens left</total_tokens>"
                ),
                ("assistant", 0, "found it"),
                ("user", 0, "thanks"),
                (
                    "system",
                    0,
                    "<total_tokens>15000000 tokens left</total_tokens>"
                ),
            ]
        );
    }

    #[test]
    fn a_replay_after_a_failed_continuation_adds_no_duplicates() {
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
            create("msg_1", json!([first, budget()])),
            streamed("msg_1", "looking"),
            continue_from("msg_1", Some("msg_2"), json!([tool_result(), budget()])),
            streamed("msg_2", "found it"),
            continue_from("msg_2", None, json!([retry, budget()])),
            thread_not_found(),
            create("msg_3", replay),
            streamed("msg_3", "no wire port"),
            continue_from("msg_3", Some("msg_4"), json!([tool_result(), budget()])),
            streamed("msg_4", "compressed"),
        ];
        let trajectory = build(timeline(events));

        assert_eq!(
            texts(&trajectory.items),
            [
                ("system", 0, MAIN_CONTEXT),
                ("user", 0, "hello"),
                (
                    "system",
                    0,
                    "<total_tokens>15000000 tokens left</total_tokens>"
                ),
                ("assistant", 0, "looking"),
                (
                    "system",
                    0,
                    "<total_tokens>15000000 tokens left</total_tokens>"
                ),
                ("assistant", 0, "found it"),
                ("user", 0, "try the wire"),
                (
                    "system",
                    0,
                    "<total_tokens>15000000 tokens left</total_tokens>"
                ),
                ("assistant", 0, "no wire port"),
                (
                    "system",
                    0,
                    "<total_tokens>15000000 tokens left</total_tokens>"
                ),
                ("assistant", 0, "compressed"),
            ]
        );
        assert_eq!(trajectory.events.len(), 10);
    }

    #[test]
    fn a_continuation_of_an_unseen_response_uses_its_stream_id_or_opens_a_stream() {
        let title = || {
            claude_request(
                None,
                None,
                Some("msg_title"),
                &["You are a title generator."],
                json!([user("name it")]),
            )
        };
        let events = vec![
            create("msg_1", json!([user("hello"), budget()])),
            title(),
            continue_from("msg_uncaptured", None, json!([user("next"), budget()])),
        ];
        let items = build(timeline(events)).items;
        assert_eq!(
            texts(&items)[items.len() - 2..],
            [
                ("user", 0, "next"),
                (
                    "system",
                    0,
                    "<total_tokens>15000000 tokens left</total_tokens>"
                ),
            ]
        );

        // Without a stream id the continuation opens a stream of its own and
        // shows only the messages it carries.
        let mut orphan = continue_from("msg_uncaptured", None, json!([user("next")]));
        orphan.stream_id = None;
        let items = build(timeline(vec![title(), orphan])).items;
        assert_eq!(texts(&items)[2..], [("user", 1, "next")]);
    }

    #[test]
    fn declared_stream_ids_keep_each_thread_apart() {
        let request = |stream: &str, messages: &str| {
            let mut event = structured_event(
                "request",
                &format!(
                    r#"{{"messages":[{{"role":"developer","text":"{stream} rules"}},{messages}]}}"#
                ),
            );
            event.stream_id = Some(stream.to_owned());
            event
        };
        let items = build(timeline(vec![
            request("main", r#"{"role":"user","text":"fix it"}"#),
            request("guardian", r#"{"role":"user","text":"review the action"}"#),
            request(
                "main",
                r#"{"role":"user","text":"fix it"},{"role":"user","text":"and test"}"#,
            ),
            request(
                "guardian",
                r#"{"role":"user","text":"review the action"},{"role":"user","text":"next action"}"#,
            ),
        ]))
        .items;

        assert_eq!(
            texts(&items),
            [
                ("developer", 0, "main rules"),
                ("user", 0, "fix it"),
                ("developer", 1, "guardian rules"),
                ("user", 1, "review the action"),
                ("user", 0, "and test"),
                ("user", 1, "next action"),
            ]
        );
    }

    #[test]
    fn requests_without_a_stream_id_each_open_a_stream_and_repeat_their_history() {
        let recap = || {
            let mut event = structured_event(
                "request",
                r#"{"messages":[{"role":"system","text":"rules"},{"role":"user","text":"hello"},{"role":"user","text":"recap"}]}"#,
            );
            event.stream_id = None;
            event
        };
        let items = build(timeline(vec![recap(), recap()])).items;

        assert_eq!(
            texts(&items),
            [
                ("system", 0, "rules"),
                ("user", 0, "hello"),
                ("user", 0, "recap"),
                ("system", 1, "rules"),
                ("user", 1, "hello"),
                ("user", 1, "recap"),
            ]
        );
    }

    #[test]
    fn a_response_joins_the_stream_of_the_request_with_its_response_id() {
        let mut main = create("msg_main", json!([user("hello")]));
        main.stream_id = Some("main".to_owned());
        let side = claude_request(
            None,
            None,
            Some("msg_side"),
            &["You are a summarizer."],
            json!([user("summarize")]),
        );
        // The main exchange answers after the side request was observed.
        let items = build(timeline(vec![
            main,
            side,
            streamed("msg_main", "hi"),
            streamed("msg_side", "summary"),
            streamed("msg_unpaired", "orphan"),
        ]))
        .items;

        assert_eq!(
            texts(&items)[2..],
            [
                (
                    "system",
                    1,
                    "x-anthropic-billing-header: cc_version=2.1.293.af3; cc_entrypoint=cli; \
                     cch=3ceb5;\nYou are a summarizer."
                ),
                ("user", 1, "summarize"),
                ("assistant", 0, "hi"),
                ("assistant", 1, "summary"),
                ("assistant", 2, "orphan"),
            ]
        );
    }

    #[test]
    fn opening_instructions_show_only_where_they_change() {
        let request = |rules: &str, messages: &str| {
            structured_event(
                "request",
                &format!(
                    r#"{{"messages":[{{"role":"system","text":"base"}},{{"role":"developer","text":"{rules}"}},{messages}],"instructions_present":true}}"#
                ),
            )
        };
        let items = build(timeline(vec![
            request("rules v1", r#"{"role":"user","text":"hello"}"#),
            request(
                "rules v1",
                r#"{"role":"user","text":"hello"},{"role":"user","text":"again"}"#,
            ),
            request(
                "rules v2",
                r#"{"role":"user","text":"hello"},{"role":"user","text":"again"},{"role":"user","text":"more"}"#,
            ),
        ]))
        .items;

        assert_eq!(
            texts(&items),
            [
                ("system", 0, "base"),
                ("developer", 0, "rules v1"),
                ("user", 0, "hello"),
                ("user", 0, "again"),
                ("developer", 0, "rules v2"),
                ("user", 0, "more"),
            ]
        );
    }

    #[test]
    fn system_messages_inside_the_conversation_keep_one_stream() {
        let first = r#"{"messages":[{"role":"system","text":"instructions"},{"id":null,"role":"user","text":"hello"},{"id":null,"role":"system","text":"reminder one"}],"instructions_present":true}"#;
        let second = r#"{"messages":[{"role":"system","text":"instructions"},{"id":null,"role":"user","text":"hello"},{"id":null,"role":"system","text":"reminder one"},{"id":null,"role":"assistant","text":"hi"},{"id":null,"role":"system","text":"reminder two"},{"id":null,"role":"user","text":"next"}],"instructions_present":true}"#;
        let items = build(timeline(vec![
            structured_event("request", first),
            structured_event("request", second),
        ]))
        .items;

        assert_eq!(
            items
                .iter()
                .map(|i| (shown(i), i.text.as_deref().unwrap_or_default()))
                .collect::<Vec<_>>(),
            [
                ("system", "instructions"),
                ("user", "hello"),
                ("system", "reminder one"),
                ("assistant", "hi"),
                ("system", "reminder two"),
                ("user", "next"),
            ]
        );
        assert!(items.iter().all(|i| i.stream == 0));
    }

    const PROMPT: &str = "You are performing a CONTEXT CHECKPOINT COMPACTION. Create a \
                          handoff summary for another LLM that will resume the task.";
    const SUMMARY: &str = "## Goal\n\nFinish the report.";

    /// One exchange of a call the collector declares as a compaction.
    fn compaction(texts: &[&str], response_id: &str) -> [UsageEventResponse; 2] {
        let mut request = user_request(texts);
        request.metadata["purpose"] = json!(COMPACTION_PURPOSE);
        request.response_id = Some(response_id.to_owned());
        let mut response = event("response", SUMMARY, json!({"purpose": COMPACTION_PURPOSE}));
        response.response_id = Some(response_id.to_owned());
        [request, response]
    }

    #[test]
    fn a_compaction_exchange_forms_one_item_and_its_replay_is_ordinary_input() {
        let [request, response] = compaction(
            &[
                "write the report",
                "<environment_context>date</environment_context>",
                PROMPT,
            ],
            "resp_compact",
        );
        let replay = format!("Here is the summary of the earlier conversation:\n{SUMMARY}");
        let events = vec![
            user_request(&["write the report"]),
            event("response", "working", json!({})),
            request,
            response,
            user_request(&["<environment_context>cwd</environment_context>", &replay]),
            event("response", "continuing", json!({})),
        ];

        let items = build(timeline(events)).items;

        assert_eq!(
            items.iter().map(shown).collect::<Vec<_>>(),
            ["user", "text", "compaction", "user", "user", "text",]
        );
        // The new input of the compaction request is the item's input.
        assert_eq!(
            items[2].input.as_deref(),
            Some(&*format!(
                "<environment_context>date</environment_context>\n\n{PROMPT}"
            ))
        );
        // The summary comes from the response of the same exchange.
        assert_eq!(items[2].output.as_deref(), Some(SUMMARY));
        assert!(items[2].output_event_pk.is_some());
        assert_ne!(items[2].output_event_pk, Some(items[2].event_pk));
        // The replayed summary is shown like any other new input.
        assert_eq!(items[4].text.as_deref(), Some(&*replay));
    }

    #[test]
    fn compaction_prompt_text_alone_is_ordinary_input() {
        let items = build(timeline(vec![
            user_request(&[PROMPT]),
            event("response", SUMMARY, json!({})),
        ]))
        .items;
        assert_eq!(
            items.iter().map(shown).collect::<Vec<_>>(),
            ["user", "text"]
        );
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
            stream_id: Some("main".to_owned()),
            response_id: None,
            previous_response_id: None,
            metadata,
            attachments: Vec::new(),
        }
    }
}
