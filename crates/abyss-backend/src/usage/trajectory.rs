//! Merged session trajectory built from accumulated request/response snapshots.
//!
//! Every event shows the conversation entries it carries, in order: messages
//! with the role the collector reported, and tool calls and results with
//! their call identifier. A request carries its whole history and shows all
//! of it.

mod conversation;

use conversation::ConversationProjection;

use std::collections::HashMap;

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
    /// A conversation entry the event carries, with the role the collector
    /// reported.
    Message,
    /// Event text of an event whose conversation could not be read.
    Text,
    /// A tool call entry: its name, call identifier, and input.
    ToolCall,
    /// A tool result entry: its call identifier and output.
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
    /// Event that carried the tool result or compaction summary.
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
    /// Compaction item index by the response id of its exchange, until the
    /// response carrying the summary arrives.
    compactions: HashMap<String, usize>,
}

impl Merger {
    fn request(&mut self, event: &UsageEventResponse) {
        let first_new = self.items.len();
        let stream = self.conversation.place(event);
        if let Some(items) = ConversationProjection::project(event, stream) {
            self.items.extend(items);
        } else {
            self.unclassified_request(event, stream);
        }
        if is_compaction(event) {
            self.compaction(event, stream, first_new);
        }
    }

    fn unclassified_request(&mut self, event: &UsageEventResponse, stream: usize) {
        if let Some(text) = event.text.as_ref().filter(|text| !text.is_empty()) {
            let mut item = TrajectoryItem::new(TrajectoryItemKind::Text, stream, event);
            item.text = Some(text.clone());
            self.items.push(item);
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
        let stream = self.conversation.place(event);
        let projected = ConversationProjection::project(event, stream);
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
    }
}

fn is_compaction(event: &UsageEventResponse) -> bool {
    event.metadata.get("purpose").and_then(Value::as_str) == Some(COMPACTION_PURPOSE)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::usage::TurnTimeline;

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

    /// Each item's role or kind, stream, and text, tool input, or tool output.
    fn texts(items: &[TrajectoryItem]) -> Vec<(&str, usize, &str)> {
        items
            .iter()
            .map(|i| {
                let text = i.text.as_ref().or(i.input.as_ref()).or(i.output.as_ref());
                (shown(i), i.stream, text.map_or("", String::as_str))
            })
            .collect()
    }

    fn tool_call(call_id: &str, input: &str) -> Value {
        json!({"role": "tool_call", "text": input, "call_id": call_id, "name": "Bash"})
    }

    fn tool_result(call_id: &str, output: &str) -> Value {
        json!({"role": "tool_result", "text": output, "call_id": call_id})
    }

    fn entry(role: &str, text: &str) -> Value {
        json!({"role": role, "text": text})
    }

    /// An event whose complete conversation holds `entries`.
    fn conversation(side: &str, entries: Value) -> UsageEventResponse {
        let mut conversation = json!({"completeness": "complete"});
        conversation["entries"] = entries;
        event(
            side,
            "flattened text",
            json!({"conversation": conversation}),
        )
    }

    /// An event with the stream and response identities its exchange declares.
    fn declared(
        mut event: UsageEventResponse,
        stream: Option<&str>,
        previous: Option<&str>,
        response: Option<&str>,
    ) -> UsageEventResponse {
        event.stream_id = stream.map(str::to_owned);
        event.previous_response_id = previous.map(str::to_owned);
        event.response_id = response.map(str::to_owned);
        event
    }

    fn user_request(texts: &[&str]) -> UsageEventResponse {
        conversation(
            "request",
            texts.iter().map(|text| entry("user", text)).collect(),
        )
    }

    #[test]
    fn every_event_is_listed_once_with_its_usage() {
        let mut request = user_request(&["hello"]);
        request.input_tokens = 2;
        request.cache_read_tokens = 77_000;
        request.cache_write_tokens = 400;
        request.total_tokens = 77_402;
        let unreadable = event("request", "", json!({}));
        let mut response = event("response", "", json!({}));
        response.llm_model = "other-model".to_owned();
        response.output_tokens = 150;
        response.reasoning_tokens = 40;
        response.total_tokens = 150;
        let mut cached = event("request", "", json!({}));
        cached.cache_read_tokens = 10;
        let ids = [request.id, unreadable.id, response.id, cached.id];

        let trajectory = build(timeline(vec![request, unreadable, response, cached]));
        assert_eq!(trajectory.items.len(), 1);
        let events = &trajectory.events;
        assert_eq!(events.iter().map(|e| e.event_pk).collect::<Vec<_>>(), ids);
        assert_eq!(
            events
                .iter()
                .map(|e| e.token_usage_available)
                .collect::<Vec<_>>(),
            [true, false, true, true]
        );
        assert_eq!(
            (
                events[0].total_tokens,
                events[0].cache_read_tokens,
                events[0].cache_write_tokens
            ),
            (77_402, 77_000, 400)
        );
        assert_eq!(events[2].llm_model, "other-model");
        assert_eq!(
            (events[2].output_tokens, events[2].reasoning_tokens),
            (150, 40)
        );
        let serialized = serde_json::to_value(events).unwrap();
        for field in ["text", "metadata", "token_usage_estimated"] {
            assert!(serialized[0].get(field).is_none(), "{field}");
        }
    }

    #[test]
    fn events_without_a_readable_conversation_show_their_text_verbatim() {
        let text = "system: instructions\n\n用户内容\n\n用户内容";
        for metadata in [
            json!({}),
            json!({"conversation":{"completeness":"partial","entries":[]}}),
            json!({"conversation":{"entries":"invalid"}}),
            json!({"conversation":{"entries":[],"completeness":"complete"}}),
            // Without completeness the conversation is outside the contract.
            json!({"conversation":{"entries":[{"role":"user","text":"用户内容"}]}}),
            // The retired field is not read.
            json!({"conversation":{"messages":[{"role":"user","text":"用户内容"}],"completeness":"complete"}}),
        ] {
            let items = build(timeline(vec![event("request", text, metadata)])).items;
            assert_eq!(texts(&items), [("text", 0, text)]);
            assert!(!items[0].continued);
        }
        // A failed response with neither a conversation nor text adds nothing.
        let failed = event(
            "response",
            "",
            json!({"conversation":{"entries":[],"completeness":"unsupported"}}),
        );
        assert!(build(timeline(vec![failed])).items.is_empty());
    }

    #[test]
    fn tool_items_keep_their_name_and_call_id() {
        let request = conversation(
            "request",
            json!([tool_call("c1", "ls"), tool_result("c1", "README.md")]),
        );
        let id = request.id;
        let items = build(timeline(vec![request])).items;
        assert_eq!(
            items
                .iter()
                .map(|i| (i.kind, i.name.as_deref(), i.call_id.as_deref()))
                .collect::<Vec<_>>(),
            [
                (TrajectoryItemKind::ToolCall, Some("Bash"), Some("c1")),
                (TrajectoryItemKind::ToolResult, None, Some("c1")),
            ]
        );
        assert_eq!(items[1].output_event_pk, Some(id));
    }

    #[test]
    fn every_event_shows_every_entry_it_carries() {
        let request = |entries: Value| conversation("request", entries);
        let response = |text: &str| conversation("response", json!([entry("assistant", text)]));
        let cases = [
            (
                "roles are kept as reported; messages without text are left out",
                vec![request(json!([
                    entry("unknown", "a"),
                    entry("unrecognized", "b"),
                    entry("user", "system: quoted"),
                    entry("reasoning", ""),
                    tool_call("c1", ""),
                    tool_result("c1", "ok"),
                ]))],
                vec![
                    ("unknown", 0, "a"),
                    ("unrecognized", 0, "b"),
                    ("user", 0, "system: quoted"),
                    ("tool_call", 0, ""),
                    ("tool_result", 0, "ok"),
                ],
            ),
            (
                "tool calls and results show wherever an event carries them",
                vec![
                    conversation("response", json!([tool_call("c1", "ls")])),
                    request(json!([
                        entry("user", "go"),
                        tool_call("c1", "ls"),
                        tool_result("c1", "README.md"),
                    ])),
                ],
                vec![
                    ("tool_call", 0, "ls"),
                    ("user", 0, "go"),
                    ("tool_call", 0, "ls"),
                    ("tool_result", 0, "README.md"),
                ],
            ),
            (
                "a full request shows its whole history again",
                vec![
                    request(json!([entry("system", "rules"), entry("user", "hi")])),
                    response("ok"),
                    request(json!([
                        entry("system", "rules"),
                        entry("user", "hi"),
                        entry("assistant", "ok"),
                        entry("user", "hi"),
                    ])),
                ],
                vec![
                    ("system", 0, "rules"),
                    ("user", 0, "hi"),
                    ("assistant", 0, "ok"),
                    ("system", 0, "rules"),
                    ("user", 0, "hi"),
                    ("assistant", 0, "ok"),
                    ("user", 0, "hi"),
                ],
            ),
        ];
        for (name, events, expected) in cases {
            assert_eq!(texts(&build(timeline(events)).items), expected, "{name}");
        }
    }

    #[test]
    fn events_are_placed_by_the_identities_their_exchange_declares() {
        fn ask(
            texts: &[&str],
            stream: Option<&str>,
            previous: Option<&str>,
            response: Option<&str>,
        ) -> UsageEventResponse {
            declared(user_request(texts), stream, previous, response)
        }
        fn answer(text: &str, response: &str) -> UsageEventResponse {
            let event = conversation("response", json!([entry("assistant", text)]));
            declared(event, None, None, Some(response))
        }
        let cases = [
            (
                "declared stream ids keep each thread apart",
                vec![
                    ask(&["fix it"], Some("main"), None, None),
                    ask(&["review"], Some("guardian"), None, None),
                    ask(&["fix it", "and test"], Some("main"), None, None),
                ],
                vec![
                    ("user", 0, "fix it"),
                    ("user", 1, "review"),
                    ("user", 0, "fix it"),
                    ("user", 0, "and test"),
                ],
            ),
            (
                "requests without a stream id each open a stream",
                vec![
                    ask(&["recap"], None, None, None),
                    ask(&["recap"], None, None, None),
                ],
                vec![("user", 0, "recap"), ("user", 1, "recap")],
            ),
            (
                "responses and continuations join the stream of the response id",
                vec![
                    ask(&["hello"], Some("main"), None, Some("r_main")),
                    ask(&["summarize"], None, None, Some("r_side")),
                    answer("hi", "r_main"),
                    answer("summary", "r_side"),
                    answer("orphan", "r_unpaired"),
                    ask(&["thanks"], None, Some("r_main"), None),
                ],
                vec![
                    ("user", 0, "hello"),
                    ("user", 1, "summarize"),
                    ("assistant", 0, "hi"),
                    ("assistant", 1, "summary"),
                    ("assistant", 2, "orphan"),
                    ("user", 0, "thanks"),
                ],
            ),
            (
                "a continuation of an unseen response uses its stream id, else opens a stream",
                vec![
                    ask(&["hello"], Some("main"), None, None),
                    ask(&["next"], Some("main"), Some("r_unseen"), None),
                    ask(&["lost"], None, Some("r_unseen"), None),
                ],
                vec![
                    ("user", 0, "hello"),
                    ("user", 0, "next"),
                    ("user", 1, "lost"),
                ],
            ),
        ];
        for (name, events, expected) in cases {
            assert_eq!(texts(&build(timeline(events)).items), expected, "{name}");
        }
    }

    const PROMPT: &str = "You are performing a CONTEXT CHECKPOINT COMPACTION. Create a \
                          handoff summary for another LLM that will resume the task.";
    const SUMMARY: &str = "## Goal\n\nFinish the report.";

    #[test]
    fn a_compaction_exchange_forms_one_item_from_its_user_input_and_summary() {
        let mut request = user_request(&["write the report", PROMPT]);
        request.metadata["purpose"] = json!(COMPACTION_PURPOSE);
        request.response_id = Some("resp_compact".to_owned());
        let mut response = event("response", SUMMARY, json!({"purpose": COMPACTION_PURPOSE}));
        response.response_id = Some("resp_compact".to_owned());
        let replay = format!("Here is the summary of the earlier conversation:\n{SUMMARY}");
        let events = vec![
            user_request(&["write the report"]),
            event("response", "working", json!({})),
            request,
            response,
            user_request(&[&replay]),
            // Prompt text alone does not make a compaction.
            user_request(&[PROMPT]),
            event("response", SUMMARY, json!({})),
        ];

        let items = build(timeline(events)).items;

        assert_eq!(
            items.iter().map(shown).collect::<Vec<_>>(),
            ["user", "text", "compaction", "user", "user", "text"]
        );
        assert_eq!(
            items[2].input.as_deref(),
            Some(&*format!("write the report\n\n{PROMPT}"))
        );
        // The summary comes from the response of the same exchange.
        assert_eq!(items[2].output.as_deref(), Some(SUMMARY));
        assert!(items[2].output_event_pk.is_some());
        assert_ne!(items[2].output_event_pk, Some(items[2].event_pk));
        assert_eq!(items[3].text.as_deref(), Some(&*replay));
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
