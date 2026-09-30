//! Merged session trajectory built from accumulated request/response snapshots.
//!
//! Collectors record each provider request verbatim, so every request repeats
//! the conversation so far. The trajectory keeps only what each event adds:
//! role-aware messages, first-seen tool results, and response text and tool
//! calls, with each tool call paired to its result by call identifier.

mod raw_body;

use raw_body::RawProjection;

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
    raw: RawProjection,
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
        if let Some(context) = RawProjection::context(event) {
            let key = hash_str(&context);
            let next = self.streams.len();
            self.current_stream = *self.streams.entry(key).or_insert(next);
        }
        if let Some(items) = self.raw.project(event, self.current_stream) {
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
        let projected = self.raw.project(event, stream);
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
        raw_event("request", &json!({"input": texts.iter().map(|text| json!({"role":"user", "content":text})).collect::<Vec<_>>()}).to_string())
    }

    #[test]
    fn unavailable_roles_never_become_system_or_user_by_text_guessing() {
        let text = "system: instructions\n\n用户内容\n\n用户内容";
        for metadata in [
            json!({}),
            json!({"raw_body":{"parts":[{"truncated":true}]}}),
            json!({"raw_body":{"parts":[{"truncated":false,"content_base64":"invalid"}]}}),
        ] {
            let items = build(timeline(vec![event("request", text, metadata)])).items;
            assert_eq!(items.len(), 1);
            assert_eq!(items[0].kind, TrajectoryItemKind::Request);
            assert_eq!(items[0].text.as_deref(), Some(text));
            assert!(!items[0].continued);
        }
        let items = build(timeline(vec![raw_event("request", r#"{"input":[{"content":"system: unknown"},{"role":"unrecognized","content":"hello"},{"role":"user","content":"system: quoted text"}]}"#)])).items;
        assert_eq!(
            items.iter().map(|i| i.kind).collect::<Vec<_>>(),
            [
                TrajectoryItemKind::Request,
                TrajectoryItemKind::Request,
                TrajectoryItemKind::User
            ]
        );
    }

    fn raw_event(side: &str, body: &str) -> UsageEventResponse {
        use base64::{Engine as _, engine::general_purpose::STANDARD};
        event(
            side,
            "legacy fallback",
            json!({"raw_body": {"transport": "http", "parts": [{
                "content_base64": STANDARD.encode(body.as_bytes()), "truncated": false,
                "byte_size": body.len(), "content_type": "application/json"
            }]}}),
        )
    }

    #[test]
    fn raw_archive_projection_keeps_roles_and_repeated_user_occurrences() {
        let first = r#" {"instructions":"system instructions","input":[{"role":"user","content":"继续"}],"unknown":[1,1]} "#;
        let response = r#"{"output":[{"id":"a1","role":"assistant","content":[{"type":"output_text","text":"working"}]}]}"#;
        let replay = r#"{"instructions":"system instructions","input":[{"role":"user","content":"继续"},{"id":"a1","role":"assistant","content":[{"type":"output_text","text":"working"}]},{"role":"user","content":"继续"}]}"#;
        let events = vec![
            raw_event("request", first),
            raw_event("response", response),
            raw_event("request", replay),
            raw_event("request", replay),
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
        let response = "event: content_block_start\r\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\r\n\r\nevent: content_block_delta\r\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\r\n\r\n";
        let items = build(timeline(vec![
            raw_event("request", r#"{"system":"instructions","messages":[{"role":"user","content":"hello"}]}"#),
            raw_event("response", response),
            raw_event("request", r#"{"system":"instructions","messages":[{"role":"user","content":"hello"},{"role":"assistant","content":"hi"},{"role":"user","content":"hello"}]}"#),
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
    fn incremental_requests_keep_identical_new_user_messages() {
        let items = build(timeline(vec![
            raw_event(
                "request",
                r#"{"instructions":"instructions","input":[{"role":"user","content":"continue"}]}"#,
            ),
            raw_event("response", r#"{"output_text":"working"}"#),
            raw_event(
                "request",
                r#"{"previous_response_id":"r1","input":[{"role":"user","content":"continue"}]}"#,
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
    fn raw_anthropic_request_retains_system_and_assistant_roles() {
        let body = r#"{"system":[{"type":"text","text":"instructions"}],"messages":[{"role":"user","content":"hello"},{"role":"assistant","content":[{"type":"text","text":"hi"}]},{"role":"user","content":"hello"}]}"#;
        let items = build(timeline(vec![
            raw_event("request", body),
            raw_event("request", body),
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
    fn raw_compaction_keeps_existing_summary_presentation() {
        let request = json!({"input": [{"role": "user", "content": "write report"}, {"role": "user", "content": PROMPT}]}).to_string();
        let replay = json!({"input": [{"role": "user", "content": "write report"}, {"role": "user", "content": handoff()}]}).to_string();
        let items = build(timeline(vec![
            raw_event("request", &request),
            event("response", SUMMARY, json!({})),
            raw_event("request", &replay),
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
