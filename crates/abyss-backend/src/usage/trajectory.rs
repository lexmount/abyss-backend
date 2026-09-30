//! Merged session trajectory built from accumulated request/response snapshots.
//!
//! Collectors record each provider request verbatim, so every request repeats
//! the conversation so far. The trajectory keeps only what each event adds:
//! new request paragraphs, first-seen tool results, and response text and tool
//! calls, with each tool call paired to its result by call identifier.

mod conversation;

use conversation::ConversationMerger;

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
    conversation: ConversationMerger,
    items: Vec<TrajectoryItem>,
    seen_paragraphs: HashSet<u64>,
    seen_results: HashSet<String>,
    streams: HashMap<u64, usize>,
    last_paragraphs: HashMap<usize, Vec<String>>,
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
        if let Some(items) = self.conversation.project(event, self.current_stream) {
            self.items.extend(items);
        } else {
            self.legacy_request_text(event);
        }
        self.mark_compaction(first_new);
        let stream = self.current_stream;
        for segment in content_segments(&event.metadata) {
            if segment_str(segment, "type") == Some("tool_call") {
                self.tool_call(event, segment, stream);
            }
        }
        self.request_tools(event, stream);
    }

    fn legacy_request_text(&mut self, event: &UsageEventResponse) {
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
        let projected = self.conversation.project(event, stream);
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
            self.tool_call(event, segment, stream);
        }
    }

    fn tool_call(&mut self, event: &UsageEventResponse, segment: &Value, stream: usize) {
        if segment_str(segment, "call_id").is_some_and(|id| self.calls.contains_key(id)) {
            return;
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
    fn snapshots_project_replay_once_and_keep_repeated_user_occurrences() {
        let system = json!({"role": "system", "text": "system instructions"});
        let developer = json!({"role": "developer", "text": "developer instructions"});
        let user = json!({"role": "user", "text": "继续"});
        let assistant = json!({"id": "a1", "role": "assistant", "text": "working"});
        let final_reply = json!({"id": "a2", "role": "assistant", "text": "done"});
        let first = vec![system, developer, user.clone()];
        let mut replay = first.clone();
        replay.push(assistant.clone());
        let call = json!({"type": "tool_call", "call_id": "c1", "name": "exec", "input": "run"});
        let mut response = snapshot_event("response", &[assistant], "r1", None);
        response.metadata["content_segments"] = json!([call]);
        let mut continuation = snapshot_event("request", &replay, "r2", None);
        continuation.metadata["content_segments"] = json!([
            call, {"type": "tool_result", "call_id": "c1", "output": "success"}
        ]);
        replay.extend([final_reply.clone(), user]);
        let items = build(timeline(vec![
            snapshot_event("request", &first, "r1", None),
            response,
            continuation,
            snapshot_event("response", &[final_reply], "r2", None),
            snapshot_event("request", &replay, "r3", None),
        ]))
        .items;

        assert_eq!(
            items.iter().map(|item| item.kind).collect::<Vec<_>>(),
            [
                TrajectoryItemKind::Context,
                TrajectoryItemKind::Context,
                TrajectoryItemKind::User,
                TrajectoryItemKind::Assistant,
                TrajectoryItemKind::ToolCall,
                TrajectoryItemKind::Assistant,
                TrajectoryItemKind::User,
            ]
        );
        assert_eq!(items[2].text.as_deref(), Some("继续"));
        assert_eq!(items[6].text.as_deref(), Some("继续"));
        assert_eq!(items[4].output.as_deref(), Some("success"));
    }

    #[test]
    fn incremental_requests_append_identical_input_and_ignore_retries() {
        let user = json!({"role": "user", "text": "继续"});
        let assistant = json!({"id": "a1", "role": "assistant", "text": "done"});
        let items = build(timeline(vec![
            snapshot_event("request", std::slice::from_ref(&user), "r1", None),
            snapshot_event("response", std::slice::from_ref(&assistant), "r1", None),
            snapshot_event("request", &[assistant, user.clone()], "r2", Some("r1")),
            snapshot_event("request", &[user], "r2", Some("r1")),
        ]))
        .items;
        assert_eq!(items.len(), 3);
        assert_eq!(items[0].kind, TrajectoryItemKind::User);
        assert_eq!(items[1].kind, TrajectoryItemKind::Assistant);
        assert_eq!(items[2].kind, TrajectoryItemKind::User);
    }

    #[test]
    fn snapshots_use_roles_and_occurrences_instead_of_paragraphs_or_tags() {
        let text = "system: quoted\n\n<developer>quoted</developer>";
        let user = json!({"role": "user", "text": text});
        let items = build(timeline(vec![
            snapshot_event("request", &[user.clone(), user.clone()], "r1", None),
            snapshot_event("request", &[user.clone(), user], "r2", None),
        ]))
        .items;
        assert_eq!(items.len(), 2);
        assert!(
            items
                .iter()
                .all(|item| item.kind == TrajectoryItemKind::User)
        );
        assert!(items.iter().all(|item| item.text.as_deref() == Some(text)));
    }

    #[test]
    fn new_context_does_not_replay_existing_dialogue() {
        let user = json!({"role": "user", "text": "hello"});
        let assistant = json!({"id": "a1", "role": "assistant", "text": "hi"});
        let items = build(timeline(vec![
            snapshot_event("request", std::slice::from_ref(&user), "r1", None),
            snapshot_event("response", std::slice::from_ref(&assistant), "r1", None),
            snapshot_event(
                "request",
                &[
                    json!({"role": "developer", "text": "updated instructions"}),
                    user,
                    assistant,
                ],
                "r2",
                None,
            ),
        ]))
        .items;
        assert_eq!(items.len(), 3);
        assert_eq!(items[2].kind, TrajectoryItemKind::Context);
        assert_eq!(items[2].text.as_deref(), Some("updated instructions"));
    }

    #[test]
    fn observed_item_ids_survive_compaction_and_distinguish_new_messages() {
        let first = json!({"id": "u1", "role": "user", "text": "continue"});
        let second = json!({"id": "u2", "role": "user", "text": "continue"});
        let items = build(timeline(vec![
            snapshot_event("request", std::slice::from_ref(&first), "r1", None),
            snapshot_event("request", &[second], "r2", None),
            snapshot_event("request", &[first], "r3", None),
        ]))
        .items;
        assert_eq!(items.len(), 2);
    }

    fn snapshot_event(
        side: &str,
        messages: &[Value],
        response: &str,
        previous: Option<&str>,
    ) -> UsageEventResponse {
        let text = messages
            .iter()
            .filter_map(|message| message["text"].as_str())
            .collect::<Vec<_>>()
            .join("\n\n");
        event(
            side,
            &text,
            json!({"conversation": {
                "messages": messages, "response_id": response, "previous_response_id": previous,
            }}),
        )
    }

    #[test]
    fn common_prefix_stops_on_character_boundaries() {
        assert_eq!(common_prefix_len("中文a", "中字b"), 3);
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
            event("request", "write the report", json!({})),
            event("response", "working", json!({})),
            event(
                "request",
                &format!(
                    "write the report\n\n<environment_context>date</environment_context>\n\n{PROMPT}"
                ),
                json!({}),
            ),
            event("response", SUMMARY, json!({})),
            event(
                "request",
                &format!(
                    "<environment_context>cwd</environment_context>\n\n{}",
                    handoff()
                ),
                json!({}),
            ),
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
            event("request", &format!("resume\n\n{}", handoff()), json!({})),
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

    #[test]
    fn snapshot_compaction_is_recognized_by_message() {
        let user = json!({"role": "user", "text": "write the report"});
        let prompt = json!({"role": "user", "text": PROMPT});
        let replay = json!({"role": "user", "text": handoff()});
        let conversation = |messages: Vec<Value>| json!({"conversation": {"messages": messages}});
        let events = vec![
            event("request", "", conversation(vec![user.clone(), prompt])),
            event(
                "response",
                SUMMARY,
                conversation(vec![json!({"role": "assistant", "text": SUMMARY})]),
            ),
            event("request", "", conversation(vec![user, replay])),
        ];

        let items = build(timeline(events)).items;

        let kinds = items.iter().map(|item| item.kind).collect::<Vec<_>>();
        assert_eq!(
            kinds,
            [TrajectoryItemKind::User, TrajectoryItemKind::Compaction]
        );
        assert_eq!(items[1].output.as_deref(), Some(SUMMARY));
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
