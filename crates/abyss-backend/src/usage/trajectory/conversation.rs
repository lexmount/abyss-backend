//! Session trajectory projection from collector-produced structured messages.

use std::collections::{HashMap, HashSet};

use serde_json::Value;

use super::{TrajectoryItem, TrajectoryItemKind, UsageEventResponse};
use crate::usage::conversation::{Conversation, Message, Role};

impl Message {
    fn matches(&self, other: &Self) -> bool {
        self.role == other.role
            && self.text == other.text
            && match (&self.id, &other.id) {
                (Some(left), Some(right)) => left == right,
                _ => true,
            }
    }

    const fn kind(&self) -> TrajectoryItemKind {
        match self.role {
            Role::System | Role::Developer => TrajectoryItemKind::Context,
            Role::User => TrajectoryItemKind::User,
            Role::Assistant | Role::Reasoning => TrajectoryItemKind::Assistant,
            Role::Tool | Role::Unknown => TrajectoryItemKind::Request,
        }
    }
}

#[derive(Default)]
pub(super) struct ConversationProjection {
    streams: HashMap<usize, StreamHistory>,
    /// Stream of each response by its provider message id, so an Anthropic
    /// thread `continue` naming that message joins the stream it continues.
    messages: HashMap<String, usize>,
    /// Stream of the latest Anthropic thread request.
    thread: Option<usize>,
}

#[derive(Default)]
struct StreamHistory {
    history: Vec<Message>,
    contexts: HashSet<(Role, String)>,
}

impl ConversationProjection {
    pub(super) fn project(
        &mut self,
        event: &UsageEventResponse,
        stream: &mut usize,
        stream_ids: &mut HashMap<u64, usize>,
    ) -> Option<Vec<TrajectoryItem>> {
        // Older collectors stored messages without completeness; their events keep
        // the flattened-text presentation they had before structured conversations.
        if !Conversation::declared(&event.metadata) {
            return None;
        }
        let conversation = Conversation::read(&event.metadata)?;
        let request = event.event_type == "request";
        let (messages, incremental) = if request {
            let messages = conversation
                .messages
                .into_iter()
                .filter(|m| m.role != Role::Tool)
                .collect::<Vec<_>>();
            if messages.is_empty() {
                return None;
            }
            let incremental = self.request_stream(
                event,
                conversation.previous_response_id.is_some(),
                conversation.instructions_present,
                &messages,
                stream,
                stream_ids,
            );
            (messages, incremental)
        } else {
            if let Some(id) = conversation.messages.iter().find_map(|m| m.id.as_deref()) {
                self.messages.insert(id.to_owned(), *stream);
            }
            let mut messages = conversation
                .messages
                .into_iter()
                .filter(|m| m.role != Role::Tool)
                .collect::<Vec<_>>();
            if messages.is_empty()
                && let Some(text) = event.text.as_ref().filter(|v| !v.is_empty())
            {
                messages.push(Message {
                    id: None,
                    role: Role::Assistant,
                    text: text.clone(),
                });
            }
            (messages, false)
        };
        // Legacy response text remains available when no message was projected.
        if !request && messages.is_empty() {
            return None;
        }
        let state = self.streams.entry(*stream).or_default();
        let mut cursor = 0;
        let mut next = Vec::new();
        let mut added = Vec::new();
        for message in messages {
            let context = message.kind() == TrajectoryItemKind::Context;
            let known = if context {
                !state
                    .contexts
                    .insert((message.role, stable_context(&message.text)))
            } else if request && !incremental {
                let found = state.history[cursor..]
                    .iter()
                    .position(|old| old.matches(&message));
                if let Some(offset) = found {
                    cursor = cursor.saturating_add(offset).saturating_add(1);
                }
                found.is_some()
            } else if request {
                state.history.iter().any(|old| {
                    old.matches(&message) && (message.id.is_some() || message.role != Role::User)
                })
            } else {
                false
            };
            if !context {
                if request && !incremental {
                    next.push(message.clone());
                } else if !known {
                    state.history.push(message.clone());
                }
            }
            if !known && !message.text.is_empty() {
                let mut item = TrajectoryItem::new(message.kind(), *stream, event);
                item.text = Some(message.text);
                added.push(item);
            }
        }
        if request && !incremental {
            state.history = next;
        }
        Some(added)
    }

    /// Points `stream` at the stream this request belongs to and tells
    /// whether the request sends only messages added since an earlier one.
    fn request_stream(
        &mut self,
        event: &UsageEventResponse,
        responds_to_previous: bool,
        instructions_present: bool,
        messages: &[Message],
        stream: &mut usize,
        stream_ids: &mut HashMap<u64, usize>,
    ) -> bool {
        // An Anthropic thread `continue` sends only the messages added
        // since the response it names, and a system prompt that is only
        // the billing header, so it neither repeats nor names its stream.
        let thread = event.metadata.get("claude_thread");
        let continued = thread
            .and_then(|thread| thread.get("type"))
            .and_then(Value::as_str)
            == Some("continue");
        let incremental = continued || responds_to_previous;
        let continued_stream = continued
            .then(|| {
                thread
                    .and_then(|thread| thread.get("previous_message_id"))
                    .and_then(Value::as_str)
                    .and_then(|id| self.messages.get(id).copied())
                    // The named response was not captured: thread
                    // requests of a session follow one another.
                    .or(self.thread)
            })
            .flatten();
        if let Some(continued) = continued_stream {
            *stream = continued;
        } else if !incremental || instructions_present {
            // A stream is named by the prompt the conversation opens with.
            // System messages later in the conversation are part of it:
            // counting them made each new one start another stream.
            let context = messages
                .iter()
                .take_while(|message| message.kind() == TrajectoryItemKind::Context)
                .map(|message| {
                    format!(
                        "{}: {}",
                        message.role.as_str(),
                        stable_context(&message.text)
                    )
                })
                .collect::<Vec<_>>()
                .join("\n\n");
            let next = stream_ids.len();
            *stream = *stream_ids.entry(super::hash_str(&context)).or_insert(next);
        }
        if thread.is_some() {
            self.thread = Some(*stream);
        }
        incremental
    }
}

/// Claude Code adds two lines to its system prompt that change on every
/// request: a leading billing header whose request hash and previous-request
/// id differ each time, for example `x-anthropic-billing-header:
/// cc_version=2.1.288.9e5; cc_entrypoint=cli; cch=e4f32; cc_is_subagent=true;
/// cc_prev_req=req_…; cc_prompt_id=…;`, and a trailing context budget such as
/// `<total_tokens>14964936 tokens left</total_tokens>`. Counting them made
/// every request a new stream that replayed the whole conversation, so
/// streams and system prompts are told apart without them. The stored conversation
/// keeps both, so they can still be counted for billing.
const BILLING_HEADER_PREFIX: &str = "x-anthropic-billing-header:";
const TOKEN_BUDGET_OPEN: &str = "<total_tokens>";
const TOKEN_BUDGET_CLOSE: &str = "</total_tokens>";

/// A system prompt without the lines that change on every request.
fn stable_context(text: &str) -> String {
    text.lines()
        .filter(|line| !changes_every_request(line.trim()))
        .collect::<Vec<_>>()
        .join("\n")
}

fn changes_every_request(line: &str) -> bool {
    line.starts_with(BILLING_HEADER_PREFIX)
        || (line.starts_with(TOKEN_BUDGET_OPEN) && line.ends_with(TOKEN_BUDGET_CLOSE))
}
