//! Read-only projection of archived provider bodies; the archive is never edited.

use std::collections::{BTreeMap, HashMap, HashSet};

use serde_json::Value;

use super::{TrajectoryItem, TrajectoryItemKind, UsageEventResponse};
use crate::usage::provider_body::{
    Message, bodies, message, request_messages, request_payload, text,
};

impl Message {
    fn matches(&self, other: &Self) -> bool {
        self.role == other.role
            && self.text == other.text
            && match (&self.id, &other.id) {
                (Some(left), Some(right)) => left == right,
                _ => true,
            }
    }

    fn kind(&self) -> TrajectoryItemKind {
        match self.role.as_str() {
            "system" | "developer" => TrajectoryItemKind::Context,
            "user" => TrajectoryItemKind::User,
            "assistant" | "reasoning" => TrajectoryItemKind::Assistant,
            _ => TrajectoryItemKind::Request,
        }
    }
}

#[derive(Default)]
pub(super) struct RawProjection {
    streams: HashMap<usize, StreamHistory>,
}

#[derive(Default)]
struct StreamHistory {
    history: Vec<Message>,
    contexts: HashSet<(String, String)>,
}

impl RawProjection {
    pub(super) fn project(
        &mut self,
        event: &UsageEventResponse,
        stream: &mut usize,
        stream_ids: &mut HashMap<u64, usize>,
    ) -> Option<Vec<TrajectoryItem>> {
        let values = bodies(&event.metadata)?;
        let request = event.event_type == "request";
        let (messages, incremental) = if request {
            let payload = request_payload(&values)?;
            let messages = request_messages(payload);
            let incremental = payload
                .get("previous_response_id")
                .is_some_and(Value::is_string);
            if !incremental
                || payload.get("instructions").is_some()
                || payload.get("system").is_some()
            {
                let context = messages
                    .iter()
                    .filter(|message| message.kind() == TrajectoryItemKind::Context)
                    .map(|message| format!("{}: {}", message.role, message.text))
                    .collect::<Vec<_>>()
                    .join("\n\n");
                let next = stream_ids.len();
                *stream = *stream_ids.entry(super::hash_str(&context)).or_insert(next);
            }
            (messages, incremental)
        } else {
            let mut messages = response_messages(&values);
            if messages.is_empty()
                && let Some(text) = event.text.as_ref().filter(|v| !v.is_empty())
            {
                messages.push(Message {
                    id: None,
                    role: "assistant".to_owned(),
                    text: text.clone(),
                });
            }
            (messages, false)
        };
        // Incomplete/unrecognized response formats still have the older parsed
        // text projection. Archival success must not depend on this parser.
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
                    .insert((message.role.clone(), message.text.clone()))
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
                    old.matches(&message) && (message.id.is_some() || message.role != "user")
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
}

fn response_messages(values: &[Value]) -> Vec<Message> {
    let mut items = BTreeMap::new();
    let mut anthropic_blocks = BTreeMap::<u64, String>::new();
    let mut chat_text = String::new();
    for value in values {
        let kind = value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let index = value.get("index").and_then(Value::as_u64).unwrap_or(0);
        if kind == "content_block_start" {
            if let Some(block) = value.get("content_block") {
                anthropic_blocks.insert(index, text(block));
            }
        } else if kind == "content_block_delta"
            && let Some(delta) = value.get("delta")
        {
            anthropic_blocks
                .entry(index)
                .or_default()
                .push_str(&text(delta));
        }
        if let Some(delta) = value
            .pointer("/choices/0/delta/content")
            .and_then(Value::as_str)
        {
            chat_text.push_str(delta);
        }
        let response = value.get("response").unwrap_or(value);
        if let Some(output) = response.get("output").and_then(Value::as_array) {
            for (index, value) in output.iter().enumerate() {
                if let Some(message) = message(value, "assistant") {
                    items.insert(u64::try_from(index).unwrap_or(u64::MAX), message);
                }
            }
        } else if value.get("type").and_then(Value::as_str) == Some("response.output_item.done") {
            if let Some(message) = value.get("item").and_then(|v| message(v, "assistant")) {
                items.insert(
                    value
                        .get("output_index")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                    message,
                );
            }
        } else if let Some(message) = value
            .get("choices")
            .and_then(Value::as_array)
            .and_then(|v| v.first())
            .and_then(|v| v.get("message"))
            .and_then(|v| message(v, "assistant"))
        {
            items.insert(0, message);
        } else if (value.get("content").is_some() || value.get("output_text").is_some())
            && let Some(message) = message(value, "assistant")
        {
            items.insert(0, message);
        }
    }
    if items.is_empty() {
        let text = if anthropic_blocks.is_empty() {
            chat_text
        } else {
            anthropic_blocks
                .into_values()
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>()
                .join("\n")
        };
        if !text.is_empty() {
            items.insert(
                0,
                Message {
                    id: None,
                    role: "assistant".to_owned(),
                    text,
                },
            );
        }
    }
    items.into_values().collect()
}
