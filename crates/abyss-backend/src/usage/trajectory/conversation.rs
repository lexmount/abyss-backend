//! Session trajectory projection from collector-produced conversation entries.
//!
//! Each exchange is placed in a stream by the identities the collector
//! declares, compared only for equality: a request continuing a known
//! response joins that response's stream, else a request naming a stream
//! joins it, and any other request opens a stream of its own. A response
//! joins the stream of the request that produced it.

use std::collections::HashMap;

use super::{TrajectoryItem, TrajectoryItemKind, UsageEventResponse};
use crate::usage::conversation::Conversation;

#[derive(Default)]
pub(super) struct ConversationProjection {
    /// Number of streams opened so far.
    streams: usize,
    /// Stream of each declared stream id.
    keys: HashMap<String, usize>,
    /// Stream of each exchange by the response id it declares.
    responses: HashMap<String, usize>,
}

impl ConversationProjection {
    /// The stream of an event, by the identities its exchange declares.
    pub(super) fn place(&mut self, event: &UsageEventResponse) -> usize {
        let continued = if event.event_type == "request" {
            event
                .previous_response_id
                .as_ref()
                .and_then(|id| self.responses.get(id).copied())
        } else {
            // The request of the same exchange registered the response id.
            event
                .response_id
                .as_ref()
                .and_then(|id| self.responses.get(id).copied())
        };
        let stream = match (continued, &event.stream_id) {
            (Some(stream), _) => stream,
            (None, Some(key)) => {
                if let Some(&stream) = self.keys.get(key) {
                    stream
                } else {
                    let stream = self.open();
                    self.keys.insert(key.clone(), stream);
                    stream
                }
            }
            (None, None) => self.open(),
        };
        if let Some(id) = &event.response_id {
            self.responses.entry(id.clone()).or_insert(stream);
        }
        stream
    }

    const fn open(&mut self) -> usize {
        self.streams = self.streams.saturating_add(1);
        self.streams.saturating_sub(1)
    }

    /// Every entry the event carries, in order: tool calls and results as
    /// tool items, every other entry with text as a message.
    pub(super) fn project(
        event: &UsageEventResponse,
        stream: usize,
    ) -> Option<Vec<TrajectoryItem>> {
        let items = Conversation::read(&event.metadata)?
            .entries
            .into_iter()
            .filter_map(|entry| {
                let kind = match entry.role.as_str() {
                    "tool_call" => TrajectoryItemKind::ToolCall,
                    "tool_result" => TrajectoryItemKind::ToolResult,
                    _ if entry.text.is_empty() => return None,
                    _ => TrajectoryItemKind::Message,
                };
                let mut item = TrajectoryItem::new(kind, stream, event);
                item.call_id = entry.call_id;
                match kind {
                    TrajectoryItemKind::ToolCall => {
                        item.name = entry.name;
                        item.input = Some(entry.text);
                    }
                    TrajectoryItemKind::ToolResult => {
                        item.output = Some(entry.text);
                        item.output_event_pk = Some(event.id);
                    }
                    _ => {
                        item.role = Some(entry.role);
                        item.text = Some(entry.text);
                    }
                }
                Some(item)
            })
            .collect::<Vec<_>>();
        (!items.is_empty()).then_some(items)
    }
}
