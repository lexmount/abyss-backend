//! Session trajectory projection from collector-produced structured messages.
//!
//! Each exchange is placed in a stream by the identities the collector
//! declares, compared only for equality: a request continuing a known
//! response joins that response's stream, else a request naming a stream
//! joins it, and any other request opens a stream of its own. A response
//! joins the stream of the request that produced it.

use std::collections::HashMap;

use super::{TrajectoryItem, TrajectoryItemKind, UsageEventResponse};
use crate::usage::conversation::{Conversation, Message};

impl Message {
    fn matches(&self, other: &Self) -> bool {
        self.role == other.role
            && self.text == other.text
            && match (&self.id, &other.id) {
                (Some(left), Some(right)) => left == right,
                _ => true,
            }
    }
}

/// The stream an event belongs to.
#[derive(Clone, Copy)]
pub(super) struct Placement {
    pub(super) stream: usize,
    /// The request carries only the messages added since the response it
    /// continues.
    incremental: bool,
}

#[derive(Default)]
pub(super) struct ConversationProjection {
    streams: Vec<StreamHistory>,
    /// Stream of each declared stream id.
    keys: HashMap<String, usize>,
    /// Stream of each exchange by the response id it declares.
    responses: HashMap<String, usize>,
}

#[derive(Default)]
struct StreamHistory {
    /// Instructions the latest full request opened with.
    opening: Vec<Message>,
    /// Messages after the opening, as the stream has seen them so far.
    history: Vec<Message>,
}

impl ConversationProjection {
    /// Places an event by the identities its exchange declares.
    pub(super) fn place(&mut self, event: &UsageEventResponse) -> Placement {
        let request = event.event_type == "request";
        let continued = if request {
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
        Placement {
            stream,
            incremental: request && event.previous_response_id.is_some(),
        }
    }

    fn open(&mut self) -> usize {
        self.streams.push(StreamHistory::default());
        self.streams.len().saturating_sub(1)
    }

    pub(super) fn project(
        &mut self,
        event: &UsageEventResponse,
        placement: Placement,
    ) -> Option<Vec<TrajectoryItem>> {
        let conversation = Conversation::read(&event.metadata)?;
        let request = event.event_type == "request";
        // A request that declares its own instructions carries them as its
        // first message. Messages without text, such as tool items whose
        // content the tool fields carry, take no part in the trajectory.
        let mut messages = conversation.messages;
        let opening_len = usize::from(request && conversation.instructions_present);
        let rest = messages
            .split_off(opening_len.min(messages.len()))
            .into_iter()
            .filter(|message| !message.text.is_empty())
            .collect::<Vec<_>>();
        let (opening, messages) = (messages, rest);
        if opening.is_empty() && messages.is_empty() {
            return None;
        }
        let stream = placement.stream;
        let state = self.streams.get_mut(stream)?;
        let item = |message: &Message| {
            let mut item = TrajectoryItem::new(TrajectoryItemKind::Message, stream, event);
            item.role = Some(message.role.clone());
            item.text = Some(message.text.clone());
            item
        };
        let mut added = Vec::new();
        // A response and a continuation carry only messages added since the
        // response they continue, so each one is new; the history keeps them
        // for a later full request that replays them. A continuation does not
        // repeat the conversation's instructions.
        if !request || placement.incremental {
            for message in messages {
                added.push(item(&message));
                state.history.push(message);
            }
            return Some(added);
        }

        // A full request shows its instructions only where they changed, and
        // of its history only what the stream has not seen in that order.
        for message in &opening {
            if !message.text.is_empty() && !state.opening.iter().any(|old| old.matches(message)) {
                added.push(item(message));
            }
        }
        state.opening = opening;

        let mut cursor = 0;
        for message in &messages {
            let found = state.history[cursor..]
                .iter()
                .position(|old| old.matches(message));
            match found {
                Some(offset) => cursor = cursor.saturating_add(offset).saturating_add(1),
                None => added.push(item(message)),
            }
        }
        state.history = messages;
        Some(added)
    }
}
