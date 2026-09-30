//! Projects role-aware snapshots without changing stored request or response text.
//!
//! Match occurrences in order, rather than deduplicating strings globally: a
//! second identical user message is still a new message. Provider item ids are
//! used when available, and incremental requests append their new user input.

use std::collections::{HashMap, HashSet};

use serde::Deserialize;

use super::{TrajectoryItem, TrajectoryItemKind, UsageEventResponse};

#[derive(Clone, Deserialize)]
struct Message {
    #[serde(default)]
    id: Option<String>,
    role: Role,
    text: String,
}

#[derive(Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Role {
    System,
    Developer,
    User,
    Assistant,
    Reasoning,
}

impl Role {
    const fn kind(self) -> TrajectoryItemKind {
        match self {
            Self::System | Self::Developer => TrajectoryItemKind::Context,
            Self::User => TrajectoryItemKind::User,
            Self::Assistant | Self::Reasoning => TrajectoryItemKind::Assistant,
        }
    }

    const fn is_context(self) -> bool {
        matches!(self, Self::System | Self::Developer)
    }
}

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

#[derive(Deserialize)]
struct Snapshot {
    messages: Vec<Message>,
    response_id: Option<String>,
    previous_response_id: Option<String>,
}

#[derive(Default)]
pub(super) struct ConversationMerger {
    history: Vec<Message>,
    contexts: Vec<Message>,
    identified: HashMap<String, Message>,
    /// The same provider response may be observed again after a retry.
    requests: HashSet<String>,
    responses: HashSet<String>,
}

impl ConversationMerger {
    /// None keeps the existing legacy projection for events without snapshots.
    pub(super) fn project(
        &mut self,
        event: &UsageEventResponse,
        stream: usize,
    ) -> Option<Vec<TrajectoryItem>> {
        let snapshot: Snapshot =
            serde_json::from_value(event.metadata.get("conversation")?.clone()).ok()?;
        let is_request = event.event_type == "request";
        if let Some(id) = snapshot.response_id {
            let seen = if is_request {
                &mut self.requests
            } else {
                &mut self.responses
            };
            if !seen.insert(id) {
                return Some(Vec::new());
            }
        }
        let incremental = snapshot.previous_response_id.is_some();
        let mut cursor = 0;
        let mut history = Vec::new();
        let mut added = Vec::new();
        for message in snapshot.messages {
            let known_id = message
                .id
                .as_ref()
                .and_then(|id| self.identified.get(id))
                .is_some_and(|earlier| earlier.matches(&message));
            let already_seen = if message.role.is_context() {
                let known = self
                    .contexts
                    .iter()
                    .any(|earlier| earlier.matches(&message));
                if !known {
                    self.contexts.push(message.clone());
                }
                known
            } else if is_request && !incremental {
                let found = self.history[cursor..]
                    .iter()
                    .position(|earlier| earlier.matches(&message));
                if let Some(offset) = found {
                    cursor = cursor.saturating_add(offset).saturating_add(1);
                }
                history.push(message.clone());
                known_id || found.is_some()
            } else {
                // Incremental user messages represent new occurrences even if
                // they say the same thing as an earlier message. Replayed
                // assistant items can still refer to an observed response.
                let replay = is_request
                    && message.role != Role::User
                    && self.history.iter().any(|earlier| earlier.matches(&message));
                if !known_id && !replay {
                    self.history.push(message.clone());
                }
                known_id || replay
            };
            if let Some(id) = &message.id {
                self.identified.insert(id.clone(), message.clone());
            }
            if !already_seen && !message.text.trim().is_empty() {
                let mut item = TrajectoryItem::new(message.role.kind(), stream, event);
                item.text = Some(message.text);
                added.push(item);
            }
        }
        if is_request && !incremental {
            self.history = history;
        }
        Some(added)
    }
}
