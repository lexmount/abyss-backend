//! Typed access to collector-produced conversation projections; no provider decoding.

use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
pub struct Conversation {
    pub messages: Vec<Message>,
    #[serde(default)]
    pub previous_response_id: Option<String>,
    #[serde(default)]
    pub instructions_present: bool,
    #[serde(default)]
    completeness: Option<Completeness>,
}

#[derive(Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum Completeness {
    Complete,
    Partial,
    Unsupported,
}

/// Message roles the collector emits. Roles outside this set, including the
/// placeholder `request` from older collectors, read as [`Role::Unknown`].
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    System,
    Developer,
    User,
    Assistant,
    Reasoning,
    Tool,
    #[serde(other)]
    Unknown,
}

impl Role {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::Developer => "developer",
            Self::User => "user",
            Self::Assistant => "assistant",
            Self::Reasoning => "reasoning",
            Self::Tool => "tool",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Clone, Deserialize)]
pub struct Message {
    #[serde(default)]
    pub id: Option<String>,
    pub role: Role,
    pub text: String,
}

impl Conversation {
    /// Whether the event carries a conversation from a collector that reports
    /// completeness. Older collectors stored messages without it; their
    /// requests keep using the flattened text where they did before.
    pub fn declared(metadata: &Value) -> bool {
        metadata.pointer("/conversation/completeness").is_some()
    }

    /// Thread source the Agent declared for the request, read without
    /// decoding its messages.
    pub fn thread_source(metadata: &Value) -> Option<&str> {
        metadata.get("conversation")?.get("thread_source")?.as_str()
    }

    pub fn read(metadata: &Value) -> Option<Self> {
        let value = metadata.get("conversation")?;
        let conversation: Self = Self::deserialize(value).ok()?;
        if conversation
            .completeness
            .as_ref()
            .is_some_and(|state| *state != Completeness::Complete)
        {
            return None;
        }
        Some(conversation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_conversations_read_with_unknown_roles() {
        let metadata = serde_json::json!({"conversation": {"messages": [
            {"role": "request", "text": "placeholder"},
            {"role": "function", "text": "legacy"},
            {"role": "user", "text": "hello"}
        ]}});
        let conversation = Conversation::read(&metadata).expect("legacy shape");
        assert_eq!(
            conversation
                .messages
                .iter()
                .map(|message| message.role)
                .collect::<Vec<_>>(),
            [Role::Unknown, Role::Unknown, Role::User]
        );
    }
}
