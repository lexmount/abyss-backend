//! Typed access to collector-produced conversation projections; no provider decoding.

use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
pub struct Conversation {
    pub messages: Vec<Message>,
    /// Whether the first message is the request's own instructions, such as
    /// a system prompt or OpenAI `instructions`; absent means none.
    #[serde(default)]
    pub instructions_present: bool,
    completeness: Completeness,
}

#[derive(Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum Completeness {
    Complete,
    Partial,
    Unsupported,
}

/// One message as the collector reports it; its role is kept as reported.
#[derive(Clone, Deserialize)]
pub struct Message {
    #[serde(default)]
    pub id: Option<String>,
    pub role: String,
    pub text: String,
}

impl Conversation {
    /// The completely decoded conversation of an event, as the `AgentEvent`
    /// contract defines it.
    pub fn read(metadata: &Value) -> Option<Self> {
        let value = metadata.get("conversation")?;
        let conversation: Self = Self::deserialize(value).ok()?;
        (conversation.completeness == Completeness::Complete).then_some(conversation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roles_are_kept_as_reported() {
        let metadata = serde_json::json!({"conversation": {
            "messages": [
                {"role": "reasoning", "text": "plan"},
                {"role": "some_new_role", "text": "other"},
                {"role": "user", "text": "hello"}
            ],
            "instructions_present": false,
            "completeness": "complete"
        }});
        let conversation = Conversation::read(&metadata).expect("complete conversation");
        assert_eq!(
            conversation
                .messages
                .iter()
                .map(|message| message.role.as_str())
                .collect::<Vec<_>>(),
            ["reasoning", "some_new_role", "user"]
        );
    }

    #[test]
    fn conversations_outside_the_contract_are_not_read() {
        let messages = serde_json::json!([{"role": "user", "text": "hello"}]);
        for conversation in [
            serde_json::json!({"messages": messages, "instructions_present": false}),
            serde_json::json!({
                "messages": messages,
                "instructions_present": false,
                "completeness": "partial"
            }),
        ] {
            let metadata = serde_json::json!({"conversation": conversation});
            assert!(Conversation::read(&metadata).is_none());
        }
    }
}
