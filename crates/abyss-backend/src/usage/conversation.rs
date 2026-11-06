//! Typed access to collector-produced conversation projections; no provider decoding.

use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
pub struct Conversation {
    /// One entry per protocol block or item, in provider order.
    pub entries: Vec<Entry>,
    completeness: Completeness,
}

#[derive(Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum Completeness {
    Complete,
    Partial,
    Unsupported,
}

/// One entry as the collector reports it; its role is kept as reported.
#[derive(Clone, Deserialize)]
pub struct Entry {
    pub role: String,
    /// Visible text; a tool call's input, a tool result's output.
    pub text: String,
    /// Identifier pairing a tool call with its result; tool entries only.
    #[serde(default)]
    pub call_id: Option<String>,
    /// Identity of the item the entry belongs to, as the traffic carries it.
    #[serde(default)]
    pub item_id: Option<String>,
}

impl Conversation {
    /// The completely decoded conversation of an event, as the `AgentEvent`
    /// contract defines it.
    pub fn read(conversation: Option<&Value>) -> Option<Self> {
        let conversation: Self = Self::deserialize(conversation?).ok()?;
        (conversation.completeness == Completeness::Complete).then_some(conversation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roles_are_kept_as_reported() {
        let value = serde_json::json!({
            "entries": [
                {"role": "reasoning", "text": "plan", "item_id": "rs_1"},
                {"role": "some_new_role", "text": "other"},
                {"role": "tool_call", "text": "{}", "call_id": "call_1", "name": "exec"},
                {"role": "user", "text": "hello"}
            ],
            "completeness": "complete"
        });
        let conversation = Conversation::read(Some(&value)).expect("complete conversation");
        assert_eq!(
            conversation
                .entries
                .iter()
                .map(|entry| entry.role.as_str())
                .collect::<Vec<_>>(),
            ["reasoning", "some_new_role", "tool_call", "user"]
        );
        let call = &conversation.entries[2];
        assert_eq!(call.call_id.as_deref(), Some("call_1"));
    }

    #[test]
    fn conversations_outside_the_contract_are_not_read() {
        let entries = serde_json::json!([{"role": "user", "text": "hello"}]);
        for conversation in [
            serde_json::json!({"entries": entries}),
            serde_json::json!({"entries": entries, "completeness": "partial"}),
            serde_json::json!({"messages": entries, "completeness": "complete"}),
        ] {
            assert!(Conversation::read(Some(&conversation)).is_none());
        }
        assert!(Conversation::read(None).is_none());
    }
}
