//! The searchable pieces of one event: the `text` and the `name` of each of
//! its conversation entries.

use serde_json::Value;

use crate::search::PieceField;

impl PieceField {
    /// The stored and serialized name of the field.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Name => "name",
            Self::Text => "text",
        }
    }

    /// Reads a stored field name.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "name" => Some(Self::Name),
            "text" => Some(Self::Text),
            _ => None,
        }
    }
}

/// One nonempty string value of one entry.
#[derive(Debug, PartialEq, Eq)]
pub struct Piece<'a> {
    /// Position of the entry in `metadata.conversation.entries`.
    pub entry_index: usize,
    /// Which value of the entry it is.
    pub field: PieceField,
    /// The value as the event holds it.
    pub text: &'a str,
}

/// The pieces of `metadata.conversation.entries`, in entry order and `name`
/// before `text` within an entry. Nothing outside the entries is read, so
/// headers and other collector metadata are never searched; an empty value
/// is not a piece, and entries are never joined.
#[must_use]
pub fn pieces(metadata: &Value) -> Vec<Piece<'_>> {
    metadata
        .pointer("/conversation/entries")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .enumerate()
        .flat_map(|(entry_index, entry)| {
            [PieceField::Name, PieceField::Text]
                .into_iter()
                .filter_map(move |field| {
                    let text = entry.get(field.as_str())?.as_str()?;
                    (!text.is_empty()).then_some(Piece {
                        entry_index,
                        field,
                        text,
                    })
                })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{Piece, PieceField, pieces};

    #[test]
    fn reads_the_text_and_name_of_every_entry_and_nothing_else() {
        let metadata = json!({
            "request_headers": [{"name": "authorization", "value": "Bearer secret"}],
            "conversation": {"entries": [
                {"role": "system", "text": "You are an agent"},
                {"role": "reasoning", "text": ""},
                {"role": "tool_call", "text": "{\"command\":\"cargo test\"}", "call_id": "c1", "name": "Bash"},
                {"role": "tool_result", "text": "all tests passed", "call_id": "c1"},
                {"role": "unknown"},
                {"role": "user", "text": 3_i64, "name": ""}
            ]}
        });

        assert_eq!(
            pieces(&metadata),
            [
                Piece {
                    entry_index: 0,
                    field: PieceField::Text,
                    text: "You are an agent"
                },
                Piece {
                    entry_index: 2,
                    field: PieceField::Name,
                    text: "Bash"
                },
                Piece {
                    entry_index: 2,
                    field: PieceField::Text,
                    text: "{\"command\":\"cargo test\"}"
                },
                Piece {
                    entry_index: 3,
                    field: PieceField::Text,
                    text: "all tests passed"
                },
            ]
        );
        assert!(pieces(&json!({})).is_empty());
        assert!(pieces(&json!({"conversation": {"entries": "none"}})).is_empty());
    }

    #[test]
    fn field_names_round_trip_and_order_name_first() {
        for field in [PieceField::Name, PieceField::Text] {
            assert_eq!(PieceField::parse(field.as_str()), Some(field));
            assert_eq!(
                serde_json::to_value(field).expect("serializes"),
                json!(field.as_str())
            );
        }
        assert_eq!(PieceField::parse("role"), None);
        assert!(PieceField::Name < PieceField::Text);
    }
}
