//! The stream facts ingest reads from one event's conversation entries alone:
//! roles, call ids, item ids, and whether a user entry has text.

use serde_json::Value;

use crate::usage::conversation::{Conversation, Entry};

/// Roles of the entries the model produced.
pub(super) const MODEL_ROLES: [&str; 3] = ["assistant", "reasoning", "tool_call"];

/// Prefix that keeps item ids apart from call ids among entry identities.
const ITEM_PREFIX: &str = "item:";

impl Entry {
    fn is_model_side(&self) -> bool {
        MODEL_ROLES.contains(&self.role.as_str())
    }

    /// The item id of a model-side entry, with [`ITEM_PREFIX`].
    fn item(&self) -> Option<String> {
        self.item_id
            .as_deref()
            .filter(|_| self.is_model_side())
            .map(|item_id| format!("{ITEM_PREFIX}{item_id}"))
    }

    /// The provider-made ids of the entry: its item, then the call id of a
    /// `tool_call` entry.
    fn identities(&self) -> impl Iterator<Item = String> {
        let call = self.call_id.clone().filter(|_| self.role == "tool_call");
        self.item().into_iter().chain(call)
    }

    /// The entry's key in a skeleton: its call id, else its item.
    fn key(&self) -> Option<String> {
        self.call_id.clone().or_else(|| self.item())
    }
}

/// The entries of the event's conversation; none when it is not completely
/// decoded.
fn conversation_entries(metadata: &Value) -> Vec<Entry> {
    Conversation::read(metadata)
        .map(|conversation| conversation.entries)
        .unwrap_or_default()
}

/// One skeleton entry: an entry's role and key, its call id, else its
/// `item:`-prefixed item id.
pub type SkeletonEntry = (String, Option<String>);

/// The `(role, key)` of each entry in order.
fn skeleton(entries: &[Entry]) -> Vec<SkeletonEntry> {
    entries
        .iter()
        .map(|entry| (entry.role.clone(), entry.key()))
        .collect()
}

/// Call ids of the `tool_result` entries, in order.
fn result_ids(entries: &[Entry]) -> Vec<String> {
    entries
        .iter()
        .filter(|entry| entry.role == "tool_result")
        .filter_map(|entry| entry.call_id.clone())
        .collect()
}

/// Identities of the entries, in order.
fn identities(entries: &[Entry]) -> Vec<String> {
    entries.iter().flat_map(Entry::identities).collect()
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
/// Facts of one request event, read from its conversation entries alone.
pub struct RequestFacts {
    /// Index after the last model-side entry, or 0 without one; the entries
    /// from here on are the request's new part.
    pub new_from: i32,
    /// The last identity in the last run, the run reaching from the last
    /// model-side entry back through model-side and `tool_result` entries to
    /// any other role or the start: the call id of a `tool_call` entry, else
    /// the `item:`-prefixed item id of a model-side entry.
    pub last_identity: Option<String>,
    /// Roles of the model-side entries after the one holding
    /// `last_identity`, in order; none of them has an identity. Empty without
    /// `last_identity`.
    pub trailing: Vec<String>,
    /// Without model-side entries, the call ids of every `tool_result`
    /// entry; otherwise empty.
    pub result_ids: Vec<String>,
    /// Whether the new part holds a `user` entry with text and no
    /// `tool_result` entry.
    pub starts_turn: bool,
    /// The last `user` entry with text in the new part.
    pub label: Option<Label>,
}

/// Most UTF-16 code units of a label's text.
const LABEL_TEXT_UNITS: usize = 50;

#[derive(Clone, Debug, PartialEq, Eq)]
/// Where a request's new user words are, and how they begin.
pub struct Label {
    /// Index of the entry among the request's entries.
    pub index: i32,
    /// The entry's text, at most its first 50 UTF-16 code units, never
    /// ending inside a character.
    pub text: String,
    /// Length of the entry's whole text in UTF-16 code units.
    pub length: i32,
}

impl Label {
    /// A label from its stored columns; none unless all three are there.
    pub fn from_columns(
        index: Option<i32>,
        text: Option<String>,
        length: Option<i32>,
    ) -> Option<Self> {
        Some(Self {
            index: index?,
            text: text?,
            length: length?,
        })
    }

    /// The last `user` entry with text from `new_from` on.
    fn of(entries: &[Entry], new_from: usize) -> Option<Self> {
        let offset = entries[new_from..]
            .iter()
            .rposition(|entry| entry.role == "user" && !entry.text.is_empty())?;
        let index = new_from.saturating_add(offset);
        let text = &entries[index].text;
        let mut units = 0_usize;
        let end = text
            .char_indices()
            .find(|(_, character)| {
                units = units.saturating_add(character.len_utf16());
                units > LABEL_TEXT_UNITS
            })
            .map_or(text.len(), |(at, _)| at);
        Some(Self {
            index: i32::try_from(index).unwrap_or(i32::MAX),
            text: text[..end].to_owned(),
            length: i32::try_from(text.encode_utf16().count()).unwrap_or(i32::MAX),
        })
    }
}

impl RequestFacts {
    /// Reads the facts of a request event from its metadata.
    /// core logic of connect exchanges.
    pub fn read(metadata: &Value) -> Self {
        let entries = conversation_entries(metadata);
        let Some(last) = entries.iter().rposition(Entry::is_model_side) else {
            return Self {
                new_from: 0,
                last_identity: None,
                trailing: Vec::new(),
                result_ids: result_ids(&entries),
                starts_turn: starts_turn(&entries),
                label: Label::of(&entries, 0),
            };
        };
        let start = entries[..last]
            .iter()
            .rposition(|entry| !entry.is_model_side() && entry.role != "tool_result")
            .map_or(0, |other| other.saturating_add(1));
        let run = &entries[start..=last];
        let held = run
            .iter()
            .rposition(|entry| entry.identities().next().is_some());
        let last_identity = held.and_then(|held| run[held].identities().last());
        let trailing = held.map_or_else(Vec::new, |held| {
            run[held.saturating_add(1)..]
                .iter()
                .filter(|entry| entry.is_model_side())
                .map(|entry| entry.role.clone())
                .collect()
        });
        let new_from = last.saturating_add(1);
        Self {
            new_from: i32::try_from(new_from).unwrap_or(i32::MAX),
            last_identity,
            trailing,
            result_ids: Vec::new(),
            starts_turn: starts_turn(&entries[new_from..]),
            label: Label::of(&entries, new_from),
        }
    }
}

/// Whether a new part opens a turn: it has user text and no tool result.
fn starts_turn(new_part: &[Entry]) -> bool {
    new_part
        .iter()
        .any(|entry| entry.role == "user" && !entry.text.is_empty())
        && !new_part.iter().any(|entry| entry.role == "tool_result")
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
/// Facts of one response event, read from its conversation entries alone.
pub struct ResponseFacts {
    /// Identities of every entry, in order: for each entry, the
    /// `item:`-prefixed item id of a model-side entry, then the call id of a
    /// `tool_call` entry.
    pub emitted_ids: Vec<String>,
    /// Skeleton of every entry.
    pub output_skeleton: Vec<SkeletonEntry>,
}

impl ResponseFacts {
    /// Reads the facts of a response event from its metadata.
    pub fn read(metadata: &Value) -> Self {
        let entries = conversation_entries(metadata);
        Self {
            emitted_ids: identities(&entries),
            output_skeleton: skeleton(&entries),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{Label, RequestFacts, ResponseFacts};
    use crate::usage::streams::fixtures::{
        assistant, call, conversation, ids, item, reasoning, result, skeleton, system, user,
    };

    #[test]
    fn request_facts_follow_the_last_run_and_the_new_part() {
        // A tool loop resent with its history: the last run walks back
        // through the client's tool result to the user entry.
        let facts = RequestFacts::read(&conversation(&json!([
            system("rules"),
            user("fix it"),
            reasoning(),
            call("a"),
            result("a"),
            reasoning(),
            call("b"),
            result("b")
        ])));
        assert_eq!(
            facts,
            RequestFacts {
                new_from: 7,
                last_identity: Some("b".to_owned()),
                trailing: Vec::new(),
                result_ids: Vec::new(),
                starts_turn: false,
                label: None,
            }
        );
        // New user text after the last answer opens a turn; the answer has no
        // identity, so it trails the last call.
        let facts = RequestFacts::read(&conversation(&json!([
            user("fix it"),
            call("a"),
            result("a"),
            assistant("fixed"),
            system("<reminder>"),
            user("next")
        ])));
        assert_eq!(facts.new_from, 4_i32);
        assert_eq!(facts.last_identity.as_deref(), Some("a"));
        assert_eq!(facts.trailing, ["assistant"]);
        assert!(facts.starts_turn);
        assert_eq!(facts.result_ids, Vec::<String>::new());
        // Without model output, every entry is new and tool results answer
        // calls that live elsewhere.
        assert_eq!(
            RequestFacts::read(&conversation(&json!([
                result("a"),
                result("b"),
                user("go")
            ]))),
            RequestFacts {
                new_from: 0,
                last_identity: None,
                trailing: Vec::new(),
                result_ids: ids(&["a", "b"]),
                starts_turn: false,
                label: Some(Label {
                    index: 2,
                    text: "go".to_owned(),
                    length: 2,
                }),
            }
        );
        // A run without identities has nothing to align on and nothing
        // trailing; the run stops at the user entry before it.
        let facts = RequestFacts::read(&conversation(&json!([
            call("a"),
            result("a"),
            user("thanks"),
            assistant("sure")
        ])));
        assert_eq!((facts.last_identity, facts.trailing), (None, Vec::new()));
        // Empty user text opens no turn; a request without a complete
        // conversation has no facts.
        assert!(!RequestFacts::read(&conversation(&json!([assistant("a"), user("")]))).starts_turn);
        assert_eq!(RequestFacts::read(&json!({})), RequestFacts::default());
        assert_eq!(
            RequestFacts::read(&json!({"conversation": {
                "entries": [assistant("a"), user("next")],
                "completeness": "partial"
            }})),
            RequestFacts::default()
        );
    }

    #[test]
    fn response_facts_list_emitted_calls_and_the_output_skeleton() {
        assert_eq!(
            ResponseFacts::read(&conversation(&json!([
                reasoning(),
                assistant("let me look"),
                call("a"),
                call("b"),
                {"role": "tool_result", "text": "found", "call_id": "s"}
            ]))),
            ResponseFacts {
                emitted_ids: ids(&["a", "b"]),
                output_skeleton: skeleton(&[
                    ("reasoning", None),
                    ("assistant", None),
                    ("tool_call", Some("a")),
                    ("tool_call", Some("b")),
                    ("tool_result", Some("s"))
                ]),
            }
        );
        assert_eq!(
            ResponseFacts::read(&json!({"conversation": {}})),
            ResponseFacts::default()
        );
    }

    #[test]
    fn identities_are_call_ids_and_prefixed_item_ids_of_model_output() {
        // OpenAI Responses: every output item carries its id, and a function
        // call its call id as well.
        let output = [
            item(reasoning(), "rs_1"),
            item(assistant("let me look"), "msg_1"),
            item(call("a"), "fc_1"),
        ];
        let facts = ResponseFacts::read(&conversation(&json!(output)));
        assert_eq!(
            facts.emitted_ids,
            ids(&["item:rs_1", "item:msg_1", "item:fc_1", "a"])
        );
        assert_eq!(
            facts.output_skeleton,
            skeleton(&[
                ("reasoning", Some("item:rs_1")),
                ("assistant", Some("item:msg_1")),
                ("tool_call", Some("a"))
            ]),
            "a skeleton keys entries by call id, else by item id"
        );
        // Entries of one item share its id; item ids of other roles are no
        // identities; a call's last identity is its call id.
        let mut request = vec![item(user("go"), "u_1")];
        request.extend(output.clone());
        request.extend([
            item(result("a"), "fco_1"),
            item(assistant("done,"), "msg_2"),
            item(assistant("really"), "msg_2"),
        ]);
        let facts = RequestFacts::read(&conversation(&json!(request)));
        assert_eq!(facts.last_identity.as_deref(), Some("item:msg_2"));
        assert_eq!(facts.trailing, Vec::<String>::new());
        let mut request = vec![user("go")];
        request.extend(output);
        request.extend([result("a"), reasoning(), assistant("done")]);
        let facts = RequestFacts::read(&conversation(&json!(request)));
        assert_eq!(facts.last_identity.as_deref(), Some("a"));
        assert_eq!(
            facts.trailing,
            ["reasoning", "assistant"],
            "trailing roles keep reasoning and leave out tool results"
        );
    }

    #[test]
    fn a_label_is_the_last_user_text_of_the_new_part_cut_to_fifty_units() {
        let label = |entries: &serde_json::Value| RequestFacts::read(&conversation(entries)).label;
        // Injected context comes first; the user's own words come last.
        let words = format!("{}😀tail", "a".repeat(49));
        assert_eq!(
            label(&json!([
                user("old question"),
                assistant("old answer"),
                user("<system-reminder>"),
                user(&words),
                user("")
            ])),
            Some(Label {
                index: 3,
                text: "a".repeat(49),
                length: 55,
            }),
            "the cut never splits the emoji's surrogate pair"
        );
        assert_eq!(
            label(&json!([user("ask"), call("a"), result("a")])),
            None,
            "user text before the model output is not new"
        );
    }
}
