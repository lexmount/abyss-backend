//! Session names: the title an Agent generated for its own session, else the
//! session's first user input.
//!
//! The collector reports a naming call as event fields: both sides carry a
//! `purpose`, and the response carries the `session_title` it produced for its
//! own session. Titles are therefore read without interpreting message text.
//!
//! An Agent may instead name a task from a separate thread whose requests do
//! not say which session they name. The collector marks those requests with
//! `naming_thread`; their sessions are only marked, and the task sessions keep
//! their first user input.

use serde_json::Value;

use super::conversation::Conversation;

/// Session purpose stored for threads that name another session.
pub const TITLE_GENERATION_PURPOSE: &str = "title_generation";
/// Longest default title, in characters.
const DEFAULT_TITLE_CHARS: usize = 80;

/// What one stored event contributes to its session's name.
#[derive(Debug, PartialEq, Eq)]
pub enum TitleContribution<'a> {
    /// Title the Agent generated for this event's session.
    Agent(&'a str),
    /// An ordinary request; its first user input may name the session.
    UserInput,
    /// The session is a thread that names another session.
    TitleThread,
}

impl<'a> TitleContribution<'a> {
    /// Classifies an event from collector fields without reading its entries.
    /// Requests the Agent made for its own purposes, such as naming calls, are
    /// not user input.
    pub fn of(event_type: &str, metadata: &'a Value, naming_thread: bool) -> Option<Self> {
        if let Some(title) = metadata.get("session_title").and_then(Value::as_str) {
            return Some(Self::Agent(title));
        }
        if event_type != "request" || metadata.get("purpose").is_some() {
            return None;
        }
        if naming_thread {
            return Some(Self::TitleThread);
        }
        Some(Self::UserInput)
    }
}

/// Default name: the first non-empty user input, on one line, at most 80 chars.
pub fn default_title(metadata: &Value) -> Option<String> {
    let input = Conversation::read(metadata)?
        .entries
        .into_iter()
        .find(|entry| entry.role == "user" && !entry.text.trim().is_empty())?
        .text;
    let input = input.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut chars = input.chars();
    let mut title: String = chars.by_ref().take(DEFAULT_TITLE_CHARS).collect();
    if chars.next().is_some() {
        title.pop();
        title.push('…');
    }
    (!title.is_empty()).then_some(title)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// Metadata from the current collector, which always reports completeness.
    fn request(conversation: &Value) -> Value {
        let mut conversation = conversation.clone();
        if conversation.get("completeness").is_none() {
            conversation["completeness"] = json!("complete");
        }
        json!({"conversation": conversation})
    }

    fn default_of(entries: &Value) -> Option<String> {
        default_title(&request(&json!({"entries": entries})))
    }

    #[test]
    fn default_title_reads_the_first_nonempty_user_input() {
        assert_eq!(
            default_of(&json!([
                {"role":"system","text":"Instructions"},
                {"role":"assistant","text":"Previous reply"},
                {"role":"tool_result","text":"Tool output","call_id":"call_1"},
                {"role":"user","text":"  "},
                {"role":"user","text":" 修复\n  登录问题 "},
                {"role":"user","text":"Second request"}
            ]))
            .as_deref(),
            Some("修复 登录问题")
        );
    }

    #[test]
    fn default_title_truncates_unicode_and_leaves_short_input_intact() {
        for (input, expected) in [
            ("修复登录".to_owned(), "修复登录".to_owned()),
            ("修".repeat(80), "修".repeat(80)),
            ("修".repeat(81), format!("{}…", "修".repeat(79))),
        ] {
            assert_eq!(
                default_of(&json!([{"role":"user","text":input}])),
                Some(expected)
            );
        }
    }

    #[test]
    fn default_title_requires_readable_user_input() {
        for entries in [
            json!([]),
            json!([{"role":"system","text":"Instructions"}]),
            json!([{"role":"user","text":" \n "}]),
            json!([{"role":"tool_result","text":"Output","call_id":"call_1"}]),
        ] {
            assert_eq!(default_of(&entries), None);
        }
        assert_eq!(default_title(&json!({"text":"No role"})), None);
    }

    #[test]
    fn agent_titles_come_only_from_the_collector_field() {
        assert_eq!(
            TitleContribution::of(
                "response",
                &json!({"purpose":"session_title","session_title":"登录修复"}),
                false
            ),
            Some(TitleContribution::Agent("登录修复"))
        );
        // A reply that merely looks like a title object is ordinary output.
        assert_eq!(
            TitleContribution::of(
                "response",
                &request(&json!({"entries":[
                    {"role":"assistant","text":"{\"title\":\"Quoted\"}"}
                ]})),
                false
            ),
            None
        );
    }

    #[test]
    fn naming_requests_are_not_user_input() {
        let mut metadata = request(&json!({"entries":[{"role":"user","text":"<session>…"}]}));
        metadata["purpose"] = json!("session_title");
        assert_eq!(TitleContribution::of("request", &metadata, false), None);
    }

    #[test]
    fn naming_threads_are_marked_from_the_collector_flag() {
        let metadata =
            request(&json!({"entries":[{"role":"user","text":"Generate a concise title"}]}));
        assert_eq!(
            TitleContribution::of("request", &metadata, true),
            Some(TitleContribution::TitleThread)
        );
        assert_eq!(
            TitleContribution::of("request", &metadata, false),
            Some(TitleContribution::UserInput)
        );
    }

    #[test]
    fn incomplete_conversations_give_no_default() {
        for status in ["partial", "unsupported"] {
            let metadata =
                request(&json!({"completeness":status,"entries":[{"role":"user","text":"hello"}]}));
            assert_eq!(default_title(&metadata), None);
        }
    }

    #[test]
    fn content_outside_collector_entries_is_never_interpreted() {
        let metadata = json!({"raw_body":{"parts":[{"content_base64":"e30="}]},
            "conversation":{"entries":[], "completeness":"complete", "provider":{"input":"secret"}}});
        assert_eq!(default_title(&metadata), None);
    }
}
