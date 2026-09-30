//! Session names recovered from the Agents' own title-generation calls.
//!
//! Claude Code and Codex ask a model to name each session and record the
//! answer as `{"title": "..."}`. Those calls are captured like any other, so
//! the name can be read back from them without guessing: a call counts only
//! when its request carries an official naming instruction and its response
//! consists solely of title objects that agree.

use serde::Deserialize;

/// Claude Code instruction for naming the session it runs in.
const CLAUDE_CODE_NAMING_INSTRUCTION: &str = "You are naming a coding session";
/// Codex instruction for naming a task; Codex sends it from its own thread.
const CODEX_NAMING_INSTRUCTION: &str = "Generate a concise, single-line task title";
/// Longest title kept, in characters; longer answers are not titles.
const MAX_TITLE_CHARS: usize = 200;

/// Session purpose stored for Codex's standalone naming threads.
pub const TITLE_GENERATION_PURPOSE: &str = "title_generation";

/// Where a naming call's title belongs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TitleTask {
    /// The call names the session it was sent from (Claude Code).
    NamesOwnSession,
    /// The call runs in a separate naming session (Codex); that session is
    /// auxiliary and the title names the naming session itself.
    AuxiliarySession,
}

impl TitleTask {
    /// Recognizes a naming call from its request text.
    pub fn from_request(text: &str) -> Option<Self> {
        if text.contains(CLAUDE_CODE_NAMING_INSTRUCTION) {
            Some(Self::NamesOwnSession)
        } else if text.contains(CODEX_NAMING_INSTRUCTION) {
            Some(Self::AuxiliarySession)
        } else {
            None
        }
    }
}

#[derive(Deserialize)]
struct TitleObject {
    title: String,
}

/// Reads the title from a naming response.
///
/// Streaming clients may repeat the answer, so several identical objects are
/// accepted. Anything else, such as reasoning text or a normal reply to a
/// user who quoted the naming instruction, yields no title.
pub fn title_from_response(text: &str) -> Option<String> {
    let mut title: Option<String> = None;
    for value in serde_json::Deserializer::from_str(text.trim()).into_iter::<TitleObject>() {
        let candidate = value.ok()?.title.trim().to_owned();
        if candidate.is_empty() || candidate.chars().count() > MAX_TITLE_CHARS {
            return None;
        }
        match &title {
            Some(existing) if *existing != candidate => return None,
            Some(_) => {}
            None => title = Some(candidate),
        }
    }
    title
}

/// Returns `true` when a response text could carry a title, so ingestion can
/// skip the pairing lookup for ordinary replies.
pub fn may_carry_title(text: &str) -> bool {
    let trimmed = text.trim_start();
    trimmed.starts_with('{') && trimmed.contains("\"title\"")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_official_naming_instructions() {
        assert_eq!(
            TitleTask::from_request("system: …\n\nYou are naming a coding session so the user…"),
            Some(TitleTask::NamesOwnSession)
        );
        assert_eq!(
            TitleTask::from_request("Generate a concise, single-line task title of at most 36"),
            Some(TitleTask::AuxiliarySession)
        );
        assert_eq!(TitleTask::from_request("please name this session"), None);
    }

    #[test]
    fn reads_single_and_repeated_title_objects() {
        assert_eq!(
            title_from_response(r#"{"title":"Timeline颗粒度细化"}"#).as_deref(),
            Some("Timeline颗粒度细化")
        );
        assert_eq!(
            title_from_response("{\"title\":\"Send a ping\"}\n\n{\"title\":\"Send a ping\"}")
                .as_deref(),
            Some("Send a ping")
        );
    }

    #[test]
    fn rejects_everything_that_is_not_one_agreed_title() {
        for text in [
            "We need respond title only, imperative verb, under 5 words",
            r#"{"title":"A"}{"title":"B"}"#,
            r#"{"title":""}"#,
            r#"{"name":"x"}"#,
            r#"Here is the title: {"title":"x"}"#,
            "",
        ] {
            assert_eq!(title_from_response(text), None, "{text:?}");
        }
        assert_eq!(
            title_from_response(&format!(r#"{{"title":"{}"}}"#, "x".repeat(201))),
            None
        );
    }

    #[test]
    fn prefilters_candidate_responses() {
        assert!(may_carry_title("  {\"title\": \"x\"}"));
        assert!(!may_carry_title("The title is x"));
    }
}
