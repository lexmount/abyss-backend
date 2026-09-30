//! Session names recovered from the Agents' own title-generation calls.
//!
//! Claude Code and Codex ask a model to name each session and record the
//! answer as `{"title": "..."}`. Those calls are captured like any other, so
//! the name can be read back from them without guessing: a call counts only
//! when its request carries an official naming instruction and its response
//! consists solely of title objects that agree.
//!
//! Codex asks from a separate thread whose requests carry no link to the task
//! they name. The naming request quotes the task's user message, and Codex
//! starts the naming turn right after the task turn, so a task turn is matched
//! when it starts close to the naming turn and its user message opens with the
//! quoted prompt.

use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

/// Claude Code instruction for naming the session it runs in.
const CLAUDE_CODE_NAMING_INSTRUCTION: &str = "You are naming a coding session";
/// Codex instruction for naming a task; Codex sends it from its own thread.
const CODEX_NAMING_INSTRUCTION: &str = "Generate a concise, single-line task title";
/// Longest title kept, in characters; longer answers are not titles.
const MAX_TITLE_CHARS: usize = 200;

/// Precedes the quoted task message in a Codex naming request.
const CODEX_PROMPT_MARKER: &str = "User prompt:\n";
/// Widest start gap between a Codex naming turn and the task turn it names.
/// Observed gaps are mostly under half a second and at most about 31 s.
const CODEX_LINK_WINDOW_MS: u64 = 60_000;

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

/// Returns the task message a Codex naming request quotes. Codex may cut long
/// messages short, so the quote can be a prefix of the task message.
pub fn codex_naming_prompt(request_text: &str) -> Option<&str> {
    let (_, prompt) = request_text.rsplit_once(CODEX_PROMPT_MARKER)?;
    Some(prompt.trim()).filter(|prompt| !prompt.is_empty())
}

/// Returns the Codex turn ID an event's collector recorded.
pub fn codex_turn_id(metadata: &Value) -> Option<&str> {
    metadata.get("codex_turn_id")?.as_str()
}

/// Returns the range of Codex turn IDs that start within the link window of
/// `turn_id`. Codex turn IDs are version 7 UUIDs, whose text sorts by start
/// time held in the top 48 bits.
pub fn codex_turn_id_range(turn_id: &str) -> Option<(String, String)> {
    const TIME_SHIFT: u32 = 80;
    let uuid = Uuid::parse_str(turn_id).ok()?;
    if uuid.get_version_num() != 7 {
        return None;
    }
    let started_ms = u64::try_from(uuid.as_u128().checked_shr(TIME_SHIFT)?).ok()?;
    let low =
        u128::from(started_ms.saturating_sub(CODEX_LINK_WINDOW_MS)).checked_shl(TIME_SHIFT)?;
    let high = u128::from(started_ms.saturating_add(CODEX_LINK_WINDOW_MS))
        .checked_shl(TIME_SHIFT)?
        | u128::MAX.checked_shr(u128::BITS - TIME_SHIFT)?;
    Some((
        Uuid::from_u128(low).to_string(),
        Uuid::from_u128(high).to_string(),
    ))
}

/// Returns `true` when a request's latest user message is the task a naming
/// request quoted.
pub fn opens_with_prompt(metadata: &Value, prompt: &str) -> bool {
    metadata
        .pointer("/conversation/messages")
        .and_then(Value::as_array)
        .and_then(|messages| {
            messages
                .iter()
                .rev()
                .find(|message| message.get("role").and_then(Value::as_str) == Some("user"))
        })
        .and_then(|message| message.get("text")?.as_str())
        .is_some_and(|text| text.trim_start().starts_with(prompt))
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
    fn reads_the_quoted_task_from_codex_naming_requests() {
        assert_eq!(
            codex_naming_prompt("Generate a concise title.\n\nUser prompt:\n画一只鹈鹕\n"),
            Some("画一只鹈鹕")
        );
        assert_eq!(codex_naming_prompt("User prompt:\n  "), None);
        assert_eq!(codex_naming_prompt("no quote"), None);
    }

    #[test]
    fn bounds_codex_turn_ids_by_start_time() {
        let (low, high) =
            codex_turn_id_range("01a0f320-138e-7d21-99ea-03a7ae2a9b50").expect("UUIDv7");
        assert!(low.as_str() < "01a0f320-1350-71c2-9c93-5fc5fd7a6bfb");
        assert!(high.as_str() > "01a0f320-1350-71c2-9c93-5fc5fd7a6bfb");
        assert!(high.as_str() < "01a0f321-0000-7000-8000-000000000000");
        assert_eq!(codex_turn_id_range("turn-1"), None);
        assert_eq!(
            codex_turn_id_range("f81d4fae-7dec-11d0-a765-00a0c91e6bf6"),
            None
        );
    }

    #[test]
    fn matches_the_latest_user_message_by_prefix() {
        let metadata = serde_json::json!({"conversation": {"messages": [
            {"role": "user", "text": "<environment_context>…"},
            {"role": "user", "text": "画一只鹈鹕骑自行车，不要联网"},
            {"role": "assistant", "text": "好的"},
        ]}});
        assert!(opens_with_prompt(&metadata, "画一只鹈鹕"));
        assert!(!opens_with_prompt(&metadata, "<environment_context>"));
        assert!(!opens_with_prompt(&serde_json::json!({}), "画"));
    }

    #[test]
    fn prefilters_candidate_responses() {
        assert!(may_carry_title("  {\"title\": \"x\"}"));
        assert!(!may_carry_title("The title is x"));
    }
}
