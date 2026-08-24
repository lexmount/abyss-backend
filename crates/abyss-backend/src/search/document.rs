//! Search document projection and bounded extraction from usage-event metadata.
//!
//! Only explicitly allowlisted metadata enters the index. Extraction limits
//! depth, value count, and character length to bound CPU, allocation, request
//! size, and accidental indexing of unrelated collector metadata. PostgreSQL
//! retains the complete source event and remains the recovery source.

use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

use crate::db::models::UsageEvent;

use super::elasticsearch::{HIGHLIGHT_END, HIGHLIGHT_START};

const MAX_CONTENT_CHARACTERS: usize = 1_000_000;
const MAX_METADATA_FIELD_CHARACTERS: usize = 100_000;
const MAX_EXTRACTED_VALUES: usize = 64;
const MAX_JSON_DEPTH: usize = 8;

/// Elasticsearch representation of one immutable usage event.
#[derive(Serialize)]
pub struct SearchDocument {
    /// Source usage-event primary key and Elasticsearch document identifier.
    pub event_pk: Uuid,
    /// Owner identifier used as a mandatory search filter.
    pub user_id: Uuid,
    /// Parent session key used to collapse event hits into sessions.
    pub session_pk: Uuid,
    /// Agent-native session identifier searchable as text and keyword.
    pub session_id: String,
    /// Parent turn primary key.
    pub turn_pk: Uuid,
    /// Backend-normalized turn number.
    pub turn_index: i32,
    /// Canonical Agent name used for exact filtering.
    pub agent_name: String,
    /// Canonical LLM provider slug used for exact filtering.
    pub llm_provider: String,
    /// Provider-specific model identifier used for exact filtering.
    pub llm_model: String,
    /// Request or response value used for exact filtering.
    pub event_type: String,
    /// Collector observation time used for range filtering.
    pub observed_at: chrono::DateTime<chrono::Utc>,
    /// Bounded prompt or response text.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    /// Bounded names extracted from tool-call metadata.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tool_names: Vec<String>,
    /// Bounded raw tool inputs and outputs.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tool_content: Vec<String>,
    /// Bounded command strings extracted from structured tool input.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub commands: Vec<String>,
    /// Bounded paths and working directories extracted from allowlisted fields.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub file_paths: Vec<String>,
}

impl SearchDocument {
    /// Projects one source event into the strict Elasticsearch mapping.
    #[must_use]
    pub fn from_event(event: UsageEvent) -> Self {
        let metadata = SearchableMetadata::extract(&event.metadata);
        Self {
            event_pk: event.id,
            user_id: event.user_id,
            session_pk: event.session_pk,
            session_id: sanitize_search_text(&event.session_id),
            turn_pk: event.turn_pk,
            turn_index: event.turn_index,
            agent_name: event.agent_name,
            llm_provider: event.llm_provider,
            llm_model: event.llm_model,
            event_type: event.event_type,
            observed_at: event.observed_at,
            content: event
                .text
                .map(|text| sanitize_search_text(&text))
                .map(|text| truncate_characters(text, MAX_CONTENT_CHARACTERS)),
            tool_names: metadata.tool_names,
            tool_content: metadata.tool_content,
            commands: metadata.commands,
            file_paths: metadata.file_paths,
        }
    }
}

struct SearchableMetadata {
    tool_names: Vec<String>,
    tool_content: Vec<String>,
    commands: Vec<String>,
    file_paths: Vec<String>,
}

impl SearchableMetadata {
    fn extract(metadata: &Value) -> Self {
        let mut extracted = Self {
            tool_names: Vec::new(),
            tool_content: Vec::new(),
            commands: Vec::new(),
            file_paths: Vec::new(),
        };

        if let Some(working_directory) = metadata.get("working_directory").and_then(Value::as_str) {
            extracted.push_file_path(working_directory);
        }

        // Deliberately ignore every top-level key except working_directory and
        // content_segments so headers, credentials, and image data stay out of
        // the derived index even when present in raw metadata.
        let Some(segments) = metadata.get("content_segments").and_then(Value::as_array) else {
            return extracted;
        };
        for segment in segments.iter().take(MAX_EXTRACTED_VALUES) {
            let Some(object) = segment.as_object() else {
                continue;
            };
            match object.get("type").and_then(Value::as_str) {
                Some("tool_call") => {
                    if let Some(name) = object.get("name").and_then(Value::as_str) {
                        push_bounded(&mut extracted.tool_names, name);
                    }
                    if let Some(input) = object.get("input").and_then(Value::as_str) {
                        push_bounded(&mut extracted.tool_content, input);
                        if let Ok(value) = serde_json::from_str::<Value>(input) {
                            extracted.extract_structured_input(&value, MAX_JSON_DEPTH);
                        }
                    }
                }
                Some("tool_result") => {
                    if let Some(output) = object.get("output").and_then(Value::as_str) {
                        push_bounded(&mut extracted.tool_content, output);
                    }
                }
                _ => {}
            }
        }
        extracted
    }

    fn extract_structured_input(&mut self, value: &Value, remaining_depth: usize) {
        if remaining_depth == 0 {
            return;
        }
        match value {
            Value::Object(object) => {
                for (key, child) in object.iter().take(MAX_EXTRACTED_VALUES) {
                    if let Some(text) = child.as_str() {
                        match key.as_str() {
                            "command" | "cmd" => push_bounded(&mut self.commands, text),
                            "file" | "file_path" | "path" => self.push_file_path(text),
                            _ => {}
                        }
                    }
                    // Recurse after examining the current key so nested command
                    // or path fields are discoverable without indexing all JSON.
                    self.extract_structured_input(child, remaining_depth.saturating_sub(1));
                }
            }
            Value::Array(items) => {
                for item in items.iter().take(MAX_EXTRACTED_VALUES) {
                    self.extract_structured_input(item, remaining_depth.saturating_sub(1));
                }
            }
            _ => {}
        }
    }

    fn push_file_path(&mut self, value: &str) {
        push_bounded(&mut self.file_paths, value);
    }
}

fn push_bounded(values: &mut Vec<String>, value: &str) {
    let value = sanitize_search_text(value);
    let value = value.trim();
    if value.is_empty() || values.len() >= MAX_EXTRACTED_VALUES {
        return;
    }
    let value = truncate_characters(value.to_owned(), MAX_METADATA_FIELD_CHARACTERS);
    if !values.contains(&value) {
        values.push(value);
    }
}

fn sanitize_search_text(value: &str) -> String {
    // Marker tokens are controlled by this service during highlighting. Strip
    // collector-supplied copies so clients cannot forge highlighted segments.
    value
        .replace(HIGHLIGHT_START, "")
        .replace(HIGHLIGHT_END, "")
}

fn truncate_characters(mut value: String, maximum: usize) -> String {
    let Some((byte_index, _character)) = value.char_indices().nth(maximum) else {
        return value;
    };
    value.truncate(byte_index);
    value
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde_json::json;
    use uuid::Uuid;

    use crate::db::models::UsageEvent;

    use super::{HIGHLIGHT_END, HIGHLIGHT_START, MAX_CONTENT_CHARACTERS, SearchDocument};

    #[test]
    fn extracts_only_searchable_tool_metadata() {
        let mut event = usage_event();
        event.metadata = json!({
            "working_directory": "/Users/alice/repo",
            "authorization": "Bearer secret",
            "content_segments": [
                {
                    "type": "tool_call",
                    "name": "Bash",
                    "input": "{\"command\":\"cargo test\",\"nested\":{\"file_path\":\"src/lib.rs\"}}"
                },
                {
                    "type": "tool_result",
                    "output": "all tests passed"
                },
                {
                    "type": "image",
                    "content_base64": "secret-image"
                }
            ]
        });

        let document = SearchDocument::from_event(event);

        assert_eq!(document.tool_names, ["Bash"]);
        assert_eq!(document.commands, ["cargo test"]);
        assert_eq!(document.file_paths, ["/Users/alice/repo", "src/lib.rs"]);
        assert_eq!(document.tool_content.len(), 2);
        let serialized = serde_json::to_string(&document).expect("document should serialize");
        assert!(!serialized.contains("Bearer secret"));
        assert!(!serialized.contains("secret-image"));
    }

    #[test]
    fn truncates_oversized_content_on_a_utf8_boundary() {
        let mut event = usage_event();
        event.text = Some("会".repeat(MAX_CONTENT_CHARACTERS + 1));

        let document = SearchDocument::from_event(event);

        assert_eq!(
            document
                .content
                .expect("content should remain")
                .chars()
                .count(),
            MAX_CONTENT_CHARACTERS
        );
    }

    #[test]
    fn strips_highlight_markers_from_every_searchable_field() {
        let mut event = usage_event();
        event.session_id = format!("session-{HIGHLIGHT_START}one{HIGHLIGHT_END}");
        event.text = Some(format!(
            "before {HIGHLIGHT_START}match{HIGHLIGHT_END} after"
        ));
        event.metadata = json!({
            "working_directory": format!("/tmp/{HIGHLIGHT_START}repo"),
            "content_segments": [
                {
                    "type": "tool_call",
                    "name": format!("{HIGHLIGHT_START}Bash{HIGHLIGHT_END}"),
                    "input": serde_json::to_string(&json!({
                        "command": format!("cargo {HIGHLIGHT_START}test{HIGHLIGHT_END}"),
                        "file_path": format!("src/{HIGHLIGHT_END}lib.rs")
                    }))
                    .expect("tool input should serialize")
                },
                {
                    "type": "tool_result",
                    "output": format!("{HIGHLIGHT_START}passed{HIGHLIGHT_END}")
                }
            ]
        });

        let serialized = serde_json::to_string(&SearchDocument::from_event(event))
            .expect("document should serialize");

        assert!(!serialized.contains(HIGHLIGHT_START));
        assert!(!serialized.contains(HIGHLIGHT_END));
    }

    fn usage_event() -> UsageEvent {
        UsageEvent {
            id: Uuid::now_v7(),
            user_id: Uuid::now_v7(),
            device_context_id: Uuid::now_v7(),
            session_pk: Uuid::now_v7(),
            turn_pk: Uuid::now_v7(),
            event_id: "event-1".to_owned(),
            agent_name: "codex".to_owned(),
            agent_version: None,
            session_id: "session-1".to_owned(),
            turn_index: 1,
            llm_provider: "openai".to_owned(),
            llm_model: "gpt-5".to_owned(),
            event_type: "request".to_owned(),
            text: Some("hello".to_owned()),
            text_sha256: None,
            input_tokens: 1,
            output_tokens: 0,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
            reasoning_tokens: 0,
            total_tokens: 1,
            observed_at: Utc::now(),
            metadata: json!({}),
            created_at: Utc::now(),
        }
    }
}
