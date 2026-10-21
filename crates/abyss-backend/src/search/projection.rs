//! Bounded, allowlisted extraction shared by every full-text implementation.

use serde_json::Value;

use super::{HIGHLIGHT_END, HIGHLIGHT_START};

pub const MAX_CONTENT_CHARACTERS: usize = 1_000_000;
const MAX_METADATA_FIELD_CHARACTERS: usize = 100_000;
const MAX_EXTRACTED_VALUES: usize = 64;

/// Searchable values derived from one source usage event.
pub struct SearchProjection {
    pub content: Option<String>,
    pub tool_names: Vec<String>,
    pub tool_content: Vec<String>,
}

impl SearchProjection {
    /// Reads the event's conversation entries: the text of every entry, of
    /// every role, in order, and the names of the tools it calls. Every other
    /// metadata key is ignored, so headers, credentials, and image data never
    /// enter a derived full-text index.
    pub fn from_source(metadata: &Value) -> Self {
        let entries = metadata
            .get("conversation")
            .and_then(|conversation| conversation.get("entries"))
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let mut tool_names = Vec::new();
        let mut texts = Vec::new();
        for entry in entries {
            if entry.get("role").and_then(Value::as_str) == Some("tool_call")
                && let Some(name) = entry.get("name").and_then(Value::as_str)
            {
                push_bounded(&mut tool_names, name);
            }
            if let Some(text) = entry.get("text").and_then(Value::as_str)
                && !text.is_empty()
            {
                texts.push(text);
            }
        }
        Self {
            content: (!texts.is_empty())
                .then(|| sanitize_search_text(&texts.join("\n")))
                .map(|value| truncate_characters(value, MAX_CONTENT_CHARACTERS)),
            tool_names,
            tool_content: Vec::new(),
        }
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

pub fn sanitize_search_text(value: &str) -> String {
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
