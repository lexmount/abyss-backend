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
    pub fn from_source(text: Option<String>, metadata: &Value) -> Self {
        let extracted = SearchableMetadata::extract(metadata);
        Self {
            content: text
                .map(|value| sanitize_search_text(&value))
                .map(|value| truncate_characters(value, MAX_CONTENT_CHARACTERS)),
            tool_names: extracted.tool_names,
            tool_content: extracted.tool_content,
        }
    }
}

struct SearchableMetadata {
    tool_names: Vec<String>,
    tool_content: Vec<String>,
}

impl SearchableMetadata {
    fn extract(metadata: &Value) -> Self {
        let mut extracted = Self {
            tool_names: Vec::new(),
            tool_content: Vec::new(),
        };

        // Ignore every other top-level key so headers, credentials, and image
        // data never enter a derived full-text index.
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
