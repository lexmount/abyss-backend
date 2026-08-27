//! Bounded, allowlisted extraction shared by every full-text implementation.

use serde_json::Value;

use super::{HIGHLIGHT_END, HIGHLIGHT_START};

pub const MAX_CONTENT_CHARACTERS: usize = 1_000_000;
const MAX_METADATA_FIELD_CHARACTERS: usize = 100_000;
const MAX_EXTRACTED_VALUES: usize = 64;
const MAX_JSON_DEPTH: usize = 8;

/// Searchable values derived from one source usage event.
pub struct SearchProjection {
    pub content: Option<String>,
    pub tool_names: Vec<String>,
    pub tool_content: Vec<String>,
    pub commands: Vec<String>,
    pub file_paths: Vec<String>,
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
            commands: extracted.commands,
            file_paths: extracted.file_paths,
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
