//! Bounded Elasticsearch projection of an event's conversation entries, and
//! the marker pair Elasticsearch places around highlighted text.

use serde_json::Value;

use crate::search::{SearchFragment, SearchFragmentSegment};

/// Sentinel Elasticsearch inserts before highlighted text.
pub const HIGHLIGHT_START: &str = "[[[ABYSS_SEARCH_HIGHLIGHT_START]]]";
/// Sentinel Elasticsearch inserts after highlighted text.
pub const HIGHLIGHT_END: &str = "[[[ABYSS_SEARCH_HIGHLIGHT_END]]]";

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

/// Splits an Elasticsearch fragment at its highlight markers.
pub fn parse_fragment(fragment: &str) -> SearchFragment {
    let mut segments = Vec::new();
    let mut remainder = fragment;
    while let Some(start) = remainder.find(HIGHLIGHT_START) {
        let (plain, tagged) = remainder.split_at(start);
        push_fragment_segment(&mut segments, plain, false);
        let highlighted = tagged
            .strip_prefix(HIGHLIGHT_START)
            .expect("the marker position came from find");
        // Treat malformed or truncated marker pairs as plain text. The API
        // never emits raw HTML and therefore does not trust ES fragments.
        let Some(end) = highlighted.find(HIGHLIGHT_END) else {
            push_fragment_segment(&mut segments, tagged, false);
            remainder = "";
            break;
        };
        let (matched, after_match) = highlighted.split_at(end);
        push_fragment_segment(&mut segments, matched, true);
        remainder = after_match
            .strip_prefix(HIGHLIGHT_END)
            .expect("the marker position came from find");
    }
    push_fragment_segment(&mut segments, remainder, false);
    SearchFragment { segments }
}

fn push_fragment_segment(segments: &mut Vec<SearchFragmentSegment>, text: &str, highlighted: bool) {
    if text.is_empty() {
        return;
    }
    segments.push(SearchFragmentSegment {
        text: text.to_owned(),
        highlighted,
    });
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

#[cfg(test)]
mod tests {
    use super::{HIGHLIGHT_END, HIGHLIGHT_START, parse_fragment};

    #[test]
    fn parses_highlights_without_rendering_html() {
        let fragment = parse_fragment(&format!(
            "before {HIGHLIGHT_START}timeout{HIGHLIGHT_END} after"
        ));

        assert_eq!(fragment.segments.len(), 3);
        assert_eq!(fragment.segments[1].text, "timeout");
        assert!(fragment.segments[1].highlighted);
    }
}
