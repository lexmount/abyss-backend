//! Search document projection and bounded extraction from usage-event metadata.
//!
//! Only explicitly allowlisted metadata enters the index. Extraction limits
//! depth, value count, and character length to bound CPU, allocation, request
//! size, and accidental indexing of unrelated collector metadata. PostgreSQL
//! retains the complete source event and remains the recovery source.

use serde::Serialize;
use uuid::Uuid;

use crate::{
    db::models::UsageEvent,
    search::projection::{SearchProjection, sanitize_search_text},
};

#[cfg(test)]
use crate::search::{HIGHLIGHT_END, HIGHLIGHT_START, projection::MAX_CONTENT_CHARACTERS};

/// Full-text search document for one immutable usage event.
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
        let projection = SearchProjection::from_source(event.text, &event.metadata);
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
            content: projection.content,
            tool_names: projection.tool_names,
            tool_content: projection.tool_content,
            commands: projection.commands,
            file_paths: projection.file_paths,
        }
    }
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
