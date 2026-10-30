//! Search document projection and bounded extraction from usage-event
//! conversations.
//!
//! Only conversation entries enter the index; metadata never does. Extraction
//! limits depth, value count, and character length to bound CPU, allocation,
//! and request size. PostgreSQL
//! retains the complete source event and remains the recovery source.

use serde::Serialize;
use uuid::Uuid;

use super::projection::{SearchProjection, sanitize_search_text};
use crate::db::models::UsageEvent;

#[cfg(test)]
use super::projection::{HIGHLIGHT_END, HIGHLIGHT_START, MAX_CONTENT_CHARACTERS};

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
    /// Agent name as reported by the collector, used for exact filtering.
    pub agent_name: String,
    /// LLM provider as reported by the collector, used for exact filtering.
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
    /// Bounded names extracted from tool-call entries.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tool_names: Vec<String>,
    /// Bounded raw tool inputs and outputs.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tool_content: Vec<String>,
}

impl SearchDocument {
    /// Projects one source event into the strict Elasticsearch mapping.
    #[must_use]
    pub fn from_event(event: UsageEvent) -> Self {
        let projection = SearchProjection::from_source(event.conversation.as_ref());
        Self {
            event_pk: event.id,
            user_id: event.user_id,
            session_pk: event.session_pk,
            session_id: sanitize_search_text(&event.session_id),
            agent_name: event.agent_name,
            llm_provider: event.llm_provider,
            llm_model: event.llm_model,
            event_type: event.event_type,
            observed_at: event.observed_at,
            content: projection.content,
            tool_names: projection.tool_names,
            tool_content: projection.tool_content,
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

    /// `entries` as an event's complete conversation.
    fn conversation(entries: &serde_json::Value) -> serde_json::Value {
        json!({"entries": entries, "completeness": "complete"})
    }

    #[test]
    fn indexes_every_entry_text_and_the_called_tool_names_only() {
        let mut event = usage_event();
        event.conversation = Some(conversation(&json!([
            {"role": "user", "text": "run the tests"},
            {"role": "tool_call", "text": "{\"command\":\"cargo test\"}", "call_id": "c1", "name": "Bash"},
            {"role": "tool_result", "text": "all tests passed", "call_id": "c1"}
        ])));
        event.metadata["headers"] = json!([{"name": "authorization", "value": "Bearer secret"}]);

        let document = SearchDocument::from_event(event);

        assert_eq!(document.tool_names, ["Bash"]);
        assert_eq!(
            document.content.as_deref(),
            Some("run the tests\n{\"command\":\"cargo test\"}\nall tests passed")
        );
        assert!(document.tool_content.is_empty());
        let serialized = serde_json::to_string(&document).expect("document should serialize");
        assert!(!serialized.contains("Bearer secret"));
    }

    #[test]
    fn truncates_oversized_content_on_a_utf8_boundary() {
        let mut event = usage_event();
        event.conversation = Some(conversation(&json!([
            {"role": "user", "text": "会".repeat(MAX_CONTENT_CHARACTERS + 1)}
        ])));

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
        event.conversation = Some(conversation(&json!([
            {"role": "user", "text": format!("before {HIGHLIGHT_START}match{HIGHLIGHT_END} after")},
            {
                "role": "tool_call",
                "text": format!("cargo {HIGHLIGHT_START}test{HIGHLIGHT_END}"),
                "call_id": "c1",
                "name": format!("{HIGHLIGHT_START}Bash{HIGHLIGHT_END}")
            }
        ])));

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
            event_id: "event-1".to_owned(),
            agent_name: "codex".to_owned(),
            agent_version: None,
            session_id: "session-1".to_owned(),
            llm_provider: "openai".to_owned(),
            llm_model: "gpt-5".to_owned(),
            event_type: "request".to_owned(),
            text: None,
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
            stream_id: None,
            response_id: None,
            previous_response_id: None,
            conversation: None,
        }
    }
}
