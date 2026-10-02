//! Owner-scoped session index: one page of sessions ordered by latest activity.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    error::AppError,
    usage::persistence::{agent_name_filter_values, normalize_slug, normalized_optional},
};

const DEFAULT_PAGE_SIZE: u32 = 20;
const MAX_PAGE_SIZE: u32 = 100;

/// Query parameters for `GET /v1/agent-usage/sessions`.
#[derive(Debug, Default, Deserialize)]
pub struct SessionListQuery {
    /// Keeps sessions whose latest activity is at or after this instant.
    pub from: Option<DateTime<Utc>>,
    /// Keeps sessions that started before this instant.
    pub to: Option<DateTime<Utc>>,
    /// Agent-name filter; Claude aliases match together.
    pub agent_name: Option<String>,
    /// Keeps sessions with at least one event from this LLM provider.
    pub llm_provider: Option<String>,
    /// Keeps sessions with at least one event from this LLM model.
    pub llm_model: Option<String>,
    /// 1-based page number.
    pub page: Option<u32>,
    /// Sessions per page, at most 100.
    pub page_size: Option<u32>,
}

/// Session list query with normalized filters and a resolved page window.
#[derive(Debug)]
pub struct ValidatedSessionListQuery {
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    /// Stored agent names to match, or empty for every Agent.
    pub agent_names: Vec<String>,
    /// Normalized provider slug a session must have used.
    pub llm_provider: Option<String>,
    /// Exact model a session must have used.
    pub llm_model: Option<String>,
    pub page: u32,
    pub page_size: u32,
    pub offset: i64,
}

impl SessionListQuery {
    pub fn validate(self) -> Result<ValidatedSessionListQuery, AppError> {
        if let (Some(from), Some(to)) = (self.from, self.to)
            && from >= to
        {
            return Err(AppError::validation(
                "from must be earlier than to".to_owned(),
            ));
        }
        let page = self.page.unwrap_or(1);
        if page == 0 {
            return Err(AppError::validation("page must be at least 1".to_owned()));
        }
        let page_size = self.page_size.unwrap_or(DEFAULT_PAGE_SIZE);
        if page_size == 0 || page_size > MAX_PAGE_SIZE {
            return Err(AppError::validation(format!(
                "page_size must be between 1 and {MAX_PAGE_SIZE}"
            )));
        }
        let offset = i64::from(page.saturating_sub(1)).saturating_mul(i64::from(page_size));
        Ok(ValidatedSessionListQuery {
            from: self.from,
            to: self.to,
            agent_names: normalized_optional(self.agent_name.as_deref())
                .map(|value| agent_name_filter_values(&value))
                .unwrap_or_default(),
            llm_provider: normalized_optional(self.llm_provider.as_deref())
                .map(|value| normalize_slug(&value)),
            llm_model: normalized_optional(self.llm_model.as_deref()),
            page,
            page_size,
            offset,
        })
    }
}

/// One page of sessions, newest activity first.
#[derive(Debug, Serialize)]
pub struct SessionListResponse {
    /// Sessions matching the filters across all pages.
    pub total_sessions: i64,
    pub page: u32,
    pub page_size: u32,
    pub has_more: bool,
    pub items: Vec<SessionListItem>,
}

/// Summary of one session for the session index.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct SessionListItem {
    pub session_pk: Uuid,
    pub session_id: String,
    pub agent_name: String,
    pub agent_version: Option<String>,
    pub host_name: String,
    pub platform: String,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    /// Latest event observation, or the session start when it has no events.
    pub last_activity_at: DateTime<Utc>,
    pub turns: i64,
    pub events: i64,
    pub total_tokens: i64,
    /// Agent-generated title, or a first-user-input preview for other Agents.
    pub title: Option<String>,
    /// Why the session exists when it only serves the Agent, such as
    /// `title_generation`; `None` for ordinary sessions.
    pub purpose: Option<String>,
}

impl SessionListResponse {
    pub fn new(
        query: &ValidatedSessionListQuery,
        total_sessions: i64,
        items: Vec<SessionListItem>,
    ) -> Self {
        let shown = query
            .offset
            .saturating_add(i64::try_from(items.len()).unwrap_or(i64::MAX));
        Self {
            total_sessions,
            page: query.page,
            page_size: query.page_size,
            has_more: shown < total_sessions,
            items,
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn defaults_to_the_first_page_of_every_agent() {
        let query = SessionListQuery::default().validate().expect("valid");
        assert_eq!((query.page, query.page_size, query.offset), (1, 20, 0));
        assert!(query.agent_names.is_empty());
    }

    #[test]
    fn expands_claude_aliases_and_computes_the_offset() {
        let query = SessionListQuery {
            agent_name: Some(" Claude ".to_owned()),
            page: Some(3),
            page_size: Some(25),
            ..SessionListQuery::default()
        }
        .validate()
        .expect("valid");
        assert_eq!(query.offset, 50);
        assert_eq!(
            query.agent_names,
            ["claude-code", "claude", "claude-desktop"]
        );
    }

    #[test]
    fn normalizes_provider_and_keeps_the_model_verbatim() {
        let query = SessionListQuery {
            llm_provider: Some(" OpenAI ".to_owned()),
            llm_model: Some(" gpt-5 ".to_owned()),
            ..SessionListQuery::default()
        }
        .validate()
        .expect("valid");
        assert_eq!(query.llm_provider.as_deref(), Some("openai"));
        assert_eq!(query.llm_model.as_deref(), Some("gpt-5"));
        let blank = SessionListQuery {
            llm_model: Some("  ".to_owned()),
            ..SessionListQuery::default()
        }
        .validate()
        .expect("valid");
        assert!(blank.llm_model.is_none());
    }

    #[test]
    fn rejects_bad_windows_and_page_sizes() {
        let at = Utc.with_ymd_and_hms(2026, 9, 1, 0, 0, 0).unwrap();
        let reversed = SessionListQuery {
            from: Some(at),
            to: Some(at),
            ..SessionListQuery::default()
        };
        assert!(reversed.validate().is_err());
        for page_size in [0, 101] {
            let query = SessionListQuery {
                page_size: Some(page_size),
                ..SessionListQuery::default()
            };
            assert!(query.validate().is_err());
        }
        let zero_page = SessionListQuery {
            page: Some(0),
            ..SessionListQuery::default()
        };
        assert!(zero_page.validate().is_err());
    }
}
