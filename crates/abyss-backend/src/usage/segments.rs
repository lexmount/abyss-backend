//! The exchange layer of a session page: one segment's exchanges with their
//! times, models, token counts, and a preview of each request and response.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    UsageEventResponse,
    preview::{ConversationPreview, PreviewQuery, Window, preview},
    streams::{SegmentExchange, SegmentPage},
};
use crate::error::AppError;

/// Exchanges a segment page holds without `limit`.
const DEFAULT_LIMIT: usize = 50;
/// Most exchanges one segment page holds.
const MAX_LIMIT: usize = 200;

#[derive(Debug, Default, Deserialize)]
/// Query of a segment page.
pub struct SegmentQuery {
    /// Runs of entries each preview gives; see [`PreviewQuery`].
    pub entries: Option<String>,
    /// Most UTF-16 code units of each previewed entry text.
    pub text_preview: Option<usize>,
    /// Most exchanges in the page.
    pub limit: Option<usize>,
    /// Exchange the page continues after.
    pub after: Option<String>,
}

#[derive(Debug)]
/// A validated [`SegmentQuery`].
pub struct ValidatedSegmentQuery {
    pub window: Window,
    pub limit: usize,
    pub after: Option<String>,
}

impl SegmentQuery {
    pub fn validate(self) -> Result<ValidatedSegmentQuery, AppError> {
        let limit = self.limit.unwrap_or(DEFAULT_LIMIT);
        if limit == 0 || limit > MAX_LIMIT {
            return Err(AppError::validation(format!(
                "limit must be between 1 and {MAX_LIMIT}"
            )));
        }
        Ok(ValidatedSegmentQuery {
            window: Window::parse(&PreviewQuery {
                entries: self.entries,
                text_preview: self.text_preview,
            })?,
            limit,
            after: self.after,
        })
    }
}

#[derive(Debug, Serialize)]
/// One segment's exchanges from the asked one on.
pub struct SegmentResponse {
    /// Session primary key.
    pub session_pk: Uuid,
    /// Head of the segment that now holds the asked exchange.
    pub segment: String,
    /// The exchanges in order.
    pub exchanges: Vec<SegmentExchangeResponse>,
    /// Exchange to pass as `after` for the next page, while there is one.
    pub next: Option<String>,
}

#[derive(Debug, Serialize)]
/// One exchange with what its card shows.
pub struct SegmentExchangeResponse {
    #[serde(flatten)]
    pub exchange: SegmentExchange,
    /// Observation time of the request event.
    pub requested_at: Option<DateTime<Utc>>,
    /// Observation time of the response event.
    pub responded_at: Option<DateTime<Utc>>,
    /// Model of the request, else of the response.
    pub model: Option<String>,
    pub request_tokens: Option<TokenCounts>,
    pub response_tokens: Option<TokenCounts>,
    pub request: Option<ConversationPreview>,
    pub response: Option<ConversationPreview>,
}

#[derive(Debug, Default, PartialEq, Eq, Serialize)]
/// Token counts of one event as stored, or a sum of them.
pub struct TokenCounts {
    pub input: i64,
    pub output: i64,
    pub cache_read: i64,
    pub cache_write: i64,
    pub reasoning: i64,
    pub total: i64,
}

impl TokenCounts {
    /// Adds `other` to these counts.
    pub const fn add(&mut self, other: &Self) {
        self.input = self.input.saturating_add(other.input);
        self.output = self.output.saturating_add(other.output);
        self.cache_read = self.cache_read.saturating_add(other.cache_read);
        self.cache_write = self.cache_write.saturating_add(other.cache_write);
        self.reasoning = self.reasoning.saturating_add(other.reasoning);
        self.total = self.total.saturating_add(other.total);
    }

    const fn of(event: &UsageEventResponse) -> Self {
        Self {
            input: event.input_tokens,
            output: event.output_tokens,
            cache_read: event.cache_read_tokens,
            cache_write: event.cache_write_tokens,
            reasoning: event.reasoning_tokens,
            total: event.total_tokens,
        }
    }
}

/// The primary keys of the events a page's exchanges hold.
pub fn page_event_pks(page: &SegmentPage) -> Vec<Uuid> {
    page.exchanges
        .iter()
        .flat_map(|exchange| [exchange.request_pk, exchange.response_pk])
        .flatten()
        .collect()
}

/// Fills a page with its events, keyed by primary key.
pub fn segment_response(
    session_pk: Uuid,
    page: SegmentPage,
    mut events: HashMap<Uuid, UsageEventResponse>,
    window: &Window,
) -> SegmentResponse {
    let exchanges = page
        .exchanges
        .into_iter()
        .map(|exchange| {
            let request = exchange.request_pk.and_then(|pk| events.remove(&pk));
            let response = exchange.response_pk.and_then(|pk| events.remove(&pk));
            SegmentExchangeResponse {
                requested_at: request.as_ref().map(|event| event.observed_at),
                responded_at: response.as_ref().map(|event| event.observed_at),
                model: request
                    .as_ref()
                    .or(response.as_ref())
                    .map(|event| event.llm_model.clone()),
                request_tokens: request.as_ref().map(TokenCounts::of),
                response_tokens: response.as_ref().map(TokenCounts::of),
                request: request.map(|event| preview(event, window)),
                response: response.map(|event| preview(event, window)),
                exchange,
            }
        })
        .collect();
    SegmentResponse {
        session_pk,
        segment: page.segment,
        exchanges,
        next: page.next,
    }
}
