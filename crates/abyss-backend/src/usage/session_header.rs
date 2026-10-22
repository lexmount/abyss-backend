//! The header of a session page: who took part, when, and how much.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

use super::{
    segments::TokenCounts,
    streams::{EntryLabel, StreamSummary},
};

/// The columns of one session event the header reads.
pub struct HeaderEvent {
    pub session_id: String,
    pub agent_name: String,
    pub agent_version: Option<String>,
    pub provider: String,
    pub model: String,
    pub observed_at: DateTime<Utc>,
    pub tokens: TokenCounts,
}

#[derive(Debug, Serialize)]
/// One session's agents, time span, counts, token totals, and models.
pub struct SessionHeaderResponse {
    /// Session primary key.
    pub session_pk: Uuid,
    /// Session id the agent declared.
    pub session_id: String,
    /// The earliest request's new-part label, shared with every session view.
    pub title: Option<EntryLabel>,
    /// Agents that reported events, by name and version.
    pub agents: Vec<HeaderAgent>,
    /// Earliest event time.
    pub start: DateTime<Utc>,
    /// Latest event time.
    pub end: DateTime<Utc>,
    pub event_count: usize,
    pub exchange_count: usize,
    /// Requests that open a turn.
    pub turn_count: usize,
    pub segment_count: usize,
    /// Token counts summed over every event.
    pub tokens: TokenCounts,
    pub providers: Vec<String>,
    pub models: Vec<String>,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct HeaderAgent {
    pub name: String,
    pub version: Option<String>,
}

/// The header of a session with at least one event.
pub fn session_header(
    session_pk: Uuid,
    events: Vec<HeaderEvent>,
    summary: &StreamSummary,
    title: Option<EntryLabel>,
) -> SessionHeaderResponse {
    let start = events.iter().map(|event| event.observed_at).min();
    let end = events.iter().map(|event| event.observed_at).max();
    let event_count = events.len();
    let session_id = events
        .first()
        .map(|event| event.session_id.clone())
        .unwrap_or_default();
    let mut agents = BTreeSet::new();
    let mut providers = BTreeSet::new();
    let mut models = BTreeSet::new();
    let mut tokens = TokenCounts::default();
    for event in events {
        tokens.add(&event.tokens);
        agents.insert(HeaderAgent {
            name: event.agent_name,
            version: event.agent_version,
        });
        providers.insert(event.provider);
        models.insert(event.model);
    }
    SessionHeaderResponse {
        session_pk,
        session_id,
        title,
        agents: agents.into_iter().collect(),
        start: start.unwrap_or_default(),
        end: end.unwrap_or_default(),
        event_count,
        exchange_count: summary.exchanges,
        turn_count: summary.turns,
        segment_count: summary.segments,
        tokens,
        providers: providers.into_iter().collect(),
        models: models.into_iter().collect(),
    }
}
