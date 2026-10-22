//! Conversation entries and sessions the stream tests build on.

use chrono::{DateTime, TimeDelta, TimeZone as _, Utc};
use serde_json::{Value, json};
use uuid::Uuid;

use super::{
    BaseRule, EventFacts, NoBaseReason, RequestFacts, ResponseFacts, SessionStructure, StreamEvent,
};

pub(super) fn conversation(entries: &Value) -> Value {
    json!({"conversation": {"entries": entries, "completeness": "complete"}})
}

pub(super) fn user(text: &str) -> Value {
    json!({"role": "user", "text": text})
}

pub(super) fn system(text: &str) -> Value {
    json!({"role": "system", "text": text})
}

pub(super) fn assistant(text: &str) -> Value {
    json!({"role": "assistant", "text": text})
}

pub(super) fn reasoning() -> Value {
    json!({"role": "reasoning", "text": ""})
}

pub(super) fn call(id: &str) -> Value {
    json!({"role": "tool_call", "text": "{}", "call_id": id, "name": "Bash"})
}

pub(super) fn result(id: &str) -> Value {
    json!({"role": "tool_result", "text": "ok", "call_id": id})
}

/// `entry` as part of the model output item `id`.
pub(super) fn item(mut entry: Value, id: &str) -> Value {
    entry["item_id"] = json!(id);
    entry
}

pub(super) fn skeleton(entries: &[(&str, Option<&str>)]) -> Vec<(String, Option<String>)> {
    entries
        .iter()
        .map(|(role, id)| ((*role).to_owned(), id.map(str::to_owned)))
        .collect()
}

pub(super) fn ids(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

/// A session built from exchanges with conversation entries; exchange
/// `name` shares `name` as its response id.
pub(super) struct Session {
    start: DateTime<Utc>,
    pub(super) events: Vec<StreamEvent>,
}

impl Session {
    pub(super) fn new() -> Self {
        Self {
            start: Utc
                .with_ymd_and_hms(2026, 10, 20, 0, 0, 0)
                .single()
                .expect("valid time"),
            events: Vec::new(),
        }
    }

    pub(super) fn add(
        &mut self,
        name: &str,
        millis: i64,
        previous: Option<&str>,
        facts: EventFacts,
    ) {
        let at = self
            .start
            .checked_add_signed(TimeDelta::milliseconds(millis))
            .expect("test times stay in range");
        self.events.push(StreamEvent {
            pk: Uuid::now_v7(),
            event_id: format!("{name}.{millis}"),
            observed_at: at,
            response_id: Some(name.to_owned()),
            previous_response_id: previous.map(str::to_owned),
            facts,
        });
    }

    /// Adds the request of exchange `name` at `second`.
    pub(super) fn ask(&mut self, name: &str, second: i64, previous: Option<&str>, entries: &Value) {
        let facts = EventFacts::Request(RequestFacts::read(&conversation(entries)));
        self.add(name, second.saturating_mul(1_000), previous, facts);
    }

    /// Adds the response of exchange `name`, half a second after `second`.
    pub(super) fn answer(&mut self, name: &str, second: i64, entries: &Value) {
        let facts = EventFacts::Response(ResponseFacts::read(&conversation(entries)));
        let millis = second.saturating_mul(1_000).saturating_add(500);
        self.add(name, millis, None, facts);
    }

    pub(super) fn exchange(&mut self, name: &str, second: i64, request: &Value, response: &Value) {
        self.ask(name, second, None, request);
        self.answer(name, second, response);
    }

    /// The base and rule of exchange `name`, or why it has none.
    pub(super) fn base(&self, name: &str) -> Result<(String, BaseRule), NoBaseReason> {
        let streams = SessionStructure::derive(self.events.clone());
        let index = streams
            .exchanges
            .iter()
            .position(|exchange| exchange.id == name)
            .expect("the exchange exists");
        streams.bases[index].map(|(base, rule)| (streams.exchanges[base].id.clone(), rule))
    }
}
