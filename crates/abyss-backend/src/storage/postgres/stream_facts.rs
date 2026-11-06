//! Stream facts of PostgreSQL events read at ingest from their conversations.

use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

use crate::{
    db::models::NewStreamFacts,
    error::AppError,
    usage::{
        UsageEventType,
        streams::{RequestFacts, ResponseFacts},
    },
};

fn json_value<T: Serialize>(value: &T, field: &str) -> Result<Value, AppError> {
    serde_json::to_value(value)
        .map_err(|error| AppError::internal(format!("serialize {field}: {error}")))
}

/// Fact columns read from an event's conversation, by its type; the other
/// side's columns and the ids the client declared stay NULL.
pub(super) fn read(
    event_pk: Uuid,
    event_type: &str,
    conversation: Option<&Value>,
    names_previous: bool,
) -> Result<NewStreamFacts, AppError> {
    Ok(if event_type == UsageEventType::Request.as_str() {
        let facts = RequestFacts::read(conversation);
        NewStreamFacts {
            event_pk,
            new_from: Some(facts.new_from),
            continues_context: Some(facts.new_from > 0_i32 || names_previous),
            starts_turn: Some(facts.starts_turn),
            last_identity: facts.last_identity,
            previous_identity: facts.previous_identity,
            trailing: Some(json_value(&facts.trailing, "trailing roles")?),
            result_ids: Some(json_value(&facts.result_ids, "result ids")?),
            label_index: facts.label.as_ref().map(|label| label.index),
            label_length: facts.label.as_ref().map(|label| label.length),
            label_text: facts.label.map(|label| label.text),
            ..NewStreamFacts::default()
        }
    } else {
        let facts = ResponseFacts::read(conversation);
        NewStreamFacts {
            event_pk,
            emitted_ids: Some(json_value(&facts.emitted_ids, "emitted ids")?),
            output_skeleton: Some(json_value(&facts.output_skeleton, "output skeleton")?),
            ..NewStreamFacts::default()
        }
    })
}
