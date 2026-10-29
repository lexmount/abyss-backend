//! Stream facts of PostgreSQL events read from their metadata: at ingest, and
//! once more for every stored event when the way facts are read changes.

use std::collections::HashSet;

use diesel::{
    ExpressionMethods, PgConnection, QueryDsl, RunQueryDsl, sql_query,
    sql_types::{Bool, Integer, Jsonb, Nullable, Text, Uuid as SqlUuid},
};
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

use crate::{
    db::{
        models::NewStreamFacts,
        schema::{agent_sessions, llm_usage_events},
    },
    error::AppError,
    usage::{
        UsageEventType,
        streams::{RequestFacts, ResponseFacts},
    },
};

/// Events whose metadata one page holds.
const PAGE: i64 = 64;

/// Writes the facts read from an event's metadata unless they are stored
/// already; the ids the client declared stay as ingest stored them.
/// `trailing` is a reserved word in PostgreSQL.
const UPSERT: &str = "\
    INSERT INTO llm_usage_event_stream_facts AS f \
        (event_pk, new_from, starts_turn, last_identity, \"trailing\", result_ids, \
         emitted_ids, output_skeleton, label_index, label_text, label_length) \
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) \
    ON CONFLICT (event_pk) DO UPDATE SET \
        new_from = EXCLUDED.new_from, starts_turn = EXCLUDED.starts_turn, \
        last_identity = EXCLUDED.last_identity, \"trailing\" = EXCLUDED.\"trailing\", \
        result_ids = EXCLUDED.result_ids, emitted_ids = EXCLUDED.emitted_ids, \
        output_skeleton = EXCLUDED.output_skeleton, label_index = EXCLUDED.label_index, \
        label_text = EXCLUDED.label_text, label_length = EXCLUDED.label_length \
    WHERE (f.new_from, f.starts_turn, f.last_identity, f.\"trailing\", f.result_ids, \
           f.emitted_ids, f.output_skeleton, f.label_index, f.label_text, f.label_length) \
        IS DISTINCT FROM (EXCLUDED.new_from, EXCLUDED.starts_turn, EXCLUDED.last_identity, \
           EXCLUDED.\"trailing\", EXCLUDED.result_ids, EXCLUDED.emitted_ids, \
           EXCLUDED.output_skeleton, EXCLUDED.label_index, EXCLUDED.label_text, \
           EXCLUDED.label_length)";

fn json_value<T: Serialize>(value: &T, field: &str) -> Result<Value, AppError> {
    serde_json::to_value(value)
        .map_err(|error| AppError::internal(format!("serialize {field}: {error}")))
}

/// Fact columns read from an event's metadata, by its type; the other
/// side's columns and the ids the client declared stay NULL.
pub(super) fn read(
    event_pk: Uuid,
    event_type: &str,
    metadata: &Value,
) -> Result<NewStreamFacts, AppError> {
    Ok(if event_type == UsageEventType::Request.as_str() {
        let facts = RequestFacts::read(metadata);
        NewStreamFacts {
            event_pk,
            new_from: Some(facts.new_from),
            starts_turn: Some(facts.starts_turn),
            last_identity: facts.last_identity,
            trailing: Some(json_value(&facts.trailing, "trailing roles")?),
            result_ids: Some(json_value(&facts.result_ids, "result ids")?),
            label_index: facts.label.as_ref().map(|label| label.index),
            label_length: facts.label.as_ref().map(|label| label.length),
            label_text: facts.label.map(|label| label.text),
            ..NewStreamFacts::default()
        }
    } else {
        let facts = ResponseFacts::read(metadata);
        NewStreamFacts {
            event_pk,
            emitted_ids: Some(json_value(&facts.emitted_ids, "emitted ids")?),
            output_skeleton: Some(json_value(&facts.output_skeleton, "output skeleton")?),
            ..NewStreamFacts::default()
        }
    })
}

/// Reads the stream facts of every stored event again from its metadata,
/// page by page inside the caller's transaction, and bumps the structure
/// revision of each session whose facts changed. An event stored before
/// facts were read gets them now, without declared ids.
pub(super) fn rebuild(connection: &mut PgConnection) -> Result<(), AppError> {
    let mut changed = HashSet::new();
    let mut after = Uuid::nil();
    loop {
        let page = llm_usage_events::table
            .filter(llm_usage_events::id.gt(after))
            .order(llm_usage_events::id.asc())
            .limit(PAGE)
            .select((
                llm_usage_events::id,
                llm_usage_events::session_pk,
                llm_usage_events::event_type,
                llm_usage_events::metadata,
            ))
            .load::<(Uuid, Uuid, String, Value)>(connection)?;
        let Some(&(last, ..)) = page.last() else {
            break;
        };
        after = last;
        for (event_pk, session_pk, event_type, metadata) in page {
            if write(connection, &read(event_pk, &event_type, &metadata)?)? > 0 {
                changed.insert(session_pk);
            }
        }
    }
    bump(connection, &changed.into_iter().collect::<Vec<_>>())
}

/// Stores `facts`; the number of rows inserted or changed.
fn write(connection: &mut PgConnection, facts: &NewStreamFacts) -> Result<usize, AppError> {
    Ok(sql_query(UPSERT)
        .bind::<SqlUuid, _>(facts.event_pk)
        .bind::<Nullable<Integer>, _>(facts.new_from)
        .bind::<Nullable<Bool>, _>(facts.starts_turn)
        .bind::<Nullable<Text>, _>(facts.last_identity.as_deref())
        .bind::<Nullable<Jsonb>, _>(facts.trailing.as_ref())
        .bind::<Nullable<Jsonb>, _>(facts.result_ids.as_ref())
        .bind::<Nullable<Jsonb>, _>(facts.emitted_ids.as_ref())
        .bind::<Nullable<Jsonb>, _>(facts.output_skeleton.as_ref())
        .bind::<Nullable<Integer>, _>(facts.label_index)
        .bind::<Nullable<Text>, _>(facts.label_text.as_deref())
        .bind::<Nullable<Integer>, _>(facts.label_length)
        .execute(connection)?)
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "Diesel builds an atomic SQL increment, not a Rust integer addition."
)]
fn bump(connection: &mut PgConnection, sessions: &[Uuid]) -> Result<(), AppError> {
    diesel::update(agent_sessions::table.filter(agent_sessions::id.eq_any(sessions)))
        .set(agent_sessions::structure_revision.eq(agent_sessions::structure_revision + 1_i64))
        .execute(connection)?;
    Ok(())
}
