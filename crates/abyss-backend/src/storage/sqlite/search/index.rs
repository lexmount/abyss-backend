//! The search index of one event, written in its ingest transaction: its
//! distinct texts once per owner with their folded trigrams, and one
//! reference per piece. Events stored before the index existed are indexed
//! once at startup.

use std::time::Instant;

use diesel::{
    ExpressionMethods, OptionalExtension, QueryDsl, QueryableByName, RunQueryDsl, SqliteConnection,
    sql_query,
    sql_types::{BigInt, Text},
};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use crate::{
    error::AppError,
    search::matching::{fold::fold, pieces::pieces},
};

use super::super::{
    models::{NewSearchRef, NewSearchText},
    schema::{search_refs, search_texts},
};

/// References written per insert statement, well below SQLite's bound
/// parameter limit.
const REFS_PER_INSERT: usize = 1_000;

/// Events read and indexed per backfill transaction.
const BACKFILL_EVENTS_PER_BATCH: i64 = 100;

/// Indexes every piece of one newly stored event of `owner`. Only this
/// event is read: a text is found again by its hash, never by comparing it
/// with other events.
pub(in crate::storage::sqlite) fn index_event(
    connection: &mut SqliteConnection,
    owner: &str,
    event_pk: &str,
    metadata: &Value,
) -> Result<(), AppError> {
    let mut refs = Vec::new();
    for piece in pieces(metadata) {
        refs.push(NewSearchRef {
            event_pk,
            entry_index: i32::try_from(piece.entry_index).map_err(|error| {
                AppError::validation(format!("conversation has too many entries: {error}"))
            })?,
            field: piece.field.as_str(),
            text_id: text_id(connection, owner, piece.text)?,
        });
    }
    for chunk in refs.chunks(REFS_PER_INSERT) {
        diesel::insert_into(search_refs::table)
            .values(chunk)
            .execute(connection)?;
    }
    Ok(())
}

/// The id of `text` among the owner's texts. A new text is inserted, and
/// its folded form enters the trigram index under the same id.
fn text_id(connection: &mut SqliteConnection, owner: &str, text: &str) -> Result<i64, AppError> {
    let sha256 = Sha256::digest(text.as_bytes());
    let inserted = diesel::insert_or_ignore_into(search_texts::table)
        .values(NewSearchText {
            user_id: owner,
            sha256: sha256.as_slice(),
            text,
        })
        .returning(search_texts::id)
        .get_result::<i64>(connection)
        .optional()?;
    if let Some(id) = inserted {
        // The trigram index is a virtual table outside Diesel's schema.
        sql_query("INSERT INTO search_texts_trigram (rowid, folded) VALUES (?, ?)")
            .bind::<BigInt, _>(id)
            .bind::<Text, _>(fold(text))
            .execute(connection)?;
        return Ok(id);
    }
    Ok(search_texts::table
        .filter(search_texts::user_id.eq(owner))
        .filter(search_texts::sha256.eq(sha256.as_slice()))
        .select(search_texts::id)
        .first::<i64>(connection)?)
}

/// Stored events that have no reference yet.
const UNINDEXED_EVENTS: &str = "\
    FROM llm_usage_events AS e \
    WHERE NOT EXISTS (SELECT 1 FROM search_refs AS r WHERE r.event_pk = e.id)";

#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    count: i64,
}

#[derive(QueryableByName)]
struct UnindexedEvent {
    #[diesel(sql_type = BigInt)]
    rowid: i64,
    #[diesel(sql_type = Text)]
    id: String,
    #[diesel(sql_type = Text)]
    user_id: String,
    #[diesel(sql_type = Text)]
    metadata: String,
}

/// Indexes the stored events that have no reference yet, in batches that
/// each commit, logging progress. An event without pieces stays without
/// references and is read again, writing nothing, on a later start.
pub(in crate::storage::sqlite) fn backfill(
    connection: &mut SqliteConnection,
) -> Result<(), AppError> {
    let pending = sql_query(format!("SELECT COUNT(*) AS count {UNINDEXED_EVENTS}"))
        .get_result::<CountRow>(connection)?
        .count;
    if pending == 0 {
        return Ok(());
    }
    tracing::info!(events = pending, "indexing stored events for search");
    let started = Instant::now();
    let mut after_rowid = 0_i64;
    let mut indexed = 0_usize;
    loop {
        let batch = connection.immediate_transaction(|connection| {
            let events = sql_query(format!(
                "SELECT e.rowid AS rowid, e.id AS id, e.user_id AS user_id, \
                     e.metadata AS metadata \
                 {UNINDEXED_EVENTS} AND e.rowid > ? \
                 ORDER BY e.rowid LIMIT ?"
            ))
            .bind::<BigInt, _>(after_rowid)
            .bind::<BigInt, _>(BACKFILL_EVENTS_PER_BATCH)
            .load::<UnindexedEvent>(connection)?;
            for event in &events {
                let metadata = serde_json::from_str::<Value>(&event.metadata).map_err(|error| {
                    AppError::internal(format!("stored event metadata is invalid JSON: {error}"))
                })?;
                index_event(connection, &event.user_id, &event.id, &metadata)?;
            }
            Ok::<_, AppError>(events.last().map(|event| (event.rowid, events.len())))
        })?;
        let Some((last_rowid, events)) = batch else {
            break;
        };
        after_rowid = last_rowid;
        indexed = indexed.saturating_add(events);
        tracing::info!(
            indexed,
            pending,
            elapsed_ms = started.elapsed().as_millis(),
            "search backfill progress"
        );
    }
    tracing::info!(
        indexed,
        elapsed_ms = started.elapsed().as_millis(),
        "search backfill finished"
    );
    Ok(())
}
