//! The search index of one event, written in its ingest transaction: its
//! distinct texts once per owner with their folded trigrams, and one
//! reference per piece.

use diesel::{
    ExpressionMethods, OptionalExtension, QueryDsl, RunQueryDsl, SqliteConnection, sql_query,
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

/// Indexes every piece of one newly stored event of `owner`. Only this
/// event is read: a text is found again by its hash, never by comparing it
/// with other events.
pub(in crate::storage::sqlite) fn index_event(
    connection: &mut SqliteConnection,
    owner: &str,
    event_pk: &str,
    conversation: Option<&Value>,
) -> Result<(), AppError> {
    let mut refs = Vec::new();
    for piece in pieces(conversation) {
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
