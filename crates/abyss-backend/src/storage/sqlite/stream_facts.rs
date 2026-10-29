//! One-time rebuild of the SQLite stream facts of stored events from their
//! metadata, for a change in how facts are read.

use std::collections::HashSet;

use diesel::{
    ExpressionMethods, QueryDsl, RunQueryDsl, SqliteConnection, sql_query,
    sql_types::{Bool, Integer, Nullable, Text},
};
use serde_json::Value;

use super::{
    models::NewStreamFacts,
    schema::{agent_sessions, llm_usage_events},
};
use crate::error::AppError;

/// Events whose metadata one page holds.
const PAGE: i64 = 64;

/// Sessions whose revision one statement bumps.
const SESSIONS_PER_UPDATE: usize = 500;

/// Writes the facts read from an event's metadata unless they are stored
/// already; the ids the client declared stay as ingest stored them.
const UPSERT: &str = "\
    INSERT INTO llm_usage_event_stream_facts AS f \
        (event_pk, new_from, starts_turn, last_identity, trailing, result_ids, \
         emitted_ids, output_skeleton, label_index, label_text, label_length) \
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
    ON CONFLICT (event_pk) DO UPDATE SET \
        new_from = excluded.new_from, starts_turn = excluded.starts_turn, \
        last_identity = excluded.last_identity, trailing = excluded.trailing, \
        result_ids = excluded.result_ids, emitted_ids = excluded.emitted_ids, \
        output_skeleton = excluded.output_skeleton, label_index = excluded.label_index, \
        label_text = excluded.label_text, label_length = excluded.label_length \
    WHERE (f.new_from, f.starts_turn, f.last_identity, f.trailing, f.result_ids, \
           f.emitted_ids, f.output_skeleton, f.label_index, f.label_text, f.label_length) \
        IS NOT (excluded.new_from, excluded.starts_turn, excluded.last_identity, \
           excluded.trailing, excluded.result_ids, excluded.emitted_ids, \
           excluded.output_skeleton, excluded.label_index, excluded.label_text, \
           excluded.label_length)";

/// Reads the stream facts of every stored event again from its metadata,
/// page by page inside the caller's write transaction, and bumps the
/// structure revision of each session whose facts changed. An event stored
/// before facts were read gets them now, without declared ids.
pub(super) fn rebuild(connection: &mut SqliteConnection) -> Result<(), AppError> {
    let mut changed = HashSet::new();
    let mut after = String::new();
    loop {
        let page = llm_usage_events::table
            .filter(llm_usage_events::id.gt(&after))
            .order(llm_usage_events::id.asc())
            .limit(PAGE)
            .select((
                llm_usage_events::id,
                llm_usage_events::session_pk,
                llm_usage_events::event_type,
                llm_usage_events::metadata,
                llm_usage_events::previous_response_id,
            ))
            .load::<(String, String, String, String, Option<String>)>(connection)?;
        let Some((last, ..)) = page.last() else {
            break;
        };
        after.clone_from(last);
        for (event_pk, session_pk, event_type, metadata, previous) in page {
            let metadata = serde_json::from_str::<Value>(&metadata).map_err(|error| {
                AppError::internal(format!("stored metadata of event {event_pk}: {error}"))
            })?;
            let facts = NewStreamFacts::read(event_pk, &event_type, &metadata, previous.is_some())?;
            if write(connection, &facts)? > 0 {
                changed.insert(session_pk);
            }
        }
    }
    bump(connection, &changed.into_iter().collect::<Vec<_>>())
}

/// Stores `facts`; the number of rows inserted or changed.
fn write(connection: &mut SqliteConnection, facts: &NewStreamFacts) -> Result<usize, AppError> {
    Ok(sql_query(UPSERT)
        .bind::<Text, _>(&facts.event_pk)
        .bind::<Nullable<Integer>, _>(facts.new_from)
        .bind::<Nullable<Bool>, _>(facts.starts_turn)
        .bind::<Nullable<Text>, _>(facts.last_identity.as_deref())
        .bind::<Nullable<Text>, _>(facts.trailing.as_deref())
        .bind::<Nullable<Text>, _>(facts.result_ids.as_deref())
        .bind::<Nullable<Text>, _>(facts.emitted_ids.as_deref())
        .bind::<Nullable<Text>, _>(facts.output_skeleton.as_deref())
        .bind::<Nullable<Integer>, _>(facts.label_index)
        .bind::<Nullable<Text>, _>(facts.label_text.as_deref())
        .bind::<Nullable<Integer>, _>(facts.label_length)
        .execute(connection)?)
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "Diesel builds an atomic SQL increment, not a Rust integer addition."
)]
fn bump(connection: &mut SqliteConnection, sessions: &[String]) -> Result<(), AppError> {
    for sessions in sessions.chunks(SESSIONS_PER_UPDATE) {
        diesel::update(agent_sessions::table.filter(agent_sessions::id.eq_any(sessions)))
            .set(agent_sessions::structure_revision.eq(agent_sessions::structure_revision + 1_i64))
            .execute(connection)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use diesel::{Connection, ExpressionMethods, QueryDsl, RunQueryDsl, SqliteConnection};
    use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
    use serde_json::{Value, json};
    use uuid::Uuid;

    use super::super::{
        migrations,
        repository::{ingest_events, session_streams},
        schema::{agent_sessions, llm_usage_event_stream_facts, llm_usage_events},
    };
    use crate::{
        storage::stream_cache::StreamCache,
        usage::{IngestEventsRequest, streams::SegmentKind},
    };

    const OWNER_ID: Uuid = Uuid::from_u128(1);
    const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations-sqlite");

    fn event(session: &str, id: &str, second: u32, kind: &str, entries: &Value) -> Value {
        json!({
            "event_id": format!("{session}.{id}.{kind}"),
            "observed_at": format!("2026-10-29T00:00:{second:02}Z"),
            "device": {"host_name": "local", "platform": "macos"},
            "agent": {"name": "opencode"},
            "session_id": session,
            "response_id": format!("{session}.{id}"),
            "turn_id": format!("turn.{id}"),
            "llm": {"provider": "deepseek", "model": "deepseek-chat"},
            "event_type": kind,
            "metadata": {"conversation": {"entries": entries, "completeness": "complete"}}
        })
    }

    /// Two turns of `session`; the second resends the first answer.
    fn turns(session: &str, answer: &Value) -> Vec<Value> {
        let ask = json!({"role": "user", "text": "fix it"});
        let next = json!([ask, answer, {"role": "user", "text": "test it"}]);
        let tested = json!([{"role": "assistant", "text": "tested"}]);
        vec![
            event(session, "a", 1, "request", &json!([ask])),
            event(session, "a", 2, "response", &json!([answer])),
            event(session, "b", 3, "request", &next),
            event(session, "b", 4, "response", &tested),
        ]
    }

    fn session(database: &mut SqliteConnection, session: &str) -> (String, i64) {
        agent_sessions::table
            .filter(agent_sessions::session_id.eq(session))
            .select((agent_sessions::id, agent_sessions::structure_revision))
            .first::<(String, i64)>(database)
            .expect("session")
    }

    /// Stores the facts an older backend read from the first answer, which
    /// had no identity then.
    fn forget_the_answer(database: &mut SqliteConnection, response_pk: &str) {
        diesel::update(llm_usage_event_stream_facts::table.find(response_pk))
            .set((
                llm_usage_event_stream_facts::emitted_ids.eq("[]"),
                llm_usage_event_stream_facts::output_skeleton.eq(r#"[["assistant",null]]"#),
            ))
            .execute(database)
            .expect("stale facts");
    }

    /// Segments of the session that begin without a base.
    fn roots(database: &mut SqliteConnection, session_pk: &str) -> usize {
        let session_pk = Uuid::parse_str(session_pk).expect("uuid");
        session_streams(database, OWNER_ID, session_pk, &StreamCache::default())
            .expect("streams")
            .segments
            .iter()
            .filter(|segment| segment.kind == SegmentKind::Root)
            .count()
    }

    #[test]
    fn the_rebuild_reads_stale_and_missing_facts_again_once() {
        let mut database = SqliteConnection::establish(":memory:").expect("SQLite should open");
        migrations::run(&mut database).expect("migrate");
        let mut events = turns("chat", &json!({"role": "assistant", "text": "fixed"}));
        events.extend(turns(
            "responses",
            &json!({"role": "assistant", "text": "fixed", "item_id": "msg_1"}),
        ));
        let request: IngestEventsRequest =
            serde_json::from_value(json!({ "events": events })).expect("request should parse");
        ingest_events(&mut database, &request, OWNER_ID, 100).expect("ingest");
        let (chat, chat_revision) = session(&mut database, "chat");
        let (responses, responses_revision) = session(&mut database, "responses");
        assert_eq!(
            (
                roots(&mut database, &chat),
                roots(&mut database, &responses)
            ),
            (1, 1)
        );

        // Facts as an older backend read them, and an event stored before
        // facts were read at all.
        let chat_events = llm_usage_events::table
            .filter(llm_usage_events::session_pk.eq(&chat))
            .order(llm_usage_events::observed_at.asc())
            .select(llm_usage_events::id)
            .load::<String>(&mut database)
            .expect("events");
        forget_the_answer(&mut database, &chat_events[1]);
        diesel::delete(llm_usage_event_stream_facts::table.find(&chat_events[3]))
            .execute(&mut database)
            .expect("missing facts");
        for version in ["20261029130000", "20261029120000"] {
            let reverted = database
                .revert_last_migration(MIGRATIONS)
                .expect("revert down to the rebuild");
            assert_eq!(reverted.to_string(), version);
        }
        assert_eq!(
            roots(&mut database, &chat),
            2,
            "stale facts break the stream"
        );

        migrations::run(&mut database).expect("rebuild");
        assert_eq!(roots(&mut database, &chat), 1);
        let stored = llm_usage_event_stream_facts::table
            .filter(llm_usage_event_stream_facts::event_pk.eq_any(&chat_events))
            .select(llm_usage_event_stream_facts::turn_id)
            .load::<Option<String>>(&mut database)
            .expect("facts");
        assert_eq!(stored.len(), 4, "every event has facts");
        assert_eq!(
            stored.iter().filter(|turn| turn.is_some()).count(),
            3,
            "declared ids stay; the recreated row has none"
        );
        assert_eq!(
            (
                session(&mut database, "chat").1,
                session(&mut database, "responses").1
            ),
            (chat_revision + 1, responses_revision),
            "only the changed session gets a new revision"
        );

        // Recorded once: a later start reads no facts again.
        forget_the_answer(&mut database, &chat_events[1]);
        migrations::run(&mut database).expect("already rebuilt");
        assert_eq!(roots(&mut database, &chat), 2);
    }
}
