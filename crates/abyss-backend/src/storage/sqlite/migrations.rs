//! Diesel-managed embedded migrations for the SQLite event store.

use diesel::{Connection, SqliteConnection};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};

use crate::error::AppError;

const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations-sqlite");

pub(super) fn run(connection: &mut SqliteConnection) -> Result<(), AppError> {
    connection.transaction(|connection| {
        let applied = connection
            .run_pending_migrations(MIGRATIONS)
            .map_err(|error| AppError::internal(format!("run SQLite migrations: {error}")))?
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let applied = |version: &str| applied.iter().any(|applied| applied == version);
        if applied("20261019120000") {
            super::session_titles::rebuild(connection)?;
        }
        if applied("20261029120000") {
            super::stream_facts::rebuild(connection)?;
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use diesel::{
        Connection, QueryDsl, QueryableByName, RunQueryDsl, SqliteConnection, sql_query,
        sql_types::{Nullable, Text},
    };
    use diesel_migrations::MigrationHarness;
    use serde_json::{Value, json};
    use uuid::Uuid;

    use super::{
        super::{repository::ingest_events, schema::llm_usage_events},
        MIGRATIONS, run,
    };
    use crate::usage::IngestEventsRequest;

    #[derive(QueryableByName)]
    struct Stored {
        #[diesel(sql_type = Text)]
        metadata: String,
        #[diesel(sql_type = Nullable<Text>)]
        conversation: Option<String>,
    }

    fn stored(database: &mut SqliteConnection) -> Vec<(Value, Option<Value>)> {
        sql_query("SELECT metadata, conversation FROM llm_usage_events ORDER BY event_id")
            .load::<Stored>(database)
            .expect("events")
            .into_iter()
            .map(|row| {
                let parse = |text: &str| serde_json::from_str::<Value>(text).expect("JSON");
                (parse(&row.metadata), row.conversation.as_deref().map(parse))
            })
            .collect()
    }

    #[test]
    fn stored_conversations_move_out_of_metadata() {
        let mut database = SqliteConnection::establish(":memory:").expect("SQLite should open");
        run(&mut database).expect("migrate");
        let conversation = json!({
            "entries": [{"role": "user", "text": "hello"}],
            "completeness": "complete"
        });
        let event = |id: &str, conversation: Option<&Value>| {
            json!({
                "event_id": id,
                "observed_at": "2026-10-29T00:00:00Z",
                "device": {"host_name": "local", "platform": "macos"},
                "agent": {"name": "codex"},
                "session_id": "session",
                "llm": {"provider": "openai", "model": "gpt-5"},
                "event_type": "request",
                "conversation": conversation,
                "metadata": {"purpose": "main"}
            })
        };
        let request: IngestEventsRequest = serde_json::from_value(json!({"events": [
            event("a", Some(&conversation)),
            event("b", None)
        ]}))
        .expect("request should parse");
        ingest_events(&mut database, &request, Uuid::from_u128(1), 100).expect("ingest");
        let current = stored(&mut database);
        assert_eq!(
            current,
            [
                (json!({"purpose": "main"}), Some(conversation.clone())),
                (json!({"purpose": "main"}), None)
            ]
        );

        // The previous schema kept the conversation in metadata.
        let reverted = database
            .revert_last_migration(MIGRATIONS)
            .expect("revert the move");
        assert_eq!(reverted.to_string(), "20261029140000");
        let metadata = sql_query(
            "SELECT metadata, NULL AS conversation FROM llm_usage_events ORDER BY event_id",
        )
        .load::<Stored>(&mut database)
        .expect("events")
        .into_iter()
        .map(|row| serde_json::from_str::<Value>(&row.metadata).expect("JSON"))
        .collect::<Vec<_>>();
        assert_eq!(
            metadata,
            [
                json!({"purpose": "main", "conversation": conversation}),
                json!({"purpose": "main"})
            ]
        );

        run(&mut database).expect("migrate again");
        assert_eq!(stored(&mut database), current);
        assert_eq!(
            llm_usage_events::table
                .select(llm_usage_events::event_type)
                .load::<String>(&mut database)
                .expect("sides"),
            ["request", "request"]
        );
    }
}
