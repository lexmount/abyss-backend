//! Diesel-managed embedded migrations for the SQLite event store.

use diesel::{Connection, SqliteConnection};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};

use crate::error::AppError;

const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations-sqlite");

pub(super) fn run(connection: &mut SqliteConnection) -> Result<(), AppError> {
    connection.transaction(|connection| {
        let applied = connection
            .run_pending_migrations(MIGRATIONS)
            .map_err(|error| AppError::internal(format!("run SQLite migrations: {error}")))?;
        if applied
            .iter()
            .any(|version| version.to_string() == "20261019120000")
        {
            super::session_titles::rebuild(connection)?;
        }
        Ok(())
    })
}
