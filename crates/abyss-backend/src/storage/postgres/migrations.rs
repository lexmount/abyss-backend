//! Embedded PostgreSQL migrations and their one-time data rebuilds.

use diesel::{Connection, PgConnection};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};

use crate::error::AppError;

const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

pub(super) fn run(connection: &mut PgConnection) -> Result<(), AppError> {
    connection.transaction(|connection| {
        let applied = connection
            .run_pending_migrations(MIGRATIONS)
            .map_err(|error| AppError::internal(format!("run PostgreSQL migrations: {error}")))?;
        if applied
            .iter()
            .any(|version| version.to_string() == "20261019120000")
        {
            super::session_titles::rebuild(connection)?;
        }
        Ok(())
    })
}
