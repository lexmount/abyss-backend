//! Diesel-managed embedded migrations for the SQLite event store.

use diesel::SqliteConnection;
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};

use crate::error::AppError;

const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations-sqlite");

pub(super) fn run(connection: &mut SqliteConnection) -> Result<(), AppError> {
    connection
        .run_pending_migrations(MIGRATIONS)
        .map_err(|error| AppError::internal(format!("run SQLite migrations: {error}")))?;
    Ok(())
}
