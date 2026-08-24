//! PostgreSQL connection pool, embedded migrations, and persistence modules.
//!
//! A synchronous Diesel pool is shared by HTTP handlers and the search worker.
//! Callers running on Tokio are responsible for entering this module through a
//! blocking task so neither connection acquisition nor SQL blocks the executor.

/// Diesel models used for reads and inserts.
pub mod models;
/// Diesel's compile-time representation of the PostgreSQL schema.
pub mod schema;

use diesel::{PgConnection, RunQueryDsl, r2d2::ConnectionManager, sql_query};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};

use crate::{config::Config, error::AppError};

/// Cloneable pool of synchronous PostgreSQL connections.
pub type DbPool = r2d2::Pool<ConnectionManager<PgConnection>>;

const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

/// Creates the PostgreSQL pool and verifies that its initial connection opens.
pub fn create_pool(config: &Config) -> Result<DbPool, AppError> {
    let manager = ConnectionManager::<PgConnection>::new(config.database_url.clone());
    r2d2::Pool::builder()
        .max_size(config.database_pool_size)
        .build(manager)
        .map_err(AppError::from)
}

/// Applies all embedded migrations that are not recorded by Diesel yet.
pub fn run_migrations(pool: &DbPool) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut connection = pool.get()?;
    connection.run_pending_migrations(MIGRATIONS)?;
    Ok(())
}

/// Executes the minimal PostgreSQL query used by the readiness endpoint.
pub fn check_ready(connection: &mut PgConnection) -> Result<(), AppError> {
    sql_query("SELECT 1").execute(connection)?;
    Ok(())
}
