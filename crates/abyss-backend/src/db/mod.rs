//! Database connection pool, migrations, and persistence modules.

pub mod models;
pub mod schema;

use diesel::{PgConnection, RunQueryDsl, r2d2::ConnectionManager, sql_query};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};

use crate::{config::Config, error::AppError};

pub type DbPool = r2d2::Pool<ConnectionManager<PgConnection>>;

const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

pub fn create_pool(config: &Config) -> Result<DbPool, AppError> {
    let manager = ConnectionManager::<PgConnection>::new(config.database_url.clone());
    r2d2::Pool::builder()
        .max_size(config.database_pool_size)
        .build(manager)
        .map_err(AppError::from)
}

pub fn run_migrations(pool: &DbPool) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut connection = pool.get()?;
    connection.run_pending_migrations(MIGRATIONS)?;
    Ok(())
}

pub fn check_ready(connection: &mut PgConnection) -> Result<(), AppError> {
    sql_query("SELECT 1").execute(connection)?;
    Ok(())
}
