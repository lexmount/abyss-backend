//! Diesel SQLite connection pooling and per-connection safety configuration.

use std::{fs, path::Path};

use diesel::{
    QueryableByName, RunQueryDsl, SqliteConnection,
    connection::SimpleConnection,
    r2d2::{ConnectionManager, CustomizeConnection, Error as DieselPoolError},
    sql_query,
    sql_types::Text,
};

use crate::error::AppError;

pub(super) type SqlitePool = r2d2::Pool<ConnectionManager<SqliteConnection>>;

#[derive(Debug)]
struct SqliteConnectionCustomizer;

impl CustomizeConnection<SqliteConnection, DieselPoolError> for SqliteConnectionCustomizer {
    fn on_acquire(&self, connection: &mut SqliteConnection) -> Result<(), DieselPoolError> {
        connection
            .batch_execute(
                "PRAGMA foreign_keys = ON;
                 PRAGMA busy_timeout = 5000;
                 PRAGMA synchronous = NORMAL;",
            )
            .map_err(DieselPoolError::from)
    }
}

#[derive(QueryableByName)]
struct JournalMode {
    #[diesel(sql_type = Text)]
    journal_mode: String,
}

pub(super) fn create_pool(database_url: &str, pool_size: u32) -> Result<SqlitePool, AppError> {
    let database_url = normalized_database_url(database_url)?;
    let maximum_size = if database_url == ":memory:" {
        1
    } else {
        pool_size
    };
    let manager = ConnectionManager::<SqliteConnection>::new(database_url);
    r2d2::Pool::builder()
        .max_size(maximum_size)
        .connection_customizer(Box::new(SqliteConnectionCustomizer))
        .build(manager)
        .map_err(AppError::from)
}

pub(super) fn configure_database(connection: &mut SqliteConnection) -> Result<(), AppError> {
    let journal_mode = sql_query("PRAGMA journal_mode = WAL")
        .get_result::<JournalMode>(connection)?
        .journal_mode;
    if journal_mode != "wal" && journal_mode != "memory" {
        return Err(AppError::internal(format!(
            "SQLite did not enable WAL journal mode: {journal_mode}"
        )));
    }
    Ok(())
}

fn normalized_database_url(database_url: &str) -> Result<String, AppError> {
    let normalized = database_url
        .strip_prefix("sqlite://")
        .unwrap_or(database_url);
    if normalized == ":memory:" {
        return Ok(normalized.to_owned());
    }
    let path = Path::new(normalized);
    if path.as_os_str().is_empty() {
        return Err(AppError::config(
            "ABYSS_BACKEND_DATABASE_URL must contain a SQLite file path".to_owned(),
        ));
    }
    create_parent_directory(path)?;
    Ok(normalized.to_owned())
}

fn create_parent_directory(path: &Path) -> Result<(), AppError> {
    let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    else {
        return Ok(());
    };
    fs::create_dir_all(parent).map_err(|error| {
        AppError::config(format!(
            "create SQLite database directory {}: {error}",
            parent.display()
        ))
    })
}
