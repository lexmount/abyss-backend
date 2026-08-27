//! SQLite connection pooling and per-connection safety configuration.

use std::{
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use r2d2::ManageConnection;
use rusqlite::{Connection, OpenFlags};

use crate::error::AppError;

pub(super) type SqlitePool = r2d2::Pool<SqliteConnectionManager>;

#[derive(Clone)]
pub(super) struct SqliteConnectionManager {
    location: SqliteLocation,
}

#[derive(Clone)]
enum SqliteLocation {
    File(PathBuf),
    Memory,
}

impl SqliteConnectionManager {
    fn new(database_url: &str) -> Result<Self, AppError> {
        let normalized = database_url
            .strip_prefix("sqlite://")
            .unwrap_or(database_url);
        if normalized == ":memory:" {
            return Ok(Self {
                location: SqliteLocation::Memory,
            });
        }

        let path = PathBuf::from(normalized);
        if path.as_os_str().is_empty() {
            return Err(AppError::config(
                "ABYSS_BACKEND_DATABASE_URL must contain a SQLite file path".to_owned(),
            ));
        }
        create_parent_directory(&path)?;
        Ok(Self {
            location: SqliteLocation::File(path),
        })
    }

    fn open(&self) -> Result<Connection, rusqlite::Error> {
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_FULL_MUTEX;
        let connection = match &self.location {
            SqliteLocation::File(path) => Connection::open_with_flags(path, flags)?,
            SqliteLocation::Memory => Connection::open_in_memory_with_flags(flags)?,
        };
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.pragma_update(None, "foreign_keys", true)?;
        connection.pragma_update(None, "synchronous", "NORMAL")?;
        Ok(connection)
    }
}

impl ManageConnection for SqliteConnectionManager {
    type Connection = Connection;
    type Error = rusqlite::Error;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.open()
    }

    fn is_valid(&self, connection: &mut Self::Connection) -> Result<(), Self::Error> {
        connection.query_row("SELECT 1", [], |_row| Ok(()))
    }

    fn has_broken(&self, _connection: &mut Self::Connection) -> bool {
        false
    }
}

pub(super) fn create_pool(database_url: &str, pool_size: u32) -> Result<SqlitePool, AppError> {
    let manager = SqliteConnectionManager::new(database_url)?;
    let maximum_size = if matches!(manager.location, SqliteLocation::Memory) {
        1
    } else {
        pool_size
    };
    r2d2::Pool::builder()
        .max_size(maximum_size)
        .build(manager)
        .map_err(AppError::from)
}

pub(super) fn configure_database(connection: &Connection) -> Result<(), AppError> {
    let journal_mode = connection.query_row("PRAGMA journal_mode = WAL", [], |row| {
        row.get::<_, String>(0)
    })?;
    if journal_mode != "wal" && journal_mode != "memory" {
        return Err(AppError::internal(format!(
            "SQLite did not enable WAL journal mode: {journal_mode}"
        )));
    }
    Ok(())
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
