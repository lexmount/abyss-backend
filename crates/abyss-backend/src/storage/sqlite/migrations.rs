//! Embedded, transactional migrations for the SQLite event store.

use rusqlite::{Connection, TransactionBehavior};

use crate::error::AppError;

const MIGRATIONS: &[&str] = &[include_str!(
    "../../../migrations-sqlite/0001_initial_schema.sql"
)];

pub(super) fn run(connection: &mut Connection) -> Result<(), AppError> {
    let current_version =
        connection.pragma_query_value(None, "user_version", |row| row.get::<_, usize>(0))?;
    if current_version > MIGRATIONS.len() {
        return Err(AppError::internal(format!(
            "SQLite schema version {current_version} is newer than supported version {}",
            MIGRATIONS.len()
        )));
    }

    for (index, migration) in MIGRATIONS.iter().enumerate().skip(current_version) {
        let target_version = index.saturating_add(1);
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Exclusive)?;
        transaction.execute_batch(migration)?;
        transaction.pragma_update(None, "user_version", target_version)?;
        transaction.commit()?;
    }
    Ok(())
}
