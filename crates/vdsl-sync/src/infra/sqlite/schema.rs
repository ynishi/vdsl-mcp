//! SQLite schema — PRAGMA initialization and table migrations.

use rusqlite::Connection;

use crate::domain::error::SyncError;

/// Per-connection initialization: PRAGMAs that must be set on every connection.
pub(crate) fn init_connection(conn: &mut Connection) -> Result<(), SyncError> {
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA foreign_keys = ON;
         PRAGMA busy_timeout = 5000;",
    )
    .map_err(|e| SyncError::Store(format!("pragma init failed: {e}")))?;
    migrate(conn)?;
    Ok(())
}

fn migrate(conn: &mut Connection) -> Result<(), SyncError> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS sync_entries (
            id            TEXT PRIMARY KEY,
            relative_path TEXT UNIQUE NOT NULL,
            file_type     TEXT NOT NULL,
            file_hash     TEXT NOT NULL,
            content_hash  TEXT,
            file_size     INTEGER,
            gen_id        TEXT,
            error         TEXT,
            synced_at     TEXT,
            updated_at    TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_entries_file_hash ON sync_entries(file_hash);
        CREATE INDEX IF NOT EXISTS idx_entries_content_hash ON sync_entries(content_hash);
        CREATE INDEX IF NOT EXISTS idx_entries_gen ON sync_entries(gen_id);

        CREATE TABLE IF NOT EXISTS sync_locations (
            entry_id    TEXT NOT NULL REFERENCES sync_entries(id) ON DELETE CASCADE,
            location_id TEXT NOT NULL,
            state       TEXT NOT NULL DEFAULT 'unknown',
            updated_at  TEXT NOT NULL,
            PRIMARY KEY (entry_id, location_id)
        );
        CREATE INDEX IF NOT EXISTS idx_locations_state
            ON sync_locations(location_id, state);

        CREATE TABLE IF NOT EXISTS sync_remotes (
            location_id TEXT PRIMARY KEY,
            backend     TEXT NOT NULL,
            remote_root TEXT NOT NULL DEFAULT '',
            config      TEXT NOT NULL DEFAULT '{}',
            created_at  TEXT NOT NULL
        );
        ",
    )
    .map_err(|e| SyncError::Store(format!("migration failed: {e}")))?;
    Ok(())
}
