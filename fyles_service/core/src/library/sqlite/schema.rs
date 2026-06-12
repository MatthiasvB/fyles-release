use rusqlite::{Connection, Result};
use semver::Version;

pub static CURRENT_VERSION: Version = Version::new(1, 19, 1);

pub fn initialize_tables(conn: &mut Connection) -> Result<()> {
    // First create schema version table if it doesn't exist
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_version (
            version TEXT NOT NULL
        )",
        [],
    )?;

    // Check if DB is already initialized
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM schema_version", [], |row| row.get(0))?;

    if count > 0 {
        return Ok(()); // DB already initialized
    }

    // Initialize new DB with current schema version
    let tx = conn.transaction()?;

    tx.execute_batch(
        "CREATE TABLE IF NOT EXISTS filerequests (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            description TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            status TEXT CHECK (status IN ('active', 'paused')) NOT NULL DEFAULT 'active',
            access_type TEXT CHECK (access_type IN ('public', 'audience')) NOT NULL DEFAULT 'audience'
        );

        CREATE TABLE IF NOT EXISTS contacts (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            dilithium_public_key BLOB,
            ed25519_public_key BLOB
        );

        CREATE TABLE IF NOT EXISTS filerequest_audience (
            filerequest_id TEXT NOT NULL,
            contact_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (filerequest_id, contact_id),
            FOREIGN KEY (filerequest_id) REFERENCES filerequests(id) ON DELETE CASCADE,
            FOREIGN KEY (contact_id) REFERENCES contacts(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS node_keys (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            key_bytes BLOB NOT NULL
        );

        CREATE TABLE IF NOT EXISTS remote_filerequests (
            id TEXT PRIMARY KEY,
            peer_id TEXT NOT NULL,
            filerequest_id TEXT NOT NULL,
            name TEXT NOT NULL,
            created_at TEXT NOT NULL,
            contact_id TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS pending_files (
            id TEXT PRIMARY KEY,
            file_path TEXT NOT NULL,
            target_filerequest_id TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'Pending'
                CHECK (status IN ('Pending', 'Sending', 'Interrupted', 'Sent', 'Rejected', 'Failed')),
            retry_count INTEGER NOT NULL DEFAULT 0,
            progress_bytes INTEGER,
            file_size_bytes INTEGER,
            transfer_id TEXT,
            display_name TEXT,
            interruption_reasons TEXT NOT NULL DEFAULT '[]',
            created_at TEXT NOT NULL,
            FOREIGN KEY(target_filerequest_id) REFERENCES remote_filerequests(id) ON DELETE CASCADE,

            CHECK (
                -- Pending: no transfer data
                (status = 'Pending'
                    AND progress_bytes IS NULL AND file_size_bytes IS NULL AND transfer_id IS NULL)
                OR
                -- Sending/Interrupted: transfer data required
                (status IN ('Sending', 'Interrupted')
                    AND progress_bytes IS NOT NULL AND progress_bytes >= 0
                    AND file_size_bytes IS NOT NULL AND file_size_bytes >= 0
                    AND transfer_id IS NOT NULL)
                OR
                -- Terminal states: transfer metadata is optional (preserved for diagnostics)
                (status IN ('Sent', 'Rejected', 'Failed'))
            )
        );

        CREATE TABLE IF NOT EXISTS received_files (
            id              TEXT PRIMARY KEY,
            contact_id      TEXT,
            peer_id         TEXT NOT NULL,
            filerequest_id  TEXT NOT NULL,
            transfer_id     TEXT UNIQUE,
            file_name       TEXT NOT NULL,
            file_path       TEXT,
            file_size_bytes INTEGER NOT NULL,
            progress_bytes  INTEGER NOT NULL DEFAULT 0,
            status          TEXT NOT NULL DEFAULT 'Receiving'
                CHECK (status IN ('Receiving', 'Interrupted', 'Completed', 'Failed')),
            started_at_ms   INTEGER NOT NULL DEFAULT 0,
            received_at_ms  INTEGER,
            FOREIGN KEY(filerequest_id) REFERENCES filerequests(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS self_contact (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            contact_id TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            dilithium_private_key BLOB,
            dilithium_public_key BLOB,
            ed25519_private_key BLOB,
            ed25519_public_key BLOB
        );

        CREATE TABLE IF NOT EXISTS settings (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            data BLOB NOT NULL DEFAULT X'',
            version TEXT NOT NULL DEFAULT '0.0.0'
        );"
    )?;

    // Record current schema version for new DB
    tx.execute(
        "INSERT INTO schema_version (version) VALUES (?)",
        [CURRENT_VERSION.to_string()],
    )?;

    tx.commit()?;
    Ok(())
}
