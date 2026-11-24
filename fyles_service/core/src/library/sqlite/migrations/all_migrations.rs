use crate::core::sync::{self, DocKind};

use super::doc_v1::{
    ContactEntryV1, ContactsDocV1, DeviceEntryV1, DevicesDocV1, RemoteFrDocV1, RemoteFrEntryV1,
    fc1_contact_id, fc1_fingerprint, key_fingerprint_v1, new_blob,
};
use super::{Migration, MigrationError};
use autosurgeon::{Hydrate, Reconcile};
use crypto::{
    ContactKeys, ContactPrivateKeys, deserialize_ed25519_private_key,
    deserialize_pq_signing_private_key, serialize_ed25519_public_key,
    serialize_pq_signing_public_key,
};
use rusqlite::{Connection, OptionalExtension};
use semver::Version;
use std::collections::HashMap;
use uuid::Uuid;

/// Create the `sync_docs` table and seed the three CRDT documents from the existing
/// (local) SQLite rows. Every document starts from the shared deterministic genesis so
/// that documents seeded independently on different devices still converge on sync.
fn seed_sync_docs(conn: &Connection) -> Result<(), MigrationError> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS sync_docs (
             doc_kind TEXT PRIMARY KEY,
             data BLOB NOT NULL
         )",
        [],
    )?;

    // Contacts -> ContactsDoc, fingerprinted over the stored, already-serialized key blobs.
    let mut contacts = HashMap::new();
    {
        let mut stmt = conn
            .prepare("SELECT id, name, pq_signing_public_key, ed25519_public_key FROM contacts")?;
        let rows = stmt.query_map([], |row| {
            let id: String = row.get(0)?;
            let name: String = row.get(1)?;
            // Migration 1.10 -> 1.11 mints contacts for unrecognised peers with no key material.
            let pq: Option<Vec<u8>> = row.get(2)?;
            let ed: Option<Vec<u8>> = row.get(3)?;
            Ok((id, name, pq, ed))
        })?;
        for row in rows {
            let (id, name, pq, ed) = row?;
            let key_fingerprint = match (pq, ed) {
                (Some(pq), Some(ed)) => key_fingerprint_v1(&pq, &ed),
                _ => vec![],
            };
            contacts.insert(
                id,
                ContactEntryV1 {
                    name,
                    key_fingerprint,
                },
            );
        }
    }

    // Remote filerequests -> RemoteFrDoc, keyed by their own local id.
    let mut remote_filerequests = HashMap::new();
    {
        let mut stmt = conn.prepare(
            "SELECT id, peer_id, filerequest_id, contact_id, name FROM remote_filerequests",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
            ))
        })?;
        for row in rows {
            let (id, peer_id, filerequest_id, contact_id, name) = row?;
            remote_filerequests.insert(
                id,
                RemoteFrEntryV1 {
                    peer_id_hint: peer_id,
                    filerequest_id,
                    contact_id,
                    name,
                },
            );
        }
    }

    let contacts_blob = new_blob(DocKind::Contacts, &ContactsDocV1 { contacts })?;
    let remote_fr_blob = new_blob(
        DocKind::RemoteFilerequests,
        &RemoteFrDocV1 {
            remote_filerequests,
        },
    )?;

    // Seed this device into the Devices document, derived from the stored node keypair. A fresh
    // install goes through `initialize_tables`, not this migration, so a real upgrade has one.
    // Any failure falls back to an empty devices doc, and the device then registers itself at
    // runtime via `ensure_own_device_registered`.
    let own_peer_id: Option<String> =
        match conn.query_row("SELECT key_bytes FROM node_keys WHERE id = 1", [], |row| {
            row.get::<_, Vec<u8>>(0)
        }) {
            Ok(key_bytes) => match crate::core::p2p::peer_id_from_node_keypair(&key_bytes) {
                Ok(peer_id) => Some(peer_id),
                Err(e) => {
                    tracing::warn!("seed: could not derive peer id from node keys: {e}");
                    None
                }
            },
            Err(e) => {
                tracing::warn!("seed: could not read node keys, seeding empty devices doc: {e}");
                None
            }
        };
    let devices_blob = match own_peer_id {
        Some(peer_id) => {
            let mut devices = HashMap::new();
            devices.insert(
                peer_id,
                DeviceEntryV1 {
                    name: Uuid::new_v4().to_string(),
                },
            );
            new_blob(DocKind::Devices, &DevicesDocV1 { devices })?
        }
        None => sync::empty_blob(DocKind::Devices)?,
    };

    for (kind, blob) in [
        (DocKind::Contacts, contacts_blob),
        (DocKind::RemoteFilerequests, remote_fr_blob),
        (DocKind::Devices, devices_blob),
    ] {
        conn.execute(
            "INSERT OR REPLACE INTO sync_docs (doc_kind, data) VALUES (?, ?)",
            rusqlite::params![kind.as_str(), blob],
        )?;
    }

    // The full remote_filerequests rows now live in the CRDT document, so the SQL table is
    // redundant and gets dropped. `pending_files` foreign-keyed to it; SQLite can't drop a FK in
    // place, so rebuild `pending_files` without the FK first (same shape as schema.rs), then drop
    // the now-unreferenced table. `contact_id` for pending files is resolved from the document.
    conn.execute_batch(
        "CREATE TABLE pending_files_new (
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

            CHECK (
                (status = 'Pending'
                    AND progress_bytes IS NULL AND file_size_bytes IS NULL AND transfer_id IS NULL)
                OR
                (status IN ('Sending', 'Interrupted')
                    AND progress_bytes IS NOT NULL AND progress_bytes >= 0
                    AND file_size_bytes IS NOT NULL AND file_size_bytes >= 0
                    AND transfer_id IS NOT NULL)
                OR
                (status IN ('Sent', 'Rejected', 'Failed'))
            )
        );

        INSERT INTO pending_files_new (id, file_path, target_filerequest_id, status, retry_count, progress_bytes, file_size_bytes, transfer_id, display_name, interruption_reasons, created_at)
        SELECT id, file_path, target_filerequest_id, status, retry_count, progress_bytes, file_size_bytes, transfer_id, display_name, interruption_reasons, created_at
        FROM pending_files;

        DROP TABLE pending_files;
        ALTER TABLE pending_files_new RENAME TO pending_files;

        DROP TABLE remote_filerequests;",
    )?;

    Ok(())
}

pub fn get_migrations() -> Vec<Migration> {
    vec![
        Migration::new(
            Version::new(1, 0, 0), // applies to version
            Version::new(1, 1, 0), // target version
            "Add name to remote_filerequests",
            |conn: &Connection| {
                // First add the column as nullable
                conn.execute("ALTER TABLE remote_filerequests ADD COLUMN name TEXT", [])?;
                // Set initial values using filerequest_id
                conn.execute("UPDATE remote_filerequests SET name = filerequest_id", [])?;
                // Make it NOT NULL by rebuilding the table
                conn.execute_batch(
                    "CREATE TABLE new_remote_filerequests (
                        id TEXT PRIMARY KEY,
                        peer_id TEXT NOT NULL,
                        filerequest_id TEXT NOT NULL,
                        name TEXT NOT NULL,
                        created_at TEXT NOT NULL
                    );
                    INSERT INTO new_remote_filerequests 
                    SELECT id, peer_id, filerequest_id, name, created_at 
                    FROM remote_filerequests;
                    DROP TABLE remote_filerequests;
                    ALTER TABLE new_remote_filerequests RENAME TO remote_filerequests;",
                )?;
                Ok(())
            },
            |conn: &Connection| {
                conn.execute_batch(
                    "CREATE TABLE new_remote_filerequests (
                        id TEXT PRIMARY KEY,
                        peer_id TEXT NOT NULL,
                        filerequest_id TEXT NOT NULL,
                        created_at TEXT NOT NULL
                    );
                    INSERT INTO new_remote_filerequests 
                    SELECT id, peer_id, filerequest_id, created_at 
                    FROM remote_filerequests;
                    DROP TABLE remote_filerequests;
                    ALTER TABLE new_remote_filerequests RENAME TO remote_filerequests;",
                )?;
                Ok(())
            },
        ),
        Migration::new(
            Version::new(1, 1, 0),
            Version::new(1, 2, 0),
            "Replace multiaddr with name in peers table",
            |conn: &Connection| {
                conn.execute_batch(
                    "CREATE TABLE new_peers (
                        id TEXT NOT NULL,
                        peer_id TEXT NOT NULL,
                        contact_id TEXT NOT NULL,
                        name TEXT NOT NULL,
                        is_active BOOLEAN NOT NULL,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        PRIMARY KEY (id, contact_id),
                        FOREIGN KEY (contact_id) REFERENCES contacts(id) ON DELETE CASCADE
                    );
                    INSERT INTO new_peers 
                    SELECT id, peer_id, contact_id, peer_id as name, is_active, created_at, updated_at 
                    FROM peers;
                    DROP TABLE peers;
                    ALTER TABLE new_peers RENAME TO peers;"
                )?;
                Ok(())
            },
            |conn: &Connection| {
                conn.execute_batch(
                    "CREATE TABLE new_peers (
                        id TEXT NOT NULL,
                        peer_id TEXT NOT NULL,
                        contact_id TEXT NOT NULL,
                        multiaddr TEXT NOT NULL,
                        is_active BOOLEAN NOT NULL,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        PRIMARY KEY (id, contact_id),
                        FOREIGN KEY (contact_id) REFERENCES contacts(id) ON DELETE CASCADE
                    );
                    INSERT INTO new_peers 
                    SELECT id, peer_id, contact_id, 'N/A' as multiaddr, is_active, created_at, updated_at 
                    FROM peers;
                    DROP TABLE peers;
                    ALTER TABLE new_peers RENAME TO peers;"
                )?;
                Ok(())
            },
        ),
        Migration::new(
            Version::new(1, 2, 0),
            Version::new(1, 3, 0),
            "Add received_files table, fix peers primary key, and update remote_filerequests to use internal peer IDs",
            |conn| {
                // First fix the peers table to have a single primary key
                conn.execute_batch(
                    "CREATE TABLE new_peers (
                        id TEXT PRIMARY KEY,
                        peer_id TEXT NOT NULL,
                        contact_id TEXT NOT NULL,
                        name TEXT NOT NULL,
                        is_active BOOLEAN NOT NULL,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        FOREIGN KEY (contact_id) REFERENCES contacts(id) ON DELETE CASCADE
                    );
                    INSERT INTO new_peers 
                    SELECT id, peer_id, contact_id, name, is_active, created_at, updated_at 
                    FROM peers;
                    DROP TABLE peers;
                    ALTER TABLE new_peers RENAME TO peers;",
                )?;

                // Then create received_files table with correct foreign key reference
                conn.execute_batch(
                    "CREATE TABLE received_files (
                        id               TEXT PRIMARY KEY,
                        peer_id          TEXT NOT NULL,
                        filerequest_id   TEXT NOT NULL,
                        file_name        TEXT NOT NULL,
                        file_path        TEXT NOT NULL,
                        size             INTEGER NOT NULL,
                        received_at_ms   INTEGER NOT NULL,
                        contact_id       TEXT NOT NULL,
                        FOREIGN KEY(contact_id) REFERENCES contacts(id),
                        FOREIGN KEY(peer_id)    REFERENCES peers(id),
                        FOREIGN KEY(filerequest_id) REFERENCES filerequests(id)
                    );",
                )?;

                // Handle remote_filerequests conversion
                conn.execute_batch(
                    "CREATE TEMP TABLE remote_fr_temp AS SELECT * FROM remote_filerequests;
                     ALTER TABLE remote_filerequests RENAME TO remote_fr_old;",
                )?;

                // Create new table with correct foreign key
                conn.execute_batch(
                    "CREATE TABLE remote_filerequests (
                        id TEXT PRIMARY KEY,
                        peer_id TEXT NOT NULL,
                        filerequest_id TEXT NOT NULL,
                        name TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        FOREIGN KEY(peer_id) REFERENCES peers(id)
                    );",
                )?;

                // Convert existing records
                let old_records: Vec<(String, String, String, String, String)> = {
                    let mut stmt = conn.prepare(
                        "SELECT id, peer_id, filerequest_id, name, created_at FROM remote_fr_temp",
                    )?;
                    let rows = stmt.query_map([], |row| {
                        Ok((
                            row.get(0)?, // id
                            row.get(1)?, // peer_id (libp2p)
                            row.get(2)?, // filerequest_id
                            row.get(3)?, // name
                            row.get(4)?, // created_at
                        ))
                    })?;
                    rows.collect::<Result<Vec<_>, _>>()?
                };

                for (id, libp2p_peer_id, filerequest_id, name, created_at) in old_records {
                    // Try to find existing peer with this libp2p_peer_id
                    let peer_id: Option<String> = conn
                        .query_row(
                            "SELECT id FROM peers WHERE peer_id = ?",
                            [&libp2p_peer_id],
                            |row| row.get(0),
                        )
                        .optional()?;

                    let peer_id = match peer_id {
                        Some(id) => id,
                        None => {
                            // Create new contact for this peer
                            let contact_id = Uuid::new_v4().to_string();
                            conn.execute(
                                "INSERT INTO contacts (id, name, created_at, updated_at)
                                 VALUES (?, ?, datetime('now'), datetime('now'))",
                                [
                                    &contact_id,
                                    &format!("Auto-created for peer {}", &libp2p_peer_id[..8]),
                                ],
                            )?;

                            // Create new peer under its own contact
                            let new_peer_id = Uuid::new_v4().to_string();
                            conn.execute(
                                "INSERT INTO peers (id, peer_id, contact_id, name, is_active, created_at, updated_at)
                                 VALUES (?, ?, ?, ?, true, datetime('now'), datetime('now'))",
                                [
                                    &new_peer_id,
                                    &libp2p_peer_id,
                                    &contact_id,
                                    &format!("Auto-created peer {}", &libp2p_peer_id[..8]),
                                ],
                            )?;
                            new_peer_id
                        }
                    };

                    // Insert record with internal peer_id
                    conn.execute(
                        "INSERT INTO remote_filerequests (id, peer_id, filerequest_id, name, created_at)
                         VALUES (?, ?, ?, ?, ?)",
                        [&id, &peer_id, &filerequest_id, &name, &created_at],
                    )?;
                }

                conn.execute_batch("DROP TABLE remote_fr_old; DROP TABLE remote_fr_temp;")?;
                Ok(())
            },
            |conn| {
                // Downgrade path - first restore the composite key in peers
                conn.execute_batch(
                    "CREATE TABLE new_peers (
                        id TEXT NOT NULL,
                        peer_id TEXT NOT NULL,
                        contact_id TEXT NOT NULL,
                        name TEXT NOT NULL,
                        is_active BOOLEAN NOT NULL,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        PRIMARY KEY (id, contact_id),
                        FOREIGN KEY (contact_id) REFERENCES contacts(id) ON DELETE CASCADE
                    );
                    INSERT INTO new_peers 
                    SELECT id, peer_id, contact_id, name, is_active, created_at, updated_at 
                    FROM peers;
                    DROP TABLE peers;
                    ALTER TABLE new_peers RENAME TO peers;",
                )?;

                // Convert back to old schema
                conn.execute_batch(
                    "CREATE TEMP TABLE remote_fr_temp AS SELECT * FROM remote_filerequests;
                     ALTER TABLE remote_filerequests RENAME TO remote_fr_old;",
                )?;

                // Recreate old schema
                conn.execute_batch(
                    "CREATE TABLE remote_filerequests (
                        id TEXT PRIMARY KEY,
                        peer_id TEXT NOT NULL,
                        filerequest_id TEXT NOT NULL,
                        name TEXT NOT NULL,
                        created_at TEXT NOT NULL
                    );",
                )?;

                // Convert back using original libp2p peer IDs
                let records: Vec<(String, String, String, String, String)> = {
                    let mut stmt = conn.prepare(
                        "SELECT r.id, p.peer_id, r.filerequest_id, r.name, r.created_at 
                         FROM remote_fr_temp r
                         JOIN peers p ON r.peer_id = p.id",
                    )?;
                    let rows = stmt.query_map([], |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                        ))
                    })?;
                    rows.collect::<Result<Vec<_>, _>>()?
                };

                for (id, libp2p_peer_id, filerequest_id, name, created_at) in records {
                    conn.execute(
                        "INSERT INTO remote_filerequests (id, peer_id, filerequest_id, name, created_at)
                         VALUES (?, ?, ?, ?, ?)",
                        [&id, &libp2p_peer_id, &filerequest_id, &name, &created_at],
                    )?;
                }

                // Cleanup
                conn.execute_batch(
                    "DROP TABLE remote_fr_old;
                     DROP TABLE remote_fr_temp;
                     DROP TABLE IF EXISTS received_files;",
                )?;

                Ok(())
            },
        ),
        Migration::new(
            Version::new(1, 3, 0),
            Version::new(1, 4, 0),
            "Add index on peer_id column in peers table",
            |conn| {
                conn.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_peers_peer_id ON peers(peer_id)",
                    [],
                )?;
                Ok(())
            },
            |conn| {
                conn.execute("DROP INDEX IF EXISTS idx_peers_peer_id", [])?;
                Ok(())
            },
        ),
        Migration::new(
            Version::new(1, 4, 0),
            Version::new(1, 5, 0),
            "Add soft deletion support for contacts and peers",
            |conn| {
                // Add deleted_at columns
                conn.execute_batch(
                    "ALTER TABLE contacts ADD COLUMN deleted_at TEXT;
                     ALTER TABLE peers ADD COLUMN deleted_at TEXT;
                     
                     -- Create indices for better performance on common queries
                     CREATE INDEX idx_contacts_deleted_at ON contacts(deleted_at);
                     CREATE INDEX idx_peers_deleted_at ON peers(deleted_at);",
                )?;
                Ok(())
            },
            |conn| {
                conn.execute_batch(
                    "DROP INDEX IF EXISTS idx_peers_deleted_at;
                     DROP INDEX IF EXISTS idx_contacts_deleted_at;
                     
                     CREATE TABLE new_contacts (
                         id TEXT PRIMARY KEY,
                         name TEXT NOT NULL,
                         created_at TEXT NOT NULL,
                         updated_at TEXT NOT NULL
                     );
                     INSERT INTO new_contacts SELECT id, name, created_at, updated_at FROM contacts;
                     DROP TABLE contacts;
                     ALTER TABLE new_contacts RENAME TO contacts;
                     
                     CREATE TABLE new_peers (
                         id TEXT PRIMARY KEY,
                         peer_id TEXT NOT NULL,
                         contact_id TEXT NOT NULL,
                         name TEXT NOT NULL,
                         is_active BOOLEAN NOT NULL,
                         created_at TEXT NOT NULL,
                         updated_at TEXT NOT NULL,
                         FOREIGN KEY (contact_id) REFERENCES contacts(id) ON DELETE CASCADE
                     );
                     INSERT INTO new_peers SELECT id, peer_id, contact_id, name, is_active, created_at, updated_at FROM peers;
                     DROP TABLE peers;
                     ALTER TABLE new_peers RENAME TO peers;
                     CREATE UNIQUE INDEX idx_peers_peer_id ON peers(peer_id);"
                )?;
                Ok(())
            },
        ),
        Migration::new(
            Version::new(1, 5, 0),
            Version::new(1, 6, 0),
            "Update filerequest access_type constraint and modify audience table to use contacts",
            |conn| {
                conn.execute("PRAGMA foreign_keys = OFF", [])?;
                conn.execute("PRAGMA legacy_alter_table = ON", [])?;

                // Create new tables with updated schema
                conn.execute_batch(
                    "CREATE TABLE new_filerequests (
                        id TEXT PRIMARY KEY,
                        title TEXT NOT NULL,
                        description TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        status TEXT CHECK (status IN ('active', 'paused')) NOT NULL DEFAULT 'active',
                        access_type TEXT CHECK (access_type IN ('public', 'audience')) NOT NULL DEFAULT 'audience'
                    );

                    -- Create new audience table referencing new filerequests table
                    CREATE TABLE new_filerequest_audience (
                        filerequest_id TEXT NOT NULL,
                        contact_id TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        PRIMARY KEY (filerequest_id, contact_id),
                        FOREIGN KEY (filerequest_id) REFERENCES new_filerequests(id) ON DELETE CASCADE,
                        FOREIGN KEY (contact_id) REFERENCES contacts(id) ON DELETE CASCADE
                    );

                    -- Create new received_files table referencing new filerequests table
                    CREATE TABLE new_received_files (
                        id TEXT PRIMARY KEY,
                        peer_id TEXT NOT NULL,
                        filerequest_id TEXT NOT NULL,
                        file_name TEXT NOT NULL,
                        file_path TEXT NOT NULL,
                        size INTEGER NOT NULL,
                        received_at_ms INTEGER NOT NULL,
                        contact_id TEXT NOT NULL,
                        FOREIGN KEY(contact_id) REFERENCES contacts(id),
                        FOREIGN KEY(peer_id) REFERENCES peers(id),
                        FOREIGN KEY(filerequest_id) REFERENCES new_filerequests(id)
                    );"
                )?;

                // Copy all data to new tables
                conn.execute_batch(
                    "INSERT INTO new_filerequests 
                     SELECT id, title, description, created_at, updated_at, status,
                            CASE WHEN access_type IN ('public', 'audience') 
                                 THEN access_type 
                                 ELSE 'audience' 
                            END
                     FROM filerequests;
                     
                     INSERT INTO new_filerequest_audience 
                     SELECT filerequest_id, peer_id, created_at
                     FROM filerequest_audience;

                     INSERT INTO new_received_files
                     SELECT * FROM received_files;",
                )?;

                // Drop old tables and rename new ones
                conn.execute_batch(
                    "DROP TABLE received_files;
                     DROP TABLE filerequest_audience;
                     DROP TABLE filerequests;
                     
                     ALTER TABLE new_filerequests RENAME TO filerequests;
                     ALTER TABLE new_filerequest_audience RENAME TO filerequest_audience;
                     ALTER TABLE new_received_files RENAME TO received_files;",
                )?;

                conn.execute("PRAGMA legacy_alter_table = OFF", [])?;
                conn.execute("PRAGMA foreign_keys = ON", [])?;

                Ok(())
            },
            |conn| {
                // Same pattern for downgrade
                conn.execute("PRAGMA foreign_keys = OFF", [])?;
                conn.execute("PRAGMA legacy_alter_table = ON", [])?;

                // Create new tables first
                conn.execute_batch(
                    "CREATE TABLE new_filerequests (
                        id TEXT PRIMARY KEY,
                        title TEXT NOT NULL,
                        description TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        status TEXT CHECK (status IN ('active', 'paused')) NOT NULL DEFAULT 'active',
                        access_type TEXT NOT NULL
                    );

                    CREATE TABLE new_filerequest_audience (
                        filerequest_id TEXT NOT NULL,
                        peer_id TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        PRIMARY KEY (filerequest_id, peer_id),
                        FOREIGN KEY (filerequest_id) REFERENCES new_filerequests(id) ON DELETE CASCADE
                    );

                    CREATE TABLE new_received_files (
                        id TEXT PRIMARY KEY,
                        peer_id TEXT NOT NULL,
                        filerequest_id TEXT NOT NULL,
                        file_name TEXT NOT NULL,
                        file_path TEXT NOT NULL,
                        size INTEGER NOT NULL,
                        received_at_ms INTEGER NOT NULL,
                        contact_id TEXT NOT NULL,
                        FOREIGN KEY(contact_id) REFERENCES contacts(id),
                        FOREIGN KEY(peer_id) REFERENCES peers(id),
                        FOREIGN KEY(filerequest_id) REFERENCES new_filerequests(id)
                    );"
                )?;

                // Copy data
                conn.execute_batch(
                    "INSERT INTO new_filerequests SELECT * FROM filerequests;
                     
                     INSERT INTO new_filerequest_audience (filerequest_id, peer_id, created_at)
                     SELECT filerequest_id, contact_id, created_at FROM filerequest_audience;

                     INSERT INTO new_received_files SELECT * FROM received_files;",
                )?;

                // Drop and rename
                conn.execute_batch(
                    "DROP TABLE received_files;
                     DROP TABLE filerequest_audience;
                     DROP TABLE filerequests;
                     
                     ALTER TABLE new_filerequests RENAME TO filerequests;
                     ALTER TABLE new_filerequest_audience RENAME TO filerequest_audience;
                     ALTER TABLE new_received_files RENAME TO received_files;",
                )?;

                conn.execute("PRAGMA legacy_alter_table = OFF", [])?;
                conn.execute("PRAGMA foreign_keys = ON", [])?;

                Ok(())
            },
        ),
        Migration::new(
            Version::new(1, 6, 0),
            Version::new(1, 7, 0),
            "Update received_files to use libp2p peer IDs directly",
            |conn| {
                // Create new table without contact_id and with peer_id as libp2p ID
                conn.execute_batch(
                    "CREATE TABLE new_received_files (
                        id TEXT PRIMARY KEY,
                        peer_id TEXT NOT NULL,
                        filerequest_id TEXT NOT NULL,
                        file_name TEXT NOT NULL,
                        file_path TEXT NOT NULL,
                        size INTEGER NOT NULL,
                        received_at_ms INTEGER NOT NULL,
                        FOREIGN KEY(filerequest_id) REFERENCES filerequests(id)
                    );

                    -- Copy existing records, getting original libp2p peer_id from peers table
                    INSERT INTO new_received_files (id, peer_id, filerequest_id, file_name, file_path, size, received_at_ms)
                    SELECT r.id, p.peer_id, r.filerequest_id, r.file_name, r.file_path, r.size, r.received_at_ms
                    FROM received_files r
                    JOIN peers p ON r.peer_id = p.id;

                    -- Drop old table and rename new one
                    DROP TABLE received_files;
                    ALTER TABLE new_received_files RENAME TO received_files;"
                )?;
                Ok(())
            },
            |conn| {
                // Downgrade - recreate table with old structure
                conn.execute_batch(
                    "CREATE TABLE new_received_files (
                        id TEXT PRIMARY KEY,
                        peer_id TEXT NOT NULL,
                        filerequest_id TEXT NOT NULL,
                        file_name TEXT NOT NULL,
                        file_path TEXT NOT NULL,
                        size INTEGER NOT NULL,
                        received_at_ms INTEGER NOT NULL,
                        contact_id TEXT NOT NULL,
                        FOREIGN KEY(contact_id) REFERENCES contacts(id),
                        FOREIGN KEY(peer_id) REFERENCES peers(id),
                        FOREIGN KEY(filerequest_id) REFERENCES filerequests(id)
                    );

                    -- We'll lose some data on downgrade as we can't reliably reconstruct the relationships
                    -- Copy only records where we can find matching peers
                    INSERT INTO new_received_files (id, peer_id, filerequest_id, file_name, file_path, size, received_at_ms, contact_id)
                    SELECT r.id, p.id, r.filerequest_id, r.file_name, r.file_path, r.size, r.received_at_ms, p.contact_id
                    FROM received_files r
                    JOIN peers p ON r.peer_id = p.peer_id;

                    -- Drop old table and rename new one
                    DROP TABLE received_files;
                    ALTER TABLE new_received_files RENAME TO received_files;"
                )?;
                Ok(())
            },
        ),
        Migration::new(
            Version::new(1, 7, 0),
            Version::new(1, 7, 1),
            "Add ON DELETE CASCADE to received_files and pending_files foreign keys",
            |conn| {
                conn.execute_batch(
                    "PRAGMA foreign_keys=OFF;
                     
                     -- Update received_files
                     CREATE TABLE received_files_new (
                         id            TEXT PRIMARY KEY,
                         peer_id       TEXT NOT NULL,
                         filerequest_id TEXT NOT NULL,
                         file_name     TEXT NOT NULL,
                         file_path     TEXT NOT NULL,
                         size          INTEGER NOT NULL,
                         received_at_ms INTEGER NOT NULL,
                         FOREIGN KEY(filerequest_id) REFERENCES filerequests(id) ON DELETE CASCADE
                     );

                     INSERT INTO received_files_new 
                     SELECT id, peer_id, filerequest_id, file_name, file_path, size, received_at_ms
                     FROM received_files;

                     DROP TABLE received_files;
                     ALTER TABLE received_files_new RENAME TO received_files;

                     -- Update pending_files
                     CREATE TABLE pending_files_new (
                         id TEXT PRIMARY KEY,
                         file_path TEXT NOT NULL,
                         target_filerequest_id TEXT NOT NULL,
                         status TEXT CHECK (status IN ('Pending', 'Sending', 'Sent', 'Rejected', 'Failed')) NOT NULL DEFAULT 'Pending',
                         created_at TEXT NOT NULL,
                         FOREIGN KEY(target_filerequest_id) REFERENCES remote_filerequests(id) ON DELETE CASCADE
                     );

                     INSERT INTO pending_files_new 
                     SELECT id, file_path, target_filerequest_id, status, created_at
                     FROM pending_files;

                     DROP TABLE pending_files;
                     ALTER TABLE pending_files_new RENAME TO pending_files;
                     
                     PRAGMA foreign_keys=ON;"
                )?;
                Ok(())
            },
            |conn| {
                conn.execute_batch(
                    "PRAGMA foreign_keys=OFF;
                     
                     -- Revert received_files
                     CREATE TABLE received_files_new (
                         id            TEXT PRIMARY KEY,
                         peer_id       TEXT NOT NULL,
                         filerequest_id TEXT NOT NULL,
                         file_name     TEXT NOT NULL,
                         file_path     TEXT NOT NULL,
                         size          INTEGER NOT NULL,
                         received_at_ms INTEGER NOT NULL,
                         FOREIGN KEY(filerequest_id) REFERENCES filerequests(id)
                     );

                     INSERT INTO received_files_new 
                     SELECT id, peer_id, filerequest_id, file_name, file_path, size, received_at_ms
                     FROM received_files;

                     DROP TABLE received_files;
                     ALTER TABLE received_files_new RENAME TO received_files;

                     -- Revert pending_files
                     CREATE TABLE pending_files_new (
                         id TEXT PRIMARY KEY,
                         file_path TEXT NOT NULL,
                         target_filerequest_id TEXT NOT NULL,
                         status TEXT CHECK (status IN ('Pending', 'Sending', 'Sent', 'Rejected', 'Failed')) NOT NULL DEFAULT 'Pending',
                         created_at TEXT NOT NULL,
                         FOREIGN KEY(target_filerequest_id) REFERENCES remote_filerequests(id)
                     );

                     INSERT INTO pending_files_new 
                     SELECT id, file_path, target_filerequest_id, status, created_at
                     FROM pending_files;

                     DROP TABLE pending_files;
                     ALTER TABLE pending_files_new RENAME TO pending_files;
                     
                     PRAGMA foreign_keys=ON;"
                )?;
                Ok(())
            },
        ),
        // Migration to remove is_active from peers
        Migration::new(
            Version::new(1, 7, 1),
            Version::new(1, 8, 0),
            "Remove is_active column from peers table",
            |conn| {
                conn.execute_batch(
                    "PRAGMA foreign_keys=OFF;
                     CREATE TABLE new_peers (
                         id TEXT PRIMARY KEY,
                         peer_id TEXT NOT NULL,
                         contact_id TEXT NOT NULL,
                         name TEXT NOT NULL,
                         created_at TEXT NOT NULL,
                         updated_at TEXT NOT NULL,
                         FOREIGN KEY(contact_id) REFERENCES contacts(id) ON DELETE CASCADE
                     );
                     INSERT INTO new_peers (id, peer_id, contact_id, name, created_at, updated_at)
                     SELECT id, peer_id, contact_id, name, created_at, updated_at FROM peers;
                     DROP TABLE peers;
                     ALTER TABLE new_peers RENAME TO peers;
                     PRAGMA foreign_keys=ON;",
                )?;
                Ok(())
            },
            |conn| {
                conn.execute_batch(
                    "PRAGMA foreign_keys=OFF;
                     CREATE TABLE new_peers (
                         id TEXT PRIMARY KEY,
                         peer_id TEXT NOT NULL,
                         contact_id TEXT NOT NULL,
                         name TEXT NOT NULL,
                         is_active BOOLEAN NOT NULL DEFAULT 0,
                         created_at TEXT NOT NULL,
                         updated_at TEXT NOT NULL,
                         FOREIGN KEY(contact_id) REFERENCES contacts(id) ON DELETE CASCADE
                     );
                     INSERT INTO new_peers (id, peer_id, contact_id, name, is_active, created_at, updated_at)
                     SELECT id, peer_id, contact_id, name, 0, created_at, updated_at FROM peers;
                     DROP TABLE peers;
                     ALTER TABLE new_peers RENAME TO peers;
                     PRAGMA foreign_keys=ON;"
                )?;
                Ok(())
            },
        ),
        // Migration to add self_contact CRDT storage
        Migration::new(
            Version::new(1, 8, 0),
            Version::new(1, 9, 0),
            "Add self_contact table for storing CRDT blob",
            |conn| {
                conn.execute_batch(
                    "CREATE TABLE IF NOT EXISTS self_contact (
                        data BLOB NOT NULL,
                        schema_version TEXT NOT NULL
                     );",
                )?;
                Ok(())
            },
            |conn| Ok(conn.execute_batch("DROP TABLE IF EXISTS self_contact;")?),
        ),
        Migration::new(
            Version::new(1, 9, 0),
            Version::new(1, 10, 0),
            "Add post-quantum crypto support: extended node_keys, required contact_id in RemoteFilerequest, contact public keys",
            |conn| {
                // Simply drop the old table and create a new empty one with the updated schema
                conn.execute_batch(
                    "DROP TABLE IF EXISTS remote_filerequests;
                     
                     CREATE TABLE remote_filerequests (
                        id TEXT PRIMARY KEY,
                        peer_id TEXT NOT NULL,
                        filerequest_id TEXT NOT NULL,
                        name TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        requires_authentication BOOLEAN DEFAULT 1,
                        contact_id TEXT NOT NULL
                    );",
                )?;

                // Modify node_keys table to store additional cryptographic keys
                conn.execute_batch(
                    "DROP TABLE IF EXISTS self_contact;

                     CREATE TABLE self_contact (
                        id INTEGER PRIMARY KEY CHECK (id = 1),
                        contact_id TEXT NOT NULL,
                        name TEXT NOT NULL,
                        dilithium_private_key BLOB,
                        dilithium_public_key BLOB,
                        ed25519_private_key BLOB,
                        ed25519_public_key BLOB
                     );",
                )?;

                // Add public key columns to contacts table
                conn.execute_batch(
                    "ALTER TABLE contacts ADD COLUMN dilithium_public_key BLOB;
                     ALTER TABLE contacts ADD COLUMN ed25519_public_key BLOB;",
                )?;

                Ok(())
            },
            |conn| {
                // Downgrade - create a simpler table without the required columns
                conn.execute_batch(
                    "DROP TABLE IF EXISTS remote_filerequests;
                     
                     CREATE TABLE remote_filerequests (
                        id TEXT PRIMARY KEY,
                        peer_id TEXT NOT NULL,
                        filerequest_id TEXT NOT NULL,
                        name TEXT NOT NULL,
                        created_at TEXT NOT NULL
                     );",
                )?;

                conn.execute_batch(
                    "CREATE TABLE node_keys_old AS 
                     SELECT id, key_bytes FROM node_keys;
                     DROP TABLE node_keys;
                     ALTER TABLE node_keys_old RENAME TO node_keys;",
                )?;

                // Remove public key columns from contacts
                conn.execute_batch(
                    "CREATE TABLE contacts_old AS 
                     SELECT id, name, created_at, updated_at FROM contacts;
                     DROP TABLE contacts;
                     ALTER TABLE contacts_old RENAME TO contacts;",
                )?;

                // Restore old self contact format
                conn.execute_batch(
                    "DROP TABLE IF EXISTS self_contact;
                     
                     CREATE TABLE self_contact (
                        data BLOB NOT NULL,
                        schema_version TEXT NOT NULL
                     );",
                )?;

                Ok(())
            },
        ),
        Migration::new(
            Version::new(1, 10, 0),
            Version::new(1, 11, 0),
            "Modify received_files table to replace peer_id with contact_id and add verified_sender",
            |conn| {
                // Lookup table for converting peer_id to contact_id
                let mut peer_to_contact: HashMap<String, String> = HashMap::new();

                // Create a mapping of peer_id to contact_id
                {
                    let mut stmt = conn.prepare("SELECT peer_id, contact_id FROM peers")?;
                    let rows = stmt.query_map([], |row| {
                        let peer_id: String = row.get(0)?;
                        let contact_id: String = row.get(1)?;
                        Ok((peer_id, contact_id))
                    })?;

                    for result in rows {
                        let (peer_id, contact_id) = result?;
                        peer_to_contact.insert(peer_id, contact_id);
                    }
                }

                // Create new table with updated schema (removed foreign key to contacts)
                conn.execute_batch(
                    "CREATE TABLE received_files_new (
                        id TEXT PRIMARY KEY,
                        contact_id TEXT NOT NULL,
                        filerequest_id TEXT NOT NULL,
                        file_name TEXT NOT NULL,
                        file_path TEXT NOT NULL,
                        size INTEGER NOT NULL,
                        received_at_ms INTEGER NOT NULL,
                        verified_sender BOOLEAN NOT NULL DEFAULT 0,
                        FOREIGN KEY(filerequest_id) REFERENCES filerequests(id) ON DELETE CASCADE
                    );",
                )?;

                // Copy existing data to new table
                {
                    let mut stmt = conn.prepare(
                        "SELECT id, peer_id, filerequest_id, file_name, file_path, size, received_at_ms 
                         FROM received_files"
                    )?;

                    let rows = stmt.query_map([], |row| {
                        let id: String = row.get(0)?;
                        let peer_id: String = row.get(1)?;
                        let filerequest_id: String = row.get(2)?;
                        let file_name: String = row.get(3)?;
                        let file_path: String = row.get(4)?;
                        let size: i64 = row.get(5)?;
                        let received_at_ms: i64 = row.get(6)?;

                        Ok((
                            id,
                            peer_id,
                            filerequest_id,
                            file_name,
                            file_path,
                            size,
                            received_at_ms,
                        ))
                    })?;

                    for result in rows {
                        let (
                            id,
                            peer_id,
                            filerequest_id,
                            file_name,
                            file_path,
                            size,
                            received_at_ms,
                        ) = result?;

                        // Try to find the contact_id for this peer_id
                        // If we can't find it, use a placeholder unknown contact id
                        let contact_id =
                            peer_to_contact.get(&peer_id).cloned().unwrap_or_else(|| {
                                // If we can't find the contact, create a new one for this peer
                                let contact_id = Uuid::new_v4().to_string();
                                conn.execute(
                                    "INSERT INTO contacts (id, name, created_at, updated_at) 
                                 VALUES (?, ?, datetime('now'), datetime('now'))",
                                    [&contact_id, &format!("Unknown sender {}", &peer_id[..8])],
                                )
                                .ok();

                                contact_id
                            });

                        // Insert into new table - set verified_sender to false for migrated entries
                        conn.execute(
                            "INSERT INTO received_files_new 
                             (id, contact_id, filerequest_id, file_name, file_path, size, received_at_ms, verified_sender) 
                             VALUES (?, ?, ?, ?, ?, ?, ?, 0)",
                            [&id, &contact_id, &filerequest_id, &file_name, &file_path, &size.to_string(), &received_at_ms.to_string()],
                        )?;
                    }
                }

                // Replace old table with new table
                conn.execute("DROP TABLE received_files", [])?;
                conn.execute(
                    "ALTER TABLE received_files_new RENAME TO received_files",
                    [],
                )?;

                Ok(())
            },
            |conn| {
                // Downgrade path - restore the original schema
                conn.execute_batch(
                    "CREATE TABLE received_files_new (
                        id TEXT PRIMARY KEY,
                        peer_id TEXT NOT NULL,
                        filerequest_id TEXT NOT NULL,
                        file_name TEXT NOT NULL,
                        file_path TEXT NOT NULL,
                        size INTEGER NOT NULL,
                        received_at_ms INTEGER NOT NULL,
                        FOREIGN KEY(filerequest_id) REFERENCES filerequests(id) ON DELETE CASCADE
                    );",
                )?;

                // For the downgrade, we'll make a best effort - get first peer for each contact
                let contact_to_peer: HashMap<String, String> = {
                    let mut stmt =
                        conn.prepare("SELECT contact_id, peer_id FROM peers GROUP BY contact_id")?;

                    let rows = stmt.query_map([], |row| {
                        let contact_id: String = row.get(0)?;
                        let peer_id: String = row.get(1)?;
                        Ok((contact_id, peer_id))
                    })?;

                    rows.map(|r| r.unwrap()).collect()
                };

                // Copy data to new table, using a default peer_id if we can't find one
                {
                    let mut stmt = conn.prepare(
                        "SELECT id, contact_id, filerequest_id, file_name, file_path, size, received_at_ms
                         FROM received_files"
                    )?;

                    let rows = stmt.query_map([], |row| {
                        let id: String = row.get(0)?;
                        let contact_id: String = row.get(1)?;
                        let filerequest_id: String = row.get(2)?;
                        let file_name: String = row.get(3)?;
                        let file_path: String = row.get(4)?;
                        let size: i64 = row.get(5)?;
                        let received_at_ms: i64 = row.get(6)?;

                        Ok((
                            id,
                            contact_id,
                            filerequest_id,
                            file_name,
                            file_path,
                            size,
                            received_at_ms,
                        ))
                    })?;

                    for result in rows {
                        let (
                            id,
                            contact_id,
                            filerequest_id,
                            file_name,
                            file_path,
                            size,
                            received_at_ms,
                        ) = result?;

                        // Find a peer_id for this contact or use a placeholder
                        let peer_id = contact_to_peer
                            .get(&contact_id)
                            .cloned()
                            .unwrap_or_else(|| "unknown".to_string());

                        conn.execute(
                            "INSERT INTO received_files_new
                             (id, peer_id, filerequest_id, file_name, file_path, size, received_at_ms)
                             VALUES (?, ?, ?, ?, ?, ?, ?)",
                            [&id, &peer_id, &filerequest_id, &file_name, &file_path, &size.to_string(), &received_at_ms.to_string()],
                        )?;
                    }
                }

                // Replace old table with new table
                conn.execute("DROP TABLE received_files", [])?;
                conn.execute(
                    "ALTER TABLE received_files_new RENAME TO received_files",
                    [],
                )?;

                Ok(())
            },
        ),
        Migration::new(
            Version::new(1, 11, 0),
            Version::new(1, 12, 0),
            "Remove peers table",
            |conn| {
                // Drop the peers table and its index
                conn.execute("PRAGMA foreign_keys = OFF", [])?;
                conn.execute("DROP INDEX IF EXISTS idx_peers_peer_id", [])?;
                conn.execute("DROP TABLE IF EXISTS peers", [])?;
                conn.execute("PRAGMA foreign_keys = ON", [])?;

                Ok(())
            },
            |conn| {
                // Recreate the peers table on downgrade
                conn.execute("PRAGMA foreign_keys = OFF", [])?;

                // Just recreate the empty peers table with its structure
                conn.execute_batch(
                    "CREATE TABLE IF NOT EXISTS peers (
                        id TEXT PRIMARY KEY,
                        peer_id TEXT NOT NULL,
                        contact_id TEXT NOT NULL,
                        name TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        FOREIGN KEY (contact_id) REFERENCES contacts(id) ON DELETE CASCADE
                    );
                    
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_peers_peer_id ON peers(peer_id);",
                )?;

                conn.execute("PRAGMA foreign_keys = ON", [])?;

                Ok(())
            },
        ),
        Migration::new(
            Version::new(1, 12, 0),
            Version::new(1, 13, 0),
            "Remove requires_authentication from remote_filerequests",
            |conn| {
                // Start transaction
                conn.execute("PRAGMA foreign_keys = OFF", [])?;

                // Create new table without the requires_authentication column
                conn.execute_batch(
                    "CREATE TABLE remote_filerequests_new (
                        id TEXT PRIMARY KEY,
                        peer_id TEXT NOT NULL,
                        filerequest_id TEXT NOT NULL,
                        name TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        contact_id TEXT NOT NULL
                    );
                    
                    -- Copy data, excluding requires_authentication
                    INSERT INTO remote_filerequests_new (id, peer_id, filerequest_id, name, created_at, contact_id)
                    SELECT id, peer_id, filerequest_id, name, created_at, contact_id
                    FROM remote_filerequests;
                    
                    -- Replace old table
                    DROP TABLE remote_filerequests;
                    ALTER TABLE remote_filerequests_new RENAME TO remote_filerequests;"
                )?;

                conn.execute("PRAGMA foreign_keys = ON", [])?;

                Ok(())
            },
            |conn| {
                // Downgrade path - restore the column with a default value
                conn.execute("PRAGMA foreign_keys = OFF", [])?;

                conn.execute_batch(
                    "CREATE TABLE remote_filerequests_new (
                        id TEXT PRIMARY KEY,
                        peer_id TEXT NOT NULL,
                        filerequest_id TEXT NOT NULL,
                        name TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        requires_authentication BOOLEAN DEFAULT 1,
                        contact_id TEXT NOT NULL
                    );
                    
                    -- Copy data, adding default requires_authentication value (1/true)
                    INSERT INTO remote_filerequests_new (id, peer_id, filerequest_id, name, created_at, requires_authentication, contact_id)
                    SELECT id, peer_id, filerequest_id, name, created_at, 1, contact_id
                    FROM remote_filerequests;
                    
                    -- Replace old table
                    DROP TABLE remote_filerequests;
                    ALTER TABLE remote_filerequests_new RENAME TO remote_filerequests;"
                )?;

                conn.execute("PRAGMA foreign_keys = ON", [])?;

                Ok(())
            },
        ),
        Migration::new(
            Version::new(1, 14, 0),
            Version::new(1, 15, 0),
            "Make contact_id nullable and remove verified_sender from received_files",
            |conn: &Connection| {
                // Create a new table with the updated structure
                conn.execute(
                    "CREATE TABLE received_files_new (
                        id            TEXT PRIMARY KEY,
                        contact_id    TEXT,
                        filerequest_id TEXT NOT NULL,
                        file_name     TEXT NOT NULL,
                        file_path     TEXT NOT NULL,
                        size          INTEGER NOT NULL,
                        received_at_ms INTEGER NOT NULL,
                        FOREIGN KEY(filerequest_id) REFERENCES filerequests(id) ON DELETE CASCADE
                    )",
                    [],
                )?;

                // Copy data from old table to new table (excluding verified_sender)
                conn.execute(
                    "INSERT INTO received_files_new (id, contact_id, filerequest_id, file_name, file_path, size, received_at_ms)
                     SELECT id, contact_id, filerequest_id, file_name, file_path, size, received_at_ms
                     FROM received_files",
                    [],
                )?;

                // Drop the old table
                conn.execute("DROP TABLE received_files", [])?;

                // Rename the new table
                conn.execute(
                    "ALTER TABLE received_files_new RENAME TO received_files",
                    [],
                )?;

                Ok(())
            },
            |conn: &Connection| {
                // Rollback: recreate the old table structure
                conn.execute(
                    "CREATE TABLE received_files_new (
                        id            TEXT PRIMARY KEY,
                        contact_id    TEXT NOT NULL,
                        filerequest_id TEXT NOT NULL,
                        file_name     TEXT NOT NULL,
                        file_path     TEXT NOT NULL,
                        size          INTEGER NOT NULL,
                        received_at_ms INTEGER NOT NULL,
                        verified_sender BOOLEAN NOT NULL DEFAULT 0,
                        FOREIGN KEY(filerequest_id) REFERENCES filerequests(id) ON DELETE CASCADE
                    )",
                    [],
                )?;

                // Copy data back (adding default value for verified_sender)
                conn.execute(
                    "INSERT INTO received_files_new (id, contact_id, filerequest_id, file_name, file_path, size, received_at_ms, verified_sender)
                     SELECT id, contact_id, filerequest_id, file_name, file_path, size, received_at_ms, 0
                     FROM received_files",
                    [],
                )?;

                // Drop the current table
                conn.execute("DROP TABLE received_files", [])?;

                // Rename the new table
                conn.execute(
                    "ALTER TABLE received_files_new RENAME TO received_files",
                    [],
                )?;

                Ok(())
            },
        ),
        Migration::new(
            Version::new(1, 15, 0),
            Version::new(1, 16, 0),
            "Add opaque settings table",
            |conn: &Connection| {
                conn.execute_batch(
                    "CREATE TABLE IF NOT EXISTS settings (
                        id INTEGER PRIMARY KEY CHECK (id = 1),
                        data BLOB NOT NULL DEFAULT X'',
                        version TEXT NOT NULL DEFAULT '0.0.0'
                    );

                    INSERT OR IGNORE INTO settings (id, data, version) VALUES (1, X'', '0.0.0');",
                )?;
                Ok(())
            },
            |conn: &Connection| {
                conn.execute("DROP TABLE IF EXISTS settings", [])?;
                Ok(())
            },
        ),
        Migration::new(
            Version::new(1, 16, 0),
            Version::new(1, 17, 0),
            "Expand pending_files status with progress tracking; extend received_files with receive-state tracking",
            |conn: &Connection| {
                conn.execute("PRAGMA foreign_keys = OFF", [])?;

                // ── pending_files: add structured status columns ──
                conn.execute_batch(
                    "CREATE TABLE pending_files_new (
                        id TEXT PRIMARY KEY,
                        file_path TEXT NOT NULL,
                        target_filerequest_id TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'Pending'
                            CHECK (status IN ('Pending', 'Sending', 'Interrupted', 'Sent', 'Rejected', 'Failed')),
                        retry_count INTEGER NOT NULL DEFAULT 0,
                        progress_bytes INTEGER,
                        file_size_bytes INTEGER,
                        transfer_id TEXT,
                        created_at TEXT NOT NULL,
                        FOREIGN KEY(target_filerequest_id) REFERENCES remote_filerequests(id) ON DELETE CASCADE,

                        CHECK (
                            -- Pending: no transfer data
                            (status = 'Pending'
                                AND progress_bytes IS NULL
                                AND file_size_bytes IS NULL
                                AND transfer_id IS NULL)
                            OR
                            -- Sending/Interrupted: transfer data required
                            (status IN ('Prepared', 'Sending', 'Interrupted')
                                AND progress_bytes IS NOT NULL
                                AND progress_bytes >= 0
                                AND file_size_bytes IS NOT NULL
                                AND file_size_bytes >= 0
                                AND transfer_id IS NOT NULL)
                            OR
                            -- Sent: terminal success, no transfer data needed
                            (status = 'Sent'
                                AND progress_bytes IS NULL
                                AND file_size_bytes IS NULL
                                AND transfer_id IS NULL)
                            OR
                            -- Rejected: terminal, no transfer data needed
                            (status = 'Rejected'
                                AND progress_bytes IS NULL
                                AND file_size_bytes IS NULL
                                AND transfer_id IS NULL)
                            OR
                            -- Failed: terminal, no transfer data needed
                            (status = 'Failed'
                                AND progress_bytes IS NULL
                                AND file_size_bytes IS NULL
                                AND transfer_id IS NULL)
                        )
                    );"
                )?;

                // Cleanup during migration: any row that was in 'Sending' state is evidence
                // of a dirty exit. Reset those to 'Pending' so they get retried cleanly.
                conn.execute_batch(
                    "INSERT INTO pending_files_new (id, file_path, target_filerequest_id, status, retry_count, progress_bytes, file_size_bytes, transfer_id, created_at)
                     SELECT id, file_path, target_filerequest_id,
                            CASE WHEN status = 'Sending' THEN 'Pending' ELSE status END,
                            0,
                            NULL,
                            NULL,
                            NULL,
                            created_at
                     FROM pending_files;

                     DROP TABLE pending_files;
                     ALTER TABLE pending_files_new RENAME TO pending_files;"
                )?;

                // ── received_files: extend with receive-state tracking columns ──
                conn.execute_batch(
                    "CREATE TABLE received_files_new (
                        id              TEXT PRIMARY KEY,
                        contact_id      TEXT,
                        filerequest_id  TEXT NOT NULL,
                        transfer_id     TEXT UNIQUE,
                        file_name       TEXT NOT NULL,
                        file_path       TEXT,
                        file_size_bytes INTEGER NOT NULL,
                        progress_bytes  INTEGER NOT NULL DEFAULT 0,
                        status          TEXT NOT NULL DEFAULT 'Receiving'
                            CHECK (status IN ('Receiving', 'Interrupted', 'Completed', 'Failed')),
                        received_at_ms  INTEGER,
                        FOREIGN KEY(filerequest_id) REFERENCES filerequests(id) ON DELETE CASCADE
                    );",
                )?;

                // Migrate existing received files: all existing rows are already completed.
                conn.execute_batch(
                    "INSERT INTO received_files_new (id, contact_id, filerequest_id, transfer_id, file_name, file_path, file_size_bytes, progress_bytes, status, received_at_ms)
                     SELECT id, contact_id, filerequest_id, NULL, file_name, file_path, size, size, 'Completed', received_at_ms
                     FROM received_files;

                     DROP TABLE received_files;
                     ALTER TABLE received_files_new RENAME TO received_files;"
                )?;

                conn.execute("PRAGMA foreign_keys = ON", [])?;
                Ok(())
            },
            |conn: &Connection| {
                conn.execute("PRAGMA foreign_keys = OFF", [])?;

                // Downgrade received_files: drop state-tracking columns, keep only completed rows
                conn.execute_batch(
                    "CREATE TABLE received_files_old (
                        id            TEXT PRIMARY KEY,
                        contact_id    TEXT,
                        filerequest_id TEXT NOT NULL,
                        file_name     TEXT NOT NULL,
                        file_path     TEXT NOT NULL,
                        size          INTEGER NOT NULL,
                        received_at_ms INTEGER NOT NULL,
                        FOREIGN KEY(filerequest_id) REFERENCES filerequests(id) ON DELETE CASCADE
                    );

                    INSERT INTO received_files_old (id, contact_id, filerequest_id, file_name, file_path, size, received_at_ms)
                    SELECT id, contact_id, filerequest_id, file_name, file_path, file_size_bytes, received_at_ms
                    FROM received_files
                    WHERE status = 'Completed' AND file_path IS NOT NULL AND received_at_ms IS NOT NULL;

                    DROP TABLE received_files;
                    ALTER TABLE received_files_old RENAME TO received_files;"
                )?;

                // Downgrade pending_files: drop extra columns, collapse new statuses back to old ones
                conn.execute_batch(
                    "CREATE TABLE pending_files_old (
                        id TEXT PRIMARY KEY,
                        file_path TEXT NOT NULL,
                        target_filerequest_id TEXT NOT NULL,
                        status TEXT CHECK (status IN ('Pending', 'Sending', 'Sent', 'Rejected', 'Failed')) NOT NULL DEFAULT 'Pending',
                        created_at TEXT NOT NULL,
                        FOREIGN KEY(target_filerequest_id) REFERENCES remote_filerequests(id) ON DELETE CASCADE
                    );

                    INSERT INTO pending_files_old (id, file_path, target_filerequest_id, status, created_at)
                    SELECT id, file_path, target_filerequest_id,
                           CASE
                               WHEN status IN ('Prepared', 'Interrupted') THEN 'Pending'
                               ELSE status
                           END,
                           created_at
                    FROM pending_files;

                    DROP TABLE pending_files;
                    ALTER TABLE pending_files_old RENAME TO pending_files;"
                )?;

                conn.execute("PRAGMA foreign_keys = ON", [])?;
                Ok(())
            },
        ),
        Migration::new(
            Version::new(1, 17, 0),
            Version::new(1, 18, 0),
            "Add display_name column to pending_files",
            |conn: &Connection| {
                conn.execute("ALTER TABLE pending_files ADD COLUMN display_name TEXT", [])?;
                Ok(())
            },
            |conn| {
                conn.execute("ALTER TABLE pending_files DROP COLUMN display_name", [])?;
                Ok(())
            },
        ),
    ]
    .into_iter()
    .chain([
        Migration::new(
            Version::new(1, 18, 0),
            Version::new(1, 18, 1),
            "Relax CHECK constraints on pending_files terminal states to preserve transfer metadata for diagnostics",
            |conn: &Connection| {
                conn.execute("PRAGMA foreign_keys = OFF", [])?;

                conn.execute_batch(
                    "CREATE TABLE pending_files_new (
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

                    INSERT INTO pending_files_new (id, file_path, target_filerequest_id, status, retry_count, progress_bytes, file_size_bytes, transfer_id, display_name, created_at)
                    SELECT id, file_path, target_filerequest_id, status, retry_count, progress_bytes, file_size_bytes, transfer_id, display_name, created_at
                    FROM pending_files;

                    DROP TABLE pending_files;
                    ALTER TABLE pending_files_new RENAME TO pending_files;"
                )?;

                conn.execute("PRAGMA foreign_keys = ON", [])?;
                Ok(())
            },
            |conn: &Connection| {
                conn.execute("PRAGMA foreign_keys = OFF", [])?;

                // Downgrade: restore strict NULL constraints on terminal states
                // Must clear transfer metadata from terminal rows first to satisfy the old CHECK
                conn.execute_batch(
                    "UPDATE pending_files SET progress_bytes = NULL, file_size_bytes = NULL, transfer_id = NULL
                     WHERE status IN ('Sent', 'Rejected', 'Failed');

                    CREATE TABLE pending_files_old (
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
                        created_at TEXT NOT NULL,
                        FOREIGN KEY(target_filerequest_id) REFERENCES remote_filerequests(id) ON DELETE CASCADE,

                        CHECK (
                            (status = 'Pending'
                                AND progress_bytes IS NULL AND file_size_bytes IS NULL AND transfer_id IS NULL)
                            OR
                            (status IN ('Sending', 'Interrupted')
                                AND progress_bytes IS NOT NULL AND progress_bytes >= 0
                                AND file_size_bytes IS NOT NULL AND file_size_bytes >= 0
                                AND transfer_id IS NOT NULL)
                            OR
                            (status = 'Sent'
                                AND progress_bytes IS NULL AND file_size_bytes IS NULL AND transfer_id IS NULL)
                            OR
                            (status = 'Rejected'
                                AND progress_bytes IS NULL AND file_size_bytes IS NULL AND transfer_id IS NULL)
                            OR
                            (status = 'Failed'
                                AND progress_bytes IS NULL AND file_size_bytes IS NULL AND transfer_id IS NULL)
                        )
                    );

                    INSERT INTO pending_files_old (id, file_path, target_filerequest_id, status, retry_count, progress_bytes, file_size_bytes, transfer_id, display_name, created_at)
                    SELECT id, file_path, target_filerequest_id, status, retry_count, progress_bytes, file_size_bytes, transfer_id, display_name, created_at
                    FROM pending_files;

                    DROP TABLE pending_files;
                    ALTER TABLE pending_files_old RENAME TO pending_files;"
                )?;

                conn.execute("PRAGMA foreign_keys = ON", [])?;
                Ok(())
            },
        ),
        Migration::new(
            Version::new(1, 18, 1),
            Version::new(1, 18, 2),
            "Add interruption_reasons to pending_files",
            |conn: &Connection| {
                conn.execute_batch(
                    "ALTER TABLE pending_files ADD COLUMN interruption_reasons TEXT NOT NULL DEFAULT '[]';"
                )?;
                Ok(())
            },
            |conn: &Connection| {
                conn.execute("PRAGMA foreign_keys = OFF", [])?;
                conn.execute_batch(
                    "CREATE TABLE pending_files_old (
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

                    INSERT INTO pending_files_old (id, file_path, target_filerequest_id, status, retry_count, progress_bytes, file_size_bytes, transfer_id, display_name, created_at)
                    SELECT id, file_path, target_filerequest_id, status, retry_count, progress_bytes, file_size_bytes, transfer_id, display_name, created_at
                    FROM pending_files;

                    DROP TABLE pending_files;
                    ALTER TABLE pending_files_old RENAME TO pending_files;"
                )?;
                conn.execute("PRAGMA foreign_keys = ON", [])?;
                Ok(())
            },
        ),
        Migration::new(
            Version::new(1, 18, 2),
            Version::new(1, 19, 0),
            "Add started_at_ms column to received_files for deterministic .part filenames",
            |conn: &Connection| {
                conn.execute_batch(
                    "ALTER TABLE received_files ADD COLUMN started_at_ms INTEGER NOT NULL DEFAULT 0;"
                )?;
                Ok(())
            },
            |conn: &Connection| {
                conn.execute("PRAGMA foreign_keys = OFF", [])?;
                conn.execute_batch(
                    "CREATE TABLE received_files_old (
                        id              TEXT PRIMARY KEY,
                        contact_id      TEXT,
                        filerequest_id  TEXT NOT NULL,
                        transfer_id     TEXT UNIQUE,
                        file_name       TEXT NOT NULL,
                        file_path       TEXT,
                        file_size_bytes INTEGER NOT NULL,
                        progress_bytes  INTEGER NOT NULL DEFAULT 0,
                        status          TEXT NOT NULL DEFAULT 'Receiving'
                            CHECK (status IN ('Receiving', 'Interrupted', 'Completed', 'Failed')),
                        received_at_ms  INTEGER,
                        FOREIGN KEY(filerequest_id) REFERENCES filerequests(id) ON DELETE CASCADE
                    );

                    INSERT INTO received_files_old (id, contact_id, filerequest_id, transfer_id, file_name, file_path, file_size_bytes, progress_bytes, status, received_at_ms)
                    SELECT id, contact_id, filerequest_id, transfer_id, file_name, file_path, file_size_bytes, progress_bytes, status, received_at_ms
                    FROM received_files;

                    DROP TABLE received_files;
                    ALTER TABLE received_files_old RENAME TO received_files;"
                )?;
                conn.execute("PRAGMA foreign_keys = ON", [])?;
                Ok(())
            },
        ),
        Migration::new(
            Version::new(1, 19, 0),
            Version::new(1, 19, 1),
            "Add peer_id column to received_files for peer-scoped transfer ownership",
            |conn: &Connection| {
                conn.execute_batch(
                    "ALTER TABLE received_files ADD COLUMN peer_id TEXT NOT NULL DEFAULT '';"
                )?;
                Ok(())
            },
            |conn: &Connection| {
                conn.execute("PRAGMA foreign_keys = OFF", [])?;
                conn.execute_batch(
                    "CREATE TABLE received_files_old (
                        id              TEXT PRIMARY KEY,
                        contact_id      TEXT,
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

                    INSERT INTO received_files_old (id, contact_id, filerequest_id, transfer_id, file_name, file_path, file_size_bytes, progress_bytes, status, started_at_ms, received_at_ms)
                    SELECT id, contact_id, filerequest_id, transfer_id, file_name, file_path, file_size_bytes, progress_bytes, status, started_at_ms, received_at_ms
                    FROM received_files;

                    DROP TABLE received_files;
                    ALTER TABLE received_files_old RENAME TO received_files;"
                )?;
                conn.execute("PRAGMA foreign_keys = ON", [])?;
                Ok(())
            },
        ),
        Migration::new(
            Version::new(1, 19, 1),
            Version::new(1, 20, 0),
            "Reset cryptographic identity for the ml-dsa/ml-kem migration: rename signing-key \
             columns, clear the old signing keys (regenerated on next launch while keeping the \
             stable libp2p PeerId and contact id), wipe contacts and stale transfer history",
            |conn: &Connection| {
                conn.execute_batch(
                    // Rebuild self_contact into its new shape: drop the public-key columns
                    // (public keys are now derived from the private ones) and rename the
                    // private signing column. The old signing keys are discarded (left NULL)
                    // so the brain regenerates them on next load; the libp2p keypair (PeerId)
                    // and contact_id are intentionally kept stable.
                    "CREATE TABLE self_contact_new (
                         id INTEGER PRIMARY KEY CHECK (id = 1),
                         contact_id TEXT NOT NULL UNIQUE,
                         name TEXT NOT NULL,
                         pq_signing_private_key BLOB,
                         ed25519_private_key BLOB
                     );
                     INSERT INTO self_contact_new (id, contact_id, name, pq_signing_private_key, ed25519_private_key)
                         SELECT 1, contact_id, 'Reset after update', NULL, NULL FROM self_contact WHERE id = 1;
                     DROP TABLE self_contact;
                     ALTER TABLE self_contact_new RENAME TO self_contact;

                     -- Rename the contacts signing column to the algorithm-agnostic name.
                     ALTER TABLE contacts RENAME COLUMN dilithium_public_key TO pq_signing_public_key;

                     -- Contacts' stored public keys are now wrong-format / stale identity.
                     DELETE FROM filerequest_audience;
                     DELETE FROM contacts;

                     -- Transfer history points at dead contact ids / counterpart keys.
                     DELETE FROM pending_files;
                     DELETE FROM remote_filerequests;
                     DELETE FROM received_files;",
                )?;
                Ok(())
            },
            |conn: &Connection| {
                conn.execute_batch(
                    // The discarded signing keys and the wiped contacts and transfers cannot be
                    // restored.
                    "CREATE TABLE self_contact_old (
                         id INTEGER PRIMARY KEY CHECK (id = 1),
                         contact_id TEXT NOT NULL,
                         name TEXT NOT NULL,
                         dilithium_private_key BLOB,
                         dilithium_public_key BLOB,
                         ed25519_private_key BLOB,
                         ed25519_public_key BLOB
                     );
                     INSERT INTO self_contact_old (id, contact_id, name)
                         SELECT id, contact_id, name FROM self_contact WHERE id = 1;
                     DROP TABLE self_contact;
                     ALTER TABLE self_contact_old RENAME TO self_contact;

                     ALTER TABLE contacts RENAME COLUMN pq_signing_public_key TO dilithium_public_key;",
                )?;
                Ok(())
            },
        ),
        Migration::new(
            Version::new(1, 20, 0),
            Version::new(1, 21, 0),
            "Add persisted_as_name to received_files: the name a received file is actually \
             stored under on disk (after sanitization/deduplication), used for display. \
             Backfill existing rows from file_name.",
            |conn: &Connection| {
                conn.execute_batch(
                    "ALTER TABLE received_files ADD COLUMN persisted_as_name TEXT NOT NULL DEFAULT '';
                     UPDATE received_files SET persisted_as_name = file_name;",
                )?;
                Ok(())
            },
            |conn: &Connection| {
                conn.execute_batch("ALTER TABLE received_files DROP COLUMN persisted_as_name;")?;
                Ok(())
            },
        ),
        Migration::new(
            Version::new(1, 21, 0),
            Version::new(1, 22, 0),
            "Add sync_docs table for multi-device CRDT sync and seed the contacts, \
             remote_filerequests and devices documents from existing local rows (each \
             built from the shared deterministic genesis).",
            seed_sync_docs,
            unseed_sync_docs,
        ),
        Migration::new(
            Version::new(1, 22, 0),
            Version::new(1, 23, 0),
            "Add license_cache table for the cached pro-licensing state.",
            |conn: &Connection| {
                conn.execute_batch(
                    "CREATE TABLE IF NOT EXISTS license_cache (
                        id INTEGER PRIMARY KEY CHECK (id = 1),
                        data BLOB NOT NULL DEFAULT X'',
                        version TEXT NOT NULL DEFAULT '0.0.0'
                    );",
                )?;
                Ok(())
            },
            |conn: &Connection| {
                conn.execute_batch("DROP TABLE IF EXISTS license_cache;")?;
                Ok(())
            },
        ),
        Migration::new(
            Version::new(1, 23, 0),
            Version::new(1, 24, 0),
            "Re-key every contact onto an id derived from its public keys, and drop the \
             self contact's stored id in favour of deriving it from the signing keys.",
            derive_contact_ids,
            underive_contact_ids,
        ),
        Migration::new(
            Version::new(1, 24, 0),
            Version::new(1, 25, 0),
            "Store the port peers reach this node on, keeping 24256 for every existing install \
             so the firewall rules made for it still apply.",
            |conn: &Connection| {
                conn.execute_batch(
                    "CREATE TABLE IF NOT EXISTS listen_port (
                        id INTEGER PRIMARY KEY CHECK (id = 1),
                        port INTEGER NOT NULL CHECK (port BETWEEN 1024 AND 65535)
                    );
                    INSERT INTO listen_port (id, port) VALUES (1, 24256);",
                )?;
                Ok(())
            },
            |conn: &Connection| {
                conn.execute_batch("DROP TABLE IF EXISTS listen_port;")?;
                Ok(())
            },
        ),
        Migration::new(
            Version::new(1, 25, 0),
            Version::new(1, 26, 0),
            "Remember when the source file of a transfer was last modified, so a resume can tell \
             whether it changed since.",
            |conn: &Connection| {
                // A transfer recorded without the stamp cannot be resumed safely, so it starts over.
                conn.execute_batch(
                    "CREATE TABLE pending_files_new (
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
                        source_modified_ms INTEGER,

                        CHECK (
                            (status = 'Pending'
                                AND progress_bytes IS NULL AND file_size_bytes IS NULL
                                AND transfer_id IS NULL AND source_modified_ms IS NULL)
                            OR
                            (status IN ('Sending', 'Interrupted')
                                AND progress_bytes IS NOT NULL AND progress_bytes >= 0
                                AND file_size_bytes IS NOT NULL AND file_size_bytes >= 0
                                AND transfer_id IS NOT NULL AND source_modified_ms IS NOT NULL)
                            OR
                            (status IN ('Sent', 'Rejected', 'Failed'))
                        )
                    );

                    INSERT INTO pending_files_new (id, file_path, target_filerequest_id, status, retry_count, progress_bytes, file_size_bytes, transfer_id, display_name, interruption_reasons, created_at)
                    SELECT id, file_path, target_filerequest_id,
                           CASE WHEN status IN ('Sending', 'Interrupted') THEN 'Pending' ELSE status END,
                           retry_count,
                           CASE WHEN status IN ('Sending', 'Interrupted') THEN NULL ELSE progress_bytes END,
                           CASE WHEN status IN ('Sending', 'Interrupted') THEN NULL ELSE file_size_bytes END,
                           CASE WHEN status IN ('Sending', 'Interrupted') THEN NULL ELSE transfer_id END,
                           display_name, interruption_reasons, created_at
                    FROM pending_files;

                    DROP TABLE pending_files;
                    ALTER TABLE pending_files_new RENAME TO pending_files;",
                )?;
                Ok(())
            },
            |conn: &Connection| {
                conn.execute_batch(&format!(
                    "{}

                    INSERT INTO pending_files_new (id, file_path, target_filerequest_id, status, retry_count, progress_bytes, file_size_bytes, transfer_id, display_name, interruption_reasons, created_at)
                    SELECT id, file_path, target_filerequest_id, status, retry_count, progress_bytes, file_size_bytes, transfer_id, display_name, interruption_reasons, created_at
                    FROM pending_files;

                    DROP TABLE pending_files;
                    ALTER TABLE pending_files_new RENAME TO pending_files;",
                    pending_files_1_25("pending_files_new")
                ))?;
                Ok(())
            },
        ),
        Migration::new(
            Version::new(1, 26, 0),
            Version::new(1, 27, 0),
            "Keep the key derived from this device's backup password and when to ask for it again.",
            |conn: &Connection| {
                conn.execute_batch(
                    "CREATE TABLE backup_secret (
                        id INTEGER PRIMARY KEY CHECK (id = 1),
                        salt BLOB NOT NULL CHECK (length(salt) = 16),
                        m_cost_kib INTEGER NOT NULL,
                        t_cost INTEGER NOT NULL,
                        p_cost INTEGER NOT NULL,
                        key BLOB NOT NULL CHECK (length(key) = 32),
                        verify_gap INTEGER NOT NULL CHECK (verify_gap BETWEEN 0 AND 3),
                        verify_due_at_ms INTEGER NOT NULL,
                        verify_missed INTEGER NOT NULL CHECK (verify_missed IN (0, 1))
                    );",
                )?;
                Ok(())
            },
            |conn: &Connection| {
                conn.execute_batch("DROP TABLE IF EXISTS backup_secret;")?;
                Ok(())
            },
        ),
    ])
    .collect()
}

/// Reverse of [`seed_sync_docs`]: the forward migration dropped the `remote_filerequests` table
/// entirely (the CRDT document is authoritative), so recreate it and backfill every column from
/// the synced document, then drop the `sync_docs` table. The `pending_files` foreign key is not
/// restored — downgrades are best-effort and unsupported across this codebase.
pub(super) fn pending_files_1_25(table: &str) -> String {
    format!(
        "CREATE TABLE {table} (
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

            CHECK (
                (status = 'Pending'
                    AND progress_bytes IS NULL AND file_size_bytes IS NULL AND transfer_id IS NULL)
                OR
                (status IN ('Sending', 'Interrupted')
                    AND progress_bytes IS NOT NULL AND progress_bytes >= 0
                    AND file_size_bytes IS NOT NULL AND file_size_bytes >= 0
                    AND transfer_id IS NOT NULL)
                OR
                (status IN ('Sent', 'Rejected', 'Failed'))
            )
        );"
    )
}

fn unseed_sync_docs(conn: &Connection) -> Result<(), MigrationError> {
    // Rehydrate the remote-filerequests document before we drop it.
    let blob: Option<Vec<u8>> = conn
        .query_row(
            "SELECT data FROM sync_docs WHERE doc_kind = ?",
            [DocKind::RemoteFilerequests.as_str()],
            |row| row.get(0),
        )
        .optional()?;
    let remote_fr = match blob {
        Some(blob) => {
            let doc = sync::load(&blob)?;
            let view: RemoteFrDocV1 = sync::hydrate(&doc)?;
            view.remote_filerequests
        }
        None => Default::default(),
    };

    // Recreate the table in its pre-sync shape, then backfill from the document.
    conn.execute_batch(
        "CREATE TABLE remote_filerequests (
            id TEXT PRIMARY KEY,
            peer_id TEXT NOT NULL,
            filerequest_id TEXT NOT NULL,
            contact_id TEXT NOT NULL,
            name TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );",
    )?;
    for (id, entry) in remote_fr {
        conn.execute(
            "INSERT INTO remote_filerequests (id, peer_id, filerequest_id, contact_id, name, created_at)
             VALUES (?, ?, ?, ?, ?, datetime('now'))",
            rusqlite::params![
                id,
                entry.peer_id_hint,
                entry.filerequest_id,
                entry.contact_id,
                entry.name
            ],
        )?;
    }

    conn.execute("DROP TABLE sync_docs", [])?;
    Ok(())
}

/// Re-keys every contact onto the id its public keys derive, taken from the cached key columns or
/// else the contacts document's fingerprint. The documents are rewritten in place, because a
/// rebuilt one would drop whatever another device has not yet sent.
fn derive_contact_ids(conn: &Connection) -> Result<(), MigrationError> {
    conn.execute_batch("PRAGMA defer_foreign_keys = ON;")?;

    let mut mapping: HashMap<String, String> = HashMap::new();

    let mut contacts_doc = match load_doc(conn, DocKind::Contacts)? {
        Some(blob) => Some(sync::load(&blob)?),
        None => None,
    };
    let contacts_view: ContactsDocV1 = match &contacts_doc {
        Some(doc) => sync::hydrate(doc)?,
        None => ContactsDocV1::default(),
    };
    for (old_id, entry) in &contacts_view.contacts {
        match fc1_contact_id(&entry.key_fingerprint) {
            Ok(new_id) => {
                mapping.insert(old_id.clone(), new_id);
            }
            Err(error) => tracing::error!(
                contact_id = %old_id,
                %error,
                "Synced contact has no usable key fingerprint"
            ),
        }
    }

    {
        let mut stmt =
            conn.prepare("SELECT id, pq_signing_public_key, ed25519_public_key FROM contacts")?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<Vec<u8>>>(1)?,
                row.get::<_, Option<Vec<u8>>>(2)?,
            ))
        })?;
        for row in rows {
            let (old_id, Some(pq), Some(ed)) = row? else {
                continue;
            };
            let new_id = fc1_contact_id(&key_fingerprint_v1(&pq, &ed))?;
            match mapping.get(&old_id) {
                Some(from_doc) if *from_doc != new_id => tracing::error!(
                    contact_id = %old_id,
                    %from_doc,
                    from_columns = %new_id,
                    "Cached key bytes disagree with the synced fingerprint. Keeping the \
                     document's, which is the authoritative copy"
                ),
                _ => {
                    mapping.insert(old_id, new_id);
                }
            }
        }
    }

    // Two contacts rows could in theory hold one key pair. Remapping them collides on the primary
    // key and fails the migration, which is unlikely enough to leave unhandled.
    for (table, column, on_miss) in [
        ("contacts", "id", OnMiss::DeleteRow),
        ("filerequest_audience", "contact_id", OnMiss::DeleteRow),
        ("received_files", "contact_id", OnMiss::SetNull),
    ] {
        for contact_id in remap_column(conn, table, column, &mapping, on_miss)? {
            tracing::error!(
                %table,
                %contact_id,
                "No derived id for this contact, so the rows referencing it cannot be kept"
            );
        }
    }

    if let Some(doc) = contacts_doc.as_mut() {
        let contacts = contacts_view
            .contacts
            .iter()
            .filter_map(|(old_id, entry)| {
                Some((
                    mapping.get(old_id)?.clone(),
                    ContactEntryV2 {
                        name: entry.name.clone(),
                    },
                ))
            })
            .collect();
        sync::reconcile(doc, &ContactsDocV2 { contacts })?;
        save_doc(conn, DocKind::Contacts, doc.save())?;
    }

    if let Some(blob) = load_doc(conn, DocKind::RemoteFilerequests)? {
        let mut doc = sync::load(&blob)?;
        let view: RemoteFrDocV1 = sync::hydrate(&doc)?;
        let remote_filerequests = view
            .remote_filerequests
            .into_iter()
            .filter_map(|(id, entry)| match mapping.get(&entry.contact_id) {
                Some(new_id) => Some((
                    id,
                    RemoteFrEntryV1 {
                        contact_id: new_id.clone(),
                        ..entry
                    },
                )),
                None => {
                    // An unmapped value would fail `Hydrate` for the whole document from here on.
                    tracing::warn!(
                        remote_filerequest = %id,
                        contact_id = %entry.contact_id,
                        "Dropping a remote filerequest whose contact is unknown"
                    );
                    None
                }
            })
            .collect();
        sync::reconcile(
            &mut doc,
            &RemoteFrDocV1 {
                remote_filerequests,
            },
        )?;
        save_doc(conn, DocKind::RemoteFilerequests, doc.save())?;
    }

    conn.execute_batch(
        "CREATE TABLE self_contact_new (
             id INTEGER PRIMARY KEY CHECK (id = 1),
             name TEXT NOT NULL,
             pq_signing_private_key BLOB,
             ed25519_private_key BLOB
         );
         INSERT INTO self_contact_new (id, name, pq_signing_private_key, ed25519_private_key)
             SELECT 1, name, pq_signing_private_key, ed25519_private_key
             FROM self_contact WHERE id = 1;
         DROP TABLE self_contact;
         ALTER TABLE self_contact_new RENAME TO self_contact;",
    )?;

    Ok(())
}

/// The contact ids stay derived, since the ones they replaced are gone. The self contact's stored
/// id is the one its keys derive, so devices of one identity still agree after downgrading.
fn underive_contact_ids(conn: &Connection) -> Result<(), MigrationError> {
    if let Some(blob) = load_doc(conn, DocKind::Contacts)? {
        let mut doc = sync::load(&blob)?;
        let view: ContactsDocV2 = sync::hydrate(&doc)?;
        let contacts = view
            .contacts
            .into_iter()
            .map(|(id, entry)| {
                let key_fingerprint = fc1_fingerprint(&id)?;
                Ok((
                    id,
                    ContactEntryV1 {
                        name: entry.name,
                        key_fingerprint,
                    },
                ))
            })
            .collect::<Result<_, MigrationError>>()?;
        sync::reconcile(&mut doc, &ContactsDocV1 { contacts })?;
        save_doc(conn, DocKind::Contacts, doc.save())?;
    }

    let private_keys = conn
        .query_row(
            "SELECT pq_signing_private_key, ed25519_private_key FROM self_contact WHERE id = 1",
            [],
            |row| {
                Ok(row
                    .get::<_, Option<Vec<u8>>>(0)?
                    .zip(row.get::<_, Option<Vec<u8>>>(1)?))
            },
        )
        .optional()?
        .flatten();
    let self_contact_id = match private_keys {
        Some((pq, ed)) => self_contact_id_from_private_keys(pq, ed)?,
        None => Uuid::new_v4().to_string(),
    };

    conn.execute_batch(
        "CREATE TABLE self_contact_old (
             id INTEGER PRIMARY KEY CHECK (id = 1),
             contact_id TEXT NOT NULL UNIQUE,
             name TEXT NOT NULL,
             pq_signing_private_key BLOB,
             ed25519_private_key BLOB
         );",
    )?;
    conn.execute(
        "INSERT INTO self_contact_old (id, contact_id, name, pq_signing_private_key, ed25519_private_key)
             SELECT 1, ?, name, pq_signing_private_key, ed25519_private_key
             FROM self_contact WHERE id = 1",
        [self_contact_id],
    )?;
    conn.execute_batch(
        "DROP TABLE self_contact;
         ALTER TABLE self_contact_old RENAME TO self_contact;",
    )?;

    Ok(())
}

fn self_contact_id_from_private_keys(
    pq_signing_private: Vec<u8>,
    ed25519_private: Vec<u8>,
) -> Result<String, MigrationError> {
    let public = ContactKeys::from_private(ContactPrivateKeys {
        pq_signing: Box::new(deserialize_pq_signing_private_key(pq_signing_private)?),
        ed25519: deserialize_ed25519_private_key(ed25519_private)?,
    })
    .public;
    let fingerprint = key_fingerprint_v1(
        &serialize_pq_signing_public_key(&public.pq_signing),
        &serialize_ed25519_public_key(&public.ed25519),
    );
    Ok(fc1_contact_id(&fingerprint).expect("a SHA-256 fingerprint to be 32 bytes"))
}

#[derive(Reconcile, Hydrate)]
struct ContactEntryV2 {
    name: String,
}

#[derive(Reconcile, Hydrate)]
struct ContactsDocV2 {
    #[autosurgeon(with = "autosurgeon::map_with_parseable_keys")]
    contacts: HashMap<String, ContactEntryV2>,
}

enum OnMiss {
    DeleteRow,
    SetNull,
}

/// Returns the values that had no mapping, after applying `on_miss` to their rows.
fn remap_column(
    conn: &Connection,
    table: &str,
    column: &str,
    mapping: &HashMap<String, String>,
    on_miss: OnMiss,
) -> rusqlite::Result<Vec<String>> {
    let existing: Vec<String> = conn
        .prepare(&format!(
            "SELECT DISTINCT {column} FROM {table} WHERE {column} IS NOT NULL"
        ))?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<_, _>>()?;

    let mut unmapped = Vec::new();
    for old_id in existing {
        match mapping.get(&old_id) {
            Some(new_id) => {
                conn.execute(
                    &format!("UPDATE {table} SET {column} = ? WHERE {column} = ?"),
                    rusqlite::params![new_id, old_id],
                )?;
            }
            None => {
                let statement = match on_miss {
                    OnMiss::DeleteRow => format!("DELETE FROM {table} WHERE {column} = ?"),
                    OnMiss::SetNull => {
                        format!("UPDATE {table} SET {column} = NULL WHERE {column} = ?")
                    }
                };
                conn.execute(&statement, [&old_id])?;
                unmapped.push(old_id);
            }
        }
    }
    Ok(unmapped)
}

fn load_doc(conn: &Connection, kind: DocKind) -> rusqlite::Result<Option<Vec<u8>>> {
    conn.query_row(
        "SELECT data FROM sync_docs WHERE doc_kind = ?",
        [kind.as_str()],
        |row| row.get(0),
    )
    .optional()
}

fn save_doc(conn: &Connection, kind: DocKind, blob: Vec<u8>) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO sync_docs (doc_kind, data) VALUES (?, ?)
         ON CONFLICT(doc_kind) DO UPDATE SET data = excluded.data",
        rusqlite::params![kind.as_str(), blob],
    )?;
    Ok(())
}
