use rusqlite::{Connection, Result};
use semver::Version;
use std::fmt;
use tracing::info;

mod all_migrations;
pub use all_migrations::get_migrations;

#[allow(unused)]
pub struct Migration {
    applies_to_version: Version,
    target_version: Version,
    description: &'static str,
    #[allow(clippy::type_complexity)]
    up: Box<dyn Fn(&Connection) -> Result<()>>,
    // TODO: implement downgrades
    #[allow(clippy::type_complexity)]
    down: Box<dyn Fn(&Connection) -> Result<()>>,
}

impl fmt::Debug for Migration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Migration")
            .field("from", &self.applies_to_version)
            .field("to", &self.target_version)
            .field("description", &self.description)
            .finish()
    }
}

impl Migration {
    pub fn new<F1, F2>(
        applies_to_version: Version,
        target_version: Version,
        description: &'static str,
        up: F1,
        down: F2,
    ) -> Self
    where
        F1: Fn(&Connection) -> Result<()> + 'static,
        F2: Fn(&Connection) -> Result<()> + 'static,
    {
        Migration {
            applies_to_version,
            target_version,
            description,
            up: Box::new(up),
            down: Box::new(down),
        }
    }

    pub fn upgrade(&self, conn: &Connection) -> Result<()> {
        (self.up)(conn)?;
        conn.execute(
            "UPDATE schema_version SET version = ?",
            [self.target_version.to_string()],
        )?;
        Ok(())
    }

    #[allow(unused)]
    pub fn rollback(&self, conn: &Connection) -> Result<()> {
        (self.down)(conn)?;
        conn.execute(
            "UPDATE schema_version SET version = ?",
            [self.applies_to_version.to_string()],
        )?;
        Ok(())
    }
}

fn get_db_version(conn: &Connection) -> Result<Version> {
    let version_str: String =
        conn.query_row("SELECT version FROM schema_version", [], |row| row.get(0))?;
    Ok(Version::parse(&version_str).unwrap())
}

pub fn run_migrations(conn: &mut Connection) -> Result<()> {
    let current_version = get_db_version(conn)?;
    info!("Current DB version: {}", current_version);

    let migrations: Vec<Migration> = get_migrations();

    for migration in migrations
        .iter()
        .filter(|m| m.applies_to_version >= current_version)
    {
        info!(
            "Applying migration: {} ({} -> {})",
            migration.description, migration.applies_to_version, migration.target_version
        );
        let tx = conn.transaction()?;
        migration.upgrade(&tx)?;
        tx.commit()?;
        info!(
            "Applied migration: {} ({} -> {})",
            migration.description, migration.applies_to_version, migration.target_version
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds an in-memory DB shaped like schema version 1.19.1 (pre-migration column
    /// names) with some seeded identity / contact / transfer rows.
    fn seed_db_at_v1_19_1() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE schema_version (version TEXT NOT NULL);
             INSERT INTO schema_version (version) VALUES ('1.19.1');

             CREATE TABLE node_keys (id INTEGER PRIMARY KEY CHECK (id = 1), key_bytes BLOB NOT NULL);
             INSERT INTO node_keys (id, key_bytes) VALUES (1, X'1122');

             CREATE TABLE self_contact (
                 id INTEGER PRIMARY KEY CHECK (id = 1),
                 contact_id TEXT NOT NULL UNIQUE,
                 name TEXT NOT NULL,
                 dilithium_private_key BLOB,
                 dilithium_public_key BLOB,
                 ed25519_private_key BLOB,
                 ed25519_public_key BLOB
             );
             INSERT INTO self_contact VALUES (1, 'my-stable-id', 'Alice', X'01', X'02', X'03', X'04');

             CREATE TABLE filerequests (id TEXT PRIMARY KEY, title TEXT NOT NULL);
             INSERT INTO filerequests VALUES ('fr-own', 'mine');

             CREATE TABLE contacts (
                 id TEXT PRIMARY KEY,
                 name TEXT NOT NULL,
                 created_at TEXT NOT NULL,
                 updated_at TEXT NOT NULL,
                 dilithium_public_key BLOB,
                 ed25519_public_key BLOB
             );
             INSERT INTO contacts VALUES ('c1', 'Bob', 'now', 'now', X'05', X'06');

             CREATE TABLE filerequest_audience (
                 filerequest_id TEXT NOT NULL,
                 contact_id TEXT NOT NULL,
                 created_at TEXT NOT NULL
             );
             INSERT INTO filerequest_audience VALUES ('fr-own', 'c1', 'now');

             CREATE TABLE remote_filerequests (
                 id TEXT PRIMARY KEY,
                 peer_id TEXT NOT NULL DEFAULT '',
                 filerequest_id TEXT NOT NULL DEFAULT '',
                 name TEXT NOT NULL DEFAULT '',
                 created_at TEXT NOT NULL DEFAULT 'now',
                 contact_id TEXT NOT NULL
             );
             INSERT INTO remote_filerequests (id, contact_id) VALUES ('rfr1', 'c1');

             CREATE TABLE pending_files (
                 id TEXT PRIMARY KEY,
                 file_path TEXT NOT NULL,
                 target_filerequest_id TEXT NOT NULL,
                 status TEXT NOT NULL DEFAULT 'Pending',
                 retry_count INTEGER NOT NULL DEFAULT 0,
                 progress_bytes INTEGER,
                 file_size_bytes INTEGER,
                 transfer_id TEXT,
                 display_name TEXT,
                 interruption_reasons TEXT NOT NULL DEFAULT '[]',
                 created_at TEXT NOT NULL
             );
             INSERT INTO pending_files (id, file_path, target_filerequest_id, created_at)
                 VALUES ('pf1', '/p', 'rfr1', 'now');

             CREATE TABLE received_files (id TEXT PRIMARY KEY, peer_id TEXT NOT NULL, file_name TEXT NOT NULL);
             INSERT INTO received_files VALUES ('rf1', 'peer-x', 'photo.jpg');",
        )
        .unwrap();
        conn
    }

    #[test]
    fn reset_migration_clears_identity_but_keeps_stable_ids() {
        let mut conn = seed_db_at_v1_19_1();
        let node_key_before: Vec<u8> = conn
            .query_row("SELECT key_bytes FROM node_keys WHERE id = 1", [], |r| {
                r.get(0)
            })
            .unwrap();

        run_migrations(&mut conn).unwrap();

        // Version advanced.
        assert_eq!(get_db_version(&conn).unwrap(), Version::new(1, 22, 0));

        // Stable identity preserved: PeerId keypair and contact_id untouched.
        let node_key_after: Vec<u8> = conn
            .query_row("SELECT key_bytes FROM node_keys WHERE id = 1", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(node_key_before, node_key_after);
        let (contact_id, name): (String, String) = conn
            .query_row(
                "SELECT contact_id, name FROM self_contact WHERE id = 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(contact_id, "my-stable-id");
        assert_eq!(name, "Reset after update");

        // Only private signing columns remain, and they are cleared (regenerated on boot).
        let (pq_priv, ed_priv): (Option<Vec<u8>>, Option<Vec<u8>>) = conn
            .query_row(
                "SELECT pq_signing_private_key, ed25519_private_key FROM self_contact WHERE id = 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert!(pq_priv.is_none() && ed_priv.is_none());

        // The public-key columns were dropped from self_contact.
        assert!(
            conn.query_row(
                "SELECT pq_signing_public_key FROM self_contact",
                [],
                |_| Ok(())
            )
            .is_err()
        );

        let count = |sql: &str| -> i64 { conn.query_row(sql, [], |r| r.get(0)).unwrap() };

        // The contacts table keeps its public key column, under the new name
        // (COUNT over the column succeeds only if the column exists).
        assert_eq!(
            count("SELECT COUNT(pq_signing_public_key) FROM contacts"),
            0
        );

        // Contacts + stale transfers wiped; own filerequest kept.
        assert_eq!(count("SELECT COUNT(*) FROM contacts"), 0);
        assert_eq!(count("SELECT COUNT(*) FROM filerequest_audience"), 0);
        assert_eq!(count("SELECT COUNT(*) FROM pending_files"), 0);
        assert_eq!(count("SELECT COUNT(*) FROM received_files"), 0);
        assert_eq!(count("SELECT COUNT(*) FROM filerequests"), 1);

        // The remote_filerequests SQL table (and its transient projection) is gone entirely; the
        // synced document is now authoritative.
        assert!(
            conn.query_row("SELECT COUNT(*) FROM remote_filerequests", [], |r| r
                .get::<_, i64>(0))
                .is_err(),
            "remote_filerequests table should have been dropped by the seed migration"
        );

        // sync_docs table created and seeded with the three documents (empty here, since
        // contacts and remote_filerequests were wiped by the reset migration).
        assert_eq!(count("SELECT COUNT(*) FROM sync_docs"), 3);
    }

    #[test]
    fn seed_sync_docs_populates_documents_from_existing_rows() {
        use crate::core::domain_models::{ContactId, FylesId, PeerIdWrapper};
        use crate::core::sync::{self, ContactsDoc, DevicesDoc, DocKind, RemoteFrDoc};

        // A DB already at the current pre-sync version with real contact / remote-fr rows and a
        // valid node keypair (so the migration can seed this device into the Devices doc).
        let keypair = libp2p::identity::Keypair::generate_ed25519();
        let key_bytes = keypair.to_protobuf_encoding().unwrap();
        let expected_peer_id = crate::core::p2p::peer_id_from_node_keypair(&key_bytes).unwrap();

        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE schema_version (version TEXT NOT NULL);
             INSERT INTO schema_version (version) VALUES ('1.21.0');

             CREATE TABLE node_keys (id INTEGER PRIMARY KEY CHECK (id = 1), key_bytes BLOB NOT NULL);

             CREATE TABLE contacts (
                 id TEXT PRIMARY KEY, name TEXT NOT NULL,
                 created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                 pq_signing_public_key BLOB, ed25519_public_key BLOB
             );
             INSERT INTO contacts VALUES ('c1', 'Bob', 'now', 'now', X'aabb', X'ccdd');

             CREATE TABLE remote_filerequests (
                 id TEXT PRIMARY KEY, peer_id TEXT NOT NULL, filerequest_id TEXT NOT NULL,
                 name TEXT NOT NULL, created_at TEXT NOT NULL, contact_id TEXT NOT NULL
             );
             INSERT INTO remote_filerequests VALUES ('rfr1', 'peerX', 'fr-remote', 'drop', 'now', 'c1');

             CREATE TABLE pending_files (
                 id TEXT PRIMARY KEY, file_path TEXT NOT NULL, target_filerequest_id TEXT NOT NULL,
                 status TEXT NOT NULL DEFAULT 'Pending', retry_count INTEGER NOT NULL DEFAULT 0,
                 progress_bytes INTEGER, file_size_bytes INTEGER, transfer_id TEXT,
                 display_name TEXT, interruption_reasons TEXT NOT NULL DEFAULT '[]',
                 created_at TEXT NOT NULL
             );
             INSERT INTO pending_files (id, file_path, target_filerequest_id, created_at)
                 VALUES ('pf1', '/p', 'rfr1', 'now');",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO node_keys (id, key_bytes) VALUES (1, ?)",
            [&key_bytes],
        )
        .unwrap();

        run_migrations(&mut conn).unwrap();
        assert_eq!(get_db_version(&conn).unwrap(), Version::new(1, 22, 0));

        // remote_filerequests table dropped; pending_files kept (FK removed) with its row.
        assert!(
            conn.query_row("SELECT COUNT(*) FROM remote_filerequests", [], |r| r
                .get::<_, i64>(0))
                .is_err()
        );
        let pending_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM pending_files", [], |r| r.get(0))
            .unwrap();
        assert_eq!(pending_count, 1);

        let blob = |kind: DocKind| -> Vec<u8> {
            conn.query_row(
                "SELECT data FROM sync_docs WHERE doc_kind = ?",
                [kind.as_str()],
                |r| r.get(0),
            )
            .unwrap()
        };

        let contacts_doc: ContactsDoc =
            sync::hydrate(&sync::load(&blob(DocKind::Contacts)).unwrap()).unwrap();
        let bob = &contacts_doc.contacts[&ContactId("c1".into())];
        assert_eq!(bob.name, "Bob");
        assert_eq!(
            bob.key_fingerprint,
            sync::fingerprint(&[0xaa, 0xbb], &[0xcc, 0xdd])
        );

        let remote_doc: RemoteFrDoc =
            sync::hydrate(&sync::load(&blob(DocKind::RemoteFilerequests)).unwrap()).unwrap();
        let rfr = &remote_doc.remote_filerequests[&FylesId("rfr1".into())];
        assert_eq!(rfr.contact_id.0, "c1");
        assert_eq!(rfr.peer_id_hint.0, "peerX");
        assert_eq!(rfr.filerequest_id.0, "fr-remote");

        // This device was seeded into the Devices doc from the stored node keypair, so it shows up
        // in the owned-device list immediately (e.g. after a restore) without a runtime restart.
        let devices_doc: DevicesDoc =
            sync::hydrate(&sync::load(&blob(DocKind::Devices)).unwrap()).unwrap();
        assert!(
            devices_doc
                .devices
                .contains_key(&PeerIdWrapper(expected_peer_id)),
            "seed migration should register this device in the Devices doc"
        );
    }
}
