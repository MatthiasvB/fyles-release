use crate::core::sync::SyncDocError;
use crypto::{PqSigningPrivateKeyDeserializationError, PrivateEd25519DeserializationError};
use rusqlite::Connection;
use semver::Version;
use std::fmt;
use thiserror::Error;
use tracing::info;

mod all_migrations;
mod doc_v1;
pub use all_migrations::get_migrations;
use doc_v1::{FingerprintLengthError, NotAnFc1IdError};

#[derive(Debug, Error)]
pub enum MigrationError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("Sync document error: {0}")]
    SyncDoc(Box<SyncDocError>),
    #[error("Key fingerprint error: {0}")]
    Fingerprint(#[from] FingerprintLengthError),
    #[error("Contact id error: {0}")]
    ContactId(#[from] NotAnFc1IdError),
    #[error("Ed25519 signing key error: {0}")]
    Ed25519Key(#[from] PrivateEd25519DeserializationError),
    #[error("ML-DSA signing key error: {0}")]
    PqSigningKey(#[from] PqSigningPrivateKeyDeserializationError),
}

/// Boxed so that `MigrationError` stays small enough to sit in every migration's `Result`.
impl From<SyncDocError> for MigrationError {
    fn from(error: SyncDocError) -> Self {
        Self::SyncDoc(Box::new(error))
    }
}

type Result<T> = std::result::Result<T, MigrationError>;

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
    use super::all_migrations::pending_files_1_25;
    use super::doc_v1::{
        ContactEntryV1, ContactsDocV1, fc1_contact_id, key_fingerprint_v1, new_blob,
    };
    use super::*;
    use crate::library::sqlite::schema::CURRENT_SCHEMA_VERSION;

    /// Builds an in-memory DB shaped like schema version 1.19.1 (pre-migration column
    /// names) with some seeded identity / contact / transfer rows.
    fn seed_db_at_v1_19_1() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
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
                 created_at TEXT NOT NULL,
                 PRIMARY KEY (filerequest_id, contact_id),
                 FOREIGN KEY (contact_id) REFERENCES contacts(id) ON DELETE CASCADE
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

             CREATE TABLE received_files (
                 id TEXT PRIMARY KEY,
                 contact_id TEXT,
                 filerequest_id TEXT NOT NULL,
                 transfer_id TEXT UNIQUE,
                 file_name TEXT NOT NULL,
                 file_path TEXT,
                 file_size_bytes INTEGER NOT NULL,
                 progress_bytes INTEGER NOT NULL DEFAULT 0,
                 status TEXT NOT NULL DEFAULT 'Receiving',
                 received_at_ms INTEGER,
                 started_at_ms INTEGER NOT NULL DEFAULT 0,
                 peer_id TEXT NOT NULL DEFAULT ''
             );
             INSERT INTO received_files (id, contact_id, filerequest_id, file_name, file_size_bytes, peer_id)
                 VALUES ('rf1', 'c1', 'fr-own', 'photo.jpg', 1, 'peer-x');",
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
        assert_eq!(get_db_version(&conn).unwrap(), CURRENT_SCHEMA_VERSION);

        // Stable identity preserved: PeerId keypair and contact_id untouched.
        let node_key_after: Vec<u8> = conn
            .query_row("SELECT key_bytes FROM node_keys WHERE id = 1", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(node_key_before, node_key_after);
        let name: String = conn
            .query_row("SELECT name FROM self_contact WHERE id = 1", [], |r| {
                r.get(0)
            })
            .unwrap();
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
    fn the_chain_rolls_back_to_where_it_started() {
        let mut conn = seed_db_at_v1_19_1();
        run_migrations(&mut conn).unwrap();

        let start = Version::new(1, 19, 1);
        for migration in get_migrations()
            .iter()
            .rev()
            .take_while(|m| m.applies_to_version >= start)
        {
            migration.rollback(&conn).unwrap();
        }

        assert_eq!(get_db_version(&conn).unwrap(), start);
        for column in [
            "contacts.dilithium_public_key",
            "self_contact.dilithium_public_key",
        ] {
            let (table, column) = column.split_once('.').unwrap();
            conn.query_row(&format!("SELECT COUNT({column}) FROM {table}"), [], |_| {
                Ok(())
            })
            .unwrap_or_else(|e| panic!("{table}.{column} should be back: {e}"));
        }
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
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
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
             INSERT INTO contacts VALUES ('c2', 'Keyless', 'now', 'now', NULL, NULL);

             CREATE TABLE self_contact (
                 id INTEGER PRIMARY KEY CHECK (id = 1),
                 contact_id TEXT NOT NULL UNIQUE,
                 name TEXT NOT NULL,
                 pq_signing_private_key BLOB,
                 ed25519_private_key BLOB
             );
             INSERT INTO self_contact VALUES (1, 'my-stable-id', 'Alice', NULL, NULL);

             CREATE TABLE filerequest_audience (
                 filerequest_id TEXT NOT NULL, contact_id TEXT NOT NULL,
                 created_at TEXT NOT NULL,
                 PRIMARY KEY (filerequest_id, contact_id),
                 FOREIGN KEY (contact_id) REFERENCES contacts(id) ON DELETE CASCADE
             );
             INSERT INTO filerequest_audience VALUES ('fr-own', 'c1', 'now');

             CREATE TABLE received_files (
                 id TEXT PRIMARY KEY, contact_id TEXT, filerequest_id TEXT NOT NULL,
                 transfer_id TEXT UNIQUE, file_name TEXT NOT NULL, file_path TEXT,
                 file_size_bytes INTEGER NOT NULL, progress_bytes INTEGER NOT NULL DEFAULT 0,
                 status TEXT NOT NULL DEFAULT 'Receiving', received_at_ms INTEGER,
                 started_at_ms INTEGER NOT NULL DEFAULT 0, peer_id TEXT NOT NULL DEFAULT '',
                 persisted_as_name TEXT NOT NULL DEFAULT ''
             );
             INSERT INTO received_files (id, filerequest_id, file_name, file_size_bytes, contact_id)
                 VALUES ('rf1', 'fr-own', 'a.txt', 1, 'c1');
             INSERT INTO received_files (id, filerequest_id, file_name, file_size_bytes, contact_id)
                 VALUES ('rf2', 'fr-own', 'b.txt', 1, 'gone');

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
        assert_eq!(get_db_version(&conn).unwrap(), CURRENT_SCHEMA_VERSION);

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

        let bob_id: ContactId = fc1_contact_id(&key_fingerprint_v1(&[0xaa, 0xbb], &[0xcc, 0xdd]))
            .unwrap()
            .parse()
            .unwrap();
        let contacts_doc: ContactsDoc =
            sync::hydrate(&sync::load(&blob(DocKind::Contacts)).unwrap()).unwrap();
        assert_eq!(contacts_doc.contacts.len(), 1);
        assert_eq!(contacts_doc.contacts[&bob_id].name, "Bob");

        // Bob keeps his rows under the derived id. The keyless contact has no key material
        // anywhere, so it cannot be given one and goes, along with its references.
        let contact_rows: Vec<String> = conn
            .prepare("SELECT id FROM contacts ORDER BY id")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(contact_rows, vec![bob_id.to_string()]);

        let received: Vec<(String, Option<String>)> = conn
            .prepare("SELECT id, contact_id FROM received_files ORDER BY id")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            received,
            vec![
                ("rf1".to_string(), Some(bob_id.to_string())),
                // The sender cannot be identified any more, but the file was still received.
                ("rf2".to_string(), None),
            ]
        );
        let audience: String = conn
            .query_row("SELECT contact_id FROM filerequest_audience", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(audience, bob_id.to_string());
        assert!(
            conn.query_row("SELECT contact_id FROM self_contact", [], |r| r
                .get::<_, String>(0))
                .is_err(),
            "the self contact's id is derived from its keys, not stored"
        );

        let remote_doc: RemoteFrDoc =
            sync::hydrate(&sync::load(&blob(DocKind::RemoteFilerequests)).unwrap()).unwrap();
        let rfr = &remote_doc.remote_filerequests[&FylesId("rfr1".into())];
        assert_eq!(rfr.contact_id, bob_id);
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

    #[test]
    fn deriving_contact_ids_downgrades_and_upgrades_again() {
        use std::collections::HashMap;

        use crypto::{
            ContactKeys, serialize_ed25519_private_key, serialize_ed25519_public_key,
            serialize_pq_signing_private_key, serialize_pq_signing_public_key,
        };

        use crate::core::sync::{self, ContactsDoc, DocKind};

        let public_bytes = |keys: &ContactKeys| {
            (
                serialize_pq_signing_public_key(&keys.public.pq_signing),
                serialize_ed25519_public_key(&keys.public.ed25519),
            )
        };
        let me = ContactKeys::new();
        let keyed = ContactKeys::new();
        let keyless = ContactKeys::new();
        let (keyed_pq, keyed_ed) = public_bytes(&keyed);
        let (keyless_pq, keyless_ed) = public_bytes(&keyless);
        let keyed_fingerprint = key_fingerprint_v1(&keyed_pq, &keyed_ed);
        let keyless_fingerprint = key_fingerprint_v1(&keyless_pq, &keyless_ed);

        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(&pending_files_1_25("pending_files"))
            .unwrap();
        conn.execute_batch(
            "CREATE TABLE schema_version (version TEXT NOT NULL);
             INSERT INTO schema_version (version) VALUES ('1.23.0');
             CREATE TABLE contacts (
                 id TEXT PRIMARY KEY, name TEXT NOT NULL,
                 created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                 pq_signing_public_key BLOB, ed25519_public_key BLOB
             );
             CREATE TABLE filerequest_audience (
                 filerequest_id TEXT NOT NULL, contact_id TEXT NOT NULL, created_at TEXT NOT NULL,
                 PRIMARY KEY (filerequest_id, contact_id)
             );
             CREATE TABLE received_files (id TEXT PRIMARY KEY, contact_id TEXT);
             CREATE TABLE self_contact (
                 id INTEGER PRIMARY KEY CHECK (id = 1),
                 contact_id TEXT NOT NULL UNIQUE, name TEXT NOT NULL,
                 pq_signing_private_key BLOB, ed25519_private_key BLOB
             );
             CREATE TABLE sync_docs (doc_kind TEXT PRIMARY KEY, data BLOB NOT NULL);",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO contacts VALUES ('keyed-uuid', 'Keyed', 'now', 'now', ?, ?)",
            rusqlite::params![keyed_pq, keyed_ed],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO self_contact VALUES (1, 'my-uuid', 'Me', ?, ?)",
            rusqlite::params![
                serialize_pq_signing_private_key(&me.private.pq_signing),
                serialize_ed25519_private_key(&me.private.ed25519)
            ],
        )
        .unwrap();
        let contacts_v1 = ContactsDocV1 {
            contacts: HashMap::from([
                (
                    "keyed-uuid".to_string(),
                    ContactEntryV1 {
                        name: "Keyed".into(),
                        key_fingerprint: keyed_fingerprint.clone(),
                    },
                ),
                (
                    "keyless-uuid".to_string(),
                    ContactEntryV1 {
                        name: "Keyless".into(),
                        key_fingerprint: keyless_fingerprint.clone(),
                    },
                ),
            ]),
        };
        conn.execute(
            "INSERT INTO sync_docs (doc_kind, data) VALUES (?, ?)",
            rusqlite::params![
                DocKind::Contacts.as_str(),
                new_blob(DocKind::Contacts, &contacts_v1).unwrap()
            ],
        )
        .unwrap();
        let contacts_doc = |conn: &Connection| {
            let blob: Vec<u8> = conn
                .query_row(
                    "SELECT data FROM sync_docs WHERE doc_kind = ?",
                    [DocKind::Contacts.as_str()],
                    |r| r.get(0),
                )
                .unwrap();
            sync::load(&blob).unwrap()
        };
        let assert_upgraded = |conn: &Connection| {
            assert_eq!(get_db_version(conn).unwrap(), CURRENT_SCHEMA_VERSION);
            let view: ContactsDoc = sync::hydrate(&contacts_doc(conn)).unwrap();
            assert_eq!(view.contacts[&keyed.id()].name, "Keyed");
            assert_eq!(view.contacts[&keyless.id()].name, "Keyless");
            assert_eq!(view.contacts.len(), 2);
        };

        run_migrations(&mut conn).unwrap();
        assert_upgraded(&conn);

        let start = Version::new(1, 23, 0);
        for migration in get_migrations()
            .iter()
            .rev()
            .take_while(|m| m.applies_to_version >= start)
        {
            migration.rollback(&conn).unwrap();
        }

        assert_eq!(get_db_version(&conn).unwrap(), start);
        let self_contact_id: String = conn
            .query_row("SELECT contact_id FROM self_contact", [], |r| r.get(0))
            .unwrap();
        assert_eq!(self_contact_id, me.id().to_string());
        let downgraded: ContactsDocV1 = sync::hydrate(&contacts_doc(&conn)).unwrap();
        assert_eq!(
            downgraded.contacts[keyed.id().as_ref()].key_fingerprint,
            keyed_fingerprint
        );
        assert_eq!(
            downgraded.contacts[keyless.id().as_ref()].key_fingerprint,
            keyless_fingerprint
        );

        run_migrations(&mut conn).unwrap();
        assert_upgraded(&conn);
    }

    #[test]
    fn existing_installs_keep_listening_on_24256() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(&pending_files_1_25("pending_files"))
            .unwrap();
        conn.execute_batch(
            "CREATE TABLE schema_version (version TEXT NOT NULL);
             INSERT INTO schema_version (version) VALUES ('1.24.0');",
        )
        .unwrap();
        run_migrations(&mut conn).unwrap();

        let port: u16 = conn
            .query_row("SELECT port FROM listen_port WHERE id = 1", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(port, 24256);

        let start = Version::new(1, 24, 0);
        for migration in get_migrations()
            .iter()
            .rev()
            .take_while(|m| m.applies_to_version >= start)
        {
            migration.rollback(&conn).unwrap();
        }
        assert!(
            conn.query_row("SELECT COUNT(*) FROM listen_port", [], |_| Ok(()))
                .is_err()
        );
    }

    #[test]
    fn a_transfer_recorded_without_its_source_stamp_starts_over() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(&pending_files_1_25("pending_files"))
            .unwrap();
        conn.execute_batch(
            "CREATE TABLE schema_version (version TEXT NOT NULL);
             INSERT INTO schema_version (version) VALUES ('1.25.0');
             INSERT INTO pending_files (id, file_path, target_filerequest_id, status, progress_bytes, file_size_bytes, transfer_id, created_at)
                 VALUES ('interrupted', '/a', 'fr', 'Interrupted', 4, 7, 'transfer', 'now');
             INSERT INTO pending_files (id, file_path, target_filerequest_id, status, created_at)
                 VALUES ('sent', '/b', 'fr', 'Sent', 'now');",
        )
        .unwrap();
        run_migrations(&mut conn).unwrap();

        let row = |id: &str| {
            conn.query_row(
                "SELECT status, transfer_id FROM pending_files WHERE id = ?",
                [id],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?)),
            )
            .unwrap()
        };
        assert_eq!(row("interrupted"), ("Pending".into(), None));
        assert_eq!(row("sent"), ("Sent".into(), None));

        let stamp_and_later = get_migrations()
            .into_iter()
            .rev()
            .take_while(|m| m.applies_to_version >= Version::new(1, 25, 0));
        for migration in stamp_and_later {
            migration.rollback(&conn).unwrap();
        }
        assert_eq!(get_db_version(&conn).unwrap(), Version::new(1, 25, 0));
        assert_eq!(row("interrupted"), ("Pending".into(), None));
    }
}
