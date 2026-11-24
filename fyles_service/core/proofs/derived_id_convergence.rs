//! Two devices that were paired and converged at 1.23.0, migrated while disconnected, then
//! synced again. See `README.md` for the fixtures and how to run this.

use std::collections::HashMap;
use std::path::PathBuf;

use automerge::AutoCommit;
use crypto::{
    ContactId, ContactPublicKeys, deserialize_ed25519_public_key, deserialize_pq_signing_public_key,
};
use rusqlite::Connection;

use super::run_migrations;
use crate::core::domain_models::FylesId;
use crate::core::sync::{self, ContactsDoc, DocKind, RemoteFrDoc};

fn fixture(name: &str) -> Vec<u8> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("proofs/testdata")
        .join(name);
    std::fs::read(&path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display()))
}

fn derived_id(slug: &str) -> ContactId {
    ContactPublicKeys {
        pq_signing: Box::new(
            deserialize_pq_signing_public_key(fixture(&format!("{slug}.pq.bin"))).unwrap(),
        ),
        ed25519: deserialize_ed25519_public_key(fixture(&format!("{slug}.ed.bin"))).unwrap(),
    }
    .id()
}

/// A database at 1.23.0 holding the given documents, and nothing else that matters here.
fn device_at_1_23(contacts: &[u8], remote_fr: &[u8]) -> Connection {
    let mut conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
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
         CREATE TABLE received_files (
             id TEXT PRIMARY KEY, contact_id TEXT, filerequest_id TEXT NOT NULL,
             transfer_id TEXT UNIQUE, file_name TEXT NOT NULL, file_path TEXT,
             file_size_bytes INTEGER NOT NULL, progress_bytes INTEGER NOT NULL DEFAULT 0,
             status TEXT NOT NULL DEFAULT 'Receiving', received_at_ms INTEGER,
             started_at_ms INTEGER NOT NULL DEFAULT 0, peer_id TEXT NOT NULL DEFAULT '',
             persisted_as_name TEXT NOT NULL DEFAULT ''
         );
         CREATE TABLE self_contact (
             id INTEGER PRIMARY KEY CHECK (id = 1),
             contact_id TEXT NOT NULL UNIQUE, name TEXT NOT NULL,
             pq_signing_private_key BLOB, ed25519_private_key BLOB
         );
         INSERT INTO self_contact VALUES (1, 'old-uuid', 'Me', NULL, NULL);
         CREATE TABLE sync_docs (doc_kind TEXT PRIMARY KEY, data BLOB NOT NULL);",
    )
    .unwrap();
    for (kind, blob) in [
        (DocKind::Contacts, contacts),
        (DocKind::RemoteFilerequests, remote_fr),
    ] {
        conn.execute(
            "INSERT INTO sync_docs (doc_kind, data) VALUES (?, ?)",
            rusqlite::params![kind.as_str(), blob],
        )
        .unwrap();
    }
    run_migrations(&mut conn).unwrap();
    conn
}

fn doc(conn: &Connection, kind: DocKind) -> AutoCommit {
    let blob: Vec<u8> = conn
        .query_row(
            "SELECT data FROM sync_docs WHERE doc_kind = ?",
            [kind.as_str()],
            |r| r.get(0),
        )
        .unwrap();
    sync::load(&blob).unwrap()
}

fn device_a() -> Connection {
    device_at_1_23(
        &fixture("contacts_a.v1.automerge"),
        &fixture("remote_fr_a.v1.automerge"),
    )
}

fn device_b() -> Connection {
    device_at_1_23(
        &fixture("contacts_b.v1.automerge"),
        &fixture("remote_fr_b.v1.automerge"),
    )
}

fn names(view: &ContactsDoc) -> HashMap<String, String> {
    view.contacts
        .iter()
        .map(|(id, entry)| (id.to_string(), entry.name.clone()))
        .collect()
}

#[test]
fn both_devices_migrate_to_the_same_ids_without_talking_to_each_other() {
    let a: ContactsDoc = sync::hydrate(&doc(&device_a(), DocKind::Contacts)).unwrap();
    let b: ContactsDoc = sync::hydrate(&doc(&device_b(), DocKind::Contacts)).unwrap();

    for (view, own) in [(&a, "a_only"), (&b, "b_only")] {
        assert!(view.contacts.contains_key(&derived_id("shared")));
        assert!(
            view.contacts.contains_key(&derived_id("keyless")),
            "a contact whose key bytes never arrived still migrates, via its fingerprint"
        );
        assert!(view.contacts.contains_key(&derived_id(own)));
        assert_eq!(view.contacts.len(), 3);
    }

    assert_eq!(
        a.contacts[&derived_id("shared")],
        b.contacts[&derived_id("shared")],
        "the shared contact must land on the same id and value on both devices"
    );
}

#[test]
fn merging_after_both_migrated_converges_and_resurrects_nothing() {
    let mut a = doc(&device_a(), DocKind::Contacts);
    let mut b = doc(&device_b(), DocKind::Contacts);
    let mut b_into_a = a.fork();
    let mut a_into_b = b.fork();

    a.merge(&mut b).unwrap();
    b_into_a.merge(&mut a_into_b).unwrap();

    let merged: ContactsDoc = sync::hydrate(&a).unwrap();
    let merged_other_way: ContactsDoc = sync::hydrate(&b_into_a).unwrap();
    assert_eq!(
        names(&merged),
        names(&merged_other_way),
        "merging must converge whichever side folds in the other"
    );

    // Each device's own pre-migration entry survives: migrating in place did not discard
    // history the other side had not yet seen.
    assert_eq!(merged.contacts.len(), 4);
    for slug in ["shared", "keyless", "a_only", "b_only"] {
        assert!(merged.contacts.contains_key(&derived_id(slug)), "{slug}");
    }
}

/// Re-keying offline makes both devices write the shared entry concurrently, so an edit
/// one of them makes before the first post-migration sync is a concurrent write against
/// the other's migration write, and may lose. What must hold is that they converge on one
/// answer, from either direction, with the entry present exactly once.
#[test]
fn a_device_that_migrates_days_later_still_converges() {
    let mut a_doc = doc(&device_a(), DocKind::Contacts);
    let mut view: ContactsDoc = sync::hydrate(&a_doc).unwrap();
    view.contacts.insert(
        derived_id("shared"),
        sync::ContactEntry {
            name: "Renamed after migrating".into(),
        },
    );
    sync::reconcile(&mut a_doc, &view).unwrap();

    // B migrates only now, days later, and the two meet.
    let mut b_doc = doc(&device_b(), DocKind::Contacts);
    let mut a_into_b = a_doc.fork();
    let mut b_into_a = b_doc.fork();
    b_doc.merge(&mut a_into_b).unwrap();
    a_doc.merge(&mut b_into_a).unwrap();

    let from_b: ContactsDoc = sync::hydrate(&b_doc).unwrap();
    let from_a: ContactsDoc = sync::hydrate(&a_doc).unwrap();
    assert_eq!(names(&from_b), names(&from_a));
    assert_eq!(from_b.contacts.len(), 4);
    assert!(from_b.contacts.contains_key(&derived_id("shared")));
    assert!(
        from_b.contacts.contains_key(&derived_id("b_only")),
        "B's own entry must survive a late migration"
    );
}

#[test]
fn remote_filerequests_follow_their_contact_and_orphans_are_dropped() {
    let a = device_a();
    let view: RemoteFrDoc = sync::hydrate(&doc(&a, DocKind::RemoteFilerequests)).unwrap();

    let shared = &view.remote_filerequests[&FylesId("rfr-shared".into())];
    assert_eq!(shared.contact_id, derived_id("shared"));
    assert_eq!(
        view.remote_filerequests.get(&FylesId("rfr-orphan".into())),
        None,
        "an entry naming a contact that cannot be migrated has to go, or the document \
         stops hydrating"
    );
}

#[test]
fn the_self_contact_id_column_is_gone() {
    let a = device_a();
    assert!(
        a.query_row("SELECT contact_id FROM self_contact", [], |r| r
            .get::<_, String>(0))
            .is_err()
    );
}
