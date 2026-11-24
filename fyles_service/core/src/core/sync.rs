//! Multi-device sync of contacts and remote filerequests via per-concern automerge
//! documents ([`DocKind`]).
//!
//! Load-bearing invariants:
//! - Each collection is a map keyed by a globally-stable id, so documents converge by key
//!   without a shared ancestor (contacts key on [`ContactId`], remote filerequests on their
//!   local [`FylesId`]).
//! - Public keys never enter a document. A contact's key bytes are synced out of band and
//!   checked against the [`ContactId`] they derive, which is the map key.

use std::collections::HashMap;

use automerge::sync::SyncDoc;
use automerge::transaction::CommitOptions;
use automerge::{ActorId, AutoCommit, ChangeHash};
use autosurgeon::{Hydrate, Reconcile};
use sha2::{Digest, Sha256};

use crate::core::db::{DbError, FilerequestDb};
use crate::core::domain_models::{
    ContactId, DisplayContact, FylesId, GivenName, PeerIdWrapper, RemoteFilerequest,
};

/// Which synced document — each is an independent automerge blob with its own schema major.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DocKind {
    Contacts,
    RemoteFilerequests,
    Devices,
}

#[derive(Debug, Clone)]
pub enum ContactChange {
    Upserted(DisplayContact),
    Removed(ContactId),
}

#[derive(Debug, Clone)]
pub enum RemoteFilerequestChange {
    Upserted(RemoteFilerequest),
    Removed(FylesId),
}

#[derive(Debug, Clone)]
pub enum DeviceChange {
    Upserted(PeerIdWrapper, String),
    Removed(PeerIdWrapper),
}

/// What a local write did, split by concern so the sync side can match on the concern alone.
#[derive(Debug, Clone)]
pub enum DocChange {
    Contacts(ContactChange),
    RemoteFilerequests(RemoteFilerequestChange),
    Devices(DeviceChange),
}

/// Evidence that a *local* call wrote to a synced document, carrying the change it made. A
/// convergence merge writes through `mutate_sync_doc`, which returns `()`, so it cannot announce
/// and cannot gossip.
#[must_use = "a local write to a synced document should reach the attached clients and the user's \
              other devices — pass it to `GigaBrain::commit_local_write`, or discard it explicitly \
              with a comment saying why"]
#[derive(Debug, Clone)]
pub struct LocalDocWrite(DocChange);

impl LocalDocWrite {
    pub(crate) fn new(change: DocChange) -> Self {
        Self(change)
    }

    pub fn kind(&self) -> DocKind {
        match self.0 {
            DocChange::Contacts(_) => DocKind::Contacts,
            DocChange::RemoteFilerequests(_) => DocKind::RemoteFilerequests,
            DocChange::Devices(_) => DocKind::Devices,
        }
    }

    pub fn change(&self) -> &DocChange {
        &self.0
    }
}

impl DocKind {
    pub const ALL: [DocKind; 3] = [
        DocKind::Contacts,
        DocKind::RemoteFilerequests,
        DocKind::Devices,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            DocKind::Contacts => "contacts",
            DocKind::RemoteFilerequests => "remote_filerequests",
            DocKind::Devices => "devices",
        }
    }

    /// Peers on a different major skip this document rather than risk a corrupt hydrate.
    /// Bump only on an incompatible shape change (add-via-`missing`/removal are compatible;
    /// see `core/proofs/schema_evolution_spike.rs`).
    pub fn schema_major(self) -> u32 {
        match self {
            DocKind::Contacts => 2,
            DocKind::RemoteFilerequests => 2,
            DocKind::Devices => 1,
        }
    }
}

impl std::str::FromStr for DocKind {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "contacts" => Ok(DocKind::Contacts),
            "remote_filerequests" => Ok(DocKind::RemoteFilerequests),
            "devices" => Ok(DocKind::Devices),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Reconcile, Hydrate)]
pub struct ContactEntry {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Reconcile, Hydrate)]
pub struct RemoteFrEntry {
    /// A re-resolvable locator, not identity — resolve via `contact_id` when stale.
    pub peer_id_hint: PeerIdWrapper,
    pub filerequest_id: FylesId,
    #[autosurgeon(with = "contact_id_codec")]
    pub contact_id: ContactId,
    pub name: String,
}

mod contact_id_codec {
    use autosurgeon::{Hydrate, HydrateError, Prop, ReadDoc, Reconcile, Reconciler};

    use super::ContactId;

    pub(super) fn reconcile<R: Reconciler>(id: &ContactId, reconciler: R) -> Result<(), R::Error> {
        id.as_ref().to_string().reconcile(reconciler)
    }

    pub(super) fn hydrate<D: ReadDoc>(
        doc: &D,
        obj: &automerge::ObjId,
        prop: Prop<'_>,
    ) -> Result<ContactId, HydrateError> {
        String::hydrate(doc, obj, prop)?
            .parse()
            .map_err(|e: crypto::ContactIdParseError| {
                HydrateError::unexpected("a contact id", e.to_string())
            })
    }
}

#[derive(Debug, Clone, PartialEq, Reconcile, Hydrate)]
pub struct DeviceEntry {
    pub name: String,
}

#[derive(Debug, Clone, Default, Reconcile, Hydrate)]
pub struct ContactsDoc {
    #[autosurgeon(with = "autosurgeon::map_with_parseable_keys")]
    pub contacts: HashMap<ContactId, ContactEntry>,
}

#[derive(Debug, Clone, Default, Reconcile, Hydrate)]
pub struct RemoteFrDoc {
    #[autosurgeon(with = "autosurgeon::map_with_parseable_keys")]
    pub remote_filerequests: HashMap<FylesId, RemoteFrEntry>,
}

#[derive(Debug, Clone, Default, Reconcile, Hydrate)]
pub struct DevicesDoc {
    #[autosurgeon(with = "autosurgeon::map_with_parseable_keys")]
    pub devices: HashMap<PeerIdWrapper, DeviceEntry>,
}

#[derive(Debug, thiserror::Error)]
pub enum SyncDocError {
    #[error("automerge error: {0}")]
    Automerge(#[from] automerge::AutomergeError),
    #[error("hydrate error: {0}")]
    Hydrate(#[from] autosurgeon::HydrateError),
    #[error("reconcile error: {0}")]
    Reconcile(#[from] autosurgeon::ReconcileError),
    #[error("sync message decode error: {0}")]
    SyncMessage(#[from] automerge::sync::ReadMessageError),
}

/// Fixed actor + timestamp for the genesis change only, so every device's genesis is
/// byte-identical. That shared change hash gives the top-level maps the same object id
/// everywhere, so per-key entries merge instead of the whole container conflicting.
const GENESIS_ACTOR: &[u8] = b"fyles-sync-genesis-v1";
const GENESIS_TIME: i64 = 0;

/// A document MUST start from `genesis` (or a blob descended from one), never a bare
/// `AutoCommit::new()` — otherwise the top-level maps are distinct objects and a merge
/// discards one side. The actor is then randomized so concurrent local edits don't collide.
pub fn genesis(kind: DocKind) -> Result<AutoCommit, SyncDocError> {
    let mut doc = AutoCommit::new();
    doc.set_actor(ActorId::from(GENESIS_ACTOR));
    match kind {
        DocKind::Contacts => reconcile(&mut doc, &ContactsDoc::default())?,
        DocKind::RemoteFilerequests => reconcile(&mut doc, &RemoteFrDoc::default())?,
        DocKind::Devices => reconcile(&mut doc, &DevicesDoc::default())?,
    }
    doc.commit_with(CommitOptions::default().with_time(GENESIS_TIME));
    doc.set_actor(ActorId::random());
    Ok(doc)
}

pub fn load(blob: &[u8]) -> Result<AutoCommit, SyncDocError> {
    Ok(AutoCommit::load(blob)?)
}

/// Flatten a document-level failure into a `DbError` for the blob-transform boundary of
/// [`FilerequestDb::mutate_sync_doc`], whose transform must report a single error type.
pub fn sync_doc_err(e: SyncDocError) -> DbError {
    DbError::Validation {
        message: format!("sync document error: {e}"),
    }
}

pub fn hydrate<T: Hydrate>(doc: &AutoCommit) -> Result<T, SyncDocError> {
    Ok(autosurgeon::hydrate(doc)?)
}

pub fn reconcile<T: Reconcile>(doc: &mut AutoCommit, value: &T) -> Result<(), SyncDocError> {
    autosurgeon::reconcile(doc, value)?;
    Ok(())
}

/// Fold `from`'s changes into `into` (they must share the genesis change). Used to reconcile
/// an exchange's received changes back into the currently-persisted doc without losing local
/// writes that landed during the exchange.
pub fn merge(into: &mut AutoCommit, from: &mut AutoCommit) -> Result<(), SyncDocError> {
    into.merge(from)?;
    Ok(())
}

pub fn new_contacts_blob(
    contacts: HashMap<ContactId, ContactEntry>,
) -> Result<Vec<u8>, SyncDocError> {
    let mut doc = genesis(DocKind::Contacts)?;
    reconcile(&mut doc, &ContactsDoc { contacts })?;
    Ok(doc.save())
}

pub fn new_remote_fr_blob(
    remote_filerequests: HashMap<FylesId, RemoteFrEntry>,
) -> Result<Vec<u8>, SyncDocError> {
    let mut doc = genesis(DocKind::RemoteFilerequests)?;
    reconcile(
        &mut doc,
        &RemoteFrDoc {
            remote_filerequests,
        },
    )?;
    Ok(doc.save())
}

pub fn new_devices_blob(
    devices: HashMap<PeerIdWrapper, DeviceEntry>,
) -> Result<Vec<u8>, SyncDocError> {
    let mut doc = genesis(DocKind::Devices)?;
    reconcile(&mut doc, &DevicesDoc { devices })?;
    Ok(doc.save())
}

pub fn empty_blob(kind: DocKind) -> Result<Vec<u8>, SyncDocError> {
    Ok(genesis(kind)?.save())
}

#[derive(Debug, thiserror::Error)]
pub enum CrdtPersistenceError {
    #[error(transparent)]
    Db(#[from] DbError),
    #[error(transparent)]
    Document(#[from] SyncDocError),
}

/// Identifies one sync exchange. Both ends run at most one exchange per document at a time; if
/// two start concurrently (a glare) the ids give a deterministic total order so each side keeps
/// the same one. Serializes as a plain string on the wire.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct SyncId(String);

impl SyncId {
    /// A fresh, unique exchange id.
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
}

impl Default for SyncId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for SyncId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The automerge-carrying payload of one exchange step, shared by both directions. `message`
/// is exactly what `generate_sync_message` produced: `Some` = a message to apply; `None` = "my
/// automerge is quiescent this round" (part of the convergence handshake). Either is fed to the
/// peer's [`sync_step`].
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SyncPayload {
    pub doc_kind: String,
    pub schema_major: u32,
    pub message: Option<Vec<u8>>,
    /// The exchange this payload belongs to (see [`SyncId`]).
    pub sync_id: SyncId,
}

/// Initiator → responder. An initiator speaks automerge (including the final `Sync{None}` that
/// lets the responder converge), or aborts to release the responder's parked state.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SyncRequest {
    Sync(SyncPayload),
    /// The initiator hit an internal error mid-exchange: drop your sync state for this exchange.
    InternalError {
        doc_kind: String,
        sync_id: SyncId,
    },
}

/// Responder → initiator.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SyncResponse {
    /// Automerge's reply for this exchange.
    Sync(SyncPayload),
    /// Pure request-response artifact — carries nothing. Emitted only by a responder that just
    /// reached both-sides-quiescent convergence, so its receipt is a guaranteed no-op.
    NoMoreToSay,
    /// The responder cannot handle this document (unknown `doc_kind` or unsupported
    /// `schema_major`); the initiator abandons this document's slot.
    DocumentVersionNotSupported { doc_kind: String, sync_id: SyncId },
    /// The responder hit an internal error; the initiator abandons its slot.
    InternalError { doc_kind: String, sync_id: SyncId },
}

impl SyncRequest {
    pub fn sync_id(&self) -> &SyncId {
        match self {
            SyncRequest::Sync(p) => &p.sync_id,
            SyncRequest::InternalError { sync_id, .. } => sync_id,
        }
    }

    pub fn doc_kind(&self) -> &str {
        match self {
            SyncRequest::Sync(p) => &p.doc_kind,
            SyncRequest::InternalError { doc_kind, .. } => doc_kind,
        }
    }
}

pub fn sync_step(
    doc: &mut AutoCommit,
    state: &mut automerge::sync::State,
    incoming: Option<&[u8]>,
) -> Result<Option<Vec<u8>>, SyncDocError> {
    if let Some(bytes) = incoming {
        let msg = automerge::sync::Message::decode(bytes)?;
        doc.sync().receive_sync_message(state, msg)?;
    }
    Ok(doc.sync().generate_sync_message(state).map(|m| m.encode()))
}

pub fn heads_fingerprint(heads: &[ChangeHash]) -> Vec<u8> {
    let mut sorted: Vec<&ChangeHash> = heads.iter().collect();
    sorted.sort();
    let mut hasher = Sha256::new();
    for head in sorted {
        hasher.update(head.0);
    }
    hasher.finalize().to_vec()
}

pub fn doc_heads_fingerprint(doc: &mut AutoCommit) -> Vec<u8> {
    heads_fingerprint(&doc.get_heads())
}

/// Every caller is a local change to the owned-device list, so this may mint a [`LocalDocWrite`].
async fn mutate_devices_doc<F>(
    db: &dyn FilerequestDb,
    f: F,
) -> Result<LocalDocWrite, CrdtPersistenceError>
where
    F: FnOnce(&mut DevicesDoc) -> DeviceChange + Send + 'static,
{
    let change = db.mutate_devices_doc(Box::new(f)).await?;
    Ok(LocalDocWrite::new(DocChange::Devices(change)))
}

pub async fn add_or_update_owned_device(
    db: &dyn FilerequestDb,
    peer_id: PeerIdWrapper,
    name: String,
) -> Result<LocalDocWrite, CrdtPersistenceError> {
    mutate_devices_doc(db, move |view| {
        view.devices
            .insert(peer_id.clone(), DeviceEntry { name: name.clone() });
        DeviceChange::Upserted(peer_id, name)
    })
    .await
}

/// A device only ever writes its *own* entry; the CRDT exchange propagates it.
pub async fn ensure_own_device_registered(
    db: &dyn FilerequestDb,
    own_peer_id: PeerIdWrapper,
) -> Result<LocalDocWrite, CrdtPersistenceError> {
    mutate_devices_doc(db, move |view| {
        let entry = view
            .devices
            .entry(own_peer_id.clone())
            .or_insert_with(|| DeviceEntry {
                name: GivenName::unnamed(),
            });
        DeviceChange::Upserted(own_peer_id, entry.name.clone())
    })
    .await
}

/// Cosmetic only — see the revocation limitation.
pub async fn remove_owned_device(
    db: &dyn FilerequestDb,
    peer_id: &PeerIdWrapper,
) -> Result<LocalDocWrite, CrdtPersistenceError> {
    let peer_id = peer_id.clone();
    mutate_devices_doc(db, move |view| {
        view.devices.remove(&peer_id);
        DeviceChange::Removed(peer_id)
    })
    .await
}

pub async fn list_owned_devices(
    db: &dyn FilerequestDb,
) -> Result<Vec<(PeerIdWrapper, String)>, CrdtPersistenceError> {
    let doc = load(&db.get_sync_doc(DocKind::Devices).await?)?;
    let view: DevicesDoc = hydrate(&doc)?;
    Ok(view
        .devices
        .into_iter()
        .map(|(id, entry)| (id, entry.name))
        .collect())
}

pub async fn is_owned_device(
    db: &dyn FilerequestDb,
    peer_id: &PeerIdWrapper,
) -> Result<bool, CrdtPersistenceError> {
    let doc = load(&db.get_sync_doc(DocKind::Devices).await?)?;
    let view: DevicesDoc = hydrate(&doc)?;
    Ok(view.devices.contains_key(peer_id))
}

#[cfg(test)]
mod doc_tests {
    use super::*;
    use automerge::sync::{self, SyncDoc};

    /// Drive the automerge sync protocol between two docs to quiescence (test harness for
    /// the real request/response exchange). Mutates both to convergence.
    fn drive_sync(a: &mut AutoCommit, b: &mut AutoCommit) {
        let mut a_state = sync::State::new();
        let mut b_state = sync::State::new();
        loop {
            let a_to_b = a.sync().generate_sync_message(&mut a_state);
            if let Some(m) = a_to_b.as_ref() {
                b.sync()
                    .receive_sync_message(&mut b_state, m.clone())
                    .unwrap();
            }
            let b_to_a = b.sync().generate_sync_message(&mut b_state);
            if let Some(m) = b_to_a.as_ref() {
                a.sync()
                    .receive_sync_message(&mut a_state, m.clone())
                    .unwrap();
            }
            if a_to_b.is_none() && b_to_a.is_none() {
                break;
            }
        }
    }

    fn contact_entry(name: &str) -> ContactEntry {
        ContactEntry { name: name.into() }
    }

    /// A contacts document seeded from the shared genesis with a single entry — the way
    /// a real device builds its doc (genesis, then insert local rows).
    fn seeded_contacts(id: &str, name: &str) -> AutoCommit {
        let mut doc = genesis(DocKind::Contacts).unwrap();
        let mut view: ContactsDoc = hydrate(&doc).unwrap();
        view.contacts
            .insert(ContactId::for_test_labelled(id), contact_entry(name));
        reconcile(&mut doc, &view).unwrap();
        doc
    }

    #[test]
    fn genesis_is_deterministic() {
        // Two independent genesis constructions must share the same initial change hash,
        // which is what lets their top-level maps merge by key.
        let mut a = genesis(DocKind::Contacts).unwrap();
        let mut b = genesis(DocKind::Contacts).unwrap();
        assert_eq!(a.get_heads(), b.get_heads(), "genesis heads must match");
    }

    #[test]
    fn roundtrips_through_blob() {
        let mut doc = seeded_contacts("c1", "alice");
        let blob = doc.save();

        let loaded = load(&blob).unwrap();
        let hydrated: ContactsDoc = hydrate(&loaded).unwrap();
        assert_eq!(
            hydrated.contacts[&ContactId::for_test_labelled("c1")].name,
            "alice"
        );
    }

    #[test]
    fn independent_docs_converge_on_same_contact_id() {
        // Two devices seed from the shared genesis holding the SAME contact id. They
        // must converge to a single keyed entry.
        let mut a = seeded_contacts("shared", "alice");
        let mut b = seeded_contacts("shared", "alice");

        a.merge(&mut b).unwrap();
        let hydrated: ContactsDoc = hydrate(&a).unwrap();
        assert_eq!(hydrated.contacts.len(), 1, "same key must not duplicate");
    }

    #[test]
    fn sync_protocol_brings_empty_doc_up_to_date() {
        // A freshly-genesised (empty) doc syncs to a populated one via the uniform
        // sync-protocol exchange — same path used for both first-join and later updates.
        let mut populated = seeded_contacts("c1", "alice");
        let mut fresh = genesis(DocKind::Contacts).unwrap();

        drive_sync(&mut populated, &mut fresh);
        let hydrated: ContactsDoc = hydrate(&fresh).unwrap();
        assert_eq!(
            hydrated.contacts[&ContactId::for_test_labelled("c1")].name,
            "alice"
        );
    }

    /// The auto-sync dirty gate compares the current doc-heads fingerprint against the
    /// fingerprint recorded at convergence from the exchange's `shared_heads`. For the gate
    /// to ever register "clean" (and not re-push on every connect), those two must be equal
    /// at the moment each side converges. This mirrors the brain's exact request/response
    /// state machine (`Option<Vec<u8>>` messages, convergence on a `None` round) and checks it.
    #[test]
    fn shared_heads_equal_doc_heads_at_convergence() {
        let mut initiator = seeded_contacts("c1", "alice");
        let mut responder = genesis(DocKind::Contacts).unwrap();
        let mut i_state = sync::State::new();
        let mut r_state = sync::State::new();

        // initiate_sync: the initiator produces the opening message.
        let mut to_responder = sync_step(&mut initiator, &mut i_state, None).unwrap();
        let initiator_fp;
        let mut responder_fp = None;

        loop {
            // Responder side (handle_sync_receive): always replies, converges on empty/empty.
            let incoming = to_responder.clone();
            let reply = sync_step(&mut responder, &mut r_state, incoming.as_deref()).unwrap();
            if incoming.is_none() && reply.is_none() {
                responder_fp = Some(heads_fingerprint(&r_state.shared_heads));
            }
            // Initiator side (handle_sync_reply): stops when it and the reply are both empty.
            let outgoing = sync_step(&mut initiator, &mut i_state, reply.as_deref()).unwrap();
            if outgoing.is_none() && reply.is_none() {
                initiator_fp = heads_fingerprint(&i_state.shared_heads);
                break;
            }
            to_responder = outgoing;
        }

        let initiator_doc_fp = doc_heads_fingerprint(&mut initiator);
        let responder_doc_fp = doc_heads_fingerprint(&mut responder);
        assert_eq!(
            initiator_fp, initiator_doc_fp,
            "initiator's recorded shared_heads must equal its converged doc heads"
        );
        assert_eq!(
            responder_fp.expect("responder must converge"),
            responder_doc_fp,
            "responder's recorded shared_heads must equal its converged doc heads"
        );
        assert_eq!(
            initiator_doc_fp, responder_doc_fp,
            "both docs converge to the same heads"
        );
    }

    #[test]
    fn sync_protocol_converges_independent_edits() {
        // Both sides made independent local edits; the sync protocol converges them to
        // the union, just like a direct merge.
        let mut a = seeded_contacts("c_a", "alice");
        let mut b = seeded_contacts("c_b", "bob");

        drive_sync(&mut a, &mut b);
        let ha: ContactsDoc = hydrate(&a).unwrap();
        let hb: ContactsDoc = hydrate(&b).unwrap();
        assert_eq!(ha.contacts.len(), 2);
        assert_eq!(hb.contacts.len(), 2);
    }

    #[test]
    fn independent_docs_union_distinct_contacts() {
        // The pre-provisioned case: two devices independently seeded distinct contacts
        // before ever syncing. Sharing the genesis root, the merge is a clean union.
        let mut a = seeded_contacts("c_a", "alice");
        let mut b = seeded_contacts("c_b", "bob");

        a.merge(&mut b).unwrap();
        let hydrated: ContactsDoc = hydrate(&a).unwrap();
        assert_eq!(hydrated.contacts.len(), 2);
        assert!(
            hydrated
                .contacts
                .contains_key(&ContactId::for_test_labelled("c_a"))
        );
        assert!(
            hydrated
                .contacts
                .contains_key(&ContactId::for_test_labelled("c_b"))
        );
    }

    #[test]
    fn bare_new_docs_do_not_converge() {
        // Documents the trap: WITHOUT the shared genesis, two `AutoCommit::new()` docs
        // have distinct top-level map objects, so a merge keeps only one side's map.
        let mut a = AutoCommit::new();
        let mut va = ContactsDoc::default();
        va.contacts
            .insert(ContactId::for_test_labelled("c_a"), contact_entry("alice"));
        reconcile(&mut a, &va).unwrap();

        let mut b = AutoCommit::new();
        let mut vb = ContactsDoc::default();
        vb.contacts
            .insert(ContactId::for_test_labelled("c_b"), contact_entry("bob"));
        reconcile(&mut b, &vb).unwrap();

        a.merge(&mut b).unwrap();
        let hydrated: ContactsDoc = hydrate(&a).unwrap();
        assert_eq!(
            hydrated.contacts.len(),
            1,
            "bare new() docs conflict on the container and lose one side"
        );
    }

    #[test]
    fn remote_fr_duplicates_coexist_under_distinct_local_ids() {
        // Same logical remote filerequest recorded independently on two devices keeps
        // its own local FylesId -> both survive the merge (resolved manually).
        let entry = |peer: &str| RemoteFrEntry {
            peer_id_hint: PeerIdWrapper(peer.into()),
            filerequest_id: FylesId("fr-remote".into()),
            contact_id: ContactId::for_test_labelled("contact-x"),
            name: "shared drop".into(),
        };
        let seed = |local_id: &str, peer: &str| {
            let mut doc = genesis(DocKind::RemoteFilerequests).unwrap();
            let mut view: RemoteFrDoc = hydrate(&doc).unwrap();
            view.remote_filerequests
                .insert(FylesId(local_id.into()), entry(peer));
            reconcile(&mut doc, &view).unwrap();
            doc
        };
        let mut a = seed("local-a", "peerA");
        let mut b = seed("local-b", "peerB");

        a.merge(&mut b).unwrap();
        let hydrated: RemoteFrDoc = hydrate(&a).unwrap();
        assert_eq!(hydrated.remote_filerequests.len(), 2);
    }

    #[test]
    fn key_removal_deletes_entry() {
        let mut doc = seeded_contacts("c1", "alice");
        let mut d: ContactsDoc = hydrate(&doc).unwrap();
        d.contacts
            .insert(ContactId::for_test_labelled("c2"), contact_entry("bob"));
        reconcile(&mut doc, &d).unwrap();

        // Remove c1 and reconcile the smaller map back in.
        d.contacts.remove(&ContactId::for_test_labelled("c1"));
        reconcile(&mut doc, &d).unwrap();

        let hydrated: ContactsDoc = hydrate(&doc).unwrap();
        assert!(
            !hydrated
                .contacts
                .contains_key(&ContactId::for_test_labelled("c1"))
        );
        assert!(
            hydrated
                .contacts
                .contains_key(&ContactId::for_test_labelled("c2"))
        );
    }
}

#[cfg(test)]
mod service_tests {
    use super::*;
    use crate::core::db::test::setup_test_db;
    use crate::core::domain_models::{Contact, CreateRemoteFilerequest, PeerIdWrapper};

    /// One protocol step against a device's stored document, driven by a *live* sync
    /// `State` (exactly what the brain's `sync_step_owned` does): load the blob, advance the
    /// protocol, persist if the incoming message mutated the doc. The `State` stays a live
    /// object across the whole exchange — never serialized — which is what lets it converge.
    async fn sync_one(
        db: &dyn crate::core::db::FilerequestDb,
        state: &mut automerge::sync::State,
        kind: DocKind,
        incoming: Option<Vec<u8>>,
    ) -> Option<Vec<u8>> {
        let mut doc = load(&db.get_sync_doc(kind).await.unwrap()).unwrap();
        let out = sync_step(&mut doc, state, incoming.as_deref()).unwrap();
        if incoming.is_some() {
            db.save_sync_doc(kind, doc.save()).await.unwrap();
        }
        out
    }

    async fn doc_fingerprint(db: &dyn crate::core::db::FilerequestDb, kind: DocKind) -> Vec<u8> {
        let mut doc = load(&db.get_sync_doc(kind).await.unwrap()).unwrap();
        doc_heads_fingerprint(&mut doc)
    }

    /// Drive the automerge sync protocol for one `DocKind` between two devices to
    /// quiescence, exactly mirroring how the brain drives it over the wire: the initiator
    /// (A) sends a message, the responder (B) *always* replies (with a `None`-payload wire
    /// once quiescent), and the initiator keeps going until a `None`↔`None` handshake — the
    /// empty "flush" round is what lets both sides reach quiescence. Returns the number of
    /// round-trips. A generous cap guards the test against a protocol regression looping.
    async fn run_doc_sync(
        a: &dyn crate::core::db::FilerequestDb,
        b: &dyn crate::core::db::FilerequestDb,
        kind: DocKind,
    ) -> usize {
        let mut state_a = automerge::sync::State::new();
        let mut state_b = automerge::sync::State::new();
        // A initiates; `to_b` is the payload (possibly `None`) A sends to B this round.
        let mut to_b = sync_one(a, &mut state_a, kind, None).await;
        let mut rounds = 0;
        loop {
            rounds += 1;
            assert!(rounds < 100, "sync for {kind:?} did not converge");
            // Responder side (handle_sync_receive): always produce a reply wire.
            let out_b = sync_one(b, &mut state_b, kind, to_b).await;
            let reply_none = out_b.is_none();
            // Initiator side (handle_sync_reply): advance and, unless converged, send again.
            let out_a = sync_one(a, &mut state_a, kind, out_b).await;
            if out_a.is_some() || !reply_none {
                to_b = out_a;
            } else {
                break;
            }
        }
        rounds
    }

    async fn run_full_sync(
        a: &dyn crate::core::db::FilerequestDb,
        b: &dyn crate::core::db::FilerequestDb,
    ) -> usize {
        let mut total = 0;
        for kind in DocKind::ALL {
            total += run_doc_sync(a, b, kind).await;
        }
        total
    }

    /// Two owned devices, each with independent local data, converge via the automerge
    /// sync protocol. Keys are *not* carried — they arrive out-of-band via pubkey-fetch —
    /// so only the documents (contacts + remote filerequests) converge here.
    #[tokio::test]
    async fn two_devices_converge_via_sync_protocol() {
        // Two independent in-memory Sqlite devices.
        let dev_a = setup_test_db(None).await;
        let dev_b = setup_test_db(None).await;
        let a = &*dev_a;
        let b = &*dev_b;

        // Independent local state on each device.
        let alice = Contact::for_test();
        let bob = Contact::for_test();
        let _ = a.update_contact_projection(alice.clone()).await.unwrap();
        let _ = b.update_contact_projection(bob.clone()).await.unwrap();

        let (rid_a, _) = a
            .create_remote_filerequest(&CreateRemoteFilerequest {
                peer_id: PeerIdWrapper("peer-a".into()),
                filerequest_id: "fr-a".into(),
                name: "from A".into(),
                contact_id: alice.id(),
            })
            .await
            .unwrap();

        run_full_sync(a, b).await;

        // Both devices now see both contacts and A's remote filerequest.
        for (label, db) in [("A", a), ("B", b)] {
            let contacts = db.get_contacts().await.unwrap();
            assert!(
                contacts.iter().any(|c| c.id == alice.id())
                    && contacts.iter().any(|c| c.id == bob.id()),
                "device {label} should have both contacts, got {contacts:?}"
            );
            let all = db.get_all_remote_filerequests().await.unwrap();
            assert!(
                all.iter().any(|r| r.id == rid_a),
                "device {label} should have A's remote filerequest"
            );
        }
    }

    /// A repeat sync with no intervening changes is delta-empty: after convergence, the
    /// documents are equal and re-running the protocol transfers only the initial
    /// heads-comparison message (no change payloads).
    #[tokio::test]
    async fn repeat_sync_is_delta_empty() {
        let dev_a = setup_test_db(None).await;
        let dev_b = setup_test_db(None).await;
        let a = &*dev_a;
        let b = &*dev_b;

        let _ = a
            .update_contact_projection(Contact::for_test())
            .await
            .unwrap();
        run_full_sync(a, b).await;

        // Both sides now hold identical documents. A second pass exchanges at most the
        // heads-only handshake and never re-initiates over already-known data.
        let fp_a = doc_fingerprint(a, DocKind::Contacts).await;
        let fp_b = doc_fingerprint(b, DocKind::Contacts).await;
        assert_eq!(fp_a, fp_b, "documents should be equal after convergence");

        run_full_sync(a, b).await;
        assert_eq!(
            doc_fingerprint(a, DocKind::Contacts).await,
            fp_a,
            "a delta-empty re-sync must not change the document"
        );
    }

    #[tokio::test]
    async fn owned_device_list_add_list_remove() {
        let dev = setup_test_db(None).await;
        let db = &*dev;
        let p1 = PeerIdWrapper("peer-1".into());
        let p2 = PeerIdWrapper("peer-2".into());

        assert!(!is_owned_device(db, &p1).await.unwrap());
        let _ = add_or_update_owned_device(db, p1.clone(), "Laptop".into())
            .await
            .unwrap();
        let _ = add_or_update_owned_device(db, p2.clone(), "Phone".into())
            .await
            .unwrap();

        assert!(is_owned_device(db, &p1).await.unwrap());
        let mut list = list_owned_devices(db).await.unwrap();
        list.sort_by(|a, b| a.0.0.cmp(&b.0.0));
        assert_eq!(
            list,
            vec![
                (PeerIdWrapper("peer-1".into()), "Laptop".to_string()),
                (PeerIdWrapper("peer-2".into()), "Phone".to_string()),
            ]
        );

        let _ = remove_owned_device(db, &p1).await.unwrap();
        assert!(!is_owned_device(db, &p1).await.unwrap());
        assert_eq!(list_owned_devices(db).await.unwrap().len(), 1);
    }

    /// A contact removed on another device drops out of the synced doc; pruning then
    /// deletes its cached keys locally, while a contact still in the doc keeps them.
    #[tokio::test]
    async fn prune_reconciles_key_cache_to_synced_doc() {
        let dev = setup_test_db(None).await;
        let db = &*dev;
        let alice = Contact::for_test();
        let bob = Contact::for_test();
        let _ = db.update_contact_projection(alice.clone()).await.unwrap();
        let _ = db.update_contact_projection(bob.clone()).await.unwrap();

        // The synced doc now lists only bob — as if alice were removed elsewhere. Her key
        // bytes are still in the local cache until we prune.
        let mut only_bob = HashMap::new();
        only_bob.insert(
            bob.id(),
            ContactEntry {
                name: bob.name.clone(),
            },
        );
        db.save_sync_doc(DocKind::Contacts, new_contacts_blob(only_bob).unwrap())
            .await
            .unwrap();

        db.prune_contact_key_cache().await.unwrap();

        assert!(
            db.get_contact_public_keys(alice.id())
                .await
                .unwrap()
                .is_none(),
            "removed contact's keys should be pruned"
        );
        assert!(
            db.get_contact_public_keys(bob.id())
                .await
                .unwrap()
                .is_some(),
            "present contact's keys should be kept"
        );
    }

    /// The per-`DocKind` dirty-tracking primitive: the heads fingerprint is stable while
    /// nothing changes and changes whenever that document is written.
    #[tokio::test]
    async fn sync_doc_fingerprint_tracks_changes() {
        let dev = setup_test_db(None).await;
        let db = &*dev;

        let fp0 = doc_fingerprint(db, DocKind::Contacts).await;
        // Reading again with no change yields the same fingerprint.
        assert_eq!(fp0, doc_fingerprint(db, DocKind::Contacts).await);

        // A contact write changes the contacts document.
        let _ = db
            .update_contact_projection(Contact::for_test())
            .await
            .unwrap();
        let fp1 = doc_fingerprint(db, DocKind::Contacts).await;
        assert_ne!(
            fp0, fp1,
            "registering a contact should change the contacts doc"
        );

        // ...but leaves the remote-filerequest document untouched.
        let rfp0 = doc_fingerprint(db, DocKind::RemoteFilerequests).await;
        let _ = db
            .create_remote_filerequest(&CreateRemoteFilerequest {
                peer_id: PeerIdWrapper("p".into()),
                filerequest_id: "fr".into(),
                name: "n".into(),
                contact_id: ContactId::for_test_labelled("c"),
            })
            .await
            .unwrap();
        let rfp1 = doc_fingerprint(db, DocKind::RemoteFilerequests).await;
        assert_ne!(rfp0, rfp1);

        // Still stable when nothing changes.
        assert_eq!(fp1, doc_fingerprint(db, DocKind::Contacts).await);
    }

    /// `DocKind` round-trips through its wire string and exposes a per-kind schema major.
    #[test]
    fn dockind_from_str_roundtrip_and_schema() {
        for kind in DocKind::ALL {
            let s = kind.as_str();
            assert_eq!(
                s.parse::<DocKind>().unwrap(),
                kind,
                "round-trip via FromStr"
            );
            // Every kind currently exposes a positive major it can gate mismatches on.
            assert!(kind.schema_major() >= 1);
        }
        assert!("not-a-doc-kind".parse::<DocKind>().is_err());
    }
}
