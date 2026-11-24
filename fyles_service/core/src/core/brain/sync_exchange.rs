use automerge::sync::State as SyncProtocolState;
use tracing::{Instrument, error, info, trace, warn};

use crate::constants::{SYNC_SLOT_COUNT_WARN, SYNC_SLOT_STALL_AFTER};
use crate::core::brain::handle_action::SyncMessage;
use crate::core::brain::types::{FetchedContactKeys, PubkeyFetchResponse};
use crate::core::brain::{ActiveExchange, ExchangeSlot, GigaBrain, SlotState};
use crate::core::db::DbError;
use crate::core::domain_models::{Contact, ContactId, PeerIdWrapper};
use crate::core::sync::{
    self, ContactsDoc, DocKind, LocalDocWrite, SyncId, SyncPayload, SyncRequest, SyncResponse,
    ensure_own_device_registered, list_owned_devices, sync_doc_err,
};

enum Claim {
    Continue(Box<ActiveExchange>),
    Fresh {
        sync_id: SyncId,
        last_synced: Option<Vec<u8>>,
    },
    Bail,
}

enum Settlement {
    Converged(Vec<u8>),
    Parked(Box<ActiveExchange>),
}

enum Release {
    Abandoned,
    /// The peer cannot handle this document, so its current version counts as synced.
    Declined(Vec<u8>),
}

impl ExchangeSlot {
    /// Claims the slot for one step. `incoming` is `None` to initiate. `Fresh` carries
    /// `last_synced` for the dirty gate only for an initiation or an empty message, since a peer's
    /// real message is always serviced.
    fn claim(
        &mut self,
        incoming: Option<&SyncId>,
        empty_message: bool,
        peer: &PeerIdWrapper,
        kind: DocKind,
    ) -> Claim {
        let Some(in_id) = incoming else {
            if !matches!(self.state(), SlotState::Idle) {
                return self.bail(peer, kind);
            }
            let sync_id = SyncId::new();
            let last_synced = self.last_synced.clone();
            self.set_state(SlotState::InFlight {
                sync_id: sync_id.clone(),
            });
            return Claim::Fresh {
                sync_id,
                last_synced,
            };
        };
        match std::mem::take(&mut self.state) {
            SlotState::Idle => {
                let last_synced = self.last_synced.clone();
                self.set_state(SlotState::InFlight {
                    sync_id: in_id.clone(),
                });
                Claim::Fresh {
                    sync_id: in_id.clone(),
                    last_synced: empty_message.then_some(last_synced).flatten(),
                }
            }
            SlotState::Parked(active) => {
                if active.sync_id == *in_id {
                    self.set_state(SlotState::InFlight {
                        sync_id: in_id.clone(),
                    });
                    Claim::Continue(active)
                } else {
                    let ours = active.sync_id.clone();
                    self.glare(in_id, ours, SlotState::Parked(active), peer, kind)
                }
            }
            SlotState::InFlight { sync_id } => {
                let ours = sync_id.clone();
                self.glare(in_id, ours, SlotState::InFlight { sync_id }, peer, kind)
            }
        }
    }

    /// Both ends compare the same pair of ids and agree that the lower one wins. A losing or
    /// duplicate id leaves the slot as it was.
    fn glare(
        &mut self,
        theirs: &SyncId,
        ours: SyncId,
        current: SlotState,
        peer: &PeerIdWrapper,
        kind: DocKind,
    ) -> Claim {
        if *theirs < ours {
            warn!(?peer, ?kind, %ours, %theirs, "sync glare: adopting peer's exchange");
            self.set_state(SlotState::InFlight {
                sync_id: theirs.clone(),
            });
            Claim::Fresh {
                sync_id: theirs.clone(),
                last_synced: None,
            }
        } else {
            warn!(?peer, ?kind, %ours, %theirs, "sync glare: keeping ours");
            // Back as it was, so no transition to stamp.
            self.state = current;
            self.bail(peer, kind)
        }
    }

    /// Every bail is a piece of work we chose not to do because the slot was occupied, which
    /// makes this exactly the point where a wedged slot does its damage. Reports rather than
    /// reclaims: a slot that outlives every legitimate exit means one of those exits was missed,
    /// and repairing it here would hide the defect.
    fn bail(&mut self, peer: &PeerIdWrapper, kind: DocKind) -> Claim {
        if self.should_report_stall(SYNC_SLOT_STALL_AFTER) {
            error!(
                ?peer,
                ?kind,
                state_age = ?self.state_age(),
                "Sync slot has been non-idle far longer than any exchange can last; sync for \
                 this document is blocked. An exchange terminated without releasing its slot."
            );
        }
        Claim::Bail
    }

    /// Only while the slot still carries `sync_id`. Otherwise a glare winner superseded us, and
    /// our exchange is dropped.
    fn settle(&mut self, sync_id: &SyncId, settlement: Settlement) {
        let SlotState::InFlight { sync_id: current } = self.state() else {
            return;
        };
        if current != sync_id {
            return;
        }
        match settlement {
            Settlement::Converged(shared_heads) => {
                self.set_state(SlotState::Idle);
                self.last_synced = Some(shared_heads);
            }
            Settlement::Parked(exchange) => {
                self.set_state(SlotState::Parked(exchange));
            }
        }
    }

    /// Back to `Idle`, but only if the slot still carries `sync_id`, in flight or parked, so a
    /// glare winner that superseded us is never clobbered.
    fn release(&mut self, sync_id: &SyncId, release: Release) {
        let ours = match self.state() {
            SlotState::InFlight { sync_id: current } => current == sync_id,
            SlotState::Parked(active) => &active.sync_id == sync_id,
            SlotState::Idle => false,
        };
        if !ours {
            return;
        }
        self.set_state(SlotState::Idle);
        if let Release::Declined(fingerprint) = release {
            self.last_synced = Some(fingerprint);
        }
    }
}

/// A responder steps only a document it knows, on the schema major it speaks. Anything else is
/// declined for that document alone, which is how a newer peer's unknown document is handled.
fn admit(payload: SyncPayload) -> Result<(DocKind, SyncMessage), SyncResponse> {
    let Ok(kind) = payload.doc_kind.parse::<DocKind>() else {
        info!(kind = %payload.doc_kind, "Declining sync for unknown document kind");
        return Err(SyncResponse::DocumentVersionNotSupported {
            doc_kind: payload.doc_kind,
            sync_id: payload.sync_id,
        });
    };
    if payload.schema_major != kind.schema_major() {
        info!(kind = %payload.doc_kind, "Declining sync for incompatible schema major");
        return Err(SyncResponse::DocumentVersionNotSupported {
            doc_kind: payload.doc_kind,
            sync_id: payload.sync_id,
        });
    }
    Ok((
        kind,
        SyncMessage {
            sync_id: payload.sync_id,
            message: payload.message,
        },
    ))
}

impl GigaBrain {
    /// Run one automerge step for `(peer, kind)`. `incoming` is `None` to *initiate*, or the
    /// automerge content of a received `Sync` payload. Returns `Ok(None)` when nothing was done
    /// (dirty-gate/single-flight/glare bail), else the automerge output plus whether this step
    /// converged (both sides quiescent → the doc was persisted and the slot returned to `Idle`).
    async fn sync_advance(
        &self,
        peer: &PeerIdWrapper,
        kind: DocKind,
        incoming: Option<SyncMessage>,
    ) -> Result<Option<(SyncMessage, bool)>, DbError> {
        let (incoming_id, msg) = match incoming {
            Some(SyncMessage { sync_id, message }) => (Some(sync_id), message),
            None => (None, None),
        };
        let msg_none = msg.is_none();

        let claim = {
            let mut ephemeral = self.ephemeral().await;
            if ephemeral.sync_slots.len() > SYNC_SLOT_COUNT_WARN
                && !ephemeral.sync_slot_growth_reported
            {
                ephemeral.sync_slot_growth_reported = true;
                error!(
                    count = ephemeral.sync_slots.len(),
                    "Sync slot map has grown far beyond the number of devices that can plausibly \
                     exist; nothing prunes it, so this is a leak."
                );
            }
            ephemeral
                .sync_slots
                .entry((peer.clone(), kind))
                .or_default()
                .claim(incoming_id.as_ref(), msg_none, peer, kind)
        };

        // Phase 2 — off-lock: load the doc for a fresh exchange, then run one protocol step.
        let mut exchange = match claim {
            Claim::Bail => return Ok(None),
            Claim::Continue(exchange) => exchange,
            Claim::Fresh {
                sync_id,
                last_synced,
            } => {
                let mut doc = match self.load_sync_doc(kind).await {
                    Ok(doc) => doc,
                    Err(e) => {
                        self.abandon_sync_claim(peer, kind, &sync_id).await;
                        return Err(e);
                    }
                };
                // Dirty gate. `last_synced` is only ever carried for an initiation or for a
                // trailing empty message — a peer's real message is always serviced — so a
                // document that has not changed since we last confirmed it synced with this peer
                // neither initiates nor answers, which is what ends the exchange.
                if last_synced.as_deref() == Some(sync::doc_heads_fingerprint(&mut doc).as_slice())
                {
                    self.abandon_sync_claim(peer, kind, &sync_id).await;
                    return Ok(None);
                }
                Box::new(ActiveExchange {
                    sync_id,
                    state: SyncProtocolState::new(),
                    doc,
                })
            }
        };

        let outgoing = match sync::sync_step(&mut exchange.doc, &mut exchange.state, msg.as_deref())
        {
            Ok(outgoing) => outgoing,
            Err(e) => {
                self.abandon_sync_claim(peer, kind, &exchange.sync_id).await;
                return Err(sync_doc_err(e));
            }
        };
        let sync_id = exchange.sync_id.clone();
        // Convergence: both sides quiescent this round (we received `Sync{None}` and have nothing
        // to send). Persist once and return to `Idle`.
        let converged = msg_none && outgoing.is_none();

        let settlement = if converged {
            // The confirmed shared heads, not the merged doc heads, so a local write stays dirty.
            let shared_heads = sync::heads_fingerprint(&exchange.state.shared_heads);
            if let Err(e) = self.db.merge_sync_doc(kind, Box::new(exchange.doc)).await {
                self.abandon_sync_claim(peer, kind, &sync_id).await;
                return Err(e);
            }
            Settlement::Converged(shared_heads)
        } else {
            Settlement::Parked(exchange)
        };
        if let Some(slot) = self
            .ephemeral()
            .await
            .sync_slots
            .get_mut(&(peer.clone(), kind))
        {
            slot.settle(&sync_id, settlement);
        }
        Ok(Some((
            SyncMessage {
                sync_id,
                message: outgoing,
            },
            converged,
        )))
    }

    /// Test-only forwarder so `mod tests` (a sibling module) can drive the private protocol
    /// step directly, shuttling each side's output into the other to prove the exchange settles.
    #[cfg(test)]
    pub(crate) async fn sync_advance_for_test(
        &self,
        peer: &PeerIdWrapper,
        kind: DocKind,
        incoming: Option<SyncMessage>,
    ) -> Result<Option<(SyncMessage, bool)>, DbError> {
        self.sync_advance(peer, kind, incoming).await
    }

    /// Test-only forwarder for the responder entry point.
    #[cfg(test)]
    pub(crate) async fn handle_sync_receive_for_test(
        &self,
        contact_id: ContactId,
        peer: PeerIdWrapper,
        request: SyncRequest,
    ) -> Option<SyncResponse> {
        self.handle_sync_receive(contact_id, peer, request).await
    }

    /// Load `kind`'s document as a fresh `AutoCommit`.
    async fn load_sync_doc(&self, kind: DocKind) -> Result<automerge::AutoCommit, DbError> {
        sync::load(&self.db.get_sync_doc(kind).await?).map_err(sync_doc_err)
    }

    async fn release_claim(
        &self,
        peer: &PeerIdWrapper,
        kind: DocKind,
        sync_id: &SyncId,
        release: Release,
    ) {
        if let Some(slot) = self
            .ephemeral()
            .await
            .sync_slots
            .get_mut(&(peer.clone(), kind))
        {
            slot.release(sync_id, release);
        }
    }

    async fn abandon_sync_claim(&self, peer: &PeerIdWrapper, kind: DocKind, sync_id: &SyncId) {
        self.release_claim(peer, kind, sync_id, Release::Abandoned)
            .await;
    }

    /// A peer permanently can't handle this document (unknown kind / incompatible schema major).
    /// Record the current document version as synced with it so the dirty gate stops re-initiating
    /// — the peer will initiate the sync itself once it upgrades and understands the document.
    async fn mark_document_version_declined(
        &self,
        peer: &PeerIdWrapper,
        kind: DocKind,
        sync_id: &SyncId,
    ) {
        let release = match self.load_sync_doc(kind).await {
            Ok(mut doc) => Release::Declined(sync::doc_heads_fingerprint(&mut doc)),
            Err(e) => {
                error!("Failed to load {kind:?} to record declined sync version: {e:?}");
                Release::Abandoned
            }
        };
        self.release_claim(peer, kind, sync_id, release).await;
    }

    /// Side effects once a document converges. For contacts, reconcile the public-key cache and
    /// fetch keys for newly-learned contacts over the side channel. Every kind then pushes
    /// its full new state to the client so the UI reflects changes synced from another device.
    async fn post_convergence(&self, peer: &PeerIdWrapper, kind: DocKind) {
        if kind == DocKind::Contacts {
            if let Err(e) = self.db.prune_contact_key_cache().await {
                error!("Failed to prune contact key cache: {e:?}");
            }
            self.fetch_missing_contact_keys(peer).await;
        }
        if kind == DocKind::RemoteFilerequests {
            // A remote filerequest removed on another device leaves orphaned pending files here;
            // clean them up.
            if let Err(e) = self.db.prune_orphaned_pending_files().await {
                error!("Failed to prune orphaned pending files: {e:?}");
            }
        }
        self.push_converged_state(kind).await;
        // Re-sync if the document became dirty during this exchange — e.g. a local write that
        // raced it, or an initiation that was single-flight-bailed while this one was in flight.
        // The dirty gate inside `sync_advance` makes this a no-op (no message sent) once the doc
        // is stable, so it settles instead of looping.
        self.initiate_sync_if_dirty(peer, kind).await;
    }

    async fn fetch_missing_contact_keys(&self, peer: &PeerIdWrapper) {
        match self.db.keyless_synced_contacts().await {
            Ok(ids) if !ids.is_empty() => {
                self.network
                    .read()
                    .await
                    .fetch_contact_keys(peer.clone(), ids)
                    .await;
            }
            Ok(_) => {
                trace!("No keyless contacts found");
            }
            Err(e) => error!("Failed to list keyless synced contacts: {e:?}"),
        }
    }

    pub(super) async fn handle_store_fetched_contact_keys(&self, response: PubkeyFetchResponse) {
        let blob = match self.db.get_sync_doc(DocKind::Contacts).await {
            Ok(blob) => blob,
            Err(e) => {
                error!("Failed to load contacts document to verify fetched keys: {e:?}");
                return;
            }
        };
        let view: ContactsDoc = match sync::load(&blob)
            .map_err(sync_doc_err)
            .and_then(|doc| sync::hydrate(&doc).map_err(sync_doc_err))
        {
            Ok(view) => view,
            Err(e) => {
                error!("Failed to hydrate contacts document to verify fetched keys: {e:?}");
                return;
            }
        };
        for FetchedContactKeys { keys } in response.keys {
            let Some(entry) = view.contacts.get(&keys.id()) else {
                warn!("Ignoring fetched keys for a contact not in our synced document");
                continue;
            };
            let contact = Contact {
                name: entry.name.clone(),
                public_keys: keys,
            };
            match self.db.update_contact_projection(contact).await {
                // Not announced: this fills in key bytes for an entry sync already gave us.
                Ok(_write) => {}
                Err(e) => error!("Failed to cache fetched contact keys: {e:?}"),
            }
        }
    }

    fn sync_payload(kind: DocKind, msg: SyncMessage) -> SyncPayload {
        SyncPayload {
            doc_kind: kind.as_str().to_string(),
            schema_major: kind.schema_major(),
            message: msg.message,
            sync_id: msg.sync_id,
        }
    }

    async fn send_sync_request(&self, peer: &PeerIdWrapper, request: SyncRequest) {
        self.network
            .read()
            .await
            .send_sync_message(peer.clone(), request)
            .await;
    }

    pub(super) async fn initiate_sync(&self, peer: &PeerIdWrapper) {
        for kind in DocKind::ALL {
            self.initiate_sync_if_dirty(peer, kind).await;
        }
    }

    /// Publish a local write to everyone who cares: the attached clients, then the user's other
    /// devices. The client push is awaited so it is enqueued before the handler answers the
    /// request that caused the write.
    pub(super) async fn commit_local_write(&self, write: LocalDocWrite) {
        self.push_doc_change(write.change()).await;
        self.announce(write);
    }

    /// Push a local change to the user's other devices instead of waiting for the next sweep.
    ///
    /// Spawned, so a dial and a sync round trip stay off the critical path of the write that
    /// triggered it. Terminates because a merge cannot mint a [`LocalDocWrite`]. The dirty gate and
    /// single-flight in `sync_advance` also absorb bursts, so no debouncing is needed.
    fn announce(&self, write: LocalDocWrite) {
        let brain = self.clone();
        let shutdown = self.shutdown.clone();
        tokio::spawn(
            shutdown
                .run_until_cancelled_owned(async move { brain.announce_now(write).await })
                .in_current_span(),
        );
    }

    /// The body of [`Self::announce`], as an awaitable future.
    async fn announce_now(&self, write: LocalDocWrite) {
        let devices = match list_owned_devices(self.db.as_ref()).await {
            Ok(devices) => devices,
            Err(e) => {
                error!("Failed to list owned devices to announce a local change: {e:?}");
                return;
            }
        };
        if devices.is_empty() {
            return;
        }
        // The network answers with one `SyncDocWithPeer` per device it can actually reach
        self.network
            .read()
            .await
            .sync_docs_with_reachable(
                devices.into_iter().map(|(peer, _)| peer).collect(),
                write.kind(),
            )
            .await;
    }

    pub(super) async fn initiate_sync_if_dirty(&self, peer: &PeerIdWrapper, kind: DocKind) {
        match self.sync_advance(peer, kind, None).await {
            Ok(Some((outgoing, _converged))) => {
                self.send_sync_request(peer, SyncRequest::Sync(Self::sync_payload(kind, outgoing)))
                    .await;
            }
            // Dirty-gate/single-flight bail. Worth a line: it is the one outcome where a trigger
            // is dropped rather than acted on, so a change that fails to propagate shows up here.
            Ok(None) => trace!(?peer, ?kind, "Sync initiation declined"),
            Err(e) => error!("Failed to start sync for {kind:?}: {e:?}"),
        }
    }

    /// Responder: turn one inbound request into the single response frame the exchange requires.
    /// `None` = decline to answer at all.
    pub(super) async fn handle_sync_receive(
        &self,
        contact_id: ContactId,
        peer: PeerIdWrapper,
        request: SyncRequest,
    ) -> Option<SyncResponse> {
        let self_id = self.db.get_self_contact_id().await.ok();
        if self_id.as_ref() != Some(&contact_id) {
            warn!("Declining sync from a peer that isn't one of our devices");
            return None;
        }

        let payload = match request {
            // The initiator aborted: release our parked state for this exchange, then bare-ack.
            SyncRequest::InternalError { doc_kind, sync_id } => {
                if let Ok(kind) = doc_kind.parse::<DocKind>() {
                    self.abandon_sync_claim(&peer, kind, &sync_id).await;
                }
                return Some(SyncResponse::NoMoreToSay);
            }
            SyncRequest::Sync(payload) => payload,
        };

        let (kind, incoming) = match admit(payload) {
            Ok(admitted) => admitted,
            Err(declined) => return Some(declined),
        };
        let sync_id = incoming.sync_id.clone();
        match self.sync_advance(&peer, kind, Some(incoming)).await {
            Ok(Some((outgoing, converged))) => {
                if converged {
                    self.post_convergence(&peer, kind).await;
                }
                Some(SyncResponse::Sync(Self::sync_payload(kind, outgoing)))
            }
            Ok(None) => Some(SyncResponse::NoMoreToSay),
            Err(e) => {
                error!("Sync step (receive) failed for {kind:?}: {e:?}");
                Some(SyncResponse::InternalError {
                    doc_kind: kind.as_str().to_string(),
                    sync_id,
                })
            }
        }
    }

    pub(super) async fn handle_sync_reply(&self, peer: PeerIdWrapper, reply: SyncResponse) {
        match reply {
            SyncResponse::NoMoreToSay => {}
            SyncResponse::DocumentVersionNotSupported { doc_kind, sync_id } => {
                warn!(kind = %doc_kind, "Peer can't handle this document version; suppressing further sync attempts");
                if let Ok(kind) = doc_kind.parse::<DocKind>() {
                    self.mark_document_version_declined(&peer, kind, &sync_id)
                        .await;
                }
            }
            SyncResponse::InternalError { doc_kind, sync_id } => {
                warn!(kind = %doc_kind, "Peer hit an internal error; abandoning this document's exchange");
                if let Ok(kind) = doc_kind.parse::<DocKind>() {
                    self.abandon_sync_claim(&peer, kind, &sync_id).await;
                }
            }
            SyncResponse::Sync(payload) => {
                let Ok(kind) = payload.doc_kind.parse::<DocKind>() else {
                    return;
                };
                let sync_id = payload.sync_id.clone();
                let incoming = SyncMessage {
                    sync_id: payload.sync_id,
                    message: payload.message,
                };
                match self.sync_advance(&peer, kind, Some(incoming)).await {
                    Ok(Some((outgoing, converged))) => {
                        // Answer even when converged: the responder reaches quiescence on the step
                        // that applies our last changes, but its own incoming was non-empty there,
                        // so only this terminal empty makes it converge and persist. Withholding it
                        // leaves the peer's merge in memory and never written.
                        self.send_sync_request(
                            &peer,
                            SyncRequest::Sync(Self::sync_payload(kind, outgoing)),
                        )
                        .await;
                        if converged {
                            self.post_convergence(&peer, kind).await;
                        }
                    }
                    // Glare-loser / duplicate — drop silently.
                    Ok(None) => {}
                    Err(e) => {
                        error!("Sync step (reply) failed for {kind:?}: {e:?}");
                        // Our step failed: release the responder's parked state.
                        self.send_sync_request(
                            &peer,
                            SyncRequest::InternalError {
                                doc_kind: kind.as_str().to_string(),
                                sync_id,
                            },
                        )
                        .await;
                    }
                }
            }
        }
    }

    pub(super) async fn ensure_own_device_registered(&self) {
        match self.network.read().await.get_node_info().await {
            Ok(info) => match ensure_own_device_registered(self.db.as_ref(), info.peer_id).await {
                Ok(write) => self.commit_local_write(write).await,
                Err(e) => error!("Failed to self-register this device: {e:?}"),
            },
            Err(e) => error!("Failed to get node info for self-registration: {e:?}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KIND: DocKind = DocKind::Contacts;

    fn peer() -> PeerIdWrapper {
        PeerIdWrapper::for_test()
    }

    fn exchange(sync_id: &SyncId) -> Box<ActiveExchange> {
        Box::new(ActiveExchange {
            sync_id: sync_id.clone(),
            state: SyncProtocolState::new(),
            doc: sync::genesis(KIND).expect("a genesis document"),
        })
    }

    /// Two ids, the lower one first.
    fn ordered_ids() -> (SyncId, SyncId) {
        let (a, b) = (SyncId::new(), SyncId::new());
        if a < b { (a, b) } else { (b, a) }
    }

    fn in_flight(sync_id: &SyncId) -> ExchangeSlot {
        let mut slot = ExchangeSlot::default();
        slot.set_state(SlotState::InFlight {
            sync_id: sync_id.clone(),
        });
        slot
    }

    fn parked(sync_id: &SyncId) -> ExchangeSlot {
        let mut slot = ExchangeSlot::default();
        slot.set_state(SlotState::Parked(exchange(sync_id)));
        slot
    }

    fn carries(slot: &ExchangeSlot, sync_id: &SyncId) -> bool {
        match slot.state() {
            SlotState::InFlight { sync_id: current } => current == sync_id,
            SlotState::Parked(active) => active.sync_id == *sync_id,
            SlotState::Idle => false,
        }
    }

    #[tokio::test]
    async fn an_idle_slot_initiates_with_its_dirty_gate() {
        let mut slot = ExchangeSlot {
            last_synced: Some(vec![1]),
            ..Default::default()
        };
        let Claim::Fresh {
            sync_id,
            last_synced,
        } = slot.claim(None, true, &peer(), KIND)
        else {
            panic!("an idle slot to initiate");
        };
        assert_eq!(last_synced, Some(vec![1]));
        assert!(carries(&slot, &sync_id));
    }

    #[tokio::test]
    async fn a_busy_slot_does_not_initiate() {
        let (ours, _) = ordered_ids();
        let mut slot = in_flight(&ours);
        assert!(matches!(slot.claim(None, true, &peer(), KIND), Claim::Bail));
        assert!(carries(&slot, &ours));
    }

    #[tokio::test]
    async fn an_idle_slot_gates_only_an_empty_incoming_message() {
        let theirs = SyncId::new();
        for (empty_message, expected) in [(true, Some(vec![1])), (false, None)] {
            let mut slot = ExchangeSlot {
                last_synced: Some(vec![1]),
                ..Default::default()
            };
            let Claim::Fresh { last_synced, .. } =
                slot.claim(Some(&theirs), empty_message, &peer(), KIND)
            else {
                panic!("an idle slot to take the peer's exchange");
            };
            assert_eq!(last_synced, expected);
            assert!(carries(&slot, &theirs));
        }
    }

    #[tokio::test]
    async fn a_parked_exchange_continues_with_its_own_id() {
        let ours = SyncId::new();
        let mut slot = parked(&ours);
        let Claim::Continue(active) = slot.claim(Some(&ours), false, &peer(), KIND) else {
            panic!("the parked exchange to continue");
        };
        assert_eq!(active.sync_id, ours);
        assert!(matches!(slot.state(), SlotState::InFlight { sync_id } if *sync_id == ours));
    }

    #[tokio::test]
    async fn in_glare_the_lower_id_wins_on_both_ends() {
        let (lower, higher) = ordered_ids();
        for mut slot in [parked(&higher), in_flight(&higher)] {
            assert!(matches!(
                slot.claim(Some(&lower), false, &peer(), KIND),
                Claim::Fresh {
                    last_synced: None,
                    ..
                }
            ));
            assert!(carries(&slot, &lower));
        }
        for mut slot in [parked(&lower), in_flight(&lower)] {
            assert!(matches!(
                slot.claim(Some(&higher), false, &peer(), KIND),
                Claim::Bail
            ));
            assert!(carries(&slot, &lower));
        }
    }

    #[tokio::test]
    async fn a_duplicate_of_the_exchange_in_flight_bails() {
        let ours = SyncId::new();
        let mut slot = in_flight(&ours);
        assert!(matches!(
            slot.claim(Some(&ours), false, &peer(), KIND),
            Claim::Bail
        ));
        assert!(carries(&slot, &ours));
    }

    #[tokio::test]
    async fn settling_commits_only_while_the_exchange_is_still_ours() {
        let (lower, higher) = ordered_ids();
        let mut slot = in_flight(&higher);
        slot.settle(&higher, Settlement::Converged(vec![7]));
        assert!(matches!(slot.state(), SlotState::Idle));
        assert_eq!(slot.last_synced, Some(vec![7]));

        let mut slot = in_flight(&lower);
        slot.settle(&higher, Settlement::Parked(exchange(&higher)));
        assert!(carries(&slot, &lower));
        slot.settle(&higher, Settlement::Converged(vec![7]));
        assert!(carries(&slot, &lower));
        assert_eq!(slot.last_synced, None);
    }

    #[tokio::test]
    async fn releasing_leaves_a_superseding_exchange_alone() {
        let (lower, higher) = ordered_ids();
        let mut slot = in_flight(&lower);
        slot.release(&higher, Release::Declined(vec![7]));
        assert!(carries(&slot, &lower));
        assert_eq!(slot.last_synced, None);

        for mut slot in [in_flight(&lower), parked(&lower)] {
            slot.release(&lower, Release::Declined(vec![7]));
            assert!(matches!(slot.state(), SlotState::Idle));
            assert_eq!(slot.last_synced, Some(vec![7]));
        }

        let mut slot = in_flight(&lower);
        slot.release(&lower, Release::Abandoned);
        assert!(matches!(slot.state(), SlotState::Idle));
        assert_eq!(slot.last_synced, None);
    }

    fn payload(doc_kind: &str, schema_major: u32) -> SyncPayload {
        SyncPayload {
            doc_kind: doc_kind.into(),
            schema_major,
            message: None,
            sync_id: SyncId::new(),
        }
    }

    #[test]
    fn only_a_known_document_on_our_schema_major_is_admitted() {
        assert!(matches!(
            admit(payload(KIND.as_str(), KIND.schema_major())),
            Ok((DocKind::Contacts, _))
        ));
        assert!(matches!(
            admit(payload("a_document_from_the_future", 1)),
            Err(SyncResponse::DocumentVersionNotSupported { .. })
        ));
        assert!(matches!(
            admit(payload(KIND.as_str(), KIND.schema_major() + 1)),
            Err(SyncResponse::DocumentVersionNotSupported { .. })
        ));
    }
}
