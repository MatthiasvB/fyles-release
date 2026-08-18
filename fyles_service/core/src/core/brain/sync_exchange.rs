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
use crate::library::wire::api::ServerMessage;

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

        enum Claim {
            Continue(Box<ActiveExchange>),
            Fresh {
                sync_id: SyncId,
                last_synced: Option<Vec<u8>>,
            },
            Bail,
        }
        /// Decline to act, reporting the slot first if it has been blocking for far too long.
        ///
        /// Every bail is a piece of work we chose not to do because the slot was occupied, which
        /// makes this exactly the point where a wedged slot does its damage. Deliberately reports
        /// rather than reclaims: a slot that outlives every legitimate exit means one of those
        /// exits was missed, and repairing it here would hide the defect.
        fn bail(slot: &mut ExchangeSlot, peer: &PeerIdWrapper, kind: DocKind) -> Claim {
            if slot.should_report_stall(SYNC_SLOT_STALL_AFTER) {
                error!(
                    ?peer,
                    ?kind,
                    state_age = ?slot.state_age(),
                    "Sync slot has been non-idle far longer than any exchange can last; sync for \
                     this document is blocked. An exchange terminated without releasing its slot."
                );
            }
            Claim::Bail
        }

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
            let slot = ephemeral
                .sync_slots
                .entry((peer.clone(), kind))
                .or_default();
            match &incoming_id {
                // Initiation: single-flight; the dirty gate needs the loaded doc, so carry
                // `last_synced` and check it in phase 2.
                None => {
                    if matches!(slot.state(), SlotState::Idle) {
                        let sync_id = SyncId::new();
                        let last_synced = slot.last_synced.clone();
                        slot.set_state(SlotState::InFlight {
                            sync_id: sync_id.clone(),
                        });
                        Claim::Fresh {
                            sync_id,
                            last_synced,
                        }
                    } else {
                        bail(slot, peer, kind)
                    }
                }
                Some(in_id) => match slot.state() {
                    SlotState::Idle => {
                        let last_synced = slot.last_synced.clone();
                        slot.set_state(SlotState::InFlight {
                            sync_id: in_id.clone(),
                        });
                        Claim::Fresh {
                            sync_id: in_id.clone(),
                            // Carried only for an empty message, which opens no exchange of its
                            // own; a real message is always serviced.
                            last_synced: msg_none.then_some(last_synced).flatten(),
                        }
                    }
                    SlotState::Parked(active) if &active.sync_id == in_id => {
                        let SlotState::Parked(active) = slot.set_state(SlotState::InFlight {
                            sync_id: in_id.clone(),
                        }) else {
                            unreachable!("slot was Parked in the arm guard")
                        };
                        Claim::Continue(active)
                    }
                    // Glare (parked or in-flight): the lower `sync_id` wins; both ends compare the
                    // same pair and agree. A losing/duplicate id leaves the slot as it was.
                    SlotState::Parked(active) => {
                        if in_id < &active.sync_id {
                            warn!(?peer, ?kind, ours = %active.sync_id, theirs = %in_id, "sync glare: adopting peer's exchange");
                            slot.set_state(SlotState::InFlight {
                                sync_id: in_id.clone(),
                            });
                            Claim::Fresh {
                                sync_id: in_id.clone(),
                                last_synced: None,
                            }
                        } else {
                            warn!(?peer, ?kind, ours = %active.sync_id, theirs = %in_id, "sync glare: keeping ours, rejecting peer's");
                            bail(slot, peer, kind)
                        }
                    }
                    SlotState::InFlight { sync_id: other } => {
                        if other != in_id && in_id < other {
                            warn!(?peer, ?kind, ours = %other, theirs = %in_id, "sync glare: superseding in-flight exchange");
                            slot.set_state(SlotState::InFlight {
                                sync_id: in_id.clone(),
                            });
                            Claim::Fresh {
                                sync_id: in_id.clone(),
                                last_synced: None,
                            }
                        } else {
                            bail(slot, peer, kind)
                        }
                    }
                },
            }
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
                if last_synced.as_deref()
                    == Some(sync::doc_heads_fingerprint(&mut doc).as_slice())
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

        if converged {
            let shared_heads = sync::heads_fingerprint(&exchange.state.shared_heads);
            let mut from = exchange.doc;
            if let Err(e) = self
                .db
                .mutate_sync_doc(
                    kind,
                    Box::new(move |blob| {
                        let mut stored = sync::load(&blob).map_err(sync_doc_err)?;
                        sync::merge(&mut stored, &mut from).map_err(sync_doc_err)?;
                        Ok(stored.save())
                    }),
                )
                .await
            {
                self.abandon_sync_claim(peer, kind, &sync_id).await;
                return Err(e);
            }
            // Commit iff we still hold the slot (else a glare winner superseded us). Record the
            // confirmed shared heads, not the merged doc heads — a local write stays dirty.
            let mut ephemeral = self.ephemeral().await;
            if let Some(slot) = ephemeral.sync_slots.get_mut(&(peer.clone(), kind))
                && matches!(slot.state(), SlotState::InFlight { sync_id: s } if *s == sync_id)
            {
                slot.set_state(SlotState::Idle);
                slot.last_synced = Some(shared_heads);
            }
        } else {
            // Park for the next step — unless a glare winner superseded us, then drop our exchange.
            let mut ephemeral = self.ephemeral().await;
            if let Some(slot) = ephemeral.sync_slots.get_mut(&(peer.clone(), kind))
                && matches!(slot.state(), SlotState::InFlight { sync_id: s } if *s == sync_id)
            {
                slot.set_state(SlotState::Parked(exchange));
            }
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

    /// Release our claim on `(peer, kind)` back to `Idle` — but only if the slot still carries our
    /// `sync_id`, so we never clobber a glare winner that superseded us. The slot may be `InFlight`
    /// (mid-step) or `Parked` (parked awaiting a reply); both are ours to release. When
    /// `last_synced` is `Some`, also record it as the dirty-gate high-water mark.
    async fn release_claim(
        &self,
        peer: &PeerIdWrapper,
        kind: DocKind,
        sync_id: &SyncId,
        last_synced: Option<Vec<u8>>,
    ) {
        let mut ephemeral = self.ephemeral().await;
        if let Some(slot) = ephemeral.sync_slots.get_mut(&(peer.clone(), kind)) {
            let ours = match slot.state() {
                SlotState::InFlight { sync_id: s } => s == sync_id,
                SlotState::Parked(active) => &active.sync_id == sync_id,
                SlotState::Idle => false,
            };
            if ours {
                slot.set_state(SlotState::Idle);
                if let Some(fp) = last_synced {
                    slot.last_synced = Some(fp);
                }
            }
        }
    }

    async fn abandon_sync_claim(&self, peer: &PeerIdWrapper, kind: DocKind, sync_id: &SyncId) {
        self.release_claim(peer, kind, sync_id, None).await;
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
        let last_synced = match self.load_sync_doc(kind).await {
            Ok(mut doc) => Some(sync::doc_heads_fingerprint(&mut doc)),
            Err(e) => {
                error!("Failed to load {kind:?} to record declined sync version: {e:?}");
                None
            }
        };
        self.release_claim(peer, kind, sync_id, last_synced).await;
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
        self.push_synced_state(kind).await;
        // Re-sync if the document became dirty during this exchange — e.g. a local write that
        // raced it, or an initiation that was single-flight-bailed while this one was in flight.
        // The dirty gate inside `sync_advance` makes this a no-op (no message sent) once the doc
        // is stable, so it settles instead of looping.
        self.initiate_sync_if_dirty(peer, kind).await;
    }

    /// Push the full current state for `kind` to the client, so the UI reflects a change synced in
    /// from another of the user's devices without the client having to poll or refetch.
    async fn push_synced_state(&self, kind: DocKind) {
        let message = match kind {
            DocKind::Contacts => match self.db.get_contacts().await {
                Ok(contacts) => ServerMessage::contacts_changed(contacts),
                Err(e) => {
                    error!("Failed to load contacts to push after sync: {e:?}");
                    return;
                }
            },
            DocKind::RemoteFilerequests => {
                let remote_frs = match self.db.get_all_remote_filerequests().await {
                    Ok(v) => v,
                    Err(e) => {
                        error!("Failed to load remote filerequests to push after sync: {e:?}");
                        return;
                    }
                };
                let pending = match self.db.get_all_pending_files().await {
                    Ok(v) => v,
                    Err(e) => {
                        error!("Failed to load pending files to push after sync: {e:?}");
                        return;
                    }
                };
                ServerMessage::remote_filerequests_changed(remote_frs, pending)
            }
            DocKind::Devices => match list_owned_devices(self.db.as_ref()).await {
                Ok(devices) => ServerMessage::owned_devices_changed(devices),
                Err(e) => {
                    error!("Failed to load owned devices to push after sync: {e:?}");
                    return;
                }
            },
        };
        if let Err(e) = self.client_push_sender.send(message).await {
            error!("Failed to push synced state to client: {e:?}");
        }
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
        for FetchedContactKeys { contact_id, keys } in response.keys {
            let Some(entry) = view.contacts.get(&contact_id) else {
                warn!("Ignoring fetched keys for a contact not in our synced document");
                continue;
            };
            if sync::fingerprint_of(&keys) != entry.key_fingerprint {
                warn!("Ignoring fetched keys whose fingerprint does not match the synced contact");
                continue;
            }
            let contact = Contact {
                id: contact_id,
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

    /// Push a local change to the user's other devices instead of waiting for the next sweep.
    ///
    /// Spawned, so a dial and a sync round trip stay off the critical path of the write that
    /// triggered it. Terminates because a merge cannot mint a [`LocalDocWrite`]; the dirty gate and
    /// single-flight in `sync_advance` also absorb bursts, so no debouncing is needed.
    pub(super) fn announce(&self, write: LocalDocWrite) {
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
            .sync_docs_with_reachable(devices.into_iter().map(|(peer, _)| peer).collect(), write.kind())
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

        // Unknown document kind or incompatible schema major — decline *this* document only.
        // (This is how a newer peer's document unknown to us is handled.)
        let Ok(kind) = payload.doc_kind.parse::<DocKind>() else {
            info!(kind = %payload.doc_kind, "Declining sync for unknown document kind");
            return Some(SyncResponse::DocumentVersionNotSupported {
                doc_kind: payload.doc_kind,
                sync_id: payload.sync_id,
            });
        };
        if payload.schema_major != kind.schema_major() {
            info!(kind = %payload.doc_kind, "Declining sync for incompatible schema major");
            return Some(SyncResponse::DocumentVersionNotSupported {
                doc_kind: payload.doc_kind,
                sync_id: payload.sync_id,
            });
        }

        let sync_id = payload.sync_id.clone();
        let incoming = SyncMessage {
            sync_id: payload.sync_id,
            message: payload.message,
        };
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
            Ok(info) => {
                match ensure_own_device_registered(self.db.as_ref(), info.peer_id).await {
                    Ok(write) => self.announce(write),
                    Err(e) => error!("Failed to self-register this device: {e:?}"),
                }
            }
            Err(e) => error!("Failed to get node info for self-registration: {e:?}"),
        }
    }
}
