use crate::data_structures::scoped_expiry_map::ScopedExpiryMap;
use crate::event_loop::with_swarm::{GetInnerEventSender, WithSwarm};
use crate::event_loop::{EventLoopSender, LocalNetworkSwarm};
use crate::send_receive_traits::session::Session;
use crate::sessions::{
    InitiatorSessionCreation, InitiatorSessionEstablishmentError,
    ResponderSessionEstablishmentError, SessionBuildStore, SessionEstablishmentError, SessionStore,
};
use crate::utils::{W, Wrapper};
use async_trait::async_trait;
use crypto::ContactKeys;
use crypto::session::{
    AuthWireRequest, AuthWireResponse, KeyExchangeResponseMessage, Session as CryptoSession,
    SessionFromRequestError, SessionInitiationRequest, SessionInitiatorSecrets,
};
use fyles_core::core::brain::action::BrainAction;
use fyles_core::core::brain::action_p2p::NetworkNodeAction;
use fyles_core::core::brain::types::BrainRequest;
use fyles_core::core::domain_models::ContactId;
use fyles_core::library::util::util::TimeoutLock;
use itertools::Itertools;
use libp2p::PeerId;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use std::time::SystemTime;
use tap::Pipe;
use tokio::sync::{Mutex, RwLock, mpsc};
use tracing::{debug, error, trace, warn};

use crate::constants::{
    EXPIRED_SESSION_IDLE, EXPIRED_SESSION_MAX, EXPIRED_SESSION_SCOPE,
    SESSION_ESTABLISHMENT_TIMEOUT, SESSION_LIFETIME,
};

#[async_trait]
pub trait GetSession {
    type Session: Session;
    type NodeId;
    type ContactId;
    type Error;

    async fn get_or_establish_session(
        &self,
        node_id: Self::NodeId,
        for_contact_id: Option<Self::ContactId>,
    ) -> Result<Self::Session, Arc<Self::Error>>;
    async fn get_established_session(
        &self,
        node_id: Self::NodeId,
        use_for_send: bool,
    ) -> Option<Self::Session>;

    /// Returns all recent (probably few seconds) sessions that existed with [`node_id`]. Use this
    /// if you need to decrypt an incoming request and suspect that either there temporarily isn't a session (because
    /// a new one is currently negotiated) or the response may have been sent with a previous session.
    ///
    /// Implementations should return an iterator that has only sessions that are not expired for reception and
    /// the current session, if one exists, should be in the first position.
    async fn get_recent_receive_sessions(&self, node_id: &Self::NodeId) -> Vec<Self::Session>;

    /// Moves the active session to the expired-session cache, where late messages can still be
    /// decrypted. Does not affect sessions that are still being established.
    async fn retire_session(&self, node_id: Self::NodeId);
    /// Unlike [`Self::retire_session`], keeps no copy for late messages. A peer still sending on
    /// its own copy is answered `MissingSession`, and so establishes a new session.
    async fn discard_session(&self, node_id: Self::NodeId);
}

pub struct SessionManager<NodeId: Hash + Eq + Send, ContactId: Clone, Swarm: LocalNetworkSwarm> {
    session_store: Mutex<SessionStore<NodeId>>,
    session_build_store: Mutex<SessionBuildStore<NodeId>>,
    brain_action_sender: mpsc::Sender<BrainAction>,
    event_loop_sender: EventLoopSender<Swarm>,
    self_contact_id: RwLock<ContactId>,
    keys: RwLock<ContactKeys>,
    expired_session_cache: ScopedExpiryMap<NodeId, W<Arc<CryptoSession>>>,
}

impl<NodeId: Hash + Eq + Clone + Send + Debug, Swarm: LocalNetworkSwarm>
    SessionManager<NodeId, ContactId, Swarm>
{
    pub fn new(
        brain_action_sender: mpsc::Sender<BrainAction>,
        event_loop_sender: EventLoopSender<Swarm>,
        contact_id: ContactId,
        contact_keys: ContactKeys,
    ) -> Self {
        Self {
            session_store: Default::default(),
            session_build_store: Default::default(),
            brain_action_sender,
            event_loop_sender,
            self_contact_id: contact_id.into(),
            keys: contact_keys.into(),
            expired_session_cache: ScopedExpiryMap::new(
                EXPIRED_SESSION_IDLE,
                EXPIRED_SESSION_SCOPE,
                EXPIRED_SESSION_MAX,
            ),
        }
    }

    /// Our own contact id (shared across our devices). Used e.g. to bind an outgoing
    /// sync session to our own identity so the peer authenticates us as an owned device.
    pub async fn self_contact_id(&self) -> ContactId {
        self.self_contact_id.read().await.clone()
    }

    pub async fn update_identity(&self, contact_id: ContactId, keys: ContactKeys) {
        *self.self_contact_id.write().await = contact_id;
        *self.keys.write().await = keys;
        // FIXME: This is not 100% correct. Theoretically (probably not a real-live concern)
        // a new session could be created after the session build store is cleared but before
        // the session store is cleared, leading to a session for the new identity being removed.
        // The timing would have to be truly insane and the app could probably recover in most cases,
        // but still not ideal.
        self.session_build_store.timeout_lock().await.store.clear();
        self.session_store.timeout_lock().await.store.clear();
    }

    pub async fn handle_session_establishment_request(
        &self,
        peer: NodeId,
        request: AuthWireRequest<Box<SessionInitiationRequest>>,
    ) -> Result<AuthWireResponse<Box<KeyExchangeResponseMessage>>, ResponderSessionEstablishmentError>
    {
        debug!(?peer, "Received session establishment request");
        trace!("Optimistically verifying other end");
        let claimed_contact_id: ContactId = request
            .claimed_signed_by()
            .parse()
            .map_err(|_| ResponderSessionEstablishmentError::MalformedContactId)?;
        let (brain_request, keys_response) =
            BrainRequest::with_receiver(claimed_contact_id.clone());
        trace!("Requesting public keys to verify other end during request");
        self.brain_action_sender
            .send(BrainAction::NetworkNode(
                NetworkNodeAction::GetContactPublicKeys(brain_request),
            ))
            .await
            .expect("Brain action sender to be alive");
        let maybe_keys = keys_response.await.expect("Keys response to be received");
        let self_contact_id = self.self_contact_id.read().await.clone();
        trace!("Signing session response for peer {peer:?} with self contact id {self_contact_id}");
        let result = request.to_session_and_response(
            match maybe_keys {
                Some(ref keys) => Some((claimed_contact_id.clone(), keys)),
                None => None,
            },
            self_contact_id,
            &mut self.keys.write().await.private,
            SESSION_LIFETIME,
        );
        match result {
            Err(SessionFromRequestError::SignatureVerification(e)) => {
                error!("Initiator signature verification failed: {e:?}");
                Err(
                    ResponderSessionEstablishmentError::ContactVerificationFailed(
                        claimed_contact_id,
                    ),
                )
            }
            Err(SessionFromRequestError::Signing(e)) => {
                error!("Failed to sign the key exchange response: {e:?}");
                Err(ResponderSessionEstablishmentError::SharedSecretGenerationFailed)
            }
            Err(SessionFromRequestError::Serialize(e)) => {
                error!("Failed to serialize the handshake transcript: {e:?}");
                Err(ResponderSessionEstablishmentError::Internal(e.to_string()))
            }
            Ok((session, response)) => {
                debug!(
                    ?peer,
                    "Storing session after receiving session establishment request"
                );
                let mut session_store_lock = self.session_store.timeout_lock().await;
                let old_session = session_store_lock
                    .store
                    .insert(peer.clone(), Wrapper(session.into()));
                if let Some(session) = old_session {
                    trace!("Retaining old session in the cache");
                    self.expired_session_cache.push(peer, session).await;
                }
                drop(session_store_lock);
                trace!("Initiating return of signed response");
                Ok(response)
            }
        }
    }

    pub async fn handle_session_establishment_response(
        &self,
        peer: NodeId,
        response: Result<
            AuthWireResponse<Box<KeyExchangeResponseMessage>>,
            ResponderSessionEstablishmentError,
        >,
    ) {
        debug!(?peer, "Received session establishment response");
        match response {
            Err(e) => {
                warn!("Session establishment error: {e:?}");
                match self
                    .session_build_store
                    .timeout_lock()
                    .await
                    .store
                    .remove(&peer)
                {
                    Some(session_builder) => {
                        error!("Session establishment error: {e:?}");
                        session_builder.cancel(e.into());
                    }
                    None => error!(
                        "Got session establishment response but corresponding session build is missing"
                    ),
                }
            }
            Ok(response) => {
                trace!(
                    "Response is Ok. Authenticated: {}",
                    response.payload_unverified().is_authenticated
                );
                let session_builder = match self
                    .session_build_store
                    .timeout_lock()
                    .await
                    .store
                    .remove(&peer)
                {
                    None => {
                        error!("No session builder found for peer {peer:?}");
                        return;
                    }
                    Some(session_builder) => session_builder,
                };
                trace!("Session builder found for peer {peer:?}");
                let Ok(claimed_contact_id) = response.claimed_signed_by().parse::<ContactId>()
                else {
                    error!(peer=?peer, "Session response claims a malformed contact id");
                    return;
                };
                let (request, brain_response) = BrainRequest::with_receiver(claimed_contact_id);
                self.brain_action_sender
                    .send(BrainAction::NetworkNode(
                        NetworkNodeAction::GetContactPublicKeys(request),
                    ))
                    .await
                    .expect("Sending to work");
                let maybe_contact_public_keys =
                    brain_response.await.expect("Sender not to be dropped");
                let session = session_builder.to_session(response, maybe_contact_public_keys);
                match session {
                    Err(e) => error!("Error during session creation: {e:?}"),
                    Ok(session) => {
                        debug!(
                            ?peer,
                            ?session,
                            "Session successfully created. Inserting into session store"
                        );
                        let mut session_store_lock = self.session_store.timeout_lock().await;
                        let old_session = session_store_lock
                            .store
                            .insert(peer.clone(), session.into());
                        if let Some(session) = old_session {
                            trace!("Retaining old session in the cache");
                            self.expired_session_cache.push(peer, session).await;
                        } else {
                            trace!("Found no previous session to retain");
                        }
                        drop(session_store_lock);
                    }
                }
            }
        };
    }

    /// A responder stores its session before answering, so its message can overtake the
    /// establishment response we are still waiting for. Needs no timeout: every build entry ends in
    /// a session, an error, or the initiator's timeout dropping the builder.
    async fn session_from_pending_build(&self, peer: &NodeId) -> Option<W<Arc<CryptoSession>>> {
        let mut pending_build = {
            let session_build_store = self.session_build_store.timeout_lock().await;
            session_build_store
                .store
                .get(peer)?
                .pipe(|build| build.get_session_receiver())
        };
        trace!(?peer, "Waiting for the session being built for this peer");
        if let Ok(Ok(session)) = pending_build.recv().await {
            return Some(session.into());
        }
        // Subscribing after the broadcast already fired receives nothing, so the store is the only
        // way to tell "never established" from "established just before we looked".
        self.session_store
            .timeout_lock()
            .await
            .store
            .get(peer)
            .cloned()
    }

    pub async fn handle_session_establishment_failure(&self, peer: NodeId) {
        if let Some(session_builder) = self
            .session_build_store
            .timeout_lock()
            .await
            .store
            .remove(&peer)
        {
            session_builder.cancel(SessionEstablishmentError::Initiator(
                InitiatorSessionEstablishmentError::NoResponse,
            ));
        }
    }
}

impl<NodeId: Hash + Eq + Send, ContactId: Clone, Swarm: LocalNetworkSwarm>
    GetInnerEventSender<Swarm> for SessionManager<NodeId, ContactId, Swarm>
{
    fn get_inner_event_sender(&self) -> &EventLoopSender<Swarm> {
        &self.event_loop_sender
    }
}

#[async_trait]
impl<Swarm: LocalNetworkSwarm + 'static> GetSession for SessionManager<PeerId, ContactId, Swarm> {
    type Session = W<Arc<CryptoSession>>;
    type NodeId = PeerId;
    type ContactId = ContactId;
    type Error = SessionEstablishmentError;

    async fn get_or_establish_session(
        &self,
        node_id: Self::NodeId,
        for_contact_id: Option<Self::ContactId>,
    ) -> Result<Self::Session, Arc<Self::Error>> {
        // we attempt this _exactly_ twice. The reason we may need to repeat is that there is a tiny chance for a
        // use after check issue where we see that a session build is in progress but miss receiving the resulting session
        // because we subscribe too late. In that case, the second iteration should find the session in the session store.
        // If that is not the case, session establishment must have failed. We will try once again and give up if that doesn't
        // work, either. (There is a small chance we might succeed in case our authentication requirement is more lax. Or
        // random things fixed themselves)
        for attempt in 0..2 {
            trace!("Attempt no {attempt}");
            if let Some(existing_session) = self.get_established_session(node_id, true).await {
                trace!("Found existing session");
                return Ok(existing_session);
            }
            debug!(?node_id, "Found no existing session");

            let mut session_build_store_lock = self.session_build_store.timeout_lock().await;
            {
                if let Some(session_build) = session_build_store_lock.store.get(&node_id) {
                    trace!("Found a session in progress of being established in the build store");
                    let mut session_receiver = session_build.get_session_receiver();
                    drop(session_build_store_lock);
                    match session_receiver.recv().await {
                        Err(_) => {
                            trace!("Late subscribe to session build. Try again");
                            continue;
                        }
                        Ok(res) => match res {
                            Err(_) => {
                                error!("Received an error from session establishment");
                                // TODO: Only retry if it makes sense given the type of error (e.g. if the cause
                                // is that other end couldn't authenticate me, it ain't gonna work the second time)
                                continue;
                            }
                            Ok(session) => {
                                trace!("Received a session from session establishment");
                                return Ok(session.into());
                            }
                        },
                    }
                }
            }

            trace!("No session build in progress");

            // there was no session build in progress, or it yielded a session with insufficient requirements

            {
                let session_initiator_secrets = Box::new(SessionInitiatorSecrets::new());
                let request = {
                    let self_contact_id = self.self_contact_id.read().await.clone();
                    let mut signing_keys = self.keys.write().await;
                    SessionInitiationRequest::new(
                        Box::new(session_initiator_secrets.public),
                        for_contact_id.as_ref(),
                    )
                    .sign(&mut signing_keys.private, self_contact_id)
                    .map_err(|_| {
                        Arc::new(InitiatorSessionEstablishmentError::SigningError.into())
                    })?
                };
                let request_transcript = request.canonical_payload_bytes().map_err(|e| {
                    Arc::new(InitiatorSessionEstablishmentError::Internal(e.to_string()).into())
                })?;
                let session_creation = InitiatorSessionCreation::new(
                    session_initiator_secrets.private,
                    request_transcript,
                );
                let mut response_receiver = session_creation.get_session_receiver();
                session_build_store_lock
                    .store
                    .insert(node_id, session_creation);
                drop(session_build_store_lock);
                debug!(?node_id, "Sending session establishment request");
                self.with_swarm(move |swarm| {
                    swarm
                        .get_session_establishment_behaviour()
                        .send_request(&node_id, request);
                });
                let response =
                    tokio::time::timeout(SESSION_ESTABLISHMENT_TIMEOUT, response_receiver.recv())
                        .await;
                if response.is_err() {
                    // Drops the only sender on the builder's channel, which `session_from_pending_build`
                    // parks on. `TtlMap` reclaims only when a later `insert` vacuums.
                    self.session_build_store
                        .timeout_lock()
                        .await
                        .store
                        .remove(&node_id);
                }
                return response
                    .map_err(|_| {
                        error!("Did not receive a response from session establishment request within timeout {SESSION_ESTABLISHMENT_TIMEOUT:?}");
                        Arc::new(InitiatorSessionEstablishmentError::NoResponse.into())
                    })?
                    .expect("Session negotiation will not be this fast")
                    .map_err(|e| {
                        error!("Error during session establishment: {e:?}");
                        e
                    }).map(Into::into);
            }
        }
        error!("Unable to obtain a session using two tries. This should not be possible");
        Err(Arc::new(
            InitiatorSessionEstablishmentError::Internal(
                "Did not obtain a session in two loops which should be impossible".into(),
            )
            .into(),
        ))
    }

    async fn get_established_session(
        &self,
        node_id: Self::NodeId,
        use_for_send: bool,
    ) -> Option<Self::Session> {
        let existing_session = self
            .session_store
            .timeout_lock()
            .await
            .store
            .get(&node_id)
            .cloned();
        match existing_session {
            Some(session) => {
                let now = SystemTime::now();

                if use_for_send && session.use_expiry > now || session.accept_expiry > now {
                    Some(session)
                } else {
                    None
                }
            }
            None => None,
        }
    }

    async fn get_recent_receive_sessions(&self, node_id: &Self::NodeId) -> Vec<Self::Session> {
        let mut current_session = {
            let session_store = self.session_store.timeout_lock().await;
            session_store.store.get(node_id).cloned()
        };
        let expired_sessions = self.expired_session_cache.get(node_id).await;

        // Waiting delays this payload, so only where the alternative is a certain `MissingSession`.
        if current_session.is_none() && expired_sessions.is_empty() {
            current_session = self.session_from_pending_build(node_id).await;
        }

        trace!(peer=?node_id, "{}", {
            match &current_session {
                None => "There is no current session",
                Some(_) => "Found current session"
            }
        });

        current_session
            .into_iter()
            .chain(expired_sessions.into_iter().map_into())
            .collect()
    }

    async fn retire_session(&self, node_id: Self::NodeId) {
        debug!(peer=?node_id, "Retiring session");
        let mut session_store = self.session_store.timeout_lock().await;
        if let Some(session) = session_store.store.remove(&node_id) {
            self.expired_session_cache.push(node_id, session).await;
        }
    }

    async fn discard_session(&self, node_id: Self::NodeId) {
        debug!(peer=?node_id, "Discarding session");
        self.session_store
            .timeout_lock()
            .await
            .store
            .remove(&node_id);
    }
}
