use crate::crypto::{
    AuthWireRequest, AuthWireResponse, InitiatorSessionCreation,
    InitiatorSessionEstablishmentError, KeyExchangeResponseMessage,
    ResponderSessionEstablishmentError, SessionBuildStore, SessionFromRequestError,
    SessionInitiationRequest, SessionInitiatorSecrets, SessionStore,
};
use crate::crypto::{Session as CryptoSession, SessionEstablishmentError};
use crate::data_structures::scoped_expiry_map::ScopedExpiryMap;
use crate::event_loop::with_swarm::{GetInnerEventSender, WithSwarm};
use crate::event_loop::{EventLoopSender, LocalNetworkSwarm};
use crate::send_receive_traits::session::Session;
use crate::utils::{W, Wrapper};
use async_trait::async_trait;
use crypto::ContactKeys;
use fyles_core::core::brain::action::BrainAction;
use fyles_core::core::brain::action_p2p::NetworkNodeAction;
use fyles_core::core::brain::types::BrainRequest;
use fyles_core::core::domain_models::ContactId;
use fyles_core::library::util::duration_ext::DurationExt;
use fyles_core::library::util::util::TimeoutLock;
use itertools::Itertools;
use libp2p::PeerId;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{Mutex, RwLock, mpsc};
use tracing::{debug, error, trace, warn};

#[cfg(test)]
const SESSION_ESTABLISHMENT_TIMEOUT: Duration = Duration::from_millis(500);

#[cfg(not(test))]
const SESSION_ESTABLISHMENT_TIMEOUT: Duration = Duration::from_secs(10);

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

    /// Clears the active session. Does not affect sessions that are in the process
    /// of being established
    async fn clear_session(&self, node_id: Self::NodeId);
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
            expired_session_cache: ScopedExpiryMap::new(15.seconds(), 15.seconds(), 60.seconds()),
        }
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
        let (brain_request, keys_response) = BrainRequest::with_receiver(request.sender.clone());
        trace!("Requesting public keys to verify other end during request");
        self.brain_action_sender
            .send(BrainAction::NetworkNode(
                NetworkNodeAction::GetContactPublicKeys(brain_request),
            ))
            .await
            .expect("Brain action sender to be alive");
        let maybe_keys = keys_response.await.expect("Keys response to be received");
        match request.to_session_and_response(match maybe_keys {
            Some(ref keys) => Some((request.sender.clone(), keys)),
            None => None,
        }) {
            Err(SessionFromRequestError::SignatureVerification(e)) => {
                error!("Initiator signature verification failed: {e:?}");
                Err(ResponderSessionEstablishmentError::ContactVerificationFailed(
                    request.sender,
                ))
            }
            Err(SessionFromRequestError::Kyber(e)) => {
                error!(error=?e, "Kyber error during session creation");
                Err(ResponderSessionEstablishmentError::SharedSecretGenerationFailed)
            }
            Ok((session, response)) => {
                debug!(
                    ?peer,
                    "Storing session after receiving session establishment request"
                );
                let self_contact_id = self.self_contact_id.read().await.clone();
                trace!(
                    "Signing session response for peer {peer:?} with self contact id {self_contact_id}"
                );
                match response.sign(
                    &mut self.keys.write().await.private,
                    self_contact_id,
                    maybe_keys.is_some(),
                ) {
                    Ok(signed) => {
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
                        Ok(signed)
                    }
                    Err(e) => {
                        error!("Failed to create shared secrets: {e:?}");
                        Err(ResponderSessionEstablishmentError::SharedSecretGenerationFailed)
                    }
                }
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
                    response.is_authenticated
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
                let (request, brain_response) = BrainRequest::with_receiver(response.sender.clone());
                self.brain_action_sender.send(BrainAction::NetworkNode(NetworkNodeAction::GetContactPublicKeys(request))).await.expect("Sending to work");
                let maybe_contact_public_keys = brain_response.await.expect("Sender not to be dropped");
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
                        let old_session = session_store_lock.store.insert(peer.clone(), session.into());
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
impl<
    Swarm: LocalNetworkSwarm + 'static,
> GetSession for SessionManager<PeerId, ContactId, Swarm>
{
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
            if let Some(existing_session) = self.get_established_session(node_id, true).await
            {
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
                let session_initiator_secrets =
                    Box::new(SessionInitiatorSecrets::new().map_err(|_| {
                        Arc::new(
                            InitiatorSessionEstablishmentError::EphemeralSecretGenerationError
                                .into(),
                        )
                    })?);
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
                let session_creation =
                    InitiatorSessionCreation::new(session_initiator_secrets.private);
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
                return tokio::time::timeout(SESSION_ESTABLISHMENT_TIMEOUT, response_receiver.recv())
                    .await
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

                if use_for_send && session.use_expiry > now {
                    Some(session)
                } else if session.accept_expiry > now {
                    Some(session)
                } else {
                    None
                }
            }
            None => None,
        }
    }

    async fn get_recent_receive_sessions(&self, node_id: &Self::NodeId) -> Vec<Self::Session> {
        let current_session = {
            let session_store = self.session_store.timeout_lock().await;
            session_store.store.get(node_id).cloned()
        };

        trace!(peer=?node_id, "{}", {
            match &current_session {
                None => "There is no current session",
                Some(_) => "Found current session"
            }
        });

        current_session
            .into_iter()
            .chain(
                self.expired_session_cache
                    .get(node_id)
                    .await
                    .into_iter()
                    .map_into(),
            )
            .collect()
    }

    async fn clear_session(&self, node_id: Self::NodeId) {
        debug!(peer=?node_id, "Clearing session");
        let mut session_store = self.session_store.timeout_lock().await;
        if let Some(session) = session_store.store.remove(&node_id) {
            self.expired_session_cache.push(node_id, session).await;
        }
    }
}
