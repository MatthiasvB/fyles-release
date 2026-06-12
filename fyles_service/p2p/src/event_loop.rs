use async_trait::async_trait;
use crypto::{ContactKeys, DeSerCryptError, SerCryptError};
use fyles_core::core::brain::types::{ContactShareChallenge, SelfContactInviteChallenge};
use libp2p::{
    identity::Keypair, noise, request_response::ResponseChannel, swarm::DialError, tcp,
    yamux, Swarm, TransportError,
};
use std::sync::Arc;
use std::{
    collections::HashMap,
    error::Error,
    io,
    time::SystemTime,
};
use thiserror::Error;
use tracing::{
    debug, error, info, instrument, span, trace, warn, Instrument, Level, Span,
};

use crate::types::{Unwrap, Wrap};
use libp2p::{
    core::transport::ListenerId, multiaddr::Protocol, request_response::OutboundRequestId, swarm::SwarmEvent,
    Multiaddr, PeerId,
};
use tokio::{
    sync::{mpsc, oneshot, Mutex, SetOnce},
    time::sleep,
};

use derive_more::Deref;

use fyles_core::{
    core::{
        brain::{
            action::BrainAction,
            action_p2p::{NetworkNodeAction, NodeInfo},
            error::FilerequestError,
            types::BrainRequest,
        },
        domain_models::{ContactId, PendingFile, SendStatus},
        p2p::NodeStatusInfo,
    },
    library::util::{
        duration_ext::DurationExt,
        error_handling::ToArcedDynError,
        util::TimeoutLock,
    },
};

pub use crate::{
    behaviour::{
        ContactShareBehaviour, FilerequestBehaviour, LocalNetworkBehaviour,
        LocalNetworkBehaviourEvent, SelfContactInviteBehaviour, SessionErrorResponse,
        SessionEstablishmentBehaviour,
    },
    crypto::{
        AuthWireResponse, InitiatorSessionCreation, InitiatorSessionEstablishmentError,
        KeyExchangeResponseMessage, ResponderSessionEstablishmentError, Session as CryptoSession,
        SessionBuildStore, SessionEstablishmentError, SessionInitiationRequest,
        SessionInitiatorSecrets, SessionStore,
    },
    event_loop::{
        filerequest::file_tracker::{CoreFileTracker, FileTracker},
        handle_file_request_event::FilerequestEvent,
        with_swarm::WithSwarm,
    },
    file_encryptor::ReadCryptResult,
    send_receive_traits::request_sender::RequestSender,
    types::FileResponse,
    utils::{decode_stored_keys, W},
    SwarmFactory,
};

use super::command::P2pCommand;
use crate::event_loop::filerequest::file_tracker::new_core_file_tracker;
use crate::event_loop::filerequest::receive_manager::FilerequestReceiver;
use crate::event_loop::with_swarm::GetInnerEventSender;
use crate::send_receive_traits::get_session::SessionManager;
#[cfg(test)]
use crate::send_receive_traits::get_session::GetSession;
use crate::send_receive_traits::request_registry::{
    TimeoutRequestRegistry, TimeoutRequestRegistryWorker,
};
use crate::send_receive_traits::response_receiver::ResponseReceiver;
use crate::send_receive_traits::session_send_receive::{
    LocalReceiveError, ReceiveError, SessionSend,
};
use crate::types::FileRequest;
use futures::StreamExt;
use fyles_core::core::domain_models::{Contact, SelfContact};
#[cfg(target_os = "android")]
use libp2p::dns::ResolverConfig;

pub mod filerequest;
mod handle_behavior_event;
mod handle_contact_invite_event;
mod handle_file_request_event;
mod handle_mdns_event;
mod handle_self_contact_invite_event;
mod handle_session_establishment_event;
mod trait_impls;
pub mod with_swarm;

pub enum InnerEvent<T: LocalNetworkSwarm = Swarm<LocalNetworkBehaviour>> {
    /// The signature could not be confirmed to belong to the contact
    SessionEstablishmentError {
        channel: ResponseChannel<
            Result<
                AuthWireResponse<Box<KeyExchangeResponseMessage>>,
                ResponderSessionEstablishmentError,
            >,
        >,
        error: ResponderSessionEstablishmentError,
    },
    SessionEstablished {
        channel: ResponseChannel<
            Result<
                AuthWireResponse<Box<KeyExchangeResponseMessage>>,
                ResponderSessionEstablishmentError,
            >,
        >,
        response: AuthWireResponse<Box<KeyExchangeResponseMessage>>,
    },
    DoWithSwarm {
        block: Box<dyn FnOnce(&mut T) + Send>,
        span: Span,
    },
}

pub enum InnerAsyncEvent<T: FileTracker> {
    // FIXME: I have not been able to make this work nicely
    // https://stackoverflow.com/q/79744394/8447743
    // DoWithSwarm {
    //     block: Box<
    //         dyn for<'a> Fn(
    //             &'a mut NetworkSwarm
    //         ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>
    //         + Send,
    //     >,
    // },
    ConnectOtherSwarm {
        other: Swarm<LocalNetworkBehaviour>,
        return_connected_sender: oneshot::Sender<Swarm<LocalNetworkBehaviour>>,
    },
    GetEventLoop(oneshot::Sender<RefCountEventLoopData<T>>),
}

pub type EventLoopSender<T = Swarm<LocalNetworkBehaviour>> = mpsc::UnboundedSender<InnerEvent<T>>;
type EventLoopReceiver<T = Swarm<LocalNetworkBehaviour>> = mpsc::UnboundedReceiver<InnerEvent<T>>;
pub type AsyncEventLoopSender<T> = mpsc::UnboundedSender<InnerAsyncEvent<T>>;
pub type AsyncEventLoopReceiver<T> = mpsc::UnboundedReceiver<InnerAsyncEvent<T>>;

#[async_trait]
impl<T: LocalNetworkSwarm + 'static> GetInnerEventSender<T> for EventLoopSender<T> {
    fn get_inner_event_sender(&self) -> &EventLoopSender<T> {
        self
    }
}

#[derive(Error, Debug)]
pub enum SessionError {
    #[error("Error during session establishment: {0}")]
    Send(#[from] SessionSendError),
    #[error("Error during session receive: {0}")]
    Receive(#[from] SessionReceiveError),
}

#[derive(Error, Debug)]
pub enum SessionSendError {
    #[error("Error during session establishment: {0}")]
    SessionEstablishment(#[from] Arc<SessionEstablishmentError>),
    #[error("Error during serialization or encryption: {0}")]
    SerCryptError(#[from] SerCryptError),
}

#[derive(Error, Debug)]
pub enum SessionReceiveError {
    #[error("Error during deserialization or decryption: {0}")]
    DeSerCryptError(#[from] DeSerCryptError),
    #[error("Could not find expected session")]
    MissingExpectedSession,
}

pub struct LocalEventLoop<T: FileTracker> {
    // public for testing, see if can be done better
    #[cfg(test)]
    pub swarm: Swarm<LocalNetworkBehaviour>,
    #[cfg(not(test))]
    swarm: Swarm<LocalNetworkBehaviour>,
    filerequest_request_timeouts: TimeoutRequestRegistryWorker<usize, PeerId>,
    contact_share_request_timeouts: TimeoutRequestRegistryWorker<usize, PeerId>,
    self_contact_invite_request_timeouts: TimeoutRequestRegistryWorker<usize, PeerId>,
    command_receiver: tokio::sync::mpsc::Receiver<P2pCommand>,
    inner_event_receiver: EventLoopReceiver<Swarm<LocalNetworkBehaviour>>,
    #[cfg(test)]
    inner_async_event_receiver: AsyncEventLoopReceiver<T>,
    #[cfg(test)]
    pub data: RefCountEventLoopData<T, Swarm<LocalNetworkBehaviour>>,
    #[cfg(not(test))]
    data: RefCountEventLoopData<T, Swarm<LocalNetworkBehaviour>>,
}

pub trait LocalEventLoopData<T: FileTracker, S: LocalNetworkSwarm = Swarm<LocalNetworkBehaviour>> {
    fn get_data(&self) -> &RefCountEventLoopData<T, S>;
}

impl<T: FileTracker> LocalEventLoopData<T> for LocalEventLoop<T> {
    fn get_data(&self) -> &RefCountEventLoopData<T> {
        &self.data
    }
}

impl<T: FileTracker> GetSwarm<Swarm<LocalNetworkBehaviour>> for LocalEventLoop<T> {
    fn get_swarm_mut(&mut self) -> &mut Swarm<LocalNetworkBehaviour> {
        &mut self.swarm
    }
}

pub trait LocalNetworkSwarm {
    fn dial(&mut self, opts: &PeerId) -> Result<(), DialError>;

    fn local_peer_id(&self) -> &PeerId;

    fn external_addresses<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Multiaddr> + 'a>;

    fn listeners<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Multiaddr> + 'a>;

    fn connected_peers<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PeerId> + 'a>;
    fn listen_on(&mut self, addr: Multiaddr) -> Result<ListenerId, TransportError<io::Error>>;

    fn get_filerequest_behaviour(&mut self) -> &mut FilerequestBehaviour;

    fn get_self_contact_invite_behaviour(&mut self) -> &mut SelfContactInviteBehaviour;

    fn get_contact_share_behaviour(&mut self) -> &mut ContactShareBehaviour;

    fn get_session_establishment_behaviour(&mut self) -> &mut SessionEstablishmentBehaviour;
}

impl LocalNetworkSwarm for Swarm<LocalNetworkBehaviour> {
    fn dial(&mut self, opts: &PeerId) -> Result<(), DialError> {
        self.dial(*opts)
    }

    fn local_peer_id(&self) -> &PeerId {
        self.local_peer_id()
    }

    fn external_addresses<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Multiaddr> + 'a> {
        Box::new(self.external_addresses())
    }

    fn listeners<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Multiaddr> + 'a> {
        Box::new(self.listeners())
    }

    fn connected_peers<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PeerId> + 'a> {
        Box::new(self.connected_peers())
    }

    fn listen_on(&mut self, addr: Multiaddr) -> Result<ListenerId, TransportError<io::Error>> {
        self.listen_on(addr)
    }

    fn get_filerequest_behaviour(&mut self) -> &mut FilerequestBehaviour {
        &mut self.behaviour_mut().core.filerequest
    }

    fn get_self_contact_invite_behaviour(&mut self) -> &mut SelfContactInviteBehaviour {
        &mut self.behaviour_mut().core.self_contact_invite
    }

    fn get_contact_share_behaviour(&mut self) -> &mut ContactShareBehaviour {
        &mut self.behaviour_mut().core.contact_share
    }

    fn get_session_establishment_behaviour(&mut self) -> &mut SessionEstablishmentBehaviour {
        &mut self.behaviour_mut().core.session_establishment
    }
}

impl<T: FileTracker> LocalNetworkSwarm for LocalEventLoop<T> {
    fn local_peer_id(&self) -> &PeerId {
        self.swarm.local_peer_id()
    }

    fn external_addresses<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Multiaddr> + 'a> {
        Box::new(self.swarm.external_addresses())
    }

    fn listeners<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Multiaddr> + 'a> {
        Box::new(self.swarm.listeners())
    }

    fn connected_peers<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PeerId> + 'a> {
        Box::new(self.swarm.connected_peers())
    }

    fn listen_on(&mut self, addr: Multiaddr) -> Result<ListenerId, TransportError<io::Error>> {
        self.swarm.listen_on(addr)
    }

    fn get_filerequest_behaviour(&mut self) -> &mut FilerequestBehaviour {
        self.swarm.get_filerequest_behaviour()
    }

    fn get_self_contact_invite_behaviour(&mut self) -> &mut SelfContactInviteBehaviour {
        self.swarm.get_self_contact_invite_behaviour()
    }

    fn get_contact_share_behaviour(&mut self) -> &mut ContactShareBehaviour {
        self.swarm.get_contact_share_behaviour()
    }

    fn get_session_establishment_behaviour(&mut self) -> &mut SessionEstablishmentBehaviour {
        self.swarm.get_session_establishment_behaviour()
    }

    fn dial(&mut self, opts: &PeerId) -> Result<(), DialError> {
        self.swarm.dial(*opts)
    }
}

pub struct EventLoopData<T: FileTracker, S: LocalNetworkSwarm = Swarm<LocalNetworkBehaviour>> {
    /// Timestamp of when the node started
    pub start_timestamp: u128,
    pub brain_action_sender: tokio::sync::mpsc::Sender<BrainAction>,
    // not sure if this is needed
    pub pending_dial:
        Mutex<HashMap<PeerId, oneshot::Sender<Result<(), Arc<dyn Error + Send + Sync>>>>>,
    pub file_tracker: T,
    pub inner_event_sender: EventLoopSender<S>,
    pub session_manager: SessionManager<PeerId, ContactId, S>,
    pub receive_manager: Mutex<FilerequestReceiver>,
    pub filerequest_request_registry: TimeoutRequestRegistry<
        PeerId,
        OutboundRequestId,
        usize,
        FileRequest,
        ContactId,
        W<Arc<CryptoSession>>,
    >,
    pub self_contact_invite_challenge_request_registry: TimeoutRequestRegistry<
        PeerId,
        OutboundRequestId,
        usize,
        SelfContactInviteChallenge,
        ContactId,
        W<Arc<CryptoSession>>,
    >,
    pub contact_invite_challenge_request_registry: TimeoutRequestRegistry<
        PeerId,
        OutboundRequestId,
        usize,
        ContactShareChallenge,
        ContactId,
        W<Arc<CryptoSession>>,
    >,
}

#[derive(Deref)]
#[deref(forward)]
pub struct RefCountEventLoopData<
    T: FileTracker,
    S: LocalNetworkSwarm = Swarm<LocalNetworkBehaviour>,
>(pub Arc<EventLoopData<T, S>>);

impl<T: FileTracker, S: LocalNetworkSwarm> Clone for RefCountEventLoopData<T, S> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: FileTracker, S: LocalNetworkSwarm> From<EventLoopData<T, S>>
    for RefCountEventLoopData<T, S>
{
    fn from(data: EventLoopData<T, S>) -> Self {
        Self(Arc::new(data))
    }
}

#[derive(Error, Debug)]
pub enum EventLoopError {
    #[error("An error occurred in the P2P event loop: {msg}, {source}")]
    Generic {
        msg: String,
        #[source]
        source: Box<dyn Error + Send + Sync>,
    },
    #[error("Communication error in event loop: {0}")]
    CommunicationError(String),
    #[error("Receiver error in event loop: {source}")]
    RecvError {
        #[from]
        source: tokio::sync::oneshot::error::RecvError,
    },
    #[error("Filerequest error in event loop: {source}")]
    Filerequest {
        #[from]
        source: FilerequestError,
    },
    #[error("Validation error in event loop: {msg}, {source}")]
    Validation {
        msg: String,
        #[source]
        source: Box<dyn Error + Send + Sync>,
    },
    #[error("A generic error occurred: {source}")]
    RawGeneric {
        #[from]
        source: Arc<dyn Error + Send + Sync>,
    },
    #[error("Tried initializing the event loop twice")]
    DoubleInit,
}

impl LocalEventLoop<CoreFileTracker> {
    #[instrument(skip_all, level = "info")]
    pub async fn new(
        command_receiver: tokio::sync::mpsc::Receiver<P2pCommand>,
        brain_action_sender: tokio::sync::mpsc::Sender<BrainAction>,
        swarm_factory: SwarmFactory<Swarm<LocalNetworkBehaviour>>,
        #[cfg(any(test, feature = "test-support"))]
        inner_async_event_receiver: AsyncEventLoopReceiver<CoreFileTracker>,
    ) -> Result<LocalEventLoop<CoreFileTracker>, EventLoopError> {
        #[cfg(all(not(test), feature = "test-support"))]
        let _ = inner_async_event_receiver;

        info!("Setup: Running P2P node");

        let mut loop_count = 1usize;
        // Get keys through brain action
        let node_info_result = loop {
            // Wait for brain to be ready
            sleep(100.millis()).await;
            let (request, receiver) = BrainRequest::with_receiver(());
            brain_action_sender
                .send(BrainAction::NetworkNode(NetworkNodeAction::GetNodeInfo(
                    request,
                )))
                .await
                .map_err(|_| {
                    EventLoopError::CommunicationError(
                        "Unable to send GetNodeKeys action to brain".into(),
                    )
                })?;
            info!("Setup: Requesting keys from brain");
            if let Ok(result) = receiver.await {
                trace!("Received response from brain after {} attempts", loop_count);
                break result; // else retry after delay
            }
            loop_count += 1;
        };
        info!("Setup: Received keys from brain");

        let (keypair, self_contact_id, self_contact_keys, persisted_settings) =
            match node_info_result {
                Ok((
                    NodeInfo {
                        node_key_pair,
                        self_contact_id,
                        self_contact_keys,
                    },
                    persisted_settings,
                )) => (
                    decode_stored_keys(&node_key_pair).map_err(|e| EventLoopError::Validation {
                        msg: "Could not decode stored keys".into(),
                        source: Box::new(e),
                    })?,
                    self_contact_id,
                    self_contact_keys,
                    persisted_settings,
                ),
                Err(e) => panic!("Communication error: {}", e),
            };

        let peer_id = keypair.public().to_peer_id();
        info!("Local peer id: {}", peer_id.to_base58());

        let swarm = swarm_factory(keypair, &persisted_settings).unwrap();

        brain_action_sender
            .send(BrainAction::NetworkNode(NetworkNodeAction::Ready))
            .await
            .map_err(|_| {
                EventLoopError::CommunicationError("Unable to send Ready action to brain".into())
            })?;
        info!("Starting P2P event loop");
        let (inner_sender, inner_receiver) = mpsc::unbounded_channel();
        let future_event_loop_data: Arc<SetOnce<RefCountEventLoopData<CoreFileTracker>>> =
            Arc::new(tokio::sync::SetOnce::new());
        let future_event_loop_data_clone_1 = future_event_loop_data.clone();
        let future_event_loop_data_clone_2 = future_event_loop_data_clone_1.clone();
        let (filerequest_request_registry, filerequest_timeouts) =
            TimeoutRequestRegistry::new(15.seconds());
        let (self_contact_invite_request_registry, self_contact_invite_timeouts) =
            TimeoutRequestRegistry::new(15.seconds());
        let (contact_share_request_registry, contact_share_timeouts) =
            TimeoutRequestRegistry::new(15.seconds());
        let data = RefCountEventLoopData(Arc::new(EventLoopData {
            start_timestamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("Time to be valid")
                .as_millis(),
            filerequest_request_registry,
            self_contact_invite_challenge_request_registry: self_contact_invite_request_registry,
            contact_invite_challenge_request_registry: contact_share_request_registry,
            file_tracker: new_core_file_tracker(
                brain_action_sender.clone(),
                Box::new(move |peer, contact_id, request| {
                    let clone_clone = future_event_loop_data_clone_1.clone();
                    Box::pin(async move {
                        let event_loop = clone_clone.wait().await;
                        event_loop.send_request(peer, contact_id, request).await
                    })
                }),
                Box::new(move |peer, contact_id, request| {
                    let clone_clone = future_event_loop_data_clone_2.clone();
                    Box::pin(async move {
                        let event_loop = clone_clone.wait().await;
                        event_loop
                            .send_request_idempotent(peer, contact_id, request)
                            .await
                    })
                }),
            ),
            pending_dial: Default::default(),
            session_manager: SessionManager::new(
                brain_action_sender.clone(),
                inner_sender.clone(),
                self_contact_id,
                self_contact_keys,
            ),
            receive_manager: FilerequestReceiver::new(brain_action_sender.clone()).into(),
            brain_action_sender,
            inner_event_sender: inner_sender,
        }));
        if let Err(_e) = future_event_loop_data.set(data.clone()) {
            panic!("Multi init");
        }
        Ok(LocalEventLoop {
            swarm,
            inner_event_receiver: inner_receiver,
            #[cfg(test)]
            inner_async_event_receiver,
            command_receiver,
            data,
            filerequest_request_timeouts: filerequest_timeouts,
            contact_share_request_timeouts: contact_share_timeouts,
            self_contact_invite_request_timeouts: self_contact_invite_timeouts,
        })
    }

    pub fn swarm_factory(
        keypair: Keypair,
    ) -> Result<Swarm<LocalNetworkBehaviour>, Arc<dyn Error + Send + Sync>> {
        Ok(libp2p::SwarmBuilder::with_existing_identity(keypair)
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                noise::Config::new,
                yamux::Config::default,
            )
            .tade()?
            .with_quic()
            .with_behaviour(LocalNetworkBehaviour::new)
            .tade()?
            .build())
    }
}

impl LocalEventLoop<CoreFileTracker> {
    #[cfg(test)]
    pub async fn with_swarm(
        swarm: Swarm<LocalNetworkBehaviour>,
        contact_id: ContactId,
        contact_keys: ContactKeys,
        command_receiver: tokio::sync::mpsc::Receiver<P2pCommand>,
        brain_action_sender: tokio::sync::mpsc::Sender<BrainAction>,
        inner_async_event_receiver: AsyncEventLoopReceiver<CoreFileTracker>,
    ) -> Result<Self, EventLoopError> {
        brain_action_sender
            .send(BrainAction::NetworkNode(NetworkNodeAction::Ready))
            .await
            .map_err(|_| {
                EventLoopError::CommunicationError("Unable to send Ready action to brain".into())
            })?;
        info!("Starting P2P event loop");
        let (inner_sender, inner_receiver) = mpsc::unbounded_channel();
        let (filerequest_request_registry, filerequest_timeouts) =
            TimeoutRequestRegistry::new(15.seconds());
        let (self_contact_invite_request_registry, self_contact_invite_timeouts) =
            TimeoutRequestRegistry::new(15.seconds());
        let (contact_share_request_registry, contact_share_timeouts) =
            TimeoutRequestRegistry::new(15.seconds());
        let future_event_loop_data: SetOnce<RefCountEventLoopData<CoreFileTracker>> =
            tokio::sync::SetOnce::new();
        let future_event_loop_data_clone_1 = Arc::new(future_event_loop_data.clone());
        let future_event_loop_data_clone_2 = future_event_loop_data_clone_1.clone();
        let data = RefCountEventLoopData(Arc::new(EventLoopData {
            start_timestamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("Time to be valid")
                .as_millis(),
            filerequest_request_registry: filerequest_request_registry,
            contact_invite_challenge_request_registry: contact_share_request_registry,
            self_contact_invite_challenge_request_registry: self_contact_invite_request_registry,
            file_tracker: new_core_file_tracker(
                brain_action_sender.clone(),
                Box::new(move |peer, contact_id, request| {
                    let clone_clone = future_event_loop_data_clone_1.clone();
                    Box::pin(async move {
                        let event_loop = clone_clone.wait().await;
                        event_loop.send_request(peer, contact_id, request).await
                    })
                }),
                Box::new(move |peer, contact_id, request| {
                    let clone_clone = future_event_loop_data_clone_2.clone();
                    Box::pin(async move {
                        let event_loop = clone_clone.wait().await;
                        event_loop
                            .send_request_idempotent(peer, contact_id, request)
                            .await
                    })
                }),
            ),
            session_manager: SessionManager::new(
                brain_action_sender.clone(),
                inner_sender.clone(),
                contact_id,
                contact_keys,
            ),
            receive_manager: FilerequestReceiver::new(brain_action_sender.clone()).into(),
            brain_action_sender,
            pending_dial: Default::default(),
            inner_event_sender: inner_sender,
        }));
        if let Err(_e) = future_event_loop_data.set(data.clone()) {
            panic!("Multi init");
        }
        Ok(Self {
            swarm,
            inner_event_receiver: inner_receiver,
            #[cfg(test)]
            inner_async_event_receiver,
            command_receiver,
            data,
            filerequest_request_timeouts: filerequest_timeouts,
            contact_share_request_timeouts: contact_share_timeouts,
            self_contact_invite_request_timeouts: self_contact_invite_timeouts,
        })
    }
}

impl LocalEventLoop<CoreFileTracker> {
    pub async fn run(mut self) -> Result<(), ()> {
        loop {
            #[cfg(not(test))]
            tokio::select! {
                event = self.swarm.select_next_some() => { tokio::spawn(self.data.clone().handle_local_event(event).in_current_span()); },
                command = self.command_receiver.recv() => match command {
                    Some(c) => self.data.clone().handle_command(c),
                    // Command channel closed, thus shutting down the network event loop.
                    None => return Err(()),
                },
                inner_event = self.inner_event_receiver.recv() => { self.handle_inner_event(inner_event.expect("Channel to be open")); },
                filerequest_timeout = self.filerequest_request_timeouts.next() => {
                    let (request_id, peer_id) = filerequest_timeout.unwrap();
                    let data = self.data.clone();
                    tokio::spawn(async move { <RefCountEventLoopData<_> as ResponseReceiver<FileResponse>>::handle_receive_error(&data, peer_id, Ok(request_id), ReceiveError::LocalError(LocalReceiveError::Timeout)).await });
                },
                contact_share_timeout = self.contact_share_request_timeouts.next() => {
                    let (request_id, peer_id) = contact_share_timeout.unwrap();
                    let data = self.data.clone();
                    tokio::spawn(async move { <RefCountEventLoopData<_> as ResponseReceiver<Contact>>::handle_receive_error(&data, peer_id, Ok(request_id), ReceiveError::LocalError(LocalReceiveError::Timeout)).await });
                },
                self_contact_invite_timeout = self.self_contact_invite_request_timeouts.next() => {
                    let (request_id, peer_id) = self_contact_invite_timeout.unwrap();
                    let data = self.data.clone();
                    tokio::spawn(async move { <RefCountEventLoopData<_> as ResponseReceiver<SelfContact>>::handle_receive_error(&data, peer_id, Ok(request_id), ReceiveError::LocalError(LocalReceiveError::Timeout)).await });
                }
            }

            #[cfg(test)]
            tokio::select! {
                event = {
                    self.swarm.select_next_some()
                } => { tokio::spawn(self.data.clone().handle_local_event(event).in_current_span()); },
                command = self.command_receiver.recv() => match command {
                    Some(c) => self.data.clone().handle_command(c),
                    // Command channel closed, thus shutting down the network event loop.
                    None => return Err(()),
                },
                inner_event = self.inner_event_receiver.recv() => { self.handle_inner_event(inner_event.expect("Channel to be open")); }
                async_event = self.inner_async_event_receiver.recv() => {
                    self.handle_inner_async_event(async_event.expect("Channel to be open")).await;
                },
                filerequest_timeout = self.filerequest_request_timeouts.next() => {
                    let (request_id, peer_id) = filerequest_timeout.unwrap();
                    let data = self.data.clone();
                    tokio::spawn(async move { <RefCountEventLoopData<_> as ResponseReceiver<FileResponse>>::handle_receive_error(&data, peer_id, Ok(request_id), ReceiveError::LocalError(LocalReceiveError::Timeout)).await });
                },
                contact_share_timeout = self.contact_share_request_timeouts.next() => {
                    let (request_id, peer_id) = contact_share_timeout.unwrap();
                    let data = self.data.clone();
                    tokio::spawn(async move { <RefCountEventLoopData<_> as ResponseReceiver<Contact>>::handle_receive_error(&data, peer_id, Ok(request_id), ReceiveError::LocalError(LocalReceiveError::Timeout)).await });
                },
                self_contact_invite_timeout = self.self_contact_invite_request_timeouts.next() => {
                    let (request_id, peer_id) = self_contact_invite_timeout.unwrap();
                    let data = self.data.clone();
                    tokio::spawn(async move { <RefCountEventLoopData<_> as ResponseReceiver<SelfContact>>::handle_receive_error(&data, peer_id, Ok(request_id), ReceiveError::LocalError(LocalReceiveError::Timeout)).await });
                }
            }
        }
    }

    #[cfg(test)]
    #[instrument(skip(self, event))]
    async fn handle_inner_async_event(&mut self, event: InnerAsyncEvent<CoreFileTracker>) {
        match event {
            InnerAsyncEvent::ConnectOtherSwarm {
                mut other,
                return_connected_sender,
            } => {
                use libp2p_swarm_test::SwarmExt;

                self.swarm.connect(&mut other).await;
                if return_connected_sender.send(other).is_err() {
                    panic!("Failed to send the swarm back to the event loop");
                }
            }
            InnerAsyncEvent::GetEventLoop(sender) => {
                sender
                    .send(self.data.clone())
                    .unwrap_or_else(|_| panic!("Sending to work"));
            }
        }
    }
}

pub trait LocalEventLoopBehaviour<
    D,
    T: GetSwarm<S>,
    S: LocalNetworkSwarm = Swarm<LocalNetworkBehaviour>,
>
{
    fn handle_inner_event(&mut self, event: InnerEvent<S>);
}

pub trait GetSwarm<T: LocalNetworkSwarm> {
    fn get_swarm_mut(&mut self) -> &mut T;
}

impl<D, T, S> LocalEventLoopBehaviour<D, T, S> for T
where
    S: LocalNetworkSwarm + 'static,
    D: FileTracker + 'static,
    T: LocalEventLoopData<D, S> + GetSwarm<S>,
{
    #[instrument(skip(self, event), level = "trace")]
    fn handle_inner_event(&mut self, event: InnerEvent<S>) {
        match event {
            InnerEvent::SessionEstablishmentError { channel, error } => {
                self.get_swarm_mut()
                    .get_session_establishment_behaviour()
                    .send_response(channel, Err(error))
                    .expect("Sending session establishment error to go okay");
            }
            InnerEvent::SessionEstablished { channel, response } => {
                trace!("Sending session establishment response");
                self.get_swarm_mut()
                    .get_session_establishment_behaviour()
                    .send_response(channel, Ok(response))
                    .expect("Sending session established response to go okay");
            }
            InnerEvent::DoWithSwarm { block, span } => {
                let _entered = span.enter();
                span!(Level::TRACE, "DoWithSwarm block execution").in_scope(|| {
                    block(self.get_swarm_mut());
                });
            }
        }
    }
}

#[async_trait]
impl<T: FileTracker, S: LocalNetworkSwarm + 'static> GetInnerEventSender<S>
    for RefCountEventLoopData<T, S>
{
    fn get_inner_event_sender(&self) -> &EventLoopSender<S> {
        &self.inner_event_sender
    }
}

impl<T: FileTracker + 'static, S: LocalNetworkSwarm + 'static> RefCountEventLoopData<T, S> {
    pub async fn get_node_info(&self) -> NodeStatusInfo {
        let event_loop = self.clone();
        let event_loop2 = self.clone();
        let response = event_loop2.with_swarm_res(move |swarm| NodeStatusInfo {
            start_timestamp: event_loop.start_timestamp,
            peer_id: swarm.local_peer_id().wrap(),
            external_addresses: swarm
                .external_addresses()
                .map(|addr| addr.to_string())
                .collect(),
            internal_addresses: swarm.listeners().map(|addr| addr.to_string()).collect(),
            connected_peers: swarm.connected_peers().count(),
        });
        response.await
    }

    pub async fn update_identity(&self, contact_id: ContactId, keys: ContactKeys) {
        self.session_manager.update_identity(contact_id, keys).await;
    }

    #[instrument(skip(self))]
    fn handle_discovered_peer(mut self, peer_id: PeerId, addr: Multiaddr) {
        if addr.iter().any(|p| p == Protocol::P2pCircuit) {
            trace!("Discovered relay peer {peer_id} at addr {addr}");
            return;
        }

        self.trigger_interaction_with_peer(peer_id);
    }

    fn trigger_interaction_with_peer(&mut self, peer_id: PeerId) {
        tokio::spawn(self.clone().send_files_to_peer(peer_id).in_current_span());
    }

    async fn handle_local_event(self, event: SwarmEvent<LocalNetworkBehaviourEvent>) {
        match event {
            SwarmEvent::Behaviour(event) => self.handle_local_behaviour_event(event),
            SwarmEvent::NewListenAddr { address, .. } => {
                tokio::spawn(self.handle_new_listen_address(address).in_current_span());
            }
            SwarmEvent::ListenerClosed {
                addresses, reason, ..
            } => {
                tokio::spawn(
                    self.handle_listener_closed(addresses, reason)
                        .in_current_span(),
                );
            }
            SwarmEvent::ListenerError { error, .. } => {
                tokio::spawn(self.handle_listener_error(error).in_current_span());
            }
            SwarmEvent::ExpiredListenAddr {
                listener_id,
                address,
            } => {
                handle_expired_listen_address(listener_id, address);
            }
            SwarmEvent::ExternalAddrConfirmed { address } => {
                handle_external_address_confirmed(address);
            }
            SwarmEvent::IncomingConnection {
                local_addr,
                send_back_addr,
                ..
            } => {
                handle_incoming_connection(local_addr, send_back_addr);
            }
            SwarmEvent::ConnectionEstablished {
                peer_id, endpoint, ..
            } => {
                tokio::spawn(
                    self.handle_connection_established(peer_id, endpoint)
                        .in_current_span(),
                );
            }
            SwarmEvent::ConnectionClosed {
                peer_id,
                cause,
                num_established,
                ..
            } => {
                handle_connection_closed(peer_id, cause);
                if num_established == 0 {
                    self.file_tracker.reset_for_disconnected_peer(peer_id).await;
                }
            }
            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                tokio::spawn(
                    self.handle_outgoing_connection_error(peer_id, error)
                        .in_current_span(),
                );
            }
            SwarmEvent::IncomingConnectionError {
                local_addr,
                send_back_addr,
                error,
                ..
            } => {
                handle_incoming_connection_error(local_addr, send_back_addr, error);
            }
            SwarmEvent::Dialing {
                peer_id: Some(peer_id),
                ..
            } => {
                handle_dialing(peer_id);
            }
            SwarmEvent::NewExternalAddrOfPeer { peer_id, address } => {
                trace!("New external address candidate of peer {peer_id:?}: {address}");
            }
            SwarmEvent::NewExternalAddrCandidate { address, .. } => {
                trace!("New external address candidate: {address}");
            }
            SwarmEvent::ExternalAddrExpired { address } => {
                trace!("External address expired: {address}");
            }
            non_exhaustive => warn!("Non-exhaustive BS hit: {non_exhaustive:?}"),
        }
    }

    async fn handle_outgoing_connection_error(
        self,
        peer_id: Option<PeerId>,
        error: libp2p::swarm::DialError,
    ) {
        match error {
            libp2p::swarm::DialError::LocalPeerId { ref address } => {
                error!("Dialled self on address {address:?}")
            }
            libp2p::swarm::DialError::NoAddresses => warn!("No addresses to dial"),
            libp2p::swarm::DialError::DialPeerConditionFalse(ref peer_condition) => {
                trace!("Dial peer condition false: {peer_condition:?}")
            }
            libp2p::swarm::DialError::Aborted => trace!("Dial aborted"),
            libp2p::swarm::DialError::WrongPeerId {
                ref obtained,
                ref address,
            } => warn!("Wrong peer id: {obtained:?} {address:?}"),
            libp2p::swarm::DialError::Denied { ref cause } => warn!("Dial denied: {cause:?}"),
            libp2p::swarm::DialError::Transport(ref items) => {
                trace!("Transport error: {items:?}")
            }
        }
        if let Some(peer_id) = peer_id {
            if let Some(sender) = self.pending_dial.timeout_lock().await.remove(&peer_id) {
                let _ = sender.send(Err(Arc::new(error)));
            }
        }
    }

    async fn handle_connection_established(
        self,
        peer_id: PeerId,
        endpoint: libp2p::core::ConnectedPoint,
    ) {
        if endpoint.is_dialer() {
            trace!("Established connection to {peer_id}");
            if let Some(sender) = self.pending_dial.timeout_lock().await.remove(&peer_id) {
                let _ = sender.send(Ok(()));
            }
        }
        if self.file_tracker.has_pending_files_for(&peer_id).await {
            self.send_files_to_peer(peer_id).await;
        }
    }

    async fn handle_listener_error(self, error: io::Error) {
        debug!("Listener error {:?}", error,);
    }

    pub async fn handle_listener_closed(
        self,
        addresses: Vec<Multiaddr>,
        reason: Result<(), io::Error>,
    ) {
        debug!(
            "Listener closed for {:?} because of {:?}",
            addresses, reason
        );
    }

    #[cfg(test)]
    pub fn with_swarm_test(&self, block: impl FnOnce(&mut S) + Send + 'static) {
        let _ = self.inner_event_sender.send(InnerEvent::DoWithSwarm {
            block: Box::new(block),
            span: Span::current(),
        });
    }

    pub async fn handle_new_listen_address(self, address: Multiaddr) {
        self.clone().with_swarm(|swarm| {
            let local_peer_id = swarm.local_peer_id();
            trace!(
                "Local node is listening on {:?}",
                address.with(Protocol::P2p(*local_peer_id))
            );
        });
    }

    #[instrument(skip_all, level = "trace")]
    pub async fn send_files_to_peer(self, peer: PeerId) {
        self.file_tracker.trigger_interaction_with(peer).await;
    }

    #[instrument(skip(self))]
    pub fn handle_command(self, command: P2pCommand) {
        match command {
            P2pCommand::StartListening { addr, sender } => {
                self.with_swarm(|swarm| {
                    match swarm.listen_on(addr) {
                        Ok(_) => sender.send(Ok(())).expect("Sending value going okay"),
                        Err(e) => sender
                            .send(Err(Arc::new(e)))
                            .expect("Sending error going okay"),
                    };
                });
            }
            P2pCommand::SendFiles {
                files_to_send,
                sender,
            } => {
                tokio::spawn(
                    async move {
                        for file_to_send in files_to_send {
                            let peer_id = file_to_send.peer_id.unwrap_thing();
                            self.file_tracker
                                .add_pending_file(
                                    peer_id,
                                    PendingFile {
                                        id: file_to_send.id,
                                        target_filerequest_id: file_to_send.filerequest_id,
                                        file_path: file_to_send.file_path,
                                        status: SendStatus::Pending,
                                        contact_id: file_to_send.contact_id,
                                        retry_count: 0,
                                        display_name: None,
                                        interruption_reasons: vec![],
                                    },
                                )
                                .await;
                            tokio::spawn(
                                self.clone().send_files_to_peer(peer_id).in_current_span(),
                            );
                        }
                        sender.send(Ok(())).expect("Sending value going okay");
                    }
                    .in_current_span(),
                );
            }
            P2pCommand::InitialFilesToSend {
                files_to_send,
                sender,
            } => {
                tokio::spawn(
                    async move {
                        for file_to_send in files_to_send {
                            let peer_id = file_to_send.peer_id.unwrap_thing();
                            let pending_file = PendingFile {
                                        id: file_to_send.id,
                                        contact_id: file_to_send.contact_id,
                                        target_filerequest_id: file_to_send.filerequest_id,
                                        file_path: file_to_send.file_path,
                                        status: file_to_send.status,
                                        retry_count: file_to_send.retry_count,
                                        display_name: None,
                                        interruption_reasons: vec![],
                                    };
                            trace!(?pending_file, "Adding initial file to file tracker");
                            self.file_tracker
                                .add_pending_file(
                                    peer_id,
                                    pending_file,
                                )
                                .await;
                        }
                        sender.send(Ok(())).expect("Sending value going okay");
                    }
                    .in_current_span(),
                );
            }
            P2pCommand::GetNodeInfo { sender } => {
                tokio::spawn(
                    async move {
                        let node_info = self.get_node_info().await;
                        debug!(
                            "Local internal addresses: {:?}",
                            node_info
                                .internal_addresses
                                .iter()
                                .filter(|a| !a.contains("circuit"))
                                .collect::<Vec<_>>()
                        );
                        let _ = sender.send(node_info);
                    }
                    .in_current_span(),
                );
            }
            P2pCommand::CancelFile { file_id, sender } => {
                tokio::spawn(
                    async move {
                        let was_cancelled = self.file_tracker.cancel_file(file_id).await;

                        sender
                            .send(Ok(was_cancelled.is_some()))
                            .expect("Sending value going okay");
                    }
                    .in_current_span(),
                );
            }
            P2pCommand::CancelFilesForRemoteFilerequest {
                target_filerequest_id,
                peer_id,
                sender,
            } => {
                tokio::spawn(
                    async move {
                        let was_cancelled = self
                            .file_tracker
                            .cancel_file_by_target(target_filerequest_id, peer_id)
                            .await;

                        sender
                            .send(Ok(was_cancelled.is_some()))
                            .expect("Sending value going okay");
                    }
                    .in_current_span(),
                );
            }
            P2pCommand::UpdateIdentity { contact_id, keys } => {
                tokio::spawn(
                    async move {
                        self.update_identity(contact_id, keys).await;
                    }
                    .in_current_span(),
                );
            }
            P2pCommand::UseSelfContactInviteChallenge {
                invite_code,
                peer_id,
            } => {
                debug!("Using self contact invite for peer {peer_id}");
                if let Ok(peer_id) = peer_id.parse() {
                    tokio::spawn(
                        async move {
                            let _ = self.send_request(peer_id, None, invite_code).await;
                        }
                        .in_current_span(),
                    );
                } else {
                    error!("Invalid peer ID provided for self contact invite: {peer_id}");
                }
            }
            P2pCommand::UseContactShareChallenge {
                share_code,
                peer_id,
            } => {
                debug!("Using contact share for peer {peer_id}");
                if let Ok(peer_id) = peer_id.parse() {
                    tokio::spawn(
                        async move {
                            let _ = self.send_request(peer_id, None, share_code).await;
                        }
                        .in_current_span(),
                    );
                } else {
                    error!("Invalid peer ID provided for contact share: {peer_id}");
                }
            }
            P2pCommand::ApplySettings {
                settings: _,
                sender,
            } => {
                // Base p2p crate does not interpret settings. No-op.
                let _ = sender.send(Ok(()));
            }
        }
    }

    #[cfg(test)]
    #[instrument(skip_all)]
    pub async fn get_session_for_test(
        &self,
        peer_id: PeerId,
        other_contact_id: Option<ContactId>,
        use_for_send: bool,
    ) -> Result<W<Arc<CryptoSession>>, SessionError> {
        if use_for_send {
            self.get_or_establish_session(peer_id, other_contact_id)
                .await
                .map_err(|e| SessionError::Send(SessionSendError::SessionEstablishment(e)))
        } else {
            self.get_established_session(peer_id, use_for_send)
                .await
                .ok_or(SessionError::Receive(
                    SessionReceiveError::MissingExpectedSession,
                ))
        }
    }
}

pub fn handle_dialing(peer_id: PeerId) {
    trace!("Dialing {peer_id}");
}

pub fn handle_incoming_connection_error<T: Error>(
    local_addr: Multiaddr,
    send_back_addr: Multiaddr,
    error: T,
) {
    trace!(
        "Error accepting incoming connection from {:?} to {:?}: {error}",
        send_back_addr, local_addr
    );
}

pub fn handle_connection_closed<T: Error>(peer_id: PeerId, cause: Option<T>) {
    trace!("Connection to {peer_id} closed: {cause:?}");
}

pub fn handle_incoming_connection(local_addr: Multiaddr, send_back_addr: Multiaddr) {
    trace!(
        "Incoming connection from remote addr {:?} to local addr {:?}",
        send_back_addr, local_addr
    );
}

pub fn handle_external_address_confirmed(address: Multiaddr) {
    info!("External address confirmed: {:?}", address);
}

pub fn handle_expired_listen_address(listener_id: ListenerId, address: Multiaddr) {
    debug!("Listener {:?} expired for {:?}", listener_id, address);
}
