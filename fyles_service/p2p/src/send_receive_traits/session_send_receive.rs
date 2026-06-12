use crate::behaviour::SessionErrorResponse;
use crate::send_receive_traits::get_session::GetSession;
use crate::send_receive_traits::request_registry::{
    RequestForResend, RequestForResendAfterSessionError, RequestRegistry,
};
use crate::send_receive_traits::request_sender::RequestSender;
use crate::send_receive_traits::response_receiver::ResponseReceiver;
use crate::send_receive_traits::session::Session;
use async_trait::async_trait;
use crypto::{Encrypt, Encrypted};
use fyles_core::core::domain_models::ContactId;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::error;
use std::fmt::Debug;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, error, instrument, trace, warn};

#[derive(Debug, Error)]
pub enum SessionSendError<EE: error::Error, GSE: error::Error> {
    #[error("{0}")]
    EncryptError(EE),
    #[error("{0}")]
    GetSessionError(GSE),
}

/// Important: This enum must only contain errors that, when they occur, means that the remote's
/// business logic could not have read the request (because it could not have been decrypted). Should
/// this invariant ever cease holding true, a lot of code needs to be reconsidered!
#[derive(Debug, Error)]
pub enum RemoteReceiveError {
    #[error("Remote side reported not knowing a session to decrypt this request")]
    MissingSession,
    #[error("Remote side reported not being able to decrypt this request")]
    CannotDecrypt,
}

impl From<SessionErrorResponse> for RemoteReceiveError {
    fn from(value: SessionErrorResponse) -> Self {
        match value {
            SessionErrorResponse::MissingSession => Self::MissingSession,
            SessionErrorResponse::CryptError => Self::CannotDecrypt,
        }
    }
}

#[derive(Debug, Error)]
pub enum LocalReceiveError<TE: error::Error, DE: error::Error, EE: error::Error, GSE: error::Error>
{
    #[error("Timeout error waiting for response")]
    /// This timeout is separate from anything libp2p's request-response behaviour may throw.
    /// Instead, there is the theoretical possibility that we send a request, then rebuild the
    /// Swarm due to a config change. In this case, the new swarm will not know how to receive
    /// the response and likely surface no error at all. We need to ensure that this form of
    /// error is surfaced to the application, and this is a neat spot to place this logic.
    Timeout,
    #[error("Transport error: {0}")]
    TransportError(#[from] TE),
    #[error("No session available to decrypt response")]
    MissingSession,
    #[error("No recent sessions could decrypt response: {0}")]
    DecryptError(Vec<DE>),
    #[error("Error while trying to resend a request: {0}")]
    ResendError(SessionSendError<EE, GSE>),
}

#[derive(Debug, Error)]
pub enum ReceiveError<TE: error::Error, DE: error::Error, EE: error::Error, GSE: error::Error> {
    #[error("Remote receive error: {0}")]
    RemoteError(RemoteReceiveError),
    #[error("Local receive error: {0}")]
    LocalError(LocalReceiveError<TE, DE, EE, GSE>),
}

pub enum ReceivePayload<R, TE: error::Error, DE: error::Error, EE: error::Error, GSE: error::Error>
{
    Response(Encrypted<R>),
    ReceiveError(ReceiveError<TE, DE, EE, GSE>),
}

#[async_trait]
pub trait SessionSend<Req: Encrypt + Clone> {
    type NodeId;
    type RequestId;
    type DecryptError: error::Error;
    type EncryptError: error::Error;
    type GetSessionError: error::Error;
    type ContactId;
    type ExternalRequestId;

    /// Sends a request. This request will only be retried for remote session errors, because in that case it is clear
    /// that the other end's business logic did not yet process this request.
    async fn send_request(
        &self,
        node_id: Self::NodeId,
        contact_id: Option<Self::ContactId>,
        request: Req,
    ) -> Result<Self::ExternalRequestId, SessionSendError<Self::EncryptError, Self::GetSessionError>>
    where
        Self::RequestId: Eq;

    /// Sends a request. Only use this method if sending this request multiple times has no negative consequences, because the [`SessionSendReceive`]
    /// will attempt to resend the request in many error cases, even in those where the request may have already been processed
    /// by the other end's business logic.
    async fn send_request_idempotent(
        &self,
        node_id: Self::NodeId,
        contact_id: Option<Self::ContactId>,
        request: Req,
    ) -> Result<Self::ExternalRequestId, SessionSendError<Self::EncryptError, Self::GetSessionError>>
    where
        Self::RequestId: Eq;
}

#[async_trait]
pub trait SessionReceive<
    Res: Serialize + DeserializeOwned,
    Req: Encrypt + Clone,
    Reg: RequestRegistry<Req>,
>
{
    type NodeId;
    type RequestId;
    type TransportError: error::Error;
    type DecryptError: error::Error;
    type EncryptError: error::Error;
    type GetSessionError: error::Error;

    /// Receives a request. In error cases, if it semantically makes sense, i.e. it can be guaranteed that the business logic
    /// on the responding side was not executed (e.g. because the message could not be decrypted) and we can send the request again,
    /// the [`SessionSendReceive`] may attempt to resend the request instead of immediately reporting the error. How often this may be attempted
    /// is implementation dependent.
    ///
    /// If the error is local to this node, i.e. we cannot tell if the business logic on the other side ran, maybe because we
    /// cannot decrypt the response, the [`SessionSendReceive`] may only attempt to resend the request if it was originally sent with `send_idempotent`,
    /// meaning the sender was certain that sending the message multiple times would have no negative consequences.
    ///
    /// The request registry needs to be passed in explicitly to avoid overload resoultion ambiguities at callsites, because by
    /// passing it in the `Req` type can be inferred
    // TODO: Not sure if node_id is needed
    async fn receive_encrypted_response(
        &self,
        node_id: Self::NodeId,
        request_id: Self::RequestId,
        request: ReceivePayload<
            Res,
            Self::TransportError,
            Self::DecryptError,
            Self::EncryptError,
            Self::GetSessionError,
        >,
        registry: &Reg,
    );
}

#[async_trait]
impl<
    T,
    Req: Encrypt + Eq + Clone + DeserializeOwned + Sync,
    NodeId: Clone,
    ContactId: Clone + Debug,
    RequestId: Clone + Debug,
    ExternalRequestId: Debug + Send + 'static,
    DecryptError,
    GetSessionError,
    EncryptError,
    S,
> SessionSend<Req> for T
where
    T: RequestSender<Encrypted<Req>, NodeId = NodeId, RequestId = RequestId>,
    T: GetSession<NodeId = NodeId, ContactId = ContactId, Error = GetSessionError, Session = S>,
    S: Session<EncryptError = EncryptError, DecryptError = DecryptError, ContactId = ContactId>,
    T: RequestRegistry<
            Req,
            RequestId = RequestId,
            Session = S,
            ContactId = ContactId,
            ExternalRequestId = ExternalRequestId,
            NodeId = NodeId,
        >,
    DecryptError: error::Error + Send + Sync + 'static,
    // TransportError: error::Error + Send + Sync + 'static,
    EncryptError: error::Error + Send + Sync + 'static,
    GetSessionError: error::Error + Send + Sync + 'static,
    T: Send + Sync,
    S: Send + Sync + Clone,
    Req: Send + 'static,
    ContactId: Send + 'static,
    NodeId: Send + 'static,
    RequestId: Send + 'static,
{
    type NodeId = NodeId;
    type RequestId = RequestId;
    type DecryptError = DecryptError;
    type EncryptError = EncryptError;
    type GetSessionError = Arc<GetSessionError>;
    type ContactId = ContactId;
    type ExternalRequestId = ExternalRequestId;

    #[instrument(skip_all, level = "trace")]
    async fn send_request(
        &self,
        node_id: Self::NodeId,
        contact_id: Option<Self::ContactId>,
        request: Req,
    ) -> Result<Self::ExternalRequestId, SessionSendError<Self::EncryptError, Self::GetSessionError>>
    where
        Self::RequestId: Eq,
        Req: Clone,
    {
        self.send_internal(
            node_id,
            contact_id,
            request,
            self,
            SendInternalConfig::Register { idempotent: false },
        )
        .await
    }

    #[instrument(skip_all, level = "trace")]
    async fn send_request_idempotent(
        &self,
        node_id: Self::NodeId,
        contact_id: Option<Self::ContactId>,
        request: Req,
    ) -> Result<Self::ExternalRequestId, SessionSendError<Self::EncryptError, Self::GetSessionError>>
    where
        Self::RequestId: Eq,
        Req: Clone,
    {
        self.send_internal(
            node_id,
            contact_id,
            request,
            self,
            SendInternalConfig::Register { idempotent: true },
        )
        .await
    }
}

#[async_trait]
impl<
    T,
    Req: Encrypt + Eq + Clone + DeserializeOwned + Send + Sync,
    Res: Serialize + DeserializeOwned + Send + Sync + 'static,
    Reg: RequestRegistry<
            Req,
            RequestId = RequestId,
            ExternalRequestId = ExternalRequestId,
            Session = S,
            ContactId = ContactId,
        > + Sync,
    NodeId: Clone,
    RequestId: Clone + Debug,
    ExternalRequestId: Debug + Send + 'static,
    TransportError,
    DecryptError,
    GetSessionError,
    EncryptError,
    S,
> SessionReceive<Res, Req, Reg> for T
where
    T: RequestSender<Encrypted<Req>, NodeId = NodeId, RequestId = RequestId>,
    T: ResponseReceiver<
            Res,
            NodeId = NodeId,
            TransportError = TransportError,
            DecryptError = DecryptError,
            EncryptError = EncryptError,
            GetSessionError = Arc<GetSessionError>,
            RequestId = ExternalRequestId,
        >,
    T: GetSession<NodeId = NodeId, ContactId = ContactId, Error = GetSessionError, Session = S>,
    S: Session<EncryptError = EncryptError, DecryptError = DecryptError, ContactId = ContactId>,
    T: SessionSend<Req>,
    T: GetSession<NodeId = NodeId, ContactId = ContactId, Error = GetSessionError, Session = S>,
    DecryptError: error::Error + Send + Sync + 'static,
    TransportError: error::Error + Send + Sync + 'static,
    EncryptError: error::Error + Send + Sync + 'static,
    GetSessionError: error::Error + Send + Sync + 'static,
    T: Send + Sync,
    S: Send + Sync + Clone,
    NodeId: Send + 'static,
    RequestId: Send + 'static,
    ExternalRequestId: Send,
    T: SessionSendReceiveInternal<
            Req,
            Reg,
            ContactId = ContactId,
            NodeId = NodeId,
            RequestId = RequestId,
            GetSessionError = Arc<GetSessionError>,
            EncryptError = EncryptError,
            ExternalRequestId = ExternalRequestId,
        >,
{
    type NodeId = NodeId;
    type RequestId = RequestId;
    type TransportError = TransportError;
    type DecryptError = DecryptError;
    type EncryptError = EncryptError;
    type GetSessionError = Arc<GetSessionError>;
    // type ContactId = ContactId;
    // type ExternalRequestId = ExternalRequestId;

    async fn receive_encrypted_response(
        &self,
        node_id: Self::NodeId,
        request_id: Self::RequestId,
        request: ReceivePayload<
            Res,
            Self::TransportError,
            Self::DecryptError,
            Self::EncryptError,
            Self::GetSessionError,
        >,
        registry: &Reg,
    ) {
        let response = match request {
            ReceivePayload::ReceiveError(e) => {
                let Some((retry_count, external_id)) =
                    registry.check_request_metadata(&request_id).await
                else {
                    error!(
                        "Unable to retry request, as either it escaped recording or id {request_id:?} is incorrect"
                    );
                    self.handle_receive_error(node_id, Err(()), e).await;
                    return;
                };
                if retry_count > 0 {
                    warn!("request was already retried, giving up");
                    self.handle_receive_error(node_id, Ok(external_id), e).await;
                    return;
                }
                match &e {
                    ReceiveError::RemoteError(remote_error) => {
                        trace!(
                            ?remote_error,
                            "Error pertains to request that could not have been processed by remote side. Resending"
                        );
                        // `use_for_send` parameter is not important here, we just need to compare sessions. We don't use them here
                        let current_session =
                            self.get_established_session(node_id.clone(), false).await;
                        let Some(RequestForResendAfterSessionError {
                            request: request_to_resend,
                            requires_new_session,
                            contact_id,
                        }) = registry
                            .get_request_for_resend_after_session_error(
                                current_session,
                                request_id.clone(),
                            )
                            .await
                        else {
                            trace!("Unable to resend request, not in registry");
                            self.handle_receive_error(node_id, Ok(external_id), e).await;
                            return;
                        };
                        if requires_new_session {
                            self.clear_session(node_id.clone()).await;
                        }
                        match self
                            .send_internal(
                                node_id.clone(),
                                contact_id,
                                request_to_resend,
                                registry,
                                SendInternalConfig::Update {
                                    old_request_id: request_id.clone(),
                                },
                            )
                            .await
                        {
                            Ok(_unchanged_external_request_id) => {}
                            Err(e) => {
                                warn!(
                                    ?request_id,
                                    "Could not resend request. Error must be handled by business logic"
                                );
                                self.handle_receive_error(
                                    node_id,
                                    Ok(external_id),
                                    ReceiveError::LocalError(LocalReceiveError::ResendError(e)),
                                )
                                .await
                            }
                        }
                    }
                    ReceiveError::LocalError(local_error) => {
                        let Some(RequestForResend {
                            request: request_to_resend,
                            contact_id,
                            idempotent,
                        }) = registry.get_request_for_resend(request_id.clone()).await
                        else {
                            self.handle_receive_error(node_id, Ok(external_id), e).await;
                            return;
                        };
                        if !idempotent {
                            debug!(
                                ?contact_id,
                                "Request was not idempotent and cannot be retried"
                            );
                            self.handle_receive_error(node_id, Ok(external_id), e).await;
                            return;
                        }
                        trace!(
                            ?local_error,
                            "Resending idempotent request after local error"
                        );
                        warn!("Perhaps we should check the retry count here. If this ever appears in a \"loop\", check this code section again");
                        match self
                            .send_internal(
                                node_id.clone(),
                                contact_id,
                                request_to_resend,
                                registry,
                                SendInternalConfig::Update {
                                    old_request_id: request_id.clone(),
                                },
                            )
                            .await
                        {
                            Ok(_unchanged_external_request_id) => {}
                            Err(e) => {
                                warn!(
                                    ?request_id,
                                    "Could not resend request. Error must be handled by business logic"
                                );
                                self.handle_receive_error(
                                    node_id,
                                    Ok(external_id),
                                    ReceiveError::LocalError(LocalReceiveError::ResendError(e)),
                                )
                                .await
                            }
                        }
                    }
                }
                return;
            }
            ReceivePayload::Response(response) => response,
        };

        let removed_request = registry.remove_request(request_id).await;
        if removed_request.is_none() {
            error!("Incoming request could not be mapped back to a tracked request");
        }
        let external_id = removed_request.map(|it| it.external_request_id).ok_or(());

        // Do _not_ preallocate space for all errors, as in most cases no errors are produced, and
        // we get away without a heap allocation
        let mut decrypt_errors = vec![];
        let recent_sessions = self.get_recent_receive_sessions(&node_id).await;
        for session in recent_sessions {
            let decrypted = match session.decrypt(&response).await {
                Err(e) => {
                    decrypt_errors.push(e);
                    continue;
                }
                Ok(decrypted) => decrypted,
            };

            self.receive_response(node_id, session.get_contact_id(), external_id, decrypted).await;
            return;
        }
        match decrypt_errors.len() {
            0 => {
                self.handle_receive_error(
                    node_id,
                    external_id,
                    ReceiveError::LocalError(LocalReceiveError::MissingSession),
                )
                .await
            }
            _ => {
                self.handle_receive_error(
                    node_id,
                    external_id,
                    ReceiveError::LocalError(LocalReceiveError::DecryptError(decrypt_errors)),
                )
                .await
            }
        };
    }
}

#[async_trait]
trait SessionSendReceiveInternal<R: Encrypt + Clone, Reg: RequestRegistry<R>> {
    type NodeId;
    type RequestId;
    type ContactId;
    type ExternalRequestId;
    type EncryptError: error::Error;
    type GetSessionError: error::Error;

    async fn send_internal(
        &self,
        node_id: Self::NodeId,
        contact_id: Option<Self::ContactId>,
        request: R,
        registry: &Reg,
        config: SendInternalConfig<Self::RequestId>,
    ) -> Result<Self::ExternalRequestId, SessionSendError<Self::EncryptError, Self::GetSessionError>>;
}

enum SendInternalConfig<RequestId> {
    Register { idempotent: bool },
    Update { old_request_id: RequestId },
    #[allow(dead_code)] // we may need this in the future
    RegisterOnlyId,
}

#[async_trait]
impl<
    T,
    Req: Encrypt + Clone + DeserializeOwned + Sync + 'static,
    Reg: RequestRegistry<
            Req,
            RequestId = RequestId,
            Session = S,
            ContactId = ContactId,
            ExternalRequestId = ExternalRequestId,
            NodeId = NodeId,
        > + Send
        + Sync,
    NodeId: Clone,
    ContactId: Clone,
    RequestId: Clone + Debug + Send + 'static,
    GetSessionError: error::Error,
    S,
    EncryptError: error::Error,
    ExternalRequestId: Send,
> SessionSendReceiveInternal<Req, Reg> for T
where
    T: RequestSender<Encrypted<Req>, NodeId = NodeId, RequestId = RequestId>,
    T: GetSession<NodeId = NodeId, ContactId = ContactId, Error = GetSessionError, Session = S>,
    S: Session<EncryptError = EncryptError> + Clone,
    T: Send + Sync,
    S: Send + Sync,
    Req: Send + 'static,
    ContactId: Send + 'static,
    NodeId: Send + 'static,
{
    type NodeId = NodeId;
    type RequestId = RequestId;
    type ContactId = ContactId;
    type ExternalRequestId = ExternalRequestId;
    type EncryptError = EncryptError;
    type GetSessionError = Arc<GetSessionError>;

    async fn send_internal(
        &self,
        node_id: Self::NodeId,
        contact_id: Option<Self::ContactId>,
        request: Req,
        registry: &Reg,
        config: SendInternalConfig<Self::RequestId>,
    ) -> Result<Self::ExternalRequestId, SessionSendError<Self::EncryptError, Self::GetSessionError>>
    {
        let session = match self
            .get_or_establish_session(node_id.clone(), contact_id.clone())
            .await
        {
            Err(e) => {
                return Err(SessionSendError::GetSessionError(e));
            }
            Ok(session) => session,
        };
        let encrypted = match session.encrypt(&request).await {
            Err(e) => {
                return Err(SessionSendError::EncryptError(e));
            }
            Ok(encrypted) => encrypted,
        };

        let request_id = self
            .send_encrypted_request(node_id.clone(), encrypted)
            .await;

        let external_request_id = match config {
            SendInternalConfig::Register { idempotent } => {
                registry
                    .register_request(
                        session.clone(),
                        request_id.clone(),
                        node_id,
                        contact_id,
                        request,
                        idempotent,
                    )
                    .await
            }
            SendInternalConfig::RegisterOnlyId => {
                registry.register_request_id(request_id, node_id).await
            }
            SendInternalConfig::Update { old_request_id } => {
                let external_request_id = registry
                    .update_request_id(&old_request_id, request_id.clone())
                    .await;
                match external_request_id {
                    Ok(external_request_id) => external_request_id,
                    Err(_) => {
                        error!(
                            ?old_request_id,
                            "Could not update request id, generating new one"
                        );
                        registry
                            .register_request_id(request_id, node_id)
                            .await
                    }
                }
            }
        };

        Ok(external_request_id)
    }
}
