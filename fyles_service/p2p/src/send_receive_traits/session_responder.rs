use crate::behaviour::SessionErrorResponse;
use crate::send_receive_traits::get_session::GetSession;
use crate::send_receive_traits::request_receiver::RequestReceiver;
use crate::send_receive_traits::session::Session;
use async_trait::async_trait;
use crypto::{Encrypted};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::error;
use std::fmt::Debug;
use libp2p::request_response::InboundRequestId;
use thiserror::Error;
use tracing::warn;

#[async_trait]
pub trait SessionResponder<Req: Serialize + DeserializeOwned, Res: Serialize + DeserializeOwned> {
    type NodeId;
    type Session: Session;

    type InboundRequestId;
    async fn decrypt_respond(
        &self,
        node_id: Self::NodeId,
        request: Encrypted<Req>,
        request_id: Self::InboundRequestId
    ) -> Option<
        Result<Encrypted<Res>, SessionResponderError<<Self::Session as Session>::EncryptError>>,
    >;
}

#[derive(Debug, Error)]
pub enum SessionResponderError<EE: error::Error> {
    #[error("Could not decrypt because no session is known")]
    MissingSession,
    #[error("Error during decryption")]
    CouldNotDecrypt,
    #[error("Error during encryption: {0}")]
    EncryptError(EE),
}

impl<EE: std::error::Error> From<SessionResponderError<EE>> for SessionErrorResponse {
    fn from(value: SessionResponderError<EE>) -> Self {
        match value {
            SessionResponderError::MissingSession => SessionErrorResponse::MissingSession,
            SessionResponderError::CouldNotDecrypt => SessionErrorResponse::CryptError,
            SessionResponderError::EncryptError(_) => SessionErrorResponse::CryptError,
        }
    }
}

#[async_trait]
impl<T, Req, Res, NodeId, ContactId, DecryptError, S> SessionResponder<Req, Res> for T
where
    T: GetSession<Session = S, NodeId = NodeId>,
    T: RequestReceiver<Req, Res, NodeId = NodeId, ContactId = ContactId, InboundRequestId = InboundRequestId>,
    T: Send + Sync + 'static,
    DecryptError: Debug + Send + Sync + 'static,
    S: Session<ContactId = ContactId, DecryptError = DecryptError> + Send + Sync + 'static,
    Req: DeserializeOwned + Serialize + Send + Sync + 'static,
    Res: DeserializeOwned + Serialize + Send + Sync + 'static,
    NodeId: Debug + Send + Sync + 'static,
{
    type NodeId = NodeId;
    type Session = S;
    type InboundRequestId = InboundRequestId;

    async fn decrypt_respond(
        &self,
        node_id: Self::NodeId,
        request: Encrypted<Req>,
        request_id: Self::InboundRequestId,
    ) -> Option<Result<Encrypted<Res>, SessionResponderError<S::EncryptError>>> {
        let mut decrypt_errors = vec![];
        let recent_sessions = self.get_recent_receive_sessions(&node_id).await;
        warn!("receive_session_count: {}", recent_sessions.len());
        for session in &recent_sessions {
            let decrypted = match session.decrypt(&request).await {
                Err(e) => {
                    decrypt_errors.push(e);
                    continue;
                }
                Ok(decrypted) => decrypted,
            };

            let Some(response) = self
                .respond_to(node_id, session.get_contact_id(), decrypted, request_id)
                .await
            else {
                return None;
            };
            let encrypted = session.encrypt(&response).await;
            return Some(encrypted.map_err(SessionResponderError::EncryptError));
        }
        match decrypt_errors.len() {
            0 => Some(Err(SessionResponderError::MissingSession)),
            _ => {
                warn!(peer=?node_id, errors=?decrypt_errors, "None of {} sessions was able to decrypt request", recent_sessions.len());
                Some(Err(SessionResponderError::CouldNotDecrypt))
            }
        }
    }
}
