use std::error::Error;

use crypto::signed::SignError;
use thiserror::Error;
use tokio::task::JoinError;

use crate::{
    core::{db::DbError, p2p::P2pError, sync::CrdtPersistenceError},
    io_controller::BoxedError,
};

#[derive(Error, Debug)]
pub enum FilerequestError {
    #[error("Database error: {0}")]
    DatabaseError(#[from] DbError),
    #[error("Internal communication error: {0}")]
    InternalCommunicationError(#[from] Box<dyn std::error::Error + Send + Sync>),
    #[error("P2p node error: {0}")]
    P2pNodeError(#[from] P2pError),
    #[error("Owned-device error: {0}")]
    OwnedDeviceError(#[from] CrdtPersistenceError),
    #[error("Generic error: {msg}")]
    GenericError {
        msg: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
    #[error("Host error: {source}")]
    HostError { source: BoxedError },
    #[error("Signing error: {0}")]
    Sign(#[from] SignError),
    #[error("Input error: {0}")]
    InputError(String),
    // Message is English and used only for logs; the UI builds a localized
    // message off the gRPC ALREADY_EXISTS status code, not this string.
    #[error("The folder \"{folder}\" is already used by another file request.")]
    FolderClaimed { folder: String },
}

impl FilerequestError {
    pub fn generic(msg: &str, source: impl Error + Send + Sync + 'static) -> Self {
        Self::GenericError {
            msg: msg.to_string(),
            source: Some(Box::new(source)),
        }
    }
}

impl From<JoinError> for FilerequestError {
    fn from(error: JoinError) -> Self {
        Self::InternalCommunicationError(Box::new(error))
    }
}
