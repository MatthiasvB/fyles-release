use thiserror::Error;

use crate::{
    core::{db::DbError, p2p::P2pError},
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
    #[error("Generic error: {msg}")]
    GenericError {
        msg: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
    #[error("Host error: {source}")]
    HostError { source: BoxedError },
    #[error("Input error: {0}")]
    InputError(String),
}
