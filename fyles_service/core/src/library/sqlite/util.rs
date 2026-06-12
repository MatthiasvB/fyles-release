use crate::core::db::{DbError, DbOperationInfo};
use rusqlite::Result;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot::error::RecvError;

use super::SqliteMsg;

#[derive(Error, Debug)]
pub(super) enum DbOperationError {
    #[error("Rusqlite error: {source}")]
    Rusqlite {
        #[from]
        source: rusqlite::Error,
    },
    #[error("Custom error: {source}")]
    Custom {
        #[from]
        source: DbError,
    },
}

/// Executes [operation] and wraps any rusqlite::Error in a DbError,
/// with the provided, lazily evaluated, [db_info] for context.
pub(super) fn db_op<T>(
    db_info: impl FnOnce() -> DbOperationInfo,
    operation: impl FnOnce() -> Result<T, DbOperationError>,
) -> Result<T, DbError> {
    operation().map_err(|e| match e {
        DbOperationError::Rusqlite { source } => DbError::Operation {
            info: db_info(),
            source: Arc::new(source),
        },
        DbOperationError::Custom { source } => source,
    })
}

impl From<SendError<SqliteMsg>> for DbError {
    fn from(e: SendError<SqliteMsg>) -> Self {
        DbError::Communication {
            source: Arc::new(e),
        }
    }
}

impl From<RecvError> for DbError {
    fn from(e: RecvError) -> Self {
        DbError::Communication {
            source: Arc::new(e),
        }
    }
}
