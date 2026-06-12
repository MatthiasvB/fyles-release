use crypto::{Encrypted, SerCryptError};

use crate::Chunk;


#[derive(Debug)]
pub enum ReadCryptResult {
    Ok(Chunk, Encrypted<Chunk>),
    /// This is basically an internal error
    AlreadyDone,
    IoError(std::io::Error),
    CryptError(SerCryptError),
    /// Also an internal error
    TaskError,
}
