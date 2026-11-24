use chacha20poly1305::aead::AeadInPlace;
use chacha20poly1305::{ChaCha20Poly1305, Key, KeyInit, Nonce, Tag};
use hkdf::Hkdf;
use sha2::Sha256;
use thiserror::Error;
use zeroize::Zeroizing;

/// HKDF `info` for the two directions of a file data stream, so each has its own key and both
/// counters can start at zero.
const SENDER_TO_RECEIVER_INFO: &[u8] = b"fyles-file-stream-sender-to-receiver";
const RECEIVER_TO_SENDER_INFO: &[u8] = b"fyles-file-stream-receiver-to-sender";

pub const TAG_LEN: usize = 16;

pub enum Role {
    Sender,
    Receiver,
}

pub struct DirectionKeys {
    outgoing: ChaCha20Poly1305,
    incoming: ChaCha20Poly1305,
}

impl DirectionKeys {
    pub fn derive(attempt: &[u8; 16], key: &[u8; 32], role: Role) -> Self {
        let hk = Hkdf::<Sha256>::new(Some(attempt), key);
        let expand = |info: &[u8]| {
            let mut okm = Zeroizing::new([0u8; 32]);
            hk.expand(info, &mut *okm)
                .expect("32 bytes is a valid HKDF-SHA256 length");
            ChaCha20Poly1305::new(Key::from_slice(okm.as_slice()))
        };
        let sender_to_receiver = expand(SENDER_TO_RECEIVER_INFO);
        let receiver_to_sender = expand(RECEIVER_TO_SENDER_INFO);
        match role {
            Role::Sender => Self {
                outgoing: sender_to_receiver,
                incoming: receiver_to_sender,
            },
            Role::Receiver => Self {
                outgoing: receiver_to_sender,
                incoming: sender_to_receiver,
            },
        }
    }

    pub fn into_halves(self) -> (Sealer, Opener) {
        (
            Sealer {
                cipher: self.outgoing,
                counter: 0,
            },
            Opener {
                cipher: self.incoming,
                counter: 0,
            },
        )
    }
}

#[derive(Debug, Error)]
#[error("stream counter exhausted")]
pub struct CounterExhausted;

#[derive(Debug, Error)]
pub enum OpenError {
    #[error(transparent)]
    CounterExhausted(#[from] CounterExhausted),
    #[error("does not authenticate")]
    Unauthentic,
}

pub struct Sealer {
    cipher: ChaCha20Poly1305,
    counter: u64,
}

pub struct Opener {
    cipher: ChaCha20Poly1305,
    counter: u64,
}

fn nonce(counter: u64) -> Nonce {
    let mut nonce = Nonce::default();
    nonce[..8].copy_from_slice(&counter.to_le_bytes());
    nonce
}

impl Sealer {
    pub fn seal_in_place(
        &mut self,
        plaintext: &mut [u8],
    ) -> Result<[u8; TAG_LEN], CounterExhausted> {
        let next = self.counter.checked_add(1).ok_or(CounterExhausted)?;
        let tag = self
            .cipher
            .encrypt_in_place_detached(&nonce(self.counter), b"", plaintext)
            .expect("ChaCha20-Poly1305 seals anything below 256 GiB");
        self.counter = next;
        Ok(tag.into())
    }
}

impl Opener {
    /// Leaves the plaintext at the front of `sealed` and returns its length.
    pub fn open_in_place(&mut self, sealed: &mut [u8]) -> Result<usize, OpenError> {
        let next = self.counter.checked_add(1).ok_or(CounterExhausted)?;
        let (ciphertext, tag) = sealed
            .split_last_chunk_mut::<TAG_LEN>()
            .ok_or(OpenError::Unauthentic)?;
        self.cipher
            .decrypt_in_place_detached(&nonce(self.counter), b"", ciphertext, Tag::from_slice(tag))
            .map_err(|_| OpenError::Unauthentic)?;
        self.counter = next;
        Ok(ciphertext.len())
    }
}
