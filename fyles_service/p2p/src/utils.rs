use derive_more::{Deref, DerefMut, From};
use fyles_core::core::p2p::NodeKeyPairBinary;
use libp2p::identity::{DecodingError, Keypair};



pub fn decode_stored_keys(key_bytes: &NodeKeyPairBinary) -> Result<Keypair, DecodingError> {
    Keypair::from_protobuf_encoding(key_bytes)
}

pub fn encode_keypair(keypair: &Keypair) -> Result<NodeKeyPairBinary, DecodingError> {
    keypair.to_protobuf_encoding()
}

pub fn extract_peer_id(key_bytes: &NodeKeyPairBinary) -> Result<String, DecodingError> {
    // First decode the keys to validate them
    let keypair = decode_stored_keys(key_bytes)?;
    // Then extract and encode the peer ID
    Ok(bs58::encode(keypair.public().to_peer_id().to_bytes()).into_string())
}

/// Helper for the Newtype pattern
#[derive(Deref, DerefMut, Clone, From, Eq, PartialEq)]
pub struct Wrapper<T>(
    #[deref(forward)]
    #[deref_mut(forward)]
    pub T,
);
pub type W<T> = Wrapper<T>;
