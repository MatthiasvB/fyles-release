use erased_serde::Serialize;
use serde::Serialize as DeriveSerialize;

pub trait SerializeDebug: Serialize + std::fmt::Debug {}

erased_serde::serialize_trait_object!(SerializeDebug);

#[derive(DeriveSerialize, Debug)]
pub struct NoContent;

impl SerializeDebug for NoContent {}

#[derive(DeriveSerialize, Debug)]
pub struct DebugBlob {
    pub swarm: Box<dyn SerializeDebug + Send>,
    pub file_tracker: Box<dyn SerializeDebug + Send>,
}

impl SerializeDebug for DebugBlob {}
