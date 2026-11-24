pub mod file_tracker;
pub mod receive_manager;
mod send_decision;
pub mod send_manager;
pub mod stream_receiver;

const PARALLEL_CHUNKS: u32 = 8;
