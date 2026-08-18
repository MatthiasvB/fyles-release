use super::PARALLEL_CHUNKS;
use crate::crypto::SessionEstablishmentError;
use crate::send_receive_traits::session_send_receive::SessionSendError;
use crate::{file_reader::FileReader, types::DataChunk};
use crypto::SerCryptError;
use fyles_core::core::domain_models::{ContactId, FylesId, PendingFile};
use std::sync::Arc;
use std::{collections::HashMap, hash::Hash};

pub struct LockedSendData<ID: Hash + Eq + Clone> {
    send_manager: SendManager<ID>,
    pub pending_file: PendingFile,
    /// Whenever a transfer is started (or continued), we check if we know the contact associated
    /// with this filerequest. If so, we must, on each message, check that we are actually talking
    /// to that contact, not an impostor. However, we also want to be able to send files to unknown contacts.
    /// In that case, we can only rely on libp2p's crypto implementation and cannot further pin the target.
    pub must_verify_contact_id: bool,
    /// An occupied slot blocks every future send to that peer, so a stalled one has to be visible.
    /// `last_progress` is bumped in [`Self::unlock_mut`], the only way to mutate the transfer.
    claimed_at: tokio::time::Instant,
    last_progress: tokio::time::Instant,
    /// Latch: report a wedged slot once per episode, not once per sweep.
    stall_reported: bool,
}

pub struct SendDataMut<'a, ID: Hash + Eq + Clone> {
    pub send_manager: &'a mut SendManager<ID>,
    pub pending_file: &'a mut PendingFile,
}

pub struct SendDataRef<'a, ID: Hash + Eq + Clone> {
    pub send_manager: &'a SendManager<ID>,
    pub pending_file: &'a PendingFile,
}

pub struct SendData<ID: Hash + Eq + Clone> {
    pub send_manager: SendManager<ID>,
    pub pending_file: PendingFile,
}

impl<ID: Hash + Eq + Clone> LockedSendData<ID> {
    pub fn new(
        pending_file: PendingFile,
        transfer_id: FylesId,
        file_size_bytes: u64,
        file: tokio::fs::File,
    ) -> Self {
        let now = tokio::time::Instant::now();
        Self {
            send_manager: SendManager::new(transfer_id, file_size_bytes, file),
            pending_file,
            must_verify_contact_id: true, // safe default, must set to false if contact isn't known
            claimed_at: now,
            last_progress: now,
            stall_reported: false,
        }
    }

    pub fn get_transfer_id(&self) -> &FylesId {
        &self.send_manager.transfer_id
    }

    pub fn get_file_size_bytes(&self) -> u64 {
        self.send_manager.file_size_bytes()
    }

    /// How long the transfer has been standing still. See the field docs on `last_progress`.
    pub fn stalled_for(&self) -> std::time::Duration {
        self.last_progress.elapsed()
    }

    pub fn held_for(&self) -> std::time::Duration {
        self.claimed_at.elapsed()
    }

    /// Report a stall exactly once per episode. Returns whether the caller should log.
    pub fn should_report_stall(&mut self, threshold: std::time::Duration) -> bool {
        if self.stalled_for() > threshold && !self.stall_reported {
            self.stall_reported = true;
            true
        } else {
            false
        }
    }

    pub fn unlock_mut<'a>(
        &'a mut self,
        connected_contact_id: &Option<ContactId>,
    ) -> Option<SendDataMut<'a, ID>> {
        let permitted = !self.must_verify_contact_id
            || connected_contact_id
                .as_ref()
                .is_some_and(|connected_id| *connected_id == self.pending_file.contact_id);
        if !permitted {
            // Handing out nothing is not progress — a peer that keeps failing the contact check
            // must still show up as stalled.
            return None;
        }
        self.last_progress = tokio::time::Instant::now();
        self.stall_reported = false;
        Some(SendDataMut {
            send_manager: &mut self.send_manager,
            pending_file: &mut self.pending_file,
        })
    }

    pub fn unlock_into(self, connected_contact_id: &Option<ContactId>) -> Option<SendData<ID>> {
        if !self.must_verify_contact_id
            || connected_contact_id
                .as_ref()
                .is_some_and(|connected_id| *connected_id == self.pending_file.contact_id)
        {
            Some(SendData {
                send_manager: self.send_manager,
                pending_file: self.pending_file,
            })
        } else {
            None
        }
    }

    pub fn unlock(
        &'_ self,
        connected_contact_id: &Option<ContactId>,
    ) -> Option<SendDataRef<'_, ID>> {
        if !self.must_verify_contact_id
            || connected_contact_id
                .as_ref()
                .is_some_and(|connected_id| *connected_id == self.pending_file.contact_id)
        {
            Some(SendDataRef {
                send_manager: &self.send_manager,
                pending_file: &self.pending_file,
            })
        } else {
            None
        }
    }
}

pub struct SendManager<ID: Hash + Eq + Clone> {
    chunk_to_index: HashMap<ID, u32>,
    chunks_in_flight: Vec<Slot>,
    earliest_not_sent: u32,
    next_slot: u32,
    file: FileReader,
    progress_bytes: u64,
    file_size_bytes: u64,
    pub transfer_id: FylesId,
}

impl<ID: Hash + Eq + Clone> SendManager<ID> {
    pub fn new(transfer_id: FylesId, file_size_bytes: u64, file: tokio::fs::File) -> Self {
        let file_reader = FileReader::new(file);
        Self {
            transfer_id,
            chunk_to_index: Default::default(),
            chunks_in_flight: vec![Slot::Empty; PARALLEL_CHUNKS as usize],
            earliest_not_sent: 0,
            next_slot: 0,
            file: file_reader,
            file_size_bytes,
            progress_bytes: 0,
        }
    }

    pub async fn set_progress(&mut self, offset: u64) {
        self.progress_bytes = offset;
        self.file.set_progress(offset).await;
    }
}

pub enum SendRes<ID> {
    NotReady,
    Sent(ID),
    Done,
    IoError(std::io::Error),
    /// When something has gone wrong, like sending didn't work
    Interrupt,
}

#[derive(Clone)]
enum Slot {
    Empty,
    Pending { bytes: u64 },
    Acknowledged { bytes: u64 },
}

impl<ID: Hash + Eq + Clone> SendManager<ID> {
    pub(crate) async fn send_next_chunk(
        &mut self,
        send: impl AsyncFnOnce(
            DataChunk,
        ) -> Result<
            ID,
            SessionSendError<SerCryptError, Arc<SessionEstablishmentError>>,
        >,
    ) -> SendRes<ID> {
        if !self.is_ready() {
            return SendRes::NotReady;
        }
        let Some(read) = self.file.read().await else {
            return if self.earliest_not_sent == self.next_slot
                && !self
                    .chunks_in_flight
                    .iter()
                    .any(|slot| matches!(slot, Slot::Pending { .. }))
            {
                SendRes::Done
            } else {
                SendRes::NotReady
            };
        };

        let (num_bytes, data) = match read {
            Err(e) => return SendRes::IoError(e),
            Ok(read) => read,
        };

        let chunk = DataChunk {
            transfer_uuid: self.transfer_id.clone(),
            data: data[..num_bytes].to_vec(),
            idx: self.next_slot,
        };

        let Ok(id) = send(chunk.clone()).await else {
            return SendRes::Interrupt;
        };
        self.chunk_to_index.insert(id.clone(), self.next_slot);
        let real_index = (self.next_slot % PARALLEL_CHUNKS) as usize;
        assert!(
            real_index < self.chunks_in_flight.len(),
            "Real index {real_index} out of bounds {}",
            self.chunks_in_flight.len()
        );
        self.chunks_in_flight[real_index] = Slot::Pending {
            bytes: num_bytes as u64,
        };
        self.next_slot += 1;
        SendRes::Sent(id)
    }

    /// Acknowlegde reception of file chunk
    pub fn receive_ack(&mut self, id: ID) {
        self.mark_done(id);
    }

    pub fn progress_bytes(&self) -> u64 {
        self.progress_bytes
    }

    pub fn file_size_bytes(&self) -> u64 {
        self.file_size_bytes
    }

    fn is_ready(&self) -> bool {
        self.next_slot - self.earliest_not_sent < PARALLEL_CHUNKS
    }

    fn mark_done(&mut self, id: ID) {
        let idx = self
            .chunk_to_index
            .remove(&id)
            .expect("id to be registered");
        assert!(
            idx >= self.earliest_not_sent && idx <= self.next_slot,
            "Index out of bounds"
        );
        let mut real_index = (idx % PARALLEL_CHUNKS) as usize;
        assert!(
            real_index < self.chunks_in_flight.len(),
            "Real index out of bounds"
        );
        let Slot::Pending { bytes, .. } = self.chunks_in_flight[real_index] else {
            panic!("Chunk is not pending");
        };
        self.chunks_in_flight[real_index] = Slot::Acknowledged { bytes };
        if idx == self.earliest_not_sent {
            while let Slot::Acknowledged { bytes } = self.chunks_in_flight[real_index]
                && self.earliest_not_sent < self.next_slot
            {
                self.progress_bytes += bytes;
                self.chunks_in_flight[real_index] = Slot::Empty;
                self.earliest_not_sent += 1;
                real_index = (self.earliest_not_sent % PARALLEL_CHUNKS) as usize;
            }
        }
    }
}

#[cfg(test)]
mod locked_send_data_tests {
    use super::*;
    use crate::constants::STALL_ERROR_AFTER;
    use fyles_core::core::domain_models::SendStatus;

    async fn slot() -> LockedSendData<usize> {
        let path = tempfile::NamedTempFile::new()
            .expect("a temp file to be creatable")
            .into_temp_path();
        let file = tokio::fs::File::open(&path)
            .await
            .expect("the temp file to be openable");
        let mut data = LockedSendData::new(
            PendingFile {
                id: FylesId("file".into()),
                contact_id: ContactId("contact".into()),
                file_path: path.to_string_lossy().into_owned(),
                retry_count: 0,
                target_filerequest_id: FylesId("fr".into()),
                status: SendStatus::Pending,
                display_name: None,
                interruption_reasons: Vec::new(),
            },
            FylesId("transfer".into()),
            0,
            file,
        );
        // The contact check is orthogonal to what these tests exercise.
        data.must_verify_contact_id = false;
        data
    }

    #[tokio::test(start_paused = true)]
    async fn a_fresh_slot_is_not_stalled() {
        let mut data = slot().await;
        assert!(!data.should_report_stall(STALL_ERROR_AFTER));
    }

    #[tokio::test(start_paused = true)]
    async fn a_wedged_slot_reports_exactly_once() {
        let mut data = slot().await;
        tokio::time::advance(STALL_ERROR_AFTER * 2).await;

        assert!(data.should_report_stall(STALL_ERROR_AFTER));
        // Every subsequent sweep hits the same gate; the latch keeps the log readable.
        assert!(!data.should_report_stall(STALL_ERROR_AFTER));
        assert!(!data.should_report_stall(STALL_ERROR_AFTER));
    }

    /// The property that makes the timestamp trustworthy: advancing the transfer is the *same*
    /// operation as taking a mutable view of it, so progress cannot be made without recording it.
    #[tokio::test(start_paused = true)]
    async fn touching_the_transfer_clears_the_stall() {
        let mut data = slot().await;
        tokio::time::advance(STALL_ERROR_AFTER * 2).await;
        assert!(data.should_report_stall(STALL_ERROR_AFTER));

        assert!(data.unlock_mut(&None).is_some());
        assert!(data.stalled_for() < STALL_ERROR_AFTER);
        assert!(!data.should_report_stall(STALL_ERROR_AFTER));

        // ...and a fresh stall after real progress is reported again, not swallowed by the latch.
        tokio::time::advance(STALL_ERROR_AFTER * 2).await;
        assert!(data.should_report_stall(STALL_ERROR_AFTER));
    }

    /// A peer that keeps failing the contact check makes no progress, so it must still be visible
    /// as stalled — `unlock_mut` returning `None` is not an advance.
    #[tokio::test(start_paused = true)]
    async fn a_refused_unlock_is_not_progress() {
        let mut data = slot().await;
        data.must_verify_contact_id = true;

        tokio::time::advance(STALL_ERROR_AFTER * 2).await;
        assert!(
            data.unlock_mut(&Some(ContactId("impostor".into())))
                .is_none()
        );
        assert!(data.should_report_stall(STALL_ERROR_AFTER));
    }
}
