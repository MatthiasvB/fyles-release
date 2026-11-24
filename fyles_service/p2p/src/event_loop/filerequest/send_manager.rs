use super::PARALLEL_CHUNKS;
use crate::send_receive_traits::session_send_receive::SessionSendError;
use crate::sessions::SessionEstablishmentError;
use crate::source::{Source, SourceWatch};
use crate::types::AttemptId;
use crate::{file_reader::FileReader, types::DataChunk};
use crypto::SerCryptError;
use fyles_core::core::domain_models::{ContactId, FylesId, PendingFile};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{collections::HashMap, hash::Hash};
use tokio_util::sync::CancellationToken;

pub struct SendSlot<ID: Hash + Eq + Clone> {
    path: DataPath<ID>,
    pub pending_file: PendingFile,
    pin: ContactPin,
    /// An occupied slot blocks every future send to that peer, so a stalled one has to be visible.
    /// `last_progress` is bumped in [`Self::advance_for`], the only way to advance the transfer.
    claimed_at: tokio::time::Instant,
    last_progress: tokio::time::Instant,
    /// Latch: report a wedged slot once per episode, not once per sweep.
    stall_reported: bool,
}

/// Whether only a session that proves the target contact may advance the transfer. A session can
/// prove a contact only when we hold its keys, which is also what pins a slot from the start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContactPin {
    Unpinned,
    Pinned,
}

pub enum DataPath<ID: Hash + Eq + Clone> {
    Chunked(SendManager<ID>),
    Stream(StreamPath<ID>),
}

impl<ID: Hash + Eq + Clone> DataPath<ID> {
    pub fn chunked_mut(&mut self) -> Option<&mut SendManager<ID>> {
        match self {
            DataPath::Chunked(send_manager) => Some(send_manager),
            DataPath::Stream(_) => None,
        }
    }
}

pub struct StreamPath<ID: Hash + Eq + Clone> {
    transfer_id: FylesId,
    file_size_bytes: u64,
    control: HashMap<ID, OpeningRequest>,
    pub phase: StreamPhase,
}

pub enum StreamPhase {
    Negotiating(Box<Source>),
    Running(StreamAttempt),
}

/// Dropping it cancels the attempt's task, so whatever clears a slot also stops its stream.
pub struct StreamAttempt {
    pub attempt: AttemptId,
    pub progress: Arc<AtomicU64>,
    cancel: CancellationToken,
}

impl StreamAttempt {
    pub fn new(attempt: AttemptId, offset: u64) -> Self {
        Self {
            attempt,
            progress: Arc::new(AtomicU64::new(offset)),
            cancel: CancellationToken::new(),
        }
    }

    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    pub fn progress_bytes(&self) -> u64 {
        self.progress.load(Ordering::Relaxed)
    }
}

impl Drop for StreamAttempt {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

pub struct SendDataMut<'a, ID: Hash + Eq + Clone> {
    pub path: &'a mut DataPath<ID>,
    pub pending_file: &'a mut PendingFile,
}

impl<ID: Hash + Eq + Clone> SendSlot<ID> {
    pub fn chunked(
        pending_file: PendingFile,
        transfer_id: FylesId,
        source: Source,
        pin: ContactPin,
    ) -> Self {
        Self::with_path(
            DataPath::Chunked(SendManager::new(transfer_id, source)),
            pending_file,
            pin,
        )
    }

    pub fn streamed(
        pending_file: PendingFile,
        transfer_id: FylesId,
        source: Source,
        pin: ContactPin,
    ) -> Self {
        Self::with_path(
            DataPath::Stream(StreamPath {
                transfer_id,
                file_size_bytes: source.stamp().size_bytes,
                control: Default::default(),
                phase: StreamPhase::Negotiating(Box::new(source)),
            }),
            pending_file,
            pin,
        )
    }

    fn with_path(path: DataPath<ID>, pending_file: PendingFile, pin: ContactPin) -> Self {
        let now = tokio::time::Instant::now();
        Self {
            path,
            pending_file,
            pin,
            claimed_at: now,
            last_progress: now,
            stall_reported: false,
        }
    }

    pub fn get_transfer_id(&self) -> &FylesId {
        match &self.path {
            DataPath::Chunked(send_manager) => &send_manager.transfer_id,
            DataPath::Stream(stream) => &stream.transfer_id,
        }
    }

    pub fn get_file_size_bytes(&self) -> u64 {
        match &self.path {
            DataPath::Chunked(send_manager) => send_manager.file_size_bytes(),
            DataPath::Stream(stream) => stream.file_size_bytes,
        }
    }

    pub fn path(&self) -> &DataPath<ID> {
        &self.path
    }

    pub fn running_attempt(&self) -> Option<&StreamAttempt> {
        match &self.path {
            DataPath::Stream(StreamPath {
                phase: StreamPhase::Running(attempt),
                ..
            }) => Some(attempt),
            _ => None,
        }
    }

    pub fn await_response(&mut self, id: ID, request: OpeningRequest) {
        match &mut self.path {
            DataPath::Chunked(send_manager) => send_manager.await_response(id, request.into()),
            DataPath::Stream(stream) => {
                stream.control.insert(id, request);
            }
        }
    }

    /// `None` when `id` was not sent for this transfer, for example by an earlier attempt.
    pub fn claim(&mut self, id: &ID) -> Option<Request> {
        match &mut self.path {
            DataPath::Chunked(send_manager) => send_manager.requests.remove(id),
            DataPath::Stream(stream) => stream
                .control
                .remove(id)
                .map(|it| Request::Control(it.into())),
        }
    }

    /// How long the transfer has been standing still. See the field docs on `last_progress`.
    pub fn stalled_for(&self) -> std::time::Duration {
        self.last_progress.elapsed()
    }

    pub fn held_for(&self) -> std::time::Duration {
        self.claimed_at.elapsed()
    }

    /// Report a stall exactly once per episode. Returns whether the caller should log. A running
    /// stream attempt is exempt, because its own idle timeout already ends it.
    pub fn should_report_stall(&mut self, threshold: std::time::Duration) -> bool {
        if self.running_attempt().is_none()
            && self.stalled_for() > threshold
            && !self.stall_reported
        {
            self.stall_reported = true;
            true
        } else {
            false
        }
    }

    /// `None` when the session's contact may not advance the transfer. A session that proves the
    /// target pins the slot.
    pub fn advance_for<'a>(
        &'a mut self,
        connected_contact_id: &Option<ContactId>,
    ) -> Option<SendDataMut<'a, ID>> {
        if connected_contact_id.as_ref() == Some(&self.pending_file.contact_id) {
            self.pin = ContactPin::Pinned;
        } else if self.pin == ContactPin::Pinned {
            // Handing out nothing is not progress, so a peer that keeps failing the contact check
            // still shows up as stalled.
            return None;
        }
        self.last_progress = tokio::time::Instant::now();
        self.stall_reported = false;
        Some(SendDataMut {
            path: &mut self.path,
            pending_file: &mut self.pending_file,
        })
    }
}

/// Opens an attempt on either path. Only the chunked path also awaits a `Done`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpeningRequest {
    Offer,
    Continue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlRequest {
    Offer,
    Continue,
    Done,
}

impl From<OpeningRequest> for ControlRequest {
    fn from(request: OpeningRequest) -> Self {
        match request {
            OpeningRequest::Offer => ControlRequest::Offer,
            OpeningRequest::Continue => ControlRequest::Continue,
        }
    }
}

/// Only [`SendManager`] hands these out, so an acknowledgement always names a chunk in flight.
#[derive(Debug, PartialEq, Eq)]
pub struct ChunkTicket(u32);

#[derive(Debug, PartialEq, Eq)]
pub enum Request {
    Control(ControlRequest),
    Chunk(ChunkTicket),
}

pub struct SendManager<ID: Hash + Eq + Clone> {
    requests: HashMap<ID, Request>,
    chunks_in_flight: Vec<Slot>,
    earliest_not_sent: u32,
    next_slot: u32,
    file: FileReader,
    source: SourceWatch,
    progress_bytes: u64,
    next_offset: u64,
    seek_to: Option<u64>,
    pub transfer_id: FylesId,
}

impl<ID: Hash + Eq + Clone> SendManager<ID> {
    pub fn new(transfer_id: FylesId, source: Source) -> Self {
        Self {
            transfer_id,
            requests: Default::default(),
            chunks_in_flight: vec![Slot::Empty; PARALLEL_CHUNKS as usize],
            earliest_not_sent: 0,
            next_slot: 0,
            file: FileReader::new(source.file),
            source: source.watch,
            progress_bytes: 0,
            next_offset: 0,
            seek_to: None,
        }
    }

    pub fn await_response(&mut self, id: ID, request: ControlRequest) {
        self.requests.insert(id, Request::Control(request));
    }

    /// The reader seeks before its next read.
    pub fn resume_at(&mut self, offset: u64) {
        self.progress_bytes = offset;
        self.next_offset = offset;
        self.seek_to = Some(offset);
    }
}

pub enum SendRes<ID> {
    NotReady,
    Sent(ID),
    Done,
    IoError(std::io::Error),
    /// When something has gone wrong, like sending didn't work
    Interrupt,
    SourceChanged,
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
        if self.source.changed().await {
            return SendRes::SourceChanged;
        }
        if let Some(offset) = self.seek_to.take() {
            self.file.set_progress(offset).await;
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
            offset: self.next_offset,
            data: data[..num_bytes].to_vec(),
        };

        let Ok(id) = send(chunk).await else {
            return SendRes::Interrupt;
        };
        self.requests
            .insert(id.clone(), Request::Chunk(ChunkTicket(self.next_slot)));
        self.next_offset += num_bytes as u64;
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

    pub fn receive_ack(&mut self, ticket: ChunkTicket) {
        self.mark_done(ticket.0);
    }

    pub fn progress_bytes(&self) -> u64 {
        self.progress_bytes
    }

    pub fn file_size_bytes(&self) -> u64 {
        self.source.stamp().size_bytes
    }

    fn is_ready(&self) -> bool {
        self.next_slot - self.earliest_not_sent < PARALLEL_CHUNKS
    }

    fn mark_done(&mut self, idx: u32) {
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
mod send_slot_tests {
    use super::*;
    use crate::constants::STALL_ERROR_AFTER;
    use fyles_core::core::domain_models::SendStatus;

    fn target() -> ContactId {
        ContactId::for_test(3)
    }

    async fn slot(pin: ContactPin) -> SendSlot<usize> {
        let path = tempfile::NamedTempFile::new()
            .expect("a temp file to be creatable")
            .into_temp_path();
        let source = Source::open(
            tokio::fs::File::open(&path)
                .await
                .expect("the temp file to be openable"),
        )
        .await
        .expect("the temp file to be inspectable");
        SendSlot::chunked(
            PendingFile {
                id: FylesId("file".into()),
                contact_id: target(),
                file_path: path.to_string_lossy().into_owned(),
                retry_count: 0,
                target_filerequest_id: FylesId("fr".into()),
                status: SendStatus::Pending,
                display_name: None,
                interruption_reasons: Vec::new(),
            },
            FylesId("transfer".into()),
            source,
            pin,
        )
    }

    #[tokio::test]
    async fn only_requests_of_this_transfer_can_be_claimed() {
        let mut data = slot(ContactPin::Unpinned).await;
        data.await_response(7, OpeningRequest::Offer);

        assert_eq!(data.claim(&8), None);
        assert_eq!(
            data.claim(&7),
            Some(Request::Control(ControlRequest::Offer))
        );
        assert_eq!(data.claim(&7), None);
    }

    #[tokio::test]
    async fn a_pinned_slot_admits_only_the_target() {
        let mut data = slot(ContactPin::Pinned).await;
        assert!(data.advance_for(&None).is_none());
        assert!(data.advance_for(&Some(ContactId::for_test(119))).is_none());
        assert!(data.advance_for(&Some(target())).is_some());
    }

    #[tokio::test]
    async fn an_unpinned_slot_admits_any_session() {
        let mut data = slot(ContactPin::Unpinned).await;
        assert!(data.advance_for(&Some(ContactId::for_test(119))).is_some());
        assert!(data.advance_for(&None).is_some());
    }

    #[tokio::test]
    async fn a_session_that_proves_the_target_pins_the_slot() {
        let mut data = slot(ContactPin::Unpinned).await;
        assert!(data.advance_for(&Some(target())).is_some());
        assert!(data.advance_for(&None).is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn a_fresh_slot_is_not_stalled() {
        let mut data = slot(ContactPin::Unpinned).await;
        assert!(!data.should_report_stall(STALL_ERROR_AFTER));
    }

    #[tokio::test(start_paused = true)]
    async fn a_wedged_slot_reports_exactly_once() {
        let mut data = slot(ContactPin::Unpinned).await;
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
        let mut data = slot(ContactPin::Unpinned).await;
        tokio::time::advance(STALL_ERROR_AFTER * 2).await;
        assert!(data.should_report_stall(STALL_ERROR_AFTER));

        assert!(data.advance_for(&None).is_some());
        assert!(data.stalled_for() < STALL_ERROR_AFTER);
        assert!(!data.should_report_stall(STALL_ERROR_AFTER));

        // ...and a fresh stall after real progress is reported again, not swallowed by the latch.
        tokio::time::advance(STALL_ERROR_AFTER * 2).await;
        assert!(data.should_report_stall(STALL_ERROR_AFTER));
    }

    /// A peer that keeps failing the contact check makes no progress, so it must still be visible
    /// as stalled — `advance_for` returning `None` is not an advance.
    #[tokio::test(start_paused = true)]
    async fn a_refused_advance_is_not_progress() {
        let mut data = slot(ContactPin::Pinned).await;

        tokio::time::advance(STALL_ERROR_AFTER * 2).await;
        assert!(data.advance_for(&Some(ContactId::for_test(119))).is_none());
        assert!(data.should_report_stall(STALL_ERROR_AFTER));
    }
}

#[cfg(test)]
mod send_manager_tests {
    use super::*;
    use crate::CHUNK_SIZE;
    use crate::constants::SOURCE_FILE_TIMESTAMP_CHECK_INTERVAL;
    use std::io::Write;

    async fn open(path: &std::path::Path) -> Source {
        Source::open(
            tokio::fs::File::open(path)
                .await
                .expect("the temp file to be openable"),
        )
        .await
        .expect("the temp file to be inspectable")
    }

    #[tokio::test(start_paused = true)]
    async fn a_source_that_grows_while_sending_is_reported() {
        let mut source = tempfile::NamedTempFile::new().expect("a temp file to be creatable");
        source
            .write_all(&vec![7u8; CHUNK_SIZE])
            .expect("the temp file to be writable");
        let mut manager =
            SendManager::<usize>::new(FylesId("transfer".into()), open(source.path()).await);

        source
            .write_all(b"more")
            .expect("the temp file to be writable");
        tokio::time::advance(SOURCE_FILE_TIMESTAMP_CHECK_INTERVAL).await;

        let res = manager.send_next_chunk(async |_| Ok(0)).await;
        assert!(matches!(res, SendRes::SourceChanged));
    }

    #[tokio::test]
    async fn chunks_carry_contiguous_offsets_from_the_resume_point() {
        let size = 2 * CHUNK_SIZE + 10;
        let mut source = tempfile::NamedTempFile::new().expect("a temp file to be creatable");
        source
            .write_all(&vec![7u8; size])
            .expect("the temp file to be writable");
        let mut manager =
            SendManager::<usize>::new(FylesId("transfer".into()), open(source.path()).await);
        manager.resume_at(5);

        let mut sent = Vec::new();
        for id in 0.. {
            let mut chunk_seen = None;
            let res = manager
                .send_next_chunk(async |chunk| {
                    chunk_seen = Some((chunk.offset, chunk.data.len()));
                    Ok(id)
                })
                .await;
            let SendRes::Sent(_) = res else {
                break;
            };
            sent.push(chunk_seen.expect("the chunk to be handed to send"));
        }

        let chunk = CHUNK_SIZE as u64;
        assert_eq!(
            sent,
            [(5, CHUNK_SIZE), (5 + chunk, CHUNK_SIZE), (5 + 2 * chunk, 5)]
        );
    }
}
