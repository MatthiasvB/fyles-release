use std::io::SeekFrom;
use std::sync::atomic::{AtomicU64, Ordering};

use asynchronous_codec::{FramedRead, FramedWrite};
use futures::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt, SinkExt, StreamExt};
use tokio::io::{AsyncReadExt as _, AsyncSeekExt, BufReader};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use crate::CHUNK_SIZE;
use crate::constants::{STREAM_FRAME_PAYLOAD, STREAM_IDLE_TIMEOUT};
use crate::file_stream::codec::{self, Failure, FrameError, Outgoing, Report};
use crate::source::Source;
use crate::types::AttemptGrant;
use crypto::stream::Role;

/// The writer and the report reader run inside one task, so `sent` is never touched by two threads
/// at once. It is atomic only because that task's future must be `Send`, and it guards no other
/// data.
const SENT_ORDERING: Ordering = Ordering::Relaxed;

#[derive(Debug)]
pub enum AttemptOutcome {
    Committed,
    ReceiverFailed(Failure),
    ProtocolViolation,
    Unauthentic,
    Interrupted(&'static str),
    SourceChanged,
    SourceIo(std::io::Error),
}

/// Sends `source` from `offset` on and waits for the receiver to settle the attempt. Only an
/// authenticated `Committed` after every byte was sent completes it. `None` when cancelled, since
/// whoever cancels has already decided what becomes of the transfer.
pub async fn run_send_attempt<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    grant: &AttemptGrant,
    source: Source,
    offset: u64,
    progress: &AtomicU64,
    mut report: impl FnMut(u64),
    cancel: &CancellationToken,
) -> Option<AttemptOutcome> {
    let Source {
        mut file,
        mut watch,
    } = source;
    let declared = watch.stamp().size_bytes;
    if let Err(error) = file.seek(SeekFrom::Start(offset)).await {
        return Some(AttemptOutcome::SourceIo(error));
    }
    // A file that grows changes its stamp, which ends the attempt as `SourceChanged`. The cap only
    // keeps the extra bytes off the wire, where the receiver would take them for our violation.
    let mut reader =
        BufReader::with_capacity(CHUNK_SIZE, file.take(declared.saturating_sub(offset)));
    if stream.write_all(grant.attempt.as_bytes()).await.is_err() {
        return Some(AttemptOutcome::Interrupted("could not name the attempt"));
    }
    let (read, write) = stream.split();
    let (data_encoder, report_decoder) = codec::sender(grant.keys(Role::Sender));
    let mut data = FramedWrite::new(write, data_encoder);
    let mut reports = FramedRead::new(read, report_decoder);
    let sent = AtomicU64::new(offset);

    let writer = async {
        if data.send(Outgoing::Begin).await.is_err() {
            return Err(AttemptOutcome::Interrupted("could not begin the attempt"));
        }
        let mut frame = vec![0; STREAM_FRAME_PAYLOAD];
        while sent.load(SENT_ORDERING) < declared {
            if watch.changed().await {
                return Err(AttemptOutcome::SourceChanged);
            }
            let len = match reader.read(&mut frame).await {
                Ok(0) => return Err(AttemptOutcome::SourceChanged),
                Ok(len) => len,
                Err(error) => return Err(AttemptOutcome::SourceIo(error)),
            };
            if data.send(Outgoing::Data(&frame[..len])).await.is_err() {
                return Err(AttemptOutcome::Interrupted("could not send data"));
            }
            sent.fetch_add(len as u64, SENT_ORDERING);
        }
        // The throttled check may not have looked since the last read, and `End` vouches for
        // every byte sent.
        if watch.changed_now().await {
            return Err(AttemptOutcome::SourceChanged);
        }
        if data.send(Outgoing::End).await.is_err() || data.close().await.is_err() {
            return Err(AttemptOutcome::Interrupted("could not finish sending"));
        }
        Ok(())
    };

    let settled = async {
        let mut last = offset;
        loop {
            match timeout(STREAM_IDLE_TIMEOUT, reports.next()).await {
                Err(_) => return AttemptOutcome::Interrupted("receiver went quiet"),
                Ok(None) | Ok(Some(Err(FrameError::Io(_)))) => {
                    return AttemptOutcome::Interrupted("stream closed");
                }
                Ok(Some(Err(FrameError::Unproven(_)))) => return AttemptOutcome::Unauthentic,
                Ok(Some(Err(FrameError::Violation(_)))) => {
                    return AttemptOutcome::ProtocolViolation;
                }
                Ok(Some(Ok(Report::Progress(bytes)))) => {
                    if bytes < last || bytes > sent.load(SENT_ORDERING) {
                        return AttemptOutcome::ProtocolViolation;
                    }
                    last = bytes;
                    progress.store(bytes, Ordering::Relaxed);
                    report(bytes);
                }
                Ok(Some(Ok(Report::Committed))) => {
                    if sent.load(SENT_ORDERING) < declared {
                        return AttemptOutcome::ProtocolViolation;
                    }
                    return AttemptOutcome::Committed;
                }
                Ok(Some(Ok(Report::Failed(failure)))) => {
                    return AttemptOutcome::ReceiverFailed(failure);
                }
            }
        }
    };

    tokio::select! {
        biased;
        _ = cancel.cancelled() => None,
        outcome = settled => Some(outcome),
        Err(outcome) = writer => Some(outcome),
    }
}
