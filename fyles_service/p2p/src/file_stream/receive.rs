use asynchronous_codec::{FramedRead, FramedWrite};
use futures::io::{ReadHalf, WriteHalf};
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, SinkExt, StreamExt};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::time::{Instant, timeout};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::constants::{ATTEMPT_OPEN_TIMEOUT, PROGRESS_INTERVAL, STREAM_IDLE_TIMEOUT};
use crate::file_stream::codec::{
    self, DataDecoder, Failure, FrameError, Incoming, Report, ReportEncoder,
};
use crate::types::AttemptGrant;
use crypto::stream::Role;

/// The one writer of a part file. Every byte it holds is correct plaintext, in order, and never
/// past the size the sender declared.
pub struct ReceivingFile {
    file: File,
    written: u64,
    declared: u64,
}

#[derive(Debug)]
pub enum AcceptError {
    BeyondDeclared,
    Io(std::io::Error),
}

impl ReceivingFile {
    pub fn starting(file: File, declared: u64) -> Self {
        Self {
            file,
            written: 0,
            declared,
        }
    }

    /// `None` when the part file already holds more than the transfer declared.
    pub fn resuming(file: File, written: u64, declared: u64) -> Option<Self> {
        (written <= declared).then_some(Self {
            file,
            written,
            declared,
        })
    }

    pub fn written(&self) -> u64 {
        self.written
    }

    pub fn declared(&self) -> u64 {
        self.declared
    }

    pub async fn accept(&mut self, data: &[u8]) -> Result<(), AcceptError> {
        let end = self
            .written
            .checked_add(data.len() as u64)
            .filter(|end| *end <= self.declared)
            .ok_or(AcceptError::BeyondDeclared)?;
        self.file.write_all(data).await.map_err(AcceptError::Io)?;
        self.written = end;
        Ok(())
    }

    pub async fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush().await
    }
}

pub struct ReceiverEnds<S> {
    pub incoming: FramedRead<ReadHalf<S>, DataDecoder>,
    pub reports: FramedWrite<WriteHalf<S>, ReportEncoder>,
}

impl<S: AsyncRead + AsyncWrite> ReceiverEnds<S> {
    pub fn new(stream: S, grant: &AttemptGrant) -> Self {
        let (read, write) = stream.split();
        let (report_encoder, data_decoder) = codec::receiver(grant.keys(Role::Receiver));
        Self {
            incoming: FramedRead::new(read, data_decoder),
            reports: FramedWrite::new(write, report_encoder),
        }
    }
}

#[derive(Debug)]
pub enum BeginError {
    Timeout,
    Closed,
    Frame(FrameError),
    NotBegin,
}

/// Until this succeeds the stream has proven nothing, so it must not touch the transfer it names.
pub async fn await_begin<S: AsyncRead + AsyncWrite + Unpin>(
    ends: &mut ReceiverEnds<S>,
) -> Result<(), BeginError> {
    match timeout(ATTEMPT_OPEN_TIMEOUT, ends.incoming.next()).await {
        Err(_) => Err(BeginError::Timeout),
        Ok(None) => Err(BeginError::Closed),
        Ok(Some(Err(error))) => Err(BeginError::Frame(error)),
        Ok(Some(Ok(Incoming::Begin))) => Ok(()),
        Ok(Some(Ok(Incoming::Data(_) | Incoming::End))) => Err(BeginError::NotBegin),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiveExit {
    Committed,
    FinalizeFailed,
    Unfinished(Unfinished),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unfinished {
    Interrupted,
    Violation,
    IoError,
    Cancelled,
}

async fn tell<S: AsyncWrite + Unpin>(
    reports: &mut FramedWrite<WriteHalf<S>, ReportEncoder>,
    report: Report,
) -> Result<(), ()> {
    match timeout(STREAM_IDLE_TIMEOUT, reports.send(report)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) | Err(_) => Err(()),
    }
}

/// Runs after [`await_begin`]. The sender's `End` comes only after its last check on the source,
/// so the file is finalized on `End`, never merely on reaching the declared size. `finalize` moves
/// the part file into place and records it, and its result decides what the sender is told.
pub async fn receive<S: AsyncRead + AsyncWrite + Unpin>(
    ends: &mut ReceiverEnds<S>,
    file: &mut ReceivingFile,
    mut report: impl FnMut(u64),
    finalize: impl AsyncFnOnce() -> bool,
    cancel: &CancellationToken,
) -> ReceiveExit {
    let mut reported_at = Instant::now();
    let ended = loop {
        let next = tokio::select! {
            biased;
            _ = cancel.cancelled() => break Err(Unfinished::Cancelled),
            next = timeout(STREAM_IDLE_TIMEOUT, ends.incoming.next()) => next,
        };
        let data = match next {
            Err(_) | Ok(None) | Ok(Some(Err(FrameError::Io(_)))) => {
                break Err(Unfinished::Interrupted);
            }
            Ok(Some(Err(FrameError::Unproven(error)))) => {
                warn!(%error, "Data stream ended on a frame that proves nothing");
                break Err(Unfinished::Interrupted);
            }
            Ok(Some(Ok(Incoming::End))) => {
                if file.written == file.declared {
                    break Ok(());
                }
                break Err(Unfinished::Violation);
            }
            Ok(Some(Err(FrameError::Violation(_)))) | Ok(Some(Ok(Incoming::Begin))) => {
                break Err(Unfinished::Violation);
            }
            Ok(Some(Ok(Incoming::Data(data)))) => data,
        };
        match file.accept(&data).await {
            Ok(()) => {
                if reported_at.elapsed() >= PROGRESS_INTERVAL {
                    reported_at = Instant::now();
                    report(file.written);
                    if tell(&mut ends.reports, Report::Progress(file.written))
                        .await
                        .is_err()
                    {
                        break Err(Unfinished::Interrupted);
                    }
                }
            }
            Err(AcceptError::BeyondDeclared) => break Err(Unfinished::Violation),
            Err(AcceptError::Io(_)) => break Err(Unfinished::IoError),
        }
    };
    let flushed = file.flush().await.is_ok();
    match ended {
        Ok(()) => {
            if flushed && finalize().await {
                let _ = tell(&mut ends.reports, Report::Committed).await;
                ReceiveExit::Committed
            } else {
                let _ = tell(&mut ends.reports, Report::Failed(Failure::Internal)).await;
                ReceiveExit::FinalizeFailed
            }
        }
        Err(unfinished) => {
            let failure = match unfinished {
                Unfinished::Violation => Some(Failure::Violation),
                Unfinished::IoError => Some(Failure::Internal),
                Unfinished::Interrupted | Unfinished::Cancelled => None,
            };
            if let Some(failure) = failure {
                let _ = tell(&mut ends.reports, Report::Failed(failure)).await;
            }
            ReceiveExit::Unfinished(unfinished)
        }
    }
}
