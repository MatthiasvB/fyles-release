use std::io::{Error, SeekFrom};

use tokio::io::AsyncSeekExt;
use tokio::{fs::File, io::AsyncReadExt, task::JoinHandle};
use tracing::{error, instrument, span, Instrument, Level, Span};

use crate::{chunk, Chunk};

pub(super) struct FileReader {
    pub(super) lazy_buffer: Box<Chunk>,
    pub(super) pending_read: Option<JoinHandle<(File, Box<Chunk>, Result<usize, Error>)>>,
    read_index: usize,
}

impl std::fmt::Debug for FileReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileReader").finish()
    }
}

impl FileReader {
    pub fn new(mut file: File) -> Self {
        let mut busy_buffer = Box::new(chunk());
        Self {
            lazy_buffer: Box::new(chunk()),
            pending_read: Some(tokio::spawn(
                async move {
                    let read_res = file.read(&mut **busy_buffer.as_mut()).await;
                    (file, busy_buffer, read_res)
                }
                .in_current_span(),
            )),
            read_index: 0,
        }
    }

    fn swap(&mut self, busy_buffer: &mut Box<Chunk>) {
        std::mem::swap(&mut *busy_buffer, &mut self.lazy_buffer);
    }

    /// Returns `None` if file reading is done or
    /// `Some(bytes_read, read_bytes)` if bytes where read.
    ///
    /// Will never return `Some` when no bytes were read
    #[instrument(skip_all, level = "trace")]
    pub(super) async fn read(&mut self) -> Option<Result<(usize, &Chunk), Error>> {
        let current_read_index = self.read_index;
        tracing::trace!("Reading chunk {}", current_read_index);
        self.read_index += 1;
        let current_span = Span::current();
        let prepared_read_span = span!(parent: &current_span, Level::TRACE, "prepared read");
        let preload_next_span = span!(parent: &current_span, Level::TRACE, "preloading next chunk", read_index=current_read_index);
        async move {
            match self.pending_read.take() {
                Some(handle) => {
                    let (mut file, mut buffer, read_res) =
                        handle.await.expect("Pending task to complete");
                    self.swap(&mut buffer);
                    if let Ok(bytes_read) = read_res {
                        if bytes_read > 0 {
                            self.pending_read = Some(tokio::spawn(
                                async move {
                                    let read_res = file.read(&mut **buffer.as_mut()).await;
                                    (file, buffer, read_res)
                                }
                                .instrument(preload_next_span),
                            ));
                        }
                    }
                    match read_res {
                        Err(e) => Some(Err(e)),
                        Ok(0) => None,
                        Ok(bytes_read) => Some(Ok((bytes_read, &*self.lazy_buffer))),
                    }
                }
                None => None,
            }
        }
        .instrument(prepared_read_span)
        .await
    }
    #[instrument(skip_all, level = "trace")]
    pub(super) async fn set_progress(&mut self, offset: u64) {
        if let Some(handle) = self.pending_read.take() {
            let (mut file, mut buffer, _read_res) = handle.await.expect("Pending task to complete");
            if let Err(e) = file.seek(SeekFrom::Start(offset)).await {
                error!(errer=?e, "Error while seeking");
            }
            self.pending_read = Some(tokio::spawn(async move {
                let read_res = file.read(&mut **buffer.as_mut()).await;
                (file, buffer, read_res)
            }));
        }
    }
}
