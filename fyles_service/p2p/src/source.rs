use std::time::UNIX_EPOCH;

use fyles_core::core::domain_models::TransferData;
use tokio::fs::File;
use tokio::time::Instant;

use crate::constants::SOURCE_FILE_TIMESTAMP_CHECK_INTERVAL;

/// The source file as a transfer found it. A file that differs from its stamp has changed since,
/// and resuming would splice two versions together.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceStamp {
    pub size_bytes: u64,
    pub modified_ms: i64,
}

impl SourceStamp {
    pub async fn of(file: &File) -> std::io::Result<Self> {
        let metadata = file.metadata().await?;
        let modified_ms = match metadata.modified()?.duration_since(UNIX_EPOCH) {
            Ok(after) => after.as_millis() as i64,
            Err(before) => -(before.duration().as_millis() as i64),
        };
        Ok(Self {
            size_bytes: metadata.len(),
            modified_ms,
        })
    }

    pub fn recorded_in(transfer_data: &TransferData) -> Self {
        Self {
            size_bytes: transfer_data.file_size_bytes,
            modified_ms: transfer_data.source_modified_ms,
        }
    }
}

pub struct Source {
    pub file: File,
    pub watch: SourceWatch,
}

impl Source {
    pub async fn open(file: File) -> std::io::Result<Self> {
        let (stamp, stat_handle) = tokio::try_join!(SourceStamp::of(&file), file.try_clone())?;
        Ok(Self {
            file,
            watch: SourceWatch {
                stat_handle,
                stamp,
                checked_at: Instant::now(),
            },
        })
    }

    pub fn stamp(&self) -> SourceStamp {
        self.watch.stamp
    }
}

pub struct SourceWatch {
    /// A second handle on the source, since the reader's own lives in its read task.
    stat_handle: File,
    stamp: SourceStamp,
    checked_at: Instant,
}

impl SourceWatch {
    pub fn stamp(&self) -> SourceStamp {
        self.stamp
    }

    /// At most once per [`SOURCE_FILE_TIMESTAMP_CHECK_INTERVAL`], so it can run before every read.
    pub async fn changed(&mut self) -> bool {
        if self.checked_at.elapsed() < SOURCE_FILE_TIMESTAMP_CHECK_INTERVAL {
            return false;
        }
        self.changed_now().await
    }

    pub async fn changed_now(&mut self) -> bool {
        self.checked_at = Instant::now();
        SourceStamp::of(&self.stat_handle)
            .await
            .is_ok_and(|now| now != self.stamp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[tokio::test(start_paused = true)]
    async fn a_source_that_grows_is_noticed_once_the_interval_has_passed() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"before").unwrap();
        let mut source = Source::open(File::open(file.path()).await.unwrap())
            .await
            .unwrap();

        file.write_all(b" and after").unwrap();
        assert!(!source.watch.changed().await);

        tokio::time::advance(SOURCE_FILE_TIMESTAMP_CHECK_INTERVAL).await;
        assert!(source.watch.changed().await);
    }
}
