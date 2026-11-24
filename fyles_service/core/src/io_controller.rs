use std::path::PathBuf;
use std::sync::OnceLock;

use async_trait::async_trait;
use derive_more::{AsRef, Deref, DerefMut, Display, From};
use tokio::fs::File;
use tokio::sync::watch;

use crate::core::backup::BackupFileName;
use crate::core::notification::Notification;

pub mod pushed_state;

#[derive(Debug, Display, Clone, Deref, DerefMut, From, AsRef)]
pub struct Uri(pub String);
pub type OutDirSubPath = String;
pub type CorrelationId = u64;
pub type FileReadRequester = fn(CorrelationId, Uri) -> Result<(), ()>;
pub type FileWriteRequester = fn(CorrelationId, OutDirSubPath) -> Result<(), ()>;

#[derive(Debug)]
pub enum FileNamespace {
    Namespace {
        namespace: String,
        child: Option<Box<FileNamespace>>,
    },
    Filename(String),
}

impl From<FileNamespace> for PathBuf {
    fn from(ns: FileNamespace) -> Self {
        match ns {
            FileNamespace::Namespace { namespace, child } => {
                let mut path = PathBuf::from(namespace);
                if let Some(child) = child {
                    path.push(PathBuf::from(*child));
                }
                path
            }
            FileNamespace::Filename(name) => PathBuf::from(name),
        }
    }
}

pub type BoxedError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Filename of the hidden ownership marker written into each filerequest
/// directory. It pins the directory to exactly one filerequest so files from two
/// requests can never be mixed. Written by a dedicated primitive (not the normal
/// file-write path), so it is exempt from the `.`-prefix rejection that applies
/// to user-provided filenames.
pub const OWNER_MARKER_FILENAME: &str = ".fyles-owner";

/// Outcome of a directory rename. A directory rename refuses to overwrite an
/// existing target (so files from two requests can never be merged); the
/// `DestinationOccupied` variant reports that refusal atomically — the same way
/// creation reports a folder collision via [`OwnershipOutcome::Conflict`] — so
/// the caller can surface a clean, localizable error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RenameOutcome {
    /// The rename completed.
    Renamed,
    /// The destination directory already exists; refused to avoid mixing files.
    DestinationOccupied,
}

/// Outcome of claiming/verifying a filerequest directory's `.fyles-owner` marker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnershipOutcome {
    /// The directory already carried our marker. No-op.
    Owned,
    /// The directory had no marker; we wrote ours (created or adopted).
    Adopted,
    /// The directory is owned by a different filerequest. Caller must refuse.
    Conflict { other_id: String },
}

pub struct FileMeta {
    pub file: tokio::fs::File,
    pub path: String,
    pub file_name: String,
}

/// A file found in the temporary outgoing directory, with its last-modified time.
#[derive(Debug, Clone)]
pub struct OutgoingFileInfo {
    pub path: String,
    pub modified_ms: i64,
}

/// A host reading the platform may be unable to answer. `Unsupported` is a property of the build
/// or the machine, `Unknown` only of the moment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostSignal<T> {
    Known(T),
    Unknown,
    Unsupported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PowerSource {
    External,
    Battery,
}

/// Broader than who pays for the bytes: `Metered` also covers a policy that merely asks background
/// work to stay off, such as Low Data Mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkCost {
    Unmetered,
    Metered,
}

/// `Present` means the host has a link it would route over, not that anything on the far side
/// answers. No platform probes. `Absent` is the only reading no setting can override, so a host
/// reports it only where it is sure there is no route at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkStatus {
    Absent,
    Present(NetworkCost),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HostState {
    pub power: HostSignal<PowerSource>,
    pub network: HostSignal<NetworkStatus>,
}

impl HostState {
    /// What a host whose first reading is still in flight starts on. [`Self::UNSUPPORTED`] would
    /// let discretionary work run in the window before that reading lands.
    pub const UNKNOWN: Self = Self {
        power: HostSignal::Unknown,
        network: HostSignal::Unknown,
    };

    pub const UNSUPPORTED: Self = Self {
        power: HostSignal::Unsupported,
        network: HostSignal::Unsupported,
    };
}

#[async_trait]
pub trait HostController: Send + Sync + 'static {
    async fn access_file_for_reading(&self, uri: Uri) -> Result<FileMeta, BoxedError>;

    async fn create_file_for_writing(
        &self,
        uri: FileNamespace,
    ) -> Result<(File, String), BoxedError>;

    // No deduplication
    async fn get_exact_file_for_writing(&self, uri: FileNamespace) -> Result<File, BoxedError>;

    async fn remove_file(&self, uri: FileNamespace) -> Result<(), BoxedError>;

    /// Create the filerequest directory `dir` if it is missing and ensure it
    /// carries an ownership marker (`.fyles-owner`) for `owner_id`. This is the
    /// single atomic "create + claim" operation for a request directory:
    /// - no marker present  → write ours and return `Adopted`
    /// - marker == owner_id → return `Owned`
    /// - marker != owner_id → return `Conflict { other_id }` (caller must refuse),
    ///   unless `force` is set, in which case the marker is overwritten with ours
    ///   and `Adopted` is returned (used to reclaim a directory whose marker
    ///   points at a filerequest that no longer exists).
    ///
    /// The marker is hidden where the platform supports it. Must be idempotent.
    async fn ensure_exists_and_is_owned(
        &self,
        dir: FileNamespace,
        owner_id: &str,
        force: bool,
    ) -> Result<OwnershipOutcome, BoxedError>;

    /// Recursively remove a filerequest directory (marker and all received
    /// files). Used when a filerequest is deleted so its name is fully released.
    async fn remove_filerequest_resource(&self, dir: FileNamespace) -> Result<(), BoxedError>;

    /// Rename `from` to `to`. For file renames the target is deduplicated and the
    /// result is always [`RenameOutcome::Renamed`]. For a directory rename the
    /// target must not already exist (overwriting could mix files from two
    /// requests); when it does, the rename is refused and
    /// [`RenameOutcome::DestinationOccupied`] is returned instead of an error so
    /// the caller can map it to a clean collision message.
    async fn rename_resource(
        &self,
        from: FileNamespace,
        to: FileNamespace,
    ) -> Result<RenameOutcome, BoxedError>;

    /// Creates the file a database backup is written to, overwriting one of the same name.
    async fn get_path_to_db_backup_file(
        &self,
        file_name: &BackupFileName,
    ) -> Result<std::fs::File, BoxedError>;

    /// Attempt to remove a source file if it is in a temporary/cache location.
    /// Implementations should only delete files that are within known temp directories.
    async fn remove_source_file_if_temporary(&self, path: &str) -> Result<bool, BoxedError>;

    /// List all files in the temporary outgoing directory along with their modification timestamps (in ms).
    async fn list_temporary_outgoing_files(&self) -> Result<Vec<OutgoingFileInfo>, BoxedError>;

    /// Get a file for writing a partial transfer in a dedicated "Partial" directory.
    async fn get_partial_file_for_writing(
        &self,
        filename: &str,
    ) -> Result<tokio::fs::File, BoxedError>;

    /// Remove a partial file from the "Partial" directory.
    async fn remove_partial_file(&self, filename: &str) -> Result<(), BoxedError>;

    /// Rename a partial file to its final destination resource. The target is
    /// deduplicated, so the actual on-disk name may differ from the requested
    /// one; the returned `String` is the final on-disk **basename** (not a path).
    async fn finalize_partial_file(
        &self,
        partial_filename: &str,
        final_namespace: FileNamespace,
    ) -> Result<String, BoxedError>;

    /// List all partial files in the "Partial" directory.
    async fn list_partial_files(&self) -> Result<Vec<String>, BoxedError>;

    fn send_notification(&self, notification: Notification) -> ();

    /// Absolute filesystem path of the drop directory (the root under which
    /// filerequest folders live), if this host is backed by a directly
    /// accessible filesystem. Desktop returns the real path so the UI can open
    /// files without knowing where the daemon was pointed; hosts that use
    /// scoped/opaque storage (Android SAF, iOS sandbox) return `None`, and the
    /// client resolves the location itself. Default: `None`.
    fn drop_dir_absolute_path(&self) -> Option<String> {
        None
    }

    /// Gates discretionary background work. Default: a channel that never changes.
    fn host_state(&self) -> watch::Receiver<HostState> {
        static CHANNEL: OnceLock<watch::Sender<HostState>> = OnceLock::new();
        CHANNEL
            .get_or_init(|| watch::channel(HostState::UNSUPPORTED).0)
            .subscribe()
    }
}

#[cfg(any(test, feature = "test-support"))]
pub mod test {
    use std::path::{Path, PathBuf};

    use tempfile::TempDir;
    use tokio::fs::{self, File};
    use tokio::sync::watch;

    use crate::{
        core::backup::BackupFileName,
        core::notification::Notification,
        io_controller::{
            BoxedError, FileMeta, FileNamespace, HostController, HostState, OWNER_MARKER_FILENAME,
            OutgoingFileInfo, OwnershipOutcome, RenameOutcome, Uri,
        },
        library::util::error_handling::AutoMapError,
    };

    #[derive(Debug)]
    enum HostDir {
        Tempdir(TempDir),
        Path(PathBuf),
    }

    impl From<TempDir> for HostDir {
        fn from(tempdir: TempDir) -> Self {
            HostDir::Tempdir(tempdir)
        }
    }

    impl From<PathBuf> for HostDir {
        fn from(path: PathBuf) -> Self {
            HostDir::Path(path)
        }
    }

    impl HostDir {
        fn path(&self) -> &Path {
            match self {
                HostDir::Tempdir(t) => t.path(),
                HostDir::Path(p) => p.as_path(),
            }
        }
    }
    #[derive(Debug)]
    pub struct TestHostController {
        base_dir: HostDir,
        host_state: watch::Sender<HostState>,
    }

    #[allow(unused)]
    impl TestHostController {
        pub fn new(out_dir: Option<PathBuf>) -> Self {
            Self {
                base_dir: out_dir
                    .map(HostDir::from)
                    .unwrap_or_else(|| tempfile::tempdir().expect("tempdir").into()),
                host_state: watch::channel(HostState::UNSUPPORTED).0,
            }
        }

        pub fn set_host_state(&self, state: HostState) {
            self.host_state.send_replace(state);
        }

        fn resolve_namespace_path(
            base: &std::path::Path,
            ns: &FileNamespace,
        ) -> std::path::PathBuf {
            match ns {
                FileNamespace::Filename(name) => base.join(name),
                FileNamespace::Namespace { namespace, child } => {
                    let next = base.join(namespace);
                    if let Some(child) = child {
                        Self::resolve_namespace_path(&next, child.as_ref())
                    } else {
                        next
                    }
                }
            }
        }

        async fn ensure_parent_dir(path: &std::path::Path) -> Result<(), std::io::Error> {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).await?;
            }
            Ok(())
        }

        pub fn base_path(&self) -> &std::path::Path {
            self.base_dir.path()
        }
    }

    use async_trait::async_trait;

    #[async_trait]
    impl HostController for TestHostController {
        async fn access_file_for_reading(&self, uri: Uri) -> Result<FileMeta, BoxedError> {
            let p = if std::path::Path::new(&uri.as_ref()).is_absolute() {
                std::path::PathBuf::from(uri.as_ref())
            } else {
                self.base_path().join(uri.as_ref())
            };
            File::open(p.clone())
                .await
                .map_err(|e| Box::new(e) as BoxedError)
                .map(|f| FileMeta {
                    file: f,
                    path: p.to_string_lossy().to_string(),
                    file_name: p.file_name().unwrap().to_string_lossy().to_string(),
                })
        }

        async fn create_file_for_writing(
            &self,
            uri: FileNamespace,
        ) -> Result<(File, String), BoxedError> {
            let p = Self::resolve_namespace_path(self.base_path(), &uri);
            Self::ensure_parent_dir(&p)
                .await
                .map_err(|_| "parent_dir")?;
            let f = File::create(&p).await.map_err(|_| "create")?;
            Ok((f, p.to_string_lossy().to_string()))
        }

        async fn get_exact_file_for_writing(&self, uri: FileNamespace) -> Result<File, BoxedError> {
            let p = Self::resolve_namespace_path(self.base_path(), &uri);
            Self::ensure_parent_dir(&p)
                .await
                .map_err(|_| "parent_dir")?;
            let f = tokio::fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(&p)
                .await
                .map_err(|_| "create")?;
            Ok(f)
        }

        async fn remove_file(&self, uri: FileNamespace) -> Result<(), BoxedError> {
            let p = Self::resolve_namespace_path(self.base_path(), &uri);
            fs::remove_file(p)
                .await
                .map_err(|e| Box::new(e) as BoxedError)
        }

        async fn rename_resource(
            &self,
            from: FileNamespace,
            to: FileNamespace,
        ) -> Result<RenameOutcome, BoxedError> {
            let is_dir_rename = matches!(&from, FileNamespace::Namespace { child: None, .. });
            let from_p = Self::resolve_namespace_path(self.base_path(), &from);
            let to_p = Self::resolve_namespace_path(self.base_path(), &to);
            // A directory rename refuses to overwrite an existing target.
            if is_dir_rename && to_p.exists() {
                return Ok(RenameOutcome::DestinationOccupied);
            }
            Self::ensure_parent_dir(&to_p).await?;
            fs::rename(from_p, to_p)
                .await
                .map(|()| RenameOutcome::Renamed)
                .map_err(|e| Box::new(e) as BoxedError)
        }

        async fn ensure_exists_and_is_owned(
            &self,
            dir: FileNamespace,
            owner_id: &str,
            force: bool,
        ) -> Result<OwnershipOutcome, BoxedError> {
            let dir_p = Self::resolve_namespace_path(self.base_path(), &dir);
            fs::create_dir_all(&dir_p)
                .await
                .map_err(|e| Box::new(e) as BoxedError)?;
            let marker = dir_p.join(OWNER_MARKER_FILENAME);
            match fs::read_to_string(&marker).await {
                Ok(existing) if existing.trim() == owner_id => Ok(OwnershipOutcome::Owned),
                Ok(existing) if !force => Ok(OwnershipOutcome::Conflict {
                    other_id: existing.trim().to_string(),
                }),
                _ => {
                    fs::write(&marker, owner_id)
                        .await
                        .map_err(|e| Box::new(e) as BoxedError)?;
                    Ok(OwnershipOutcome::Adopted)
                }
            }
        }

        async fn remove_filerequest_resource(&self, dir: FileNamespace) -> Result<(), BoxedError> {
            let dir_p = Self::resolve_namespace_path(self.base_path(), &dir);
            if dir_p.exists() {
                fs::remove_dir_all(&dir_p)
                    .await
                    .map_err(|e| Box::new(e) as BoxedError)?;
            }
            Ok(())
        }

        async fn get_path_to_db_backup_file(
            &self,
            file_name: &BackupFileName,
        ) -> Result<std::fs::File, BoxedError> {
            let backup_dir = self.base_path().join("Backup");
            fs::create_dir_all(&backup_dir).await?;
            std::fs::File::create(backup_dir.join(file_name.as_str())).auto_map_err()
        }

        fn send_notification(&self, _notification: Notification) {
            // Do nothing for now
        }

        fn host_state(&self) -> watch::Receiver<HostState> {
            self.host_state.subscribe()
        }

        async fn remove_source_file_if_temporary(&self, _path: &str) -> Result<bool, BoxedError> {
            // Test files are not temporary outgoing files — never delete them
            Ok(false)
        }

        async fn list_temporary_outgoing_files(&self) -> Result<Vec<OutgoingFileInfo>, BoxedError> {
            Ok(vec![])
        }

        async fn get_partial_file_for_writing(
            &self,
            filename: &str,
        ) -> Result<tokio::fs::File, BoxedError> {
            let p = self.base_path().join("Partial").join(filename);
            Self::ensure_parent_dir(&p).await?;
            let f = tokio::fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(&p)
                .await
                .map_err(|e| Box::new(e) as BoxedError)?;
            Ok(f)
        }

        async fn remove_partial_file(&self, filename: &str) -> Result<(), BoxedError> {
            let p = self.base_path().join("Partial").join(filename);
            if p.exists() {
                fs::remove_file(p)
                    .await
                    .map_err(|e| Box::new(e) as BoxedError)?;
            }
            Ok(())
        }

        async fn finalize_partial_file(
            &self,
            partial_filename: &str,
            final_namespace: FileNamespace,
        ) -> Result<String, BoxedError> {
            let from_p = self.base_path().join("Partial").join(partial_filename);
            let to_p = Self::resolve_namespace_path(self.base_path(), &final_namespace);
            Self::ensure_parent_dir(&to_p).await?;
            let final_name = to_p
                .file_name()
                .ok_or_else(|| -> BoxedError { "Invalid path: no filename".into() })?
                .to_string_lossy()
                .into_owned();
            fs::rename(from_p, to_p)
                .await
                .map(|()| final_name)
                .map_err(|e| Box::new(e) as BoxedError)
        }

        async fn list_partial_files(&self) -> Result<Vec<String>, BoxedError> {
            Ok(vec![])
        }
    }
}
