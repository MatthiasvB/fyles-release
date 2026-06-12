use std::path::PathBuf;

use async_trait::async_trait;
use derive_more::{AsRef, Deref, DerefMut, Display, From};
use tokio::fs::File;

use crate::core::{domain_models::FylesId, notification::Notification};

#[derive(Debug, Display, Clone, Deref, DerefMut, From, AsRef)]
pub struct Uri(pub String);
pub type OutDirSubPath = String;
pub type CorrelationId = u64;
pub type FileReadRequester = fn(CorrelationId, Uri) -> Result<(), ()>;
pub type FileWriteRequester = fn(CorrelationId, OutDirSubPath) -> Result<(), ()>;

pub trait CoreHandler
where
    Self: Sized,
{
    fn run(
        file_read_requester: FileReadRequester,
        file_write_requester: FileWriteRequester,
        // args: Config,
    ) -> Self;

    fn share_file(&self, uri: Uri, filerequest_id: FylesId) -> Result<(), ()>;
}

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

    /// Will typically create a directory
    async fn create_filerequest_resource(&self, name: FileNamespace) -> Result<(), BoxedError>;

    async fn rename_resource(
        &self,
        from: FileNamespace,
        to: FileNamespace,
    ) -> Result<(), BoxedError>;

    /// Get a location where a Backup of a single file (likely Sqlite) can be dropped.
    /// If the file already exists, it will be overwritten
    async fn get_path_to_db_backup_file(&self) -> Result<std::fs::File, BoxedError>;

    /// Attempt to remove a source file if it is in a temporary/cache location.
    /// Implementations should only delete files that are within known temp directories.
    async fn remove_source_file_if_temporary(&self, path: &str) -> Result<bool, BoxedError>;

    /// List all files in the temporary outgoing directory along with their modification timestamps (in ms).
    async fn list_temporary_outgoing_files(&self) -> Result<Vec<OutgoingFileInfo>, BoxedError>;

    /// Get a file for writing a partial transfer in a dedicated "Partial" directory.
    async fn get_partial_file_for_writing(&self, filename: &str) -> Result<tokio::fs::File, BoxedError>;

    /// Remove a partial file from the "Partial" directory.
    async fn remove_partial_file(&self, filename: &str) -> Result<(), BoxedError>;

    /// Rename a partial file to its final destination resource.
    async fn finalize_partial_file(&self, partial_filename: &str, final_namespace: FileNamespace) -> Result<(), BoxedError>;

    /// List all partial files in the "Partial" directory.
    async fn list_partial_files(&self) -> Result<Vec<String>, BoxedError>;

    fn send_notification(&self, notification: Notification) -> ();
}

#[cfg(any(test, feature = "test-support"))]
pub mod test {
    use std::path::{Path, PathBuf};

    use tempfile::TempDir;
    use tokio::fs::{self, File};

    use crate::{
        core::notification::Notification,
        io_controller::{BoxedError, FileMeta, FileNamespace, HostController, OutgoingFileInfo, Uri},
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
    }

    #[allow(unused)]
    impl TestHostController {
        pub fn new(out_dir: Option<PathBuf>) -> Self {
            Self {
                base_dir: out_dir
                    .map(HostDir::from)
                    .unwrap_or_else(|| tempfile::tempdir().expect("tempdir").into()),
            }
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

        async fn create_filerequest_resource(
            &self,
            resource: FileNamespace,
        ) -> Result<(), BoxedError> {
            let res_path = Self::resolve_namespace_path(&self.base_path(), &resource);
            Self::ensure_parent_dir(&self.base_path()).await?;
            fs::create_dir_all(res_path)
                .await
                .map_err(|e| Box::new(e) as BoxedError)
        }

        async fn rename_resource(
            &self,
            from: FileNamespace,
            to: FileNamespace,
        ) -> Result<(), BoxedError> {
            let from_p = Self::resolve_namespace_path(self.base_path(), &from);
            let to_p = Self::resolve_namespace_path(self.base_path(), &to);
            Self::ensure_parent_dir(&to_p).await?;
            fs::rename(from_p, to_p)
                .await
                .map_err(|e| Box::new(e) as BoxedError)
        }

        async fn get_path_to_db_backup_file(&self) -> Result<std::fs::File, BoxedError> {
            let backup_path = self.base_path().join("backup");
            fs::create_dir_all(&backup_path).await?;
            std::fs::File::create_new(backup_path).auto_map_err()
        }

        fn send_notification(&self, _notification: Notification) {
            // Do nothing for now
        }

        async fn remove_source_file_if_temporary(&self, _path: &str) -> Result<bool, BoxedError> {
            // Test files are not temporary outgoing files — never delete them
            Ok(false)
        }

        async fn list_temporary_outgoing_files(&self) -> Result<Vec<OutgoingFileInfo>, BoxedError> {
            Ok(vec![])
        }

        async fn get_partial_file_for_writing(&self, filename: &str) -> Result<tokio::fs::File, BoxedError> {
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
                fs::remove_file(p).await.map_err(|e| Box::new(e) as BoxedError)?;
            }
            Ok(())
        }

        async fn finalize_partial_file(&self, partial_filename: &str, final_namespace: FileNamespace) -> Result<(), BoxedError> {
            let from_p = self.base_path().join("Partial").join(partial_filename);
            let to_p = Self::resolve_namespace_path(self.base_path(), &final_namespace);
            Self::ensure_parent_dir(&to_p).await?;
            fs::rename(from_p, to_p).await.map_err(|e| Box::new(e) as BoxedError)
        }

        async fn list_partial_files(&self) -> Result<Vec<String>, BoxedError> {
            Ok(vec![])
        }
    }
}
