use async_trait::async_trait;
use chrono::Local;
use derive_more::Deref;
use fyles_core::core::filerequest_drive_handler::GetFileError;
use fyles_core::core::notification::Notification;
use fyles_core::io_controller::{BoxedError, FileMeta, FileNamespace, HostController, OutgoingFileInfo, Uri};
use sanitize_filename::sanitize;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tap::Tap;
use thiserror::Error;
use tokio::fs::{self, File};
use tracing::{error, info, instrument, trace, warn};
use crate::i18n::{init_i18n, Language};
use crate::tr;

#[derive(Debug, Error)]
pub enum FileReadError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
}

#[derive(Debug, Error)]
pub enum FileWriteError {
    #[error("Invalid path: {0}")]
    InvalidPath(String),
    #[error("Not writing to root directory")]
    NotWritingToRoot,
    #[error("Failed to create parent directories: {0}")]
    CreateParentDirs(#[from] io::Error),
    #[error("Could not canonicalize path: {0}")]
    CanonicalizePathFailed(io::Error),
    #[error("Target path resolved outside of parent path")]
    PathOutsideParent,
    #[error(transparent)]
    FilerequestDirError(#[from] FilerequestDirError),
    #[error(transparent)]
    GetFileError(#[from] GetFileError),
}


#[derive(Debug, Error)]
pub enum FileRenameError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Path asymmetry")]
    AsymmetricPaths,
    #[error("Renaming too much")]
    RenamingTooMuch,
    #[error("Target already exists: {0}")]
    TargetAlreadyExists(PathBuf),
    #[error("Could not find unique rename target after {0} attempts")]
    UniqueVariantNotFound(usize),
}

#[derive(Debug, Error)]
pub enum FilerequestDirError {
    #[error("Failed to create filerequest directory {path}: {source}")]
    Create {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("Failed to canonicalize filerequest directory {path}: {source}")]
    Canonicalize {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("Security violation: directory {actual} resolved outside root {root}")]
    Escape { actual: PathBuf, root: PathBuf },
}

#[derive(Debug, Error)]
pub enum DeescalatePathError {
    #[error("Invalid base path {0}")]
    InvalidBasePath(PathBuf),
    #[error("Failed to create directory {path}: {source}")]
    CreateDirectory {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("Failed to find accessible directory variant for {0} after {1} attempts")]
    NoAccessibleVariantFound(PathBuf, usize),
}

#[derive(Debug, Error)]
pub enum DirectFsControllerError {
    #[error("Failed to create output directory {path}: {source}")]
    CreateDirectory {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("Failed to canonicalize output directory {path}: {source}")]
    CanonicalizePath {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error(transparent)]
    DeescalatePathError(#[from] DeescalatePathError),
}

type GetFileResult<T> = Result<T, GetFileError>;
type FilerequestDirResult<T> = Result<T, FilerequestDirError>;
type DeescalatePathResult<T> = Result<T, DeescalatePathError>;

fn io_err(path: &Path, e: io::Error) -> GetFileError {
    GetFileError::Io {
        path: path.to_path_buf(),
        source: e,
    }
}

#[derive(Debug, Error)]
pub enum FileNamespaceError {
    #[error("Path is absolute: {0}")]
    AbsolutePath(String),
}

trait FileNamespaceExt {
    fn parse_sanitized(self) -> Result<PathBuf, FileNamespaceError>;
}

impl FileNamespaceExt for FileNamespace {
    fn parse_sanitized(self) -> Result<PathBuf, FileNamespaceError> {
        let mut path = PathBuf::new();
        let mut current = self;
        loop {
            match current {
                FileNamespace::Namespace { namespace, child } => {
                    path.push(sanitize(namespace));
                    match child {
                        Some(c) => current = *c,
                        None => {
                            break;
                        }
                    }
                }
                FileNamespace::Filename(name) => {
                    path.push(sanitize(name));
                    break;
                }
            }
        }
        if path.is_absolute() {
            Err(FileNamespaceError::AbsolutePath(
                path.to_string_lossy().to_string(),
            ))
        } else {
            Ok(path)
        }
    }
}

pub struct DirectFsController {
    file_drop_dir: PathBuf,
    backup_dir: PathBuf,
    partial_dir: PathBuf,
    lang: Language,
}

impl DirectFsController {
    pub async fn new<P: Into<PathBuf>>(out_dir: P) -> Result<Self, DirectFsControllerError> {
        let requested_path: PathBuf = out_dir.into();
        let out_dir = match fs::metadata(&requested_path).await.is_ok() {
            true => {
                match fs::metadata(&requested_path)
                    .await
                    .map(|m| m.is_dir())
                    .unwrap_or(false)
                {
                    true => match test_write_permission_in_dir(&requested_path).await {
                        true => {
                            trace!(
                                "Output directory {} exists and is writable",
                                requested_path.display()
                            );
                            requested_path
                        }
                        false => {
                            warn!("Missing write permissions. Deescalating to a new directory.");
                            deescalate_existing_inaccessible_path(&requested_path).await?
                        }
                    },
                    false => {
                        warn!("Output path exists but is not a directory. Deescalating.");
                        deescalate_existing_inaccessible_path(&requested_path).await?
                    }
                }
            }
            false => {
                fs::create_dir_all(&requested_path)
                    .await
                    .map_err(|source| DirectFsControllerError::CreateDirectory {
                        path: requested_path.clone(),
                        source,
                    })?;
                requested_path
            }
        };

        let canonical = std::fs::canonicalize(&out_dir).map_err(|source| {
            DirectFsControllerError::CanonicalizePath {
                path: out_dir.clone(),
                source,
            }
        })?;

        if canonical != out_dir {
            info!(
                "Output directory '{}' resolved (canonicalized) to '{}'",
                out_dir.display(),
                canonical.display()
            );
        }

        Ok(Self {
            file_drop_dir: canonical.join(PathBuf::from("Received")),
            backup_dir: canonical.join(PathBuf::from("Backup")),
            partial_dir: canonical.join(PathBuf::from("Partial")),
            lang: init_i18n(),
        })
    }

    pub fn dynamic(self) -> Arc<dyn HostController> {
        Arc::new(DirectFsControllerWrapper(self))
    }
}

impl DirectFsController {
    async fn access_file_for_reading(&self, uri: Uri) -> Result<FileMeta, FileReadError> {
        let path = PathBuf::from(uri.0);
        fs::OpenOptions::new()
            .read(true)
            .open(path.clone())
            .await
            .map_err(|e| {
                error!(?e, "Failed to open file for reading");
                FileReadError::Io(e)
            })
            .map(|f| FileMeta {
                file: f,
                path: path.to_string_lossy().to_string(),
                file_name: path.file_name().unwrap().to_string_lossy().to_string(),
            })
    }

    async fn create_exact_file_for_writing(
        &self,
        uri: FileNamespace,
    ) -> Result<(File, String), FileWriteError> {
        let path = match uri.parse_sanitized() {
            Ok(p) => p,
            Err(e) => {
                error!(?e, "Failed to parse sanitized path");
                return Err(FileWriteError::InvalidPath(format!("{:?}", e)));
            }
        };
        trace!("Sanitized path: {}", path.display());

        let raw_path = self.file_drop_dir.join(path);

        let parent_path = raw_path.parent().ok_or(FileWriteError::NotWritingToRoot)?;
        fs::create_dir_all(parent_path)
            .await
            .map_err(FileWriteError::CreateParentDirs)?;

        let abs_path = std::path::absolute(&raw_path)
            .tap(|r| match r {
                Ok(p) => trace!("Canonical absolute path: {}", p.display()),
                Err(e) => error!("Could not canonicalize path: {e}"),
            })
            .map_err(FileWriteError::CanonicalizePathFailed)?;

        if !abs_path.starts_with(&self.file_drop_dir) {
            return Err(FileWriteError::PathOutsideParent);
        }

        let _out = ensure_filerequest_dir(
            &self.file_drop_dir,
            abs_path.parent().expect("Not writing to root"),
        )
        .await
        .map_err(FileWriteError::FilerequestDirError)?;

        match fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&abs_path)
            .await
        {
            Ok(f) => Ok((f, abs_path.to_string_lossy().into_owned())),
            Err(e) => Err(FileWriteError::GetFileError(GetFileError::Io {
                path: abs_path,
                source: e,
            })),
        }
    }


    async fn create_file_for_writing(
        &self,
        uri: FileNamespace,
    ) -> Result<(File, String), FileWriteError> {
        let path = match uri.parse_sanitized() {
            Ok(p) => p,
            Err(e) => {
                error!(?e, "Failed to parse sanitized path");
                return Err(FileWriteError::InvalidPath(format!("{:?}", e)));
            }
        };
        trace!("Sanitized path: {}", path.display());

        let raw_path = self.file_drop_dir.join(path);

        let parent_path = raw_path.parent().ok_or(FileWriteError::NotWritingToRoot)?;
        fs::create_dir_all(parent_path)
            .await
            .map_err(FileWriteError::CreateParentDirs)?;

        let abs_path = std::path::absolute(&raw_path)
            .tap(|r| match r {
                Ok(p) => trace!("Canonical absolute path: {}", p.display()),
                Err(e) => error!("Could not canonicalize path: {e}"),
            })
            .map_err(FileWriteError::CanonicalizePathFailed)?;

        if !abs_path.starts_with(&self.file_drop_dir) {
            return Err(FileWriteError::PathOutsideParent);
        }

        let _out = ensure_filerequest_dir(
            &self.file_drop_dir,
            abs_path.parent().expect("Not writing to root"),
        )
        .await
        .map_err(FileWriteError::FilerequestDirError)?;

        get_unique_file(
            abs_path.parent().expect("Not writing to root"),
            abs_path
                .file_name()
                .expect("Filename exists")
                .to_string_lossy()
                .as_ref(),
        )
        .await
        .map(|(path, file)| (file, path.to_string_lossy().into()))
        .map_err(FileWriteError::GetFileError)
    }

    async fn remove_file(&self, uri: FileNamespace) -> Result<(), ()> {
        let path = match uri.parse_sanitized() {
            Ok(p) => p,
            Err(e) => {
                error!(?e, "Failed to parse sanitized path");
                return Err(());
            }
        };
        trace!("Sanitized path: {}", path.display());

        let raw_path = self.file_drop_dir.join(path);

        let abs_path = std::path::absolute(&raw_path)
            .tap(|r| match r {
                Ok(p) => trace!("Canonical absolute path: {}", p.display()),
                Err(e) => error!("Could not canonicalize path: {e}"),
            })
            .map_err(FileWriteError::CanonicalizePathFailed)
            .map_err(|_| ())?;

        if !abs_path.starts_with(&self.file_drop_dir) {
            return Err(());
        }

        tokio::fs::remove_file(abs_path).await.map_err(|_| ())
    }

    async fn rename_resource(
        &self,
        from: FileNamespace,
        to: FileNamespace,
    ) -> Result<(), FileRenameError> {
        let mut from_path = self.file_drop_dir.clone();
        let mut to_path = self.file_drop_dir.clone();
        let mut current_from = from;
        let mut current_to = to;
        let is_file_rename;

        loop {
            match (current_from, current_to) {
                (
                    FileNamespace::Namespace {
                        namespace: namespace_from,
                        child: child_from,
                    },
                    FileNamespace::Namespace {
                        namespace: namespace_to,
                        child: child_to,
                    },
                ) => match (child_from, child_to) {
                    (None, None) => {
                        from_path.push(sanitize(namespace_from));
                        to_path.push(sanitize(namespace_to));
                        // Namespace with no child = directory rename
                        is_file_rename = false;
                        break;
                    }
                    (None, Some(_)) | (Some(_), None) => {
                        return Err(FileRenameError::AsymmetricPaths);
                    }
                    (Some(child_from), Some(child_to)) => {
                        if namespace_from != namespace_to {
                            return Err(FileRenameError::RenamingTooMuch);
                        }
                        from_path.push(namespace_from);
                        to_path.push(namespace_to);
                        current_from = *child_from;
                        current_to = *child_to;
                    }
                },
                (FileNamespace::Namespace { .. }, FileNamespace::Filename(_))
                | (FileNamespace::Filename(_), FileNamespace::Namespace { .. }) => {
                    return Err(FileRenameError::AsymmetricPaths);
                }
                (FileNamespace::Filename(name_from), FileNamespace::Filename(name_to)) => {
                    from_path.push(sanitize(name_from));
                    to_path.push(sanitize(name_to));
                    is_file_rename = true;
                    break;
                }
            }
        }

        if fs::metadata(&to_path).await.is_ok() {
            if is_file_rename {
                // For files, find a unique name variant to avoid collisions
                trace!(
                    "Target file {} already exists, finding unique variant",
                    to_path.display()
                );
                to_path = escalate_existing_path(&to_path)
                    .await
                    .map_err(FileRenameError::UniqueVariantNotFound)?;
            } else {
                // For directories, refuse to rename if target already exists
                return Err(FileRenameError::TargetAlreadyExists(to_path));
            }
        }

        fs::rename(&from_path, &to_path)
            .await
            .map_err(FileRenameError::Io)
    }
}

#[instrument(skip_all, level = "trace")]
async fn deescalate_existing_inaccessible_path(path: &Path) -> DeescalatePathResult<PathBuf> {
    let mut to_append = 0;
    let base = path
        .file_name()
        .ok_or_else(|| DeescalatePathError::InvalidBasePath(path.to_path_buf()))?
        .to_string_lossy();
    loop {
        let candidate = path.with_file_name(format!("{}_{}", base, to_append));

        if fs::metadata(&candidate).await.is_ok() {
            if fs::metadata(&candidate)
                .await
                .map(|m| m.is_dir())
                .unwrap_or(false)
                && test_write_permission_in_dir(&candidate).await
            {
                trace!(
                    "Deescalated output directory to existing writable path {}",
                    candidate.display()
                );
                break Ok(candidate);
            }
        } else {
            fs::create_dir_all(&candidate).await.map_err(|source| {
                DeescalatePathError::CreateDirectory {
                    path: candidate.clone(),
                    source,
                }
            })?;
            trace!(
                "Deescalated output directory to new path {}",
                candidate.display()
            );
            break Ok(candidate);
        }

        if to_append >= NUMBER_OF_VARIATIONS_TO_TRY {
            break Err(DeescalatePathError::NoAccessibleVariantFound(
                path.to_path_buf(),
                NUMBER_OF_VARIATIONS_TO_TRY,
            ));
        }
        to_append += 1;
    }
}

#[instrument(skip_all, level = "trace")]
async fn test_write_permission_in_dir(dir: &Path) -> bool {
    use std::time::{SystemTime, UNIX_EPOCH};

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let pid = std::process::id() as u128;
    // Simple mixed value for a "random enough" suffix without external crates.
    let mixed = nanos ^ (pid.rotate_left(17));
    let suffix = format!("{mixed:x}");
    let test_file = dir.join(format!(".test_write_permissions_{suffix}"));
    trace!(
        "Testing write permissions by creating {}",
        test_file.display()
    );
    match fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&test_file)
        .await
    {
        Ok(_) => fs::remove_file(&test_file).await.is_ok(),
        Err(_) => false,
    }
}

const NUMBER_OF_VARIATIONS_TO_TRY: usize = 100;

/// Validate that `filename` is a bare basename (no path separators, not `.`/`..`).
///
/// Partial filenames are derived from the wire-supplied `transfer_uuid`, so even though
/// callers sanitize upstream we defend the join here to keep these file primitives safe
/// in isolation (the stale-cleanup sweep calls them without going through the drive handler).
fn validate_partial_basename(filename: &str) -> Result<(), BoxedError> {
    let is_bare_basename = Path::new(filename)
        .file_name()
        .is_some_and(|name| name == std::ffi::OsStr::new(filename));
    if !is_bare_basename {
        return Err(format!("Refusing partial filename that is not a bare basename: '{filename}'").into());
    }
    Ok(())
}

// Helper: create (or deescalate) a unique file for writing.
// Ensures final path is strictly inside base_dir.
async fn get_unique_file(base_dir: &Path, filename: &str) -> GetFileResult<(PathBuf, File)> {
    if fs::metadata(base_dir).await.is_err() {
        return Err(GetFileError::BaseDirMissing {
            base: base_dir.to_path_buf(),
        });
    }

    let mut path = base_dir.join(filename);

    if !path.starts_with(base_dir) {
        return Err(GetFileError::ConstructedPathEscapedBaseDirectory);
    }

    if fs::metadata(&path).await.is_ok() {
        trace!("File {} exists, deescalating name", path.display());
        path = escalate_existing_file(&path).await?;
    }

    match fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .await
    {
        Ok(f) => Ok((path, f)),
        Err(e) => Err(io_err(&path, e)),
    }
}

#[instrument(skip_all, level = "trace")]
async fn escalate_existing_file(path: &Path) -> GetFileResult<PathBuf> {
    let file_name = path
        .file_name()
        .ok_or_else(|| GetFileError::Io {
            path: path.to_path_buf(),
            source: io::Error::new(io::ErrorKind::InvalidInput, "invalid file path"),
        })?
        .to_string_lossy()
        .to_string();

    // Split stem + extension
    let (stem, ext) = match file_name.rsplit_once('.') {
        Some((s, e)) => (s, Some(e)),
        None => (file_name.as_str(), None),
    };

    for i in 0..NUMBER_OF_VARIATIONS_TO_TRY {
        let candidate_name = match &ext {
            Some(e) => format!("{}_{}.{}", stem, i, e),
            None => format!("{}_{}", stem, i),
        };
        let candidate = path.with_file_name(candidate_name);
        if fs::metadata(&candidate).await.is_err() {
            return Ok(candidate);
        }
    }
    Err(GetFileError::UniqueFileNameVariantNotFound {
        base: path.to_path_buf(),
    })
}

/// Find a unique path variant for a file by appending `_N` suffixes.
/// Returns `Err(NUMBER_OF_VARIATIONS_TO_TRY)` if no unique variant was found.
#[instrument(skip_all, level = "trace")]
async fn escalate_existing_path(path: &Path) -> Result<PathBuf, usize> {
    let file_name = path
        .file_name()
        .expect("escalate_existing_path called with path without file name")
        .to_string_lossy()
        .to_string();

    // Split stem + extension (for files like "photo.jpg" -> ("photo", Some("jpg")))
    let (stem, ext) = match file_name.rsplit_once('.') {
        Some((s, e)) => (s, Some(e)),
        None => (file_name.as_str(), None),
    };

    for i in 0..NUMBER_OF_VARIATIONS_TO_TRY {
        let candidate_name = match &ext {
            Some(e) => format!("{}_{}.{}", stem, i, e),
            None => format!("{}_{}", stem, i),
        };
        let candidate = path.with_file_name(candidate_name);
        if fs::metadata(&candidate).await.is_err() {
            return Ok(candidate);
        }
    }
    Err(NUMBER_OF_VARIATIONS_TO_TRY)
}

#[instrument(skip_all, level = "trace")]
async fn ensure_filerequest_dir(
    out_dir: &PathBuf,
    safe_dir_name: &Path,
) -> FilerequestDirResult<PathBuf> {
    let dir = out_dir.join(safe_dir_name);

    if fs::metadata(&dir).await.is_err() {
        fs::create_dir_all(&dir)
            .await
            .map_err(|e| FilerequestDirError::Create {
                path: dir.clone(),
                source: e,
            })?;
    }

    match std::fs::canonicalize(&dir) {
        Ok(actual) => {
            if !actual.starts_with(out_dir) {
                return Err(FilerequestDirError::Escape {
                    actual,
                    root: out_dir.clone(),
                });
            }
            Ok(actual)
        }
        Err(e) => Err(FilerequestDirError::Canonicalize {
            path: dir,
            source: e,
        }),
    }
}

#[derive(Deref)]
#[repr(transparent)]
struct DirectFsControllerWrapper(DirectFsController);

#[async_trait]
impl HostController for DirectFsControllerWrapper {
    async fn access_file_for_reading(&self, uri: Uri) -> Result<FileMeta, BoxedError> {
        trace!("Accessing file for reading: {:?}", uri);
        self.0
            .access_file_for_reading(uri)
            .await
            .map_err(|e| Box::new(e) as BoxedError)
    }

    async fn create_file_for_writing(
        &self,
        uri: FileNamespace,
    ) -> Result<(File, String), BoxedError> {
        trace!("Creating file for writing: {:?}", uri);
        self.0
            .create_file_for_writing(uri)
            .await
            .map_err(|e| Box::new(e) as BoxedError)
    }

    async fn get_exact_file_for_writing(&self, uri: FileNamespace) -> Result<File, BoxedError> {
        trace!("Creating exact file for writing: {:?}", uri);
        self.0
            .create_exact_file_for_writing(uri)
            .await
            .map_err(|e| Box::new(e) as BoxedError)
            .map(|it| it.0)
    }

    async fn remove_file(&self, uri: FileNamespace) -> Result<(), BoxedError> {
        trace!("Removing file for writing {uri:?}");
        self.0
            .remove_file(uri)
            .await
            .map_err(|_| "Could not remove file".into())
    }

    async fn create_filerequest_resource(&self, name: FileNamespace) -> Result<(), BoxedError> {
        trace!("Creating filerequest resource: {:?}", name);
        let safe_path = name
            .parse_sanitized()
            .map_err(|e| Box::new(e) as BoxedError)?;
        let dir = ensure_filerequest_dir(&self.0.file_drop_dir, &safe_path).await?;
        fs::create_dir_all(dir)
            .await
            .map_err(|e| Box::new(e) as BoxedError)?;
        Ok(())
    }

    async fn rename_resource(
        &self,
        from: FileNamespace,
        to: FileNamespace,
    ) -> Result<(), BoxedError> {
        trace!("Renaming resource from {:?} to {:?}", from, to);
        self.0
            .rename_resource(from, to)
            .await
            .map_err(|e| Box::new(e) as BoxedError)
    }

    async fn get_path_to_db_backup_file(&self) -> Result<std::fs::File, BoxedError> {
        fs::create_dir_all(self.backup_dir.clone()).await?;
        let date = Local::now();
        let backup_path = self.backup_dir.join(PathBuf::from(format!(
            "{}_fyles.bak",
            // Current date in "yyyy-MM-dd" format
            date.format("%Y-%m-%d")
        )));
        let backup_file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&backup_path)?;
        Ok(backup_file)
    }

    async fn remove_source_file_if_temporary(&self, _path: &str) -> Result<bool, BoxedError> {
        // Desktop doesn't copy files to a temporary outgoing cache, so we return false
        Ok(false)
    }

    async fn list_temporary_outgoing_files(&self) -> Result<Vec<OutgoingFileInfo>, BoxedError> {
        // Desktop doesn't copy files to a temporary outgoing cache
        Ok(vec![])
    }

    async fn get_partial_file_for_writing(&self, filename: &str) -> Result<File, BoxedError> {
        validate_partial_basename(filename)?;
        fs::create_dir_all(&self.0.partial_dir).await?;
        let p = self.0.partial_dir.join(filename);
        let f = fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&p)
            .await?;
        Ok(f)
    }

    async fn remove_partial_file(&self, filename: &str) -> Result<(), BoxedError> {
        validate_partial_basename(filename)?;
        let p = self.0.partial_dir.join(filename);
        if p.exists() {
            fs::remove_file(p).await?;
        }
        Ok(())
    }

    async fn finalize_partial_file(&self, partial_filename: &str, final_namespace: FileNamespace) -> Result<(), BoxedError> {
        validate_partial_basename(partial_filename)?;
        let from_p = self.0.partial_dir.join(partial_filename);

        let safe_path = final_namespace
            .parse_sanitized()
            .map_err(|e| Box::new(e) as BoxedError)?;

        // safe_path is relative like "RequestTitle/file.txt" — split into dir and file parts
        let filename = safe_path
            .file_name()
            .ok_or_else(|| -> BoxedError { "Invalid path: no filename".into() })?;
        let dir_rel = safe_path.parent().unwrap_or_else(|| Path::new(""));

        // ensure_filerequest_dir creates the directory, canonicalizes it, and checks for path escape
        let dest_dir = ensure_filerequest_dir(&self.0.file_drop_dir, dir_rel).await?;

        let raw_to = dest_dir.join(filename);

        let to_p = if raw_to.exists() {
            escalate_existing_path(&raw_to)
                .await
                .map_err(|n| -> BoxedError { format!("Could not find unique filename after {n} tries").into() })?
        } else {
            raw_to
        };

        fs::rename(from_p, to_p).await?;
        Ok(())
    }

    async fn list_partial_files(&self) -> Result<Vec<String>, BoxedError> {
        if !self.0.partial_dir.exists() {
            return Ok(vec![]);
        }
        
        let mut entries = fs::read_dir(&self.0.partial_dir).await?;
        let mut files = Vec::new();
        
        while let Some(entry) = entries.next_entry().await? {
            if let Ok(name) = entry.file_name().into_string() {
                files.push(name);
            }
        }
        
        Ok(files)
    }

    fn send_notification(&self, notification: Notification) {
        trace!("Sending notification: {:?}", notification);
        let (title, body): (&str, &str) = match notification {
            Notification::FileReceived {
                contact_name,
                filerequest_name,
                file_name,
                file_size,
            } => {
                let (file_size_value, file_size_unit) = crate::i18n::format_file_size(file_size);

                (
                    &tr!(&self.lang, "file-received-title"),
                    &match contact_name {
                        Some(name) => {
                            tr!(
                                &self.lang,
                                "file-received-mess-with-contact",
                                senderName = name,
                                fileName = file_name,
                                fileSizeValue = file_size_value,
                                fileSizeUnit = file_size_unit,
                                requestName = filerequest_name
                            )
                        }
                        None => tr!(
                            &self.lang,
                            "received-file-via-filerequest",
                            fileName = file_name,
                            fileSizeValue = file_size_value,
                            fileSizeUnit = file_size_unit,
                            requestName = filerequest_name
                        ),
                    },
                )
            }
        };
        match notify_rust::Notification::new()
            .summary(title)
            .body(body)
            .show()
        {
            Ok(_) => {}
            Err(e) => error!("Failed to send notification: {}", e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fyles_core::core::{
        domain_models::{FilerequestAccess, FylesId},
        filerequest_drive_handler::FilerequestDriveHandler,
    };
    use std::fs;
    use tempfile::TempDir;

    fn deep_root_path(tmp: &TempDir) -> PathBuf {
        let mut p = tmp.path().to_path_buf();
        for i in 0..12 {
            p.push(format!("level_{i}"));
        }
        p
    }

    fn make_filerequest(name: &str) -> fyles_core::core::domain_models::Filerequest {
        fyles_core::core::domain_models::Filerequest {
            id: FylesId::new(),
            title: name.to_string(),
            description: "".into(),
            access: FilerequestAccess::Public,
            is_active: true,
        }
    }

    async fn setup(name: &str) -> (TempDir, PathBuf, FilerequestDriveHandler) {
        let tmp = TempDir::new().unwrap();
        let root = deep_root_path(&tmp);
        fs::create_dir_all(&root).unwrap();
        assert!(
            root.components().count() > 5,
            "Root not deep enough: {}",
            root.display()
        );
        let handler = DirectFsController::new(root.to_string_lossy().to_string())
            .await
            .unwrap();
        let fr = make_filerequest(name);
        let fr_handler = FilerequestDriveHandler::new(handler.dynamic(), fr);
        (tmp, root, fr_handler)
    }

    fn assert_inside(root: &Path, p: &Path) {
        let canon_root = fs::canonicalize(root).unwrap();
        let canon_p = fs::canonicalize(p).unwrap();
        assert!(
            canon_p.starts_with(&canon_root),
            "Path {} escaped root {}",
            canon_p.display(),
            canon_root.display()
        );
    }

    fn dir_file_count(dir: &Path) -> usize {
        fs::read_dir(dir)
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .map(|e| e.file_type().map(|ft| ft.is_file()).unwrap_or(false))
                    .unwrap_or(false)
            })
            .count()
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn outdir_deescalates_non_writable_directory() {
        use std::os::unix::fs::PermissionsExt;

        // Prepare a non-writable directory
        let tmp = TempDir::new().unwrap();
        let base = deep_root_path(&tmp);
        fs::create_dir_all(&base).unwrap();
        let target = base.join("requested_dir");
        std::fs::create_dir(&target).unwrap();
        let mut perms = std::fs::metadata(&target).unwrap().permissions();
        perms.set_mode(0o555); // read/exec only
        std::fs::set_permissions(&target, perms).unwrap();

        // Construct handler pointing at the non-writable directory
        let handler = DirectFsController::new(target.to_string_lossy().to_string())
            .await
            .expect("handler creation");
        // Should have deescalated (added _0)
        assert_ne!(
            handler.file_drop_dir,
            std::fs::canonicalize(&target).unwrap(),
            "Expected deescalation to a different directory"
        );
        assert!(
            handler
                .file_drop_dir
                .parent() // Avoid getting nested "Received" dir
                .unwrap()
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("requested_dir_"),
            "Expected suffix added, got {}",
            handler.file_drop_dir.display()
        );

        // Confirm new directory is writable by creating a test file
        let test_file = handler.file_drop_dir.parent().unwrap().join("write_probe");
        tokio::fs::write(&test_file, b"ok")
            .await
            .expect("write probe");
        assert!(test_file.exists());
    }

    #[tokio::test]
    #[cfg(unix)]
    #[test_log::test]
    async fn outdir_deescalates_multiple_non_writable_directories() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = TempDir::new().unwrap();
        let base = deep_root_path(&tmp);
        std::fs::create_dir_all(&base).unwrap();

        let original = base.join("zap");
        let first = base.join("zap_0");

        for d in [&original, &first] {
            std::fs::create_dir(d).unwrap();
            let mut perms = std::fs::metadata(d).unwrap().permissions();
            perms.set_mode(0o555);
            std::fs::set_permissions(d, perms).unwrap();
        }

        let handler = DirectFsController::new(original.to_string_lossy().to_string())
            .await
            .expect("handler creation");

        // Should have skipped zap and zap_0 -> landing on zap_1 (or higher if already present)
        let fname = handler
            .file_drop_dir
            .parent() // To avoid looking at the "Received" dir
            .unwrap()
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        assert!(
            fname.starts_with("zap_"),
            "Expected deescalated directory starting with zap_, got {}",
            fname
        );
        assert_ne!(
            fname, "zap",
            "Should not be original non-writable directory name"
        );
        assert_ne!(
            fname, "zap_0",
            "Should not be first non-writable variant name"
        );

        // Writable check
        let probe = handler.file_drop_dir.parent().unwrap().join("probe_file");
        tokio::fs::write(&probe, b"x").await.expect("write probe");
        assert!(probe.exists());
    }

    // This test ensures that empty filerequest names (or such that become empty after sanitization) are
    // - not leading to security violations
    // - still result in files being created inside a safe directory
    // It does this by falling back to the filerequest's unique ID when the sanitized name is empty
    #[tokio::test]
    async fn filerequest_name_collapses_safely_when_sanitized() {
        let (_tmp, root, frh) = setup(":::::://///\\\\\\*****???").await;
        let (p, f) = frh.get_file_for_writing("still.txt").await.unwrap();
        drop(f);
        let p = PathBuf::from(p);
        assert_inside(&root, p.parent().unwrap());
    }

    #[tokio::test]
    async fn rename_files_with_identical_names() {
        let (_tmp, root, frh) = setup("multi").await;
        let mut dir: Option<PathBuf> = None;
        let iterations = 12;
        let mut created = 0;
        for _ in 0..iterations {
            let (p, f) = frh.get_file_for_writing("file.txt").await.unwrap();
            drop(f);
            if dir.is_none() {
                let p = PathBuf::from(p);
                dir = Some(p.parent().unwrap().to_path_buf());
            }
            created += 1;
        }
        let dir = dir.unwrap();
        let count = dir_file_count(&dir);
        assert!(
            count == created,
            "Directory should have exactly {created} files, found {count}"
        );
        assert_inside(&root, &dir);
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn symlink_escape_detected() {
        use std::os::unix::fs as unix_fs;

        let (_tmp, root, frh) = setup("symlink-attack").await;

        // First create a normal file to ensure the directory exists.
        let (first_path, f) = frh
            .get_file_for_writing("first.txt")
            .await
            .expect("initial file");
        drop(f);

        let first_path = PathBuf::from(first_path);
        let fr_dir = first_path.parent().unwrap().to_path_buf();

        // Clean up: remove created file and directory so we can replace it with a symlink.
        std::fs::remove_file(&first_path).expect("remove first file");
        std::fs::remove_dir(&fr_dir).expect("remove original fr dir");

        // Point the symlink to the parent of the allowed root (or root itself if no parent).
        let target = root.parent().unwrap_or(&root);
        unix_fs::symlink(target, &fr_dir).expect("create symlink attack");

        // Attempt to create another file; should now fail with security violation.
        let res = frh.get_file_for_writing("second.txt").await;
        assert!(
            matches!(res, Err(GetFileError::FilerequestDir(_))),
            "Expected directory Escape error, got {res:?}"
        );
    }

    #[tokio::test]
    async fn traversal_in_filerequest_name_does_not_escape() {
        let (_tmp, root, frh) = setup("../../../../etc/passwd").await;
        let (path, f) = frh
            .get_file_for_writing("legit.txt")
            .await
            .expect("file create");
        drop(f);
        let path = PathBuf::from(path);
        let parent = path.parent().unwrap();
        assert_inside(&root, parent);
    }

    #[tokio::test]
    #[ignore = "There is a major issue (probably across host controller implementations) that lets multiple filerequests write to the same directory if their names are identical (possibly only after sanitization). This needs to be fixed."]
    async fn different_filerequests_with_same_sanitized_names_dont_collide() {
        let tmp = TempDir::new().unwrap();
        let root = deep_root_path(&tmp);
        fs::create_dir_all(&root).unwrap();

        let handler1 = DirectFsController::new(root.to_string_lossy().to_string())
            .await
            .unwrap();
        let handler2 = DirectFsController::new(root.to_string_lossy().to_string())
            .await
            .unwrap();

        // Create two different filerequests with names that sanitize to the same result
        let fr1 = fyles_core::core::domain_models::Filerequest {
            id: FylesId::new(),
            title: "Test/Request".to_string(), // Will sanitize to "TestRequest"
            description: "".into(),
            access: FilerequestAccess::Public,
            is_active: true,
        };

        let fr2 = fyles_core::core::domain_models::Filerequest {
            id: FylesId::new(),
            title: "Test\\Request".to_string(), // Will also sanitize to "TestRequest"
            description: "".into(),
            access: FilerequestAccess::Public,
            is_active: true,
        };

        let frh1 = FilerequestDriveHandler::new(handler1.dynamic(), fr1);
        let frh2 = FilerequestDriveHandler::new(handler2.dynamic(), fr2);

        // Create files in both filerequests
        let (path1, f1) = frh1.get_file_for_writing("file1.txt").await.unwrap();
        drop(f1);
        let (path2, f2) = frh2.get_file_for_writing("file2.txt").await.unwrap();
        drop(f2);

        // Extract the parent directories
        let dir1 = PathBuf::from(path1).parent().unwrap().to_path_buf();
        let dir2 = PathBuf::from(path2).parent().unwrap().to_path_buf();

        // This assertion will fail, demonstrating the bug
        assert_ne!(
            dir1,
            dir2,
            "SECURITY ISSUE: Different filerequests are using the same directory: {}",
            dir1.display()
        );
    }
}
