use sanitize_filename::sanitize;
use std::fmt::{Debug, Formatter};
use std::{path::PathBuf, sync::Arc};
use thiserror::Error;
// use thiserror::Error;
use tokio::fs::File;
use tracing::{error, warn};

use crate::{
    core::domain_models::Filerequest,
    io_controller::{FileNamespace, HostController, OwnershipOutcome, RenameOutcome},
};

#[derive(Debug, Error)]
pub enum GetFileError {
    #[error("Refusing to create file: filenames starting with '.' are not allowed")]
    FileNameStartsWithDot,
    #[error("Refusing to create file: sanitized filename from '{original}' is empty")]
    SanitizedEmpty { original: String },
    #[error("Refusing to create file: filename '{sanitized}' contains a path separator")]
    ContainsSeparator { sanitized: String },
    #[error("Base directory {base} does not exist")]
    BaseDirMissing { base: PathBuf },
    #[error("Security violation: constructed path not within base directory")]
    ConstructedPathEscapedBaseDirectory,
    #[error("Failed to find unique filename for {base}")]
    UniqueFileNameVariantNotFound { base: PathBuf },
    #[error("File already exists")]
    FileExists,
    #[error("Filerequest directory is owned by another filerequest ({other_id})")]
    Claimed { other_id: String },
    #[error("IO error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("Error in filerequest directory handling: {0}")]
    FilerequestDir(#[source] Box<dyn std::error::Error + Send + Sync>),
}

type GetFileResult<T> = Result<T, GetFileError>;

/// Outcome of attempting to rename a filerequest's directory.
#[derive(Debug, Error)]
pub enum RenameFilerequestError {
    /// The destination directory is already claimed by another filerequest;
    /// renaming onto it would risk mixing files, so it is refused. Surfaced to
    /// the user as a clean folder-collision error.
    #[error("Destination folder is already claimed by another filerequest")]
    DestinationOccupied,
    /// The rename failed for any other reason (empty title, lost ownership of
    /// the source, IO error, …).
    #[error("Failed to rename filerequest directory")]
    Failed,
}

pub struct FilerequestDriveHandler {
    dir_handler: Arc<dyn HostController>,
    safe_dir_name: String,
    /// The owning filerequest's id. Stamped into the directory's `.fyles-owner`
    /// marker so no other filerequest can write into this directory.
    owner_id: String,
}

impl Debug for FilerequestDriveHandler {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilerequestDriveHandler { ---opaque--- }")
            .finish()
    }
}

impl FilerequestDriveHandler {
    pub fn new(dir_handler: Arc<dyn HostController>, filerequest: Filerequest) -> Self {
        let mut safe = Self::get_name_from_filerequest(&filerequest);
        if safe.is_empty() {
            warn!(
                "Sanitized filerequest title '{}' became empty; falling back to its id",
                filerequest.id
            );

            safe = filerequest.id.to_string();
        }
        Self {
            dir_handler,
            safe_dir_name: safe,
            owner_id: filerequest.id.to_string(),
        }
    }

    /// The sanitized directory name (with id fallback) this handler writes into,
    /// relative to the host's drop directory. Used to resolve on-disk locations
    /// for the UI without re-implementing the sanitization/fallback logic.
    pub fn safe_dir_name(&self) -> &str {
        &self.safe_dir_name
    }

    fn dir_namespace(&self) -> FileNamespace {
        FileNamespace::Namespace {
            namespace: self.safe_dir_name.clone(),
            child: None,
        }
    }

    /// Guard run before any operation that touches this filerequest's directory:
    /// ensures the directory carries our ownership marker, adopting it if
    /// unclaimed, and refusing with [`GetFileError::Claimed`] if another
    /// filerequest owns it.
    async fn ensure_owned(&self) -> GetFileResult<()> {
        match self
            .dir_handler
            .ensure_exists_and_is_owned(self.dir_namespace(), &self.owner_id, false)
            .await
            .map_err(GetFileError::FilerequestDir)?
        {
            OwnershipOutcome::Owned | OwnershipOutcome::Adopted => Ok(()),
            OwnershipOutcome::Conflict { other_id } => {
                warn!(
                    "Refusing operation on directory '{}' owned by filerequest {other_id}",
                    self.safe_dir_name
                );
                Err(GetFileError::Claimed { other_id })
            }
        }
    }

    fn get_name_from_filerequest(filerequest: &Filerequest) -> String {
        Self::get_name_from_string(&filerequest.title)
    }

    fn get_name_from_string(name: &String) -> String {
        sanitize(name)
    }

    pub async fn rename_filerquest(
        &mut self,
        new_title: &String,
    ) -> Result<(), RenameFilerequestError> {
        let safe = Self::get_name_from_string(new_title);
        if safe.is_empty() {
            warn!(
                "Cannot rename {} based filerequest to {new_title}",
                self.safe_dir_name
            );
            return Err(RenameFilerequestError::Failed);
        }

        // Ensure we still own the source directory before moving it. The marker
        // travels inside the directory, so after a successful rename the new
        // location remains ours.
        if let Err(e) = self.ensure_owned().await {
            warn!(?e, "Refusing to rename filerequest directory we do not own");
            return Err(RenameFilerequestError::Failed);
        }

        // The rename atomically refuses to overwrite an existing directory and
        // reports that as `DestinationOccupied` (the no-mixing guarantee), the
        // same way creation reports a folder collision.
        match self
            .dir_handler
            .rename_resource(
                FileNamespace::Namespace {
                    namespace: self.safe_dir_name.clone(),
                    child: None,
                },
                FileNamespace::Namespace {
                    namespace: safe.clone(),
                    child: None,
                },
            )
            .await
        {
            Ok(RenameOutcome::Renamed) => {}
            Ok(RenameOutcome::DestinationOccupied) => {
                warn!(
                    "Refusing to rename '{}' onto already-claimed folder '{safe}'",
                    self.safe_dir_name
                );
                return Err(RenameFilerequestError::DestinationOccupied);
            }
            Err(e) => {
                error!(?e, "Failed to rename filerequest");
                return Err(RenameFilerequestError::Failed);
            }
        }

        // Rename associated directory on disk
        // let base_dir = ensure_filerequest_dir(&self.dir_handler, &self.safe_dir_name)
        //     .await
        //     .map_err(|e| {
        //         warn!(
        //             "Failed to access existing filerequest directory {}: {}",
        //             self.safe_dir_name, e
        //         );
        //         ()
        //     })?;
        // let new_dir = base_dir.parent().unwrap().join(&safe);
        // fs::rename(&base_dir, &new_dir).await.map_err(|e| {
        //     warn!(
        //         "Failed to rename filerequest directory from {} to {}: {}",
        //         base_dir.display(),
        //         new_dir.display(),
        //         e
        //     );
        //     ()
        // })?;

        self.safe_dir_name = safe;
        Ok(())
    }

    pub async fn get_file_for_writing(&self, filename: &str) -> GetFileResult<(String, File)> {
        let safe_name = Self::sanitize_filename(filename)?;
        self.ensure_owned().await?;

        self.dir_handler
            .create_file_for_writing(FileNamespace::Namespace {
                namespace: self.safe_dir_name.clone(),
                child: Some(Box::new(FileNamespace::Filename(safe_name))),
            })
            .await
            .map(|(a, b)| (b, a))
            .map_err(GetFileError::FilerequestDir)
    }

    /// No deduplication on the file system
    pub async fn get_exact_file_for_writing(&self, filename: &str) -> GetFileResult<File> {
        let safe_name = Self::sanitize_filename(filename)?;
        self.ensure_owned().await?;

        self.dir_handler
            .get_exact_file_for_writing(FileNamespace::Namespace {
                namespace: self.safe_dir_name.clone(),
                child: Some(Box::new(FileNamespace::Filename(safe_name))),
            })
            .await
            .map_err(GetFileError::FilerequestDir)
    }

    pub async fn get_partial_file_for_writing(&self, filename: &str) -> GetFileResult<File> {
        let safe_name = Self::sanitize_filename(filename)?;
        self.dir_handler
            .get_partial_file_for_writing(&safe_name)
            .await
            .map_err(GetFileError::FilerequestDir)
    }

    pub async fn remove_partial_file(&self, filename: &str) -> GetFileResult<()> {
        let safe_name = Self::sanitize_filename(filename)?;
        self.dir_handler
            .remove_partial_file(&safe_name)
            .await
            .map_err(GetFileError::FilerequestDir)
    }

    pub async fn finalize_partial_file(
        &self,
        partial_filename: &str,
        final_filename: &str,
    ) -> GetFileResult<String> {
        let safe_part_name = Self::sanitize_filename(partial_filename)?;
        let safe_final_name = Self::sanitize_filename(final_filename)?;
        self.ensure_owned().await?;

        let final_namespace = FileNamespace::Namespace {
            namespace: self.safe_dir_name.clone(),
            child: Some(Box::new(FileNamespace::Filename(safe_final_name))),
        };

        // The host layer deduplicates the target, so the actual on-disk name may
        // differ from `safe_final_name`. Return the real persisted basename.
        self.dir_handler
            .finalize_partial_file(&safe_part_name, final_namespace)
            .await
            .map_err(GetFileError::FilerequestDir)
    }

    fn sanitize_filename(filename: &str) -> GetFileResult<String> {
        if filename.starts_with('.') {
            error!("Rejected filename starting with '.': '{filename}'");
            return Err(GetFileError::FileNameStartsWithDot);
        }

        let safe_name = sanitize(filename);
        if safe_name.is_empty() {
            error!("Sanitized filename from '{filename}' became empty");
            return Err(GetFileError::SanitizedEmpty {
                original: filename.into(),
            });
        }
        if safe_name.contains(std::path::MAIN_SEPARATOR) {
            error!("Sanitized filename still contained a path separator: '{safe_name}'");
            return Err(GetFileError::ContainsSeparator {
                sanitized: safe_name,
            });
        };

        Ok(safe_name)
    }

    pub async fn remove_file(&self, filename: &str) -> GetFileResult<()> {
        let safe_name = Self::sanitize_filename(filename)?;

        if *filename != safe_name {
            warn!("File to remove was sanitized from {filename} to {safe_name}");
        }
        self.ensure_owned().await?;

        self.dir_handler
            .remove_file(FileNamespace::Namespace {
                namespace: self.safe_dir_name.clone(),
                child: Some(Box::new(FileNamespace::Filename(safe_name))),
            })
            .await
            .map_err(GetFileError::FilerequestDir)
    }

    pub async fn rename_file(&self, from: &str, to: &str) -> GetFileResult<String> {
        let safe_from = Self::sanitize_filename(from)?;
        let safe_to = Self::sanitize_filename(to)?;
        self.ensure_owned().await?;

        // File renames deduplicate the target, so the result is always `Renamed`;
        // `DestinationOccupied` only applies to directory renames.
        match self
            .dir_handler
            .rename_resource(
                FileNamespace::Namespace {
                    namespace: self.safe_dir_name.clone(),
                    child: Some(Box::new(FileNamespace::Filename(safe_from))),
                },
                FileNamespace::Namespace {
                    namespace: self.safe_dir_name.clone(),
                    child: Some(Box::new(FileNamespace::Filename(safe_to.clone()))),
                },
            )
            .await
            .map_err(GetFileError::FilerequestDir)?
        {
            RenameOutcome::Renamed => Ok(safe_to),
            RenameOutcome::DestinationOccupied => Err(GetFileError::FileExists),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        core::domain_models::{FilerequestAccess, FylesId},
        io_controller::test::TestHostController,
        // library::p2p_node::tests::test_utils::TestHostController,
    };

    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn deep_root_path(tmp: &TempDir) -> PathBuf {
        let mut p = tmp.path().to_path_buf();
        for i in 0..12 {
            p.push(format!("level_{i}"));
        }
        p
    }

    fn make_filerequest(name: &str) -> Filerequest {
        Filerequest {
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
        let handler = Arc::new(TestHostController::new(None)) as _;
        let fr = make_filerequest(name);
        let fr_handler = FilerequestDriveHandler::new(handler, fr);
        (tmp, root, fr_handler)
    }

    #[tokio::test]
    async fn empty_filename_rejected() {
        let (_tmp, _root, frh) = setup("normal").await;
        let res = frh.get_file_for_writing("").await;
        assert!(
            matches!(res, Err(GetFileError::SanitizedEmpty { .. })),
            "Expected SanitizedEmpty, got {res:?}"
        );
    }

    #[tokio::test]
    async fn reserved_filename_rejected() {
        let (_tmp, _root, frh) = setup("normal").await;
        for bad in &[".", "..", ".hidden", "..tricky"] {
            let res = frh.get_file_for_writing(bad).await;
            assert!(
                matches!(res, Err(GetFileError::FileNameStartsWithDot)),
                "Expected FileNameStartsWithDot for '{bad}', got {res:?}"
            );
        }
    }

    /// Two filerequests whose titles sanitize to the same directory name must not
    /// share it: the first to write claims it, the second is refused.
    #[tokio::test]
    async fn colliding_directory_is_not_shared() {
        let host = Arc::new(TestHostController::new(None)) as Arc<dyn HostController>;

        // Same title → same sanitized directory, but different ids.
        let a = FilerequestDriveHandler::new(host.clone(), make_filerequest("Shared Name"));
        let b = FilerequestDriveHandler::new(host.clone(), make_filerequest("Shared Name"));
        assert_ne!(a.owner_id, b.owner_id);
        assert_eq!(a.safe_dir_name, b.safe_dir_name);

        // A claims the directory by writing into it.
        a.get_file_for_writing("from_a.txt")
            .await
            .expect("A should claim and write");
        // A can keep writing (idempotent ownership).
        a.get_file_for_writing("from_a_again.txt")
            .await
            .expect("A still owns its directory");

        // B is refused because A owns the directory.
        let res = b.get_file_for_writing("from_b.txt").await;
        assert!(
            matches!(&res, Err(GetFileError::Claimed { other_id }) if *other_id == a.owner_id),
            "Expected Claimed by A, got {res:?}"
        );
    }

    /// An unclaimed (markerless) directory gets adopted by the first writer.
    #[tokio::test]
    async fn ensure_owned_adopts_then_owns() {
        let (_tmp, _root, frh) = setup("Adoptable").await;
        // First call adopts.
        frh.ensure_owned().await.expect("adopt");
        // Subsequent calls are no-ops (Owned).
        frh.ensure_owned().await.expect("own");
    }

    /// Renaming a filerequest onto a folder owned by another is refused with
    /// `DestinationOccupied` (the atomic, no-mixing guarantee), while renaming
    /// onto a free name succeeds and carries the marker along.
    #[tokio::test]
    async fn rename_onto_claimed_folder_is_refused() {
        let host = Arc::new(TestHostController::new(None)) as Arc<dyn HostController>;

        // "Alpha" is claimed by request A.
        let a = FilerequestDriveHandler::new(host.clone(), make_filerequest("Alpha"));
        a.ensure_owned().await.expect("A claims Alpha");

        // B (originally "Beta") tries to rename onto "Alpha" → refused.
        let mut b = FilerequestDriveHandler::new(host.clone(), make_filerequest("Beta"));
        let res = b.rename_filerquest(&"Alpha".to_string()).await;
        assert!(
            matches!(res, Err(RenameFilerequestError::DestinationOccupied)),
            "Expected DestinationOccupied, got {res:?}"
        );
        // The failed rename left B pointed at its original folder.
        assert_eq!(b.safe_dir_name, "Beta");

        // Renaming onto a free name succeeds and moves B's directory.
        b.rename_filerquest(&"Gamma".to_string())
            .await
            .expect("rename onto free name");
        assert_eq!(b.safe_dir_name, "Gamma");
    }
}
