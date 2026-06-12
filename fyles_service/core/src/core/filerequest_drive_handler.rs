use sanitize_filename::sanitize;
use std::fmt::{Debug, Formatter};
use std::{path::PathBuf, sync::Arc};
use thiserror::Error;
// use thiserror::Error;
use tokio::fs::File;
use tracing::{error, warn};

use crate::{
    core::domain_models::Filerequest,
    io_controller::{FileNamespace, HostController},
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

pub struct FilerequestDriveHandler {
    dir_handler: Arc<dyn HostController>,
    safe_dir_name: String,
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
        }
    }

    fn get_name_from_filerequest(filerequest: &Filerequest) -> String {
        Self::get_name_from_string(&filerequest.title)
    }

    fn get_name_from_string(name: &String) -> String {
        sanitize(name)
    }

    pub async fn rename_filerquest(&mut self, new_title: &String) -> Result<(), ()> {
        let safe = Self::get_name_from_string(new_title);
        if safe.is_empty() {
            warn!(
                "Cannot rename {} based filerequest to {new_title}",
                self.safe_dir_name
            );
            return Err(());
        }

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
            Ok(_) => {}
            Err(e) => {
                error!(?e, "Failed to rename filerequest");
                return Err(());
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

    pub async fn finalize_partial_file(&self, partial_filename: &str, final_filename: &str) -> GetFileResult<String> {
        let safe_part_name = Self::sanitize_filename(partial_filename)?;
        let safe_final_name = Self::sanitize_filename(final_filename)?;

        let final_namespace = FileNamespace::Namespace {
            namespace: self.safe_dir_name.clone(),
            child: Some(Box::new(FileNamespace::Filename(safe_final_name.clone()))),
        };

        self.dir_handler
            .finalize_partial_file(&safe_part_name, final_namespace)
            .await
            .map_err(GetFileError::FilerequestDir)?;

        Ok(safe_final_name)
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

        self.dir_handler
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
            .map_err(GetFileError::FilerequestDir)?;

        Ok(safe_to)
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
}
