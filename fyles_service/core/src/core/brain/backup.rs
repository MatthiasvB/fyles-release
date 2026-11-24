use std::fs::File;
use std::path::Path;

use crypto::backup::BackupSecret;
use tempfile::{Builder, NamedTempFile, TempPath};
use tokio::task::spawn_blocking;
use tracing::warn;

use crate::core::backup::{
    self, BackupFileName, BackupOutcome, BackupPasswordStatus, NewBackupPassword, Password,
    RestoreOutcome, RestoreRequest, StoredBackupSecret, UnpackError, UnpackedFormat,
    VerificationSchedule, VerifyEvent,
};
use crate::core::brain::GigaBrain;
use crate::core::brain::error::FilerequestError;
use crate::core::brain::types::FilerequestResult;
use crate::library::util::epoch::unix_epoch_millis;

fn scratch_file(dir: &Path, purpose: &str) -> FilerequestResult<TempPath> {
    Builder::new()
        .prefix(purpose)
        .suffix(".sqlite")
        .tempfile_in(dir)
        .map(NamedTempFile::into_temp_path)
        .map_err(|e| FilerequestError::generic("creating a scratch file failed", e))
}

fn now_ms() -> FilerequestResult<i64> {
    unix_epoch_millis().map_err(|e| FilerequestError::generic("the system clock is before 1970", e))
}

impl GigaBrain {
    pub(super) async fn backup_data(&self) -> FilerequestResult<BackupOutcome> {
        // A large backup can outlast `timeout_lock`'s deadline, and nothing is locked while this
        // is held.
        let _exclusive = self.backup_files.lock().await;
        let Some(stored) = self.db.get_backup_secret().await? else {
            return Ok(BackupOutcome::PasswordNotSet);
        };
        let snapshot = scratch_file(&self.internal_data_dir, "backup")?;
        self.db.snapshot_database(snapshot.to_path_buf()).await?;
        let target = self
            .host_controller
            .get_path_to_db_backup_file(&BackupFileName::now())
            .await
            .map_err(|source| FilerequestError::HostError { source })?;
        spawn_blocking(move || {
            let sqlite = File::open(&snapshot)?;
            backup::pack(sqlite, target, &stored.secret)
        })
        .await?
        .map_err(|e| FilerequestError::generic("writing the backup failed", e))?;
        Ok(BackupOutcome::Written)
    }

    pub(super) async fn restore_data(
        &self,
        request: RestoreRequest,
    ) -> FilerequestResult<RestoreOutcome> {
        let _exclusive = self.backup_files.lock().await;
        let source = self
            .host_controller
            .access_file_for_reading(request.uri)
            .await
            .map_err(|source| FilerequestError::HostError { source })?
            .file
            .into_std()
            .await;
        let unpacked = scratch_file(&self.internal_data_dir, "restore")?;
        let destination = unpacked.to_path_buf();
        let password = request.password;
        let format = spawn_blocking(move || {
            let mut sqlite = File::create(destination)?;
            backup::unpack(source, password.as_ref(), &mut sqlite)
        })
        .await?;
        let format = match format {
            Ok(format) => format,
            Err(UnpackError::Refused(refusal)) => return Ok(RestoreOutcome::Refused(refusal)),
            Err(e) => return Err(FilerequestError::generic("reading the backup failed", e)),
        };
        if format == UnpackedFormat::LegacyPlain {
            warn!("restoring a deprecated unencrypted backup");
        }
        self.db
            .restore_database(unpacked.to_path_buf(), format.restored_at(now_ms()?))
            .await?;
        Ok(RestoreOutcome::Restored(format))
    }

    pub(super) async fn backup_password_status(&self) -> FilerequestResult<BackupPasswordStatus> {
        let stored = self.db.get_backup_secret().await?;
        Ok(BackupPasswordStatus::of(stored.as_ref(), now_ms()?))
    }

    pub(super) async fn set_backup_password(
        &self,
        password: NewBackupPassword,
    ) -> FilerequestResult<()> {
        let secret = spawn_blocking(move || BackupSecret::create(password.as_str()))
            .await?
            .map_err(|e| FilerequestError::generic("deriving the backup key failed", e))?;
        self.db
            .store_backup_secret(StoredBackupSecret {
                secret,
                schedule: VerificationSchedule::fresh(now_ms()?),
            })
            .await?;
        Ok(())
    }

    pub(super) async fn verify_backup_password(
        &self,
        password: Password,
    ) -> FilerequestResult<bool> {
        let stored = self
            .db
            .get_backup_secret()
            .await?
            .ok_or_else(|| FilerequestError::InputError("no backup password is set".into()))?;
        let StoredBackupSecret { secret, schedule } = stored;
        let correct = spawn_blocking(move || secret.verify(password.as_str()))
            .await?
            .map_err(|e| FilerequestError::generic("deriving the backup key failed", e))?;
        let updated = schedule.after_attempt(correct, now_ms()?);
        if updated != schedule {
            self.db.store_verification_schedule(updated).await?;
        }
        Ok(correct)
    }

    pub(super) async fn snooze_backup_password_verification(&self) -> FilerequestResult<()> {
        if let Some(stored) = self.db.get_backup_secret().await? {
            self.db
                .store_verification_schedule(
                    stored.schedule.verified(VerifyEvent::Snoozed, now_ms()?),
                )
                .await?;
        }
        Ok(())
    }
}
