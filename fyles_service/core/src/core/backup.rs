use std::fmt;
use std::io::{self, Read, Write};

use chrono::Local;
use crypto::backup::{
    BackupSecret, EncryptingWriter, KdfError, OpenedBackup, UnlockError, is_corrupt_backup,
};
use derive_more::{Deref, TryFrom};
use thiserror::Error;
use zeroize::Zeroizing;

use crate::io_controller::Uri;
use password_check::{MIN_PASSWORD_CHARS, meets_minimum};
pub use password_check::{PasswordCheck, PasswordSuggestion, PasswordWarning, check_password};

mod password_check;

const BACKUP_FILE_SUFFIX: &str = "_fyles.bak.crypt";

const COMPRESSION_LEVEL: i32 = 10;

const DAY_MS: i64 = 24 * 60 * 60 * 1000;
const SNOOZE_MS: i64 = DAY_MS;

pub struct BackupFileName(String);

impl BackupFileName {
    pub fn now() -> Self {
        Self(format!(
            "{}{BACKUP_FILE_SUFFIX}",
            Local::now().format("%Y-%m-%d_%H-%M-%S")
        ))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Error)]
#[error("a backup password needs at least {MIN_PASSWORD_CHARS} characters")]
pub struct PasswordTooShort;

pub struct Password(Zeroizing<String>);

impl Password {
    pub fn new(password: String) -> Self {
        Self(Zeroizing::new(password))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Password {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Password(<redacted>)")
    }
}

/// A password the user is choosing. One typed to unlock something is a plain [`Password`],
/// because any input may be tried against an existing secret.
#[derive(Debug, Deref)]
pub struct NewBackupPassword(Password);

impl TryFrom<Password> for NewBackupPassword {
    type Error = PasswordTooShort;

    fn try_from(password: Password) -> Result<Self, Self::Error> {
        meets_minimum(&password)
            .then(|| Self(password))
            .ok_or(PasswordTooShort)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, TryFrom)]
#[try_from(repr)]
#[repr(i64)]
pub enum Gap {
    Day = 0,
    Week = 1,
    Month = 2,
    Quarter = 3,
}

impl Gap {
    fn duration_ms(self) -> i64 {
        DAY_MS
            * match self {
                Self::Day => 1,
                Self::Week => 7,
                Self::Month => 30,
                Self::Quarter => 90,
            }
    }

    fn longer(self) -> Self {
        match self {
            Self::Day => Self::Week,
            Self::Week => Self::Month,
            Self::Month | Self::Quarter => Self::Quarter,
        }
    }

    fn shorter(self) -> Self {
        match self {
            Self::Day | Self::Week => Self::Day,
            Self::Month => Self::Week,
            Self::Quarter => Self::Month,
        }
    }
}

impl From<Gap> for i64 {
    fn from(gap: Gap) -> Self {
        gap as i64
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerifyEvent {
    Correct,
    Wrong,
    Snoozed,
}

/// When the user is next asked to retype the backup password. The gaps grow while they remember
/// it and shrink after they struggled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VerificationSchedule {
    gap: Gap,
    due_at_ms: i64,
    missed: bool,
}

impl VerificationSchedule {
    pub fn fresh(now_ms: i64) -> Self {
        Self::waiting(Gap::Day, now_ms)
    }

    fn waiting(gap: Gap, now_ms: i64) -> Self {
        Self {
            gap,
            due_at_ms: now_ms + gap.duration_ms(),
            missed: false,
        }
    }

    pub fn from_parts(gap: Gap, due_at_ms: i64, missed: bool) -> Self {
        Self {
            gap,
            due_at_ms,
            missed,
        }
    }

    pub fn gap(&self) -> Gap {
        self.gap
    }

    pub fn due_at_ms(&self) -> i64 {
        self.due_at_ms
    }

    pub fn missed(&self) -> bool {
        self.missed
    }

    pub fn is_due(&self, now_ms: i64) -> bool {
        now_ms >= self.due_at_ms
    }

    pub fn verified(self, event: VerifyEvent, now_ms: i64) -> Self {
        match event {
            VerifyEvent::Correct => {
                let gap = if self.missed {
                    self.gap.shorter()
                } else {
                    self.gap.longer()
                };
                Self::waiting(gap, now_ms)
            }
            VerifyEvent::Wrong => Self {
                missed: true,
                ..self
            },
            VerifyEvent::Snoozed => Self {
                due_at_ms: now_ms + SNOOZE_MS,
                ..self
            },
        }
    }

    /// An attempt only counts while a verification is due, so checking the current password
    /// before changing it leaves the schedule alone.
    pub fn after_attempt(self, correct: bool, now_ms: i64) -> Self {
        if !self.is_due(now_ms) {
            return self;
        }
        let event = if correct {
            VerifyEvent::Correct
        } else {
            VerifyEvent::Wrong
        };
        self.verified(event, now_ms)
    }
}

pub struct StoredBackupSecret {
    pub secret: BackupSecret,
    pub schedule: VerificationSchedule,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackupPasswordStatus {
    NotSet,
    Set { verification_due: bool },
}

impl BackupPasswordStatus {
    pub fn of(stored: Option<&StoredBackupSecret>, now_ms: i64) -> Self {
        match stored {
            None => Self::NotSet,
            Some(stored) => Self::Set {
                verification_due: stored.schedule.is_due(now_ms),
            },
        }
    }
}

pub fn pack(mut sqlite: impl Read, out: impl Write, secret: &BackupSecret) -> io::Result<()> {
    let mut compressor =
        zstd::Encoder::new(EncryptingWriter::new(out, secret)?, COMPRESSION_LEVEL)?;
    io::copy(&mut sqlite, &mut compressor)?;
    compressor.finish()?.finish()?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnpackedFormat {
    Encrypted,
    /// Deprecated, see [`OpenedBackup::LegacyPlain`].
    LegacyPlain,
}

impl UnpackedFormat {
    pub fn restored_at(self, now_ms: i64) -> RestoredFrom {
        match self {
            Self::Encrypted => RestoredFrom::Encrypted { at_ms: now_ms },
            Self::LegacyPlain => RestoredFrom::LegacyPlain,
        }
    }
}

/// An encrypted restore proves the user knows the password, so it counts as a verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoredFrom {
    Encrypted { at_ms: i64 },
    LegacyPlain,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum RestoreRefusal {
    #[error("not a Fyles backup")]
    NotABackup,
    #[error("the backup was made by a newer version of Fyles")]
    Unsupported,
    #[error("the backup is encrypted and no password was given")]
    PasswordRequired,
    #[error("wrong backup password")]
    WrongPassword,
    #[error("the backup is damaged")]
    Corrupt,
}

#[derive(Debug, Error)]
pub enum UnpackError {
    #[error(transparent)]
    Refused(#[from] RestoreRefusal),
    #[error(transparent)]
    Kdf(#[from] KdfError),
    #[error(transparent)]
    Io(io::Error),
}

impl From<io::Error> for UnpackError {
    fn from(error: io::Error) -> Self {
        if is_corrupt_backup(&error) {
            Self::Refused(RestoreRefusal::Corrupt)
        } else {
            Self::Io(error)
        }
    }
}

impl From<UnlockError> for UnpackError {
    fn from(error: UnlockError) -> Self {
        match error {
            UnlockError::WrongPassword => Self::Refused(RestoreRefusal::WrongPassword),
            UnlockError::Kdf(e) => Self::Kdf(e),
        }
    }
}

pub fn unpack(
    backup: impl Read,
    password: Option<&Password>,
    sqlite: &mut impl Write,
) -> Result<UnpackedFormat, UnpackError> {
    match crypto::backup::open(backup)? {
        OpenedBackup::NotABackup => Err(RestoreRefusal::NotABackup.into()),
        OpenedBackup::Unsupported => Err(RestoreRefusal::Unsupported.into()),
        OpenedBackup::LegacyPlain(compressed) => {
            io::copy(&mut zstd::Decoder::new(compressed)?, sqlite)?;
            Ok(UnpackedFormat::LegacyPlain)
        }
        OpenedBackup::Encrypted(encrypted) => {
            let password = password.ok_or(RestoreRefusal::PasswordRequired)?;
            let decrypted = encrypted.unlock(password.as_str())?;
            io::copy(&mut zstd::Decoder::new(decrypted)?, sqlite)?;
            Ok(UnpackedFormat::Encrypted)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackupOutcome {
    Written,
    PasswordNotSet,
}

#[derive(Debug)]
pub struct RestoreRequest {
    pub uri: Uri,
    pub password: Option<Password>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoreOutcome {
    Restored(UnpackedFormat),
    Refused(RestoreRefusal),
}

#[cfg(any(test, feature = "test-support"))]
pub mod test_support {
    use std::fs::{self, File};
    use std::path::Path;

    use crypto::backup::{BackupSecret, KdfParams};

    use super::{StoredBackupSecret, VerificationSchedule, pack};
    use crate::core::db::FilerequestDb;

    pub const TEST_PASSWORD: &str = "correct horse battery";

    pub fn cheap_secret(password: &str) -> BackupSecret {
        BackupSecret::create_with(
            password,
            KdfParams::new(64, 1, 1).expect("valid parameters"),
        )
        .expect("derivation succeeds")
    }

    /// Gives `db` a backup password ([`TEST_PASSWORD`]) and writes an encrypted backup of it.
    pub async fn write_backup(db: &dyn FilerequestDb, schedule: VerificationSchedule, path: &Path) {
        db.store_backup_secret(StoredBackupSecret {
            secret: cheap_secret(TEST_PASSWORD),
            schedule,
        })
        .await
        .expect("the secret is stored");
        let stored = db
            .get_backup_secret()
            .await
            .expect("the secret is read")
            .expect("the secret exists");
        let snapshot = path.with_extension("snapshot");
        db.snapshot_database(snapshot.clone())
            .await
            .expect("the snapshot is written");
        pack(
            File::open(&snapshot).expect("the snapshot opens"),
            File::create(path).expect("the backup file is created"),
            &stored.secret,
        )
        .expect("the backup is packed");
        fs::remove_file(snapshot).expect("the snapshot is removed");
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use tempfile::TempDir;

    use super::test_support::{TEST_PASSWORD, cheap_secret, write_backup};
    use super::*;
    use crate::core::db::test::setup_test_db;

    const NOW: i64 = 1_000_000_000_000;

    fn database_bytes() -> Vec<u8> {
        (0..200_000u32)
            .flat_map(|i| (i % 97).to_le_bytes())
            .collect()
    }

    #[test]
    fn passwords_never_reach_debug_output() {
        let password = Password::new("hunter22".into());
        assert!(!format!("{password:?}").contains("hunter22"));
        let new = NewBackupPassword::try_from(Password::new("hunter22".into())).unwrap();
        assert!(!format!("{new:?}").contains("hunter22"));
    }

    #[test]
    fn new_passwords_need_six_characters() {
        assert!(NewBackupPassword::try_from(Password::new("12345".into())).is_err());
        assert!(NewBackupPassword::try_from(Password::new("äöüßéè".into())).is_ok());
    }

    #[test]
    fn correct_answers_grow_the_gap_up_to_a_quarter() {
        let mut schedule = VerificationSchedule::fresh(NOW);
        let mut gaps = Vec::new();
        for _ in 0..5 {
            let now = schedule.due_at_ms();
            schedule = schedule.after_attempt(true, now);
            gaps.push(schedule.gap());
        }
        assert_eq!(
            gaps,
            [
                Gap::Week,
                Gap::Month,
                Gap::Quarter,
                Gap::Quarter,
                Gap::Quarter
            ]
        );
    }

    #[test]
    fn a_miss_before_the_right_answer_shrinks_the_gap() {
        let schedule = VerificationSchedule::from_parts(Gap::Month, NOW, false)
            .after_attempt(false, NOW)
            .after_attempt(true, NOW);
        assert_eq!(schedule.gap(), Gap::Week);
        assert_eq!(schedule.due_at_ms(), NOW + 7 * DAY_MS);
        assert!(!schedule.missed());
    }

    #[test]
    fn attempts_before_the_due_date_change_nothing() {
        let schedule = VerificationSchedule::fresh(NOW);
        assert_eq!(schedule.after_attempt(false, NOW), schedule);
        assert_eq!(schedule.after_attempt(true, NOW), schedule);
    }

    #[test]
    fn snoozing_postpones_by_a_day_and_keeps_a_miss() {
        let due = NOW + DAY_MS;
        let schedule = VerificationSchedule::fresh(NOW)
            .after_attempt(false, due)
            .verified(VerifyEvent::Snoozed, due);
        assert!(!schedule.is_due(due));
        assert!(schedule.is_due(due + SNOOZE_MS));
        assert!(schedule.missed());
        assert_eq!(schedule.gap(), Gap::Day);
    }

    #[test]
    fn gaps_survive_storage() {
        for gap in [Gap::Day, Gap::Week, Gap::Month, Gap::Quarter] {
            assert_eq!(Gap::try_from(i64::from(gap)).unwrap(), gap);
        }
        assert!(Gap::try_from(4).is_err());
    }

    #[test]
    fn pack_then_unpack_round_trips() {
        let secret = cheap_secret("correct horse");
        let mut backup = Vec::new();
        pack(database_bytes().as_slice(), &mut backup, &secret).unwrap();
        let mut sqlite = Vec::new();
        let format = unpack(
            backup.as_slice(),
            Some(&Password::new("correct horse".into())),
            &mut sqlite,
        )
        .unwrap();
        assert_eq!(format, UnpackedFormat::Encrypted);
        assert_eq!(sqlite, database_bytes());
    }

    #[test]
    fn unpack_refusals() {
        let secret = cheap_secret("correct horse");
        let mut backup = Vec::new();
        pack(database_bytes().as_slice(), &mut backup, &secret).unwrap();
        let mut sink = Vec::new();
        assert!(matches!(
            unpack(backup.as_slice(), None, &mut sink),
            Err(UnpackError::Refused(RestoreRefusal::PasswordRequired))
        ));
        assert!(matches!(
            unpack(
                backup.as_slice(),
                Some(&Password::new("battery staple".into())),
                &mut sink
            ),
            Err(UnpackError::Refused(RestoreRefusal::WrongPassword))
        ));
        let last = backup.len() - 1;
        backup[last] ^= 1;
        assert!(matches!(
            unpack(
                backup.as_slice(),
                Some(&Password::new("correct horse".into())),
                &mut sink
            ),
            Err(UnpackError::Refused(RestoreRefusal::Corrupt))
        ));
        assert!(matches!(
            unpack(&b"SQLite format 3\0"[..], None, &mut sink),
            Err(UnpackError::Refused(RestoreRefusal::NotABackup))
        ));
    }

    #[test]
    fn a_legacy_backup_unpacks_without_a_password() {
        let legacy = zstd::encode_all(database_bytes().as_slice(), 3).unwrap();
        let mut sqlite = Vec::new();
        let format = unpack(legacy.as_slice(), None, &mut sqlite).unwrap();
        assert_eq!(format, UnpackedFormat::LegacyPlain);
        assert_eq!(sqlite, database_bytes());
    }

    #[tokio::test]
    async fn an_encrypted_restore_brings_the_secret_and_counts_as_a_verification() {
        let dir = TempDir::new().unwrap();
        let backup = dir.path().join("backup.bak.crypt");
        let original = setup_test_db(None).await;
        write_backup(
            original.as_ref(),
            VerificationSchedule::from_parts(Gap::Week, NOW, false),
            &backup,
        )
        .await;

        let sqlite = dir.path().join("restored.sqlite");
        let format = unpack(
            File::open(&backup).unwrap(),
            Some(&Password::new(TEST_PASSWORD.into())),
            &mut File::create(&sqlite).unwrap(),
        )
        .unwrap();
        let restored = setup_test_db(None).await;
        assert!(restored.get_backup_secret().await.unwrap().is_none());
        restored
            .restore_database(sqlite, format.restored_at(NOW))
            .await
            .unwrap();

        let stored = restored.get_backup_secret().await.unwrap().unwrap();
        assert!(stored.secret.verify(TEST_PASSWORD).unwrap());
        assert_eq!(stored.schedule.gap(), Gap::Month);
        assert_eq!(stored.schedule.due_at_ms(), NOW + 30 * DAY_MS);
    }
}
