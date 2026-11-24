use std::io::{self, Chain, Cursor, Read, Write};
use std::mem;

use argon2::{Algorithm, Argon2, Params, Version};
use chacha20poly1305::aead::generic_array::GenericArray;
use chacha20poly1305::aead::stream::{DecryptorBE32, EncryptorBE32};
use chacha20poly1305::aead::{KeyInit, Payload};
use chacha20poly1305::{Key, XChaCha20Poly1305};
use hkdf::Hkdf;
use rand_core::{OsRng, RngCore};
use sha2::Sha256;
use subtle::ConstantTimeEq;
use thiserror::Error;
use zeroize::Zeroizing;

const MAGIC: [u8; 7] = *b"FYLESBK";
const FORMAT_VERSION: u8 = 1;
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];
const PREAMBLE_LEN: usize = MAGIC.len() + 1;

const SALT_LEN: usize = 16;
const KEY_LEN: usize = 32;
const NONCE_PREFIX_LEN: usize = 19;
const KEY_CHECK_LEN: usize = 16;

const CHUNK_LEN: usize = 64 * 1024;
const TAG_LEN: usize = 16;
const SEALED_CHUNK_LEN: usize = CHUNK_LEN + TAG_LEN;

const ENCRYPTION_KEY_INFO: &[u8] = b"fyles-backup-encryption-v1";
const KEY_CHECK_INFO: &[u8] = b"fyles-backup-key-check-v1";

/// The ceilings bound what a crafted header can make restore allocate and spend.
const MAX_M_COST_KIB: u32 = 256 * 1024;
const MAX_T_COST: u32 = 16;
const MAX_P_COST: u32 = 8;

#[derive(Debug, Error)]
#[error("argon2 parameters outside the supported range")]
pub struct UnsupportedKdfParams;

#[derive(Debug, Error)]
#[error("key derivation failed: {0}")]
pub struct KdfError(argon2::Error);

#[derive(Debug, Error)]
#[error("the backup failed authentication")]
pub struct CorruptBackup;

pub fn is_corrupt_backup(error: &io::Error) -> bool {
    error
        .get_ref()
        .is_some_and(|inner| inner.is::<CorruptBackup>())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KdfParams(Params);

impl KdfParams {
    pub fn current() -> Self {
        Self::new(64 * 1024, 3, 1).expect("the current parameters are in range")
    }

    pub fn new(m_cost_kib: u32, t_cost: u32, p_cost: u32) -> Result<Self, UnsupportedKdfParams> {
        if m_cost_kib > MAX_M_COST_KIB || t_cost > MAX_T_COST || p_cost > MAX_P_COST {
            return Err(UnsupportedKdfParams);
        }
        Params::new(m_cost_kib, t_cost, p_cost, None)
            .map(Self)
            .map_err(|_| UnsupportedKdfParams)
    }

    pub fn m_cost_kib(&self) -> u32 {
        self.0.m_cost()
    }

    pub fn t_cost(&self) -> u32 {
        self.0.t_cost()
    }

    pub fn p_cost(&self) -> u32 {
        self.0.p_cost()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Salt([u8; SALT_LEN]);

impl Salt {
    fn random() -> Self {
        let mut bytes = [0u8; SALT_LEN];
        OsRng.fill_bytes(&mut bytes);
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; SALT_LEN] {
        &self.0
    }
}

impl From<[u8; SALT_LEN]> for Salt {
    fn from(bytes: [u8; SALT_LEN]) -> Self {
        Self(bytes)
    }
}

pub struct BackupKey(Zeroizing<[u8; KEY_LEN]>);

impl BackupKey {
    fn derive(password: &str, salt: &Salt, params: &KdfParams) -> Result<Self, KdfError> {
        let mut key = Zeroizing::new([0u8; KEY_LEN]);
        Argon2::new(Algorithm::Argon2id, Version::V0x13, params.0.clone())
            .hash_password_into(password.as_bytes(), salt.as_bytes(), &mut *key)
            .map_err(KdfError)?;
        Ok(Self(key))
    }

    pub fn as_bytes(&self) -> &[u8; KEY_LEN] {
        &self.0
    }

    fn expand(&self, info: &[u8], out: &mut [u8]) {
        Hkdf::<Sha256>::new(None, self.0.as_slice())
            .expand(info, out)
            .expect("the subkey lengths are valid HKDF-SHA256 output lengths");
    }

    fn cipher(&self) -> XChaCha20Poly1305 {
        let mut key = Zeroizing::new([0u8; KEY_LEN]);
        self.expand(ENCRYPTION_KEY_INFO, &mut *key);
        XChaCha20Poly1305::new(Key::from_slice(key.as_slice()))
    }

    fn check(&self) -> [u8; KEY_CHECK_LEN] {
        let mut check = [0u8; KEY_CHECK_LEN];
        self.expand(KEY_CHECK_INFO, &mut check);
        check
    }
}

impl From<[u8; KEY_LEN]> for BackupKey {
    fn from(bytes: [u8; KEY_LEN]) -> Self {
        Self(Zeroizing::new(bytes))
    }
}

/// What a device keeps so it can encrypt backups and re-verify the password without storing it.
pub struct BackupSecret {
    salt: Salt,
    params: KdfParams,
    key: BackupKey,
}

impl BackupSecret {
    pub fn create(password: &str) -> Result<Self, KdfError> {
        Self::create_with(password, KdfParams::current())
    }

    pub fn create_with(password: &str, params: KdfParams) -> Result<Self, KdfError> {
        let salt = Salt::random();
        let key = BackupKey::derive(password, &salt, &params)?;
        Ok(Self { salt, params, key })
    }

    pub fn from_parts(salt: Salt, params: KdfParams, key: BackupKey) -> Self {
        Self { salt, params, key }
    }

    pub fn verify(&self, password: &str) -> Result<bool, KdfError> {
        let candidate = BackupKey::derive(password, &self.salt, &self.params)?;
        Ok(candidate.0.as_slice().ct_eq(self.key.0.as_slice()).into())
    }

    pub fn salt(&self) -> &Salt {
        &self.salt
    }

    pub fn params(&self) -> &KdfParams {
        &self.params
    }

    pub fn key(&self) -> &BackupKey {
        &self.key
    }
}

struct Header {
    params: KdfParams,
    salt: Salt,
    nonce_prefix: [u8; NONCE_PREFIX_LEN],
    key_check: [u8; KEY_CHECK_LEN],
}

impl Header {
    fn for_secret(secret: &BackupSecret) -> Self {
        let mut nonce_prefix = [0u8; NONCE_PREFIX_LEN];
        OsRng.fill_bytes(&mut nonce_prefix);
        Self {
            params: secret.params.clone(),
            salt: secret.salt,
            nonce_prefix,
            key_check: secret.key.check(),
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        [
            MAGIC.as_slice(),
            &[FORMAT_VERSION],
            &self.params.m_cost_kib().to_le_bytes(),
            &self.params.t_cost().to_le_bytes(),
            &self.params.p_cost().to_le_bytes(),
            self.salt.as_bytes(),
            &self.nonce_prefix,
            &self.key_check,
        ]
        .concat()
    }
}

enum Encryption {
    Open(EncryptorBE32<XChaCha20Poly1305>),
    /// A chunk was sealed but not written, so the next one would carry the wrong counter.
    Failed,
}

/// Writes to `inner` a whole chunk at a time, so `flush` leaves up to one chunk buffered until
/// [`Self::finish`] seals it.
pub struct EncryptingWriter<W: Write> {
    inner: W,
    state: Encryption,
    aad: Vec<u8>,
    buffer: Vec<u8>,
}

impl<W: Write> EncryptingWriter<W> {
    pub fn new(mut inner: W, secret: &BackupSecret) -> io::Result<Self> {
        let header = Header::for_secret(secret);
        let aad = header.to_bytes();
        inner.write_all(&aad)?;
        Ok(Self {
            inner,
            state: Encryption::Open(EncryptorBE32::from_aead(
                secret.key.cipher(),
                GenericArray::from_slice(&header.nonce_prefix),
            )),
            aad,
            buffer: Vec::with_capacity(CHUNK_LEN),
        })
    }

    /// Seals the final chunk. Without it the backup does not authenticate.
    pub fn finish(mut self) -> io::Result<W> {
        let Encryption::Open(encryptor) = self.state else {
            return Err(earlier_write_failed());
        };
        let sealed = encryptor
            .encrypt_last(Payload {
                msg: &self.buffer,
                aad: &self.aad,
            })
            .map_err(|_| io::Error::other("sealing the last backup chunk failed"))?;
        self.inner.write_all(&sealed)?;
        self.inner.flush()?;
        Ok(self.inner)
    }
}

impl<W: Write> Write for EncryptingWriter<W> {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        let Encryption::Open(encryptor) = &mut self.state else {
            return Err(earlier_write_failed());
        };
        if data.is_empty() {
            return Ok(0);
        }
        if self.buffer.len() == CHUNK_LEN {
            let written = encryptor
                .encrypt_next(Payload {
                    msg: &self.buffer,
                    aad: &self.aad,
                })
                .map_err(|_| io::Error::other("the backup is too large to encrypt"))
                .and_then(|sealed| self.inner.write_all(&sealed));
            if let Err(e) = written {
                self.state = Encryption::Failed;
                return Err(e);
            }
            self.buffer.clear();
        }
        let taken = data.len().min(CHUNK_LEN - self.buffer.len());
        self.buffer.extend_from_slice(&data[..taken]);
        Ok(taken)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

fn earlier_write_failed() -> io::Error {
    io::Error::other("an earlier write to the backup failed")
}

#[derive(Debug, Error)]
pub enum UnlockError {
    #[error("wrong password")]
    WrongPassword,
    #[error(transparent)]
    Kdf(#[from] KdfError),
}

pub struct EncryptedBackup<R> {
    header: Header,
    inner: R,
}

impl<R: Read> EncryptedBackup<R> {
    pub fn unlock(self, password: &str) -> Result<DecryptingReader<R>, UnlockError> {
        let key = BackupKey::derive(password, &self.header.salt, &self.header.params)?;
        if !bool::from(
            key.check()
                .as_slice()
                .ct_eq(self.header.key_check.as_slice()),
        ) {
            return Err(UnlockError::WrongPassword);
        }
        Ok(DecryptingReader {
            inner: self.inner,
            state: Decryption::Open(DecryptorBE32::from_aead(
                key.cipher(),
                GenericArray::from_slice(&self.header.nonce_prefix),
            )),
            aad: self.header.to_bytes(),
            sealed: Vec::with_capacity(SEALED_CHUNK_LEN + 1),
            plain: Cursor::default(),
        })
    }
}

pub enum OpenedBackup<R> {
    Encrypted(EncryptedBackup<R>),
    /// Deprecated: plain zstd backups from before encryption. Remove once they no longer circulate.
    LegacyPlain(Chain<Cursor<[u8; PREAMBLE_LEN]>, R>),
    Unsupported,
    NotABackup,
}

pub fn open<R: Read>(mut inner: R) -> io::Result<OpenedBackup<R>> {
    let Some(preamble) = read_array::<PREAMBLE_LEN>(&mut inner)? else {
        return Ok(OpenedBackup::NotABackup);
    };
    if preamble.starts_with(&ZSTD_MAGIC) {
        return Ok(OpenedBackup::LegacyPlain(
            Cursor::new(preamble).chain(inner),
        ));
    }
    let Some((magic, [version])) = preamble.split_first_chunk::<{ MAGIC.len() }>() else {
        return Ok(OpenedBackup::NotABackup);
    };
    if *magic != MAGIC {
        return Ok(OpenedBackup::NotABackup);
    }
    if *version != FORMAT_VERSION {
        return Ok(OpenedBackup::Unsupported);
    }
    let Some(params) = read_kdf_params(&mut inner)? else {
        return Ok(OpenedBackup::NotABackup);
    };
    let Ok(params) = params else {
        return Ok(OpenedBackup::Unsupported);
    };
    let (Some(salt), Some(nonce_prefix), Some(key_check)) = (
        read_array(&mut inner)?,
        read_array(&mut inner)?,
        read_array(&mut inner)?,
    ) else {
        return Ok(OpenedBackup::NotABackup);
    };
    let header = Header {
        params,
        salt: Salt(salt),
        nonce_prefix,
        key_check,
    };
    Ok(OpenedBackup::Encrypted(EncryptedBackup { header, inner }))
}

fn read_kdf_params(
    inner: &mut impl Read,
) -> io::Result<Option<Result<KdfParams, UnsupportedKdfParams>>> {
    let (Some(m_cost), Some(t_cost), Some(p_cost)) =
        (read_array(inner)?, read_array(inner)?, read_array(inner)?)
    else {
        return Ok(None);
    };
    Ok(Some(KdfParams::new(
        u32::from_le_bytes(m_cost),
        u32::from_le_bytes(t_cost),
        u32::from_le_bytes(p_cost),
    )))
}

fn read_array<const N: usize>(inner: &mut impl Read) -> io::Result<Option<[u8; N]>> {
    let mut bytes = [0u8; N];
    Ok(read_full(inner, &mut bytes)?.then_some(bytes))
}

/// Fills `buf` completely, or reports that the stream ended first.
fn read_full(inner: &mut impl Read, buf: &mut [u8]) -> io::Result<bool> {
    match inner.read_exact(buf) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(false),
        Err(e) => Err(e),
    }
}

enum Decryption {
    Open(DecryptorBE32<XChaCha20Poly1305>),
    Finished,
    Failed,
}

pub struct DecryptingReader<R> {
    inner: R,
    state: Decryption,
    aad: Vec<u8>,
    sealed: Vec<u8>,
    plain: Cursor<Vec<u8>>,
}

impl<R: Read> DecryptingReader<R> {
    /// Reads one byte past a full chunk, because only the end of the stream tells the last chunk
    /// apart from the others.
    fn fill_sealed(&mut self) -> io::Result<()> {
        let mut scratch = [0u8; 8 * 1024];
        while self.sealed.len() <= SEALED_CHUNK_LEN {
            let wanted = (SEALED_CHUNK_LEN + 1 - self.sealed.len()).min(scratch.len());
            match self.inner.read(&mut scratch[..wanted]) {
                Ok(0) => break,
                Ok(n) => self.sealed.extend_from_slice(&scratch[..n]),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    fn open_next_chunk(
        &mut self,
        mut decryptor: DecryptorBE32<XChaCha20Poly1305>,
    ) -> Result<Decryption, chacha20poly1305::aead::Error> {
        if self.sealed.len() > SEALED_CHUNK_LEN {
            self.plain = Cursor::new(decryptor.decrypt_next(Payload {
                msg: &self.sealed[..SEALED_CHUNK_LEN],
                aad: &self.aad,
            })?);
            self.sealed.drain(..SEALED_CHUNK_LEN);
            Ok(Decryption::Open(decryptor))
        } else {
            self.plain = Cursor::new(decryptor.decrypt_last(Payload {
                msg: &self.sealed,
                aad: &self.aad,
            })?);
            self.sealed.clear();
            Ok(Decryption::Finished)
        }
    }
}

impl<R: Read> Read for DecryptingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            let n = self.plain.read(buf)?;
            if n > 0 || buf.is_empty() {
                return Ok(n);
            }
            match mem::replace(&mut self.state, Decryption::Failed) {
                Decryption::Finished => {
                    self.state = Decryption::Finished;
                    return Ok(0);
                }
                Decryption::Failed => {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, CorruptBackup));
                }
                Decryption::Open(decryptor) => {
                    if let Err(e) = self.fill_sealed() {
                        self.state = Decryption::Open(decryptor);
                        return Err(e);
                    }
                    self.state = self
                        .open_next_chunk(decryptor)
                        .unwrap_or(Decryption::Failed);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEADER_LEN: usize = PREAMBLE_LEN + 3 * 4 + SALT_LEN + NONCE_PREFIX_LEN + KEY_CHECK_LEN;

    fn cheap_secret(password: &str) -> BackupSecret {
        BackupSecret::create_with(password, KdfParams::new(64, 1, 1).unwrap()).unwrap()
    }

    fn encrypt(secret: &BackupSecret, plain: &[u8]) -> Vec<u8> {
        let mut writer = EncryptingWriter::new(Vec::new(), secret).unwrap();
        writer.write_all(plain).unwrap();
        writer.finish().unwrap()
    }

    fn unlock<'a>(
        file: &'a [u8],
        password: &str,
    ) -> Result<DecryptingReader<&'a [u8]>, UnlockError> {
        match open(file).unwrap() {
            OpenedBackup::Encrypted(backup) => backup.unlock(password),
            _ => panic!("expected an encrypted backup"),
        }
    }

    fn decrypt(file: &[u8], password: &str) -> io::Result<Vec<u8>> {
        let mut plain = Vec::new();
        unlock(file, password)
            .unwrap()
            .read_to_end(&mut plain)
            .map(|_| plain)
    }

    fn pattern(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    #[test]
    fn round_trips_at_chunk_boundaries() {
        let secret = cheap_secret("correct horse");
        for len in [
            0,
            1,
            CHUNK_LEN - 1,
            CHUNK_LEN,
            CHUNK_LEN + 1,
            3 * CHUNK_LEN + 7,
        ] {
            let plain = pattern(len);
            let file = encrypt(&secret, &plain);
            assert_eq!(decrypt(&file, "correct horse").unwrap(), plain, "len {len}");
        }
    }

    #[test]
    fn a_wrong_password_is_told_apart_from_corruption() {
        let secret = cheap_secret("correct horse");
        let file = encrypt(&secret, &pattern(10));
        assert!(matches!(
            unlock(&file, "battery staple"),
            Err(UnlockError::WrongPassword)
        ));
    }

    #[test]
    fn a_flipped_byte_in_a_later_chunk_is_corrupt() {
        let secret = cheap_secret("correct horse");
        let mut file = encrypt(&secret, &pattern(2 * CHUNK_LEN + 5));
        let index = HEADER_LEN + SEALED_CHUNK_LEN + 3;
        file[index] ^= 1;
        let error = decrypt(&file, "correct horse").unwrap_err();
        assert!(is_corrupt_backup(&error));
    }

    #[test]
    fn a_tampered_header_is_corrupt() {
        let secret = cheap_secret("correct horse");
        let mut file = encrypt(&secret, &pattern(10));
        file[HEADER_LEN - KEY_CHECK_LEN - 1] ^= 1;
        let error = decrypt(&file, "correct horse").unwrap_err();
        assert!(is_corrupt_backup(&error));
    }

    #[test]
    fn a_tampered_salt_or_key_check_reads_as_a_wrong_password() {
        let secret = cheap_secret("correct horse");
        let file = encrypt(&secret, &pattern(10));
        for index in [PREAMBLE_LEN + 3 * 4, HEADER_LEN - 1] {
            let mut tampered = file.clone();
            tampered[index] ^= 1;
            assert!(matches!(
                unlock(&tampered, "correct horse"),
                Err(UnlockError::WrongPassword)
            ));
        }
    }

    #[test]
    fn bytes_appended_after_the_last_chunk_are_corrupt() {
        let secret = cheap_secret("correct horse");
        for len in [10, CHUNK_LEN] {
            let mut file = encrypt(&secret, &pattern(len));
            file.extend_from_slice(b"trailing");
            let error = decrypt(&file, "correct horse").unwrap_err();
            assert!(is_corrupt_backup(&error), "len {len}");
        }
    }

    struct FailsAfterHeader {
        writes: usize,
    }

    impl Write for FailsAfterHeader {
        fn write(&mut self, data: &[u8]) -> io::Result<usize> {
            self.writes += 1;
            if self.writes > 1 {
                return Err(io::Error::other("the disk is full"));
            }
            Ok(data.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn after_a_failed_write_the_writer_refuses_everything() {
        let secret = cheap_secret("correct horse");
        let mut writer = EncryptingWriter::new(FailsAfterHeader { writes: 0 }, &secret).unwrap();
        writer.write_all(&pattern(CHUNK_LEN)).unwrap();
        assert!(writer.write(&[1]).is_err());
        assert_eq!(
            writer.write(&[1]).unwrap_err().to_string(),
            earlier_write_failed().to_string()
        );
        assert!(writer.finish().is_err());
    }

    #[test]
    fn a_backup_cut_at_a_chunk_boundary_is_corrupt() {
        let secret = cheap_secret("correct horse");
        let file = encrypt(&secret, &pattern(2 * CHUNK_LEN + 5));
        let cut = &file[..HEADER_LEN + 2 * SEALED_CHUNK_LEN];
        let error = decrypt(cut, "correct horse").unwrap_err();
        assert!(is_corrupt_backup(&error));
    }

    /// Fails once, after `fail_after` bytes, then serves the rest.
    struct FailsOnce<'a> {
        data: &'a [u8],
        fail_after: usize,
        failed: bool,
    }

    impl Read for FailsOnce<'_> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if !self.failed && self.fail_after == 0 {
                self.failed = true;
                return Err(io::Error::other("the disk hiccuped"));
            }
            let limit = if self.failed {
                buf.len()
            } else {
                self.fail_after
            };
            let n = buf.len().min(limit).min(self.data.len());
            buf[..n].copy_from_slice(&self.data[..n]);
            self.data = &self.data[n..];
            if !self.failed {
                self.fail_after -= n;
            }
            Ok(n)
        }
    }

    #[test]
    fn a_read_error_is_reported_as_such_and_reading_can_go_on() {
        let secret = cheap_secret("correct horse");
        let plain = pattern(2 * CHUNK_LEN + 5);
        let file = encrypt(&secret, &plain);
        let flaky = FailsOnce {
            data: &file,
            fail_after: HEADER_LEN + 100,
            failed: false,
        };
        let OpenedBackup::Encrypted(backup) = open(flaky).unwrap() else {
            panic!("expected an encrypted backup");
        };
        let mut reader = backup.unlock("correct horse").unwrap();
        let mut decrypted = Vec::new();
        let error = reader.read_to_end(&mut decrypted).unwrap_err();
        assert!(!is_corrupt_backup(&error));
        reader.read_to_end(&mut decrypted).unwrap();
        assert_eq!(decrypted, plain);
    }

    #[test]
    fn the_stored_secret_verifies_its_password() {
        let secret = cheap_secret("correct horse");
        assert!(secret.verify("correct horse").unwrap());
        assert!(!secret.verify("correct horsE").unwrap());
    }

    #[test]
    fn a_secret_rebuilt_from_its_parts_decrypts_earlier_backups() {
        let secret = cheap_secret("correct horse");
        let file = encrypt(&secret, &pattern(100));
        let rebuilt = BackupSecret::from_parts(
            *secret.salt(),
            secret.params().clone(),
            BackupKey::from(*secret.key().as_bytes()),
        );
        let file_again = encrypt(&rebuilt, &pattern(100));
        assert_eq!(decrypt(&file, "correct horse").unwrap(), pattern(100));
        assert_eq!(decrypt(&file_again, "correct horse").unwrap(), pattern(100));
    }

    #[test]
    fn a_zstd_stream_opens_as_legacy_with_its_first_bytes_intact() {
        let compressed = zstd_frame(b"legacy database");
        let OpenedBackup::LegacyPlain(reader) = open(compressed.as_slice()).unwrap() else {
            panic!("expected a legacy backup");
        };
        let mut bytes = Vec::new();
        let mut reader = reader;
        reader.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, compressed);
    }

    #[test]
    fn other_files_are_not_backups() {
        assert!(matches!(
            open(&b"hi"[..]).unwrap(),
            OpenedBackup::NotABackup
        ));
        assert!(matches!(
            open(&b"SQLite format 3\0"[..]).unwrap(),
            OpenedBackup::NotABackup
        ));
    }

    #[test]
    fn a_newer_format_version_is_unsupported() {
        let secret = cheap_secret("correct horse");
        let mut file = encrypt(&secret, &pattern(10));
        file[MAGIC.len()] = FORMAT_VERSION + 1;
        assert!(matches!(
            open(file.as_slice()).unwrap(),
            OpenedBackup::Unsupported
        ));
    }

    #[test]
    fn a_header_demanding_too_much_memory_is_unsupported() {
        let secret = cheap_secret("correct horse");
        let mut file = encrypt(&secret, &pattern(10));
        file[PREAMBLE_LEN..PREAMBLE_LEN + 4].copy_from_slice(&(MAX_M_COST_KIB + 1).to_le_bytes());
        assert!(matches!(
            open(file.as_slice()).unwrap(),
            OpenedBackup::Unsupported
        ));
    }

    fn zstd_frame(payload: &[u8]) -> Vec<u8> {
        let mut frame = ZSTD_MAGIC.to_vec();
        frame.extend_from_slice(payload);
        frame
    }
}
