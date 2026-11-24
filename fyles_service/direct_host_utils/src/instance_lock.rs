use std::fs::{File, OpenOptions, TryLockError};
use std::io;
use std::path::{Path, PathBuf};
use std::process;

use fyles_core::Endpoint;
use thiserror::Error;

/// The exit code of a daemon that found another one on its database or socket. Service managers
/// count it as a clean exit.
pub const EXIT_ALREADY_RUNNING: i32 = 75;

/// Held for the daemon's whole life, and dropped by the kernel when the holder dies. The database
/// lock keeps one identity to one daemon. The socket lock makes it safe for the server to remove a
/// leftover socket before binding.
pub struct InstanceLock {
    _database: File,
    _socket: Option<File>,
}

#[derive(Debug, Error)]
pub enum InstanceLockError {
    #[error("a Fyles daemon already uses {}", .0.display())]
    DatabaseInUse(PathBuf),
    #[error("a Fyles daemon already serves {0}")]
    SocketServed(String),
    #[error("cannot lock {}: {source}", .path.display())]
    Io { path: PathBuf, source: io::Error },
}

pub fn claim(db_path: &Path, endpoint: &Endpoint) -> Result<InstanceLock, InstanceLockError> {
    let database = lock_beside(db_path, || {
        InstanceLockError::DatabaseInUse(db_path.to_path_buf())
    })?;
    let socket = match endpoint {
        Endpoint::Uds { path } => Some(lock_beside(Path::new(path), || {
            InstanceLockError::SocketServed(path.clone())
        })?),
        Endpoint::Tcp { .. } => None,
    };
    Ok(InstanceLock {
        _database: database,
        _socket: socket,
    })
}

pub fn claim_or_exit(db_path: &Path, endpoint: &Endpoint) -> InstanceLock {
    match claim(db_path, endpoint) {
        Ok(lock) => lock,
        Err(e @ (InstanceLockError::DatabaseInUse(_) | InstanceLockError::SocketServed(_))) => {
            eprintln!("{e}");
            process::exit(EXIT_ALREADY_RUNNING);
        }
        Err(e) => panic!("{e}"),
    }
}

fn lock_beside(
    target: &Path,
    held: impl FnOnce() -> InstanceLockError,
) -> Result<File, InstanceLockError> {
    let mut path = target.as_os_str().to_owned();
    path.push(".lock");
    let path = PathBuf::from(path);
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path);
    let file = match file {
        Ok(file) => file,
        Err(source) => return Err(InstanceLockError::Io { path, source }),
    };
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(TryLockError::WouldBlock) => Err(held()),
        Err(TryLockError::Error(source)) => Err(InstanceLockError::Io { path, source }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uds(dir: &Path, name: &str) -> Endpoint {
        Endpoint::Uds {
            path: dir.join(name).to_string_lossy().into_owned(),
        }
    }

    fn tcp() -> Endpoint {
        Endpoint::Tcp {
            host: "127.0.0.1".into(),
            port: None,
        }
    }

    #[test]
    fn a_second_daemon_on_the_same_database_is_refused_until_the_first_ends() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("fyles.db");
        let first = claim(&db, &tcp()).unwrap();
        assert!(matches!(
            claim(&db, &uds(dir.path(), "other.sock")),
            Err(InstanceLockError::DatabaseInUse(_))
        ));
        drop(first);
        assert!(claim(&db, &tcp()).is_ok());
    }

    #[test]
    fn a_second_daemon_on_the_same_socket_is_refused_even_with_its_own_database() {
        let dir = tempfile::tempdir().unwrap();
        let _first = claim(&dir.path().join("a.db"), &uds(dir.path(), "fyles.sock")).unwrap();
        assert!(matches!(
            claim(&dir.path().join("b.db"), &uds(dir.path(), "fyles.sock")),
            Err(InstanceLockError::SocketServed(_))
        ));
    }

    #[test]
    fn daemons_on_their_own_database_and_endpoint_run_side_by_side() {
        let dir = tempfile::tempdir().unwrap();
        let _prod = claim(&dir.path().join("a.db"), &uds(dir.path(), "fyles.sock")).unwrap();
        assert!(claim(&dir.path().join("b.db"), &tcp()).is_ok());
    }
}
