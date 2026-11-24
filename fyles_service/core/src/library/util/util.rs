use tokio::{
    sync::{MutexGuard, RwLockReadGuard, RwLockWriteGuard},
    time::timeout,
};

use crate::constants::LOCK_TIMEOUT;
use crate::core::brain::types::ByteChallenge;

pub fn generate_random_bytes(length: usize) -> Vec<u8> {
    generate_random_bytes_internal(length)
}

fn generate_random_bytes_internal(length: usize) -> Vec<u8> {
    use rand::RngExt;
    let mut rng = rand::rng();
    (0..length).map(|_| rng.random()).collect()
}

pub fn generate_byte_challenge() -> ByteChallenge {
    generate_random_bytes_internal(32)
}

pub trait OptionInspectMut<T> {
    fn inspect_mut<F>(self, f: F) -> Self
    where
        F: FnOnce(&mut T);

    fn inspect_mut_ref<F>(&mut self, f: F) -> &mut Self
    where
        F: FnOnce(&mut T);
}

impl<T> OptionInspectMut<T> for Option<T> {
    fn inspect_mut<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut T),
    {
        if let Some(x) = self.as_mut() {
            f(x);
        }
        self
    }

    fn inspect_mut_ref<F>(&mut self, f: F) -> &mut Self
    where
        F: FnOnce(&mut T),
    {
        if let Some(x) = self.as_mut() {
            f(x);
        }
        self
    }
}

impl<T> OptionInspectMut<T> for &mut Option<T> {
    fn inspect_mut<F>(self, f: F) -> Self
    where
        F: FnOnce(&mut T),
    {
        if let Some(x) = self.as_mut() {
            f(x);
        }
        self
    }

    fn inspect_mut_ref<F>(&mut self, f: F) -> &mut Self
    where
        F: FnOnce(&mut T),
    {
        if let Some(x) = self.as_mut() {
            f(x);
        }
        self
    }
}

#[async_trait::async_trait]
pub trait TimeoutLock<T> {
    async fn timeout_lock(&self) -> MutexGuard<'_, T>;
}

#[async_trait::async_trait]
impl<T: Send> TimeoutLock<T> for tokio::sync::Mutex<T> {
    async fn timeout_lock(&self) -> MutexGuard<'_, T> {
        if cfg!(debug_assertions) {
            timeout(LOCK_TIMEOUT, self.lock())
                .await
                .expect("Locking timed out")
        } else {
            self.lock().await
        }
    }
}

/// The [`TimeoutLock`] counterpart for `RwLock`, which needs one method per guard type.
#[async_trait::async_trait]
pub trait TimeoutRwLock<T> {
    async fn timeout_read(&self) -> RwLockReadGuard<'_, T>;
    async fn timeout_write(&self) -> RwLockWriteGuard<'_, T>;
}

#[async_trait::async_trait]
impl<T: Send + Sync> TimeoutRwLock<T> for tokio::sync::RwLock<T> {
    async fn timeout_read(&self) -> RwLockReadGuard<'_, T> {
        if cfg!(debug_assertions) {
            timeout(LOCK_TIMEOUT, self.read())
                .await
                .expect("Read locking timed out")
        } else {
            self.read().await
        }
    }

    async fn timeout_write(&self) -> RwLockWriteGuard<'_, T> {
        if cfg!(debug_assertions) {
            timeout(LOCK_TIMEOUT, self.write())
                .await
                .expect("Write locking timed out")
        } else {
            self.write().await
        }
    }
}
