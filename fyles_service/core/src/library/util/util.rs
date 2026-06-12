use tokio::{sync::MutexGuard, time::timeout};

use crate::{core::brain::types::ByteChallenge, library::util::duration_ext::DurationExt};

pub fn generate_random_bytes(length: usize) -> Vec<u8> {
    generate_random_bytes_internal(length)
}

fn generate_random_bytes_internal(length: usize) -> Vec<u8> {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    // Should be able to call rng.random() but rust analyzer claims it does not exist.
    // But it does. Idk
    (0..length).map(|_| rng.r#gen()).collect()
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
            timeout(5.seconds(), self.lock())
                .await
                .expect("Locking timed out")
        } else {
            self.lock().await
        }
    }
}
