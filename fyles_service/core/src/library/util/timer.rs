use std::future::Future;
use std::time::Duration;
use tokio::time;

pub struct IntervalGuard {
    abort_handle: tokio::task::AbortHandle,
}

impl Drop for IntervalGuard {
    fn drop(&mut self) {
        self.abort_handle.abort();
    }
}

#[must_use = "You must keep this guard alive or the timeout will be cancelled"]
#[derive(Debug)]
pub struct TimeoutGuard {
    abort_handle: tokio::task::AbortHandle,
}

impl Drop for TimeoutGuard {
    fn drop(&mut self) {
        self.abort_handle.abort();
    }
}

pub fn set_interval<F, Fut>(mut f: F, dur: Duration, initial_delay: Duration) -> IntervalGuard
where
    F: Send + 'static + FnMut() -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    let mut interval = time::interval(dur);

    let handle = tokio::spawn(async move {
        // Skip the first tick at 0ms.
        interval.tick().await;
        // Wait for the initial delay.
        time::sleep(initial_delay).await;
        loop {
            interval.tick().await;
            tokio::spawn(f());
        }
    });

    IntervalGuard {
        abort_handle: handle.abort_handle(),
    }
}

pub fn set_timeout<F>(f: F, delay: Duration) -> TimeoutGuard
where
    F: Send + 'static + FnOnce(),
{
    let handle = tokio::spawn(async move {
        time::sleep(delay).await;
        f();
    });

    TimeoutGuard {
        abort_handle: handle.abort_handle(),
    }
}

pub fn set_timeout_detached<F>(f: F, delay: Duration)
where
    F: Send + 'static + FnOnce(),
{
    tokio::spawn(async move {
        time::sleep(delay).await;
        f();
    });
}
