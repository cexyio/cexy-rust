//! Time source and sleeping, injectable so that tests run on a fake clock.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub(crate) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The SDK's clock: every wait (retry backoff, rate limiter, the cancel-all loop) goes through
/// `sleep`, and elapsed time is measured with `now`, so a fake clock sees all of it.
pub(crate) trait Clock: Send + Sync {
    fn now(&self) -> Instant;
    fn sleep(&self, d: Duration) -> BoxFuture<'_, ()>;
}

/// The real clock (tokio timers).
pub(crate) struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
    fn sleep(&self, d: Duration) -> BoxFuture<'_, ()> {
        Box::pin(tokio::time::sleep(d))
    }
}

pub(crate) fn system() -> Arc<dyn Clock> {
    Arc::new(SystemClock)
}

/// A fake clock for tests: sleeping advances it instantly and records the duration.
#[cfg(test)]
#[derive(Clone)]
pub(crate) struct FakeClock {
    inner: Arc<Mutex<FakeState>>,
}

#[cfg(test)]
struct FakeState {
    base: Instant,
    elapsed: Duration,
    sleeps: Vec<Duration>,
}

#[cfg(test)]
impl Default for FakeClock {
    fn default() -> Self {
        FakeClock {
            inner: Arc::new(Mutex::new(FakeState {
                base: Instant::now(),
                elapsed: Duration::ZERO,
                sleeps: vec![],
            })),
        }
    }
}

#[cfg(test)]
impl FakeClock {
    /// Every sleep so far.
    pub(crate) fn sleeps(&self) -> Vec<Duration> {
        self.inner.lock().unwrap().sleeps.clone()
    }
    /// Time advanced so far.
    pub(crate) fn elapsed(&self) -> Duration {
        self.inner.lock().unwrap().elapsed
    }
}

#[cfg(test)]
impl Clock for FakeClock {
    fn now(&self) -> Instant {
        let s = self.inner.lock().unwrap();
        s.base + s.elapsed
    }
    fn sleep(&self, d: Duration) -> BoxFuture<'_, ()> {
        let mut s = self.inner.lock().unwrap();
        s.elapsed += d;
        s.sleeps.push(d);
        Box::pin(std::future::ready(()))
    }
}
