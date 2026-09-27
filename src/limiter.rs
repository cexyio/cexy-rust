//! Client-side rate limiter: a token bucket that adapts to the server's `X-RateLimit-*` headers.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use reqwest::header::HeaderMap;

use crate::clock::Clock;
use crate::error::{MAX_SERVER_WAIT, header, server_secs};

/// A snapshot of the client-side rate limiter.
#[derive(Debug, Clone, PartialEq)]
pub struct RateLimitState {
    /// Current limit (it only ever adapts downwards).
    pub requests_per_minute: f64,
    /// Tokens available now.
    pub tokens: f64,
    /// Requests wait until this much time has passed (a 429 or an exhausted window).
    pub blocked_for: Duration,
}

/// Token bucket. It adapts downwards to `X-RateLimit-Limit`, `X-RateLimit-Remaining` and
/// `X-RateLimit-Reset` (seconds until the window resets), and to the Retry-After of a 429. It
/// never raises the configured limit.
pub(crate) struct RateLimiter {
    state: Mutex<Bucket>,
    clock: Arc<dyn Clock>,
}

struct Bucket {
    rpm: f64,
    tokens: f64,
    last: Instant,
    blocked_until: Option<Instant>,
}

impl RateLimiter {
    pub(crate) fn new(rpm: u32, clock: Arc<dyn Clock>) -> Self {
        let now = clock.now();
        RateLimiter {
            state: Mutex::new(Bucket {
                rpm: rpm as f64,
                tokens: rpm as f64,
                last: now,
                blocked_until: None,
            }),
            clock,
        }
    }

    pub(crate) fn state(&self) -> RateLimitState {
        let now = self.clock.now();
        let mut b = self.state.lock().unwrap();
        b.refill(now);
        RateLimitState {
            requests_per_minute: b.rpm,
            tokens: b.tokens,
            blocked_for: b
                .blocked_until
                .map(|u| u.saturating_duration_since(now))
                .unwrap_or_default(),
        }
    }

    /// Waits until a request may be sent, then takes a token.
    pub(crate) async fn acquire(&self) {
        loop {
            let wait = {
                let now = self.clock.now();
                let mut b = self.state.lock().unwrap();
                b.refill(now);
                match b.blocked_until {
                    Some(u) if now < u => u - now,
                    _ if b.tokens >= 1.0 => {
                        b.tokens -= 1.0;
                        return;
                    }
                    _ => {
                        let per_token = 60.0 / b.rpm;
                        server_secs((1.0 - b.tokens) * per_token)
                            .unwrap_or(Duration::from_millis(1))
                            .min(MAX_SERVER_WAIT)
                    }
                }
            };
            self.clock.sleep(wait).await;
        }
    }

    /// Adapts to the server's rate-limit headers.
    pub(crate) fn update(&self, headers: &HeaderMap) {
        let now = self.clock.now();
        let mut b = self.state.lock().unwrap();
        b.refill(now);
        // A limit below 1 a minute is not a real limit (and would stall the client): ignored.
        if let Some(limit) = num(headers, "x-ratelimit-limit")
            && limit >= 1.0
            && limit < b.rpm
        {
            b.rpm = limit;
            b.tokens = b.tokens.min(limit);
        }
        let Some(remaining) = num(headers, "x-ratelimit-remaining") else {
            return;
        };
        if remaining >= 0.0 && remaining < b.tokens {
            b.tokens = remaining;
        }
        if remaining == 0.0 {
            let d = match num(headers, "x-ratelimit-reset") {
                Some(reset) => reset_duration(reset),
                None => server_secs(60.0 / b.rpm).unwrap_or(Duration::ZERO),
            };
            b.block(now, d);
        }
    }

    /// Blocks every request for `d` (a 429's Retry-After).
    pub(crate) fn block_for(&self, d: Duration) {
        let now = self.clock.now();
        self.state.lock().unwrap().block(now, d);
    }
}

impl Bucket {
    fn refill(&mut self, now: Instant) {
        if now > self.last {
            let elapsed = (now - self.last).as_secs_f64() / 60.0;
            self.tokens = self.rpm.min(self.tokens + elapsed * self.rpm);
            self.last = now;
        }
    }

    /// Blocks until `now + d`, with `d` capped at [`MAX_SERVER_WAIT`]: a server hint never
    /// stalls the client longer than that.
    fn block(&mut self, now: Instant, d: Duration) {
        if d.is_zero() {
            return;
        }
        let Some(until) = now.checked_add(d.min(MAX_SERVER_WAIT)) else {
            return;
        };
        if self.blocked_until.is_none_or(|u| until > u) {
            self.blocked_until = Some(until);
        }
    }
}

/// `X-RateLimit-Reset` is seconds until the window resets. Values that can only be epoch
/// timestamps (seconds or milliseconds) are tolerated.
fn reset_duration(reset: f64) -> Duration {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as f64;
    let secs = if reset > 1e12 {
        (reset - now_ms) / 1000.0
    } else if reset > 1e9 {
        reset - now_ms / 1000.0
    } else {
        reset
    };
    server_secs(secs).unwrap_or(Duration::ZERO)
}

fn num(headers: &HeaderMap, name: &str) -> Option<f64> {
    header(headers, name)
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|f| f.is_finite())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::FakeClock;
    use reqwest::header::{HeaderName, HeaderValue};

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(
                HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        h
    }

    #[tokio::test]
    async fn spends_tokens_then_waits() {
        let clock = FakeClock::default();
        let l = RateLimiter::new(60, Arc::new(clock.clone()));
        for _ in 0..60 {
            l.acquire().await;
        }
        assert!(clock.sleeps().is_empty());
        l.acquire().await;
        assert_eq!(clock.sleeps(), vec![Duration::from_secs(1)]);
    }

    #[tokio::test]
    async fn adapts_to_headers_and_reset_seconds() {
        let clock = FakeClock::default();
        let l = RateLimiter::new(300, Arc::new(clock.clone()));
        l.update(&headers(&[
            ("X-RateLimit-Limit", "120"),
            ("X-RateLimit-Remaining", "0"),
            ("X-RateLimit-Reset", "7"),
        ]));
        let s = l.state();
        assert_eq!(s.requests_per_minute, 120.0);
        assert_eq!(s.blocked_for, Duration::from_secs(7));
        l.acquire().await;
        assert_eq!(clock.sleeps()[0], Duration::from_secs(7));
    }

    #[tokio::test]
    async fn absurd_server_hints_never_panic_or_stall() {
        let clock = FakeClock::default();
        let l = RateLimiter::new(300, Arc::new(clock.clone()));
        for (limit, reset) in [("1e-300", "1e300"), ("0.0001", "9999999999"), ("1", "1e20")] {
            l.update(&headers(&[
                ("X-RateLimit-Limit", limit),
                ("X-RateLimit-Remaining", "0"),
                ("X-RateLimit-Reset", reset),
            ]));
        }
        l.block_for(Duration::MAX);
        assert!(l.state().blocked_for <= MAX_SERVER_WAIT);
        l.acquire().await;
        l.acquire().await;
        assert!(clock.sleeps().iter().all(|d| *d <= MAX_SERVER_WAIT));
    }

    #[tokio::test]
    async fn never_raises_the_limit() {
        let l = RateLimiter::new(100, Arc::new(FakeClock::default()));
        l.update(&headers(&[("X-RateLimit-Limit", "600")]));
        assert_eq!(l.state().requests_per_minute, 100.0);
    }
}
