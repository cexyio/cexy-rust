//! `cancel_all_until_done`: repeats cancel-all until nothing is left to do.

use std::collections::HashMap;
use std::time::Duration;

use crate::error::{Error, ErrorCategory, Result};
use crate::models_gen::{CancelAllResult, CancelFailure, ErrorCode};
use crate::services::Trading;

/// What `cancel_all_until_done` cancels. There is no default, so an account-wide cancel is
/// always explicit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancelAllTarget {
    /// One market, such as `"BTC/USDT"`.
    Symbol(String),
    /// Every market.
    AllMarkets,
}

/// Target and bounds of [`Trading::cancel_all_until_done`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelAllOptions {
    /// What to cancel.
    pub target: CancelAllTarget,
    /// Calls at most. Default 20 (the server allows 30 cancel-all calls a minute).
    pub max_rounds: u32,
    /// Stop before a back-off would bring the elapsed time to or past this. Default 120 s (the
    /// server closes an order stuck being placed within about 90 s).
    pub time_budget: Duration,
}

impl CancelAllOptions {
    /// One market, with the default bounds.
    pub fn symbol(symbol: impl Into<String>) -> Self {
        CancelAllOptions::target(CancelAllTarget::Symbol(symbol.into()))
    }

    /// Every market, with the default bounds.
    pub fn all_markets() -> Self {
        CancelAllOptions::target(CancelAllTarget::AllMarkets)
    }

    fn target(target: CancelAllTarget) -> Self {
        CancelAllOptions {
            target,
            max_rounds: 20,
            time_budget: Duration::from_secs(120),
        }
    }
}

/// Why `cancel_all_until_done` stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelAllStop {
    /// Nothing left to do.
    Done,
    /// `max_rounds` calls were made.
    MaxRounds,
    /// The next back-off would have reached the time budget.
    TimeBudget,
    /// An error stopped the loop (see [`Error::CancelAllInterrupted`]).
    Error,
}

impl CancelAllStop {
    /// `"done"`, `"max_rounds"`, `"time_budget"` or `"error"`.
    pub fn as_str(self) -> &'static str {
        match self {
            CancelAllStop::Done => "done",
            CancelAllStop::MaxRounds => "max_rounds",
            CancelAllStop::TimeBudget => "time_budget",
            CancelAllStop::Error => "error",
        }
    }
}

/// The rounds of `cancel_all_until_done` merged by order id; an order's latest state wins (an
/// order that failed in one round and was cancelled in a later one is only in `cancelled`).
#[derive(Debug, Clone, PartialEq)]
pub struct CancelAllSummary {
    /// Orders cancelled.
    pub cancelled: Vec<String>,
    /// Orders that closed on their own first (not failures).
    pub already_closed: Vec<String>,
    /// Orders still failed after the last round.
    pub failed: Vec<String>,
    /// The latest reason for each order in `failed`.
    pub failures: Vec<CancelFailure>,
    /// The last round's `has_more`.
    pub has_more: bool,
    /// Calls made (each round is exactly one HTTP request).
    pub rounds: u32,
    /// Why the loop stopped.
    pub stopped: CancelAllStop,
    /// The error code of the last round, if that round failed with a retryable error (for
    /// example `SERVICE_UNAVAILABLE` or `RATE_LIMITED`); `CONNECTION` for a network failure.
    /// Also `RATE_LIMITED` when the loop stopped because the client-side rate limiter (after
    /// `X-RateLimit-Remaining: 0`) would have held the next call past the budget.
    pub last_error_code: Option<String>,
}

const BACKOFF: [Duration; 5] = [
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
    Duration::from_secs(8),
    Duration::from_secs(15),
];

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Cancelled,
    AlreadyClosed,
    Failed,
}

#[derive(Default)]
struct Merge {
    order: Vec<String>,
    state: HashMap<String, State>,
    failures: HashMap<String, CancelFailure>,
}

impl Merge {
    fn set(&mut self, id: &str, st: State) {
        if self.state.insert(id.to_string(), st).is_none() {
            self.order.push(id.to_string());
        }
    }

    fn add(&mut self, r: &CancelAllResult) {
        for id in &r.cancelled {
            self.set(id, State::Cancelled);
        }
        for id in &r.already_closed {
            self.set(id, State::AlreadyClosed);
        }
        for id in &r.failed {
            self.set(id, State::Failed);
            let reason = r
                .failures
                .iter()
                .find(|f| &f.order_id == id)
                .cloned()
                .unwrap_or_else(|| CancelFailure {
                    order_id: id.clone(),
                    code: String::new(),
                    message: String::new(),
                });
            self.failures.insert(id.clone(), reason);
        }
    }

    fn summary(&self, rounds: u32, stopped: CancelAllStop, has_more: bool) -> CancelAllSummary {
        let mut s = CancelAllSummary {
            cancelled: vec![],
            already_closed: vec![],
            failed: vec![],
            failures: vec![],
            has_more,
            rounds,
            stopped,
            last_error_code: None,
        };
        for id in &self.order {
            match self.state[id] {
                State::Cancelled => s.cancelled.push(id.clone()),
                State::AlreadyClosed => s.already_closed.push(id.clone()),
                State::Failed => {
                    s.failed.push(id.clone());
                    s.failures.push(self.failures[id].clone());
                }
            }
        }
        s
    }
}

/// Whether a failed round is worth repeating: a network failure, or an API error the server
/// marked retryable (429 included, whatever its Retry-After: the loop decides against its
/// budget).
fn round_retryable(e: &Error) -> bool {
    match e {
        Error::Connection(_) => true,
        Error::Api(a) => a.retryable || a.code == ErrorCode::ConcurrentModification,
        _ => false,
    }
}

fn error_code(e: &Error) -> String {
    match e {
        Error::Api(a) => a.code.as_str().to_string(),
        _ => "CONNECTION".to_string(),
    }
}

impl Trading<'_> {
    /// Repeats cancel-all until nothing is left to do: while `has_more` is set, or while an
    /// order failed with `INVALID_STATE` (still being placed) or `SERVICE_UNAVAILABLE`.
    ///
    /// Each round is exactly one HTTP request (the transport does not retry inside the loop),
    /// so at most `max_rounds` requests are sent. After a round with no progress (nothing
    /// cancelled or already closed, or a retryable error such as a 503 or a network failure)
    /// it waits 1, 2, 4, 8, then 15 s before the next call; any progress resets the wait. After
    /// a 429 it waits exactly the server's Retry-After instead, without advancing the back-off.
    /// It stops after `max_rounds` calls, or when the next wait would bring the elapsed time to
    /// or past `time_budget` (that wait is not taken; `last_error_code` says why if a round
    /// failed). Other failure codes are returned in the summary, never retried by the loop.
    ///
    /// A non-retryable error (such as 403) stops it with [`Error::CancelAllInterrupted`], which
    /// carries the summary so far.
    pub async fn cancel_all_until_done(
        &self,
        options: &CancelAllOptions,
    ) -> Result<CancelAllSummary> {
        let symbol = match &options.target {
            CancelAllTarget::Symbol(s) if s.is_empty() => {
                return Err(Error::config(
                    "trading.cancel_all_until_done: symbol is empty; use CancelAllTarget::AllMarkets to cancel in every market",
                ));
            }
            CancelAllTarget::Symbol(s) => Some(s.as_str()),
            CancelAllTarget::AllMarkets => None,
        };
        let max_rounds = if options.max_rounds == 0 {
            20
        } else {
            options.max_rounds
        };
        let budget = if options.time_budget.is_zero() {
            Duration::from_secs(120)
        } else {
            options.time_budget
        };
        let clock = &self.c.t.clock;

        let mut m = Merge::default();
        let start = clock.now();
        let mut wait = 0usize;
        let mut round = 0u32;
        let mut has_more = false;
        loop {
            // The client-side rate limiter may be holding requests back (X-RateLimit-Remaining 0
            // with a Reset, or a 429): the next call would wait that long inside the transport,
            // unseen by the budget check below. Count it here, and stop instead of calling when it
            // would reach the budget.
            if round > 0 {
                let blocked = self
                    .c
                    .rate_limit()
                    .map(|s| s.blocked_for)
                    .unwrap_or_default();
                let elapsed = clock.now().saturating_duration_since(start);
                if !blocked.is_zero() && elapsed.saturating_add(blocked) >= budget {
                    let mut s = m.summary(round, CancelAllStop::TimeBudget, has_more);
                    s.last_error_code = Some(ErrorCode::RateLimited.as_str().to_string());
                    return Ok(s);
                }
            }
            round += 1;
            let (d, last_error) = match self.cancel_all_single_request(symbol).await {
                Ok(res) => {
                    has_more = res.has_more;
                    m.add(&res);
                    let retry = res.failures.iter().any(|f| {
                        f.code == ErrorCode::InvalidState.as_str()
                            || f.code == ErrorCode::ServiceUnavailable.as_str()
                    });
                    if !res.has_more && !retry {
                        return Ok(m.summary(round, CancelAllStop::Done, has_more));
                    }
                    if !res.cancelled.is_empty() || !res.already_closed.is_empty() {
                        wait = 0;
                        (Duration::ZERO, None)
                    } else {
                        let d = BACKOFF[wait.min(BACKOFF.len() - 1)];
                        wait += 1;
                        (d, None)
                    }
                }
                Err(e) if round_retryable(&e) => {
                    let d = match e.api().and_then(|a| a.retry_after) {
                        Some(ra) if e.is(ErrorCategory::RateLimited) => ra,
                        _ => {
                            let d = BACKOFF[wait.min(BACKOFF.len() - 1)];
                            wait += 1;
                            d
                        }
                    };
                    (d, Some(error_code(&e)))
                }
                Err(e) => {
                    let summary = m.summary(round, CancelAllStop::Error, has_more);
                    return Err(Error::CancelAllInterrupted {
                        source: Box::new(e),
                        summary: Box::new(summary),
                    });
                }
            };
            let stop = |m: &Merge, why| {
                let mut s = m.summary(round, why, has_more);
                s.last_error_code = last_error.clone();
                s
            };
            if round >= max_rounds {
                return Ok(stop(&m, CancelAllStop::MaxRounds));
            }
            let elapsed = clock.now().saturating_duration_since(start);
            if elapsed.saturating_add(d) >= budget {
                return Ok(stop(&m, CancelAllStop::TimeBudget));
            }
            if !d.is_zero() {
                clock.sleep(d).await;
            }
        }
    }
}
