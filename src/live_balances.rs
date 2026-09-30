//! Live balances: the `balances` channel plus REST snapshots (see [`LiveBalances`]).

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use futures_util::future::BoxFuture;
use serde::Deserialize;
use serde_json::Value;

use crate::amount::Amount;
use crate::error::{Error, Result, WsError};
use crate::models_gen::Balance;
use crate::ws::{AuthChangeReason, Inner, WebSocket, WsEvent, WsTimer};

/// Where snapshots come from.
pub type BalanceSnapshotFn =
    Arc<dyn Fn() -> BoxFuture<'static, Result<Vec<Balance>>> + Send + Sync>;
/// Where the snapshot source's owner user id comes from.
pub type OwnerIdFn = Arc<dyn Fn() -> BoxFuture<'static, Result<String>> + Send + Sync>;

/// Configures [`WebSocket::live_balances`].
#[derive(Clone, Default)]
pub struct LiveBalancesOptions {
    /// Where snapshots come from. Default: the [`crate::Client`]'s `account().balances()` (use
    /// [`crate::Client::websocket`]). It must return the balances of the same account the
    /// WebSocket is authenticated as.
    pub snapshot: Option<BalanceSnapshotFn>,
    /// The user id the snapshot source belongs to, compared with the WebSocket's authenticated
    /// user at the start and after every account change. Default: the Client's `account().id()` (GET /api/v1/account/id).
    pub owner_id: Option<OwnerIdFn>,
    /// A fixed owner user id instead of `owner_id`.
    pub account_id: Option<String>,
    /// Minimum time between successful snapshots (the API key's rate limit is shared). `None`:
    /// 2 s; `Some(Duration::ZERO)`: no minimum.
    pub min_snapshot_interval: Option<Duration>,
    /// Retry delay after a failed snapshot or owner lookup. Default 1 s (doubles up to 30 s).
    pub retry_delay: Option<Duration>,
}

impl std::fmt::Debug for LiveBalancesOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveBalancesOptions")
            .field("account_id", &self.account_id)
            .field("min_snapshot_interval", &self.min_snapshot_interval)
            .finish_non_exhaustive()
    }
}

/// A [`LiveBalances`] changed state (read from [`crate::WsEvents`] as `WsEvent::Balances`).
#[derive(Debug)]
#[non_exhaustive]
pub enum BalancesEvent {
    /// One asset changed; `None`: the row was removed because its total reached 0.
    Updated {
        /// The asset.
        asset: String,
        /// The new row.
        balance: Option<Balance>,
    },
    /// A snapshot was applied.
    Snapshot {
        /// Why it was taken (`start`, `resubscribed`, `sequence_gap`, `balances_resync`,
        /// `concurrent_modification`, `retry`).
        reason: String,
    },
    /// The snapshot source belongs to another account: nothing was merged.
    AccountMismatch {
        /// The WebSocket's authenticated user.
        websocket_user_id: String,
        /// The snapshot source's owner.
        snapshot_user_id: String,
    },
    /// A snapshot or owner lookup failed; retried with backoff.
    Error(Error),
}

/// The data of `balance.updated`. `sequence` is missing from servers that predate live balances.
#[derive(Debug, Clone, Deserialize)]
struct BalanceUpdate {
    asset: String,
    #[serde(default)]
    available: Option<Amount>,
    #[serde(default)]
    locked: Option<Amount>,
    #[serde(default)]
    pending: Option<Amount>,
    total: Amount,
    #[serde(default)]
    sequence: Option<i64>,
}

/// The last error of a [`LiveBalances`] (see [`LiveBalances::last_error`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveBalancesError {
    /// `ACCOUNT_MISMATCH` (nothing was merged), or the failed request's error code.
    pub code: String,
    /// A description.
    pub message: String,
}

#[derive(Default)]
struct LbState {
    last_error: Option<LiveBalancesError>,
    stale: bool,
    rows: HashMap<String, Balance>,
    tombstones: HashMap<String, i64>,
    buffer: Vec<BalanceUpdate>,
    verified_user: Option<String>,
    fetching: bool,
    again: Option<String>,
    last_success: Option<Duration>,
    timer: Option<Box<dyn WsTimer>>,
    attempt: u32,
    closed: bool,
    generation: u64,
    warned_no_sequence: bool,
}

struct LbInner {
    ws: Weak<Inner>,
    snapshot: BalanceSnapshotFn,
    owner_id: OwnerIdFn,
    min_interval: Duration,
    retry: Duration,
    st: Mutex<LbState>,
}

/// Live balances of the authenticated account, fed by the `balances` channel and REST snapshots.
/// Create it with [`WebSocket::live_balances`] after `auth`. Cloning is cheap.
///
/// An event applies only if its `data.sequence` is greater than the stored one for that asset; a
/// total of 0 removes the row (a snapshot row at or below that sequence cannot bring it back). A
/// new snapshot is taken on a frame gap, `balances.resync`, `CONCURRENT_MODIFICATION`, a reconnect
/// and after an account change, never because `data.sequence` skipped values. At the start and after every account change
/// the snapshot source's owner is checked against the WebSocket user.
#[derive(Clone)]
pub struct LiveBalances {
    inner: Arc<LbInner>,
}

#[cfg(test)]
impl LiveBalances {
    pub(crate) fn buffered_events(&self) -> usize {
        self.inner.st.lock().unwrap().buffer.len()
    }
}

impl std::fmt::Debug for LiveBalances {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cexy::LiveBalances(stale: {})", self.is_stale())
    }
}

fn is_zero(a: &Amount) -> bool {
    rust_decimal_like_zero(a.as_str())
}

/// True when a decimal string is zero (`0`, `0.000`, `-0`).
fn rust_decimal_like_zero(s: &str) -> bool {
    let s = s.trim_start_matches(['+', '-']);
    !s.is_empty() && s.chars().all(|c| c == '0' || c == '.') && s.chars().any(|c| c == '0')
}

impl WebSocket {
    /// Live balances of the authenticated account (call `auth` first): subscribes `balances`,
    /// takes a REST snapshot, applies newer `balance.updated` events and refetches by itself when
    /// events may be missing. At the start and after every account change the snapshot source's owner must equal the
    /// WebSocket's authenticated user; otherwise nothing is merged
    /// ([`BalancesEvent::AccountMismatch`]).
    pub async fn live_balances(&self, options: LiveBalancesOptions) -> Result<LiveBalances> {
        let custom_snapshot = options.snapshot.is_some();
        let snapshot = match options.snapshot {
            Some(f) => f,
            None => {
                let Some(client) = self.inner.snapshots.clone() else {
                    return Err(WsError::local(
                        "CONFIG",
                        "live_balances needs options.snapshot or a WebSocket made by Client::websocket",
                    )
                    .into());
                };
                Arc::new(move || -> BoxFuture<'static, Result<Vec<Balance>>> {
                    let c = client.clone();
                    Box::pin(async move { c.account().balances().await })
                })
            }
        };
        let owner_id: OwnerIdFn = match (
            options.owner_id,
            options.account_id,
            self.inner.snapshots.clone(),
        ) {
            (Some(f), _, _) => f,
            (None, Some(id), _) => Arc::new(move || -> BoxFuture<'static, Result<String>> {
                let id = id.clone();
                Box::pin(async move { Ok(id) })
            }),
            // The REST key's account owns only the REST key's own snapshots: a custom snapshot
            // source must name its owner.
            (None, None, Some(client)) if !custom_snapshot => {
                Arc::new(move || -> BoxFuture<'static, Result<String>> {
                    let c = client.clone();
                    Box::pin(async move { c.account().id().await })
                })
            }
            (None, None, _) => {
                return Err(WsError::local(
                    "CONFIG",
                    "live_balances needs options.owner_id or options.account_id to check the snapshot's account",
                )
                .into());
            }
        };
        let lb = LiveBalances {
            inner: Arc::new(LbInner {
                ws: Arc::downgrade(&self.inner),
                snapshot,
                owner_id,
                min_interval: options
                    .min_snapshot_interval
                    .unwrap_or(Duration::from_secs(2)),
                retry: options.retry_delay.unwrap_or(Duration::from_secs(1)),
                st: Mutex::new(LbState {
                    stale: true,
                    ..LbState::default()
                }),
            }),
        };
        let held = {
            let mut s = self.inner.st.lock().unwrap();
            let held = s.holds("balances");
            s.live_balances.push(lb.clone());
            if !held {
                s.balances_by_us = true;
            }
            held
        };
        match self.subscribe(&["balances"]).await {
            Ok(res) if res.refused.is_empty() => {}
            Ok(_) => {
                lb.close();
                return Err(WsError::local(
                    "LOCAL_SUBSCRIPTION_LIMIT",
                    "cannot subscribe to balances: cap reached",
                )
                .into());
            }
            Err(e) => {
                lb.close();
                return Err(e);
            }
        }
        if held {
            lb.trigger("start"); // no subscribed reply comes for a held channel
        }
        Ok(lb)
    }
}

impl LiveBalances {
    /// True until the first snapshot, and from every refetch trigger until the next snapshot is
    /// applied.
    pub fn is_stale(&self) -> bool {
        self.inner.st.lock().unwrap().stale
    }

    /// The last error, also delivered as [`BalancesEvent::AccountMismatch`] or
    /// [`BalancesEvent::Error`]. `code == "ACCOUNT_MISMATCH"` means nothing was merged. The next
    /// snapshot clears it.
    pub fn last_error(&self) -> Option<LiveBalancesError> {
        self.inner.st.lock().unwrap().last_error.clone()
    }

    /// One asset's balance.
    pub fn get(&self, asset: &str) -> Option<Balance> {
        self.inner.st.lock().unwrap().rows.get(asset).cloned()
    }

    /// Every non-zero balance.
    pub fn all(&self) -> Vec<Balance> {
        self.inner
            .st
            .lock()
            .unwrap()
            .rows
            .values()
            .cloned()
            .collect()
    }

    /// Stops following (unsubscribes `balances` unless something else on this socket needs it).
    pub fn close(&self) {
        {
            let mut st = self.inner.st.lock().unwrap();
            if st.closed {
                return;
            }
            st.closed = true;
            if let Some(t) = st.timer.take() {
                t.cancel();
            }
        }
        let Some(ws) = self.inner.ws.upgrade() else {
            return;
        };
        let unsub = {
            let mut s = ws.st.lock().unwrap();
            s.live_balances
                .retain(|x| !Arc::ptr_eq(&x.inner, &self.inner));
            let unsub = s.live_balances.is_empty() && s.balances_by_us;
            if unsub {
                s.balances_by_us = false;
            }
            unsub
        };
        if unsub && let Ok(rt) = tokio::runtime::Handle::try_current() {
            rt.spawn(async move {
                let _ = Inner::unsubscribe(&ws, vec!["balances".to_string()]).await;
            });
        }
    }

    fn emit(&self, ev: BalancesEvent) {
        if let Some(ws) = self.inner.ws.upgrade() {
            ws.emit(WsEvent::Balances(ev));
        }
    }

    pub(crate) fn mark_stale(&self) {
        let mut st = self.inner.st.lock().unwrap();
        Self::mark_stale_locked(&mut st);
    }

    fn mark_stale_locked(st: &mut LbState) {
        st.stale = true;
        st.generation += 1;
        st.buffer.clear();
    }

    pub(crate) fn on_auth_changed(&self, reason: AuthChangeReason) {
        let mut st = self.inner.st.lock().unwrap();
        st.verified_user = None;
        Self::mark_stale_locked(&mut st);
        if reason == AuthChangeReason::UserChanged {
            // Another account's balances must never show: forget everything until the owner check.
            st.rows.clear();
            st.tombstones.clear();
        }
    }

    pub(crate) fn on_event(&self, data: &Value) {
        let Ok(d) = serde_json::from_value::<BalanceUpdate>(data.clone()) else {
            return;
        };
        let mut st = self.inner.st.lock().unwrap();
        if st.closed {
            return;
        }
        if d.sequence.is_none() && !st.warned_no_sequence {
            st.warned_no_sequence = true;
            if let Some(ws) = self.inner.ws.upgrade() {
                ws.emit(WsEvent::Warning(
                    "balance.updated without data.sequence; live balances apply every event in arrival order".into(),
                ));
            }
        }
        // Buffered only while a snapshot is in flight (it is applied on top). Without a verified
        // owner and no fetch (mismatch, retry backoff, signed out), events are dropped: the next
        // snapshot is complete anyway.
        if st.fetching {
            st.buffer.push(d);
            return;
        }
        if st.verified_user.is_none() {
            return;
        }
        if let Some(ev) = Self::apply(&mut st, d, true) {
            drop(st);
            self.emit(ev);
        }
    }

    pub(crate) fn trigger(&self, reason: &str) {
        let Some(ws) = self.inner.ws.upgrade() else {
            return;
        };
        let ws_user = ws.st.lock().unwrap().auth_user_id.clone();
        let mut st = self.inner.st.lock().unwrap();
        self.trigger_locked(&mut st, &ws, ws_user, reason);
    }

    fn trigger_locked(
        &self,
        st: &mut LbState,
        ws: &Arc<Inner>,
        ws_user: Option<String>,
        reason: &str,
    ) {
        if st.closed {
            return;
        }
        st.stale = true;
        if st.fetching {
            if st.again.is_none() {
                st.again = Some(reason.to_string());
            }
            return;
        }
        if st.timer.is_some() {
            return;
        }
        if let Some(last) = st.last_success {
            let due = last + self.inner.min_interval;
            let now = ws.clock.now();
            if due > now {
                let me = self.clone();
                let reason = reason.to_string();
                st.timer = Some(ws.clock.call_later(
                    due - now,
                    Box::new(move || {
                        me.inner.st.lock().unwrap().timer = None;
                        me.trigger(&reason);
                    }),
                ));
                return;
            }
        }
        let Some(user) = ws_user else {
            return; // signed out: the re-subscribe after the next auth triggers again
        };
        st.fetching = true;
        st.buffer.clear();
        st.generation += 1;
        let generation_at_start = st.generation;
        let check_owner = st.verified_user.as_deref() != Some(user.as_str());
        let me = self.clone();
        let reason = reason.to_string();
        tokio::spawn(async move {
            me.fetch(reason, user, generation_at_start, check_owner)
                .await
        });
    }

    async fn fetch(
        &self,
        reason: String,
        ws_user: String,
        generation_at_start: u64,
        check_owner: bool,
    ) {
        if check_owner {
            match (self.inner.owner_id)().await {
                Err(e) => return self.fail(e),
                Ok(owner) => {
                    let mut st = self.inner.st.lock().unwrap();
                    if self.overtaken(&mut st, generation_at_start) {
                        return;
                    }
                    if owner != ws_user {
                        st.last_error = Some(LiveBalancesError {
                            code: "ACCOUNT_MISMATCH".into(),
                            message: format!(
                                "the snapshot source belongs to {owner}, the WebSocket to {ws_user}; not merging"
                            ),
                        });
                        st.rows.clear();
                        st.tombstones.clear();
                        st.fetching = false;
                        st.again = None;
                        drop(st);
                        self.emit(BalancesEvent::AccountMismatch {
                            websocket_user_id: ws_user,
                            snapshot_user_id: owner,
                        });
                        return;
                    }
                    st.verified_user = Some(ws_user.clone());
                }
            }
        }
        let rows = match (self.inner.snapshot)().await {
            Err(e) => return self.fail(e),
            Ok(r) => r,
        };
        let Some(ws) = self.inner.ws.upgrade() else {
            return;
        };
        let mut st = self.inner.st.lock().unwrap();
        if self.overtaken(&mut st, generation_at_start) {
            return;
        }
        Self::apply_snapshot(&mut st, rows);
        st.attempt = 0;
        st.last_error = None;
        st.last_success = Some(ws.clock.now());
        st.stale = false;
        st.fetching = false;
        let again = st.again.take();
        drop(st);
        self.emit(BalancesEvent::Snapshot { reason });
        if let Some(a) = again {
            self.trigger(&a);
        }
    }

    /// A fetch overtaken by a stale mark (account change, disconnect) or a close.
    fn overtaken(&self, st: &mut LbState, generation_at_start: u64) -> bool {
        if st.closed || st.generation != generation_at_start {
            st.fetching = false;
            if let Some(a) = st.again.take()
                && !st.closed
            {
                let me = self.clone();
                tokio::spawn(async move { me.trigger(&a) });
            }
            return true;
        }
        false
    }

    fn fail(&self, e: Error) {
        let Some(ws) = self.inner.ws.upgrade() else {
            return;
        };
        {
            let mut st = self.inner.st.lock().unwrap();
            st.fetching = false;
            if st.closed {
                return;
            }
            let code = match &e {
                Error::Api(a) => a.code.as_str().to_string(),
                Error::WebSocket(w) => w.code.clone(),
                _ => "ERROR".to_string(),
            };
            st.last_error = Some(LiveBalancesError {
                code,
                message: e.to_string(),
            });
            let delay = (self.inner.retry * 2u32.saturating_pow(st.attempt.min(5)))
                .min(Duration::from_secs(30));
            st.attempt += 1;
            if let Some(t) = st.timer.take() {
                t.cancel();
            }
            let me = self.clone();
            st.timer = Some(ws.clock.call_later(
                delay,
                Box::new(move || {
                    me.inner.st.lock().unwrap().timer = None;
                    me.trigger("retry");
                }),
            ));
        }
        self.emit(BalancesEvent::Error(e));
    }

    fn apply_snapshot(st: &mut LbState, rows: Vec<Balance>) {
        let mut fresh = HashMap::new();
        for r in rows {
            if let Some(&tomb) = st.tombstones.get(&r.asset) {
                if r.sequence <= tomb {
                    continue;
                }
                st.tombstones.remove(&r.asset);
            }
            if is_zero(&r.total) {
                continue;
            }
            fresh.insert(r.asset.clone(), r);
        }
        st.rows = fresh;
        for d in std::mem::take(&mut st.buffer) {
            let _ = Self::apply(st, d, false);
        }
    }

    fn apply(st: &mut LbState, d: BalanceUpdate, emit: bool) -> Option<BalancesEvent> {
        let prev = st.rows.get(&d.asset).cloned();
        let current = prev
            .as_ref()
            .map(|p| p.sequence)
            .or_else(|| st.tombstones.get(&d.asset).copied())
            .unwrap_or(-1);
        // A server that predates live balances sends no data.sequence: such an event always
        // applies and keeps the stored sequence (a later sequenced snapshot or event takes over).
        let seq = match d.sequence {
            Some(n) if n <= current => return None, // duplicate or older
            Some(n) => n,
            None => current.max(0),
        };
        if is_zero(&d.total) {
            st.rows.remove(&d.asset);
            if d.sequence.is_some() {
                st.tombstones.insert(d.asset.clone(), seq);
            }
            return emit.then_some(BalancesEvent::Updated {
                asset: d.asset,
                balance: None,
            });
        }
        let zero = || Amount::from_str("0").expect("zero");
        let mut row = prev.unwrap_or_else(|| Balance {
            asset: d.asset.clone(),
            available: zero(),
            held_incoming: vec![],
            locked: zero(),
            pending: zero(),
            sequence: 0,
            total: zero(),
        });
        if let Some(a) = d.available {
            row.available = a;
        }
        if let Some(a) = d.locked {
            row.locked = a;
        }
        if let Some(a) = d.pending {
            row.pending = a;
        }
        row.total = d.total;
        row.sequence = seq;
        st.rows.insert(d.asset.clone(), row.clone());
        st.tombstones.remove(&d.asset);
        emit.then_some(BalancesEvent::Updated {
            asset: d.asset,
            balance: Some(row),
        })
    }
}
