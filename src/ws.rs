//! The WebSocket client: heartbeat, liveness, subscriptions with local limits, automatic
//! reconnect with re-auth and re-subscribe, and live order books.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use url::Url;

use crate::amount::Amount;
use crate::client::{Client, check_secure_url};
use crate::error::{Error, Result, WsError};
use crate::live_balances::LiveBalances;
use crate::models_gen::ErrorCode;
use crate::orderbook::{BookSnapshot, LiveOrderBook};

/// The production WebSocket endpoint.
pub const DEFAULT_WEBSOCKET_URL: &str = "wss://api.cexy.io/api/v1/ws";

/// The WebSocket protocol version this SDK was written for.
pub const SUPPORTED_PROTOCOL_VERSION: i64 = 1;

/// Channels that need [`WebSocket::auth`] with a session access token.
pub const PRIVATE_CHANNELS: [&str; 5] =
    ["orders", "balances", "deposits", "withdrawals", "account"];

const MAX_CHANNEL_LENGTH: usize = 64;
const MAX_FRAME_SIZE: usize = 4 << 20;

const KNOWN_EVENT_TYPES: [&str; 17] = [
    "ticker.update",
    "orderbook.update",
    "trade.new",
    "market.status",
    "order.created",
    "order.updated",
    "order.cancelled",
    "order.filled",
    "balance.updated",
    "deposit.detected",
    "deposit.updated",
    "deposit.completed",
    "withdrawal.updated",
    "session.revoked",
    "balances.resync",
    "deposits.resync",
    "withdrawals.resync",
];

/// The server's first frame on every connection.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Welcome {
    /// Protocol version of the server.
    #[serde(default)]
    pub protocol_version: i64,
    /// The server's own pong cadence (30), not a client deadline.
    #[serde(default)]
    pub heartbeat_interval_seconds: i64,
    /// The server's subscription cap.
    #[serde(default)]
    pub max_subscriptions: i64,
    /// Connection id, for support requests.
    #[serde(default)]
    pub connection_id: String,
}

/// A channel event such as `ticker.update`, `orderbook.update` or `order.filled`. Decode `data`
/// with [`WsFrame::decode`]. Unknown event types are ignored (they may be added without notice).
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct WsFrame {
    /// Event type.
    #[serde(rename = "type")]
    pub r#type: String,
    /// Channel, such as `ticker:BTC/USDT`.
    #[serde(default)]
    pub channel: String,
    /// Per channel, +1 per update. Resets per connection and when the server restarts.
    #[serde(default)]
    pub sequence: Option<i64>,
    /// Server time of the event.
    #[serde(default)]
    pub timestamp: Option<String>,
    /// The payload.
    #[serde(default)]
    pub data: Value,
}

impl WsFrame {
    /// Decodes `data`, for example into [`OrderBookUpdate`] or [`crate::Order`].
    pub fn decode<T: serde::de::DeserializeOwned>(&self) -> Result<T> {
        serde_json::from_value(self.data.clone())
            .map_err(|e| Error::Decode(format!("{}: {e}", self.r#type)))
    }
}

/// The data of `orderbook.update`: always the complete top 50 levels of both sides (`full` is
/// always true). It replaces the previous book; there are no deltas.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct OrderBookUpdate {
    /// Market.
    #[serde(default)]
    pub symbol: String,
    /// Always true.
    #[serde(default)]
    pub full: bool,
    /// Bids, best first.
    #[serde(default)]
    pub bids: Vec<Vec<Amount>>,
    /// Asks, best first.
    #[serde(default)]
    pub asks: Vec<Vec<Amount>>,
}

/// The data of `session.revoked` on the account channel.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct SessionRevoked {
    /// The revoked session, if one.
    #[serde(default)]
    pub session_id: Option<String>,
    /// Why.
    #[serde(default)]
    pub reason: String,
    /// Whether it is this connection's session.
    #[serde(default)]
    pub current: bool,
}

/// A closed connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseInfo {
    /// WebSocket close code (4000 for a liveness timeout).
    pub code: u16,
    /// Close reason.
    pub reason: String,
    /// Whether the client reconnects.
    pub will_reconnect: bool,
}

/// What [`WebSocket::subscribe`] did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SubscribeResult {
    /// Channels the server confirmed as newly added.
    pub added: Vec<String>,
    /// Channels refused locally because the subscription cap was reached.
    pub refused: Vec<String>,
    /// Channels already held (nothing was sent for them).
    pub already_subscribed: Vec<String>,
}

/// What [`WebSocket::auth`] did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuthResult {
    /// From the authenticated acknowledgement; `None` when queued.
    pub user_id: Option<String>,
    /// True when not connected: the token is kept and sent (and acknowledged) on connect.
    pub queued: bool,
}

/// Why state may have been missed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ResyncReason {
    /// The server dropped messages (`CONCURRENT_MODIFICATION` without a request id).
    ConcurrentModification,
    /// The connection was re-established.
    Reconnect,
    /// Private channels were re-subscribed after the server signed the connection out or
    /// switched it to another account: refetch private state through REST.
    Reauth,
    /// A private channel skipped sequence numbers (see [`WsEvent::SequenceGap`]).
    SequenceGap,
    /// `balances.resync`: the server could not resume its balance change stream.
    BalancesResync,
    /// `deposits.resync` (planned server frame): refetch the deposit list.
    DepositsResync,
    /// `withdrawals.resync` (planned server frame): refetch the withdrawal list.
    WithdrawalsResync,
}

/// Carried by [`WsEvent::SequenceGap`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequenceGap {
    /// The private channel.
    pub channel: String,
    /// The first missing sequence number.
    pub expected: i64,
    /// The number received instead.
    pub received: i64,
}

/// A stoppable timer from a [`WsClock`].
pub trait WsTimer: Send {
    /// Stops the timer if it has not fired.
    fn cancel(&self);
}

/// TEST-ONLY time source for the reorder-window timer and [`LiveBalances`] scheduling (minimum
/// snapshot interval, retry backoff). Socket timeouts always use the real clock. Leave
/// `WsOptions::clock` unset in production.
pub trait WsClock: Send + Sync + std::fmt::Debug {
    /// Time since an arbitrary fixed origin.
    fn now(&self) -> Duration;
    /// Runs `f` once after `delay`.
    fn call_later(&self, delay: Duration, f: Box<dyn FnOnce() + Send>) -> Box<dyn WsTimer>;
}

#[derive(Debug)]
struct RealClock(Instant);

struct TaskTimer(tokio::task::AbortHandle);

impl WsTimer for TaskTimer {
    fn cancel(&self) {
        self.0.abort();
    }
}

impl WsClock for RealClock {
    fn now(&self) -> Duration {
        self.0.elapsed()
    }
    fn call_later(&self, delay: Duration, f: Box<dyn FnOnce() + Send>) -> Box<dyn WsTimer> {
        let h = tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            f();
        });
        Box::new(TaskTimer(h.abort_handle()))
    }
}

#[derive(Default)]
struct SeqState {
    next: i64,
    holes: std::collections::BTreeSet<i64>,
    first: Option<SequenceGap>,
    timer: Option<Box<dyn WsTimer>>,
    id: u64,
}

/// Why the server ended the connection's private subscriptions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AuthChangeReason {
    /// `auth` succeeded as a different user.
    UserChanged,
    /// An `auth` failed; the server signs the connection out on any auth error.
    AuthFailed,
    /// This connection's own session was revoked (`session.revoked` with `current: true`, or
    /// the `signed_out` frame with reason `revoked`).
    SessionRevoked,
    /// the `signed_out` frame with reason `expired`. Re-send `auth` on every token refresh to
    /// avoid it.
    TokenExpired,
    /// A server sign-out with a reason this SDK does not know (raw value in `code`).
    SignedOut,
}

/// Carried by [`WsEvent::AuthChanged`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthChange {
    /// Why.
    pub reason: AuthChangeReason,
    /// The user of the last successful auth on this connection, if any.
    pub previous_user_id: Option<String>,
    /// The new user ([`AuthChangeReason::UserChanged`]), otherwise `None`: signed out.
    pub user_id: Option<String>,
    /// The server's error code ([`AuthChangeReason::AuthFailed`]), or the raw `signed_out` reason
    /// ([`AuthChangeReason::SignedOut`]).
    pub code: Option<String>,
    /// Private channels the server dropped. They are re-subscribed automatically: at once for
    /// `UserChanged`, after the next successful `auth` otherwise (then
    /// [`ResyncReason::Reauth`]).
    pub dropped: Vec<String>,
}

/// A live order book changed state.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum BookEvent {
    /// A snapshot or an update was applied.
    Updated(BookSnapshot),
    /// A sequence gap: the book is stale until the next update replaces it.
    Stale {
        /// Market.
        symbol: String,
        /// Expected sequence.
        expected: i64,
        /// Received sequence.
        received: i64,
    },
    /// The update after a gap arrived in order; the book is current again.
    Healed {
        /// Market.
        symbol: String,
    },
    /// A fresh REST snapshot is being taken (reconnect, `CONCURRENT_MODIFICATION`).
    Resync {
        /// Market.
        symbol: String,
    },
    /// A snapshot failed; it is retried with backoff.
    SnapshotFailed {
        /// Market.
        symbol: String,
        /// What failed.
        message: String,
    },
}

/// Everything the WebSocket reports, in order. Read them from [`WebSocket::events`].
#[derive(Debug)]
#[non_exhaustive]
pub enum WsEvent {
    /// A welcome frame (every connection).
    Welcome(Welcome),
    /// A known channel event.
    Event(WsFrame),
    /// Channels the server confirmed.
    Subscribed(Vec<String>),
    /// Channels the server removed.
    Unsubscribed(Vec<String>),
    /// The authenticated acknowledgement (after `auth` or the re-auth on reconnect).
    Authenticated {
        /// The user id.
        user_id: Option<String>,
    },
    /// A pong; `id` is `None` for the server's own pongs.
    Pong {
        /// The id of the ping it answers.
        id: Option<String>,
    },
    /// An error frame from the server.
    ServerError(WsError),
    /// A transport problem, or a failed automatic re-subscription or re-authentication.
    Error(Error),
    /// Something to know that is not an error (a newer protocol version, the subscription cap).
    Warning(String),
    /// The connection closed.
    Close(CloseInfo),
    /// A reconnect attempt is scheduled.
    Reconnecting {
        /// 1 for the first attempt.
        attempt: u32,
        /// Delay before it.
        delay: Duration,
    },
    /// Reconnected (re-authentication and re-subscription follow automatically).
    Reconnected(Welcome),
    /// State may have been missed: refetch anything you keep from private or public channels.
    Resync(ResyncReason),
    /// `session.revoked` arrived for this connection's own session (`current: true`): private
    /// channels are dead. The socket stays open and public channels keep working. Call `auth`
    /// with a new token to restore private channels.
    AuthLost(WsFrame),
    /// The server ended this connection's private subscriptions: `auth` succeeded as another
    /// user, an `auth` failed, this connection's own session was revoked, or the server signed it
    /// out (the `signed_out` frame).
    AuthChanged(AuthChange),
    /// A private channel skipped sequence numbers on this connection (after the reorder window):
    /// events were lost. Followed by `Resync(ResyncReason::SequenceGap)`.
    SequenceGap(SequenceGap),
    /// A [`LiveBalances`] changed state.
    Balances(crate::live_balances::BalancesEvent),
    /// A live order book changed state.
    Book(BookEvent),
}

/// The receiving end of the event channel.
pub type WsEvents = mpsc::Receiver<WsEvent>;

/// Configures a [`WebSocket`].
#[derive(Debug, Clone)]
pub struct WsOptions {
    /// Default [`DEFAULT_WEBSOCKET_URL`]. Must be `wss://` (see `allow_insecure`).
    pub url: Option<String>,
    /// Allow `ws://`, but ONLY for localhost, 127.0.0.1 or ::1 (local test servers).
    pub allow_insecure: bool,
    /// Client ping cadence, required by the server. Default 30 s.
    pub ping_interval: Duration,
    /// Reconnect when no frame arrives for this long. Default 75 s.
    pub liveness_timeout: Duration,
    /// How long to wait for the welcome frame. Default 10 s.
    pub welcome_timeout: Duration,
    /// How long `subscribe`, `unsubscribe`, `auth` and `ping` wait for the acknowledgement.
    /// Default 5 s.
    pub ack_timeout: Duration,
    /// Reconnect automatically after a drop. Default true.
    pub reconnect: bool,
    /// Reconnect backoff: full jitter from this (default 1 s), doubling up to
    /// `reconnect_max_delay` (default 30 s).
    pub reconnect_base_delay: Duration,
    /// See `reconnect_base_delay`.
    pub reconnect_max_delay: Duration,
    /// 0 means unlimited.
    pub max_reconnect_attempts: u32,
    /// Local subscription cap. Default 100 (the server's limit).
    pub max_subscriptions: usize,
    /// Local cap on client messages per fixed minute. Default 200 (the server closes above 240).
    pub max_messages_per_minute: u32,
    /// Sent as the User-Agent of the handshake. `Client::websocket` sets the SDK's.
    pub user_agent: Option<String>,
    /// Capacity of the event channel. When it is full (events not read), further events are
    /// dropped and counted in [`WebSocket::dropped_events`]; live order books still update.
    /// Default 10 000.
    pub event_buffer: usize,
    /// Private channels with several publishers (orders, account) can deliver two adjacent frames
    /// swapped: a missing sequence number gets this long to arrive before it counts as a gap.
    /// Default 250 ms.
    pub reorder_window: Duration,
    /// TEST-ONLY: see [`WsClock`].
    pub clock: Option<Arc<dyn WsClock>>,
}

impl Default for WsOptions {
    fn default() -> Self {
        WsOptions {
            url: None,
            allow_insecure: false,
            ping_interval: Duration::from_secs(30),
            liveness_timeout: Duration::from_secs(75),
            welcome_timeout: Duration::from_secs(10),
            ack_timeout: Duration::from_secs(5),
            reconnect: true,
            reconnect_base_delay: Duration::from_secs(1),
            reconnect_max_delay: Duration::from_secs(30),
            max_reconnect_attempts: 0,
            max_subscriptions: 100,
            max_messages_per_minute: 200,
            user_agent: None,
            event_buffer: 10_000,
            reorder_window: Duration::from_millis(250),
            clock: None,
        }
    }
}

struct Pending {
    kind: &'static str,
    channels: Vec<String>,
    tx: oneshot::Sender<std::result::Result<Value, WsError>>,
}

#[derive(Default)]
pub(crate) struct State {
    out: Option<mpsc::UnboundedSender<Message>>,
    stop: Option<oneshot::Sender<()>>,
    conn_gen: u64,
    welcome: Option<Welcome>,
    channels: Vec<String>,
    pub(crate) token: Option<String>,
    /// The user of the last successful auth on the current connection.
    pub(crate) auth_user_id: Option<String>,
    /// Private channels dropped by a server sign-out, re-subscribed after the next successful auth.
    pending_private: Vec<String>,
    pending: HashMap<String, Pending>,
    next_id: u64,
    closed_by_user: bool,
    ever_connected: bool,
    reconnecting: bool,
    window_start: Option<Instant>,
    window_count: u32,
    warned_version: bool,
    books: HashMap<String, LiveOrderBook>,
    seq: HashMap<String, SeqState>,
    seq_ids: u64,
    pub(crate) live_balances: Vec<LiveBalances>,
    pub(crate) balances_by_us: bool,
}

impl State {
    /// Whether `c` is held, or pending re-subscription after a sign-out.
    pub(crate) fn holds(&self, c: &str) -> bool {
        self.channels.iter().any(|x| x == c) || self.pending_private.iter().any(|x| x == c)
    }
}

pub(crate) struct Inner {
    url: String,
    opts: WsOptions,
    pub(crate) snapshots: Option<Client>,
    pub(crate) st: Mutex<State>,
    events: mpsc::Sender<WsEvent>,
    events_rx: Mutex<Option<WsEvents>>,
    dropped: AtomicU64,
    connect_lock: tokio::sync::Mutex<()>,
    closing: Notify,
    pub(crate) clock: Arc<dyn WsClock>,
}

/// The CEXY.io WebSocket client. Cloning is cheap; clones share the connection.
///
/// API-key authentication on the WebSocket is not available yet: [`WebSocket::auth`] takes a
/// session access token. Programs holding only an API key get public channels and poll REST for
/// private state.
#[derive(Clone)]
pub struct WebSocket {
    pub(crate) inner: Arc<Inner>,
}

impl std::fmt::Debug for WebSocket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cexy::WebSocket({})", self.inner.url)
    }
}

impl Client {
    /// A WebSocket client for the same deployment, wired to this client for order-book
    /// snapshots. An unset `options.url` is derived from the base URL.
    pub fn websocket(&self, mut options: WsOptions) -> Result<WebSocket> {
        if options.url.is_none() {
            let base = &self.t.base_url;
            let ws = if let Some(rest) = base.strip_prefix("https") {
                format!("wss{rest}")
            } else {
                format!("ws{}", base.strip_prefix("http").unwrap_or(base))
            };
            options.url = Some(format!("{ws}/api/v1/ws"));
        }
        options.allow_insecure |= self.t.allow_insecure;
        if options.user_agent.is_none() {
            options.user_agent = Some(self.t.user_agent.clone());
        }
        WebSocket::build(options, Some(self.clone()))
    }
}

impl WebSocket {
    /// Checks the options. Call [`WebSocket::connect`] to open the connection.
    pub fn new(options: WsOptions) -> Result<WebSocket> {
        WebSocket::build(options, None)
    }

    fn build(mut opts: WsOptions, snapshots: Option<Client>) -> Result<WebSocket> {
        let url = opts
            .url
            .clone()
            .unwrap_or_else(|| DEFAULT_WEBSOCKET_URL.to_string());
        let u = Url::parse(&url)
            .map_err(|_| Error::config(format!("invalid WebSocket URL: {url:?}")))?;
        if u.host_str().is_none_or(str::is_empty) {
            return Err(Error::config(format!("invalid WebSocket URL: {url:?}")));
        }
        check_secure_url(&u, "wss", "ws", opts.allow_insecure, "WebSocket URL")?;
        // Credentials never go in a URL (userinfo), and the endpoint takes no query string:
        // either would end up in logs and proxies.
        if !u.username().is_empty() || u.password().is_some() {
            return Err(Error::config(
                "WebSocket URL must not contain credentials (user:password@)",
            ));
        }
        if u.query().is_some() || u.fragment().is_some() {
            return Err(Error::config(
                "WebSocket URL must not contain a query string or fragment",
            ));
        }
        if opts
            .user_agent
            .as_deref()
            .is_some_and(|s| s.contains(['\r', '\n']))
        {
            return Err(Error::config("user_agent must not contain line breaks"));
        }
        let d = WsOptions::default();
        for (v, dv) in [
            (&mut opts.ping_interval, d.ping_interval),
            (&mut opts.liveness_timeout, d.liveness_timeout),
            (&mut opts.welcome_timeout, d.welcome_timeout),
            (&mut opts.ack_timeout, d.ack_timeout),
            (&mut opts.reconnect_base_delay, d.reconnect_base_delay),
            (&mut opts.reconnect_max_delay, d.reconnect_max_delay),
        ] {
            if v.is_zero() {
                *v = dv;
            }
        }
        if opts.max_subscriptions == 0 {
            opts.max_subscriptions = d.max_subscriptions;
        }
        if opts.max_messages_per_minute == 0 {
            opts.max_messages_per_minute = d.max_messages_per_minute;
        }
        let (tx, rx) = mpsc::channel(opts.event_buffer.max(1));
        let st = State {
            closed_by_user: true,
            next_id: 1,
            ..Default::default()
        };
        Ok(WebSocket {
            inner: Arc::new(Inner {
                url,
                clock: opts
                    .clock
                    .clone()
                    .unwrap_or_else(|| Arc::new(RealClock(Instant::now()))),
                opts,
                snapshots,
                st: Mutex::new(st),
                events: tx,
                events_rx: Mutex::new(Some(rx)),
                dropped: AtomicU64::new(0),
                connect_lock: tokio::sync::Mutex::new(()),
                closing: Notify::new(),
            }),
        })
    }

    /// The event receiver (once; later calls return `None`). Read it continuously: when its
    /// buffer is full, new events are dropped.
    pub fn events(&self) -> Option<WsEvents> {
        self.inner.events_rx.lock().unwrap().take()
    }

    /// Events dropped because the event channel was full.
    pub fn dropped_events(&self) -> u64 {
        self.inner.dropped.load(Ordering::Relaxed)
    }

    /// The WebSocket URL.
    pub fn url(&self) -> &str {
        &self.inner.url
    }

    /// The last welcome frame, or `None` while not connected.
    pub fn welcome(&self) -> Option<Welcome> {
        self.inner.st.lock().unwrap().welcome.clone()
    }

    /// Whether the connection is open and welcomed.
    pub fn is_connected(&self) -> bool {
        let s = self.inner.st.lock().unwrap();
        s.out.is_some() && s.welcome.is_some()
    }

    /// The channels currently held (restored after every reconnect).
    pub fn channels(&self) -> Vec<String> {
        self.inner.st.lock().unwrap().channels.clone()
    }

    /// Whether a session token is kept for automatic re-authentication. A refused token and a
    /// revoked session are forgotten.
    pub fn has_token(&self) -> bool {
        self.inner.st.lock().unwrap().token.is_some()
    }

    /// The user of the last successful `auth` on the current connection (`None`: signed out).
    pub fn user_id(&self) -> Option<String> {
        self.inner.st.lock().unwrap().auth_user_id.clone()
    }

    /// Opens the connection and returns the server's welcome frame. After it succeeds, dropped
    /// connections are reopened automatically until [`WebSocket::close`] (unless `reconnect` is
    /// false).
    pub async fn connect(&self) -> Result<Welcome> {
        let _g = self.inner.connect_lock.lock().await;
        {
            let mut s = self.inner.st.lock().unwrap();
            if s.out.is_some()
                && let Some(w) = &s.welcome
            {
                return Ok(w.clone());
            }
            s.closed_by_user = false;
        }
        Inner::open(&self.inner).await
    }

    /// Authenticates private channels with a session access token. It returns on the server's
    /// `authenticated` acknowledgement; it fails on an error frame with the request id (the
    /// token is then forgotten) or when no acknowledgement arrives within `ack_timeout`. The
    /// token is kept in memory and re-sent after each reconnect. When not connected, the token
    /// is queued (`queued` is true). API-key authentication is not available on the WebSocket
    /// yet.
    pub async fn auth(&self, token: &str) -> Result<AuthResult> {
        if token.is_empty() {
            return Err(WsError::local("CONFIG", "auth: token is required").into());
        }
        let connected = {
            let mut s = self.inner.st.lock().unwrap();
            s.token = Some(token.to_string());
            s.out.is_some() && s.welcome.is_some()
        };
        if !connected {
            return Ok(AuthResult {
                user_id: None,
                queued: true,
            });
        }
        Inner::auth(&self.inner, token).await
    }

    /// Sends a ping with an id and returns the round-trip time.
    pub async fn ping(&self) -> Result<Duration> {
        let start = Instant::now();
        Inner::request(&self.inner, "ping", Map::new(), vec![], true).await?;
        Ok(start.elapsed())
    }

    /// Subscribes to channels such as `"ticker:BTC/USDT"` and `"trades:BTC/USDT"`, and returns
    /// when the server confirms. Channels beyond `max_subscriptions` are refused locally. When
    /// not connected, channels are queued and subscribed on connect.
    pub async fn subscribe(&self, channels: &[&str]) -> Result<SubscribeResult> {
        let wanted = uniq(channels.iter().map(|c| c.to_string()));
        if let Some(bad) = wanted
            .iter()
            .find(|c| c.is_empty() || c.len() > MAX_CHANNEL_LENGTH)
        {
            return Err(WsError::local("CONFIG", format!("invalid channel name: {bad:?}")).into());
        }
        let mut res = SubscribeResult::default();
        let (accepted, connected) = {
            let mut s = self.inner.st.lock().unwrap();
            let mut fresh = vec![];
            for c in wanted {
                // Private channels waiting for the next successful auth count as held.
                if s.channels.contains(&c) || s.pending_private.contains(&c) {
                    res.already_subscribed.push(c);
                } else {
                    fresh.push(c);
                }
            }
            let room = self
                .inner
                .opts
                .max_subscriptions
                .saturating_sub(s.channels.len() + s.pending_private.len());
            let accepted: Vec<String> = fresh.iter().take(room).cloned().collect();
            res.refused = fresh[accepted.len()..].to_vec();
            s.channels.extend(accepted.iter().cloned());
            (accepted, s.out.is_some() && s.welcome.is_some())
        };
        if !res.refused.is_empty() {
            self.inner.emit(WsEvent::Warning(format!(
                "subscription cap of {} reached; refused {:?}",
                self.inner.opts.max_subscriptions, res.refused
            )));
        }
        if accepted.is_empty() || !connected {
            return Ok(res);
        }
        match Inner::send_subscribe(&self.inner, accepted.clone()).await {
            Ok(added) => res.added = added,
            Err(e) => {
                if let Error::WebSocket(w) = &e
                    && w.from_server
                {
                    // Refused by the server (e.g. UNAUTHENTICATED for a private channel): not held.
                    self.inner
                        .st
                        .lock()
                        .unwrap()
                        .channels
                        .retain(|c| !accepted.contains(c));
                }
                return Err(e);
            }
        }
        Ok(res)
    }

    /// Unsubscribes and returns on the `unsubscribed` acknowledgement (or after `ack_timeout`
    /// without one); it fails on an error frame with the request id.
    pub async fn unsubscribe(&self, channels: &[&str]) -> Result<()> {
        Inner::unsubscribe(
            &self.inner,
            channels.iter().map(|c| c.to_string()).collect(),
        )
        .await
    }

    /// A live order book for `symbol` that follows the sync rules: subscribe first, then take a
    /// REST snapshot (sequence S); drop updates with sequence <= S; each update replaces the
    /// top 50 levels; a gap marks the book stale until the next update; a fresh snapshot after
    /// every reconnect and after `CONCURRENT_MODIFICATION`. It returns after the first snapshot
    /// and needs a WebSocket made by [`Client::websocket`].
    pub async fn order_book(&self, symbol: &str) -> Result<LiveOrderBook> {
        if self.inner.snapshots.is_none() {
            return Err(WsError::local(
                "CONFIG",
                "order_book needs a WebSocket made by Client::websocket",
            )
            .into());
        }
        let book = {
            let mut s = self.inner.st.lock().unwrap();
            if let Some(b) = s.books.get(symbol) {
                return Ok(b.clone());
            }
            let b = LiveOrderBook::new(symbol, Arc::downgrade(&self.inner));
            s.books.insert(symbol.to_string(), b.clone());
            b
        };
        let channel = format!("orderbook:{symbol}");
        // Subscribe BEFORE the snapshot; updates are buffered until it arrives.
        let mut result = self.subscribe(&[&channel]).await;
        if let Ok(r) = &result
            && !r.refused.is_empty()
        {
            result = Err(WsError::local(
                "LOCAL_SUBSCRIPTION_LIMIT",
                format!("cannot subscribe to {channel}: cap reached"),
            )
            .into());
        }
        let result = match result {
            Ok(_) => book.initial_sync().await,
            Err(e) => Err(e),
        };
        if let Err(e) = result {
            self.inner.st.lock().unwrap().books.remove(symbol);
            book.mark_closed();
            let _ = self.unsubscribe(&[&channel]).await;
            return Err(e);
        }
        Ok(book)
    }

    /// Closes the connection for good (no reconnect).
    pub async fn close(&self) {
        Inner::close(&self.inner);
    }
}

impl Inner {
    pub(crate) fn emit(&self, ev: WsEvent) {
        if let Err(mpsc::error::TrySendError::Full(_)) = self.events.try_send(ev) {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn emit_error(&self, e: Error) {
        // Requests cut short by a disconnect are redone by the reconnect.
        if let Error::WebSocket(w) = &e
            && (w.code == "DISCONNECTED" || w.code == "CLOSED")
        {
            return;
        }
        self.emit(WsEvent::Error(e));
    }

    async fn open(this: &Arc<Inner>) -> Result<Welcome> {
        let mut req = this
            .url
            .as_str()
            .into_client_request()
            .map_err(|e| Error::config(format!("WebSocket URL: {e}")))?;
        if let Some(ua) = &this.opts.user_agent {
            let v = HeaderValue::from_str(ua).map_err(|_| Error::config("invalid User-Agent"))?;
            req.headers_mut().insert("user-agent", v);
        }
        let mut cfg = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default();
        cfg.max_message_size = Some(MAX_FRAME_SIZE);
        cfg.max_frame_size = Some(MAX_FRAME_SIZE);
        let welcome_timeout = this.opts.welcome_timeout;
        let handshake = async {
            // tungstenite does not follow redirects: a 3xx fails the handshake.
            let connector = tokio_tungstenite::Connector::Rustls(crate::tls::client_config());
            let (mut stream, _) = tokio_tungstenite::connect_async_tls_with_config(
                req,
                Some(cfg),
                false,
                Some(connector),
            )
            .await
            .map_err(|e| {
                WsError::local(
                    "CONNECT_FAILED",
                    format!("could not connect to {}: {e}", this.url),
                )
            })?;
            let mut early = vec![];
            loop {
                let msg = match stream.next().await {
                    Some(Ok(m)) => m,
                    Some(Err(e)) => {
                        return Err(WsError::local(
                            "CONNECT_FAILED",
                            format!("connection closed before welcome: {e}"),
                        ));
                    }
                    None => {
                        return Err(WsError::local(
                            "CONNECT_FAILED",
                            "connection closed before welcome",
                        ));
                    }
                };
                let Some(frame) = parse_frame(&msg) else {
                    continue;
                };
                if frame.get("type").and_then(Value::as_str) == Some("welcome") {
                    let welcome: Welcome =
                        serde_json::from_value(Value::Object(frame)).unwrap_or(Welcome {
                            protocol_version: 0,
                            heartbeat_interval_seconds: 0,
                            max_subscriptions: 0,
                            connection_id: String::new(),
                        });
                    return Ok((stream, welcome, early));
                }
                early.push(frame);
            }
        };
        let (stream, welcome, early) = match tokio::time::timeout(welcome_timeout, handshake).await
        {
            Ok(r) => r?,
            Err(_) => return Err(WsError::local("TIMEOUT", "no welcome frame from server").into()),
        };

        let (out_tx, out_rx) = mpsc::unbounded_channel();
        let (stop_tx, stop_rx) = oneshot::channel();
        let generation = {
            let mut s = this.st.lock().unwrap();
            if s.closed_by_user {
                None
            } else {
                s.conn_gen += 1;
                s.out = Some(out_tx);
                s.stop = Some(stop_tx);
                s.welcome = Some(welcome.clone());
                s.window_start = None;
                s.window_count = 0;
                Some(s.conn_gen)
            }
        };
        let Some(generation) = generation else {
            let (mut sink, _) = stream.split();
            let _ = sink.send(Message::Close(None)).await;
            return Err(WsError::local("CLOSED", "connection closed by client").into());
        };
        tokio::spawn(run_connection(
            Arc::downgrade(this),
            stream,
            out_rx,
            stop_rx,
            generation,
            this.opts.clone(),
        ));
        for f in early {
            Inner::on_frame(this, f);
        }
        Inner::on_welcome(this, welcome.clone());
        Ok(welcome)
    }

    fn on_welcome(this: &Arc<Inner>, welcome: Welcome) {
        let (is_reconnect, token, channels, books, warn) = {
            let mut s = this.st.lock().unwrap();
            let warn = welcome.protocol_version != SUPPORTED_PROTOCOL_VERSION && !s.warned_version;
            if warn {
                s.warned_version = true;
            }
            let r = s.ever_connected;
            s.ever_connected = true;
            s.auth_user_id = None; // a new connection starts signed out
            Inner::reset_seq(&mut s, None); // sequences on a new connection are unrelated
            (
                r,
                s.token.clone(),
                s.channels.clone(),
                s.books.values().cloned().collect::<Vec<_>>(),
                warn,
            )
        };
        if warn {
            this.emit(WsEvent::Warning(format!(
                "server WebSocket protocol_version {} differs from the {SUPPORTED_PROTOCOL_VERSION} this SDK supports; continuing",
                welcome.protocol_version
            )));
        }
        this.emit(WsEvent::Welcome(welcome.clone()));
        if token.is_some() || !channels.is_empty() {
            let inner = this.clone();
            tokio::spawn(async move {
                if let Some(t) = token
                    && let Err(e) = Inner::auth(&inner, &t).await
                {
                    inner.emit_error(e);
                }
                if !channels.is_empty()
                    && let Err(e) = Inner::send_subscribe(&inner, channels).await
                {
                    inner.emit_error(e);
                }
            });
        }
        if is_reconnect {
            this.emit(WsEvent::Reconnected(welcome));
            this.emit(WsEvent::Resync(ResyncReason::Reconnect));
            for b in books {
                b.spawn_resync(0);
            }
        }
    }

    async fn auth(this: &Arc<Inner>, token: &str) -> Result<AuthResult> {
        let mut payload = Map::new();
        payload.insert("token".into(), Value::String(token.to_string()));
        match Inner::request(this, "auth", payload, vec![], true).await {
            Ok(ack) => Ok(AuthResult {
                user_id: ack
                    .get("user_id")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                queued: false,
            }),
            Err(e) => {
                if let Error::WebSocket(w) = &e
                    && w.from_server
                {
                    let mut s = this.st.lock().unwrap();
                    if s.token.as_deref() == Some(token) {
                        s.token = None; // the server refused it: do not resend on reconnect
                    }
                }
                Err(e)
            }
        }
    }

    async fn send_subscribe(this: &Arc<Inner>, channels: Vec<String>) -> Result<Vec<String>> {
        let mut payload = Map::new();
        payload.insert("channels".into(), json!(channels));
        // `subscribed` is sent only when something was added: silence means nothing new.
        let ack = Inner::request(this, "subscribe", payload, channels, false).await?;
        Ok(string_list(ack.get("channels")))
    }

    pub(crate) async fn unsubscribe(this: &Arc<Inner>, channels: Vec<String>) -> Result<()> {
        let (held, connected) = {
            let mut s = this.st.lock().unwrap();
            let mut held = vec![];
            for c in uniq(channels) {
                s.pending_private.retain(|x| x != &c);
                Inner::reset_seq(&mut s, Some(&c));
                if let Some(i) = s.channels.iter().position(|x| x == &c) {
                    s.channels.remove(i);
                    held.push(c);
                }
            }
            (held, s.out.is_some() && s.welcome.is_some())
        };
        if held.is_empty() || !connected {
            return Ok(());
        }
        let mut payload = Map::new();
        payload.insert("channels".into(), json!(held));
        Inner::request(this, "unsubscribe", payload, held, false)
            .await
            .map(|_| ())
    }

    /// Sends `{op, id, ...payload}` and waits for the acknowledgement with the same id, or an
    /// error frame with that id. With `strict`, no acknowledgement within `ack_timeout` is an
    /// error; otherwise it returns an empty object.
    async fn request(
        this: &Arc<Inner>,
        kind: &'static str,
        mut payload: Map<String, Value>,
        channels: Vec<String>,
        strict: bool,
    ) -> Result<Map<String, Value>> {
        let (tx, rx) = oneshot::channel();
        let id = {
            let mut s = this.st.lock().unwrap();
            let id = s.next_id.to_string();
            s.next_id += 1;
            s.pending.insert(id.clone(), Pending { kind, channels, tx });
            id
        };
        payload.insert("op".into(), Value::String(kind.to_string()));
        payload.insert("id".into(), Value::String(id.clone()));
        if let Err(e) = this.send(Value::Object(payload)) {
            this.st.lock().unwrap().pending.remove(&id);
            return Err(e.into());
        }
        match tokio::time::timeout(this.opts.ack_timeout, rx).await {
            Ok(Ok(Ok(Value::Object(m)))) => Ok(m),
            Ok(Ok(Ok(_))) => Ok(Map::new()),
            Ok(Ok(Err(e))) => Err(e.into()),
            Ok(Err(_)) => Err(WsError::local("DISCONNECTED", "connection lost").into()),
            Err(_) => {
                this.st.lock().unwrap().pending.remove(&id);
                if strict {
                    let ack = match kind {
                        "auth" => "authenticated",
                        "subscribe" => "subscribed",
                        "unsubscribe" => "unsubscribed",
                        _ => "pong",
                    };
                    Err(WsError::local(
                        "TIMEOUT",
                        format!("no {ack} acknowledgement for {kind} (id {id})"),
                    )
                    .into())
                } else {
                    Ok(Map::new())
                }
            }
        }
    }

    fn send(&self, frame: Value) -> std::result::Result<(), WsError> {
        let is_ping = frame.get("op").and_then(Value::as_str) == Some("ping");
        let text = frame.to_string();
        let mut s = self.st.lock().unwrap();
        let Some(out) = s.out.clone() else {
            return Err(WsError::local(
                "NOT_CONNECTED",
                "WebSocket is not connected",
            ));
        };
        let now = Instant::now();
        if s.window_start
            .is_none_or(|w| now.duration_since(w) >= Duration::from_secs(60))
        {
            s.window_start = Some(now);
            s.window_count = 0;
        }
        // Pings are never refused locally: without them the server closes the connection.
        if !is_ping && s.window_count >= self.opts.max_messages_per_minute {
            return Err(WsError::local(
                "LOCAL_RATE_LIMIT",
                format!(
                    "more than {} messages this minute; the server closes the socket above 240",
                    self.opts.max_messages_per_minute
                ),
            ));
        }
        s.window_count += 1;
        drop(s);
        out.send(Message::text(text))
            .map_err(|_| WsError::local("NOT_CONNECTED", "WebSocket is not connected"))
    }

    fn settle(&self, id: &str, kind: Option<&str>, result: std::result::Result<Value, WsError>) {
        let p = {
            let mut s = self.st.lock().unwrap();
            match s.pending.get(id) {
                Some(p) if kind.is_none_or(|k| k == p.kind) => s.pending.remove(id),
                _ => None,
            }
        };
        if let Some(p) = p {
            let _ = p.tx.send(result);
        }
    }

    fn settle_by_channel(&self, kind: &str, channels: &[String], frame: Value) {
        let p = {
            let mut s = self.st.lock().unwrap();
            let id = s
                .pending
                .iter()
                .find(|(_, p)| p.kind == kind && p.channels.iter().any(|c| channels.contains(c)))
                .map(|(id, _)| id.clone());
            id.and_then(|id| s.pending.remove(&id))
        };
        if let Some(p) = p {
            let _ = p.tx.send(Ok(frame));
        }
    }

    fn on_frame(this: &Arc<Inner>, frame: Map<String, Value>) {
        let typ = frame
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let id = frame.get("id").and_then(Value::as_str).map(str::to_string);
        let value = Value::Object(frame.clone());
        match typ.as_str() {
            "welcome" => {
                if let Ok(w) = serde_json::from_value::<Welcome>(value) {
                    this.st.lock().unwrap().welcome = Some(w.clone());
                    Inner::on_welcome(this, w);
                }
            }
            "pong" => {
                // Replies to our pings echo the id; the server's own pongs every 30 s have none.
                if let Some(id) = &id {
                    this.settle(id, Some("ping"), Ok(value));
                }
                this.emit(WsEvent::Pong { id });
            }
            "authenticated" => {
                let user_id = frame
                    .get("user_id")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                this.emit(WsEvent::Authenticated {
                    user_id: user_id.clone(),
                });
                // Update the state before `auth` returns, and before any later frame.
                Inner::on_authenticated(this, user_id);
                if let Some(id) = &id {
                    this.settle(id, Some("auth"), Ok(value));
                }
            }
            "subscribed" | "unsubscribed" => {
                let channels = string_list(frame.get("channels"));
                let kind = if typ == "subscribed" {
                    "subscribe"
                } else {
                    "unsubscribe"
                };
                if typ == "subscribed" {
                    let helpers = {
                        let mut s = this.st.lock().unwrap();
                        for c in &channels {
                            Inner::reset_seq(&mut s, Some(c)); // the next frame is the new baseline
                        }
                        s.live_balances.clone()
                    };
                    if channels.iter().any(|c| c == "balances") {
                        for lb in helpers {
                            lb.trigger("resubscribed");
                        }
                    }
                }
                match &id {
                    Some(id) => this.settle(id, Some(kind), Ok(value)),
                    None => this.settle_by_channel(kind, &channels, value), // acks that carry no id
                }
                this.emit(if typ == "subscribed" {
                    WsEvent::Subscribed(channels)
                } else {
                    WsEvent::Unsubscribed(channels)
                });
            }
            "signed_out" => {
                // signed_out (a planned server frame): the server signed this connection out (token
                // expired, session revoked, or a future reason). Private subscriptions are gone; a
                // fresh auth on this socket restores them.
                let raw = frame
                    .get("reason")
                    .and_then(Value::as_str)
                    .filter(|r| !r.is_empty())
                    .unwrap_or("unknown")
                    .to_string();
                this.st.lock().unwrap().token = None;
                match raw.as_str() {
                    "revoked" => {
                        this.signed_out(AuthChangeReason::SessionRevoked, None);
                        this.emit(WsEvent::AuthLost(WsFrame {
                            r#type: "session.revoked".into(),
                            channel: "account".into(),
                            sequence: None,
                            timestamp: None,
                            data: json!({"session_id": null, "reason": "signed_out", "current": true}),
                        }));
                    }
                    "expired" => this.signed_out(AuthChangeReason::TokenExpired, None),
                    _ => this.signed_out(AuthChangeReason::SignedOut, Some(raw)),
                }
            }
            "error" => {
                let code = frame
                    .get("code")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .unwrap_or("UNKNOWN");
                let message = frame
                    .get("message")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .unwrap_or(code);
                let err = WsError {
                    code: code.to_string(),
                    message: message.to_string(),
                    from_server: true,
                };
                if let Some(id) = &id {
                    // Any error on an auth frame signs the connection out (an UNAUTHENTICATED
                    // error on a subscribe is only a refused subscribe).
                    let is_auth = this
                        .st
                        .lock()
                        .unwrap()
                        .pending
                        .get(id)
                        .is_some_and(|p| p.kind == "auth");
                    if is_auth {
                        this.signed_out(AuthChangeReason::AuthFailed, Some(code.to_string()));
                    }
                    this.settle(id, None, Err(err.clone()));
                }
                this.emit(WsEvent::ServerError(err));
                if code == ErrorCode::ConcurrentModification.as_str() && id.is_none() {
                    // The server dropped messages: resynchronise every book and channel.
                    this.emit(WsEvent::Resync(ResyncReason::ConcurrentModification));
                    let (books, helpers) = {
                        let s = this.st.lock().unwrap();
                        (
                            s.books.values().cloned().collect::<Vec<_>>(),
                            s.live_balances.clone(),
                        )
                    };
                    for b in books {
                        b.spawn_resync(0);
                    }
                    for lb in helpers {
                        lb.trigger("concurrent_modification");
                    }
                }
            }
            t if KNOWN_EVENT_TYPES.contains(&t) => {
                let Ok(ev) = serde_json::from_value::<WsFrame>(value) else {
                    return;
                };
                if PRIVATE_CHANNELS.contains(&ev.channel.as_str())
                    && let Some(n) = ev.sequence
                {
                    Inner::track_seq(this, &ev.channel, n);
                }
                let resync = match ev.r#type.as_str() {
                    "balances.resync" => Some(ResyncReason::BalancesResync),
                    "deposits.resync" => Some(ResyncReason::DepositsResync),
                    "withdrawals.resync" => Some(ResyncReason::WithdrawalsResync),
                    _ => None,
                };
                if let Some(reason) = resync {
                    this.emit(WsEvent::Event(ev));
                    this.emit(WsEvent::Resync(reason));
                    if reason == ResyncReason::BalancesResync {
                        for lb in this.helpers() {
                            lb.trigger("balances_resync");
                        }
                    }
                    return;
                }
                if ev.r#type == "balance.updated" {
                    for lb in this.helpers() {
                        lb.on_event(&ev.data);
                    }
                }
                // Only this connection's own session signs it out (the server checks
                // current == true exactly); current false, missing or not a boolean changes
                // nothing.
                if ev.r#type == "session.revoked"
                    && frame
                        .get("data")
                        .and_then(|d| d.get("current"))
                        .and_then(Value::as_bool)
                        == Some(true)
                {
                    this.st.lock().unwrap().token = None; // never re-auth with a revoked session
                    this.signed_out(AuthChangeReason::SessionRevoked, None);
                    this.emit(WsEvent::AuthLost(ev.clone()));
                }
                if ev.r#type == "orderbook.update" {
                    let symbol = match ev.channel.strip_prefix("orderbook:") {
                        Some(s) => s.to_string(),
                        None => ev
                            .decode::<OrderBookUpdate>()
                            .map(|d| d.symbol)
                            .unwrap_or_default(),
                    };
                    let book = this.st.lock().unwrap().books.get(&symbol).cloned();
                    if let Some(b) = book {
                        b.on_update(&ev);
                    }
                }
                this.emit(WsEvent::Event(ev));
            }
            _ => {} // unknown frame types are ignored
        }
    }

    fn helpers(&self) -> Vec<LiveBalances> {
        self.st.lock().unwrap().live_balances.clone()
    }

    /// Forgets the sequence baseline of `channel` (all channels when `None`).
    fn reset_seq(s: &mut State, channel: Option<&str>) {
        let keys: Vec<String> = s
            .seq
            .keys()
            .filter(|k| channel.is_none_or(|c| c == k.as_str()))
            .cloned()
            .collect();
        for k in keys {
            if let Some(st) = s.seq.remove(&k)
                && let Some(t) = st.timer
            {
                t.cancel();
            }
        }
    }

    /// The first frame of a private channel is the baseline, a lower number is late (never a
    /// gap), and a higher one opens holes that must fill within the reorder window.
    fn track_seq(this: &Arc<Inner>, channel: &str, n: i64) {
        let mut s = this.st.lock().unwrap();
        s.seq_ids += 1;
        let fresh_id = s.seq_ids;
        let Some(st) = s.seq.get_mut(channel) else {
            s.seq.insert(
                channel.to_string(),
                SeqState {
                    next: n + 1,
                    id: fresh_id,
                    ..SeqState::default()
                },
            );
            return;
        };
        if n < st.next {
            if st.holes.remove(&n) && st.holes.is_empty() {
                if let Some(t) = st.timer.take() {
                    t.cancel();
                }
                st.first = None;
            }
            return;
        }
        if n > st.next && st.first.is_none() {
            st.first = Some(SequenceGap {
                channel: channel.to_string(),
                expected: st.next,
                received: n,
            });
        }
        st.holes.extend(st.next..n);
        st.next = n + 1;
        if !st.holes.is_empty() && st.timer.is_none() {
            let weak = Arc::downgrade(this);
            let ch = channel.to_string();
            let id = st.id;
            st.timer = Some(this.clock.call_later(
                this.opts.reorder_window,
                Box::new(move || {
                    let Some(inner) = weak.upgrade() else { return };
                    let (gap, helpers) = {
                        let mut s = inner.st.lock().unwrap();
                        let Some(st) = s.seq.get_mut(&ch) else { return };
                        if st.id != id || st.holes.is_empty() {
                            return;
                        }
                        st.timer = None;
                        st.holes.clear();
                        let gap = st.first.take();
                        (gap, s.live_balances.clone())
                    };
                    if let Some(g) = gap {
                        inner.emit(WsEvent::SequenceGap(g));
                    }
                    inner.emit(WsEvent::Resync(ResyncReason::SequenceGap));
                    if ch == "balances" {
                        for lb in helpers {
                            lb.trigger("sequence_gap");
                        }
                    }
                }),
            ));
        }
    }

    /// Moves the held private channels to the pending set and returns them.
    fn drop_private(s: &mut State) -> Vec<String> {
        let dropped: Vec<String> = s
            .channels
            .iter()
            .filter(|c| PRIVATE_CHANNELS.contains(&c.as_str()))
            .cloned()
            .collect();
        s.channels.retain(|c| !dropped.contains(c));
        for c in &dropped {
            if !s.pending_private.contains(c) {
                s.pending_private.push(c.clone());
            }
        }
        dropped
    }

    /// The server signed the connection out and ended every private subscription.
    fn signed_out(&self, reason: AuthChangeReason, code: Option<String>) {
        let (change, helpers) = {
            let mut s = self.st.lock().unwrap();
            for c in PRIVATE_CHANNELS {
                Inner::reset_seq(&mut s, Some(c));
            }
            let change = AuthChange {
                reason,
                previous_user_id: s.auth_user_id.take(),
                user_id: None,
                code,
                dropped: Inner::drop_private(&mut s),
            };
            (change, s.live_balances.clone())
        };
        for lb in helpers {
            lb.on_auth_changed(reason);
        }
        self.emit(WsEvent::AuthChanged(change));
    }

    /// A successful auth: detects an account switch, then restores the pending private channels.
    fn on_authenticated(this: &Arc<Inner>, user_id: Option<String>) {
        let (change, channels, helpers) = {
            let mut s = this.st.lock().unwrap();
            let previous = std::mem::replace(&mut s.auth_user_id, user_id.clone());
            let switched = previous.is_some() && previous != user_id;
            if switched {
                for c in PRIVATE_CHANNELS {
                    Inner::reset_seq(&mut s, Some(c));
                }
            }
            let change = match previous {
                Some(prev) if user_id.as_ref() != Some(&prev) => Some(AuthChange {
                    reason: AuthChangeReason::UserChanged,
                    previous_user_id: Some(prev),
                    user_id,
                    code: None,
                    dropped: Inner::drop_private(&mut s),
                }),
                _ => None,
            };
            let channels = std::mem::take(&mut s.pending_private);
            s.channels.extend(channels.iter().cloned());
            (change, channels, s.live_balances.clone())
        };
        if let Some(c) = change {
            for lb in &helpers {
                lb.on_auth_changed(AuthChangeReason::UserChanged);
            }
            this.emit(WsEvent::AuthChanged(c));
        }
        if channels.is_empty() {
            return;
        }
        // Not on the read task: the acknowledgement arrives through it.
        let inner = this.clone();
        let sent = channels.clone();
        tokio::spawn(async move {
            if let Err(e) = Inner::send_subscribe(&inner, sent.clone()).await {
                if let Error::WebSocket(w) = &e
                    && w.from_server
                {
                    // Refused by the server (e.g. signed out again meanwhile): back to pending.
                    let mut s = inner.st.lock().unwrap();
                    for c in &sent {
                        if let Some(i) = s.channels.iter().position(|x| x == c) {
                            s.channels.remove(i);
                            if !s.pending_private.contains(c) {
                                s.pending_private.push(c.clone());
                            }
                        }
                    }
                }
                inner.emit_error(e);
            }
        });
        this.emit(WsEvent::Resync(ResyncReason::Reauth));
    }

    fn on_dropped(this: &Arc<Inner>, generation: u64, code: u16, reason: String) {
        let (books, will_reconnect, start) = {
            let mut s = this.st.lock().unwrap();
            if generation != s.conn_gen || s.out.is_none() {
                return;
            }
            Inner::teardown(
                &mut s,
                WsError::local("DISCONNECTED", format!("connection lost (code {code})")),
            );
            let will = !s.closed_by_user && this.opts.reconnect;
            let start = will && !s.reconnecting;
            if start {
                s.reconnecting = true;
            }
            (s.books.values().cloned().collect::<Vec<_>>(), will, start)
        };
        for b in books {
            b.mark_disconnected(); // sequences reset per connection
        }
        for lb in this.helpers() {
            lb.mark_stale(); // the re-subscribe after the reconnect takes a new snapshot
        }
        this.emit(WsEvent::Close(CloseInfo {
            code,
            reason,
            will_reconnect,
        }));
        if start {
            tokio::spawn(Inner::reconnect_loop(this.clone()));
        }
    }

    async fn reconnect_loop(this: Arc<Inner>) {
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            if this.opts.max_reconnect_attempts > 0 && attempt > this.opts.max_reconnect_attempts {
                this.emit_error(
                    WsError::local(
                        "RECONNECT_FAILED",
                        format!(
                            "gave up after {} reconnect attempts",
                            this.opts.max_reconnect_attempts
                        ),
                    )
                    .into(),
                );
                break;
            }
            let base = this.opts.reconnect_base_delay.as_secs_f64()
                * 2f64.powi((attempt - 1).min(30) as i32);
            let capped = base.min(this.opts.reconnect_max_delay.as_secs_f64());
            let delay = Duration::from_secs_f64(rand::random::<f64>() * capped); // full jitter
            this.emit(WsEvent::Reconnecting { attempt, delay });
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = this.closing.notified() => break,
            }
            if this.st.lock().unwrap().closed_by_user {
                break;
            }
            match Inner::open(&this).await {
                Ok(_) => break,
                Err(e) => {
                    if this.st.lock().unwrap().closed_by_user {
                        break;
                    }
                    this.emit_error(e);
                }
            }
        }
        this.st.lock().unwrap().reconnecting = false;
    }

    fn teardown(s: &mut State, err: WsError) {
        if let Some(stop) = s.stop.take() {
            let _ = stop.send(());
        }
        s.out = None;
        s.welcome = None;
        for (_, p) in s.pending.drain() {
            let _ = p.tx.send(Err(err.clone()));
        }
    }

    fn close(this: &Arc<Inner>) {
        let (books, was_open) = {
            let mut s = this.st.lock().unwrap();
            if s.closed_by_user && s.out.is_none() {
                return;
            }
            s.closed_by_user = true;
            s.auth_user_id = None;
            Inner::reset_seq(&mut s, None);
            let was_open = s.out.is_some();
            Inner::teardown(
                &mut s,
                WsError::local("CLOSED", "connection closed by client"),
            );
            (s.books.values().cloned().collect::<Vec<_>>(), was_open)
        };
        this.closing.notify_waiters();
        for b in books {
            b.mark_disconnected();
        }
        for lb in this.helpers() {
            lb.mark_stale(); // no connection: nothing is live any more
        }
        if was_open {
            this.emit(WsEvent::Close(CloseInfo {
                code: 1000,
                reason: "client closing".into(),
                will_reconnect: false,
            }));
        }
    }

    pub(crate) fn remove_book(&self, symbol: &str, book: &LiveOrderBook) {
        let mut s = self.st.lock().unwrap();
        if s.books.get(symbol).is_some_and(|b| b.same(book)) {
            s.books.remove(symbol);
        }
    }

    pub(crate) fn unsubscribe_later(this: &Arc<Inner>, channel: String) {
        let inner = this.clone();
        tokio::spawn(async move {
            let _ = Inner::unsubscribe(&inner, vec![channel]).await;
        });
    }
}

async fn run_connection(
    inner: Weak<Inner>,
    stream: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    mut out: mpsc::UnboundedReceiver<Message>,
    mut stop: oneshot::Receiver<()>,
    generation: u64,
    opts: WsOptions,
) {
    let (mut sink, mut read) = stream.split();
    let mut ping = tokio::time::interval_at(
        tokio::time::Instant::now() + opts.ping_interval,
        opts.ping_interval,
    );
    // One deadline for the whole connection, pushed back only when a frame arrives. (A
    // per-read timeout would restart on every select! pass, e.g. on each outgoing ping, and
    // so never fire while the client keeps pinging a silent server.)
    let liveness = tokio::time::sleep(opts.liveness_timeout);
    tokio::pin!(liveness);
    let (code, reason) = loop {
        tokio::select! {
            _ = &mut stop => {
                let _ = sink.send(Message::Close(None)).await;
                return;
            }
            msg = out.recv() => match msg {
                Some(m) => {
                    if let Err(e) = sink.send(m).await {
                        break (1006, e.to_string());
                    }
                }
                None => {
                    let _ = sink.send(Message::Close(None)).await;
                    return;
                }
            },
            _ = ping.tick() => {
                // A dead socket is handled by the reader.
                if let Some(i) = inner.upgrade() {
                    let _ = i.send(json!({"op": "ping"}));
                }
            }
            _ = &mut liveness => break (4000, "liveness timeout".to_string()),
            r = read.next() => {
                liveness
                    .as_mut()
                    .reset(tokio::time::Instant::now() + opts.liveness_timeout);
                match r {
                    None => break (1006, "connection closed".to_string()),
                    Some(Err(e)) => break (1006, e.to_string()),
                    Some(Ok(Message::Close(frame))) => {
                        let (c, r) = frame
                            .map(|f| (u16::from(f.code), f.reason.to_string()))
                            .unwrap_or((1005, String::new()));
                        break (c, r);
                    }
                    Some(Ok(m)) => {
                        let Some(i) = inner.upgrade() else { return };
                        if let Some(frame) = parse_frame(&m) {
                            Inner::on_frame(&i, frame);
                        }
                    }
                }
            }
        }
    };
    if let Some(i) = inner.upgrade() {
        Inner::on_dropped(&i, generation, code, reason);
    }
}

fn parse_frame(m: &Message) -> Option<Map<String, Value>> {
    let text = match m {
        Message::Text(t) => t.as_str(),
        Message::Binary(b) => std::str::from_utf8(b).ok()?,
        _ => return None,
    };
    match serde_json::from_str::<Value>(text) {
        Ok(Value::Object(m)) => Some(m),
        _ => None,
    }
}

fn string_list(v: Option<&Value>) -> Vec<String> {
    v.and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn uniq(it: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut out: Vec<String> = vec![];
    for s in it {
        if !out.contains(&s) {
            out.push(s);
        }
    }
    out
}
