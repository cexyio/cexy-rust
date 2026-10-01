//! The REST client and its options.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use url::Url;

use crate::auth::{ApiKeyAuthenticator, Authenticator, REDACTED};
use crate::clock::{self, Clock};
use crate::error::{Error, Result};
use crate::limiter::{RateLimitState, RateLimiter};
use crate::models_gen::{ExchangeConfig, ServerTime};
use crate::operations_gen::OperationId;
use crate::services::{Account, Assets, Exports, Fees, Markets, Networks, Pools, Trading, Wallet};
use crate::signing::HmacAuthenticator;
use crate::transport::{
    Call, Random, Resolved, RetryHook, RetryInfo, Transport, decode_data, origin,
};

/// A retry callback: see [`ClientOptions::on_retry`].
pub type OnRetry = Arc<dyn Fn(&RetryInfo<'_>) + Send + Sync>;

/// The production API.
pub const DEFAULT_BASE_URL: &str = "https://api.cexy.io";

/// Default client-side limit without credentials (the server allows about 120 a minute per IP).
pub const DEFAULT_RPM_ANONYMOUS: u32 = 100;

/// Default client-side limit with an API key (the server allows about 600 a minute per key).
pub const DEFAULT_RPM_WITH_KEY: u32 = 300;

/// How a [`Client`] sends `api_key` and `api_secret`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum AuthScheme {
    /// `X-API-Key` and `X-API-Secret` headers (today's scheme).
    #[default]
    Headers,
    /// PLANNED, not accepted by the API yet: every private request is signed
    /// ([`HmacAuthenticator`]) and the secret never leaves the process. A key issued before
    /// signing existed fails with `KEY_NOT_SIGNABLE` (create a new key); there is no fallback.
    Hmac,
}

/// Configures a [`Client`]. The default gives an anonymous client for public data.
#[derive(Clone, Default)]
pub struct ClientOptions {
    /// API key id (`ak_…`). Give it together with `api_secret`, or not at all.
    pub api_key: Option<String>,
    /// API key secret. Never logged, never put in a URL.
    pub api_secret: Option<String>,
    /// How `api_key`/`api_secret` are sent. Default [`AuthScheme::Headers`]; keep it until the
    /// API announces request signing.
    pub auth: AuthScheme,
    /// A custom credentials scheme. Mutually exclusive with `api_key`/`api_secret`.
    pub authenticator: Option<Arc<dyn Authenticator>>,
    /// Default [`DEFAULT_BASE_URL`]. Must be `https://` (see `allow_insecure`).
    pub base_url: Option<String>,
    /// Allow plain `http://` (and `ws://` for WebSocket), but ONLY for a loopback host
    /// (localhost, 127.0.0.1, ::1), for example a local test server.
    pub allow_insecure: bool,
    /// Per-attempt timeout. Default 10 s.
    pub timeout: Option<Duration>,
    /// Retries after the first attempt for retryable failures. Default 3; `Some(0)` disables.
    pub max_retries: Option<u32>,
    /// Client-side rate limit in requests per minute. Default [`DEFAULT_RPM_ANONYMOUS`] without
    /// credentials and [`DEFAULT_RPM_WITH_KEY`] with them. It adapts downwards to the server's
    /// `X-RateLimit-*` headers.
    pub requests_per_minute: Option<u32>,
    /// Disables the client-side rate limiter.
    pub disable_rate_limit: bool,
    /// Appended to the User-Agent: `"my-bot/1.2"` gives `"cexy-rust/<VERSION> my-bot/1.2"`.
    pub user_agent_suffix: Option<String>,
    /// Called before each retry (for logging or metrics). Never receives credentials.
    pub on_retry: Option<OnRetry>,
}

impl ClientOptions {
    /// Options for an API key pair.
    pub fn with_api_key(api_key: impl Into<String>, api_secret: impl Into<String>) -> Self {
        ClientOptions {
            api_key: Some(api_key.into()),
            api_secret: Some(api_secret.into()),
            ..Default::default()
        }
    }
}

impl fmt::Debug for ClientOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let key = self.api_key.as_ref().map(|_| REDACTED);
        f.debug_struct("ClientOptions")
            .field("base_url", &self.base_url)
            .field("api_key", &key)
            .field("api_secret", &self.api_secret.as_ref().map(|_| REDACTED))
            .field("auth", &self.auth)
            .field(
                "authenticator",
                &self.authenticator.as_ref().map(|a| a.kind().to_string()),
            )
            .field("allow_insecure", &self.allow_insecure)
            .finish_non_exhaustive()
    }
}

/// Per-call overrides: see [`Client::with_options`].
#[derive(Debug, Clone, Default)]
pub struct CallOptions {
    /// Per-attempt timeout.
    pub timeout: Option<Duration>,
    /// Retries after the first attempt (`Some(0)` disables).
    pub max_retries: Option<u32>,
    /// The Idempotency-Key of a mutation (generated when absent). The server honours it on pool
    /// join and exit; set it yourself to make a retry across process restarts safe there.
    /// It is sent only there: orders and cancels do not honour it (their safety comes from
    /// `client_order_id`), so the SDK sends none.
    pub idempotency_key: Option<String>,
}

/// The CEXY.io REST client. Cloning is cheap and clones share the connection pool and the rate
/// limiter. Use one client per API key.
#[derive(Clone)]
pub struct Client {
    pub(crate) t: Arc<Transport>,
    pub(crate) opts: CallOptions,
}

impl Client {
    /// Checks the options and returns a client. It sends nothing.
    pub fn new(options: ClientOptions) -> Result<Client> {
        Client::build(options, clock::system(), Arc::new(rand::random::<f64>))
    }

    pub(crate) fn build(o: ClientOptions, clock: Arc<dyn Clock>, random: Random) -> Result<Client> {
        let auth: Option<Arc<dyn Authenticator>> =
            match (&o.api_key, &o.api_secret, &o.authenticator) {
                (Some(_), None, _) | (None, Some(_), _) => {
                    return Err(Error::config(
                        "api_key and api_secret must be given together (got only one of them)",
                    ));
                }
                (Some(_), Some(_), Some(_)) => {
                    return Err(Error::config(
                        "pass either api_key/api_secret or authenticator, not both",
                    ));
                }
                (Some(k), Some(s), None) => match o.auth {
                    AuthScheme::Hmac => {
                        Some(Arc::new(HmacAuthenticator::new(k.clone(), s.clone())?))
                    }
                    AuthScheme::Headers => {
                        Some(Arc::new(ApiKeyAuthenticator::new(k.clone(), s.clone())?))
                    }
                },
                (None, None, a) => a.clone(),
            };

        let base = o
            .base_url
            .as_deref()
            .unwrap_or(DEFAULT_BASE_URL)
            .trim_end_matches('/')
            .to_string();
        let u = Url::parse(&base)
            .map_err(|_| Error::config(format!("base_url is not a valid URL: {base:?}")))?;
        if u.host_str().is_none_or(str::is_empty) {
            return Err(Error::config(format!(
                "base_url is not a valid URL: {base:?}"
            )));
        }
        if !u.username().is_empty()
            || u.password().is_some()
            || u.query().is_some()
            || u.fragment().is_some()
        {
            return Err(Error::config(
                "base_url must not contain credentials, a query or a fragment",
            ));
        }
        check_secure_url(&u, "https", "http", o.allow_insecure, "base_url")?;

        let timeout = o.timeout.unwrap_or(Duration::from_secs(10));
        if timeout.is_zero() {
            return Err(Error::config("timeout must be > 0"));
        }
        let limiter = if o.disable_rate_limit {
            None
        } else {
            let rpm = o.requests_per_minute.unwrap_or(if auth.is_some() {
                DEFAULT_RPM_WITH_KEY
            } else {
                DEFAULT_RPM_ANONYMOUS
            });
            if rpm == 0 {
                return Err(Error::config("requests_per_minute must be > 0"));
            }
            Some(RateLimiter::new(rpm, clock.clone()))
        };
        let mut user_agent = crate::USER_AGENT.to_string();
        if let Some(s) = o
            .user_agent_suffix
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            if s.contains(['\r', '\n']) {
                return Err(Error::config(
                    "user_agent_suffix must not contain line breaks",
                ));
            }
            user_agent.push(' ');
            user_agent.push_str(s);
        }
        let http = no_redirect_client()?;
        let on_retry: Option<RetryHook> = o.on_retry.clone();
        let t = Transport {
            base_url: base,
            origin: origin(&u),
            http,
            auth,
            limiter,
            user_agent,
            timeout,
            max_retries: o.max_retries.unwrap_or(3),
            clock,
            random,
            on_retry,
            allow_insecure: o.allow_insecure,
        };
        Ok(Client {
            t: Arc::new(t),
            opts: CallOptions::default(),
        })
    }

    /// A client that applies `options` to every call made through it (it shares everything
    /// else with `self`): `client.with_options(CallOptions { timeout: Some(..), ..Default::default() })`.
    pub fn with_options(&self, options: CallOptions) -> Client {
        Client {
            t: self.t.clone(),
            opts: options,
        }
    }

    pub(crate) fn resolved(&self) -> Resolved {
        Resolved {
            timeout: self.opts.timeout.unwrap_or(self.t.timeout),
            max_retries: self.opts.max_retries.unwrap_or(self.t.max_retries),
            idempotency_key: self.opts.idempotency_key.clone(),
        }
    }

    /// Whether private endpoints are available.
    pub fn has_credentials(&self) -> bool {
        self.t.auth.is_some()
    }

    /// The API base URL.
    pub fn base_url(&self) -> &str {
        &self.t.base_url
    }

    /// The User-Agent the client sends.
    pub fn user_agent(&self) -> &str {
        &self.t.user_agent
    }

    /// The client-side rate limiter state, or `None` when it is disabled.
    pub fn rate_limit(&self) -> Option<RateLimitState> {
        self.t.limiter.as_ref().map(RateLimiter::state)
    }

    /// The server clock. Compare it with yours to detect skew.
    pub async fn time(&self) -> Result<ServerTime> {
        self.get(Call::new(OperationId::ServerTime)).await
    }

    /// The public exchange configuration (maintenance state, page sizes, ...).
    pub async fn config(&self) -> Result<ExchangeConfig> {
        self.get(Call::new(OperationId::ExchangeConfig)).await
    }

    /// Markets, tickers, order books, public trades and candles.
    pub fn markets(&self) -> Markets<'_> {
        Markets { c: self }
    }
    /// The asset catalogue.
    pub fn assets(&self) -> Assets<'_> {
        Assets { c: self }
    }
    /// Blockchain networks.
    pub fn networks(&self) -> Networks<'_> {
        Networks { c: self }
    }
    /// Fee schedules.
    pub fn fees(&self) -> Fees<'_> {
        Fees { c: self }
    }
    /// Liquidity pools. Join and exit need an API key with the trade scope.
    pub fn pools(&self) -> Pools<'_> {
        Pools { c: self }
    }
    /// Balances, ledger, notifications, sub-accounts and API keys.
    pub fn account(&self) -> Account<'_> {
        Account { c: self }
    }
    /// CSV exports.
    pub fn exports(&self) -> Exports<'_> {
        Exports { c: self }
    }
    /// Wallet reads. API keys can never withdraw or transfer; there are no such methods.
    pub fn wallet(&self) -> Wallet<'_> {
        Wallet { c: self }
    }
    /// Orders and your trades. Placing and cancelling need the trade scope.
    pub fn trading(&self) -> Trading<'_> {
        Trading { c: self }
    }

    pub(crate) async fn get<T: serde::de::DeserializeOwned>(&self, c: Call) -> Result<T> {
        let op = c.op;
        let raw = self.t.request(c, &self.resolved()).await?;
        decode_data(op, &raw)
    }
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let auth = match &self.t.auth {
            Some(a) => format!("{} {REDACTED}", a.kind()),
            None => "none".to_string(),
        };
        write!(f, "cexy::Client({}, auth={auth})", self.t.base_url)
    }
}

/// A reqwest client that never follows redirects. Following one could send X-API-Key and
/// X-API-Secret to another host, even over plain http, and a 307/308 would re-send an order. The
/// 3xx response is returned instead and becomes an `UNEXPECTED_REDIRECT` error.
fn no_redirect_client() -> Result<reqwest::Client> {
    let mut tls = (*crate::tls::client_config()).clone();
    tls.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .tls_backend_preconfigured(tls)
        .build()
        .map_err(|e| Error::config(format!("cannot build the HTTP client: {e}")))
}

/// Whether `host` is a loopback name or address, the only hosts where plain-text transport may
/// be allowed.
pub fn is_local_host(host: &str) -> bool {
    let h = host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_lowercase();
    h == "localhost" || h == "127.0.0.1" || h == "::1"
}

/// Requires scheme `secure`; `insecure` is accepted only with `allow_insecure` and a loopback
/// host. Credentials must never travel in clear text.
pub(crate) fn check_secure_url(
    u: &Url,
    secure: &str,
    insecure: &str,
    allow_insecure: bool,
    what: &str,
) -> Result<()> {
    let scheme = u.scheme().to_lowercase();
    if scheme == secure {
        return Ok(());
    }
    if scheme == insecure {
        if !allow_insecure {
            return Err(Error::config(format!(
                "{what} must use {secure}:// (set allow_insecure only for a local test server)"
            )));
        }
        if !is_local_host(u.host_str().unwrap_or("")) {
            return Err(Error::config(format!(
                "{what}: {insecure}:// is only allowed for localhost, 127.0.0.1 or ::1"
            )));
        }
        return Ok(());
    }
    Err(Error::config(format!("{what} must use {secure}://")))
}
