//! Errors: API error responses, local checks, connection failures and WebSocket errors.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use reqwest::header::HeaderMap;
use serde_json::Value;

use crate::cancel_all::CancelAllSummary;
use crate::models_gen::ErrorCode;

/// `Result` with this crate's [`Error`].
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Every error the SDK returns.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// An error response from the API (the `{"error": {...}}` envelope), or a 3xx the SDK
    /// refused to follow (code `UNEXPECTED_REDIRECT`).
    #[error("{0}")]
    Api(Box<ApiError>),

    /// An invalid client configuration or call, detected before anything is sent.
    #[error("cexy: {0}")]
    Config(String),

    /// A malformed decimal string in an amount field, detected before sending.
    #[error("cexy: {message}")]
    InvalidAmount {
        /// The field name (empty for [`crate::Amount::new`]).
        field: String,
        /// What is wrong.
        message: String,
    },

    /// The request produced no HTTP response (DNS, TLS, connection reset, or the per-attempt
    /// timeout). Always retryable.
    #[error("{0}")]
    Connection(ConnectionError),

    /// `place_order` failed ambiguously and the lookup by `client_order_id` failed too, or the
    /// server accepted the order but its response could not be decoded (`source` is then a
    /// [`Error::Decode`]). Either way the order may exist: check `order_by_client_id` before
    /// placing it again.
    #[error(
        "cexy: order state unknown for client_order_id {client_order_id}; check order_by_client_id before retrying: {source}"
    )]
    OrderStateUnknown {
        /// The `client_order_id` that was sent.
        client_order_id: String,
        /// The failure of the order request.
        source: Box<Error>,
    },

    /// `cancel_all_until_done` stopped on an error; `summary` holds what the earlier rounds did.
    #[error("cexy: cancel-all interrupted after {} round(s): {source}", .summary.rounds)]
    CancelAllInterrupted {
        /// The error that stopped the loop.
        source: Box<Error>,
        /// The merged result of the rounds before the error.
        summary: Box<CancelAllSummary>,
    },

    /// A WebSocket protocol error, a server error frame, or a local guard.
    #[error("{0}")]
    WebSocket(WsError),

    /// A response that could not be decoded.
    #[error("cexy: {0}")]
    Decode(String),

    /// An iterate-all helper ([`crate::Futures::all_fills`], [`crate::Futures::all_funding`])
    /// kept getting an empty page whose `next_cursor` was the cursor it had sent: the server's
    /// data source is busy. It waited and asked again `retries` times (see
    /// [`crate::Futures::with_max_busy_retries`]), then gave up. Code [`PAGING_STALLED`], a local
    /// error (never sent by the server). Retryable: page again later from `cursor`. The rows
    /// yielded before it are NOT the complete history.
    #[error(
        "cexy: [{PAGING_STALLED}] {operation}: still no rows after {retries} retries of the same cursor; page again later"
    )]
    PagingStalled {
        /// The listing operation, such as `"fills"`.
        operation: &'static str,
        /// The cursor that stalled, exactly as the server gave it (opaque).
        cursor: String,
        /// How many times the same cursor was asked again.
        retries: u32,
    },

    /// An iterate-all helper got a page WITH rows whose `next_cursor` was a cursor it had already
    /// sent: following it would repeat rows forever. The rows of that page were yielded, then
    /// this. Code [`PAGING_CURSOR_REPEATED`], a local error; not retryable. The rows yielded
    /// before it are NOT the complete history.
    #[error(
        "cexy: [{PAGING_CURSOR_REPEATED}] {operation}: the server repeated a cursor after a page of rows; paging stopped"
    )]
    PagingCursorRepeated {
        /// The listing operation, such as `"fills"`.
        operation: &'static str,
        /// The repeated cursor, exactly as the server gave it (opaque).
        cursor: String,
    },
}

/// The code of [`Error::PagingStalled`].
pub const PAGING_STALLED: &str = "PAGING_STALLED";

/// The code of [`Error::PagingCursorRepeated`].
pub const PAGING_CURSOR_REPEATED: &str = "PAGING_CURSOR_REPEATED";

impl Error {
    /// The API error, if this is one.
    pub fn api(&self) -> Option<&ApiError> {
        match self {
            Error::Api(e) => Some(e),
            _ => None,
        }
    }

    /// Whether this is an API error of `category` (see [`ApiError::is`]).
    pub fn is(&self, category: ErrorCategory) -> bool {
        self.api().is_some_and(|e| e.is(category))
    }

    /// The machine-readable code: the API error's code, a WebSocket error's code, or
    /// [`PAGING_STALLED`] / [`PAGING_CURSOR_REPEATED`]. `None` for the other local errors.
    pub fn code(&self) -> Option<&str> {
        match self {
            Error::Api(e) => Some(e.code.as_str()),
            Error::WebSocket(e) => Some(&e.code),
            Error::PagingStalled { .. } => Some(PAGING_STALLED),
            Error::PagingCursorRepeated { .. } => Some(PAGING_CURSOR_REPEATED),
            _ => None,
        }
    }

    /// Whether an identical retry could succeed: a connection failure, [`Error::PagingStalled`],
    /// or an API error marked retryable (including 409 `CONCURRENT_MODIFICATION`). A 4xx is never retryable except 429
    /// and 409 `CONCURRENT_MODIFICATION`, whatever its body says.
    ///
    /// An error whose server wait (Retry-After) exceeds [`MAX_SERVER_WAIT`] is not retryable:
    /// the SDK fails fast instead of waiting that long. `retry_after` still carries the value.
    pub fn is_retryable(&self) -> bool {
        match self {
            Error::Connection(_) | Error::PagingStalled { .. } => true,
            Error::Api(e) => {
                e.retryable_ignoring_wait() && e.retry_after.is_none_or(|d| d <= MAX_SERVER_WAIT)
            }
            _ => false,
        }
    }

    /// The server asked for a wait longer than [`MAX_SERVER_WAIT`].
    pub(crate) fn server_wait_too_long(&self) -> bool {
        self.api()
            .and_then(|e| e.retry_after)
            .is_some_and(|d| d > MAX_SERVER_WAIT)
    }

    /// A failure after which it is unknown whether a mutation took effect.
    pub(crate) fn is_ambiguous(&self) -> bool {
        match self {
            Error::Connection(_) => true,
            Error::Api(e) => e.status >= 500,
            _ => false,
        }
    }

    pub(crate) fn config(msg: impl Into<String>) -> Error {
        Error::Config(msg.into())
    }
}

impl From<ApiError> for Error {
    fn from(e: ApiError) -> Error {
        Error::Api(Box::new(e))
    }
}

impl From<WsError> for Error {
    fn from(e: WsError) -> Error {
        Error::WebSocket(e)
    }
}

/// The category of an API error. An error matches at most one, except that
/// `JurisdictionBlocked` also matches `Forbidden`. An error with a code this SDK does not know
/// matches none: check [`ApiError::code`] instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorCategory {
    /// 400.
    Validation,
    /// 401: missing or invalid credentials.
    Authentication,
    /// 403: the key lacks a scope, or the route is session-only (and 451).
    Forbidden,
    /// 451 `JURISDICTION_BLOCKED`: not available in the caller's jurisdiction.
    JurisdictionBlocked,
    /// 404.
    NotFound,
    /// 409.
    Conflict,
    /// 422: refused by a business rule.
    Unprocessable,
    /// 429.
    RateLimited,
    /// 5xx.
    Server,
    /// 3xx: the SDK never follows redirects.
    UnexpectedRedirect,
}

/// An error response from the API.
#[derive(Clone)]
pub struct ApiError {
    /// HTTP status.
    pub status: u16,
    /// Machine-readable code. Branch on it, never on the message. A code missing from
    /// `errors.yaml` arrives as `ErrorCode::Other`; a response without the envelope (a proxy
    /// error page) gets `HTTP_<status>`.
    pub code: ErrorCode,
    /// Human-readable; may change between releases. Credentials are redacted from it.
    pub message: String,
    /// Structured context. String values have credentials redacted.
    pub details: BTreeMap<String, Value>,
    /// Per-field validation messages (400), with credentials redacted.
    pub fields: BTreeMap<String, String>,
    /// Quote it in support requests.
    pub request_id: Option<String>,
    /// Whether an identical retry could succeed.
    pub retryable: bool,
    /// How long the server asked to wait (Retry-After or `details.retry_after_seconds`).
    pub retry_after: Option<Duration>,
    category: Option<ErrorCategory>,
}

impl ApiError {
    /// Marked retryable (or 409 `CONCURRENT_MODIFICATION`), and not a 4xx other than 429 and
    /// 409 `CONCURRENT_MODIFICATION`; the server's wait is not considered.
    pub(crate) fn retryable_ignoring_wait(&self) -> bool {
        let concurrent = self.code == ErrorCode::ConcurrentModification;
        if (400..500).contains(&self.status)
            && self.status != 429
            && !(self.status == 409 && concurrent)
        {
            return false;
        }
        self.retryable || concurrent
    }

    /// The category, or `None` for a code this SDK does not know.
    pub fn category(&self) -> Option<ErrorCategory> {
        self.category
    }

    /// Whether the error belongs to `category`. `JurisdictionBlocked` also matches `Forbidden`.
    pub fn is(&self, category: ErrorCategory) -> bool {
        match self.category {
            Some(c) if c == category => true,
            Some(ErrorCategory::JurisdictionBlocked) => category == ErrorCategory::Forbidden,
            _ => false,
        }
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "cexy: [{} {}] {}", self.status, self.code, self.message)?;
        if let Some(rid) = &self.request_id {
            write!(f, " (request_id {rid})")?;
        }
        Ok(())
    }
}

impl fmt::Debug for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApiError")
            .field("status", &self.status)
            .field("code", &self.code)
            .field("message", &self.message)
            .field("details", &self.details)
            .field("fields", &self.fields)
            .field("request_id", &self.request_id)
            .field("retryable", &self.retryable)
            .field("retry_after", &self.retry_after)
            .finish()
    }
}

/// The request produced no HTTP response.
#[derive(Debug, Clone)]
pub struct ConnectionError {
    /// HTTP method.
    pub method: String,
    /// Path template of the operation.
    pub path: String,
    /// True when the per-attempt timeout expired.
    pub timeout: bool,
    /// What happened (credentials redacted).
    pub message: String,
}

impl fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.timeout {
            write!(f, "cexy: {} {} timed out", self.method, self.path)
        } else {
            write!(
                f,
                "cexy: {} {} failed: {}",
                self.method, self.path, self.message
            )
        }
    }
}

/// A WebSocket protocol error, a server error frame (`from_server`), or a local guard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WsError {
    /// Such as `NOT_CONNECTED`, `TIMEOUT`, `LOCAL_RATE_LIMIT`, or the server's code.
    pub code: String,
    /// What happened.
    pub message: String,
    /// True when it came from a server error frame.
    pub from_server: bool,
}

impl WsError {
    pub(crate) fn local(code: &str, message: impl Into<String>) -> WsError {
        WsError {
            code: code.to_string(),
            message: message.into(),
            from_server: false,
        }
    }
}

impl fmt::Display for WsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "cexy websocket: [{}] {}", self.code, self.message)
    }
}

impl std::error::Error for WsError {}

fn default_retryable(status: u16) -> bool {
    matches!(status, 429 | 500 | 502 | 503 | 504)
}

/// Builds the API error for an error response. A known code maps by HTTP status; an unknown
/// code keeps no category (never a crash); a body without the envelope maps by status with the
/// code `HTTP_<status>`.
pub(crate) fn error_from_response(
    status: u16,
    body: &[u8],
    headers: &HeaderMap,
    redact: &dyn Fn(&str) -> String,
) -> ApiError {
    let header_rid = header(headers, "x-request-id");
    let mut e = ApiError {
        status,
        code: ErrorCode::Other(format!("HTTP_{status}")),
        message: format!("HTTP {status}"),
        details: BTreeMap::new(),
        fields: BTreeMap::new(),
        request_id: header_rid,
        retryable: default_retryable(status),
        retry_after: None,
        category: None,
    };
    let env: Option<Value> = serde_json::from_slice(body).ok();
    let err_obj = env
        .as_ref()
        .and_then(|v| v.get("error"))
        .and_then(Value::as_object);
    let code = err_obj.and_then(|o| o.get("code")).and_then(Value::as_str);
    let has_envelope = code.is_some();
    if let (Some(o), Some(code)) = (err_obj, code) {
        e.code = ErrorCode::from(code);
        e.message = o
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or(code)
            .to_string();
        if let Some(d) = o.get("details").and_then(Value::as_object) {
            // The server may echo request values back, at any depth and even as keys: keep
            // credentials out of all of them.
            for (k, v) in d {
                e.details.insert(redact(k), redact_json(v, redact));
            }
        }
        if let Some(fl) = o.get("fields").and_then(Value::as_object) {
            for (k, v) in fl {
                if let Some(s) = v.as_str() {
                    e.fields.insert(redact(k), redact(s));
                }
            }
        }
        if let Some(rid) = o
            .get("request_id")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
        {
            e.request_id = Some(redact(rid));
        }
        if let Some(r) = o.get("retryable").and_then(Value::as_bool) {
            e.retryable = r;
        }
    }
    e.message = redact(&e.message);
    e.retry_after = retry_after(headers, &e.details);
    e.category = if status == 429 || e.code == ErrorCode::RateLimited {
        Some(ErrorCategory::RateLimited)
    } else if e.code == ErrorCode::JurisdictionBlocked {
        Some(ErrorCategory::JurisdictionBlocked)
    } else if has_envelope && !e.code.is_known() {
        None
    } else if status >= 500 {
        Some(ErrorCategory::Server)
    } else {
        match status {
            400 => Some(ErrorCategory::Validation),
            401 => Some(ErrorCategory::Authentication),
            403 => Some(ErrorCategory::Forbidden),
            404 => Some(ErrorCategory::NotFound),
            409 => Some(ErrorCategory::Conflict),
            422 => Some(ErrorCategory::Unprocessable),
            451 => Some(ErrorCategory::JurisdictionBlocked),
            _ => None,
        }
    };
    e
}

/// The API error for a 3xx response. `details["location"]` holds the Location header, if any.
pub(crate) fn redirect_error(
    status: u16,
    headers: &HeaderMap,
    redact: &dyn Fn(&str) -> String,
) -> ApiError {
    let loc = header(headers, "location").map(|l| redact(&l));
    let mut message =
        format!("unexpected redirect (HTTP {status}); the SDK does not follow redirects");
    let mut details = BTreeMap::new();
    if let Some(l) = &loc {
        message.push_str(&format!(" (Location: {l})"));
        details.insert("location".to_string(), Value::String(l.clone()));
    }
    ApiError {
        status,
        code: ErrorCode::UnexpectedRedirect,
        message,
        details,
        fields: BTreeMap::new(),
        request_id: header(headers, "x-request-id"),
        retryable: false,
        retry_after: None,
        category: Some(ErrorCategory::UnexpectedRedirect),
    }
}

/// `v` with credentials redacted from every string and object key, at any depth.
fn redact_json(v: &Value, redact: &dyn Fn(&str) -> String) -> Value {
    match v {
        Value::String(s) => Value::String(redact(s)),
        Value::Array(a) => Value::Array(a.iter().map(|x| redact_json(x, redact)).collect()),
        Value::Object(o) => Value::Object(
            o.iter()
                .map(|(k, x)| (redact(k), redact_json(x, redact)))
                .collect(),
        ),
        other => other.clone(),
    }
}

pub(crate) fn header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// The longest wait the SDK accepts from a server hint (Retry-After,
/// `details.retry_after_seconds`, `X-RateLimit-Reset`). A longer hint is not waited: the call
/// fails with the error, and the client-side limiter never blocks longer than this.
pub const MAX_SERVER_WAIT: Duration = Duration::from_secs(120);

/// A server-supplied number of seconds as a wait. Unparseable, non-finite, zero or negative
/// values are ignored (`None`); values too large for a `Duration` saturate to `Duration::MAX`
/// (never a panic).
pub(crate) fn server_secs(secs: f64) -> Option<Duration> {
    if !secs.is_finite() || secs <= 0.0 {
        return None;
    }
    Some(Duration::try_from_secs_f64(secs).unwrap_or(Duration::MAX))
}

/// Reads Retry-After (seconds or an HTTP date) and `details.retry_after_seconds`; the larger wins.
/// Both are untrusted: garbage is ignored and nothing here can panic.
fn retry_after(headers: &HeaderMap, details: &BTreeMap<String, Value>) -> Option<Duration> {
    let mut best: Option<Duration> = None;
    if let Some(v) = header(headers, "retry-after") {
        if let Ok(secs) = v.parse::<f64>() {
            best = server_secs(secs);
        } else if let Ok(at) = chrono::DateTime::parse_from_rfc2822(&v) {
            let d = at.with_timezone(&chrono::Utc) - chrono::Utc::now();
            if let Ok(d) = d.to_std()
                && !d.is_zero()
            {
                best = Some(d);
            }
        }
    }
    let secs = match details.get("retry_after_seconds") {
        Some(Value::Number(n)) => n.as_f64(),
        Some(Value::String(s)) => s.parse::<f64>().ok(),
        _ => None,
    };
    if let Some(d) = secs.and_then(server_secs)
        && best.is_none_or(|b| d > b)
    {
        best = Some(d);
    }
    best
}
