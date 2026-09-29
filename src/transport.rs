//! HTTP transport: rate limiter, credentials, per-attempt timeout, error mapping and retries.

use std::sync::Arc;
use std::time::Duration;

use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::de::DeserializeOwned;
use url::Url;

use crate::auth::{AuthRequest, Authenticator};
use crate::clock::Clock;
use crate::error::{
    ConnectionError, Error, ErrorCategory, MAX_SERVER_WAIT, Result, error_from_response,
    redirect_error,
};
use crate::limiter::RateLimiter;
use crate::operations_gen::OperationId;

const BACKOFF_BASE: Duration = Duration::from_millis(500);
const BACKOFF_MAX: Duration = Duration::from_secs(10);
const MAX_RESPONSE_BODY: usize = 64 << 20;

/// Passed to `ClientOptions::on_retry` before the SDK waits and retries. It never holds
/// credentials.
#[derive(Debug)]
pub struct RetryInfo<'a> {
    /// The operation.
    pub operation: OperationId,
    /// HTTP method.
    pub method: &'static str,
    /// Path template.
    pub path: &'static str,
    /// 1 for the first retry.
    pub attempt: u32,
    /// How long the SDK waits before retrying.
    pub delay: Duration,
    /// The failure being retried.
    pub error: &'a Error,
    /// The Idempotency-Key reused on every attempt, if one is sent.
    pub idempotency_key: Option<&'a str>,
}

pub(crate) type RetryHook = Arc<dyn Fn(&RetryInfo<'_>) + Send + Sync>;
pub(crate) type Random = Arc<dyn Fn() -> f64 + Send + Sync>;

pub(crate) struct Call {
    pub(crate) op: OperationId,
    pub(crate) path: Vec<(&'static str, String)>,
    pub(crate) query: Vec<(&'static str, String)>,
    pub(crate) body: Option<Vec<u8>>,
    pub(crate) text: bool,
    pub(crate) idempotency_key: Option<String>,
    /// The endpoint does not honour Idempotency-Key, so none is sent.
    pub(crate) no_idempotency_key: bool,
}

impl Call {
    pub(crate) fn new(op: OperationId) -> Call {
        Call {
            op,
            path: vec![],
            query: vec![],
            body: None,
            text: false,
            idempotency_key: None,
            no_idempotency_key: false,
        }
    }
    pub(crate) fn path(mut self, name: &'static str, value: &str) -> Call {
        self.path.push((name, value.to_string()));
        self
    }
    pub(crate) fn query(mut self, q: Vec<(&'static str, String)>) -> Call {
        self.query = q;
        self
    }
    pub(crate) fn json<T: serde::Serialize>(mut self, body: &T) -> Result<Call> {
        let bytes = serde_json::to_vec(body).map_err(|e| {
            Error::config(format!(
                "{}: cannot encode the request body: {e}",
                self.op.as_str()
            ))
        })?;
        self.body = Some(bytes);
        Ok(self)
    }
}

/// Resolved per-call options.
#[derive(Clone)]
pub(crate) struct Resolved {
    pub(crate) timeout: Duration,
    pub(crate) max_retries: u32,
    pub(crate) idempotency_key: Option<String>,
}

pub(crate) struct Raw {
    pub(crate) body: Vec<u8>,
}

pub(crate) struct Transport {
    pub(crate) base_url: String,
    pub(crate) origin: String,
    pub(crate) http: reqwest::Client,
    pub(crate) auth: Option<Arc<dyn Authenticator>>,
    pub(crate) limiter: Option<RateLimiter>,
    pub(crate) user_agent: String,
    pub(crate) timeout: Duration,
    pub(crate) max_retries: u32,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) random: Random,
    pub(crate) on_retry: Option<RetryHook>,
    pub(crate) allow_insecure: bool,
}

impl Transport {
    /// Sends `c` with the standard retry policy: retryable errors and connection failures are
    /// retried. A mutation carries an Idempotency-Key reused on every attempt; the server honours
    /// it on pool join and exit. A mutation is retried only when it is repeat-safe (see
    /// [`repeat_safe`]); any other is sent once. `place_order` and `cancel_order` use `attempt`
    /// with their own policies.
    pub(crate) async fn request(&self, mut c: Call, o: &Resolved) -> Result<Raw> {
        let info = c.op.info();
        if info.method != "GET" && c.idempotency_key.is_none() && !c.no_idempotency_key {
            c.idempotency_key = Some(o.idempotency_key.clone().unwrap_or_else(new_id));
        }
        let max_retries = if repeat_safe(&c) { o.max_retries } else { 0 };
        let mut attempt = 0;
        loop {
            match self.attempt(&c, o).await {
                Ok(raw) => return Ok(raw),
                Err(e) => {
                    if attempt >= max_retries || !e.is_retryable() {
                        return Err(e);
                    }
                    self.backoff(c.op, attempt, &e, c.idempotency_key.as_deref())
                        .await;
                    attempt += 1;
                }
            }
        }
    }

    /// Waits before retry number `attempt + 1`, honouring server hints.
    pub(crate) async fn backoff(
        &self,
        op: OperationId,
        attempt: u32,
        cause: &Error,
        idem: Option<&str>,
    ) {
        let d = self.retry_delay(attempt, cause);
        if let Some(hook) = &self.on_retry {
            let info = op.info();
            hook(&RetryInfo {
                operation: op,
                method: info.method,
                path: info.path,
                attempt: attempt + 1,
                delay: d,
                error: cause,
                idempotency_key: idem,
            });
        }
        self.clock.sleep(d).await;
    }

    /// The server's hint plus up to 250 ms of jitter, or full-jitter exponential backoff
    /// (500 ms doubling, capped at 10 s). A hint is never taken beyond [`MAX_SERVER_WAIT`]
    /// (callers stop before that: such errors are not retryable).
    pub(crate) fn retry_delay(&self, attempt: u32, cause: &Error) -> Duration {
        if let Some(ra) = cause.api().and_then(|e| e.retry_after) {
            let jitter = Duration::from_millis(((self.random)() * 250.0) as u64);
            return ra.min(MAX_SERVER_WAIT).saturating_add(jitter);
        }
        let capped = BACKOFF_BASE
            .saturating_mul(2u32.saturating_pow(attempt.min(20)))
            .min(BACKOFF_MAX);
        Duration::from_nanos(((self.random)() * capped.as_nanos() as f64).ceil() as u64)
    }

    /// One try: rate limiter, credentials, timeout, error mapping. No retries.
    pub(crate) async fn attempt(&self, c: &Call, o: &Resolved) -> Result<Raw> {
        let info = c.op.info();
        let url = self.build_url(c)?;
        if info.auth == "api_key" && self.auth.is_none() {
            return Err(Error::config(format!(
                "{} {} needs an API key: create the client with api_key and api_secret",
                info.method, info.path
            )));
        }
        if let Some(l) = &self.limiter {
            l.acquire().await;
        }

        let mut headers = HeaderMap::new();
        let accept = if c.text {
            "text/csv, application/json"
        } else {
            "application/json"
        };
        headers.insert("accept", HeaderValue::from_static(accept));
        headers.insert(
            "user-agent",
            HeaderValue::from_str(&self.user_agent)
                .map_err(|_| Error::config("invalid User-Agent"))?,
        );
        if c.body.is_some() {
            headers.insert("content-type", HeaderValue::from_static("application/json"));
        }
        if info.method != "GET"
            && let Some(k) = &c.idempotency_key
        {
            headers.insert(
                "idempotency-key",
                HeaderValue::from_str(k).map_err(|_| Error::config("invalid Idempotency-Key"))?,
            );
        }
        if info.auth == "api_key" {
            // Defence in depth: requests are always built from base_url, so this only fails on a bug.
            if origin(&url) != self.origin {
                return Err(Error::config(format!(
                    "{} {}: refusing to send credentials to {}",
                    info.method,
                    info.path,
                    origin(&url)
                )));
            }
            let auth = self.auth.as_ref().expect("checked above");
            let mut req = AuthRequest {
                method: info.method,
                url: &url,
                body: c.body.as_deref(),
                headers: vec![],
            };
            auth.authenticate(&mut req)?;
            for (k, v) in req.headers {
                let name = HeaderName::from_bytes(k.as_bytes())
                    .map_err(|_| Error::config("invalid header name"))?;
                let mut value = HeaderValue::from_str(&v)
                    .map_err(|_| Error::config("invalid credential header value"))?;
                value.set_sensitive(true);
                headers.insert(name, value);
            }
        }

        let method = reqwest::Method::from_bytes(info.method.as_bytes()).expect("valid method");
        let mut rb = self
            .http
            .request(method, url)
            .headers(headers)
            .timeout(o.timeout);
        if let Some(b) = &c.body {
            rb = rb.body(b.clone());
        }
        let conn_err = |e: reqwest::Error| {
            Error::Connection(ConnectionError {
                method: info.method.to_string(),
                path: info.path.to_string(),
                timeout: e.is_timeout(),
                message: self.redact(&e.to_string()),
            })
        };
        let mut resp = rb.send().await.map_err(conn_err)?;
        let status = resp.status().as_u16();
        let resp_headers = resp.headers().clone();
        let mut body = Vec::new();
        while let Some(chunk) = resp.chunk().await.map_err(conn_err)? {
            if body.len() + chunk.len() > MAX_RESPONSE_BODY {
                return Err(Error::Decode(format!(
                    "{} {}: response larger than 64 MiB",
                    info.method, info.path
                )));
            }
            body.extend_from_slice(&chunk);
        }

        if let Some(l) = &self.limiter {
            l.update(&resp_headers);
        }
        let redact = |s: &str| self.redact(s);
        if (300..=399).contains(&status) {
            // Redirects are never followed (the client is built with Policy::none()): not
            // retryable, not ambiguous.
            return Err(redirect_error(status, &resp_headers, &redact).into());
        }
        if !(200..=299).contains(&status) {
            let e = error_from_response(status, &body, &resp_headers, &redact);
            if e.is(ErrorCategory::RateLimited)
                && let (Some(ra), Some(l)) = (e.retry_after, &self.limiter)
            {
                l.block_for(ra);
            }
            return Err(e.into());
        }
        if !c.text
            && !body.is_empty()
            && serde_json::from_slice::<serde::de::IgnoredAny>(&body).is_err()
        {
            let ct = resp_headers
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("unknown content");
            return Err(Error::Decode(format!(
                "{} {}: expected JSON, got {ct:?}",
                info.method, info.path
            )));
        }
        Ok(Raw { body })
    }

    pub(crate) fn build_url(&self, c: &Call) -> Result<Url> {
        let info = c.op.info();
        let mut path = String::new();
        let mut rest = info.path;
        while let Some(start) = rest.find('{') {
            let end = rest[start..]
                .find('}')
                .map(|e| start + e)
                .expect("well-formed path template");
            path.push_str(&rest[..start]);
            let name = &rest[start + 1..end];
            let value = c
                .path
                .iter()
                .find(|(n, _)| *n == name)
                .map(|(_, v)| v.as_str())
                .unwrap_or("");
            if value.is_empty() || value.contains(['\r', '\n']) {
                return Err(Error::config(format!(
                    "{} {}: {name} is required",
                    info.method, info.path
                )));
            }
            // "." and ".." would be dot segments: the url crate resolves them (even as %2E),
            // so the request would silently go to a different route.
            if value == "." || value == ".." {
                return Err(Error::config(format!(
                    "{} {}: {name} must not be \".\" or \"..\"",
                    info.method, info.path
                )));
            }
            path.push_str(&encode_segment(value));
            rest = &rest[end + 1..];
        }
        path.push_str(rest);
        let mut url = Url::parse(&format!("{}{}", self.base_url, path)).map_err(|e| {
            Error::config(format!("{} {}: invalid URL: {e}", info.method, info.path))
        })?;
        if !c.query.is_empty() {
            let mut q = url.query_pairs_mut();
            for (k, v) in &c.query {
                q.append_pair(k, v);
            }
        }
        Ok(url)
    }

    pub(crate) fn redact(&self, s: &str) -> String {
        match &self.auth {
            Some(a) => a.redact(s),
            None => s.to_string(),
        }
    }
}

/// Whether `request` may retry `c`: reads, and the mutations that are safe to repeat: pool join
/// and exit with their Idempotency-Key (the server honours it there), and cancel-all (naturally
/// repeatable). The server ignores Idempotency-Key elsewhere, so no other mutation is retried.
fn repeat_safe(c: &Call) -> bool {
    c.op.info().method == "GET"
        || c.op == OperationId::CancelAll
        || (matches!(c.op, OperationId::JoinPool | OperationId::ExitPool)
            && c.idempotency_key.is_some())
}

/// Percent-encodes one path segment (like encodeURIComponent: `/` becomes `%2F`).
fn encode_segment(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    for b in v.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.~!*'()".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// The scheme and host (and port) of `u`, lower case: `"https://api.cexy.io"`.
pub(crate) fn origin(u: &Url) -> String {
    let mut s = format!(
        "{}://{}",
        u.scheme().to_lowercase(),
        u.host_str().unwrap_or("").to_lowercase()
    );
    if let Some(p) = u.port() {
        s.push_str(&format!(":{p}"));
    }
    s
}

#[derive(serde::Deserialize)]
struct Envelope<T> {
    data: T,
}

/// Decodes the `{"data": T}` envelope.
pub(crate) fn decode_data<T: DeserializeOwned>(op: OperationId, raw: &Raw) -> Result<T> {
    serde_json::from_slice::<Envelope<T>>(&raw.body)
        .map(|e| e.data)
        .map_err(|e| Error::Decode(format!("{}: cannot decode the response: {e}", op.as_str())))
}

/// Decodes a bare JSON body (a page).
pub(crate) fn decode<T: DeserializeOwned>(op: OperationId, raw: &Raw) -> Result<T> {
    serde_json::from_slice::<T>(&raw.body)
        .map_err(|e| Error::Decode(format!("{}: cannot decode the response: {e}", op.as_str())))
}

/// A random UUID v4.
pub(crate) fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}
