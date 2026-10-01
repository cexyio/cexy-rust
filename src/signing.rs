//! HMAC request signing (`CEXY-HMAC-SHA256-v1`): the default ([`AuthScheme::Hmac`](crate::AuthScheme)).
//! The API refuses the old secret header with `SIGNATURE_REQUIRED`.
//!
//! Canonical request: 7 lines joined by `\n` (no trailing newline): the scheme, the method, the
//! canonical path, the canonical query, the timestamp (unix ms), the nonce and the hex SHA-256 of
//! the exact body bytes. Signature: lowercase hex HMAC-SHA256 keyed with the UTF-8 bytes of the
//! secret string as issued (never decoded).

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ring::rand::SecureRandom;

use crate::auth::{ApiKeyAuthenticator, AuthRequest, Authenticator, REDACTED};
use crate::error::Result;

/// The signing scheme, the first line of every canonical request.
pub const SIGNING_SCHEME: &str = "CEXY-HMAC-SHA256-v1";

/// The furthest the client clock may be corrected after `SIGNATURE_EXPIRED`; beyond it the
/// request fails with a clock error.
pub const MAX_CLOCK_OFFSET: Duration = Duration::from_secs(60 * 60);

fn is_unreserved(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b"-._~".contains(&b)
}

/// RFC 3986 encoding of raw bytes: unreserved kept, everything else `%XX` with uppercase hex.
pub(crate) fn encode_bytes(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len());
    for &b in bytes {
        if is_unreserved(b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Percent-decodes to raw bytes (a `%` not followed by two hex digits is kept literally).
fn percent_decode(s: &str) -> Vec<u8> {
    let raw = s.as_bytes();
    let mut out = Vec::with_capacity(raw.len());
    let mut i = 0;
    while i < raw.len() {
        if raw[i] == b'%'
            && i + 2 < raw.len()
            && raw[i + 1].is_ascii_hexdigit()
            && raw[i + 2].is_ascii_hexdigit()
        {
            let hex = std::str::from_utf8(&raw[i + 1..i + 3]).expect("ascii");
            out.push(u8::from_str_radix(hex, 16).expect("hex"));
            i += 3;
            continue;
        }
        out.push(raw[i]);
        i += 1;
    }
    out
}

/// Canonical path: split on `/` BEFORE decoding; each segment decoded, then re-encoded.
pub(crate) fn canonical_path(path: &str) -> String {
    path.split('/')
        .map(|seg| encode_bytes(&percent_decode(seg)))
        .collect::<Vec<_>>()
        .join("/")
}

/// Canonical query: split on `&` (empty parts dropped); decode and re-encode names and values;
/// sort bytewise. `query` is everything after the FIRST `?` of the request target, so a further
/// `?` is data.
pub(crate) fn canonical_query(query: &str) -> String {
    let mut pairs: Vec<(String, String)> = query
        .split('&')
        .filter(|part| !part.is_empty()) // "a=1&&b=2" is "a=1&b=2"
        .map(|part| {
            let (name, value) = part.split_once('=').unwrap_or((part, ""));
            (
                encode_bytes(&percent_decode(name)),
                encode_bytes(&percent_decode(value)),
            )
        })
        .collect();
    pairs.sort();
    pairs
        .into_iter()
        .map(|(n, v)| format!("{n}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub(crate) fn sha256_hex(data: &[u8]) -> String {
    hex(ring::digest::digest(&ring::digest::SHA256, data).as_ref())
}

pub(crate) fn hmac_hex(secret: &str, message: &str) -> String {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret.as_bytes());
    hex(ring::hmac::sign(&key, message.as_bytes()).as_ref())
}

/// The canonical request for `method`, the path and query as sent, and the exact body.
pub(crate) fn canonical_request(
    method: &str,
    path: &str,
    query: &str,
    timestamp: &str,
    nonce: &str,
    body: &[u8],
) -> String {
    [
        SIGNING_SCHEME,
        &method.to_uppercase(),
        &canonical_path(path),
        &canonical_query(query),
        timestamp,
        nonce,
        &sha256_hex(body),
    ]
    .join("\n")
}

/// The query string the SDK sends: RFC 3986 names and values in call order, joined with `&`.
pub(crate) fn encode_query(pairs: &[(&str, String)]) -> String {
    pairs
        .iter()
        .map(|(k, v)| {
            format!(
                "{}={}",
                encode_bytes(k.as_bytes()),
                encode_bytes(v.as_bytes())
            )
        })
        .collect::<Vec<_>>()
        .join("&")
}

/// A 16-byte CSPRNG nonce, base64url without padding (22 characters).
pub(crate) fn new_nonce() -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut b = [0u8; 16];
    ring::rand::SystemRandom::new()
        .fill(&mut b)
        .expect("the system random number generator failed");
    let mut out = String::with_capacity(22);
    for chunk in b.chunks(3) {
        let n = chunk
            .iter()
            .enumerate()
            .fold(0u32, |acc, (i, &x)| acc | (u32::from(x) << (16 - 8 * i)));
        let chars = chunk.len() + 1;
        for i in 0..chars {
            out.push(ALPHABET[((n >> (18 - 6 * i)) & 63) as usize] as char);
        }
    }
    out
}

type NowMs = Arc<dyn Fn() -> i64 + Send + Sync>;
type Nonce = Arc<dyn Fn() -> String + Send + Sync>;

fn system_now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Signs every private request (see [`SIGNING_SCHEME`]). The secret never leaves
/// the process: only `X-API-Key`, `X-API-Timestamp`, `X-API-Nonce` and `X-API-Signature` are sent.
/// Called once per attempt, so every retry has a fresh timestamp and nonce.
///
/// Its `Debug` and `Display` never reveal the secret.
#[derive(Clone)]
pub struct HmacAuthenticator {
    key: String,
    secret: String,
    offset_ms: Arc<AtomicI64>,
    now: NowMs,
    nonce: Nonce,
}

impl HmacAuthenticator {
    /// Checks the pair and returns the authenticator.
    pub fn new(api_key: impl Into<String>, api_secret: impl Into<String>) -> Result<Self> {
        let (key, secret) = (api_key.into(), api_secret.into());
        ApiKeyAuthenticator::new(key.clone(), secret.clone())?;
        Ok(HmacAuthenticator {
            key,
            secret,
            offset_ms: Arc::new(AtomicI64::new(0)),
            now: Arc::new(system_now_ms),
            nonce: Arc::new(new_nonce),
        })
    }

    #[cfg(test)]
    pub(crate) fn with_sources(mut self, now: NowMs, nonce: Option<Nonce>) -> Self {
        self.now = now;
        if let Some(n) = nonce {
            self.nonce = n;
        }
        self
    }

    /// The correction applied to the local clock after `SIGNATURE_EXPIRED`, in milliseconds
    /// (positive: the local clock is behind the server's). For diagnostics.
    pub fn clock_offset_ms(&self) -> i64 {
        self.offset_ms.load(Ordering::SeqCst)
    }

    /// A non-secret hint for logs: the first characters of the key id.
    pub fn key_hint(&self) -> String {
        let head: String = self.key.chars().take(6).collect();
        format!("{head}…")
    }
}

impl Authenticator for HmacAuthenticator {
    fn kind(&self) -> &str {
        "hmac"
    }

    /// Signs what is on the wire: the serialized path and query of the URL (exactly the request
    /// line sent) and the exact body bytes.
    fn authenticate(&self, request: &mut AuthRequest<'_>) -> Result<()> {
        let ts = ((self.now)() + self.offset_ms.load(Ordering::SeqCst)).to_string();
        let nonce = (self.nonce)();
        let canonical = canonical_request(
            request.method,
            request.url.path(),
            request.url.query().unwrap_or(""),
            &ts,
            &nonce,
            request.body.unwrap_or(&[]),
        );
        request.set_header("X-API-Key", self.key.clone());
        request.set_header("X-API-Timestamp", ts);
        request.set_header("X-API-Nonce", nonce);
        request.set_header("X-API-Signature", hmac_hex(&self.secret, &canonical));
        Ok(())
    }

    fn redact(&self, text: &str) -> String {
        let mut out = text.to_string();
        for s in [&self.secret, &self.key] {
            if !s.is_empty() {
                out = out.replace(s.as_str(), REDACTED);
            }
        }
        out
    }

    fn adjust_clock(&self, server_time_ms: i64) -> Option<bool> {
        let offset = server_time_ms - (self.now)();
        if offset.unsigned_abs() > MAX_CLOCK_OFFSET.as_millis() as u64 {
            return Some(false);
        }
        self.offset_ms.store(offset, Ordering::SeqCst);
        Some(true)
    }

    fn sign_websocket_challenge(
        &self,
        connection_id: &str,
        challenge: &str,
    ) -> Option<(String, String)> {
        Some((
            self.key.clone(),
            hmac_hex(
                &self.secret,
                &format!("CEXY-WS-AUTH-v1\n{connection_id}\n{challenge}"),
            ),
        ))
    }
}

impl fmt::Display for HmacAuthenticator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "HmacAuthenticator({}, secret={REDACTED})",
            self.key_hint()
        )
    }
}

impl fmt::Debug for HmacAuthenticator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
