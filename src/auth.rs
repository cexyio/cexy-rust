//! Credentials: the [`Authenticator`] trait and today's API-key scheme.

use std::fmt;

use url::Url;

use crate::error::{Error, Result};

pub(crate) const REDACTED: &str = "[REDACTED]";

/// One request an [`Authenticator`] adds credentials to.
pub struct AuthRequest<'a> {
    /// HTTP method, upper case.
    pub method: &'a str,
    /// The full URL (never add credentials to it).
    pub url: &'a Url,
    /// The exact JSON body that will be sent, if any.
    pub body: Option<&'a [u8]>,
    pub(crate) headers: Vec<(String, String)>,
}

impl AuthRequest<'_> {
    /// Sets a request header.
    pub fn set_header(&mut self, name: impl Into<String>, value: impl Into<String>) {
        self.headers.push((name.into(), value.into()));
    }
}

/// Adds credentials to requests for operations that need an API key.
///
/// `authenticate` is called once per attempt (retries included), so a future signing scheme can
/// put a fresh timestamp and nonce on each attempt. It is never called for public operations.
pub trait Authenticator: Send + Sync + 'static {
    /// A short, non-secret description such as `"api-key"`.
    fn kind(&self) -> &str;
    /// Adds the credentials to the request.
    fn authenticate(&self, request: &mut AuthRequest<'_>) -> Result<()>;
    /// Removes any secret material from text (used on error messages).
    fn redact(&self, text: &str) -> String;

    /// For request-signing schemes: adopt the server clock after `SIGNATURE_EXPIRED`
    /// (`details.server_time_ms`). `Some(true)`: the request is signed again and resent once;
    /// `Some(false)`: the local clock is too far off (a clock error is returned). `None` (the
    /// default): this scheme does not sign, and the error is returned as is.
    fn adjust_clock(&self, _server_time_ms: i64) -> Option<bool> {
        None
    }

    /// Signs a WebSocket `auth_key` challenge: `(key_id, signature)`. `None` (the default): this
    /// scheme cannot authenticate a WebSocket.
    fn sign_websocket_challenge(
        &self,
        _connection_id: &str,
        _challenge: &str,
    ) -> Option<(String, String)> {
        None
    }
}

/// Today's scheme: `X-API-Key` and `X-API-Secret` headers on every private request. HMAC request
/// signing ([`crate::HmacAuthenticator`], planned) is the other scheme.
///
/// Its `Debug` and `Display` never reveal the secret.
#[derive(Clone)]
pub struct ApiKeyAuthenticator {
    key: String,
    secret: String,
}

impl ApiKeyAuthenticator {
    /// Checks the pair and returns the authenticator.
    pub fn new(api_key: impl Into<String>, api_secret: impl Into<String>) -> Result<Self> {
        let (key, secret) = (api_key.into(), api_secret.into());
        if key.trim().is_empty() {
            return Err(Error::config("api_key must be a non-empty string"));
        }
        if secret.trim().is_empty() {
            return Err(Error::config("api_secret must be a non-empty string"));
        }
        if [&key, &secret].iter().any(|s| s.contains(['\r', '\n'])) {
            return Err(Error::config(
                "api_key and api_secret must not contain line breaks",
            ));
        }
        Ok(ApiKeyAuthenticator { key, secret })
    }

    /// A non-secret hint for logs: the first characters of the key id.
    pub fn key_hint(&self) -> String {
        let head: String = self.key.chars().take(6).collect();
        format!("{head}…")
    }
}

impl Authenticator for ApiKeyAuthenticator {
    fn kind(&self) -> &str {
        "api-key"
    }

    fn authenticate(&self, request: &mut AuthRequest<'_>) -> Result<()> {
        request.set_header("X-API-Key", self.key.clone());
        request.set_header("X-API-Secret", self.secret.clone());
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
}

impl fmt::Display for ApiKeyAuthenticator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ApiKeyAuthenticator({}, secret={REDACTED})",
            self.key_hint()
        )
    }
}

impl fmt::Debug for ApiKeyAuthenticator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
