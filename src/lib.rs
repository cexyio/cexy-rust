//! The official Rust SDK for the [CEXY.io](https://cexy.io) exchange API: REST and WebSocket.
//!
//! - Typed models generated from the public OpenAPI spec (cexy-api-spec).
//! - Safe by default: retries with backoff, order placement designed to avoid duplicates (via
//!   `client_order_id`), a client-side rate limiter, and HTTP redirects are never followed.
//! - A WebSocket client with heartbeat, reconnect and a live order book.
//!
//! ```no_run
//! # async fn demo() -> Result<(), cexy::Error> {
//! let client = cexy::Client::new(cexy::ClientOptions::default())?;
//! let markets = client.markets().list().await?;
//! let now = client.time().await?;
//! println!("{} markets, server time {}", markets.len(), now.iso);
//! # Ok(()) }
//! ```
//!
//! Amounts are decimal strings ([`Amount`]), never floating point. API keys cannot withdraw or
//! transfer funds, whatever their scopes.

#[macro_use]
mod macros;

mod amount;
mod auth;
mod cancel_all;
mod client;
mod clock;
mod error;
mod limiter;
mod models_gen;
mod operations_gen;
mod orderbook;
mod pagination;
mod services;
mod tls;
mod transport;
mod ws;

pub use amount::{Amount, BookLevel, levels};
pub use auth::{ApiKeyAuthenticator, AuthRequest, Authenticator};
pub use cancel_all::{CancelAllOptions, CancelAllStop, CancelAllSummary, CancelAllTarget};
pub use client::{
    CallOptions, Client, ClientOptions, DEFAULT_BASE_URL, DEFAULT_RPM_ANONYMOUS,
    DEFAULT_RPM_WITH_KEY, OnRetry, is_local_host,
};
pub use error::{
    ApiError, ConnectionError, Error, ErrorCategory, MAX_SERVER_WAIT, Result, WsError,
};
pub use limiter::RateLimitState;
pub use models_gen::*;
pub use operations_gen::*;
pub use orderbook::{BookSnapshot, LiveOrderBook, WS_BOOK_DEPTH};
pub use pagination::{ItemStream, Page};
pub use services::{
    Account, Assets, Exports, Fees, Markets, Networks, PlaceOrderResult, Pools, Trading, Wallet,
};
pub use transport::RetryInfo;
pub use ws::{
    AuthResult, BookEvent, CloseInfo, DEFAULT_WEBSOCKET_URL, OrderBookUpdate, PRIVATE_CHANNELS,
    ResyncReason, SUPPORTED_PROTOCOL_VERSION, SessionRevoked, SubscribeResult, WebSocket, Welcome,
    WsEvent, WsEvents, WsFrame, WsOptions,
};

/// SDK version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Default User-Agent product token: `cexy-rust/<VERSION>`.
pub const USER_AGENT: &str = concat!("cexy-rust/", env!("CARGO_PKG_VERSION"));

#[cfg(test)]
mod tests;

/// The README's examples, compiled by `cargo test --doc`.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;
