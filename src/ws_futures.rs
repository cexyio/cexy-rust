//! Futures WebSocket channels: channel names (checked locally) and the data of their events.
//!
//! Public: `futures.mids`, `futures.orderbook:{coin}`, `futures.trades:{coin}`,
//! `futures.candles:{coin}:{interval}` and `futures.status`. Private: `futures.account` (held until
//! [`crate::WebSocket::auth`] or [`crate::WebSocket::auth_key`] succeeds). Public futures channels
//! send no snapshot on subscribe: seed from REST ([`crate::Futures`]).

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::amount::Amount;
use crate::error::{Error, Result};
use crate::models_gen::{FuturesCandle, FuturesPublicTrade, Level, OpenOrder, Positions};

/// The candle intervals of `futures.candles:{coin}:{interval}` (exact, case-sensitive).
pub const FUTURES_INTERVALS: [&str; 6] = ["1m", "5m", "15m", "1h", "4h", "1d"];

/// The private futures channel: the account's positions and open orders.
pub const FUTURES_ACCOUNT_CHANNEL: &str = "futures.account";

/// Futures channel names. A coin is sent exactly as given: it is case-sensitive (as
/// `GET /api/v1/futures/markets` lists it) and must be 1 to 20 ASCII letters or digits. A bad coin
/// or interval is an [`Error::Config`] and nothing is sent.
#[derive(Debug, Clone, Copy)]
pub struct FuturesChannel;

impl FuturesChannel {
    /// `futures.mids`: every coin's mid price (full set each time, `futures.mids` events).
    pub fn mids() -> String {
        "futures.mids".into()
    }

    /// `futures.orderbook:{coin}`: the complete book on every `futures.orderbook.update`.
    pub fn orderbook(coin: &str) -> Result<String> {
        check_coin(coin)?;
        Ok(format!("futures.orderbook:{coin}"))
    }

    /// `futures.trades:{coin}`: `futures.trades.new` events.
    pub fn trades(coin: &str) -> Result<String> {
        check_coin(coin)?;
        Ok(format!("futures.trades:{coin}"))
    }

    /// `futures.candles:{coin}:{interval}`: `futures.candle.update` events. `interval` is one of
    /// [`FUTURES_INTERVALS`].
    pub fn candles(coin: &str, interval: &str) -> Result<String> {
        check_coin(coin)?;
        check_interval(interval)?;
        Ok(format!("futures.candles:{coin}:{interval}"))
    }

    /// `futures.status`: `futures.status` events when the server's market-data link goes `live`
    /// or `degraded` (nothing on subscribe).
    pub fn status() -> String {
        "futures.status".into()
    }

    /// `futures.account` (private): `futures.positions` and `futures.orders`, in full, on
    /// subscribe and on change.
    pub fn account() -> String {
        FUTURES_ACCOUNT_CHANNEL.into()
    }
}

fn check_coin(coin: &str) -> Result<()> {
    if coin.is_empty() || coin.len() > 20 || !coin.bytes().all(|b| b.is_ascii_alphanumeric()) {
        return Err(Error::config(format!(
            "futures channel: {coin:?} is not a futures coin (1 to 20 ASCII letters or digits, as GET /api/v1/futures/markets lists it)"
        )));
    }
    Ok(())
}

fn check_interval(interval: &str) -> Result<()> {
    if !FUTURES_INTERVALS.contains(&interval) {
        return Err(Error::config(format!(
            "futures channel: interval {interval:?} must be one of {}",
            FUTURES_INTERVALS.join(", ")
        )));
    }
    Ok(())
}

/// Checks a `futures.*` channel name given to [`crate::WebSocket::subscribe`] like the helpers
/// do. Other names, and futures kinds this SDK does not know, pass unchanged.
pub(crate) fn check_channel(name: &str) -> Result<()> {
    let Some(rest) = name.strip_prefix("futures.") else {
        return Ok(());
    };
    let mut parts = rest.split(':');
    let kind = parts.next().unwrap_or("");
    let args: Vec<&str> = parts.collect();
    let bad = || {
        Err(Error::config(format!(
            "futures channel {name:?} is malformed"
        )))
    };
    match (kind, args.as_slice()) {
        ("mids" | "status" | "account", []) => Ok(()),
        ("mids" | "status" | "account", _) => bad(),
        ("orderbook" | "trades", [coin]) => check_coin(coin),
        ("orderbook" | "trades", _) => bad(),
        ("candles", [coin, interval]) => {
            check_coin(coin)?;
            check_interval(interval)
        }
        ("candles", _) => bad(),
        _ => Ok(()),
    }
}

/// The data of `futures.mids`: every coin's mid price, the full set each time (not a diff).
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct FuturesMids {
    /// Coin to mid price.
    #[serde(default)]
    pub mids: BTreeMap<String, Amount>,
    /// When the server published the frame.
    #[serde(default)]
    pub as_of: Option<DateTime<Utc>>,
}

/// The data of `futures.orderbook.update`: the complete book (`full` is always true), replacing
/// the previous one. Levels are `{price, size}` objects, best first.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct FuturesBookUpdate {
    /// Coin.
    #[serde(default)]
    pub coin: String,
    /// Always true: replace the local book.
    #[serde(default)]
    pub full: bool,
    /// Bids, best (highest) first.
    #[serde(default)]
    pub bids: Vec<Level>,
    /// Asks, best (lowest) first.
    #[serde(default)]
    pub asks: Vec<Level>,
    /// When the server relayed the book.
    #[serde(default)]
    pub as_of: Option<DateTime<Utc>>,
}

/// The data of `futures.trades.new`: one or more public trades.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct FuturesTradesUpdate {
    /// Coin.
    #[serde(default)]
    pub coin: String,
    /// In the order the provider sent them.
    #[serde(default)]
    pub trades: Vec<FuturesPublicTrade>,
}

/// The data of `futures.candle.update`: the current (possibly still open) candle.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct FuturesCandleUpdate {
    /// Coin.
    #[serde(default)]
    pub coin: String,
    /// Interval.
    #[serde(default)]
    pub interval: String,
    /// The candle; `open_time` tells an update of the current bar from a new bar.
    pub candle: FuturesCandle,
}

/// The data of `futures.status`, sent on a transition only.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct FuturesStatus {
    /// `live` or `degraded` (what is served may be out of date).
    #[serde(default)]
    pub state: String,
    /// When the server published the transition.
    #[serde(default)]
    pub since: Option<DateTime<Utc>>,
}

/// The data of `futures.positions` on `futures.account`: the full state.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct FuturesPositionsUpdate {
    /// The margin summary and the open positions.
    #[serde(default)]
    pub positions: Option<Positions>,
    /// When it was read from the provider.
    #[serde(default)]
    pub as_of: Option<DateTime<Utc>>,
    /// Served from an older read.
    #[serde(default)]
    pub stale: bool,
}

/// The data of `futures.orders` on `futures.account`: every open order, in full.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct FuturesOrdersUpdate {
    /// All open orders: replace the local list.
    #[serde(default)]
    pub orders: Vec<OpenOrder>,
    /// When it was read from the provider.
    #[serde(default)]
    pub as_of: Option<DateTime<Utc>>,
    /// Served from an older read.
    #[serde(default)]
    pub stale: bool,
}
