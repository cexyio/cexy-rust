# cexy (Rust)

The official Rust SDK for the [CEXY.io](https://cexy.io) REST and WebSocket API.

- Typed models generated from the public OpenAPI spec ([cexy-api-spec](https://github.com/cexyio/cexy-api-spec)).
- Safe by default: retries with backoff, order placement designed to avoid duplicate orders (via `client_order_id`), a client-side rate limiter, and no HTTP redirects followed.
- A WebSocket client with heartbeat, reconnect and a live order book that applies the sync rules for you.
- Async on [tokio](https://tokio.rs), with [reqwest](https://docs.rs/reqwest) and [tokio-tungstenite](https://docs.rs/tokio-tungstenite) over rustls (the `ring` provider and the `webpki-roots` Mozilla root store, for REST and WebSocket alike). No OpenSSL, no system trust store, no C toolchain needed to build.
- Rust 1.94 or newer.

> **Status: 0.x, pre-release.** The API is not yet frozen. It stays 0.x until the exchange ships HMAC
> request signing, which will change how credentials are sent.

## Install

```toml
[dependencies]
cexy = "=0.1.0-dev.6"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Cargo never picks a pre-release (a version with `-dev.N`) on its own: name it explicitly. The
exact requirement above (`=0.1.0-dev.6`) is the safest way; move it by hand for each new pre-release.

## Quick start: public data

```rust,no_run
use cexy::{CandleInterval, Client, ClientOptions, GetCandlesParams, GetOrderBookParams};

#[tokio::main]
async fn main() -> cexy::Result<()> {
    let c = Client::new(ClientOptions::default())?; // no key needed for market data

    let markets = c.markets().list().await?;
    let btc = c.markets().get("BTC/USDT").await?;
    let book = c.markets().order_book("BTC/USDT", Some(&GetOrderBookParams { depth: Some(10) })).await?;
    let mut p = GetCandlesParams::new(CandleInterval::H1);
    p.limit = Some(24);
    let candles = c.markets().candles("BTC/USDT", &p).await?;
    let now = c.time().await?;
    println!("{} markets; {} at sequence {}; {} candles; server time {}", markets.len(), btc.symbol, book.sequence, candles.len(), now.iso);
    Ok(())
}
```

## Quick start: your account

```rust
use cexy::{Amount, Client, ClientOptions, ListOpenOrdersParams, OrderSide, OrderType, PlaceOrderRequest};

# async fn run() -> cexy::Result<()> {
let c = Client::new(ClientOptions::with_api_key(
    std::env::var("CEXY_API_KEY").unwrap(),    // ak_your_key_here
    std::env::var("CEXY_API_SECRET").unwrap(), // your_secret_here
))?;

let balances = c.account().balances().await?;
// A sub-account's balances (parent account only; same shape, incl. held_incoming):
let sub_balances = c.account().sub_account_balances("sub-account-id").await?;
let open = c.trading().open_orders(Some(&ListOpenOrdersParams { symbol: Some("BTC/USDT".into()), ..Default::default() })).await?;

let mut order = PlaceOrderRequest::new("BTC/USDT", OrderSide::Buy, OrderType::Limit);
order.price = Some(Amount::new("60000.00")?);
order.quantity = Some(Amount::new("0.001")?);
order.client_order_id = Some("my-order-1".into()); // optional: a UUID is generated otherwise
let placed = c.trading().place_order(&order).await?;
println!("{} {:?} (recovered: {})", placed.response.order.id, placed.response.order.status, placed.recovered);

c.trading().cancel_order(&placed.response.order.id).await?;
c.trading().cancel_all("BTC/USDT").await?; // one market; every market needs cancel_all_markets()
# Ok(()) }
```

Placing and cancelling need a key with the **trade** scope; everything else needs **read**.

### Cancel-all

Cancel-all also cancels stop orders that have not triggered yet (`pending_trigger`) and releases their
reservations. One `cancel_all` call handles at most 500 orders. Every order it handled is in exactly one of
`cancelled`, `already_closed` (it closed on its own first: not a failure) and `failed` (the reason is
in `failures`; `INVALID_STATE` means the order was still being placed). `has_more` means there are
more. An unknown symbol is a `NotFound` error, and the server allows 30 cancel-all calls a minute.

`cancel_all_until_done` repeats the call until nothing is left: while `has_more` is set or orders are
still being placed. Each round is exactly one HTTP request (the loop does its own retrying, so it
never sends more than `max_rounds` requests). It backs off 1, 2, 4, 8, then 15 s after rounds without
progress or with a retryable error (a 503, a network failure), waits a 429's `Retry-After` exactly,
stops after `max_rounds` (default 20) or before a wait would reach `time_budget` (default 120 s; that
wait is not taken, and `last_error_code` says why), and merges the rounds by order id. A
non-retryable error returns `Error::CancelAllInterrupted` with the summary so far. The target is
explicit:

```rust
# async fn run(c: cexy::Client) -> cexy::Result<()> {
let s = c.trading().cancel_all_until_done(&cexy::CancelAllOptions::symbol("BTC/USDT")).await?;
println!("{} cancelled, {} still failing, stopped: {}", s.cancelled.len(), s.failed.len(), s.stopped.as_str());
// every market: cexy::CancelAllOptions::all_markets()
# Ok(()) }
```

## Held incoming transfers

`Balance::held_incoming` lists incoming internal transfers still held, each a `HeldIncoming` with
`transfer_id`, `amount` and `available_at`. Their sum is **already included in `locked`**, so never
add it to `locked` or `total` again. There are at most 100 entries, soonest `available_at` first
(millisecond precision), with no sender identity. An entry disappears once the transfer is
released (the amount moves to `available`) or cancelled by the exchange. It is always a `Vec`
(empty when none, including from servers that predate the field).

## Amounts

Every amount is an exact decimal string (`Amount::new("0.00150000")?`), in responses and requests.
Floats cannot hold most decimals exactly. `Amount::new` accepts only plain decimals (no exponents, no
spaces), and the SDK checks every amount field again before sending (`Error::InvalidAmount`). Do
arithmetic with a decimal crate such as `rust_decimal`:

```rust,ignore
# fn f(price: &cexy::Amount, qty: &cexy::Amount) {
use std::str::FromStr;
let total = rust_decimal::Decimal::from_str(price.as_str()).unwrap() * rust_decimal::Decimal::from_str(qty.as_str()).unwrap();
# }
```

## Errors

Every API failure is `Error::Api(ApiError)` with `status`, `code`, `message`, `details`, `fields`,
`request_id`, `retryable` and `retry_after`. Branch on `code`, never on `message`. Match the category
with `err.is(ErrorCategory::…)`:

| Category | When |
|---|---|
| `Authentication` | 401: missing or invalid credentials |
| `Forbidden` | 403: the key lacks a scope (`FORBIDDEN`), or the route is session-only (`API_KEY_NOT_ALLOWED`); also matches 451 |
| `JurisdictionBlocked` | 451: `JURISDICTION_BLOCKED` (not available in the caller's jurisdiction) |
| `Validation` | 400: see `fields` |
| `NotFound` | 404 |
| `Conflict` | 409: `ALREADY_EXISTS`, `IDEMPOTENCY_KEY_CONFLICT`, `CONCURRENT_MODIFICATION` |
| `Unprocessable` | 422: `INSUFFICIENT_FUNDS`, `MARKET_UNAVAILABLE`, ... |
| `RateLimited` | 429, with `retry_after` |
| `Server` | 5xx |
| `UnexpectedRedirect` | 3xx: code `UNEXPECTED_REDIRECT`, set by the SDK (see Security) |
| (none) | a code this SDK version does not know yet: check `code` |

Local problems are `Error::Config`, `Error::InvalidAmount`, `Error::Connection` (with `timeout` for
the per-attempt timeout), `Error::OrderStateUnknown` and `Error::CancelAllInterrupted`. `ErrorCode`
is an open enum: codes added to the API later arrive as `ErrorCode::Other(String)`, so keep a
wildcard arm.

```rust
# async fn run(c: cexy::Client, order: cexy::PlaceOrderRequest) -> cexy::Result<()> {
use cexy::{ErrorCategory, ErrorCode};
match c.trading().place_order(&order).await {
    Ok(r) => println!("placed {}", r.response.order.id),
    Err(e) if e.api().is_some_and(|a| a.code == ErrorCode::InsufficientFunds) => {
        println!("not enough funds: {:?}", e.api().unwrap().details)
    }
    Err(e) if e.is(ErrorCategory::RateLimited) => println!("slow down for {:?}", e.api().unwrap().retry_after),
    Err(e) => return Err(e),
}
# Ok(()) }
```

## Retries and idempotency

- Timeout per attempt: `ClientOptions::timeout` (default 10 s). Retries: `max_retries` (default 3; `Some(0)` turns them off), exponential backoff with full jitter, capped at 10 s.
- Retried: connection errors, timeouts and responses with `retryable: true` (and 409
  `CONCURRENT_MODIFICATION`). A 4xx is never retried except 429 and 409 `CONCURRENT_MODIFICATION`,
  whatever its body says.
- A 429 waits at least `Retry-After` / `details.retry_after_seconds`.
- GETs retry freely.
- **Orders:** safety rests on `client_order_id`, not on `Idempotency-Key` (the server does not honour
  that header on orders, order cancels or cancel-all, so the SDK sends it only on pool join and exit). `place_order` always sends a `client_order_id`
  (a UUID if you do not set one); it is unique per account and a repeat is refused before any funds
  move. After an ambiguous failure (connection error, timeout, 5xx) the SDK first looks the order up by
  that id. If the order exists it is returned with `recovered: true`; only if it does not exist is it
  sent again, with the same id. If even the lookup fails, or the order was accepted but its response
  could not be decoded, you get `Error::OrderStateUnknown` with the `client_order_id`: check
  `order_by_client_id` before placing the order again.
- **Cancels:** `cancel_order` retries connection errors; if a *retry* gets `INVALID_STATE`, the first
  attempt most likely cancelled the order, so the SDK fetches and returns it. Check the returned
  `status`: it is usually `cancelled`, but can be `filled` if the order traded before the cancel
  landed. Cancel-all is naturally repeatable and is retried the same way (a retry reports only what it
  cancelled); it sends no `Idempotency-Key`. `cancel_all_until_done` does its own retrying instead
  (one request per round, see above).
- **Server waits are bounded.** A `Retry-After` (or `details.retry_after_seconds`) up to 120 s
  (`MAX_SERVER_WAIT`) is honoured; a longer one is not waited: the call fails at once with the
  rate-limit error, whose `retry_after` still says what the server asked. Unreadable values are
  ignored, and `X-RateLimit-*` headers never make the client-side limiter wait longer than 120 s.
- **Pool join and exit** send a generated `Idempotency-Key`, reused on every retry; the server honours
  it there, so they execute once. A 409 `CONCURRENT_MODIFICATION` (the same key still in flight) is
  retried with the same key. Set it yourself with `CallOptions::idempotency_key`.
- `ClientOptions::on_retry` lets you log retries.

Per-call overrides: `client.with_options(CallOptions { timeout: Some(..), max_retries: Some(..), idempotency_key: Some(..) })`
returns a client that shares everything else with the original.

## Pagination

Histories use opaque cursors. Each listing returns one `Page` (`items`, `has_more`, `next_cursor`), and
an `all_…` method returns a stream that fetches pages lazily:

```rust
# async fn run(c: cexy::Client) -> cexy::Result<()> {
use futures_util::StreamExt;
let params = cexy::OrderHistoryParams { symbol: Some("BTC/USDT".into()), ..Default::default() };
let mut orders = c.trading().all_order_history(Some(&params), Some(500)); // at most 500 items
while let Some(order) = orders.next().await {
    let order = order?;
    println!("{} {:?}", order.id, order.status);
}
# Ok(()) }
```

## Rate limits

The client has a token-bucket limiter: **100 requests a minute without a key** (the server allows
120 a minute per IP for anonymous calls) and **300 a minute with an API key** (the server allows 600 a
minute per key). Cancel-all is limited separately to 30 a minute per account. It adapts downwards to
`X-RateLimit-Limit` / `X-RateLimit-Remaining` / `X-RateLimit-Reset` (seconds until the window resets),
and pauses after a 429. Change it with `requests_per_minute`, or turn it off with `disable_rate_limit`.
The limiter belongs to one `Client`; clones share it, so clone the client rather than creating new ones.

## WebSocket

```rust
# async fn run(c: cexy::Client) -> cexy::Result<()> {
use cexy::{BookEvent, WsEvent, WsOptions};
let ws = c.websocket(WsOptions::default())?; // wss://api.cexy.io/api/v1/ws
let mut events = ws.events().expect("taken once");
ws.connect().await?;
ws.subscribe(&["ticker:BTC/USDT", "trades:BTC/USDT"]).await?;
let book = ws.order_book("BTC/USDT").await?;

while let Some(ev) = events.recv().await {
    match ev {
        WsEvent::Event(e) if e.r#type == "ticker.update" => println!("{} {}", e.channel, e.data),
        WsEvent::Book(BookEvent::Updated(b)) => {
            let (bid, ask) = b.best();
            println!("{:?} {:?} stale={}", bid.map(|l| &l.price), ask.map(|l| &l.price), b.stale);
        }
        WsEvent::Reconnected(_) => println!("reconnected and re-subscribed"),
        WsEvent::Resync(_) => { /* refetch anything you derive from events */ }
        _ => {}
    }
}
# let _ = book; Ok(()) }
```

Everything the client reports arrives, in order, on the event channel from `ws.events()`. Read it
continuously: when its buffer (`event_buffer`, default 10 000) is full, new events are dropped and
counted in `ws.dropped_events()`. Live order books keep updating either way (`book.snapshot()`).

What the client does for you:

- Sends `{"op":"ping"}` every 30 s (required: the server closes idle connections) and accepts the server's
  unsolicited pongs. No frame from the server for 75 s (`liveness_timeout`; the client's own pings do not
  count) means a dead connection and a reconnect.
- Reconnects with exponential backoff and full jitter, then re-authenticates and re-subscribes everything.
- Correlates every request with its acknowledgement by `id`: `auth` returns on `authenticated`
  (and fails on an `error` with its id, or on timeout), `subscribe` on `subscribed`, `unsubscribe` on
  `unsubscribed`, `ping` on `pong`.
- Guards locally: at most 100 subscriptions (extras are returned in `refused`) and 200 messages a minute.
- Warns once (`WsEvent::Warning`) if the server speaks another `protocol_version`, and ignores unknown event types.

**Order-book rules** (applied by `ws.order_book`; follow them if you build your own):

1. Subscribe to `orderbook:{symbol}` first, then take the REST snapshot (its `sequence` is S).
2. Drop updates with `sequence <= S`.
3. Every update carries the complete top 50 of both sides (`"full": true`) and replaces the previous state.
   There are no deltas. Never merge REST levels deeper than 50 into WebSocket state.
4. A sequence gap marks the book stale until the next update, which heals it. There is no forced resync.
5. Sequences reset when the server restarts: take a fresh snapshot after every reconnect.
6. An `error` frame `CONCURRENT_MODIFICATION` with a null `id` means messages were dropped: resync every
   book and channel (the client emits `WsEvent::Resync`).

While a snapshot is missing (for example, the REST call keeps failing), `LiveOrderBook` keeps at most
the newest 256 updates to replay; older ones are dropped (`book.dropped_updates()`, with one
`WsEvent::Warning`). Nothing is lost, since every update is a complete top 50.

**Private channels** (`orders`, `balances`, `deposits`, `withdrawals`, `account`) need
`ws.auth(token)` with a session access token (it returns the `user_id` from `authenticated`).
**API-key authentication on the WebSocket is not available yet**: with an API key, use public channels
and poll REST for private state. If the session is revoked, the client emits `WsEvent::AuthLost`;
public channels keep working.

The server ends private subscriptions, without any frame, when `auth` succeeds as another user,
when an `auth` fails (any error signs the connection out), or when this connection's own session
is revoked (`session.revoked` with `current: true`). The client emits `WsEvent::AuthChanged`
(`AuthChange`: `reason` `UserChanged`, `AuthFailed` or `SessionRevoked`, plus the `dropped`
channels) and re-subscribes those channels itself: at once for another user, after the next
successful `auth` otherwise, followed by `WsEvent::Resync(ResyncReason::Reauth)` (refetch private
state).

The server can also sign a connection out by itself: `signed_out` (a planned server frame; this
SDK already handles it). Reason `expired` gives `WsEvent::AuthChanged` with `TokenExpired`, reason
`revoked` gives `SessionRevoked` plus `WsEvent::AuthLost`, and any other reason gives `SignedOut`
with the raw value in `code`. Call `auth` again with the fresh token on every token refresh; that
keeps the private subscriptions.

**Missed private events.** Every private frame carries a per-connection `sequence`. When numbers are
skipped (after a short reorder window, `WsOptions::reorder_window`, default 250 ms), the client emits
`WsEvent::SequenceGap` and `WsEvent::Resync(ResyncReason::SequenceGap)`: refetch that channel's
state over REST. `balances.resync`, `deposits.resync` and `withdrawals.resync` (the last two
planned) emit `Resync` with `BalancesResync`, `DepositsResync` or `WithdrawalsResync`.

### Live balances

```rust,no_run
# async fn run(c: cexy::Client, token: &str) -> cexy::Result<()> {
let ws = c.websocket(cexy::WsOptions::default())?; // c has an API key
let mut events = ws.events().expect("events");
ws.connect().await?;
ws.auth(token).await?;
let lb = ws.live_balances(cexy::LiveBalancesOptions::default()).await?;
while let Some(ev) = events.recv().await {
    if let cexy::WsEvent::Balances(cexy::BalancesEvent::Updated { asset, balance }) = ev {
        println!("{asset}: {:?}", balance.map(|b| b.total));
    }
}
let _ = (lb.get("USDT"), lb.is_stale(), lb.last_error());
# Ok(()) }
```

`live_balances` subscribes `balances`, takes a REST snapshot and applies newer `balance.updated`
events (only when their `sequence` is greater than the one it holds; a total of 0 removes the row).
It refetches by itself on a missed event, `balances.resync`, `CONCURRENT_MODIFICATION`, a reconnect
or an account change, at most every `min_snapshot_interval` (default 2 s; `Duration::ZERO`: none),
and never because a balance's own sequence skipped values. Before every merge it checks that the
REST key's account (`account().id()`) is the WebSocket's authenticated user: otherwise nothing is
merged (`BalancesEvent::AccountMismatch`, and `last_error()` has code `ACCOUNT_MISMATCH`).

## Security

- API keys **cannot withdraw or transfer funds**, whatever their scopes.
- Use a **read-only** key unless you need to trade, and restrict keys to your IPs (`allowed_ips`).
- Credentials go only in the `X-API-Key` / `X-API-Secret` headers and only on private endpoints; never in URLs.
- The SDK **never follows HTTP redirects**, so credentials and orders are never re-sent to another URL.
  A 3xx response becomes an API error with code `UNEXPECTED_REDIRECT` (category `UnexpectedRedirect`);
  it is not retried. The WebSocket handshake does not follow redirects either.
- The SDK **always builds its own HTTP client**, and there is no option to pass your own
  `reqwest::Client`: redirects cannot be disabled on a client the SDK did not create, so a caller's
  client could leak the credentials. Use `ClientOptions` (timeout, retries, rate limit, User-Agent)
  to tune it.
- Only `https://` base URLs and `wss://` WebSocket URLs are accepted. `allow_insecure` permits
  `http://` / `ws://` solely for `localhost`, `127.0.0.1` or `::1` (local test servers).
- The SDK redacts the secret from `Debug`/`Display` output and error messages, including values the
  server echoes back in `ApiError::details` (at any depth, object keys included), `fields` and
  `request_id`.
- WebSocket URLs must not carry credentials (`user:pass@`), a query string or a fragment.
- Live tests (`tests/live.rs`: public REST reads and one unauthenticated WebSocket connection) run only
  with `CEXY_LIVE_TESTS=1`, never in CI.
- Keep keys in environment variables or a secret manager, not in code.

Report vulnerabilities as described in [SECURITY.md](SECURITY.md).

## Trading risk

- **Orders are real** and irreversible once filled. Test your code with a read-only key and small
  amounts first, and add your own limits (maximum order size, allowed markets) before automating trades.
- Retries and the `client_order_id` lookup are designed to avoid duplicate orders, but when a failure
  is ambiguous and even the lookup fails you get `Error::OrderStateUnknown`: check the order before
  placing it again.
- Markets move between reading data and placing an order; prices and balances you read may be stale.
- This SDK is provided under the MIT licence, without warranty. Nothing in it is investment advice.

## For tool builders

`OperationId::ALL` and `OperationId::info()` describe the 40 operations (method, path, auth and scope).
The crate also exports every model type, the error types, the `Authenticator` trait (HMAC signing will
plug in here) and `ClientOptions::user_agent_suffix` to identify your tool.

## Development

```bash
pip install pyyaml
python3 tools/generate.py          # regenerate src/models_gen.rs and src/operations_gen.rs from ../cexy-api-spec
python3 tools/generate.py --check  # CI: the generated files match the spec
cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test
CEXY_LIVE_TESTS=1 cargo test --test live   # optional: anonymous reads against api.cexy.io
```

Tests read the shared conformance cases from a `cexy-api-spec` checkout next to this repo
(override with `CEXY_API_SPEC`; `CEXY_REQUIRE_SPEC=1` fails instead of skipping when it is missing).

## License

MIT, see [LICENSE](LICENSE).
