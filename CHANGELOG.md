# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/). Versions stay 0.x until the API ships request signing.

## [Unreleased]

### Added
- `Balance::held_incoming` (`Vec<HeldIncoming>`: `transfer_id`, `amount`, `available_at`): incoming
  internal transfers still held, at most 100, soonest first. Their sum is already included in
  `locked`: never add it again. It decodes as an empty `Vec` when a server omits the field.

## [0.1.0-dev.3] (2026-09-28)

Synced with the API's H-1 release.

### Added
- `LedgerEntry.reference` is typed: `LedgerReference`, an enum with one variant per cause (`Deposit`,
  `Withdrawal`, `Order`, `Trade`, `Transfer`, `Adjustment`, `Pool`, `FuturesTransfer`, `System`) and
  `kind()`. A cause this version doesn't know, or a malformed one, decodes as `Unknown(raw JSON)`, and
  a missing reference as `Unknown(null)`, so decoding never fails on it. It used to be a raw
  `serde_json::Value`.
- Id aliases `OrderId`, `TradeId`, `DepositId`, `WithdrawalId`, `PoolId`, `UserId`,
  `FuturesTransferId`: plain `String`s, deliberately not validated.
- Error code `PRICE_UNAVAILABLE` (422, `Unprocessable`); withdrawal status `reverted`; new ledger entry
  kinds for held transfers, releases, reversals and withdrawal refunds.

### Changed
- `JoinPoolRequest.max_ratio_deviation_percent` is an `Amount` (it was a `String`), checked before
  sending like the other amounts.
- Docs: cancel-all also cancels stop orders that have not triggered yet (`pending_trigger`).

### Changed (CI)
- The `ci/consumer` check now makes one real request, without internet, through the normal
  (non-test) client to a local TLS server that only speaks HTTP/2 (ALPN h2). A client that can't
  do HTTP/2 over TLS, as in 0.1.0-dev.1, now fails CI, not just the feature check. It trusts the
  server's self-signed certificate only when built with `RUSTFLAGS="--cfg cexy_test_extra_root"`
  and given `CEXY_TEST_EXTRA_ROOT_PEM`. That's a rustc cfg, not a Cargo feature, so no dependency
  can switch it on. A second run checks that a normal build ignores the variable and rejects the
  server.

## [0.1.0-dev.2] (2026-09-27)

### Fixed
- **Every REST call panicked in a normal project** ("http2 feature is not enabled"). The client
  advertises HTTP/2 over TLS (ALPN `h2`), and api.cexy.io selects it, but reqwest was built without
  its `http2` feature. The SDK's own tests passed only because a dev-dependency enabled the
  feature. reqwest's `http2` feature is now enabled, and CI builds and runs a separate consumer crate
  (`ci/consumer`) with no dev-dependencies, plus a check that hyper's `http2` feature is on in a
  normal build. **0.1.0-dev.1 is unusable against the live API; use 0.1.0-dev.2.**

## [0.1.0-dev.1] (2026-09-27)

First pre-release. Built from `openapi.sdk.json` (spec `info.version` 1.0.0), including cancel-all v2.

### Added
- `Client` covering the 40 operations of the SDK surface: public market data, account, exports, wallet
  reads, trading and liquidity pools. Async on tokio, reqwest and rustls.
- Models generated from the spec by `tools/generate.py` (checked in CI). Amounts are validated decimal
  strings (`Amount`), never floats; enums are open (`Other(String)`).
- Errors mapped from the API envelope into `ApiError` with an `ErrorCategory`
  (`JurisdictionBlocked` also matches `Forbidden`); secrets are redacted from messages, details and fields.
- Retries with exponential backoff and full jitter, `Retry-After` handling, Idempotency-Key (only on
  pool join and exit, reused across retries), duplicate-safe `place_order` (lookup by `client_order_id` after an ambiguous
  failure) and `cancel_order` (`INVALID_STATE` on a retry means already cancelled).
- `cancel_all` / `cancel_all_markets` (one request, with `already_closed`, `failures` and `has_more`) and
  `cancel_all_until_done` (repeats while orders remain or are still being placed; each round is one
  HTTP request, so at most 20 requests; backoff 1-2-4-8-15 s after rounds without progress or with a
  retryable error, a 429 waits its Retry-After; never waits past the 120 s budget; rounds merged by
  order id).
- Client-side rate limiter (100/min anonymous, 300/min with a key) that adapts to `X-RateLimit-*`.
- Cursor pagination as streams (`all_…`).
- `WebSocket`: heartbeat, liveness, local limits, reconnect with re-auth and re-subscribe, events on a
  channel, and `LiveOrderBook` implementing the order-book sync rules.
- `Authenticator` trait so request signing can be added without breaking callers.
- `MAX_SERVER_WAIT` (120 s): server wait hints (`Retry-After`, `retry_after_seconds`,
  `X-RateLimit-*`) are parsed safely and never waited longer; a longer Retry-After fails the call at
  once with the rate-limit error.

### Security
- HTTP redirects are never followed; a 3xx is an error with code `UNEXPECTED_REDIRECT`, never retried.
- Credentials are attached only to requests for the base URL's origin, and are redacted from error
  messages, details (at any depth, keys included), fields and request ids.
- One TLS configuration for REST and WebSocket: rustls with the `ring` provider and the
  `webpki-roots` Mozilla root store. No OpenSSL, no C toolchain needed to build.
- WebSocket URLs with credentials (userinfo), a query string or a fragment are rejected.
- Only `https://` and `wss://`; `allow_insecure` permits plain text for loopback hosts only.
