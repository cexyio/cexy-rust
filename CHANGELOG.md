# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/). Versions stay 0.x until the API ships request signing.

## [Unreleased]

### Added
- Error codes from the live API: `KEY_NOT_SIGNABLE`, `SIGNATURE_EXPIRED`, `NONCE_REUSED` and
  `SIGNATURE_REQUIRED` (`ErrorCode` variants). `SIGNATURE_REQUIRED` is reserved: the API will return it
  (400, not retryable) once header mode is switched off; switch to `hmac` before then.
- Request signing, accepted by the API since 2026-10-01 (opt-in; the default is unchanged):
  `ClientOptions { auth: AuthScheme::Hmac, ..ClientOptions::with_api_key(key, secret) }` signs every
  private request (`CEXY-HMAC-SHA256-v1`: `X-API-Key`, `X-API-Timestamp`, `X-API-Nonce`,
  `X-API-Signature`) instead of sending `X-API-Secret`. Every attempt, retries included, is signed
  with a fresh timestamp and nonce. After `SIGNATURE_EXPIRED` the client adopts the server clock (at
  most 1 h away) and resends once. `KEY_NOT_SIGNABLE` (a key issued before signing) is an error
  that names the fix; there is no fallback to `X-API-Secret`. Checked against the spec's signing
  vectors and a raw test server that verifies every signature from the request line and body it
  received. `HmacAuthenticator`, `SIGNING_SCHEME` and `MAX_CLOCK_OFFSET` are exported; the
  `Authenticator` trait gains `adjust_clock` and `sign_websocket_challenge`, with defaults.
  `ring` (already used through rustls) is now a direct dependency for HMAC, SHA-256 and the nonce.
- `WebSocket::auth_key`: authenticates with the client's API key by signing the
  server's single-use challenge. It re-signs the new challenge after each reconnect, stops
  automatic key re-auth after a refused key, and reports `AuthChangeReason::KeyRevoked` /
  `KeyExpired` sign-outs. It needs a WebSocket from `Client::websocket` on an `AuthScheme::Hmac`
  client. `AuthResult::auth` says how the connection is authenticated; `Welcome::challenge` is the
  challenge. A signature that finishes after the connection changed is dropped (`STALE_CHALLENGE`);
  the new connection signs its own challenge.

### Changed
- Path values and query strings are encoded per RFC 3986 with uppercase hex (a space is `%20`,
  not `+`; `!*'()` in path values are encoded too). The server decodes both forms the same way; this
  makes the signed request exactly the sent one.
- Frames that arrive from a connection already replaced by a reconnect are ignored.
- `ClientOptions` has a new public field, `auth`, and `Welcome` and `AuthResult` have one each.
  Code that builds them with a full struct literal (without `..Default::default()` /
  `..ClientOptions::with_api_key(..)`) must set it.

### Fixed
- `LiveBalances`: events that arrived while the owner lookup was in flight are dropped when the
  lookup ends in `AccountMismatch` (they were kept until the next snapshot).
- CHANGELOG 0.1.0-dev.6: the owner check is described as it works (at the start and after every
  account change).

## [0.1.0-dev.6] (2026-09-30)

### Added
- `Account::id`: the account id of the API key (`GET /api/v1/account/id`, read scope).
- `WebSocket::live_balances` / `LiveBalances`: live balances from a REST snapshot plus
  `balance.updated` events. An event applies only when its `sequence` is greater than the stored
  one (a total of 0 removes the row, and an older snapshot row cannot bring it back); a refetch
  happens on a missed event, `balances.resync`, `CONCURRENT_MODIFICATION`, a reconnect or an account
  change, at most every `min_snapshot_interval` (default 2 s), with retry backoff. At the start and after
  every account change the REST key's account (`Account::id`) must be the WebSocket's user, otherwise nothing is
  merged (`BalancesEvent::AccountMismatch`). A custom `snapshot` must name its owner (`owner_id` or `account_id`),
  otherwise `live_balances` fails with a `CONFIG` error. Events without `sequence` (older servers) always apply
  and emit one `WsEvent::Warning`. `is_stale`, `last_error`, `get`, `all`, `close`; events through
  `WsEvent::Balances` (`Updated`, `Snapshot`, `AccountMismatch`, `Error`).
- WebSocket: frame-sequence tracking on private channels. A gap that is not filled within
  `WsOptions::reorder_window` (default 250 ms) emits `WsEvent::SequenceGap` and
  `Resync(ResyncReason::SequenceGap)`.
- WebSocket: `balances.resync` (and the planned `deposits.resync` / `withdrawals.resync`) are known
  events and emit `Resync` with `BalancesResync`, `DepositsResync` or `WithdrawalsResync`.
- WebSocket: the planned `signed_out` server frame is handled as a server sign-out:
  `AuthChangeReason::TokenExpired`, `SessionRevoked` plus `WsEvent::AuthLost` (synthetic
  `session.revoked` frame with `data.reason` `"signed_out"`), or `SignedOut` with the raw reason in
  `code` (`"unknown"` when the frame has none). The token is forgotten.
- `Balance::sequence` (a missing value decodes as 0), `WebSocket::user_id`, `WsClock` / `WsTimer` /
  `WsOptions::clock` (test-only time source).

## [0.1.0-dev.5] (2026-09-30)

### Fixed
- WebSocket: private channels no longer go silent after a server-side sign-out. The server ends
  every private subscription (without a frame) when `auth` succeeds as another user, when an
  `auth` fails, or when this connection's own session is revoked. The client kept those channels
  as held (after a failed auth) or never noticed the switch, so `subscribe` for them sent
  nothing. It now drops them, emits the new `WsEvent::AuthChanged` (`AuthChange`: `reason`,
  `previous_user_id`, `user_id`, `code`, `dropped`) and re-subscribes them: at once after a switch
  to another user, after the next successful `auth` otherwise, followed by
  `WsEvent::Resync(ResyncReason::Reauth)`. Re-authenticating as the same user changes nothing.
- WebSocket: a `subscribe` refused by the server (e.g. `UNAUTHENTICATED` for a private channel)
  no longer leaves the channels in `channels()`.

### Changed
- WebSocket: `session.revoked` acts only when `data.current` is exactly `true` (this connection's
  own session). Another session's revocation (`current: false`) no longer emits
  `WsEvent::AuthLost`, drops private channels or forgets the token. **Behaviour change.**
- `ResyncReason` is now `#[non_exhaustive]` and gains `Reauth`: a `match` on it needs a wildcard
  arm. **Breaking for exhaustive matches** (pre-1.0).

### Added
- `WebSocket::has_token`, `WsEvent::AuthChanged`, `AuthChange` and `AuthChangeReason`
  (`#[non_exhaustive]`).
- Conformance: runs `cexy-api-spec/conformance/ws/private_signout.json` against a local scripted
  server, each case in its own task so every failing case is reported.

## [0.1.0-dev.4] (2026-09-29)

### Added
- `account().sub_account_balances(id)`: a sub-account's balances, read by its parent account
  (`GET /account/sub-accounts/{id}/balances`, read scope). Same `Vec<Balance>` as `balances()`,
  including `held_incoming`. An id that is not the caller's sub-account is `ErrorCategory::NotFound`
  (not retried); an empty id is a config error before any request.
- `Balance::held_incoming` (`Vec<HeldIncoming>`: `transfer_id`, `amount`, `available_at`): incoming
  internal transfers still held, at most 100, soonest first. Their sum is already included in
  `locked`: never add it again. It decodes as an empty `Vec` when a server omits the field.

### Changed
- A 4xx response is never retried except 429 and 409 `CONCURRENT_MODIFICATION`, even when its body
  says `retryable: true`; `Error::is_retryable()` reports this. A 408 is no longer retried either,
  and its default `retryable` (no field in the body) is now false.
  409 `CONCURRENT_MODIFICATION` and 429 are still retried only where they were before. A mutation
  sent through the shared retry loop is retried only when it is repeat-safe (pool join/exit with
  their `Idempotency-Key`, cancel-all); `place_order` and `cancel_order` keep their own policies.

### Security
- Path values `"."` and `".."` are rejected with a config error: previously they escaped their URL
  segment, so e.g. `sub_account_balances("..")` returned the parent's own balances and
  `order_by_client_id("..")` the open-orders list. A write request could only be redirected to a
  route that does not exist and is refused by the server; no write could reach a different operation.

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
