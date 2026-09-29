//! The REST services: public market data, account, exports, wallet reads, trading and pools.

use crate::amount::check_amounts;
use crate::client::Client;
use crate::error::{Error, ErrorCategory, Result};
use crate::models_gen::*;
use crate::operations_gen::*;
use crate::pagination::{ItemStream, Page, paginate};
use crate::transport::{Call, decode, decode_data, new_id};

fn q<P>(
    params: Option<&P>,
    f: fn(&P) -> Vec<(&'static str, String)>,
) -> Vec<(&'static str, String)> {
    params.map(f).unwrap_or_default()
}

impl Client {
    pub(crate) async fn page<T: serde::de::DeserializeOwned>(&self, c: Call) -> Result<Page<T>> {
        let op = c.op;
        let raw = self.t.request(c, &self.resolved()).await?;
        decode(op, &raw)
    }

    pub(crate) async fn text(&self, mut c: Call) -> Result<String> {
        c.text = true;
        let raw = self.t.request(c, &self.resolved()).await?;
        Ok(String::from_utf8_lossy(&raw.body).into_owned())
    }
}

/// Defines `all_*`: every item of a listing, following the cursor.
macro_rules! all_items {
    ($(#[$m:meta])* $name:ident, $page:ident, $params:ty, $item:ty) => {
        $(#[$m])*
        /// `max_items` stops the stream after that many items in total.
        pub fn $name(&self, params: Option<&$params>, max_items: Option<usize>) -> ItemStream<'a, $item> {
            let base = params.cloned().unwrap_or_default();
            let this = Self { c: self.c };
            paginate(base.cursor.clone(), max_items, move |cursor| {
                let mut p = base.clone();
                p.cursor = cursor;
                async move { this.$page(Some(&p)).await }
            })
        }
    };
}

// ---------------------------------------------------------------------------------------
// Public market data (no credentials)
// ---------------------------------------------------------------------------------------

/// Markets, tickers, order books, public trades and candles.
#[derive(Clone, Copy)]
pub struct Markets<'a> {
    pub(crate) c: &'a Client,
}

impl<'a> Markets<'a> {
    /// Every market with its current ticker.
    pub async fn list(&self) -> Result<Vec<Market>> {
        self.c.get(Call::new(OperationId::ListMarkets)).await
    }

    /// One market, such as `"BTC/USDT"`.
    pub async fn get(&self, symbol: &str) -> Result<Market> {
        self.c
            .get(Call::new(OperationId::GetMarket).path("symbol", symbol))
            .await
    }

    /// An order-book snapshot aggregated by price, with the realtime sequence it is current as
    /// of. To follow a book live, use [`crate::WebSocket::order_book`], which applies the sync rules.
    pub async fn order_book(
        &self,
        symbol: &str,
        params: Option<&GetOrderBookParams>,
    ) -> Result<OrderBook> {
        let c = Call::new(OperationId::GetOrderBook)
            .path("symbol", symbol)
            .query(q(params, GetOrderBookParams::query));
        self.c.get(c).await
    }

    /// One page of recent public trades.
    pub async fn trades(
        &self,
        symbol: &str,
        params: Option<&GetMarketTradesParams>,
    ) -> Result<Page<PublicTrade>> {
        let c = Call::new(OperationId::GetMarketTrades)
            .path("symbol", symbol)
            .query(q(params, GetMarketTradesParams::query));
        self.c.page(c).await
    }

    /// Every public trade of a market, following the cursor. `max_items` stops the stream after
    /// that many items in total.
    pub fn all_trades(
        &self,
        symbol: &'a str,
        params: Option<&GetMarketTradesParams>,
        max_items: Option<usize>,
    ) -> ItemStream<'a, PublicTrade> {
        let base = params.cloned().unwrap_or_default();
        let this = *self;
        paginate(base.cursor.clone(), max_items, move |cursor| {
            let mut p = base.clone();
            p.cursor = cursor;
            async move { this.trades(symbol, Some(&p)).await }
        })
    }

    /// OHLCV candles; `params.interval` is required.
    pub async fn candles(&self, symbol: &str, params: &GetCandlesParams) -> Result<Vec<Candle>> {
        let c = Call::new(OperationId::GetCandles)
            .path("symbol", symbol)
            .query(params.query());
        self.c.get(c).await
    }
}

/// The asset catalogue.
pub struct Assets<'a> {
    pub(crate) c: &'a Client,
}

impl Assets<'_> {
    /// Every asset.
    pub async fn list(&self) -> Result<Vec<Asset>> {
        self.c.get(Call::new(OperationId::ListAssets)).await
    }

    /// One asset.
    pub async fn get(&self, symbol: &str) -> Result<Asset> {
        self.c
            .get(Call::new(OperationId::GetAsset).path("symbol", symbol))
            .await
    }
}

/// Blockchain networks.
pub struct Networks<'a> {
    pub(crate) c: &'a Client,
}

impl Networks<'_> {
    /// Every network.
    pub async fn list(&self) -> Result<Vec<Network>> {
        self.c.get(Call::new(OperationId::ListNetworks)).await
    }
}

/// Fee schedules.
pub struct Fees<'a> {
    pub(crate) c: &'a Client,
}

impl Fees<'_> {
    /// The fee schedules (maker and taker rates by tier).
    pub async fn list(&self) -> Result<Vec<FeeSchedule>> {
        self.c.get(Call::new(OperationId::ListFeeSchedules)).await
    }
}

/// Liquidity pools. Join and exit need an API key with the trade scope.
pub struct Pools<'a> {
    pub(crate) c: &'a Client,
}

impl Pools<'_> {
    /// Every pool.
    pub async fn list(&self) -> Result<Vec<Pool>> {
        self.c.get(Call::new(OperationId::ListPools)).await
    }

    /// One pool.
    pub async fn get(&self, symbol: &str) -> Result<Pool> {
        self.c
            .get(Call::new(OperationId::GetPool).path("symbol", symbol))
            .await
    }

    /// Adds liquidity. Amounts are decimal strings. The Idempotency-Key (generated, or set with
    /// [`crate::CallOptions::idempotency_key`]) makes retries safe.
    pub async fn join(&self, symbol: &str, req: &JoinPoolRequest) -> Result<JoinPoolResult> {
        check_amounts(
            "pools.join",
            &[
                ("base_amount", Some(&req.base_amount)),
                ("quote_amount", Some(&req.quote_amount)),
                (
                    "max_ratio_deviation_percent",
                    req.max_ratio_deviation_percent.as_ref(),
                ),
            ],
        )?;
        self.c
            .get(
                Call::new(OperationId::JoinPool)
                    .path("symbol", symbol)
                    .json(req)?,
            )
            .await
    }

    /// Removes liquidity. The Idempotency-Key makes retries safe.
    pub async fn exit(&self, symbol: &str, req: &ExitPoolRequest) -> Result<ExitPoolResult> {
        check_amounts("pools.exit", &[("shares", Some(&req.shares))])?;
        self.c
            .get(
                Call::new(OperationId::ExitPool)
                    .path("symbol", symbol)
                    .json(req)?,
            )
            .await
    }
}

// ---------------------------------------------------------------------------------------
// Private (API key; read scope unless stated)
// ---------------------------------------------------------------------------------------

/// Balances, ledger, notifications, sub-accounts and API keys.
#[derive(Clone, Copy)]
pub struct Account<'a> {
    pub(crate) c: &'a Client,
}

impl<'a> Account<'a> {
    /// Every balance.
    ///
    /// [`Balance::held_incoming`] lists incoming internal transfers still held. Their sum is
    /// ALREADY INCLUDED in `locked`: never add them to `locked` or `total` again. At most 100
    /// entries, soonest `available_at` first (millisecond precision), with no sender identity. An
    /// entry disappears once the transfer is released (its amount moves to `available`) or
    /// cancelled by the exchange. It is empty, never missing, when a server omits the field.
    pub async fn balances(&self) -> Result<Vec<Balance>> {
        self.c.get(Call::new(OperationId::ListBalances)).await
    }

    /// The balance of one asset. See [`Account::balances`] for `held_incoming`.
    pub async fn balance(&self, asset: &str) -> Result<Balance> {
        self.c
            .get(Call::new(OperationId::GetBalance).path("asset", asset))
            .await
    }

    /// One page of ledger entries.
    pub async fn ledger(&self, params: Option<&GetLedgerParams>) -> Result<Page<LedgerEntry>> {
        self.c
            .page(Call::new(OperationId::GetLedger).query(q(params, GetLedgerParams::query)))
            .await
    }

    all_items!(
        /// Every ledger entry.
        all_ledger, ledger, GetLedgerParams, LedgerEntry
    );

    /// One page of notifications.
    pub async fn notifications(
        &self,
        params: Option<&ListNotificationsParams>,
    ) -> Result<Page<Notification>> {
        self.c
            .page(
                Call::new(OperationId::ListNotifications)
                    .query(q(params, ListNotificationsParams::query)),
            )
            .await
    }

    all_items!(
        /// Every notification.
        all_notifications, notifications, ListNotificationsParams, Notification
    );

    /// The sub-accounts.
    pub async fn sub_accounts(&self) -> Result<Vec<SubAccount>> {
        self.c.get(Call::new(OperationId::ListSubAccounts)).await
    }

    /// A sub-account's balances, read by its PARENT account: the same shape as
    /// [`Account::balances`] (zero balances omitted, sorted by asset), including `held_incoming`,
    /// whose sum is already inside `locked`. An id that is not one of the caller's sub-accounts
    /// (or a call made with the sub-account's own key) is an API error in
    /// [`ErrorCategory::NotFound`](crate::ErrorCategory::NotFound) and is not retried; a
    /// sub-account's own key reads its balances with [`Account::balances`]. `id` must be
    /// non-empty (a config error before any request); it is sent as one URL path segment.
    pub async fn sub_account_balances(&self, id: &str) -> Result<Vec<Balance>> {
        self.c
            .get(Call::new(OperationId::SubAccountBalances).path("id", id))
            .await
    }

    /// Your API keys (metadata only; secrets are never returned).
    pub async fn api_keys(&self) -> Result<Vec<ApiKey>> {
        self.c.get(Call::new(OperationId::ListApiKeys)).await
    }
}

/// CSV exports. Each method returns the CSV text.
pub struct Exports<'a> {
    pub(crate) c: &'a Client,
}

impl Exports<'_> {
    async fn export(&self, op: OperationId, params: Option<&ExportParams>) -> Result<String> {
        self.c
            .text(Call::new(op).query(q(params, ExportParams::query)))
            .await
    }
    /// Deposits as CSV.
    pub async fn deposits(&self, params: Option<&ExportParams>) -> Result<String> {
        self.export(OperationId::ExportDeposits, params).await
    }
    /// The ledger as CSV.
    pub async fn ledger(&self, params: Option<&ExportParams>) -> Result<String> {
        self.export(OperationId::ExportLedger, params).await
    }
    /// Orders as CSV.
    pub async fn orders(&self, params: Option<&ExportParams>) -> Result<String> {
        self.export(OperationId::ExportOrders, params).await
    }
    /// Your trades as CSV.
    pub async fn trades(&self, params: Option<&ExportParams>) -> Result<String> {
        self.export(OperationId::ExportTrades, params).await
    }
    /// Withdrawals as CSV.
    pub async fn withdrawals(&self, params: Option<&ExportParams>) -> Result<String> {
        self.export(OperationId::ExportWithdrawals, params).await
    }
}

/// Wallet reads. API keys can never withdraw or transfer; there are no such methods.
#[derive(Clone, Copy)]
pub struct Wallet<'a> {
    pub(crate) c: &'a Client,
}

impl<'a> Wallet<'a> {
    /// One page of deposits.
    pub async fn deposits(&self, params: Option<&ListDepositsParams>) -> Result<Page<Deposit>> {
        self.c
            .page(Call::new(OperationId::ListDeposits).query(q(params, ListDepositsParams::query)))
            .await
    }

    all_items!(
        /// Every deposit.
        all_deposits, deposits, ListDepositsParams, Deposit
    );

    /// One deposit.
    pub async fn deposit(&self, deposit_id: &str) -> Result<Deposit> {
        self.c
            .get(Call::new(OperationId::GetDeposit).path("deposit_id", deposit_id))
            .await
    }

    /// One page of withdrawals.
    pub async fn withdrawals(
        &self,
        params: Option<&ListWithdrawalsParams>,
    ) -> Result<Page<Withdrawal>> {
        self.c
            .page(
                Call::new(OperationId::ListWithdrawals)
                    .query(q(params, ListWithdrawalsParams::query)),
            )
            .await
    }

    all_items!(
        /// Every withdrawal.
        all_withdrawals, withdrawals, ListWithdrawalsParams, Withdrawal
    );

    /// One withdrawal.
    pub async fn withdrawal(&self, withdrawal_id: &str) -> Result<Withdrawal> {
        self.c
            .get(Call::new(OperationId::GetWithdrawal).path("withdrawal_id", withdrawal_id))
            .await
    }

    /// The saved withdrawal addresses.
    pub async fn withdrawal_addresses(&self) -> Result<Vec<WithdrawalAddress>> {
        self.c
            .get(Call::new(OperationId::ListWithdrawalAddresses))
            .await
    }

    /// Your deposit address for an asset on a network.
    ///
    /// Side effect: the first call for an asset and network CREATES the address, and it is
    /// permanent; later calls return the same address. Always send the memo too when the
    /// response has one, or the deposit may be unrecoverable.
    pub async fn deposit_address(&self, params: &DepositAddressParams) -> Result<DepositAddress> {
        if params.asset.is_empty() || params.network.is_empty() {
            return Err(Error::config(
                "wallet.deposit_address: asset and network are required",
            ));
        }
        self.c
            .get(Call::new(OperationId::DepositAddress).query(params.query()))
            .await
    }
}

// ---------------------------------------------------------------------------------------
// Trading
// ---------------------------------------------------------------------------------------

/// Orders and your trades. Placing and cancelling need the trade scope.
#[derive(Clone, Copy)]
pub struct Trading<'a> {
    pub(crate) c: &'a Client,
}

/// What [`Trading::place_order`] returns.
#[derive(Debug, Clone, PartialEq)]
pub struct PlaceOrderResult {
    /// The order and anything it executed immediately.
    pub response: PlaceOrderResponse,
    /// The `client_order_id` that was sent (generated when you did not set one).
    pub client_order_id: String,
    /// True when the POST failed ambiguously and the order was then found by
    /// `client_order_id`. `response.fills` is empty then; use `trades` for the executions.
    pub recovered: bool,
}

impl<'a> Trading<'a> {
    /// Open orders, optionally filtered by market or status.
    pub async fn open_orders(&self, params: Option<&ListOpenOrdersParams>) -> Result<Vec<Order>> {
        self.c
            .get(
                Call::new(OperationId::ListOpenOrders)
                    .query(q(params, ListOpenOrdersParams::query)),
            )
            .await
    }

    /// One order.
    pub async fn order(&self, order_id: &str) -> Result<Order> {
        self.c
            .get(Call::new(OperationId::GetOrder).path("order_id", order_id))
            .await
    }

    /// The order with your `client_order_id`.
    pub async fn order_by_client_id(&self, client_order_id: &str) -> Result<Order> {
        self.c
            .get(
                Call::new(OperationId::GetOrderByClientId).path("client_order_id", client_order_id),
            )
            .await
    }

    /// One page of past orders.
    pub async fn order_history(&self, params: Option<&OrderHistoryParams>) -> Result<Page<Order>> {
        self.c
            .page(Call::new(OperationId::OrderHistory).query(q(params, OrderHistoryParams::query)))
            .await
    }

    all_items!(
        /// Every past order.
        all_order_history, order_history, OrderHistoryParams, Order
    );

    /// One page of your executions.
    pub async fn trades(&self, params: Option<&TradeHistoryParams>) -> Result<Page<Fill>> {
        self.c
            .page(Call::new(OperationId::TradeHistory).query(q(params, TradeHistoryParams::query)))
            .await
    }

    all_items!(
        /// Every execution.
        all_trades, trades, TradeHistoryParams, Fill
    );

    /// Places a REAL order (needs the trade scope). Amounts are decimal strings.
    ///
    /// Retry safety rests on `client_order_id` (a UUID is generated when absent): it is unique
    /// per account, and the server refuses a repeat before any funds move. The server does NOT
    /// honour Idempotency-Key on orders. After an ambiguous failure (connection error, timeout or
    /// 5xx) the SDK first looks the order up by `client_order_id` and returns it if it exists
    /// (`recovered`); only if it does not exist is the order sent again, with the same
    /// `client_order_id`, so a late first attempt makes the resend fail as a duplicate, which
    /// the lookup resolves again. It returns [`Error::OrderStateUnknown`] when even the lookup
    /// fails.
    pub async fn place_order(&self, order: &PlaceOrderRequest) -> Result<PlaceOrderResult> {
        if order.symbol.is_empty() {
            return Err(Error::config("trading.place_order: symbol is required"));
        }
        check_amounts(
            "trading.place_order",
            &[
                ("price", order.price.as_ref()),
                ("quantity", order.quantity.as_ref()),
                ("quote_quantity", order.quote_quantity.as_ref()),
                ("stop_price", order.stop_price.as_ref()),
            ],
        )?;
        let mut order = order.clone();
        let client_order_id = order
            .client_order_id
            .clone()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(new_id);
        order.client_order_id = Some(client_order_id.clone());
        let o = self.c.resolved();
        // No Idempotency-Key: the server does not honour one on orders (client_order_id does the job).
        let call = Call::new(OperationId::PlaceOrder).json(&order)?;

        let mut attempt = 0;
        loop {
            let err = match self.c.t.attempt(&call, &o).await {
                Ok(raw) => {
                    // The order was accepted: an unreadable response must still say which
                    // client_order_id to look up.
                    let response: PlaceOrderResponse = decode_data(OperationId::PlaceOrder, &raw)
                        .map_err(|e| Error::OrderStateUnknown {
                        client_order_id: client_order_id.clone(),
                        source: Box::new(e),
                    })?;
                    return Ok(PlaceOrderResult {
                        response,
                        client_order_id,
                        recovered: false,
                    });
                }
                Err(e) => e,
            };
            let duplicate_after_retry = attempt > 0
                && err.is(ErrorCategory::Conflict)
                && err.api().is_some_and(|a| {
                    a.code == ErrorCode::AlreadyExists
                        || a.code == ErrorCode::IdempotencyKeyConflict
                });
            if err.is_ambiguous() || duplicate_after_retry {
                if let Some(existing) = self.lookup(&client_order_id, &err, o.timeout).await? {
                    return Ok(PlaceOrderResult {
                        response: PlaceOrderResponse {
                            order: existing,
                            fills: vec![],
                        },
                        client_order_id,
                        recovered: true,
                    });
                }
                if duplicate_after_retry || attempt >= o.max_retries || err.server_wait_too_long() {
                    return Err(err);
                }
            } else if !err.is_retryable() || attempt >= o.max_retries {
                // Definitive refusals that did not execute (such as 429) are resent below.
                return Err(err);
            }
            self.c
                .t
                .backoff(OperationId::PlaceOrder, attempt, &err, None)
                .await;
            attempt += 1;
        }
    }

    async fn lookup(
        &self,
        client_order_id: &str,
        original: &Error,
        timeout: std::time::Duration,
    ) -> Result<Option<Order>> {
        let c = self.c.with_options(crate::CallOptions {
            timeout: Some(timeout),
            ..self.c.opts.clone()
        });
        match c.trading().order_by_client_id(client_order_id).await {
            Ok(o) => Ok(Some(o)),
            Err(e) if e.is(ErrorCategory::NotFound) => Ok(None),
            Err(_) => Err(Error::OrderStateUnknown {
                client_order_id: client_order_id.to_string(),
                source: Box::new(clone_error(original)),
            }),
        }
    }

    /// Cancels one order (needs the trade scope). It is retried on connection errors and
    /// retryable responses. If a RETRY gets `INVALID_STATE` (the order is no longer open,
    /// typically because the first attempt did cancel it), the cancel is treated as done and
    /// the order is fetched and returned. `INVALID_STATE` on the first attempt is returned as an
    /// error (for example, the order was already filled).
    ///
    /// Check the returned order's `status`: after a retry it is whatever state the order is in,
    /// which is `cancelled` in the usual case but can be `filled` (or partially filled and
    /// cancelled) if it traded before the cancel landed.
    pub async fn cancel_order(&self, order_id: &str) -> Result<Order> {
        let o = self.c.resolved();
        // No Idempotency-Key: the server does not honour one on cancels.
        let call = Call::new(OperationId::CancelOrder).path("order_id", order_id);
        let mut attempt = 0;
        loop {
            let err = match self.c.t.attempt(&call, &o).await {
                Ok(raw) => return decode_data(OperationId::CancelOrder, &raw),
                Err(e) => e,
            };
            if attempt > 0 && err.api().is_some_and(|a| a.code == ErrorCode::InvalidState) {
                return self.order(order_id).await;
            }
            if !err.is_retryable() || attempt >= o.max_retries {
                return Err(err);
            }
            self.c
                .t
                .backoff(OperationId::CancelOrder, attempt, &err, None)
                .await;
            attempt += 1;
        }
    }

    /// Cancels every open order in ONE market, such as `"BTC/USDT"`, in one request. An empty
    /// symbol is an error, so an account-wide cancel never happens by accident; use
    /// [`Trading::cancel_all_markets`] for that. An unknown symbol is an API error in the
    /// `NotFound` category.
    ///
    /// It also cancels stop orders that have not triggered yet (status `pending_trigger`) and
    /// releases their reservations, so nothing fires into the market after the call.
    ///
    /// One call handles at most 500 orders. Every order it handled is in exactly one of
    /// `cancelled`, `already_closed` (it closed on its own first: not a failure) and `failed`
    /// (with the reason in `failures`; `INVALID_STATE` means it was still being placed).
    /// `has_more` means there are more: call again, or use [`Trading::cancel_all_until_done`].
    ///
    /// The server limits cancel-all to 30 calls a minute per account (retried after the server's
    /// wait by the normal retry policy). It is naturally repeatable, so it is retried after
    /// connection errors; a retry reports only what that retry did. No Idempotency-Key is sent:
    /// the server does not honour one here.
    pub async fn cancel_all(&self, symbol: &str) -> Result<CancelAllResult> {
        if symbol.is_empty() {
            return Err(Error::config(
                "trading.cancel_all: symbol is required (\"BASE/QUOTE\"); use cancel_all_markets to cancel in every market",
            ));
        }
        self.cancel_all_once(Some(symbol)).await
    }

    /// Cancels every open order in EVERY market, in one request. See [`Trading::cancel_all`].
    pub async fn cancel_all_markets(&self) -> Result<CancelAllResult> {
        self.cancel_all_once(None).await
    }

    pub(crate) async fn cancel_all_once(&self, symbol: Option<&str>) -> Result<CancelAllResult> {
        self.c.get(cancel_all_call(symbol)?).await
    }

    /// One cancel-all request with no transport retries: `cancel_all_until_done` owns the
    /// retries, so each of its rounds is exactly one HTTP request.
    pub(crate) async fn cancel_all_single_request(
        &self,
        symbol: Option<&str>,
    ) -> Result<CancelAllResult> {
        let o = self.c.resolved();
        let raw = self.c.t.attempt(&cancel_all_call(symbol)?, &o).await?;
        decode_data(OperationId::CancelAll, &raw)
    }
}

fn cancel_all_call(symbol: Option<&str>) -> Result<Call> {
    let mut call = Call::new(OperationId::CancelAll).json(&CancelAllRequest {
        symbol: symbol.map(str::to_string),
    })?;
    call.no_idempotency_key = true;
    Ok(call)
}

/// A copy of an error for wrapping (errors hold no resources).
pub(crate) fn clone_error(e: &Error) -> Error {
    match e {
        Error::Api(a) => Error::Api(a.clone()),
        Error::Connection(c) => Error::Connection(c.clone()),
        Error::WebSocket(w) => Error::WebSocket(w.clone()),
        other => Error::Decode(other.to_string()),
    }
}
