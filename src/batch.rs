//! Batch place and batch cancel: local checks and the reading of a result after retries.
//!
//! The HTTP calls are in `services.rs` ([`crate::Trading::place_orders`],
//! [`crate::Trading::cancel_orders`]). The rules are the shared conformance cases in
//! `cexy-api-spec/conformance/trading/batch_orders.json`.

use crate::amount::Amount;
use crate::error::{Error, ErrorBody, Result};
use crate::models_gen::{
    BatchOrderResult, CancelBatchItem, CancelBatchResult, ErrorCode, Fill, Order, OrderSide,
    OrderStatus, OrderType, TimeInForce, TriggerDirection,
};

/// The most orders one [`crate::Trading::place_orders`] call takes.
pub const MAX_BATCH_ORDERS: usize = 20;
/// The most entries one [`crate::Trading::cancel_orders`] call takes.
pub const MAX_CANCEL_ENTRIES: usize = 50;

/// One order for [`crate::Trading::place_orders`]: the fields of a
/// [`crate::PlaceOrderRequest`] without the symbol. Amounts are decimal strings.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchOrderInput {
    /// Buy or sell.
    pub side: OrderSide,
    /// The order type.
    pub r#type: OrderType,
    /// Your own identifier, unique per account: 1 to 64 characters of `A-Z a-z 0-9 _ - . :`.
    /// A UUID is generated when it is `None`.
    pub client_order_id: Option<String>,
    /// Limit price.
    pub price: Option<Amount>,
    /// Quantity in the base asset.
    pub quantity: Option<Amount>,
    /// A market buy's budget in the quote asset, instead of `quantity`.
    pub quote_quantity: Option<Amount>,
    /// Trigger price of a stop order.
    pub stop_price: Option<Amount>,
    /// Time in force.
    pub time_in_force: Option<TimeInForce>,
    /// Trigger direction of a stop order.
    pub trigger_direction: Option<TriggerDirection>,
}

impl BatchOrderInput {
    /// An order with the required fields (side, type); the optional ones are unset.
    pub fn new(side: OrderSide, r#type: OrderType) -> Self {
        BatchOrderInput {
            side,
            r#type,
            client_order_id: None,
            price: None,
            quantity: None,
            quote_quantity: None,
            stop_price: None,
            time_in_force: None,
            trigger_direction: None,
        }
    }
}

impl CancelBatchItem {
    /// An entry naming the order by its id.
    pub fn by_order_id(order_id: impl Into<String>) -> Self {
        CancelBatchItem {
            order_id: Some(order_id.into()),
            client_order_id: None,
        }
    }

    /// An entry naming the order by your `client_order_id`.
    pub fn by_client_order_id(client_order_id: impl Into<String>) -> Self {
        CancelBatchItem {
            order_id: None,
            client_order_id: Some(client_order_id.into()),
        }
    }
}

/// What became of one order of [`crate::Trading::place_orders`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlaceOutcome {
    /// The server placed it now: `order` and `fills` are set.
    Placed,
    /// Refused: `error` says why. `ALREADY_EXISTS` here means the id was used before this call.
    Failed,
    /// An earlier attempt of this call placed it (its answer was lost), so the retry was answered
    /// `ALREADY_EXISTS`. `order_id` and `status` say what became of it; `order` is set only when
    /// the SDK had to look it up. A `rejected` status means it was refused (funds): its id is
    /// spent and it must not be placed again under that id.
    Recovered,
    /// As `Recovered`, but its state could not be read. It was not placed again; look it up with
    /// [`crate::Trading::order_by_client_id`].
    Unknown,
}

/// What became of one entry of [`crate::Trading::cancel_orders`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelOutcome {
    /// Cancelled now: `order` is the cancelled order.
    Cancelled,
    /// Not cancelled: `error` says why.
    Failed,
    /// An earlier attempt of this call may have cancelled it, so the retry was answered
    /// `INVALID_STATE`. `order` is its current state and `status` says cancelled or, for
    /// example, filled.
    Recovered,
    /// As `Recovered`, but its state could not be read.
    Unknown,
}

/// One order of [`crate::Trading::place_orders`], in request order.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchOrderOutcome {
    /// Its position in the request, from 0.
    pub index: i64,
    /// Its `client_order_id`, as sent (generated when you gave none).
    pub client_order_id: String,
    /// What became of it.
    pub outcome: PlaceOutcome,
    /// The order's id, when known.
    pub order_id: Option<String>,
    /// The order's status, when known.
    pub status: Option<OrderStatus>,
    /// The order, when placed now or looked up.
    pub order: Option<Order>,
    /// What it executed on entry (placed now only).
    pub fills: Vec<Fill>,
    /// The server's refusal (`Failed`), or the `ALREADY_EXISTS` that was reinterpreted.
    pub error: Option<ErrorBody>,
}

/// What [`crate::Trading::place_orders`] did. The counts are computed from `results`: `placed`
/// counts placed and recovered orders that were not rejected, `failed` the refused ones (a
/// recovered `rejected` included), `unknown` those whose state could not be read.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchPlaceResult {
    /// One per order, in request order.
    pub results: Vec<BatchOrderOutcome>,
    /// Orders that are on the book or executed.
    pub placed: usize,
    /// Orders that were refused.
    pub failed: usize,
    /// Orders whose state could not be read.
    pub unknown: usize,
}

/// One entry of [`crate::Trading::cancel_orders`], in request order.
#[derive(Debug, Clone, PartialEq)]
pub struct CancelEntryOutcome {
    /// Its position in the request, from 0.
    pub index: i64,
    /// What became of it.
    pub outcome: CancelOutcome,
    /// The order id, as sent, or the order's id when it was cancelled or looked up.
    pub order_id: Option<String>,
    /// The client order id, as sent.
    pub client_order_id: Option<String>,
    /// The order's status, when known.
    pub status: Option<OrderStatus>,
    /// The order, when cancelled now or looked up.
    pub order: Option<Order>,
    /// The server's refusal (`Failed`), or the `INVALID_STATE` that was reinterpreted.
    pub error: Option<ErrorBody>,
}

/// What [`crate::Trading::cancel_orders`] did. `cancelled` counts entries now cancelled
/// (recovered ones included), `failed` the rest that have a known state, `unknown` those that
/// do not.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchCancelResult {
    /// One per entry, in request order.
    pub results: Vec<CancelEntryOutcome>,
    /// Entries now cancelled.
    pub cancelled: usize,
    /// Entries not cancelled.
    pub failed: usize,
    /// Entries whose state could not be read.
    pub unknown: usize,
}

/// The server's rule for a client order id. It trims surrounding whitespace; the SDK refuses
/// it instead, so the id in a result is always the one sent.
/// `.` and `..` are refused too: the server accepts them, but they cannot be a path segment, so
/// the order could never be looked up by its id.
pub(crate) fn valid_client_order_id(cid: &str) -> bool {
    (1..=64).contains(&cid.len())
        && !is_dot_segment(cid)
        && cid
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b':'))
}

fn is_dot_segment(id: &str) -> bool {
    id == "." || id == ".."
}

/// A new `client_order_id` (a UUID v4), as `place_order` and `place_orders` generate for an
/// order without one. Create and save your ids with it before the call when you need to
/// reconcile after an error: a dropped future returns nothing.
pub fn new_client_order_id() -> String {
    crate::transport::new_id()
}

pub(crate) fn check_place(orders: &[BatchOrderInput]) -> Result<()> {
    if !(1..=MAX_BATCH_ORDERS).contains(&orders.len()) {
        return Err(Error::config(format!(
            "trading.place_orders takes 1 to {MAX_BATCH_ORDERS} orders, got {}",
            orders.len()
        )));
    }
    let mut seen = std::collections::HashSet::new();
    for (i, o) in orders.iter().enumerate() {
        let Some(cid) = &o.client_order_id else {
            continue;
        };
        if !valid_client_order_id(cid) {
            return Err(Error::config(format!(
                "trading.place_orders: orders[{i}]: client_order_id must be 1 to 64 characters of A-Z a-z 0-9 _ - . :"
            )));
        }
        if !seen.insert(cid.as_str()) {
            return Err(Error::config(format!(
                "trading.place_orders: orders[{i}]: client_order_id {cid:?} is used twice in this batch"
            )));
        }
    }
    Ok(())
}

/// Checks the entries and keeps only the id each one names.
pub(crate) fn check_cancel(entries: &[CancelBatchItem]) -> Result<Vec<CancelBatchItem>> {
    if !(1..=MAX_CANCEL_ENTRIES).contains(&entries.len()) {
        return Err(Error::config(format!(
            "trading.cancel_orders takes 1 to {MAX_CANCEL_ENTRIES} entries, got {}",
            entries.len()
        )));
    }
    let nonempty = |v: &Option<String>| v.clone().filter(|s| !s.is_empty());
    entries
        .iter()
        .enumerate()
        .map(|(i, e)| match (nonempty(&e.order_id), nonempty(&e.client_order_id)) {
            (Some(id), None) => Ok(CancelBatchItem::by_order_id(id)),
            (None, Some(cid)) if is_dot_segment(&cid) => Err(Error::config(format!(
                "trading.cancel_orders: orders[{i}]: client_order_id must not be \".\" or \"..\""
            ))),
            (None, Some(cid)) => Ok(CancelBatchItem::by_client_order_id(cid)),
            _ => Err(Error::config(format!(
                "trading.cancel_orders: orders[{i}]: give exactly one of order_id and client_order_id"
            ))),
        })
        .collect()
}

/// Reads one result. A recovered result that still needs a lookup has `status` `None`.
pub(crate) fn place_from_result(r: BatchOrderResult, after_ambiguous: bool) -> BatchOrderOutcome {
    let mut o = BatchOrderOutcome {
        index: r.index,
        client_order_id: r.client_order_id,
        outcome: PlaceOutcome::Failed,
        order_id: None,
        status: None,
        order: None,
        fills: r.fills,
        error: r.error,
    };
    match (r.order, &o.error) {
        (Some(order), None) => {
            o.outcome = PlaceOutcome::Placed;
            o.order_id = Some(order.id.clone());
            o.status = Some(order.status.clone());
            o.order = Some(order);
        }
        // An earlier attempt of this call placed it (its answer was lost).
        (_, Some(err)) if after_ambiguous && err.code == ErrorCode::AlreadyExists => {
            o.outcome = PlaceOutcome::Recovered;
            o.order_id = err.detail_str("order_id").map(str::to_string);
            o.status = err.detail_str("status").map(OrderStatus::from);
            o.fills = vec![];
        }
        // On any attempt: an error naming an order (MARKET_UNAVAILABLE when the market stopped
        // while matching) means the order exists; an INTERNAL may follow a committed order and
        // is looked up. ALREADY_EXISTS on a first attempt is the caller's reused id.
        (_, Some(err))
            if err.code != ErrorCode::AlreadyExists
                && (err.detail_str("order_id").is_some() || err.code == ErrorCode::Internal) =>
        {
            o.outcome = PlaceOutcome::Recovered;
            o.order_id = err.detail_str("order_id").map(str::to_string);
            o.status = err.detail_str("status").map(OrderStatus::from);
            o.fills = vec![];
        }
        _ => {}
    }
    o
}

pub(crate) fn needs_lookup(o: &BatchOrderOutcome) -> bool {
    o.outcome == PlaceOutcome::Recovered && (o.order_id.is_none() || o.status.is_none())
}

/// Whether a place answer matches the request: one result per order sent, in order, each with
/// its index and `client_order_id`.
pub(crate) fn place_matches(sent: &[String], results: &[BatchOrderResult]) -> bool {
    results.len() == sent.len()
        && results
            .iter()
            .zip(sent)
            .enumerate()
            .all(|(i, (r, cid))| r.index == i as i64 && &r.client_order_id == cid)
}

/// Whether a cancel answer matches the request: one result per entry, in order, echoing the
/// entry's id where it echoes one.
pub(crate) fn cancel_matches(sent: &[CancelBatchItem], results: &[CancelBatchResult]) -> bool {
    // Only the id the entry was sent with is compared: the other may name the order found.
    let same = |echo: &Option<String>, sent: &Option<String>| {
        sent.is_none() || echo.is_none() || echo == sent
    };
    results.len() == sent.len()
        && results.iter().zip(sent).enumerate().all(|(i, (r, e))| {
            r.index == i as i64
                && same(&r.order_id, &e.order_id)
                && same(&r.client_order_id, &e.client_order_id)
        })
}

/// Reads one entry. A recovered entry still needs its lookup.
pub(crate) fn cancel_from_result(
    r: CancelBatchResult,
    after_ambiguous: bool,
) -> CancelEntryOutcome {
    let mut o = CancelEntryOutcome {
        index: r.index,
        outcome: CancelOutcome::Failed,
        order_id: r.order_id,
        client_order_id: r.client_order_id,
        status: None,
        order: None,
        error: r.error,
    };
    match (r.order, &o.error) {
        (Some(order), None) => {
            o.outcome = CancelOutcome::Cancelled;
            o.order_id = Some(order.id.clone());
            o.status = Some(order.status.clone());
            o.order = Some(order);
        }
        (_, Some(err)) if after_ambiguous && err.code == ErrorCode::InvalidState => {
            o.outcome = CancelOutcome::Recovered;
        }
        _ => {}
    }
    o
}

pub(crate) fn place_result(results: Vec<BatchOrderOutcome>) -> BatchPlaceResult {
    let placed = results
        .iter()
        .filter(|o| match o.outcome {
            PlaceOutcome::Placed => true,
            PlaceOutcome::Recovered => o.status != Some(OrderStatus::Rejected),
            _ => false,
        })
        .count();
    let unknown = results
        .iter()
        .filter(|o| o.outcome == PlaceOutcome::Unknown)
        .count();
    BatchPlaceResult {
        failed: results.len() - placed - unknown,
        results,
        placed,
        unknown,
    }
}

pub(crate) fn cancel_result(results: Vec<CancelEntryOutcome>) -> BatchCancelResult {
    let cancelled = results
        .iter()
        .filter(|o| match o.outcome {
            CancelOutcome::Cancelled => true,
            CancelOutcome::Recovered => o.status == Some(OrderStatus::Cancelled),
            _ => false,
        })
        .count();
    let unknown = results
        .iter()
        .filter(|o| o.outcome == CancelOutcome::Unknown)
        .count();
    BatchCancelResult {
        failed: results.len() - cancelled - unknown,
        results,
        cancelled,
        unknown,
    }
}
