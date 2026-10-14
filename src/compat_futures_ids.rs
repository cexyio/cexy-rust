//! Names from before the server's `futures_` operationId prefix (API spec 2026-10-14). They
//! alias the new names, so existing code keeps compiling.

use crate::operations_gen::{
    FuturesCandlesParams, FuturesFillsParams, FuturesFundingParams, FuturesOrderbookParams,
    FuturesTradesParams, OperationId,
};

/// Renamed to [`FuturesCandlesParams`].
#[deprecated(note = "use FuturesCandlesParams")]
pub type CandlesParams = FuturesCandlesParams;
/// Renamed to [`FuturesFillsParams`].
#[deprecated(note = "use FuturesFillsParams")]
pub type FillsParams = FuturesFillsParams;
/// Renamed to [`FuturesFundingParams`].
#[deprecated(note = "use FuturesFundingParams")]
pub type FundingParams = FuturesFundingParams;
/// Renamed to [`FuturesOrderbookParams`].
#[deprecated(note = "use FuturesOrderbookParams")]
pub type OrderbookParams = FuturesOrderbookParams;
/// Renamed to [`FuturesTradesParams`].
#[deprecated(note = "use FuturesTradesParams")]
pub type TradesParams = FuturesTradesParams;

#[allow(non_upper_case_globals)]
impl OperationId {
    /// Renamed to [`OperationId::FuturesCandles`].
    #[deprecated(note = "use OperationId::FuturesCandles")]
    pub const Candles: OperationId = OperationId::FuturesCandles;
    /// Renamed to [`OperationId::FuturesFills`].
    #[deprecated(note = "use OperationId::FuturesFills")]
    pub const Fills: OperationId = OperationId::FuturesFills;
    /// Renamed to [`OperationId::FuturesFunding`].
    #[deprecated(note = "use OperationId::FuturesFunding")]
    pub const Funding: OperationId = OperationId::FuturesFunding;
    /// Renamed to [`OperationId::FuturesMarket`].
    #[deprecated(note = "use OperationId::FuturesMarket")]
    pub const Market: OperationId = OperationId::FuturesMarket;
    /// Renamed to [`OperationId::FuturesMarkets`].
    #[deprecated(note = "use OperationId::FuturesMarkets")]
    pub const Markets: OperationId = OperationId::FuturesMarkets;
    /// Renamed to [`OperationId::FuturesOpenOrders`].
    #[deprecated(note = "use OperationId::FuturesOpenOrders")]
    pub const OpenOrders: OperationId = OperationId::FuturesOpenOrders;
    /// Renamed to [`OperationId::FuturesOrderbook`].
    #[deprecated(note = "use OperationId::FuturesOrderbook")]
    pub const Orderbook: OperationId = OperationId::FuturesOrderbook;
    /// Renamed to [`OperationId::FuturesPositions`].
    #[deprecated(note = "use OperationId::FuturesPositions")]
    pub const Positions: OperationId = OperationId::FuturesPositions;
    /// Renamed to [`OperationId::FuturesTrades`].
    #[deprecated(note = "use OperationId::FuturesTrades")]
    pub const Trades: OperationId = OperationId::FuturesTrades;
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;

    #[test]
    fn old_names_alias_the_new_ones() {
        assert_eq!(OperationId::Fills, OperationId::FuturesFills);
        assert_eq!(OperationId::OpenOrders, OperationId::FuturesOpenOrders);
        assert_eq!(OperationId::Markets.info().path, "/api/v1/futures/markets");
        let _: FuturesCandlesParams = CandlesParams::default();
        let _: FuturesTradesParams = TradesParams::default();
        let _: FuturesOrderbookParams = OrderbookParams::default();
        let _: FuturesFillsParams = FillsParams::default();
        let _: FuturesFundingParams = FundingParams::default();
    }
}
