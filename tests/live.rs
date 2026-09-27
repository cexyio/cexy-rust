//! Live smoke test against the production API: public, unauthenticated reads only.
//!
//!     CEXY_LIVE_TESTS=1 cargo test --test live -- --nocapture
//!
//! It never uses credentials and never places, cancels or changes anything.

use futures_util::StreamExt;

fn enabled() -> bool {
    std::env::var("CEXY_LIVE_TESTS").as_deref() == Ok("1")
}

#[tokio::test]
async fn public_endpoints_decode() {
    if !enabled() {
        eprintln!("skipped: set CEXY_LIVE_TESTS=1");
        return;
    }
    let c = cexy::Client::new(cexy::ClientOptions::default()).unwrap();
    let now = c.time().await.unwrap();
    assert!(now.time().is_some());
    c.config().await.unwrap();
    let markets = c.markets().list().await.unwrap();
    assert!(!markets.is_empty());
    c.assets().list().await.unwrap();
    c.networks().list().await.unwrap();
    c.fees().list().await.unwrap();
    c.pools().list().await.unwrap();
    let symbol = markets[0].symbol.clone();
    let book = c.markets().order_book(&symbol, None).await.unwrap();
    assert_eq!(book.symbol, symbol);
    c.markets().trades(&symbol, None).await.unwrap();
    let trades: Vec<_> = c
        .markets()
        .all_trades(&symbol, None, Some(5))
        .collect()
        .await;
    assert!(trades.iter().all(Result::is_ok));
    let p = cexy::GetCandlesParams::new(cexy::CandleInterval::H1);
    c.markets().candles(&symbol, &p).await.unwrap();
    println!(
        "{} markets, server time {}, {symbol} book at sequence {}",
        markets.len(),
        now.iso,
        book.sequence
    );
}

#[tokio::test]
async fn websocket_welcome() {
    if !enabled() {
        return;
    }
    let c = cexy::Client::new(cexy::ClientOptions::default()).unwrap();
    let ws = c.websocket(cexy::WsOptions::default()).unwrap();
    let w = ws.connect().await.unwrap();
    assert_eq!(w.protocol_version, cexy::SUPPORTED_PROTOCOL_VERSION);
    ws.close().await;
}
