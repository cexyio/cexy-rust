//! Futures data (read only): every operation, and conformance/futures/history_paging.json.

use futures_util::{StreamExt, TryStreamExt};
use serde_json::{Value, json};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::helpers::*;
use crate::{
    CandlesParams, DEFAULT_MAX_BUSY_RETRIES, Error, OperationId, PAGING_CURSOR_REPEATED,
    PAGING_STALLED,
};

const AS_OF: &str = "2026-10-02T08:00:00Z";

fn perp(coin: &str) -> Value {
    json!({
        "coin": coin, "funding_rate": "0.0000125", "mark_price": "60000.5",
        "max_leverage": 40, "mid_price": "60000.4", "open_interest": "1234.5",
        "oracle_price": "60001.0", "price_24h_ago": "59000.0", "size_decimals": 5,
        "volume_24h": "987654321.0"
    })
}

fn fill(id: &str, time: i64) -> Value {
    json!({
        "id": id, "order_id": "o1", "coin": "BTC", "side": "buy", "price": "60000.0",
        "size": "0.01", "direction": "Open Long", "closed_pnl": "0", "taker": true, "time": time
    })
}

fn funding(time: i64) -> Value {
    json!({"coin": "BTC", "amount": "-0.12", "position_size": "0.5", "rate": "0.0000125", "time": time})
}

/// Mounts one GET route answering `body` in the data envelope.
async fn route(s: &MockServer, p: &str, body: Value) {
    Mock::given(method("GET"))
        .and(path(p))
        .respond_with(data(body))
        .mount(s)
        .await;
}

#[tokio::test]
async fn every_futures_operation_is_covered() {
    let s = MockServer::start().await;
    route(
        &s,
        "/api/v1/futures/markets",
        json!({"as_of": AS_OF, "stale": false, "markets": [perp("BTC"), perp("kPEPE")]}),
    )
    .await;
    route(
        &s,
        "/api/v1/futures/markets/BTC",
        json!({"as_of": AS_OF, "stale": true, "market": perp("BTC")}),
    )
    .await;
    route(
        &s,
        "/api/v1/futures/markets/BTC/orderbook",
        json!({"as_of": AS_OF, "stale": false, "coin": "BTC",
               "bids": [{"price": "60000.0", "size": "1.5"}], "asks": [{"price": "60001.0", "size": "2"}]}),
    )
    .await;
    route(
        &s,
        "/api/v1/futures/markets/BTC/candles",
        json!({"as_of": AS_OF, "stale": false, "coin": "BTC", "interval": "1h", "candles": [{
            "open_time": 1790000000000_i64, "close_time": 1790003599999_i64, "open": "1", "high": "2",
            "low": "0.5", "close": "1.5", "volume": "10", "trades": 7}]}),
    )
    .await;
    route(
        &s,
        "/api/v1/futures/markets/BTC/trades",
        json!({"coin": "BTC", "stale": false,
               "trades": [{"side": "sell", "price": "60000.0", "size": "0.1", "time": 1790000000000_i64}]}),
    )
    .await;
    route(
        &s,
        "/api/v1/futures/positions",
        json!({"as_of": AS_OF, "stale": false, "has_account": true, "positions": {
            "account_value": "1000", "maintenance_margin": "10", "margin_used": "100",
            "total_notional": "3000", "positions": [{
                "coin": "BTC", "entry_price": "60000", "funding_since_open": "-0.5", "leverage": 10,
                "leverage_type": "cross", "liquidation_price": null, "margin_used": "100",
                "position_value": "3000", "return_on_equity": "0.01", "size": "0.05",
                "unrealized_pnl": "1.2"}]}}),
    )
    .await;
    route(
        &s,
        "/api/v1/futures/orders",
        json!({"as_of": AS_OF, "stale": false, "has_account": true, "orders": [{
            "id": "fo1", "coin": "BTC", "side": "buy", "order_type": "Limit", "price": "59000",
            "size": "0.01", "original_size": "0.02", "reduce_only": false,
            "placed_at": 1790000000000_i64}]}),
    )
    .await;
    route(
        &s,
        "/api/v1/futures/fills",
        json!({"has_account": true, "fills": [fill("f1", 1)], "next_cursor": null}),
    )
    .await;
    route(
        &s,
        "/api/v1/futures/funding",
        json!({"has_account": true, "funding": [funding(1)], "next_cursor": "n:1"}),
    )
    .await;

    let (c, _) = client(&s);
    let f = c.futures();
    let m = f.markets().await.unwrap();
    assert_eq!(m.markets[1].coin, "kPEPE");
    assert_eq!(m.markets[0].mark_price.as_str(), "60000.5");
    assert!(f.market("BTC").await.unwrap().stale);
    let b = f.order_book("BTC", Some(5)).await.unwrap();
    assert_eq!(b.bids[0].size.as_str(), "1.5");
    let mut cp = CandlesParams::new("1h");
    cp.before = Some(1790000000000);
    assert_eq!(f.candles("BTC", &cp).await.unwrap().candles[0].trades, 7);
    assert_eq!(
        f.trades("BTC", Some(10)).await.unwrap().trades[0].side,
        "sell"
    );
    let p = f.positions().await.unwrap();
    let pos = &p.positions.unwrap().positions[0];
    assert_eq!(pos.liquidation_price, None);
    assert_eq!(pos.entry_price.as_ref().unwrap().as_str(), "60000");
    let o = f.open_orders().await.unwrap();
    assert_eq!(
        (o.orders[0].id.as_str(), o.orders[0].trigger_price.is_none()),
        ("fo1", true)
    );
    assert_eq!(f.fills(None).await.unwrap().fills[0].id, "f1");
    let fu = f.funding(Some("n:0")).await.unwrap();
    assert_eq!(
        (fu.funding[0].amount.as_str(), fu.next_cursor.as_deref()),
        ("-0.12", Some("n:1"))
    );
    // The defaults send no query at all.
    f.order_book("BTC", None).await.unwrap();
    f.trades("BTC", None).await.unwrap();

    let reqs = s.received_requests().await.unwrap();
    let seen: Vec<(String, Option<String>)> = reqs
        .iter()
        .map(|r| (r.url.path().to_string(), r.url.query().map(str::to_string)))
        .collect();
    let q = |p: &str, q: Option<&str>| (p.to_string(), q.map(str::to_string));
    assert_eq!(
        seen,
        [
            q("/api/v1/futures/markets", None),
            q("/api/v1/futures/markets/BTC", None),
            q("/api/v1/futures/markets/BTC/orderbook", Some("depth=5")),
            q(
                "/api/v1/futures/markets/BTC/candles",
                Some("interval=1h&before=1790000000000")
            ),
            q("/api/v1/futures/markets/BTC/trades", Some("limit=10")),
            q("/api/v1/futures/positions", None),
            q("/api/v1/futures/orders", None),
            q("/api/v1/futures/fills", None),
            q("/api/v1/futures/funding", Some("cursor=n%3A0")),
            q("/api/v1/futures/markets/BTC/orderbook", None),
            q("/api/v1/futures/markets/BTC/trades", None),
        ]
    );
    // Account reads are signed; market data never carries credentials.
    for r in &reqs {
        let signed =
            r.headers.contains_key("x-api-signature") && r.headers.contains_key("x-api-key");
        let account = !r.url.path().starts_with("/api/v1/futures/markets");
        assert_eq!(signed, account, "{}", r.url.path());
    }
    // Every futures operation of the surface was exercised.
    let param = regex::Regex::new(r"\{[a-z_]+\}").unwrap();
    let futures_ops: Vec<OperationId> = OperationId::ALL
        .into_iter()
        .filter(|o| o.info().path.starts_with("/api/v1/futures/"))
        .collect();
    assert_eq!(futures_ops.len(), 9);
    for op in futures_ops {
        let info = op.info();
        assert_eq!(info.method, "GET", "{op:?}");
        let account = !info.path.starts_with("/api/v1/futures/markets");
        assert_eq!(
            (info.auth, info.scope),
            if account {
                ("api_key", "read")
            } else {
                ("none", "")
            },
            "{op:?}"
        );
        let re = format!("^{}$", param.replace_all(info.path, "[^/]+"));
        let re = regex::Regex::new(&re).unwrap();
        assert!(
            seen.iter().any(|(p, _)| re.is_match(p)),
            "{op:?} not called"
        );
    }
}

#[tokio::test]
async fn market_data_needs_no_credentials_and_account_data_does() {
    let s = MockServer::start().await;
    route(
        &s,
        "/api/v1/futures/markets",
        json!({"as_of": AS_OF, "stale": false, "markets": []}),
    )
    .await;
    let (c, _) = client_with(&s, false, |_| {});
    assert!(c.futures().markets().await.unwrap().markets.is_empty());
    let e = c.futures().positions().await.unwrap_err();
    assert!(matches!(e, Error::Config(_)), "{e}");
    let e = c.futures().all_fills(None).collect::<Vec<_>>().await;
    assert!(matches!(e[..], [Err(Error::Config(_))]));
    assert_eq!(s.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn no_futures_account_reads_as_has_account_false() {
    let s = MockServer::start().await;
    route(
        &s,
        "/api/v1/futures/positions",
        json!({"has_account": false, "stale": false}),
    )
    .await;
    route(
        &s,
        "/api/v1/futures/orders",
        json!({"has_account": false, "stale": false, "orders": []}),
    )
    .await;
    route(
        &s,
        "/api/v1/futures/funding",
        json!({"has_account": false, "funding": [], "next_cursor": null}),
    )
    .await;
    let (c, _) = client(&s);
    let p = c.futures().positions().await.unwrap();
    assert!(!p.has_account && p.positions.is_none());
    assert!(!c.futures().open_orders().await.unwrap().has_account);
    let rows: Vec<_> = c.futures().all_funding(None).try_collect().await.unwrap();
    assert!(rows.is_empty());
}

#[tokio::test]
async fn unavailable_market_data_is_retried_after_the_server_wait() {
    let s = MockServer::start().await;
    let unavailable = ResponseTemplate::new(503)
        .insert_header("retry-after", "2")
        .set_body_json(
            json!({"error": {"code": "SERVICE_UNAVAILABLE", "message": "unavailable",
            "retryable": true, "details": {"reason": "futures_data_unavailable"}}}),
        );
    Mock::given(path("/api/v1/futures/markets/BTC/trades"))
        .respond_with(Sequence::new(vec![
            unavailable,
            data(json!({"coin": "BTC", "stale": false, "trades": []})),
        ]))
        .mount(&s)
        .await;
    let (c, clock) = client(&s);
    c.futures().trades("BTC", None).await.unwrap();
    assert_eq!(s.received_requests().await.unwrap().len(), 2);
    assert!(clock.sleeps()[0] >= std::time::Duration::from_secs(2));
}

#[tokio::test]
async fn all_funding_pages_like_fills_and_stalls_the_same_way() {
    let page = |rows: Vec<Value>, next: Option<&str>| {
        data(json!({"has_account": true, "funding": rows, "next_cursor": next}))
    };
    // (client max_retries, busy retries set on the service, sleeps expected)
    for (max_retries, busy, sleeps) in [
        (Some(0), None, 3),
        (None, Some(1), 1),
        (Some(5), Some(0), 0),
    ] {
        let s = MockServer::start().await;
        Mock::given(path("/api/v1/futures/funding"))
            .respond_with(Sequence::new(vec![
                page(vec![funding(3), funding(2)], Some("x")),
                page(vec![], Some("x")),
            ]))
            .mount(&s)
            .await;
        // Busy retries are a setting of their own: the client's request retries do not change them.
        let (c, clock) = client_with(&s, true, |o| o.max_retries = max_retries);
        let mut f = c.futures();
        if let Some(n) = busy {
            f = f.with_max_busy_retries(n);
        }
        let items: Vec<_> = f.all_funding(None).collect().await;
        let case = format!("{max_retries:?} {busy:?}");
        assert_eq!(items.len(), 3, "{case}");
        assert_eq!(items[1].as_ref().unwrap().time, 2, "{case}");
        let e = items[2].as_ref().unwrap_err();
        assert_eq!(e.code(), Some(PAGING_STALLED), "{case}");
        assert!(e.is_retryable(), "{case}");
        assert!(
            matches!(e, Error::PagingStalled { operation: "funding", cursor, retries } if cursor == "x" && *retries == sleeps),
            "{case}: {e:?}"
        );
        assert!(e.to_string().contains("PAGING_STALLED"), "{e}");
        assert_eq!(
            s.received_requests().await.unwrap().len() as u32,
            2 + sleeps,
            "{case}"
        );
        assert_eq!(clock.sleeps().len() as u32, sleeps, "{case}");
    }
    assert_eq!(DEFAULT_MAX_BUSY_RETRIES, 3);
}

#[tokio::test]
async fn a_cursor_sent_twice_after_rows_fails_instead_of_looping() {
    let s = MockServer::start().await;
    let page = |rows: Vec<Value>, next: Option<&str>| {
        data(json!({"has_account": true, "fills": rows, "next_cursor": next}))
    };
    Mock::given(path("/api/v1/futures/fills"))
        .respond_with(Sequence::new(vec![
            page(vec![fill("f1", 3)], Some("a")),
            page(vec![fill("f2", 2)], Some("b")),
            page(vec![fill("f3", 1)], Some("a")),
        ]))
        .mount(&s)
        .await;
    let (c, _) = client(&s);
    let items: Vec<_> = c.futures().all_fills(None).collect().await;
    let ids: Vec<_> = items
        .iter()
        .filter_map(|r| r.as_ref().ok())
        .map(|f| f.id.as_str())
        .collect();
    assert_eq!(ids, ["f1", "f2", "f3"]);
    let e = items.last().unwrap().as_ref().unwrap_err();
    assert!(
        matches!(e, Error::PagingCursorRepeated { operation: "fills", cursor } if cursor == "a"),
        "{e:?}"
    );
    assert_eq!(e.code(), Some(PAGING_CURSOR_REPEATED));
    assert!(!e.is_retryable());
    assert_eq!(s.received_requests().await.unwrap().len(), 3);
}

#[tokio::test]
async fn max_items_stops_paging_early() {
    let s = MockServer::start().await;
    Mock::given(path("/api/v1/futures/fills"))
        .respond_with(data(
            json!({"has_account": true, "fills": [fill("f1", 2), fill("f2", 1)], "next_cursor": "c"}),
        ))
        .mount(&s)
        .await;
    let (c, _) = client(&s);
    let ids: Vec<String> = c
        .futures()
        .all_fills(Some(1))
        .map_ok(|f| f.id)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(ids, ["f1"]);
    assert_eq!(s.received_requests().await.unwrap().len(), 1);
}

/// conformance/futures/history_paging.json: cursors sent, rows yielded, requests, sleeps.
#[tokio::test]
async fn history_paging_conformance() {
    let Some(file) = load("futures/history_paging.json") else {
        return;
    };
    assert_eq!(file["operation"], "GET /api/v1/futures/fills");
    // `max_busy_retries` (`max_retries` in the first version of the file).
    let busy = file
        .get("max_busy_retries")
        .or_else(|| file.get("max_retries"))
        .and_then(Value::as_u64)
        .unwrap();
    assert_eq!(busy, u64::from(DEFAULT_MAX_BUSY_RETRIES));
    let cases = file["cases"].as_array().unwrap();
    assert!(cases.len() >= 5);
    // The busy retries do not depend on the client's request retries: run every case on the
    // default client and on one with retries disabled.
    for (case, max_retries) in cases.iter().flat_map(|c| [(c, None), (c, Some(0))]) {
        let id = &format!(
            "{} (max_retries {max_retries:?})",
            case["id"].as_str().unwrap()
        );
        let expect = &case["expect"];
        let pages = case["pages"].as_array().unwrap();
        let s = MockServer::start().await;
        Mock::given(path("/api/v1/futures/fills"))
            .respond_with(Sequence::new(
                pages.iter().map(|p| data(p["response"].clone())).collect(),
            ))
            .mount(&s)
            .await;
        let (c, clock) = client_with(&s, true, |o| o.max_retries = max_retries);
        let items: Vec<_> = c.futures().all_fills(None).collect().await;
        let ids: Vec<&str> = items
            .iter()
            .filter_map(|r| r.as_ref().ok())
            .map(|f| f.id.as_str())
            .collect();
        let err = items.iter().find_map(|r| r.as_ref().err());

        if let Some(code) = expect["error_code"].as_str() {
            let want: Vec<&str> = expect["ids_before_error"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap())
                .collect();
            assert_eq!(ids, want, "{id}: ids before the error");
            let e = err.unwrap_or_else(|| panic!("{id}: want an error"));
            assert!(
                items.last().unwrap().is_err(),
                "{id}: the error ends the stream"
            );
            assert_eq!(e.code(), Some(code), "{id}");
            assert!(
                [PAGING_STALLED, PAGING_CURSOR_REPEATED].contains(&code),
                "{id}"
            );
            assert_eq!(
                e.is_retryable(),
                expect["error_retryable"].as_bool().unwrap(),
                "{id}"
            );
        } else {
            assert!(err.is_none(), "{id}: {err:?}");
            let want: Vec<&str> = expect["ids"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap())
                .collect();
            assert_eq!(ids, want, "{id}: ids");
        }

        let reqs = s.received_requests().await.unwrap();
        assert_eq!(
            reqs.len() as u64,
            expect["requests"].as_u64().unwrap(),
            "{id}: requests"
        );
        assert_eq!(
            clock.sleeps().len() as u64,
            expect["sleeps"].as_u64().unwrap(),
            "{id}: sleeps"
        );
        for (i, (req, page)) in reqs.iter().zip(pages).enumerate() {
            let sent: Option<String> = req
                .url
                .query_pairs()
                .find(|(k, _)| k == "cursor")
                .map(|(_, v)| v.into_owned());
            assert_eq!(
                sent.as_deref(),
                page["request_cursor"].as_str(),
                "{id}: cursor of request {}",
                i + 1
            );
            assert!(req.headers.contains_key("x-api-signature"), "{id}: signed");
        }
        if let Some(q) = expect["encoded_query_of_request_2"].as_str() {
            assert_eq!(reqs[1].url.query(), Some(q), "{id}: encoded query");
        }
        if let Some(has) = expect["has_account"].as_bool() {
            assert_eq!(
                c.futures().fills(None).await.unwrap().has_account,
                has,
                "{id}"
            );
        }
    }
}
