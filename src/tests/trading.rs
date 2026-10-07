use std::time::Duration;

use serde_json::{Value, json};
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::helpers::*;
use crate::{Amount, Error, ErrorCategory, OrderSide, OrderType, PlaceOrderRequest};

fn limit_order() -> PlaceOrderRequest {
    let mut o = PlaceOrderRequest::new("BTC/USDT", OrderSide::Buy, OrderType::Limit);
    o.price = Some(Amount::new("60000.00").unwrap());
    o.quantity = Some(Amount::new("0.001").unwrap());
    o
}

fn placed() -> ResponseTemplate {
    data(json!({"order": order("ord_1", "open"), "fills": []}))
}

fn body(r: &wiremock::Request) -> Value {
    serde_json::from_slice(&r.body).unwrap()
}

#[tokio::test]
async fn generates_a_client_order_id_and_keeps_yours() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/trading/orders"))
        .respond_with(placed())
        .mount(&server)
        .await;
    let (c, _) = client(&server);
    let r = c.trading().place_order(&limit_order()).await.unwrap();
    assert_eq!(r.client_order_id.len(), 36);
    assert!(!r.recovered);
    let mut mine = limit_order();
    mine.client_order_id = Some("mine-1".into());
    assert_eq!(
        c.trading()
            .place_order(&mine)
            .await
            .unwrap()
            .client_order_id,
        "mine-1"
    );
    let reqs = server.received_requests().await.unwrap();
    assert_eq!(body(&reqs[0])["client_order_id"], json!(r.client_order_id));
    assert_eq!(body(&reqs[1])["client_order_id"], json!("mine-1"));
    assert_eq!(body(&reqs[0])["type"], json!("limit"));
    // The server does not honour Idempotency-Key on orders: none is sent.
    assert!(
        reqs.iter()
            .all(|r| !r.headers.contains_key("idempotency-key"))
    );
}

#[tokio::test]
async fn ambiguous_failure_then_found_by_client_order_id_is_recovered() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/trading/orders"))
        .respond_with(api_error(503, "SERVICE_UNAVAILABLE", true))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v1/trading/orders/by-client-id/cid-42"))
        .respond_with(data(order("ord_9", "open")))
        .mount(&server)
        .await;
    let (c, _) = client(&server);
    let mut o = limit_order();
    o.client_order_id = Some("cid-42".into());
    let r = c.trading().place_order(&o).await.unwrap();
    assert!(r.recovered);
    assert_eq!(r.response.order.id, "ord_9");
    assert!(r.response.fills.is_empty());
}

#[tokio::test]
async fn not_found_after_an_ambiguous_failure_resends_the_same_client_order_id() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/trading/orders"))
        .respond_with(Sequence::new(vec![
            api_error(502, "INTERNAL", true),
            placed(),
        ]))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex("^/api/v1/trading/orders/by-client-id/"))
        .respond_with(api_error(404, "NOT_FOUND", false))
        .mount(&server)
        .await;
    let (c, _) = client(&server);
    let r = c.trading().place_order(&limit_order()).await.unwrap();
    assert!(!r.recovered);
    let posts: Vec<_> = server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.method.as_str() == "POST")
        .collect();
    assert_eq!(posts.len(), 2);
    assert_eq!(
        body(&posts[0])["client_order_id"],
        body(&posts[1])["client_order_id"]
    );
}

#[tokio::test]
async fn failed_lookup_means_order_state_unknown() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/trading/orders"))
        .respond_with(api_error(503, "SERVICE_UNAVAILABLE", true))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex("^/api/v1/trading/orders/by-client-id/"))
        .respond_with(api_error(500, "INTERNAL", false))
        .mount(&server)
        .await;
    let (c, _) = client(&server);
    let e = c.trading().place_order(&limit_order()).await.unwrap_err();
    assert!(matches!(e, Error::OrderStateUnknown { .. }), "{e}");
}

#[tokio::test]
async fn cancel_order_invalid_state_on_a_retry_counts_as_cancelled() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/api/v1/trading/orders/ord_5"))
        .respond_with(Sequence::new(vec![
            api_error(503, "SERVICE_UNAVAILABLE", true),
            api_error(409, "INVALID_STATE", false),
        ]))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v1/trading/orders/ord_5"))
        .respond_with(data(order("ord_5", "cancelled")))
        .mount(&server)
        .await;
    let (c, _) = client(&server);
    let o = c.trading().cancel_order("ord_5").await.unwrap();
    assert_eq!(o.status, crate::OrderStatus::Cancelled);
    let reqs = server.received_requests().await.unwrap();
    assert!(
        reqs.iter()
            .all(|r| !r.headers.contains_key("idempotency-key"))
    );
}

#[tokio::test]
async fn cancel_order_invalid_state_on_the_first_attempt_is_an_error() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/api/v1/trading/orders/ord_6"))
        .respond_with(api_error(409, "INVALID_STATE", false))
        .mount(&server)
        .await;
    let (c, _) = client(&server);
    let e = c.trading().cancel_order("ord_6").await.unwrap_err();
    assert_eq!(e.api().unwrap().code, crate::ErrorCode::InvalidState);
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

fn cancel_result(cancelled: &[&str]) -> ResponseTemplate {
    data(
        json!({"cancelled": cancelled, "already_closed": [], "failed": [], "failures": [], "has_more": false}),
    )
}

#[tokio::test]
async fn cancel_all_is_explicit_and_sends_no_idempotency_key() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/trading/orders/cancel-all"))
        .respond_with(cancel_result(&["a"]))
        .mount(&server)
        .await;
    let (c, _) = client(&server);
    assert!(matches!(
        c.trading().cancel_all("").await,
        Err(Error::Config(_))
    ));
    let r = c.trading().cancel_all("BTC/USDT").await.unwrap();
    assert_eq!(r.cancelled, vec!["a"]);
    c.trading().cancel_all_markets().await.unwrap();
    let reqs = server.received_requests().await.unwrap();
    assert_eq!(reqs.len(), 2, "a single call each, by default");
    assert_eq!(body(&reqs[0]), json!({"symbol": "BTC/USDT"}));
    assert_eq!(body(&reqs[1]), json!({}));
    assert!(
        reqs.iter()
            .all(|r| !r.headers.contains_key("idempotency-key"))
    );
}

#[tokio::test]
async fn cancel_all_v2_fields_and_unknown_symbol() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/trading/orders/cancel-all"))
        .respond_with(Sequence::new(vec![
            data(json!({"cancelled": ["a"], "already_closed": ["b"], "failed": ["c"], "has_more": true,
                        "failures": [{"order_id": "c", "code": "MARKET_UNAVAILABLE", "message": "halted"}]})),
            api_error(404, "NOT_FOUND", false),
        ]))
        .mount(&server)
        .await;
    let (c, _) = client(&server);
    let r = c.trading().cancel_all("BTC/USDT").await.unwrap();
    assert_eq!(
        (r.already_closed.clone(), r.failed.clone(), r.has_more),
        (vec!["b".to_string()], vec!["c".to_string()], true)
    );
    assert_eq!(r.failures[0].code, "MARKET_UNAVAILABLE");
    assert!(
        c.trading()
            .cancel_all("NOPE/USDT")
            .await
            .unwrap_err()
            .is(ErrorCategory::NotFound)
    );
}

#[tokio::test]
async fn an_undecodable_order_response_carries_the_client_order_id() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/trading/orders"))
        .respond_with(data(json!({"order": {"id": 42}})))
        .mount(&server)
        .await;
    let (c, _) = client(&server);
    let mut req = limit_order();
    req.client_order_id = Some("cid-decode".into());
    match c.trading().place_order(&req).await {
        Err(Error::OrderStateUnknown {
            client_order_id,
            source,
        }) => {
            assert_eq!(client_order_id, "cid-decode");
            assert!(matches!(*source, Error::Decode(_)));
        }
        other => panic!("{other:?}"),
    }
}

fn armed(symbol: Value, timeout_ms: i64) -> ResponseTemplate {
    data(json!({
        "armed": timeout_ms != 0,
        "deadline": if timeout_ms != 0 { json!("2026-10-07T12:00:10Z") } else { Value::Null },
        "server_time": "2026-10-07T12:00:00Z",
        "symbol": symbol,
        "timeout_ms": timeout_ms,
    }))
}

const AFTER_PATH: &str = "/api/v1/trading/orders/cancel-all-after";

#[tokio::test]
async fn cancel_all_after_bodies_and_response() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(AFTER_PATH))
        .respond_with(armed(json!("BTC/USDT"), 10_000))
        .mount(&server)
        .await;
    let (c, _) = client(&server);
    let r = c
        .trading()
        .cancel_all_after("BTC/USDT", Duration::from_secs(10))
        .await
        .unwrap();
    assert!(r.armed);
    assert_eq!(r.timeout_ms, 10_000);
    assert_eq!(r.symbol.as_deref(), Some("BTC/USDT"));
    assert!(r.deadline.is_some());
    c.trading()
        .cancel_all_after_markets(Duration::from_secs(10))
        .await
        .unwrap();
    c.trading()
        .cancel_all_after("BTC/USDT", Duration::ZERO)
        .await
        .unwrap();
    let reqs = server.received_requests().await.unwrap();
    assert_eq!(reqs.len(), 3);
    assert_eq!(
        body(&reqs[0]),
        json!({"symbol": "BTC/USDT", "timeout_ms": 10000})
    );
    // Every market is an explicit null, not an omitted field.
    assert_eq!(body(&reqs[1]), json!({"symbol": null, "timeout_ms": 10000}));
    assert_eq!(
        body(&reqs[2]),
        json!({"symbol": "BTC/USDT", "timeout_ms": 0})
    );
    assert!(
        reqs.iter()
            .all(|r| !r.headers.contains_key("idempotency-key"))
    );
}

#[tokio::test]
async fn cancel_all_after_disarm_decodes_null_deadline() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(AFTER_PATH))
        .respond_with(armed(Value::Null, 0))
        .mount(&server)
        .await;
    let (c, _) = client(&server);
    let r = c
        .trading()
        .cancel_all_after_markets(Duration::ZERO)
        .await
        .unwrap();
    assert!(!r.armed);
    assert!(r.deadline.is_none());
    assert!(r.symbol.is_none());
    let reqs = server.received_requests().await.unwrap();
    assert_eq!(body(&reqs[0]), json!({"symbol": null, "timeout_ms": 0}));
}

#[tokio::test]
async fn cancel_all_after_rejects_bad_input_without_a_request() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(armed(Value::Null, 0))
        .mount(&server)
        .await;
    let (c, _) = client(&server);
    let t = c.trading();
    let ten = Duration::from_secs(10);
    for sym in ["", " ", "\t\n"] {
        assert!(matches!(
            t.cancel_all_after(sym, ten).await,
            Err(Error::Config(_))
        ));
    }
    for d in [
        Duration::from_micros(999),
        Duration::from_micros(1500),
        Duration::from_nanos(1),
        Duration::new(10, 1),
        Duration::MAX,
        Duration::from_millis(u64::MAX),
    ] {
        assert!(
            matches!(
                t.cancel_all_after("BTC/USDT", d).await,
                Err(Error::Config(_))
            ),
            "{d:?}"
        );
        assert!(
            matches!(t.cancel_all_after_markets(d).await, Err(Error::Config(_))),
            "{d:?}"
        );
    }
    assert!(server.received_requests().await.unwrap().is_empty());
    // The server owns the range check: 1 ms is sent as is.
    t.cancel_all_after("BTC/USDT", Duration::from_millis(1))
        .await
        .unwrap();
    let reqs = server.received_requests().await.unwrap();
    assert_eq!(body(&reqs[0])["timeout_ms"], json!(1));
}

#[tokio::test]
async fn cancel_all_after_is_retried_after_a_connection_error() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(AFTER_PATH))
        .respond_with(Sequence::new(vec![
            api_error(503, "SERVICE_UNAVAILABLE", true),
            armed(json!("BTC/USDT"), 10_000),
        ]))
        .mount(&server)
        .await;
    let (c, _) = client(&server);
    let r = c
        .trading()
        .cancel_all_after("BTC/USDT", Duration::from_secs(10))
        .await
        .unwrap();
    assert!(r.armed);
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
}

#[tokio::test]
async fn dead_man_not_armed_on_place_order_is_not_retried_or_recovered() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/trading/orders"))
        .respond_with(ResponseTemplate::new(409).set_body_json(json!({"error": {
            "code": "DEAD_MAN_NOT_ARMED",
            "message": "arm cancel-all-after first",
            "details": {"market": "BTC/USDT"},
            "retryable": false,
        }})))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex("^/api/v1/trading/orders/by-client-id/"))
        .respond_with(data(order("ord_9", "open")))
        .mount(&server)
        .await;
    let (c, _) = client(&server);
    let e = c.trading().place_order(&limit_order()).await.unwrap_err();
    let api = e.api().unwrap();
    assert_eq!(api.code, crate::ErrorCode::DeadManNotArmed);
    assert_eq!(api.details.get("market"), Some(&json!("BTC/USDT")));
    assert!(e.is(ErrorCategory::Conflict));
    assert!(!e.is_retryable());
    assert!(!e.is_ambiguous());
    // One POST, and no by-client-id lookup.
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}
