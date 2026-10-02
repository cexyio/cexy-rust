use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::json;
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::helpers::*;
use crate::{CallOptions, Client, ClientOptions, Error, ErrorCategory, OperationId};

#[test]
fn surface_has_51_operations() {
    assert_eq!(OperationId::ALL.len(), 51);
    let place = OperationId::PlaceOrder.info();
    assert_eq!(
        (place.method, place.path, place.auth, place.scope),
        ("POST", "/api/v1/trading/orders", "api_key", "trade")
    );
    assert_eq!(OperationId::ServerTime.info().auth, "none");
}

#[test]
fn options_are_checked() {
    let err = |o: ClientOptions| Client::new(o).unwrap_err().to_string();
    assert!(
        err(ClientOptions {
            api_key: Some("ak_x".into()),
            ..Default::default()
        })
        .contains("together")
    );
    assert!(
        err(ClientOptions {
            base_url: Some("http://api.cexy.io".into()),
            ..Default::default()
        })
        .contains("https://")
    );
    assert!(
        err(ClientOptions {
            base_url: Some("http://example.com".into()),
            allow_insecure: true,
            ..Default::default()
        })
        .contains("only allowed for localhost")
    );
    assert!(
        err(ClientOptions {
            base_url: Some("https://u:p@api.cexy.io".into()),
            ..Default::default()
        })
        .contains("credentials")
    );
    assert!(
        err(ClientOptions {
            user_agent_suffix: Some("a\r\nb".into()),
            ..Default::default()
        })
        .contains("line breaks")
    );
    let ok = Client::new(ClientOptions {
        base_url: Some("http://127.0.0.1:1".into()),
        allow_insecure: true,
        ..Default::default()
    });
    assert!(ok.is_ok());
    let c = Client::new(ClientOptions {
        user_agent_suffix: Some("my-bot/1.2".into()),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(
        c.user_agent(),
        format!("cexy-rust/{} my-bot/1.2", crate::VERSION)
    );
    assert_eq!(c.rate_limit().unwrap().requests_per_minute, 100.0);
    let k = Client::new(ClientOptions::with_api_key(KEY, SECRET)).unwrap();
    assert_eq!(k.rate_limit().unwrap().requests_per_minute, 300.0);
}

#[test]
fn debug_output_never_shows_the_secret() {
    let o = ClientOptions::with_api_key(KEY, SECRET);
    let c = Client::new(o.clone()).unwrap();
    for text in [
        format!("{o:?}"),
        format!("{c:?}"),
        format!(
            "{:?}",
            crate::ApiKeyAuthenticator::new(KEY, SECRET).unwrap()
        ),
    ] {
        assert!(!text.contains(SECRET), "{text}");
        assert!(!text.contains(KEY), "{text}");
    }
}

#[tokio::test]
async fn public_call_decodes_the_envelope_and_sends_the_user_agent() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/time"))
        .and(header("user-agent", crate::USER_AGENT))
        .respond_with(data(
            json!({"epoch_ms": 1790500000000i64, "iso": "2026-09-27T10:00:00Z"}),
        ))
        .expect(1)
        .mount(&server)
        .await;
    let (c, _) = client_with(&server, false, |_| {});
    let t = c.time().await.unwrap();
    assert_eq!(t.epoch_ms, 1790500000000);
    assert!(t.time().is_some());
}

#[tokio::test]
async fn query_parameters_and_path_encoding() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/markets/BTC%2FUSDT/candles"))
        .and(query_param("interval", "1h"))
        .and(query_param("limit", "24"))
        .respond_with(data(json!([])))
        .expect(1)
        .mount(&server)
        .await;
    let (c, _) = client_with(&server, false, |_| {});
    let mut p = crate::GetCandlesParams::new(crate::CandleInterval::H1);
    p.limit = Some(24);
    assert!(
        c.markets()
            .candles("BTC/USDT", &p)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn private_call_without_credentials_fails_locally() {
    let server = MockServer::start().await;
    let (c, _) = client_with(&server, false, |_| {});
    let e = c.account().balances().await.unwrap_err();
    assert!(
        matches!(e, Error::Config(ref m) if m.contains("needs an API key")),
        "{e}"
    );
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn retries_retryable_errors_with_backoff_and_reports_them() {
    let server = MockServer::start().await;
    Mock::given(path("/api/v1/markets"))
        .respond_with(Sequence::new(vec![
            api_error(503, "SERVICE_UNAVAILABLE", true),
            api_error(502, "INTERNAL", true),
            data(json!([])),
        ]))
        .expect(3)
        .mount(&server)
        .await;
    let seen = Arc::new(Mutex::new(vec![]));
    let s2 = seen.clone();
    let (c, clock) = client_with(&server, false, move |o| {
        o.on_retry = Some(Arc::new(move |r: &crate::RetryInfo<'_>| {
            s2.lock().unwrap().push((r.attempt, r.delay))
        }));
    });
    c.markets().list().await.unwrap();
    // Full jitter with random 0.5: 250 ms, then 500 ms.
    assert_eq!(
        clock.sleeps(),
        vec![Duration::from_millis(250), Duration::from_millis(500)]
    );
    assert_eq!(
        *seen.lock().unwrap(),
        vec![
            (1, Duration::from_millis(250)),
            (2, Duration::from_millis(500))
        ]
    );
    assert_eq!(clock.elapsed(), Duration::from_millis(750));
}

#[tokio::test]
async fn max_retries_zero_and_non_retryable_errors_are_not_retried() {
    let server = MockServer::start().await;
    Mock::given(path("/api/v1/markets"))
        .respond_with(api_error(503, "SERVICE_UNAVAILABLE", true))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(path("/api/v1/assets"))
        .respond_with(api_error(400, "VALIDATION_FAILED", false))
        .expect(1)
        .mount(&server)
        .await;
    let (c, _) = client_with(&server, false, |_| {});
    let once = c.with_options(CallOptions {
        max_retries: Some(0),
        ..Default::default()
    });
    assert!(
        once.markets()
            .list()
            .await
            .unwrap_err()
            .is(ErrorCategory::Server)
    );
    assert!(
        c.assets()
            .list()
            .await
            .unwrap_err()
            .is(ErrorCategory::Validation)
    );
}

#[tokio::test]
async fn pool_join_reuses_one_idempotency_key_across_retries() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/pools/BTC%2FUSDT/join"))
        .respond_with(Sequence::new(vec![
            api_error(409, "CONCURRENT_MODIFICATION", true),
            data(json!({"shares_minted": "1", "base_used": "1", "quote_used": "1", "pool": null})),
        ]))
        .mount(&server)
        .await;
    let (c, _) = client(&server);
    let req = crate::JoinPoolRequest::new(
        crate::Amount::new("1").unwrap(),
        crate::Amount::new("2").unwrap(),
    );
    let _ = c.pools().join("BTC/USDT", &req).await;
    let reqs = server.received_requests().await.unwrap();
    assert_eq!(reqs.len(), 2);
    let keys: Vec<_> = reqs
        .iter()
        .map(|r| {
            r.headers
                .get("idempotency-key")
                .unwrap()
                .to_str()
                .unwrap()
                .to_string()
        })
        .collect();
    assert_eq!(keys[0], keys[1]);
    assert_eq!(keys[0].len(), 36);
}

#[tokio::test]
async fn invalid_amounts_are_rejected_before_sending() {
    let server = MockServer::start().await;
    let (c, _) = client(&server);
    let mut o =
        crate::PlaceOrderRequest::new("BTC/USDT", crate::OrderSide::Buy, crate::OrderType::Limit);
    o.price = Some(serde_json::from_value(json!("1e5")).unwrap());
    let e = c.trading().place_order(&o).await.unwrap_err();
    assert!(
        matches!(e, Error::InvalidAmount { ref field, .. } if field == "price"),
        "{e}"
    );
    let mut j = crate::JoinPoolRequest::new(
        crate::Amount::new("1").unwrap(),
        crate::Amount::new("1").unwrap(),
    );
    j.max_ratio_deviation_percent = Some(serde_json::from_value(json!("one")).unwrap());
    let e = c.pools().join("BTC/USDT", &j).await.unwrap_err();
    assert!(
        matches!(e, Error::InvalidAmount { ref field, .. } if field == "max_ratio_deviation_percent"),
        "{e}"
    );
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn csv_exports_return_text() {
    let server = MockServer::start().await;
    Mock::given(path("/api/v1/exports/orders"))
        .respond_with(ResponseTemplate::new(200).set_body_string("id,symbol\n1,BTC/USDT\n"))
        .mount(&server)
        .await;
    let (c, _) = client(&server);
    assert_eq!(
        c.exports().orders(None).await.unwrap(),
        "id,symbol\n1,BTC/USDT\n"
    );
    let reqs = server.received_requests().await.unwrap();
    assert_eq!(
        reqs[0].headers.get("accept").unwrap(),
        "text/csv, application/json"
    );
}

#[tokio::test]
async fn rate_limiter_blocks_after_a_429() {
    let server = MockServer::start().await;
    Mock::given(path("/api/v1/markets"))
        .respond_with(Sequence::new(vec![
            ResponseTemplate::new(429)
                .insert_header("Retry-After", "3")
                .set_body_json(json!({"error": {"code": "RATE_LIMITED", "message": "slow down", "retryable": true}})),
            data(json!([])),
        ]))
        .mount(&server)
        .await;
    let (c, clock) = client_with(&server, false, |o| o.disable_rate_limit = false);
    c.markets().list().await.unwrap();
    // Retry-After 3 s plus 125 ms jitter; the limiter's block has expired by then.
    assert_eq!(clock.sleeps(), vec![Duration::from_millis(3125)]);
}

#[tokio::test]
async fn sub_account_balances_path_auth_and_held_incoming() {
    let server = MockServer::start().await;
    let row = json!({"asset": "USDT", "available": "90.00", "locked": "10.00", "pending": "0", "total": "100.00",
        "held_incoming": [{"transfer_id": "cccccccccccccccccccccccc", "amount": "1.50", "available_at": "2026-09-30T10:00:00.001Z"}]});
    Mock::given(method("GET"))
        .and(path("/api/v1/account/sub-accounts/sub%2F1%20%3Fx/balances"))
        .and(header("x-api-key", KEY))
        .respond_with(data(json!([row])))
        .expect(1)
        .mount(&server)
        .await;
    let (c, _) = client_with(&server, true, |_| {});
    let bs = c.account().sub_account_balances("sub/1 ?x").await.unwrap();
    assert_eq!(bs.len(), 1);
    assert_eq!(bs[0].held_incoming.len(), 1);
    assert_eq!(bs[0].held_incoming[0].amount.as_str(), "1.50");
    let reqs = server.received_requests().await.unwrap();
    assert!(reqs[0].headers.get("idempotency-key").is_none());
}

#[tokio::test]
async fn sub_account_balances_missing_held_incoming_is_empty() {
    let server = MockServer::start().await;
    Mock::given(path("/api/v1/account/sub-accounts/sub_1/balances"))
        .respond_with(data(json!([{"asset": "USDT", "available": "1", "locked": "0", "pending": "0", "total": "1"}])))
        .mount(&server)
        .await;
    let (c, _) = client_with(&server, true, |_| {});
    let bs = c.account().sub_account_balances("sub_1").await.unwrap();
    assert!(bs[0].held_incoming.is_empty());
}

#[tokio::test]
async fn sub_account_balances_404_is_not_found_with_one_request() {
    let server = MockServer::start().await;
    Mock::given(path("/api/v1/account/sub-accounts/other/balances"))
        .respond_with(api_error(404, "NOT_FOUND", false))
        .expect(1)
        .mount(&server)
        .await;
    let (c, _) = client_with(&server, true, |_| {});
    let e = c.account().sub_account_balances("other").await.unwrap_err();
    assert!(e.is(ErrorCategory::NotFound), "{e}");
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn sub_account_balances_empty_id_is_rejected_before_any_request() {
    let server = MockServer::start().await;
    let (c, _) = client_with(&server, true, |_| {});
    let e = c.account().sub_account_balances("").await.unwrap_err();
    assert!(matches!(e, Error::Config(_)), "{e}");
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn account_id_path_and_auth() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/account/id"))
        .and(header("x-api-key", KEY))
        .respond_with(data(json!({"user_id": "aaaa0001"})))
        .expect(1)
        .mount(&server)
        .await;
    let (c, _) = client_with(&server, true, |_| {});
    assert_eq!(c.account().id().await.unwrap(), "aaaa0001");
}
