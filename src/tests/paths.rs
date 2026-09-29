//! Path values stay one URL segment; which failures are retried (4xx, mutations).

use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::helpers::*;
use crate::transport::Call;
use crate::{
    Amount, Error, ErrorCategory, JoinPoolRequest, OperationId, OrderSide, OrderType,
    PlaceOrderRequest,
};

fn join_req() -> JoinPoolRequest {
    JoinPoolRequest::new(Amount::new("1").unwrap(), Amount::new("2").unwrap())
}

#[tokio::test]
async fn builder_rejects_dot_segments() {
    // The url crate resolves dot segments (even as %2E): the request would reach another route.
    let server = MockServer::start().await;
    let (c, _) = client_with(&server, true, |_| {});
    for v in [".", ".."] {
        let e =
            c.t.build_url(&Call::new(OperationId::SubAccountBalances).path("id", v))
                .unwrap_err();
        assert!(matches!(e, Error::Config(_)), "{v:?}: {e}");
    }
}

#[tokio::test]
async fn other_values_stay_one_segment() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(data(json!([])))
        .mount(&server)
        .await;
    let (c, _) = client_with(&server, true, |_| {});
    let cases = [
        ("a/b", "a%2Fb"),
        ("%2F", "%252F"),
        ("a?b", "a%3Fb"),
        ("a#b", "a%23b"),
        ("é✓", "%C3%A9%E2%9C%93"),
        ("%2e%2e", "%252e%252e"),
        ("a b", "a%20b"),
        ("...", "..."),
        (".a", ".a"),
    ];
    for (i, (v, seg)) in cases.iter().enumerate() {
        let url =
            c.t.build_url(&Call::new(OperationId::SubAccountBalances).path("id", v))
                .unwrap();
        let want = format!("/api/v1/account/sub-accounts/{seg}/balances");
        assert_eq!(url.path(), want, "{v:?}");
        assert!(url.query().is_none() && url.fragment().is_none(), "{v:?}");
        c.account().sub_account_balances(v).await.unwrap();
        let reqs = server.received_requests().await.unwrap();
        assert_eq!(reqs[i].url.path(), want, "{v:?}");
        assert!(reqs[i].url.query().is_none(), "{v:?}");
    }
}

#[tokio::test]
async fn a_get_and_mutations_reject_dot_segments_before_any_request() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(data(json!({})))
        .mount(&server)
        .await;
    let (c, _) = client_with(&server, true, |_| {});
    for v in [".", ".."] {
        let errs = [
            c.account().sub_account_balances(v).await.unwrap_err(),
            c.trading().order_by_client_id(v).await.unwrap_err(),
            c.trading().cancel_order(v).await.unwrap_err(),
            c.pools().join(v, &join_req()).await.unwrap_err(),
        ];
        for e in errs {
            assert!(matches!(e, Error::Config(_)), "{v:?}: {e}");
        }
    }
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn a_4xx_is_never_retried_even_if_marked_retryable() {
    for (status, code) in [
        (400, "VALIDATION_FAILED"),
        (404, "NOT_FOUND"),
        (408, "HTTP_408"),
        (409, "ALREADY_EXISTS"),
        (422, "INVALID_STATE"),
    ] {
        let server = MockServer::start().await;
        Mock::given(path("/api/v1/markets"))
            .respond_with(Sequence::new(vec![
                api_error(status, code, true),
                data(json!([])),
            ]))
            .mount(&server)
            .await;
        let (c, _) = client_with(&server, false, |_| {});
        assert!(c.markets().list().await.is_err(), "{status} {code}");
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            1,
            "{status} {code}"
        );
    }
}

#[tokio::test]
async fn rate_limit_and_concurrent_modification_are_still_retried() {
    for (status, code) in [(429, "RATE_LIMITED"), (409, "CONCURRENT_MODIFICATION")] {
        let server = MockServer::start().await;
        Mock::given(path("/api/v1/markets"))
            .respond_with(Sequence::new(vec![
                api_error(status, code, true),
                data(json!([])),
            ]))
            .mount(&server)
            .await;
        let (c, _) = client_with(&server, false, |_| {});
        c.markets().list().await.unwrap();
        assert_eq!(server.received_requests().await.unwrap().len(), 2, "{code}");
    }
}

#[tokio::test]
async fn sub_account_balances_404_variants_are_not_found_with_one_request() {
    let bodies = [
        ResponseTemplate::new(404).set_body_json(
            json!({"error": {"code": "NOT_FOUND", "message": "no such sub-account"}}),
        ),
        ResponseTemplate::new(404).set_body_raw("<html>not found</html>", "text/html"),
        api_error(404, "NOT_FOUND", true),
    ];
    for (i, reply) in bodies.into_iter().enumerate() {
        let server = MockServer::start().await;
        Mock::given(path("/api/v1/account/sub-accounts/other/balances"))
            .respond_with(Sequence::new(vec![reply, data(json!([]))]))
            .mount(&server)
            .await;
        let (c, _) = client_with(&server, true, |_| {});
        let e = c.account().sub_account_balances("other").await.unwrap_err();
        assert!(e.is(ErrorCategory::NotFound), "case {i}: {e}");
        if i < 2 {
            // No retryable field / no JSON body: the status default applies.
            assert!(!e.api().unwrap().retryable, "case {i}: {e}");
        }
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            1,
            "case {i}"
        );
    }
}

#[tokio::test]
async fn place_order_does_not_retry_another_409_even_if_retryable() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(api_error(409, "ALREADY_EXISTS", true))
        .mount(&server)
        .await;
    let (c, _) = client_with(&server, true, |_| {});
    let mut o = PlaceOrderRequest::new("BTC/USDT", OrderSide::Buy, OrderType::Market);
    o.quantity = Some(Amount::new("0.01").unwrap());
    let e = c.trading().place_order(&o).await.unwrap_err();
    assert!(e.is(ErrorCategory::Conflict), "{e}");
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn a_mutation_that_is_not_repeat_safe_is_sent_once() {
    // Every public mutation is repeat-safe (place_order and cancel_order have their own
    // policies; pool join/exit carry an Idempotency-Key; cancel-all is repeatable), so exercise
    // the transport rule directly: an order sent through `request` without a key.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(api_error(409, "CONCURRENT_MODIFICATION", true))
        .mount(&server)
        .await;
    let (c, _) = client_with(&server, true, |_| {});
    let mut call = Call::new(OperationId::PlaceOrder)
        .json(&json!({"symbol": "BTC/USDT"}))
        .unwrap();
    call.no_idempotency_key = true;
    let Err(e) = c.t.request(call, &c.resolved()).await else {
        panic!("want an error");
    };
    assert!(e.is(ErrorCategory::Conflict), "{e}");
    let reqs = server.received_requests().await.unwrap();
    assert_eq!(reqs.len(), 1);
    assert!(reqs[0].headers.get("idempotency-key").is_none());
}
