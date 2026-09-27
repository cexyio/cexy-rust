//! conformance/trading/cancel_all_until_done.json, plus the review's extra cases.

use std::collections::BTreeSet;
use std::time::Duration;

use serde_json::{Value, json};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::helpers::*;
use crate::{CancelAllOptions, CancelAllStop, Error};

fn set(v: &Value) -> BTreeSet<String> {
    v.as_array()
        .unwrap()
        .iter()
        .map(|x| x.as_str().unwrap().to_string())
        .collect()
}

fn set_of(v: &[String]) -> BTreeSet<String> {
    v.iter().cloned().collect()
}

/// A conformance response item: a `data` object (200), or `{http_status, headers, error}`.
fn reply(r: &Value) -> ResponseTemplate {
    let Some(status) = r.get("http_status").and_then(Value::as_u64) else {
        return data(r.clone());
    };
    let mut t = ResponseTemplate::new(status as u16).set_body_json(json!({"error": r["error"]}));
    for (k, v) in r["headers"].as_object().into_iter().flatten() {
        t = t.insert_header(k.as_str(), v.as_str().unwrap());
    }
    t
}

async fn server_with(replies: Vec<ResponseTemplate>) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/trading/orders/cancel-all"))
        .respond_with(Sequence::new(replies))
        .mount(&server)
        .await;
    server
}

#[tokio::test]
async fn shared_cases() {
    let Some(file) = load("trading/cancel_all_until_done.json") else {
        return;
    };
    for case in file["cases"].as_array().unwrap() {
        let id = case["id"].as_str().unwrap();
        let replies: Vec<_> = case["responses"]
            .as_array()
            .unwrap()
            .iter()
            .map(reply)
            .collect();
        let server = server_with(replies).await;
        let (c, clock) = client(&server);
        let mut o = CancelAllOptions::symbol("BTC/USDT");
        if let Some(n) = case.pointer("/options/max_rounds").and_then(Value::as_u64) {
            o.max_rounds = n as u32;
        }
        if let Some(s) = case
            .pointer("/options/time_budget_s")
            .and_then(Value::as_f64)
        {
            o.time_budget = Duration::from_secs_f64(s);
        }
        let result = c.trading().cancel_all_until_done(&o).await;
        let e = &case["expect"];
        let reqs = server.received_requests().await.unwrap();
        assert_eq!(
            reqs.len() as u64,
            e["calls"].as_u64().unwrap(),
            "{id}: calls"
        );
        if let Some(code) = e["error_code"].as_str() {
            match result {
                Err(Error::CancelAllInterrupted { source, summary }) => {
                    assert_eq!(source.api().unwrap().code.as_str(), code, "{id}: error");
                    assert_eq!(
                        set_of(&summary.cancelled),
                        set(&e["partial_cancelled"]),
                        "{id}: partial"
                    );
                }
                other => panic!("{id}: expected an interrupted loop, got {other:?}"),
            }
            continue;
        }
        let r = result.unwrap();
        assert_eq!(
            r.last_error_code.as_deref(),
            e["last_error_code"].as_str(),
            "{id}: last_error_code"
        );
        assert!(
            reqs.iter()
                .all(|r| !r.headers.contains_key("idempotency-key")),
            "{id}: no Idempotency-Key"
        );
        let sleeps: Vec<f64> = clock.sleeps().iter().map(Duration::as_secs_f64).collect();
        let want: Vec<f64> = e["sleeps_s"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_f64().unwrap())
            .collect();
        assert_eq!(sleeps, want, "{id}: sleeps");
        assert_eq!(
            r.stopped.as_str(),
            e["stopped"].as_str().unwrap(),
            "{id}: stopped"
        );
        assert_eq!(
            set_of(&r.cancelled),
            set(&e["cancelled"]),
            "{id}: cancelled"
        );
        assert_eq!(
            set_of(&r.already_closed),
            set(&e["already_closed"]),
            "{id}: already_closed"
        );
        assert_eq!(set_of(&r.failed), set(&e["failed"]), "{id}: failed");
        let codes: serde_json::Map<String, Value> = r
            .failures
            .iter()
            .map(|f| (f.order_id.clone(), Value::String(f.code.clone())))
            .collect();
        assert_eq!(
            Value::Object(codes),
            e["failure_codes"],
            "{id}: failure codes"
        );
        assert_eq!(
            r.rounds as u64,
            e["calls"].as_u64().unwrap(),
            "{id}: rounds"
        );
    }
}

#[tokio::test]
async fn twenty_rounds_with_progress_send_at_most_twenty_requests() {
    let server = server_with(vec![data(
        json!({"cancelled": ["x"], "already_closed": [], "failed": [], "failures": [], "has_more": true}),
    )])
    .await;
    let (c, clock) = client(&server);
    let r = c
        .trading()
        .cancel_all_until_done(&CancelAllOptions::all_markets())
        .await
        .unwrap();
    assert_eq!(server.received_requests().await.unwrap().len(), 20);
    assert_eq!((r.rounds, r.stopped), (20, CancelAllStop::MaxRounds));
    assert!(clock.sleeps().is_empty());
    let body: Value =
        serde_json::from_slice(&server.received_requests().await.unwrap()[0].body).unwrap();
    assert_eq!(
        body,
        json!({}),
        "every market, sent explicitly as no symbol"
    );
}

#[tokio::test]
async fn a_429_retry_after_counts_against_the_budget() {
    let stuck = json!({"cancelled": [], "already_closed": [], "failed": ["p1"], "has_more": false,
                       "failures": [{"order_id": "p1", "code": "INVALID_STATE", "message": "still being placed"}]});
    let server = server_with(vec![
        ResponseTemplate::new(429)
            .insert_header("Retry-After", "119")
            .set_body_json(
                json!({"error": {"code": "RATE_LIMITED", "message": "slow down", "retryable": true,
                                            "details": {"retry_after_seconds": 119}}}),
            ),
        data(stuck),
    ])
    .await;
    let (c, clock) = client(&server);
    let r = c
        .trading()
        .cancel_all_until_done(&CancelAllOptions::symbol("BTC/USDT"))
        .await
        .unwrap();
    // The loop (not the transport) waited exactly the 119 s Retry-After; the next 1 s
    // back-off would reach the 120 s budget, so it stops there. Two requests in all.
    assert_eq!(r.stopped, CancelAllStop::TimeBudget);
    assert_eq!(clock.sleeps(), vec![Duration::from_secs(119)]);
    assert_eq!(r.failed, vec!["p1"]);
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
}

#[tokio::test]
async fn an_order_that_ends_closed_is_never_also_failed() {
    let pending = |id: &str| json!({"order_id": id, "code": "INVALID_STATE", "message": "still being placed"});
    let server = server_with(vec![
        data(json!({"cancelled": [], "already_closed": [], "failed": ["p1", "p2"], "has_more": false,
                    "failures": [pending("p1"), pending("p2")]})),
        data(json!({"cancelled": ["p1"], "already_closed": ["p2"], "failed": [], "failures": [], "has_more": false})),
    ])
    .await;
    let (c, _) = client(&server);
    let r = c
        .trading()
        .cancel_all_until_done(&CancelAllOptions::symbol("BTC/USDT"))
        .await
        .unwrap();
    assert_eq!(
        (r.cancelled.clone(), r.already_closed.clone()),
        (vec!["p1".to_string()], vec!["p2".to_string()])
    );
    assert!(r.failed.is_empty() && r.failures.is_empty());
}

#[tokio::test]
async fn an_error_returns_the_summary_so_far() {
    let server = server_with(vec![
        data(json!({"cancelled": ["a"], "already_closed": [], "failed": [], "failures": [], "has_more": true})),
        api_error(403, "FORBIDDEN", false),
    ])
    .await;
    let (c, _) = client(&server);
    match c
        .trading()
        .cancel_all_until_done(&CancelAllOptions::symbol("BTC/USDT"))
        .await
    {
        Err(Error::CancelAllInterrupted { source, summary }) => {
            assert!(source.is(crate::ErrorCategory::Forbidden));
            assert_eq!(
                (summary.cancelled.clone(), summary.rounds, summary.stopped),
                (vec!["a".to_string()], 2, CancelAllStop::Error)
            );
        }
        other => panic!("{other:?}"),
    }
    let mut empty = CancelAllOptions::symbol("");
    empty.max_rounds = 1;
    assert!(matches!(
        c.trading().cancel_all_until_done(&empty).await,
        Err(Error::Config(_))
    ));
}

#[tokio::test]
async fn transport_retries_are_off_inside_the_loop() {
    // QA probe: 503 and progress alternate. Before, each round retried its 503 in the
    // transport (up to 3 times), so 20 rounds sent 40 requests. Now a round is one request.
    let prog = json!({"cancelled": ["x"], "already_closed": [], "failed": [], "failures": [], "has_more": true});
    let server = server_with(vec![
        api_error(503, "SERVICE_UNAVAILABLE", true),
        data(prog.clone()),
        api_error(503, "SERVICE_UNAVAILABLE", true),
        data(prog.clone()),
        api_error(503, "SERVICE_UNAVAILABLE", true),
        data(prog),
    ])
    .await;
    let (c, _) = client(&server);
    let r = c
        .trading()
        .cancel_all_until_done(&CancelAllOptions::all_markets())
        .await
        .unwrap();
    let requests = server.received_requests().await.unwrap().len() as u32;
    assert_eq!(requests, r.rounds);
    assert!(requests <= 20);
}

#[tokio::test]
async fn a_retry_after_beyond_the_budget_is_not_waited() {
    let server = server_with(vec![
        ResponseTemplate::new(429)
            .insert_header("Retry-After", "300")
            .set_body_json(
                json!({"error": {"code": "RATE_LIMITED", "message": "slow", "retryable": true}}),
            ),
    ])
    .await;
    let (c, clock) = client(&server);
    let r = c
        .trading()
        .cancel_all_until_done(&CancelAllOptions::symbol("BTC/USDT"))
        .await
        .unwrap();
    assert_eq!(r.stopped, CancelAllStop::TimeBudget);
    assert_eq!(r.last_error_code.as_deref(), Some("RATE_LIMITED"));
    assert!(clock.sleeps().is_empty());
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn a_rate_limiter_block_counts_against_the_budget() {
    // QA probe: a successful round that exhausts the rate limit (Remaining 0, Reset 170 s) would
    // make the limiter hold the next call for 170 s, past the 120 s budget. The loop must stop
    // instead of calling, without sleeping.
    let stuck = json!({"cancelled": [], "already_closed": [], "failed": ["p1"], "has_more": false,
                       "failures": [{"order_id": "p1", "code": "INVALID_STATE", "message": "still being placed"}]});
    let server = server_with(vec![
        data(stuck)
            .insert_header("X-RateLimit-Limit", "30")
            .insert_header("X-RateLimit-Remaining", "0")
            .insert_header("X-RateLimit-Reset", "170"),
    ])
    .await;
    let (c, clock) = client_with(&server, true, |o| o.disable_rate_limit = false);
    let r = c
        .trading()
        .cancel_all_until_done(&CancelAllOptions::symbol("BTC/USDT"))
        .await
        .unwrap();
    assert_eq!(r.stopped, CancelAllStop::TimeBudget);
    assert_eq!(r.last_error_code.as_deref(), Some("RATE_LIMITED"));
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
    assert!(clock.sleeps().iter().sum::<Duration>() < Duration::from_secs(120));
}
