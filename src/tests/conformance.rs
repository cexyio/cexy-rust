//! conformance/auth.json, conformance/transport/*.json and conformance/ws/*.json from
//! cexy-api-spec.

use std::time::Duration;

use serde_json::{Value, json};
use wiremock::matchers::any;
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::helpers::*;
use crate::{Client, ClientOptions, ErrorCategory, OperationId};

fn op_for(operation: &str) -> OperationId {
    let (m, p) = operation.split_once(' ').unwrap();
    *OperationId::ALL
        .iter()
        .find(|o| o.info().method == m && o.info().path == p)
        .expect(operation)
}

async fn call(c: &Client, op: OperationId) -> crate::Result<()> {
    match op {
        OperationId::ListBalances => c.account().balances().await.map(|_| ()),
        OperationId::ListMarkets => c.markets().list().await.map(|_| ()),
        OperationId::ServerTime => c.time().await.map(|_| ()),
        other => panic!("no runner for {other:?}"),
    }
}

#[tokio::test]
async fn auth_cases() {
    let Some(file) = load("auth.json") else {
        return;
    };
    for case in file["cases"].as_array().unwrap() {
        let id = case["id"].as_str().unwrap();
        let expect = &case["expect"];
        if let Some(client) = case.get("client") {
            let o = ClientOptions {
                api_key: client["api_key"].as_str().map(str::to_string),
                api_secret: client["api_secret"].as_str().map(str::to_string),
                ..Default::default()
            };
            assert_eq!(
                Client::new(o).is_err(),
                expect["construct_error"] == json!(true),
                "{id}"
            );
            continue;
        }
        let server = MockServer::start().await;
        Mock::given(any())
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"data": []})))
            .mount(&server)
            .await;
        let (c, _) = client(&server);
        let op = op_for(case["request"]["operation"].as_str().unwrap());
        let _ = call(&c, op).await;
        let reqs = server.received_requests().await.unwrap();
        let req = reqs.last().unwrap_or_else(|| panic!("{id}: no request"));
        for h in expect["headers_present"].as_array().into_iter().flatten() {
            assert!(
                req.headers.contains_key(h.as_str().unwrap()),
                "{id}: {h} present"
            );
        }
        for h in expect["headers_absent"].as_array().into_iter().flatten() {
            assert!(
                !req.headers.contains_key(h.as_str().unwrap()),
                "{id}: {h} absent"
            );
        }
        for s in expect["url_must_not_contain"]
            .as_array()
            .into_iter()
            .flatten()
        {
            assert!(
                !req.url.as_str().contains(s.as_str().unwrap()),
                "{id}: {s} not in URL"
            );
        }
        if expect.get("secret_not_in").is_some() {
            assert!(!format!("{c:?}").contains(SECRET), "{id}: client Debug");
        }
        if let Some(m) = expect["header_matches"].as_object() {
            for (h, pattern) in m {
                let pattern = pattern.as_str().unwrap();
                let value = req.headers.get(h.as_str()).unwrap().to_str().unwrap();
                assert!(
                    regex::Regex::new(pattern).unwrap().is_match(value),
                    "{id}: {h} = {value:?} does not match {pattern}"
                );
            }
        }
    }
    for case in file["server_responses"].as_array().unwrap() {
        let server = MockServer::start().await;
        Mock::given(any())
            .respond_with(
                ResponseTemplate::new(case["status"].as_u64().unwrap() as u16)
                    .set_body_json(case["body"].clone()),
            )
            .mount(&server)
            .await;
        let (c, _) = client(&server);
        let e = c.account().balances().await.unwrap_err();
        let want = match case["expect"]["error_class"].as_str().unwrap() {
            "AuthenticationError" => ErrorCategory::Authentication,
            "PermissionError" => ErrorCategory::Forbidden,
            other => panic!("unmapped error class {other}"),
        };
        assert!(e.is(want), "{}: {e}", case["id"]);
    }
}

#[test]
fn ws_frames_decode() {
    let Some(welcome) = load("ws/welcome.json") else {
        return;
    };
    let w: crate::Welcome = serde_json::from_value(welcome).unwrap();
    assert_eq!((w.protocol_version, w.max_subscriptions), (1, 100));

    let ob: crate::WsFrame =
        serde_json::from_value(load("ws/orderbook_update.json").unwrap()).unwrap();
    assert_eq!(ob.sequence, Some(1042));
    let d: crate::OrderBookUpdate = ob.decode().unwrap();
    assert!(d.full);
    assert_eq!(crate::levels(&d.bids)[0].price.as_str(), "61000.10");

    let rev: crate::WsFrame =
        serde_json::from_value(load("ws/session_revoked.json").unwrap()).unwrap();
    let r: crate::SessionRevoked = rev.decode().unwrap();
    assert_eq!(
        (r.session_id, r.reason.as_str(), r.current),
        (None, "logout_all", true)
    );

    let cm: Value = load("ws/concurrent_modification.json").unwrap();
    assert_eq!(cm["code"], "CONCURRENT_MODIFICATION");
    assert!(cm["id"].is_null());
}

/// conformance/transport/server_waits.json: server wait hints are bounded and never panic.
#[tokio::test]
async fn server_wait_cases() {
    let Some(file) = load("transport/server_waits.json") else {
        return;
    };
    let cap = file["max_server_wait_s"].as_f64().unwrap();
    assert_eq!(crate::MAX_SERVER_WAIT.as_secs_f64(), cap);
    for case in file["cases"].as_array().unwrap() {
        let id = case["id"].as_str().unwrap();
        let replies: Vec<ResponseTemplate> = case["responses"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| {
                let status = r["http_status"].as_u64().unwrap() as u16;
                let body = if r.get("error").is_some() {
                    json!({"error": r["error"]})
                } else {
                    r["body"].clone()
                };
                let mut t = ResponseTemplate::new(status).set_body_json(body);
                for (k, v) in r["headers"].as_object().into_iter().flatten() {
                    t = t.insert_header(k.as_str(), v.as_str().unwrap());
                }
                t
            })
            .collect();
        let uses_limiter = case["responses"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["headers"].get("X-RateLimit-Remaining").is_some());
        let server = MockServer::start().await;
        Mock::given(any())
            .respond_with(Sequence::new(replies))
            .mount(&server)
            .await;
        let (c, clock) = client_with(&server, false, |o| o.disable_rate_limit = !uses_limiter);
        let mut result = c.time().await.map(|_| ());
        for _ in 0..case["then_calls"].as_u64().unwrap_or(0) {
            result = c.time().await.map(|_| ());
        }
        let e = &case["expect"];
        let calls = server.received_requests().await.unwrap().len() as u64;
        assert_eq!(calls, e["calls"].as_u64().unwrap(), "{id}: calls");
        let sleeps: Vec<f64> = clock.sleeps().iter().map(Duration::as_secs_f64).collect();
        if let Some(want) = e["sleeps_s"].as_array() {
            assert_eq!(sleeps.len(), want.len(), "{id}: sleeps {sleeps:?}");
            for (got, w) in sleeps.iter().zip(want) {
                let w = w.as_f64().unwrap();
                assert!(*got >= w && *got <= w + 1.0, "{id}: slept {got}, want {w}");
            }
        }
        if let Some(max) = e["no_sleep_longer_than_s"].as_f64() {
            assert!(sleeps.iter().all(|s| *s <= max), "{id}: {sleeps:?}");
        }
        if e["ok"] == json!(true) {
            assert!(result.is_ok(), "{id}: {result:?}");
        }
        if let Some(code) = e["error_code"].as_str() {
            let err = result.expect_err(id);
            let api = err.api().unwrap_or_else(|| panic!("{id}: {err}"));
            assert_eq!(api.code.as_str(), code, "{id}");
            if let Some(ra) = e["retry_after_s"].as_f64() {
                assert_eq!(api.retry_after, Some(Duration::from_secs_f64(ra)), "{id}");
            }
        }
    }
}
