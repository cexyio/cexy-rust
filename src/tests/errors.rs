use std::time::Duration;

use serde_json::{Value, json};
use wiremock::matchers::path;
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::helpers::*;
use crate::{Error, ErrorCategory, ErrorCode};

fn template(case: &Value) -> ResponseTemplate {
    let mut t = ResponseTemplate::new(case["status"].as_u64().unwrap() as u16)
        .set_body_json(case["body"].clone());
    if let Some(h) = case.get("headers").and_then(Value::as_object) {
        for (k, v) in h {
            t = t.insert_header(k.as_str(), v.as_str().unwrap());
        }
    }
    t
}

/// conformance/errors/*.json: the error mapping and retry behaviour every SDK must share.
#[tokio::test]
async fn shared_error_cases() {
    for name in [
        "rate_limited",
        "idempotency_in_flight",
        "insufficient_funds",
        "dead_man_not_armed",
        "unknown_code",
    ] {
        let Some(case) = load(&format!("errors/{name}.json")) else {
            return;
        };
        let expect = &case["expect"];
        let server = MockServer::start().await;
        Mock::given(path("/api/v1/pools/BTC%2FUSDT/join"))
            .respond_with(Sequence::new(vec![template(&case), data(json!({}))]))
            .mount(&server)
            .await;
        let (c, clock) = client(&server);
        let req = crate::JoinPoolRequest::new(
            crate::Amount::new("1").unwrap(),
            crate::Amount::new("1").unwrap(),
        );
        let res = c.pools().join("BTC/USDT", &req).await;
        let reqs = server.received_requests().await.unwrap();
        if expect["retry"] == json!(true) {
            assert_eq!(reqs.len(), 2, "{name}: retried once");
        } else {
            assert_eq!(reqs.len(), 1, "{name}: not retried");
            let e = res.as_ref().unwrap_err();
            let api = e.api().unwrap();
            if let Some(code) = expect["error_code"].as_str() {
                assert_eq!(api.code.as_str(), code);
            }
            if let Some(details) = expect["details"].as_object() {
                for (k, v) in details {
                    assert_eq!(api.details.get(k), Some(v), "{name}: details.{k}");
                }
            }
        }
        if let Some(w) = expect["wait_seconds_at_least"].as_f64() {
            assert!(
                clock.sleeps()[0] >= Duration::from_secs_f64(w),
                "{name}: waited {:?}",
                clock.sleeps()
            );
        }
        if expect["same_idempotency_key"] == json!(true) {
            let k = |i: usize| reqs[i].headers.get("idempotency-key").unwrap().clone();
            assert_eq!(k(0), k(1), "{name}: same Idempotency-Key");
        }
        if expect["error_class"] == json!("CexyApiError") {
            // A code this SDK does not know: no category, the code passes through, no crash.
            let api = res.as_ref().unwrap_err().api().unwrap();
            assert_eq!(api.category(), None);
            assert!(!api.code.is_known());
        }
    }
}

#[tokio::test]
async fn status_mapping_and_proxy_pages() {
    let server = MockServer::start().await;
    let cases = [
        (400, "VALIDATION_FAILED", ErrorCategory::Validation),
        (401, "UNAUTHENTICATED", ErrorCategory::Authentication),
        (403, "FORBIDDEN", ErrorCategory::Forbidden),
        (404, "NOT_FOUND", ErrorCategory::NotFound),
        (409, "ALREADY_EXISTS", ErrorCategory::Conflict),
        (422, "INSUFFICIENT_FUNDS", ErrorCategory::Unprocessable),
    ];
    for (status, code, cat) in cases {
        server.reset().await;
        Mock::given(path("/api/v1/assets"))
            .respond_with(api_error(status, code, false))
            .mount(&server)
            .await;
        let (c, _) = client(&server);
        let e = c.assets().list().await.unwrap_err();
        assert!(e.is(cat), "{status}: {e}");
    }
    server.reset().await;
    Mock::given(path("/api/v1/assets"))
        .respond_with(api_error(451, "JURISDICTION_BLOCKED", false))
        .mount(&server)
        .await;
    let (c, _) = client(&server);
    let e = c.assets().list().await.unwrap_err();
    assert!(e.is(ErrorCategory::JurisdictionBlocked) && e.is(ErrorCategory::Forbidden));

    server.reset().await;
    Mock::given(path("/api/v1/assets"))
        .respond_with(ResponseTemplate::new(502).set_body_string("<html>bad gateway</html>"))
        .mount(&server)
        .await;
    let (c, _) = client_with(&server, false, |o| o.max_retries = Some(0));
    let e = c.assets().list().await.unwrap_err();
    let api = e.api().unwrap();
    assert_eq!(api.code, ErrorCode::Other("HTTP_502".into()));
    assert!(e.is(ErrorCategory::Server) && api.retryable);
}

#[tokio::test]
async fn secrets_echoed_by_the_server_are_redacted() {
    let server = MockServer::start().await;
    Mock::given(path("/api/v1/account/balances"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({"error": {
            "code": "VALIDATION_FAILED",
            "message": format!("bad key {KEY} / {SECRET}"),
            "details": {"got": SECRET, "n": 3,
                        "nested": {"list": [SECRET, {"deep": format!("k={KEY}")}], SECRET: 1}},
            "fields": {"x": format!("was {SECRET}"), SECRET: "field named after the secret"},
            "request_id": format!("req-{SECRET}"),
        }})))
        .mount(&server)
        .await;
    let (c, _) = client(&server);
    let e = c.account().balances().await.unwrap_err();
    for text in [e.to_string(), format!("{e:?}")] {
        assert!(!text.contains(SECRET) && !text.contains(KEY), "{text}");
    }
    let api = e.api().unwrap();
    assert_eq!(api.details["got"], json!("[REDACTED]"));
    assert_eq!(api.details["n"], json!(3));
    assert_eq!(api.fields["x"], "was [REDACTED]");
    // Nested values, array items and object keys are redacted too, at any depth.
    let all = serde_json::to_string(&api.details).unwrap()
        + &serde_json::to_string(&api.fields).unwrap()
        + api.request_id.as_deref().unwrap_or_default();
    assert!(!all.contains(SECRET) && !all.contains(KEY), "{all}");
    assert_eq!(api.details["nested"]["list"][0], json!("[REDACTED]"));
    assert_eq!(api.details["nested"]["[REDACTED]"], json!(1));
}

#[tokio::test]
async fn connection_errors_are_retryable_and_redacted() {
    let (c, clock) = {
        let o = crate::ClientOptions {
            base_url: Some("http://127.0.0.1:9".into()),
            allow_insecure: true,
            disable_rate_limit: true,
            max_retries: Some(1),
            ..crate::ClientOptions::with_api_key(KEY, SECRET)
        };
        let clock = crate::clock::FakeClock::default();
        (
            crate::Client::build(
                o,
                std::sync::Arc::new(clock.clone()),
                std::sync::Arc::new(|| 0.5),
            )
            .unwrap(),
            clock,
        )
    };
    let e = c.account().balances().await.unwrap_err();
    assert!(matches!(e, Error::Connection(_)) && e.is_retryable(), "{e}");
    assert_eq!(clock.sleeps().len(), 1);
}
