//! Batch place, batch cancel and cancel by client order id: every case of
//! conformance/trading/batch_orders.json from cexy-api-spec, plus the placement hold, the
//! order-actions budget and per-attempt signing.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use serde_json::{Value, json};
use wiremock::matchers::{any, method, path, path_regex};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

use super::helpers::*;
use crate::clock::FakeClock;
use crate::{
    Amount, BatchOrderInput, CallOptions, CancelBatchItem, Client, ClientOptions, Error, OrderSide,
    OrderType, PlaceOutcome,
};

const BATCH: &str = "/api/v1/trading/orders/batch";
const CANCEL_BATCH: &str = "/api/v1/trading/orders/cancel-batch";
const BY_CID: &str = "/api/v1/trading/orders/by-client-id/";
const ORDERS: &str = "/api/v1/trading/orders/";
/// Per-attempt timeout in these tests; a `network_error` answer is delayed beyond it.
const TIMEOUT: Duration = Duration::from_millis(150);

/// A client with the limiter on (high enough never to wait unless a test asks) and a short
/// timeout, on a fake clock.
fn batch_client(
    server: &MockServer,
    tweak: impl FnOnce(&mut ClientOptions),
) -> (Client, FakeClock) {
    client_with(server, true, |o| {
        o.disable_rate_limit = false;
        o.requests_per_minute = Some(6000);
        o.timeout = Some(TIMEOUT);
        tweak(o);
    })
}

fn overlay(template: &Value, fields: &Value) -> Value {
    let mut o = template.clone();
    for (k, v) in fields.as_object().unwrap() {
        o[k] = v.clone();
    }
    o
}

/// One answer of the conformance file as a response; `echo` is the request body, used for
/// `echo_cids`.
fn reply(item: &Value, template: &Value, echo: Option<&Value>) -> ResponseTemplate {
    if item.get("network_error").is_some() {
        return ResponseTemplate::new(200).set_delay(TIMEOUT * 10);
    }
    let status = item["http_status"].as_u64().unwrap() as u16;
    let mut t = if let Some(text) = item.get("text") {
        ResponseTemplate::new(status).set_body_raw(text.as_str().unwrap().as_bytes(), "text/html")
    } else if let Some(body) = item.get("body") {
        ResponseTemplate::new(status).set_body_json(body.clone())
    } else if let Some(err) = item.get("error") {
        ResponseTemplate::new(status).set_body_json(json!({ "error": err }))
    } else {
        let mut d = item["data"].clone();
        if let Some(results) = d.get_mut("results").and_then(Value::as_array_mut) {
            for (i, r) in results.iter_mut().enumerate() {
                if r["order"].is_object() {
                    r["order"] = overlay(template, &r["order"]);
                }
                if item["echo_cids"] == json!(true) {
                    r["client_order_id"] = echo.unwrap()["orders"][i]["client_order_id"].clone();
                }
            }
        } else {
            d = overlay(template, &d);
        }
        ResponseTemplate::new(status).set_body_json(json!({ "data": d }))
    };
    for (k, v) in item["headers"].as_object().into_iter().flatten() {
        t = t.insert_header(k.as_str(), v.as_str().unwrap());
    }
    t
}

/// Answers each attempt in turn.
struct Attempts {
    items: Vec<Value>,
    template: Value,
    n: Arc<AtomicUsize>,
}

impl Respond for Attempts {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let i = self.n.fetch_add(1, Ordering::SeqCst);
        let item = self
            .items
            .get(i)
            .unwrap_or_else(|| panic!("attempt {i} not expected"));
        let body: Option<Value> = serde_json::from_slice(&req.body).ok();
        reply(item, &self.template, body.as_ref())
    }
}

/// Answers a batch place or cancel with every item done, matching the request.
struct Echo;

impl Respond for Echo {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let b: Value = serde_json::from_slice(&req.body).unwrap();
        let items = b["orders"].as_array().unwrap();
        let results: Vec<Value> = items
            .iter()
            .enumerate()
            .map(|(i, it)| {
                if b.get("symbol").is_some() {
                    json!({"index": i, "client_order_id": it["client_order_id"],
                        "order": order(&format!("o{i}"), "open"), "fills": [], "error": null})
                } else {
                    json!({"index": i, "order_id": it.get("order_id"),
                        "client_order_id": it.get("client_order_id"),
                        "order": order(&format!("o{i}"), "cancelled"), "error": null})
                }
            })
            .collect();
        let n = results.len();
        if b.get("symbol").is_some() {
            data(json!({"placed": n, "failed": 0, "results": results}))
        } else {
            data(json!({"cancelled": n, "failed": 0, "results": results}))
        }
    }
}

async fn mount_case(server: &MockServer, verb: &str, p: &str, case: &Value, template: &Value) {
    Mock::given(method(verb))
        .and(path(p))
        .respond_with(Attempts {
            items: case["attempts"].as_array().unwrap().clone(),
            template: template.clone(),
            n: Arc::new(AtomicUsize::new(0)),
        })
        .mount(server)
        .await;
    for (key, item) in case["lookups"].as_object().into_iter().flatten() {
        let p = match key.split_once(':').unwrap() {
            ("cid", cid) => format!("{BY_CID}{cid}"),
            ("order", id) => format!("{ORDERS}{id}"),
            other => panic!("lookup key {other:?}"),
        };
        Mock::given(method("GET"))
            .and(path(p))
            .respond_with(reply(item, template, None))
            .mount(server)
            .await;
    }
}

/// The lookups sent, in order, as `cid:<cid>` / `order:<id>`.
fn lookups(reqs: &[Request]) -> Vec<String> {
    reqs.iter()
        .filter(|r| r.method.as_str() == "GET")
        .map(|r| {
            let p = r.url.path();
            match p.strip_prefix(BY_CID) {
                Some(cid) => format!("cid:{cid}"),
                None => format!("order:{}", p.strip_prefix(ORDERS).unwrap()),
            }
        })
        .collect()
}

fn calls_to<'a>(reqs: &'a [Request], verb: &str, p: &str) -> Vec<&'a Request> {
    reqs.iter()
        .filter(|r| r.method.as_str() == verb && r.url.path() == p)
        .collect()
}

fn amount(v: &Value) -> Option<Amount> {
    v.as_str().map(|s| Amount::new(s).unwrap())
}

fn limit_buy() -> BatchOrderInput {
    let mut o = BatchOrderInput::new(OrderSide::Buy, OrderType::Limit);
    o.price = Some(Amount::new("100").unwrap());
    o.quantity = Some(Amount::new("1").unwrap());
    o
}

/// A request's orders; `orders_count: N` means N limit buys with generated ids.
fn place_inputs(req: &Value) -> Vec<BatchOrderInput> {
    if let Some(n) = req["orders_count"].as_u64() {
        return (0..n).map(|_| limit_buy()).collect();
    }
    req["orders"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| {
            let mut i = BatchOrderInput::new(
                OrderSide::from(o["side"].as_str().unwrap()),
                OrderType::from(o["type"].as_str().unwrap()),
            );
            i.client_order_id = o["client_order_id"].as_str().map(str::to_string);
            i.price = amount(&o["price"]);
            i.quantity = amount(&o["quantity"]);
            i
        })
        .collect()
}

fn cancel_inputs(req: &Value) -> Vec<CancelBatchItem> {
    if let Some(n) = req["orders_count"].as_u64() {
        return (0..n)
            .map(|i| CancelBatchItem::by_order_id(format!("o{i}")))
            .collect();
    }
    serde_json::from_value(req["orders"].clone()).unwrap()
}

fn outcome_name<T: std::fmt::Debug>(o: T) -> String {
    format!("{o:?}").to_lowercase()
}

fn same_bodies(reqs: &[&Request], id: &str) {
    assert!(
        reqs.windows(2).all(|w| w[0].body == w[1].body),
        "{id}: every attempt sends the same body"
    );
}

/// The ids of the first request body, in order.
fn first_body_cids(sent: &[&Request]) -> Vec<String> {
    let b: Value = serde_json::from_slice(&sent[0].body).unwrap();
    b["orders"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["client_order_id"].as_str().unwrap().to_string())
        .collect()
}

/// The error a place_orders failure is wrapped in, or `e` itself.
fn cause_of(e: &Error) -> &Error {
    match e {
        Error::BatchPlace { source, .. } => source,
        other => other,
    }
}

fn check_error(id: &str, e: &Error, expect: &Value, sent: &[&Request]) {
    if let Some(code) = expect["error_code"].as_str() {
        assert_eq!(e.code(), Some(code), "{id}: {e}");
    }
    if let Some(limit) = expect.get("error_details_limit") {
        assert_eq!(&e.api().unwrap().details["limit"], limit, "{id}");
    }
    if expect["error"] == json!("connection") {
        assert!(matches!(cause_of(e), Error::Connection(_)), "{id}: {e}");
    }
    if expect["error"] == json!("batch") {
        let Error::BatchPlace {
            client_order_ids,
            maybe_placed,
            source,
        } = e
        else {
            panic!("{id}: not a batch error: {e}");
        };
        if let Some(m) = expect["maybe_placed"].as_bool() {
            assert_eq!(*maybe_placed, m, "{id}: maybe_placed");
        }
        if expect["error_cids"] == json!("sent") {
            assert_eq!(client_order_ids, &first_body_cids(sent), "{id}: cids");
        }
        for i in expect["generated_cid_indexes"]
            .as_array()
            .into_iter()
            .flatten()
        {
            let cid = &client_order_ids[i.as_u64().unwrap() as usize];
            assert_eq!(cid.len(), 36, "{id}: generated {cid}");
        }
        match expect["cause"].as_str() {
            Some("connection") => assert!(matches!(**source, Error::Connection(_)), "{id}: {e}"),
            Some("bad_response") => assert!(matches!(**source, Error::Decode(_)), "{id}: {e}"),
            Some(code) => assert_eq!(source.code(), Some(code), "{id}: {e}"),
            None => {}
        }
    }
}

/// `expect.hold == "placements"`: placements were held, the whole client was not.
fn check_hold(id: &str, c: &Client, expect: &Value) {
    if expect["hold"] == json!("placements") {
        assert!(
            c.t.placement_hold.as_ref().unwrap().held_until().is_some(),
            "{id}: placements held"
        );
        assert_eq!(
            c.rate_limit().unwrap().blocked_for,
            Duration::ZERO,
            "{id}: the client is not held"
        );
    }
}

#[tokio::test]
async fn conformance_place() {
    let Some(file) = load("trading/batch_orders.json") else {
        return;
    };
    let template = &file["order_template"];
    for case in file["place"].as_array().unwrap() {
        let id = case["id"].as_str().unwrap();
        let expect = &case["expect"];
        let server = MockServer::start().await;
        mount_case(&server, "POST", BATCH, case, template).await;
        let (c, _) = batch_client(&server, |_| {});
        let req = &case["request"];
        let result = c
            .trading()
            .place_orders(req["symbol"].as_str().unwrap(), &place_inputs(req))
            .await;
        let reqs = server.received_requests().await.unwrap();
        let sent = calls_to(&reqs, "POST", BATCH);
        assert_eq!(
            sent.len() as u64,
            expect["calls"].as_u64().unwrap(),
            "{id}: calls"
        );
        if expect["same_body"] == json!(true) {
            same_bodies(&sent, id);
        }
        check_hold(id, &c, expect);
        let Some(want) = expect["results"].as_array() else {
            check_error(id, &result.expect_err(id), expect, &sent);
            continue;
        };
        let r = result.unwrap_or_else(|e| panic!("{id}: {e}"));
        assert_eq!(
            lookups(&reqs),
            expect["lookups"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap().to_string())
                .collect::<Vec<_>>(),
            "{id}: lookups"
        );
        assert_eq!(r.results.len(), want.len(), "{id}");
        for (got, w) in r.results.iter().zip(want) {
            assert_eq!(got.index, w["index"].as_i64().unwrap(), "{id}");
            assert_eq!(
                outcome_name(got.outcome),
                w["outcome"].as_str().unwrap(),
                "{id}"
            );
            if let Some(oid) = w["order_id"].as_str() {
                assert_eq!(got.order_id.as_deref(), Some(oid), "{id}: order_id");
            }
            if let Some(st) = w["status"].as_str() {
                assert_eq!(
                    got.status.as_ref().map(|s| s.as_str()),
                    Some(st),
                    "{id}: status"
                );
            }
            if let Some(code) = w["error_code"].as_str() {
                assert_eq!(got.error.as_ref().unwrap().code.as_str(), code, "{id}");
            }
        }
        let counts = (r.placed, r.failed, r.unknown);
        let want_counts = (
            expect["placed"].as_u64().unwrap() as usize,
            expect["failed"].as_u64().unwrap() as usize,
            expect["unknown"].as_u64().unwrap() as usize,
        );
        assert_eq!(counts, want_counts, "{id}: counts");
        // Generated ids: UUIDs, returned in the result and the same on every attempt.
        for i in expect["generated_cid_indexes"]
            .as_array()
            .into_iter()
            .flatten()
        {
            let i = i.as_u64().unwrap() as usize;
            let cid = &r.results[i].client_order_id;
            assert_eq!(cid.len(), 36, "{id}: generated {cid}");
            for s in &sent {
                let b: Value = serde_json::from_slice(&s.body).unwrap();
                assert_eq!(b["orders"][i]["client_order_id"], json!(cid), "{id}");
            }
        }
        // Caller ids are sent as given.
        for (i, o) in req["orders"].as_array().into_iter().flatten().enumerate() {
            if let Some(cid) = o["client_order_id"].as_str() {
                assert_eq!(r.results[i].client_order_id, cid, "{id}");
            }
        }
    }
}

#[tokio::test]
async fn conformance_cancel() {
    let Some(file) = load("trading/batch_orders.json") else {
        return;
    };
    let template = &file["order_template"];
    for case in file["cancel"].as_array().unwrap() {
        let id = case["id"].as_str().unwrap();
        let expect = &case["expect"];
        let server = MockServer::start().await;
        mount_case(&server, "POST", CANCEL_BATCH, case, template).await;
        let (c, _) = batch_client(&server, |_| {});
        let result = c
            .trading()
            .cancel_orders(&cancel_inputs(&case["request"]))
            .await;
        let reqs = server.received_requests().await.unwrap();
        let sent = calls_to(&reqs, "POST", CANCEL_BATCH);
        assert_eq!(
            sent.len() as u64,
            expect["calls"].as_u64().unwrap(),
            "{id}: calls"
        );
        // Rule 13: a result that does not match the entries sent is a bad response.
        if expect["error"] == json!("bad_response") {
            let e = result.expect_err(id);
            assert!(matches!(e, Error::Decode(_)), "{id}: {e}");
            continue;
        }
        let r = result.unwrap_or_else(|e| panic!("{id}: {e}"));
        if expect["same_body"] == json!(true) {
            same_bodies(&sent, id);
        }
        check_hold(id, &c, expect);
        assert_eq!(
            lookups(&reqs),
            expect["lookups"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap().to_string())
                .collect::<Vec<_>>(),
            "{id}: lookups"
        );
        let want = expect["results"].as_array().unwrap();
        assert_eq!(r.results.len(), want.len(), "{id}");
        for (got, w) in r.results.iter().zip(want) {
            assert_eq!(got.index, w["index"].as_i64().unwrap(), "{id}");
            assert_eq!(
                outcome_name(got.outcome),
                w["outcome"].as_str().unwrap(),
                "{id}"
            );
            if let Some(oid) = w["order_id"].as_str() {
                assert_eq!(got.order_id.as_deref(), Some(oid), "{id}: order_id");
            }
            if let Some(st) = w["status"].as_str() {
                assert_eq!(
                    got.status.as_ref().map(|s| s.as_str()),
                    Some(st),
                    "{id}: status"
                );
            }
            if let Some(code) = w["error_code"].as_str() {
                assert_eq!(got.error.as_ref().unwrap().code.as_str(), code, "{id}");
            }
        }
        let counts = (r.cancelled, r.failed, r.unknown);
        let want_counts = (
            expect["cancelled"].as_u64().unwrap() as usize,
            expect["failed"].as_u64().unwrap() as usize,
            expect["unknown"].as_u64().unwrap() as usize,
        );
        assert_eq!(counts, want_counts, "{id}: counts");
    }
}

#[tokio::test]
async fn conformance_cancel_by_client_id() {
    let Some(file) = load("trading/batch_orders.json") else {
        return;
    };
    let template = &file["order_template"];
    for case in file["cancel_by_client_id"].as_array().unwrap() {
        let id = case["id"].as_str().unwrap();
        let expect = &case["expect"];
        let cid = case["client_order_id"].as_str().unwrap();
        let p = format!("{BY_CID}{cid}");
        let server = MockServer::start().await;
        mount_case(&server, "DELETE", &p, case, template).await;
        let (c, _) = batch_client(&server, |_| {});
        let result = c.trading().cancel_order_by_client_id(cid).await;
        let reqs = server.received_requests().await.unwrap();
        let sent = calls_to(&reqs, "DELETE", &p);
        assert_eq!(
            sent.len() as u64,
            expect["calls"].as_u64().unwrap(),
            "{id}: calls"
        );
        assert_eq!(
            lookups(&reqs),
            expect["lookups"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap().to_string())
                .collect::<Vec<_>>(),
            "{id}: lookups"
        );
        match expect["order_id"].as_str() {
            Some(oid) => {
                let o = result.unwrap_or_else(|e| panic!("{id}: {e}"));
                assert_eq!(o.id, oid, "{id}");
                assert_eq!(
                    o.status.as_str(),
                    expect["status"].as_str().unwrap(),
                    "{id}"
                );
            }
            None => check_error(id, &result.expect_err(id), expect, &sent),
        }
    }
}

#[tokio::test]
async fn conformance_local_checks_and_paths() {
    let Some(file) = load("trading/batch_orders.json") else {
        return;
    };
    let template = &file["order_template"];
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(BATCH))
        .respond_with(Echo)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(CANCEL_BATCH))
        .respond_with(Echo)
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .respond_with(data(overlay(
            template,
            &json!({"id": "o1", "status": "cancelled"}),
        )))
        .mount(&server)
        .await;
    let (c, _) = batch_client(&server, |_| {});
    let t = c.trading();

    for case in file["validation"].as_array().unwrap() {
        let id = case["id"].as_str().unwrap();
        let req = &case["request"];
        let e = match case["call"].as_str().unwrap() {
            "place" => t
                .place_orders(req["symbol"].as_str().unwrap(), &place_inputs(req))
                .await
                .map(|_| ()),
            "cancel" => t.cancel_orders(&cancel_inputs(req)).await.map(|_| ()),
            other => panic!("{other}"),
        }
        .expect_err(id);
        assert!(matches!(e, Error::Config(_)), "{id}: {e}");
    }
    assert!(
        server.received_requests().await.unwrap().is_empty(),
        "refused locally, before any request"
    );

    for case in file["valid"].as_array().unwrap() {
        let id = case["id"].as_str().unwrap();
        let before = server.received_requests().await.unwrap().len();
        match case["call"].as_str().unwrap() {
            "place" => {
                let orders = match case["client_order_id"].as_str() {
                    Some(cid) => {
                        let mut o = limit_buy();
                        o.client_order_id = Some(cid.into());
                        vec![o]
                    }
                    None => place_inputs(case),
                };
                t.place_orders("BTC/USDT", &orders).await.map(|_| ())
            }
            "cancel" => t.cancel_orders(&cancel_inputs(case)).await.map(|_| ()),
            other => panic!("{other}"),
        }
        .unwrap_or_else(|e| panic!("{id}: {e}"));
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            before + 1,
            "{id}"
        );
    }

    for case in file["cid_path"].as_array().unwrap() {
        let cid = case["client_order_id"].as_str().unwrap();
        t.cancel_order_by_client_id(cid).await.unwrap();
        let reqs = server.received_requests().await.unwrap();
        assert_eq!(
            reqs.last().unwrap().url.path(),
            case["path"].as_str().unwrap(),
            "{cid}"
        );
    }
}

/// conformance `cost`: request tokens and order actions per call, with N = 3.
#[tokio::test]
async fn conformance_cost() {
    let Some(file) = load("trading/batch_orders.json") else {
        return;
    };
    let template = &file["order_template"];
    let order = overlay(template, &json!({"id": "o1"}));
    let server = MockServer::start().await;
    for p in [BATCH, CANCEL_BATCH] {
        Mock::given(method("POST"))
            .and(path(p))
            .respond_with(Echo)
            .mount(&server)
            .await;
    }
    for (verb, p, body) in [
        (
            "POST",
            "/api/v1/trading/orders",
            json!({"order": order, "fills": []}),
        ),
        (
            "POST",
            "/api/v1/trading/orders/cancel-all",
            json!({"cancelled": [], "already_closed": [], "failed": [], "failures": [], "has_more": false}),
        ),
    ] {
        Mock::given(method(verb))
            .and(path(p))
            .respond_with(data(body))
            .mount(&server)
            .await;
    }
    Mock::given(method("DELETE"))
        .respond_with(data(order.clone()))
        .mount(&server)
        .await;
    const N: f64 = 3.0;
    for (call, want) in file["cost"]["calls"].as_object().unwrap() {
        let (c, _) = batch_client(&server, |o| o.order_actions_per_second = Some(100.0));
        let tokens = |c: &Client| {
            (
                c.rate_limit().unwrap().tokens,
                c.t.order_limiter.as_ref().unwrap().state().tokens,
            )
        };
        let before = tokens(&c);
        let t = c.trading();
        let three = vec![limit_buy(), limit_buy(), limit_buy()];
        let entries: Vec<_> = (0..3)
            .map(|i| CancelBatchItem::by_order_id(format!("o{i}")))
            .collect();
        match call.as_str() {
            "place_order" => {
                let mut o =
                    crate::PlaceOrderRequest::new("BTC/USDT", OrderSide::Buy, OrderType::Limit);
                o.price = Some(Amount::new("100").unwrap());
                o.quantity = Some(Amount::new("1").unwrap());
                t.place_order(&o).await.map(|_| ())
            }
            "place_orders" => t.place_orders("BTC/USDT", &three).await.map(|_| ()),
            "cancel_order" => t.cancel_order("o1").await.map(|_| ()),
            "cancel_order_by_client_id" => t.cancel_order_by_client_id("c1").await.map(|_| ()),
            "cancel_orders" => t.cancel_orders(&entries).await.map(|_| ()),
            "cancel_all" => t.cancel_all("BTC/USDT").await.map(|_| ()),
            other => panic!("no runner for {other}"),
        }
        .unwrap_or_else(|e| panic!("{call}: {e}"));
        let after = tokens(&c);
        let n = |v: &Value| match v.as_str().unwrap() {
            "N" => N,
            s => s.parse().unwrap(),
        };
        assert_eq!(
            before.0 - after.0,
            n(&want["request_tokens"]),
            "{call}: request tokens"
        );
        assert_eq!(
            before.1 - after.1,
            n(&want["order_actions"]),
            "{call}: order actions"
        );
    }
}

fn rate_limited(limit: Option<&str>) -> ResponseTemplate {
    let mut details = json!({"retry_after_seconds": 5});
    if let Some(l) = limit {
        details["limit"] = json!(l);
    }
    ResponseTemplate::new(429).set_body_json(json!({"error": {
        "code": "RATE_LIMITED", "message": "slow down", "retryable": true, "details": details
    }}))
}

/// Mounts answers for every call the hold tests make; the batch place answers `first`.
async fn hold_server(first: impl Respond + 'static) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(BATCH))
        .respond_with(first)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v1/trading/orders"))
        .respond_with(data(json!({"order": order("o1", "open"), "fills": []})))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(CANCEL_BATCH))
        .respond_with(Echo)
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .respond_with(data(order("o1", "cancelled")))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex("^/api/v1/markets$"))
        .respond_with(data(json!([])))
        .mount(&server)
        .await;
    server
}

fn limit_order() -> crate::PlaceOrderRequest {
    let mut o = crate::PlaceOrderRequest::new("BTC/USDT", OrderSide::Buy, OrderType::Limit);
    o.price = Some(Amount::new("100").unwrap());
    o.quantity = Some(Amount::new("1").unwrap());
    o
}

#[tokio::test]
async fn order_actions_429_holds_placements_not_reads_or_cancels() {
    let server = hold_server(rate_limited(Some("order_actions"))).await;
    let (c, clock) = batch_client(&server, |_| {});
    let once = c.with_options(CallOptions {
        max_retries: Some(0),
        ..Default::default()
    });
    let e = once
        .trading()
        .place_orders("BTC/USDT", &[limit_buy()])
        .await
        .unwrap_err();
    assert!(e.is(crate::ErrorCategory::RateLimited));
    // Reads and cancels go at once.
    c.markets().list().await.unwrap();
    c.trading().cancel_order("o1").await.unwrap();
    c.trading().cancel_order_by_client_id("c1").await.unwrap();
    c.trading()
        .cancel_orders(&[CancelBatchItem::by_order_id("o1")])
        .await
        .unwrap();
    assert!(clock.sleeps().is_empty(), "{:?}", clock.sleeps());
    // A placement waits out the hold.
    c.trading().place_order(&limit_order()).await.unwrap();
    assert_eq!(clock.sleeps(), vec![Duration::from_secs(5)]);
}

#[tokio::test]
async fn request_limit_429_still_holds_everything() {
    let server = hold_server(rate_limited(None)).await;
    let (c, clock) = batch_client(&server, |_| {});
    let once = c.with_options(CallOptions {
        max_retries: Some(0),
        ..Default::default()
    });
    once.trading()
        .place_orders("BTC/USDT", &[limit_buy()])
        .await
        .unwrap_err();
    c.trading()
        .cancel_orders(&[CancelBatchItem::by_order_id("o1")])
        .await
        .unwrap();
    assert_eq!(clock.sleeps(), vec![Duration::from_secs(5)]);
}

#[tokio::test]
async fn a_batch_takes_n_request_tokens() {
    let server = hold_server(Echo).await;
    // 20 a minute: one token every 3 s. A batch of 20 spends them all, so the next read waits.
    let (c, clock) = batch_client(&server, |o| o.requests_per_minute = Some(20));
    let twenty: Vec<_> = (0..20).map(|_| limit_buy()).collect();
    c.trading().place_orders("BTC/USDT", &twenty).await.unwrap();
    assert!(clock.sleeps().is_empty());
    c.markets().list().await.unwrap();
    assert_eq!(clock.sleeps(), vec![Duration::from_secs(3)]);
}

#[tokio::test]
async fn a_cancel_batch_takes_n_request_tokens() {
    let server = hold_server(Echo).await;
    let (c, clock) = batch_client(&server, |o| o.requests_per_minute = Some(50));
    let fifty: Vec<_> = (0..50)
        .map(|i| CancelBatchItem::by_order_id(format!("o{i}")))
        .collect();
    c.trading().cancel_orders(&fifty).await.unwrap();
    assert!(clock.sleeps().is_empty());
    c.markets().list().await.unwrap();
    let s = clock.sleeps();
    assert_eq!(s.len(), 1);
    assert!((s[0].as_secs_f64() - 1.2).abs() < 1e-6, "{s:?}");
}

#[tokio::test]
async fn order_actions_budget_spends_placements_only() {
    let server = hold_server(Echo).await;
    let (c, clock) = batch_client(&server, |o| o.order_actions_per_second = Some(2.0));
    let t = c.trading();
    t.place_orders("BTC/USDT", &[limit_buy(), limit_buy()])
        .await
        .unwrap();
    // Cancels and reads never spend it.
    let entries: Vec<_> = (0..5)
        .map(|i| CancelBatchItem::by_order_id(format!("o{i}")))
        .collect();
    t.cancel_orders(&entries).await.unwrap();
    t.cancel_order("o1").await.unwrap();
    c.markets().list().await.unwrap();
    assert!(clock.sleeps().is_empty());
    // The budget is spent: the next placement waits for one action (0.5 s at 2 a second).
    t.place_order(&limit_order()).await.unwrap();
    assert_eq!(clock.sleeps(), vec![Duration::from_millis(500)]);
}

#[test]
fn order_actions_per_second_must_be_positive() {
    for bad in [0.0, -1.0, f64::NAN, f64::INFINITY] {
        let o = ClientOptions {
            order_actions_per_second: Some(bad),
            ..Default::default()
        };
        assert!(Client::new(o).is_err(), "{bad}");
    }
}

#[tokio::test]
async fn every_attempt_is_signed_afresh_with_the_same_body() {
    let server = MockServer::start().await;
    let template = json!({"symbol": "BTC/USDT", "side": "buy", "type": "limit", "time_in_force": "gtc",
        "status": "open", "quantity": "1", "filled_quantity": "0", "remaining_quantity": "1",
        "filled_quote_quantity": "0", "fee_paid": "0", "reserved_remaining": "0",
        "created_at": "2026-10-11T00:00:00Z", "updated_at": "2026-10-11T00:00:00Z"});
    let case = json!({"attempts": [
        {"network_error": "timeout"},
        {"http_status": 502, "error": {"code": "SERVICE_UNAVAILABLE", "message": "x", "retryable": true}},
        {"http_status": 200, "echo_cids": true, "data": {"placed": 1, "failed": 0, "results": [
            {"index": 0, "client_order_id": "", "order": {"id": "o1"}, "fills": [], "error": null}]}}
    ]});
    mount_case(&server, "POST", BATCH, &case, &template).await;
    let (c, _) = batch_client(&server, |_| {});
    let r = c
        .trading()
        .place_orders("BTC/USDT", &[limit_buy()])
        .await
        .unwrap();
    assert_eq!(r.placed, 1);
    let reqs = server.received_requests().await.unwrap();
    assert_eq!(reqs.len(), 3);
    same_bodies(&reqs.iter().collect::<Vec<_>>(), "nonce");
    let nonces: std::collections::HashSet<_> = reqs
        .iter()
        .map(|r| {
            r.headers
                .get("x-api-nonce")
                .unwrap()
                .to_str()
                .unwrap()
                .to_string()
        })
        .collect();
    assert_eq!(nonces.len(), 3, "a fresh nonce per attempt");
    assert!(
        reqs.iter()
            .all(|r| !r.headers.contains_key("idempotency-key"))
    );
}

#[tokio::test]
async fn already_exists_on_the_first_attempt_is_not_looked_up() {
    let server = MockServer::start().await;
    Mock::given(any())
        .respond_with(data(json!({"placed": 0, "failed": 1, "results": [{
            "index": 0, "client_order_id": "c1", "order": null, "fills": [],
            "error": {"code": "ALREADY_EXISTS", "message": "exists", "retryable": false,
                      "details": {"client_order_id": "c1"}}}]})))
        .mount(&server)
        .await;
    let (c, _) = batch_client(&server, |_| {});
    let mut o = limit_buy();
    o.client_order_id = Some("c1".into());
    let r = c.trading().place_orders("BTC/USDT", &[o]).await.unwrap();
    assert_eq!(r.results[0].outcome, PlaceOutcome::Failed);
    assert_eq!((r.placed, r.failed, r.unknown), (0, 1, 0));
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

/// A lookup that fails for any reason (here a timeout) makes only that item `Unknown`: the call
/// still returns every other item.
#[tokio::test]
async fn a_failed_lookup_leaves_the_other_items_intact() {
    let server = MockServer::start().await;
    let template = json!({"symbol": "BTC/USDT", "side": "buy", "type": "limit", "time_in_force": "gtc",
        "status": "open", "quantity": "1", "filled_quantity": "0", "remaining_quantity": "1",
        "filled_quote_quantity": "0", "fee_paid": "0", "reserved_remaining": "0",
        "created_at": "2026-10-11T00:00:00Z", "updated_at": "2026-10-11T00:00:00Z"});
    let exists = |cid: &str| {
        json!({"index": 0, "client_order_id": cid, "order": null, "fills": [],
        "error": {"code": "ALREADY_EXISTS", "message": "exists", "retryable": false,
                  "details": {"client_order_id": cid}}})
    };
    let mut second = exists("c2");
    second["index"] = json!(1);
    let case = json!({
        "attempts": [
            {"network_error": "timeout"},
            {"http_status": 200, "data": {"placed": 0, "failed": 3, "results": [exists("c1"), second,
                {"index": 2, "client_order_id": "c3", "order": {"id": "o3"}, "fills": [], "error": null}]}}
        ],
        "lookups": {
            "cid:c1": {"network_error": "timeout"},
            "cid:c2": {"http_status": 200, "data": {"id": "o2", "status": "filled"}}
        }
    });
    mount_case(&server, "POST", BATCH, &case, &template).await;
    let (c, _) = batch_client(&server, |_| {});
    let orders: Vec<_> = ["c1", "c2", "c3"]
        .iter()
        .map(|cid| {
            let mut o = limit_buy();
            o.client_order_id = Some(cid.to_string());
            o
        })
        .collect();
    let r = c.trading().place_orders("BTC/USDT", &orders).await.unwrap();
    let outcomes: Vec<_> = r.results.iter().map(|o| o.outcome).collect();
    assert_eq!(
        outcomes,
        vec![
            PlaceOutcome::Unknown,
            PlaceOutcome::Recovered,
            PlaceOutcome::Placed
        ]
    );
    assert_eq!(r.results[1].order_id.as_deref(), Some("o2"));
    assert_eq!((r.placed, r.failed, r.unknown), (2, 0, 1));
    let reqs = server.received_requests().await.unwrap();
    assert_eq!(lookups(&reqs), vec!["cid:c1", "cid:c2"]);
}

#[tokio::test]
async fn a_cancel_answer_that_does_not_match_is_an_error() {
    let server = MockServer::start().await;
    Mock::given(any())
        .respond_with(data(json!({"cancelled": 1, "failed": 0, "results": [
            {"index": 0, "order_id": "other", "client_order_id": null,
             "order": order("other", "cancelled"), "error": null}]})))
        .mount(&server)
        .await;
    let (c, _) = batch_client(&server, |_| {});
    let e = c
        .trading()
        .cancel_orders(&[CancelBatchItem::by_order_id("o1")])
        .await
        .unwrap_err();
    assert!(matches!(e, Error::Decode(_)), "{e}");
}

#[test]
fn new_client_order_id_is_a_fresh_uuid() {
    let (a, b) = (crate::new_client_order_id(), crate::new_client_order_id());
    assert_eq!(a.len(), 36);
    assert_ne!(a, b);
    assert!(crate::batch::valid_client_order_id(&a));
}

/// Signs batch calls but fails on every GET: the recovery lookup fails inside the
/// authenticator, not on the wire.
struct FailingOnGets;

impl crate::Authenticator for FailingOnGets {
    fn kind(&self) -> &str {
        "test"
    }
    fn authenticate(&self, request: &mut crate::AuthRequest<'_>) -> crate::Result<()> {
        if request.method == "GET" {
            return Err(Error::Config("authenticator refused".into()));
        }
        Ok(())
    }
    fn redact(&self, text: &str) -> String {
        text.to_string()
    }
}

/// Rule 12: a lookup that fails in any way (here the authenticator) makes the item `Unknown`, in
/// place and in cancel, and the call still returns.
#[tokio::test]
async fn a_lookup_failing_in_the_authenticator_is_unknown() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(BATCH))
        .respond_with(data(json!({"placed": 0, "failed": 1, "results": [{
            "index": 0, "client_order_id": "c1", "order": null, "fills": [],
            "error": {"code": "INTERNAL", "message": "internal", "retryable": false}}]})))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(CANCEL_BATCH))
        .respond_with(Sequence::new(vec![
            api_error(502, "SERVICE_UNAVAILABLE", true),
            data(json!({"cancelled": 0, "failed": 1, "results": [{
                "index": 0, "order_id": "o1", "client_order_id": null, "order": null,
                "error": {"code": "INVALID_STATE", "message": "closed", "retryable": false}}]})),
        ]))
        .mount(&server)
        .await;
    let (c, _) = batch_client(&server, |o| {
        o.api_key = None;
        o.api_secret = None;
        o.authenticator = Some(Arc::new(FailingOnGets));
    });
    let mut o = limit_buy();
    o.client_order_id = Some("c1".into());
    let r = c.trading().place_orders("BTC/USDT", &[o]).await.unwrap();
    assert_eq!(r.results[0].outcome, PlaceOutcome::Unknown);
    assert_eq!((r.placed, r.failed, r.unknown), (0, 0, 1));
    let r = c
        .trading()
        .cancel_orders(&[CancelBatchItem::by_order_id("o1")])
        .await
        .unwrap();
    assert_eq!(r.results[0].outcome, crate::CancelOutcome::Unknown);
    assert_eq!((r.cancelled, r.failed, r.unknown), (0, 0, 1));
    // Nothing reached the server for the lookups.
    let reqs = server.received_requests().await.unwrap();
    assert!(reqs.iter().all(|r| r.method.as_str() == "POST"));
}
