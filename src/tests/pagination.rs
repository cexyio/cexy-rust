use futures_util::{StreamExt, TryStreamExt};
use serde_json::json;
use wiremock::matchers::{path, query_param, query_param_is_missing};
use wiremock::{Mock, MockServer};

use super::helpers::*;

fn page(ids: &[&str], next: Option<&str>) -> wiremock::ResponseTemplate {
    let items: Vec<_> = ids.iter().map(|id| order(id, "filled")).collect();
    wiremock::ResponseTemplate::new(200)
        .set_body_json(json!({"items": items, "has_more": next.is_some(), "next_cursor": next}))
}

async fn server() -> MockServer {
    let s = MockServer::start().await;
    let p = "/api/v1/trading/orders/history";
    Mock::given(path(p))
        .and(query_param_is_missing("cursor"))
        .respond_with(page(&["o1", "o2"], Some("c2")))
        .mount(&s)
        .await;
    Mock::given(path(p))
        .and(query_param("cursor", "c2"))
        .respond_with(page(&["o3"], Some("c3")))
        .mount(&s)
        .await;
    Mock::given(path(p))
        .and(query_param("cursor", "c3"))
        .respond_with(page(&["o4"], None))
        .mount(&s)
        .await;
    s
}

#[tokio::test]
async fn follows_the_cursor_to_the_last_page() {
    let s = server().await;
    let (c, _) = client(&s);
    let ids: Vec<String> = c
        .trading()
        .all_order_history(None, None)
        .map_ok(|o| o.id)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(ids, ["o1", "o2", "o3", "o4"]);
    assert_eq!(s.received_requests().await.unwrap().len(), 3);
}

#[tokio::test]
async fn max_items_stops_early_and_fetches_lazily() {
    let s = server().await;
    let (c, _) = client(&s);
    let ids: Vec<String> = c
        .trading()
        .all_order_history(None, Some(2))
        .map_ok(|o| o.id)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(ids, ["o1", "o2"]);
    assert_eq!(s.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn a_repeated_cursor_stops_and_an_error_is_yielded_once() {
    let s = MockServer::start().await;
    let p = "/api/v1/account/ledger";
    Mock::given(path(p))
        .respond_with(
            wiremock::ResponseTemplate::new(200)
                .set_body_json(json!({"items": [], "has_more": true, "next_cursor": "same"})),
        )
        .mount(&s)
        .await;
    let (c, _) = client(&s);
    let n = c
        .account()
        .all_ledger(None, None)
        .collect::<Vec<_>>()
        .await
        .len();
    assert_eq!(n, 0);
    assert_eq!(
        s.received_requests().await.unwrap().len(),
        2,
        "stops when the cursor repeats"
    );

    let s = MockServer::start().await;
    Mock::given(path(p))
        .respond_with(api_error(403, "FORBIDDEN", false))
        .mount(&s)
        .await;
    let (c, _) = client(&s);
    let items: Vec<_> = c.account().all_ledger(None, None).collect().await;
    assert_eq!(items.len(), 1);
    assert!(items[0].is_err());
}
