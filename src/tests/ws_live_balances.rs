//! conformance/ws/live_balances.json and private_sequence_gap.json, run step by step against the
//! local server (it answers only pings) with an injected test clock.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::future::BoxFuture;
use serde_json::{Value, json};
use tokio::sync::oneshot;

use super::helpers::load;
use super::ws::{Conn, Server, options};
use super::ws_signout::{norm_sent, settle, sorted, tracked};
use crate::{
    Balance, BalancesEvent, LiveBalances, LiveBalancesOptions, WebSocket, WsClock, WsEvent,
    WsEvents, WsTimer,
};

/// Timers run only when `advance` passes their due time.
type Timers = Vec<(Duration, u64, Box<dyn FnOnce() + Send>)>;

#[derive(Default)]
struct FakeClock {
    st: Mutex<(Duration, u64, Timers)>,
}

impl std::fmt::Debug for FakeClock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FakeClock")
    }
}

struct FakeTimer(Arc<FakeClock>, u64);

impl WsTimer for FakeTimer {
    fn cancel(&self) {
        self.0.st.lock().unwrap().2.retain(|t| t.1 != self.1);
    }
}

#[derive(Debug)]
struct ClockHandle(Arc<FakeClock>);

impl WsClock for ClockHandle {
    fn now(&self) -> Duration {
        self.0.st.lock().unwrap().0
    }
    fn call_later(&self, delay: Duration, f: Box<dyn FnOnce() + Send>) -> Box<dyn WsTimer> {
        let mut st = self.0.st.lock().unwrap();
        st.1 += 1;
        let id = st.1;
        let due = st.0 + delay;
        st.2.push((due, id, f));
        Box::new(FakeTimer(self.0.clone(), id))
    }
}

impl FakeClock {
    fn advance(&self, d: Duration) {
        let end = self.st.lock().unwrap().0 + d;
        loop {
            let next = {
                let mut st = self.st.lock().unwrap();
                let i =
                    st.2.iter()
                        .enumerate()
                        .filter(|(_, t)| t.0 <= end)
                        .min_by_key(|(_, t)| (t.0, t.1))
                        .map(|(i, _)| i);
                match i {
                    Some(i) => {
                        let t = st.2.remove(i);
                        st.0 = t.0;
                        Some(t.2)
                    }
                    None => None,
                }
            };
            match next {
                Some(f) => f(),
                None => break,
            }
        }
        self.st.lock().unwrap().0 = end;
    }
}

/// A source whose calls the script answers one by one.
#[derive(Default)]
struct Scripted<T> {
    calls: usize,
    pending: Vec<oneshot::Sender<T>>,
}

fn source<T: Send + 'static>(
    s: &Arc<Mutex<Scripted<T>>>,
) -> Arc<dyn Fn() -> BoxFuture<'static, crate::Result<T>> + Send + Sync> {
    let s = s.clone();
    Arc::new(move || -> BoxFuture<'static, crate::Result<T>> {
        let (tx, rx) = oneshot::channel();
        {
            let mut g = s.lock().unwrap();
            g.calls += 1;
            g.pending.push(tx);
        }
        Box::pin(async move { Ok(rx.await.expect("answered")) })
    })
}

async fn wait_pending<T>(s: &Arc<Mutex<Scripted<T>>>, what: &str) {
    for _ in 0..2000 {
        if !s.lock().unwrap().pending.is_empty() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    panic!("timed out waiting for a {what} request");
}

fn classify(ev: WsEvent, events: &mut Vec<Value>, errors: &mut Vec<String>) {
    match ev {
        WsEvent::Balances(BalancesEvent::AccountMismatch { .. }) => errors.push("ACCOUNT_MISMATCH".into()),
        WsEvent::Balances(BalancesEvent::Error(_)) => errors.push("ERROR".into()),
        WsEvent::SequenceGap(g) => {
            events.push(json!({"type": "sequence_gap", "channel": g.channel, "expected": g.expected, "received": g.received}))
        }
        other => {
            if let Some(v) = tracked(other)
                && v["type"] == "resync"
            {
                events.push(v);
            }
        }
    }
}

fn drain(rx: &mut WsEvents, events: &mut Vec<Value>, errors: &mut Vec<String>) {
    while let Ok(ev) = rx.try_recv() {
        classify(ev, events, errors);
    }
}

#[tokio::test]
async fn live_balances_conformance() {
    let Some(live) = load("ws/live_balances.json") else {
        return;
    };
    let gaps = load("ws/private_sequence_gap.json").unwrap();
    let mut failed = vec![];
    for case in live["cases"]
        .as_array()
        .unwrap()
        .iter()
        .chain(gaps["cases"].as_array().unwrap())
    {
        let id = case["id"].as_str().unwrap().to_string();
        if tokio::spawn(run_case(case.clone())).await.is_err() {
            failed.push(id);
        }
    }
    assert!(failed.is_empty(), "failed cases: {failed:?}");
}

async fn run_case(case: Value) {
    let id = case["id"].as_str().unwrap().to_string();
    let opts = &case["options"];
    let clock = Arc::new(FakeClock::default());
    let mut srv = Server::start().await;
    let mut o = options(&srv.url);
    o.reconnect = false;
    o.ack_timeout = Duration::from_secs(2);
    o.reorder_window = Duration::from_millis(opts["reorder_window_ms"].as_u64().unwrap_or(250));
    o.clock = Some(Arc::new(ClockHandle(clock.clone())));
    let ws = WebSocket::new(o).unwrap();
    let mut rx = ws.events().unwrap();
    ws.connect().await.unwrap();
    let mut conn: Conn = srv.conn().await;
    let owner: Arc<Mutex<Scripted<String>>> = Arc::default();
    let snapshot: Arc<Mutex<Scripted<Vec<Balance>>>> = Arc::default();
    let helper: Arc<Mutex<Option<LiveBalances>>> = Arc::default();
    let min_interval =
        Duration::from_millis(opts["min_snapshot_interval_ms"].as_u64().unwrap_or(2000));
    let mut received: Vec<Value> = vec![];
    let mut answered: Vec<Value> = vec![];
    let mut events: Vec<Value> = vec![];
    let mut errors: Vec<String> = vec![];
    let mut sent_mark = 0;
    for (i, st) in case["steps"].as_array().unwrap().iter().enumerate() {
        let at = format!("{id} step {i}");
        if let Some(client) = st.get("client").and_then(Value::as_str) {
            let ws2 = ws.clone();
            match client {
                "auth" => {
                    let token = st["token"].as_str().unwrap().to_string();
                    tokio::spawn(async move { ws2.auth(&token).await });
                }
                "subscribe" => {
                    let chans: Vec<String> = st["channels"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|c| c.as_str().unwrap().to_string())
                        .collect();
                    tokio::spawn(async move {
                        let refs: Vec<&str> = chans.iter().map(String::as_str).collect();
                        ws2.subscribe(&refs).await
                    });
                }
                _ => {
                    let lb_opts = LiveBalancesOptions {
                        snapshot: Some(source(&snapshot)),
                        owner_id: Some(source(&owner)),
                        min_snapshot_interval: Some(min_interval),
                        ..Default::default()
                    };
                    let h = helper.clone();
                    tokio::spawn(async move {
                        if let Ok(lb) = ws2.live_balances(lb_opts).await {
                            *h.lock().unwrap() = Some(lb);
                        }
                    });
                }
            }
            received.push(conn.recv().await);
        } else if let Some(server) = st.get("server") {
            let mut frame = server.clone();
            if let Some(op) = st.get("reply_to") {
                let req = received
                    .iter()
                    .rev()
                    .find(|m| &m["op"] == op && !answered.contains(&m["id"]))
                    .unwrap_or_else(|| panic!("{at}: no unanswered {op}"))
                    .clone();
                answered.push(req["id"].clone());
                frame["id"] = req["id"].clone();
            }
            conn.send(frame);
            settle(&ws, &mut conn, &mut received).await;
        } else if let Some(o) = st.get("owner") {
            wait_pending(&owner, "owner").await;
            let tx = owner.lock().unwrap().pending.remove(0);
            let _ = tx.send(o.as_str().unwrap().to_string());
            settle(&ws, &mut conn, &mut received).await;
        } else if let Some(rows) = st.get("snapshot") {
            wait_pending(&snapshot, "snapshot").await;
            let rows: Vec<Balance> = serde_json::from_value(rows.clone()).unwrap();
            let tx = snapshot.lock().unwrap().pending.remove(0);
            let _ = tx.send(rows);
            settle(&ws, &mut conn, &mut received).await;
        } else if let Some(ms) = st.get("advance_ms").and_then(Value::as_u64) {
            clock.advance(Duration::from_millis(ms));
            settle(&ws, &mut conn, &mut received).await;
        } else if let Some(want) = st.get("expect_sent") {
            settle(&ws, &mut conn, &mut received).await;
            let got: Vec<Value> = received[sent_mark..].iter().map(norm_sent).collect();
            sent_mark = received.len();
            let want: Vec<Value> = want.as_array().unwrap().iter().map(norm_sent).collect();
            assert_eq!(got, want, "{at}: sent");
        } else if let Some(want) = st.get("expect_requests") {
            settle(&ws, &mut conn, &mut received).await;
            let got = json!({"owner": owner.lock().unwrap().calls, "snapshot": snapshot.lock().unwrap().calls});
            assert_eq!(&got, want, "{at}: requests");
        } else if let Some(want) = st.get("expect_state") {
            settle(&ws, &mut conn, &mut received).await;
            let rows = helper
                .lock()
                .unwrap()
                .as_ref()
                .map(|h| h.all())
                .unwrap_or_default();
            let mut got_assets: Vec<String> = rows.iter().map(|r| r.asset.clone()).collect();
            got_assets.sort();
            let mut want_assets: Vec<String> = want.as_object().unwrap().keys().cloned().collect();
            want_assets.sort();
            assert_eq!(got_assets, want_assets, "{at}: assets");
            for r in rows {
                let w = &want[&r.asset];
                assert_eq!(
                    r.total.as_str(),
                    w["total"].as_str().unwrap(),
                    "{at}: {} total",
                    r.asset
                );
                assert_eq!(
                    r.sequence,
                    w["sequence"].as_i64().unwrap(),
                    "{at}: {} sequence",
                    r.asset
                );
            }
        } else if let Some(want) = st.get("expect_stale").and_then(Value::as_bool) {
            settle(&ws, &mut conn, &mut received).await;
            let stale = helper
                .lock()
                .unwrap()
                .as_ref()
                .map(|h| h.is_stale())
                .unwrap_or(true);
            assert_eq!(stale, want, "{at}: stale");
        } else if let Some(want) = st.get("expect_errors") {
            settle(&ws, &mut conn, &mut received).await;
            drain(&mut rx, &mut events, &mut errors);
            let got = json!(std::mem::take(&mut errors));
            assert_eq!(&got, want, "{at}: errors");
        } else if let Some(want) = st.get("expect_events") {
            settle(&ws, &mut conn, &mut received).await;
            drain(&mut rx, &mut events, &mut errors);
            let got = std::mem::take(&mut events);
            let want = want.as_array().unwrap();
            assert_eq!(got.len(), want.len(), "{at}: events {got:?}");
            for w in want {
                let hit = got.iter().any(|g| {
                    w.as_object().unwrap().iter().all(|(k, v)| {
                        if k == "dropped" {
                            g[k] == sorted(v)
                        } else {
                            &g[k] == v
                        }
                    })
                });
                assert!(hit, "{at}: no event {w} in {got:?}");
            }
        } else {
            panic!("{at}: unknown step {st}");
        }
        drain(&mut rx, &mut events, &mut errors);
    }
    let h = helper.lock().unwrap().take();
    if let Some(h) = h {
        h.close();
    }
    ws.close().await;
}

#[tokio::test]
async fn unsequenced_events_apply_keep_the_sequence_and_warn_once() {
    let mut srv = Server::start().await;
    let ws = WebSocket::new(options(&srv.url)).unwrap();
    let mut rx = ws.events().unwrap();
    ws.connect().await.unwrap();
    let mut c = srv.conn().await;
    let ws2 = ws.clone();
    let a = tokio::spawn(async move { ws2.auth("tok").await });
    let req = c.recv().await;
    c.send(json!({"type": "authenticated", "user_id": "u1", "id": req["id"]}));
    a.await.unwrap().unwrap();
    let snap: crate::BalanceSnapshotFn =
        Arc::new(|| -> BoxFuture<'static, crate::Result<Vec<Balance>>> {
            Box::pin(async {
                Ok(vec![
                    serde_json::from_value(
                        json!({"asset": "USDT", "available": "100", "locked": "0",
                "pending": "0", "total": "100", "held_incoming": [], "sequence": 40}),
                    )
                    .unwrap(),
                ])
            })
        });
    let ws2 = ws.clone();
    let h = tokio::spawn(async move {
        ws2.live_balances(LiveBalancesOptions {
            snapshot: Some(snap),
            account_id: Some("u1".into()),
            ..Default::default()
        })
        .await
    });
    let req = c.recv().await;
    c.send(json!({"type": "subscribed", "channels": ["balances"], "id": req["id"]}));
    let lb = h.await.unwrap().unwrap();
    for _ in 0..200 {
        if !lb.is_stale() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(!lb.is_stale());
    let push = |d: Value| {
        let mut data = json!({"available": "0", "locked": "0", "pending": "0"});
        data.as_object_mut()
            .unwrap()
            .extend(d.as_object().unwrap().clone());
        c.send(json!({"type": "balance.updated", "channel": "balances", "data": data}));
    };
    push(json!({"asset": "USDT", "total": "90"}));
    push(json!({"asset": "USDT", "total": "80"}));
    ws.ping().await.unwrap();
    let row = lb.get("USDT").unwrap();
    assert_eq!((row.total.as_str(), row.sequence), ("80", 40));
    push(json!({"asset": "USDT", "total": "70", "sequence": 41}));
    push(json!({"asset": "USDT", "total": "60", "sequence": 41}));
    ws.ping().await.unwrap();
    let row = lb.get("USDT").unwrap();
    assert_eq!((row.total.as_str(), row.sequence), ("70", 41));
    let mut warnings = 0;
    while let Ok(ev) = rx.try_recv() {
        if let WsEvent::Warning(w) = ev
            && w.contains("without data.sequence")
        {
            warnings += 1;
        }
    }
    assert_eq!(warnings, 1);
    lb.close();
    ws.close().await;
}

#[tokio::test]
async fn default_owner_mismatch_never_merges() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer};
    let rest = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/account/id"))
        .respond_with(super::helpers::data(json!({"user_id": "someone_else"})))
        .mount(&rest)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v1/account/balances"))
        .respond_with(super::helpers::data(json!([])))
        .expect(0)
        .mount(&rest)
        .await;
    let (client, _) = super::helpers::client(&rest);
    let mut srv = Server::start().await;
    let ws = client.websocket(options(&srv.url)).unwrap();
    ws.connect().await.unwrap();
    let mut c = srv.conn().await;
    let ws2 = ws.clone();
    let a = tokio::spawn(async move { ws2.auth("tok").await });
    let req = c.recv().await;
    c.send(json!({"type": "authenticated", "user_id": "u1", "id": req["id"]}));
    a.await.unwrap().unwrap();
    let ws2 = ws.clone();
    let h = tokio::spawn(async move { ws2.live_balances(LiveBalancesOptions::default()).await });
    let req = c.recv().await;
    c.send(json!({"type": "subscribed", "channels": ["balances"], "id": req["id"]}));
    let lb = h.await.unwrap().unwrap();
    for _ in 0..400 {
        if lb.last_error().is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        lb.last_error().map(|e| e.code),
        Some("ACCOUNT_MISMATCH".to_string())
    );
    assert!(lb.all().is_empty() && lb.is_stale());
    lb.close();
    ws.close().await;
}

async fn authed_ws(
    srv: &mut Server,
    client: Option<&crate::Client>,
) -> (WebSocket, WsEvents, Conn) {
    let ws = match client {
        Some(c) => c.websocket(options(&srv.url)).unwrap(),
        None => WebSocket::new(options(&srv.url)).unwrap(),
    };
    let rx = ws.events().unwrap();
    ws.connect().await.unwrap();
    let mut c = srv.conn().await;
    let ws2 = ws.clone();
    let a = tokio::spawn(async move { ws2.auth("tok").await });
    let req = c.recv().await;
    c.send(json!({"type": "authenticated", "user_id": "u1", "id": req["id"]}));
    a.await.unwrap().unwrap();
    (ws, rx, c)
}

#[tokio::test]
async fn custom_snapshot_needs_an_owner() {
    let rest = wiremock::MockServer::start().await;
    let (client, _) = super::helpers::client(&rest);
    let mut srv = Server::start().await;
    let (ws, _rx, _c) = authed_ws(&mut srv, Some(&client)).await;
    let snap: crate::BalanceSnapshotFn =
        Arc::new(|| -> BoxFuture<'static, crate::Result<Vec<Balance>>> {
            Box::pin(async { Ok(vec![]) })
        });
    let err = ws
        .live_balances(LiveBalancesOptions {
            snapshot: Some(snap),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(
        matches!(&err, crate::Error::WebSocket(w) if w.code == "CONFIG"),
        "{err}"
    );
    assert!(rest.received_requests().await.unwrap().is_empty());
    ws.close().await;
}

#[tokio::test]
async fn no_buffering_while_unverified_and_close_marks_stale() {
    let mut srv = Server::start().await;
    let (ws, _rx, mut c) = authed_ws(&mut srv, None).await;
    let owner = Arc::new(Mutex::new("someone_else".to_string()));
    let o = owner.clone();
    let owner_fn: crate::OwnerIdFn =
        Arc::new(move || -> BoxFuture<'static, crate::Result<String>> {
            let v = o.lock().unwrap().clone();
            Box::pin(async move { Ok(v) })
        });
    let snap: crate::BalanceSnapshotFn =
        Arc::new(|| -> BoxFuture<'static, crate::Result<Vec<Balance>>> {
            Box::pin(async {
                Ok(vec![
                    serde_json::from_value(
                        json!({"asset": "USDT", "available": "5", "locked": "0",
                "pending": "0", "total": "5", "held_incoming": [], "sequence": 10}),
                    )
                    .unwrap(),
                ])
            })
        });
    let ws2 = ws.clone();
    let h = tokio::spawn(async move {
        ws2.live_balances(LiveBalancesOptions {
            snapshot: Some(snap),
            owner_id: Some(owner_fn),
            min_snapshot_interval: Some(Duration::ZERO),
            ..Default::default()
        })
        .await
    });
    let req = c.recv().await;
    c.send(json!({"type": "subscribed", "channels": ["balances"], "id": req["id"]}));
    let lb = h.await.unwrap().unwrap();
    for _ in 0..400 {
        if lb.last_error().is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        lb.last_error().map(|e| e.code),
        Some("ACCOUNT_MISMATCH".to_string())
    );
    for i in 0..500 {
        c.send(json!({"type": "balance.updated", "channel": "balances",
            "data": {"asset": "USDT", "available": "9", "locked": "0", "pending": "0", "total": "9", "sequence": 50 + i}}));
    }
    ws.ping().await.unwrap();
    assert_eq!(lb.buffered_events(), 0);
    *owner.lock().unwrap() = "u1".into();
    c.send(json!({"type": "balances.resync", "channel": "balances", "data": {}}));
    for _ in 0..400 {
        if !lb.is_stale() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let row = lb.get("USDT").unwrap();
    assert_eq!((row.total.as_str(), row.sequence), ("5", 10));
    ws.close().await;
    assert!(lb.is_stale());
}

#[tokio::test]
async fn signed_out_auth_lost_payload_and_unknown_reason() {
    let mut srv = Server::start().await;
    let (ws, mut rx, mut c) = authed_ws(&mut srv, None).await;
    c.send(json!({"type": "signed_out", "reason": "revoked"}));
    ws.ping().await.unwrap();
    let mut lost = vec![];
    while let Ok(ev) = rx.try_recv() {
        if let WsEvent::AuthLost(f) = ev {
            lost.push(f);
        }
    }
    assert_eq!(lost.len(), 1);
    assert_eq!(
        (lost[0].r#type.as_str(), lost[0].channel.as_str()),
        ("session.revoked", "account")
    );
    assert_eq!(
        lost[0].data,
        json!({"session_id": null, "reason": "signed_out", "current": true})
    );
    let ws2 = ws.clone();
    let a = tokio::spawn(async move { ws2.auth("tok2").await });
    let req = c.recv().await;
    c.send(json!({"type": "authenticated", "user_id": "u1", "id": req["id"]}));
    a.await.unwrap().unwrap();
    c.send(json!({"type": "signed_out"}));
    ws.ping().await.unwrap();
    let mut last = None;
    while let Ok(ev) = rx.try_recv() {
        if let WsEvent::AuthChanged(ch) = ev {
            last = Some(ch);
        }
    }
    let last = last.unwrap();
    assert_eq!(last.reason, crate::AuthChangeReason::SignedOut);
    assert_eq!(last.code.as_deref(), Some("unknown"));
    ws.close().await;
}
