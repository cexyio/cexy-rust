//! conformance/ws/futures.json: futures event frames, channel names and scenarios, run against the
//! local WebSocket server (it answers only pings; the script sends every other frame).

use std::time::Duration;

use serde_json::{Value, json};

use super::helpers::{KEY, SECRET, load};
use super::ws::{Conn, Server, options};
use super::ws_signout::{norm_sent, sorted};
use crate::{
    Client, ClientOptions, Error, FuturesBookUpdate, FuturesCandleUpdate, FuturesChannel,
    FuturesMids, FuturesOrdersUpdate, FuturesPositionsUpdate, FuturesStatus, FuturesTradesUpdate,
    MAX_PING_INTERVAL, WebSocket, WsEvent, WsEvents, WsOptions,
};

fn welcome() -> Value {
    json!({"type": "welcome", "protocol_version": 1, "heartbeat_interval_seconds": 30,
           "max_subscriptions": 100, "connection_id": "c1", "challenge": "ch-1"})
}

/// A WebSocket from a client that signs (so `auth_key` works), connected to `srv`.
async fn connected(srv: &mut Server) -> (WebSocket, WsEvents, Conn) {
    let client = Client::new(ClientOptions {
        base_url: Some("http://127.0.0.1:1".into()),
        allow_insecure: true,
        ..ClientOptions::with_api_key(KEY, SECRET)
    })
    .unwrap();
    let mut o = options(&srv.url);
    o.reconnect = false;
    let ws = client.websocket(o).unwrap();
    let rx = ws.events().unwrap();
    ws.connect().await.unwrap();
    let conn = srv.conn().await;
    (ws, rx, conn)
}

/// A client frame, if one arrives within `wait`.
async fn maybe_recv(conn: &mut Conn, wait: Duration) -> Option<Value> {
    tokio::time::timeout(wait, conn.from_client.recv())
        .await
        .ok()
        .flatten()
}

/// Two ping round trips (every earlier frame handled), then every frame the client sent.
async fn settle(ws: &WebSocket, conn: &mut Conn) -> Vec<Value> {
    ws.ping().await.unwrap();
    ws.ping().await.unwrap();
    let mut out = vec![];
    while let Ok(m) = conn.from_client.try_recv() {
        out.push(m);
    }
    out
}

fn strings(v: &Value) -> Vec<String> {
    v.as_array()
        .into_iter()
        .flatten()
        .map(|x| x.as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn futures_frames_are_known_events() {
    let Some(spec) = load("ws/futures.json") else {
        return;
    };
    let mut srv = Server::start_with(welcome()).await;
    let (ws, mut rx, conn) = connected(&mut srv).await;
    for case in spec["frames"].as_array().unwrap() {
        let id = case["id"].as_str().unwrap();
        let expect = &case["expect"];
        conn.send(case["frame"].clone());
        let ev = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let WsEvent::Event(f) = rx.recv().await.unwrap() {
                    return f;
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{id}: no event"));
        assert_eq!(ev.r#type, expect["event_type"].as_str().unwrap(), "{id}");
        assert_eq!(ev.channel, expect["channel"].as_str().unwrap(), "{id}");
        assert_eq!(ev.sequence, case["frame"]["sequence"].as_i64(), "{id}");
        if let Some(n) = expect["sequence"].as_i64() {
            assert_eq!(ev.sequence, Some(n), "{id}");
        }
        match ev.r#type.as_str() {
            "futures.mids" => {
                let d: FuturesMids = ev.decode().unwrap();
                assert_eq!(d.mids["kPEPE"].as_str(), "0.009871", "{id}");
                assert!(d.as_of.is_some(), "{id}");
            }
            "futures.orderbook.update" => {
                let d: FuturesBookUpdate = ev.decode().unwrap();
                assert!(d.full, "{id}");
                for (side, levels) in [("best_bid", &d.bids), ("best_ask", &d.asks)] {
                    let want = strings(&expect[side]);
                    assert_eq!(
                        [levels[0].price.as_str(), levels[0].size.as_str()],
                        [want[0].as_str(), want[1].as_str()],
                        "{id}: {side}"
                    );
                }
            }
            "futures.trades.new" => {
                let d: FuturesTradesUpdate = ev.decode().unwrap();
                assert_eq!(
                    d.trades.len() as u64,
                    expect["trade_count"].as_u64().unwrap(),
                    "{id}"
                );
            }
            "futures.candle.update" => {
                let d: FuturesCandleUpdate = ev.decode().unwrap();
                assert_eq!(
                    d.candle.open_time,
                    expect["open_time"].as_i64().unwrap(),
                    "{id}"
                );
            }
            "futures.status" => {
                let d: FuturesStatus = ev.decode().unwrap();
                assert_eq!(d.state, expect["state"].as_str().unwrap(), "{id}");
            }
            "futures.positions" => {
                let d: FuturesPositionsUpdate = ev.decode().unwrap();
                let n = d.positions.unwrap().positions.len() as u64;
                assert_eq!(n, expect["position_count"].as_u64().unwrap(), "{id}");
                assert_eq!(d.stale, expect["stale"].as_bool().unwrap(), "{id}");
            }
            "futures.orders" => {
                let d: FuturesOrdersUpdate = ev.decode().unwrap();
                assert_eq!(
                    d.orders.len() as u64,
                    expect["order_count"].as_u64().unwrap(),
                    "{id}"
                );
            }
            other => panic!("{id}: no check for {other}"),
        }
    }
    ws.close().await;
}

fn channel(kind: &str, args: &[String]) -> crate::Result<String> {
    match (kind, args) {
        ("mids", []) => Ok(FuturesChannel::mids()),
        ("status", []) => Ok(FuturesChannel::status()),
        ("account", []) => Ok(FuturesChannel::account()),
        ("orderbook", [coin]) => FuturesChannel::orderbook(coin),
        ("trades", [coin]) => FuturesChannel::trades(coin),
        ("candles", [coin, interval]) => FuturesChannel::candles(coin, interval),
        other => panic!("no helper for {other:?}"),
    }
}

#[tokio::test]
async fn futures_channel_names() {
    let Some(spec) = load("ws/futures.json") else {
        return;
    };
    let names = &spec["channel_names"];
    for v in names["valid"].as_array().unwrap() {
        let name = channel(v[0].as_str().unwrap(), &strings(&v[1])).unwrap();
        assert_eq!(name, v[2].as_str().unwrap());
        crate::ws_futures::check_channel(&name).unwrap();
    }
    let mut srv = Server::start_with(welcome()).await;
    let (ws, _rx, mut conn) = connected(&mut srv).await;
    for v in names["invalid"].as_array().unwrap() {
        let (kind, args) = (v[0].as_str().unwrap(), strings(&v[1]));
        let e = channel(kind, &args).unwrap_err();
        assert!(matches!(e, Error::Config(_)), "{v}: {e}");
        // The same name given to subscribe is refused locally too, before anything is sent.
        let raw = format!("futures.{kind}:{}", args.join(":"));
        let e = ws.subscribe(&[&raw]).await.unwrap_err();
        assert!(
            matches!(e, Error::Config(_)) || raw.len() > 64,
            "{raw}: {e}"
        );
    }
    assert!(settle(&ws, &mut conn).await.is_empty(), "nothing sent");
    assert!(ws.channels().is_empty());
    ws.close().await;
}

#[test]
fn ping_interval_above_60_seconds_is_a_config_error() {
    let max = load("ws/futures.json")
        .and_then(|s| s["ping_interval_max_seconds"].as_u64())
        .unwrap_or(60);
    assert!(MAX_PING_INTERVAL <= Duration::from_secs(max));
    assert!(WsOptions::default().ping_interval <= MAX_PING_INTERVAL);
    let build = |secs| {
        WebSocket::new(WsOptions {
            ping_interval: Duration::from_secs(secs),
            ..Default::default()
        })
    };
    let e = build(61).unwrap_err();
    assert!(matches!(e, Error::Config(_)), "{e}");
    assert_eq!(
        build(60).unwrap().inner.opts.ping_interval,
        MAX_PING_INTERVAL
    );
}

/// The request of `op` most recently sent and not yet answered.
fn unanswered(sent: &[Value], answered: &mut Vec<Value>, op: &str) -> Option<Value> {
    let id = sent
        .iter()
        .rev()
        .find(|m| m["op"] == op && !answered.contains(&m["id"]))?["id"]
        .clone();
    answered.push(id.clone());
    Some(id)
}

#[tokio::test]
async fn futures_scenarios() {
    let Some(spec) = load("ws/futures.json") else {
        return;
    };
    let scenarios = spec["scenarios"].as_array().unwrap();
    assert!(!scenarios.is_empty());
    // Each scenario runs in its own task, so one failure does not hide the others.
    let mut failed = vec![];
    for sc in scenarios {
        let id = sc["id"].as_str().unwrap().to_string();
        if tokio::spawn(run_scenario(sc.clone())).await.is_err() {
            failed.push(id);
        }
    }
    assert!(failed.is_empty(), "failed scenarios: {failed:?}");
}

async fn run_scenario(sc: Value) {
    let id = sc["id"].as_str().unwrap();
    let mut srv = Server::start_with(welcome()).await;
    let (ws, mut rx, mut conn) = connected(&mut srv).await;
    let mut sent: Vec<Value> = vec![]; // every frame the client sent
    let mut automatic: Vec<Value> = vec![]; // the ones no client step asked for
    let mut answered: Vec<Value> = vec![];
    for (i, step) in sc["steps"].as_array().unwrap().iter().enumerate() {
        let at = format!("{id} step {i}");
        if let Some(client) = step.get("client").and_then(Value::as_str) {
            let ws2 = ws.clone();
            let wait = match client {
                "auth_key" => {
                    tokio::spawn(async move { ws2.auth_key().await });
                    Duration::from_secs(5)
                }
                "subscribe" => {
                    let channels = strings(&step["channels"]);
                    tokio::spawn(async move {
                        let refs: Vec<&str> = channels.iter().map(String::as_str).collect();
                        ws2.subscribe(&refs).await
                    });
                    // A held private channel sends nothing.
                    Duration::from_millis(300)
                }
                other => panic!("{at}: unknown client step {other}"),
            };
            if let Some(m) = maybe_recv(&mut conn, wait).await {
                sent.push(m);
            }
        } else {
            let (mut frame, op) = match (step.get("server"), step.get("server_reply_to")) {
                (Some(f), _) => {
                    // An acknowledgement carries the id of the request it answers.
                    let op = match f["type"].as_str() {
                        Some("authenticated") => Some("auth_key"),
                        Some("subscribed") => Some("subscribe"),
                        Some("unsubscribed") => Some("unsubscribe"),
                        _ => None,
                    };
                    (f.clone(), op)
                }
                (None, Some(op)) => (step["frame"].clone(), op.as_str()),
                _ => panic!("{at}: unknown step {step}"),
            };
            if let Some(op) = op
                && let Some(rid) = unanswered(&sent, &mut answered, op)
            {
                frame["id"] = rid;
            } else if step.get("server_reply_to").is_some() {
                panic!("{at}: no unanswered {op:?} request");
            }
            conn.send(frame);
            let more = settle(&ws, &mut conn).await;
            sent.extend(more.iter().cloned());
            automatic.extend(more);
        }
    }
    let expect = &sc["expect"];
    if let Some(want) = expect.get("client_sends_after") {
        let want: Vec<Value> = want.as_array().unwrap().iter().map(norm_sent).collect();
        while automatic.len() < want.len() {
            match maybe_recv(&mut conn, Duration::from_secs(5)).await {
                Some(m) => automatic.push(m),
                None => break,
            }
        }
        if let Some(m) = maybe_recv(&mut conn, Duration::from_millis(300)).await {
            automatic.push(m);
        }
        let got: Vec<Value> = automatic.iter().map(norm_sent).collect();
        assert_eq!(got, want, "{id}: client_sends_after");
    }
    settle(&ws, &mut conn).await;
    let (mut events, mut resyncs, mut errors) = (vec![], vec![], vec![]);
    while let Ok(ev) = rx.try_recv() {
        match ev {
            WsEvent::Event(f) => events.push(f.r#type),
            WsEvent::ChannelResync(c) => resyncs.push(c),
            WsEvent::ServerError(e) => errors.push(e.code),
            _ => {}
        }
    }
    if let Some(want) = expect.get("events") {
        assert_eq!(events, strings(want), "{id}: events");
    }
    if let Some(want) = expect.get("resync_channels") {
        assert_eq!(resyncs, strings(want), "{id}: resync_channels");
    }
    if let Some(want) = expect.get("errors") {
        assert_eq!(errors, strings(want), "{id}: errors");
    }
    if let Some(want) = expect.get("held_channels_after") {
        assert_eq!(sorted(&json!(ws.channels())), sorted(want), "{id}: held");
        if want.as_array().unwrap().is_empty() {
            assert!(ws.pending_channels().is_empty(), "{id}: nothing pending");
        }
    }
    if let Some(want) = expect.get("held_pending_private") {
        assert_eq!(
            sorted(&json!(ws.pending_channels())),
            sorted(want),
            "{id}: pending"
        );
        assert!(ws.channels().is_empty(), "{id}: held");
    }
    ws.close().await;
}

fn subscribe_task(
    ws: &WebSocket,
    channels: &[&str],
) -> tokio::task::JoinHandle<crate::Result<crate::SubscribeResult>> {
    let ws2 = ws.clone();
    let owned: Vec<String> = channels.iter().map(|c| c.to_string()).collect();
    tokio::spawn(async move {
        let refs: Vec<&str> = owned.iter().map(String::as_str).collect();
        ws2.subscribe(&refs).await
    })
}

fn refusal(code: &str, id: &Value) -> Value {
    json!({"type": "error", "code": code, "message": code.to_lowercase(), "id": id})
}

/// The server answers a subscribe with one error frame per refused channel (with the request id,
/// not naming the channel) BEFORE its single `subscribed` ack, and sends no ack when it refused
/// every channel. Spot and futures alike; the channels stay in one frame.
#[tokio::test]
async fn subscribe_refusals_are_collected_until_the_ack() {
    let mut srv = Server::start_with(welcome()).await;
    let (ws, mut rx, mut conn) = connected(&mut srv).await;

    // Partly refused: accepted ones held, refused ones reported and not held.
    let t = subscribe_task(
        &ws,
        &[
            "ticker:BTC/USDT",
            "trades:NOPE/USDT",
            "futures.mids",
            "futures.trades:BTC",
        ],
    );
    let req = conn.recv().await;
    assert_eq!(
        req["channels"],
        json!([
            "ticker:BTC/USDT",
            "trades:NOPE/USDT",
            "futures.mids",
            "futures.trades:BTC"
        ])
    );
    conn.send(refusal("NOT_FOUND", &req["id"]));
    conn.send(refusal("RATE_LIMITED", &req["id"]));
    conn.send(json!({"type": "subscribed", "channels": ["ticker:BTC/USDT", "futures.mids"], "id": req["id"]}));
    let r = t.await.unwrap().unwrap();
    assert_eq!(r.added, ["ticker:BTC/USDT", "futures.mids"]);
    let rejected: Vec<(&str, &str)> = r
        .rejected
        .iter()
        .map(|(c, e)| (c.as_str(), e.code.as_str()))
        .collect();
    assert_eq!(
        rejected,
        [
            ("trades:NOPE/USDT", "NOT_FOUND"),
            ("futures.trades:BTC", "RATE_LIMITED")
        ]
    );
    assert_eq!(ws.channels(), ["ticker:BTC/USDT", "futures.mids"]);

    // Every channel refused: complete at the last error (no ack follows), an error, nothing held.
    let t = subscribe_task(&ws, &["futures.orderbook:XYZ", "ticker:NOPE/USDT"]);
    let req = conn.recv().await;
    conn.send(refusal("NOT_FOUND", &req["id"]));
    conn.send(refusal("NOT_FOUND", &req["id"]));
    let e = tokio::time::timeout(Duration::from_millis(300), t)
        .await
        .expect("done at the last refusal, before ack_timeout")
        .unwrap()
        .unwrap_err();
    assert!(
        matches!(&e, Error::WebSocket(w) if w.code == "NOT_FOUND" && w.from_server),
        "{e}"
    );

    // No ack within ack_timeout after at least one error: all refused.
    let t = subscribe_task(&ws, &["trades:A/B", "trades:C/D"]);
    let req = conn.recv().await;
    conn.send(refusal("VALIDATION_FAILED", &req["id"]));
    let e = t.await.unwrap().unwrap_err();
    assert!(
        matches!(&e, Error::WebSocket(w) if w.code == "VALIDATION_FAILED"),
        "{e}"
    );
    assert_eq!(ws.channels(), ["ticker:BTC/USDT", "futures.mids"]);

    // Nothing is resent; every refusal was reported as a server error.
    assert!(settle(&ws, &mut conn).await.is_empty());
    let mut refusals = 0;
    while let Ok(ev) = rx.try_recv() {
        if matches!(ev, WsEvent::ServerError(_)) {
            refusals += 1;
        }
    }
    assert_eq!(refusals, 5);

    // Spot names come back normalised: compared ignoring case. Futures coins exactly.
    let t = subscribe_task(&ws, &["ticker:eth/usdt", "futures.trades:btc"]);
    let req = conn.recv().await;
    conn.send(refusal("NOT_FOUND", &req["id"]));
    conn.send(json!({"type": "subscribed", "channels": ["ticker:ETH/USDT"], "id": req["id"]}));
    let r = t.await.unwrap().unwrap();
    assert_eq!(r.added, ["ticker:ETH/USDT"]);
    assert_eq!(r.rejected.len(), 1);
    assert_eq!(r.rejected[0].0, "futures.trades:btc");

    // The server rewrites `_` to `/` in a spot market: `ticker:btc_usdt` is acked as
    // `ticker:BTC/USDT`, so only the other channel was refused.
    let t = subscribe_task(&ws, &["ticker:btc_usdt", "ticker:nope_usdt"]);
    let req = conn.recv().await;
    conn.send(refusal("NOT_FOUND", &req["id"]));
    conn.send(json!({"type": "subscribed", "channels": ["ticker:BTC/USDT"], "id": req["id"]}));
    let r = t.await.unwrap().unwrap();
    let rejected: Vec<&str> = r.rejected.iter().map(|(c, _)| c.as_str()).collect();
    assert_eq!(rejected, ["ticker:nope_usdt"]);
    assert!(ws.channels().contains(&"ticker:btc_usdt".to_string()));

    // Fewer errors than missing channels (the server's subscription cap stops the frame): the last
    // error covers the rest.
    let t = subscribe_task(&ws, &["ticker:A/B", "ticker:C/D", "ticker:E/F"]);
    let req = conn.recv().await;
    conn.send(refusal("RATE_LIMITED", &req["id"]));
    conn.send(json!({"type": "subscribed", "channels": ["ticker:A/B"], "id": req["id"]}));
    let r = t.await.unwrap().unwrap();
    let rejected: Vec<(&str, &str)> = r
        .rejected
        .iter()
        .map(|(c, e)| (c.as_str(), e.code.as_str()))
        .collect();
    assert_eq!(
        rejected,
        [
            ("ticker:C/D", "RATE_LIMITED"),
            ("ticker:E/F", "RATE_LIMITED")
        ]
    );

    // No ack and no error within ack_timeout: TIMEOUT, and the channel stays held (a reconnect
    // sends it again).
    let t = subscribe_task(&ws, &["ticker:ETH/BTC"]);
    conn.recv().await;
    let e = t.await.unwrap().unwrap_err();
    assert!(
        matches!(&e, Error::WebSocket(w) if w.code == "TIMEOUT" && !w.from_server),
        "{e}"
    );
    assert!(ws.channels().contains(&"ticker:ETH/BTC".to_string()));
    ws.close().await;
}

/// After a reconnect, a private channel refused as UNAUTHENTICATED waits for the next auth; any
/// other refusal drops the channel and is reported. Spot and futures alike.
#[tokio::test]
async fn resubscribe_after_reconnect_sorts_refusals() {
    let mut srv = Server::start_with(welcome()).await;
    let ws = WebSocket::new(options(&srv.url)).unwrap();
    let mut rx = ws.events().unwrap();
    ws.connect().await.unwrap();
    let mut c1 = srv.conn().await;
    let ws2 = ws.clone();
    let t = tokio::spawn(async move { ws2.auth("tok").await });
    let req = c1.recv().await;
    c1.send(json!({"type": "authenticated", "user_id": "u1", "id": req["id"]}));
    t.await.unwrap().unwrap();
    let t = subscribe_task(&ws, &["orders", "futures.mids", "ticker:BTC/USDT"]);
    let req = c1.recv().await;
    c1.send(json!({"type": "subscribed", "channels": req["channels"], "id": req["id"]}));
    t.await.unwrap().unwrap();

    c1.close();
    let mut c2 = srv.conn().await;
    let auth = c2.recv().await;
    assert_eq!(auth["op"], "auth");
    c2.send(json!({"type": "authenticated", "user_id": "u1", "id": auth["id"]}));
    let sub = c2.recv().await;
    assert_eq!(
        sub["channels"],
        json!(["orders", "futures.mids", "ticker:BTC/USDT"])
    );
    c2.send(refusal("UNAUTHENTICATED", &sub["id"]));
    c2.send(refusal("SERVICE_UNAVAILABLE", &sub["id"]));
    c2.send(json!({"type": "subscribed", "channels": ["ticker:BTC/USDT"], "id": sub["id"]}));
    assert!(settle(&ws, &mut c2).await.is_empty(), "no automatic retry");
    assert_eq!(ws.channels(), ["ticker:BTC/USDT"]);
    assert_eq!(ws.pending_channels(), ["orders"]);
    let mut reported = vec![];
    while let Ok(ev) = rx.try_recv() {
        if let WsEvent::Error(Error::WebSocket(w)) = ev {
            reported.push(w.code);
        }
    }
    assert_eq!(reported, ["UNAUTHENTICATED", "SERVICE_UNAVAILABLE"]);
    ws.close().await;
}

/// After a re-auth, the pending private channels are re-subscribed: a refusal other than
/// UNAUTHENTICATED drops the channel (not pending any more).
#[tokio::test]
async fn resubscribe_after_reauth_drops_a_refused_channel() {
    let mut srv = Server::start_with(welcome()).await;
    let (ws, mut rx, mut conn) = connected(&mut srv).await;
    let ws2 = ws.clone();
    let t = tokio::spawn(async move { ws2.auth_key().await });
    let req = conn.recv().await;
    conn.send(
        json!({"type": "authenticated", "user_id": "u1", "challenge": "ch-2", "id": req["id"]}),
    );
    t.await.unwrap().unwrap();
    let t = subscribe_task(&ws, &["orders", "futures.account"]);
    let req = conn.recv().await;
    conn.send(json!({"type": "subscribed", "channels": req["channels"], "id": req["id"]}));
    t.await.unwrap().unwrap();

    conn.send(json!({"type": "signed_out", "reason": "expired"}));
    settle(&ws, &mut conn).await;
    assert_eq!(ws.pending_channels(), ["orders", "futures.account"]);
    let ws2 = ws.clone();
    let t = tokio::spawn(async move { ws2.auth_key().await });
    let req = conn.recv().await;
    conn.send(
        json!({"type": "authenticated", "user_id": "u1", "challenge": "ch-3", "id": req["id"]}),
    );
    t.await.unwrap().unwrap();
    let sub = conn.recv().await;
    assert_eq!(sub["channels"], json!(["orders", "futures.account"]));
    conn.send(refusal("NOT_FOUND", &sub["id"]));
    conn.send(json!({"type": "subscribed", "channels": ["orders"], "id": sub["id"]}));
    assert!(settle(&ws, &mut conn).await.is_empty());
    assert_eq!(ws.channels(), ["orders"]);
    assert!(ws.pending_channels().is_empty());
    let reported: Vec<String> = std::iter::from_fn(|| rx.try_recv().ok())
        .filter_map(|ev| match ev {
            WsEvent::Error(Error::WebSocket(w)) => Some(w.code),
            _ => None,
        })
        .collect();
    assert_eq!(reported, ["NOT_FOUND"]);
    ws.close().await;
}

#[tokio::test]
async fn futures_account_waits_for_auth_then_follows_sign_out() {
    let mut srv = Server::start_with(welcome()).await;
    let (ws, _rx, mut conn) = connected(&mut srv).await;
    let ws2 = ws.clone();
    let t =
        tokio::spawn(async move { ws2.subscribe(&["futures.account", "futures.status"]).await });
    // futures.status is sent alone; futures.account waits for auth.
    let req = conn.recv().await;
    assert_eq!(req["channels"], json!(["futures.status"]));
    conn.send(json!({"type": "subscribed", "channels": ["futures.status"], "id": req["id"]}));
    let r = t.await.unwrap().unwrap();
    assert_eq!(
        (r.added, r.pending),
        (
            vec!["futures.status".to_string()],
            vec!["futures.account".to_string()]
        )
    );
    assert_eq!(ws.pending_channels(), ["futures.account"]);
    assert_eq!(ws.channels(), ["futures.status"]);

    // auth_key succeeds: the held channel is subscribed.
    let ws2 = ws.clone();
    let t = tokio::spawn(async move { ws2.auth_key().await });
    let auth = conn.recv().await;
    assert_eq!(auth["op"], "auth_key");
    conn.send(json!({"type": "authenticated", "user_id": "u1", "auth": "api_key", "challenge": "ch-2", "id": auth["id"]}));
    t.await.unwrap().unwrap();
    let sub = conn.recv().await;
    assert_eq!(sub["channels"], json!(["futures.account"]));
    conn.send(json!({"type": "subscribed", "channels": ["futures.account"], "id": sub["id"]}));
    settle(&ws, &mut conn).await;
    assert!(ws.pending_channels().is_empty());
    assert!(ws.channels().contains(&"futures.account".to_string()));

    // A server sign-out ends it like the other private channels: pending again.
    conn.send(json!({"type": "signed_out", "reason": "key_revoked"}));
    settle(&ws, &mut conn).await;
    assert_eq!(ws.pending_channels(), ["futures.account"]);
    assert_eq!(ws.channels(), ["futures.status"]);
    ws.close().await;
}

/// conformance/ws/subscribe_refusals.json: refusals collected per request id, paired with the
/// channels missing from the ack (canonical spot names), never attributed to another request.
#[tokio::test]
async fn subscribe_refusals_conformance() {
    let Some(spec) = load("ws/subscribe_refusals.json") else {
        return;
    };
    let cases = spec["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 7);
    for case in cases {
        let id = case["id"].as_str().unwrap();
        let mut srv = Server::start_with(welcome()).await;
        let (ws, mut rx, mut conn) = connected(&mut srv).await;
        // (name, channels) of each request; a single request is "r".
        let requests: Vec<(String, Vec<String>)> = match case.get("concurrent") {
            Some(c) => c
                .as_array()
                .unwrap()
                .iter()
                .map(|r| {
                    (
                        r["request"].as_str().unwrap().to_string(),
                        strings(&r["send"]),
                    )
                })
                .collect(),
            None => vec![("r".to_string(), strings(&case["send"]))],
        };
        let start = std::time::Instant::now();
        let mut tasks = vec![];
        let mut ids = std::collections::HashMap::new();
        for (name, channels) in &requests {
            let refs: Vec<&str> = channels.iter().map(String::as_str).collect();
            tasks.push((name.clone(), channels.clone(), subscribe_task(&ws, &refs)));
            let req = conn.recv().await;
            assert_eq!(
                strings(&req["channels"]),
                *channels,
                "{id}: one frame per subscribe"
            );
            ids.insert(name.clone(), req["id"].clone());
        }
        for step in case["server"].as_array().unwrap() {
            let (to, mut frame) = match step.get("frame") {
                Some(f) => (step["to"].as_str().unwrap().to_string(), f.clone()),
                None => ("r".to_string(), step.clone()),
            };
            frame["id"] = ids[&to].clone();
            conn.send(frame);
        }
        for (name, sent, task) in tasks {
            let at = format!("{id} {name}");
            let expect = match case["expect"].get(&name) {
                Some(e) => e,
                None => &case["expect"],
            };
            let result = task.await.unwrap();
            if let Some(code) = expect["error_code"].as_str() {
                let e = result.unwrap_err();
                assert!(
                    matches!(&e, Error::WebSocket(w) if w.code == code),
                    "{at}: {e}"
                );
                for c in strings(&expect["held_after"]) {
                    assert!(ws.channels().contains(&c), "{at}: {c} held");
                }
                continue;
            }
            let refused = expect["refused"].as_object().unwrap();
            if expect["fails"].as_bool().unwrap() {
                let e = result.unwrap_err();
                let first = refused.values().next().unwrap().as_str().unwrap();
                assert!(
                    matches!(&e, Error::WebSocket(w) if w.code == first && w.from_server),
                    "{at}: {e}"
                );
                if expect["completes_before_timeout"].as_bool() == Some(true) {
                    assert!(
                        start.elapsed() < Duration::from_millis(400),
                        "{at}: waited for the timeout"
                    );
                }
            } else {
                let r = result.unwrap_or_else(|e| panic!("{at}: {e}"));
                assert_eq!(r.added, strings(&expect["added"]), "{at}: added");
                let got: Vec<(String, String)> = r
                    .rejected
                    .iter()
                    .map(|(c, e)| (c.clone(), e.code.clone()))
                    .collect();
                let want: Vec<(String, String)> = sent
                    .iter()
                    .filter_map(|c| {
                        refused
                            .get(c)
                            .map(|v| (c.clone(), v.as_str().unwrap().to_string()))
                    })
                    .collect();
                assert_eq!(got, want, "{at}: refused");
            }
            for c in refused.keys() {
                assert!(!ws.channels().contains(c), "{at}: refused {c} not held");
            }
        }
        // Nothing is retried.
        assert!(
            settle(&ws, &mut conn).await.is_empty(),
            "{id}: nothing resent"
        );
        let errors = std::iter::from_fn(|| rx.try_recv().ok())
            .filter(|ev| matches!(ev, WsEvent::ServerError(_)))
            .count();
        let frames = case["server"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|f| f["type"] == "error" || f["frame"]["type"] == "error")
            .count();
        assert_eq!(errors, frames, "{id}: every error frame reported once");
        ws.close().await;
    }
}
