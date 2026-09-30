//! conformance/ws/private_signout.json, run step by step against the local server (it answers
//! only pings; the script sends every other frame).

use serde_json::{Map, Value, json};

use super::helpers::load;
use super::ws::{Conn, Server, options};
use crate::{AuthChangeReason, ResyncReason, WebSocket, WsEvent, WsEvents};

pub(super) fn sorted(v: &Value) -> Value {
    let mut out: Vec<String> = v
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    out.sort();
    json!(out)
}

pub(super) fn norm_sent(m: &Value) -> Value {
    let mut o = Map::new();
    o.insert("op".into(), m["op"].clone());
    if let Some(t) = m.get("token") {
        o.insert("token".into(), t.clone());
    }
    if let Some(c) = m.get("channels") {
        o.insert("channels".into(), sorted(c));
    }
    Value::Object(o)
}

pub(super) fn tracked(ev: WsEvent) -> Option<Value> {
    match ev {
        WsEvent::AuthChanged(a) => {
            let reason = match a.reason {
                AuthChangeReason::UserChanged => "user_changed",
                AuthChangeReason::AuthFailed => "auth_failed",
                AuthChangeReason::SessionRevoked => "session_revoked",
                AuthChangeReason::TokenExpired => "token_expired",
                AuthChangeReason::SignedOut => "signed_out",
            };
            let mut v = json!({"type": "auth_changed", "reason": reason, "previous_user_id": a.previous_user_id,
                               "user_id": a.user_id, "dropped": sorted(&json!(a.dropped))});
            if let Some(c) = a.code {
                v["code"] = json!(c);
            }
            Some(v)
        }
        WsEvent::Resync(r) => Some(json!({"type": "resync", "reason": match r {
            ResyncReason::Reauth => "reauth",
            ResyncReason::Reconnect => "reconnect",
            ResyncReason::SequenceGap => "sequence_gap",
            ResyncReason::BalancesResync => "balances_resync",
            ResyncReason::DepositsResync => "deposits_resync",
            ResyncReason::WithdrawalsResync => "withdrawals_resync",
            _ => "concurrent_modification",
        }})),
        WsEvent::AuthLost(_) => Some(json!({"type": "auth_lost"})),
        _ => None,
    }
}

/// Two ping round trips: the client has handled every earlier frame, and everything it sent in
/// reaction has reached the server.
pub(super) async fn settle(ws: &WebSocket, conn: &mut Conn, received: &mut Vec<Value>) {
    ws.ping().await.unwrap();
    ws.ping().await.unwrap();
    while let Ok(m) = conn.from_client.try_recv() {
        received.push(m);
    }
}

fn drain(rx: &mut WsEvents) -> Vec<Value> {
    let mut out = vec![];
    while let Ok(ev) = rx.try_recv() {
        if let Some(v) = tracked(ev) {
            out.push(v);
        }
    }
    out
}

#[tokio::test]
async fn private_signout_conformance() {
    let Some(spec) = load("ws/private_signout.json") else {
        return;
    };
    let server = load("ws/server_signout.json").unwrap();
    let cases: Vec<Value> = spec["cases"]
        .as_array()
        .unwrap()
        .iter()
        .chain(server["cases"].as_array().unwrap())
        .cloned()
        .collect();
    assert!(!cases.is_empty());
    // Each case runs in its own task, so one failure does not hide the others.
    let mut failed = vec![];
    for case in &cases {
        let id = case["id"].as_str().unwrap().to_string();
        if tokio::spawn(run_case(case.clone())).await.is_err() {
            failed.push(id);
        }
    }
    assert!(failed.is_empty(), "failed cases: {failed:?}");
}

async fn run_case(case: Value) {
    {
        let id = case["id"].as_str().unwrap();
        let mut srv = Server::start().await;
        let mut o = options(&srv.url);
        o.reconnect = false;
        let ws = WebSocket::new(o).unwrap();
        let mut rx = ws.events().unwrap();
        ws.connect().await.unwrap();
        let mut conn = srv.conn().await;
        let mut received: Vec<Value> = vec![];
        let mut answered: Vec<Value> = vec![];
        let mut sent_mark = 0;
        drain(&mut rx);
        for (i, step) in case["steps"].as_array().unwrap().iter().enumerate() {
            let at = format!("{id} step {i}");
            if let Some(client) = step.get("client").and_then(Value::as_str) {
                let ws2 = ws.clone();
                match client {
                    "auth" => {
                        let token = step["token"].as_str().unwrap().to_string();
                        tokio::spawn(async move { ws2.auth(&token).await });
                    }
                    _ => {
                        let channels: Vec<String> = step["channels"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|c| c.as_str().unwrap().to_string())
                            .collect();
                        tokio::spawn(async move {
                            let refs: Vec<&str> = channels.iter().map(String::as_str).collect();
                            ws2.subscribe(&refs).await
                        });
                    }
                }
                received.push(conn.recv().await);
            } else if let Some(server) = step.get("server") {
                let mut frame = server.clone();
                if let Some(op) = step.get("reply_to") {
                    let req = received
                        .iter()
                        .rev()
                        .find(|m| &m["op"] == op && !answered.contains(&m["id"]))
                        .unwrap_or_else(|| panic!("{at}: no unanswered {op} request"))
                        .clone();
                    answered.push(req["id"].clone());
                    frame["id"] = req["id"].clone();
                }
                conn.send(frame);
                settle(&ws, &mut conn, &mut received).await;
            } else if let Some(want) = step.get("expect_sent") {
                settle(&ws, &mut conn, &mut received).await;
                let got: Vec<Value> = received[sent_mark..].iter().map(norm_sent).collect();
                sent_mark = received.len();
                let want: Vec<Value> = want.as_array().unwrap().iter().map(norm_sent).collect();
                assert_eq!(got, want, "{at}: sent");
            } else if let Some(want) = step.get("expect_events") {
                settle(&ws, &mut conn, &mut received).await;
                let got = drain(&mut rx);
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
            } else if let Some(want) = step.get("expect_held") {
                settle(&ws, &mut conn, &mut received).await;
                assert_eq!(sorted(&json!(ws.channels())), sorted(want), "{at}: held");
            } else if let Some(want) = step.get("expect_token").and_then(Value::as_bool) {
                settle(&ws, &mut conn, &mut received).await;
                assert_eq!(ws.has_token(), want, "{at}: token");
            } else {
                panic!("{at}: unknown step {step}");
            }
        }
        ws.close().await;
    }
}

async fn ack(conn: &mut Conn, op: &str, mut reply: Value) -> Value {
    let req = conn.recv().await;
    assert_eq!(req["op"], op, "{req}");
    reply["id"] = req["id"].clone();
    conn.send(reply);
    req
}

async fn authed(ws: &WebSocket, conn: &mut Conn, token: &str, user: &str) {
    let ws2 = ws.clone();
    let token = token.to_string();
    let t = tokio::spawn(async move { ws2.auth(&token).await });
    ack(
        conn,
        "auth",
        json!({"type": "authenticated", "user_id": user}),
    )
    .await;
    t.await.unwrap().unwrap();
}

async fn subscribed(ws: &WebSocket, conn: &mut Conn, channels: &[&str]) {
    let ws2 = ws.clone();
    let owned: Vec<String> = channels.iter().map(|c| c.to_string()).collect();
    let t = tokio::spawn(async move {
        let refs: Vec<&str> = owned.iter().map(String::as_str).collect();
        ws2.subscribe(&refs).await
    });
    ack(
        conn,
        "subscribe",
        json!({"type": "subscribed", "channels": channels}),
    )
    .await;
    t.await.unwrap().unwrap();
}

#[tokio::test]
async fn failed_reauth_then_reconnect_restores_private_on_next_auth() {
    let mut srv = Server::start().await;
    let ws = WebSocket::new(options(&srv.url)).unwrap();
    let mut rx = ws.events().unwrap();
    ws.connect().await.unwrap();
    let mut c1 = srv.conn().await;
    authed(&ws, &mut c1, "tok", "u1").await;
    subscribed(&ws, &mut c1, &["orders", "ticker:BTC/USDT"]).await;
    let ws2 = ws.clone();
    let bad = tokio::spawn(async move { ws2.auth("expired").await });
    ack(
        &mut c1,
        "auth",
        json!({"type": "error", "code": "TOKEN_EXPIRED", "message": "expired"}),
    )
    .await;
    assert!(bad.await.unwrap().is_err());
    assert_eq!(ws.channels(), vec!["ticker:BTC/USDT"]);
    assert!(!ws.has_token());
    let got = drain(&mut rx);
    assert!(
        got.iter().any(|e| e["reason"] == "auth_failed"
            && e["code"] == "TOKEN_EXPIRED"
            && e["previous_user_id"] == "u1"
            && e["dropped"] == json!(["orders"])),
        "{got:?}"
    );
    // A private subscribe while signed out is not re-sent: it is pending.
    let res = ws.subscribe(&["orders"]).await.unwrap();
    assert_eq!(res.already_subscribed, vec!["orders"]);

    c1.close();
    let mut c2 = srv.conn().await;
    // No token: only the public channel comes back, and no auth is sent.
    let req = ack(
        &mut c2,
        "subscribe",
        json!({"type": "subscribed", "channels": ["ticker:BTC/USDT"]}),
    )
    .await;
    assert_eq!(req["channels"], json!(["ticker:BTC/USDT"]));
    authed(&ws, &mut c2, "tok2", "u1").await;
    let req = ack(
        &mut c2,
        "subscribe",
        json!({"type": "subscribed", "channels": ["orders"]}),
    )
    .await;
    assert_eq!(req["channels"], json!(["orders"]));
    ws.ping().await.unwrap();
    let mut held = ws.channels();
    held.sort();
    assert_eq!(held, vec!["orders", "ticker:BTC/USDT"]);
    assert!(
        drain(&mut rx)
            .iter()
            .any(|e| e["type"] == "resync" && e["reason"] == "reauth")
    );
    ws.close().await;
}

#[tokio::test]
async fn session_revoked_needs_current_true() {
    let mut srv = Server::start().await;
    let ws = WebSocket::new(options(&srv.url)).unwrap();
    ws.connect().await.unwrap();
    let mut c = srv.conn().await;
    authed(&ws, &mut c, "tok", "u1").await;
    subscribed(&ws, &mut c, &["account", "orders"]).await;
    for current in [json!(false), json!("true"), Value::Null] {
        c.send(json!({"type": "session.revoked", "channel": "account",
                      "data": {"session_id": null, "reason": "logout", "current": current}}));
    }
    ws.ping().await.unwrap();
    ws.ping().await.unwrap();
    assert_eq!(ws.channels().len(), 2);
    assert!(ws.has_token());
    ws.close().await;
}

#[tokio::test]
async fn refused_private_resubscribe_goes_back_to_pending() {
    let mut srv = Server::start().await;
    let ws = WebSocket::new(options(&srv.url)).unwrap();
    ws.connect().await.unwrap();
    let mut c = srv.conn().await;
    authed(&ws, &mut c, "tok", "u1").await;
    subscribed(&ws, &mut c, &["orders"]).await;
    c.send(json!({"type": "session.revoked", "channel": "account",
                  "data": {"session_id": null, "reason": "logout_all", "current": true}}));
    ws.ping().await.unwrap();
    assert!(ws.channels().is_empty());
    authed(&ws, &mut c, "tok2", "u1").await;
    ack(
        &mut c,
        "subscribe",
        json!({"type": "error", "code": "UNAUTHENTICATED", "message": "authentication required"}),
    )
    .await;
    ws.ping().await.unwrap();
    assert!(ws.channels().is_empty());
    // Still pending: the next successful auth tries again.
    authed(&ws, &mut c, "tok3", "u1").await;
    let req = ack(
        &mut c,
        "subscribe",
        json!({"type": "subscribed", "channels": ["orders"]}),
    )
    .await;
    assert_eq!(req["channels"], json!(["orders"]));
    ws.ping().await.unwrap();
    assert_eq!(ws.channels(), vec!["orders"]);
    ws.close().await;
}

#[tokio::test]
async fn refused_subscribe_is_not_held() {
    let mut srv = Server::start().await;
    let ws = WebSocket::new(options(&srv.url)).unwrap();
    ws.connect().await.unwrap();
    let mut c = srv.conn().await;
    let ws2 = ws.clone();
    let t = tokio::spawn(async move { ws2.subscribe(&["orders"]).await });
    ack(
        &mut c,
        "subscribe",
        json!({"type": "error", "code": "UNAUTHENTICATED", "message": "authentication required"}),
    )
    .await;
    assert!(t.await.unwrap().is_err());
    assert!(ws.channels().is_empty());
    ws.close().await;
}
