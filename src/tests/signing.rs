//! Request signing (planned): the spec's vectors, a raw HTTP server that recomputes every
//! signature from the request line and body it received, the SIGNATURE_EXPIRED and
//! KEY_NOT_SIGNABLE rules, and WebSocket auth_key challenges.

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

use super::helpers::load;
use crate::auth::{AuthRequest, Authenticator};
use crate::clock::FakeClock;
use crate::signing::{
    canonical_path, canonical_query, canonical_request, encode_query, hmac_hex, new_nonce,
};
use crate::{
    AuthChangeReason, AuthScheme, Client, ClientOptions, Error, HmacAuthenticator, SIGNING_SCHEME,
    WsEvent, WsOptions,
};

const KEY: &str = "ak_test_key";
const SECRET: &str = "test_secret_for_signing";
const T0: i64 = 1_790_000_000_000;

fn split_target(t: &str) -> (&str, &str) {
    t.split_once('?').unwrap_or((t, ""))
}

#[test]
fn signing_vectors() {
    let Some(v) = load("signing/vectors.json") else {
        return;
    };
    assert_eq!(v["scheme"], SIGNING_SCHEME);
    let (key, secret) = (v["key_id"].as_str().unwrap(), v["secret"].as_str().unwrap());
    let (ts, nonce) = (
        v["timestamp"].as_str().unwrap(),
        v["nonce"].as_str().unwrap(),
    );
    let rest = v["rest"].as_array().unwrap();
    assert!(!rest.is_empty());
    for c in rest {
        let name = c["name"].as_str().unwrap();
        let (path, query) = split_target(c["request_target"].as_str().unwrap());
        let body = c["body"].as_str().unwrap().as_bytes();
        assert_eq!(canonical_path(path), c["canonical_path"], "{name}");
        assert_eq!(canonical_query(query), c["canonical_query"], "{name}");
        let method = c["method"].as_str().unwrap();
        let canonical = canonical_request(method, path, query, ts, nonce, body);
        assert_eq!(canonical, c["canonical_request"], "{name}");
        assert_eq!(
            hmac_hex(secret, &canonical),
            c["headers"]["X-API-Signature"],
            "{name}"
        );

        // The authenticator produces exactly the vector's headers for the URL as sent.
        let ms: i64 = ts.parse().unwrap();
        let nonce_owned = nonce.to_string();
        let a = HmacAuthenticator::new(key, secret).unwrap().with_sources(
            Arc::new(move || ms),
            Some(Arc::new(move || nonce_owned.clone())),
        );
        let url = url::Url::parse(&format!(
            "https://api.cexy.io{}",
            c["request_target"].as_str().unwrap()
        ))
        .unwrap();
        let mut req = AuthRequest {
            method,
            url: &url,
            body: (!body.is_empty()).then_some(body),
            headers: vec![],
        };
        a.authenticate(&mut req).unwrap();
        let got: BTreeMap<String, String> = req.headers.into_iter().collect();
        for (h, want) in c["headers"].as_object().unwrap() {
            assert_eq!(got.get(h).map(String::as_str), want.as_str(), "{name} {h}");
        }
        assert!(!got.contains_key("X-API-Secret"), "{name}");
    }
    let ws = &v["ws"];
    let a = HmacAuthenticator::new(key, secret).unwrap();
    let (k, sig) = a
        .sign_websocket_challenge(
            ws["welcome"]["connection_id"].as_str().unwrap(),
            ws["welcome"]["challenge"].as_str().unwrap(),
        )
        .unwrap();
    assert_eq!(k, ws["auth_key"]["key_id"]);
    assert_eq!(sig, ws["auth_key"]["signature"]);
    let n = &v["negative"];
    let cr = n["canonical_request"].as_str().unwrap();
    assert_eq!(
        hmac_hex(n["wrong_secret"].as_str().unwrap(), cr),
        n["signature_with_wrong_secret"]
    );
    assert_eq!(hmac_hex(secret, cr), n["signature_with_right_secret"]);
    assert_ne!(
        n["signature_with_wrong_secret"],
        n["signature_with_right_secret"]
    );
}

#[test]
fn query_encoding_and_nonce() {
    let q = encode_query(&[("b", "x y+z/é~".into()), ("a", "1".into())]);
    assert_eq!(q, "b=x%20y%2Bz%2F%C3%A9~&a=1");
    let n = new_nonce();
    assert_eq!(n.len(), 22, "{n}");
    assert!(
        n.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    );
    let many: HashSet<String> = (0..100).map(|_| new_nonce()).collect();
    assert_eq!(many.len(), 100);
}

#[test]
fn default_scheme_stays_headers() {
    let c = Client::new(ClientOptions::with_api_key(KEY, SECRET)).unwrap();
    assert_eq!(c.t.auth.as_ref().unwrap().kind(), "api-key");
    let c = Client::new(ClientOptions {
        auth: AuthScheme::Hmac,
        ..ClientOptions::with_api_key(KEY, SECRET)
    })
    .unwrap();
    assert_eq!(c.t.auth.as_ref().unwrap().kind(), "hmac");
}

#[test]
fn never_prints_the_secret() {
    let a = HmacAuthenticator::new(KEY, SECRET).unwrap();
    let c = Client::new(ClientOptions {
        auth: AuthScheme::Hmac,
        ..ClientOptions::with_api_key(KEY, SECRET)
    })
    .unwrap();
    let o = ClientOptions {
        auth: AuthScheme::Hmac,
        ..ClientOptions::with_api_key(KEY, SECRET)
    };
    let out = format!("{a} {a:?} {a:#?} {c:?} {o:?}");
    assert!(!out.contains(SECRET) && !out.contains(KEY), "{out}");
    let r = a.redact(&format!("k={KEY} s={SECRET}"));
    assert!(!r.contains(SECRET) && !r.contains(KEY), "{r}");
}

// An independent canonicaliser, written from the spec text (not the SDK's code): regex decoding
// and a table encoder, so the raw server catches canonicalisation bugs too.
fn indep_enc(s: &str) -> String {
    let re = regex::bytes::Regex::new("%([0-9A-Fa-f]{2})").unwrap();
    let raw = re.replace_all(s.as_bytes(), |c: &regex::bytes::Captures<'_>| {
        vec![u8::from_str_radix(std::str::from_utf8(&c[1]).unwrap(), 16).unwrap()]
    });
    const UNRESERVED: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~";
    raw.iter()
        .map(|b| {
            if UNRESERVED.contains(b) {
                (*b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

fn indep_path(p: &str) -> String {
    p.split('/').map(indep_enc).collect::<Vec<_>>().join("/")
}

fn indep_query(q: &str) -> String {
    let mut pairs: Vec<(String, String)> = q
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (n, v) = p.split_once('=').unwrap_or((p, ""));
            (indep_enc(n), indep_enc(v))
        })
        .collect();
    pairs.sort_by(|a, b| {
        a.0.as_bytes()
            .cmp(b.0.as_bytes())
            .then(a.1.as_bytes().cmp(b.1.as_bytes()))
    });
    pairs
        .iter()
        .map(|(n, v)| format!("{n}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn indep_canonical(method: &str, target: &str, ts: &str, nonce: &str, body: &[u8]) -> String {
    let (path, query) = split_target(target);
    let sum: String = ring::digest::digest(&ring::digest::SHA256, body)
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    [
        "CEXY-HMAC-SHA256-v1",
        method,
        &indep_path(path),
        &indep_query(query),
        ts,
        nonce,
        &sum,
    ]
    .join("\n")
}

#[test]
fn independent_canonicaliser_agrees_with_vectors() {
    let Some(v) = load("signing/vectors.json") else {
        return;
    };
    let (ts, nonce) = (
        v["timestamp"].as_str().unwrap(),
        v["nonce"].as_str().unwrap(),
    );
    for c in v["rest"].as_array().unwrap() {
        let target = c["request_target"].as_str().unwrap();
        let body = c["body"].as_str().unwrap().as_bytes();
        let got = indep_canonical(c["method"].as_str().unwrap(), target, ts, nonce, body);
        assert_eq!(got, c["canonical_request"], "{}", c["name"]);
    }
}

#[test]
fn query_rules() {
    assert_eq!(canonical_query("a=1&&b=2&"), "a=1&b=2"); // empty parts are dropped
    assert_eq!(canonical_query("?a=1"), "%3Fa=1"); // a "?" inside the query is data
}

/// What the raw server saw.
#[derive(Debug, Clone)]
struct Seen {
    target: String,
    body: Vec<u8>,
    headers: BTreeMap<String, String>,
    valid: bool,
}

type Reply = Arc<dyn Fn(&Seen, usize) -> (u16, Value) + Send + Sync>;

/// A raw HTTP/1.1 server: it reads the request line, headers and body off the socket and
/// recomputes the signature from them, independent of what the client thinks it sent.
async fn raw_server(reply: Reply) -> (String, Arc<Mutex<Vec<Seen>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let n = Arc::new(AtomicUsize::new(0));
    let seen2 = seen.clone();
    tokio::spawn(async move {
        while let Ok((mut tcp, _)) = listener.accept().await {
            let (seen, reply, n) = (seen2.clone(), reply.clone(), n.clone());
            tokio::spawn(async move {
                let mut buf = Vec::new();
                loop {
                    let head_end = buf.windows(4).position(|w| w == b"\r\n\r\n");
                    if let Some(end) = head_end {
                        let head = String::from_utf8_lossy(&buf[..end]).to_string();
                        let mut lines = head.split("\r\n");
                        let mut rl = lines.next().unwrap().split(' ');
                        let (method, target) = (
                            rl.next().unwrap().to_string(),
                            rl.next().unwrap().to_string(),
                        );
                        let headers: BTreeMap<String, String> = lines
                            .filter_map(|l| l.split_once(':'))
                            .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
                            .collect();
                        let len: usize = headers
                            .get("content-length")
                            .and_then(|v| v.parse().ok())
                            .unwrap_or(0);
                        while buf.len() < end + 4 + len {
                            let mut chunk = [0u8; 4096];
                            let k = tcp.read(&mut chunk).await.unwrap();
                            if k == 0 {
                                return;
                            }
                            buf.extend_from_slice(&chunk[..k]);
                        }
                        let body = buf[end + 4..end + 4 + len].to_vec();
                        buf.drain(..end + 4 + len);
                        let h = |k: &str| headers.get(k).cloned().unwrap_or_default();
                        let canonical = indep_canonical(
                            &method,
                            &target,
                            &h("x-api-timestamp"),
                            &h("x-api-nonce"),
                            &body,
                        );
                        let valid = h("x-api-key") == KEY
                            && h("x-api-signature") == hmac_hex(SECRET, &canonical);
                        let s = Seen {
                            target,
                            body,
                            headers,
                            valid,
                        };
                        seen.lock().unwrap().push(s.clone());
                        let (status, mut v) = if valid {
                            reply(&s, n.fetch_add(1, Ordering::SeqCst) + 1)
                        } else {
                            (
                                401,
                                json!({"error": {"code": "INVALID_SIGNATURE", "message": "bad signature", "retryable": false}}),
                            )
                        };
                        // A top-level "__retry_after" in a reply becomes a Retry-After header.
                        let retry_after = v
                            .as_object_mut()
                            .and_then(|o| o.remove("__retry_after"))
                            .map(|r| format!("retry-after: {}\r\n", r.as_str().unwrap_or("")))
                            .unwrap_or_default();
                        let b = v.to_string();
                        let resp = format!(
                            "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\n{retry_after}content-length: {}\r\n\r\n{b}",
                            b.len()
                        );
                        if tcp.write_all(resp.as_bytes()).await.is_err() {
                            return;
                        }
                        continue;
                    }
                    let mut chunk = [0u8; 4096];
                    match tcp.read(&mut chunk).await {
                        Ok(0) | Err(_) => return,
                        Ok(k) => buf.extend_from_slice(&chunk[..k]),
                    }
                }
            });
        }
    });
    (base, seen)
}

fn signing_client(base: &str) -> (Client, Arc<HmacAuthenticator>) {
    let a = Arc::new(
        HmacAuthenticator::new(KEY, SECRET)
            .unwrap()
            .with_sources(Arc::new(|| T0), None),
    );
    let o = ClientOptions {
        base_url: Some(base.to_string()),
        allow_insecure: true,
        disable_rate_limit: true,
        authenticator: Some(a.clone()),
        ..Default::default()
    };
    let c = Client::build(o, Arc::new(FakeClock::default()), Arc::new(|| 0.5)).unwrap();
    (c, a)
}

fn api_err(code: &str, details: Value) -> (u16, Value) {
    (
        401,
        json!({"error": {"code": code, "message": "x", "retryable": code == "SIGNATURE_EXPIRED", "details": details}}),
    )
}

#[tokio::test]
async fn signed_requests_match_the_wire() {
    let (base, seen) = raw_server(Arc::new(|s: &Seen, _| {
        if s.target.starts_with("/api/v1/trading/orders/by-client-id/") {
            (
                404,
                json!({"error": {"code": "ORDER_NOT_FOUND", "message": "x", "retryable": false}}),
            )
        } else if s.target.starts_with("/api/v1/trading/orders/history") {
            (200, json!({"data": [], "next_cursor": null}))
        } else {
            (200, json!({"data": []}))
        }
    }))
    .await;
    let (c, _) = signing_client(&base);
    c.account().balances().await.unwrap();
    let _ = c.trading().order_by_client_id("a/b+c d~é").await;
    let history = crate::OrderHistoryParams {
        symbol: Some("BTC/USDT".into()),
        cursor: Some("x y+z=&".into()),
        ..Default::default()
    };
    c.trading().order_history(Some(&history)).await.unwrap();
    let mut order =
        crate::PlaceOrderRequest::new("BTC/USDT", crate::OrderSide::Buy, crate::OrderType::Limit);
    order.price = Some(crate::Amount::new("100.5").unwrap());
    order.quantity = Some(crate::Amount::new("0.1").unwrap());
    let _ = c.trading().place_order(&order).await;

    let seen = seen.lock().unwrap().clone();
    assert!(seen.len() >= 4, "{seen:?}");
    for s in &seen {
        assert!(
            s.valid,
            "{}: signature does not match the raw request",
            s.target
        );
        assert!(!s.headers.contains_key("x-api-secret"), "{}", s.target);
    }
    assert_eq!(
        seen[1].target,
        "/api/v1/trading/orders/by-client-id/a%2Fb%2Bc%20d~%C3%A9"
    );
    assert!(
        seen[2].target.contains("symbol=BTC%2FUSDT"),
        "{}",
        seen[2].target
    );
    assert!(
        seen[2].target.contains("cursor=x%20y%2Bz%3D%26"),
        "{}",
        seen[2].target
    );
    assert!(seen.last().unwrap().body.starts_with(b"{"));
}

#[tokio::test]
async fn retries_are_signed_with_a_fresh_nonce() {
    let (base, seen) = raw_server(Arc::new(|_: &Seen, n| {
        if n == 1 {
            (503, json!({"error": {"code": "SERVICE_UNAVAILABLE", "message": "busy", "retryable": true}}))
        } else {
            (200, json!({"data": []}))
        }
    }))
    .await;
    let (c, _) = signing_client(&base);
    c.account().balances().await.unwrap();
    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2);
    assert!(seen.iter().all(|s| s.valid));
    assert_ne!(
        seen[0].headers["x-api-nonce"],
        seen[1].headers["x-api-nonce"]
    );
}

#[tokio::test]
async fn signature_expired_resends_once_with_the_server_clock() {
    let server_ms = T0 + 10 * 60 * 1000;
    let (base, seen) = raw_server(Arc::new(move |s: &Seen, _| {
        let ts: i64 = s.headers["x-api-timestamp"].parse().unwrap();
        if ts < server_ms - 30_000 {
            api_err("SIGNATURE_EXPIRED", json!({"server_time_ms": server_ms}))
        } else {
            (200, json!({"data": []}))
        }
    }))
    .await;
    let (c, a) = signing_client(&base);
    c.account().balances().await.unwrap();
    assert_eq!(a.clock_offset_ms(), 10 * 60 * 1000);
    c.account().balances().await.unwrap(); // signed with the corrected clock straight away
    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 3);
    assert_eq!(seen[1].headers["x-api-timestamp"], server_ms.to_string());
    assert_ne!(
        seen[0].headers["x-api-nonce"],
        seen[1].headers["x-api-nonce"]
    );
}

#[tokio::test]
async fn nonce_store_warming_waits_retry_after_and_keeps_the_offset() {
    let (base, seen) = raw_server(Arc::new(|_: &Seen, n| {
        if n == 1 {
            (
                503,
                json!({"__retry_after": "2", "error": {"code": "SERVICE_UNAVAILABLE", "message": "x",
                       "retryable": true, "details": {"reason": "nonce_store_warming"}}}),
            )
        } else {
            (200, json!({"data": []}))
        }
    }))
    .await;
    let a = Arc::new(
        HmacAuthenticator::new(KEY, SECRET)
            .unwrap()
            .with_sources(Arc::new(|| T0), None),
    );
    let clock = FakeClock::default();
    let o = ClientOptions {
        base_url: Some(base),
        allow_insecure: true,
        disable_rate_limit: true,
        authenticator: Some(a.clone()),
        ..Default::default()
    };
    let c = Client::build(o, Arc::new(clock.clone()), Arc::new(|| 0.5)).unwrap();
    c.account().balances().await.unwrap();
    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2);
    assert_ne!(
        seen[0].headers["x-api-nonce"],
        seen[1].headers["x-api-nonce"]
    );
    let sleeps = clock.sleeps();
    assert_eq!(sleeps.len(), 1, "{sleeps:?}");
    assert!(
        sleeps[0] >= Duration::from_secs(2) && sleeps[0] <= Duration::from_secs(3),
        "{sleeps:?}"
    );
    assert_eq!(a.clock_offset_ms(), 0, "warming must not touch the clock");
}

#[tokio::test]
async fn signature_expired_twice_is_returned() {
    let (base, seen) = raw_server(Arc::new(|_: &Seen, _| {
        api_err(
            "SIGNATURE_EXPIRED",
            json!({"server_time_ms": T0 + 5 * 60 * 1000}),
        )
    }))
    .await;
    let (c, _) = signing_client(&base);
    let e = c.account().balances().await.unwrap_err();
    assert_eq!(e.api().unwrap().code.as_str(), "SIGNATURE_EXPIRED");
    assert_eq!(
        seen.lock().unwrap().len(),
        2,
        "one resend, outside the retry budget"
    );
}

#[tokio::test]
async fn a_clock_beyond_an_hour_is_a_clock_error() {
    let (base, seen) = raw_server(Arc::new(|_: &Seen, _| {
        api_err(
            "SIGNATURE_EXPIRED",
            json!({"server_time_ms": T0 + 2 * 60 * 60 * 1000}),
        )
    }))
    .await;
    let (c, a) = signing_client(&base);
    let e = c.account().balances().await.unwrap_err();
    let api = e.api().unwrap();
    assert!(api.message.contains("clock") && !api.retryable, "{e}");
    assert_eq!(seen.lock().unwrap().len(), 1);
    assert_eq!(a.clock_offset_ms(), 0);
}

#[tokio::test]
async fn key_not_signable_has_no_fallback() {
    let (base, seen) = raw_server(Arc::new(|_: &Seen, _| {
        api_err("KEY_NOT_SIGNABLE", json!({}))
    }))
    .await;
    let (c, _) = signing_client(&base);
    let e = c.account().balances().await.unwrap_err();
    let api = e.api().unwrap();
    assert_eq!(api.code.as_str(), "KEY_NOT_SIGNABLE");
    assert_eq!(
        api.message,
        "create a new API key; keys issued before request signing can't sign"
    );
    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 1, "no fallback to headers auth");
    assert!(!seen[0].headers.contains_key("x-api-secret"));
}

/// A WebSocket server with challenges: every welcome and every auth_key reply carries a fresh
/// challenge, each accepted once.
struct KeyAuthWs {
    url: String,
    auth_keys: Arc<Mutex<Vec<Value>>>,
    conns: Arc<Mutex<Vec<tokio::sync::mpsc::UnboundedSender<Message>>>>,
    refuse: Arc<std::sync::atomic::AtomicBool>,
    silent: Arc<std::sync::atomic::AtomicBool>,
    held: Arc<Mutex<Option<Value>>>,
}

impl KeyAuthWs {
    async fn start() -> KeyAuthWs {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "ws://127.0.0.1:{}/api/v1/ws",
            listener.local_addr().unwrap().port()
        );
        let s = KeyAuthWs {
            url,
            auth_keys: Arc::default(),
            conns: Arc::default(),
            refuse: Arc::default(),
            silent: Arc::default(),
            held: Arc::default(),
        };
        let (auth_keys, conns, refuse, silent, held) = (
            s.auth_keys.clone(),
            s.conns.clone(),
            s.refuse.clone(),
            s.silent.clone(),
            s.held.clone(),
        );
        let issued = Arc::new(AtomicUsize::new(0));
        let signed = Arc::new(Mutex::new(HashSet::<String>::new()));
        tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                let (auth_keys, conns, refuse, silent, held, issued, signed) = (
                    auth_keys.clone(),
                    conns.clone(),
                    refuse.clone(),
                    silent.clone(),
                    held.clone(),
                    issued.clone(),
                    signed.clone(),
                );
                tokio::spawn(async move {
                    let Ok(ws) = tokio_tungstenite::accept_async(tcp).await else {
                        return;
                    };
                    let (mut sink, mut read) = ws.split();
                    let (tx, mut out) = tokio::sync::mpsc::unbounded_channel::<Message>();
                    let conn_id = {
                        let mut c = conns.lock().unwrap();
                        c.push(tx.clone());
                        format!("conn-{}", c.len())
                    };
                    let next =
                        || format!("challenge-{:02}", issued.fetch_add(1, Ordering::SeqCst) + 1);
                    let mut challenge = next();
                    let welcome = json!({"type": "welcome", "protocol_version": 1, "heartbeat_interval_seconds": 30,
                                         "max_subscriptions": 100, "connection_id": conn_id, "challenge": challenge});
                    let _ = sink.send(Message::text(welcome.to_string())).await;
                    loop {
                        tokio::select! {
                            m = out.recv() => match m {
                                Some(Message::Close(f)) => { let _ = sink.send(Message::Close(f)).await; break; }
                                Some(m) => { if sink.send(m).await.is_err() { break; } }
                                None => break,
                            },
                            m = read.next() => match m {
                                Some(Ok(Message::Text(t))) => {
                                    let v: Value = serde_json::from_str(t.as_str()).unwrap();
                                    match v["op"].as_str() {
                                        Some("ping") => if let Some(id) = v.get("id") {
                                            let _ = sink.send(Message::text(json!({"type": "pong", "id": id}).to_string())).await;
                                        },
                                        Some("auth_key") => {
                                            auth_keys.lock().unwrap().push(v.clone());
                                            let want = hmac_hex(SECRET, &format!("CEXY-WS-AUTH-v1\n{conn_id}\n{challenge}"));
                                            let fresh = signed.lock().unwrap().insert(challenge.clone());
                                            let ok = v["key_id"] == KEY && v["signature"] == want.as_str() && fresh
                                                && !refuse.load(Ordering::SeqCst);
                                            challenge = next();
                                            let reply = if ok {
                                                json!({"type": "authenticated", "user_id": "u1", "auth": "api_key", "challenge": challenge, "id": v["id"]})
                                            } else {
                                                json!({"type": "error", "code": "UNAUTHENTICATED", "message": "bad key signature", "challenge": challenge, "id": v["id"]})
                                            };
                                            if silent.load(Ordering::SeqCst) {
                                                *held.lock().unwrap() = Some(reply); // sent late, see release()
                                                continue;
                                            }
                                            let _ = sink.send(Message::text(reply.to_string())).await;
                                        }
                                        _ => {}
                                    }
                                }
                                Some(Ok(_)) => {}
                                _ => break,
                            },
                        }
                    }
                });
            }
        });
        s
    }

    fn sent(&self) -> Vec<Value> {
        self.auth_keys.lock().unwrap().clone()
    }

    fn conn_count(&self) -> usize {
        self.conns.lock().unwrap().len()
    }

    fn drop_last(&self) {
        let c = self.conns.lock().unwrap().last().cloned().unwrap();
        let _ = c.send(Message::Close(None));
    }

    /// Sends the held auth_key reply late, on the latest connection.
    fn release(&self) {
        let v = self.held.lock().unwrap().take().expect("a held reply");
        self.push_last(v);
    }

    fn push_last(&self, v: Value) {
        let c = self.conns.lock().unwrap().last().cloned().unwrap();
        let _ = c.send(Message::text(v.to_string()));
    }
}

async fn eventually(what: &str, cond: impl Fn() -> bool) {
    for _ in 0..600 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("timed out waiting for {what}");
}

fn key_auth_ws(server: &KeyAuthWs, scheme: AuthScheme, ack_timeout: Duration) -> crate::WebSocket {
    let c = Client::new(ClientOptions {
        auth: scheme,
        base_url: Some("http://127.0.0.1:1".into()),
        allow_insecure: true,
        ..ClientOptions::with_api_key(KEY, SECRET)
    })
    .unwrap();
    c.websocket(WsOptions {
        url: Some(server.url.clone()),
        reconnect_base_delay: Duration::from_millis(10),
        reconnect_max_delay: Duration::from_millis(20),
        ack_timeout,
        ..Default::default()
    })
    .unwrap()
}

#[tokio::test]
async fn auth_key_needs_an_hmac_client() {
    let server = KeyAuthWs::start().await;
    let ws = key_auth_ws(&server, AuthScheme::Headers, Duration::from_secs(1));
    assert!(matches!(ws.auth_key().await, Err(Error::Config(_))));
    assert!(matches!(
        crate::WebSocket::new(WsOptions::default())
            .unwrap()
            .auth_key()
            .await,
        Err(Error::Config(_))
    ));
}

#[tokio::test]
async fn auth_key_signs_each_challenge_once() {
    let server = KeyAuthWs::start().await;
    let ws = key_auth_ws(&server, AuthScheme::Hmac, Duration::from_secs(1));
    ws.connect().await.unwrap();
    let r = ws.auth_key().await.unwrap();
    assert_eq!(r.user_id.as_deref(), Some("u1"));
    assert_eq!(r.auth.as_deref(), Some("api_key"));
    // A second auth_key signs the challenge carried by the first reply, not the welcome's.
    ws.auth_key().await.unwrap();
    // After a reconnect, only the new welcome's challenge is signed.
    server.drop_last();
    eventually("re-auth after reconnect", || {
        server.conn_count() == 2 && server.sent().len() == 3
    })
    .await;
    eventually("authenticated again", || {
        ws.user_id().as_deref() == Some("u1")
    })
    .await;
    let sigs: HashSet<String> = server
        .sent()
        .iter()
        .map(|f| f["signature"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(sigs.len(), 3, "a challenge was signed twice");
    assert!(server.sent().iter().all(|f| f.get("secret").is_none()));
}

#[tokio::test]
async fn a_refused_auth_key_stops_reauth() {
    let server = KeyAuthWs::start().await;
    let ws = key_auth_ws(&server, AuthScheme::Hmac, Duration::from_secs(1));
    ws.connect().await.unwrap();
    server.refuse.store(true, Ordering::SeqCst);
    let e = ws.auth_key().await.unwrap_err();
    assert!(
        matches!(&e, Error::WebSocket(w) if w.from_server && w.code == "UNAUTHENTICATED"),
        "{e}"
    );
    // The refusal carried the next challenge: a manual retry signs that one.
    server.refuse.store(false, Ordering::SeqCst);
    ws.auth_key().await.unwrap();
    server.refuse.store(true, Ordering::SeqCst);
    assert!(ws.auth_key().await.is_err());
    server.drop_last();
    eventually("reconnect", || {
        server.conn_count() == 2 && ws.is_connected()
    })
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        server.sent().len(),
        3,
        "a refused key must not be re-sent automatically"
    );
}

#[tokio::test]
async fn a_late_auth_key_reply_racing_a_reconnect() {
    let server = KeyAuthWs::start().await;
    let ws = key_auth_ws(&server, AuthScheme::Hmac, Duration::from_millis(100));
    ws.connect().await.unwrap();
    server.silent.store(true, Ordering::SeqCst); // the reply is held back
    assert!(ws.auth_key().await.is_err(), "expected a timeout");
    eventually("server held the reply", || {
        server.held.lock().unwrap().is_some()
    })
    .await;
    server.silent.store(false, Ordering::SeqCst);
    server.release(); // the late reply (with its next challenge) arrives ...
    server.drop_last(); // ... as the connection drops
    eventually("re-auth on the new connection", || {
        server.conn_count() == 2 && server.sent().len() == 2
    })
    .await;
    // The server accepts only the new welcome's challenge: signing the late reply's fails.
    eventually("authenticated", || ws.user_id().as_deref() == Some("u1")).await;
    let s = server.sent();
    assert_ne!(s[0]["signature"], s[1]["signature"]);
}

#[tokio::test]
async fn a_challenge_is_consumed_when_signed() {
    let server = KeyAuthWs::start().await;
    let ws = key_auth_ws(&server, AuthScheme::Hmac, Duration::from_millis(100));
    ws.connect().await.unwrap();
    server.silent.store(true, Ordering::SeqCst);
    let e = ws.auth_key().await.unwrap_err();
    assert!(
        matches!(&e, Error::WebSocket(w) if w.code == "TIMEOUT"),
        "{e}"
    );
    let e = ws.auth_key().await.unwrap_err();
    assert!(
        matches!(&e, Error::WebSocket(w) if w.code == "NO_CHALLENGE"),
        "{e}"
    );
    eventually("server saw the frame", || server.sent().len() == 1).await;
    assert_eq!(server.sent().len(), 1);
}

#[tokio::test]
async fn a_late_refusal_stops_automatic_key_reauth() {
    let server = KeyAuthWs::start().await;
    let ws = key_auth_ws(&server, AuthScheme::Hmac, Duration::from_millis(100));
    ws.connect().await.unwrap();
    server.silent.store(true, Ordering::SeqCst);
    assert!(ws.auth_key().await.is_err());
    eventually("server held the reply", || {
        server.held.lock().unwrap().is_some()
    })
    .await;
    let id = server.sent()[0]["id"].clone();
    server.push_last(json!({"type": "error", "code": "UNAUTHENTICATED", "message": "bad key", "challenge": "late", "id": id}));
    eventually("key auth cleared", || !ws.inner.st.lock().unwrap().key_auth).await;
    server.silent.store(false, Ordering::SeqCst);
    server.drop_last();
    eventually("reconnect", || {
        server.conn_count() == 2 && ws.is_connected()
    })
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        server.sent().len(),
        1,
        "a late refusal must stop the automatic re-auth"
    );
}

/// Blocks its first real signature until released (a KMS or HSM).
struct SlowSigner {
    inner: HmacAuthenticator,
    gate: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
    calls: AtomicUsize,
}

impl Authenticator for SlowSigner {
    fn kind(&self) -> &str {
        "slow"
    }
    fn authenticate(&self, request: &mut AuthRequest<'_>) -> crate::Result<()> {
        self.inner.authenticate(request)
    }
    fn redact(&self, text: &str) -> String {
        self.inner.redact(text)
    }
    fn sign_websocket_challenge(
        &self,
        connection_id: &str,
        challenge: &str,
    ) -> Option<(String, String)> {
        if !challenge.is_empty() && self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            let rx = self.gate.lock().unwrap().take().unwrap();
            let _ = rx.recv();
        }
        self.inner
            .sign_websocket_challenge(connection_id, challenge)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slow_signer_racing_a_reconnect_never_sends_a_stale_signature() {
    let server = KeyAuthWs::start().await;
    let (tx, rx) = std::sync::mpsc::channel();
    let slow = Arc::new(SlowSigner {
        inner: HmacAuthenticator::new(KEY, SECRET).unwrap(),
        gate: Mutex::new(Some(rx)),
        calls: AtomicUsize::new(0),
    });
    let c = Client::new(ClientOptions {
        authenticator: Some(slow.clone()),
        base_url: Some("http://127.0.0.1:1".into()),
        allow_insecure: true,
        ..Default::default()
    })
    .unwrap();
    let ws = c
        .websocket(WsOptions {
            url: Some(server.url.clone()),
            reconnect_base_delay: Duration::from_millis(10),
            reconnect_max_delay: Duration::from_millis(20),
            ..Default::default()
        })
        .unwrap();
    ws.connect().await.unwrap();
    let ws2 = ws.clone();
    let pending = tokio::spawn(async move { ws2.auth_key().await });
    eventually("signing started", || slow.calls.load(Ordering::SeqCst) == 1).await;
    server.drop_last(); // the connection changes while the signer works
    eventually("re-auth on the new connection", || {
        server.conn_count() == 2 && ws.user_id().as_deref() == Some("u1")
    })
    .await;
    tx.send(()).unwrap();
    let e = pending.await.unwrap().unwrap_err();
    assert!(
        matches!(&e, Error::WebSocket(w) if w.code == "STALE_CHALLENGE"),
        "{e}"
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        server.sent().len(),
        1,
        "the stale signature must never be sent"
    );
}

#[tokio::test]
async fn signature_expired_without_server_time_is_returned_as_is() {
    let (base, seen) = raw_server(Arc::new(|_: &Seen, _| {
        api_err("SIGNATURE_EXPIRED", json!({}))
    }))
    .await;
    let (c, a) = signing_client(&base);
    let e = c.account().balances().await.unwrap_err();
    let api = e.api().unwrap();
    assert_eq!(api.code.as_str(), "SIGNATURE_EXPIRED");
    assert!(!api.message.contains("clock"), "{e}");
    assert_eq!(seen.lock().unwrap().len(), 1);
    assert_eq!(a.clock_offset_ms(), 0);
}

#[tokio::test]
async fn key_revoked_signs_out_and_stops_reauth() {
    let server = KeyAuthWs::start().await;
    let ws = key_auth_ws(&server, AuthScheme::Hmac, Duration::from_secs(1));
    let mut events = ws.events().unwrap();
    ws.connect().await.unwrap();
    ws.auth_key().await.unwrap();
    server.push_last(json!({"type": "signed_out", "reason": "key_revoked"}));
    let change = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(WsEvent::AuthChanged(c)) = events.recv().await {
                return c;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(change.reason, AuthChangeReason::KeyRevoked);
    assert_eq!(change.previous_user_id.as_deref(), Some("u1"));
    server.drop_last();
    eventually("reconnect", || {
        server.conn_count() == 2 && ws.is_connected()
    })
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(server.sent().len(), 1, "auth_key re-sent after key_revoked");
}
