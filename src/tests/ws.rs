//! WebSocket client against a local tokio-tungstenite server.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use wiremock::matchers::path;
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::{
    BookEvent, Client, ClientOptions, ResyncReason, WebSocket, WsEvent, WsEvents, WsOptions,
};

/// One accepted connection: frames the client sent, and a way to send frames to it.
pub(super) struct Conn {
    pub(super) from_client: mpsc::UnboundedReceiver<Value>,
    to_client: mpsc::UnboundedSender<Message>,
    user_agent: Option<String>,
}

impl Conn {
    pub(super) async fn recv(&mut self) -> Value {
        tokio::time::timeout(Duration::from_secs(5), self.from_client.recv())
            .await
            .expect("a client frame")
            .expect("open")
    }
    pub(super) fn send(&self, v: Value) {
        let _ = self.to_client.send(Message::text(v.to_string()));
    }
    pub(super) fn close(&self) {
        let _ = self.to_client.send(Message::Close(None));
    }
}

/// A local WebSocket server. It sends the welcome frame on connect and answers pings that
/// carry an id; every other client frame is handed to the test.
pub(super) struct Server {
    pub(super) url: String,
    conns: mpsc::UnboundedReceiver<Conn>,
}

impl Server {
    pub(super) async fn start() -> Server {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "ws://127.0.0.1:{}/api/v1/ws",
            listener.local_addr().unwrap().port()
        );
        let (tx, conns) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                let tx = tx.clone();
                tokio::spawn(async move {
                    let ua = Arc::new(Mutex::new(None));
                    let ua2 = ua.clone();
                    #[allow(clippy::result_large_err)] // the signature is tungstenite's
                    let cb = move |req: &Request, resp: Response| {
                        *ua2.lock().unwrap() = req
                            .headers()
                            .get("user-agent")
                            .and_then(|v| v.to_str().ok())
                            .map(str::to_string);
                        Ok(resp)
                    };
                    let Ok(ws) = tokio_tungstenite::accept_hdr_async(tcp, cb).await else {
                        return;
                    };
                    let (mut sink, mut read) = ws.split();
                    let (to_client, mut out) = mpsc::unbounded_channel::<Message>();
                    let (fc_tx, from_client) = mpsc::unbounded_channel();
                    let welcome = json!({"type": "welcome", "protocol_version": 1, "heartbeat_interval_seconds": 30,
                                         "max_subscriptions": 100, "connection_id": "c1"});
                    let _ = sink.send(Message::text(welcome.to_string())).await;
                    let user_agent = ua.lock().unwrap().clone();
                    let _ = tx.send(Conn {
                        from_client,
                        to_client: to_client.clone(),
                        user_agent,
                    });
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
                                    if v["op"] == "ping" {
                                        if let Some(id) = v.get("id") {
                                            let _ = to_client.send(Message::text(json!({"type": "pong", "id": id}).to_string()));
                                        }
                                        continue;
                                    }
                                    let _ = fc_tx.send(v);
                                }
                                Some(Ok(_)) => {}
                                _ => break,
                            },
                        }
                    }
                });
            }
        });
        Server { url, conns }
    }

    pub(super) async fn conn(&mut self) -> Conn {
        tokio::time::timeout(Duration::from_secs(5), self.conns.recv())
            .await
            .expect("a connection")
            .unwrap()
    }
}

pub(super) fn options(url: &str) -> WsOptions {
    WsOptions {
        url: Some(url.to_string()),
        allow_insecure: true,
        reconnect_base_delay: Duration::from_millis(10),
        reconnect_max_delay: Duration::from_millis(20),
        ack_timeout: Duration::from_millis(500),
        ..Default::default()
    }
}

async fn next_event(rx: &mut WsEvents, pred: impl Fn(&WsEvent) -> bool) -> WsEvent {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let ev = rx.recv().await.expect("events open");
            if pred(&ev) {
                return ev;
            }
        }
    })
    .await
    .expect("the expected event")
}

/// Acknowledges the client's next request of `op` with `reply` (the id is filled in).
async fn ack(conn: &mut Conn, op: &str, mut reply: Value) -> Value {
    let req = conn.recv().await;
    assert_eq!(req["op"], op, "{req}");
    reply["id"] = req["id"].clone();
    conn.send(reply);
    req
}

#[tokio::test]
async fn connect_subscribe_auth_ping() {
    let mut srv = Server::start().await;
    let ws = WebSocket::new(options(&srv.url)).unwrap();
    let mut ev = ws.events().unwrap();
    let w = ws.connect().await.unwrap();
    assert_eq!(w.connection_id, "c1");
    assert!(ws.is_connected());
    let mut conn = srv.conn().await;

    let ws2 = ws.clone();
    let sub =
        tokio::spawn(async move { ws2.subscribe(&["ticker:BTC/USDT", "ticker:BTC/USDT"]).await });
    let req = ack(
        &mut conn,
        "subscribe",
        json!({"type": "subscribed", "channels": ["ticker:BTC/USDT"]}),
    )
    .await;
    assert_eq!(req["channels"], json!(["ticker:BTC/USDT"]), "deduplicated");
    let r = sub.await.unwrap().unwrap();
    assert_eq!(r.added, vec!["ticker:BTC/USDT"]);
    next_event(&mut ev, |e| matches!(e, WsEvent::Subscribed(_))).await;
    // Already held: nothing is sent.
    assert_eq!(
        ws.subscribe(&["ticker:BTC/USDT"])
            .await
            .unwrap()
            .already_subscribed,
        vec!["ticker:BTC/USDT"]
    );

    let ws2 = ws.clone();
    let auth = tokio::spawn(async move { ws2.auth("tok").await });
    ack(
        &mut conn,
        "auth",
        json!({"type": "authenticated", "user_id": "u1"}),
    )
    .await;
    assert_eq!(auth.await.unwrap().unwrap().user_id.as_deref(), Some("u1"));

    assert!(ws.ping().await.unwrap() < Duration::from_secs(5));

    conn.send(json!({"type": "ticker.update", "channel": "ticker:BTC/USDT", "sequence": 1, "data": {"last": "1"}}));
    conn.send(json!({"type": "some.future.event", "channel": "x"}));
    match next_event(&mut ev, |e| matches!(e, WsEvent::Event(_))).await {
        WsEvent::Event(f) => assert_eq!(f.r#type, "ticker.update"),
        _ => unreachable!(),
    }
    ws.close().await;
    match next_event(&mut ev, |e| matches!(e, WsEvent::Close(_))).await {
        WsEvent::Close(c) => assert!(!c.will_reconnect && c.code == 1000),
        _ => unreachable!(),
    }
    assert!(!ws.is_connected());
}

#[tokio::test]
async fn server_error_with_id_fails_the_request_and_forgets_the_token() {
    let mut srv = Server::start().await;
    let ws = WebSocket::new(options(&srv.url)).unwrap();
    ws.connect().await.unwrap();
    let mut conn = srv.conn().await;
    let ws2 = ws.clone();
    let auth = tokio::spawn(async move { ws2.auth("bad").await });
    ack(
        &mut conn,
        "auth",
        json!({"type": "error", "code": "INVALID_CREDENTIALS", "message": "no"}),
    )
    .await;
    let e = auth.await.unwrap().unwrap_err();
    match e {
        crate::Error::WebSocket(w) => assert!(w.from_server && w.code == "INVALID_CREDENTIALS"),
        other => panic!("{other}"),
    }
    assert!(ws.inner.st.lock().unwrap().token.is_none());
}

#[tokio::test]
async fn local_limits() {
    let mut srv = Server::start().await;
    let mut o = options(&srv.url);
    o.max_subscriptions = 1;
    o.max_messages_per_minute = 1;
    let ws = WebSocket::new(o).unwrap();
    let mut ev = ws.events().unwrap();
    ws.connect().await.unwrap();
    let mut conn = srv.conn().await;
    let ws2 = ws.clone();
    let sub = tokio::spawn(async move { ws2.subscribe(&["a:1", "b:1"]).await });
    ack(
        &mut conn,
        "subscribe",
        json!({"type": "subscribed", "channels": ["a:1"]}),
    )
    .await;
    let r = sub.await.unwrap().unwrap();
    assert_eq!(
        (r.added, r.refused),
        (vec!["a:1".to_string()], vec!["b:1".to_string()])
    );
    next_event(&mut ev, |e| matches!(e, WsEvent::Warning(_))).await;
    // The single message of this minute is spent; pings are still sent.
    let e = ws.unsubscribe(&["a:1"]).await.unwrap_err();
    assert!(
        matches!(e, crate::Error::WebSocket(ref w) if w.code == "LOCAL_RATE_LIMIT"),
        "{e}"
    );
    assert!(ws.ping().await.is_ok());
}

#[tokio::test]
async fn reconnects_with_reauth_and_resubscribe() {
    let mut srv = Server::start().await;
    let ws = WebSocket::new(options(&srv.url)).unwrap();
    let mut ev = ws.events().unwrap();
    ws.connect().await.unwrap();
    let mut c1 = srv.conn().await;
    let ws2 = ws.clone();
    let sub = tokio::spawn(async move { ws2.subscribe(&["trades:BTC/USDT", "orders"]).await });
    ack(
        &mut c1,
        "subscribe",
        json!({"type": "subscribed", "channels": ["trades:BTC/USDT", "orders"]}),
    )
    .await;
    sub.await.unwrap().unwrap();
    let ws2 = ws.clone();
    let auth = tokio::spawn(async move { ws2.auth("tok").await });
    ack(
        &mut c1,
        "auth",
        json!({"type": "authenticated", "user_id": "u1"}),
    )
    .await;
    auth.await.unwrap().unwrap();

    c1.close();
    match next_event(&mut ev, |e| matches!(e, WsEvent::Close(_))).await {
        WsEvent::Close(c) => assert!(c.will_reconnect),
        _ => unreachable!(),
    }
    let mut c2 = srv.conn().await;
    ack(
        &mut c2,
        "auth",
        json!({"type": "authenticated", "user_id": "u1"}),
    )
    .await;
    let req = ack(
        &mut c2,
        "subscribe",
        json!({"type": "subscribed", "channels": ["trades:BTC/USDT", "orders"]}),
    )
    .await;
    assert_eq!(req["channels"], json!(["trades:BTC/USDT", "orders"]));
    next_event(&mut ev, |e| matches!(e, WsEvent::Reconnected(_))).await;
    next_event(&mut ev, |e| {
        matches!(e, WsEvent::Resync(ResyncReason::Reconnect))
    })
    .await;

    // session.revoked: private channels and the token are dropped; the socket stays open.
    c2.send(json!({"type": "session.revoked", "channel": "account", "data": {"session_id": null, "reason": "logout_all", "current": true}}));
    next_event(&mut ev, |e| matches!(e, WsEvent::AuthLost(_))).await;
    assert_eq!(ws.channels(), vec!["trades:BTC/USDT"]);
    assert!(ws.is_connected());

    // CONCURRENT_MODIFICATION without an id: resync.
    c2.send(json!({"type": "error", "code": "CONCURRENT_MODIFICATION", "message": "12 messages dropped", "id": null}));
    next_event(&mut ev, |e| {
        matches!(e, WsEvent::Resync(ResyncReason::ConcurrentModification))
    })
    .await;
    ws.close().await;
}

#[tokio::test]
async fn liveness_timeout_reconnects() {
    let mut srv = Server::start().await;
    let mut o = options(&srv.url);
    // The client pings more often than the liveness window, as with the defaults (30 s vs
    // 75 s). The test server answers only pings that carry an id, so the heartbeat pings get
    // no reply: the connection is silent, and the outgoing pings must not keep it "alive".
    o.liveness_timeout = Duration::from_millis(400);
    o.ping_interval = Duration::from_millis(100);
    let ws = WebSocket::new(o).unwrap();
    let mut ev = ws.events().unwrap();
    ws.connect().await.unwrap();
    let _c1 = srv.conn().await;
    match next_event(&mut ev, |e| matches!(e, WsEvent::Close(_))).await {
        WsEvent::Close(c) => assert_eq!((c.code, c.will_reconnect), (4000, true)),
        _ => unreachable!(),
    }
    let _c2 = srv.conn().await;
    next_event(&mut ev, |e| matches!(e, WsEvent::Reconnected(_))).await;
    ws.close().await;
}

#[tokio::test]
async fn client_websocket_sends_the_sdk_user_agent_and_keeps_a_live_order_book() {
    let mut srv = Server::start().await;
    let rest = MockServer::start().await;
    Mock::given(path("/api/v1/markets/BTC%2FUSDT/orderbook"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(300))
                .set_body_json(json!({"data": {"symbol": "BTC/USDT", "sequence": 100, "timestamp": "2026-09-27T10:00:00Z",
                                                "bids": [["100", "1"]], "asks": [["101", "1"]]}})),
        )
        .mount(&rest)
        .await;
    let client = Client::new(ClientOptions {
        base_url: Some(rest.uri()),
        allow_insecure: true,
        ..Default::default()
    })
    .unwrap();
    let mut o = options(&srv.url);
    o.user_agent = None;
    let ws = client.websocket(o).unwrap();
    let mut ev = ws.events().unwrap();
    ws.connect().await.unwrap();
    let mut conn = srv.conn().await;
    assert_eq!(conn.user_agent.as_deref(), Some(crate::USER_AGENT));

    let ws2 = ws.clone();
    let book = tokio::spawn(async move { ws2.order_book("BTC/USDT").await });
    ack(
        &mut conn,
        "subscribe",
        json!({"type": "subscribed", "channels": ["orderbook:BTC/USDT"]}),
    )
    .await;
    let update = |seq: i64, bid: &str| {
        json!({"type": "orderbook.update", "channel": "orderbook:BTC/USDT", "sequence": seq,
               "data": {"symbol": "BTC/USDT", "full": true, "bids": [[bid, "1"]], "asks": [["101", "1"]]}})
    };
    // Arrive while the snapshot is in flight: 100 is covered by it, 101 is replayed after it.
    conn.send(update(100, "99"));
    conn.send(update(101, "100.5"));
    let book = book.await.unwrap().unwrap();
    let snap = book.snapshot();
    assert!(snap.synced && !snap.stale);
    assert_eq!(
        (snap.sequence, snap.bids[0].price.as_str()),
        (Some(101), "100.5")
    );

    conn.send(update(103, "100.7")); // gap
    next_event(&mut ev, |e| {
        matches!(
            e,
            WsEvent::Book(BookEvent::Stale {
                expected: 102,
                received: 103,
                ..
            })
        )
    })
    .await;
    assert!(book.snapshot().stale);
    conn.send(update(104, "100.8"));
    next_event(&mut ev, |e| {
        matches!(e, WsEvent::Book(BookEvent::Healed { .. }))
    })
    .await;
    let snap = book.snapshot();
    assert!(!snap.stale && snap.bids[0].price.as_str() == "100.8" && snap.bids.len() == 1);
    book.close();
    let req = conn.recv().await;
    assert_eq!(
        (req["op"].as_str(), &req["channels"]),
        (Some("unsubscribe"), &json!(["orderbook:BTC/USDT"]))
    );
    ws.close().await;
}

#[test]
fn urls_must_be_secure_unless_loopback() {
    assert!(
        WebSocket::new(WsOptions {
            url: Some("ws://api.cexy.io/ws".into()),
            ..Default::default()
        })
        .is_err()
    );
    assert!(
        WebSocket::new(WsOptions {
            url: Some("ws://example.com/ws".into()),
            allow_insecure: true,
            ..Default::default()
        })
        .is_err()
    );
    assert!(WebSocket::new(WsOptions::default()).is_ok());
    for bad in [
        "wss://user:pass@api.cexy.io/api/v1/ws",
        "wss://token@api.cexy.io/api/v1/ws",
        "wss://api.cexy.io/api/v1/ws?api_key=x",
        "wss://api.cexy.io/api/v1/ws#frag",
    ] {
        assert!(
            WebSocket::new(WsOptions {
                url: Some(bad.into()),
                ..Default::default()
            })
            .is_err(),
            "{bad}"
        );
    }
    let c = Client::new(ClientOptions::default()).unwrap();
    assert_eq!(
        c.websocket(WsOptions::default()).unwrap().url(),
        "wss://api.cexy.io/api/v1/ws"
    );
}

#[tokio::test]
async fn frames_from_the_server_keep_the_connection_alive() {
    let mut srv = Server::start().await;
    let mut o = options(&srv.url);
    o.liveness_timeout = Duration::from_millis(400);
    o.ping_interval = Duration::from_millis(100);
    let ws = WebSocket::new(o).unwrap();
    let mut ev = ws.events().unwrap();
    ws.connect().await.unwrap();
    let c1 = srv.conn().await;
    // A frame every 150 ms for 1.2 s: well past the 400 ms window in total, never inside it.
    for _ in 0..8 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        c1.send(json!({"type": "pong", "id": null}));
    }
    assert!(ws.is_connected());
    while let Ok(e) = ev.try_recv() {
        assert!(!matches!(e, WsEvent::Close(_)), "unexpected close: {e:?}");
    }
    ws.close().await;
}
