//! Uses cexy like a user's project does.
//!
//! 1. Offline, always: one real `time()` request through cexy's normal client to a local TLS
//!    server that only speaks HTTP/2 (ALPN h2). This fails if the client cannot do HTTP/2 over
//!    TLS (the 0.1.0-dev.1 bug), without depending on the internet.
//! 2. Live, only with CEXY_LIVE_TESTS=1: the same call against api.cexy.io.
use std::sync::Arc;

use bytes::Bytes;
use cexy::{Client, ClientOptions};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

const TIME_BODY: &str = r#"{"data":{"epoch_ms":1790500000000,"iso":"2026-09-27T12:00:00.000Z"}}"#;

async fn serve_h2(listener: TcpListener, acceptor: TlsAcceptor) {
    loop {
        let Ok((tcp, _)) = listener.accept().await else {
            return;
        };
        let acceptor = acceptor.clone();
        tokio::spawn(async move {
            let Ok(tls) = acceptor.accept(tcp).await else {
                return;
            };
            let alpn = tls.get_ref().1.alpn_protocol().map(|p| p.to_vec());
            assert_eq!(
                alpn.as_deref(),
                Some(&b"h2"[..]),
                "client did not negotiate h2"
            );
            let Ok(mut conn) = h2::server::handshake(tls).await else {
                return;
            };
            while let Some(Ok((req, mut respond))) = conn.accept().await {
                let (status, body) = if req.uri().path() == "/api/v1/time" {
                    (200, TIME_BODY)
                } else {
                    (
                        404,
                        r#"{"error":{"code":"NOT_FOUND","message":"no route","retryable":false}}"#,
                    )
                };
                let resp = http::Response::builder()
                    .status(status)
                    .header("content-type", "application/json")
                    .body(())
                    .unwrap();
                if let Ok(mut send) = respond.send_response(resp, false) {
                    let _ = send.send_data(Bytes::from_static(body.as_bytes()), true);
                }
            }
        });
    }
}

#[tokio::main]
async fn main() -> cexy::Result<()> {
    // Self-signed certificate for localhost, trusted by cexy through CEXY_TEST_EXTRA_ROOT_PEM
    // (compiled in only with the `__test-extra-root` feature).
    let cert =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).expect("certificate");
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der()));
    let chain = vec![CertificateDer::from(cert.cert.der().to_vec())];
    let mut server = ServerConfig::builder_with_provider(Arc::new(
        tokio_rustls::rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("protocol versions")
    .with_no_client_auth()
    .with_single_cert(chain, key)
    .expect("server config");
    server.alpn_protocols = vec![b"h2".to_vec()];
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(serve_h2(listener, TlsAcceptor::from(Arc::new(server))));

    // SAFETY: single-threaded setup before any client exists; nothing else reads the environment.
    unsafe { std::env::set_var("CEXY_TEST_EXTRA_ROOT_PEM", cert.cert.pem()) };
    let local = Client::new(ClientOptions {
        base_url: Some(format!("https://localhost:{port}")),
        ..ClientOptions::default()
    })?;
    let now = local.time().await?;
    assert_eq!(now.epoch_ms, 1790500000000);
    println!("offline h2 time call ok: {}", now.iso);

    if std::env::var("CEXY_LIVE_TESTS").as_deref() == Ok("1") {
        let live = Client::new(ClientOptions::default())?;
        let now = live.time().await?;
        println!("live time call ok: {}", now.iso);
    }
    Ok(())
}
