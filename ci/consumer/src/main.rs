//! Uses cexy like a user's project does. Modes (first argument):
//!
//! - `trusted` (build with `RUSTFLAGS="--cfg cexy_test_extra_root"`): one real `time()` request
//!   through cexy's normal client to a local TLS server that only speaks HTTP/2 (ALPN h2), whose
//!   self-signed certificate cexy trusts via CEXY_TEST_EXTRA_ROOT_PEM. Fails if the client cannot
//!   do HTTP/2 over TLS (the 0.1.0-dev.1 bug), without internet. With CEXY_LIVE_TESTS=1 it also
//!   calls api.cexy.io.
//! - `expect-reject` (a normal build, without the cfg): the same request with the variable set must
//!   FAIL with a connection/TLS error, proving that a normal build ignores CEXY_TEST_EXTRA_ROOT_PEM.
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use cexy::{Client, ClientOptions};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

const TIME_BODY: &str = r#"{"data":{"epoch_ms":1790500000000,"iso":"2026-09-27T12:00:00.000Z"}}"#;

/// What the local server saw: TLS handshake errors and served request paths.
#[derive(Default)]
struct Seen {
    handshake_errors: Vec<String>,
    requests: Vec<String>,
}

async fn serve_h2(listener: TcpListener, acceptor: TlsAcceptor, seen: Arc<Mutex<Seen>>) {
    loop {
        let Ok((tcp, _)) = listener.accept().await else {
            return;
        };
        let acceptor = acceptor.clone();
        let seen = seen.clone();
        tokio::spawn(async move {
            let tls = match acceptor.accept(tcp).await {
                Ok(tls) => tls,
                Err(e) => {
                    seen.lock().unwrap().handshake_errors.push(format!("{e:?}"));
                    return;
                }
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
                seen.lock()
                    .unwrap()
                    .requests
                    .push(req.uri().path().to_string());
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
    let mode = std::env::args().nth(1).unwrap_or_default();
    if mode != "trusted" && mode != "expect-reject" {
        eprintln!("usage: cexy-consumer-check trusted|expect-reject");
        std::process::exit(2);
    }
    // Self-signed certificate for localhost. cexy trusts it through CEXY_TEST_EXTRA_ROOT_PEM only
    // when it was compiled with --cfg cexy_test_extra_root.
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
    let seen = Arc::new(Mutex::new(Seen::default()));
    tokio::spawn(serve_h2(
        listener,
        TlsAcceptor::from(Arc::new(server)),
        seen.clone(),
    ));

    // SAFETY: single-threaded setup before any client exists; nothing else reads the environment.
    unsafe { std::env::set_var("CEXY_TEST_EXTRA_ROOT_PEM", cert.cert.pem()) };
    let local = Client::new(ClientOptions {
        base_url: Some(format!("https://localhost:{port}")),
        ..ClientOptions::default()
    })?;
    if mode == "expect-reject" {
        match local.time().await {
            Ok(_) => {
                eprintln!("FAILED: a normal build trusted CEXY_TEST_EXTRA_ROOT_PEM");
                std::process::exit(1);
            }
            Err(cexy::Error::Connection(ref c)) => {
                // It must be the certificate check that failed: the server saw the client abort the
                // TLS handshake (an alert about the certificate) and served no request.
                let seen = seen.lock().unwrap();
                let cert_rejected = seen.handshake_errors.iter().any(|e| {
                    e.contains("Certificate")
                        || e.contains("UnknownCA")
                        || e.contains("certificate")
                });
                if !cert_rejected || !seen.requests.is_empty() {
                    eprintln!(
                        "FAILED: rejected ({c}), but not by the certificate check: handshake errors {:?}, requests {:?}",
                        seen.handshake_errors, seen.requests
                    );
                    std::process::exit(1);
                }
                println!(
                    "ok: a normal build rejects the self-signed server despite CEXY_TEST_EXTRA_ROOT_PEM ({})",
                    seen.handshake_errors[0]
                );
                return Ok(());
            }
            Err(e) => {
                eprintln!("FAILED: expected a TLS/connection error, got: {e}");
                std::process::exit(1);
            }
        }
    }
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
