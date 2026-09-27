//! The SDK never follows HTTP redirects: credentials never reach another origin and an order is
//! never re-posted.

use serde_json::json;
use wiremock::matchers::{any, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::helpers::*;
use crate::{ErrorCategory, ErrorCode};

async fn redirecting(status: u16, to: &MockServer) -> MockServer {
    let api = MockServer::start().await;
    Mock::given(any())
        .respond_with(
            ResponseTemplate::new(status)
                .insert_header("Location", format!("{}/steal", to.uri()).as_str()),
        )
        .mount(&api)
        .await;
    api
}

async fn target() -> MockServer {
    let t = MockServer::start().await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"data": []})))
        .mount(&t)
        .await;
    t
}

#[tokio::test]
async fn every_3xx_is_an_error_and_nothing_reaches_the_target() {
    for status in [301, 302, 303, 307, 308] {
        let t = target().await;
        let api = redirecting(status, &t).await;
        let (c, clock) = client(&api);
        let e = c.account().balances().await.unwrap_err();
        let a = e.api().expect("an API error");
        assert_eq!(a.status, status);
        assert_eq!(a.code, ErrorCode::UnexpectedRedirect);
        assert!(e.is(ErrorCategory::UnexpectedRedirect) && !e.is_retryable());
        assert!(a.details["location"].as_str().unwrap().ends_with("/steal"));
        assert_eq!(
            api.received_requests().await.unwrap().len(),
            1,
            "{status}: not retried"
        );
        assert!(
            t.received_requests().await.unwrap().is_empty(),
            "{status}: the target received nothing"
        );
        assert!(clock.sleeps().is_empty());
    }
}

#[tokio::test]
async fn place_order_307_is_sent_once_and_not_looked_up() {
    let t = target().await;
    let api = redirecting(307, &t).await;
    let (c, _) = client(&api);
    let mut o =
        crate::PlaceOrderRequest::new("BTC/USDT", crate::OrderSide::Buy, crate::OrderType::Limit);
    o.price = Some(crate::Amount::new("1").unwrap());
    o.quantity = Some(crate::Amount::new("1").unwrap());
    let e = c.trading().place_order(&o).await.unwrap_err();
    assert_eq!(e.api().unwrap().code, ErrorCode::UnexpectedRedirect);
    let reqs = api.received_requests().await.unwrap();
    assert_eq!(
        reqs.iter()
            .map(|r| r.method.to_string())
            .collect::<Vec<_>>(),
        vec!["POST"],
        "one POST, no lookup"
    );
    assert!(t.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn credentials_go_only_to_the_base_origin() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/account/balances"))
        .respond_with(data(json!([])))
        .mount(&server)
        .await;
    let (c, _) = client(&server);
    // Simulate a bug that would build a URL for another origin: the transport refuses to attach
    // the credentials and sends nothing.
    let mut t = std::sync::Arc::try_unwrap(c.t).ok().expect("sole owner");
    t.origin = "https://elsewhere.example".into();
    let c = crate::Client {
        t: std::sync::Arc::new(t),
        opts: Default::default(),
    };
    let e = c.account().balances().await.unwrap_err();
    assert!(
        e.to_string().contains("refusing to send credentials"),
        "{e}"
    );
    assert!(server.received_requests().await.unwrap().is_empty());
}

/// A local HTTPS server (self-signed for `localhost`, trusted in test builds only) that
/// answers every request with `status` and `Location: to`.
async fn https_redirecting(status: u16, to: String) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let der = rustls::pki_types::CertificateDer::from(cert.cert.der().to_vec());
    crate::tls::TEST_ROOTS.lock().unwrap().push(der.clone());
    let key = rustls::pki_types::PrivateKeyDer::try_from(cert.key_pair.serialize_der()).unwrap();
    let server = rustls::ServerConfig::builder_with_provider(std::sync::Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(vec![der], key)
    .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(server));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((tcp, _)) = listener.accept().await {
            let acceptor = acceptor.clone();
            let to = to.clone();
            tokio::spawn(async move {
                let Ok(mut tls) = acceptor.accept(tcp).await else {
                    return;
                };
                let mut buf = vec![0u8; 8192];
                let mut got = Vec::new();
                while !got.windows(4).any(|w| w == b"\r\n\r\n") {
                    match tls.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => got.extend_from_slice(&buf[..n]),
                    }
                }
                let resp = format!(
                    "HTTP/1.1 {status} Redirect\r\nLocation: {to}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                );
                let _ = tls.write_all(resp.as_bytes()).await;
                let _ = tls.shutdown().await;
            });
        }
    });
    format!("https://localhost:{port}")
}

#[tokio::test]
async fn https_to_http_redirect_is_refused_and_the_target_gets_nothing() {
    // A real TLS server as the API: a downgrade from https to plain http is refused like any
    // other redirect, and the credentials never reach the http target.
    let t = target().await;
    for status in [302, 307] {
        let base = https_redirecting(status, format!("{}/steal", t.uri())).await;
        let c = crate::Client::new(crate::ClientOptions {
            base_url: Some(base),
            api_key: Some(KEY.into()),
            api_secret: Some(SECRET.into()),
            disable_rate_limit: true,
            max_retries: Some(0),
            ..Default::default()
        })
        .unwrap();
        let e = c.account().balances().await.unwrap_err();
        let a = e.api().unwrap_or_else(|| panic!("{status}: {e}"));
        assert_eq!(
            (a.status, a.code.clone()),
            (status, ErrorCode::UnexpectedRedirect)
        );
        assert!(t.received_requests().await.unwrap().is_empty(), "{status}");
    }
}
