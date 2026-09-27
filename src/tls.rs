//! The one TLS configuration used by both the REST client and the WebSocket client: rustls with
//! the `ring` crypto provider and the Mozilla root store from `webpki-roots` (the same roots on
//! every platform, no OpenSSL, no system trust store, no C toolchain needed to build).

use std::sync::Arc;
#[cfg(not(test))]
use std::sync::OnceLock;

use rustls::{ClientConfig, RootCertStore};

/// Extra trust anchors for the tests' local TLS servers. Test builds only.
#[cfg(test)]
pub(crate) static TEST_ROOTS: std::sync::Mutex<Vec<rustls::pki_types::CertificateDer<'static>>> =
    std::sync::Mutex::new(Vec::new());

fn build() -> Arc<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    #[cfg(test)]
    for c in TEST_ROOTS.lock().unwrap().iter() {
        roots.add(c.clone()).expect("test root");
    }
    let config =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .expect("ring supports the default protocol versions")
            .with_root_certificates(roots)
            .with_no_client_auth();
    Arc::new(config)
}

/// The shared rustls client configuration.
pub(crate) fn client_config() -> Arc<ClientConfig> {
    #[cfg(not(test))]
    {
        static CONFIG: OnceLock<Arc<ClientConfig>> = OnceLock::new();
        CONFIG.get_or_init(build).clone()
    }
    #[cfg(test)]
    build()
}
