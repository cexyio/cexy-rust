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
    #[cfg(feature = "__test-extra-root")]
    extra_test_roots(&mut roots);
    let config =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .expect("ring supports the default protocol versions")
            .with_root_certificates(roots)
            .with_no_client_auth();
    Arc::new(config)
}

/// `__test-extra-root` feature only (off by default, never for users): also trust the PEM
/// certificates in `CEXY_TEST_EXTRA_ROOT_PEM`. The repository's `ci/consumer` check uses it to
/// reach a local TLS test server through the real, non-test build of the client. Whoever controls
/// the environment of a process built with this feature can add a trusted root, which is why it is
/// a compile-time opt-in and not a runtime option.
#[cfg(feature = "__test-extra-root")]
fn extra_test_roots(roots: &mut RootCertStore) {
    use rustls::pki_types::{CertificateDer, pem::PemObject};
    if let Ok(pem) = std::env::var("CEXY_TEST_EXTRA_ROOT_PEM") {
        for cert in CertificateDer::pem_slice_iter(pem.as_bytes()).flatten() {
            let _ = roots.add(cert);
        }
    }
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
