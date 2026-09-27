use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::{Value, json};
use wiremock::{MockServer, Request, Respond, ResponseTemplate};

use crate::clock::FakeClock;
use crate::{Client, ClientOptions};

pub(crate) const KEY: &str = "ak_test_key";
pub(crate) const SECRET: &str = "test_secret";

/// A client for `server` on a fake clock with fixed jitter (0.5) and no client-side rate limit.
pub(crate) fn client_with(
    server: &MockServer,
    creds: bool,
    tweak: impl FnOnce(&mut ClientOptions),
) -> (Client, FakeClock) {
    let mut o = ClientOptions {
        base_url: Some(server.uri()),
        allow_insecure: true,
        disable_rate_limit: true,
        ..Default::default()
    };
    if creds {
        o.api_key = Some(KEY.into());
        o.api_secret = Some(SECRET.into());
    }
    tweak(&mut o);
    let clock = FakeClock::default();
    let c = Client::build(o, Arc::new(clock.clone()), Arc::new(|| 0.5)).unwrap();
    (c, clock)
}

pub(crate) fn client(server: &MockServer) -> (Client, FakeClock) {
    client_with(server, true, |_| {})
}

pub(crate) fn data(v: Value) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(json!({ "data": v }))
}

pub(crate) fn api_error(status: u16, code: &str, retryable: bool) -> ResponseTemplate {
    ResponseTemplate::new(status).set_body_json(
        json!({"error": {"code": code, "message": code.to_lowercase(), "retryable": retryable}}),
    )
}

/// Replies with each response in turn; the last one repeats.
pub(crate) struct Sequence {
    replies: Vec<ResponseTemplate>,
    n: Arc<AtomicUsize>,
}

impl Sequence {
    pub(crate) fn new(replies: Vec<ResponseTemplate>) -> Sequence {
        Sequence {
            replies,
            n: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl Respond for Sequence {
    fn respond(&self, _: &Request) -> ResponseTemplate {
        let i = self.n.fetch_add(1, Ordering::SeqCst);
        self.replies[i.min(self.replies.len() - 1)].clone()
    }
}

pub(crate) fn order(id: &str, status: &str) -> Value {
    json!({
        "id": id, "client_order_id": "cid", "symbol": "BTC/USDT", "side": "buy", "type": "limit",
        "status": status, "price": "60000.00", "quantity": "0.001", "filled_quantity": "0",
        "remaining_quantity": "0.001", "filled_quote_quantity": "0", "fee_paid": "0",
        "reserved_remaining": "0", "time_in_force": "gtc",
        "created_at": "2026-09-27T10:00:00Z", "updated_at": "2026-09-27T10:00:00Z"
    })
}

/// The cexy-api-spec checkout: `$CEXY_API_SPEC`, or next to this repository. Tests that need it
/// are skipped when it is missing, unless `CEXY_REQUIRE_SPEC=1` (CI).
pub(crate) fn spec_dir() -> Option<PathBuf> {
    let dir = std::env::var("CEXY_API_SPEC")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../cexy-api-spec"));
    if dir.join("conformance").is_dir() {
        return Some(dir);
    }
    assert!(
        std::env::var("CEXY_REQUIRE_SPEC").as_deref() != Ok("1"),
        "cexy-api-spec not found at {}",
        dir.display()
    );
    None
}

pub(crate) fn load(rel: &str) -> Option<Value> {
    let dir = spec_dir()?;
    let text = std::fs::read_to_string(dir.join("conformance").join(rel)).unwrap();
    Some(serde_json::from_str(&text).unwrap())
}
