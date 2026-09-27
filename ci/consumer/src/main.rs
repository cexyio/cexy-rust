//! Builds a client like a user's project. With CEXY_LIVE_TESTS=1 it also calls the public
//! time endpoint over the real TLS stack (ALPN negotiates h2 with api.cexy.io).
use cexy::{Client, ClientOptions};

#[tokio::main]
async fn main() -> cexy::Result<()> {
    let client = Client::new(ClientOptions::default())?;
    if std::env::var("CEXY_LIVE_TESTS").as_deref() == Ok("1") {
        let now = client.time().await?;
        println!("live time call ok: {}", now.iso);
    } else {
        println!("client built (set CEXY_LIVE_TESTS=1 for a live call)");
    }
    Ok(())
}
