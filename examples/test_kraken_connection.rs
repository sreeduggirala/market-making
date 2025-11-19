//! Simple test to verify Kraken WebSocket connection and trade reception
//!
//! Run with: cargo run --example test_kraken_connection

use adapters::kraken::KrakenSpotWs;
use adapters::traits::SpotWs;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("debug")
        .init();

    println!("\n🔌 Testing Kraken WebSocket connection...\n");

    let adapter = KrakenSpotWs::new(String::new(), String::new());
    let mut trades = adapter.subscribe_trades(&["BTC/USD"]).await?;

    println!("✅ Connected! Waiting for trades...\n");

    let mut count = 0;
    while let Some(event) = trades.recv().await {
        count += 1;
        println!("Trade #{}: {:?}", count, event);

        if count >= 5 {
            break;
        }
    }

    println!("\n✅ Successfully received {} trades!", count);

    Ok(())
}
