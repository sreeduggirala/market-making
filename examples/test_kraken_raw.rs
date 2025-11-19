//! Raw Kraken WebSocket test to see actual messages
//!
//! Run with: cargo run --example test_kraken_raw

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("\n🔌 Connecting to Kraken WebSocket...\n");

    let url = "wss://ws.kraken.com/v2";
    let (mut ws, _) = connect_async(url).await?;

    println!("✅ Connected to {}!", url);

    // Subscribe to BTC/USD trades
    let subscribe_msg = serde_json::json!({
        "method": "subscribe",
        "params": {
            "channel": "trade",
            "symbol": ["BTC/USD"]
        }
    });

    println!("\n📤 Sending subscription: {}\n", subscribe_msg);
    ws.send(Message::Text(subscribe_msg.to_string())).await?;

    println!("📥 Waiting for messages...\n");

    let mut count = 0;
    while let Some(msg) = ws.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                count += 1;
                println!("Message #{}: {}", count, text);

                if count >= 10 {
                    break;
                }
            }
            Ok(Message::Ping(_)) => println!("PING received"),
            Ok(Message::Pong(_)) => println!("PONG received"),
            Ok(Message::Close(frame)) => {
                println!("Connection closed: {:?}", frame);
                break;
            }
            Err(e) => {
                println!("Error: {}", e);
                break;
            }
            _ => {}
        }
    }

    Ok(())
}
