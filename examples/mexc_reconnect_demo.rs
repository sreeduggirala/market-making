use adapters::mexc::MexcSpotWs;
use adapters::traits::SpotWs;
use anyhow::Result;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    info!("Starting MEXC WebSocket auto-reconnection demo");

    // Create MEXC spot WebSocket adapter
    // Note: Replace with actual API credentials if needed for authenticated endpoints
    let ws = MexcSpotWs::new(
        "your_api_key".to_string(),
        "your_api_secret".to_string(),
    );

    // Subscribe to trades for BTC/USDT
    info!("Subscribing to BTCUSDT trades...");
    let mut trade_rx = ws.subscribe_trades(&["BTCUSDT"]).await?;

    // Spawn a task to print trade events
    let trade_task = tokio::spawn(async move {
        let mut trade_count = 0;
        while let Some(trade) = trade_rx.recv().await {
            trade_count += 1;
            info!(
                "Trade #{}: {} @ {} (qty: {})",
                trade_count,
                trade.symbol,
                trade.px,
                trade.qty
            );
        }
    });

    // Spawn a task to monitor health and trigger disconnections
    let ws_clone = ws.clone();
    let health_task = tokio::spawn(async move {
        for i in 0..5 {
            sleep(Duration::from_secs(5)).await;

            // Check health
            match ws_clone.health().await {
                Ok(health) => {
                    info!(
                        "Health check #{}: status={:?}, reconnects={}, latency={:?}ms",
                        i + 1,
                        health.status,
                        health.reconnect_count,
                        health.latency_ms
                    );
                }
                Err(e) => {
                    error!("Health check failed: {}", e);
                }
            }

            // Force disconnect after 10 seconds to demonstrate auto-reconnection
            if i == 1 {
                info!("Forcing WebSocket disconnect to demonstrate auto-reconnection...");
                if let Err(e) = ws_clone.force_disconnect().await {
                    error!("Failed to force disconnect: {}", e);
                } else {
                    info!("WebSocket disconnected - should auto-reconnect immediately!");
                }
            }
        }
    });

    // Wait for both tasks (trade printing and health monitoring)
    tokio::select! {
        _ = trade_task => info!("Trade task completed"),
        _ = health_task => info!("Health task completed"),
    }

    info!("Demo completed - WebSocket should have reconnected automatically after disconnect");

    Ok(())
}
