//! Avellaneda-Stoikov Market Maker
//!
//! Production-ready market making bot using the Avellaneda-Stoikov optimal market making model.
//!
//! Features:
//! - Real-time market data from exchange WebSocket
//! - Optimal bid/ask placement based on inventory and volatility
//! - Dynamic spread adjustment
//! - Position tracking with limits
//! - Graceful shutdown handling
//!
//! Usage:
//!   cargo run --bin run_avellaneda_stoikov
//!
//! Environment variables required:
//!   KRAKEN_API_KEY     - Kraken API key
//!   KRAKEN_API_SECRET  - Kraken API secret

use adapters::kraken::spot::KrakenSpotAdapter;
use adapters::traits::SpotWs;
use anyhow::Result;
use inventory::PositionManager;
use oms::{Exchange, OrderManager};
use std::sync::Arc;
use strategies::avellaneda_stoikov::{Adapter, AvellanedaStoikov, AvellanedaStoikovConfig, SpotAdapter};
use strategies::Strategy;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables from .env file if present
    dotenvy::dotenv().ok();

    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .with_thread_ids(false)
        .init();

    info!("🚀 Starting Avellaneda-Stoikov Market Maker");

    // =================================================================
    // 1. Load Configuration
    // =================================================================

    let kraken_key = std::env::var("KRAKEN_API_KEY")
        .expect("KRAKEN_API_KEY environment variable not set");
    let kraken_secret = std::env::var("KRAKEN_API_SECRET")
        .expect("KRAKEN_API_SECRET environment variable not set");

    // Strategy configuration
    let config = AvellanedaStoikovConfig {
        exchange: Exchange::Kraken,
        symbol: "XBT/USD".to_string(),     // Kraken Spot BTC/USD
        risk_aversion: 0.1,                // Moderate risk aversion
        time_horizon_secs: 180.0,          // 3 minute horizon
        order_size: 0.001,                 // 0.001 BTC per order (~$100 at $100k)
        min_spread_bps: 5.0,               // Minimum 5 bps (0.05%)
        max_spread_bps: 100.0,             // Maximum 100 bps (1%)
        volatility_window: 100,            // 100 samples for volatility
        quote_refresh_interval_ms: 5000,   // Refresh every 5 seconds
        order_arrival_rate: 1.0,           // Expect ~1 fill per second
        max_inventory: 0.01,               // Max 0.01 BTC position
        min_order_size: 0.001,             // Exchange minimum
    };

    info!(
        exchange = ?config.exchange,
        symbol = %config.symbol,
        risk_aversion = config.risk_aversion,
        "Configuration loaded"
    );

    // =================================================================
    // 2. Initialize Exchange Adapter
    // =================================================================

    info!("📡 Connecting to Kraken Spot...");

    let kraken_spot = Arc::new(KrakenSpotAdapter::new(kraken_key, kraken_secret));

    // Wrap in unified adapter interface
    let adapter = Arc::new(Adapter::Spot(SpotAdapter::new(
        kraken_spot.clone() as Arc<dyn SpotWs>
    )));

    info!("✅ Exchange adapter initialized");

    // =================================================================
    // 3. Initialize Order Management System
    // =================================================================

    info!("⚙️  Initializing OMS...");

    let oms = Arc::new(OrderManager::new());

    // Register Kraken Spot
    let user_stream = kraken_spot.subscribe_user().await?;
    oms.register_exchange(Exchange::Kraken, kraken_spot.clone(), user_stream)
        .await;

    info!("✅ OMS initialized");

    // =================================================================
    // 4. Initialize Position Manager
    // =================================================================

    info!("📊 Initializing position manager...");

    let position_manager = Arc::new(PositionManager::new());

    // Listen to fills from OMS and update position manager
    let mut fill_rx = oms.subscribe_fills();
    let pm = position_manager.clone();
    let config_exchange = config.exchange;
    tokio::spawn(async move {
        while let Ok(fill) = fill_rx.recv().await {
            pm.update_from_fill(config_exchange, &fill);
        }
    });

    info!("✅ Position manager initialized");

    // =================================================================
    // 5. Initialize Avellaneda-Stoikov Strategy
    // =================================================================

    info!("💹 Initializing Avellaneda-Stoikov strategy...");

    let mut strategy = AvellanedaStoikov::new(
        config.clone(),
        oms.clone(),
        position_manager.clone(),
        adapter.clone(),
    );

    // Initialize (cancel existing orders, etc.)
    strategy.initialize().await?;

    info!("✅ Strategy initialized");

    // =================================================================
    // 6. Setup Graceful Shutdown
    // =================================================================

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

    // Handle Ctrl+C
    let shutdown_tx_clone = shutdown_tx.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        info!("🛑 Shutdown signal received (Ctrl+C)");
        let _ = shutdown_tx_clone.send(());
    });

    // =================================================================
    // 7. Run Strategy
    // =================================================================

    info!("🎯 Starting market making...");
    info!("📈 Trading {} on {}", config.symbol, "Kraken Spot");
    info!("⚠️  Press Ctrl+C to stop");

    // Run strategy in a task
    let strategy = Arc::new(strategy);
    let strategy_clone = strategy.clone();
    let mut shutdown_rx_clone = shutdown_rx.resubscribe();

    let strategy_handle = tokio::spawn(async move {
        tokio::select! {
            result = strategy_clone.run() => {
                if let Err(e) = result {
                    error!("Strategy error: {}", e);
                }
            }
            _ = shutdown_rx_clone.recv() => {
                info!("Strategy received shutdown signal");
            }
        }
    });

    // Monitor stats periodically
    let stats_handle = {
        let oms = oms.clone();
        let position_manager = position_manager.clone();
        let config = config.clone();
        let mut shutdown_rx_clone = shutdown_rx.resubscribe();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Print OMS stats
                        let order_stats = oms.get_order_stats();
                        info!(
                            "📊 Orders: {} total | {} open | {} filled | {} canceled",
                            order_stats.total_orders,
                            order_stats.new_orders + order_stats.partially_filled,
                            order_stats.filled_orders,
                            order_stats.canceled_orders
                        );

                        // Print current position
                        if let Some(position) = position_manager.get_position(config.exchange, &config.symbol) {
                            info!(
                                "💼 Position: {} BTC | Entry: ${:.2} | Mark: ${:.2} | PnL: ${:.2}",
                                position.qty,
                                position.entry_px,
                                position.mark_px.unwrap_or(0.0),
                                position.unrealized_pnl.unwrap_or(0.0)
                            );
                        } else {
                            info!("💼 Position: FLAT (no position)");
                        }
                    }
                    _ = shutdown_rx_clone.recv() => {
                        break;
                    }
                }
            }
        })
    };

    // Wait for shutdown signal
    let _ = shutdown_rx.recv().await;

    info!("🛑 Shutting down...");

    // =================================================================
    // 8. Cleanup
    // =================================================================

    // Stop tasks
    strategy_handle.abort();
    stats_handle.abort();

    // Shutdown strategy (cancels all orders)
    info!("❌ Canceling all orders...");
    if let Err(e) = strategy.shutdown().await {
        error!("Error during strategy shutdown: {}", e);
    }

    // Shutdown adapter
    info!("🔌 Disconnecting from exchange...");
    kraken_spot.shutdown().await;

    info!("✅ Shutdown complete");
    info!("👋 Goodbye!");

    Ok(())
}
