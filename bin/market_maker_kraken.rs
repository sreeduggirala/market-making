//! Example: Market Making on Kraken
//!
//! Demonstrates a complete market-making setup with:
//! - Exchange adapters (Kraken + MEXC for hedging)
//! - Order Management System
//! - Inventory management with cross-exchange hedging
//! - Market making strategy
//!
//! Usage:
//!   cargo run --example market_maker_kraken

use adapters::kraken::KrakenSpotAdapter;
use adapters::mexc::MexcSpotAdapter;
use adapters::traits::SpotWs;
use anyhow::Result;
use inventory::{HedgingPolicy, InventoryManager, PositionLimits};
use oms::{Exchange, OrderManager};
use std::sync::Arc;
use strategies::market_maker::{MarketMaker, MarketMakerConfig};
use strategies::Strategy;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables
    dotenvy::dotenv().ok();

    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .init();

    info!("Starting Market Making System");

    // =================================================================
    // 1. Initialize Exchange Adapters
    // =================================================================

    info!("Initializing exchange adapters...");

    let kraken_key = std::env::var("KRAKEN_API_KEY")
        .expect("KRAKEN_API_KEY not set");
    let kraken_secret = std::env::var("KRAKEN_API_SECRET")
        .expect("KRAKEN_API_SECRET not set");

    let kraken = Arc::new(KrakenSpotAdapter::new(kraken_key, kraken_secret));

    let mexc_key = std::env::var("MEXC_API_KEY")
        .expect("MEXC_API_KEY not set");
    let mexc_secret = std::env::var("MEXC_API_SECRET")
        .expect("MEXC_API_SECRET not set");

    let mexc = Arc::new(MexcSpotAdapter::new(mexc_key, mexc_secret));

    info!("Adapters initialized");

    // =================================================================
    // 2. Initialize Order Management System
    // =================================================================

    info!("Initializing OMS...");

    let oms = Arc::new(OrderManager::new());

    // Register Kraken (primary exchange for market making)
    let kraken_user_stream = kraken.subscribe_user().await?;
    oms.register_exchange(Exchange::Kraken, kraken.clone(), kraken_user_stream)
        .await;

    // Register MEXC (for hedging)
    let mexc_user_stream = mexc.subscribe_user().await?;
    oms.register_exchange(Exchange::Mexc, mexc.clone(), mexc_user_stream)
        .await;

    info!("OMS initialized with 2 exchanges");

    // =================================================================
    // 3. Initialize Inventory Manager
    // =================================================================

    info!("Initializing inventory manager...");

    let hedging_policy = HedgingPolicy::CrossExchange {
        primary: Exchange::Kraken, // Make markets here
        hedge: Exchange::Mexc,      // Hedge here
        threshold: 1000.0,          // Hedge when position > $1000
    };

    let position_limits = PositionLimits {
        max_total_exposure_usd: Some(10_000.0),
        max_per_symbol: std::collections::HashMap::new(),
        default_max_position: 5_000.0,
    };

    let inventory = Arc::new(InventoryManager::new(
        oms.clone(),
        hedging_policy,
        position_limits,
    ));

    // Start inventory monitoring
    inventory.start().await;

    info!("Inventory manager started (cross-exchange hedging enabled)");

    // =================================================================
    // 4. Initialize Market Making Strategy
    // =================================================================

    info!("Initializing market maker...");

    let config = MarketMakerConfig {
        exchange: Exchange::Kraken,
        symbol: "BTCUSD".to_string(),
        spread_bps: 10.0,              // 10 basis points (0.1%)
        quote_size: 0.01,              // 0.01 BTC per side
        min_order_size: 0.001,
        max_order_size: 0.1,
        refresh_interval_ms: 1000,     // Update quotes every 1s
        inventory_skew_factor: 0.5,    // Moderate inventory skewing
        max_position: 0.1,             // Max 0.1 BTC position
    };

    let mut market_maker = MarketMaker::new(config, oms.clone(), inventory.clone());

    // Initialize (cancel existing orders, etc.)
    market_maker.initialize().await?;

    info!("Market maker initialized");

    // =================================================================
    // 5. Run the Strategy
    // =================================================================

    info!("Starting market making...");
    info!("Making markets on {} for {}",
        Exchange::Kraken,
        "BTCUSD"
    );
    info!("Hedging on {} when position > $1000", Exchange::Mexc);

    // Set up graceful shutdown
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel(1);

    // Handle Ctrl+C
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        info!("Shutdown signal received");
        let _ = shutdown_tx.send(());
    });

    // Run strategy in a task
    let strategy_handle = {
        let mm = Arc::new(market_maker);
        let mut rx = shutdown_rx.resubscribe();

        tokio::spawn(async move {
            tokio::select! {
                result = mm.run() => {
                    if let Err(e) = result {
                        error!("Strategy error: {}", e);
                    }
                }
                _ = rx.recv() => {
                    info!("Strategy shutting down...");
                }
            }
        })
    };

    // Monitor stats periodically
    let stats_handle = {
        let oms = oms.clone();
        let inventory = inventory.clone();
        let mut rx = shutdown_rx.resubscribe();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Print OMS stats
                        let order_stats = oms.get_order_stats();
                        info!(
                            "Orders: {} total, {} open, {} filled",
                            order_stats.total_orders,
                            order_stats.new_orders + order_stats.partially_filled,
                            order_stats.filled_orders
                        );

                        // Print inventory stats
                        let hedge_stats = inventory.get_stats().await;
                        info!(
                            "Hedges: {} executed, ${:.2} volume",
                            hedge_stats.total_hedges,
                            hedge_stats.hedge_volume_usd
                        );

                        // Print position
                        let net_pos = inventory.get_net_position("BTCUSD");
                        info!(
                            "Position: {:.4} BTC (avg entry: ${:.2})",
                            net_pos.net_qty,
                            net_pos.avg_entry_px
                        );
                    }
                    _ = rx.recv() => {
                        break;
                    }
                }
            }
        })
    };

    // Wait for shutdown
    let _ = shutdown_rx.recv().await;

    info!("Shutting down...");

    // Cleanup
    strategy_handle.abort();
    stats_handle.abort();

    // Cancel all orders
    info!("Canceling all orders...");
    let _ = oms.cancel_all_orders(Exchange::Kraken, None).await;
    let _ = oms.cancel_all_orders(Exchange::Mexc, None).await;

    // Shutdown adapters
    kraken.shutdown().await;
    mexc.shutdown().await;

    info!("Shutdown complete");

    Ok(())
}
