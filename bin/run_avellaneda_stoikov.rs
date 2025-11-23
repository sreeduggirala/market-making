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

use adapters::binance_us::BinanceUsSpotAdapter;
use adapters::bybit::spot::BybitSpotAdapter;
use adapters::kalshi::spot::KalshiSpotAdapter;
use adapters::kraken::spot::KrakenSpotAdapter;
use adapters::mexc::spot::MexcSpotAdapter;
use adapters::traits::SpotWs;
use anyhow::Result;
use inventory::PositionManager;
use oms::{Exchange, OrderManager};
use std::sync::Arc;
use strategies::avellaneda_stoikov::{Adapter, AvellanedaStoikov, AvellanedaStoikovConfig, SpotAdapter};
use strategies::{load_avellaneda_config, Strategy};
use tokio::sync::broadcast;
use tracing::{error, info, warn};

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

    // Try to load from config file, fall back to defaults
    let config_path = std::env::var("CONFIG_PATH")
        .unwrap_or_else(|_| "config/avellaneda_stoikov.toml".to_string());

    let config = if std::path::Path::new(&config_path).exists() {
        info!("Loading configuration from: {}", config_path);
        load_avellaneda_config(&config_path)?
    } else {
        info!("Config file not found, using defaults");
        AvellanedaStoikovConfig {
            exchange: Exchange::Kraken,
            symbol: "XBT/USD".to_string(),
            risk_aversion: 0.1,
            time_horizon_secs: 180.0,
            order_size: 0.001,
            min_spread_bps: 5.0,
            max_spread_bps: 100.0,
            volatility_window: 100,
            quote_refresh_interval_ms: 5000,
            order_arrival_rate: 1.0,
            max_inventory: 0.01,
            min_order_size: 0.001,
        }
    };

    info!(
        exchange = ?config.exchange,
        symbol = %config.symbol,
        risk_aversion = config.risk_aversion,
        order_size = config.order_size,
        max_inventory = config.max_inventory,
        "Configuration loaded"
    );

    // Load API credentials based on exchange
    let (api_key, api_secret) = match config.exchange {
        Exchange::Kraken => (
            std::env::var("KRAKEN_API_KEY")
                .expect("KRAKEN_API_KEY environment variable not set"),
            std::env::var("KRAKEN_API_SECRET")
                .expect("KRAKEN_API_SECRET environment variable not set"),
        ),
        Exchange::Mexc => (
            std::env::var("MEXC_API_KEY")
                .expect("MEXC_API_KEY environment variable not set"),
            std::env::var("MEXC_API_SECRET")
                .expect("MEXC_API_SECRET environment variable not set"),
        ),
        Exchange::Bybit => (
            std::env::var("BYBIT_API_KEY")
                .expect("BYBIT_API_KEY environment variable not set"),
            std::env::var("BYBIT_API_SECRET")
                .expect("BYBIT_API_SECRET environment variable not set"),
        ),
        Exchange::Kalshi => (
            std::env::var("KALSHI_API_KEY_ID")
                .expect("KALSHI_API_KEY_ID environment variable not set"),
            std::env::var("KALSHI_PRIVATE_KEY")
                .expect("KALSHI_PRIVATE_KEY environment variable not set"),
        ),
        Exchange::BinanceUs => (
            std::env::var("BINANCE_US_API_KEY")
                .expect("BINANCE_US_API_KEY environment variable not set"),
            std::env::var("BINANCE_US_API_SECRET")
                .expect("BINANCE_US_API_SECRET environment variable not set"),
        ),
    };

    // =================================================================
    // 2. Initialize Exchange Adapter
    // =================================================================

    info!("📡 Connecting to {}...", config.exchange);

    // Create exchange-specific adapter and register with OMS
    // We need to handle each exchange type separately due to type constraints
    enum ExchangeAdapterHandle {
        BinanceUs(Arc<BinanceUsSpotAdapter>),
        Bybit(Arc<BybitSpotAdapter>),
        Kalshi(Arc<KalshiSpotAdapter>),
        Kraken(Arc<KrakenSpotAdapter>),
        Mexc(Arc<MexcSpotAdapter>),
    }

    let (adapter, oms, adapter_handle) = match config.exchange {
        Exchange::Kraken => {
            let kraken = Arc::new(KrakenSpotAdapter::new(api_key, api_secret));

            // Create OMS and register adapter
            let oms = Arc::new(OrderManager::new());
            let user_stream = kraken.subscribe_user().await?;
            oms.register_exchange(Exchange::Kraken, kraken.clone(), user_stream).await;

            // Create unified adapter for strategy
            let adapter = Arc::new(Adapter::Spot(SpotAdapter::new(kraken.clone() as Arc<dyn SpotWs>)));

            (adapter, oms, ExchangeAdapterHandle::Kraken(kraken))
        }
        Exchange::Mexc => {
            let mexc = Arc::new(MexcSpotAdapter::new(api_key, api_secret));

            // Create OMS and register adapter
            let oms = Arc::new(OrderManager::new());
            let user_stream = mexc.subscribe_user().await?;
            oms.register_exchange(Exchange::Mexc, mexc.clone(), user_stream).await;

            // Create unified adapter for strategy
            let adapter = Arc::new(Adapter::Spot(SpotAdapter::new(mexc.clone() as Arc<dyn SpotWs>)));

            (adapter, oms, ExchangeAdapterHandle::Mexc(mexc))
        }
        Exchange::Bybit => {
            let bybit = Arc::new(BybitSpotAdapter::new(api_key, api_secret));

            // Create OMS and register adapter
            let oms = Arc::new(OrderManager::new());
            let user_stream = bybit.subscribe_user().await?;
            oms.register_exchange(Exchange::Bybit, bybit.clone(), user_stream).await;

            // Create unified adapter for strategy
            let adapter = Arc::new(Adapter::Spot(SpotAdapter::new(bybit.clone() as Arc<dyn SpotWs>)));

            (adapter, oms, ExchangeAdapterHandle::Bybit(bybit))
        }
        Exchange::Kalshi => {
            // Kalshi uses RSA-PSS authentication with key ID and private key
            let kalshi = Arc::new(KalshiSpotAdapter::new(api_key, &api_secret)?);

            // Create OMS and register adapter
            let oms = Arc::new(OrderManager::new());
            let user_stream = kalshi.subscribe_user().await?;
            oms.register_exchange(Exchange::Kalshi, kalshi.clone(), user_stream).await;

            // Create unified adapter for strategy
            let adapter = Arc::new(Adapter::Spot(SpotAdapter::new(kalshi.clone() as Arc<dyn SpotWs>)));

            (adapter, oms, ExchangeAdapterHandle::Kalshi(kalshi))
        }
        Exchange::BinanceUs => {
            let binance_us = Arc::new(BinanceUsSpotAdapter::new(api_key, api_secret));

            // Create OMS and register adapter
            let oms = Arc::new(OrderManager::new());
            let user_stream = binance_us.subscribe_user().await?;
            oms.register_exchange(Exchange::BinanceUs, binance_us.clone(), user_stream).await;

            // Create unified adapter for strategy
            let adapter = Arc::new(Adapter::Spot(SpotAdapter::new(binance_us.clone() as Arc<dyn SpotWs>)));

            (adapter, oms, ExchangeAdapterHandle::BinanceUs(binance_us))
        }
    };

    info!("✅ Exchange adapter initialized");
    info!("✅ OMS initialized");

    // =================================================================
    // 4. Setup Shutdown Channel (needed for event handlers)
    // =================================================================

    let (shutdown_tx, _shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

    // Handle Ctrl+C
    let shutdown_tx_clone = shutdown_tx.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        info!("🛑 Shutdown signal received (Ctrl+C)");
        let _ = shutdown_tx_clone.send(());
    });

    // =================================================================
    // 5. Initialize Position Manager
    // =================================================================

    info!("📊 Initializing position manager...");

    let position_manager = Arc::new(PositionManager::new());

    // Subscribe to fills from OMS and update position manager
    let mut fill_rx = oms.subscribe_fills();
    let pm_fill = position_manager.clone();
    let fill_exchange = config.exchange;
    let fill_shutdown = shutdown_tx.subscribe();
    let fill_handle = tokio::spawn(async move {
        let mut shutdown_rx = fill_shutdown;
        loop {
            tokio::select! {
                result = fill_rx.recv() => {
                    match result {
                        Ok(fill) => {
                            info!(
                                symbol = %fill.symbol,
                                qty = fill.qty,
                                price = fill.price,
                                "Fill received"
                            );
                            pm_fill.update_from_fill(fill_exchange, &fill);
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            warn!("Fill receiver lagged by {} messages", n);
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("Fill handler shutting down");
                    break;
                }
            }
        }
    });

    // Subscribe to position updates from OMS and update position manager
    let mut position_rx = oms.subscribe_positions();
    let pm_pos = position_manager.clone();
    let pos_exchange = config.exchange;
    let pos_shutdown = shutdown_tx.subscribe();
    let position_handle = tokio::spawn(async move {
        let mut shutdown_rx = pos_shutdown;
        loop {
            tokio::select! {
                result = position_rx.recv() => {
                    match result {
                        Ok(position) => {
                            info!(
                                symbol = %position.symbol,
                                qty = position.qty,
                                entry_px = position.entry_px,
                                unrealized_pnl = ?position.unrealized_pnl,
                                "Position update received"
                            );
                            pm_pos.update_position(pos_exchange, position);
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            warn!("Position receiver lagged by {} messages", n);
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("Position handler shutting down");
                    break;
                }
            }
        }
    });

    info!("✅ Position manager initialized");

    // =================================================================
    // 6. Initialize Avellaneda-Stoikov Strategy
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
    // 7. Run Strategy
    // =================================================================

    let mut shutdown_rx = shutdown_tx.subscribe();

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

    // First, cancel all orders (most important for safety)
    info!("❌ Canceling all orders...");
    if let Err(e) = strategy.shutdown().await {
        error!("Error during strategy shutdown: {}", e);
    }

    // Wait for event handlers to drain (they'll receive shutdown signal)
    info!("⏳ Waiting for event handlers to complete...");
    let drain_timeout = tokio::time::Duration::from_secs(5);

    // Wait for fill handler
    let _ = tokio::time::timeout(drain_timeout, fill_handle).await;

    // Wait for position handler
    let _ = tokio::time::timeout(drain_timeout, position_handle).await;

    // Wait for strategy and stats tasks
    let _ = tokio::time::timeout(drain_timeout, strategy_handle).await;
    let _ = tokio::time::timeout(drain_timeout, stats_handle).await;

    info!("✅ Event handlers stopped");

    // Shutdown adapter (closes WebSocket connections)
    info!("🔌 Disconnecting from exchange...");
    match adapter_handle {
        ExchangeAdapterHandle::BinanceUs(binance_us) => binance_us.shutdown().await,
        ExchangeAdapterHandle::Bybit(bybit) => bybit.shutdown().await,
        ExchangeAdapterHandle::Kalshi(kalshi) => kalshi.shutdown().await,
        ExchangeAdapterHandle::Kraken(kraken) => kraken.shutdown().await,
        ExchangeAdapterHandle::Mexc(mexc) => mexc.shutdown().await,
    }

    // Give WebSocket a moment to close cleanly
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    info!("✅ Shutdown complete");
    info!("👋 Goodbye!");

    Ok(())
}
