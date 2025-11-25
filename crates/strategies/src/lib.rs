//! Trading Strategies Module
//!
//! Provides a framework for implementing algorithmic trading strategies.
//! Strategies have access to:
//! - Order management (via OMS)
//! - Position tracking (via Inventory)
//! - Market data (via adapters or data feeds)

pub mod metrics;

pub use metrics::StrategyMetrics;

use anyhow::Result;

/// Base trait for all trading strategies
#[async_trait::async_trait]
pub trait Strategy: Send + Sync {
    /// Returns the strategy name
    fn name(&self) -> &str;

    /// Initializes the strategy (cancel existing orders, etc.)
    async fn initialize(&mut self) -> Result<()>;

    /// Main strategy loop - runs continuously until stopped
    async fn run(&self) -> Result<()>;

    /// Shuts down the strategy gracefully
    async fn shutdown(&self) -> Result<()>;
}

/// Market data snapshot needed by strategies
#[derive(Clone, Debug)]
pub struct MarketData {
    pub symbol: String,
    pub bid_price: f64,
    pub ask_price: f64,
    pub mid_price: f64,
    pub bid_size: f64,
    pub ask_size: f64,
    pub timestamp_ms: u64,
}

impl MarketData {
    pub fn spread(&self) -> f64 {
        self.ask_price - self.bid_price
    }

    pub fn spread_bps(&self) -> f64 {
        (self.spread() / self.mid_price) * 10000.0
    }
}
