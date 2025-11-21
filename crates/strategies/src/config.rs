//! Configuration loading for trading strategies
//!
//! Supports loading from TOML files with environment variable overrides.
//!
//! # Example
//!
//! ```ignore
//! use strategies::config::load_avellaneda_config;
//!
//! let config = load_avellaneda_config("config/avellaneda_stoikov.toml")?;
//! ```

use crate::avellaneda_stoikov::AvellanedaStoikovConfig;
use anyhow::{Context, Result};
use oms::Exchange;
use serde::Deserialize;
use std::path::Path;
use tracing::info;

/// Raw TOML configuration structure for Avellaneda-Stoikov strategy
#[derive(Debug, Deserialize)]
pub struct AvellanedaStoikovToml {
    pub trading: TradingConfig,
    pub strategy: StrategyConfig,
    pub spreads: SpreadConfig,
    pub risk: RiskConfig,
    pub timing: TimingConfig,
}

#[derive(Debug, Deserialize)]
pub struct TradingConfig {
    pub exchange: String,
    pub symbol: String,
    pub order_size: f64,
    pub min_order_size: f64,
}

#[derive(Debug, Deserialize)]
pub struct StrategyConfig {
    pub risk_aversion: f64,
    pub time_horizon_secs: f64,
    pub volatility_window: usize,
    pub order_arrival_rate: f64,
}

#[derive(Debug, Deserialize)]
pub struct SpreadConfig {
    pub min_spread_bps: f64,
    pub max_spread_bps: f64,
}

#[derive(Debug, Deserialize)]
pub struct RiskConfig {
    pub max_inventory: f64,
}

#[derive(Debug, Deserialize)]
pub struct TimingConfig {
    pub quote_refresh_interval_ms: u64,
    pub stats_interval_secs: u64,
}

impl AvellanedaStoikovToml {
    /// Loads configuration from a TOML file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;

        let config: Self = toml::from_str(&contents)
            .with_context(|| format!("Failed to parse config file: {}", path.display()))?;

        Ok(config)
    }

    /// Converts to the strategy configuration format
    pub fn into_strategy_config(self) -> Result<AvellanedaStoikovConfig> {
        let exchange = match self.trading.exchange.to_lowercase().as_str() {
            "kraken" => Exchange::Kraken,
            "mexc" => Exchange::Mexc,
            other => anyhow::bail!("Unknown exchange: {}", other),
        };

        Ok(AvellanedaStoikovConfig {
            exchange,
            symbol: self.trading.symbol,
            risk_aversion: self.strategy.risk_aversion,
            time_horizon_secs: self.strategy.time_horizon_secs,
            order_size: self.trading.order_size,
            min_spread_bps: self.spreads.min_spread_bps,
            max_spread_bps: self.spreads.max_spread_bps,
            volatility_window: self.strategy.volatility_window,
            quote_refresh_interval_ms: self.timing.quote_refresh_interval_ms,
            order_arrival_rate: self.strategy.order_arrival_rate,
            max_inventory: self.risk.max_inventory,
            min_order_size: self.trading.min_order_size,
        })
    }
}

/// Loads Avellaneda-Stoikov configuration from a file with environment overrides
///
/// # Environment Variables
///
/// The following environment variables can override config values:
/// - `STRATEGY_EXCHANGE` - Exchange name ("kraken" or "mexc")
/// - `STRATEGY_SYMBOL` - Trading symbol
/// - `STRATEGY_ORDER_SIZE` - Order size
/// - `STRATEGY_RISK_AVERSION` - Risk aversion parameter
/// - `STRATEGY_MAX_INVENTORY` - Maximum inventory
///
/// # Example
///
/// ```ignore
/// let config = load_avellaneda_config("config/avellaneda_stoikov.toml")?;
/// ```
pub fn load_avellaneda_config<P: AsRef<Path>>(path: P) -> Result<AvellanedaStoikovConfig> {
    let toml_config = AvellanedaStoikovToml::from_file(path)?;
    let mut config = toml_config.into_strategy_config()?;

    // Apply environment variable overrides
    if let Ok(exchange) = std::env::var("STRATEGY_EXCHANGE") {
        config.exchange = match exchange.to_lowercase().as_str() {
            "kraken" => Exchange::Kraken,
            "mexc" => Exchange::Mexc,
            _ => config.exchange,
        };
    }

    if let Ok(symbol) = std::env::var("STRATEGY_SYMBOL") {
        config.symbol = symbol;
    }

    if let Ok(order_size) = std::env::var("STRATEGY_ORDER_SIZE") {
        if let Ok(size) = order_size.parse() {
            config.order_size = size;
        }
    }

    if let Ok(risk_aversion) = std::env::var("STRATEGY_RISK_AVERSION") {
        if let Ok(ra) = risk_aversion.parse() {
            config.risk_aversion = ra;
        }
    }

    if let Ok(max_inventory) = std::env::var("STRATEGY_MAX_INVENTORY") {
        if let Ok(mi) = max_inventory.parse() {
            config.max_inventory = mi;
        }
    }

    info!(
        exchange = ?config.exchange,
        symbol = %config.symbol,
        order_size = config.order_size,
        risk_aversion = config.risk_aversion,
        max_inventory = config.max_inventory,
        "Configuration loaded"
    );

    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_toml_config() {
        let toml_str = r#"
[trading]
exchange = "kraken"
symbol = "XBT/USD"
order_size = 0.001
min_order_size = 0.0001

[strategy]
risk_aversion = 0.1
time_horizon_secs = 180.0
volatility_window = 100
order_arrival_rate = 1.0

[spreads]
min_spread_bps = 5.0
max_spread_bps = 100.0

[risk]
max_inventory = 0.01

[timing]
quote_refresh_interval_ms = 5000
stats_interval_secs = 10
"#;

        let config: AvellanedaStoikovToml = toml::from_str(toml_str).unwrap();
        assert_eq!(config.trading.exchange, "kraken");
        assert_eq!(config.trading.symbol, "XBT/USD");
        assert_eq!(config.strategy.risk_aversion, 0.1);

        let strategy_config = config.into_strategy_config().unwrap();
        assert!(matches!(strategy_config.exchange, Exchange::Kraken));
    }
}
