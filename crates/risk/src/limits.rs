//! Risk limits configuration
//!
//! Defines configurable limits for all risk checks.

use std::collections::HashMap;

/// Comprehensive risk limits configuration
#[derive(Debug, Clone)]
pub struct RiskLimits {
    /// Per-symbol order size limits (in base currency)
    pub max_order_size: HashMap<String, f64>,

    /// Default max order size for symbols not explicitly configured
    pub default_max_order_size: f64,

    /// Per-symbol position limits (in base currency)
    pub max_position: HashMap<String, f64>,

    /// Default max position for symbols not explicitly configured
    pub default_max_position: f64,

    /// Maximum total portfolio exposure in USD
    pub max_total_exposure_usd: f64,

    /// Maximum price deviation from mid in basis points
    pub max_price_deviation_bps: f64,

    /// Maximum daily loss in USD (resets at midnight UTC)
    pub max_daily_loss_usd: f64,

    /// Maximum session loss in USD (resets on strategy restart)
    pub max_session_loss_usd: f64,

    /// Maximum drawdown percentage from peak equity
    pub max_drawdown_pct: f64,

    /// Rate limiting configuration
    pub rate_limits: RateLimits,

    /// Margin/liquidation risk configuration
    pub margin_limits: MarginLimits,

    /// Minimum notional value per order in USD
    pub min_notional_usd: f64,

    /// Enable self-trade prevention
    pub self_trade_prevention: bool,
}

impl Default for RiskLimits {
    fn default() -> Self {
        Self {
            max_order_size: HashMap::new(),
            default_max_order_size: 1.0,
            max_position: HashMap::new(),
            default_max_position: 10.0,
            max_total_exposure_usd: 100_000.0,
            max_price_deviation_bps: 500.0, // 5%
            max_daily_loss_usd: 5_000.0,
            max_session_loss_usd: 2_500.0,
            max_drawdown_pct: 10.0,
            rate_limits: RateLimits::default(),
            margin_limits: MarginLimits::default(),
            min_notional_usd: 10.0,
            self_trade_prevention: true,
        }
    }
}

impl RiskLimits {
    /// Creates conservative limits suitable for initial deployment
    pub fn conservative() -> Self {
        Self {
            default_max_order_size: 0.1,
            default_max_position: 1.0,
            max_total_exposure_usd: 10_000.0,
            max_price_deviation_bps: 200.0, // 2%
            max_daily_loss_usd: 500.0,
            max_session_loss_usd: 250.0,
            max_drawdown_pct: 5.0,
            rate_limits: RateLimits::conservative(),
            margin_limits: MarginLimits::conservative(),
            ..Default::default()
        }
    }

    /// Creates aggressive limits for experienced operators
    pub fn aggressive() -> Self {
        Self {
            default_max_order_size: 10.0,
            default_max_position: 100.0,
            max_total_exposure_usd: 1_000_000.0,
            max_price_deviation_bps: 1000.0, // 10%
            max_daily_loss_usd: 50_000.0,
            max_session_loss_usd: 25_000.0,
            max_drawdown_pct: 20.0,
            rate_limits: RateLimits::aggressive(),
            margin_limits: MarginLimits::aggressive(),
            ..Default::default()
        }
    }

    /// Builder method: set max order size for a symbol
    pub fn with_max_order_size(mut self, symbol: impl Into<String>, size: f64) -> Self {
        self.max_order_size.insert(symbol.into(), size);
        self
    }

    /// Builder method: set max position for a symbol
    pub fn with_max_position(mut self, symbol: impl Into<String>, position: f64) -> Self {
        self.max_position.insert(symbol.into(), position);
        self
    }

    /// Builder method: set max daily loss
    pub fn with_max_daily_loss(mut self, loss: f64) -> Self {
        self.max_daily_loss_usd = loss;
        self
    }

    /// Builder method: set max session loss
    pub fn with_max_session_loss(mut self, loss: f64) -> Self {
        self.max_session_loss_usd = loss;
        self
    }

    /// Builder method: set max drawdown
    pub fn with_max_drawdown_pct(mut self, pct: f64) -> Self {
        self.max_drawdown_pct = pct;
        self
    }

    /// Builder method: set max exposure
    pub fn with_max_exposure(mut self, exposure: f64) -> Self {
        self.max_total_exposure_usd = exposure;
        self
    }

    /// Get max order size for a symbol
    pub fn get_max_order_size(&self, symbol: &str) -> f64 {
        self.max_order_size
            .get(symbol)
            .copied()
            .unwrap_or(self.default_max_order_size)
    }

    /// Get max position for a symbol
    pub fn get_max_position(&self, symbol: &str) -> f64 {
        self.max_position
            .get(symbol)
            .copied()
            .unwrap_or(self.default_max_position)
    }
}

/// Rate limiting configuration
#[derive(Debug, Clone)]
pub struct RateLimits {
    /// Maximum orders per second
    pub max_orders_per_second: u32,

    /// Maximum cancels per second
    pub max_cancels_per_second: u32,

    /// Maximum order-to-trade ratio (orders / fills)
    /// High ratio indicates excessive order churn
    pub max_order_to_trade_ratio: f64,

    /// Window size in seconds for rate calculation
    pub window_secs: u64,

    /// Burst allowance (extra capacity above steady rate)
    pub burst_multiplier: f64,
}

impl Default for RateLimits {
    fn default() -> Self {
        Self {
            max_orders_per_second: 10,
            max_cancels_per_second: 20,
            max_order_to_trade_ratio: 100.0,
            window_secs: 1,
            burst_multiplier: 2.0,
        }
    }
}

impl RateLimits {
    pub fn conservative() -> Self {
        Self {
            max_orders_per_second: 5,
            max_cancels_per_second: 10,
            max_order_to_trade_ratio: 50.0,
            ..Default::default()
        }
    }

    pub fn aggressive() -> Self {
        Self {
            max_orders_per_second: 50,
            max_cancels_per_second: 100,
            max_order_to_trade_ratio: 500.0,
            burst_multiplier: 3.0,
            ..Default::default()
        }
    }
}

/// Margin and liquidation risk configuration
#[derive(Debug, Clone)]
pub struct MarginLimits {
    /// Maximum margin utilization percentage
    pub max_margin_utilization_pct: f64,

    /// Warning threshold for margin utilization
    pub margin_warning_pct: f64,

    /// Minimum distance to liquidation price (percentage)
    pub min_liquidation_distance_pct: f64,

    /// Warning threshold for liquidation distance
    pub liquidation_warning_pct: f64,

    /// Maximum leverage allowed
    pub max_leverage: u32,
}

impl Default for MarginLimits {
    fn default() -> Self {
        Self {
            max_margin_utilization_pct: 80.0,
            margin_warning_pct: 60.0,
            min_liquidation_distance_pct: 10.0,
            liquidation_warning_pct: 20.0,
            max_leverage: 10,
        }
    }
}

impl MarginLimits {
    pub fn conservative() -> Self {
        Self {
            max_margin_utilization_pct: 50.0,
            margin_warning_pct: 30.0,
            min_liquidation_distance_pct: 20.0,
            liquidation_warning_pct: 30.0,
            max_leverage: 5,
        }
    }

    pub fn aggressive() -> Self {
        Self {
            max_margin_utilization_pct: 90.0,
            margin_warning_pct: 70.0,
            min_liquidation_distance_pct: 5.0,
            liquidation_warning_pct: 10.0,
            max_leverage: 20,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_limits() {
        let limits = RiskLimits::default();
        assert_eq!(limits.default_max_order_size, 1.0);
        assert_eq!(limits.get_max_order_size("BTCUSD"), 1.0);
    }

    #[test]
    fn test_custom_limits() {
        let limits = RiskLimits::default()
            .with_max_order_size("BTCUSD", 5.0)
            .with_max_position("BTCUSD", 50.0);

        assert_eq!(limits.get_max_order_size("BTCUSD"), 5.0);
        assert_eq!(limits.get_max_order_size("ETHUSD"), 1.0); // default
        assert_eq!(limits.get_max_position("BTCUSD"), 50.0);
    }

    #[test]
    fn test_conservative_limits() {
        let limits = RiskLimits::conservative();
        assert!(limits.default_max_order_size < RiskLimits::default().default_max_order_size);
        assert!(limits.max_daily_loss_usd < RiskLimits::default().max_daily_loss_usd);
    }
}
