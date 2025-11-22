//! Core types for risk management
//!
//! Defines violations, states, and common structures used across risk modules.

use std::fmt;

/// Risk violation types - reasons why an order or action was rejected
#[derive(Debug, Clone)]
pub enum RiskViolation {
    /// Order size exceeds maximum allowed
    MaxOrderSizeExceeded {
        symbol: String,
        requested: f64,
        max_allowed: f64,
    },

    /// Position would exceed maximum allowed
    MaxPositionExceeded {
        symbol: String,
        current: f64,
        requested_delta: f64,
        max_allowed: f64,
    },

    /// Total portfolio exposure would exceed limit
    MaxExposureExceeded {
        current_usd: f64,
        order_notional_usd: f64,
        max_allowed_usd: f64,
    },

    /// Order price too far from mid price
    PriceBandViolation {
        symbol: String,
        order_price: f64,
        mid_price: f64,
        max_deviation_bps: f64,
    },

    /// Daily loss limit reached
    DailyLossLimitReached {
        current_loss: f64,
        max_loss: f64,
    },

    /// Session loss limit reached
    SessionLossLimitReached {
        current_loss: f64,
        max_loss: f64,
    },

    /// Maximum drawdown exceeded
    MaxDrawdownExceeded {
        current_drawdown_pct: f64,
        max_drawdown_pct: f64,
    },

    /// Kill switch is active
    KillSwitchActive {
        reason: String,
    },

    /// Order rate limit exceeded
    OrderRateLimitExceeded {
        orders_per_second: u32,
        max_per_second: u32,
    },

    /// Cancel rate limit exceeded
    CancelRateLimitExceeded {
        cancels_per_second: u32,
        max_per_second: u32,
    },

    /// Order-to-trade ratio too high
    OrderToTradeRatioExceeded {
        ratio: f64,
        max_ratio: f64,
    },

    /// Margin utilization too high
    MarginUtilizationExceeded {
        utilization_pct: f64,
        max_utilization_pct: f64,
    },

    /// Too close to liquidation
    LiquidationRisk {
        symbol: String,
        distance_to_liquidation_pct: f64,
        min_distance_pct: f64,
    },

    /// Potential self-trade detected
    SelfTradeRisk {
        symbol: String,
        existing_order_id: String,
    },

    /// Notional value below minimum
    BelowMinNotional {
        symbol: String,
        notional: f64,
        min_notional: f64,
    },

    /// Market is not in trading state
    MarketNotTrading {
        symbol: String,
        status: String,
    },
}

impl fmt::Display for RiskViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RiskViolation::MaxOrderSizeExceeded { symbol, requested, max_allowed } => {
                write!(f, "Order size {} exceeds max {} for {}", requested, max_allowed, symbol)
            }
            RiskViolation::MaxPositionExceeded { symbol, current, requested_delta, max_allowed } => {
                write!(
                    f,
                    "Position would be {} (current {} + delta {}), exceeds max {} for {}",
                    current + requested_delta, current, requested_delta, max_allowed, symbol
                )
            }
            RiskViolation::MaxExposureExceeded { current_usd, order_notional_usd, max_allowed_usd } => {
                write!(
                    f,
                    "Exposure would be ${:.2} (${:.2} + ${:.2}), exceeds max ${:.2}",
                    current_usd + order_notional_usd, current_usd, order_notional_usd, max_allowed_usd
                )
            }
            RiskViolation::PriceBandViolation { symbol, order_price, mid_price, max_deviation_bps } => {
                let deviation_bps = ((order_price - mid_price) / mid_price * 10000.0).abs();
                write!(
                    f,
                    "Price {} deviates {:.1}bps from mid {} (max {}bps) for {}",
                    order_price, deviation_bps, mid_price, max_deviation_bps, symbol
                )
            }
            RiskViolation::DailyLossLimitReached { current_loss, max_loss } => {
                write!(f, "Daily loss ${:.2} reached limit ${:.2}", current_loss, max_loss)
            }
            RiskViolation::SessionLossLimitReached { current_loss, max_loss } => {
                write!(f, "Session loss ${:.2} reached limit ${:.2}", current_loss, max_loss)
            }
            RiskViolation::MaxDrawdownExceeded { current_drawdown_pct, max_drawdown_pct } => {
                write!(f, "Drawdown {:.2}% exceeds max {:.2}%", current_drawdown_pct, max_drawdown_pct)
            }
            RiskViolation::KillSwitchActive { reason } => {
                write!(f, "Kill switch active: {}", reason)
            }
            RiskViolation::OrderRateLimitExceeded { orders_per_second, max_per_second } => {
                write!(f, "Order rate {}/s exceeds max {}/s", orders_per_second, max_per_second)
            }
            RiskViolation::CancelRateLimitExceeded { cancels_per_second, max_per_second } => {
                write!(f, "Cancel rate {}/s exceeds max {}/s", cancels_per_second, max_per_second)
            }
            RiskViolation::OrderToTradeRatioExceeded { ratio, max_ratio } => {
                write!(f, "Order-to-trade ratio {:.1} exceeds max {:.1}", ratio, max_ratio)
            }
            RiskViolation::MarginUtilizationExceeded { utilization_pct, max_utilization_pct } => {
                write!(f, "Margin utilization {:.1}% exceeds max {:.1}%", utilization_pct, max_utilization_pct)
            }
            RiskViolation::LiquidationRisk { symbol, distance_to_liquidation_pct, min_distance_pct } => {
                write!(
                    f,
                    "Distance to liquidation {:.2}% below minimum {:.2}% for {}",
                    distance_to_liquidation_pct, min_distance_pct, symbol
                )
            }
            RiskViolation::SelfTradeRisk { symbol, existing_order_id } => {
                write!(f, "Potential self-trade with order {} on {}", existing_order_id, symbol)
            }
            RiskViolation::BelowMinNotional { symbol, notional, min_notional } => {
                write!(f, "Notional ${:.2} below minimum ${:.2} for {}", notional, min_notional, symbol)
            }
            RiskViolation::MarketNotTrading { symbol, status } => {
                write!(f, "Market {} is not trading (status: {})", symbol, status)
            }
        }
    }
}

impl std::error::Error for RiskViolation {}

/// Current risk state snapshot
#[derive(Debug, Clone)]
pub struct RiskState {
    /// Whether the kill switch is active
    pub kill_switch_active: bool,

    /// Reason for kill switch activation
    pub kill_switch_reason: Option<String>,

    /// Current drawdown percentage from peak
    pub current_drawdown: f64,

    /// Session PnL in USD
    pub session_pnl: f64,

    /// Orders submitted in current window
    pub orders_this_second: u32,

    /// Current margin utilization percentage
    pub margin_utilization: f64,
}

impl Default for RiskState {
    fn default() -> Self {
        Self {
            kill_switch_active: false,
            kill_switch_reason: None,
            current_drawdown: 0.0,
            session_pnl: 0.0,
            orders_this_second: 0,
            margin_utilization: 0.0,
        }
    }
}

/// Trigger type for kill switch activation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KillSwitchTrigger {
    /// Manual trigger by operator
    Manual,
    /// Max loss threshold breached
    MaxLoss,
    /// Max drawdown threshold breached
    MaxDrawdown,
    /// Connectivity issues detected
    Connectivity,
    /// Unusual market conditions
    MarketConditions,
    /// Margin/liquidation risk
    MarginRisk,
}

impl fmt::Display for KillSwitchTrigger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KillSwitchTrigger::Manual => write!(f, "Manual"),
            KillSwitchTrigger::MaxLoss => write!(f, "MaxLoss"),
            KillSwitchTrigger::MaxDrawdown => write!(f, "MaxDrawdown"),
            KillSwitchTrigger::Connectivity => write!(f, "Connectivity"),
            KillSwitchTrigger::MarketConditions => write!(f, "MarketConditions"),
            KillSwitchTrigger::MarginRisk => write!(f, "MarginRisk"),
        }
    }
}

/// Severity level for risk events
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RiskSeverity {
    /// Informational - no action needed
    Info,
    /// Warning - should be monitored
    Warning,
    /// Critical - action may be needed
    Critical,
    /// Emergency - immediate action required
    Emergency,
}

/// Risk event for monitoring/alerting
#[derive(Debug, Clone)]
pub struct RiskEvent {
    /// Timestamp in milliseconds
    pub timestamp_ms: u64,

    /// Severity level
    pub severity: RiskSeverity,

    /// Event description
    pub message: String,

    /// Associated symbol (if any)
    pub symbol: Option<String>,

    /// Associated violation (if any)
    pub violation: Option<RiskViolation>,
}

impl RiskEvent {
    pub fn info(message: impl Into<String>) -> Self {
        Self {
            timestamp_ms: now_ms(),
            severity: RiskSeverity::Info,
            message: message.into(),
            symbol: None,
            violation: None,
        }
    }

    pub fn warning(message: impl Into<String>) -> Self {
        Self {
            timestamp_ms: now_ms(),
            severity: RiskSeverity::Warning,
            message: message.into(),
            symbol: None,
            violation: None,
        }
    }

    pub fn critical(message: impl Into<String>) -> Self {
        Self {
            timestamp_ms: now_ms(),
            severity: RiskSeverity::Critical,
            message: message.into(),
            symbol: None,
            violation: None,
        }
    }

    pub fn emergency(message: impl Into<String>) -> Self {
        Self {
            timestamp_ms: now_ms(),
            severity: RiskSeverity::Emergency,
            message: message.into(),
            symbol: None,
            violation: None,
        }
    }

    pub fn with_symbol(mut self, symbol: impl Into<String>) -> Self {
        self.symbol = Some(symbol.into());
        self
    }

    pub fn with_violation(mut self, violation: RiskViolation) -> Self {
        self.violation = Some(violation);
        self
    }
}

/// Get current timestamp in milliseconds
fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_risk_violation_display() {
        let violation = RiskViolation::MaxOrderSizeExceeded {
            symbol: "BTCUSD".to_string(),
            requested: 10.0,
            max_allowed: 5.0,
        };
        assert!(violation.to_string().contains("10"));
        assert!(violation.to_string().contains("5"));
    }

    #[test]
    fn test_risk_event_builder() {
        let event = RiskEvent::warning("Test warning")
            .with_symbol("BTCUSD");

        assert_eq!(event.severity, RiskSeverity::Warning);
        assert_eq!(event.symbol, Some("BTCUSD".to_string()));
    }
}
