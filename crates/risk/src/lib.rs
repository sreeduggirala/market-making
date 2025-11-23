//! Risk Management Module
//!
//! Provides comprehensive risk controls for trading strategies:
//!
//! - **Pre-trade checks**: Validate orders before submission
//! - **Real-time monitoring**: Track exposure, drawdown, and losses
//! - **Kill switch**: Emergency shutdown mechanism
//! - **Rate limits**: Strategy-level order rate control
//! - **Margin monitoring**: Track margin utilization and liquidation risk
//!
//! # Example
//!
//! ```ignore
//! use risk::{RiskManager, RiskLimits, PreTradeValidator};
//!
//! // Create risk limits
//! let limits = RiskLimits::default()
//!     .with_max_order_size("BTCUSD", 1.0)
//!     .with_max_position("BTCUSD", 10.0)
//!     .with_max_daily_loss(10_000.0);
//!
//! // Create risk manager
//! let risk_manager = RiskManager::new(limits, position_manager);
//!
//! // Validate order before submission
//! if let Err(violation) = risk_manager.check_order(&order) {
//!     warn!("Order rejected: {}", violation);
//!     return;
//! }
//! ```

pub mod types;
pub mod limits;
pub mod pre_trade;
pub mod monitor;
pub mod kill_switch;
pub mod rate_limit;
pub mod margin;
pub mod monitoring;

pub use types::*;
pub use limits::*;
pub use pre_trade::*;
pub use monitor::*;
pub use kill_switch::*;
pub use rate_limit::*;
pub use margin::*;
pub use monitoring::*;

use std::sync::Arc;
use inventory::PositionManager;

/// Central risk manager that coordinates all risk components
pub struct RiskManager {
    /// Pre-trade validation
    pub pre_trade: PreTradeValidator,

    /// Real-time risk monitoring
    pub monitor: RealTimeMonitor,

    /// Kill switch for emergency shutdown
    pub kill_switch: KillSwitch,

    /// Strategy-level rate limiter
    pub rate_limiter: StrategyRateLimiter,

    /// Margin and liquidation monitoring
    pub margin_monitor: MarginMonitor,
}

impl RiskManager {
    /// Creates a new risk manager with the given limits
    pub fn new(limits: RiskLimits, position_manager: Arc<PositionManager>) -> Self {
        let kill_switch = KillSwitch::new();

        Self {
            pre_trade: PreTradeValidator::new(
                limits.clone(),
                position_manager.clone(),
                kill_switch.clone(),
            ),
            monitor: RealTimeMonitor::new(
                limits.clone(),
                position_manager.clone(),
                kill_switch.clone(),
            ),
            kill_switch,
            rate_limiter: StrategyRateLimiter::new(limits.rate_limits.clone()),
            margin_monitor: MarginMonitor::new(limits.margin_limits.clone()),
        }
    }

    /// Checks if an order is allowed (combines all pre-trade checks)
    pub async fn check_order(&self, order: &adapters::traits::NewOrder, mid_price: f64) -> std::result::Result<(), RiskViolation> {
        // Check kill switch first
        if self.kill_switch.is_triggered() {
            return Err(RiskViolation::KillSwitchActive {
                reason: self.kill_switch.get_reason().unwrap_or_default(),
            });
        }

        // Check rate limits
        self.rate_limiter.check_order()?;

        // Run pre-trade validation
        self.pre_trade.validate(order, mid_price).await
    }

    /// Records an order submission (for rate limiting)
    pub fn record_order(&self) {
        self.rate_limiter.record_order();
    }

    /// Records an order cancellation (for rate limiting)
    pub fn record_cancel(&self) {
        self.rate_limiter.record_cancel();
    }

    /// Updates the risk monitor with new market data
    pub fn update_market_price(&self, symbol: &str, price: f64) {
        self.monitor.update_price(symbol, price);
        self.margin_monitor.update_mark_price(symbol, price);
    }

    /// Triggers the kill switch
    pub fn trigger_kill_switch(&self, reason: &str) {
        self.kill_switch.trigger(reason);
    }

    /// Resets the kill switch (requires explicit action)
    pub fn reset_kill_switch(&self) {
        self.kill_switch.reset();
    }

    /// Returns current risk state snapshot
    pub fn get_risk_state(&self) -> RiskState {
        RiskState {
            kill_switch_active: self.kill_switch.is_triggered(),
            kill_switch_reason: self.kill_switch.get_reason(),
            current_drawdown: self.monitor.get_current_drawdown(),
            session_pnl: self.monitor.get_session_pnl(),
            orders_this_second: self.rate_limiter.orders_in_window(),
            margin_utilization: self.margin_monitor.get_utilization(),
        }
    }
}

/// Error types for risk operations
#[derive(Debug, thiserror::Error)]
pub enum RiskError {
    #[error("Risk violation: {0}")]
    Violation(#[from] RiskViolation),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, RiskError>;
