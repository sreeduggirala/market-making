//! Margin and liquidation risk monitoring
//!
//! Monitors margin utilization and distance to liquidation for perpetual/margin trading.

use crate::{MarginLimits, RiskViolation};
use dashmap::DashMap;
use parking_lot::RwLock;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// Margin monitor for tracking margin utilization and liquidation risk
#[derive(Clone)]
pub struct MarginMonitor {
    limits: MarginLimits,
    state: Arc<RwLock<MarginState>>,
    positions: Arc<DashMap<String, PositionMargin>>,
    mark_prices: Arc<DashMap<String, f64>>,
}

/// Global margin state
#[derive(Debug, Default)]
struct MarginState {
    /// Total equity across all accounts
    total_equity: f64,

    /// Total margin used
    margin_used: f64,

    /// Available margin
    margin_available: f64,

    /// Overall margin utilization percentage
    utilization_pct: f64,

    /// Number of positions with liquidation warnings
    warning_count: u32,

    /// Number of positions with critical liquidation risk
    critical_count: u32,
}

/// Per-position margin information
#[derive(Debug, Clone)]
pub struct PositionMargin {
    /// Symbol
    pub symbol: String,

    /// Position size (signed)
    pub qty: f64,

    /// Entry price
    pub entry_px: f64,

    /// Mark price
    pub mark_px: f64,

    /// Liquidation price
    pub liquidation_px: Option<f64>,

    /// Margin allocated to this position
    pub margin: f64,

    /// Leverage used
    pub leverage: u32,

    /// Unrealized PnL
    pub unrealized_pnl: f64,

    /// Distance to liquidation (percentage)
    pub liquidation_distance_pct: Option<f64>,
}

impl MarginMonitor {
    /// Creates a new margin monitor
    pub fn new(limits: MarginLimits) -> Self {
        Self {
            limits,
            state: Arc::new(RwLock::new(MarginState::default())),
            positions: Arc::new(DashMap::new()),
            mark_prices: Arc::new(DashMap::new()),
        }
    }

    /// Updates account equity and margin information
    pub fn update_account(&self, total_equity: f64, margin_used: f64, margin_available: f64) {
        let mut state = self.state.write();
        state.total_equity = total_equity;
        state.margin_used = margin_used;
        state.margin_available = margin_available;

        if total_equity > 0.0 {
            state.utilization_pct = (margin_used / total_equity) * 100.0;
        } else {
            state.utilization_pct = 0.0;
        }

        debug!(
            total_equity,
            margin_used,
            margin_available,
            utilization_pct = state.utilization_pct,
            "Account margin updated"
        );

        // Check for warnings
        if state.utilization_pct >= self.limits.margin_warning_pct {
            warn!(
                utilization = state.utilization_pct,
                warning_threshold = self.limits.margin_warning_pct,
                "Margin utilization warning"
            );
        }
    }

    /// Updates position margin information
    pub fn update_position(&self, position: PositionMargin) {
        let symbol = position.symbol.clone();

        // Calculate liquidation distance if we have liquidation price
        let mut position = position;
        if let Some(liq_px) = position.liquidation_px {
            if position.mark_px > 0.0 {
                let distance = if position.qty > 0.0 {
                    // Long position: liquidation is below mark price
                    ((position.mark_px - liq_px) / position.mark_px) * 100.0
                } else {
                    // Short position: liquidation is above mark price
                    ((liq_px - position.mark_px) / position.mark_px) * 100.0
                };
                position.liquidation_distance_pct = Some(distance.max(0.0));
            }
        }

        debug!(
            symbol = %position.symbol,
            qty = position.qty,
            mark_px = position.mark_px,
            liquidation_px = ?position.liquidation_px,
            distance_pct = ?position.liquidation_distance_pct,
            "Position margin updated"
        );

        self.positions.insert(symbol, position);
        self.recalculate_warnings();
    }

    /// Updates mark price for a symbol
    pub fn update_mark_price(&self, symbol: &str, price: f64) {
        self.mark_prices.insert(symbol.to_string(), price);

        // Update position if we have one
        if let Some(mut entry) = self.positions.get_mut(symbol) {
            entry.mark_px = price;

            // Recalculate liquidation distance
            if let Some(liq_px) = entry.liquidation_px {
                let distance = if entry.qty > 0.0 {
                    ((price - liq_px) / price) * 100.0
                } else {
                    ((liq_px - price) / price) * 100.0
                };
                entry.liquidation_distance_pct = Some(distance.max(0.0));
            }

            // Recalculate unrealized PnL
            let notional_diff = entry.qty * (price - entry.entry_px);
            entry.unrealized_pnl = notional_diff;
        }

        self.recalculate_warnings();
    }

    /// Removes a position (when closed)
    pub fn remove_position(&self, symbol: &str) {
        self.positions.remove(symbol);
        self.recalculate_warnings();
        info!(symbol, "Position removed from margin monitor");
    }

    /// Recalculates warning and critical counts
    fn recalculate_warnings(&self) {
        let mut warnings = 0;
        let mut criticals = 0;

        for entry in self.positions.iter() {
            if let Some(distance) = entry.liquidation_distance_pct {
                if distance < self.limits.min_liquidation_distance_pct {
                    criticals += 1;
                    error!(
                        symbol = %entry.symbol,
                        distance,
                        min = self.limits.min_liquidation_distance_pct,
                        "CRITICAL: Position near liquidation"
                    );
                } else if distance < self.limits.liquidation_warning_pct {
                    warnings += 1;
                    warn!(
                        symbol = %entry.symbol,
                        distance,
                        warning_threshold = self.limits.liquidation_warning_pct,
                        "Position approaching liquidation"
                    );
                }
            }
        }

        let mut state = self.state.write();
        state.warning_count = warnings;
        state.critical_count = criticals;
    }

    /// Gets current margin utilization percentage
    pub fn get_utilization(&self) -> f64 {
        self.state.read().utilization_pct
    }

    /// Checks if margin utilization is within limits
    pub fn check_utilization(&self) -> Result<(), RiskViolation> {
        let utilization = self.get_utilization();

        if utilization >= self.limits.max_margin_utilization_pct {
            return Err(RiskViolation::MarginUtilizationExceeded {
                utilization_pct: utilization,
                max_utilization_pct: self.limits.max_margin_utilization_pct,
            });
        }

        Ok(())
    }

    /// Checks liquidation risk for a symbol
    pub fn check_liquidation_risk(&self, symbol: &str) -> Result<(), RiskViolation> {
        if let Some(position) = self.positions.get(symbol) {
            if let Some(distance) = position.liquidation_distance_pct {
                if distance < self.limits.min_liquidation_distance_pct {
                    return Err(RiskViolation::LiquidationRisk {
                        symbol: symbol.to_string(),
                        distance_to_liquidation_pct: distance,
                        min_distance_pct: self.limits.min_liquidation_distance_pct,
                    });
                }
            }
        }

        Ok(())
    }

    /// Checks all positions for liquidation risk
    pub fn check_all_liquidation_risk(&self) -> Vec<RiskViolation> {
        let mut violations = Vec::new();

        for entry in self.positions.iter() {
            if let Some(distance) = entry.liquidation_distance_pct {
                if distance < self.limits.min_liquidation_distance_pct {
                    violations.push(RiskViolation::LiquidationRisk {
                        symbol: entry.symbol.clone(),
                        distance_to_liquidation_pct: distance,
                        min_distance_pct: self.limits.min_liquidation_distance_pct,
                    });
                }
            }
        }

        violations
    }

    /// Gets margin status snapshot
    pub fn get_status(&self) -> MarginStatus {
        let state = self.state.read();
        MarginStatus {
            total_equity: state.total_equity,
            margin_used: state.margin_used,
            margin_available: state.margin_available,
            utilization_pct: state.utilization_pct,
            warning_count: state.warning_count,
            critical_count: state.critical_count,
            position_count: self.positions.len() as u32,
        }
    }

    /// Gets all position margin information
    pub fn get_all_positions(&self) -> Vec<PositionMargin> {
        self.positions.iter().map(|e| e.value().clone()).collect()
    }

    /// Gets position margin for a specific symbol
    pub fn get_position(&self, symbol: &str) -> Option<PositionMargin> {
        self.positions.get(symbol).map(|e| e.value().clone())
    }

    /// Checks if leverage exceeds maximum allowed
    pub fn check_leverage(&self, symbol: &str, leverage: u32) -> Result<(), RiskViolation> {
        if leverage > self.limits.max_leverage {
            warn!(
                symbol,
                leverage,
                max = self.limits.max_leverage,
                "Leverage exceeds limit"
            );
            // Return a generic violation since we don't have a specific leverage violation
            return Err(RiskViolation::MarginUtilizationExceeded {
                utilization_pct: leverage as f64 * 10.0, // Approximate
                max_utilization_pct: self.limits.max_leverage as f64 * 10.0,
            });
        }

        Ok(())
    }

    /// Estimates margin required for a new position
    pub fn estimate_margin_required(&self, symbol: &str, qty: f64, leverage: u32) -> f64 {
        let price = self.mark_prices.get(symbol).map(|p| *p).unwrap_or(0.0);
        let notional = qty.abs() * price;

        if leverage > 0 {
            notional / leverage as f64
        } else {
            notional
        }
    }

    /// Checks if there's enough margin for a new position
    pub fn check_margin_available(&self, required_margin: f64) -> bool {
        let state = self.state.read();
        state.margin_available >= required_margin
    }
}

/// Margin status snapshot
#[derive(Debug, Clone)]
pub struct MarginStatus {
    pub total_equity: f64,
    pub margin_used: f64,
    pub margin_available: f64,
    pub utilization_pct: f64,
    pub warning_count: u32,
    pub critical_count: u32,
    pub position_count: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_margin_monitor_creation() {
        let limits = MarginLimits::default();
        let monitor = MarginMonitor::new(limits);

        assert_eq!(monitor.get_utilization(), 0.0);
    }

    #[test]
    fn test_account_update() {
        let limits = MarginLimits::default();
        let monitor = MarginMonitor::new(limits);

        monitor.update_account(10_000.0, 5_000.0, 5_000.0);

        assert_eq!(monitor.get_utilization(), 50.0);
    }

    #[test]
    fn test_position_margin_tracking() {
        let limits = MarginLimits::default();
        let monitor = MarginMonitor::new(limits);

        let position = PositionMargin {
            symbol: "BTCUSD".to_string(),
            qty: 1.0,
            entry_px: 50_000.0,
            mark_px: 51_000.0,
            liquidation_px: Some(40_000.0),
            margin: 5_000.0,
            leverage: 10,
            unrealized_pnl: 1_000.0,
            liquidation_distance_pct: None,
        };

        monitor.update_position(position);

        let retrieved = monitor.get_position("BTCUSD").unwrap();
        assert_eq!(retrieved.qty, 1.0);

        // Check liquidation distance was calculated
        assert!(retrieved.liquidation_distance_pct.is_some());
        let distance = retrieved.liquidation_distance_pct.unwrap();
        // Distance should be (51000 - 40000) / 51000 * 100 ≈ 21.5%
        assert!(distance > 20.0 && distance < 23.0);
    }

    #[test]
    fn test_liquidation_risk_check() {
        let limits = MarginLimits {
            min_liquidation_distance_pct: 10.0,
            ..Default::default()
        };
        let monitor = MarginMonitor::new(limits);

        // Position with liquidation distance of ~5%
        let position = PositionMargin {
            symbol: "BTCUSD".to_string(),
            qty: 1.0,
            entry_px: 50_000.0,
            mark_px: 50_000.0,
            liquidation_px: Some(47_500.0), // 5% away
            margin: 5_000.0,
            leverage: 10,
            unrealized_pnl: 0.0,
            liquidation_distance_pct: None,
        };

        monitor.update_position(position);

        // Should fail - too close to liquidation
        let result = monitor.check_liquidation_risk("BTCUSD");
        assert!(matches!(result, Err(RiskViolation::LiquidationRisk { .. })));
    }

    #[test]
    fn test_margin_utilization_check() {
        let limits = MarginLimits {
            max_margin_utilization_pct: 80.0,
            ..Default::default()
        };
        let monitor = MarginMonitor::new(limits);

        // 90% utilization
        monitor.update_account(10_000.0, 9_000.0, 1_000.0);

        let result = monitor.check_utilization();
        assert!(matches!(result, Err(RiskViolation::MarginUtilizationExceeded { .. })));
    }

    #[test]
    fn test_margin_status() {
        let limits = MarginLimits::default();
        let monitor = MarginMonitor::new(limits);

        monitor.update_account(10_000.0, 5_000.0, 5_000.0);

        let status = monitor.get_status();
        assert_eq!(status.total_equity, 10_000.0);
        assert_eq!(status.margin_used, 5_000.0);
        assert_eq!(status.utilization_pct, 50.0);
    }
}
