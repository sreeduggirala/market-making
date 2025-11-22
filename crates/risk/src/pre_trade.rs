//! Pre-trade risk validation
//!
//! Validates orders before submission to ensure they comply with risk limits.

use crate::{KillSwitch, RiskLimits, RiskViolation};
use adapters::traits::{NewOrder, Side};
use inventory::PositionManager;
use oms::Exchange;
use std::sync::Arc;
use tracing::{debug, warn};

/// Pre-trade validator that checks orders against risk limits
#[derive(Clone)]
pub struct PreTradeValidator {
    limits: RiskLimits,
    position_manager: Arc<PositionManager>,
    kill_switch: KillSwitch,
    /// Exchange for position lookups (default: Kraken)
    default_exchange: Exchange,
}

impl PreTradeValidator {
    /// Creates a new pre-trade validator
    pub fn new(
        limits: RiskLimits,
        position_manager: Arc<PositionManager>,
        kill_switch: KillSwitch,
    ) -> Self {
        Self {
            limits,
            position_manager,
            kill_switch,
            default_exchange: Exchange::Kraken,
        }
    }

    /// Sets the default exchange for position lookups
    pub fn with_exchange(mut self, exchange: Exchange) -> Self {
        self.default_exchange = exchange;
        self
    }

    /// Validates an order against all pre-trade risk checks
    ///
    /// Returns Ok(()) if the order passes all checks, or Err(RiskViolation) if any check fails.
    pub async fn validate(&self, order: &NewOrder, mid_price: f64) -> Result<(), RiskViolation> {
        debug!(
            symbol = %order.symbol,
            side = ?order.side,
            qty = order.qty,
            price = ?order.price,
            "Validating order"
        );

        // Check kill switch first
        if self.kill_switch.is_triggered() {
            return Err(RiskViolation::KillSwitchActive {
                reason: self.kill_switch.get_reason().unwrap_or_default(),
            });
        }

        // Run all checks
        self.check_order_size(order)?;
        self.check_position_limit(order)?;
        self.check_price_band(order, mid_price)?;
        self.check_notional(order, mid_price)?;

        debug!(symbol = %order.symbol, "Order passed pre-trade validation");
        Ok(())
    }

    /// Validates an order for a specific exchange
    pub async fn validate_for_exchange(
        &self,
        order: &NewOrder,
        exchange: Exchange,
        mid_price: f64,
    ) -> Result<(), RiskViolation> {
        debug!(
            symbol = %order.symbol,
            %exchange,
            side = ?order.side,
            qty = order.qty,
            "Validating order for exchange"
        );

        // Check kill switch first
        if self.kill_switch.is_triggered() {
            return Err(RiskViolation::KillSwitchActive {
                reason: self.kill_switch.get_reason().unwrap_or_default(),
            });
        }

        self.check_order_size(order)?;
        self.check_position_limit_for_exchange(order, exchange)?;
        self.check_price_band(order, mid_price)?;
        self.check_notional(order, mid_price)?;

        Ok(())
    }

    /// Check order size against maximum allowed
    fn check_order_size(&self, order: &NewOrder) -> Result<(), RiskViolation> {
        let max_size = self.limits.get_max_order_size(&order.symbol);

        if order.qty > max_size {
            warn!(
                symbol = %order.symbol,
                qty = order.qty,
                max = max_size,
                "Order size exceeds limit"
            );
            return Err(RiskViolation::MaxOrderSizeExceeded {
                symbol: order.symbol.clone(),
                requested: order.qty,
                max_allowed: max_size,
            });
        }

        Ok(())
    }

    /// Check if order would cause position to exceed limit
    fn check_position_limit(&self, order: &NewOrder) -> Result<(), RiskViolation> {
        self.check_position_limit_for_exchange(order, self.default_exchange)
    }

    /// Check position limit for a specific exchange
    fn check_position_limit_for_exchange(
        &self,
        order: &NewOrder,
        exchange: Exchange,
    ) -> Result<(), RiskViolation> {
        let max_position = self.limits.get_max_position(&order.symbol);

        // Get current position
        let current_position = self
            .position_manager
            .get_position(exchange, &order.symbol)
            .map(|p| p.qty)
            .unwrap_or(0.0);

        // Calculate position delta (positive for buys, negative for sells)
        let delta = match order.side {
            Side::Buy => order.qty,
            Side::Sell => -order.qty,
        };

        let new_position = current_position + delta;

        // Check if new position would exceed limit
        if new_position.abs() > max_position {
            warn!(
                symbol = %order.symbol,
                current = current_position,
                delta,
                new = new_position,
                max = max_position,
                "Position limit would be exceeded"
            );
            return Err(RiskViolation::MaxPositionExceeded {
                symbol: order.symbol.clone(),
                current: current_position,
                requested_delta: delta,
                max_allowed: max_position,
            });
        }

        Ok(())
    }

    /// Check if order price is within acceptable band of mid price
    fn check_price_band(&self, order: &NewOrder, mid_price: f64) -> Result<(), RiskViolation> {
        let order_price = match order.price {
            Some(p) => p,
            None => return Ok(()), // Market orders don't have a price to check
        };

        if mid_price <= 0.0 {
            return Ok(()); // Can't check without valid mid price
        }

        let deviation_bps = ((order_price - mid_price) / mid_price * 10000.0).abs();

        if deviation_bps > self.limits.max_price_deviation_bps {
            warn!(
                symbol = %order.symbol,
                order_price,
                mid_price,
                deviation_bps,
                max_bps = self.limits.max_price_deviation_bps,
                "Price band violation"
            );
            return Err(RiskViolation::PriceBandViolation {
                symbol: order.symbol.clone(),
                order_price,
                mid_price,
                max_deviation_bps: self.limits.max_price_deviation_bps,
            });
        }

        Ok(())
    }

    /// Check if order meets minimum notional value
    fn check_notional(&self, order: &NewOrder, mid_price: f64) -> Result<(), RiskViolation> {
        if mid_price <= 0.0 {
            return Ok(()); // Can't check without valid price
        }

        let price = order.price.unwrap_or(mid_price);
        let notional = order.qty * price;

        if notional < self.limits.min_notional_usd {
            warn!(
                symbol = %order.symbol,
                notional,
                min = self.limits.min_notional_usd,
                "Order below minimum notional"
            );
            return Err(RiskViolation::BelowMinNotional {
                symbol: order.symbol.clone(),
                notional,
                min_notional: self.limits.min_notional_usd,
            });
        }

        Ok(())
    }

    /// Calculate total exposure in USD for all positions
    pub fn calculate_total_exposure(&self, prices: &std::collections::HashMap<String, f64>) -> f64 {
        let positions = self.position_manager.get_all_positions();
        let mut total = 0.0;

        for (key, position) in positions {
            let price = prices.get(&key.symbol).copied().unwrap_or(position.entry_px);
            total += position.qty.abs() * price;
        }

        total
    }

    /// Check if adding an order would exceed total exposure limit
    pub fn check_total_exposure(
        &self,
        order: &NewOrder,
        prices: &std::collections::HashMap<String, f64>,
    ) -> Result<(), RiskViolation> {
        let current_exposure = self.calculate_total_exposure(prices);
        let order_price = order.price.unwrap_or_else(|| {
            prices.get(&order.symbol).copied().unwrap_or(0.0)
        });
        let order_notional = order.qty * order_price;

        let new_exposure = current_exposure + order_notional;

        if new_exposure > self.limits.max_total_exposure_usd {
            return Err(RiskViolation::MaxExposureExceeded {
                current_usd: current_exposure,
                order_notional_usd: order_notional,
                max_allowed_usd: self.limits.max_total_exposure_usd,
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adapters::traits::OrderType;

    fn make_order(symbol: &str, side: Side, qty: f64, price: Option<f64>) -> NewOrder {
        NewOrder {
            symbol: symbol.to_string(),
            side,
            ord_type: OrderType::Limit,
            qty,
            price,
            stop_price: None,
            tif: None,
            post_only: false,
            reduce_only: false,
            client_order_id: "test".to_string(),
        }
    }

    #[tokio::test]
    async fn test_order_size_check() {
        let limits = RiskLimits::default().with_max_order_size("BTCUSD", 1.0);
        let pm = Arc::new(PositionManager::new());
        let ks = KillSwitch::new();
        let validator = PreTradeValidator::new(limits, pm, ks);

        // Should pass
        let order = make_order("BTCUSD", Side::Buy, 0.5, Some(50000.0));
        assert!(validator.validate(&order, 50000.0).await.is_ok());

        // Should fail
        let order = make_order("BTCUSD", Side::Buy, 1.5, Some(50000.0));
        let result = validator.validate(&order, 50000.0).await;
        assert!(matches!(result, Err(RiskViolation::MaxOrderSizeExceeded { .. })));
    }

    #[tokio::test]
    async fn test_price_band_check() {
        let limits = RiskLimits::default();
        let pm = Arc::new(PositionManager::new());
        let ks = KillSwitch::new();
        let validator = PreTradeValidator::new(limits, pm, ks);

        // Should pass - within 5%
        let order = make_order("BTCUSD", Side::Buy, 0.1, Some(51000.0));
        assert!(validator.validate(&order, 50000.0).await.is_ok());

        // Should fail - 20% deviation
        let order = make_order("BTCUSD", Side::Buy, 0.1, Some(60000.0));
        let result = validator.validate(&order, 50000.0).await;
        assert!(matches!(result, Err(RiskViolation::PriceBandViolation { .. })));
    }

    #[tokio::test]
    async fn test_kill_switch_blocks_orders() {
        let limits = RiskLimits::default();
        let pm = Arc::new(PositionManager::new());
        let ks = KillSwitch::new();
        ks.trigger("Test trigger");

        let validator = PreTradeValidator::new(limits, pm, ks);

        let order = make_order("BTCUSD", Side::Buy, 0.1, Some(50000.0));
        let result = validator.validate(&order, 50000.0).await;
        assert!(matches!(result, Err(RiskViolation::KillSwitchActive { .. })));
    }
}
