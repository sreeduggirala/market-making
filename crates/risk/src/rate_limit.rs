//! Strategy-level rate limiting
//!
//! Provides rate limiting at the strategy level to prevent excessive order churn
//! and comply with exchange rate limits.

use crate::{RateLimits, RiskViolation};
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, warn};

/// Strategy-level rate limiter
///
/// Tracks order and cancel rates to ensure compliance with limits.
/// Uses a sliding window approach for accurate rate calculation.
pub struct StrategyRateLimiter {
    limits: RateLimits,

    /// Order timestamps within the window
    order_times: Mutex<VecDeque<u64>>,

    /// Cancel timestamps within the window
    cancel_times: Mutex<VecDeque<u64>>,

    /// Total orders submitted (for order-to-trade ratio)
    total_orders: AtomicU64,

    /// Total fills received (for order-to-trade ratio)
    total_fills: AtomicU64,
}

impl StrategyRateLimiter {
    /// Creates a new rate limiter with the given limits
    pub fn new(limits: RateLimits) -> Self {
        Self {
            limits,
            order_times: Mutex::new(VecDeque::new()),
            cancel_times: Mutex::new(VecDeque::new()),
            total_orders: AtomicU64::new(0),
            total_fills: AtomicU64::new(0),
        }
    }

    /// Checks if an order can be submitted without violating rate limits
    pub fn check_order(&self) -> Result<(), RiskViolation> {
        let now = now_ms();
        let window_start = now.saturating_sub(self.limits.window_secs * 1000);

        // Clean old entries and count current rate
        let mut order_times = self.order_times.lock();
        self.clean_old_entries(&mut order_times, window_start);

        let current_rate = order_times.len() as u32;
        let max_rate = (self.limits.max_orders_per_second as f64 * self.limits.burst_multiplier) as u32;

        if current_rate >= max_rate {
            warn!(
                current = current_rate,
                max = max_rate,
                "Order rate limit exceeded"
            );
            return Err(RiskViolation::OrderRateLimitExceeded {
                orders_per_second: current_rate / self.limits.window_secs as u32,
                max_per_second: self.limits.max_orders_per_second,
            });
        }

        Ok(())
    }

    /// Checks if a cancel can be submitted without violating rate limits
    pub fn check_cancel(&self) -> Result<(), RiskViolation> {
        let now = now_ms();
        let window_start = now.saturating_sub(self.limits.window_secs * 1000);

        let mut cancel_times = self.cancel_times.lock();
        self.clean_old_entries(&mut cancel_times, window_start);

        let current_rate = cancel_times.len() as u32;
        let max_rate = (self.limits.max_cancels_per_second as f64 * self.limits.burst_multiplier) as u32;

        if current_rate >= max_rate {
            warn!(
                current = current_rate,
                max = max_rate,
                "Cancel rate limit exceeded"
            );
            return Err(RiskViolation::CancelRateLimitExceeded {
                cancels_per_second: current_rate / self.limits.window_secs as u32,
                max_per_second: self.limits.max_cancels_per_second,
            });
        }

        Ok(())
    }

    /// Records an order submission
    pub fn record_order(&self) {
        let now = now_ms();

        let mut order_times = self.order_times.lock();
        order_times.push_back(now);
        self.total_orders.fetch_add(1, Ordering::Relaxed);

        debug!(total = self.total_orders.load(Ordering::Relaxed), "Order recorded");
    }

    /// Records a cancel submission
    pub fn record_cancel(&self) {
        let now = now_ms();

        let mut cancel_times = self.cancel_times.lock();
        cancel_times.push_back(now);

        debug!("Cancel recorded");
    }

    /// Records a fill (for order-to-trade ratio)
    pub fn record_fill(&self) {
        self.total_fills.fetch_add(1, Ordering::Relaxed);
    }

    /// Gets the current order-to-trade ratio
    pub fn get_order_to_trade_ratio(&self) -> f64 {
        let orders = self.total_orders.load(Ordering::Relaxed) as f64;
        let fills = self.total_fills.load(Ordering::Relaxed) as f64;

        if fills == 0.0 {
            return 0.0; // No fills yet
        }

        orders / fills
    }

    /// Checks if order-to-trade ratio is within limits
    pub fn check_order_to_trade_ratio(&self) -> Result<(), RiskViolation> {
        let ratio = self.get_order_to_trade_ratio();

        // Only check after we have some fills
        let fills = self.total_fills.load(Ordering::Relaxed);
        if fills < 10 {
            return Ok(()); // Not enough data
        }

        if ratio > self.limits.max_order_to_trade_ratio {
            warn!(
                ratio,
                max = self.limits.max_order_to_trade_ratio,
                "Order-to-trade ratio exceeded"
            );
            return Err(RiskViolation::OrderToTradeRatioExceeded {
                ratio,
                max_ratio: self.limits.max_order_to_trade_ratio,
            });
        }

        Ok(())
    }

    /// Gets the number of orders in the current window
    pub fn orders_in_window(&self) -> u32 {
        let now = now_ms();
        let window_start = now.saturating_sub(self.limits.window_secs * 1000);

        let mut order_times = self.order_times.lock();
        self.clean_old_entries(&mut order_times, window_start);
        order_times.len() as u32
    }

    /// Gets the number of cancels in the current window
    pub fn cancels_in_window(&self) -> u32 {
        let now = now_ms();
        let window_start = now.saturating_sub(self.limits.window_secs * 1000);

        let mut cancel_times = self.cancel_times.lock();
        self.clean_old_entries(&mut cancel_times, window_start);
        cancel_times.len() as u32
    }

    /// Gets total orders submitted
    pub fn total_orders(&self) -> u64 {
        self.total_orders.load(Ordering::Relaxed)
    }

    /// Gets total fills received
    pub fn total_fills(&self) -> u64 {
        self.total_fills.load(Ordering::Relaxed)
    }

    /// Resets all counters (e.g., for new session)
    pub fn reset(&self) {
        self.order_times.lock().clear();
        self.cancel_times.lock().clear();
        self.total_orders.store(0, Ordering::Relaxed);
        self.total_fills.store(0, Ordering::Relaxed);
    }

    /// Gets rate limiter statistics
    pub fn get_stats(&self) -> RateLimitStats {
        RateLimitStats {
            orders_in_window: self.orders_in_window(),
            cancels_in_window: self.cancels_in_window(),
            total_orders: self.total_orders(),
            total_fills: self.total_fills(),
            order_to_trade_ratio: self.get_order_to_trade_ratio(),
            max_orders_per_second: self.limits.max_orders_per_second,
            max_cancels_per_second: self.limits.max_cancels_per_second,
        }
    }

    /// Cleans entries older than the window start
    fn clean_old_entries(&self, times: &mut VecDeque<u64>, window_start: u64) {
        while let Some(&front) = times.front() {
            if front < window_start {
                times.pop_front();
            } else {
                break;
            }
        }
    }
}

/// Rate limiter statistics
#[derive(Debug, Clone)]
pub struct RateLimitStats {
    pub orders_in_window: u32,
    pub cancels_in_window: u32,
    pub total_orders: u64,
    pub total_fills: u64,
    pub order_to_trade_ratio: f64,
    pub max_orders_per_second: u32,
    pub max_cancels_per_second: u32,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limiter_allows_orders() {
        let limits = RateLimits {
            max_orders_per_second: 10,
            burst_multiplier: 1.0,
            ..Default::default()
        };
        let limiter = StrategyRateLimiter::new(limits);

        // Should allow first order
        assert!(limiter.check_order().is_ok());
        limiter.record_order();

        // Should allow more orders
        for _ in 0..5 {
            assert!(limiter.check_order().is_ok());
            limiter.record_order();
        }
    }

    #[test]
    fn test_rate_limiter_blocks_excess_orders() {
        let limits = RateLimits {
            max_orders_per_second: 5,
            burst_multiplier: 1.0,
            window_secs: 1,
            ..Default::default()
        };
        let limiter = StrategyRateLimiter::new(limits);

        // Submit up to limit
        for _ in 0..5 {
            assert!(limiter.check_order().is_ok());
            limiter.record_order();
        }

        // Next should fail
        let result = limiter.check_order();
        assert!(matches!(result, Err(RiskViolation::OrderRateLimitExceeded { .. })));
    }

    #[test]
    fn test_order_to_trade_ratio() {
        let limits = RateLimits::default();
        let limiter = StrategyRateLimiter::new(limits);

        // Record some orders and fills
        for _ in 0..50 {
            limiter.record_order();
        }
        for _ in 0..10 {
            limiter.record_fill();
        }

        assert_eq!(limiter.get_order_to_trade_ratio(), 5.0);
    }

    #[test]
    fn test_rate_limiter_stats() {
        let limits = RateLimits::default();
        let limiter = StrategyRateLimiter::new(limits);

        limiter.record_order();
        limiter.record_order();
        limiter.record_cancel();
        limiter.record_fill();

        let stats = limiter.get_stats();
        assert_eq!(stats.total_orders, 2);
        assert_eq!(stats.total_fills, 1);
        assert_eq!(stats.order_to_trade_ratio, 2.0);
    }

    #[test]
    fn test_rate_limiter_reset() {
        let limits = RateLimits::default();
        let limiter = StrategyRateLimiter::new(limits);

        limiter.record_order();
        limiter.record_fill();

        limiter.reset();

        assert_eq!(limiter.total_orders(), 0);
        assert_eq!(limiter.total_fills(), 0);
    }
}
