//! Strategy Metrics Tracking
//!
//! Provides lightweight metrics collection for trading strategies.
//! Can be extended to export to Prometheus, StatsD, or other systems.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::info;

/// Trading strategy metrics
#[derive(Clone, Default)]
pub struct StrategyMetrics {
    inner: Arc<MetricsInner>,
}

#[derive(Default)]
struct MetricsInner {
    // Order metrics
    orders_placed: AtomicU64,
    orders_filled: AtomicU64,
    orders_canceled: AtomicU64,
    orders_rejected: AtomicU64,

    // Fill metrics
    total_buy_qty: AtomicU64,   // Stored as qty * 1e8 for precision
    total_sell_qty: AtomicU64,
    total_fees_paid: AtomicU64, // Stored as fees * 1e8

    // Quote metrics
    quotes_generated: AtomicU64,
    quotes_skipped: AtomicU64,

    // Timing
    start_time: std::sync::OnceLock<Instant>,
}

impl StrategyMetrics {
    /// Creates a new metrics instance
    pub fn new() -> Self {
        let metrics = Self::default();
        let _ = metrics.inner.start_time.set(Instant::now());
        metrics
    }

    // =========================================================================
    // Order Tracking
    // =========================================================================

    /// Records an order placement
    pub fn record_order_placed(&self) {
        self.inner.orders_placed.fetch_add(1, Ordering::Relaxed);
    }

    /// Records an order fill
    pub fn record_order_filled(&self) {
        self.inner.orders_filled.fetch_add(1, Ordering::Relaxed);
    }

    /// Records an order cancellation
    pub fn record_order_canceled(&self) {
        self.inner.orders_canceled.fetch_add(1, Ordering::Relaxed);
    }

    /// Records an order rejection
    pub fn record_order_rejected(&self) {
        self.inner.orders_rejected.fetch_add(1, Ordering::Relaxed);
    }

    // =========================================================================
    // Fill Tracking
    // =========================================================================

    /// Records a buy fill
    pub fn record_buy_fill(&self, qty: f64, fee: f64) {
        let qty_scaled = (qty * 1e8) as u64;
        let fee_scaled = (fee * 1e8) as u64;
        self.inner.total_buy_qty.fetch_add(qty_scaled, Ordering::Relaxed);
        self.inner.total_fees_paid.fetch_add(fee_scaled, Ordering::Relaxed);
    }

    /// Records a sell fill
    pub fn record_sell_fill(&self, qty: f64, fee: f64) {
        let qty_scaled = (qty * 1e8) as u64;
        let fee_scaled = (fee * 1e8) as u64;
        self.inner.total_sell_qty.fetch_add(qty_scaled, Ordering::Relaxed);
        self.inner.total_fees_paid.fetch_add(fee_scaled, Ordering::Relaxed);
    }

    // =========================================================================
    // Quote Tracking
    // =========================================================================

    /// Records a quote generation
    pub fn record_quote_generated(&self) {
        self.inner.quotes_generated.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a skipped quote (e.g., due to inventory limits)
    pub fn record_quote_skipped(&self) {
        self.inner.quotes_skipped.fetch_add(1, Ordering::Relaxed);
    }

    // =========================================================================
    // Getters
    // =========================================================================

    /// Returns total orders placed
    pub fn orders_placed(&self) -> u64 {
        self.inner.orders_placed.load(Ordering::Relaxed)
    }

    /// Returns total orders filled
    pub fn orders_filled(&self) -> u64 {
        self.inner.orders_filled.load(Ordering::Relaxed)
    }

    /// Returns fill rate (fills / placed)
    pub fn fill_rate(&self) -> f64 {
        let placed = self.orders_placed();
        if placed == 0 {
            0.0
        } else {
            self.orders_filled() as f64 / placed as f64
        }
    }

    /// Returns total buy quantity
    pub fn total_buy_qty(&self) -> f64 {
        self.inner.total_buy_qty.load(Ordering::Relaxed) as f64 / 1e8
    }

    /// Returns total sell quantity
    pub fn total_sell_qty(&self) -> f64 {
        self.inner.total_sell_qty.load(Ordering::Relaxed) as f64 / 1e8
    }

    /// Returns net position delta (buys - sells)
    pub fn net_position_delta(&self) -> f64 {
        self.total_buy_qty() - self.total_sell_qty()
    }

    /// Returns total fees paid
    pub fn total_fees(&self) -> f64 {
        self.inner.total_fees_paid.load(Ordering::Relaxed) as f64 / 1e8
    }

    /// Returns uptime duration
    pub fn uptime(&self) -> Duration {
        self.inner
            .start_time
            .get()
            .map(|s| s.elapsed())
            .unwrap_or_default()
    }

    /// Logs a summary of all metrics
    pub fn log_summary(&self) {
        let uptime = self.uptime();
        let uptime_mins = uptime.as_secs() / 60;

        info!(
            orders_placed = self.orders_placed(),
            orders_filled = self.orders_filled(),
            orders_canceled = self.inner.orders_canceled.load(Ordering::Relaxed),
            fill_rate_pct = format!("{:.1}%", self.fill_rate() * 100.0),
            total_buy_qty = format!("{:.6}", self.total_buy_qty()),
            total_sell_qty = format!("{:.6}", self.total_sell_qty()),
            net_delta = format!("{:.6}", self.net_position_delta()),
            total_fees = format!("{:.4}", self.total_fees()),
            uptime_mins = uptime_mins,
            "Strategy metrics summary"
        );
    }

    /// Returns a snapshot of current metrics
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            orders_placed: self.orders_placed(),
            orders_filled: self.orders_filled(),
            orders_canceled: self.inner.orders_canceled.load(Ordering::Relaxed),
            orders_rejected: self.inner.orders_rejected.load(Ordering::Relaxed),
            total_buy_qty: self.total_buy_qty(),
            total_sell_qty: self.total_sell_qty(),
            total_fees: self.total_fees(),
            quotes_generated: self.inner.quotes_generated.load(Ordering::Relaxed),
            quotes_skipped: self.inner.quotes_skipped.load(Ordering::Relaxed),
            uptime_secs: self.uptime().as_secs(),
        }
    }
}

/// A point-in-time snapshot of metrics
#[derive(Clone, Debug)]
pub struct MetricsSnapshot {
    pub orders_placed: u64,
    pub orders_filled: u64,
    pub orders_canceled: u64,
    pub orders_rejected: u64,
    pub total_buy_qty: f64,
    pub total_sell_qty: f64,
    pub total_fees: f64,
    pub quotes_generated: u64,
    pub quotes_skipped: u64,
    pub uptime_secs: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_tracking() {
        let metrics = StrategyMetrics::new();

        metrics.record_order_placed();
        metrics.record_order_placed();
        metrics.record_order_filled();

        assert_eq!(metrics.orders_placed(), 2);
        assert_eq!(metrics.orders_filled(), 1);
        assert_eq!(metrics.fill_rate(), 0.5);
    }

    #[test]
    fn test_fill_tracking() {
        let metrics = StrategyMetrics::new();

        metrics.record_buy_fill(0.001, 0.0001);
        metrics.record_sell_fill(0.0005, 0.00005);

        assert!((metrics.total_buy_qty() - 0.001).abs() < 1e-10);
        assert!((metrics.total_sell_qty() - 0.0005).abs() < 1e-10);
        assert!((metrics.net_position_delta() - 0.0005).abs() < 1e-10);
    }
}
