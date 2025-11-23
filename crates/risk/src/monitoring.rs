//! Monitoring and Metrics Module
//!
//! Provides observability for the market-making system:
//! - Prometheus-compatible metrics
//! - Health checks
//! - Alert thresholds
//!
//! # Metrics Categories
//!
//! - **Trading**: Orders, fills, PnL, positions
//! - **System**: Latency, errors, reconnections
//! - **Risk**: Limit utilization, violations

use std::sync::atomic::{AtomicU64, AtomicI64, Ordering};
use std::time::{Duration, Instant};

// =============================================================================
// Metric Types
// =============================================================================

/// Thread-safe counter metric
#[derive(Default)]
pub struct Counter(AtomicU64);

impl Counter {
    pub fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_by(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Thread-safe gauge metric (can go up or down)
#[derive(Default)]
pub struct Gauge(AtomicI64);

impl Gauge {
    pub fn new() -> Self {
        Self(AtomicI64::new(0))
    }

    pub fn set(&self, val: i64) {
        self.0.store(val, Ordering::Relaxed);
    }

    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec(&self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn get(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Histogram for latency measurements (buckets in microseconds)
pub struct Histogram {
    buckets: Vec<(u64, AtomicU64)>, // (upper_bound_us, count)
    sum: AtomicU64,
    count: AtomicU64,
}

impl Histogram {
    /// Creates a histogram with default latency buckets (in microseconds)
    pub fn new_latency() -> Self {
        // Buckets: 100us, 500us, 1ms, 5ms, 10ms, 50ms, 100ms, 500ms, 1s, 5s
        let bounds = vec![100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 500_000, 1_000_000, 5_000_000];
        Self::with_buckets(&bounds)
    }

    pub fn with_buckets(bounds: &[u64]) -> Self {
        let buckets = bounds.iter()
            .map(|&b| (b, AtomicU64::new(0)))
            .collect();
        Self {
            buckets,
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    /// Records a value in microseconds
    pub fn observe(&self, value_us: u64) {
        self.sum.fetch_add(value_us, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);

        for (bound, count) in &self.buckets {
            if value_us <= *bound {
                count.fetch_add(1, Ordering::Relaxed);
                break;
            }
        }
    }

    /// Records a duration
    pub fn observe_duration(&self, duration: Duration) {
        self.observe(duration.as_micros() as u64);
    }

    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    pub fn sum(&self) -> u64 {
        self.sum.load(Ordering::Relaxed)
    }

    pub fn mean(&self) -> f64 {
        let count = self.count();
        if count == 0 {
            0.0
        } else {
            self.sum() as f64 / count as f64
        }
    }
}

// =============================================================================
// Trading Metrics
// =============================================================================

/// Metrics for trading operations
#[derive(Default)]
pub struct TradingMetrics {
    // Order metrics
    pub orders_submitted: Counter,
    pub orders_filled: Counter,
    pub orders_canceled: Counter,
    pub orders_rejected: Counter,

    // Fill metrics
    pub fills_received: Counter,
    pub volume_traded_usd: AtomicU64, // Store as cents to avoid floats

    // PnL metrics (stored as cents)
    pub realized_pnl_cents: AtomicI64,
    pub unrealized_pnl_cents: AtomicI64,
    pub fees_paid_cents: AtomicU64,

    // Position metrics
    pub open_positions: Gauge,
    pub total_exposure_cents: AtomicU64,
}

impl TradingMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_order_submitted(&self) {
        self.orders_submitted.inc();
    }

    pub fn record_fill(&self, quantity: f64, price: f64, fee: f64) {
        self.fills_received.inc();
        let volume_cents = (quantity * price * 100.0) as u64;
        self.volume_traded_usd.fetch_add(volume_cents, Ordering::Relaxed);
        let fee_cents = (fee * 100.0) as u64;
        self.fees_paid_cents.fetch_add(fee_cents, Ordering::Relaxed);
    }

    pub fn set_realized_pnl(&self, pnl: f64) {
        self.realized_pnl_cents.store((pnl * 100.0) as i64, Ordering::Relaxed);
    }

    pub fn set_unrealized_pnl(&self, pnl: f64) {
        self.unrealized_pnl_cents.store((pnl * 100.0) as i64, Ordering::Relaxed);
    }
}

// =============================================================================
// System Metrics
// =============================================================================

/// Metrics for system health
pub struct SystemMetrics {
    // Latency
    pub order_latency: Histogram,
    pub market_data_latency: Histogram,
    pub ws_message_latency: Histogram,

    // Errors
    pub api_errors: Counter,
    pub ws_disconnects: Counter,
    pub ws_reconnects: Counter,

    // Throughput
    pub messages_received: Counter,
    pub messages_sent: Counter,
}

impl Default for SystemMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl SystemMetrics {
    pub fn new() -> Self {
        Self {
            order_latency: Histogram::new_latency(),
            market_data_latency: Histogram::new_latency(),
            ws_message_latency: Histogram::new_latency(),
            api_errors: Counter::new(),
            ws_disconnects: Counter::new(),
            ws_reconnects: Counter::new(),
            messages_received: Counter::new(),
            messages_sent: Counter::new(),
        }
    }

    /// Times an operation and records its latency
    pub fn time_order<F, T>(&self, f: F) -> T
    where
        F: FnOnce() -> T,
    {
        let start = Instant::now();
        let result = f();
        self.order_latency.observe_duration(start.elapsed());
        result
    }
}

// =============================================================================
// Risk Metrics
// =============================================================================

/// Metrics for risk monitoring
#[derive(Default)]
pub struct RiskMetrics {
    // Limit utilization (0-100%)
    pub position_limit_pct: Gauge,
    pub daily_loss_limit_pct: Gauge,
    pub order_rate_limit_pct: Gauge,

    // Violations
    pub risk_violations: Counter,
    pub kill_switch_activations: Counter,
    pub emergency_stops: Counter,
}

impl RiskMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_position_utilization(&self, current: f64, limit: f64) {
        let pct = if limit > 0.0 { (current / limit * 100.0) as i64 } else { 0 };
        self.position_limit_pct.set(pct);
    }

    pub fn set_loss_utilization(&self, current_loss: f64, limit: f64) {
        let pct = if limit > 0.0 { (current_loss.abs() / limit.abs() * 100.0) as i64 } else { 0 };
        self.daily_loss_limit_pct.set(pct);
    }

    pub fn record_violation(&self) {
        self.risk_violations.inc();
    }

    pub fn record_kill_switch(&self) {
        self.kill_switch_activations.inc();
    }
}

// =============================================================================
// Metrics Registry
// =============================================================================

/// Central metrics registry
pub struct MetricsRegistry {
    pub trading: TradingMetrics,
    pub system: SystemMetrics,
    pub risk: RiskMetrics,
    started_at: Instant,
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self {
            trading: TradingMetrics::new(),
            system: SystemMetrics::new(),
            risk: RiskMetrics::new(),
            started_at: Instant::now(),
        }
    }

    /// Returns uptime in seconds
    pub fn uptime_secs(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }

    /// Exports metrics in Prometheus format
    pub fn export_prometheus(&self) -> String {
        let mut output = String::new();

        // Trading metrics
        output.push_str(&format!("# HELP trading_orders_submitted Total orders submitted\n"));
        output.push_str(&format!("# TYPE trading_orders_submitted counter\n"));
        output.push_str(&format!("trading_orders_submitted {}\n", self.trading.orders_submitted.get()));

        output.push_str(&format!("trading_orders_filled {}\n", self.trading.orders_filled.get()));
        output.push_str(&format!("trading_orders_canceled {}\n", self.trading.orders_canceled.get()));
        output.push_str(&format!("trading_orders_rejected {}\n", self.trading.orders_rejected.get()));
        output.push_str(&format!("trading_fills_received {}\n", self.trading.fills_received.get()));

        // System metrics
        output.push_str(&format!("\n# System metrics\n"));
        output.push_str(&format!("system_api_errors {}\n", self.system.api_errors.get()));
        output.push_str(&format!("system_ws_disconnects {}\n", self.system.ws_disconnects.get()));
        output.push_str(&format!("system_ws_reconnects {}\n", self.system.ws_reconnects.get()));
        output.push_str(&format!("system_messages_received {}\n", self.system.messages_received.get()));
        output.push_str(&format!("system_order_latency_mean_us {:.2}\n", self.system.order_latency.mean()));

        // Risk metrics
        output.push_str(&format!("\n# Risk metrics\n"));
        output.push_str(&format!("risk_position_limit_pct {}\n", self.risk.position_limit_pct.get()));
        output.push_str(&format!("risk_daily_loss_limit_pct {}\n", self.risk.daily_loss_limit_pct.get()));
        output.push_str(&format!("risk_violations {}\n", self.risk.risk_violations.get()));
        output.push_str(&format!("risk_kill_switch_activations {}\n", self.risk.kill_switch_activations.get()));

        // Uptime
        output.push_str(&format!("\n# System info\n"));
        output.push_str(&format!("system_uptime_seconds {}\n", self.uptime_secs()));

        output
    }
}

// =============================================================================
// Alert Thresholds
// =============================================================================

/// Configuration for alert thresholds
#[derive(Clone, Debug)]
pub struct AlertThresholds {
    /// Warn when position utilization exceeds this %
    pub position_warn_pct: u8,
    /// Critical when position utilization exceeds this %
    pub position_critical_pct: u8,

    /// Warn when daily loss utilization exceeds this %
    pub loss_warn_pct: u8,
    /// Critical when daily loss utilization exceeds this %
    pub loss_critical_pct: u8,

    /// Warn when order latency exceeds this (ms)
    pub latency_warn_ms: u64,
    /// Critical when order latency exceeds this (ms)
    pub latency_critical_ms: u64,

    /// Warn when error rate exceeds this per minute
    pub error_rate_warn: u64,
    /// Critical when error rate exceeds this per minute
    pub error_rate_critical: u64,
}

impl Default for AlertThresholds {
    fn default() -> Self {
        Self {
            position_warn_pct: 70,
            position_critical_pct: 90,
            loss_warn_pct: 50,
            loss_critical_pct: 80,
            latency_warn_ms: 100,
            latency_critical_ms: 500,
            error_rate_warn: 10,
            error_rate_critical: 50,
        }
    }
}

/// Alert severity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
    Emergency,
}

/// An alert event
#[derive(Debug, Clone)]
pub struct Alert {
    pub severity: AlertSeverity,
    pub category: String,
    pub message: String,
    pub value: f64,
    pub threshold: f64,
}

impl MetricsRegistry {
    /// Checks metrics against thresholds and returns any alerts
    pub fn check_alerts(&self, thresholds: &AlertThresholds) -> Vec<Alert> {
        let mut alerts = Vec::new();

        // Position utilization
        let pos_pct = self.risk.position_limit_pct.get() as u8;
        if pos_pct >= thresholds.position_critical_pct {
            alerts.push(Alert {
                severity: AlertSeverity::Critical,
                category: "position".to_string(),
                message: format!("Position utilization at {}%", pos_pct),
                value: pos_pct as f64,
                threshold: thresholds.position_critical_pct as f64,
            });
        } else if pos_pct >= thresholds.position_warn_pct {
            alerts.push(Alert {
                severity: AlertSeverity::Warning,
                category: "position".to_string(),
                message: format!("Position utilization at {}%", pos_pct),
                value: pos_pct as f64,
                threshold: thresholds.position_warn_pct as f64,
            });
        }

        // Loss utilization
        let loss_pct = self.risk.daily_loss_limit_pct.get() as u8;
        if loss_pct >= thresholds.loss_critical_pct {
            alerts.push(Alert {
                severity: AlertSeverity::Critical,
                category: "loss".to_string(),
                message: format!("Daily loss at {}% of limit", loss_pct),
                value: loss_pct as f64,
                threshold: thresholds.loss_critical_pct as f64,
            });
        } else if loss_pct >= thresholds.loss_warn_pct {
            alerts.push(Alert {
                severity: AlertSeverity::Warning,
                category: "loss".to_string(),
                message: format!("Daily loss at {}% of limit", loss_pct),
                value: loss_pct as f64,
                threshold: thresholds.loss_warn_pct as f64,
            });
        }

        // Latency
        let mean_latency_us = self.system.order_latency.mean();
        let mean_latency_ms = mean_latency_us / 1000.0;
        if mean_latency_ms >= thresholds.latency_critical_ms as f64 {
            alerts.push(Alert {
                severity: AlertSeverity::Critical,
                category: "latency".to_string(),
                message: format!("Order latency at {:.1}ms", mean_latency_ms),
                value: mean_latency_ms,
                threshold: thresholds.latency_critical_ms as f64,
            });
        } else if mean_latency_ms >= thresholds.latency_warn_ms as f64 {
            alerts.push(Alert {
                severity: AlertSeverity::Warning,
                category: "latency".to_string(),
                message: format!("Order latency at {:.1}ms", mean_latency_ms),
                value: mean_latency_ms,
                threshold: thresholds.latency_warn_ms as f64,
            });
        }

        alerts
    }
}

// =============================================================================
// Health Check
// =============================================================================

/// Health check result
#[derive(Debug, Clone)]
pub struct HealthCheck {
    pub healthy: bool,
    pub components: Vec<ComponentHealth>,
}

#[derive(Debug, Clone)]
pub struct ComponentHealth {
    pub name: String,
    pub healthy: bool,
    pub message: Option<String>,
}

impl HealthCheck {
    pub fn new() -> Self {
        Self {
            healthy: true,
            components: Vec::new(),
        }
    }

    pub fn add_component(&mut self, name: &str, healthy: bool, message: Option<&str>) {
        if !healthy {
            self.healthy = false;
        }
        self.components.push(ComponentHealth {
            name: name.to_string(),
            healthy,
            message: message.map(|s| s.to_string()),
        });
    }
}

impl Default for HealthCheck {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter_operations() {
        let counter = Counter::new();
        assert_eq!(counter.get(), 0);

        counter.inc();
        assert_eq!(counter.get(), 1);

        counter.inc_by(5);
        assert_eq!(counter.get(), 6);
    }

    #[test]
    fn test_gauge_operations() {
        let gauge = Gauge::new();
        assert_eq!(gauge.get(), 0);

        gauge.set(100);
        assert_eq!(gauge.get(), 100);

        gauge.inc();
        assert_eq!(gauge.get(), 101);

        gauge.dec();
        assert_eq!(gauge.get(), 100);

        gauge.set(-50);
        assert_eq!(gauge.get(), -50);
    }

    #[test]
    fn test_histogram_basic() {
        let hist = Histogram::new_latency();

        hist.observe(100);
        hist.observe(500);
        hist.observe(1000);

        assert_eq!(hist.count(), 3);
        assert_eq!(hist.sum(), 1600);
    }

    #[test]
    fn test_histogram_mean() {
        let hist = Histogram::new_latency();

        hist.observe(100);
        hist.observe(200);
        hist.observe(300);

        assert!((hist.mean() - 200.0).abs() < 0.01);
    }

    #[test]
    fn test_histogram_empty_mean() {
        let hist = Histogram::new_latency();
        assert_eq!(hist.mean(), 0.0);
    }

    #[test]
    fn test_trading_metrics_fill() {
        let metrics = TradingMetrics::new();

        metrics.record_fill(0.1, 50000.0, 5.0);

        assert_eq!(metrics.fills_received.get(), 1);
        // 0.1 * 50000 * 100 = 500000 cents
        assert_eq!(metrics.volume_traded_usd.load(Ordering::Relaxed), 500000);
        // 5.0 * 100 = 500 cents
        assert_eq!(metrics.fees_paid_cents.load(Ordering::Relaxed), 500);
    }

    #[test]
    fn test_risk_metrics_utilization() {
        let metrics = RiskMetrics::new();

        metrics.set_position_utilization(0.08, 0.10);
        assert_eq!(metrics.position_limit_pct.get(), 80);

        // For loss, we pass absolute values (current loss amount vs limit amount)
        metrics.set_loss_utilization(1500.0, 2000.0);
        assert_eq!(metrics.daily_loss_limit_pct.get(), 75);
    }

    #[test]
    fn test_metrics_registry_prometheus_export() {
        let registry = MetricsRegistry::new();

        registry.trading.orders_submitted.inc();
        registry.trading.orders_submitted.inc();
        registry.system.api_errors.inc();

        let output = registry.export_prometheus();

        assert!(output.contains("trading_orders_submitted 2"));
        assert!(output.contains("system_api_errors 1"));
    }

    #[test]
    fn test_alert_thresholds_default() {
        let thresholds = AlertThresholds::default();

        assert_eq!(thresholds.position_warn_pct, 70);
        assert_eq!(thresholds.position_critical_pct, 90);
        assert_eq!(thresholds.latency_warn_ms, 100);
    }

    #[test]
    fn test_check_alerts_position_warning() {
        let registry = MetricsRegistry::new();
        let thresholds = AlertThresholds::default();

        registry.risk.position_limit_pct.set(75);

        let alerts = registry.check_alerts(&thresholds);

        assert_eq!(alerts.len(), 1);
        assert!(matches!(alerts[0].severity, AlertSeverity::Warning));
        assert_eq!(alerts[0].category, "position");
    }

    #[test]
    fn test_check_alerts_position_critical() {
        let registry = MetricsRegistry::new();
        let thresholds = AlertThresholds::default();

        registry.risk.position_limit_pct.set(95);

        let alerts = registry.check_alerts(&thresholds);

        assert_eq!(alerts.len(), 1);
        assert!(matches!(alerts[0].severity, AlertSeverity::Critical));
    }

    #[test]
    fn test_check_alerts_multiple() {
        let registry = MetricsRegistry::new();
        let thresholds = AlertThresholds::default();

        registry.risk.position_limit_pct.set(95);
        registry.risk.daily_loss_limit_pct.set(85);

        let alerts = registry.check_alerts(&thresholds);

        assert_eq!(alerts.len(), 2);
    }

    #[test]
    fn test_check_alerts_none() {
        let registry = MetricsRegistry::new();
        let thresholds = AlertThresholds::default();

        registry.risk.position_limit_pct.set(50);
        registry.risk.daily_loss_limit_pct.set(30);

        let alerts = registry.check_alerts(&thresholds);

        assert!(alerts.is_empty());
    }

    #[test]
    fn test_health_check() {
        let mut health = HealthCheck::new();

        health.add_component("database", true, None);
        health.add_component("exchange_ws", true, Some("connected"));

        assert!(health.healthy);
        assert_eq!(health.components.len(), 2);

        health.add_component("risk_engine", false, Some("not initialized"));

        assert!(!health.healthy);
    }

    #[test]
    fn test_system_metrics_time_order() {
        let metrics = SystemMetrics::new();

        let result = metrics.time_order(|| {
            std::thread::sleep(Duration::from_millis(1));
            42
        });

        assert_eq!(result, 42);
        assert!(metrics.order_latency.count() > 0);
        assert!(metrics.order_latency.mean() >= 1000.0); // At least 1ms = 1000us
    }
}
