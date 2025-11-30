//! P99 Latency Tracking for Order Round-Trip Time (ORTT)
//!
//! Tracks order latency from creation to acknowledgment, providing
//! percentile-based metrics critical for market making performance.
//!
//! Key metric: p99 latency must be < 200ms for acceptable performance

use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use parking_lot::RwLock;
use std::collections::VecDeque;

/// Configuration for latency tracking
#[derive(Clone, Debug)]
pub struct LatencyConfig {
    /// Maximum samples to keep in rolling window
    pub window_size: usize,

    /// Warning threshold for p99 latency (ms)
    pub p99_warning_ms: u64,

    /// Critical threshold for p99 latency (ms)
    pub p99_critical_ms: u64,
}

impl Default for LatencyConfig {
    fn default() -> Self {
        Self {
            window_size: 1000,
            p99_warning_ms: 150,
            p99_critical_ms: 200,
        }
    }
}

/// Latency sample with timestamps
#[derive(Debug, Clone, Copy)]
pub struct LatencySample {
    /// When order was created (ms since epoch)
    pub created_ms: u64,

    /// When order was sent to exchange (ms since epoch)
    pub sent_ms: u64,

    /// When acknowledgment was received (ms since epoch)
    pub recv_ms: u64,

    /// Sample timestamp
    pub sampled_at: Instant,
}

impl LatencySample {
    /// Creates a new latency sample
    pub fn new(created_ms: u64, sent_ms: u64, recv_ms: u64) -> Self {
        Self {
            created_ms,
            sent_ms,
            recv_ms,
            sampled_at: Instant::now(),
        }
    }

    /// Calculates Order Round-Trip Time (ORTT) in milliseconds
    pub fn ortt_ms(&self) -> u64 {
        self.recv_ms.saturating_sub(self.created_ms)
    }

    /// Calculates network round-trip time in milliseconds
    pub fn network_rtt_ms(&self) -> u64 {
        self.recv_ms.saturating_sub(self.sent_ms)
    }

    /// Calculates internal processing time in milliseconds
    pub fn internal_latency_ms(&self) -> u64 {
        self.sent_ms.saturating_sub(self.created_ms)
    }
}

/// Statistics for latency tracking
#[derive(Debug, Clone, Copy)]
pub struct LatencyStats {
    pub count: usize,
    pub min_ms: u64,
    pub max_ms: u64,
    pub mean_ms: f64,
    pub p50_ms: u64,
    pub p90_ms: u64,
    pub p95_ms: u64,
    pub p99_ms: u64,
}

/// P99 Latency Tracker
///
/// Thread-safe tracker for order round-trip latency with percentile calculations
pub struct LatencyTracker {
    config: LatencyConfig,
    samples: Arc<RwLock<VecDeque<LatencySample>>>,
}

impl LatencyTracker {
    /// Creates a new latency tracker with default configuration
    pub fn new() -> Self {
        Self::with_config(LatencyConfig::default())
    }

    /// Creates a new latency tracker with custom configuration
    pub fn with_config(config: LatencyConfig) -> Self {
        let window_size = config.window_size;
        Self {
            config,
            samples: Arc::new(RwLock::new(VecDeque::with_capacity(window_size))),
        }
    }

    /// Records a new latency sample
    pub fn record(&self, created_ms: u64, sent_ms: u64, recv_ms: u64) {
        let sample = LatencySample::new(created_ms, sent_ms, recv_ms);
        let mut samples = self.samples.write();

        // Add new sample
        samples.push_back(sample);

        // Remove old samples if window is full
        while samples.len() > self.config.window_size {
            samples.pop_front();
        }
    }

    /// Records a latency sample using current time
    pub fn record_now(&self, created_ms: u64, sent_ms: u64) -> u64 {
        let recv_ms = now_ms();
        self.record(created_ms, sent_ms, recv_ms);
        recv_ms
    }

    /// Gets current latency statistics
    pub fn get_stats(&self) -> Option<LatencyStats> {
        let samples = self.samples.read();

        if samples.is_empty() {
            return None;
        }

        // Extract ORTT values and sort
        let mut latencies: Vec<u64> = samples.iter()
            .map(|s| s.ortt_ms())
            .collect();
        latencies.sort_unstable();

        let count = latencies.len();
        let sum: u64 = latencies.iter().sum();

        Some(LatencyStats {
            count,
            min_ms: latencies[0],
            max_ms: latencies[count - 1],
            mean_ms: sum as f64 / count as f64,
            p50_ms: latencies[count * 50 / 100],
            p90_ms: latencies[count * 90 / 100],
            p95_ms: latencies[count * 95 / 100],
            p99_ms: latencies[count * 99 / 100],
        })
    }

    /// Gets p99 latency in milliseconds
    pub fn get_p99(&self) -> Option<u64> {
        self.get_stats().map(|s| s.p99_ms)
    }

    /// Checks if p99 latency exceeds warning threshold
    pub fn is_warning(&self) -> bool {
        self.get_p99()
            .map(|p99| p99 >= self.config.p99_warning_ms)
            .unwrap_or(false)
    }

    /// Checks if p99 latency exceeds critical threshold
    pub fn is_critical(&self) -> bool {
        self.get_p99()
            .map(|p99| p99 >= self.config.p99_critical_ms)
            .unwrap_or(false)
    }

    /// Clears all samples
    pub fn clear(&self) {
        self.samples.write().clear();
    }

    /// Gets the number of samples currently stored
    pub fn sample_count(&self) -> usize {
        self.samples.read().len()
    }
}

impl Default for LatencyTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for LatencyTracker {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            samples: Arc::clone(&self.samples),
        }
    }
}

/// Gets current timestamp in milliseconds since UNIX epoch
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    #[test]
    fn test_latency_sample_calculations() {
        let sample = LatencySample::new(1000, 1010, 1150);

        assert_eq!(sample.ortt_ms(), 150);
        assert_eq!(sample.network_rtt_ms(), 140);
        assert_eq!(sample.internal_latency_ms(), 10);
    }

    #[test]
    fn test_latency_tracker_basic() {
        let tracker = LatencyTracker::new();

        tracker.record(1000, 1010, 1150);
        tracker.record(1000, 1010, 1120);
        tracker.record(1000, 1010, 1180);

        assert_eq!(tracker.sample_count(), 3);

        let stats = tracker.get_stats().unwrap();
        assert_eq!(stats.count, 3);
        assert_eq!(stats.min_ms, 120);
        assert_eq!(stats.max_ms, 180);
    }

    #[test]
    fn test_latency_tracker_percentiles() {
        let tracker = LatencyTracker::new();

        // Add 100 samples with latencies from 100ms to 199ms
        for i in 0..100 {
            tracker.record(1000, 1010, 1000 + 100 + i);
        }

        let stats = tracker.get_stats().unwrap();
        assert_eq!(stats.count, 100);
        assert_eq!(stats.p50_ms, 149); // Median should be ~149ms
        assert_eq!(stats.p99_ms, 198); // P99 should be ~198ms
    }

    #[test]
    fn test_latency_tracker_window_size() {
        let config = LatencyConfig {
            window_size: 10,
            ..Default::default()
        };
        let tracker = LatencyTracker::with_config(config);

        // Add 20 samples
        for i in 0..20 {
            tracker.record(1000, 1010, 1000 + 100 + i);
        }

        // Should only keep last 10
        assert_eq!(tracker.sample_count(), 10);
    }

    #[test]
    fn test_latency_tracker_thresholds() {
        let config = LatencyConfig {
            window_size: 100,
            p99_warning_ms: 150,
            p99_critical_ms: 200,
        };
        let tracker = LatencyTracker::with_config(config);

        // Add samples with p99 at 145ms (below warning)
        for i in 0..100 {
            tracker.record(1000, 1010, 1000 + 100 + (i * 45) / 100);
        }

        assert!(!tracker.is_warning());
        assert!(!tracker.is_critical());

        // Add samples with p99 at 160ms (warning level)
        tracker.clear();
        for i in 0..100 {
            tracker.record(1000, 1010, 1000 + 100 + (i * 60) / 100);
        }

        assert!(tracker.is_warning());
        assert!(!tracker.is_critical());

        // Add samples with p99 at 210ms (critical level)
        tracker.clear();
        for i in 0..100 {
            tracker.record(1000, 1010, 1000 + 100 + (i * 110) / 100);
        }

        assert!(tracker.is_warning());
        assert!(tracker.is_critical());
    }

    #[test]
    fn test_latency_tracker_empty() {
        let tracker = LatencyTracker::new();

        assert!(tracker.get_stats().is_none());
        assert!(tracker.get_p99().is_none());
        assert!(!tracker.is_warning());
        assert!(!tracker.is_critical());
    }

    #[test]
    fn test_latency_tracker_record_now() {
        let tracker = LatencyTracker::new();

        let created = now_ms();
        sleep(Duration::from_millis(5));
        let sent = now_ms();
        sleep(Duration::from_millis(5));

        tracker.record_now(created, sent);

        assert_eq!(tracker.sample_count(), 1);
        let stats = tracker.get_stats().unwrap();
        assert!(stats.min_ms >= 10); // At least 10ms total
    }
}
