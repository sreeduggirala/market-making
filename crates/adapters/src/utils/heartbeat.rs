//! Heartbeat monitoring for WebSocket connections
//!
//! Tracks ping/pong messages to detect stale connections that need to be
//! reconnected. Prevents silent connection failures that could lead to
//! missed market data or order updates.

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::{debug, warn};

/// Configuration for heartbeat monitoring
#[derive(Clone, Debug)]
pub struct HeartbeatConfig {
    /// Maximum time without receiving pong before considering connection dead (ms)
    pub timeout_ms: u64,

    /// Interval for sending ping messages if exchange doesn't send them (ms)
    /// Set to 0 to disable active pinging
    pub ping_interval_ms: u64,

    /// Whether to actively send ping messages
    pub send_pings: bool,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            timeout_ms: 60000,      // 60 seconds
            ping_interval_ms: 30000, // 30 seconds
            send_pings: true,
        }
    }
}

impl HeartbeatConfig {
    /// Conservative production configuration
    pub fn production() -> Self {
        Self::default()
    }

    /// Configuration for exchanges that send their own pings
    pub fn passive() -> Self {
        Self {
            timeout_ms: 90000,
            ping_interval_ms: 0,
            send_pings: false,
        }
    }

    /// Aggressive configuration for faster failure detection
    pub fn aggressive() -> Self {
        Self {
            timeout_ms: 30000,
            ping_interval_ms: 10000,
            send_pings: true,
        }
    }
}

struct HeartbeatState {
    last_ping_sent: Option<Instant>,
    last_pong_received: Option<Instant>,
    last_message_received: Option<Instant>,
    config: HeartbeatConfig,
}

/// Heartbeat monitor for WebSocket connections
///
/// Tracks ping/pong timing and detects stale connections. Can be configured
/// to actively send pings or just monitor incoming messages.
pub struct HeartbeatMonitor {
    state: Arc<RwLock<HeartbeatState>>,
}

impl HeartbeatMonitor {
    /// Creates a new heartbeat monitor with the given configuration
    pub fn new(config: HeartbeatConfig) -> Self {
        let now = Instant::now();
        let state = HeartbeatState {
            last_ping_sent: None,
            last_pong_received: Some(now), // Start with "fresh" connection
            last_message_received: Some(now),
            config,
        };

        Self {
            state: Arc::new(RwLock::new(state)),
        }
    }

    /// Records that a ping was sent
    pub async fn record_ping_sent(&self) {
        let mut state = self.state.write().await;
        state.last_ping_sent = Some(Instant::now());
        debug!("Heartbeat ping sent");
    }

    /// Records that a pong was received
    pub async fn record_pong_received(&self) {
        let mut state = self.state.write().await;
        let now = Instant::now();
        state.last_pong_received = Some(now);
        state.last_message_received = Some(now);

        if let Some(ping_time) = state.last_ping_sent {
            let latency = now.duration_since(ping_time);
            debug!("Heartbeat pong received - latency: {:?}", latency);
        } else {
            debug!("Heartbeat pong received");
        }
    }

    /// Records that any message was received (resets staleness timer)
    pub async fn record_message_received(&self) {
        let mut state = self.state.write().await;
        state.last_message_received = Some(Instant::now());
    }

    /// Checks if the connection is alive based on pong timing
    ///
    /// Returns true if connection appears healthy, false if it's stale
    pub async fn is_alive(&self) -> bool {
        let state = self.state.read().await;
        let now = Instant::now();

        // If we've never received a pong, check last message time instead
        let last_activity = state
            .last_pong_received
            .or(state.last_message_received)
            .unwrap_or(now);

        let elapsed = now.duration_since(last_activity);
        let timeout = Duration::from_millis(state.config.timeout_ms);

        if elapsed >= timeout {
            warn!(
                "Heartbeat timeout - no activity for {:?} (timeout: {:?})",
                elapsed, timeout
            );
            false
        } else {
            true
        }
    }

    /// Gets the latency from last ping/pong if available
    pub async fn latency(&self) -> Option<Duration> {
        let state = self.state.read().await;

        match (state.last_ping_sent, state.last_pong_received) {
            (Some(ping), Some(pong)) if pong >= ping => {
                Some(pong.duration_since(ping))
            }
            _ => None,
        }
    }

    /// Gets time since last pong in milliseconds
    pub async fn time_since_last_pong(&self) -> Option<u64> {
        let state = self.state.read().await;
        state.last_pong_received.map(|pong| {
            Instant::now().duration_since(pong).as_millis() as u64
        })
    }

    /// Resets the heartbeat monitor (call after reconnection)
    pub async fn reset(&self) {
        let mut state = self.state.write().await;
        let now = Instant::now();
        state.last_ping_sent = None;
        state.last_pong_received = Some(now);
        state.last_message_received = Some(now);
        debug!("Heartbeat monitor reset");
    }

    /// Creates a background task that monitors connection health
    ///
    /// Returns a channel that receives true when connection is healthy,
    /// false when connection is stale and should be reconnected
    pub async fn spawn_monitor_task(&self) -> tokio::sync::mpsc::Receiver<bool> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let monitor = self.clone();

        tokio::spawn(async move {
            let mut check_interval = interval(Duration::from_secs(5));

            loop {
                check_interval.tick().await;

                let is_alive = monitor.is_alive().await;

                if !is_alive {
                    warn!("Heartbeat monitor detected stale connection");
                    let _ = tx.send(false).await;
                    break;
                }
            }
        });

        rx
    }

    /// Creates a background task that sends periodic pings
    ///
    /// Returns a channel that receives () when a ping should be sent
    pub async fn spawn_ping_task(&self) -> Option<tokio::sync::mpsc::Receiver<()>> {
        let state = self.state.read().await;
        if !state.config.send_pings || state.config.ping_interval_ms == 0 {
            return None;
        }

        let ping_interval_ms = state.config.ping_interval_ms;
        drop(state);

        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let monitor = self.clone();

        tokio::spawn(async move {
            let mut ping_timer = interval(Duration::from_millis(ping_interval_ms));

            loop {
                ping_timer.tick().await;

                // Signal that a ping should be sent
                if tx.send(()).await.is_err() {
                    debug!("Ping task channel closed - stopping ping task");
                    break;
                }

                monitor.record_ping_sent().await;
            }
        });

        Some(rx)
    }
}

impl Clone for HeartbeatMonitor {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_heartbeat_monitor_healthy() {
        let config = HeartbeatConfig {
            timeout_ms: 1000,
            ping_interval_ms: 500,
            send_pings: false,
        };

        let monitor = HeartbeatMonitor::new(config);
        assert!(monitor.is_alive().await);
    }

    #[tokio::test]
    async fn test_heartbeat_monitor_timeout() {
        let config = HeartbeatConfig {
            timeout_ms: 100,
            ping_interval_ms: 0,
            send_pings: false,
        };

        let monitor = HeartbeatMonitor::new(config);

        // Wait for timeout
        sleep(Duration::from_millis(150)).await;

        assert!(!monitor.is_alive().await);
    }

    #[tokio::test]
    async fn test_heartbeat_monitor_reset() {
        let config = HeartbeatConfig {
            timeout_ms: 100,
            ping_interval_ms: 0,
            send_pings: false,
        };

        let monitor = HeartbeatMonitor::new(config);

        // Wait for timeout
        sleep(Duration::from_millis(150)).await;
        assert!(!monitor.is_alive().await);

        // Reset should make it alive again
        monitor.reset().await;
        assert!(monitor.is_alive().await);
    }

    #[tokio::test]
    async fn test_heartbeat_latency() {
        let config = HeartbeatConfig::default();
        let monitor = HeartbeatMonitor::new(config);

        monitor.record_ping_sent().await;
        sleep(Duration::from_millis(10)).await;
        monitor.record_pong_received().await;

        let latency = monitor.latency().await;
        assert!(latency.is_some());
        let latency = latency.unwrap();
        assert!(latency >= Duration::from_millis(10));
        assert!(latency < Duration::from_millis(100));
    }
}
