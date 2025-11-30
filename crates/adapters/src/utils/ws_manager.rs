//! WebSocket Connection Manager
//!
//! Integrates heartbeat monitoring and reconnection logic to provide
//! resilient WebSocket connections for market data and user events.
//!
//! Target: Reconnect within < 5 seconds of disconnection

use super::heartbeat::{HeartbeatMonitor, HeartbeatConfig};
use super::reconnect::{ReconnectStrategy, ReconnectConfig};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{info, warn, error};

/// WebSocket connection state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Initial state before first connection
    Disconnected,
    /// Attempting to connect
    Connecting,
    /// Successfully connected and receiving data
    Connected,
    /// Connection lost, attempting to reconnect
    Reconnecting,
    /// Permanently failed (max retries exceeded)
    Failed,
}

/// WebSocket connection statistics
#[derive(Debug, Clone)]
pub struct ConnectionStats {
    /// Current state
    pub state: ConnectionState,

    /// Total number of connection attempts
    pub total_connections: u64,

    /// Total number of disconnections
    pub total_disconnections: u64,

    /// Total number of successful reconnections
    pub successful_reconnects: u64,

    /// Total number of failed reconnection attempts
    pub failed_reconnects: u64,

    /// Time of last successful connection
    pub last_connected_at: Option<Instant>,

    /// Time of last disconnection
    pub last_disconnected_at: Option<Instant>,

    /// Duration of last reconnection (if any)
    pub last_reconnect_duration: Option<Duration>,

    /// Whether currently alive (heartbeat check)
    pub is_alive: bool,

    /// Current heartbeat latency
    pub current_latency: Option<Duration>,
}

impl Default for ConnectionStats {
    fn default() -> Self {
        Self {
            state: ConnectionState::Disconnected,
            total_connections: 0,
            total_disconnections: 0,
            successful_reconnects: 0,
            failed_reconnects: 0,
            last_connected_at: None,
            last_disconnected_at: None,
            last_reconnect_duration: None,
            is_alive: false,
            current_latency: None,
        }
    }
}

/// Configuration for WebSocket manager
#[derive(Clone, Debug)]
pub struct WsManagerConfig {
    /// Heartbeat configuration
    pub heartbeat: HeartbeatConfig,

    /// Reconnection configuration
    pub reconnect: ReconnectConfig,

    /// Maximum time allowed for reconnection (ms)
    pub max_reconnect_time_ms: u64,
}

impl Default for WsManagerConfig {
    fn default() -> Self {
        Self {
            heartbeat: HeartbeatConfig::aggressive(), // Fast detection
            reconnect: ReconnectConfig {
                initial_backoff_ms: 1000,  // Fast retry for market making
                max_backoff_ms: 5000,      // Max 5 seconds
                backoff_multiplier: 1.0,   // No exponential backoff
                max_attempts: Some(5),     // Limit attempts
                jitter_factor: 0.1,
            },
            max_reconnect_time_ms: 5000, // Must reconnect within 5 seconds
        }
    }
}

/// WebSocket Manager
///
/// Manages WebSocket connections with automatic reconnection and heartbeat monitoring
pub struct WsManager {
    config: WsManagerConfig,
    heartbeat: HeartbeatMonitor,
    stats: Arc<RwLock<ConnectionStats>>,
}

impl WsManager {
    /// Creates a new WebSocket manager with default configuration
    pub fn new() -> Self {
        Self::with_config(WsManagerConfig::default())
    }

    /// Creates a new WebSocket manager with custom configuration
    pub fn with_config(config: WsManagerConfig) -> Self {
        let heartbeat = HeartbeatMonitor::new(config.heartbeat.clone());

        Self {
            config,
            heartbeat,
            stats: Arc::new(RwLock::new(ConnectionStats::default())),
        }
    }

    /// Marks connection as established
    pub async fn mark_connected(&self) {
        let mut stats = self.stats.write().await;
        stats.state = ConnectionState::Connected;
        stats.total_connections += 1;
        stats.last_connected_at = Some(Instant::now());
        stats.is_alive = true;

        self.heartbeat.reset().await;

        info!(
            connections = stats.total_connections,
            "WebSocket connected"
        );
    }

    /// Marks connection as disconnected
    pub async fn mark_disconnected(&self, reason: &str) {
        let mut stats = self.stats.write().await;
        stats.state = ConnectionState::Disconnected;
        stats.total_disconnections += 1;
        stats.last_disconnected_at = Some(Instant::now());
        stats.is_alive = false;

        warn!(
            disconnections = stats.total_disconnections,
            reason = reason,
            "WebSocket disconnected"
        );
    }

    /// Records that a message was received (for heartbeat tracking)
    pub async fn record_message(&self) {
        self.heartbeat.record_message_received().await;
    }

    /// Records a ping/pong for latency tracking
    pub async fn record_ping(&self) {
        self.heartbeat.record_ping_sent().await;
    }

    pub async fn record_pong(&self) {
        self.heartbeat.record_pong_received().await;

        // Update stats with current latency
        if let Some(latency) = self.heartbeat.latency().await {
            let mut stats = self.stats.write().await;
            stats.current_latency = Some(latency);
        }
    }

    /// Checks if connection is alive based on heartbeat
    pub async fn check_alive(&self) -> bool {
        let is_alive = self.heartbeat.is_alive().await;

        let mut stats = self.stats.write().await;
        stats.is_alive = is_alive;

        is_alive
    }

    /// Performs a reconnection attempt with timing
    ///
    /// Returns Ok(duration) if reconnection succeeded within time limit
    /// Returns Err if reconnection failed or exceeded time limit
    pub async fn attempt_reconnect<F, Fut>(&self, connect_fn: F) -> Result<Duration, String>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<(), String>>,
    {
        let reconnect_start = Instant::now();
        let mut strategy = ReconnectStrategy::new(self.config.reconnect.clone());

        {
            let mut stats = self.stats.write().await;
            stats.state = ConnectionState::Reconnecting;
        }

        info!("Starting reconnection attempt");

        while strategy.can_retry() {
            // Check if we've exceeded the maximum reconnection time
            let elapsed = reconnect_start.elapsed();
            if elapsed.as_millis() as u64 > self.config.max_reconnect_time_ms {
                let mut stats = self.stats.write().await;
                stats.state = ConnectionState::Failed;
                stats.failed_reconnects += 1;

                error!(
                    duration_ms = elapsed.as_millis() as u64,
                    max_allowed_ms = self.config.max_reconnect_time_ms,
                    "Reconnection exceeded time limit"
                );

                return Err(format!("Reconnection exceeded {}ms limit", self.config.max_reconnect_time_ms));
            }

            // Attempt connection
            match connect_fn().await {
                Ok(()) => {
                    let total_duration = reconnect_start.elapsed();
                    let mut stats = self.stats.write().await;
                    stats.state = ConnectionState::Connected;
                    stats.successful_reconnects += 1;
                    stats.last_reconnect_duration = Some(total_duration);
                    stats.last_connected_at = Some(Instant::now());
                    stats.is_alive = true;

                    self.heartbeat.reset().await;

                    info!(
                        duration_ms = total_duration.as_millis() as u64,
                        attempts = strategy.current_attempt() + 1,
                        "Reconnection successful"
                    );

                    return Ok(total_duration);
                }
                Err(e) => {
                    warn!(
                        attempt = strategy.current_attempt() + 1,
                        error = %e,
                        "Reconnection attempt failed"
                    );

                    // Wait before next retry (unless we're out of time)
                    if strategy.can_retry() {
                        let delay = strategy.peek_next_delay();
                        let remaining_time = self.config.max_reconnect_time_ms.saturating_sub(elapsed.as_millis() as u64);

                        if delay <= remaining_time {
                            strategy.wait_before_retry().await;
                        } else {
                            // Not enough time for another attempt
                            break;
                        }
                    }
                }
            }
        }

        let total_duration = reconnect_start.elapsed();
        let mut stats = self.stats.write().await;
        stats.state = ConnectionState::Failed;
        stats.failed_reconnects += 1;

        error!(
            duration_ms = total_duration.as_millis() as u64,
            attempts = strategy.current_attempt(),
            "All reconnection attempts failed"
        );

        Err("All reconnection attempts exhausted".to_string())
    }

    /// Gets current connection statistics
    pub async fn get_stats(&self) -> ConnectionStats {
        self.stats.read().await.clone()
    }

    /// Gets heartbeat monitor (for advanced usage)
    pub fn heartbeat(&self) -> &HeartbeatMonitor {
        &self.heartbeat
    }

    /// Resets statistics (for testing)
    pub async fn reset_stats(&self) {
        let mut stats = self.stats.write().await;
        *stats = ConnectionStats::default();
    }
}

impl Default for WsManager {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for WsManager {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            heartbeat: self.heartbeat.clone(),
            stats: Arc::clone(&self.stats),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_ws_manager_connect_disconnect() {
        let manager = WsManager::new();

        manager.mark_connected().await;
        let stats = manager.get_stats().await;
        assert_eq!(stats.state, ConnectionState::Connected);
        assert_eq!(stats.total_connections, 1);

        manager.mark_disconnected("test").await;
        let stats = manager.get_stats().await;
        assert_eq!(stats.state, ConnectionState::Disconnected);
        assert_eq!(stats.total_disconnections, 1);
    }

    #[tokio::test]
    async fn test_ws_manager_heartbeat() {
        let config = WsManagerConfig {
            heartbeat: HeartbeatConfig {
                timeout_ms: 100,
                ping_interval_ms: 50,
                send_pings: false,
            },
            ..Default::default()
        };
        let manager = WsManager::with_config(config);

        manager.mark_connected().await;
        assert!(manager.check_alive().await);

        // Wait for timeout
        sleep(Duration::from_millis(150)).await;
        assert!(!manager.check_alive().await);
    }

    #[tokio::test]
    async fn test_ws_manager_successful_reconnect() {
        let config = WsManagerConfig {
            reconnect: ReconnectConfig {
                initial_backoff_ms: 10,
                max_backoff_ms: 100,
                backoff_multiplier: 1.0,
                max_attempts: Some(3),
                jitter_factor: 0.0,
            },
            max_reconnect_time_ms: 1000,
            ..Default::default()
        };
        let manager = WsManager::with_config(config);

        let mut attempt = 0;
        let result = manager.attempt_reconnect(|| async {
            attempt += 1;
            if attempt >= 2 {
                Ok(())
            } else {
                Err("simulated failure".to_string())
            }
        }).await;

        assert!(result.is_ok());
        let duration = result.unwrap();
        assert!(duration < Duration::from_millis(1000));

        let stats = manager.get_stats().await;
        assert_eq!(stats.state, ConnectionState::Connected);
        assert_eq!(stats.successful_reconnects, 1);
    }

    #[tokio::test]
    async fn test_ws_manager_failed_reconnect() {
        let config = WsManagerConfig {
            reconnect: ReconnectConfig {
                initial_backoff_ms: 10,
                max_backoff_ms: 100,
                backoff_multiplier: 1.0,
                max_attempts: Some(3),
                jitter_factor: 0.0,
            },
            max_reconnect_time_ms: 500,
            ..Default::default()
        };
        let manager = WsManager::with_config(config);

        let result = manager.attempt_reconnect(|| async {
            Err("always fails".to_string())
        }).await;

        assert!(result.is_err());

        let stats = manager.get_stats().await;
        assert_eq!(stats.state, ConnectionState::Failed);
        assert_eq!(stats.failed_reconnects, 1);
    }

    #[tokio::test]
    async fn test_ws_manager_reconnect_timeout() {
        let config = WsManagerConfig {
            reconnect: ReconnectConfig {
                initial_backoff_ms: 100,
                max_backoff_ms: 200,
                backoff_multiplier: 1.0,
                max_attempts: Some(10),
                jitter_factor: 0.0,
            },
            max_reconnect_time_ms: 250, // Very short timeout
            ..Default::default()
        };
        let manager = WsManager::with_config(config);

        let result = manager.attempt_reconnect(|| async {
            sleep(Duration::from_millis(100)).await;
            Err("slow failure".to_string())
        }).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("exceeded"));
    }

    #[tokio::test]
    async fn test_ws_manager_latency_tracking() {
        let manager = WsManager::new();

        manager.mark_connected().await;
        manager.record_ping().await;
        sleep(Duration::from_millis(10)).await;
        manager.record_pong().await;

        let stats = manager.get_stats().await;
        assert!(stats.current_latency.is_some());
        assert!(stats.current_latency.unwrap() >= Duration::from_millis(10));
    }
}
