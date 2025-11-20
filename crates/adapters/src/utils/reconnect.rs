//! Reconnection logic with exponential backoff for WebSocket connections
//!
//! Provides configurable reconnection strategies to handle connection failures
//! gracefully in production environments.

use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, warn};

/// Configuration for reconnection behavior
#[derive(Clone, Debug)]
pub struct ReconnectConfig {
    /// Initial backoff delay in milliseconds
    pub initial_backoff_ms: u64,

    /// Maximum backoff delay in milliseconds
    pub max_backoff_ms: u64,

    /// Backoff multiplier (typically 2.0 for exponential backoff)
    pub backoff_multiplier: f64,

    /// Maximum number of reconnection attempts (None = infinite)
    pub max_attempts: Option<u32>,

    /// Jitter factor to randomize backoff (0.0-1.0)
    pub jitter_factor: f64,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            initial_backoff_ms: 1000,      // Start at 1 second
            max_backoff_ms: 60000,         // Max 60 seconds
            backoff_multiplier: 2.0,       // Exponential backoff
            max_attempts: None,            // Infinite retries
            jitter_factor: 0.1,            // 10% jitter
        }
    }
}

impl ReconnectConfig {
    /// Creates a production configuration with reasonable defaults
    pub fn production() -> Self {
        Self::default()
    }

    /// Creates a development configuration with faster retries
    pub fn development() -> Self {
        Self {
            initial_backoff_ms: 500,
            max_backoff_ms: 10000,
            backoff_multiplier: 1.5,
            max_attempts: Some(10),
            jitter_factor: 0.2,
        }
    }

    /// Creates a configuration with no retries (fail fast)
    pub fn no_retry() -> Self {
        Self {
            initial_backoff_ms: 0,
            max_backoff_ms: 0,
            backoff_multiplier: 1.0,
            max_attempts: Some(0),
            jitter_factor: 0.0,
        }
    }
}

/// Reconnection strategy that manages backoff timing and attempt counting
pub struct ReconnectStrategy {
    config: ReconnectConfig,
    current_attempt: u32,
    current_backoff_ms: u64,
}

impl ReconnectStrategy {
    /// Creates a new reconnection strategy with the given configuration
    pub fn new(config: ReconnectConfig) -> Self {
        let initial_backoff = config.initial_backoff_ms;
        Self {
            config,
            current_attempt: 0,
            current_backoff_ms: initial_backoff,
        }
    }

    /// Resets the reconnection strategy (call after successful connection)
    pub fn reset(&mut self) {
        debug!("Reconnection strategy reset - connection successful");
        self.current_attempt = 0;
        self.current_backoff_ms = self.config.initial_backoff_ms;
    }

    /// Checks if more reconnection attempts are allowed
    pub fn can_retry(&self) -> bool {
        match self.config.max_attempts {
            Some(max) => self.current_attempt < max,
            None => true,
        }
    }

    /// Gets the current attempt number (starts from 0)
    pub fn current_attempt(&self) -> u32 {
        self.current_attempt
    }

    /// Waits for the appropriate backoff duration before the next retry
    ///
    /// This method implements exponential backoff with jitter:
    /// 1. Calculates the next backoff delay
    /// 2. Adds random jitter to prevent thundering herd
    /// 3. Sleeps for the calculated duration
    /// 4. Increments the attempt counter
    ///
    /// Returns the actual delay used in milliseconds
    pub async fn wait_before_retry(&mut self) -> u64 {
        use rand::Rng;

        // Calculate jitter
        let jitter_range = (self.current_backoff_ms as f64 * self.config.jitter_factor) as u64;
        let jitter = if jitter_range > 0 {
            rand::thread_rng().gen_range(0..jitter_range)
        } else {
            0
        };

        let actual_delay_ms = self.current_backoff_ms + jitter;

        warn!(
            "Reconnection attempt {} - waiting {}ms before retry",
            self.current_attempt + 1,
            actual_delay_ms
        );

        sleep(Duration::from_millis(actual_delay_ms)).await;

        // Update for next attempt
        self.current_attempt += 1;
        self.current_backoff_ms = ((self.current_backoff_ms as f64 * self.config.backoff_multiplier) as u64)
            .min(self.config.max_backoff_ms);

        actual_delay_ms
    }

    /// Gets the next backoff delay without actually waiting
    pub fn peek_next_delay(&self) -> u64 {
        self.current_backoff_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reconnect_config_defaults() {
        let config = ReconnectConfig::default();
        assert_eq!(config.initial_backoff_ms, 1000);
        assert_eq!(config.max_backoff_ms, 60000);
        assert_eq!(config.backoff_multiplier, 2.0);
        assert!(config.max_attempts.is_none());
    }

    #[test]
    fn test_strategy_can_retry() {
        let config = ReconnectConfig {
            max_attempts: Some(3),
            ..Default::default()
        };
        let mut strategy = ReconnectStrategy::new(config);

        assert!(strategy.can_retry());
        strategy.current_attempt = 2;
        assert!(strategy.can_retry());
        strategy.current_attempt = 3;
        assert!(!strategy.can_retry());
    }

    #[test]
    fn test_strategy_reset() {
        let config = ReconnectConfig::default();
        let mut strategy = ReconnectStrategy::new(config);

        strategy.current_attempt = 5;
        strategy.current_backoff_ms = 32000;

        strategy.reset();

        assert_eq!(strategy.current_attempt, 0);
        assert_eq!(strategy.current_backoff_ms, 1000);
    }

    #[tokio::test]
    async fn test_exponential_backoff() {
        let config = ReconnectConfig {
            initial_backoff_ms: 100,
            max_backoff_ms: 1000,
            backoff_multiplier: 2.0,
            max_attempts: Some(5),
            jitter_factor: 0.0, // No jitter for predictable testing
        };
        let mut strategy = ReconnectStrategy::new(config);

        // First delay should be ~100ms
        assert_eq!(strategy.peek_next_delay(), 100);

        // After first wait, should increase to ~200ms
        let _ = strategy.wait_before_retry().await;
        assert_eq!(strategy.peek_next_delay(), 200);

        // After second wait, should increase to ~400ms
        let _ = strategy.wait_before_retry().await;
        assert_eq!(strategy.peek_next_delay(), 400);

        // After third wait, should increase to ~800ms
        let _ = strategy.wait_before_retry().await;
        assert_eq!(strategy.peek_next_delay(), 800);

        // After fourth wait, should cap at max (1000ms)
        let _ = strategy.wait_before_retry().await;
        assert_eq!(strategy.peek_next_delay(), 1000);
    }
}
