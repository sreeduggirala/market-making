//! Rate limiting for API requests using token bucket algorithm
//!
//! Prevents exceeding exchange API rate limits and implements backoff
//! when rate limit errors are encountered.

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{debug, warn};

/// Configuration for rate limiting behavior
#[derive(Clone, Debug)]
pub struct RateLimiterConfig {
    /// Maximum number of requests per window
    pub requests_per_window: u32,

    /// Time window in milliseconds
    pub window_ms: u64,

    /// Whether to wait when limit is reached or return immediately
    pub block_on_limit: bool,

    /// Backoff duration in milliseconds when rate limited by server
    pub rate_limit_backoff_ms: u64,
}

impl Default for RateLimiterConfig {
    fn default() -> Self {
        Self {
            requests_per_window: 20,
            window_ms: 1000,
            block_on_limit: true,
            rate_limit_backoff_ms: 5000,
        }
    }
}

impl RateLimiterConfig {
    /// Kraken Spot API limits (roughly 15-20 req/sec)
    pub fn kraken_spot() -> Self {
        Self {
            requests_per_window: 15,
            window_ms: 1000,
            block_on_limit: true,
            rate_limit_backoff_ms: 2000,
        }
    }

    /// Kraken Futures API limits
    pub fn kraken_futures() -> Self {
        Self {
            requests_per_window: 20,
            window_ms: 1000,
            block_on_limit: true,
            rate_limit_backoff_ms: 2000,
        }
    }

    /// MEXC Spot API limits (varies by endpoint, conservative default)
    pub fn mexc_spot() -> Self {
        Self {
            requests_per_window: 10,
            window_ms: 1000,
            block_on_limit: true,
            rate_limit_backoff_ms: 3000,
        }
    }

    /// MEXC Futures API limits
    pub fn mexc_futures() -> Self {
        Self {
            requests_per_window: 10,
            window_ms: 1000,
            block_on_limit: true,
            rate_limit_backoff_ms: 3000,
        }
    }
}

struct RateLimiterState {
    /// Number of tokens available
    tokens: u32,

    /// Maximum tokens (capacity)
    max_tokens: u32,

    /// Last token refill time
    last_refill: Instant,

    /// Refill rate (tokens per millisecond)
    refill_rate: f64,

    /// Configuration
    config: RateLimiterConfig,
}

/// Token bucket rate limiter for API requests
///
/// Uses a token bucket algorithm where:
/// - Each request consumes one token
/// - Tokens are refilled at a constant rate
/// - If no tokens available, request waits or fails based on config
pub struct RateLimiter {
    state: Arc<Mutex<RateLimiterState>>,
}

impl RateLimiter {
    /// Creates a new rate limiter with the given configuration
    pub fn new(config: RateLimiterConfig) -> Self {
        let refill_rate = config.requests_per_window as f64 / config.window_ms as f64;

        let state = RateLimiterState {
            tokens: config.requests_per_window,
            max_tokens: config.requests_per_window,
            last_refill: Instant::now(),
            refill_rate,
            config,
        };

        Self {
            state: Arc::new(Mutex::new(state)),
        }
    }

    /// Acquires a token for making a request
    ///
    /// If blocking is enabled and no tokens are available, this will wait
    /// until a token becomes available. Otherwise, it returns immediately.
    ///
    /// Returns true if token was acquired, false if limit reached and blocking disabled
    pub async fn acquire(&self) -> bool {
        loop {
            let mut state = self.state.lock().await;

            // Refill tokens based on elapsed time
            let now = Instant::now();
            let elapsed_ms = now.duration_since(state.last_refill).as_millis() as f64;
            let tokens_to_add = (elapsed_ms * state.refill_rate) as u32;

            if tokens_to_add > 0 {
                state.tokens = (state.tokens + tokens_to_add).min(state.max_tokens);
                state.last_refill = now;
            }

            // Try to consume a token
            if state.tokens > 0 {
                state.tokens -= 1;
                debug!(
                    "Rate limiter token acquired - {} tokens remaining",
                    state.tokens
                );
                return true;
            }

            // No tokens available
            if !state.config.block_on_limit {
                warn!("Rate limit reached - request rejected");
                return false;
            }

            // Calculate wait time for next token
            let wait_ms = (1.0 / state.refill_rate) as u64;
            drop(state); // Release lock while sleeping

            debug!("Rate limit reached - waiting {}ms for next token", wait_ms);
            sleep(Duration::from_millis(wait_ms)).await;
        }
    }

    /// Handles a rate limit error from the server
    ///
    /// Applies backoff and resets the token bucket to prevent
    /// immediate retry that would also be rate limited
    pub async fn handle_rate_limit_error(&self) {
        let mut state = self.state.lock().await;
        let backoff_ms = state.config.rate_limit_backoff_ms;

        warn!(
            "Server returned rate limit error - backing off for {}ms",
            backoff_ms
        );

        // Reset tokens to 0 to prevent immediate retries
        state.tokens = 0;
        state.last_refill = Instant::now();

        drop(state); // Release lock before sleeping
        sleep(Duration::from_millis(backoff_ms)).await;
    }

    /// Gets the current number of available tokens
    pub async fn available_tokens(&self) -> u32 {
        let state = self.state.lock().await;
        state.tokens
    }

    /// Resets the rate limiter to full capacity
    pub async fn reset(&self) {
        let mut state = self.state.lock().await;
        state.tokens = state.max_tokens;
        state.last_refill = Instant::now();
        debug!("Rate limiter reset to full capacity");
    }
}

impl Clone for RateLimiter {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_rate_limiter_basic() {
        let config = RateLimiterConfig {
            requests_per_window: 5,
            window_ms: 100,
            block_on_limit: false,
            rate_limit_backoff_ms: 1000,
        };

        let limiter = RateLimiter::new(config);

        // Should acquire 5 tokens successfully
        for _ in 0..5 {
            assert!(limiter.acquire().await);
        }

        // 6th request should fail (non-blocking)
        assert!(!limiter.acquire().await);
    }

    #[tokio::test]
    async fn test_rate_limiter_refill() {
        let config = RateLimiterConfig {
            requests_per_window: 10,
            window_ms: 100,
            block_on_limit: false,
            rate_limit_backoff_ms: 1000,
        };

        let limiter = RateLimiter::new(config);

        // Use all tokens
        for _ in 0..10 {
            assert!(limiter.acquire().await);
        }

        // Should be empty
        assert_eq!(limiter.available_tokens().await, 0);

        // Wait for refill
        sleep(Duration::from_millis(100)).await;

        // Should have tokens again
        assert!(limiter.acquire().await);
    }

    #[tokio::test]
    async fn test_rate_limiter_reset() {
        let config = RateLimiterConfig {
            requests_per_window: 5,
            window_ms: 1000,
            block_on_limit: false,
            rate_limit_backoff_ms: 1000,
        };

        let limiter = RateLimiter::new(config);

        // Use all tokens
        for _ in 0..5 {
            assert!(limiter.acquire().await);
        }

        assert_eq!(limiter.available_tokens().await, 0);

        // Reset should refill
        limiter.reset().await;
        assert_eq!(limiter.available_tokens().await, 5);
    }
}
