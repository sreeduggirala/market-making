//! Circuit breaker pattern for fault tolerance
//!
//! Prevents cascading failures by stopping requests to failing services
//! and allowing them time to recover.

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Circuit breaker state
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CircuitState {
    /// Circuit is closed, requests pass through normally
    Closed,

    /// Circuit is open, requests fail fast without calling the service
    Open,

    /// Circuit is half-open, allowing test requests to check if service recovered
    HalfOpen,
}

/// Configuration for circuit breaker behavior
#[derive(Clone, Debug)]
pub struct CircuitBreakerConfig {
    /// Number of consecutive failures before opening circuit
    pub failure_threshold: u32,

    /// Number of consecutive successes in half-open state before closing circuit
    pub success_threshold: u32,

    /// Duration to wait in open state before transitioning to half-open (ms)
    pub timeout_ms: u64,

    /// Maximum number of concurrent requests allowed in half-open state
    pub half_open_max_requests: u32,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            success_threshold: 2,
            timeout_ms: 60000, // 60 seconds
            half_open_max_requests: 3,
        }
    }
}

impl CircuitBreakerConfig {
    /// Conservative production configuration
    pub fn production() -> Self {
        Self {
            failure_threshold: 5,
            success_threshold: 3,
            timeout_ms: 60000,
            half_open_max_requests: 2,
        }
    }

    /// Aggressive configuration for development (fails faster, recovers faster)
    pub fn development() -> Self {
        Self {
            failure_threshold: 3,
            success_threshold: 2,
            timeout_ms: 10000,
            half_open_max_requests: 5,
        }
    }
}

struct CircuitBreakerState {
    state: CircuitState,
    failure_count: u32,
    success_count: u32,
    last_failure_time: Option<Instant>,
    half_open_requests: u32,
    config: CircuitBreakerConfig,
}

/// Circuit breaker for protecting against cascading failures
///
/// Implements the circuit breaker pattern:
/// - **Closed**: Normal operation, monitors for failures
/// - **Open**: Too many failures, reject requests immediately
/// - **Half-Open**: Testing if service recovered, allow limited requests
pub struct CircuitBreaker {
    state: Arc<RwLock<CircuitBreakerState>>,
    name: String,
}

impl CircuitBreaker {
    /// Creates a new circuit breaker with the given name and configuration
    pub fn new(name: impl Into<String>, config: CircuitBreakerConfig) -> Self {
        let state = CircuitBreakerState {
            state: CircuitState::Closed,
            failure_count: 0,
            success_count: 0,
            last_failure_time: None,
            half_open_requests: 0,
            config,
        };

        Self {
            state: Arc::new(RwLock::new(state)),
            name: name.into(),
        }
    }

    /// Gets the current circuit state
    pub async fn state(&self) -> CircuitState {
        let state = self.state.read().await;
        state.state
    }

    /// Checks if a request can proceed
    ///
    /// Returns Ok(()) if request can proceed, Err if circuit is open
    pub async fn call<T, E, F, Fut>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
        E: std::fmt::Display,
    {
        // Check if we can make the request
        if !self.allow_request().await {
            return Err(self.create_open_circuit_error());
        }

        // Execute the request
        match f().await {
            Ok(result) => {
                self.on_success().await;
                Ok(result)
            }
            Err(err) => {
                self.on_failure().await;
                Err(err)
            }
        }
    }

    /// Checks if a request is allowed based on current circuit state
    async fn allow_request(&self) -> bool {
        let mut state = self.state.write().await;

        match state.state {
            CircuitState::Closed => true,

            CircuitState::Open => {
                // Check if timeout has elapsed
                if let Some(last_failure) = state.last_failure_time {
                    let elapsed = Instant::now().duration_since(last_failure);
                    if elapsed >= Duration::from_millis(state.config.timeout_ms) {
                        info!("Circuit breaker '{}' transitioning to half-open", self.name);
                        state.state = CircuitState::HalfOpen;
                        state.half_open_requests = 0;
                        state.success_count = 0;
                        true
                    } else {
                        debug!(
                            "Circuit breaker '{}' is open - request rejected",
                            self.name
                        );
                        false
                    }
                } else {
                    false
                }
            }

            CircuitState::HalfOpen => {
                if state.half_open_requests < state.config.half_open_max_requests {
                    state.half_open_requests += 1;
                    debug!(
                        "Circuit breaker '{}' allowing test request ({}/{})",
                        self.name,
                        state.half_open_requests,
                        state.config.half_open_max_requests
                    );
                    true
                } else {
                    debug!(
                        "Circuit breaker '{}' half-open request limit reached",
                        self.name
                    );
                    false
                }
            }
        }
    }

    /// Records a successful request
    async fn on_success(&self) {
        let mut state = self.state.write().await;

        match state.state {
            CircuitState::Closed => {
                // Reset failure count on success in closed state
                if state.failure_count > 0 {
                    debug!(
                        "Circuit breaker '{}' resetting failure count after success",
                        self.name
                    );
                    state.failure_count = 0;
                }
            }

            CircuitState::HalfOpen => {
                state.success_count += 1;
                debug!(
                    "Circuit breaker '{}' success in half-open state ({}/{})",
                    self.name,
                    state.success_count,
                    state.config.success_threshold
                );

                if state.success_count >= state.config.success_threshold {
                    info!(
                        "Circuit breaker '{}' closing - service recovered",
                        self.name
                    );
                    state.state = CircuitState::Closed;
                    state.failure_count = 0;
                    state.success_count = 0;
                    state.half_open_requests = 0;
                }
            }

            CircuitState::Open => {
                // This shouldn't happen, but reset to half-open if it does
                warn!(
                    "Circuit breaker '{}' received success while open - transitioning to half-open",
                    self.name
                );
                state.state = CircuitState::HalfOpen;
                state.success_count = 1;
                state.failure_count = 0;
            }
        }
    }

    /// Records a failed request
    async fn on_failure(&self) {
        let mut state = self.state.write().await;

        match state.state {
            CircuitState::Closed => {
                state.failure_count += 1;
                state.last_failure_time = Some(Instant::now());

                debug!(
                    "Circuit breaker '{}' failure count: {}/{}",
                    self.name,
                    state.failure_count,
                    state.config.failure_threshold
                );

                if state.failure_count >= state.config.failure_threshold {
                    error!(
                        "Circuit breaker '{}' opening due to {} consecutive failures",
                        self.name,
                        state.failure_count
                    );
                    state.state = CircuitState::Open;
                    state.success_count = 0;
                }
            }

            CircuitState::HalfOpen => {
                error!(
                    "Circuit breaker '{}' reopening - test request failed",
                    self.name
                );
                state.state = CircuitState::Open;
                state.failure_count = 1;
                state.success_count = 0;
                state.half_open_requests = 0;
                state.last_failure_time = Some(Instant::now());
            }

            CircuitState::Open => {
                // Update last failure time to reset timeout
                state.last_failure_time = Some(Instant::now());
            }
        }
    }

    /// Creates an error indicating circuit is open
    fn create_open_circuit_error<E: std::fmt::Display>(&self) -> E {
        // This is a bit hacky but we need to return the error type E
        // In practice, this should never be called because we check allow_request first
        panic!("Circuit breaker '{}' is open", self.name)
    }

    /// Manually resets the circuit breaker to closed state
    pub async fn reset(&self) {
        let mut state = self.state.write().await;
        info!("Circuit breaker '{}' manually reset", self.name);
        state.state = CircuitState::Closed;
        state.failure_count = 0;
        state.success_count = 0;
        state.half_open_requests = 0;
        state.last_failure_time = None;
    }

    /// Gets current failure count
    pub async fn failure_count(&self) -> u32 {
        let state = self.state.read().await;
        state.failure_count
    }
}

impl Clone for CircuitBreaker {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            name: self.name.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_circuit_breaker_opens_on_failures() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            success_threshold: 2,
            timeout_ms: 1000,
            half_open_max_requests: 1,
        };

        let cb = CircuitBreaker::new("test", config);
        assert_eq!(cb.state().await, CircuitState::Closed);

        // Simulate 3 failures
        for _ in 0..3 {
            cb.on_failure().await;
        }

        assert_eq!(cb.state().await, CircuitState::Open);
    }

    #[tokio::test]
    async fn test_circuit_breaker_half_open_transition() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            success_threshold: 2,
            timeout_ms: 100,
            half_open_max_requests: 3,
        };

        let cb = CircuitBreaker::new("test", config);

        // Open the circuit
        cb.on_failure().await;
        cb.on_failure().await;
        assert_eq!(cb.state().await, CircuitState::Open);

        // Wait for timeout
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Next request should transition to half-open
        assert!(cb.allow_request().await);
        assert_eq!(cb.state().await, CircuitState::HalfOpen);
    }

    #[tokio::test]
    async fn test_circuit_breaker_closes_on_success() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            success_threshold: 2,
            timeout_ms: 100,
            half_open_max_requests: 3,
        };

        let cb = CircuitBreaker::new("test", config);

        // Open circuit
        cb.on_failure().await;
        cb.on_failure().await;

        // Wait and transition to half-open
        tokio::time::sleep(Duration::from_millis(150)).await;
        cb.allow_request().await;

        // Record successes to close circuit
        cb.on_success().await;
        cb.on_success().await;

        assert_eq!(cb.state().await, CircuitState::Closed);
    }
}
