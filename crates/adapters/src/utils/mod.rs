//! Utility modules for production-ready exchange adapters
//!
//! This module provides shared utilities for:
//! - Reconnection logic with exponential backoff
//! - Rate limiting for API calls
//! - Circuit breakers for fault tolerance
//! - WebSocket heartbeat monitoring

pub mod reconnect;
pub mod rate_limiter;
pub mod circuit_breaker;
pub mod heartbeat;

pub use reconnect::{ReconnectConfig, ReconnectStrategy};
pub use rate_limiter::{RateLimiter, RateLimiterConfig};
pub use circuit_breaker::{CircuitBreaker, CircuitBreakerConfig, CircuitState};
pub use heartbeat::{HeartbeatMonitor, HeartbeatConfig};
