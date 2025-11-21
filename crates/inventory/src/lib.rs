//! Inventory Management Module
//!
//! Tracks positions across exchanges.
//! Separates inventory concerns from trading strategy logic.

pub mod position_manager;

pub use position_manager::{PositionManager, NetPosition};

use oms::Exchange;
use std::time::{SystemTime, UNIX_EPOCH};

/// Position key for tracking positions by exchange and symbol
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PositionKey {
    pub exchange: Exchange,
    pub symbol: String,
}

impl PositionKey {
    pub fn new(exchange: Exchange, symbol: impl Into<String>) -> Self {
        Self {
            exchange,
            symbol: symbol.into(),
        }
    }
}

/// Get current Unix timestamp in milliseconds
pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// Error types for inventory operations
#[derive(Debug, thiserror::Error)]
pub enum InventoryError {
    #[error("Position not found for {exchange}:{symbol}")]
    PositionNotFound { exchange: Exchange, symbol: String },

    #[error("Position limit exceeded: {current} > {limit}")]
    PositionLimitExceeded { current: f64, limit: f64 },

    #[error("Hedging failed: {0}")]
    HedgingFailed(String),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, InventoryError>;
