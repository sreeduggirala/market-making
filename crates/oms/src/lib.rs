//! Order Management System (OMS)
//!
//! Provides order execution, state tracking, and fill processing for trading strategies.
//! The OMS is the central hub for all order lifecycle management.

pub mod order_book;
pub mod event_processor;
pub mod order_router;
pub mod manager;

pub use manager::OrderManager;
pub use order_book::OrderBook;

use adapters::traits::OrderStatus;
use std::time::{SystemTime, UNIX_EPOCH};

/// Exchange identifier
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Exchange {
    Kraken,
    Mexc,
}

impl std::fmt::Display for Exchange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Exchange::Kraken => write!(f, "kraken"),
            Exchange::Mexc => write!(f, "mexc"),
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

/// Error types for OMS operations
#[derive(Debug, thiserror::Error)]
pub enum OmsError {
    #[error("Order not found: {0}")]
    OrderNotFound(String),

    #[error("Exchange adapter not configured: {0}")]
    AdapterNotConfigured(Exchange),

    #[error("Order already exists: {0}")]
    DuplicateOrder(String),

    #[error("Invalid order state: expected {expected:?}, got {actual:?}")]
    InvalidOrderState {
        expected: OrderStatus,
        actual: OrderStatus,
    },

    #[error("Exchange error: {0}")]
    ExchangeError(String),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, OmsError>;
