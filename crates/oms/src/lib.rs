//! Order Management System (OMS)
//!
//! Provides order execution, state tracking, and fill processing for trading strategies.
//! The OMS is the central hub for all order lifecycle management.

pub mod order_book;
pub mod event_processor;
pub mod order_router;
pub mod manager;
pub mod paper_trading;

#[cfg(feature = "persistence")]
pub mod persistence_handle;

pub use manager::OrderManager;
pub use order_book::OrderBook;
pub use paper_trading::{PaperTradingAdapter, PaperTradingConfig, SimulatedFill};

#[cfg(feature = "persistence")]
pub use persistence_handle::PersistenceHandle;

use adapters::traits::OrderStatus;
use std::time::{SystemTime, UNIX_EPOCH};

/// Exchange identifier
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Exchange {
    BinanceUs,
    Bybit,
    Kalshi,
    Kraken,
    Mexc,
}

impl std::fmt::Display for Exchange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Exchange::BinanceUs => write!(f, "binance_us"),
            Exchange::Bybit => write!(f, "bybit"),
            Exchange::Kalshi => write!(f, "kalshi"),
            Exchange::Kraken => write!(f, "kraken"),
            Exchange::Mexc => write!(f, "mexc"),
        }
    }
}

/// Get current Unix timestamp in milliseconds
/// Returns 0 if system time is unavailable (extremely rare edge case)
pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_else(|e| {
            tracing::error!("System time error in OMS: {}", e);
            0
        })
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
