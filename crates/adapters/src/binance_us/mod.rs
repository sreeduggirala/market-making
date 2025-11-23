//! Binance.US Exchange Adapter
//!
//! This module provides adapters for the Binance.US cryptocurrency exchange.
//!
//! # Supported Markets
//!
//! - **Spot**: Full support via `BinanceUsSpotAdapter`
//!
//! # Features
//!
//! - REST API for order management, account queries, and market data
//! - WebSocket streams for real-time orderbook, trades, and user data
//! - Automatic listen key management for user data streams
//! - Rate limiting and circuit breaker resilience patterns
//!
//! # Example
//!
//! ```ignore
//! use adapters::binance_us::BinanceUsSpotAdapter;
//! use adapters::traits::{SpotRest, SpotWs};
//!
//! let adapter = BinanceUsSpotAdapter::new(
//!     std::env::var("BINANCE_US_API_KEY")?,
//!     std::env::var("BINANCE_US_API_SECRET")?,
//! );
//!
//! // Get account balances
//! let balances = adapter.get_balances().await?;
//!
//! // Subscribe to orderbook updates
//! let mut book_rx = adapter.subscribe_books(&["BTCUSD"]).await?;
//! ```

pub mod account;
pub mod spot;

pub use account::{BinanceUsAuth, BinanceUsRestClient, BINANCE_US_REST_URL, BINANCE_US_WS_URL};
pub use spot::BinanceUsSpotAdapter;
