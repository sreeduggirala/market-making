//! Kalshi Exchange Adapter
//!
//! Provides REST and WebSocket connectivity for Kalshi prediction markets.
//!
//! # Overview
//!
//! Kalshi is a US-regulated prediction market exchange (CFTC regulated).
//! Unlike traditional exchanges, Kalshi trades event contracts with binary outcomes.
//!
//! # Key Differences from Crypto Exchanges
//!
//! - **Pricing**: Prices are in cents (1-99), representing probability (0.01-0.99)
//! - **Contracts**: Yes/No contracts instead of base/quote pairs
//! - **Settlement**: Contracts settle at $1 (yes) or $0 (no) at expiration
//! - **Order Structure**: Orders have action (buy/sell) AND side (yes/no)
//!
//! # Mapping to SpotRest Trait
//!
//! This adapter maps Kalshi concepts to the standard spot trading interface:
//! - `symbol` = Kalshi `ticker` (e.g., "PRES-24-D-AZ")
//! - `Side::Buy` = buying contracts (action=buy)
//! - `Side::Sell` = selling contracts (action=sell)
//! - Defaults to "yes" side contracts
//! - Prices are converted from cents to decimal (50 cents = 0.50)
//!
//! # Authentication
//!
//! Kalshi uses RSA-PSS signatures with SHA256:
//! - Generate an RSA key pair
//! - Create an API key in Kalshi dashboard
//! - Sign requests with: `timestamp + method + path`
//!
//! # Example
//!
//! ```rust,ignore
//! use adapters::kalshi::KalshiSpotAdapter;
//! use adapters::traits::SpotRest;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let private_key = std::fs::read_to_string("kalshi_private_key.pem")?;
//!     let adapter = KalshiSpotAdapter::new(
//!         "your-api-key-id".to_string(),
//!         &private_key,
//!     )?;
//!
//!     // Get available markets
//!     let markets = adapter.get_all_markets().await?;
//!     println!("Found {} markets", markets.len());
//!
//!     // Get ticker for a specific market
//!     let ticker = adapter.get_ticker("PRES-24-D-AZ").await?;
//!     println!("Yes bid: {}, Yes ask: {}", ticker.bid_price, ticker.ask_price);
//!
//!     Ok(())
//! }
//! ```
//!
//! # WebSocket Channels
//!
//! - `fill` - User fill notifications (requires auth)
//! - `orderbook_delta` - Order book updates
//! - `trade` - Public trade events
//!
//! # API Documentation
//!
//! - REST API: <https://docs.kalshi.com/api-reference>
//! - WebSocket: <https://docs.kalshi.com/websockets>
//! - Getting Started: <https://docs.kalshi.com/getting_started>

pub mod account;
pub mod spot;

pub use account::{
    KalshiAuth, KalshiRestClient, KALSHI_REST_URL, KALSHI_REST_URL_DEMO, KALSHI_WS_URL,
    KALSHI_WS_URL_DEMO,
};
pub use spot::KalshiSpotAdapter;
