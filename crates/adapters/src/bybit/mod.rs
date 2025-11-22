//! Bybit Exchange Adapters
//!
//! Provides unified adapters for Bybit V5 spot and linear perpetual markets.
//! Each adapter combines REST API and WebSocket functionality in a single struct.
//!
//! # Available Adapters
//!
//! - [`BybitSpotAdapter`] - Spot trading (V5 API)
//! - [`BybitPerpsAdapter`] - Linear perpetuals trading (V5 API)
//!
//! # Module Structure
//!
//! - [`account`] - Shared authentication, HTTP client, and type converters
//! - [`spot`] - Spot market adapter
//! - [`perps`] - Linear perpetuals market adapter
//!
//! # Authentication
//!
//! Bybit V5 uses HMAC-SHA256 signatures:
//! - String to sign: `timestamp + api_key + recv_window + request_params`
//! - Signature is hex-encoded (lowercase)
//! - Required headers: X-BAPI-API-KEY, X-BAPI-TIMESTAMP, X-BAPI-SIGN, X-BAPI-RECV-WINDOW
//!
//! # WebSocket Authentication
//!
//! For private WebSocket channels:
//! - Signature: HMAC-SHA256 of `GET/realtime{expires}`
//! - Auth message: `{"op": "auth", "args": ["api_key", expires, "signature"]}`
//!
//! # Key Differences from Other Exchanges
//!
//! | Feature | Bybit V5 | Kraken | MEXC |
//! |---------|----------|--------|------|
//! | Signature Algorithm | HMAC-SHA256 | HMAC-SHA512 | HMAC-SHA256 |
//! | Secret Encoding | Plain text | Base64 | Plain text |
//! | Signature Format | Hex | Base64 | Hex |
//! | Category Parameter | Required | N/A | N/A |
//!
//! # Example
//!
//! ```ignore
//! use adapters::bybit::{BybitSpotAdapter, BybitPerpsAdapter};
//! use adapters::traits::{SpotRest, PerpRest};
//!
//! // Spot trading
//! let spot = BybitSpotAdapter::new(api_key.clone(), api_secret.clone());
//! let balances = spot.get_balances().await?;
//!
//! // Linear perpetuals trading
//! let perps = BybitPerpsAdapter::new(api_key, api_secret);
//! let positions = perps.get_all_positions().await?;
//! ```
//!
//! # API Documentation
//!
//! - V5 API: <https://bybit-exchange.github.io/docs/v5/intro>
//! - Spot: <https://bybit-exchange.github.io/docs/v5/spot>
//! - Linear: <https://bybit-exchange.github.io/docs/v5/linear>

// Core modules
pub mod account;
pub mod perps;
pub mod spot;

// Re-export main adapters for convenient access
pub use perps::BybitPerpsAdapter;
pub use spot::BybitSpotAdapter;
