//! OKX Exchange Adapters
//!
//! Provides unified adapters for OKX spot and perpetual markets.
//!
//! # Available Adapters
//!
//! - [`OkxSpotAdapter`] - Spot trading
//! - [`OkxPerpsAdapter`] - Perpetual/swap futures trading
//!
//! # Authentication
//!
//! OKX uses HMAC-SHA256 signing with Base64 encoding:
//! - Requires API key, secret key, and passphrase
//! - Signature: Base64(HMAC-SHA256(timestamp + method + requestPath + body))
//! - Headers: OK-ACCESS-KEY, OK-ACCESS-SIGN, OK-ACCESS-TIMESTAMP, OK-ACCESS-PASSPHRASE
//!
//! # Rate Limits
//!
//! - Private endpoints: Limited by User ID
//! - Public endpoints: Limited by IP
//! - Order requests: 60/2s per instrument
//!
//! # API Documentation
//!
//! - REST API: <https://www.okx.com/docs-v5/en/#rest-api>
//! - WebSocket: <https://www.okx.com/docs-v5/en/#websocket-api>

pub mod account;
pub mod perps;
pub mod spot;

pub use perps::OkxPerpsAdapter;
pub use spot::OkxSpotAdapter;
