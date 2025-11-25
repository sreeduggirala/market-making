//! Bitget Exchange Adapters
//!
//! Provides unified adapters for Bitget spot and perpetual markets.
//!
//! # Available Adapters
//!
//! - [`BitgetSpotAdapter`] - Spot trading
//! - [`BitgetPerpsAdapter`] - Perpetual/swap futures trading
//!
//! # Authentication
//!
//! Bitget uses HMAC-SHA256 signing with Base64 encoding:
//! - Requires API key, secret key, and passphrase
//! - Signature: Base64(HMAC-SHA256(timestamp + method + requestPath + queryString + body))
//! - Headers: ACCESS-KEY, ACCESS-SIGN, ACCESS-TIMESTAMP, ACCESS-PASSPHRASE
//!
//! # Rate Limits
//!
//! - General endpoints: 10 requests/second
//! - Market data: 20 requests/second
//! - Order endpoints: varies by endpoint
//!
//! # API Documentation
//!
//! - REST API: <https://www.bitget.com/api-doc/common/intro>
//! - Spot: <https://www.bitget.com/api-doc/spot/intro>
//! - Futures: <https://www.bitget.com/api-doc/contract/intro>

pub mod account;
pub mod perps;
pub mod spot;

pub use perps::BitgetPerpsAdapter;
pub use spot::BitgetSpotAdapter;
