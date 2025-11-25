//! KuCoin Exchange Adapters
//!
//! Provides unified adapters for KuCoin spot and perpetual markets.
//!
//! # Available Adapters
//!
//! - [`KucoinSpotAdapter`] - Spot trading
//! - [`KucoinPerpsAdapter`] - Perpetual/swap futures trading
//!
//! # Authentication
//!
//! KuCoin uses HMAC-SHA256 signing with Base64 encoding:
//! - Requires API key, secret key, and passphrase
//! - Signature: Base64(HMAC-SHA256(timestamp + method + requestPath + body))
//! - Headers: KC-API-KEY, KC-API-SIGN, KC-API-TIMESTAMP, KC-API-PASSPHRASE
//! - KC-API-KEY-VERSION: "2" (passphrase is hashed)
//!
//! # Rate Limits
//!
//! - Spot: 10 requests/second per IP, 200/10s per account
//! - Futures: 30 requests/3s per account
//!
//! # API Documentation
//!
//! - Spot REST: <https://www.kucoin.com/docs/rest/spot-trading/orders/place-order>
//! - Futures REST: <https://www.kucoin.com/docs/rest/futures-trading/orders/place-order>

pub mod account;
// pub mod perps;  // TODO: Fix PerpRest trait implementation
pub mod spot;

// pub use perps::KucoinPerpsAdapter;
pub use spot::KucoinSpotAdapter;
