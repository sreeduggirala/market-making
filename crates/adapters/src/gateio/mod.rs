//! Gate.io Exchange Adapters
//!
//! Provides unified adapters for Gate.io spot and perpetual markets.
//!
//! # Available Adapters
//!
//! - [`GateioSpotAdapter`] - Spot trading
//! - [`GateioPerpsAdapter`] - Perpetual/swap futures trading
//!
//! # Authentication
//!
//! Gate.io uses HMAC-SHA512 signing:
//! - Sign string: method\nurl\nquery_string\nhashed_body\ntimestamp
//! - Headers: KEY, Timestamp, SIGN
//!
//! # Rate Limits
//!
//! - Spot: 900 requests/minute per IP
//! - Futures: 300 requests/minute per IP
//!
//! # API Documentation
//!
//! - Spot REST: <https://www.gate.io/docs/developers/apiv4/en/#spot>
//! - Futures REST: <https://www.gate.io/docs/developers/apiv4/en/#futures>

pub mod account;
pub mod perps;
pub mod spot;

pub use perps::GateioPerpsAdapter;
pub use spot::GateioSpotAdapter;
