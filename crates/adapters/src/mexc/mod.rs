//! MEXC Exchange Adapters
//!
//! Provides unified adapters for MEXC spot and futures markets. Each adapter
//! combines REST API and WebSocket functionality in a single struct.
//!
//! # Available Adapters
//!
//! - [`MexcSpotAdapter`] - Spot trading (complete implementation)
//! - [`MexcPerpsAdapter`] - Futures/perpetuals trading (complete implementation)
//!
//! # Module Structure
//!
//! - [`account`] - Shared authentication, HTTP client, and type converters
//! - [`spot`] - Spot market adapter
//! - [`perps`] - Perpetuals/futures market adapter
//!
//! # Authentication
//!
//! MEXC uses HMAC-SHA256 signatures:
//! - API secret is plain text (not base64-encoded like Kraken)
//! - Signature is hex-encoded
//! - Timestamp parameter required in all authenticated requests
//! - Requests expire after 5 seconds
//!
//! # Key Differences from Kraken
//!
//! | Feature | MEXC | Kraken |
//! |---------|------|--------|
//! | Signature Algorithm | HMAC-SHA256 | HMAC-SHA512 |
//! | Secret Encoding | Plain text | Base64 |
//! | Signature Format | Hex | Base64 |
//! | User Stream Auth | Listen key | WebSocket token |
//! | Listen Key Expiry | 24 hours | Token-based (no expiry) |
//!
//! # Listen Key Management
//!
//! MEXC requires listen keys for private WebSocket streams:
//! - Created via REST API: POST /api/v3/userDataStream (Spot) or POST /api/v1/private/account/getListenKey (Futures)
//! - Valid for 24 hours
//! - Should be renewed every 30-60 minutes
//! - Automatically managed by adapters
//!
//! # Example
//!
//! ```ignore
//! use market_making::adapters::mexc::{MexcSpotAdapter, MexcPerpsAdapter};
//! use market_making::adapters::traits::{SpotRest, PerpRest};
//!
//! // Spot trading
//! let spot = MexcSpotAdapter::new(api_key.clone(), api_secret.clone());
//! let balance = spot.get_balance().await?;
//!
//! // Futures trading
//! let perps = MexcPerpsAdapter::new(api_key, api_secret);
//! let positions = perps.get_all_positions().await?;
//! ```
//!
//! # API Documentation
//!
//! - Spot API v3: <https://www.mexc.com/api-docs/spot-v3/introduction>
//! - Futures API: <https://www.mexc.com/api-docs/futures/update-log>

// Core modules
pub mod account;
pub mod perps;
pub mod spot;

// Re-export main adapters for convenient access
pub use perps::MexcPerpsAdapter;
pub use spot::MexcSpotAdapter;
