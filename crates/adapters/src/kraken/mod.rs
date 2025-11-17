//! Kraken Exchange Adapters
//!
//! Provides unified adapters for Kraken spot and futures markets. Each adapter
//! combines REST API and WebSocket functionality in a single struct.
//!
//! # Available Adapters
//!
//! - [`KrakenSpotAdapter`] - Spot trading (complete implementation)
//! - [`KrakenPerpsAdapter`] - Futures/perpetuals trading (placeholder)
//!
//! # Module Structure
//!
//! - [`account`] - Shared authentication, HTTP client, and type converters
//! - [`spot`] - Spot market adapter implementation
//! - [`perps`] - Perpetuals/futures market adapter (partial)

// Core modules
pub mod account;
pub mod spot;
pub mod perps;

// Re-export main adapters for convenient access
pub use spot::KrakenSpotAdapter;
pub use perps::KrakenPerpsAdapter;