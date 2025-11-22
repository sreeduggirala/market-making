//! Kraken Exchange Adapter
//!
//! Provides a unified adapter for Kraken spot markets, combining both
//! REST API and WebSocket functionality in a single struct.
//!
//! # Available Adapters
//!
//! - [`KrakenSpotAdapter`] - Spot trading (complete implementation)
//!
//! # Module Structure
//!
//! - [`account`] - Authentication, HTTP client, and type converters
//! - [`spot`] - Spot market adapter implementation

pub mod account;
pub mod spot;

pub use spot::KrakenSpotAdapter;