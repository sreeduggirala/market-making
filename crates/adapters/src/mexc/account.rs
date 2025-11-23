//! MEXC Exchange Account Management and Authentication
//!
//! This module provides shared authentication, HTTP client functionality, and type converters
//! for both MEXC Spot and Futures markets.
//!
//! # Authentication
//!
//! MEXC uses HMAC-SHA256 signatures for request authentication:
//! 1. Create query string from parameters sorted alphabetically
//! 2. Append timestamp parameter
//! 3. Generate HMAC-SHA256 signature using API secret
//! 4. Append signature to query string
//!
//! # API Documentation
//!
//! - Spot API v3: <https://www.mexc.com/api-docs/spot-v3/introduction>
//! - Futures API: <https://www.mexc.com/api-docs/futures/update-log>

use anyhow::{Context, Result};
use reqwest::Client;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

// ============================================================================
// API Endpoint Constants
// ============================================================================

/// MEXC Spot REST API base URL
///
/// All spot market REST API calls use this as the base URL.
/// Example: `https://api.mexc.com/api/v3/order`
pub const MEXC_SPOT_REST_URL: &str = "https://api.mexc.com";

/// MEXC Spot WebSocket public stream URL
///
/// Used for public market data streams (orderbook, trades, ticker).
/// Does not require authentication.
pub const MEXC_SPOT_WS_URL: &str = "wss://wbs.mexc.com/ws";

/// MEXC Spot WebSocket private stream URL
///
/// Used for private user data streams (order updates, balance changes).
/// Requires listen key obtained via REST API.
pub const MEXC_SPOT_WS_PRIVATE_URL: &str = "wss://wbs.mexc.com/ws";

/// MEXC Futures REST API base URL
///
/// All futures/perpetuals market REST API calls use this as the base URL.
/// Example: `https://contract.mexc.com/api/v1/private/order/submit`
pub const MEXC_FUTURES_REST_URL: &str = "https://contract.mexc.com";

/// MEXC Futures WebSocket URL
///
/// Used for both public and private futures market data streams.
/// Authentication handled via request signing.
pub const MEXC_FUTURES_WS_URL: &str = "wss://contract.mexc.com/ws";

// ============================================================================
// Authentication
// ============================================================================

/// MEXC API authentication credentials
///
/// Stores API key and secret for signing requests. The API secret is used directly
/// (not base64-encoded like Kraken) to generate HMAC-SHA256 signatures for
/// authenticated REST API calls and WebSocket connections.
///
/// # Security Notes
///
/// - API keys should be stored securely (e.g., environment variables, secrets manager)
/// - Never commit API keys to version control
/// - Use IP whitelisting on MEXC's platform
/// - The secret is used directly as bytes for HMAC-SHA256 (no base64 decoding)
///
/// # Differences from Kraken
///
/// - Uses HMAC-SHA256 instead of HMAC-SHA512
/// - Secret is plain text, not base64-encoded
/// - Simpler signature scheme (no path concatenation)
#[derive(Clone)]
pub struct MexcAuth {
    /// API key string (public identifier)
    pub api_key: String,

    /// API secret in plain text format (private signing key)
    pub api_secret: String,
}

impl MexcAuth {
    /// Creates a new MexcAuth instance with the provided credentials
    ///
    /// # Arguments
    ///
    /// * `api_key` - Public API key obtained from MEXC account settings
    /// * `api_secret` - Plain text API secret (not base64-encoded)
    ///
    /// # Example
    ///
    /// ```ignore
    /// let auth = MexcAuth::new(
    ///     "YOUR_API_KEY".to_string(),
    ///     "YOUR_API_SECRET".to_string()
    /// );
    /// ```
    pub fn new(api_key: String, api_secret: String) -> Self {
        Self { api_key, api_secret }
    }

    /// Generates HMAC-SHA256 signature for MEXC REST API authenticated requests
    ///
    /// MEXC's authentication scheme:
    /// 1. Create query string from parameters (sorted alphabetically)
    /// 2. Generate HMAC-SHA256 of query string using API secret
    /// 3. Hex-encode the signature
    /// 4. Append signature to query string
    ///
    /// This signature is sent as a query parameter `signature` along with `X-MEXC-APIKEY` header.
    ///
    /// # Arguments
    ///
    /// * `query_string` - URL-encoded query parameters (without signature)
    ///
    /// # Returns
    ///
    /// Hex-encoded HMAC-SHA256 signature string
    ///
    /// # Example
    ///
    /// ```ignore
    /// let timestamp = MexcAuth::get_timestamp();
    /// let query = format!("symbol=BTCUSDT&timestamp={}", timestamp);
    /// let signature = auth.sign(&query);
    /// let full_query = format!("{}&signature={}", query, signature);
    /// ```
    pub fn sign(&self, query_string: &str) -> String {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

        // HMAC-SHA256 accepts keys of any size, this should never fail
        let mut mac = match Hmac::<Sha256>::new_from_slice(self.api_secret.as_bytes()) {
            Ok(m) => m,
            Err(e) => {
                tracing::error!("HMAC initialization failed: {}", e);
                return String::new();
            }
        };
        mac.update(query_string.as_bytes());
        let result = mac.finalize();
        hex::encode(result.into_bytes())
    }

    /// Generates a timestamp for request authentication
    ///
    /// Returns the current Unix timestamp in milliseconds. MEXC requires timestamps
    /// in API requests for replay attack prevention. Requests with timestamps older
    /// than 5 seconds may be rejected.
    ///
    /// # Returns
    ///
    /// Current timestamp as milliseconds since Unix epoch (January 1, 1970)
    ///
    /// # Note
    ///
    /// Ensure system clock is synchronized with NTP to avoid timestamp rejection.
    pub fn get_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or_else(|e| {
                tracing::error!("System time error: {}", e);
                0
            })
    }
}

// ============================================================================
// HTTP Client
// ============================================================================

/// HTTP client wrapper for MEXC REST API
///
/// Provides methods for making authenticated and public HTTP requests to MEXC's
/// REST API. Handles request signing, header management, and response parsing.
///
/// # Thread Safety
///
/// This struct is thread-safe and can be cloned cheaply (reqwest::Client uses Arc internally).
#[derive(Clone)]
pub struct MexcRestClient {
    /// Underlying HTTP client (reuses connections via connection pooling)
    client: Client,

    /// Optional authentication credentials for private endpoints
    auth: Option<MexcAuth>,

    /// Base URL for API requests (spot or futures)
    base_url: String,
}

impl MexcRestClient {
    /// Creates a production-ready HTTP client with proper connection pooling and timeouts
    fn build_client() -> Client {
        use std::time::Duration;

        Client::builder()
            .pool_max_idle_per_host(10)
            .pool_idle_timeout(Duration::from_secs(90))
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .tcp_keepalive(Duration::from_secs(60))
            .build()
            .unwrap_or_else(|e| {
                tracing::error!("Failed to build HTTP client, using default: {}", e);
                Client::new()
            })
    }

    /// Creates a new HTTP client configured for MEXC Spot REST API
    ///
    /// # Arguments
    ///
    /// * `auth` - Optional authentication credentials. Pass `None` for public endpoints only.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Public access only
    /// let client = MexcRestClient::new_spot(None);
    ///
    /// // With authentication
    /// let auth = MexcAuth::new(api_key, api_secret);
    /// let client = MexcRestClient::new_spot(Some(auth));
    /// ```
    pub fn new_spot(auth: Option<MexcAuth>) -> Self {
        Self {
            client: Self::build_client(),
            auth,
            base_url: MEXC_SPOT_REST_URL.to_string(),
        }
    }

    /// Creates a new HTTP client configured for MEXC Futures REST API
    ///
    /// # Arguments
    ///
    /// * `auth` - Optional authentication credentials. Pass `None` for public endpoints only.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let auth = MexcAuth::new(api_key, api_secret);
    /// let client = MexcRestClient::new_futures(Some(auth));
    /// ```
    pub fn new_futures(auth: Option<MexcAuth>) -> Self {
        Self {
            client: Self::build_client(),
            auth,
            base_url: MEXC_FUTURES_REST_URL.to_string(),
        }
    }

    /// Makes an unauthenticated GET request to a public endpoint
    ///
    /// Used for public market data like ticker, orderbook, recent trades, etc.
    /// Does not require API credentials.
    ///
    /// # Type Parameters
    ///
    /// * `T` - Response type that implements `Deserialize`
    ///
    /// # Arguments
    ///
    /// * `endpoint` - API endpoint path (e.g., "/api/v3/ticker/24hr")
    /// * `params` - Optional query parameters as key-value pairs
    ///
    /// # Returns
    ///
    /// Deserialized response of type `T`
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Network request fails
    /// - HTTP status code indicates failure
    /// - Response body cannot be parsed as type `T`
    ///
    /// # Example
    ///
    /// ```ignore
    /// let params = HashMap::from([
    ///     ("symbol".to_string(), "BTCUSDT".to_string())
    /// ]);
    /// let ticker: TickerResponse = client.get_public("/api/v3/ticker/24hr", Some(params)).await?;
    /// ```
    pub async fn get_public<T: for<'de> serde::Deserialize<'de>>(
        &self,
        endpoint: &str,
        params: Option<HashMap<String, String>>,
    ) -> Result<T> {
        let url = format!("{}{}", self.base_url, endpoint);
        let mut request = self.client.get(&url);

        if let Some(params) = params {
            request = request.query(&params);
        }

        let response = request.send().await.context("Failed to send request")?;
        let status = response.status();
        let body = response.text().await.context("Failed to read response")?;

        if !status.is_success() {
            anyhow::bail!("API error ({}): {}", status, body);
        }

        serde_json::from_str(&body).context("Failed to parse response")
    }

    /// Makes an authenticated GET request to a private endpoint
    ///
    /// Used for account-specific queries. Automatically adds timestamp and signature.
    ///
    /// # Type Parameters
    ///
    /// * `T` - Response type that implements `Deserialize`
    ///
    /// # Arguments
    ///
    /// * `endpoint` - API endpoint path (e.g., "/api/v3/account")
    /// * `params` - Request parameters as key-value pairs (timestamp and signature added automatically)
    ///
    /// # Returns
    ///
    /// Deserialized response of type `T`
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - No authentication credentials configured
    /// - Network request fails
    /// - HTTP status code indicates failure
    /// - Response body cannot be parsed as type `T`
    pub async fn get_private<T: for<'de> serde::Deserialize<'de>>(
        &self,
        endpoint: &str,
        mut params: HashMap<String, String>,
    ) -> Result<T> {
        let auth = self.auth.as_ref()
            .context("Authentication required for private endpoints")?;

        // Add timestamp
        let timestamp = MexcAuth::get_timestamp();
        params.insert("timestamp".to_string(), timestamp.to_string());

        // Create sorted query string for signing
        let mut sorted_params: Vec<_> = params.iter().collect();
        sorted_params.sort_by_key(|(k, _)| *k);
        let query_string = sorted_params
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join("&");

        // Generate signature
        let signature = auth.sign(&query_string);
        params.insert("signature".to_string(), signature);

        let url = format!("{}{}", self.base_url, endpoint);
        let response = self.client
            .get(&url)
            .header("X-MEXC-APIKEY", &auth.api_key)
            .query(&params)
            .send()
            .await
            .context("Failed to send request")?;

        let status = response.status();
        let body = response.text().await.context("Failed to read response")?;

        if !status.is_success() {
            anyhow::bail!("API error ({}): {}", status, body);
        }

        serde_json::from_str(&body).context("Failed to parse response")
    }

    /// Makes an authenticated POST request to a private endpoint
    ///
    /// Used for order placement, cancellation, and other trading operations.
    /// Automatically adds timestamp and signature.
    ///
    /// # Type Parameters
    ///
    /// * `T` - Response type that implements `Deserialize`
    ///
    /// # Arguments
    ///
    /// * `endpoint` - API endpoint path (e.g., "/api/v3/order")
    /// * `params` - Request parameters as key-value pairs (timestamp and signature added automatically)
    ///
    /// # Returns
    ///
    /// Deserialized response of type `T`
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - No authentication credentials configured
    /// - Network request fails
    /// - HTTP status code indicates failure
    /// - Response body cannot be parsed as type `T`
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut params = HashMap::new();
    /// params.insert("symbol".to_string(), "BTCUSDT".to_string());
    /// params.insert("side".to_string(), "BUY".to_string());
    /// params.insert("type".to_string(), "LIMIT".to_string());
    /// params.insert("quantity".to_string(), "0.001".to_string());
    /// params.insert("price".to_string(), "50000".to_string());
    ///
    /// let result: OrderResponse = client.post_private("/api/v3/order", params).await?;
    /// ```
    pub async fn post_private<T: for<'de> serde::Deserialize<'de>>(
        &self,
        endpoint: &str,
        mut params: HashMap<String, String>,
    ) -> Result<T> {
        let auth = self.auth.as_ref()
            .context("Authentication required for private endpoints")?;

        // Add timestamp
        let timestamp = MexcAuth::get_timestamp();
        params.insert("timestamp".to_string(), timestamp.to_string());

        // Create sorted query string for signing
        let mut sorted_params: Vec<_> = params.iter().collect();
        sorted_params.sort_by_key(|(k, _)| *k);
        let query_string = sorted_params
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join("&");

        // Generate signature
        let signature = auth.sign(&query_string);
        params.insert("signature".to_string(), signature);

        let url = format!("{}{}", self.base_url, endpoint);
        let response = self.client
            .post(&url)
            .header("X-MEXC-APIKEY", &auth.api_key)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .form(&params)
            .send()
            .await
            .context("Failed to send request")?;

        let status = response.status();
        let body = response.text().await.context("Failed to read response")?;

        if !status.is_success() {
            anyhow::bail!("API error ({}): {}", status, body);
        }

        serde_json::from_str(&body).context("Failed to parse response")
    }

    /// Makes an authenticated DELETE request to a private endpoint
    ///
    /// Used for order cancellation and deletion operations.
    /// Automatically adds timestamp and signature.
    ///
    /// # Type Parameters
    ///
    /// * `T` - Response type that implements `Deserialize`
    ///
    /// # Arguments
    ///
    /// * `endpoint` - API endpoint path (e.g., "/api/v3/order")
    /// * `params` - Request parameters as key-value pairs (timestamp and signature added automatically)
    ///
    /// # Returns
    ///
    /// Deserialized response of type `T`
    pub async fn delete_private<T: for<'de> serde::Deserialize<'de>>(
        &self,
        endpoint: &str,
        mut params: HashMap<String, String>,
    ) -> Result<T> {
        let auth = self.auth.as_ref()
            .context("Authentication required for private endpoints")?;

        // Add timestamp
        let timestamp = MexcAuth::get_timestamp();
        params.insert("timestamp".to_string(), timestamp.to_string());

        // Create sorted query string for signing
        let mut sorted_params: Vec<_> = params.iter().collect();
        sorted_params.sort_by_key(|(k, _)| *k);
        let query_string = sorted_params
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join("&");

        // Generate signature
        let signature = auth.sign(&query_string);
        params.insert("signature".to_string(), signature);

        let url = format!("{}{}", self.base_url, endpoint);
        let response = self.client
            .delete(&url)
            .header("X-MEXC-APIKEY", &auth.api_key)
            .query(&params)
            .send()
            .await
            .context("Failed to send request")?;

        let status = response.status();
        let body = response.text().await.context("Failed to read response")?;

        if !status.is_success() {
            anyhow::bail!("API error ({}): {}", status, body);
        }

        serde_json::from_str(&body).context("Failed to parse response")
    }
}

// ============================================================================
// Type Converters
// ============================================================================

/// Utility functions for converting between MEXC API types and internal trait types
///
/// MEXC uses uppercase string enums for order types, sides, and time-in-force values.
/// These functions handle the bidirectional conversion between MEXC's format and
/// our strongly-typed internal enums.
pub mod converters {
    use crate::traits::{OrderType, Side, TimeInForce, OrderStatus, MarketStatus, KlineInterval};

    /// Converts internal OrderType enum to MEXC API string format
    ///
    /// # Arguments
    ///
    /// * `order_type` - Internal order type enum value
    ///
    /// # Returns
    ///
    /// MEXC API order type string (uppercase)
    ///
    /// # Mapping
    ///
    /// - `Limit` → "LIMIT"
    /// - `Market` → "MARKET"
    /// - `StopLoss` → "STOP_LOSS"
    /// - `StopLossLimit` → "STOP_LOSS_LIMIT"
    /// - `TakeProfit` → "TAKE_PROFIT"
    /// - `TakeProfitLimit` → "TAKE_PROFIT_LIMIT"
    pub fn to_mexc_order_type(order_type: OrderType) -> String {
        match order_type {
            OrderType::Limit => "LIMIT".to_string(),
            OrderType::Market => "MARKET".to_string(),
            OrderType::StopLoss => "STOP_LOSS".to_string(),
            OrderType::StopLossLimit => "STOP_LOSS_LIMIT".to_string(),
            OrderType::TakeProfit => "TAKE_PROFIT".to_string(),
            OrderType::TakeProfitLimit => "TAKE_PROFIT_LIMIT".to_string(),
        }
    }

    /// Converts MEXC API order type string to internal OrderType enum
    ///
    /// # Arguments
    ///
    /// * `order_type` - MEXC API order type string
    ///
    /// # Returns
    ///
    /// Internal OrderType enum value
    ///
    /// # Default Behavior
    ///
    /// Returns `OrderType::Limit` for unrecognized strings to avoid panics
    pub fn from_mexc_order_type(order_type: &str) -> OrderType {
        match order_type {
            "LIMIT" => OrderType::Limit,
            "MARKET" => OrderType::Market,
            "STOP_LOSS" => OrderType::StopLoss,
            "STOP_LOSS_LIMIT" => OrderType::StopLossLimit,
            "TAKE_PROFIT" => OrderType::TakeProfit,
            "TAKE_PROFIT_LIMIT" => OrderType::TakeProfitLimit,
            _ => OrderType::Limit, // Default fallback
        }
    }

    /// Converts internal Side enum to MEXC API string format
    ///
    /// # Arguments
    ///
    /// * `side` - Internal side enum value (Buy or Sell)
    ///
    /// # Returns
    ///
    /// MEXC API side string ("BUY" or "SELL")
    pub fn to_mexc_side(side: Side) -> String {
        match side {
            Side::Buy => "BUY".to_string(),
            Side::Sell => "SELL".to_string(),
        }
    }

    /// Converts MEXC API side string to internal Side enum
    ///
    /// # Arguments
    ///
    /// * `side` - MEXC API side string (case-insensitive)
    ///
    /// # Returns
    ///
    /// Internal Side enum value
    ///
    /// # Default Behavior
    ///
    /// Returns `Side::Buy` for unrecognized strings to avoid panics
    pub fn from_mexc_side(side: &str) -> Side {
        match side.to_uppercase().as_str() {
            "BUY" => Side::Buy,
            "SELL" => Side::Sell,
            _ => Side::Buy, // Default fallback
        }
    }

    /// Converts internal TimeInForce enum to MEXC API string format
    ///
    /// # Arguments
    ///
    /// * `tif` - Internal time-in-force enum value
    ///
    /// # Returns
    ///
    /// MEXC API time-in-force string
    ///
    /// # Time-In-Force Types
    ///
    /// - `GTC` (Good-Til-Canceled) - Order remains active until filled or canceled
    /// - `IOC` (Immediate-Or-Cancel) - Fill immediately or cancel unfilled portion
    /// - `FOK` (Fill-Or-Kill) - Fill entire order immediately or cancel completely
    pub fn to_mexc_tif(tif: TimeInForce) -> String {
        match tif {
            TimeInForce::Gtc => "GTC".to_string(),
            TimeInForce::Ioc => "IOC".to_string(),
            TimeInForce::Fok => "FOK".to_string(),
        }
    }

    /// Converts MEXC API time-in-force string to internal TimeInForce enum
    ///
    /// # Arguments
    ///
    /// * `tif` - MEXC API time-in-force string
    ///
    /// # Returns
    ///
    /// Internal TimeInForce enum value
    ///
    /// # Default Behavior
    ///
    /// Returns `TimeInForce::Gtc` for unrecognized strings to avoid panics
    pub fn from_mexc_tif(tif: &str) -> TimeInForce {
        match tif {
            "GTC" => TimeInForce::Gtc,
            "IOC" => TimeInForce::Ioc,
            "FOK" => TimeInForce::Fok,
            _ => TimeInForce::Gtc, // Default fallback
        }
    }

    /// Converts MEXC API order status string to internal OrderStatus enum
    ///
    /// # Arguments
    ///
    /// * `status` - MEXC API order status string
    ///
    /// # Returns
    ///
    /// Internal OrderStatus enum value
    ///
    /// # Status Mapping
    ///
    /// - "NEW" → OrderStatus::New
    /// - "PARTIALLY_FILLED" → OrderStatus::PartiallyFilled
    /// - "FILLED" → OrderStatus::Filled
    /// - "CANCELED" → OrderStatus::Canceled
    /// - "REJECTED" → OrderStatus::Rejected
    /// - "EXPIRED" → OrderStatus::Expired
    ///
    /// # Default Behavior
    ///
    /// Returns `OrderStatus::New` for unrecognized strings to avoid panics
    pub fn from_mexc_order_status(status: &str) -> OrderStatus {
        match status.to_uppercase().as_str() {
            "NEW" => OrderStatus::New,
            "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
            "FILLED" => OrderStatus::Filled,
            "CANCELED" | "CANCELLED" => OrderStatus::Canceled,
            "REJECTED" => OrderStatus::Rejected,
            "EXPIRED" => OrderStatus::Expired,
            _ => OrderStatus::New, // Default fallback
        }
    }

    /// Converts MEXC API symbol status string to internal MarketStatus enum
    ///
    /// # Arguments
    ///
    /// * `status` - MEXC API symbol status string
    ///
    /// # Returns
    ///
    /// Internal MarketStatus enum value
    ///
    /// # Status Mapping
    ///
    /// - "TRADING" → MarketStatus::Trading
    /// - "HALT" → MarketStatus::Halt
    /// - "PRE_TRADING" → MarketStatus::PreTrading
    /// - "POST_TRADING" → MarketStatus::PostTrading
    /// - "DELISTED" → MarketStatus::Delisted
    ///
    /// # Default Behavior
    ///
    /// Returns `MarketStatus::Halt` for unrecognized strings to avoid panics
    pub fn from_mexc_symbol_status(status: &str) -> MarketStatus {
        match status.to_uppercase().as_str() {
            "TRADING" => MarketStatus::Trading,
            "HALT" | "HALTED" => MarketStatus::Halt,
            "PRE_TRADING" => MarketStatus::PreTrading,
            "POST_TRADING" => MarketStatus::PostTrading,
            "DELISTED" => MarketStatus::Delisted,
            _ => MarketStatus::Halt, // Default fallback (safe default)
        }
    }

    /// Converts internal KlineInterval enum to MEXC API string format
    ///
    /// # Arguments
    ///
    /// * `interval` - Internal kline interval enum value
    ///
    /// # Returns
    ///
    /// MEXC API interval string
    ///
    /// # Interval Mapping
    ///
    /// - `M1` → "1m" (1 minute)
    /// - `M5` → "5m" (5 minutes)
    /// - `M15` → "15m" (15 minutes)
    /// - `M30` → "30m" (30 minutes)
    /// - `H1` → "1h" (1 hour)
    /// - `H4` → "4h" (4 hours)
    /// - `D1` → "1d" (1 day)
    pub fn to_mexc_interval(interval: KlineInterval) -> String {
        match interval {
            KlineInterval::M1 => "1m".to_string(),
            KlineInterval::M5 => "5m".to_string(),
            KlineInterval::M15 => "15m".to_string(),
            KlineInterval::M30 => "30m".to_string(),
            KlineInterval::H1 => "1h".to_string(),
            KlineInterval::H4 => "4h".to_string(),
            KlineInterval::D1 => "1d".to_string(),
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::{OrderStatus, OrderType, Side, TimeInForce, MarketStatus, KlineInterval};

    // -------------------------------------------------------------------------
    // Authentication Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_auth_new() {
        let auth = MexcAuth::new("api_key".to_string(), "api_secret".to_string());
        assert_eq!(auth.api_key, "api_key");
        assert_eq!(auth.api_secret, "api_secret");
    }

    #[test]
    fn test_auth_sign_produces_hex_signature() {
        let auth = MexcAuth::new("api_key".to_string(), "secret123".to_string());
        let signature = auth.sign("symbol=BTCUSDT&side=BUY&type=LIMIT");

        // Signature should be hex-encoded (only hex characters)
        assert!(!signature.is_empty());
        assert!(signature.chars().all(|c| c.is_ascii_hexdigit()));
        // SHA256 produces 32 bytes = 64 hex chars
        assert_eq!(signature.len(), 64);
    }

    #[test]
    fn test_auth_sign_deterministic() {
        let auth = MexcAuth::new("api_key".to_string(), "secret123".to_string());
        let query = "symbol=BTCUSDT&timestamp=1234567890";
        let sig1 = auth.sign(query);
        let sig2 = auth.sign(query);
        assert_eq!(sig1, sig2, "same input should produce same signature");
    }

    #[test]
    fn test_auth_sign_different_secrets_produce_different_signatures() {
        let auth1 = MexcAuth::new("api_key".to_string(), "secret1".to_string());
        let auth2 = MexcAuth::new("api_key".to_string(), "secret2".to_string());
        let query = "symbol=BTCUSDT";
        let sig1 = auth1.sign(query);
        let sig2 = auth2.sign(query);
        assert_ne!(sig1, sig2, "different secrets should produce different signatures");
    }

    #[test]
    fn test_auth_get_timestamp_is_reasonable() {
        let ts = MexcAuth::get_timestamp();
        let min_ts: u64 = 1704067200000; // 2024-01-01
        let max_ts: u64 = 4102444800000; // 2100-01-01
        assert!(ts > min_ts, "timestamp {} is too old", ts);
        assert!(ts < max_ts, "timestamp {} is too far in future", ts);
    }

    // -------------------------------------------------------------------------
    // Order Type Converter Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_to_mexc_order_type() {
        assert_eq!(converters::to_mexc_order_type(OrderType::Limit), "LIMIT");
        assert_eq!(converters::to_mexc_order_type(OrderType::Market), "MARKET");
        assert_eq!(converters::to_mexc_order_type(OrderType::StopLoss), "STOP_LOSS");
        assert_eq!(converters::to_mexc_order_type(OrderType::StopLossLimit), "STOP_LOSS_LIMIT");
        assert_eq!(converters::to_mexc_order_type(OrderType::TakeProfit), "TAKE_PROFIT");
        assert_eq!(converters::to_mexc_order_type(OrderType::TakeProfitLimit), "TAKE_PROFIT_LIMIT");
    }

    #[test]
    fn test_from_mexc_order_type() {
        assert!(matches!(converters::from_mexc_order_type("LIMIT"), OrderType::Limit));
        assert!(matches!(converters::from_mexc_order_type("MARKET"), OrderType::Market));
        assert!(matches!(converters::from_mexc_order_type("STOP_LOSS"), OrderType::StopLoss));
        assert!(matches!(converters::from_mexc_order_type("STOP_LOSS_LIMIT"), OrderType::StopLossLimit));
        assert!(matches!(converters::from_mexc_order_type("TAKE_PROFIT"), OrderType::TakeProfit));
        assert!(matches!(converters::from_mexc_order_type("TAKE_PROFIT_LIMIT"), OrderType::TakeProfitLimit));
    }

    #[test]
    fn test_from_mexc_order_type_unknown_defaults_to_limit() {
        assert!(matches!(converters::from_mexc_order_type("UNKNOWN"), OrderType::Limit));
        assert!(matches!(converters::from_mexc_order_type(""), OrderType::Limit));
    }

    // -------------------------------------------------------------------------
    // Side Converter Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_to_mexc_side() {
        assert_eq!(converters::to_mexc_side(Side::Buy), "BUY");
        assert_eq!(converters::to_mexc_side(Side::Sell), "SELL");
    }

    #[test]
    fn test_from_mexc_side() {
        assert!(matches!(converters::from_mexc_side("BUY"), Side::Buy));
        assert!(matches!(converters::from_mexc_side("SELL"), Side::Sell));
        // Case insensitive
        assert!(matches!(converters::from_mexc_side("buy"), Side::Buy));
        assert!(matches!(converters::from_mexc_side("sell"), Side::Sell));
    }

    #[test]
    fn test_from_mexc_side_unknown_defaults_to_buy() {
        assert!(matches!(converters::from_mexc_side("UNKNOWN"), Side::Buy));
    }

    // -------------------------------------------------------------------------
    // Time-in-Force Converter Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_to_mexc_tif() {
        assert_eq!(converters::to_mexc_tif(TimeInForce::Gtc), "GTC");
        assert_eq!(converters::to_mexc_tif(TimeInForce::Ioc), "IOC");
        assert_eq!(converters::to_mexc_tif(TimeInForce::Fok), "FOK");
    }

    #[test]
    fn test_from_mexc_tif() {
        assert!(matches!(converters::from_mexc_tif("GTC"), TimeInForce::Gtc));
        assert!(matches!(converters::from_mexc_tif("IOC"), TimeInForce::Ioc));
        assert!(matches!(converters::from_mexc_tif("FOK"), TimeInForce::Fok));
    }

    #[test]
    fn test_from_mexc_tif_unknown_defaults_to_gtc() {
        assert!(matches!(converters::from_mexc_tif("UNKNOWN"), TimeInForce::Gtc));
    }

    // -------------------------------------------------------------------------
    // Order Status Converter Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_from_mexc_order_status() {
        assert!(matches!(converters::from_mexc_order_status("NEW"), OrderStatus::New));
        assert!(matches!(converters::from_mexc_order_status("PARTIALLY_FILLED"), OrderStatus::PartiallyFilled));
        assert!(matches!(converters::from_mexc_order_status("FILLED"), OrderStatus::Filled));
        assert!(matches!(converters::from_mexc_order_status("CANCELED"), OrderStatus::Canceled));
        assert!(matches!(converters::from_mexc_order_status("CANCELLED"), OrderStatus::Canceled));
        assert!(matches!(converters::from_mexc_order_status("REJECTED"), OrderStatus::Rejected));
        assert!(matches!(converters::from_mexc_order_status("EXPIRED"), OrderStatus::Expired));
    }

    #[test]
    fn test_from_mexc_order_status_case_insensitive() {
        assert!(matches!(converters::from_mexc_order_status("new"), OrderStatus::New));
        assert!(matches!(converters::from_mexc_order_status("filled"), OrderStatus::Filled));
    }

    #[test]
    fn test_from_mexc_order_status_unknown_defaults_to_new() {
        assert!(matches!(converters::from_mexc_order_status("UNKNOWN"), OrderStatus::New));
    }

    // -------------------------------------------------------------------------
    // Market Status Converter Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_from_mexc_symbol_status() {
        assert!(matches!(converters::from_mexc_symbol_status("TRADING"), MarketStatus::Trading));
        assert!(matches!(converters::from_mexc_symbol_status("HALT"), MarketStatus::Halt));
        assert!(matches!(converters::from_mexc_symbol_status("HALTED"), MarketStatus::Halt));
        assert!(matches!(converters::from_mexc_symbol_status("PRE_TRADING"), MarketStatus::PreTrading));
        assert!(matches!(converters::from_mexc_symbol_status("POST_TRADING"), MarketStatus::PostTrading));
        assert!(matches!(converters::from_mexc_symbol_status("DELISTED"), MarketStatus::Delisted));
    }

    #[test]
    fn test_from_mexc_symbol_status_unknown_defaults_to_halt() {
        assert!(matches!(converters::from_mexc_symbol_status("UNKNOWN"), MarketStatus::Halt));
    }

    // -------------------------------------------------------------------------
    // Kline Interval Converter Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_to_mexc_interval() {
        assert_eq!(converters::to_mexc_interval(KlineInterval::M1), "1m");
        assert_eq!(converters::to_mexc_interval(KlineInterval::M5), "5m");
        assert_eq!(converters::to_mexc_interval(KlineInterval::M15), "15m");
        assert_eq!(converters::to_mexc_interval(KlineInterval::M30), "30m");
        assert_eq!(converters::to_mexc_interval(KlineInterval::H1), "1h");
        assert_eq!(converters::to_mexc_interval(KlineInterval::H4), "4h");
        assert_eq!(converters::to_mexc_interval(KlineInterval::D1), "1d");
    }

    // -------------------------------------------------------------------------
    // REST Client Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_rest_client_new_spot() {
        let client = MexcRestClient::new_spot(None);
        assert_eq!(client.base_url, MEXC_SPOT_REST_URL);
    }

    #[test]
    fn test_rest_client_new_futures() {
        let client = MexcRestClient::new_futures(None);
        assert_eq!(client.base_url, MEXC_FUTURES_REST_URL);
    }

    #[test]
    fn test_rest_client_with_auth() {
        let auth = MexcAuth::new("key".to_string(), "secret".to_string());
        let client = MexcRestClient::new_spot(Some(auth));
        assert!(client.auth.is_some());
    }

    // -------------------------------------------------------------------------
    // URL Constants Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_mexc_urls_are_valid() {
        assert!(MEXC_SPOT_REST_URL.starts_with("https://"));
        assert!(MEXC_SPOT_WS_URL.starts_with("wss://"));
        assert!(MEXC_SPOT_WS_PRIVATE_URL.starts_with("wss://"));
        assert!(MEXC_FUTURES_REST_URL.starts_with("https://"));
        assert!(MEXC_FUTURES_WS_URL.starts_with("wss://"));
    }

    #[test]
    fn test_mexc_urls_contain_mexc() {
        assert!(MEXC_SPOT_REST_URL.contains("mexc"));
        assert!(MEXC_SPOT_WS_URL.contains("mexc"));
        assert!(MEXC_FUTURES_REST_URL.contains("mexc"));
        assert!(MEXC_FUTURES_WS_URL.contains("mexc"));
    }
}
