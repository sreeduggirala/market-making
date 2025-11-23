//! Binance.US Exchange Account Management and Authentication
//!
//! This module provides shared authentication, HTTP client functionality, and type converters
//! for Binance.US Spot markets.
//!
//! # Authentication
//!
//! Binance.US uses HMAC-SHA256 signatures for request authentication:
//! 1. Create query string from parameters
//! 2. Append timestamp parameter
//! 3. Generate HMAC-SHA256 signature using API secret
//! 4. Append signature to query string
//!
//! # API Documentation
//!
//! - Binance.US API: <https://docs.binance.us/>

use anyhow::{Context, Result};
use reqwest::Client;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

// ============================================================================
// API Endpoint Constants
// ============================================================================

/// Binance.US Spot REST API base URL
pub const BINANCE_US_REST_URL: &str = "https://api.binance.us";

/// Binance.US Spot WebSocket stream URL
pub const BINANCE_US_WS_URL: &str = "wss://stream.binance.us:9443/ws";

/// Binance.US Spot WebSocket combined stream URL
pub const BINANCE_US_WS_COMBINED_URL: &str = "wss://stream.binance.us:9443/stream";

// ============================================================================
// Authentication
// ============================================================================

/// Binance.US API authentication credentials
///
/// Stores API key and secret for signing requests. The API secret is used directly
/// to generate HMAC-SHA256 signatures for authenticated REST API calls.
///
/// # Security Notes
///
/// - API keys should be stored securely (e.g., environment variables, secrets manager)
/// - Never commit API keys to version control
/// - Use IP whitelisting on Binance.US platform
#[derive(Clone)]
pub struct BinanceUsAuth {
    /// API key string (public identifier)
    pub api_key: String,

    /// API secret in plain text format (private signing key)
    pub api_secret: String,
}

impl BinanceUsAuth {
    /// Creates a new BinanceUsAuth instance with the provided credentials
    pub fn new(api_key: String, api_secret: String) -> Self {
        Self { api_key, api_secret }
    }

    /// Generates HMAC-SHA256 signature for Binance.US REST API authenticated requests
    ///
    /// # Arguments
    ///
    /// * `query_string` - URL-encoded query parameters (without signature)
    ///
    /// # Returns
    ///
    /// Hex-encoded HMAC-SHA256 signature string
    pub fn sign(&self, query_string: &str) -> String {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

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
    /// Returns the current Unix timestamp in milliseconds.
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

/// HTTP client wrapper for Binance.US REST API
#[derive(Clone)]
pub struct BinanceUsRestClient {
    /// Underlying HTTP client
    client: Client,

    /// Optional authentication credentials for private endpoints
    auth: Option<BinanceUsAuth>,

    /// Base URL for API requests
    base_url: String,
}

impl BinanceUsRestClient {
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

    /// Creates a new HTTP client configured for Binance.US Spot REST API
    pub fn new_spot(auth: Option<BinanceUsAuth>) -> Self {
        Self {
            client: Self::build_client(),
            auth,
            base_url: BINANCE_US_REST_URL.to_string(),
        }
    }

    /// Makes an unauthenticated GET request to a public endpoint
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
    pub async fn get_private<T: for<'de> serde::Deserialize<'de>>(
        &self,
        endpoint: &str,
        mut params: HashMap<String, String>,
    ) -> Result<T> {
        let auth = self.auth.as_ref()
            .context("Authentication required for private endpoints")?;

        // Add timestamp
        let timestamp = BinanceUsAuth::get_timestamp();
        params.insert("timestamp".to_string(), timestamp.to_string());

        // Create query string for signing
        let query_string = params
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
            .header("X-MBX-APIKEY", &auth.api_key)
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
    pub async fn post_private<T: for<'de> serde::Deserialize<'de>>(
        &self,
        endpoint: &str,
        mut params: HashMap<String, String>,
    ) -> Result<T> {
        let auth = self.auth.as_ref()
            .context("Authentication required for private endpoints")?;

        // Add timestamp
        let timestamp = BinanceUsAuth::get_timestamp();
        params.insert("timestamp".to_string(), timestamp.to_string());

        // Create query string for signing
        let query_string = params
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
            .header("X-MBX-APIKEY", &auth.api_key)
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
    pub async fn delete_private<T: for<'de> serde::Deserialize<'de>>(
        &self,
        endpoint: &str,
        mut params: HashMap<String, String>,
    ) -> Result<T> {
        let auth = self.auth.as_ref()
            .context("Authentication required for private endpoints")?;

        // Add timestamp
        let timestamp = BinanceUsAuth::get_timestamp();
        params.insert("timestamp".to_string(), timestamp.to_string());

        // Create query string for signing
        let query_string = params
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
            .header("X-MBX-APIKEY", &auth.api_key)
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

/// Utility functions for converting between Binance.US API types and internal trait types
pub mod converters {
    use crate::traits::{OrderType, Side, TimeInForce, OrderStatus, MarketStatus, KlineInterval};

    /// Converts internal OrderType enum to Binance.US API string format
    pub fn to_binance_order_type(order_type: OrderType) -> String {
        match order_type {
            OrderType::Limit => "LIMIT".to_string(),
            OrderType::Market => "MARKET".to_string(),
            OrderType::StopLoss => "STOP_LOSS".to_string(),
            OrderType::StopLossLimit => "STOP_LOSS_LIMIT".to_string(),
            OrderType::TakeProfit => "TAKE_PROFIT".to_string(),
            OrderType::TakeProfitLimit => "TAKE_PROFIT_LIMIT".to_string(),
        }
    }

    /// Converts Binance.US API order type string to internal OrderType enum
    pub fn from_binance_order_type(order_type: &str) -> OrderType {
        match order_type {
            "LIMIT" => OrderType::Limit,
            "MARKET" => OrderType::Market,
            "STOP_LOSS" => OrderType::StopLoss,
            "STOP_LOSS_LIMIT" => OrderType::StopLossLimit,
            "TAKE_PROFIT" => OrderType::TakeProfit,
            "TAKE_PROFIT_LIMIT" => OrderType::TakeProfitLimit,
            _ => OrderType::Limit,
        }
    }

    /// Converts internal Side enum to Binance.US API string format
    pub fn to_binance_side(side: Side) -> String {
        match side {
            Side::Buy => "BUY".to_string(),
            Side::Sell => "SELL".to_string(),
        }
    }

    /// Converts Binance.US API side string to internal Side enum
    pub fn from_binance_side(side: &str) -> Side {
        match side.to_uppercase().as_str() {
            "BUY" => Side::Buy,
            "SELL" => Side::Sell,
            _ => Side::Buy,
        }
    }

    /// Converts internal TimeInForce enum to Binance.US API string format
    pub fn to_binance_tif(tif: TimeInForce) -> String {
        match tif {
            TimeInForce::Gtc => "GTC".to_string(),
            TimeInForce::Ioc => "IOC".to_string(),
            TimeInForce::Fok => "FOK".to_string(),
        }
    }

    /// Converts Binance.US API time-in-force string to internal TimeInForce enum
    pub fn from_binance_tif(tif: &str) -> TimeInForce {
        match tif {
            "GTC" => TimeInForce::Gtc,
            "IOC" => TimeInForce::Ioc,
            "FOK" => TimeInForce::Fok,
            _ => TimeInForce::Gtc,
        }
    }

    /// Converts Binance.US API order status string to internal OrderStatus enum
    pub fn from_binance_order_status(status: &str) -> OrderStatus {
        match status.to_uppercase().as_str() {
            "NEW" => OrderStatus::New,
            "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
            "FILLED" => OrderStatus::Filled,
            "CANCELED" | "CANCELLED" => OrderStatus::Canceled,
            "REJECTED" => OrderStatus::Rejected,
            "EXPIRED" => OrderStatus::Expired,
            _ => OrderStatus::New,
        }
    }

    /// Converts Binance.US API symbol status string to internal MarketStatus enum
    pub fn from_binance_symbol_status(status: &str) -> MarketStatus {
        match status.to_uppercase().as_str() {
            "TRADING" => MarketStatus::Trading,
            "HALT" | "HALTED" | "BREAK" => MarketStatus::Halt,
            "PRE_TRADING" => MarketStatus::PreTrading,
            "POST_TRADING" => MarketStatus::PostTrading,
            _ => MarketStatus::Halt,
        }
    }

    /// Converts internal KlineInterval enum to Binance.US API string format
    pub fn to_binance_interval(interval: KlineInterval) -> String {
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
        let auth = BinanceUsAuth::new("api_key".to_string(), "api_secret".to_string());
        assert_eq!(auth.api_key, "api_key");
        assert_eq!(auth.api_secret, "api_secret");
    }

    #[test]
    fn test_auth_sign_produces_hex_signature() {
        let auth = BinanceUsAuth::new("api_key".to_string(), "secret123".to_string());
        let signature = auth.sign("symbol=BTCUSDT&side=BUY&type=LIMIT");

        // Signature should be hex-encoded (only hex characters)
        assert!(!signature.is_empty());
        assert!(signature.chars().all(|c| c.is_ascii_hexdigit()));
        // SHA256 produces 32 bytes = 64 hex chars
        assert_eq!(signature.len(), 64);
    }

    #[test]
    fn test_auth_sign_deterministic() {
        let auth = BinanceUsAuth::new("api_key".to_string(), "secret123".to_string());
        let query = "symbol=BTCUSDT&timestamp=1234567890";
        let sig1 = auth.sign(query);
        let sig2 = auth.sign(query);
        assert_eq!(sig1, sig2, "same input should produce same signature");
    }

    #[test]
    fn test_auth_sign_different_secrets_produce_different_signatures() {
        let auth1 = BinanceUsAuth::new("api_key".to_string(), "secret1".to_string());
        let auth2 = BinanceUsAuth::new("api_key".to_string(), "secret2".to_string());
        let query = "symbol=BTCUSDT";
        let sig1 = auth1.sign(query);
        let sig2 = auth2.sign(query);
        assert_ne!(sig1, sig2, "different secrets should produce different signatures");
    }

    #[test]
    fn test_auth_get_timestamp_is_reasonable() {
        let ts = BinanceUsAuth::get_timestamp();
        let min_ts: u64 = 1704067200000; // 2024-01-01
        let max_ts: u64 = 4102444800000; // 2100-01-01
        assert!(ts > min_ts, "timestamp {} is too old", ts);
        assert!(ts < max_ts, "timestamp {} is too far in future", ts);
    }

    // -------------------------------------------------------------------------
    // Order Type Converter Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_to_binance_order_type() {
        assert_eq!(converters::to_binance_order_type(OrderType::Limit), "LIMIT");
        assert_eq!(converters::to_binance_order_type(OrderType::Market), "MARKET");
        assert_eq!(converters::to_binance_order_type(OrderType::StopLoss), "STOP_LOSS");
        assert_eq!(converters::to_binance_order_type(OrderType::StopLossLimit), "STOP_LOSS_LIMIT");
        assert_eq!(converters::to_binance_order_type(OrderType::TakeProfit), "TAKE_PROFIT");
        assert_eq!(converters::to_binance_order_type(OrderType::TakeProfitLimit), "TAKE_PROFIT_LIMIT");
    }

    #[test]
    fn test_from_binance_order_type() {
        assert!(matches!(converters::from_binance_order_type("LIMIT"), OrderType::Limit));
        assert!(matches!(converters::from_binance_order_type("MARKET"), OrderType::Market));
        assert!(matches!(converters::from_binance_order_type("STOP_LOSS"), OrderType::StopLoss));
        assert!(matches!(converters::from_binance_order_type("STOP_LOSS_LIMIT"), OrderType::StopLossLimit));
        assert!(matches!(converters::from_binance_order_type("TAKE_PROFIT"), OrderType::TakeProfit));
        assert!(matches!(converters::from_binance_order_type("TAKE_PROFIT_LIMIT"), OrderType::TakeProfitLimit));
    }

    #[test]
    fn test_from_binance_order_type_unknown_defaults_to_limit() {
        assert!(matches!(converters::from_binance_order_type("UNKNOWN"), OrderType::Limit));
        assert!(matches!(converters::from_binance_order_type(""), OrderType::Limit));
    }

    // -------------------------------------------------------------------------
    // Side Converter Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_to_binance_side() {
        assert_eq!(converters::to_binance_side(Side::Buy), "BUY");
        assert_eq!(converters::to_binance_side(Side::Sell), "SELL");
    }

    #[test]
    fn test_from_binance_side() {
        assert!(matches!(converters::from_binance_side("BUY"), Side::Buy));
        assert!(matches!(converters::from_binance_side("SELL"), Side::Sell));
        // Case insensitive
        assert!(matches!(converters::from_binance_side("buy"), Side::Buy));
        assert!(matches!(converters::from_binance_side("sell"), Side::Sell));
    }

    #[test]
    fn test_from_binance_side_unknown_defaults_to_buy() {
        assert!(matches!(converters::from_binance_side("UNKNOWN"), Side::Buy));
    }

    // -------------------------------------------------------------------------
    // Time-in-Force Converter Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_to_binance_tif() {
        assert_eq!(converters::to_binance_tif(TimeInForce::Gtc), "GTC");
        assert_eq!(converters::to_binance_tif(TimeInForce::Ioc), "IOC");
        assert_eq!(converters::to_binance_tif(TimeInForce::Fok), "FOK");
    }

    #[test]
    fn test_from_binance_tif() {
        assert!(matches!(converters::from_binance_tif("GTC"), TimeInForce::Gtc));
        assert!(matches!(converters::from_binance_tif("IOC"), TimeInForce::Ioc));
        assert!(matches!(converters::from_binance_tif("FOK"), TimeInForce::Fok));
    }

    #[test]
    fn test_from_binance_tif_unknown_defaults_to_gtc() {
        assert!(matches!(converters::from_binance_tif("UNKNOWN"), TimeInForce::Gtc));
    }

    // -------------------------------------------------------------------------
    // Order Status Converter Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_from_binance_order_status() {
        assert!(matches!(converters::from_binance_order_status("NEW"), OrderStatus::New));
        assert!(matches!(converters::from_binance_order_status("PARTIALLY_FILLED"), OrderStatus::PartiallyFilled));
        assert!(matches!(converters::from_binance_order_status("FILLED"), OrderStatus::Filled));
        assert!(matches!(converters::from_binance_order_status("CANCELED"), OrderStatus::Canceled));
        assert!(matches!(converters::from_binance_order_status("CANCELLED"), OrderStatus::Canceled));
        assert!(matches!(converters::from_binance_order_status("REJECTED"), OrderStatus::Rejected));
        assert!(matches!(converters::from_binance_order_status("EXPIRED"), OrderStatus::Expired));
    }

    #[test]
    fn test_from_binance_order_status_case_insensitive() {
        assert!(matches!(converters::from_binance_order_status("new"), OrderStatus::New));
        assert!(matches!(converters::from_binance_order_status("filled"), OrderStatus::Filled));
    }

    #[test]
    fn test_from_binance_order_status_unknown_defaults_to_new() {
        assert!(matches!(converters::from_binance_order_status("UNKNOWN"), OrderStatus::New));
    }

    // -------------------------------------------------------------------------
    // Market Status Converter Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_from_binance_symbol_status() {
        assert!(matches!(converters::from_binance_symbol_status("TRADING"), MarketStatus::Trading));
        assert!(matches!(converters::from_binance_symbol_status("HALT"), MarketStatus::Halt));
        assert!(matches!(converters::from_binance_symbol_status("HALTED"), MarketStatus::Halt));
        assert!(matches!(converters::from_binance_symbol_status("BREAK"), MarketStatus::Halt));
        assert!(matches!(converters::from_binance_symbol_status("PRE_TRADING"), MarketStatus::PreTrading));
        assert!(matches!(converters::from_binance_symbol_status("POST_TRADING"), MarketStatus::PostTrading));
    }

    #[test]
    fn test_from_binance_symbol_status_unknown_defaults_to_halt() {
        assert!(matches!(converters::from_binance_symbol_status("UNKNOWN"), MarketStatus::Halt));
    }

    // -------------------------------------------------------------------------
    // Kline Interval Converter Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_to_binance_interval() {
        assert_eq!(converters::to_binance_interval(KlineInterval::M1), "1m");
        assert_eq!(converters::to_binance_interval(KlineInterval::M5), "5m");
        assert_eq!(converters::to_binance_interval(KlineInterval::M15), "15m");
        assert_eq!(converters::to_binance_interval(KlineInterval::M30), "30m");
        assert_eq!(converters::to_binance_interval(KlineInterval::H1), "1h");
        assert_eq!(converters::to_binance_interval(KlineInterval::H4), "4h");
        assert_eq!(converters::to_binance_interval(KlineInterval::D1), "1d");
    }

    // -------------------------------------------------------------------------
    // REST Client Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_rest_client_new_spot() {
        let client = BinanceUsRestClient::new_spot(None);
        assert_eq!(client.base_url, BINANCE_US_REST_URL);
    }

    #[test]
    fn test_rest_client_with_auth() {
        let auth = BinanceUsAuth::new("key".to_string(), "secret".to_string());
        let client = BinanceUsRestClient::new_spot(Some(auth));
        assert!(client.auth.is_some());
    }

    // -------------------------------------------------------------------------
    // URL Constants Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_binance_us_urls_are_valid() {
        assert!(BINANCE_US_REST_URL.starts_with("https://"));
        assert!(BINANCE_US_WS_URL.starts_with("wss://"));
        assert!(BINANCE_US_WS_COMBINED_URL.starts_with("wss://"));
    }

    #[test]
    fn test_binance_us_urls_contain_binance_us() {
        assert!(BINANCE_US_REST_URL.contains("binance.us"));
        assert!(BINANCE_US_WS_URL.contains("binance.us"));
        assert!(BINANCE_US_WS_COMBINED_URL.contains("binance.us"));
    }
}
