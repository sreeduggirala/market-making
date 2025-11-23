//! Bybit Authentication and REST Client
//!
//! Provides shared authentication, HTTP client, and type converters for Bybit V5 API.
//!
//! # Authentication
//!
//! Bybit V5 API uses HMAC-SHA256 signatures:
//! - String to sign: `timestamp + api_key + recv_window + request_params`
//! - Signature is hex-encoded (lowercase)
//! - Required headers: X-BAPI-API-KEY, X-BAPI-TIMESTAMP, X-BAPI-SIGN, X-BAPI-RECV-WINDOW
//!
//! # API Documentation
//!
//! - V5 API: <https://bybit-exchange.github.io/docs/v5/intro>

use anyhow::{Context, Result};
use reqwest::Client;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

// =============================================================================
// API Endpoints
// =============================================================================

/// Bybit REST API base URL (mainnet)
pub const BYBIT_REST_URL: &str = "https://api.bybit.com";

/// Bybit Public WebSocket - Spot
pub const BYBIT_WS_SPOT_URL: &str = "wss://stream.bybit.com/v5/public/spot";

/// Bybit Public WebSocket - Linear (USDT perpetuals)
pub const BYBIT_WS_LINEAR_URL: &str = "wss://stream.bybit.com/v5/public/linear";

/// Bybit Private WebSocket (all products)
pub const BYBIT_WS_PRIVATE_URL: &str = "wss://stream.bybit.com/v5/private";

// =============================================================================
// Authentication
// =============================================================================

/// Bybit API authentication credentials
#[derive(Clone)]
pub struct BybitAuth {
    pub api_key: String,
    pub api_secret: String,
}

impl BybitAuth {
    pub fn new(api_key: String, api_secret: String) -> Self {
        Self { api_key, api_secret }
    }

    /// Generates current timestamp in milliseconds
    /// Returns 0 if system time is unavailable (extremely rare)
    pub fn timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or_else(|e| {
                tracing::error!("System time error: {}", e);
                0
            })
    }

    /// Generates HMAC-SHA256 signature for REST API requests
    ///
    /// String to sign: `timestamp + api_key + recv_window + param_string`
    pub fn sign_request(&self, timestamp: u64, recv_window: u64, param_string: &str) -> String {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

        let sign_str = format!(
            "{}{}{}{}",
            timestamp, self.api_key, recv_window, param_string
        );

        // HMAC-SHA256 accepts keys of any size, this should never fail
        let mut mac = match Hmac::<Sha256>::new_from_slice(self.api_secret.as_bytes()) {
            Ok(m) => m,
            Err(e) => {
                tracing::error!("HMAC initialization failed: {}", e);
                return String::new();
            }
        };
        mac.update(sign_str.as_bytes());

        hex::encode(mac.finalize().into_bytes())
    }

    /// Generates signature for WebSocket authentication
    ///
    /// String to sign: `GET/realtime{expires}`
    pub fn sign_websocket(&self, expires: u64) -> String {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

        let sign_str = format!("GET/realtime{}", expires);

        // HMAC-SHA256 accepts keys of any size, this should never fail
        let mut mac = match Hmac::<Sha256>::new_from_slice(self.api_secret.as_bytes()) {
            Ok(m) => m,
            Err(e) => {
                tracing::error!("HMAC initialization failed: {}", e);
                return String::new();
            }
        };
        mac.update(sign_str.as_bytes());

        hex::encode(mac.finalize().into_bytes())
    }
}

// =============================================================================
// REST Client
// =============================================================================

/// HTTP client for Bybit REST API
#[derive(Clone)]
pub struct BybitRestClient {
    client: Client,
    auth: Option<BybitAuth>,
    base_url: String,
    recv_window: u64,
}

impl BybitRestClient {
    /// Creates a new Bybit REST client
    pub fn new(auth: Option<BybitAuth>) -> Self {
        let client = Client::builder()
            .pool_max_idle_per_host(10)
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap_or_else(|e| {
                tracing::error!("Failed to build HTTP client, using default: {}", e);
                Client::new()
            });
        Self {
            client,
            auth,
            base_url: BYBIT_REST_URL.to_string(),
            recv_window: 5000,
        }
    }

    /// Makes a public GET request (no authentication)
    pub async fn get_public<T: DeserializeOwned>(
        &self,
        endpoint: &str,
        params: Option<HashMap<String, String>>,
    ) -> Result<T> {
        let url = format!("{}{}", self.base_url, endpoint);
        let mut request = self.client.get(&url);

        if let Some(p) = params {
            request = request.query(&p);
        }

        let response = request.send().await.context("Failed to send request")?;
        let status = response.status();
        let body = response.text().await.context("Failed to read response")?;

        if !status.is_success() {
            anyhow::bail!("API error ({}): {}", status, body);
        }

        serde_json::from_str(&body).context("Failed to parse response")
    }

    /// Makes an authenticated GET request
    pub async fn get_private<T: DeserializeOwned>(
        &self,
        endpoint: &str,
        params: HashMap<String, String>,
    ) -> Result<T> {
        let auth = self.auth.as_ref().context("Authentication required")?;

        let timestamp = BybitAuth::timestamp();
        let query_string = serde_urlencoded::to_string(&params)?;
        let signature = auth.sign_request(timestamp, self.recv_window, &query_string);

        let url = format!("{}{}?{}", self.base_url, endpoint, query_string);

        let response = self
            .client
            .get(&url)
            .header("X-BAPI-API-KEY", &auth.api_key)
            .header("X-BAPI-TIMESTAMP", timestamp.to_string())
            .header("X-BAPI-SIGN", signature)
            .header("X-BAPI-RECV-WINDOW", self.recv_window.to_string())
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

    /// Makes an authenticated POST request
    pub async fn post_private<T: DeserializeOwned>(
        &self,
        endpoint: &str,
        body: &impl serde::Serialize,
    ) -> Result<T> {
        let auth = self.auth.as_ref().context("Authentication required")?;

        let timestamp = BybitAuth::timestamp();
        let json_body = serde_json::to_string(body)?;
        let signature = auth.sign_request(timestamp, self.recv_window, &json_body);

        let url = format!("{}{}", self.base_url, endpoint);

        let response = self
            .client
            .post(&url)
            .header("X-BAPI-API-KEY", &auth.api_key)
            .header("X-BAPI-TIMESTAMP", timestamp.to_string())
            .header("X-BAPI-SIGN", signature)
            .header("X-BAPI-RECV-WINDOW", self.recv_window.to_string())
            .header("Content-Type", "application/json")
            .body(json_body)
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

// =============================================================================
// Response Types
// =============================================================================

/// Standard Bybit API response wrapper
#[derive(Debug, serde::Deserialize)]
pub struct BybitResponse<T> {
    #[serde(rename = "retCode")]
    pub ret_code: i32,
    #[serde(rename = "retMsg")]
    pub ret_msg: String,
    pub result: Option<T>,
    pub time: Option<u64>,
}

impl<T> BybitResponse<T> {
    /// Extracts result or returns error
    pub fn into_result(self) -> Result<T> {
        if self.ret_code != 0 {
            anyhow::bail!("Bybit API error ({}): {}", self.ret_code, self.ret_msg);
        }
        self.result.context("Missing result in API response")
    }
}

// =============================================================================
// Type Converters
// =============================================================================

pub mod converters {
    use crate::traits::{OrderStatus, OrderType, Side, TimeInForce};

    pub fn to_bybit_side(side: Side) -> &'static str {
        match side {
            Side::Buy => "Buy",
            Side::Sell => "Sell",
        }
    }

    pub fn from_bybit_side(side: &str) -> Side {
        match side {
            "Buy" => Side::Buy,
            "Sell" => Side::Sell,
            _ => Side::Buy,
        }
    }

    pub fn to_bybit_order_type(order_type: OrderType) -> &'static str {
        match order_type {
            OrderType::Limit => "Limit",
            OrderType::Market => "Market",
            _ => "Limit",
        }
    }

    pub fn from_bybit_order_type(order_type: &str) -> OrderType {
        match order_type {
            "Limit" => OrderType::Limit,
            "Market" => OrderType::Market,
            _ => OrderType::Limit,
        }
    }

    pub fn to_bybit_tif(tif: TimeInForce) -> &'static str {
        match tif {
            TimeInForce::Gtc => "GTC",
            TimeInForce::Ioc => "IOC",
            TimeInForce::Fok => "FOK",
        }
    }

    pub fn from_bybit_tif(tif: &str) -> TimeInForce {
        match tif {
            "GTC" => TimeInForce::Gtc,
            "IOC" => TimeInForce::Ioc,
            "FOK" => TimeInForce::Fok,
            _ => TimeInForce::Gtc,
        }
    }

    pub fn from_bybit_order_status(status: &str) -> OrderStatus {
        match status {
            "New" | "Untriggered" => OrderStatus::New,
            "PartiallyFilled" | "PartiallyFilledCanceled" => OrderStatus::PartiallyFilled,
            "Filled" => OrderStatus::Filled,
            "Cancelled" | "Canceled" | "Deactivated" => OrderStatus::Canceled,
            "Rejected" => OrderStatus::Rejected,
            _ => OrderStatus::New,
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::{OrderStatus, OrderType, Side, TimeInForce};

    // -------------------------------------------------------------------------
    // Authentication Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_auth_new() {
        let auth = BybitAuth::new("api_key".to_string(), "api_secret".to_string());
        assert_eq!(auth.api_key, "api_key");
        assert_eq!(auth.api_secret, "api_secret");
    }

    #[test]
    fn test_auth_sign_request_produces_hex_signature() {
        let auth = BybitAuth::new("api_key".to_string(), "secret123".to_string());
        let signature = auth.sign_request(1234567890000, 5000, "symbol=BTCUSDT");

        // Signature should be hex-encoded (only hex characters)
        assert!(!signature.is_empty());
        assert!(signature.chars().all(|c| c.is_ascii_hexdigit()));
        // SHA256 produces 32 bytes = 64 hex chars
        assert_eq!(signature.len(), 64);
    }

    #[test]
    fn test_auth_sign_request_deterministic() {
        let auth = BybitAuth::new("api_key".to_string(), "secret123".to_string());
        let sig1 = auth.sign_request(1234567890000, 5000, "symbol=BTCUSDT");
        let sig2 = auth.sign_request(1234567890000, 5000, "symbol=BTCUSDT");
        assert_eq!(sig1, sig2, "same input should produce same signature");
    }

    #[test]
    fn test_auth_sign_request_different_timestamps() {
        let auth = BybitAuth::new("api_key".to_string(), "secret123".to_string());
        let sig1 = auth.sign_request(1234567890000, 5000, "symbol=BTCUSDT");
        let sig2 = auth.sign_request(1234567890001, 5000, "symbol=BTCUSDT");
        assert_ne!(sig1, sig2, "different timestamps should produce different signatures");
    }

    #[test]
    fn test_auth_sign_websocket_produces_hex_signature() {
        let auth = BybitAuth::new("api_key".to_string(), "secret123".to_string());
        let signature = auth.sign_websocket(1234567890000);

        assert!(!signature.is_empty());
        assert!(signature.chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(signature.len(), 64);
    }

    #[test]
    fn test_auth_timestamp_is_reasonable() {
        let ts = BybitAuth::timestamp();
        let min_ts: u64 = 1704067200000; // 2024-01-01
        let max_ts: u64 = 4102444800000; // 2100-01-01
        assert!(ts > min_ts, "timestamp {} is too old", ts);
        assert!(ts < max_ts, "timestamp {} is too far in future", ts);
    }

    // -------------------------------------------------------------------------
    // Side Converter Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_to_bybit_side() {
        assert_eq!(converters::to_bybit_side(Side::Buy), "Buy");
        assert_eq!(converters::to_bybit_side(Side::Sell), "Sell");
    }

    #[test]
    fn test_from_bybit_side() {
        assert!(matches!(converters::from_bybit_side("Buy"), Side::Buy));
        assert!(matches!(converters::from_bybit_side("Sell"), Side::Sell));
    }

    #[test]
    fn test_from_bybit_side_unknown_defaults_to_buy() {
        assert!(matches!(converters::from_bybit_side("unknown"), Side::Buy));
        assert!(matches!(converters::from_bybit_side(""), Side::Buy));
    }

    // -------------------------------------------------------------------------
    // Order Type Converter Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_to_bybit_order_type() {
        assert_eq!(converters::to_bybit_order_type(OrderType::Limit), "Limit");
        assert_eq!(converters::to_bybit_order_type(OrderType::Market), "Market");
    }

    #[test]
    fn test_to_bybit_order_type_unsupported_defaults_to_limit() {
        assert_eq!(converters::to_bybit_order_type(OrderType::StopLoss), "Limit");
        assert_eq!(converters::to_bybit_order_type(OrderType::TakeProfit), "Limit");
    }

    #[test]
    fn test_from_bybit_order_type() {
        assert!(matches!(converters::from_bybit_order_type("Limit"), OrderType::Limit));
        assert!(matches!(converters::from_bybit_order_type("Market"), OrderType::Market));
    }

    #[test]
    fn test_from_bybit_order_type_unknown_defaults_to_limit() {
        assert!(matches!(converters::from_bybit_order_type("unknown"), OrderType::Limit));
    }

    // -------------------------------------------------------------------------
    // Time-in-Force Converter Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_to_bybit_tif() {
        assert_eq!(converters::to_bybit_tif(TimeInForce::Gtc), "GTC");
        assert_eq!(converters::to_bybit_tif(TimeInForce::Ioc), "IOC");
        assert_eq!(converters::to_bybit_tif(TimeInForce::Fok), "FOK");
    }

    #[test]
    fn test_from_bybit_tif() {
        assert!(matches!(converters::from_bybit_tif("GTC"), TimeInForce::Gtc));
        assert!(matches!(converters::from_bybit_tif("IOC"), TimeInForce::Ioc));
        assert!(matches!(converters::from_bybit_tif("FOK"), TimeInForce::Fok));
    }

    #[test]
    fn test_from_bybit_tif_unknown_defaults_to_gtc() {
        assert!(matches!(converters::from_bybit_tif("unknown"), TimeInForce::Gtc));
    }

    // -------------------------------------------------------------------------
    // Order Status Converter Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_from_bybit_order_status_new() {
        assert!(matches!(converters::from_bybit_order_status("New"), OrderStatus::New));
        assert!(matches!(converters::from_bybit_order_status("Untriggered"), OrderStatus::New));
    }

    #[test]
    fn test_from_bybit_order_status_partial() {
        assert!(matches!(converters::from_bybit_order_status("PartiallyFilled"), OrderStatus::PartiallyFilled));
        assert!(matches!(converters::from_bybit_order_status("PartiallyFilledCanceled"), OrderStatus::PartiallyFilled));
    }

    #[test]
    fn test_from_bybit_order_status_filled() {
        assert!(matches!(converters::from_bybit_order_status("Filled"), OrderStatus::Filled));
    }

    #[test]
    fn test_from_bybit_order_status_canceled() {
        assert!(matches!(converters::from_bybit_order_status("Cancelled"), OrderStatus::Canceled));
        assert!(matches!(converters::from_bybit_order_status("Canceled"), OrderStatus::Canceled));
        assert!(matches!(converters::from_bybit_order_status("Deactivated"), OrderStatus::Canceled));
    }

    #[test]
    fn test_from_bybit_order_status_rejected() {
        assert!(matches!(converters::from_bybit_order_status("Rejected"), OrderStatus::Rejected));
    }

    #[test]
    fn test_from_bybit_order_status_unknown_defaults_to_new() {
        assert!(matches!(converters::from_bybit_order_status("unknown"), OrderStatus::New));
    }

    // -------------------------------------------------------------------------
    // REST Client Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_rest_client_new() {
        let client = BybitRestClient::new(None);
        assert_eq!(client.base_url, BYBIT_REST_URL);
        assert_eq!(client.recv_window, 5000);
    }

    #[test]
    fn test_rest_client_with_auth() {
        let auth = BybitAuth::new("key".to_string(), "secret".to_string());
        let client = BybitRestClient::new(Some(auth));
        assert!(client.auth.is_some());
    }

    // -------------------------------------------------------------------------
    // Response Type Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_bybit_response_into_result_success() {
        let response: BybitResponse<String> = BybitResponse {
            ret_code: 0,
            ret_msg: "OK".to_string(),
            result: Some("test_data".to_string()),
            time: Some(1234567890),
        };
        let result = response.into_result();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "test_data");
    }

    #[test]
    fn test_bybit_response_into_result_error() {
        let response: BybitResponse<String> = BybitResponse {
            ret_code: 10001,
            ret_msg: "Invalid parameter".to_string(),
            result: None,
            time: Some(1234567890),
        };
        let result = response.into_result();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("10001"));
    }

    #[test]
    fn test_bybit_response_missing_result() {
        let response: BybitResponse<String> = BybitResponse {
            ret_code: 0,
            ret_msg: "OK".to_string(),
            result: None,
            time: Some(1234567890),
        };
        let result = response.into_result();
        assert!(result.is_err());
    }

    // -------------------------------------------------------------------------
    // URL Constants Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_bybit_urls_are_valid() {
        assert!(BYBIT_REST_URL.starts_with("https://"));
        assert!(BYBIT_WS_SPOT_URL.starts_with("wss://"));
        assert!(BYBIT_WS_LINEAR_URL.starts_with("wss://"));
        assert!(BYBIT_WS_PRIVATE_URL.starts_with("wss://"));
    }

    #[test]
    fn test_bybit_urls_contain_bybit() {
        assert!(BYBIT_REST_URL.contains("bybit"));
        assert!(BYBIT_WS_SPOT_URL.contains("bybit"));
        assert!(BYBIT_WS_LINEAR_URL.contains("bybit"));
        assert!(BYBIT_WS_PRIVATE_URL.contains("bybit"));
    }
}
