//! Kalshi Authentication and REST Client
//!
//! Provides shared authentication, HTTP client, and type converters for Kalshi API.
//!
//! # Authentication
//!
//! Kalshi API uses RSA-PSS signatures:
//! - String to sign: `timestamp + method + path` (path without query params)
//! - Required headers: KALSHI-ACCESS-KEY, KALSHI-ACCESS-TIMESTAMP, KALSHI-ACCESS-SIGNATURE
//!
//! # API Documentation
//!
//! - API: <https://docs.kalshi.com/>

use anyhow::{Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use reqwest::Client;
use rsa::pkcs1v15::SigningKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::signature::{RandomizedSigner, SignatureEncoding};
use rsa::RsaPrivateKey;
use serde::de::DeserializeOwned;
use sha2::Sha256;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

// =============================================================================
// API Endpoints
// =============================================================================

/// Kalshi REST API base URL (production)
pub const KALSHI_REST_URL: &str = "https://api.elections.kalshi.com/trade-api/v2";

/// Kalshi REST API base URL (demo)
pub const KALSHI_REST_URL_DEMO: &str = "https://demo-api.kalshi.co/trade-api/v2";

/// Kalshi WebSocket URL (production)
pub const KALSHI_WS_URL: &str = "wss://api.elections.kalshi.com/trade-api/ws/v2";

/// Kalshi WebSocket URL (demo)
pub const KALSHI_WS_URL_DEMO: &str = "wss://demo-api.kalshi.co/trade-api/ws/v2";

// =============================================================================
// Authentication
// =============================================================================

/// Kalshi API authentication credentials
#[derive(Clone)]
pub struct KalshiAuth {
    pub key_id: String,
    private_key: Arc<RsaPrivateKey>,
}

impl KalshiAuth {
    /// Creates new auth from key ID and PEM-encoded private key
    pub fn new(key_id: String, private_key_pem: &str) -> Result<Self> {
        let private_key = RsaPrivateKey::from_pkcs8_pem(private_key_pem)
            .context("Failed to parse RSA private key from PEM")?;
        Ok(Self {
            key_id,
            private_key: Arc::new(private_key),
        })
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

    /// Generates RSA-PSS signature for API requests
    ///
    /// String to sign: `timestamp + method + path`
    /// Uses RSA-PSS padding with SHA256
    pub fn sign_request(&self, timestamp: u64, method: &str, path: &str) -> Result<String> {
        let sign_str = format!("{}{}{}", timestamp, method, path);

        let signing_key = SigningKey::<Sha256>::new((*self.private_key).clone());
        let mut rng = rand::thread_rng();
        let signature = signing_key.sign_with_rng(&mut rng, sign_str.as_bytes());

        Ok(BASE64.encode(signature.to_bytes()))
    }

    /// Generates signature for WebSocket authentication
    ///
    /// String to sign: `timestamp + "GET" + "/trade-api/ws/v2"`
    pub fn sign_websocket(&self, timestamp: u64) -> Result<String> {
        self.sign_request(timestamp, "GET", "/trade-api/ws/v2")
    }
}

// =============================================================================
// REST Client
// =============================================================================

/// HTTP client for Kalshi REST API
#[derive(Clone)]
pub struct KalshiRestClient {
    client: Client,
    auth: Option<KalshiAuth>,
    base_url: String,
}

impl KalshiRestClient {
    /// Creates a new Kalshi REST client (production)
    pub fn new(auth: Option<KalshiAuth>) -> Self {
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
            base_url: KALSHI_REST_URL.to_string(),
        }
    }

    /// Creates a new Kalshi REST client for demo environment
    pub fn new_demo(auth: Option<KalshiAuth>) -> Self {
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
            base_url: KALSHI_REST_URL_DEMO.to_string(),
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
            anyhow::bail!("Kalshi API error ({}): {}", status, body);
        }

        serde_json::from_str(&body).context("Failed to parse response")
    }

    /// Makes an authenticated GET request
    pub async fn get_private<T: DeserializeOwned>(
        &self,
        endpoint: &str,
        params: Option<HashMap<String, String>>,
    ) -> Result<T> {
        let auth = self.auth.as_ref().context("Authentication required")?;

        let timestamp = KalshiAuth::timestamp();
        // Sign without query parameters
        let signature = auth.sign_request(timestamp, "GET", endpoint)?;

        let url = format!("{}{}", self.base_url, endpoint);
        let mut request = self.client
            .get(&url)
            .header("KALSHI-ACCESS-KEY", &auth.key_id)
            .header("KALSHI-ACCESS-TIMESTAMP", timestamp.to_string())
            .header("KALSHI-ACCESS-SIGNATURE", signature);

        if let Some(p) = params {
            request = request.query(&p);
        }

        let response = request.send().await.context("Failed to send request")?;
        let status = response.status();
        let body = response.text().await.context("Failed to read response")?;

        if !status.is_success() {
            anyhow::bail!("Kalshi API error ({}): {}", status, body);
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

        let timestamp = KalshiAuth::timestamp();
        let signature = auth.sign_request(timestamp, "POST", endpoint)?;
        let json_body = serde_json::to_string(body)?;

        let url = format!("{}{}", self.base_url, endpoint);

        let response = self
            .client
            .post(&url)
            .header("KALSHI-ACCESS-KEY", &auth.key_id)
            .header("KALSHI-ACCESS-TIMESTAMP", timestamp.to_string())
            .header("KALSHI-ACCESS-SIGNATURE", signature)
            .header("Content-Type", "application/json")
            .body(json_body)
            .send()
            .await
            .context("Failed to send request")?;

        let status = response.status();
        let body = response.text().await.context("Failed to read response")?;

        if !status.is_success() {
            anyhow::bail!("Kalshi API error ({}): {}", status, body);
        }

        serde_json::from_str(&body).context("Failed to parse response")
    }

    /// Makes an authenticated DELETE request
    pub async fn delete_private<T: DeserializeOwned>(
        &self,
        endpoint: &str,
    ) -> Result<T> {
        let auth = self.auth.as_ref().context("Authentication required")?;

        let timestamp = KalshiAuth::timestamp();
        let signature = auth.sign_request(timestamp, "DELETE", endpoint)?;

        let url = format!("{}{}", self.base_url, endpoint);

        let response = self
            .client
            .delete(&url)
            .header("KALSHI-ACCESS-KEY", &auth.key_id)
            .header("KALSHI-ACCESS-TIMESTAMP", timestamp.to_string())
            .header("KALSHI-ACCESS-SIGNATURE", signature)
            .send()
            .await
            .context("Failed to send request")?;

        let status = response.status();
        let body = response.text().await.context("Failed to read response")?;

        if !status.is_success() {
            anyhow::bail!("Kalshi API error ({}): {}", status, body);
        }

        serde_json::from_str(&body).context("Failed to parse response")
    }

    /// Returns the base URL for WebSocket connections
    pub fn ws_url(&self) -> &str {
        if self.base_url.contains("demo") {
            KALSHI_WS_URL_DEMO
        } else {
            KALSHI_WS_URL
        }
    }
}

// =============================================================================
// Type Converters
// =============================================================================

pub mod converters {
    use crate::traits::{OrderStatus, OrderType, Side, TimeInForce};

    /// Converts our Side to Kalshi action
    pub fn to_kalshi_action(side: Side) -> &'static str {
        match side {
            Side::Buy => "buy",
            Side::Sell => "sell",
        }
    }

    /// Converts Kalshi action to our Side
    pub fn from_kalshi_action(action: &str) -> Side {
        match action.to_lowercase().as_str() {
            "buy" => Side::Buy,
            "sell" => Side::Sell,
            _ => Side::Buy,
        }
    }

    /// Converts our OrderType to Kalshi order type
    pub fn to_kalshi_order_type(order_type: OrderType) -> &'static str {
        match order_type {
            OrderType::Limit => "limit",
            OrderType::Market => "market",
            _ => "limit",
        }
    }

    /// Converts Kalshi order type to ours
    pub fn from_kalshi_order_type(order_type: &str) -> OrderType {
        match order_type.to_lowercase().as_str() {
            "limit" => OrderType::Limit,
            "market" => OrderType::Market,
            _ => OrderType::Limit,
        }
    }

    /// Converts our TimeInForce to Kalshi expiration type
    /// GTC = no_expiration, IOC = immediate_or_cancel
    pub fn to_kalshi_tif(tif: TimeInForce) -> Option<&'static str> {
        match tif {
            TimeInForce::Gtc => None, // Default, no expiration
            TimeInForce::Ioc => Some("ioc"),
            TimeInForce::Fok => Some("fok"),
        }
    }

    /// Converts Kalshi order status to ours
    pub fn from_kalshi_order_status(status: &str) -> OrderStatus {
        match status.to_lowercase().as_str() {
            "resting" | "pending" => OrderStatus::New,
            "executed" | "filled" => OrderStatus::Filled,
            "canceled" | "cancelled" => OrderStatus::Canceled,
            "partial" => OrderStatus::PartiallyFilled,
            _ => OrderStatus::New,
        }
    }

    /// Converts price in cents (1-99) to decimal (0.01-0.99)
    pub fn cents_to_price(cents: i32) -> f64 {
        cents as f64 / 100.0
    }

    /// Converts decimal price to cents
    pub fn price_to_cents(price: f64) -> i32 {
        (price * 100.0).round() as i32
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
    // Converter Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_to_kalshi_action_buy() {
        assert_eq!(converters::to_kalshi_action(Side::Buy), "buy");
    }

    #[test]
    fn test_to_kalshi_action_sell() {
        assert_eq!(converters::to_kalshi_action(Side::Sell), "sell");
    }

    #[test]
    fn test_from_kalshi_action_buy() {
        assert!(matches!(converters::from_kalshi_action("buy"), Side::Buy));
        assert!(matches!(converters::from_kalshi_action("BUY"), Side::Buy));
        assert!(matches!(converters::from_kalshi_action("Buy"), Side::Buy));
    }

    #[test]
    fn test_from_kalshi_action_sell() {
        assert!(matches!(converters::from_kalshi_action("sell"), Side::Sell));
        assert!(matches!(converters::from_kalshi_action("SELL"), Side::Sell));
    }

    #[test]
    fn test_from_kalshi_action_unknown_defaults_to_buy() {
        assert!(matches!(converters::from_kalshi_action("unknown"), Side::Buy));
        assert!(matches!(converters::from_kalshi_action(""), Side::Buy));
    }

    #[test]
    fn test_to_kalshi_order_type_limit() {
        assert_eq!(converters::to_kalshi_order_type(OrderType::Limit), "limit");
    }

    #[test]
    fn test_to_kalshi_order_type_market() {
        assert_eq!(converters::to_kalshi_order_type(OrderType::Market), "market");
    }

    #[test]
    fn test_to_kalshi_order_type_stop_defaults_to_limit() {
        assert_eq!(converters::to_kalshi_order_type(OrderType::StopLoss), "limit");
        assert_eq!(converters::to_kalshi_order_type(OrderType::StopLossLimit), "limit");
    }

    #[test]
    fn test_from_kalshi_order_type_limit() {
        assert!(matches!(converters::from_kalshi_order_type("limit"), OrderType::Limit));
        assert!(matches!(converters::from_kalshi_order_type("LIMIT"), OrderType::Limit));
    }

    #[test]
    fn test_from_kalshi_order_type_market() {
        assert!(matches!(converters::from_kalshi_order_type("market"), OrderType::Market));
    }

    #[test]
    fn test_from_kalshi_order_type_unknown_defaults_to_limit() {
        assert!(matches!(converters::from_kalshi_order_type("unknown"), OrderType::Limit));
    }

    #[test]
    fn test_to_kalshi_tif_gtc() {
        assert_eq!(converters::to_kalshi_tif(TimeInForce::Gtc), None);
    }

    #[test]
    fn test_to_kalshi_tif_ioc() {
        assert_eq!(converters::to_kalshi_tif(TimeInForce::Ioc), Some("ioc"));
    }

    #[test]
    fn test_to_kalshi_tif_fok() {
        assert_eq!(converters::to_kalshi_tif(TimeInForce::Fok), Some("fok"));
    }

    #[test]
    fn test_from_kalshi_order_status_new() {
        assert!(matches!(converters::from_kalshi_order_status("resting"), OrderStatus::New));
        assert!(matches!(converters::from_kalshi_order_status("pending"), OrderStatus::New));
        assert!(matches!(converters::from_kalshi_order_status("RESTING"), OrderStatus::New));
    }

    #[test]
    fn test_from_kalshi_order_status_filled() {
        assert!(matches!(converters::from_kalshi_order_status("executed"), OrderStatus::Filled));
        assert!(matches!(converters::from_kalshi_order_status("filled"), OrderStatus::Filled));
        assert!(matches!(converters::from_kalshi_order_status("FILLED"), OrderStatus::Filled));
    }

    #[test]
    fn test_from_kalshi_order_status_canceled() {
        assert!(matches!(converters::from_kalshi_order_status("canceled"), OrderStatus::Canceled));
        assert!(matches!(converters::from_kalshi_order_status("cancelled"), OrderStatus::Canceled));
    }

    #[test]
    fn test_from_kalshi_order_status_partial() {
        assert!(matches!(converters::from_kalshi_order_status("partial"), OrderStatus::PartiallyFilled));
    }

    #[test]
    fn test_from_kalshi_order_status_unknown_defaults_to_new() {
        assert!(matches!(converters::from_kalshi_order_status("unknown"), OrderStatus::New));
        assert!(matches!(converters::from_kalshi_order_status(""), OrderStatus::New));
    }

    // -------------------------------------------------------------------------
    // Price Conversion Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_cents_to_price() {
        assert_eq!(converters::cents_to_price(50), 0.50);
        assert_eq!(converters::cents_to_price(1), 0.01);
        assert_eq!(converters::cents_to_price(99), 0.99);
        assert_eq!(converters::cents_to_price(0), 0.0);
    }

    #[test]
    fn test_price_to_cents() {
        assert_eq!(converters::price_to_cents(0.50), 50);
        assert_eq!(converters::price_to_cents(0.01), 1);
        assert_eq!(converters::price_to_cents(0.99), 99);
        assert_eq!(converters::price_to_cents(0.0), 0);
    }

    #[test]
    fn test_price_conversion_roundtrip() {
        for cents in [1, 25, 50, 75, 99] {
            let price = converters::cents_to_price(cents);
            let back = converters::price_to_cents(price);
            assert_eq!(back, cents, "roundtrip failed for cents={}", cents);
        }
    }

    #[test]
    fn test_price_to_cents_rounds_correctly() {
        // 0.505 should round to 51 (banker's rounding or standard rounding)
        assert_eq!(converters::price_to_cents(0.505), 51);
        // 0.504 should round to 50
        assert_eq!(converters::price_to_cents(0.504), 50);
    }

    // -------------------------------------------------------------------------
    // REST Client Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_rest_client_new_production() {
        let client = KalshiRestClient::new(None);
        assert_eq!(client.base_url, KALSHI_REST_URL);
    }

    #[test]
    fn test_rest_client_new_demo() {
        let client = KalshiRestClient::new_demo(None);
        assert_eq!(client.base_url, KALSHI_REST_URL_DEMO);
    }

    #[test]
    fn test_rest_client_ws_url_production() {
        let client = KalshiRestClient::new(None);
        assert_eq!(client.ws_url(), KALSHI_WS_URL);
    }

    #[test]
    fn test_rest_client_ws_url_demo() {
        let client = KalshiRestClient::new_demo(None);
        assert_eq!(client.ws_url(), KALSHI_WS_URL_DEMO);
    }

    // -------------------------------------------------------------------------
    // URL Constants Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_kalshi_urls_are_valid() {
        assert!(KALSHI_REST_URL.starts_with("https://"));
        assert!(KALSHI_REST_URL_DEMO.starts_with("https://"));
        assert!(KALSHI_WS_URL.starts_with("wss://"));
        assert!(KALSHI_WS_URL_DEMO.starts_with("wss://"));
    }

    #[test]
    fn test_demo_urls_contain_demo() {
        assert!(KALSHI_REST_URL_DEMO.contains("demo"));
        assert!(KALSHI_WS_URL_DEMO.contains("demo"));
    }

    // -------------------------------------------------------------------------
    // Timestamp Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_timestamp_is_reasonable() {
        let ts = KalshiAuth::timestamp();
        // Should be after 2024-01-01 and before 2100-01-01
        let min_ts: u64 = 1704067200000; // 2024-01-01
        let max_ts: u64 = 4102444800000; // 2100-01-01
        assert!(ts > min_ts, "timestamp {} is too old", ts);
        assert!(ts < max_ts, "timestamp {} is too far in future", ts);
    }

    #[test]
    fn test_timestamp_increases() {
        let ts1 = KalshiAuth::timestamp();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let ts2 = KalshiAuth::timestamp();
        assert!(ts2 >= ts1, "timestamp should increase");
    }
}
