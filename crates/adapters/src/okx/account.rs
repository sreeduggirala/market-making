//! OKX Authentication and REST Client
//!
//! Provides shared authentication, HTTP client, and type converters for OKX API.
//!
//! # Authentication
//!
//! OKX uses HMAC-SHA256 signing with Base64 encoding:
//! - Sign string: timestamp + method + requestPath + body
//! - Headers: OK-ACCESS-KEY, OK-ACCESS-SIGN, OK-ACCESS-TIMESTAMP, OK-ACCESS-PASSPHRASE

use anyhow::{Context, Result};
use base64::Engine;
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

// =============================================================================
// API Endpoints
// =============================================================================

/// OKX REST API base URL (mainnet)
pub const OKX_REST_URL: &str = "https://www.okx.com";

/// OKX REST API base URL (demo/testnet)
pub const OKX_DEMO_REST_URL: &str = "https://www.okx.com";

/// OKX WebSocket public URL
pub const OKX_WS_PUBLIC_URL: &str = "wss://ws.okx.com:8443/ws/v5/public";

/// OKX WebSocket private URL
pub const OKX_WS_PRIVATE_URL: &str = "wss://ws.okx.com:8443/ws/v5/private";

/// OKX WebSocket public URL (demo)
pub const OKX_DEMO_WS_PUBLIC_URL: &str = "wss://wspap.okx.com:8443/ws/v5/public?brokerId=9999";

/// OKX WebSocket private URL (demo)
pub const OKX_DEMO_WS_PRIVATE_URL: &str = "wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999";

// =============================================================================
// Authentication
// =============================================================================

/// OKX API authentication credentials
#[derive(Clone)]
pub struct OkxAuth {
    pub api_key: String,
    pub api_secret: String,
    pub passphrase: String,
    pub is_demo: bool,
}

impl OkxAuth {
    /// Creates new authentication credentials
    pub fn new(api_key: String, api_secret: String, passphrase: String) -> Self {
        Self {
            api_key,
            api_secret,
            passphrase,
            is_demo: false,
        }
    }

    /// Creates new authentication credentials for demo trading
    pub fn demo(api_key: String, api_secret: String, passphrase: String) -> Self {
        Self {
            api_key,
            api_secret,
            passphrase,
            is_demo: true,
        }
    }

    /// Generates ISO 8601 timestamp with milliseconds
    pub fn timestamp() -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let secs = now.as_secs();
        let millis = now.subsec_millis();

        // Convert to ISO 8601 format: 2020-12-08T09:08:57.715Z
        let dt = chrono::DateTime::from_timestamp(secs as i64, millis * 1_000_000)
            .unwrap_or_default();
        dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
    }

    /// Generates current timestamp in milliseconds
    pub fn timestamp_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    /// Signs a request using HMAC-SHA256
    ///
    /// Sign string format: timestamp + method + requestPath + body
    pub fn sign(&self, timestamp: &str, method: &str, request_path: &str, body: &str) -> String {
        let sign_str = format!("{}{}{}{}", timestamp, method, request_path, body);

        let mut mac = Hmac::<Sha256>::new_from_slice(self.api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(sign_str.as_bytes());

        base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
    }

    /// Signs for WebSocket authentication
    pub fn sign_websocket(&self, timestamp: &str) -> String {
        let sign_str = format!("{}GET/users/self/verify", timestamp);

        let mut mac = Hmac::<Sha256>::new_from_slice(self.api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(sign_str.as_bytes());

        base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
    }
}

// =============================================================================
// REST Client
// =============================================================================

/// HTTP client for OKX REST API
#[derive(Clone)]
pub struct OkxRestClient {
    client: Client,
    auth: Option<OkxAuth>,
    base_url: String,
}

impl OkxRestClient {
    /// Creates a new OKX REST client
    pub fn new(auth: Option<OkxAuth>) -> Self {
        let is_demo = auth.as_ref().map(|a| a.is_demo).unwrap_or(false);
        let base_url = if is_demo {
            OKX_DEMO_REST_URL
        } else {
            OKX_REST_URL
        };

        let client = Client::builder()
            .pool_max_idle_per_host(10)
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap_or_else(|_| Client::new());

        Self {
            client,
            auth,
            base_url: base_url.to_string(),
        }
    }

    /// Makes a GET request to a public endpoint
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
            anyhow::bail!("OKX API error ({}): {}", status, body);
        }

        serde_json::from_str(&body).context("Failed to parse response")
    }

    /// Makes a GET request to a private endpoint
    pub async fn get_private<T: DeserializeOwned>(
        &self,
        endpoint: &str,
        params: Option<HashMap<String, String>>,
    ) -> Result<T> {
        let auth = self.auth.as_ref().context("Authentication required")?;
        let timestamp = OkxAuth::timestamp();

        // Build query string for signature
        let query_string = params
            .as_ref()
            .map(|p| {
                let mut pairs: Vec<_> = p.iter().collect();
                pairs.sort_by_key(|(k, _)| *k);
                pairs
                    .into_iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect::<Vec<_>>()
                    .join("&")
            })
            .unwrap_or_default();

        let request_path = if query_string.is_empty() {
            endpoint.to_string()
        } else {
            format!("{}?{}", endpoint, query_string)
        };

        let signature = auth.sign(&timestamp, "GET", &request_path, "");

        let url = format!("{}{}", self.base_url, endpoint);
        let mut request = self.client.get(&url);

        if let Some(p) = params {
            request = request.query(&p);
        }

        request = request
            .header("OK-ACCESS-KEY", &auth.api_key)
            .header("OK-ACCESS-SIGN", signature)
            .header("OK-ACCESS-TIMESTAMP", &timestamp)
            .header("OK-ACCESS-PASSPHRASE", &auth.passphrase);

        if auth.is_demo {
            request = request.header("x-simulated-trading", "1");
        }

        let response = request.send().await.context("Failed to send request")?;
        let status = response.status();
        let body = response.text().await.context("Failed to read response")?;

        if !status.is_success() {
            anyhow::bail!("OKX API error ({}): {}", status, body);
        }

        serde_json::from_str(&body).context("Failed to parse response")
    }

    /// Makes a POST request to a private endpoint
    pub async fn post_private<T: DeserializeOwned>(
        &self,
        endpoint: &str,
        body: &impl Serialize,
    ) -> Result<T> {
        let auth = self.auth.as_ref().context("Authentication required")?;
        let timestamp = OkxAuth::timestamp();
        let body_str = serde_json::to_string(body)?;

        let signature = auth.sign(&timestamp, "POST", endpoint, &body_str);

        let url = format!("{}{}", self.base_url, endpoint);
        let mut request = self.client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("OK-ACCESS-KEY", &auth.api_key)
            .header("OK-ACCESS-SIGN", signature)
            .header("OK-ACCESS-TIMESTAMP", &timestamp)
            .header("OK-ACCESS-PASSPHRASE", &auth.passphrase)
            .body(body_str);

        if auth.is_demo {
            request = request.header("x-simulated-trading", "1");
        }

        let response = request.send().await.context("Failed to send request")?;
        let status = response.status();
        let body = response.text().await.context("Failed to read response")?;

        if !status.is_success() {
            anyhow::bail!("OKX API error ({}): {}", status, body);
        }

        serde_json::from_str(&body).context("Failed to parse response")
    }
}

// =============================================================================
// Response Types
// =============================================================================

/// OKX API response wrapper
#[derive(Debug, Deserialize)]
pub struct OkxResponse<T> {
    pub code: String,
    pub msg: String,
    pub data: Option<T>,
}

impl<T> OkxResponse<T> {
    /// Check if response is successful
    pub fn is_ok(&self) -> bool {
        self.code == "0"
    }

    /// Extract data from successful response
    pub fn into_result(self) -> Result<T> {
        if !self.is_ok() {
            anyhow::bail!("OKX API error ({}): {}", self.code, self.msg);
        }
        self.data.context("Missing data in response")
    }
}

// =============================================================================
// Type Converters
// =============================================================================

pub mod converters {
    use crate::traits::{OrderStatus, OrderType, Side, TimeInForce, MarginMode};

    /// Convert our Side to OKX side
    pub fn to_okx_side(side: Side) -> &'static str {
        match side {
            Side::Buy => "buy",
            Side::Sell => "sell",
        }
    }

    /// Convert OKX side to our Side
    pub fn from_okx_side(side: &str) -> Side {
        match side.to_lowercase().as_str() {
            "buy" => Side::Buy,
            "sell" | _ => Side::Sell,
        }
    }

    /// Convert our OrderType to OKX order type
    pub fn to_okx_order_type(order_type: OrderType) -> &'static str {
        match order_type {
            OrderType::Limit => "limit",
            OrderType::Market => "market",
            OrderType::StopLoss | OrderType::StopLossLimit => "conditional",
            OrderType::TakeProfit | OrderType::TakeProfitLimit => "conditional",
        }
    }

    /// Convert OKX order type to our OrderType
    pub fn from_okx_order_type(order_type: &str) -> OrderType {
        match order_type.to_lowercase().as_str() {
            "market" => OrderType::Market,
            "limit" | _ => OrderType::Limit,
        }
    }

    /// Convert our TimeInForce to OKX TIF
    pub fn to_okx_tif(tif: TimeInForce) -> &'static str {
        match tif {
            TimeInForce::Gtc => "GTC",
            TimeInForce::Ioc => "IOC",
            TimeInForce::Fok => "FOK",
        }
    }

    /// Convert OKX TIF to our TimeInForce
    pub fn from_okx_tif(tif: &str) -> TimeInForce {
        match tif.to_uppercase().as_str() {
            "IOC" => TimeInForce::Ioc,
            "FOK" => TimeInForce::Fok,
            "GTC" | _ => TimeInForce::Gtc,
        }
    }

    /// Convert OKX order state to our OrderStatus
    pub fn from_okx_order_status(state: &str) -> OrderStatus {
        match state.to_lowercase().as_str() {
            "live" => OrderStatus::New,
            "partially_filled" => OrderStatus::PartiallyFilled,
            "filled" => OrderStatus::Filled,
            "canceled" | "cancelled" => OrderStatus::Canceled,
            "mmp_canceled" => OrderStatus::Canceled,
            _ => OrderStatus::New,
        }
    }

    /// Convert our MarginMode to OKX margin mode
    pub fn to_okx_margin_mode(mode: MarginMode) -> &'static str {
        match mode {
            MarginMode::Cross => "cross",
            MarginMode::Isolated => "isolated",
        }
    }

    /// Convert OKX margin mode to our MarginMode
    pub fn from_okx_margin_mode(mode: &str) -> MarginMode {
        match mode.to_lowercase().as_str() {
            "isolated" => MarginMode::Isolated,
            "cross" | _ => MarginMode::Cross,
        }
    }

    /// Convert instrument type
    pub fn to_okx_inst_type(is_spot: bool) -> &'static str {
        if is_spot { "SPOT" } else { "SWAP" }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::{Side, OrderType, TimeInForce};

    #[test]
    fn test_side_conversion() {
        assert_eq!(converters::to_okx_side(Side::Buy), "buy");
        assert_eq!(converters::to_okx_side(Side::Sell), "sell");
        assert!(matches!(converters::from_okx_side("buy"), Side::Buy));
        assert!(matches!(converters::from_okx_side("sell"), Side::Sell));
    }

    #[test]
    fn test_order_type_conversion() {
        assert_eq!(converters::to_okx_order_type(OrderType::Limit), "limit");
        assert_eq!(converters::to_okx_order_type(OrderType::Market), "market");
    }

    #[test]
    fn test_tif_conversion() {
        assert_eq!(converters::to_okx_tif(TimeInForce::Gtc), "GTC");
        assert_eq!(converters::to_okx_tif(TimeInForce::Ioc), "IOC");
        assert_eq!(converters::to_okx_tif(TimeInForce::Fok), "FOK");
    }
}
