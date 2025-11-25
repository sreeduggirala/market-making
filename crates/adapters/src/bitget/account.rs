//! Bitget Authentication and REST Client
//!
//! Provides shared authentication, HTTP client, and type converters for Bitget API.
//!
//! # Authentication
//!
//! Bitget uses HMAC-SHA256 signing with Base64 encoding:
//! - Sign string: timestamp + method + requestPath + queryString + body
//! - Headers: ACCESS-KEY, ACCESS-SIGN, ACCESS-TIMESTAMP, ACCESS-PASSPHRASE

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

/// Bitget REST API base URL
pub const BITGET_REST_URL: &str = "https://api.bitget.com";

/// Bitget WebSocket public URL (spot)
pub const BITGET_WS_PUBLIC_SPOT_URL: &str = "wss://ws.bitget.com/v2/ws/public";

/// Bitget WebSocket private URL (spot)
pub const BITGET_WS_PRIVATE_SPOT_URL: &str = "wss://ws.bitget.com/v2/ws/private";

/// Bitget WebSocket public URL (futures)
pub const BITGET_WS_PUBLIC_MIX_URL: &str = "wss://ws.bitget.com/v2/ws/public";

/// Bitget WebSocket private URL (futures)
pub const BITGET_WS_PRIVATE_MIX_URL: &str = "wss://ws.bitget.com/v2/ws/private";

// =============================================================================
// Authentication
// =============================================================================

/// Bitget API authentication credentials
#[derive(Clone)]
pub struct BitgetAuth {
    pub api_key: String,
    pub api_secret: String,
    pub passphrase: String,
}

impl BitgetAuth {
    /// Creates new authentication credentials
    pub fn new(api_key: String, api_secret: String, passphrase: String) -> Self {
        Self {
            api_key,
            api_secret,
            passphrase,
        }
    }

    /// Generates current timestamp in milliseconds as string
    pub fn timestamp() -> String {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis().to_string())
            .unwrap_or_default()
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
    /// For GET requests with query params: timestamp + method + requestPath + "?" + queryString
    pub fn sign(&self, timestamp: &str, method: &str, request_path: &str, body: &str) -> String {
        let sign_str = format!("{}{}{}{}", timestamp, method.to_uppercase(), request_path, body);

        let mut mac = Hmac::<Sha256>::new_from_slice(self.api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(sign_str.as_bytes());

        base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
    }

    /// Signs for WebSocket authentication
    pub fn sign_websocket(&self, timestamp: &str) -> String {
        let sign_str = format!("{}GET/user/verify", timestamp);

        let mut mac = Hmac::<Sha256>::new_from_slice(self.api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(sign_str.as_bytes());

        base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
    }
}

// =============================================================================
// REST Client
// =============================================================================

/// HTTP client for Bitget REST API
#[derive(Clone)]
pub struct BitgetRestClient {
    client: Client,
    auth: Option<BitgetAuth>,
    base_url: String,
}

impl BitgetRestClient {
    /// Creates a new Bitget REST client
    pub fn new(auth: Option<BitgetAuth>) -> Self {
        let client = Client::builder()
            .pool_max_idle_per_host(10)
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap_or_else(|_| Client::new());

        Self {
            client,
            auth,
            base_url: BITGET_REST_URL.to_string(),
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
            anyhow::bail!("Bitget API error ({}): {}", status, body);
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
        let timestamp = BitgetAuth::timestamp();

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
            .header("ACCESS-KEY", &auth.api_key)
            .header("ACCESS-SIGN", signature)
            .header("ACCESS-TIMESTAMP", &timestamp)
            .header("ACCESS-PASSPHRASE", &auth.passphrase)
            .header("Content-Type", "application/json")
            .header("locale", "en-US");

        let response = request.send().await.context("Failed to send request")?;
        let status = response.status();
        let body = response.text().await.context("Failed to read response")?;

        if !status.is_success() {
            anyhow::bail!("Bitget API error ({}): {}", status, body);
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
        let timestamp = BitgetAuth::timestamp();
        let body_str = serde_json::to_string(body)?;

        let signature = auth.sign(&timestamp, "POST", endpoint, &body_str);

        let url = format!("{}{}", self.base_url, endpoint);
        let request = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("ACCESS-KEY", &auth.api_key)
            .header("ACCESS-SIGN", signature)
            .header("ACCESS-TIMESTAMP", &timestamp)
            .header("ACCESS-PASSPHRASE", &auth.passphrase)
            .header("locale", "en-US")
            .body(body_str);

        let response = request.send().await.context("Failed to send request")?;
        let status = response.status();
        let body = response.text().await.context("Failed to read response")?;

        if !status.is_success() {
            anyhow::bail!("Bitget API error ({}): {}", status, body);
        }

        serde_json::from_str(&body).context("Failed to parse response")
    }
}

// =============================================================================
// Response Types
// =============================================================================

/// Bitget API response wrapper
#[derive(Debug, Deserialize)]
pub struct BitgetResponse<T> {
    pub code: String,
    pub msg: String,
    pub data: Option<T>,
    #[serde(rename = "requestTime")]
    pub request_time: Option<u64>,
}

impl<T> BitgetResponse<T> {
    /// Check if response is successful
    pub fn is_ok(&self) -> bool {
        self.code == "00000"
    }

    /// Extract data from successful response
    pub fn into_result(self) -> Result<T> {
        if !self.is_ok() {
            anyhow::bail!("Bitget API error ({}): {}", self.code, self.msg);
        }
        self.data.context("Missing data in response")
    }
}

// =============================================================================
// Type Converters
// =============================================================================

pub mod converters {
    use crate::traits::{MarginMode, OrderStatus, OrderType, Side, TimeInForce};

    /// Convert our Side to Bitget side
    pub fn to_bitget_side(side: Side) -> &'static str {
        match side {
            Side::Buy => "buy",
            Side::Sell => "sell",
        }
    }

    /// Convert Bitget side to our Side
    pub fn from_bitget_side(side: &str) -> Side {
        match side.to_lowercase().as_str() {
            "buy" => Side::Buy,
            "sell" | _ => Side::Sell,
        }
    }

    /// Convert our OrderType to Bitget order type
    pub fn to_bitget_order_type(order_type: OrderType) -> &'static str {
        match order_type {
            OrderType::Limit => "limit",
            OrderType::Market => "market",
            OrderType::StopLoss | OrderType::StopLossLimit => "limit",
            OrderType::TakeProfit | OrderType::TakeProfitLimit => "limit",
        }
    }

    /// Convert Bitget order type to our OrderType
    pub fn from_bitget_order_type(order_type: &str) -> OrderType {
        match order_type.to_lowercase().as_str() {
            "market" => OrderType::Market,
            "limit" | _ => OrderType::Limit,
        }
    }

    /// Convert our TimeInForce to Bitget TIF
    pub fn to_bitget_tif(tif: TimeInForce) -> &'static str {
        match tif {
            TimeInForce::Gtc => "gtc",
            TimeInForce::Ioc => "ioc",
            TimeInForce::Fok => "fok",
        }
    }

    /// Convert Bitget TIF to our TimeInForce
    pub fn from_bitget_tif(tif: &str) -> TimeInForce {
        match tif.to_lowercase().as_str() {
            "ioc" => TimeInForce::Ioc,
            "fok" => TimeInForce::Fok,
            "gtc" | _ => TimeInForce::Gtc,
        }
    }

    /// Convert Bitget order state to our OrderStatus
    pub fn from_bitget_order_status(state: &str) -> OrderStatus {
        match state.to_lowercase().as_str() {
            "live" | "new" | "init" => OrderStatus::New,
            "partially_filled" | "partial_fill" => OrderStatus::PartiallyFilled,
            "filled" | "full_fill" => OrderStatus::Filled,
            "canceled" | "cancelled" => OrderStatus::Canceled,
            _ => OrderStatus::New,
        }
    }

    /// Convert our MarginMode to Bitget margin mode
    pub fn to_bitget_margin_mode(mode: MarginMode) -> &'static str {
        match mode {
            MarginMode::Cross => "crossed",
            MarginMode::Isolated => "isolated",
        }
    }

    /// Convert Bitget margin mode to our MarginMode
    pub fn from_bitget_margin_mode(mode: &str) -> MarginMode {
        match mode.to_lowercase().as_str() {
            "isolated" => MarginMode::Isolated,
            "crossed" | "cross" | _ => MarginMode::Cross,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::{OrderType, Side, TimeInForce};

    #[test]
    fn test_side_conversion() {
        assert_eq!(converters::to_bitget_side(Side::Buy), "buy");
        assert_eq!(converters::to_bitget_side(Side::Sell), "sell");
        assert!(matches!(converters::from_bitget_side("buy"), Side::Buy));
        assert!(matches!(converters::from_bitget_side("sell"), Side::Sell));
    }

    #[test]
    fn test_order_type_conversion() {
        assert_eq!(converters::to_bitget_order_type(OrderType::Limit), "limit");
        assert_eq!(
            converters::to_bitget_order_type(OrderType::Market),
            "market"
        );
    }

    #[test]
    fn test_tif_conversion() {
        assert_eq!(converters::to_bitget_tif(TimeInForce::Gtc), "gtc");
        assert_eq!(converters::to_bitget_tif(TimeInForce::Ioc), "ioc");
        assert_eq!(converters::to_bitget_tif(TimeInForce::Fok), "fok");
    }

    #[test]
    fn test_signature() {
        let auth = BitgetAuth::new(
            "test_key".to_string(),
            "test_secret".to_string(),
            "test_pass".to_string(),
        );
        let signature = auth.sign("1234567890", "GET", "/api/v2/spot/account/assets", "");
        assert!(!signature.is_empty());
    }
}
