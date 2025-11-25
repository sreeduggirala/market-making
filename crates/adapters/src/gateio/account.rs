//! Gate.io Authentication and REST Client
//!
//! Provides shared authentication, HTTP client, and type converters for Gate.io API.
//!
//! # Authentication
//!
//! Gate.io uses HMAC-SHA512 signing:
//! - Sign string: method\nurl\nquery_string\nhashed_body\ntimestamp
//! - Headers: KEY, Timestamp, SIGN

use anyhow::{Context, Result};
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha512};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

// =============================================================================
// API Endpoints
// =============================================================================

/// Gate.io REST API base URL
pub const GATEIO_REST_URL: &str = "https://api.gateio.ws";

/// Gate.io API version prefix
pub const GATEIO_API_PREFIX: &str = "/api/v4";

/// Gate.io WebSocket spot URL
pub const GATEIO_WS_SPOT_URL: &str = "wss://api.gateio.ws/ws/v4/";

/// Gate.io WebSocket futures URL
pub const GATEIO_WS_FUTURES_URL: &str = "wss://fx-ws.gateio.ws/v4/ws/usdt";

// =============================================================================
// Authentication
// =============================================================================

/// Gate.io API authentication credentials
#[derive(Clone)]
pub struct GateioAuth {
    pub api_key: String,
    pub api_secret: String,
}

impl GateioAuth {
    /// Creates new authentication credentials
    pub fn new(api_key: String, api_secret: String) -> Self {
        Self {
            api_key,
            api_secret,
        }
    }

    /// Generates current timestamp in seconds
    pub fn timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    /// Generates current timestamp in milliseconds
    pub fn timestamp_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    /// Hashes request body using SHA512
    pub fn hash_body(body: &str) -> String {
        let mut hasher = Sha512::new();
        hasher.update(body.as_bytes());
        hex::encode(hasher.finalize())
    }

    /// Signs a request using HMAC-SHA512
    ///
    /// Sign string format: method\nurl\nquery_string\nhashed_body\ntimestamp
    pub fn sign(
        &self,
        method: &str,
        url: &str,
        query_string: &str,
        body: &str,
        timestamp: &str,
    ) -> String {
        let hashed_body = Self::hash_body(body);
        let sign_str = format!(
            "{}\n{}\n{}\n{}\n{}",
            method, url, query_string, hashed_body, timestamp
        );

        let mut mac = Hmac::<Sha512>::new_from_slice(self.api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(sign_str.as_bytes());

        hex::encode(mac.finalize().into_bytes())
    }
}

// =============================================================================
// REST Client
// =============================================================================

/// HTTP client for Gate.io REST API
#[derive(Clone)]
pub struct GateioRestClient {
    client: Client,
    auth: Option<GateioAuth>,
    base_url: String,
}

impl GateioRestClient {
    /// Creates a new Gate.io REST client
    pub fn new(auth: Option<GateioAuth>) -> Self {
        let client = Client::builder()
            .pool_max_idle_per_host(10)
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap_or_else(|_| Client::new());

        Self {
            client,
            auth,
            base_url: GATEIO_REST_URL.to_string(),
        }
    }

    /// Makes a GET request to a public endpoint
    pub async fn get_public<T: DeserializeOwned>(
        &self,
        endpoint: &str,
        params: Option<HashMap<String, String>>,
    ) -> Result<T> {
        let url = format!("{}{}{}", self.base_url, GATEIO_API_PREFIX, endpoint);

        let mut request = self.client.get(&url);
        if let Some(p) = params {
            request = request.query(&p);
        }

        let response = request.send().await.context("Failed to send request")?;
        let status = response.status();
        let body = response.text().await.context("Failed to read response")?;

        if !status.is_success() {
            anyhow::bail!("Gate.io API error ({}): {}", status, body);
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
        let timestamp = GateioAuth::timestamp().to_string();

        // Build query string
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

        let api_url = format!("{}{}", GATEIO_API_PREFIX, endpoint);
        let signature = auth.sign("GET", &api_url, &query_string, "", &timestamp);

        let url = format!("{}{}", self.base_url, api_url);
        let mut request = self.client.get(&url);

        if let Some(p) = params {
            request = request.query(&p);
        }

        request = request
            .header("KEY", &auth.api_key)
            .header("Timestamp", &timestamp)
            .header("SIGN", signature);

        let response = request.send().await.context("Failed to send request")?;
        let status = response.status();
        let body = response.text().await.context("Failed to read response")?;

        if !status.is_success() {
            anyhow::bail!("Gate.io API error ({}): {}", status, body);
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
        let timestamp = GateioAuth::timestamp().to_string();
        let body_str = serde_json::to_string(body)?;

        let api_url = format!("{}{}", GATEIO_API_PREFIX, endpoint);
        let signature = auth.sign("POST", &api_url, "", &body_str, &timestamp);

        let url = format!("{}{}", self.base_url, api_url);
        let request = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("KEY", &auth.api_key)
            .header("Timestamp", &timestamp)
            .header("SIGN", signature)
            .body(body_str);

        let response = request.send().await.context("Failed to send request")?;
        let status = response.status();
        let body = response.text().await.context("Failed to read response")?;

        if !status.is_success() {
            anyhow::bail!("Gate.io API error ({}): {}", status, body);
        }

        serde_json::from_str(&body).context("Failed to parse response")
    }

    /// Makes a DELETE request to a private endpoint
    pub async fn delete_private<T: DeserializeOwned>(
        &self,
        endpoint: &str,
        params: Option<HashMap<String, String>>,
    ) -> Result<T> {
        let auth = self.auth.as_ref().context("Authentication required")?;
        let timestamp = GateioAuth::timestamp().to_string();

        // Build query string
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

        let api_url = format!("{}{}", GATEIO_API_PREFIX, endpoint);
        let signature = auth.sign("DELETE", &api_url, &query_string, "", &timestamp);

        let url = format!("{}{}", self.base_url, api_url);
        let mut request = self.client.delete(&url);

        if let Some(p) = params {
            request = request.query(&p);
        }

        request = request
            .header("KEY", &auth.api_key)
            .header("Timestamp", &timestamp)
            .header("SIGN", signature);

        let response = request.send().await.context("Failed to send request")?;
        let status = response.status();
        let body = response.text().await.context("Failed to read response")?;

        if !status.is_success() {
            anyhow::bail!("Gate.io API error ({}): {}", status, body);
        }

        serde_json::from_str(&body).context("Failed to parse response")
    }
}

// =============================================================================
// Response Types
// =============================================================================

/// Gate.io error response
#[derive(Debug, Deserialize)]
pub struct GateioError {
    pub label: String,
    pub message: String,
}

// =============================================================================
// Type Converters
// =============================================================================

pub mod converters {
    use crate::traits::{MarginMode, OrderStatus, OrderType, Side, TimeInForce};

    /// Convert our Side to Gate.io side
    pub fn to_gateio_side(side: Side) -> &'static str {
        match side {
            Side::Buy => "buy",
            Side::Sell => "sell",
        }
    }

    /// Convert Gate.io side to our Side
    pub fn from_gateio_side(side: &str) -> Side {
        match side.to_lowercase().as_str() {
            "buy" => Side::Buy,
            "sell" | _ => Side::Sell,
        }
    }

    /// Convert our OrderType to Gate.io order type
    pub fn to_gateio_order_type(order_type: OrderType) -> &'static str {
        match order_type {
            OrderType::Limit => "limit",
            OrderType::Market => "market",
            OrderType::StopLoss | OrderType::StopLossLimit => "limit",
            OrderType::TakeProfit | OrderType::TakeProfitLimit => "limit",
        }
    }

    /// Convert Gate.io order type to our OrderType
    pub fn from_gateio_order_type(order_type: &str) -> OrderType {
        match order_type.to_lowercase().as_str() {
            "market" => OrderType::Market,
            "limit" | _ => OrderType::Limit,
        }
    }

    /// Convert our TimeInForce to Gate.io TIF
    pub fn to_gateio_tif(tif: TimeInForce) -> &'static str {
        match tif {
            TimeInForce::Gtc => "gtc",
            TimeInForce::Ioc => "ioc",
            TimeInForce::Fok => "fok",
        }
    }

    /// Convert Gate.io TIF to our TimeInForce
    pub fn from_gateio_tif(tif: &str) -> TimeInForce {
        match tif.to_lowercase().as_str() {
            "ioc" => TimeInForce::Ioc,
            "fok" => TimeInForce::Fok,
            "gtc" | _ => TimeInForce::Gtc,
        }
    }

    /// Convert Gate.io order status to our OrderStatus
    pub fn from_gateio_order_status(status: &str) -> OrderStatus {
        match status.to_lowercase().as_str() {
            "open" => OrderStatus::New,
            "closed" => OrderStatus::Filled,
            "cancelled" => OrderStatus::Canceled,
            _ => OrderStatus::New,
        }
    }

    /// Convert our MarginMode to Gate.io margin mode
    pub fn to_gateio_margin_mode(mode: MarginMode) -> &'static str {
        match mode {
            MarginMode::Cross => "cross",
            MarginMode::Isolated => "single",
        }
    }

    /// Convert Gate.io margin mode to our MarginMode
    pub fn from_gateio_margin_mode(mode: &str) -> MarginMode {
        match mode.to_lowercase().as_str() {
            "single" => MarginMode::Isolated,
            "cross" | _ => MarginMode::Cross,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::{OrderType, Side, TimeInForce};

    #[test]
    fn test_side_conversion() {
        assert_eq!(converters::to_gateio_side(Side::Buy), "buy");
        assert_eq!(converters::to_gateio_side(Side::Sell), "sell");
        assert!(matches!(converters::from_gateio_side("buy"), Side::Buy));
        assert!(matches!(converters::from_gateio_side("sell"), Side::Sell));
    }

    #[test]
    fn test_order_type_conversion() {
        assert_eq!(converters::to_gateio_order_type(OrderType::Limit), "limit");
        assert_eq!(converters::to_gateio_order_type(OrderType::Market), "market");
    }

    #[test]
    fn test_tif_conversion() {
        assert_eq!(converters::to_gateio_tif(TimeInForce::Gtc), "gtc");
        assert_eq!(converters::to_gateio_tif(TimeInForce::Ioc), "ioc");
        assert_eq!(converters::to_gateio_tif(TimeInForce::Fok), "fok");
    }

    #[test]
    fn test_body_hash() {
        let hash = GateioAuth::hash_body("");
        // SHA512 of empty string
        assert_eq!(hash.len(), 128); // 64 bytes = 128 hex chars
    }
}
