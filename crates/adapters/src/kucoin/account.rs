//! KuCoin Authentication and REST Client
//!
//! Provides shared authentication, HTTP client, and type converters for KuCoin API.
//!
//! # Authentication
//!
//! KuCoin uses HMAC-SHA256 signing with Base64 encoding:
//! - Sign string: timestamp + method + requestPath + body
//! - Headers: KC-API-KEY, KC-API-SIGN, KC-API-TIMESTAMP, KC-API-PASSPHRASE
//! - KC-API-KEY-VERSION: "2" (passphrase is HMAC-SHA256 hashed)

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

/// KuCoin Spot REST API base URL
pub const KUCOIN_SPOT_REST_URL: &str = "https://api.kucoin.com";

/// KuCoin Futures REST API base URL
pub const KUCOIN_FUTURES_REST_URL: &str = "https://api-futures.kucoin.com";

/// KuCoin Spot WebSocket public URL (obtained via POST /api/v1/bullet-public)
pub const KUCOIN_SPOT_WS_PUBLIC_ENDPOINT: &str = "/api/v1/bullet-public";

/// KuCoin Spot WebSocket private URL (obtained via POST /api/v1/bullet-private)
pub const KUCOIN_SPOT_WS_PRIVATE_ENDPOINT: &str = "/api/v1/bullet-private";

/// KuCoin Futures WebSocket public URL (obtained via POST /api/v1/bullet-public)
pub const KUCOIN_FUTURES_WS_PUBLIC_ENDPOINT: &str = "/api/v1/bullet-public";

/// KuCoin Futures WebSocket private URL (obtained via POST /api/v1/bullet-private)
pub const KUCOIN_FUTURES_WS_PRIVATE_ENDPOINT: &str = "/api/v1/bullet-private";

// =============================================================================
// Authentication
// =============================================================================

/// KuCoin API authentication credentials
#[derive(Clone)]
pub struct KucoinAuth {
    pub api_key: String,
    pub api_secret: String,
    pub passphrase: String,
    pub hashed_passphrase: String,
}

impl KucoinAuth {
    /// Creates new authentication credentials
    pub fn new(api_key: String, api_secret: String, passphrase: String) -> Self {
        // Hash the passphrase with HMAC-SHA256
        let mut mac = Hmac::<Sha256>::new_from_slice(api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(passphrase.as_bytes());
        let hashed_passphrase =
            base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());

        Self {
            api_key,
            api_secret,
            passphrase,
            hashed_passphrase,
        }
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
}

// =============================================================================
// REST Client
// =============================================================================

/// HTTP client for KuCoin REST API
#[derive(Clone)]
pub struct KucoinRestClient {
    client: Client,
    auth: Option<KucoinAuth>,
    base_url: String,
}

impl KucoinRestClient {
    /// Creates a new KuCoin Spot REST client
    pub fn spot(auth: Option<KucoinAuth>) -> Self {
        let client = Client::builder()
            .pool_max_idle_per_host(10)
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap_or_else(|_| Client::new());

        Self {
            client,
            auth,
            base_url: KUCOIN_SPOT_REST_URL.to_string(),
        }
    }

    /// Creates a new KuCoin Futures REST client
    pub fn futures(auth: Option<KucoinAuth>) -> Self {
        let client = Client::builder()
            .pool_max_idle_per_host(10)
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap_or_else(|_| Client::new());

        Self {
            client,
            auth,
            base_url: KUCOIN_FUTURES_REST_URL.to_string(),
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
            anyhow::bail!("KuCoin API error ({}): {}", status, body);
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
        let timestamp = KucoinAuth::timestamp_ms().to_string();

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
            .header("KC-API-KEY", &auth.api_key)
            .header("KC-API-SIGN", signature)
            .header("KC-API-TIMESTAMP", &timestamp)
            .header("KC-API-PASSPHRASE", &auth.hashed_passphrase)
            .header("KC-API-KEY-VERSION", "2");

        let response = request.send().await.context("Failed to send request")?;
        let status = response.status();
        let body = response.text().await.context("Failed to read response")?;

        if !status.is_success() {
            anyhow::bail!("KuCoin API error ({}): {}", status, body);
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
        let timestamp = KucoinAuth::timestamp_ms().to_string();
        let body_str = serde_json::to_string(body)?;

        let signature = auth.sign(&timestamp, "POST", endpoint, &body_str);

        let url = format!("{}{}", self.base_url, endpoint);
        let request = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("KC-API-KEY", &auth.api_key)
            .header("KC-API-SIGN", signature)
            .header("KC-API-TIMESTAMP", &timestamp)
            .header("KC-API-PASSPHRASE", &auth.hashed_passphrase)
            .header("KC-API-KEY-VERSION", "2")
            .body(body_str);

        let response = request.send().await.context("Failed to send request")?;
        let status = response.status();
        let body = response.text().await.context("Failed to read response")?;

        if !status.is_success() {
            anyhow::bail!("KuCoin API error ({}): {}", status, body);
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
        let timestamp = KucoinAuth::timestamp_ms().to_string();

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

        let signature = auth.sign(&timestamp, "DELETE", &request_path, "");

        let url = format!("{}{}", self.base_url, endpoint);
        let mut request = self.client.delete(&url);

        if let Some(p) = params {
            request = request.query(&p);
        }

        request = request
            .header("KC-API-KEY", &auth.api_key)
            .header("KC-API-SIGN", signature)
            .header("KC-API-TIMESTAMP", &timestamp)
            .header("KC-API-PASSPHRASE", &auth.hashed_passphrase)
            .header("KC-API-KEY-VERSION", "2");

        let response = request.send().await.context("Failed to send request")?;
        let status = response.status();
        let body = response.text().await.context("Failed to read response")?;

        if !status.is_success() {
            anyhow::bail!("KuCoin API error ({}): {}", status, body);
        }

        serde_json::from_str(&body).context("Failed to parse response")
    }

    /// Gets WebSocket connection token (public)
    pub async fn get_ws_public_token(&self) -> Result<WsTokenResponse> {
        let endpoint = if self.base_url == KUCOIN_SPOT_REST_URL {
            KUCOIN_SPOT_WS_PUBLIC_ENDPOINT
        } else {
            KUCOIN_FUTURES_WS_PUBLIC_ENDPOINT
        };

        let url = format!("{}{}", self.base_url, endpoint);
        let response = self
            .client
            .post(&url)
            .send()
            .await
            .context("Failed to get WS token")?;

        let body = response.text().await?;
        let resp: KucoinResponse<WsTokenData> = serde_json::from_str(&body)?;
        resp.into_result()
            .map(|d| d.into())
            .context("Failed to get WS token")
    }

    /// Gets WebSocket connection token (private)
    pub async fn get_ws_private_token(&self) -> Result<WsTokenResponse> {
        let auth = self.auth.as_ref().context("Authentication required")?;
        let timestamp = KucoinAuth::timestamp_ms().to_string();

        let endpoint = if self.base_url == KUCOIN_SPOT_REST_URL {
            KUCOIN_SPOT_WS_PRIVATE_ENDPOINT
        } else {
            KUCOIN_FUTURES_WS_PRIVATE_ENDPOINT
        };

        let signature = auth.sign(&timestamp, "POST", endpoint, "");

        let url = format!("{}{}", self.base_url, endpoint);
        let response = self
            .client
            .post(&url)
            .header("KC-API-KEY", &auth.api_key)
            .header("KC-API-SIGN", signature)
            .header("KC-API-TIMESTAMP", &timestamp)
            .header("KC-API-PASSPHRASE", &auth.hashed_passphrase)
            .header("KC-API-KEY-VERSION", "2")
            .send()
            .await
            .context("Failed to get WS token")?;

        let body = response.text().await?;
        let resp: KucoinResponse<WsTokenData> = serde_json::from_str(&body)?;
        resp.into_result()
            .map(|d| d.into())
            .context("Failed to get WS token")
    }
}

// =============================================================================
// Response Types
// =============================================================================

/// KuCoin API response wrapper
#[derive(Debug, Deserialize)]
pub struct KucoinResponse<T> {
    pub code: String,
    #[serde(default)]
    pub msg: Option<String>,
    pub data: Option<T>,
}

impl<T> KucoinResponse<T> {
    /// Check if response is successful
    pub fn is_ok(&self) -> bool {
        self.code == "200000"
    }

    /// Extract data from successful response
    pub fn into_result(self) -> Result<T> {
        if !self.is_ok() {
            anyhow::bail!(
                "KuCoin API error ({}): {}",
                self.code,
                self.msg.unwrap_or_default()
            );
        }
        self.data.context("Missing data in response")
    }
}

/// WebSocket token data
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WsTokenData {
    pub token: String,
    pub instance_servers: Vec<WsInstanceServer>,
}

/// WebSocket instance server
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WsInstanceServer {
    pub endpoint: String,
    pub encrypt: bool,
    pub protocol: String,
    pub ping_interval: u64,
    pub ping_timeout: u64,
}

/// WebSocket token response
#[derive(Debug)]
pub struct WsTokenResponse {
    pub token: String,
    pub endpoint: String,
    pub ping_interval: u64,
}

impl From<WsTokenData> for WsTokenResponse {
    fn from(data: WsTokenData) -> Self {
        let server = data.instance_servers.first().cloned().unwrap_or(WsInstanceServer {
            endpoint: "wss://ws-api-spot.kucoin.com".to_string(),
            encrypt: true,
            protocol: "websocket".to_string(),
            ping_interval: 18000,
            ping_timeout: 10000,
        });

        Self {
            token: data.token,
            endpoint: server.endpoint,
            ping_interval: server.ping_interval,
        }
    }
}

// =============================================================================
// Type Converters
// =============================================================================

pub mod converters {
    use crate::traits::{MarginMode, OrderStatus, OrderType, Side, TimeInForce};

    /// Convert our Side to KuCoin side
    pub fn to_kucoin_side(side: Side) -> &'static str {
        match side {
            Side::Buy => "buy",
            Side::Sell => "sell",
        }
    }

    /// Convert KuCoin side to our Side
    pub fn from_kucoin_side(side: &str) -> Side {
        match side.to_lowercase().as_str() {
            "buy" => Side::Buy,
            "sell" | _ => Side::Sell,
        }
    }

    /// Convert our OrderType to KuCoin order type
    pub fn to_kucoin_order_type(order_type: OrderType) -> &'static str {
        match order_type {
            OrderType::Limit => "limit",
            OrderType::Market => "market",
            OrderType::StopLoss | OrderType::StopLossLimit => "limit",
            OrderType::TakeProfit | OrderType::TakeProfitLimit => "limit",
        }
    }

    /// Convert KuCoin order type to our OrderType
    pub fn from_kucoin_order_type(order_type: &str) -> OrderType {
        match order_type.to_lowercase().as_str() {
            "market" => OrderType::Market,
            "limit" | _ => OrderType::Limit,
        }
    }

    /// Convert our TimeInForce to KuCoin TIF
    pub fn to_kucoin_tif(tif: TimeInForce) -> &'static str {
        match tif {
            TimeInForce::Gtc => "GTC",
            TimeInForce::Ioc => "IOC",
            TimeInForce::Fok => "FOK",
        }
    }

    /// Convert KuCoin TIF to our TimeInForce
    pub fn from_kucoin_tif(tif: &str) -> TimeInForce {
        match tif.to_uppercase().as_str() {
            "IOC" => TimeInForce::Ioc,
            "FOK" => TimeInForce::Fok,
            "GTC" | _ => TimeInForce::Gtc,
        }
    }

    /// Convert KuCoin order status to our OrderStatus
    pub fn from_kucoin_order_status(status: &str, is_active: bool) -> OrderStatus {
        match status.to_lowercase().as_str() {
            "done" => OrderStatus::Filled,
            "match" | "open" if is_active => OrderStatus::PartiallyFilled,
            "active" => OrderStatus::New,
            "cancelled" => OrderStatus::Canceled,
            _ if is_active => OrderStatus::New,
            _ => OrderStatus::Filled,
        }
    }

    /// Convert our MarginMode to KuCoin margin mode
    pub fn to_kucoin_margin_mode(mode: MarginMode) -> &'static str {
        match mode {
            MarginMode::Cross => "CROSS",
            MarginMode::Isolated => "ISOLATED",
        }
    }

    /// Convert KuCoin margin mode to our MarginMode
    pub fn from_kucoin_margin_mode(mode: &str) -> MarginMode {
        match mode.to_uppercase().as_str() {
            "ISOLATED" => MarginMode::Isolated,
            "CROSS" | _ => MarginMode::Cross,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::{OrderType, Side, TimeInForce};

    #[test]
    fn test_side_conversion() {
        assert_eq!(converters::to_kucoin_side(Side::Buy), "buy");
        assert_eq!(converters::to_kucoin_side(Side::Sell), "sell");
        assert!(matches!(converters::from_kucoin_side("buy"), Side::Buy));
        assert!(matches!(converters::from_kucoin_side("sell"), Side::Sell));
    }

    #[test]
    fn test_order_type_conversion() {
        assert_eq!(converters::to_kucoin_order_type(OrderType::Limit), "limit");
        assert_eq!(
            converters::to_kucoin_order_type(OrderType::Market),
            "market"
        );
    }

    #[test]
    fn test_tif_conversion() {
        assert_eq!(converters::to_kucoin_tif(TimeInForce::Gtc), "GTC");
        assert_eq!(converters::to_kucoin_tif(TimeInForce::Ioc), "IOC");
        assert_eq!(converters::to_kucoin_tif(TimeInForce::Fok), "FOK");
    }

    #[test]
    fn test_passphrase_hashing() {
        let auth = KucoinAuth::new(
            "test_key".to_string(),
            "test_secret".to_string(),
            "test_pass".to_string(),
        );
        // Hashed passphrase should be different from original
        assert_ne!(auth.hashed_passphrase, auth.passphrase);
        // Hashed passphrase should be base64 encoded
        assert!(base64::engine::general_purpose::STANDARD
            .decode(&auth.hashed_passphrase)
            .is_ok());
    }
}
