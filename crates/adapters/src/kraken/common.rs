use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

// Kraken API base URLs
pub const KRAKEN_SPOT_REST_URL: &str = "https://api.kraken.com";
pub const KRAKEN_SPOT_WS_URL: &str = "wss://ws.kraken.com/v2";
pub const KRAKEN_SPOT_WS_AUTH_URL: &str = "wss://ws-auth.kraken.com/v2";
pub const KRAKEN_FUTURES_REST_URL: &str = "https://futures.kraken.com/derivatives/api/v3";
pub const KRAKEN_FUTURES_WS_URL: &str = "wss://futures.kraken.com/ws/v1";

/// Kraken API credentials
#[derive(Clone)]
pub struct KrakenAuth {
    pub api_key: String,
    pub api_secret: String,
}

impl KrakenAuth {
    pub fn new(api_key: String, api_secret: String) -> Self {
        Self { api_key, api_secret }
    }

    /// Generate HMAC-SHA512 signature for Kraken REST API
    pub fn sign_request(&self, path: &str, nonce: u64, postdata: &str) -> Result<String> {
        use base64::Engine;
        use base64::engine::general_purpose;
        use hmac::{Hmac, Mac};
        use sha2::{Digest, Sha256, Sha512};

        // Decode API secret from base64
        let decoded_secret = general_purpose::STANDARD.decode(&self.api_secret)
            .context("Failed to decode API secret")?;

        // Create SHA256 hash of (nonce + postdata)
        let mut sha256 = Sha256::new();
        sha256.update(format!("{}{}", nonce, postdata));
        let sha256_result = sha256.finalize();

        // Concatenate path + sha256 hash
        let mut message = path.as_bytes().to_vec();
        message.extend_from_slice(&sha256_result);

        // Create HMAC-SHA512 signature
        let mut mac = Hmac::<Sha512>::new_from_slice(&decoded_secret)
            .context("Failed to create HMAC")?;
        mac.update(&message);
        let signature = mac.finalize().into_bytes();

        // Encode signature as base64
        Ok(general_purpose::STANDARD.encode(signature))
    }

    /// Get current nonce (milliseconds since epoch)
    pub fn get_nonce() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
}

/// HTTP client wrapper for Kraken REST API
pub struct KrakenRestClient {
    client: Client,
    auth: Option<KrakenAuth>,
    base_url: String,
}

impl KrakenRestClient {
    pub fn new_spot(auth: Option<KrakenAuth>) -> Self {
        Self {
            client: Client::new(),
            auth,
            base_url: KRAKEN_SPOT_REST_URL.to_string(),
        }
    }

    pub fn new_futures(auth: Option<KrakenAuth>) -> Self {
        Self {
            client: Client::new(),
            auth,
            base_url: KRAKEN_FUTURES_REST_URL.to_string(),
        }
    }

    /// Make a public GET request
    pub async fn get_public<T: for<'de> Deserialize<'de>>(
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

    /// Make an authenticated POST request (for spot)
    pub async fn post_private<T: for<'de> Deserialize<'de>>(
        &self,
        endpoint: &str,
        params: HashMap<String, String>,
    ) -> Result<T> {
        let auth = self.auth.as_ref()
            .context("Authentication required for private endpoints")?;

        let nonce = KrakenAuth::get_nonce();
        let mut all_params = params.clone();
        all_params.insert("nonce".to_string(), nonce.to_string());

        let postdata = serde_urlencoded::to_string(&all_params)?;
        let signature = auth.sign_request(endpoint, nonce, &postdata)?;

        let url = format!("{}{}", self.base_url, endpoint);
        let response = self.client
            .post(&url)
            .header("API-Key", &auth.api_key)
            .header("API-Sign", signature)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(postdata)
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

/// Standard Kraken API response wrapper
#[derive(Debug, Deserialize)]
pub struct KrakenResponse<T> {
    pub error: Vec<String>,
    pub result: Option<T>,
}

impl<T> KrakenResponse<T> {
    pub fn into_result(self) -> Result<T> {
        if !self.error.is_empty() {
            anyhow::bail!("Kraken API error: {:?}", self.error);
        }
        self.result.context("Missing result in API response")
    }
}

/// WebSocket message types
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "method")]
pub enum WsMessage {
    #[serde(rename = "subscribe")]
    Subscribe {
        params: SubscribeParams,
        #[serde(skip_serializing_if = "Option::is_none")]
        req_id: Option<u64>,
    },
    #[serde(rename = "unsubscribe")]
    Unsubscribe {
        params: SubscribeParams,
        #[serde(skip_serializing_if = "Option::is_none")]
        req_id: Option<u64>,
    },
    #[serde(rename = "add_order")]
    AddOrder {
        params: AddOrderParams,
        req_id: u64,
    },
    #[serde(rename = "cancel_order")]
    CancelOrder {
        params: CancelOrderParams,
        req_id: u64,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SubscribeParams {
    pub channel: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AddOrderParams {
    pub order_type: String,
    pub side: String,
    pub order_qty: f64,
    pub symbol: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit_price: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_price: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_in_force: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub post_only: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reduce_only: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cl_ord_id: Option<String>,
    pub token: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CancelOrderParams {
    pub order_id: Vec<String>,
    pub token: String,
}

// Utility functions for converting between Kraken and internal types
pub mod converters {
    use crate::traits::{OrderType, Side, TimeInForce};

    pub fn to_kraken_order_type(order_type: OrderType) -> &'static str {
        match order_type {
            OrderType::Limit => "limit",
            OrderType::Market => "market",
            OrderType::StopLoss => "stop-loss",
            OrderType::StopLossLimit => "stop-loss-limit",
            OrderType::TakeProfit => "take-profit",
            OrderType::TakeProfitLimit => "take-profit-limit",
        }
    }

    pub fn from_kraken_order_type(order_type: &str) -> OrderType {
        match order_type {
            "limit" => OrderType::Limit,
            "market" => OrderType::Market,
            "stop-loss" => OrderType::StopLoss,
            "stop-loss-limit" => OrderType::StopLossLimit,
            "take-profit" => OrderType::TakeProfit,
            "take-profit-limit" => OrderType::TakeProfitLimit,
            _ => OrderType::Limit, // Default fallback
        }
    }

    pub fn to_kraken_side(side: Side) -> &'static str {
        match side {
            Side::Buy => "buy",
            Side::Sell => "sell",
        }
    }

    pub fn from_kraken_side(side: &str) -> Side {
        match side.to_lowercase().as_str() {
            "buy" => Side::Buy,
            "sell" => Side::Sell,
            _ => Side::Buy, // Default fallback
        }
    }

    pub fn to_kraken_tif(tif: TimeInForce) -> &'static str {
        match tif {
            TimeInForce::Gtc => "GTC",
            TimeInForce::Ioc => "IOC",
            TimeInForce::Fok => "FOK",
        }
    }

    pub fn from_kraken_tif(tif: &str) -> TimeInForce {
        match tif {
            "GTC" => TimeInForce::Gtc,
            "IOC" => TimeInForce::Ioc,
            "FOK" => TimeInForce::Fok,
            _ => TimeInForce::Gtc, // Default fallback
        }
    }
}
