use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

// MEXC API base URLs
pub const MEXC_SPOT_REST_URL: &str = "https://api.mexc.com";
pub const MEXC_SPOT_WS_URL: &str = "wss://wbs.mexc.com/ws";
pub const MEXC_FUTURES_REST_URL: &str = "https://contract.mexc.com";
pub const MEXC_FUTURES_WS_URL: &str = "wss://contract.mexc.com/ws";

/// MEXC API credentials
#[derive(Clone)]
pub struct MexcAuth {
    pub api_key: String,
    pub api_secret: String,
}

impl MexcAuth {
    pub fn new(api_key: String, api_secret: String) -> Self {
        Self { api_key, api_secret }
    }

    /// Generate HMAC-SHA256 signature for MEXC REST API
    pub fn sign_request(&self, query_string: &str) -> Result<String> {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

        let mut mac = Hmac::<Sha256>::new_from_slice(self.api_secret.as_bytes())
            .context("Failed to create HMAC")?;

        mac.update(query_string.as_bytes());
        let signature = mac.finalize().into_bytes();

        Ok(hex::encode(signature))
    }

    /// Get current timestamp (milliseconds since epoch)
    pub fn get_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
}

/// HTTP client wrapper for MEXC REST API
pub struct MexcRestClient {
    client: Client,
    auth: Option<MexcAuth>,
    base_url: String,
}

impl MexcRestClient {
    pub fn new_spot(auth: Option<MexcAuth>) -> Self {
        Self {
            client: Client::new(),
            auth,
            base_url: MEXC_SPOT_REST_URL.to_string(),
        }
    }

    pub fn new_futures(auth: Option<MexcAuth>) -> Self {
        Self {
            client: Client::new(),
            auth,
            base_url: MEXC_FUTURES_REST_URL.to_string(),
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

    /// Make an authenticated request (GET or POST)
    pub async fn request_private<T: for<'de> Deserialize<'de>>(
        &self,
        method: reqwest::Method,
        endpoint: &str,
        mut params: HashMap<String, String>,
    ) -> Result<T> {
        let auth = self.auth.as_ref()
            .context("Authentication required for private endpoints")?;

        let timestamp = MexcAuth::get_timestamp();
        params.insert("timestamp".to_string(), timestamp.to_string());

        let query_string = serde_urlencoded::to_string(&params)?;
        let signature = auth.sign_request(&query_string)?;
        params.insert("signature".to_string(), signature);

        let url = format!("{}{}", self.base_url, endpoint);
        let mut request = self.client.request(method, &url)
            .header("X-MEXC-APIKEY", &auth.api_key);

        request = request.query(&params);

        let response = request.send().await.context("Failed to send request")?;
        let status = response.status();
        let body = response.text().await.context("Failed to read response")?;

        if !status.is_success() {
            anyhow::bail!("API error ({}): {}", status, body);
        }

        serde_json::from_str(&body).context("Failed to parse response")
    }
}

/// Standard MEXC API response wrapper
#[derive(Debug, Deserialize)]
pub struct MexcResponse<T> {
    pub code: i32,
    #[serde(default)]
    pub msg: String,
    pub data: Option<T>,
}

impl<T> MexcResponse<T> {
    pub fn into_result(self) -> Result<T> {
        if self.code != 200 && self.code != 0 {
            anyhow::bail!("MEXC API error ({}): {}", self.code, self.msg);
        }
        self.data.context("Missing data in API response")
    }
}

/// WebSocket subscription message
#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct WsSubscribeMessage {
    pub method: String,
    pub params: Vec<String>,
}

// Utility functions for converting between MEXC and internal types
pub mod converters {
    use crate::traits::{OrderType, Side, TimeInForce};

    pub fn to_mexc_order_type(order_type: OrderType) -> &'static str {
        match order_type {
            OrderType::Limit => "LIMIT",
            OrderType::Market => "MARKET",
            OrderType::StopLoss => "STOP_LOSS",
            OrderType::StopLossLimit => "STOP_LOSS_LIMIT",
            OrderType::TakeProfit => "TAKE_PROFIT",
            OrderType::TakeProfitLimit => "TAKE_PROFIT_LIMIT",
        }
    }

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

    pub fn to_mexc_side(side: Side) -> &'static str {
        match side {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        }
    }

    pub fn from_mexc_side(side: &str) -> Side {
        match side.to_uppercase().as_str() {
            "BUY" => Side::Buy,
            "SELL" => Side::Sell,
            _ => Side::Buy, // Default fallback
        }
    }

    pub fn to_mexc_tif(tif: TimeInForce) -> &'static str {
        match tif {
            TimeInForce::Gtc => "GTC",
            TimeInForce::Ioc => "IOC",
            TimeInForce::Fok => "FOK",
        }
    }

    #[allow(dead_code)]
    pub fn from_mexc_tif(tif: &str) -> TimeInForce {
        match tif {
            "GTC" => TimeInForce::Gtc,
            "IOC" => TimeInForce::Ioc,
            "FOK" => TimeInForce::Fok,
            _ => TimeInForce::Gtc, // Default fallback
        }
    }
}
