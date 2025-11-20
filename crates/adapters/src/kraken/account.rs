use anyhow::{Context, Result};
use reqwest::Client;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

// ============================================================================
// API Endpoint Constants
// ============================================================================

/// Kraken Spot REST API base URL
///
/// All spot market REST API calls use this as the base URL.
/// Example: `https://api.kraken.com/0/public/Ticker`
pub const KRAKEN_SPOT_REST_URL: &str = "https://api.kraken.com";

/// Kraken Spot WebSocket public feed URL (API v2)
///
/// Used for public market data streams (orderbook, trades, ticker).
/// Does not require authentication.
pub const KRAKEN_SPOT_WS_URL: &str = "wss://ws.kraken.com/v2";

/// Kraken Spot WebSocket authenticated feed URL (API v2)
///
/// Used for private user data streams (order updates, balance changes).
/// Requires WebSocket token obtained via REST API.
pub const KRAKEN_SPOT_WS_AUTH_URL: &str = "wss://ws-auth.kraken.com/v2";

/// Kraken Futures REST API base URL (API v3)
///
/// All futures/perpetuals market REST API calls use this as the base URL.
/// Example: `https://futures.kraken.com/derivatives/api/v3/sendorder`
pub const KRAKEN_FUTURES_REST_URL: &str = "https://futures.kraken.com/derivatives/api/v3";

/// Kraken Futures WebSocket URL (API v1)
///
/// Used for both public and private futures market data streams.
/// Authentication happens via challenge-response mechanism over WebSocket.
pub const KRAKEN_FUTURES_WS_URL: &str = "wss://futures.kraken.com/ws/v1";

// ============================================================================
// Authentication
// ============================================================================

/// Kraken API authentication credentials
///
/// Stores API key and secret for signing requests. The API secret is base64-encoded
/// and is used to generate HMAC-SHA512 signatures for authenticated REST API calls
/// and WebSocket connections.
///
/// # Security Notes
///
/// - API keys should be stored securely (e.g., environment variables, secrets manager)
/// - Never commit API keys to version control
/// - Use IP whitelisting and permission restrictions on Kraken's platform
/// - The secret is decoded from base64 before use in signature generation
#[derive(Clone)]
pub struct KrakenAuth {
    /// API key string (public identifier)
    pub api_key: String,

    /// API secret in base64-encoded format (private signing key)
    pub api_secret: String,
}

impl KrakenAuth {
    /// Creates a new KrakenAuth instance with the provided credentials
    ///
    /// # Arguments
    ///
    /// * `api_key` - Public API key obtained from Kraken account settings
    /// * `api_secret` - Base64-encoded private API secret
    ///
    /// # Example
    ///
    /// ```ignore
    /// let auth = KrakenAuth::new(
    ///     "YOUR_API_KEY".to_string(),
    ///     "YOUR_BASE64_SECRET".to_string()
    /// );
    /// ```
    pub fn new(api_key: String, api_secret: String) -> Self {
        Self { api_key, api_secret }
    }

    /// Generates HMAC-SHA512 signature for Kraken REST API authenticated requests
    ///
    /// Kraken's authentication scheme requires:
    /// 1. SHA256 hash of (nonce + postdata)
    /// 2. Concatenate API path + SHA256 hash
    /// 3. HMAC-SHA512 of the concatenated message using decoded API secret
    /// 4. Base64 encode the signature
    ///
    /// This signature is sent in the `API-Sign` header along with the `API-Key` header.
    ///
    /// # Arguments
    ///
    /// * `path` - API endpoint path (e.g., "/0/private/AddOrder")
    /// * `nonce` - Monotonically increasing timestamp in milliseconds
    /// * `postdata` - URL-encoded POST body including the nonce parameter
    ///
    /// # Returns
    ///
    /// Base64-encoded HMAC-SHA512 signature string
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - API secret cannot be decoded from base64
    /// - HMAC creation fails
    ///
    /// # Example
    ///
    /// ```ignore
    /// let nonce = KrakenAuth::get_nonce();
    /// let postdata = format!("nonce={}&pair=XBTUSD", nonce);
    /// let signature = auth.sign_request("/0/private/Balance", nonce, &postdata)?;
    /// ```
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

    /// Generates a nonce (number used once) for request authentication
    ///
    /// Returns the current Unix timestamp in milliseconds. Kraken requires nonces
    /// to be strictly increasing for each API key, so using millisecond precision
    /// ensures uniqueness assuming requests are not sent faster than 1ms apart.
    ///
    /// # Returns
    ///
    /// Current timestamp as milliseconds since Unix epoch (January 1, 1970)
    ///
    /// # Note
    ///
    /// If making multiple requests in rapid succession from the same API key,
    /// ensure nonces are strictly increasing. Consider adding a counter if needed.
    pub fn get_nonce() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
}

// ============================================================================
// HTTP Client
// ============================================================================

/// HTTP client wrapper for Kraken REST API
///
/// Provides methods for making authenticated and public HTTP requests to Kraken's
/// REST API. Handles request signing, header management, and response parsing.
///
/// # Thread Safety
///
/// This struct is thread-safe and can be cloned cheaply (reqwest::Client uses Arc internally).
#[derive(Clone)]
pub struct KrakenRestClient {
    /// Underlying HTTP client (reuses connections via connection pooling)
    client: Client,

    /// Optional authentication credentials for private endpoints
    auth: Option<KrakenAuth>,

    /// Base URL for API requests (spot or futures)
    base_url: String,
}

impl KrakenRestClient {
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
            .expect("Failed to build HTTP client")
    }

    /// Creates a new HTTP client configured for Kraken Spot REST API
    ///
    /// # Arguments
    ///
    /// * `auth` - Optional authentication credentials. Pass `None` for public endpoints only.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Public access only
    /// let client = KrakenRestClient::new_spot(None);
    ///
    /// // With authentication
    /// let auth = KrakenAuth::new(api_key, api_secret);
    /// let client = KrakenRestClient::new_spot(Some(auth));
    /// ```
    pub fn new_spot(auth: Option<KrakenAuth>) -> Self {
        Self {
            client: Self::build_client(),
            auth,
            base_url: KRAKEN_SPOT_REST_URL.to_string(),
        }
    }

    /// Creates a new HTTP client configured for Kraken Futures REST API
    ///
    /// # Arguments
    ///
    /// * `auth` - Optional authentication credentials. Pass `None` for public endpoints only.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let auth = KrakenAuth::new(api_key, api_secret);
    /// let client = KrakenRestClient::new_futures(Some(auth));
    /// ```
    pub fn new_futures(auth: Option<KrakenAuth>) -> Self {
        Self {
            client: Self::build_client(),
            auth,
            base_url: KRAKEN_FUTURES_REST_URL.to_string(),
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
    /// * `endpoint` - API endpoint path (e.g., "/0/public/Ticker")
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
    ///     ("pair".to_string(), "XBTUSD".to_string())
    /// ]);
    /// let ticker: TickerResponse = client.get_public("/0/public/Ticker", Some(params)).await?;
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

    /// Makes an authenticated POST request to a private endpoint
    ///
    /// Used for account-specific operations like placing orders, checking balances,
    /// canceling orders, etc. Requires API credentials to be configured.
    ///
    /// # Authentication Flow
    ///
    /// 1. Generates a nonce (current timestamp in milliseconds)
    /// 2. Adds nonce to request parameters
    /// 3. URL-encodes all parameters into POST body
    /// 4. Generates HMAC-SHA512 signature using API secret
    /// 5. Sends request with `API-Key` and `API-Sign` headers
    ///
    /// # Type Parameters
    ///
    /// * `T` - Response type that implements `Deserialize`
    ///
    /// # Arguments
    ///
    /// * `endpoint` - API endpoint path (e.g., "/0/private/AddOrder")
    /// * `params` - Request parameters as key-value pairs (nonce is added automatically)
    ///
    /// # Returns
    ///
    /// Deserialized response of type `T`
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - No authentication credentials configured
    /// - Signature generation fails
    /// - Network request fails
    /// - HTTP status code indicates failure
    /// - Response body cannot be parsed as type `T`
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut params = HashMap::new();
    /// params.insert("pair".to_string(), "XBTUSD".to_string());
    /// params.insert("type".to_string(), "buy".to_string());
    /// params.insert("ordertype".to_string(), "limit".to_string());
    /// params.insert("price".to_string(), "50000".to_string());
    /// params.insert("volume".to_string(), "0.1".to_string());
    ///
    /// let result: AddOrderResponse = client.post_private("/0/private/AddOrder", params).await?;
    /// ```
    pub async fn post_private<T: for<'de> serde::Deserialize<'de>>(
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

// ============================================================================
// Response Types
// ============================================================================

/// Standard Kraken API response envelope
///
/// All Kraken REST API responses follow this structure with an `error` array
/// and an optional `result` field. If errors are present, result will be None.
///
/// # Type Parameters
///
/// * `T` - The expected result type specific to each endpoint
///
/// # Examples
///
/// Success response:
/// ```json
/// {
///   "error": [],
///   "result": { "XXBTZUSD": { "a": ["50000.00", ...], ... } }
/// }
/// ```
///
/// Error response:
/// ```json
/// {
///   "error": ["EOrder:Insufficient funds"],
///   "result": null
/// }
/// ```
#[derive(Debug, serde::Deserialize)]
pub struct KrakenResponse<T> {
    /// Array of error messages. Empty if request succeeded.
    pub error: Vec<String>,

    /// Result data if successful, None if errors occurred
    pub result: Option<T>,
}

impl<T> KrakenResponse<T> {
    /// Converts the response wrapper into a Result
    ///
    /// Checks for errors in the response and returns the result data if successful.
    /// This is the standard way to handle Kraken API responses.
    ///
    /// # Returns
    ///
    /// - `Ok(T)` if no errors and result is present
    /// - `Err` if errors occurred or result is missing
    ///
    /// # Example
    ///
    /// ```ignore
    /// let response: KrakenResponse<TickerData> = client.get_public(...).await?;
    /// let ticker = response.into_result()?; // Extracts TickerData or returns error
    /// ```
    pub fn into_result(self) -> Result<T> {
        if !self.error.is_empty() {
            anyhow::bail!("Kraken API error: {:?}", self.error);
        }
        self.result.context("Missing result in API response")
    }
}

// ============================================================================
// Type Converters
// ============================================================================

/// Utility functions for converting between Kraken API types and internal trait types
///
/// Kraken uses string-based enums for order types, sides, and time-in-force values.
/// These functions handle the bidirectional conversion between Kraken's format and
/// our strongly-typed internal enums.
pub mod converters {
    use crate::traits::{OrderType, Side, TimeInForce};

    /// Converts internal OrderType enum to Kraken API string format
    ///
    /// # Arguments
    ///
    /// * `order_type` - Internal order type enum value
    ///
    /// # Returns
    ///
    /// Kraken API order type string
    ///
    /// # Mapping
    ///
    /// - `Limit` → "limit"
    /// - `Market` → "market"
    /// - `StopLoss` → "stop-loss"
    /// - `StopLossLimit` → "stop-loss-limit"
    /// - `TakeProfit` → "take-profit"
    /// - `TakeProfitLimit` → "take-profit-limit"
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

    /// Converts Kraken API order type string to internal OrderType enum
    ///
    /// # Arguments
    ///
    /// * `order_type` - Kraken API order type string
    ///
    /// # Returns
    ///
    /// Internal OrderType enum value
    ///
    /// # Default Behavior
    ///
    /// Returns `OrderType::Limit` for unrecognized strings to avoid panics
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

    /// Converts internal Side enum to Kraken API string format
    ///
    /// # Arguments
    ///
    /// * `side` - Internal side enum value (Buy or Sell)
    ///
    /// # Returns
    ///
    /// Kraken API side string ("buy" or "sell")
    pub fn to_kraken_side(side: Side) -> &'static str {
        match side {
            Side::Buy => "buy",
            Side::Sell => "sell",
        }
    }

    /// Converts Kraken API side string to internal Side enum
    ///
    /// # Arguments
    ///
    /// * `side` - Kraken API side string (case-insensitive)
    ///
    /// # Returns
    ///
    /// Internal Side enum value
    ///
    /// # Default Behavior
    ///
    /// Returns `Side::Buy` for unrecognized strings to avoid panics
    pub fn from_kraken_side(side: &str) -> Side {
        match side.to_lowercase().as_str() {
            "buy" => Side::Buy,
            "sell" => Side::Sell,
            _ => Side::Buy, // Default fallback
        }
    }

    /// Converts internal TimeInForce enum to Kraken API string format
    ///
    /// # Arguments
    ///
    /// * `tif` - Internal time-in-force enum value
    ///
    /// # Returns
    ///
    /// Kraken API time-in-force string
    ///
    /// # Time-In-Force Types
    ///
    /// - `GTC` (Good-Til-Canceled) - Order remains active until filled or canceled
    /// - `IOC` (Immediate-Or-Cancel) - Fill immediately or cancel unfilled portion
    /// - `FOK` (Fill-Or-Kill) - Fill entire order immediately or cancel completely
    pub fn to_kraken_tif(tif: TimeInForce) -> &'static str {
        match tif {
            TimeInForce::Gtc => "GTC",
            TimeInForce::Ioc => "IOC",
            TimeInForce::Fok => "FOK",
        }
    }

    /// Converts Kraken API time-in-force string to internal TimeInForce enum
    ///
    /// # Arguments
    ///
    /// * `tif` - Kraken API time-in-force string
    ///
    /// # Returns
    ///
    /// Internal TimeInForce enum value
    ///
    /// # Default Behavior
    ///
    /// Returns `TimeInForce::Gtc` for unrecognized strings to avoid panics
    pub fn from_kraken_tif(tif: &str) -> TimeInForce {
        match tif {
            "GTC" => TimeInForce::Gtc,
            "IOC" => TimeInForce::Ioc,
            "FOK" => TimeInForce::Fok,
            _ => TimeInForce::Gtc, // Default fallback
        }
    }
}
