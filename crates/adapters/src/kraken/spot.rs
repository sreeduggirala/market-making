//! Kraken Spot Market Adapter
//!
//! This module provides a unified adapter for Kraken's spot trading market, combining
//! both REST API and WebSocket functionality in a single `KrakenSpotAdapter` struct.
//!
//! # Features
//!
//! - **Order Management**: Place, cancel, query, and track orders via REST API
//! - **Account Operations**: Query balances, get account information
//! - **Market Data**: Fetch tickers, historical klines (OHLCV), current prices
//! - **Real-Time Streams**: Subscribe to order updates, orderbook changes, and trade feeds via WebSocket
//! - **Health Monitoring**: Track connection status, latency, and reconnection attempts
//!
//! # Architecture
//!
//! The adapter follows a dual-protocol pattern:
//! - **REST**: Synchronous request/response for trading operations and queries
//! - **WebSocket**: Asynchronous event streams for real-time data
//!
//! Both protocols share the same authentication credentials and are managed by a single
//! adapter instance, making it simple to use both simultaneously.
//!
//! # WebSocket Architecture
//!
//! WebSocket subscriptions spawn background tasks that:
//! 1. Maintain the WebSocket connection
//! 2. Handle ping/pong for keepalive
//! 3. Parse incoming messages
//! 4. Send typed events through `mpsc` channels
//! 5. Track connection health metrics
//!
//! # Example Usage
//!
//! ```ignore
//! use market_making::adapters::kraken::KrakenSpotAdapter;
//! use market_making::adapters::traits::{SpotRest, SpotWs};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Initialize adapter
//!     let adapter = KrakenSpotAdapter::new(
//!         std::env::var("KRAKEN_API_KEY")?,
//!         std::env::var("KRAKEN_API_SECRET")?
//!     );
//!
//!     // REST: Get account balance
//!     let balances = adapter.get_balance().await?;
//!     println!("Balances: {:?}", balances);
//!
//!     // REST: Place a limit order
//!     let order = adapter.create_order(CreateOrderRequest {
//!         symbol: "BTC/USD".to_string(),
//!         side: Side::Buy,
//!         ord_type: OrderType::Limit,
//!         qty: 0.001,
//!         price: Some(50000.0),
//!         ..Default::default()
//!     }).await?;
//!
//!     // WebSocket: Subscribe to order updates
//!     let mut user_events = adapter.subscribe_user().await?;
//!     while let Some(event) = user_events.recv().await {
//!         match event {
//!             UserEvent::OrderUpdate(order) => {
//!                 println!("Order update: {:?}", order);
//!             }
//!             UserEvent::Balance { asset, free, locked, .. } => {
//!                 println!("Balance update: {} = {} free, {} locked", asset, free, locked);
//!             }
//!             _ => {}
//!         }
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! # API Documentation
//!
//! - Kraken Spot REST API: <https://docs.kraken.com/api/docs/guides/spot-rest-api>
//! - Kraken Spot WebSocket v2: <https://docs.kraken.com/api/docs/guides/spot-websocket-api>

use crate::kraken::account::{
    converters, KrakenAuth, KrakenResponse, KrakenRestClient,
    KRAKEN_SPOT_WS_URL, KRAKEN_SPOT_WS_AUTH_URL,
};
use crate::traits::*;
use crate::utils::{
    CircuitBreaker, CircuitBreakerConfig, HeartbeatMonitor, HeartbeatConfig,
    RateLimiter, RateLimiterConfig, ReconnectConfig, ReconnectStrategy,
};
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use tracing::{debug, error, info, warn};

/// Type alias for WebSocket stream over TLS
type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

// ============================================================================
// Main Adapter Struct
// ============================================================================

/// Unified Kraken Spot market adapter
///
/// Combines REST API and WebSocket functionality for Kraken spot trading in a single struct.
/// Implements both `SpotRest` and `SpotWs` traits for complete market access.
///
/// # Features
///
/// - **REST API**: Order management, account queries, market data, historical data
/// - **WebSocket**: Real-time user events, orderbook updates, trade streams
/// - **Health Monitoring**: Connection status, ping/pong tracking, reconnection logic
/// - **Thread-Safe**: All state is protected by Arc<Mutex> or Arc<RwLock>
///
/// # Architecture
///
/// - Single authentication instance shared between REST and WebSocket
/// - Separate HTTP client for REST operations with connection pooling
/// - WebSocket streams spawn background tasks that send events via channels
/// - Health data tracks connection quality and latency
///
/// # Example
///
/// ```ignore
/// use crate::kraken::KrakenSpotAdapter;
/// use crate::traits::{SpotRest, SpotWs};
///
/// let adapter = KrakenSpotAdapter::new(api_key, api_secret);
///
/// // Use REST API
/// let balance = adapter.get_balance().await?;
///
/// // Use WebSocket
/// let mut user_events = adapter.subscribe_user().await?;
/// while let Some(event) = user_events.recv().await {
///     println!("Event: {:?}", event);
/// }
/// ```
#[derive(Clone)]
pub struct KrakenSpotAdapter {
    /// HTTP client for REST API requests
    client: KrakenRestClient,

    /// Authentication credentials (shared with WebSocket)
    #[allow(dead_code)]
    auth: KrakenAuth,

    // WebSocket stream receivers (stored for potential reuse)
    #[allow(dead_code)]
    user_stream: Arc<Mutex<Option<mpsc::Receiver<UserEvent>>>>,
    #[allow(dead_code)]
    book_stream: Arc<Mutex<Option<mpsc::Receiver<BookUpdate>>>>,
    #[allow(dead_code)]
    trade_stream: Arc<Mutex<Option<mpsc::Receiver<TradeEvent>>>>,

    /// Current WebSocket connection status
    connection_status: Arc<RwLock<ConnectionStatus>>,

    /// Health metrics for connection monitoring
    health_data: Arc<RwLock<HealthData>>,

    // Production features
    /// Rate limiter for REST API calls (TODO: integrate)
    #[allow(dead_code)]
    rate_limiter: RateLimiter,

    /// Circuit breaker for fault tolerance (TODO: integrate)
    #[allow(dead_code)]
    circuit_breaker: CircuitBreaker,

    /// Counter for reconnection attempts
    reconnect_count: Arc<AtomicU32>,

    /// Shutdown signal sender
    shutdown_tx: Arc<Mutex<Option<tokio::sync::broadcast::Sender<()>>>>,
}

/// WebSocket connection health and performance metrics
///
/// Tracks ping/pong times, reconnection attempts, and error messages
/// for monitoring connection quality and debugging issues.
#[derive(Clone)]
struct HealthData {
    /// Timestamp of last received ping (milliseconds since epoch)
    last_ping_ms: Option<UnixMillis>,

    /// Timestamp of last received pong (milliseconds since epoch)
    last_pong_ms: Option<UnixMillis>,

    /// Number of times connection has been re-established
    reconnect_count: u32,

    /// Last error message if connection failed
    error_msg: Option<String>,
}

impl KrakenSpotAdapter {
    /// Creates a new Kraken Spot adapter instance
    ///
    /// Initializes both REST and WebSocket components with the provided credentials.
    /// WebSocket connections are established lazily when subscribe methods are called.
    ///
    /// # Arguments
    ///
    /// * `api_key` - Kraken API key
    /// * `api_secret` - Kraken API secret (base64-encoded)
    ///
    /// # Example
    ///
    /// ```ignore
    /// let adapter = KrakenSpotAdapter::new(
    ///     "YOUR_API_KEY".to_string(),
    ///     "YOUR_API_SECRET".to_string()
    /// );
    /// ```
    pub fn new(api_key: String, api_secret: String) -> Self {
        let auth = KrakenAuth::new(api_key.clone(), api_secret.clone());
        Self {
            client: KrakenRestClient::new_spot(Some(auth.clone())),
            auth,
            user_stream: Arc::new(Mutex::new(None)),
            book_stream: Arc::new(Mutex::new(None)),
            trade_stream: Arc::new(Mutex::new(None)),
            connection_status: Arc::new(RwLock::new(ConnectionStatus::Disconnected)),
            health_data: Arc::new(RwLock::new(HealthData {
                last_ping_ms: None,
                last_pong_ms: None,
                reconnect_count: 0,
                error_msg: None,
            })),
            rate_limiter: RateLimiter::new(RateLimiterConfig::kraken_spot()),
            circuit_breaker: CircuitBreaker::new("kraken_spot", CircuitBreakerConfig::production()),
            reconnect_count: Arc::new(AtomicU32::new(0)),
            shutdown_tx: Arc::new(Mutex::new(None)),
        }
    }

    /// Wraps API calls with rate limiting and circuit breaker
    ///
    /// This method ensures all REST API calls respect rate limits and benefit from
    /// circuit breaker protection to prevent cascading failures.
    async fn call_api<T, F, Fut>(&self, endpoint: &str, f: F) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        // Rate limit
        if !self.rate_limiter.acquire().await {
            anyhow::bail!("Rate limit reached for endpoint: {}", endpoint);
        }

        debug!("Calling Kraken API endpoint: {}", endpoint);

        // Circuit breaker protection
        match self.circuit_breaker.call(f).await {
            Ok(result) => Ok(result),
            Err(e) => {
                let err_str = e.to_string();

                // Handle rate limit errors with backoff
                if err_str.contains("429") || err_str.to_lowercase().contains("rate limit") {
                    warn!("Rate limit error detected on {}", endpoint);
                    self.rate_limiter.handle_rate_limit_error().await;
                }

                Err(e)
            }
        }
    }

    /// Initiates graceful shutdown of all background tasks
    pub async fn shutdown(&self) {
        info!("Initiating graceful shutdown of Kraken adapter");

        if let Some(tx) = self.shutdown_tx.lock().await.as_ref() {
            let _ = tx.send(());
        }

        // Give tasks time to clean up
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        info!("Kraken adapter shutdown complete");
    }

    /// Generates a unique client order ID for order placement
    ///
    /// Creates a client-side order identifier using the format `mm_{timestamp}`
    /// where timestamp is milliseconds since Unix epoch. This helps track orders
    /// and correlate exchange responses with our requests.
    ///
    /// # Returns
    ///
    /// Unique string identifier in format "mm_1234567890123"
    ///
    /// # Note
    ///
    /// "mm" prefix stands for "market maker" and helps identify orders from this system.
    fn generate_client_order_id() -> String {
        format!("mm_{}", SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis())
    }
}

// ============================================================================
// REST API Types and Implementations
// ============================================================================
//
// This section contains Kraken-specific response types that are deserialized
// from REST API calls. These types are internal and converted to our common
// trait types before being returned to users of the adapter.

// Kraken-specific response types
#[derive(Debug, Deserialize)]
struct KrakenAddOrderResult {
    txid: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct KrakenCancelOrderResult {
    count: usize,
}

#[derive(Debug, Deserialize)]
struct KrakenOrderInfo {
    #[serde(flatten)]
    orders: HashMap<String, KrakenOrderDetails>,
}

#[derive(Debug, Deserialize)]
struct KrakenOrderDetails {
    status: String,
    #[serde(rename = "type")]
    side: String,
    ordertype: String,
    price: String,
    vol: String,
    vol_exec: String,
    opentm: f64,
    #[serde(default)]
    closetm: Option<f64>,
    #[serde(default)]
    userref: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct KrakenBalanceResult {
    #[serde(flatten)]
    balances: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct KrakenAssetPairsResult {
    #[serde(flatten)]
    pairs: HashMap<String, KrakenAssetPairInfo>,
}

#[derive(Debug, Deserialize)]
struct KrakenAssetPairInfo {
    altname: String,
    wsname: String,
    base: String,
    quote: String,
    pair_decimals: u32,
    ordermin: String,
    #[serde(default)]
    costmin: Option<String>,
    status: String,
}

#[derive(Debug, Deserialize)]
struct KrakenTickerResult {
    #[serde(flatten)]
    tickers: HashMap<String, KrakenTickerData>,
}

#[derive(Debug, Deserialize)]
struct KrakenTickerData {
    a: Vec<String>, // ask [price, whole lot volume, lot volume]
    b: Vec<String>, // bid [price, whole lot volume, lot volume]
    c: Vec<String>, // last trade [price, lot volume]
    v: Vec<String>, // volume [today, last 24 hours]
    p: Vec<String>, // volume weighted average price [today, last 24 hours]
    t: Vec<u64>,    // number of trades [today, last 24 hours]
    l: Vec<String>, // low [today, last 24 hours]
    h: Vec<String>, // high [today, last 24 hours]
    o: String,      // opening price
}

#[derive(Debug, Deserialize)]
struct KrakenOHLCResult {
    #[serde(flatten)]
    data: HashMap<String, Vec<Vec<serde_json::Value>>>,
}

// Helper functions
fn parse_kraken_status(status: &str) -> OrderStatus {
    match status {
        "pending" => OrderStatus::New,
        "open" => OrderStatus::New,
        "closed" => OrderStatus::Filled,
        "canceled" => OrderStatus::Canceled,
        "expired" => OrderStatus::Expired,
        _ => OrderStatus::Rejected,
    }
}

fn now_millis() -> UnixMillis {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[async_trait::async_trait]
impl SpotRest for KrakenSpotAdapter {
    async fn create_order(&self, new: NewOrder) -> Result<Order> {
        let mut params = HashMap::new();
        params.insert("pair".to_string(), new.symbol.clone());
        params.insert("type".to_string(), converters::to_kraken_side(new.side).to_string());
        params.insert("ordertype".to_string(), converters::to_kraken_order_type(new.ord_type).to_string());
        params.insert("volume".to_string(), new.qty.to_string());

        if let Some(price) = new.price {
            params.insert("price".to_string(), price.to_string());
        }

        if let Some(stop_price) = new.stop_price {
            params.insert("price2".to_string(), stop_price.to_string());
        }

        if let Some(tif) = new.tif {
            params.insert("timeinforce".to_string(), converters::to_kraken_tif(tif).to_string());
        }

        if new.post_only {
            params.insert("oflags".to_string(), "post".to_string());
        }

        if new.reduce_only {
            params.insert("reduce_only".to_string(), "true".to_string());
        }

        // Kraken's userref must be a 32-bit signed integer, not a string
        // Hash the client_order_id to create a numeric reference
        // Note: cl_ord_id can be used via WebSocket for string-based tracking
        if !new.client_order_id.is_empty() {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            new.client_order_id.hash(&mut hasher);
            let userref = (hasher.finish() as i32).abs(); // Convert to positive i32
            params.insert("userref".to_string(), userref.to_string());
        }

        // Wrap with rate limiter and circuit breaker
        let response: KrakenResponse<KrakenAddOrderResult> = self
            .call_api("/0/private/AddOrder", || async {
                self.client
                    .post_private("/0/private/AddOrder", params.clone())
                    .await
            })
            .await?;

        let result = response.into_result()?;
        let order_id = result.txid.first()
            .context("No order ID in response")?
            .clone();

        let now = now_millis();

        Ok(Order {
            venue_order_id: order_id,
            client_order_id: new.client_order_id,
            symbol: new.symbol,
            ord_type: new.ord_type,
            side: new.side,
            qty: new.qty,
            price: new.price,
            stop_price: new.stop_price,
            tif: new.tif,
            status: OrderStatus::New,
            filled_qty: 0.0,
            remaining_qty: new.qty,
            created_ms: now,
            updated_ms: now,
            recv_ms: now,
            raw_status: Some("pending".to_string()),
        })
    }

    async fn cancel_order(&self, _symbol: &str, venue_order_id: &str) -> Result<bool> {
        let mut params = HashMap::new();
        params.insert("txid".to_string(), venue_order_id.to_string());

        // Wrap with rate limiter and circuit breaker
        let response: KrakenResponse<KrakenCancelOrderResult> = self
            .call_api("/0/private/CancelOrder", || async {
                self.client
                    .post_private("/0/private/CancelOrder", params.clone())
                    .await
            })
            .await?;

        let result = response.into_result()?;
        Ok(result.count > 0)
    }

    async fn cancel_all(&self, symbol: Option<&str>) -> Result<usize> {
        // Get all open orders for the symbol
        let orders = self.get_open_orders(symbol).await?;
        let mut cancelled = 0;

        for order in orders {
            if self.cancel_order(&order.symbol, &order.venue_order_id).await.unwrap_or(false) {
                cancelled += 1;
            }
        }

        Ok(cancelled)
    }

    async fn get_order(&self, _symbol: &str, venue_order_id: &str) -> Result<Order> {
        let mut params = HashMap::new();
        params.insert("txid".to_string(), venue_order_id.to_string());
        params.insert("trades".to_string(), "false".to_string());

        // Wrap with rate limiter and circuit breaker
        let response: KrakenResponse<KrakenOrderInfo> = self
            .call_api("/0/private/QueryOrders", || async {
                self.client
                    .post_private("/0/private/QueryOrders", params.clone())
                    .await
            })
            .await?;

        let result = response.into_result()?;
        let (order_id, details) = result.orders.iter().next()
            .context("Order not found")?;

        let now = now_millis();
        let filled = details.vol_exec.parse::<f64>().unwrap_or(0.0);
        let total = details.vol.parse::<f64>().unwrap_or(0.0);

        Ok(Order {
            venue_order_id: order_id.clone(),
            client_order_id: details.userref.map(|u| u.to_string()).unwrap_or_default(),
            symbol: "".to_string(), // Kraken doesn't return symbol in order query
            ord_type: converters::from_kraken_order_type(&details.ordertype),
            side: converters::from_kraken_side(&details.side),
            qty: total,
            price: Some(details.price.parse().unwrap_or(0.0)),
            stop_price: None,
            tif: None,
            status: parse_kraken_status(&details.status),
            filled_qty: filled,
            remaining_qty: total - filled,
            created_ms: (details.opentm * 1000.0) as UnixMillis,
            updated_ms: details.closetm.map(|t| (t * 1000.0) as UnixMillis).unwrap_or(now),
            recv_ms: now,
            raw_status: Some(details.status.clone()),
        })
    }

    async fn get_open_orders(&self, _symbol: Option<&str>) -> Result<Vec<Order>> {
        let params: HashMap<String, String> = HashMap::new();

        // Wrap with rate limiter and circuit breaker
        let response: KrakenResponse<HashMap<String, HashMap<String, KrakenOrderDetails>>> = self
            .call_api("/0/private/OpenOrders", || async {
                self.client
                    .post_private("/0/private/OpenOrders", params.clone())
                    .await
            })
            .await?;

        let result = response.into_result()?;
        let orders_map = result.get("open").context("No open orders in response")?;

        let now = now_millis();
        let mut orders = Vec::new();

        for (order_id, details) in orders_map {
            let filled = details.vol_exec.parse::<f64>().unwrap_or(0.0);
            let total = details.vol.parse::<f64>().unwrap_or(0.0);

            orders.push(Order {
                venue_order_id: order_id.clone(),
                client_order_id: details.userref.map(|u| u.to_string()).unwrap_or_default(),
                symbol: "".to_string(),
                ord_type: converters::from_kraken_order_type(&details.ordertype),
                side: converters::from_kraken_side(&details.side),
                qty: total,
                price: Some(details.price.parse().unwrap_or(0.0)),
                stop_price: None,
                tif: None,
                status: parse_kraken_status(&details.status),
                filled_qty: filled,
                remaining_qty: total - filled,
                created_ms: (details.opentm * 1000.0) as UnixMillis,
                updated_ms: details.closetm.map(|t| (t * 1000.0) as UnixMillis).unwrap_or(now),
                recv_ms: now,
                raw_status: Some(details.status.clone()),
            });
        }

        Ok(orders)
    }

    async fn replace_order(
        &self,
        symbol: &str,
        venue_order_id: &str,
        new_price: Option<Price>,
        new_qty: Option<Quantity>,
        new_tif: Option<TimeInForce>,
        post_only: Option<bool>,
    ) -> Result<(Order, bool)> {
        // Kraken doesn't have native replace - we cancel and re-create
        let old_order = self.get_order(symbol, venue_order_id).await?;

        let cancelled = self.cancel_order(symbol, venue_order_id).await?;
        if !cancelled {
            anyhow::bail!("Failed to cancel old order");
        }

        let new_order_req = NewOrder {
            symbol: old_order.symbol.clone(),
            side: old_order.side,
            ord_type: old_order.ord_type,
            qty: new_qty.unwrap_or(old_order.qty),
            price: new_price.or(old_order.price),
            stop_price: old_order.stop_price,
            tif: new_tif.or(old_order.tif),
            post_only: post_only.unwrap_or(false),
            reduce_only: false,
            client_order_id: Self::generate_client_order_id(),
        };

        let new_order = self.create_order(new_order_req).await?;
        Ok((new_order, true))
    }

    async fn create_batch_orders(&self, batch: BatchOrderRequest) -> Result<BatchOrderResult> {
        let mut success = Vec::new();
        let mut failed = Vec::new();

        for order in batch.orders {
            match self.create_order(order.clone()).await {
                Ok(created) => success.push(created),
                Err(e) => failed.push((order, e.to_string())),
            }
        }

        Ok(BatchOrderResult { success, failed })
    }

    async fn cancel_batch_orders(&self, _symbol: &str, order_ids: Vec<String>) -> Result<BatchCancelResult> {
        let mut success = Vec::new();
        let mut failed = Vec::new();

        for order_id in order_ids {
            match self.cancel_order("", &order_id).await {
                Ok(true) => success.push(order_id),
                Ok(false) => failed.push((order_id.clone(), "Cancellation returned false".to_string())),
                Err(e) => failed.push((order_id, e.to_string())),
            }
        }

        Ok(BatchCancelResult { success, failed })
    }

    async fn get_balances(&self) -> Result<Vec<Balance>> {
        let params: HashMap<String, String> = HashMap::new();

        // Wrap with rate limiter and circuit breaker
        let response: KrakenResponse<KrakenBalanceResult> = self
            .call_api("/0/private/Balance", || async {
                self.client
                    .post_private("/0/private/Balance", params.clone())
                    .await
            })
            .await?;

        let result = response.into_result()?;
        let mut balances = Vec::new();

        for (asset, amount_str) in result.balances {
            let amount = amount_str.parse::<f64>().unwrap_or(0.0);
            balances.push(Balance {
                asset,
                free: amount, // Kraken doesn't distinguish free/locked in Balance endpoint
                locked: 0.0,
                total: amount,
            });
        }

        Ok(balances)
    }

    async fn get_account_info(&self) -> Result<AccountInfo> {
        let balances = self.get_balances().await?;
        let now = now_millis();

        Ok(AccountInfo {
            balances,
            can_trade: true,
            can_withdraw: true,
            can_deposit: true,
            update_ms: now,
        })
    }

    async fn get_market_info(&self, symbol: &str) -> Result<MarketInfo> {
        let mut params = HashMap::new();
        params.insert("pair".to_string(), symbol.to_string());

        // Wrap with rate limiter and circuit breaker
        let response: KrakenResponse<KrakenAssetPairsResult> = self
            .call_api("/0/public/AssetPairs", || async {
                self.client
                    .get_public("/0/public/AssetPairs", Some(params.clone()))
                    .await
            })
            .await?;

        let result = response.into_result()?;
        let (_, info) = result.pairs.iter().next()
            .context("Symbol not found")?;

        let status = match info.status.as_str() {
            "online" => MarketStatus::Trading,
            "cancel_only" => MarketStatus::PostTrading,
            "post_only" => MarketStatus::PreTrading,
            "limit_only" => MarketStatus::Trading,
            "reduce_only" => MarketStatus::PostTrading,
            _ => MarketStatus::Halt,
        };

        Ok(MarketInfo {
            symbol: symbol.to_string(),
            base_asset: info.base.clone(),
            quote_asset: info.quote.clone(),
            status,
            min_qty: info.ordermin.parse().unwrap_or(0.0),
            max_qty: f64::MAX,
            step_size: 10f64.powi(-(info.pair_decimals as i32)),
            tick_size: 10f64.powi(-(info.pair_decimals as i32)),
            min_notional: info.costmin.as_ref().and_then(|s| s.parse().ok()).unwrap_or(0.0),
            max_leverage: None, // Would need separate API call
            is_spot: true,
            is_perp: false,
        })
    }

    async fn get_all_markets(&self) -> Result<Vec<MarketInfo>> {
        let response: KrakenResponse<KrakenAssetPairsResult> = self.client
            .get_public("/0/public/AssetPairs", None)
            .await?;

        let result = response.into_result()?;
        let mut markets = Vec::new();

        for (symbol, info) in result.pairs {
            let status = match info.status.as_str() {
                "online" => MarketStatus::Trading,
                _ => MarketStatus::Halt,
            };

            markets.push(MarketInfo {
                symbol,
                base_asset: info.base.clone(),
                quote_asset: info.quote.clone(),
                status,
                min_qty: info.ordermin.parse().unwrap_or(0.0),
                max_qty: f64::MAX,
                step_size: 10f64.powi(-(info.pair_decimals as i32)),
                tick_size: 10f64.powi(-(info.pair_decimals as i32)),
                min_notional: info.costmin.as_ref().and_then(|s| s.parse().ok()).unwrap_or(0.0),
                max_leverage: None,
                is_spot: true,
                is_perp: false,
            });
        }

        Ok(markets)
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        let mut params = HashMap::new();
        params.insert("pair".to_string(), symbol.to_string());

        // Wrap with rate limiter and circuit breaker
        let response: KrakenResponse<KrakenTickerResult> = self
            .call_api("/0/public/Ticker", || async {
                self.client
                    .get_public("/0/public/Ticker", Some(params.clone()))
                    .await
            })
            .await?;

        let result = response.into_result()?;
        let (_, ticker) = result.tickers.iter().next()
            .context("Symbol not found")?;

        let last_price = ticker.c[0].parse().unwrap_or(0.0);
        let open_price = ticker.o.parse().unwrap_or(0.0);
        let price_change = last_price - open_price;
        let price_change_pct = if open_price > 0.0 {
            (price_change / open_price) * 100.0
        } else {
            0.0
        };

        Ok(TickerInfo {
            symbol: symbol.to_string(),
            last_price,
            bid_price: ticker.b[0].parse().unwrap_or(0.0),
            ask_price: ticker.a[0].parse().unwrap_or(0.0),
            volume_24h: ticker.v[1].parse().unwrap_or(0.0),
            price_change_24h: price_change,
            price_change_pct_24h: price_change_pct,
            high_24h: ticker.h[1].parse().unwrap_or(0.0),
            low_24h: ticker.l[1].parse().unwrap_or(0.0),
            open_price_24h: open_price,
            ts_ms: now_millis(),
        })
    }

    async fn get_tickers(&self, symbols: Option<Vec<String>>) -> Result<Vec<TickerInfo>> {
        let params = symbols.map(|syms| {
            let mut p = HashMap::new();
            p.insert("pair".to_string(), syms.join(","));
            p
        });

        // Wrap with rate limiter and circuit breaker
        let response: KrakenResponse<KrakenTickerResult> = self
            .call_api("/0/public/Ticker", || async {
                self.client
                    .get_public("/0/public/Ticker", params.clone())
                    .await
            })
            .await?;

        let result = response.into_result()?;
        let mut tickers = Vec::new();

        for (symbol, ticker) in result.tickers {
            let last_price = ticker.c[0].parse().unwrap_or(0.0);
            let open_price = ticker.o.parse().unwrap_or(0.0);
            let price_change = last_price - open_price;
            let price_change_pct = if open_price > 0.0 {
                (price_change / open_price) * 100.0
            } else {
                0.0
            };

            tickers.push(TickerInfo {
                symbol,
                last_price,
                bid_price: ticker.b[0].parse().unwrap_or(0.0),
                ask_price: ticker.a[0].parse().unwrap_or(0.0),
                volume_24h: ticker.v[1].parse().unwrap_or(0.0),
                price_change_24h: price_change,
                price_change_pct_24h: price_change_pct,
                high_24h: ticker.h[1].parse().unwrap_or(0.0),
                low_24h: ticker.l[1].parse().unwrap_or(0.0),
                open_price_24h: open_price,
                ts_ms: now_millis(),
            });
        }

        Ok(tickers)
    }

    async fn get_klines(
        &self,
        symbol: &str,
        interval: KlineInterval,
        start_ms: Option<UnixMillis>,
        _end_ms: Option<UnixMillis>,
        _limit: Option<usize>,
    ) -> Result<Vec<Kline>> {
        let interval_mins = match interval {
            KlineInterval::M1 => 1,
            KlineInterval::M5 => 5,
            KlineInterval::M15 => 15,
            KlineInterval::M30 => 30,
            KlineInterval::H1 => 60,
            KlineInterval::H4 => 240,
            KlineInterval::D1 => 1440,
        };

        let mut params = HashMap::new();
        params.insert("pair".to_string(), symbol.to_string());
        params.insert("interval".to_string(), interval_mins.to_string());

        if let Some(since) = start_ms {
            params.insert("since".to_string(), (since / 1000).to_string());
        }

        let response: KrakenResponse<KrakenOHLCResult> = self.client
            .get_public("/0/public/OHLC", Some(params))
            .await?;

        let result = response.into_result()?;
        let (_, ohlc_data) = result.data.iter().next()
            .context("No OHLC data in response")?;

        let mut klines = Vec::new();

        for candle in ohlc_data {
            if candle.len() < 8 {
                continue;
            }

            let open_time = candle[0].as_u64().unwrap_or(0) * 1000;

            klines.push(Kline {
                symbol: symbol.to_string(),
                open_ms: open_time,
                close_ms: open_time + (interval_mins as u64 * 60 * 1000),
                open: candle[1].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0),
                high: candle[2].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0),
                low: candle[3].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0),
                close: candle[4].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0),
                volume: candle[6].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0),
                quote_volume: 0.0, // Kraken doesn't provide this directly
                trades: candle[7].as_u64().unwrap_or(0),
            });
        }

        Ok(klines)
    }
}

// ============================================================================
// WebSocket Message Types
// ============================================================================

// Kraken WebSocket message types
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum KrakenWsMessage {
    Subscription(SubscriptionResponse),
    Channel(ChannelMessage),
    Heartbeat(HeartbeatMessage),
    SystemStatus(SystemStatusMessage),
}

#[derive(Debug, Deserialize)]
struct SubscriptionResponse {
    #[serde(rename = "channelID")]
    channel_id: Option<u64>,
    #[serde(rename = "channelName")]
    channel_name: Option<String>,
    event: String,
    pair: Option<String>,
    subscription: Option<Value>,
    status: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ChannelMessage {
    channel: String,
    #[serde(rename = "type")]
    msg_type: String,
    data: Vec<Value>,
}

#[derive(Debug, Deserialize)]
struct HeartbeatMessage {
    event: String,
}

#[derive(Debug, Deserialize)]
struct SystemStatusMessage {
    event: String,
    status: String,
    version: Option<String>,
}

// Order update from WebSocket v2 executions channel
// See: https://docs.kraken.com/api/docs/websocket-v2/executions
#[derive(Debug, Deserialize)]
struct KrakenWsOrder {
    // Order identification
    order_id: String,
    #[serde(default)]
    cl_ord_id: Option<String>,
    #[serde(default)]
    order_userref: Option<i32>,

    // Order details
    symbol: String,
    side: String,
    order_type: String,
    order_qty: String,
    #[serde(default)]
    limit_price: Option<String>,
    #[serde(default)]
    stop_price: Option<String>,

    // Execution status
    order_status: String,
    #[serde(default)]
    exec_type: Option<String>, // pending_new, new, trade, filled, canceled, expired

    // Fill information
    #[serde(default)]
    cum_qty: Option<String>,   // Cumulative filled quantity
    #[serde(default)]
    cum_cost: Option<String>,  // Cumulative cost
    #[serde(default)]
    avg_price: Option<String>, // Average fill price
    #[serde(default)]
    last_qty: Option<String>,  // Last fill quantity
    #[serde(default)]
    last_price: Option<String>, // Last fill price

    // Fees
    #[serde(default)]
    fee_usd_equiv: Option<String>,

    // Timestamps
    timestamp: String,

    // Flags
    #[serde(default)]
    post_only: Option<bool>,
    #[serde(default)]
    reduce_only: Option<bool>,
}

// Book update from WebSocket
#[derive(Debug, Deserialize)]
struct KrakenWsBook {
    symbol: String,
    bids: Vec<Vec<String>>,
    asks: Vec<Vec<String>>,
    checksum: Option<u32>,
    timestamp: String,
}

// Trade update from WebSocket
#[derive(Debug, Deserialize)]
struct KrakenWsTrade {
    symbol: String,
    side: String,
    price: String,
    qty: String,
    timestamp: String,
}

// ============================================================================
// WebSocket Implementation
// ============================================================================

impl KrakenSpotAdapter {
    async fn connect_authenticated(&self) -> Result<WsStream> {
        // WebSocket v2: Connect without token in URL
        // Token is passed in subscription message params instead
        let (ws_stream, _) = connect_async(KRAKEN_SPOT_WS_AUTH_URL)
            .await
            .context("Failed to connect to Kraken authenticated WebSocket")?;

        debug!("Connected to Kraken authenticated WebSocket");
        Ok(ws_stream)
    }

    async fn connect_public(&self) -> Result<WsStream> {
        let (ws_stream, _) = connect_async(KRAKEN_SPOT_WS_URL)
            .await
            .context("Failed to connect to Kraken WebSocket")?;

        debug!("Connected to Kraken public WebSocket");
        Ok(ws_stream)
    }

    async fn get_ws_token(&self) -> Result<String> {
        use crate::kraken::account;
        use serde::Deserialize;
        use std::collections::HashMap;

        #[derive(Deserialize)]
        struct TokenResponse {
            token: String,
            expires: i64,
        }

        let params = HashMap::new();
        let response: account::KrakenResponse<TokenResponse> = self
            .client
            .post_private("/0/private/GetWebSocketsToken", params)
            .await
            .context("Failed to get WebSocket token")?;

        let token_data = response.into_result()?;
        debug!("Obtained WebSocket token (expires: {})", token_data.expires);

        Ok(token_data.token)
    }

    fn now_millis() -> UnixMillis {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    async fn handle_user_message(text: &str, tx: &mpsc::Sender<UserEvent>) -> Result<()> {
        let value: Value = serde_json::from_str(text)?;

        // Check if it's an order update
        if let Some(channel) = value.get("channel").and_then(|c| c.as_str()) {
            match channel {
                "executions" => {
                    if let Some(data) = value.get("data").and_then(|d| d.as_array()) {
                        for item in data {
                            if let Ok(order) = serde_json::from_value::<KrakenWsOrder>(item.clone()) {
                                let user_event = Self::kraken_order_to_user_event(order)?;
                                let _ = tx.send(user_event).await;
                            }
                        }
                    }
                }
                "balances" => {
                    // Handle balance updates
                    if let Some(data) = value.get("data").and_then(|d| d.as_array()) {
                        for item in data {
                            if let (Some(asset), Some(free), Some(locked)) = (
                                item.get("asset").and_then(|a| a.as_str()),
                                item.get("available").and_then(|a| a.as_str()),
                                item.get("hold").and_then(|h| h.as_str()),
                            ) {
                                let _ = tx.send(UserEvent::Balance {
                                    asset: asset.to_string(),
                                    free: free.parse().unwrap_or(0.0),
                                    locked: locked.parse().unwrap_or(0.0),
                                    ex_ts_ms: Self::now_millis(),
                                    recv_ms: Self::now_millis(),
                                }).await;
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }

    async fn handle_book_message(text: &str, tx: &mpsc::Sender<BookUpdate>) -> Result<()> {
        let value: Value = serde_json::from_str(text)?;

        if let Some(channel) = value.get("channel").and_then(|c| c.as_str()) {
            if channel == "book" {
                if let Some(data) = value.get("data").and_then(|d| d.as_array()) {
                    for item in data {
                        if let Ok(book) = serde_json::from_value::<KrakenWsBook>(item.clone()) {
                            let book_update = Self::kraken_book_to_update(book)?;
                            let _ = tx.send(book_update).await;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn handle_trade_message(text: &str, tx: &mpsc::Sender<TradeEvent>) -> Result<()> {
        let value: Value = serde_json::from_str(text)?;

        if let Some(channel) = value.get("channel").and_then(|c| c.as_str()) {
            if channel == "trade" {
                if let Some(data) = value.get("data").and_then(|d| d.as_array()) {
                    for item in data {
                        if let Ok(trade) = serde_json::from_value::<KrakenWsTrade>(item.clone()) {
                            let trade_event = Self::kraken_trade_to_event(trade)?;
                            let _ = tx.send(trade_event).await;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn kraken_order_to_user_event(order: KrakenWsOrder) -> Result<UserEvent> {
        // Use cum_qty (cumulative quantity) for filled amount
        let filled_qty = order.cum_qty
            .as_ref()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        let total_qty = order.order_qty.parse().unwrap_or(0.0);

        // Map Kraken order status to internal status
        // Kraken v2 statuses: pending_new, new, partially_filled, filled, canceled, expired
        let status = match order.order_status.as_str() {
            "pending_new" | "pending" => OrderStatus::New,
            "new" | "open" => OrderStatus::New,
            "partially_filled" => OrderStatus::PartiallyFilled,
            "filled" | "closed" => OrderStatus::Filled,
            "canceled" => OrderStatus::Canceled,
            "expired" => OrderStatus::Expired,
            _ => OrderStatus::Rejected,
        };

        let now = Self::now_millis();

        let order_obj = Order {
            venue_order_id: order.order_id,
            client_order_id: order.cl_ord_id.unwrap_or_default(),
            symbol: order.symbol,
            ord_type: converters::from_kraken_order_type(&order.order_type),
            side: converters::from_kraken_side(&order.side),
            qty: total_qty,
            price: order.limit_price.and_then(|p| p.parse().ok()),
            stop_price: order.stop_price.and_then(|p| p.parse().ok()),
            tif: None,
            status,
            filled_qty,
            remaining_qty: total_qty - filled_qty,
            created_ms: now,
            updated_ms: now,
            recv_ms: now,
            raw_status: Some(order.order_status),
        };

        Ok(UserEvent::OrderUpdate(order_obj))
    }

    fn kraken_book_to_update(book: KrakenWsBook) -> Result<BookUpdate> {
        let mut bids = Vec::new();
        for bid in book.bids {
            if bid.len() >= 2 {
                let price = bid[0].parse().unwrap_or(0.0);
                let qty = bid[1].parse().unwrap_or(0.0);
                bids.push((price, qty));
            }
        }

        let mut asks = Vec::new();
        for ask in book.asks {
            if ask.len() >= 2 {
                let price = ask[0].parse().unwrap_or(0.0);
                let qty = ask[1].parse().unwrap_or(0.0);
                asks.push((price, qty));
            }
        }

        let now = Self::now_millis();

        Ok(BookUpdate::DepthDelta {
            symbol: book.symbol,
            bids,
            asks,
            seq: 0, // Kraken doesn't provide sequence numbers in this format
            prev_seq: 0,
            checksum: book.checksum,
            ex_ts_ms: now,
            recv_ms: now,
        })
    }

    fn kraken_trade_to_event(trade: KrakenWsTrade) -> Result<TradeEvent> {
        let now = Self::now_millis();

        Ok(TradeEvent {
            symbol: trade.symbol,
            px: trade.price.parse().unwrap_or(0.0),
            qty: trade.qty.parse().unwrap_or(0.0),
            taker_is_buy: trade.side.to_lowercase() == "buy",
            ex_ts_ms: now,
            recv_ms: now,
        })
    }
}

// ============================================================================
// SpotWs Trait Implementation
// ============================================================================

#[async_trait::async_trait]
impl SpotWs for KrakenSpotAdapter {
    async fn subscribe_user(&self) -> Result<mpsc::Receiver<UserEvent>> {
        let (tx, rx) = mpsc::channel(1000);
        let adapter = self.clone();

        // Create shutdown channel
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel(1);
        {
            let mut guard = adapter.shutdown_tx.lock().await;
            *guard = Some(shutdown_tx);
        }

        tokio::spawn(async move {
            let reconnect_config = ReconnectConfig::production();
            let mut strategy = ReconnectStrategy::new(reconnect_config);

            'reconnect: loop {
                // Check for shutdown signal
                if shutdown_rx.try_recv().is_ok() {
                    info!("Kraken user stream shutdown requested");
                    break;
                }

                // Update status
                {
                    let mut status = adapter.connection_status.write().await;
                    *status = ConnectionStatus::Connecting;
                }

                // Get WebSocket token
                let token = match adapter.get_ws_token().await {
                    Ok(t) => t,
                    Err(e) => {
                        error!("Failed to get WebSocket token: {}", e);
                        if strategy.can_retry() {
                            strategy.wait_before_retry().await;
                            continue;
                        } else {
                            break;
                        }
                    }
                };

                // Connect to authenticated WebSocket
                let ws_result = adapter.connect_authenticated().await;
                let mut ws = match ws_result {
                    Ok(stream) => stream,
                    Err(e) => {
                        error!("WebSocket connection failed: {}", e);
                        if strategy.can_retry() {
                            strategy.wait_before_retry().await;
                            continue;
                        } else {
                            break;
                        }
                    }
                };

                // Connection successful
                strategy.reset();
                let count = adapter.reconnect_count.fetch_add(1, Ordering::Relaxed);
                info!("Kraken user WebSocket connected (reconnect #{})", count);

                {
                    let mut status = adapter.connection_status.write().await;
                    *status = ConnectionStatus::Connected;
                    let mut health = adapter.health_data.write().await;
                    health.reconnect_count = count;
                }

                // Initialize heartbeat
                let heartbeat = HeartbeatMonitor::new(HeartbeatConfig::production());

                // Subscribe to executions (WebSocket v2 format)
                // snap_orders: include open orders snapshot on connect
                // snap_trades: include recent trade history on connect
                let subscribe_msg = serde_json::json!({
                    "method": "subscribe",
                    "params": {
                        "channel": "executions",
                        "snap_orders": true,
                        "snap_trades": false,
                        "token": token
                    }
                });

                if let Err(e) = ws.send(Message::Text(subscribe_msg.to_string())).await {
                    error!("Failed to send subscription: {}", e);
                    continue 'reconnect;
                }

                debug!("Sent Kraken executions subscription");

                // Message handling loop
                'message_loop: loop {
                    tokio::select! {
                        // Check shutdown
                        _ = shutdown_rx.recv() => {
                            info!("Shutdown requested during message loop");
                            break 'reconnect;
                        }

                        // Check heartbeat every 5 seconds
                        _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
                            if !heartbeat.is_alive().await {
                                warn!("Kraken heartbeat timeout - reconnecting");
                                break 'message_loop;
                            }
                        }

                        // Handle WebSocket messages
                        msg = ws.next() => {
                            match msg {
                                Some(Ok(Message::Text(text))) => {
                                    heartbeat.record_message_received().await;

                                    if let Err(e) = Self::handle_user_message(&text, &tx).await {
                                        warn!("Failed to parse user message: {} - raw: {}", e, text);
                                    }
                                }
                                Some(Ok(Message::Ping(data))) => {
                                    heartbeat.record_ping_sent().await;
                                    {
                                        let mut health = adapter.health_data.write().await;
                                        health.last_ping_ms = Some(Self::now_millis());
                                    }
                                    if let Err(e) = ws.send(Message::Pong(data)).await {
                                        error!("Failed to send pong: {}", e);
                                        break 'message_loop;
                                    }
                                }
                                Some(Ok(Message::Pong(_))) => {
                                    heartbeat.record_pong_received().await;
                                    {
                                        let mut health = adapter.health_data.write().await;
                                        health.last_pong_ms = Some(Self::now_millis());
                                    }
                                }
                                Some(Ok(Message::Close(_))) => {
                                    info!("Kraken WebSocket closed by server");
                                    break 'message_loop;
                                }
                                Some(Err(e)) => {
                                    error!("Kraken WebSocket error: {}", e);
                                    {
                                        let mut health = adapter.health_data.write().await;
                                        health.error_msg = Some(e.to_string());
                                    }
                                    break 'message_loop;
                                }
                                None => {
                                    warn!("Kraken WebSocket stream ended");
                                    break 'message_loop;
                                }
                                _ => {}
                            }
                        }
                    }
                }

                // Connection lost - update status
                {
                    let mut status = adapter.connection_status.write().await;
                    *status = ConnectionStatus::Reconnecting;
                }

                if !strategy.can_retry() {
                    error!("Max Kraken reconnection attempts reached");
                    break;
                }

                strategy.wait_before_retry().await;
            }

            // Final status update
            {
                let mut status = adapter.connection_status.write().await;
                *status = ConnectionStatus::Disconnected;
            }

            error!("Kraken user WebSocket task terminated");
        });

        Ok(rx)
    }

    async fn subscribe_books(&self, symbols: &[&str]) -> Result<mpsc::Receiver<BookUpdate>> {
        let (tx, rx) = mpsc::channel(1000);
        let adapter = self.clone();
        let symbols_vec: Vec<String> = symbols.iter().map(|s| s.to_string()).collect();

        tokio::spawn(async move {
            let reconnect_config = ReconnectConfig::production();
            let mut strategy = ReconnectStrategy::new(reconnect_config);

            'reconnect: loop {
                // Connect to public WebSocket
                let ws_result = adapter.connect_public().await;
                let mut ws = match ws_result {
                    Ok(stream) => stream,
                    Err(e) => {
                        error!("Book WebSocket connection failed: {}", e);
                        if strategy.can_retry() {
                            strategy.wait_before_retry().await;
                            continue;
                        } else {
                            break;
                        }
                    }
                };

                // Connection successful
                strategy.reset();
                info!("Kraken book WebSocket connected");

                // Initialize heartbeat
                let heartbeat = HeartbeatMonitor::new(HeartbeatConfig::production());

                // Subscribe to all symbols
                for symbol in &symbols_vec {
                    let subscribe_msg = serde_json::json!({
                        "method": "subscribe",
                        "params": {
                            "channel": "book",
                            "symbol": [symbol],
                            "depth": 10
                        }
                    });

                    if let Err(e) = ws.send(Message::Text(subscribe_msg.to_string())).await {
                        error!("Failed to send book subscription for {}: {}", symbol, e);
                        continue 'reconnect;
                    }
                }

                debug!("Sent Kraken book subscriptions for {} symbols", symbols_vec.len());

                // Message handling loop
                'message_loop: loop {
                    tokio::select! {
                        // Check heartbeat every 5 seconds
                        _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
                            if !heartbeat.is_alive().await {
                                warn!("Kraken book heartbeat timeout - reconnecting");
                                break 'message_loop;
                            }
                        }

                        // Handle WebSocket messages
                        msg = ws.next() => {
                            match msg {
                                Some(Ok(Message::Text(text))) => {
                                    heartbeat.record_message_received().await;

                                    if let Err(e) = Self::handle_book_message(&text, &tx).await {
                                        warn!("Failed to parse book message: {}", e);
                                    }
                                }
                                Some(Ok(Message::Ping(data))) => {
                                    heartbeat.record_ping_sent().await;
                                    if let Err(e) = ws.send(Message::Pong(data)).await {
                                        error!("Failed to send pong: {}", e);
                                        break 'message_loop;
                                    }
                                }
                                Some(Ok(Message::Pong(_))) => {
                                    heartbeat.record_pong_received().await;
                                }
                                Some(Ok(Message::Close(_))) => {
                                    info!("Kraken book WebSocket closed by server");
                                    break 'message_loop;
                                }
                                Some(Err(e)) => {
                                    error!("Kraken book WebSocket error: {}", e);
                                    break 'message_loop;
                                }
                                None => {
                                    warn!("Kraken book WebSocket stream ended");
                                    break 'message_loop;
                                }
                                _ => {}
                            }
                        }
                    }
                }

                if !strategy.can_retry() {
                    error!("Max Kraken book reconnection attempts reached");
                    break;
                }

                strategy.wait_before_retry().await;
            }

            error!("Kraken book WebSocket task terminated");
        });

        Ok(rx)
    }

    async fn subscribe_trades(&self, symbols: &[&str]) -> Result<mpsc::Receiver<TradeEvent>> {
        let (tx, rx) = mpsc::channel(1000);
        let adapter = self.clone();
        let symbols_vec: Vec<String> = symbols.iter().map(|s| s.to_string()).collect();

        tokio::spawn(async move {
            let reconnect_config = ReconnectConfig::production();
            let mut strategy = ReconnectStrategy::new(reconnect_config);

            'reconnect: loop {
                // Connect to public WebSocket
                let ws_result = adapter.connect_public().await;
                let mut ws = match ws_result {
                    Ok(stream) => stream,
                    Err(e) => {
                        error!("Trade WebSocket connection failed: {}", e);
                        if strategy.can_retry() {
                            strategy.wait_before_retry().await;
                            continue;
                        } else {
                            break;
                        }
                    }
                };

                // Connection successful
                strategy.reset();
                info!("Kraken trade WebSocket connected");

                // Initialize heartbeat
                let heartbeat = HeartbeatMonitor::new(HeartbeatConfig::production());

                // Subscribe to all symbols
                for symbol in &symbols_vec {
                    let subscribe_msg = serde_json::json!({
                        "method": "subscribe",
                        "params": {
                            "channel": "trade",
                            "symbol": [symbol]
                        }
                    });

                    if let Err(e) = ws.send(Message::Text(subscribe_msg.to_string())).await {
                        error!("Failed to send trade subscription for {}: {}", symbol, e);
                        continue 'reconnect;
                    }
                }

                debug!("Sent Kraken trade subscriptions for {} symbols", symbols_vec.len());

                // Message handling loop
                'message_loop: loop {
                    tokio::select! {
                        // Check heartbeat every 5 seconds
                        _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
                            if !heartbeat.is_alive().await {
                                warn!("Kraken trade heartbeat timeout - reconnecting");
                                break 'message_loop;
                            }
                        }

                        // Handle WebSocket messages
                        msg = ws.next() => {
                            match msg {
                                Some(Ok(Message::Text(text))) => {
                                    heartbeat.record_message_received().await;

                                    if let Err(e) = Self::handle_trade_message(&text, &tx).await {
                                        warn!("Failed to parse trade message: {}", e);
                                    }
                                }
                                Some(Ok(Message::Ping(data))) => {
                                    heartbeat.record_ping_sent().await;
                                    if let Err(e) = ws.send(Message::Pong(data)).await {
                                        error!("Failed to send pong: {}", e);
                                        break 'message_loop;
                                    }
                                }
                                Some(Ok(Message::Pong(_))) => {
                                    heartbeat.record_pong_received().await;
                                }
                                Some(Ok(Message::Close(_))) => {
                                    info!("Kraken trade WebSocket closed by server");
                                    break 'message_loop;
                                }
                                Some(Err(e)) => {
                                    error!("Kraken trade WebSocket error: {}", e);
                                    break 'message_loop;
                                }
                                None => {
                                    warn!("Kraken trade WebSocket stream ended");
                                    break 'message_loop;
                                }
                                _ => {}
                            }
                        }
                    }
                }

                if !strategy.can_retry() {
                    error!("Max Kraken trade reconnection attempts reached");
                    break;
                }

                strategy.wait_before_retry().await;
            }

            error!("Kraken trade WebSocket task terminated");
        });

        Ok(rx)
    }

    async fn health(&self) -> Result<HealthStatus> {
        let status = *self.connection_status.read().await;
        let health = self.health_data.read().await;

        let latency_ms = if let (Some(ping), Some(pong)) = (health.last_ping_ms, health.last_pong_ms) {
            if pong > ping {
                Some(pong - ping)
            } else {
                None
            }
        } else {
            None
        };

        Ok(HealthStatus {
            status,
            last_ping_ms: health.last_ping_ms,
            last_pong_ms: health.last_pong_ms,
            latency_ms,
            reconnect_count: health.reconnect_count,
            error_msg: health.error_msg.clone(),
        })
    }

    async fn reconnect(&self) -> Result<()> {
        *self.connection_status.write().await = ConnectionStatus::Reconnecting;
        self.health_data.write().await.reconnect_count += 1;

        // Reconnect logic would go here
        // For now, just update status
        *self.connection_status.write().await = ConnectionStatus::Connecting;

        Ok(())
    }
}
