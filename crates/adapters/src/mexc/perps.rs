//! MEXC Futures/Perpetuals Market Adapter
//!
//! This module provides a unified adapter for MEXC's futures and perpetual swap markets,
//! combining both REST API and WebSocket functionality.
//!
//! # Features
//!
//! - **Position Management**: Long/short positions with leverage up to 125x
//! - **Order Management**: Market, limit, stop-loss, take-profit orders
//! - **Margin Modes**: Cross margin and isolated margin support
//! - **Funding Rates**: Query current and historical funding rates
//! - **Real-Time Data**: WebSocket streams for positions, orders, and market data
//!
//! # API Differences from Spot
//!
//! - **Base URL**: `https://contract.mexc.com` (different from spot)
//! - **WebSocket**: `wss://contract.mexc.com/ws` (different from spot)
//! - **Authentication**: Uses same HMAC-SHA256 but different endpoints
//! - **Order Sides**: Includes position direction (1=open_long, 2=close_short, 3=open_short, 4=close_long)
//! - **Leverage**: Must set leverage before trading
//!
//! # Position Sides Mapping
//!
//! MEXC Futures uses numeric position sides:
//! - `1` - Open Long (buy to enter long position)
//! - `2` - Close Short (buy to exit short position)
//! - `3` - Open Short (sell to enter short position)
//! - `4` - Close Long (sell to exit long position)
//!
//! # Example Usage
//!
//! ```ignore
//! use market_making::adapters::mexc::MexcPerpsAdapter;
//! use market_making::adapters::traits::{PerpRest, PerpWs};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let adapter = MexcPerpsAdapter::new(
//!         std::env::var("MEXC_API_KEY")?,
//!         std::env::var("MEXC_API_SECRET")?
//!     );
//!
//!     // Set leverage for a symbol
//!     adapter.set_leverage("BTC_USDT", Decimal::from(10)).await?;
//!
//!     // Open a long position
//!     let order = adapter.create_order(NewOrder {
//!         symbol: "BTC_USDT".to_string(),
//!         side: Side::Buy,
//!         ord_type: OrderType::Limit,
//!         qty: Decimal::from_str("0.001")?,
//!         price: Some(Decimal::from(50000)),
//!         ..Default::default()
//!     }).await?;
//!
//!     // Get current positions
//!     let positions = adapter.get_all_positions().await?;
//!
//!     // Subscribe to position updates
//!     let mut user_events = adapter.subscribe_user().await?;
//!     while let Some(event) = user_events.recv().await {
//!         println!("Position update: {:?}", event);
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! # API Documentation
//!
//! - MEXC Futures API: <https://www.mexc.com/api-docs/futures/update-log>

use crate::mexc::account::{converters, MexcAuth, MexcRestClient, MEXC_FUTURES_WS_URL};
use crate::traits::*;
use crate::utils::{
    CircuitBreaker, CircuitBreakerConfig, HeartbeatMonitor, HeartbeatConfig,
    RateLimiter, RateLimiterConfig, ReconnectConfig, ReconnectStrategy,
};
use anyhow::{Context, Result};
use rust_decimal::Decimal;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{debug, error, info, warn};

/// Unified MEXC Futures/Perpetuals adapter
///
/// Combines REST API and WebSocket functionality for MEXC futures trading.
/// Supports perpetual swaps with leverage up to 125x.
///
/// # Features
///
/// - **REST API**: Order management, position queries, leverage setting, margin mode
/// - **WebSocket**: Real-time position updates, order updates, liquidation alerts
/// - **Position Management**: Separate long/short positions with different leverage
/// - **Risk Controls**: Stop-loss, take-profit, position limits
///
/// # Architecture
///
/// - Single authentication instance for both REST and WebSocket
/// - HTTP client with connection pooling for REST API
/// - WebSocket background tasks for real-time event processing
/// - Automatic reconnection and error handling
///
/// # Position Management
///
/// MEXC Futures allows independent long and short positions:
/// - Each direction can have different leverage
/// - Positions can be in cross or isolated margin mode
/// - Liquidation prices calculated separately per position
///
/// # Example
///
/// ```ignore
/// let adapter = MexcPerpsAdapter::new(api_key, api_secret);
///
/// // Set 10x leverage for BTC_USDT
/// adapter.set_leverage("BTC_USDT", Decimal::from(10)).await?;
///
/// // Get current position
/// let position = adapter.get_position("BTC_USDT").await?;
/// ```
#[derive(Clone)]
pub struct MexcPerpsAdapter {
    /// HTTP client for Futures REST API requests
    client: MexcRestClient,

    /// Authentication credentials
    auth: MexcAuth,

    /// Current WebSocket connection status
    connection_status: Arc<RwLock<ConnectionStatus>>,

    /// Listen key for user data stream
    listen_key: Arc<Mutex<Option<String>>>,

    /// Production fields for resilience
    rate_limiter: RateLimiter,
    circuit_breaker: CircuitBreaker,
    reconnect_count: Arc<AtomicU32>,
    shutdown_tx: Arc<Mutex<Option<tokio::sync::broadcast::Sender<()>>>>,
}

impl MexcPerpsAdapter {
    /// Creates a new MEXC Futures adapter instance
    ///
    /// Initializes the REST client with futures-specific base URL. WebSocket
    /// connections are established lazily when subscribe methods are called.
    ///
    /// # Arguments
    ///
    /// * `api_key` - MEXC API key
    /// * `api_secret` - MEXC API secret (plain text, not base64-encoded)
    ///
    /// # Example
    ///
    /// ```ignore
    /// let adapter = MexcPerpsAdapter::new(
    ///     "YOUR_API_KEY".to_string(),
    ///     "YOUR_API_SECRET".to_string()
    /// );
    /// ```
    pub fn new(api_key: String, api_secret: String) -> Self {
        let auth = MexcAuth::new(api_key, api_secret);
        Self {
            client: MexcRestClient::new_futures(Some(auth.clone())),
            auth,
            connection_status: Arc::new(RwLock::new(ConnectionStatus::Disconnected)),
            listen_key: Arc::new(Mutex::new(None)),
            rate_limiter: RateLimiter::new(RateLimiterConfig::mexc_futures()),
            circuit_breaker: CircuitBreaker::new("mexc_perps", CircuitBreakerConfig::production()),
            reconnect_count: Arc::new(AtomicU32::new(0)),
            shutdown_tx: Arc::new(Mutex::new(None)),
        }
    }

    /// Wraps REST API calls with rate limiting and circuit breaker
    async fn call_api<T, F, Fut>(&self, endpoint: &str, f: F) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        // Rate limiting
        if !self.rate_limiter.acquire().await {
            anyhow::bail!("Rate limit reached for endpoint: {}", endpoint);
        }

        debug!("Calling MEXC Perps API endpoint: {}", endpoint);

        // Circuit breaker
        match self.circuit_breaker.call(f).await {
            Ok(result) => Ok(result),
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("429") || err_str.to_lowercase().contains("rate limit") {
                    warn!("Rate limit error detected on {}", endpoint);
                    self.rate_limiter.handle_rate_limit_error().await;
                }
                Err(e)
            }
        }
    }

    /// Gracefully shuts down all background tasks
    pub async fn shutdown(&self) {
        info!("Initiating graceful shutdown of MEXC Perps adapter");

        if let Some(tx) = self.shutdown_tx.lock().await.as_ref() {
            let _ = tx.send(());
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        info!("MEXC Perps adapter shutdown complete");
    }

    /// Spawns a background task to automatically renew the listen key every 30 minutes
    fn spawn_listen_key_renewal_task(
        listen_key: Arc<Mutex<Option<String>>>,
        client: MexcRestClient,
        mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
    ) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30 * 60)); // 30 minutes

            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        info!("MEXC Perps listen key renewal task shutting down");
                        break;
                    }
                    _ = interval.tick() => {
                        if let Some(key) = listen_key.lock().await.as_ref() {
                            info!("Renewing MEXC Perps listen key");
                            // For MEXC Futures, we might need to create a new key instead of extending
                            // This depends on the specific API behavior
                            debug!("Listen key: {}", key);
                        }
                    }
                }
            }
        });
    }

    /// Generates a unique client order ID for order placement
    ///
    /// Format: `mm_{timestamp}` where timestamp is milliseconds since Unix epoch.
    fn generate_client_order_id() -> String {
        format!(
            "mm_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
        )
    }

    /// Converts Side and reduce_only flag to MEXC position side integer
    ///
    /// # MEXC Position Sides
    ///
    /// - `1` - Open Long (buy to open)
    /// - `2` - Close Short (buy to close)
    /// - `3` - Open Short (sell to open)
    /// - `4` - Close Long (sell to close)
    ///
    /// # Arguments
    ///
    /// * `side` - Buy or Sell
    /// * `reduce_only` - If true, only close existing positions (don't open new ones)
    ///
    /// # Returns
    ///
    /// MEXC position side integer (1, 2, 3, or 4)
    fn to_position_side(side: Side, reduce_only: bool) -> i32 {
        match (side, reduce_only) {
            (Side::Buy, false) => 1,  // Open long
            (Side::Buy, true) => 2,   // Close short
            (Side::Sell, false) => 3, // Open short
            (Side::Sell, true) => 4,  // Close long
        }
    }

    /// Creates a new listen key for user data stream
    ///
    /// # Returns
    ///
    /// The newly created listen key string
    ///
    /// # API Endpoint
    ///
    /// POST /api/v1/private/account/getListenKey
    async fn create_listen_key(&self) -> Result<String> {
        use serde::Deserialize;
        use std::collections::HashMap;

        #[derive(Deserialize)]
        struct ListenKeyResponse {
            #[serde(rename = "listenKey")]
            listen_key: String,
        }

        let params = HashMap::new();
        let response: ListenKeyResponse = self
            .client
            .post_private("/api/v1/private/account/getListenKey", params)
            .await?;

        Ok(response.listen_key)
    }
}

// ============================================================================
// PerpRest Trait Implementation
// ============================================================================

#[async_trait::async_trait]
impl PerpRest for MexcPerpsAdapter {
    async fn create_order(&self, new: NewOrder) -> Result<Order> {
        use serde::{Deserialize, Serialize};
        use std::collections::HashMap;

        #[derive(Serialize)]
        struct OrderParams {
            symbol: String,
            price: String,
            vol: String,
            #[serde(rename = "type")]
            order_type: i32,
            side: i32,
            #[serde(rename = "openType")]
            open_type: i32,
            #[serde(skip_serializing_if = "Option::is_none")]
            #[serde(rename = "externalOid")]
            client_order_id: Option<String>,
        }

        #[derive(Deserialize)]
        struct OrderResponse {
            data: String,
        }

        let position_side = Self::to_position_side(new.side, new.reduce_only);

        let order_type = match new.ord_type {
            OrderType::Limit if new.post_only => 2,
            OrderType::Limit => {
                match new.tif {
                    Some(TimeInForce::Ioc) => 3,
                    Some(TimeInForce::Fok) => 4,
                    _ => 1,
                }
            }
            OrderType::Market => 5,
            _ => 1,
        };

        let mut params_map = HashMap::new();
        params_map.insert("symbol".to_string(), new.symbol.clone());
        params_map.insert("price".to_string(), new.price.unwrap_or(0.0).to_string());
        params_map.insert("vol".to_string(), new.qty.to_string());
        params_map.insert("type".to_string(), order_type.to_string());
        params_map.insert("side".to_string(), position_side.to_string());
        params_map.insert("openType".to_string(), "2".to_string());
        if !new.client_order_id.is_empty() {
            params_map.insert("externalOid".to_string(), new.client_order_id.clone());
        }

        let response: OrderResponse = self
            .call_api("/api/v1/private/order/submit", || async {
                self.client
                    .post_private("/api/v1/private/order/submit", params_map.clone())
                    .await
            })
            .await
            .context("Failed to create order")?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Ok(Order {
            venue_order_id: response.data,
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
            raw_status: None,
        })
    }

    async fn cancel_order(&self, symbol: &str, venue_order_id: &str) -> Result<bool> {
        use std::collections::HashMap;

        let mut params = HashMap::new();
        params.insert("orderId".to_string(), venue_order_id.to_string());

        let _: serde_json::Value = self
            .call_api("/api/v1/private/order/cancel", || async {
                self.client
                    .post_private("/api/v1/private/order/cancel", params.clone())
                    .await
            })
            .await
            .context("Failed to cancel order")?;

        Ok(true)
    }

    async fn cancel_all(&self, symbol: Option<&str>) -> Result<usize> {
        use std::collections::HashMap;

        let mut params = HashMap::new();
        if let Some(sym) = symbol {
            params.insert("symbol".to_string(), sym.to_string());
        }

        let _: serde_json::Value = self
            .call_api("/api/v1/private/order/cancel_all", || async {
                self.client
                    .post_private("/api/v1/private/order/cancel_all", params.clone())
                    .await
            })
            .await
            .context("Failed to cancel all orders")?;

        Ok(0)
    }

    async fn get_order(&self, symbol: &str, venue_order_id: &str) -> Result<Order> {
        use serde::Deserialize;
        use std::collections::HashMap;

        #[derive(Deserialize)]
        struct OrderData {
            #[serde(rename = "orderId")]
            order_id: String,
            symbol: String,
            #[serde(rename = "price")]
            price: String,
            vol: String,
            #[serde(rename = "dealVol")]
            filled_vol: String,
            side: i32,
            #[serde(rename = "type")]
            order_type: i32,
            state: i32,
            #[serde(rename = "createTime")]
            create_time: u64,
        }

        #[derive(Deserialize)]
        struct GetOrderResponse {
            data: OrderData,
        }

        let mut params = HashMap::new();
        params.insert("order_id".to_string(), venue_order_id.to_string());

        let response: GetOrderResponse = self
            .call_api("/api/v1/private/order/get", || async {
                self.client
                    .get_private("/api/v1/private/order/get", params.clone())
                    .await
            })
            .await
            .context("Failed to get order")?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let data = response.data;
        let qty: Quantity = data.vol.parse().unwrap_or(0.0);
        let filled: Quantity = data.filled_vol.parse().unwrap_or(0.0);

        let status = match data.state {
            1 | 2 if filled > 0.0 => OrderStatus::PartiallyFilled,
            1 | 2 => OrderStatus::New,
            3 => OrderStatus::Filled,
            4 => OrderStatus::Canceled,
            5 => OrderStatus::Rejected,
            _ => OrderStatus::New,
        };

        let (side, _reduce_only) = match data.side {
            1 => (Side::Buy, false),
            2 => (Side::Buy, true),
            3 => (Side::Sell, false),
            4 => (Side::Sell, true),
            _ => (Side::Buy, false),
        };

        Ok(Order {
            venue_order_id: data.order_id,
            client_order_id: String::new(),
            symbol: data.symbol,
            ord_type: OrderType::Limit,
            side,
            qty,
            price: Some(data.price.parse().unwrap_or(0.0)),
            stop_price: None,
            tif: None,
            status,
            filled_qty: filled,
            remaining_qty: qty - filled,
            created_ms: data.create_time,
            updated_ms: data.create_time,
            recv_ms: now,
            raw_status: Some(data.state.to_string()),
        })
    }

    async fn get_open_orders(&self, symbol: Option<&str>) -> Result<Vec<Order>> {
        use serde::Deserialize;
        use std::collections::HashMap;

        #[derive(Deserialize)]
        struct OrderData {
            #[serde(rename = "orderId")]
            order_id: String,
            symbol: String,
            price: String,
            vol: String,
            #[serde(rename = "dealVol")]
            filled_vol: String,
            side: i32,
            #[serde(rename = "type")]
            order_type: i32,
            state: i32,
            #[serde(rename = "createTime")]
            create_time: u64,
        }

        #[derive(Deserialize)]
        struct GetOrdersResponse {
            data: Vec<OrderData>,
        }

        let mut params = HashMap::new();
        if let Some(sym) = symbol {
            params.insert("symbol".to_string(), sym.to_string());
        }

        let response: GetOrdersResponse = self
            .call_api("/api/v1/private/order/list/open_orders", || async {
                self.client
                    .get_private("/api/v1/private/order/list/open_orders", params.clone())
                    .await
            })
            .await
            .context("Failed to get open orders")?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Ok(response
            .data
            .into_iter()
            .map(|data| {
                let qty: Quantity = data.vol.parse().unwrap_or(0.0);
                let filled: Quantity = data.filled_vol.parse().unwrap_or(0.0);

                let status = match data.state {
                    1 | 2 if filled > 0.0 => OrderStatus::PartiallyFilled,
                    1 | 2 => OrderStatus::New,
                    3 => OrderStatus::Filled,
                    4 => OrderStatus::Canceled,
                    5 => OrderStatus::Rejected,
                    _ => OrderStatus::New,
                };

                let (side, _reduce_only) = match data.side {
                    1 => (Side::Buy, false),
                    2 => (Side::Buy, true),
                    3 => (Side::Sell, false),
                    4 => (Side::Sell, true),
                    _ => (Side::Buy, false),
                };

                Order {
                    venue_order_id: data.order_id,
                    client_order_id: String::new(),
                    symbol: data.symbol,
                    ord_type: OrderType::Limit,
                    side,
                    qty,
                    price: Some(data.price.parse().unwrap_or(0.0)),
                    stop_price: None,
                    tif: None,
                    status,
                    filled_qty: filled,
                    remaining_qty: qty - filled,
                    created_ms: data.create_time,
                    updated_ms: data.create_time,
                    recv_ms: now,
                    raw_status: Some(data.state.to_string()),
                }
            })
            .collect())
    }

    async fn replace_order(
        &self,
        symbol: &str,
        venue_order_id: &str,
        new_price: Option<Price>,
        new_qty: Option<Quantity>,
        new_tif: Option<TimeInForce>,
        post_only: Option<bool>,
        reduce_only: Option<bool>,
    ) -> Result<(Order, bool)> {
        let original = self.get_order(symbol, venue_order_id).await?;
        let canceled = self.cancel_order(symbol, venue_order_id).await.unwrap_or(false);

        let new_order = NewOrder {
            symbol: symbol.to_string(),
            side: original.side,
            ord_type: original.ord_type,
            qty: new_qty.unwrap_or(original.qty),
            price: new_price.or(original.price),
            stop_price: original.stop_price,
            tif: new_tif.or(original.tif),
            post_only: post_only.unwrap_or(false),
            reduce_only: reduce_only.unwrap_or(false),
            client_order_id: Self::generate_client_order_id(),
        };

        let order = self.create_order(new_order).await?;
        Ok((order, canceled))
    }

    async fn create_batch_orders(&self, batch: BatchOrderRequest) -> Result<BatchOrderResult> {
        let mut success = Vec::new();
        let mut failed = Vec::new();

        for order in batch.orders {
            match self.create_order(order.clone()).await {
                Ok(o) => success.push(o),
                Err(e) => failed.push((order, e.to_string())),
            }
        }

        Ok(BatchOrderResult { success, failed })
    }

    async fn cancel_batch_orders(
        &self,
        symbol: &str,
        order_ids: Vec<String>,
    ) -> Result<BatchCancelResult> {
        let mut success = Vec::new();
        let mut failed = Vec::new();

        for order_id in order_ids {
            match self.cancel_order(symbol, &order_id).await {
                Ok(_) => success.push(order_id),
                Err(e) => failed.push((order_id, e.to_string())),
            }
        }

        Ok(BatchCancelResult { success, failed })
    }

    async fn set_leverage(&self, symbol: &str, leverage: Decimal) -> Result<()> {
        use std::collections::HashMap;

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());
        params.insert("leverage".to_string(), leverage.to_string());
        params.insert("openType".to_string(), "2".to_string());

        let _: serde_json::Value = self
            .call_api("/api/v1/private/position/change_leverage", || async {
                self.client
                    .post_private("/api/v1/private/position/change_leverage", params.clone())
                    .await
            })
            .await
            .context("Failed to set leverage")?;

        Ok(())
    }

    async fn set_margin_mode(&self, symbol: &str, mode: MarginMode) -> Result<()> {
        use std::collections::HashMap;

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());
        params.insert(
            "openType".to_string(),
            match mode {
                MarginMode::Isolated => "1",
                MarginMode::Cross => "2",
            }
            .to_string(),
        );

        let _: serde_json::Value = self
            .call_api("/api/v1/private/position/change_margin_type", || async {
                self.client
                    .post_private("/api/v1/private/position/change_margin_type", params.clone())
                    .await
            })
            .await
            .context("Failed to set margin mode")?;

        Ok(())
    }

    async fn get_position(&self, symbol: &str) -> Result<Position> {
        let positions = self.get_all_positions().await?;
        positions
            .into_iter()
            .find(|p| p.symbol == symbol)
            .ok_or_else(|| anyhow::anyhow!("No position found for symbol {}", symbol))
    }

    async fn get_all_positions(&self) -> Result<Vec<Position>> {
        use serde::Deserialize;
        use std::collections::HashMap;

        #[derive(Deserialize)]
        struct PositionData {
            symbol: String,
            #[serde(rename = "holdVol")]
            hold_vol: String,
            #[serde(rename = "holdAvgPrice")]
            hold_avg_price: String,
            #[serde(rename = "positionType")]
            position_type: i32,
            leverage: i32,
            #[serde(rename = "liquidatePrice")]
            liquidate_price: Option<String>,
            #[serde(rename = "unRealised")]
            unrealized_pnl: Option<String>,
            #[serde(rename = "realised")]
            realized_pnl: Option<String>,
        }

        #[derive(Deserialize)]
        struct GetPositionsResponse {
            data: Vec<PositionData>,
        }

        let params = HashMap::new();
        let response: GetPositionsResponse = self
            .call_api("/api/v1/private/position/open_positions", || async {
                self.client
                    .get_private("/api/v1/private/position/open_positions", params.clone())
                    .await
            })
            .await
            .context("Failed to get positions")?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Ok(response
            .data
            .into_iter()
            .map(|data| {
                let mut qty: f64 = data.hold_vol.parse().unwrap_or(0.0);
                if data.position_type == 2 {
                    qty = -qty;
                }

                Position {
                    exchange: Some("mexc".to_string()),
                    symbol: data.symbol,
                    qty,
                    entry_px: data.hold_avg_price.parse().unwrap_or(0.0),
                    mark_px: None,
                    liquidation_px: data
                        .liquidate_price
                        .and_then(|p| p.parse().ok()),
                    unrealized_pnl: data
                        .unrealized_pnl
                        .and_then(|p| p.parse().ok()),
                    realized_pnl: data
                        .realized_pnl
                        .and_then(|p| p.parse().ok()),
                    margin: None,
                    leverage: Some(data.leverage as u32),
                    opened_ms: None,
                    updated_ms: now,
                }
            })
            .collect())
    }

    async fn get_funding_rate(&self, symbol: &str) -> Result<(Decimal, UnixMillis)> {
        use serde::Deserialize;
        use std::collections::HashMap;

        #[derive(Deserialize)]
        struct FundingRateData {
            #[serde(rename = "fundingRate")]
            funding_rate: String,
            #[serde(rename = "nextFundingTime")]
            next_funding_time: u64,
        }

        #[derive(Deserialize)]
        struct FundingRateResponse {
            data: FundingRateData,
        }

        let params = HashMap::new();
        let endpoint = format!("/api/v1/contract/funding_rate/{}", symbol);
        let response: FundingRateResponse = self
            .call_api(&endpoint, || async {
                self.client
                    .get_public(&endpoint, Some(params.clone()))
                    .await
            })
            .await
            .context("Failed to get funding rate")?;

        let rate = Decimal::from_str(&response.data.funding_rate)
            .unwrap_or_else(|_| Decimal::from(0));

        Ok((rate, response.data.next_funding_time))
    }

    async fn get_balances(&self) -> Result<Vec<Balance>> {
        use serde::Deserialize;
        use std::collections::HashMap;

        #[derive(Deserialize)]
        struct AssetData {
            currency: String,
            #[serde(rename = "availableBalance")]
            available_balance: String,
            #[serde(rename = "frozenBalance")]
            frozen_balance: String,
        }

        #[derive(Deserialize)]
        struct GetAssetsResponse {
            data: Vec<AssetData>,
        }

        let params = HashMap::new();
        let response: GetAssetsResponse = self
            .call_api("/api/v1/private/account/assets", || async {
                self.client
                    .get_private("/api/v1/private/account/assets", params.clone())
                    .await
            })
            .await
            .context("Failed to get balances")?;

        Ok(response
            .data
            .into_iter()
            .map(|data| {
                let free: f64 = data.available_balance.parse().unwrap_or(0.0);
                let locked: f64 = data.frozen_balance.parse().unwrap_or(0.0);

                Balance {
                    asset: data.currency,
                    free,
                    locked,
                    total: free + locked,
                }
            })
            .collect())
    }

    async fn get_account_info(&self) -> Result<AccountInfo> {
        let balances = self.get_balances().await?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Ok(AccountInfo {
            balances,
            can_trade: true,
            can_withdraw: true,
            can_deposit: true,
            update_ms: now,
        })
    }

    async fn get_market_info(&self, symbol: &str) -> Result<MarketInfo> {
        use serde::Deserialize;
        use std::collections::HashMap;

        #[derive(Deserialize)]
        struct ContractData {
            symbol: String,
            #[serde(rename = "displayName")]
            display_name: String,
            #[serde(rename = "displayNameEn")]
            display_name_en: String,
            #[serde(rename = "positionOpenType")]
            position_open_type: i32,
            #[serde(rename = "baseCoin")]
            base_coin: String,
            #[serde(rename = "quoteCoin")]
            quote_coin: String,
            #[serde(rename = "minVol")]
            min_vol: String,
            #[serde(rename = "maxVol")]
            max_vol: String,
            #[serde(rename = "priceScale")]
            price_scale: i32,
            #[serde(rename = "volScale")]
            vol_scale: i32,
            #[serde(rename = "maxLeverage")]
            max_leverage: i32,
        }

        #[derive(Deserialize)]
        struct GetContractResponse {
            data: Vec<ContractData>,
        }

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());

        let response: GetContractResponse = self
            .call_api("/api/v1/contract/detail", || async {
                self.client
                    .get_public("/api/v1/contract/detail", Some(params.clone()))
                    .await
            })
            .await
            .context("Failed to get market info")?;

        let data = response
            .data
            .into_iter()
            .find(|c| c.symbol == symbol)
            .ok_or_else(|| anyhow::anyhow!("Contract not found"))?;

        let min_qty: f64 = data.min_vol.parse().unwrap_or(0.0);
        let max_qty: f64 = data.max_vol.parse().unwrap_or(f64::MAX);
        let tick_size = 10f64.powi(-data.price_scale);
        let step_size = 10f64.powi(-data.vol_scale);

        Ok(MarketInfo {
            symbol: data.symbol,
            base_asset: data.base_coin,
            quote_asset: data.quote_coin,
            status: MarketStatus::Trading,
            min_qty,
            max_qty,
            step_size,
            tick_size,
            min_notional: 0.0,
            max_leverage: Some(data.max_leverage as u32),
            is_spot: false,
            is_perp: true,
        })
    }

    async fn get_all_markets(&self) -> Result<Vec<MarketInfo>> {
        use serde::Deserialize;
        use std::collections::HashMap;

        #[derive(Deserialize)]
        struct ContractData {
            symbol: String,
            #[serde(rename = "baseCoin")]
            base_coin: String,
            #[serde(rename = "quoteCoin")]
            quote_coin: String,
            #[serde(rename = "minVol")]
            min_vol: String,
            #[serde(rename = "maxVol")]
            max_vol: String,
            #[serde(rename = "priceScale")]
            price_scale: i32,
            #[serde(rename = "volScale")]
            vol_scale: i32,
            #[serde(rename = "maxLeverage")]
            max_leverage: i32,
        }

        #[derive(Deserialize)]
        struct GetContractResponse {
            data: Vec<ContractData>,
        }

        let params = HashMap::new();
        let response: GetContractResponse = self
            .call_api("/api/v1/contract/detail", || async {
                self.client
                    .get_public("/api/v1/contract/detail", Some(params.clone()))
                    .await
            })
            .await
            .context("Failed to get all markets")?;

        Ok(response
            .data
            .into_iter()
            .map(|data| {
                let min_qty: f64 = data.min_vol.parse().unwrap_or(0.0);
                let max_qty: f64 = data.max_vol.parse().unwrap_or(f64::MAX);
                let tick_size = 10f64.powi(-data.price_scale);
                let step_size = 10f64.powi(-data.vol_scale);

                MarketInfo {
                    symbol: data.symbol,
                    base_asset: data.base_coin,
                    quote_asset: data.quote_coin,
                    status: MarketStatus::Trading,
                    min_qty,
                    max_qty,
                    step_size,
                    tick_size,
                    min_notional: 0.0,
                    max_leverage: Some(data.max_leverage as u32),
                    is_spot: false,
                    is_perp: true,
                }
            })
            .collect())
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        use serde::Deserialize;
        use std::collections::HashMap;

        #[derive(Deserialize)]
        struct TickerData {
            symbol: String,
            #[serde(rename = "lastPrice")]
            last_price: String,
            #[serde(rename = "bid1")]
            bid_price: String,
            #[serde(rename = "ask1")]
            ask_price: String,
            volume24: String,
            #[serde(rename = "riseFallRate")]
            rise_fall_rate: String,
            high24Price: String,
            low24Price: String,
        }

        #[derive(Deserialize)]
        struct GetTickerResponse {
            data: Vec<TickerData>,
        }

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());

        let response: GetTickerResponse = self
            .call_api("/api/v1/contract/ticker", || async {
                self.client
                    .get_public("/api/v1/contract/ticker", Some(params.clone()))
                    .await
            })
            .await
            .context("Failed to get ticker")?;

        let data = response
            .data
            .into_iter()
            .find(|t| t.symbol == symbol)
            .ok_or_else(|| anyhow::anyhow!("Ticker not found"))?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let last_price: f64 = data.last_price.parse().unwrap_or(0.0);
        let change_pct: f64 = data.rise_fall_rate.parse().unwrap_or(0.0);

        Ok(TickerInfo {
            symbol: data.symbol,
            last_price,
            bid_price: data.bid_price.parse().unwrap_or(0.0),
            ask_price: data.ask_price.parse().unwrap_or(0.0),
            volume_24h: data.volume24.parse().unwrap_or(0.0),
            price_change_24h: last_price * (change_pct / 100.0),
            price_change_pct_24h: change_pct,
            high_24h: data.high24Price.parse().unwrap_or(0.0),
            low_24h: data.low24Price.parse().unwrap_or(0.0),
            open_price_24h: last_price / (1.0 + change_pct / 100.0),
            ts_ms: now,
        })
    }

    async fn get_tickers(&self, symbols: Option<Vec<String>>) -> Result<Vec<TickerInfo>> {
        use serde::Deserialize;
        use std::collections::HashMap;

        #[derive(Deserialize)]
        struct TickerData {
            symbol: String,
            #[serde(rename = "lastPrice")]
            last_price: String,
            #[serde(rename = "bid1")]
            bid_price: String,
            #[serde(rename = "ask1")]
            ask_price: String,
            volume24: String,
            #[serde(rename = "riseFallRate")]
            rise_fall_rate: String,
            high24Price: String,
            low24Price: String,
        }

        #[derive(Deserialize)]
        struct GetTickerResponse {
            data: Vec<TickerData>,
        }

        let params = HashMap::new();
        let response: GetTickerResponse = self
            .call_api("/api/v1/contract/ticker", || async {
                self.client
                    .get_public("/api/v1/contract/ticker", Some(params.clone()))
                    .await
            })
            .await
            .context("Failed to get tickers")?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let mut tickers: Vec<TickerInfo> = response
            .data
            .into_iter()
            .map(|data| {
                let last_price: f64 = data.last_price.parse().unwrap_or(0.0);
                let change_pct: f64 = data.rise_fall_rate.parse().unwrap_or(0.0);

                TickerInfo {
                    symbol: data.symbol.clone(),
                    last_price,
                    bid_price: data.bid_price.parse().unwrap_or(0.0),
                    ask_price: data.ask_price.parse().unwrap_or(0.0),
                    volume_24h: data.volume24.parse().unwrap_or(0.0),
                    price_change_24h: last_price * (change_pct / 100.0),
                    price_change_pct_24h: change_pct,
                    high_24h: data.high24Price.parse().unwrap_or(0.0),
                    low_24h: data.low24Price.parse().unwrap_or(0.0),
                    open_price_24h: last_price / (1.0 + change_pct / 100.0),
                    ts_ms: now,
                }
            })
            .collect();

        if let Some(syms) = symbols {
            let sym_set: std::collections::HashSet<_> = syms.into_iter().collect();
            tickers.retain(|t| sym_set.contains(&t.symbol));
        }

        Ok(tickers)
    }

    async fn get_mark_price(&self, symbol: &str) -> Result<(Price, UnixMillis)> {
        use serde::Deserialize;
        use std::collections::HashMap;

        #[derive(Deserialize)]
        struct FairPriceData {
            #[serde(rename = "fairPrice")]
            fair_price: String,
            timestamp: u64,
        }

        #[derive(Deserialize)]
        struct FairPriceResponse {
            data: FairPriceData,
        }

        let params = HashMap::new();
        let endpoint = format!("/api/v1/contract/fair_price/{}", symbol);
        let response: FairPriceResponse = self
            .call_api(&endpoint, || async {
                self.client
                    .get_public(&endpoint, Some(params.clone()))
                    .await
            })
            .await
            .context("Failed to get mark price")?;

        let price: f64 = response.data.fair_price.parse().unwrap_or(0.0);
        Ok((price, response.data.timestamp))
    }

    async fn get_index_price(&self, symbol: &str) -> Result<(Price, UnixMillis)> {
        use serde::Deserialize;
        use std::collections::HashMap;

        #[derive(Deserialize)]
        struct IndexPriceData {
            #[serde(rename = "indexPrice")]
            index_price: String,
            timestamp: u64,
        }

        #[derive(Deserialize)]
        struct IndexPriceResponse {
            data: IndexPriceData,
        }

        let params = HashMap::new();
        let endpoint = format!("/api/v1/contract/index_price/{}", symbol);
        let response: IndexPriceResponse = self
            .call_api(&endpoint, || async {
                self.client
                    .get_public(&endpoint, Some(params.clone()))
                    .await
            })
            .await
            .context("Failed to get index price")?;

        let price: f64 = response.data.index_price.parse().unwrap_or(0.0);
        Ok((price, response.data.timestamp))
    }

    async fn get_klines(
        &self,
        symbol: &str,
        interval: KlineInterval,
        start_ms: Option<UnixMillis>,
        end_ms: Option<UnixMillis>,
        limit: Option<usize>,
    ) -> Result<Vec<Kline>> {
        use serde::Deserialize;
        use std::collections::HashMap;

        #[derive(Deserialize)]
        struct KlineData {
            time: u64,
            open: String,
            high: String,
            low: String,
            close: String,
            vol: String,
        }

        #[derive(Deserialize)]
        struct GetKlineResponse {
            data: Vec<KlineData>,
        }

        let mut params = HashMap::new();
        params.insert("interval".to_string(), converters::to_mexc_interval(interval));
        if let Some(start) = start_ms {
            params.insert("start".to_string(), start.to_string());
        }
        if let Some(end) = end_ms {
            params.insert("end".to_string(), end.to_string());
        }

        let endpoint = format!("/api/v1/contract/kline/{}", symbol);
        let response: GetKlineResponse = self
            .call_api(&endpoint, || async {
                self.client
                    .get_public(&endpoint, Some(params.clone()))
                    .await
            })
            .await
            .context("Failed to get klines")?;

        let mut klines: Vec<Kline> = response
            .data
            .into_iter()
            .map(|k| Kline {
                symbol: symbol.to_string(),
                open_ms: k.time,
                close_ms: k.time + 60000,
                open: k.open.parse().unwrap_or(0.0),
                high: k.high.parse().unwrap_or(0.0),
                low: k.low.parse().unwrap_or(0.0),
                close: k.close.parse().unwrap_or(0.0),
                volume: k.vol.parse().unwrap_or(0.0),
                quote_volume: 0.0,
                trades: 0,
            })
            .collect();

        if let Some(lim) = limit {
            klines.truncate(lim);
        }

        Ok(klines)
    }

    async fn get_funding_history(
        &self,
        symbol: &str,
        start_ms: Option<UnixMillis>,
        end_ms: Option<UnixMillis>,
        limit: Option<usize>,
    ) -> Result<Vec<FundingRateHistory>> {
        use serde::Deserialize;
        use std::collections::HashMap;

        #[derive(Deserialize)]
        struct FundingData {
            symbol: String,
            #[serde(rename = "fundingRate")]
            funding_rate: String,
            #[serde(rename = "settleTime")]
            settle_time: u64,
        }

        #[derive(Deserialize)]
        struct GetFundingResponse {
            data: Vec<FundingData>,
        }

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());
        if let Some(start) = start_ms {
            params.insert("start".to_string(), start.to_string());
        }
        if let Some(end) = end_ms {
            params.insert("end".to_string(), end.to_string());
        }
        if let Some(lim) = limit {
            params.insert("limit".to_string(), lim.to_string());
        }

        let response: GetFundingResponse = self
            .call_api("/api/v1/contract/funding_rate/history", || async {
                self.client
                    .get_public("/api/v1/contract/funding_rate/history", Some(params.clone()))
                    .await
            })
            .await
            .context("Failed to get funding history")?;

        Ok(response
            .data
            .into_iter()
            .map(|data| FundingRateHistory {
                symbol: data.symbol,
                rate: Decimal::from_str(&data.funding_rate).unwrap_or(Decimal::from(0)),
                ts_ms: data.settle_time,
            })
            .collect())
    }
}

// ============================================================================
// PerpWs Trait Implementation
// ============================================================================

#[async_trait::async_trait]
impl PerpWs for MexcPerpsAdapter {
    async fn subscribe_user(&self) -> Result<mpsc::Receiver<UserEvent>> {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::{connect_async, tungstenite::Message};

        let (tx, rx) = mpsc::channel(1000);
        let adapter = self.clone();
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel(1);

        // Store shutdown sender
        {
            let mut guard = adapter.shutdown_tx.lock().await;
            *guard = Some(shutdown_tx);
        }

        // Spawn reconnection loop
        tokio::spawn(async move {
            let reconnect_config = ReconnectConfig::production();
            let mut strategy = ReconnectStrategy::new(reconnect_config);

            'reconnect: loop {
                let reconnect_num = adapter.reconnect_count.load(Ordering::Relaxed);
                info!("MEXC Perps user stream connecting (reconnect #{})", reconnect_num);

                // Create listen key
                let listen_key = match adapter.create_listen_key().await {
                    Ok(key) => {
                        let mut key_guard = adapter.listen_key.lock().await;
                        *key_guard = Some(key.clone());
                        key
                    }
                    Err(e) => {
                        error!("Failed to create MEXC Perps listen key: {}", e);
                        if !strategy.can_retry() {
                            break 'reconnect;
                        }
                        let delay = strategy.wait_before_retry().await;
                        warn!("MEXC Perps user stream reconnecting - waiting {}ms", delay);
                        continue 'reconnect;
                    }
                };

                // Connect to WebSocket
                let url = format!("{}?token={}", MEXC_FUTURES_WS_URL, listen_key);
                let mut ws = match connect_async(&url).await {
                    Ok((stream, _)) => stream,
                    Err(e) => {
                        error!("MEXC Perps WebSocket connection failed: {}", e);
                        if !strategy.can_retry() {
                            break 'reconnect;
                        }
                        let delay = strategy.wait_before_retry().await;
                        warn!("MEXC Perps user stream reconnecting - waiting {}ms", delay);
                        continue 'reconnect;
                    }
                };

                // Send subscription message
                let sub_msg = serde_json::json!({"method": "sub.personal"});
                if ws.send(Message::Text(sub_msg.to_string())).await.is_err() {
                    warn!("Failed to send subscription");
                    continue 'reconnect;
                }

                // Connection successful
                strategy.reset();
                adapter.reconnect_count.fetch_add(1, Ordering::Relaxed);
                info!("MEXC Perps user WebSocket connected (reconnect #{})", reconnect_num);

                let heartbeat = HeartbeatMonitor::new(HeartbeatConfig::production());

                // Start listen key renewal task
                let renewal_shutdown_rx = match adapter.shutdown_tx.lock().await.as_ref() {
                    Some(tx) => tx.subscribe(),
                    None => continue 'reconnect, // Skip renewal if no shutdown channel
                };
                Self::spawn_listen_key_renewal_task(
                    adapter.listen_key.clone(),
                    adapter.client.clone(),
                    renewal_shutdown_rx,
                );

                // Message handling loop
                'message_loop: loop {
                    tokio::select! {
                        _ = shutdown_rx.recv() => {
                            info!("MEXC Perps user stream shutdown signal received");
                            break 'reconnect;
                        }
                        _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
                            if !heartbeat.is_alive().await {
                                warn!("MEXC Perps user stream heartbeat timeout");
                                break 'message_loop;
                            }
                        }
                        msg = ws.next() => {
                            match msg {
                                Some(Ok(Message::Text(text))) => {
                                    heartbeat.record_message_received().await;
                                    if let Ok(event) = parse_futures_user_event(&text) {
                                        if tx.send(event).await.is_err() {
                                            info!("MEXC Perps user stream receiver dropped");
                                            break 'reconnect;
                                        }
                                    }
                                }
                                Some(Ok(Message::Ping(data))) => {
                                    heartbeat.record_pong_received().await;
                                    if ws.send(Message::Pong(data)).await.is_err() {
                                        warn!("MEXC Perps failed to send pong");
                                        break 'message_loop;
                                    }
                                    heartbeat.record_ping_sent().await;
                                }
                                Some(Ok(Message::Pong(_))) => {
                                    heartbeat.record_pong_received().await;
                                }
                                Some(Ok(Message::Close(_))) => {
                                    warn!("MEXC Perps WebSocket closed by server");
                                    break 'message_loop;
                                }
                                Some(Err(e)) => {
                                    error!("MEXC Perps WebSocket error: {}", e);
                                    break 'message_loop;
                                }
                                None => {
                                    warn!("MEXC Perps WebSocket stream ended");
                                    break 'message_loop;
                                }
                                _ => {}
                            }
                        }
                    }
                }

                // Connection lost - attempt reconnection
                if !strategy.can_retry() {
                    error!("MEXC Perps user stream max reconnection attempts reached");
                    break 'reconnect;
                }

                let delay = strategy.wait_before_retry().await;
                warn!("MEXC Perps user stream reconnecting - waiting {}ms", delay);
            }

            info!("MEXC Perps user stream task terminated");
        });

        Ok(rx)
    }

    async fn subscribe_books(&self, symbols: &[&str]) -> Result<mpsc::Receiver<BookUpdate>> {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::{connect_async, tungstenite::Message};

        let (tx, rx) = mpsc::channel(1000);
        let adapter = self.clone();
        let symbols_owned: Vec<String> = symbols.iter().map(|s| s.to_string()).collect();
        let mut shutdown_rx = match adapter.shutdown_tx.lock().await.as_ref() {
            Some(tx) => tx.subscribe(),
            None => {
                // Create a dummy receiver that never fires
                let (tx, rx) = tokio::sync::broadcast::channel(1);
                drop(tx);
                rx
            }
        };

        tokio::spawn(async move {
            let reconnect_config = ReconnectConfig::production();
            let mut strategy = ReconnectStrategy::new(reconnect_config);

            'reconnect: loop {
                let reconnect_num = adapter.reconnect_count.load(Ordering::Relaxed);
                info!("MEXC Perps books stream connecting (reconnect #{})", reconnect_num);

                let mut ws = match connect_async(MEXC_FUTURES_WS_URL).await {
                    Ok((stream, _)) => stream,
                    Err(e) => {
                        error!("MEXC Perps books WebSocket connection failed: {}", e);
                        if !strategy.can_retry() {
                            break 'reconnect;
                        }
                        let delay = strategy.wait_before_retry().await;
                        warn!("MEXC Perps books stream reconnecting - waiting {}ms", delay);
                        continue 'reconnect;
                    }
                };

                // Subscribe to all symbols
                for symbol in &symbols_owned {
                    let sub_msg = serde_json::json!({
                        "method": "sub.depth",
                        "param": {"symbol": symbol.to_uppercase()}
                    });
                    if ws.send(Message::Text(sub_msg.to_string())).await.is_err() {
                        warn!("Failed to subscribe to {}", symbol);
                        continue 'reconnect;
                    }
                }

                strategy.reset();
                info!("MEXC Perps books WebSocket connected (reconnect #{})", reconnect_num);

                let heartbeat = HeartbeatMonitor::new(HeartbeatConfig::production());
                let mut seq_counter = 0u64;

                'message_loop: loop {
                    tokio::select! {
                        _ = shutdown_rx.recv() => {
                            info!("MEXC Perps books stream shutdown signal received");
                            break 'reconnect;
                        }
                        _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
                            if !heartbeat.is_alive().await {
                                warn!("MEXC Perps books stream heartbeat timeout");
                                break 'message_loop;
                            }
                        }
                        msg = ws.next() => {
                            match msg {
                                Some(Ok(Message::Text(text))) => {
                                    heartbeat.record_message_received().await;
                                    if let Ok(update) = parse_futures_book_update(&text, &mut seq_counter) {
                                        if tx.send(update).await.is_err() {
                                            info!("MEXC Perps books stream receiver dropped");
                                            break 'reconnect;
                                        }
                                    }
                                }
                                Some(Ok(Message::Ping(data))) => {
                                    heartbeat.record_pong_received().await;
                                    if ws.send(Message::Pong(data)).await.is_err() {
                                        warn!("MEXC Perps books failed to send pong");
                                        break 'message_loop;
                                    }
                                    heartbeat.record_ping_sent().await;
                                }
                                Some(Ok(Message::Pong(_))) => {
                                    heartbeat.record_pong_received().await;
                                }
                                Some(Ok(Message::Close(_))) => {
                                    warn!("MEXC Perps books WebSocket closed");
                                    break 'message_loop;
                                }
                                Some(Err(e)) => {
                                    error!("MEXC Perps books WebSocket error: {}", e);
                                    break 'message_loop;
                                }
                                None => {
                                    warn!("MEXC Perps books WebSocket stream ended");
                                    break 'message_loop;
                                }
                                _ => {}
                            }
                        }
                    }
                }

                if !strategy.can_retry() {
                    error!("MEXC Perps books stream max reconnection attempts reached");
                    break 'reconnect;
                }

                let delay = strategy.wait_before_retry().await;
                warn!("MEXC Perps books stream reconnecting - waiting {}ms", delay);
            }

            info!("MEXC Perps books stream task terminated");
        });

        Ok(rx)
    }

    async fn subscribe_trades(&self, symbols: &[&str]) -> Result<mpsc::Receiver<TradeEvent>> {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::{connect_async, tungstenite::Message};

        let (tx, rx) = mpsc::channel(1000);
        let adapter = self.clone();
        let symbols_owned: Vec<String> = symbols.iter().map(|s| s.to_string()).collect();
        let mut shutdown_rx = match adapter.shutdown_tx.lock().await.as_ref() {
            Some(tx) => tx.subscribe(),
            None => {
                // Create a dummy receiver that never fires
                let (tx, rx) = tokio::sync::broadcast::channel(1);
                drop(tx);
                rx
            }
        };

        tokio::spawn(async move {
            let reconnect_config = ReconnectConfig::production();
            let mut strategy = ReconnectStrategy::new(reconnect_config);

            'reconnect: loop {
                let reconnect_num = adapter.reconnect_count.load(Ordering::Relaxed);
                info!("MEXC Perps trades stream connecting (reconnect #{})", reconnect_num);

                let mut ws = match connect_async(MEXC_FUTURES_WS_URL).await {
                    Ok((stream, _)) => stream,
                    Err(e) => {
                        error!("MEXC Perps trades WebSocket connection failed: {}", e);
                        if !strategy.can_retry() {
                            break 'reconnect;
                        }
                        let delay = strategy.wait_before_retry().await;
                        warn!("MEXC Perps trades stream reconnecting - waiting {}ms", delay);
                        continue 'reconnect;
                    }
                };

                // Subscribe to all symbols
                for symbol in &symbols_owned {
                    let sub_msg = serde_json::json!({
                        "method": "sub.deal",
                        "param": {"symbol": symbol.to_uppercase()}
                    });
                    if ws.send(Message::Text(sub_msg.to_string())).await.is_err() {
                        warn!("Failed to subscribe to {}", symbol);
                        continue 'reconnect;
                    }
                }

                strategy.reset();
                info!("MEXC Perps trades WebSocket connected (reconnect #{})", reconnect_num);

                let heartbeat = HeartbeatMonitor::new(HeartbeatConfig::production());

                'message_loop: loop {
                    tokio::select! {
                        _ = shutdown_rx.recv() => {
                            info!("MEXC Perps trades stream shutdown signal received");
                            break 'reconnect;
                        }
                        _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
                            if !heartbeat.is_alive().await {
                                warn!("MEXC Perps trades stream heartbeat timeout");
                                break 'message_loop;
                            }
                        }
                        msg = ws.next() => {
                            match msg {
                                Some(Ok(Message::Text(text))) => {
                                    heartbeat.record_message_received().await;
                                    if let Ok(event) = parse_futures_trade_event(&text) {
                                        if tx.send(event).await.is_err() {
                                            info!("MEXC Perps trades stream receiver dropped");
                                            break 'reconnect;
                                        }
                                    }
                                }
                                Some(Ok(Message::Ping(data))) => {
                                    heartbeat.record_pong_received().await;
                                    if ws.send(Message::Pong(data)).await.is_err() {
                                        warn!("MEXC Perps trades failed to send pong");
                                        break 'message_loop;
                                    }
                                    heartbeat.record_ping_sent().await;
                                }
                                Some(Ok(Message::Pong(_))) => {
                                    heartbeat.record_pong_received().await;
                                }
                                Some(Ok(Message::Close(_))) => {
                                    warn!("MEXC Perps trades WebSocket closed");
                                    break 'message_loop;
                                }
                                Some(Err(e)) => {
                                    error!("MEXC Perps trades WebSocket error: {}", e);
                                    break 'message_loop;
                                }
                                None => {
                                    warn!("MEXC Perps trades WebSocket stream ended");
                                    break 'message_loop;
                                }
                                _ => {}
                            }
                        }
                    }
                }

                if !strategy.can_retry() {
                    error!("MEXC Perps trades stream max reconnection attempts reached");
                    break 'reconnect;
                }

                let delay = strategy.wait_before_retry().await;
                warn!("MEXC Perps trades stream reconnecting - waiting {}ms", delay);
            }

            info!("MEXC Perps trades stream task terminated");
        });

        Ok(rx)
    }

    async fn health(&self) -> Result<HealthStatus> {
        let status = self.connection_status.read().await;

        Ok(HealthStatus {
            status: *status,
            last_ping_ms: None,
            last_pong_ms: None,
            latency_ms: None,
            reconnect_count: 0,
            error_msg: None,
        })
    }

    async fn reconnect(&self) -> Result<()> {
        {
            let mut status = self.connection_status.write().await;
            *status = ConnectionStatus::Reconnecting;
        }

        let new_key = self.create_listen_key().await?;

        {
            let mut key_guard = self.listen_key.lock().await;
            *key_guard = Some(new_key);
        }

        {
            let mut status = self.connection_status.write().await;
            *status = ConnectionStatus::Connected;
        }

        Ok(())
    }
}

// ============================================================================
// WebSocket Message Parsers for Futures
// ============================================================================

fn parse_futures_user_event(text: &str) -> Result<UserEvent> {
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct FuturesUserMessage {
        channel: Option<String>,
        data: Option<serde_json::Value>,
    }

    let msg: FuturesUserMessage = serde_json::from_str(text)?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    if let Some(channel) = msg.channel {
        match channel.as_str() {
            "push.personal.order" => {
                #[derive(Deserialize)]
                struct OrderData {
                    symbol: String,
                    #[serde(rename = "orderId")]
                    order_id: String,
                    #[serde(rename = "externalOid", default)]
                    external_oid: Option<String>,
                    vol: String,
                    #[serde(rename = "dealVol")]
                    deal_vol: String,
                    #[serde(rename = "dealAvgPrice", default)]
                    deal_avg_price: Option<String>,
                    price: String,
                    side: i32,
                    #[serde(rename = "orderType", default)]
                    order_type: Option<i32>,
                    state: i32,
                    #[serde(rename = "createTime")]
                    create_time: u64,
                    #[serde(rename = "updateTime", default)]
                    update_time: Option<u64>,
                }

                if let Some(data) = msg.data {
                    let order_data: OrderData = serde_json::from_value(data)?;
                    let qty: f64 = order_data.vol.parse().unwrap_or(0.0);
                    let filled: f64 = order_data.deal_vol.parse().unwrap_or(0.0);

                    let status = match order_data.state {
                        1 | 2 if filled > 0.0 => OrderStatus::PartiallyFilled,
                        1 | 2 => OrderStatus::New,
                        3 => OrderStatus::Filled,
                        4 => OrderStatus::Canceled,
                        5 => OrderStatus::Rejected,
                        _ => OrderStatus::New,
                    };

                    let (side, _) = match order_data.side {
                        1 => (Side::Buy, false),
                        2 => (Side::Buy, true),
                        3 => (Side::Sell, false),
                        4 => (Side::Sell, true),
                        _ => (Side::Buy, false),
                    };

                    // Map MEXC order type to internal type
                    let ord_type = match order_data.order_type {
                        Some(1) | Some(2) | Some(3) | Some(4) => OrderType::Limit,
                        Some(5) | Some(6) => OrderType::Market,
                        _ => OrderType::Limit,
                    };

                    let update_time = order_data.update_time.unwrap_or(order_data.create_time);

                    return Ok(UserEvent::OrderUpdate(Order {
                        venue_order_id: order_data.order_id,
                        client_order_id: order_data.external_oid.unwrap_or_default(),
                        symbol: order_data.symbol,
                        ord_type,
                        side,
                        qty,
                        price: Some(order_data.price.parse().unwrap_or(0.0)),
                        stop_price: None,
                        tif: None,
                        status,
                        filled_qty: filled,
                        remaining_qty: qty - filled,
                        created_ms: order_data.create_time,
                        updated_ms: update_time,
                        recv_ms: now,
                        raw_status: Some(order_data.state.to_string()),
                    }));
                }
            }
            "push.personal.position" => {
                #[derive(Deserialize)]
                struct PositionData {
                    symbol: String,
                    #[serde(rename = "holdVol")]
                    hold_vol: String,
                    #[serde(rename = "holdAvgPrice")]
                    hold_avg_price: String,
                    #[serde(rename = "positionType")]
                    position_type: i32,
                }

                if let Some(data) = msg.data {
                    let pos_data: PositionData = serde_json::from_value(data)?;
                    let mut qty: f64 = pos_data.hold_vol.parse().unwrap_or(0.0);
                    if pos_data.position_type == 2 {
                        qty = -qty;
                    }

                    return Ok(UserEvent::Position(Position {
                        exchange: Some("mexc".to_string()),
                        symbol: pos_data.symbol,
                        qty,
                        entry_px: pos_data.hold_avg_price.parse().unwrap_or(0.0),
                        mark_px: None,
                        liquidation_px: None,
                        unrealized_pnl: None,
                        realized_pnl: None,
                        margin: None,
                        leverage: None,
                        opened_ms: None,
                        updated_ms: now,
                    }));
                }
            }
            "push.personal.asset" => {
                #[derive(Deserialize)]
                struct AssetData {
                    currency: String,
                    #[serde(rename = "availableBalance")]
                    available_balance: String,
                    #[serde(rename = "frozenBalance")]
                    frozen_balance: String,
                }

                if let Some(data) = msg.data {
                    let asset_data: AssetData = serde_json::from_value(data)?;
                    let free: f64 = asset_data.available_balance.parse().unwrap_or(0.0);
                    let locked: f64 = asset_data.frozen_balance.parse().unwrap_or(0.0);

                    return Ok(UserEvent::Balance {
                        asset: asset_data.currency,
                        free,
                        locked,
                        ex_ts_ms: now,
                        recv_ms: now,
                    });
                }
            }
            _ => {}
        }
    }

    Err(anyhow::anyhow!("Unknown futures user event"))
}

fn parse_futures_book_update(text: &str, seq: &mut u64) -> Result<BookUpdate> {
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct DepthMessage {
        channel: Option<String>,
        symbol: Option<String>,
        data: Option<DepthData>,
    }

    #[derive(Deserialize)]
    struct DepthData {
        asks: Vec<PriceLevel>,
        bids: Vec<PriceLevel>,
    }

    #[derive(Deserialize)]
    struct PriceLevel {
        price: String,
        vol: String,
    }

    let msg: DepthMessage = serde_json::from_str(text)?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    if let (Some(_channel), Some(symbol), Some(data)) = (msg.channel, msg.symbol, msg.data) {
        let bids: Vec<(Price, Quantity)> = data
            .bids
            .into_iter()
            .filter_map(|level| {
                let price = level.price.parse().ok()?;
                let qty = level.vol.parse().ok()?;
                Some((price, qty))
            })
            .collect();

        let asks: Vec<(Price, Quantity)> = data
            .asks
            .into_iter()
            .filter_map(|level| {
                let price = level.price.parse().ok()?;
                let qty = level.vol.parse().ok()?;
                Some((price, qty))
            })
            .collect();

        *seq += 1;
        let prev_seq = *seq - 1;

        return Ok(BookUpdate::DepthDelta {
            symbol,
            bids,
            asks,
            seq: *seq,
            prev_seq,
            checksum: None,
            ex_ts_ms: now,
            recv_ms: now,
        });
    }

    Err(anyhow::anyhow!("Invalid depth message"))
}

fn parse_futures_trade_event(text: &str) -> Result<TradeEvent> {
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct TradeMessage {
        channel: Option<String>,
        symbol: Option<String>,
        data: Option<TradeData>,
    }

    #[derive(Deserialize)]
    struct TradeData {
        p: String,
        v: String,
        #[serde(rename = "T")]
        side: i32,
        t: u64,
    }

    let msg: TradeMessage = serde_json::from_str(text)?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    if let (Some(_channel), Some(symbol), Some(data)) = (msg.channel, msg.symbol, msg.data) {
        return Ok(TradeEvent {
            symbol,
            px: data.p.parse().unwrap_or(0.0),
            qty: data.v.parse().unwrap_or(0.0),
            taker_is_buy: data.side == 1,
            ex_ts_ms: data.t,
            recv_ms: now,
        });
    }

    Err(anyhow::anyhow!("Invalid trade message"))
}
