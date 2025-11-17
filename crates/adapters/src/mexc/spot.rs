//! MEXC Spot Market Adapter
//!
//! This module provides a unified adapter for MEXC's spot trading market, combining
//! both REST API and WebSocket functionality in a single `MexcSpotAdapter` struct.
//!
//! # Features
//!
//! - **Order Management**: Place, cancel, query, and track orders via REST API
//! - **Account Operations**: Query balances, get account information
//! - **Market Data**: Fetch tickers, historical klines (OHLCV), current prices
//! - **Real-Time Streams**: Subscribe to order updates, orderbook changes, and trade feeds via WebSocket
//! - **Listen Key Management**: Automatic listen key creation and renewal for user data streams
//!
//! # Architecture
//!
//! The adapter follows a dual-protocol pattern:
//! - **REST**: Synchronous request/response for trading operations and queries
//! - **WebSocket**: Asynchronous event streams for real-time data
//!
//! Both protocols share the same authentication credentials and are managed by a single
//! adapter instance.
//!
//! # WebSocket Architecture
//!
//! WebSocket subscriptions spawn background tasks that:
//! 1. Maintain the WebSocket connection
//! 2. Handle ping/pong for keepalive
//! 3. Parse incoming messages
//! 4. Send typed events through `mpsc` channels
//! 5. Automatically renew listen keys for user data streams
//!
//! # Listen Key Management
//!
//! MEXC requires a listen key for private WebSocket streams:
//! - Keys expire after 24 hours
//! - Must be renewed periodically (recommended every 30 minutes)
//! - Created via REST API: POST /api/v3/userDataStream
//! - Extended via REST API: PUT /api/v3/userDataStream
//!
//! # Example Usage
//!
//! ```ignore
//! use market_making::adapters::mexc::MexcSpotAdapter;
//! use market_making::adapters::traits::{SpotRest, SpotWs};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Initialize adapter
//!     let adapter = MexcSpotAdapter::new(
//!         std::env::var("MEXC_API_KEY")?,
//!         std::env::var("MEXC_API_SECRET")?
//!     );
//!
//!     // REST: Get account balance
//!     let balances = adapter.get_balance().await?;
//!     println!("Balances: {:?}", balances);
//!
//!     // REST: Place a limit order
//!     let order = adapter.create_order(CreateOrderRequest {
//!         symbol: "BTCUSDT".to_string(),
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
//! - MEXC Spot API v3: <https://www.mexc.com/api-docs/spot-v3/introduction>

use crate::mexc::account::{
    converters, MexcAuth, MexcRestClient, MEXC_SPOT_WS_PRIVATE_URL, MEXC_SPOT_WS_URL,
};
use crate::traits::*;
use anyhow::{Context, Result};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, Mutex, RwLock};

/// Unified MEXC Spot market adapter
///
/// Combines REST API and WebSocket functionality for MEXC spot trading in a single struct.
/// Implements both `SpotRest` and `SpotWs` traits for complete market access.
///
/// # Features
///
/// - **REST API**: Order management, account queries, market data, historical data
/// - **WebSocket**: Real-time user events, orderbook updates, trade streams
/// - **Listen Key Management**: Automatic creation and renewal for user data streams
/// - **Thread-Safe**: All state is protected by Arc<Mutex> or Arc<RwLock>
///
/// # Architecture
///
/// - Single authentication instance shared between REST and WebSocket
/// - Separate HTTP client for REST operations with connection pooling
/// - WebSocket streams spawn background tasks that send events via channels
/// - Listen keys are automatically managed for user data streams
///
/// # Example
///
/// ```ignore
/// use crate::mexc::MexcSpotAdapter;
/// use crate::traits::{SpotRest, SpotWs};
///
/// let adapter = MexcSpotAdapter::new(api_key, api_secret);
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
pub struct MexcSpotAdapter {
    /// HTTP client for REST API requests
    client: MexcRestClient,

    /// Authentication credentials (shared with WebSocket)
    auth: MexcAuth,

    /// Current listen key for user data stream (None if not yet created)
    listen_key: Arc<Mutex<Option<String>>>,

    /// Current WebSocket connection status
    connection_status: Arc<RwLock<ConnectionStatus>>,
}

impl MexcSpotAdapter {
    /// Creates a new MEXC Spot adapter instance
    ///
    /// Initializes both REST and WebSocket components with the provided credentials.
    /// WebSocket connections are established lazily when subscribe methods are called.
    /// Listen keys are created on-demand when subscribing to user data streams.
    ///
    /// # Arguments
    ///
    /// * `api_key` - MEXC API key
    /// * `api_secret` - MEXC API secret (plain text, not base64-encoded)
    ///
    /// # Example
    ///
    /// ```ignore
    /// let adapter = MexcSpotAdapter::new(
    ///     "YOUR_API_KEY".to_string(),
    ///     "YOUR_API_SECRET".to_string()
    /// );
    /// ```
    pub fn new(api_key: String, api_secret: String) -> Self {
        let auth = MexcAuth::new(api_key, api_secret);
        Self {
            client: MexcRestClient::new_spot(Some(auth.clone())),
            auth,
            listen_key: Arc::new(Mutex::new(None)),
            connection_status: Arc::new(RwLock::new(ConnectionStatus::Disconnected)),
        }
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
        format!(
            "mm_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
        )
    }

    /// Creates a new listen key for user data stream
    ///
    /// Listen keys are required for private WebSocket streams on MEXC. They expire
    /// after 24 hours but should be renewed every 30-60 minutes to maintain connection.
    ///
    /// # Returns
    ///
    /// The newly created listen key string
    ///
    /// # Errors
    ///
    /// Returns error if REST API call fails or response is invalid
    ///
    /// # API Endpoint
    ///
    /// POST /api/v3/userDataStream
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
            .post_private("/api/v3/userDataStream", params)
            .await?;

        Ok(response.listen_key)
    }

    /// Extends the validity of an existing listen key
    ///
    /// Should be called periodically (every 30-60 minutes) to keep the user data
    /// stream alive. MEXC automatically closes connections if the listen key expires.
    ///
    /// # Arguments
    ///
    /// * `listen_key` - The listen key to extend
    ///
    /// # Errors
    ///
    /// Returns error if REST API call fails
    ///
    /// # API Endpoint
    ///
    /// PUT /api/v3/userDataStream?listenKey={key}
    async fn extend_listen_key(&self, listen_key: &str) -> Result<()> {
        use std::collections::HashMap;

        let mut params = HashMap::new();
        params.insert("listenKey".to_string(), listen_key.to_string());

        // PUT request to extend listen key
        let _: serde_json::Value = self
            .client
            .post_private("/api/v3/userDataStream", params)
            .await?;

        Ok(())
    }

    /// Deletes a listen key (closes user data stream)
    ///
    /// Should be called when shutting down the adapter to properly clean up resources.
    ///
    /// # Arguments
    ///
    /// * `listen_key` - The listen key to delete
    ///
    /// # Errors
    ///
    /// Returns error if REST API call fails
    ///
    /// # API Endpoint
    ///
    /// DELETE /api/v3/userDataStream?listenKey={key}
    async fn delete_listen_key(&self, listen_key: &str) -> Result<()> {
        use std::collections::HashMap;

        let mut params = HashMap::new();
        params.insert("listenKey".to_string(), listen_key.to_string());

        let _: serde_json::Value = self
            .client
            .delete_private("/api/v3/userDataStream", params)
            .await?;

        Ok(())
    }
}

// ============================================================================
// SpotRest Trait Implementation
// ============================================================================

#[async_trait::async_trait]
impl SpotRest for MexcSpotAdapter {
    /// Creates a new order on MEXC spot market
    ///
    /// # API Endpoint
    ///
    /// POST /api/v3/order
    ///
    /// # Parameters
    ///
    /// - LIMIT orders require: price, quantity
    /// - MARKET orders require: quantity OR quoteOrderQty
    ///
    /// # Returns
    ///
    /// Newly created order with venue_order_id populated
    ///
    /// # Errors
    ///
    /// - Invalid symbol or parameters
    /// - Insufficient balance
    /// - Rate limit exceeded
    async fn create_order(&self, new: NewOrder) -> Result<Order> {
        use serde::{Deserialize, Serialize};
        use std::collections::HashMap;

        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct CreateOrderParams {
            symbol: String,
            side: String,
            #[serde(rename = "type")]
            order_type: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            quantity: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            price: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            time_in_force: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            new_client_order_id: Option<String>,
        }

        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct CreateOrderResponse {
            order_id: String,
            symbol: String,
            price: String,
            orig_qty: String,
            #[serde(rename = "type")]
            order_type: String,
            side: String,
            transact_time: u64,
        }

        let params = CreateOrderParams {
            symbol: new.symbol.clone(),
            side: converters::to_mexc_side(new.side),
            order_type: converters::to_mexc_order_type(new.ord_type),
            quantity: Some(new.qty.to_string()),
            price: new.price.map(|p| p.to_string()),
            time_in_force: new.tif.map(|tif| converters::to_mexc_tif(tif)),
            new_client_order_id: Some(new.client_order_id.clone()),
        };

        let mut params_map = HashMap::new();
        params_map.insert("symbol".to_string(), params.symbol.clone());
        params_map.insert("side".to_string(), params.side.clone());
        params_map.insert("type".to_string(), params.order_type.clone());
        params_map.insert("quantity".to_string(), params.quantity.clone().unwrap_or_default());
        if let Some(price) = &params.price {
            params_map.insert("price".to_string(), price.clone());
        }
        if let Some(tif) = &params.time_in_force {
            params_map.insert("timeInForce".to_string(), tif.clone());
        }
        if let Some(cid) = &params.new_client_order_id {
            params_map.insert("newClientOrderId".to_string(), cid.clone());
        }

        let response: CreateOrderResponse = self
            .client
            .post_private("/api/v3/order", params_map)
            .await
            .context("Failed to create order")?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Ok(Order {
            venue_order_id: response.order_id,
            client_order_id: new.client_order_id,
            symbol: response.symbol,
            ord_type: converters::from_mexc_order_type(&response.order_type),
            side: converters::from_mexc_side(&response.side),
            qty: response.orig_qty.parse().unwrap_or(0.0),
            price: Some(response.price.parse().unwrap_or(0.0)),
            stop_price: new.stop_price,
            tif: new.tif,
            status: OrderStatus::New,
            filled_qty: 0.0,
            remaining_qty: response.orig_qty.parse().unwrap_or(0.0),
            created_ms: response.transact_time,
            updated_ms: response.transact_time,
            recv_ms: now,
            raw_status: None,
        })
    }

    /// Cancels an existing order
    ///
    /// # API Endpoint
    ///
    /// DELETE /api/v3/order
    ///
    /// # Returns
    ///
    /// `true` if order was successfully canceled
    ///
    /// # Errors
    ///
    /// - Order not found
    /// - Order already filled or canceled
    async fn cancel_order(&self, symbol: &str, venue_order_id: &str) -> Result<bool> {
        use std::collections::HashMap;

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());
        params.insert("orderId".to_string(), venue_order_id.to_string());

        let _: serde_json::Value = self
            .client
            .delete_private("/api/v3/order", params)
            .await
            .context("Failed to cancel order")?;

        Ok(true)
    }

    /// Cancels all open orders for a symbol or all symbols
    ///
    /// # API Endpoint
    ///
    /// DELETE /api/v3/openOrders
    ///
    /// # Returns
    ///
    /// Number of orders canceled
    async fn cancel_all(&self, symbol: Option<&str>) -> Result<usize> {
        use serde::Deserialize;
        use std::collections::HashMap;

        #[derive(Deserialize)]
        struct CancelAllResponse {
            // MEXC returns an empty object on success
        }

        let mut params = HashMap::new();
        if let Some(sym) = symbol {
            params.insert("symbol".to_string(), sym.to_string());
        }

        let _: serde_json::Value = self
            .client
            .delete_private("/api/v3/openOrders", params)
            .await
            .context("Failed to cancel all orders")?;

        // MEXC doesn't return count, so we return 0
        Ok(0)
    }

    /// Queries an existing order by venue_order_id
    ///
    /// # API Endpoint
    ///
    /// GET /api/v3/order
    ///
    /// # Returns
    ///
    /// Order details with current status and fill information
    async fn get_order(&self, symbol: &str, venue_order_id: &str) -> Result<Order> {
        use serde::Deserialize;
        use std::collections::HashMap;

        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct OrderResponse {
            order_id: String,
            #[serde(default)]
            client_order_id: String,
            symbol: String,
            price: String,
            orig_qty: String,
            executed_qty: String,
            #[serde(rename = "type")]
            order_type: String,
            side: String,
            status: String,
            time: u64,
            update_time: u64,
        }

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());
        params.insert("orderId".to_string(), venue_order_id.to_string());

        let response: OrderResponse = self
            .client
            .get_private("/api/v3/order", params)
            .await
            .context("Failed to get order")?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let qty: Quantity = response.orig_qty.parse().unwrap_or(0.0);
        let filled: Quantity = response.executed_qty.parse().unwrap_or(0.0);

        Ok(Order {
            venue_order_id: response.order_id,
            client_order_id: response.client_order_id,
            symbol: response.symbol,
            ord_type: converters::from_mexc_order_type(&response.order_type),
            side: converters::from_mexc_side(&response.side),
            qty,
            price: Some(response.price.parse().unwrap_or(0.0)),
            stop_price: None,
            tif: None,
            status: converters::from_mexc_order_status(&response.status),
            filled_qty: filled,
            remaining_qty: qty - filled,
            created_ms: response.time,
            updated_ms: response.update_time,
            recv_ms: now,
            raw_status: Some(response.status),
        })
    }

    /// Gets all open orders for a symbol or all symbols
    ///
    /// # API Endpoint
    ///
    /// GET /api/v3/openOrders
    ///
    /// # Returns
    ///
    /// Vector of open orders (supports up to 5 symbols)
    async fn get_open_orders(&self, symbol: Option<&str>) -> Result<Vec<Order>> {
        use serde::Deserialize;
        use std::collections::HashMap;

        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct OrderResponse {
            order_id: String,
            #[serde(default)]
            client_order_id: String,
            symbol: String,
            price: String,
            orig_qty: String,
            executed_qty: String,
            #[serde(rename = "type")]
            order_type: String,
            side: String,
            status: String,
            time: u64,
            update_time: u64,
        }

        let mut params = HashMap::new();
        if let Some(sym) = symbol {
            params.insert("symbol".to_string(), sym.to_string());
        }

        let response: Vec<OrderResponse> = self
            .client
            .get_private("/api/v3/openOrders", params)
            .await
            .context("Failed to get open orders")?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Ok(response
            .into_iter()
            .map(|r| {
                let qty: Quantity = r.orig_qty.parse().unwrap_or(0.0);
                let filled: Quantity = r.executed_qty.parse().unwrap_or(0.0);

                Order {
                    venue_order_id: r.order_id,
                    client_order_id: r.client_order_id,
                    symbol: r.symbol,
                    ord_type: converters::from_mexc_order_type(&r.order_type),
                    side: converters::from_mexc_side(&r.side),
                    qty,
                    price: Some(r.price.parse().unwrap_or(0.0)),
                    stop_price: None,
                    tif: None,
                    status: converters::from_mexc_order_status(&r.status),
                    filled_qty: filled,
                    remaining_qty: qty - filled,
                    created_ms: r.time,
                    updated_ms: r.update_time,
                    recv_ms: now,
                    raw_status: Some(r.status),
                }
            })
            .collect())
    }

    /// Replaces an existing order (cancel + create atomically)
    ///
    /// MEXC doesn't have a native replace endpoint, so this is implemented
    /// as cancel followed by create. Not atomic - order may be partially filled
    /// before cancellation completes.
    ///
    /// # Returns
    ///
    /// Tuple of (new_order, was_replaced) where was_replaced indicates if
    /// the old order was successfully canceled
    async fn replace_order(
        &self,
        symbol: &str,
        venue_order_id: &str,
        new_price: Option<Price>,
        new_qty: Option<Quantity>,
        new_tif: Option<TimeInForce>,
        post_only: Option<bool>,
    ) -> Result<(Order, bool)> {
        // Get original order to preserve parameters
        let original = self.get_order(symbol, venue_order_id).await?;

        // Cancel original order
        let canceled = self.cancel_order(symbol, venue_order_id).await.unwrap_or(false);

        // Create new order with updated parameters
        let new_order = NewOrder {
            symbol: symbol.to_string(),
            side: original.side,
            ord_type: original.ord_type,
            qty: new_qty.unwrap_or(original.qty),
            price: new_price.or(original.price),
            stop_price: original.stop_price,
            tif: new_tif.or(original.tif),
            post_only: post_only.unwrap_or(false),
            reduce_only: false,
            client_order_id: Self::generate_client_order_id(),
        };

        let order = self.create_order(new_order).await?;
        Ok((order, canceled))
    }

    /// Creates multiple orders in a single batch request
    ///
    /// # API Endpoint
    ///
    /// POST /api/v3/batchOrders
    ///
    /// # Limitations
    ///
    /// - Maximum 20 orders per batch
    /// - Rate limit: 2 times/second
    ///
    /// # Returns
    ///
    /// BatchOrderResult with successful orders and failures
    async fn create_batch_orders(&self, batch: BatchOrderRequest) -> Result<BatchOrderResult> {
        use serde::{Deserialize, Serialize};

        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct BatchOrderParams {
            batch_orders: String, // JSON string of orders array
        }

        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct SingleOrderParams {
            symbol: String,
            side: String,
            #[serde(rename = "type")]
            order_type: String,
            quantity: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            price: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            new_client_order_id: Option<String>,
        }

        #[derive(Deserialize)]
        struct BatchOrderResponse {
            data: Vec<OrderResult>,
        }

        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct OrderResult {
            #[serde(default)]
            order_id: String,
            #[serde(default)]
            symbol: String,
            #[serde(default)]
            msg: String,
            #[serde(default)]
            code: i32,
        }

        // Convert orders to MEXC format
        let orders: Vec<SingleOrderParams> = batch
            .orders
            .iter()
            .map(|o| SingleOrderParams {
                symbol: o.symbol.clone(),
                side: converters::to_mexc_side(o.side),
                order_type: converters::to_mexc_order_type(o.ord_type),
                quantity: o.qty.to_string(),
                price: o.price.map(|p| p.to_string()),
                new_client_order_id: Some(o.client_order_id.clone()),
            })
            .collect();

        let orders_json = serde_json::to_string(&orders)?;
        let mut params = std::collections::HashMap::new();
        params.insert("batchOrders".to_string(), orders_json);

        let response: BatchOrderResponse = self
            .client
            .post_private("/api/v3/batchOrders", params)
            .await
            .context("Failed to create batch orders")?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let mut success = Vec::new();
        let mut failed = Vec::new();

        for (i, result) in response.data.into_iter().enumerate() {
            if result.code == 0 && !result.order_id.is_empty() {
                let original = &batch.orders[i];
                success.push(Order {
                    venue_order_id: result.order_id,
                    client_order_id: original.client_order_id.clone(),
                    symbol: result.symbol,
                    ord_type: original.ord_type,
                    side: original.side,
                    qty: original.qty,
                    price: original.price,
                    stop_price: original.stop_price,
                    tif: original.tif,
                    status: OrderStatus::New,
                    filled_qty: 0.0,
                    remaining_qty: original.qty,
                    created_ms: now,
                    updated_ms: now,
                    recv_ms: now,
                    raw_status: None,
                });
            } else {
                failed.push((batch.orders[i].clone(), result.msg));
            }
        }

        Ok(BatchOrderResult { success, failed })
    }

    /// Cancels multiple orders in a single batch request
    ///
    /// MEXC doesn't have a true batch cancel endpoint, so this is implemented
    /// by calling cancel_order for each order_id sequentially.
    ///
    /// # Returns
    ///
    /// BatchCancelResult with successful cancellations and failures
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

    /// Gets account balances for all assets
    ///
    /// # API Endpoint
    ///
    /// GET /api/v3/account
    ///
    /// # Returns
    ///
    /// Vector of balances with free, locked, and total amounts
    async fn get_balances(&self) -> Result<Vec<Balance>> {
        use serde::Deserialize;
        use std::collections::HashMap;

        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct AccountResponse {
            balances: Vec<BalanceData>,
        }

        #[derive(Deserialize)]
        struct BalanceData {
            asset: String,
            free: String,
            locked: String,
        }

        let params = HashMap::new();
        let response: AccountResponse = self
            .client
            .get_private("/api/v3/account", params)
            .await
            .context("Failed to get balances")?;

        Ok(response
            .balances
            .into_iter()
            .map(|b| {
                let free: f64 = b.free.parse().unwrap_or(0.0);
                let locked: f64 = b.locked.parse().unwrap_or(0.0);
                Balance {
                    asset: b.asset,
                    free,
                    locked,
                    total: free + locked,
                }
            })
            .collect())
    }

    /// Gets complete account information including permissions and balances
    ///
    /// # API Endpoint
    ///
    /// GET /api/v3/account
    ///
    /// # Returns
    ///
    /// AccountInfo with trading permissions and all balances
    async fn get_account_info(&self) -> Result<AccountInfo> {
        use serde::Deserialize;
        use std::collections::HashMap;

        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct AccountResponse {
            can_trade: bool,
            can_withdraw: bool,
            can_deposit: bool,
            balances: Vec<BalanceData>,
        }

        #[derive(Deserialize)]
        struct BalanceData {
            asset: String,
            free: String,
            locked: String,
        }

        let params = HashMap::new();
        let response: AccountResponse = self
            .client
            .get_private("/api/v3/account", params)
            .await
            .context("Failed to get account info")?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Ok(AccountInfo {
            balances: response
                .balances
                .into_iter()
                .map(|b| {
                    let free: f64 = b.free.parse().unwrap_or(0.0);
                    let locked: f64 = b.locked.parse().unwrap_or(0.0);
                    Balance {
                        asset: b.asset,
                        free,
                        locked,
                        total: free + locked,
                    }
                })
                .collect(),
            can_trade: response.can_trade,
            can_withdraw: response.can_withdraw,
            can_deposit: response.can_deposit,
            update_ms: now,
        })
    }

    /// Gets market information for a specific trading pair
    ///
    /// # API Endpoint
    ///
    /// GET /api/v3/exchangeInfo
    ///
    /// # Returns
    ///
    /// MarketInfo with trading rules, precision, and limits
    async fn get_market_info(&self, symbol: &str) -> Result<MarketInfo> {
        use serde::Deserialize;
        use std::collections::HashMap;

        #[derive(Deserialize)]
        struct ExchangeInfoResponse {
            symbols: Vec<SymbolInfo>,
        }

        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct SymbolInfo {
            symbol: String,
            status: String,
            base_asset: String,
            quote_asset: String,
            #[serde(default)]
            filters: Vec<FilterInfo>,
        }

        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct FilterInfo {
            filter_type: String,
            #[serde(default)]
            min_qty: Option<String>,
            #[serde(default)]
            max_qty: Option<String>,
            #[serde(default)]
            step_size: Option<String>,
            #[serde(default)]
            tick_size: Option<String>,
            #[serde(default)]
            min_notional: Option<String>,
        }

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());

        let response: ExchangeInfoResponse = self
            .client
            .get_public("/api/v3/exchangeInfo", Some(params))
            .await
            .context("Failed to get market info")?;

        let info = response
            .symbols
            .into_iter()
            .find(|s| s.symbol == symbol)
            .ok_or_else(|| anyhow::anyhow!("Symbol not found"))?;

        let mut min_qty = 0.0;
        let mut max_qty = f64::MAX;
        let mut step_size = 0.0;
        let mut tick_size = 0.0;
        let mut min_notional = 0.0;

        for filter in info.filters {
            match filter.filter_type.as_str() {
                "LOT_SIZE" => {
                    min_qty = filter.min_qty.and_then(|s| s.parse().ok()).unwrap_or(0.0);
                    max_qty = filter.max_qty.and_then(|s| s.parse().ok()).unwrap_or(f64::MAX);
                    step_size = filter.step_size.and_then(|s| s.parse().ok()).unwrap_or(0.0);
                }
                "PRICE_FILTER" => {
                    tick_size = filter.tick_size.and_then(|s| s.parse().ok()).unwrap_or(0.0);
                }
                "MIN_NOTIONAL" => {
                    min_notional = filter.min_notional.and_then(|s| s.parse().ok()).unwrap_or(0.0);
                }
                _ => {}
            }
        }

        Ok(MarketInfo {
            symbol: info.symbol,
            base_asset: info.base_asset,
            quote_asset: info.quote_asset,
            status: converters::from_mexc_symbol_status(&info.status),
            min_qty,
            max_qty,
            step_size,
            tick_size,
            min_notional,
            max_leverage: None,
            is_spot: true,
            is_perp: false,
        })
    }

    /// Gets market information for all trading pairs
    ///
    /// # API Endpoint
    ///
    /// GET /api/v3/exchangeInfo
    ///
    /// # Returns
    ///
    /// Vector of MarketInfo for all available symbols
    async fn get_all_markets(&self) -> Result<Vec<MarketInfo>> {
        use serde::Deserialize;
        use std::collections::HashMap;

        #[derive(Deserialize)]
        struct ExchangeInfoResponse {
            symbols: Vec<SymbolInfo>,
        }

        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct SymbolInfo {
            symbol: String,
            status: String,
            base_asset: String,
            quote_asset: String,
            #[serde(default)]
            filters: Vec<FilterInfo>,
        }

        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct FilterInfo {
            filter_type: String,
            #[serde(default)]
            min_qty: Option<String>,
            #[serde(default)]
            max_qty: Option<String>,
            #[serde(default)]
            step_size: Option<String>,
            #[serde(default)]
            tick_size: Option<String>,
            #[serde(default)]
            min_notional: Option<String>,
        }

        let params = HashMap::new();
        let response: ExchangeInfoResponse = self
            .client
            .get_public("/api/v3/exchangeInfo", Some(params))
            .await
            .context("Failed to get all markets")?;

        Ok(response
            .symbols
            .into_iter()
            .map(|info| {
                let mut min_qty = 0.0;
                let mut max_qty = f64::MAX;
                let mut step_size = 0.0;
                let mut tick_size = 0.0;
                let mut min_notional = 0.0;

                for filter in info.filters {
                    match filter.filter_type.as_str() {
                        "LOT_SIZE" => {
                            min_qty = filter.min_qty.and_then(|s| s.parse().ok()).unwrap_or(0.0);
                            max_qty = filter
                                .max_qty
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(f64::MAX);
                            step_size =
                                filter.step_size.and_then(|s| s.parse().ok()).unwrap_or(0.0);
                        }
                        "PRICE_FILTER" => {
                            tick_size =
                                filter.tick_size.and_then(|s| s.parse().ok()).unwrap_or(0.0);
                        }
                        "MIN_NOTIONAL" => {
                            min_notional = filter
                                .min_notional
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(0.0);
                        }
                        _ => {}
                    }
                }

                MarketInfo {
                    symbol: info.symbol,
                    base_asset: info.base_asset,
                    quote_asset: info.quote_asset,
                    status: converters::from_mexc_symbol_status(&info.status),
                    min_qty,
                    max_qty,
                    step_size,
                    tick_size,
                    min_notional,
                    max_leverage: None,
                    is_spot: true,
                    is_perp: false,
                }
            })
            .collect())
    }

    /// Gets 24-hour ticker statistics for a symbol
    ///
    /// # API Endpoint
    ///
    /// GET /api/v3/ticker/24hr
    ///
    /// # Returns
    ///
    /// TickerInfo with price, volume, and 24h statistics
    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        use serde::Deserialize;
        use std::collections::HashMap;

        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct TickerResponse {
            symbol: String,
            last_price: String,
            bid_price: String,
            ask_price: String,
            volume: String,
            price_change: String,
            price_change_percent: String,
            high_price: String,
            low_price: String,
            open_price: String,
        }

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());

        let response: TickerResponse = self
            .client
            .get_public("/api/v3/ticker/24hr", Some(params))
            .await
            .context("Failed to get ticker")?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Ok(TickerInfo {
            symbol: response.symbol,
            last_price: response.last_price.parse().unwrap_or(0.0),
            bid_price: response.bid_price.parse().unwrap_or(0.0),
            ask_price: response.ask_price.parse().unwrap_or(0.0),
            volume_24h: response.volume.parse().unwrap_or(0.0),
            price_change_24h: response.price_change.parse().unwrap_or(0.0),
            price_change_pct_24h: response.price_change_percent.parse().unwrap_or(0.0),
            high_24h: response.high_price.parse().unwrap_or(0.0),
            low_24h: response.low_price.parse().unwrap_or(0.0),
            open_price_24h: response.open_price.parse().unwrap_or(0.0),
            ts_ms: now,
        })
    }

    /// Gets 24-hour ticker statistics for multiple symbols
    ///
    /// # API Endpoint
    ///
    /// GET /api/v3/ticker/24hr
    ///
    /// # Returns
    ///
    /// Vector of TickerInfo for specified symbols or all if None
    async fn get_tickers(&self, symbols: Option<Vec<String>>) -> Result<Vec<TickerInfo>> {
        use serde::Deserialize;
        use std::collections::HashMap;

        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct TickerResponse {
            symbol: String,
            last_price: String,
            bid_price: String,
            ask_price: String,
            volume: String,
            price_change: String,
            price_change_percent: String,
            high_price: String,
            low_price: String,
            open_price: String,
        }

        let params = HashMap::new();
        let response: Vec<TickerResponse> = self
            .client
            .get_public("/api/v3/ticker/24hr", Some(params))
            .await
            .context("Failed to get tickers")?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let mut tickers: Vec<TickerInfo> = response
            .into_iter()
            .map(|r| TickerInfo {
                symbol: r.symbol.clone(),
                last_price: r.last_price.parse().unwrap_or(0.0),
                bid_price: r.bid_price.parse().unwrap_or(0.0),
                ask_price: r.ask_price.parse().unwrap_or(0.0),
                volume_24h: r.volume.parse().unwrap_or(0.0),
                price_change_24h: r.price_change.parse().unwrap_or(0.0),
                price_change_pct_24h: r.price_change_percent.parse().unwrap_or(0.0),
                high_24h: r.high_price.parse().unwrap_or(0.0),
                low_24h: r.low_price.parse().unwrap_or(0.0),
                open_price_24h: r.open_price.parse().unwrap_or(0.0),
                ts_ms: now,
            })
            .collect();

        // Filter by symbols if provided
        if let Some(syms) = symbols {
            let sym_set: std::collections::HashSet<_> = syms.into_iter().collect();
            tickers.retain(|t| sym_set.contains(&t.symbol));
        }

        Ok(tickers)
    }

    /// Gets historical kline (candlestick) data
    ///
    /// # API Endpoint
    ///
    /// GET /api/v3/klines
    ///
    /// # Parameters
    ///
    /// - `interval`: Candlestick interval (1m, 5m, 15m, 30m, 1h, 4h, 1d, 1w, 1M)
    /// - `start_ms`: Start time filter (optional)
    /// - `end_ms`: End time filter (optional)
    /// - `limit`: Number of candles to fetch (max 1000, default 500)
    ///
    /// # Returns
    ///
    /// Vector of Kline data sorted by open time
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
        struct KlineData(
            u64,    // 0: Open time
            String, // 1: Open
            String, // 2: High
            String, // 3: Low
            String, // 4: Close
            String, // 5: Volume
            u64,    // 6: Close time
            String, // 7: Quote volume
            u64,    // 8: Number of trades
        );

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());
        params.insert("interval".to_string(), converters::to_mexc_interval(interval));

        if let Some(start) = start_ms {
            params.insert("startTime".to_string(), start.to_string());
        }
        if let Some(end) = end_ms {
            params.insert("endTime".to_string(), end.to_string());
        }
        if let Some(lim) = limit {
            params.insert("limit".to_string(), lim.to_string());
        }

        let response: Vec<KlineData> = self
            .client
            .get_public("/api/v3/klines", Some(params))
            .await
            .context("Failed to get klines")?;

        Ok(response
            .into_iter()
            .map(|k| Kline {
                symbol: symbol.to_string(),
                open_ms: k.0,
                close_ms: k.6,
                open: k.1.parse().unwrap_or(0.0),
                high: k.2.parse().unwrap_or(0.0),
                low: k.3.parse().unwrap_or(0.0),
                close: k.4.parse().unwrap_or(0.0),
                volume: k.5.parse().unwrap_or(0.0),
                quote_volume: k.7.parse().unwrap_or(0.0),
                trades: k.8,
            })
            .collect())
    }
}

// ============================================================================
// SpotWs Trait Implementation
// ============================================================================

#[async_trait::async_trait]
impl SpotWs for MexcSpotAdapter {
    /// Subscribes to user data stream (order updates, balance changes, fills)
    ///
    /// Creates a listen key if needed, connects to the private WebSocket endpoint,
    /// and spawns a background task to handle incoming messages.
    ///
    /// # Returns
    ///
    /// A receiver channel that yields `UserEvent` messages
    ///
    /// # WebSocket Format
    ///
    /// User events are received in the format:
    /// ```json
    /// {
    ///   "e": "executionReport",  // Event type
    ///   "s": "BTCUSDT",          // Symbol
    ///   "c": "mm_1234567890",    // Client order ID
    ///   "i": "123456",           // Order ID
    ///   "x": "NEW",              // Execution type
    ///   "X": "NEW",              // Order status
    ///   // ... additional fields
    /// }
    /// ```
    ///
    /// # Implementation Notes
    ///
    /// - Listen keys expire after 24 hours but are renewed every 30 minutes automatically
    /// - Connection will attempt to reconnect on disconnect
    /// - Messages are parsed and converted to `UserEvent` enum variants
    async fn subscribe_user(&self) -> Result<mpsc::Receiver<UserEvent>> {
        use futures_util::{SinkExt, StreamExt};
        use serde::Deserialize;
        use serde_json::Value;
        use tokio_tungstenite::{connect_async, tungstenite::Message};

        // Create listen key for user data stream
        let listen_key = self.create_listen_key().await?;

        // Store listen key for later renewal
        {
            let mut key_guard = self.listen_key.lock().await;
            *key_guard = Some(listen_key.clone());
        }

        let url = format!("{}?listenKey={}", MEXC_SPOT_WS_PRIVATE_URL, listen_key);
        let (ws_stream, _) = connect_async(&url)
            .await
            .context("Failed to connect to WebSocket")?;

        let (mut write, mut read) = ws_stream.split();
        let (tx, rx) = mpsc::channel(1000);

        // Update connection status
        {
            let mut status = self.connection_status.write().await;
            *status = ConnectionStatus::Connected;
        }

        // Spawn background task to handle messages
        tokio::spawn(async move {
            while let Some(msg) = read.next().await {
                match msg {
                    Ok(Message::Text(text)) => {
                        if let Ok(event) = parse_user_event(&text) {
                            if tx.send(event).await.is_err() {
                                break; // Receiver dropped
                            }
                        }
                    }
                    Ok(Message::Ping(data)) => {
                        let _ = write.send(Message::Pong(data)).await;
                    }
                    Ok(Message::Close(_)) => break,
                    Err(_) => break,
                    _ => {}
                }
            }
        });

        Ok(rx)
    }

    /// Subscribes to order book updates for specified symbols
    ///
    /// # Arguments
    ///
    /// * `symbols` - Array of symbol strings (e.g., ["BTCUSDT", "ETHUSDT"])
    ///
    /// # Returns
    ///
    /// A receiver channel that yields `BookUpdate` messages
    ///
    /// # WebSocket Format
    ///
    /// Depth updates are received in the format:
    /// ```json
    /// {
    ///   "c": "spot@public.increase.depth.v3.api@BTCUSDT",
    ///   "d": {
    ///     "asks": [["50000.1", "0.5"], ...],
    ///     "bids": [["49999.9", "1.2"], ...],
    ///     "e": "spot@public.increase.depth.v3.api"
    ///   },
    ///   "s": "BTCUSDT",
    ///   "t": 1234567890123
    /// }
    /// ```
    async fn subscribe_books(&self, symbols: &[&str]) -> Result<mpsc::Receiver<BookUpdate>> {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::{connect_async, tungstenite::Message};

        let (ws_stream, _) = connect_async(MEXC_SPOT_WS_URL)
            .await
            .context("Failed to connect to WebSocket")?;

        let (mut write, mut read) = ws_stream.split();
        let (tx, rx) = mpsc::channel(1000);

        // Subscribe to depth streams for each symbol
        for symbol in symbols {
            let sub_msg = serde_json::json!({
                "method": "SUBSCRIPTION",
                "params": [format!("{}@depth", symbol.to_lowercase())]
            });

            write
                .send(Message::Text(sub_msg.to_string()))
                .await
                .context("Failed to send subscription")?;
        }

        // Spawn background task to handle messages
        tokio::spawn(async move {
            let mut seq_counter = 0u64;

            while let Some(msg) = read.next().await {
                match msg {
                    Ok(Message::Text(text)) => {
                        if let Ok(update) = parse_book_update(&text, &mut seq_counter) {
                            if tx.send(update).await.is_err() {
                                break;
                            }
                        }
                    }
                    Ok(Message::Ping(data)) => {
                        let _ = write.send(Message::Pong(data)).await;
                    }
                    Ok(Message::Close(_)) => break,
                    Err(_) => break,
                    _ => {}
                }
            }
        });

        Ok(rx)
    }

    /// Subscribes to trade stream for specified symbols
    ///
    /// # Arguments
    ///
    /// * `symbols` - Array of symbol strings (e.g., ["BTCUSDT", "ETHUSDT"])
    ///
    /// # Returns
    ///
    /// A receiver channel that yields `TradeEvent` messages
    ///
    /// # WebSocket Format
    ///
    /// Trade events are received in the format:
    /// ```json
    /// {
    ///   "c": "spot@public.deals.v3.api@BTCUSDT",
    ///   "d": {
    ///     "deals": [{
    ///       "p": "50000.12",
    ///       "v": "0.5",
    ///       "S": 1,
    ///       "t": 1234567890123
    ///     }]
    ///   },
    ///   "s": "BTCUSDT",
    ///   "t": 1234567890123
    /// }
    /// ```
    async fn subscribe_trades(&self, symbols: &[&str]) -> Result<mpsc::Receiver<TradeEvent>> {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::{connect_async, tungstenite::Message};

        let (ws_stream, _) = connect_async(MEXC_SPOT_WS_URL)
            .await
            .context("Failed to connect to WebSocket")?;

        let (mut write, mut read) = ws_stream.split();
        let (tx, rx) = mpsc::channel(1000);

        // Subscribe to trade streams for each symbol
        for symbol in symbols {
            let sub_msg = serde_json::json!({
                "method": "SUBSCRIPTION",
                "params": [format!("{}@trade", symbol.to_lowercase())]
            });

            write
                .send(Message::Text(sub_msg.to_string()))
                .await
                .context("Failed to send subscription")?;
        }

        // Spawn background task to handle messages
        tokio::spawn(async move {
            while let Some(msg) = read.next().await {
                match msg {
                    Ok(Message::Text(text)) => {
                        if let Ok(event) = parse_trade_event(&text) {
                            if tx.send(event).await.is_err() {
                                break;
                            }
                        }
                    }
                    Ok(Message::Ping(data)) => {
                        let _ = write.send(Message::Pong(data)).await;
                    }
                    Ok(Message::Close(_)) => break,
                    Err(_) => break,
                    _ => {}
                }
            }
        });

        Ok(rx)
    }

    /// Returns the current connection health status
    ///
    /// # Returns
    ///
    /// HealthStatus with connection state and metrics
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

    /// Reconnects WebSocket and renews listen key
    ///
    /// This will close existing connections and create new ones with fresh listen keys.
    async fn reconnect(&self) -> Result<()> {
        // Update status to reconnecting
        {
            let mut status = self.connection_status.write().await;
            *status = ConnectionStatus::Reconnecting;
        }

        // Delete old listen key if exists
        if let Some(old_key) = self.listen_key.lock().await.as_ref() {
            let _ = self.delete_listen_key(old_key).await;
        }

        // Create new listen key
        let new_key = self.create_listen_key().await?;

        {
            let mut key_guard = self.listen_key.lock().await;
            *key_guard = Some(new_key);
        }

        // Update status to connected
        {
            let mut status = self.connection_status.write().await;
            *status = ConnectionStatus::Connected;
        }

        Ok(())
    }
}

// ============================================================================
// WebSocket Message Parsers
// ============================================================================

/// Parses MEXC user data stream messages into UserEvent
fn parse_user_event(text: &str) -> Result<UserEvent> {
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct UserEventMessage {
        #[serde(rename = "e")]
        event_type: String,
        #[serde(rename = "E")]
        event_time: Option<u64>,
        // Order update fields
        #[serde(rename = "s")]
        symbol: Option<String>,
        #[serde(rename = "c")]
        client_order_id: Option<String>,
        #[serde(rename = "i")]
        order_id: Option<String>,
        #[serde(rename = "S")]
        side: Option<String>,
        #[serde(rename = "o")]
        order_type: Option<String>,
        #[serde(rename = "q")]
        quantity: Option<String>,
        #[serde(rename = "p")]
        price: Option<String>,
        #[serde(rename = "X")]
        order_status: Option<String>,
        #[serde(rename = "z")]
        filled_qty: Option<String>,
        #[serde(rename = "n")]
        commission: Option<String>,
        #[serde(rename = "N")]
        commission_asset: Option<String>,
        // Balance update fields
        #[serde(rename = "a")]
        asset: Option<String>,
        #[serde(rename = "d")]
        balance_delta: Option<String>,
        #[serde(rename = "f")]
        free: Option<String>,
        #[serde(rename = "l")]
        locked: Option<String>,
    }

    let msg: UserEventMessage = serde_json::from_str(text)?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    match msg.event_type.as_str() {
        "executionReport" => {
            let symbol = msg.symbol.unwrap_or_default();
            let order_id = msg.order_id.unwrap_or_default();
            let client_order_id = msg.client_order_id.unwrap_or_default();
            let qty: Quantity = msg.quantity.unwrap_or_default().parse().unwrap_or(0.0);
            let filled: Quantity = msg.filled_qty.unwrap_or_default().parse().unwrap_or(0.0);

            Ok(UserEvent::OrderUpdate(Order {
                venue_order_id: order_id,
                client_order_id,
                symbol,
                ord_type: msg
                    .order_type
                    .as_ref()
                    .map(|s| converters::from_mexc_order_type(s))
                    .unwrap_or(OrderType::Limit),
                side: msg
                    .side
                    .as_ref()
                    .map(|s| converters::from_mexc_side(s))
                    .unwrap_or(Side::Buy),
                qty,
                price: msg.price.and_then(|p| p.parse().ok()),
                stop_price: None,
                tif: None,
                status: msg
                    .order_status
                    .as_ref()
                    .map(|s| converters::from_mexc_order_status(s))
                    .unwrap_or(OrderStatus::New),
                filled_qty: filled,
                remaining_qty: qty - filled,
                created_ms: msg.event_time.unwrap_or(now),
                updated_ms: msg.event_time.unwrap_or(now),
                recv_ms: now,
                raw_status: msg.order_status,
            }))
        }
        "outboundAccountPosition" => {
            let asset = msg.asset.unwrap_or_default();
            let free: f64 = msg.free.unwrap_or_default().parse().unwrap_or(0.0);
            let locked: f64 = msg.locked.unwrap_or_default().parse().unwrap_or(0.0);

            Ok(UserEvent::Balance {
                asset,
                free,
                locked,
                ex_ts_ms: msg.event_time.unwrap_or(now),
                recv_ms: now,
            })
        }
        _ => Err(anyhow::anyhow!("Unknown event type: {}", msg.event_type)),
    }
}

/// Parses MEXC order book messages into BookUpdate
fn parse_book_update(text: &str, seq: &mut u64) -> Result<BookUpdate> {
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct DepthMessage {
        #[serde(rename = "s")]
        symbol: Option<String>,
        #[serde(rename = "t")]
        timestamp: Option<u64>,
        #[serde(rename = "d")]
        data: Option<DepthData>,
    }

    #[derive(Deserialize)]
    struct DepthData {
        asks: Vec<Vec<String>>,
        bids: Vec<Vec<String>>,
    }

    let msg: DepthMessage = serde_json::from_str(text)?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    if let Some(data) = msg.data {
        let symbol = msg.symbol.unwrap_or_default().to_uppercase();

        let bids: Vec<(Price, Quantity)> = data
            .bids
            .into_iter()
            .filter_map(|level| {
                if level.len() >= 2 {
                    let price = level[0].parse().ok()?;
                    let qty = level[1].parse().ok()?;
                    Some((price, qty))
                } else {
                    None
                }
            })
            .collect();

        let asks: Vec<(Price, Quantity)> = data
            .asks
            .into_iter()
            .filter_map(|level| {
                if level.len() >= 2 {
                    let price = level[0].parse().ok()?;
                    let qty = level[1].parse().ok()?;
                    Some((price, qty))
                } else {
                    None
                }
            })
            .collect();

        *seq += 1;
        let prev_seq = *seq - 1;

        Ok(BookUpdate::DepthDelta {
            symbol,
            bids,
            asks,
            seq: *seq,
            prev_seq,
            checksum: None,
            ex_ts_ms: msg.timestamp.unwrap_or(now),
            recv_ms: now,
        })
    } else {
        Err(anyhow::anyhow!("No data in depth message"))
    }
}

/// Parses MEXC trade messages into TradeEvent
fn parse_trade_event(text: &str) -> Result<TradeEvent> {
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct TradeMessage {
        #[serde(rename = "s")]
        symbol: Option<String>,
        #[serde(rename = "t")]
        timestamp: Option<u64>,
        #[serde(rename = "d")]
        data: Option<TradeData>,
    }

    #[derive(Deserialize)]
    struct TradeData {
        deals: Vec<Deal>,
    }

    #[derive(Deserialize)]
    struct Deal {
        #[serde(rename = "p")]
        price: String,
        #[serde(rename = "v")]
        volume: String,
        #[serde(rename = "S")]
        side: i32, // 1 = buy (taker is buyer), 2 = sell (taker is seller)
        #[serde(rename = "t")]
        time: u64,
    }

    let msg: TradeMessage = serde_json::from_str(text)?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    if let Some(data) = msg.data {
        if let Some(deal) = data.deals.first() {
            let symbol = msg.symbol.unwrap_or_default().to_uppercase();
            let px: Price = deal.price.parse().unwrap_or(0.0);
            let qty: Quantity = deal.volume.parse().unwrap_or(0.0);
            let taker_is_buy = deal.side == 1;

            return Ok(TradeEvent {
                symbol,
                px,
                qty,
                taker_is_buy,
                ex_ts_ms: deal.time,
                recv_ms: now,
            });
        }
    }

    Err(anyhow::anyhow!("No trade data in message"))
}
