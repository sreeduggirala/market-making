//! Binance.US Spot Market Adapter
//!
//! This module provides a unified adapter for Binance.US spot trading, combining
//! both REST API and WebSocket functionality in a single `BinanceUsSpotAdapter` struct.
//!
//! # Features
//!
//! - **Order Management**: Place, cancel, query, and track orders via REST API
//! - **Account Operations**: Query balances, get account information
//! - **Market Data**: Fetch tickers, historical klines (OHLCV), current prices
//! - **Real-Time Streams**: Subscribe to order updates, orderbook changes, and trade feeds via WebSocket
//!
//! # API Documentation
//!
//! - Binance.US API: <https://docs.binance.us/>

use crate::binance_us::account::{
    converters, BinanceUsAuth, BinanceUsRestClient, BINANCE_US_WS_URL,
};
use crate::traits::*;
use crate::utils::{
    CircuitBreaker, CircuitBreakerConfig, RateLimiter, RateLimiterConfig,
};
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{debug, error, info, warn};

/// Helper function for safe f64 parsing with logging
fn parse_f64_or_warn(s: &str, field_name: &str) -> f64 {
    s.parse::<f64>().unwrap_or_else(|e| {
        warn!("Failed to parse {} '{}': {}", field_name, s, e);
        0.0
    })
}

/// Helper function for safe timestamp
fn safe_now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_else(|e| {
            error!("System time error: {}", e);
            0
        })
}

/// Unified Binance.US Spot market adapter
#[derive(Clone)]
pub struct BinanceUsSpotAdapter {
    /// HTTP client for REST API requests
    client: BinanceUsRestClient,

    /// Authentication credentials
    auth: BinanceUsAuth,

    /// Current listen key for user data stream
    listen_key: Arc<Mutex<Option<String>>>,

    /// Current WebSocket connection status
    connection_status: Arc<RwLock<ConnectionStatus>>,

    /// Rate limiter for API calls
    rate_limiter: RateLimiter,

    /// Circuit breaker for resilience
    circuit_breaker: CircuitBreaker,

    /// Reconnection counter
    reconnect_count: Arc<AtomicU32>,

    /// Shutdown signal sender
    shutdown_tx: Arc<Mutex<Option<tokio::sync::broadcast::Sender<()>>>>,
}

impl BinanceUsSpotAdapter {
    /// Creates a new Binance.US Spot adapter instance
    pub fn new(api_key: String, api_secret: String) -> Self {
        let auth = BinanceUsAuth::new(api_key, api_secret);
        Self {
            client: BinanceUsRestClient::new_spot(Some(auth.clone())),
            auth,
            listen_key: Arc::new(Mutex::new(None)),
            connection_status: Arc::new(RwLock::new(ConnectionStatus::Disconnected)),
            rate_limiter: RateLimiter::new(RateLimiterConfig::default()),
            circuit_breaker: CircuitBreaker::new("binance_us_spot", CircuitBreakerConfig::production()),
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
        if !self.rate_limiter.acquire().await {
            anyhow::bail!("Rate limit reached for endpoint: {}", endpoint);
        }

        debug!("Calling Binance.US API endpoint: {}", endpoint);

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
        info!("Initiating graceful shutdown of Binance.US Spot adapter");

        if let Some(tx) = self.shutdown_tx.lock().await.as_ref() {
            let _ = tx.send(());
        }

        if let Some(key) = self.listen_key.lock().await.as_ref() {
            let _ = self.delete_listen_key(key).await;
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        info!("Binance.US Spot adapter shutdown complete");
    }

    /// Generates a unique client order ID
    fn generate_client_order_id() -> String {
        format!("mm_{}", safe_now_millis())
    }

    /// Creates a new listen key for user data stream
    async fn create_listen_key(&self) -> Result<String> {
        use serde::Deserialize;

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
    async fn extend_listen_key(&self, listen_key: &str) -> Result<()> {
        let mut params = HashMap::new();
        params.insert("listenKey".to_string(), listen_key.to_string());

        let _: serde_json::Value = self
            .client
            .post_private("/api/v3/userDataStream", params)
            .await?;

        Ok(())
    }

    /// Deletes a listen key
    async fn delete_listen_key(&self, listen_key: &str) -> Result<()> {
        let mut params = HashMap::new();
        params.insert("listenKey".to_string(), listen_key.to_string());

        let _: serde_json::Value = self
            .client
            .delete_private("/api/v3/userDataStream", params)
            .await?;

        Ok(())
    }

    /// Spawns a background task to automatically renew the listen key
    fn spawn_listen_key_renewal_task(
        listen_key: Arc<Mutex<Option<String>>>,
        client: BinanceUsRestClient,
        mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
    ) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30 * 60));

            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        info!("Listen key renewal task shutting down");
                        break;
                    }
                    _ = interval.tick() => {
                        if let Some(key) = listen_key.lock().await.as_ref() {
                            info!("Extending Binance.US listen key");
                            let mut params = HashMap::new();
                            params.insert("listenKey".to_string(), key.clone());

                            match client.post_private::<serde_json::Value>("/api/v3/userDataStream", params).await {
                                Ok(_) => debug!("Listen key extended successfully"),
                                Err(e) => warn!("Failed to extend listen key: {}", e),
                            }
                        }
                    }
                }
            }
        });
    }
}

// ============================================================================
// SpotRest Trait Implementation
// ============================================================================

#[async_trait::async_trait]
impl SpotRest for BinanceUsSpotAdapter {
    async fn create_order(&self, new: NewOrder) -> Result<Order> {
        use serde::Deserialize;

        #[derive(Deserialize)]
        struct OrderResponse {
            symbol: String,
            #[serde(rename = "orderId")]
            order_id: u64,
            #[serde(rename = "clientOrderId")]
            client_order_id: String,
            #[serde(rename = "transactTime")]
            transact_time: u64,
            price: String,
            #[serde(rename = "origQty")]
            orig_qty: String,
            #[serde(rename = "executedQty")]
            executed_qty: String,
            status: String,
            #[serde(rename = "type")]
            order_type: String,
            side: String,
            #[serde(rename = "timeInForce")]
            time_in_force: Option<String>,
        }

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), new.symbol.replace("/", ""));
        params.insert("side".to_string(), converters::to_binance_side(new.side));
        params.insert("type".to_string(), converters::to_binance_order_type(new.ord_type));
        params.insert("quantity".to_string(), new.qty.to_string());

        if let Some(price) = new.price {
            params.insert("price".to_string(), price.to_string());
        }

        if let Some(tif) = new.tif {
            params.insert("timeInForce".to_string(), converters::to_binance_tif(tif));
        } else if new.ord_type == OrderType::Limit {
            params.insert("timeInForce".to_string(), "GTC".to_string());
        }

        if !new.client_order_id.is_empty() {
            params.insert("newClientOrderId".to_string(), new.client_order_id.clone());
        }

        let client = self.client.clone();
        let response: OrderResponse = self.call_api("/api/v3/order", || async {
            client.post_private("/api/v3/order", params.clone()).await
        }).await?;

        let filled_qty = parse_f64_or_warn(&response.executed_qty, "executed_qty");
        let orig_qty = parse_f64_or_warn(&response.orig_qty, "orig_qty");

        Ok(Order {
            venue_order_id: response.order_id.to_string(),
            client_order_id: response.client_order_id,
            symbol: new.symbol,
            ord_type: converters::from_binance_order_type(&response.order_type),
            side: converters::from_binance_side(&response.side),
            qty: orig_qty,
            price: Some(parse_f64_or_warn(&response.price, "price")),
            stop_price: None,
            tif: response.time_in_force.map(|t| converters::from_binance_tif(&t)),
            status: converters::from_binance_order_status(&response.status),
            filled_qty,
            remaining_qty: orig_qty - filled_qty,
            created_ms: response.transact_time,
            updated_ms: response.transact_time,
            recv_ms: safe_now_millis(),
            raw_status: Some(response.status),
        })
    }

    async fn cancel_order(&self, symbol: &str, venue_order_id: &str) -> Result<bool> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.replace("/", ""));
        params.insert("orderId".to_string(), venue_order_id.to_string());

        let client = self.client.clone();
        let _: serde_json::Value = self.call_api("/api/v3/order", || async {
            client.delete_private("/api/v3/order", params.clone()).await
        }).await?;

        Ok(true)
    }

    async fn cancel_all(&self, symbol: Option<&str>) -> Result<usize> {
        if let Some(sym) = symbol {
            let mut params = HashMap::new();
            params.insert("symbol".to_string(), sym.replace("/", ""));

            let client = self.client.clone();
            let response: Vec<serde_json::Value> = self.call_api("/api/v3/openOrders", || async {
                client.delete_private("/api/v3/openOrders", params.clone()).await
            }).await?;

            Ok(response.len())
        } else {
            let open_orders = self.get_open_orders(None).await?;
            let mut cancelled = 0;
            for order in open_orders {
                if self.cancel_order(&order.symbol, &order.venue_order_id).await.is_ok() {
                    cancelled += 1;
                }
            }
            Ok(cancelled)
        }
    }

    async fn get_order(&self, symbol: &str, venue_order_id: &str) -> Result<Order> {
        use serde::Deserialize;

        #[derive(Deserialize)]
        struct OrderResponse {
            symbol: String,
            #[serde(rename = "orderId")]
            order_id: u64,
            #[serde(rename = "clientOrderId")]
            client_order_id: String,
            price: String,
            #[serde(rename = "origQty")]
            orig_qty: String,
            #[serde(rename = "executedQty")]
            executed_qty: String,
            status: String,
            #[serde(rename = "type")]
            order_type: String,
            side: String,
            #[serde(rename = "timeInForce")]
            time_in_force: Option<String>,
            time: u64,
            #[serde(rename = "updateTime")]
            update_time: u64,
        }

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.replace("/", ""));
        params.insert("orderId".to_string(), venue_order_id.to_string());

        let client = self.client.clone();
        let response: OrderResponse = self.call_api("/api/v3/order", || async {
            client.get_private("/api/v3/order", params.clone()).await
        }).await?;

        let filled_qty = parse_f64_or_warn(&response.executed_qty, "executed_qty");
        let orig_qty = parse_f64_or_warn(&response.orig_qty, "orig_qty");

        Ok(Order {
            venue_order_id: response.order_id.to_string(),
            client_order_id: response.client_order_id,
            symbol: symbol.to_string(),
            ord_type: converters::from_binance_order_type(&response.order_type),
            side: converters::from_binance_side(&response.side),
            qty: orig_qty,
            price: Some(parse_f64_or_warn(&response.price, "price")),
            stop_price: None,
            tif: response.time_in_force.map(|t| converters::from_binance_tif(&t)),
            status: converters::from_binance_order_status(&response.status),
            filled_qty,
            remaining_qty: orig_qty - filled_qty,
            created_ms: response.time,
            updated_ms: response.update_time,
            recv_ms: safe_now_millis(),
            raw_status: Some(response.status),
        })
    }

    async fn get_open_orders(&self, symbol: Option<&str>) -> Result<Vec<Order>> {
        use serde::Deserialize;

        #[derive(Deserialize)]
        struct OrderResponse {
            symbol: String,
            #[serde(rename = "orderId")]
            order_id: u64,
            #[serde(rename = "clientOrderId")]
            client_order_id: String,
            price: String,
            #[serde(rename = "origQty")]
            orig_qty: String,
            #[serde(rename = "executedQty")]
            executed_qty: String,
            status: String,
            #[serde(rename = "type")]
            order_type: String,
            side: String,
            #[serde(rename = "timeInForce")]
            time_in_force: Option<String>,
            time: u64,
            #[serde(rename = "updateTime")]
            update_time: u64,
        }

        let mut params = HashMap::new();
        if let Some(sym) = symbol {
            params.insert("symbol".to_string(), sym.replace("/", ""));
        }

        let client = self.client.clone();
        let response: Vec<OrderResponse> = self.call_api("/api/v3/openOrders", || async {
            client.get_private("/api/v3/openOrders", params.clone()).await
        }).await?;

        Ok(response
            .into_iter()
            .map(|r| {
                let filled_qty = parse_f64_or_warn(&r.executed_qty, "executed_qty");
                let orig_qty = parse_f64_or_warn(&r.orig_qty, "orig_qty");
                Order {
                    venue_order_id: r.order_id.to_string(),
                    client_order_id: r.client_order_id,
                    symbol: r.symbol,
                    ord_type: converters::from_binance_order_type(&r.order_type),
                    side: converters::from_binance_side(&r.side),
                    qty: orig_qty,
                    price: Some(parse_f64_or_warn(&r.price, "price")),
                    stop_price: None,
                    tif: r.time_in_force.map(|t| converters::from_binance_tif(&t)),
                    status: converters::from_binance_order_status(&r.status),
                    filled_qty,
                    remaining_qty: orig_qty - filled_qty,
                    created_ms: r.time,
                    updated_ms: r.update_time,
                    recv_ms: safe_now_millis(),
                    raw_status: Some(r.status),
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
        _post_only: Option<bool>,
    ) -> Result<(Order, bool)> {
        // Binance.US doesn't have native replace, so cancel and recreate
        let old_order = self.get_order(symbol, venue_order_id).await?;
        self.cancel_order(symbol, venue_order_id).await?;

        let new_order = NewOrder {
            symbol: symbol.to_string(),
            side: old_order.side,
            ord_type: old_order.ord_type,
            qty: new_qty.unwrap_or(old_order.qty),
            price: new_price.or(old_order.price),
            stop_price: old_order.stop_price,
            tif: new_tif.or(old_order.tif),
            post_only: false,
            reduce_only: false,
            client_order_id: Self::generate_client_order_id(),
        };

        let order = self.create_order(new_order).await?;
        Ok((order, true))
    }

    async fn create_batch_orders(&self, batch: BatchOrderRequest) -> Result<BatchOrderResult> {
        let mut success = Vec::new();
        let mut failed = Vec::new();

        for order in batch.orders {
            match self.create_order(order.clone()).await {
                Ok(result) => success.push(result),
                Err(e) => failed.push((order, e.to_string())),
            }
        }

        Ok(BatchOrderResult { success, failed })
    }

    async fn cancel_batch_orders(&self, symbol: &str, order_ids: Vec<String>) -> Result<BatchCancelResult> {
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

    async fn get_balances(&self) -> Result<Vec<Balance>> {
        use serde::Deserialize;

        #[derive(Deserialize)]
        struct AccountResponse {
            balances: Vec<BalanceInfo>,
        }

        #[derive(Deserialize)]
        struct BalanceInfo {
            asset: String,
            free: String,
            locked: String,
        }

        let params = HashMap::new();
        let client = self.client.clone();
        let response: AccountResponse = self.call_api("/api/v3/account", || async {
            client.get_private("/api/v3/account", params.clone()).await
        }).await?;

        Ok(response
            .balances
            .into_iter()
            .map(|b| {
                let free = parse_f64_or_warn(&b.free, "free");
                let locked = parse_f64_or_warn(&b.locked, "locked");
                Balance {
                    asset: b.asset,
                    free,
                    locked,
                    total: free + locked,
                }
            })
            .filter(|b| b.total > 0.0)
            .collect())
    }

    async fn get_account_info(&self) -> Result<AccountInfo> {
        use serde::Deserialize;

        #[derive(Deserialize)]
        struct AccountResponse {
            #[serde(rename = "canTrade")]
            can_trade: bool,
            #[serde(rename = "canWithdraw")]
            can_withdraw: bool,
            #[serde(rename = "canDeposit")]
            can_deposit: bool,
            #[serde(rename = "updateTime")]
            update_time: u64,
            balances: Vec<BalanceInfo>,
        }

        #[derive(Deserialize)]
        struct BalanceInfo {
            asset: String,
            free: String,
            locked: String,
        }

        let params = HashMap::new();
        let client = self.client.clone();
        let response: AccountResponse = self.call_api("/api/v3/account", || async {
            client.get_private("/api/v3/account", params.clone()).await
        }).await?;

        let balances: Vec<Balance> = response
            .balances
            .into_iter()
            .map(|b| {
                let free = parse_f64_or_warn(&b.free, "free");
                let locked = parse_f64_or_warn(&b.locked, "locked");
                Balance {
                    asset: b.asset,
                    free,
                    locked,
                    total: free + locked,
                }
            })
            .filter(|b| b.total > 0.0)
            .collect();

        Ok(AccountInfo {
            balances,
            can_trade: response.can_trade,
            can_withdraw: response.can_withdraw,
            can_deposit: response.can_deposit,
            update_ms: response.update_time,
        })
    }

    async fn get_market_info(&self, symbol: &str) -> Result<MarketInfo> {
        use serde::Deserialize;

        #[derive(Deserialize)]
        struct ExchangeInfoResponse {
            symbols: Vec<SymbolInfo>,
        }

        #[derive(Deserialize)]
        struct SymbolInfo {
            symbol: String,
            status: String,
            #[serde(rename = "baseAsset")]
            base_asset: String,
            #[serde(rename = "quoteAsset")]
            quote_asset: String,
            filters: Vec<serde_json::Value>,
        }

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.replace("/", ""));

        let client = self.client.clone();
        let response: ExchangeInfoResponse = self.call_api("/api/v3/exchangeInfo", || async {
            client.get_public("/api/v3/exchangeInfo", Some(params.clone())).await
        }).await?;

        let sym_info = response.symbols.into_iter().next()
            .context("Symbol not found")?;

        let mut min_qty = 0.0;
        let mut max_qty = f64::MAX;
        let mut step_size = 0.00000001;
        let mut tick_size = 0.00000001;
        let mut min_notional = 0.0;

        for filter in &sym_info.filters {
            if let Some(filter_type) = filter.get("filterType").and_then(|v| v.as_str()) {
                match filter_type {
                    "LOT_SIZE" => {
                        if let Some(v) = filter.get("minQty").and_then(|v| v.as_str()) {
                            min_qty = parse_f64_or_warn(v, "minQty");
                        }
                        if let Some(v) = filter.get("maxQty").and_then(|v| v.as_str()) {
                            max_qty = parse_f64_or_warn(v, "maxQty");
                        }
                        if let Some(v) = filter.get("stepSize").and_then(|v| v.as_str()) {
                            step_size = parse_f64_or_warn(v, "stepSize");
                        }
                    }
                    "PRICE_FILTER" => {
                        if let Some(v) = filter.get("tickSize").and_then(|v| v.as_str()) {
                            tick_size = parse_f64_or_warn(v, "tickSize");
                        }
                    }
                    "MIN_NOTIONAL" | "NOTIONAL" => {
                        if let Some(v) = filter.get("minNotional").and_then(|v| v.as_str()) {
                            min_notional = parse_f64_or_warn(v, "minNotional");
                        }
                    }
                    _ => {}
                }
            }
        }

        Ok(MarketInfo {
            symbol: symbol.to_string(),
            base_asset: sym_info.base_asset,
            quote_asset: sym_info.quote_asset,
            status: converters::from_binance_symbol_status(&sym_info.status),
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

    async fn get_all_markets(&self) -> Result<Vec<MarketInfo>> {
        use serde::Deserialize;

        #[derive(Deserialize)]
        struct ExchangeInfoResponse {
            symbols: Vec<SymbolInfo>,
        }

        #[derive(Deserialize)]
        struct SymbolInfo {
            symbol: String,
            status: String,
            #[serde(rename = "baseAsset")]
            base_asset: String,
            #[serde(rename = "quoteAsset")]
            quote_asset: String,
            filters: Vec<serde_json::Value>,
        }

        let client = self.client.clone();
        let response: ExchangeInfoResponse = self.call_api("/api/v3/exchangeInfo", || async {
            client.get_public::<ExchangeInfoResponse>("/api/v3/exchangeInfo", None).await
        }).await?;

        Ok(response
            .symbols
            .into_iter()
            .map(|sym| {
                let mut min_qty = 0.0;
                let mut max_qty = f64::MAX;
                let mut step_size = 0.00000001;
                let mut tick_size = 0.00000001;
                let mut min_notional = 0.0;

                for filter in &sym.filters {
                    if let Some(filter_type) = filter.get("filterType").and_then(|v| v.as_str()) {
                        match filter_type {
                            "LOT_SIZE" => {
                                if let Some(v) = filter.get("minQty").and_then(|v| v.as_str()) {
                                    min_qty = parse_f64_or_warn(v, "minQty");
                                }
                                if let Some(v) = filter.get("maxQty").and_then(|v| v.as_str()) {
                                    max_qty = parse_f64_or_warn(v, "maxQty");
                                }
                                if let Some(v) = filter.get("stepSize").and_then(|v| v.as_str()) {
                                    step_size = parse_f64_or_warn(v, "stepSize");
                                }
                            }
                            "PRICE_FILTER" => {
                                if let Some(v) = filter.get("tickSize").and_then(|v| v.as_str()) {
                                    tick_size = parse_f64_or_warn(v, "tickSize");
                                }
                            }
                            "MIN_NOTIONAL" | "NOTIONAL" => {
                                if let Some(v) = filter.get("minNotional").and_then(|v| v.as_str()) {
                                    min_notional = parse_f64_or_warn(v, "minNotional");
                                }
                            }
                            _ => {}
                        }
                    }
                }

                MarketInfo {
                    symbol: format!("{}/{}", sym.base_asset, sym.quote_asset),
                    base_asset: sym.base_asset,
                    quote_asset: sym.quote_asset,
                    status: converters::from_binance_symbol_status(&sym.status),
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

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        use serde::Deserialize;

        #[derive(Deserialize)]
        struct TickerResponse {
            symbol: String,
            #[serde(rename = "lastPrice")]
            last_price: String,
            #[serde(rename = "bidPrice")]
            bid_price: String,
            #[serde(rename = "askPrice")]
            ask_price: String,
            volume: String,
            #[serde(rename = "priceChange")]
            price_change: String,
            #[serde(rename = "priceChangePercent")]
            price_change_percent: String,
            #[serde(rename = "highPrice")]
            high_price: String,
            #[serde(rename = "lowPrice")]
            low_price: String,
            #[serde(rename = "openPrice")]
            open_price: String,
        }

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.replace("/", ""));

        let client = self.client.clone();
        let response: TickerResponse = self.call_api("/api/v3/ticker/24hr", || async {
            client.get_public("/api/v3/ticker/24hr", Some(params.clone())).await
        }).await?;

        Ok(TickerInfo {
            symbol: symbol.to_string(),
            last_price: parse_f64_or_warn(&response.last_price, "last_price"),
            bid_price: parse_f64_or_warn(&response.bid_price, "bid_price"),
            ask_price: parse_f64_or_warn(&response.ask_price, "ask_price"),
            volume_24h: parse_f64_or_warn(&response.volume, "volume"),
            price_change_24h: parse_f64_or_warn(&response.price_change, "price_change"),
            price_change_pct_24h: parse_f64_or_warn(&response.price_change_percent, "price_change_percent"),
            high_24h: parse_f64_or_warn(&response.high_price, "high_price"),
            low_24h: parse_f64_or_warn(&response.low_price, "low_price"),
            open_price_24h: parse_f64_or_warn(&response.open_price, "open_price"),
            ts_ms: safe_now_millis(),
        })
    }

    async fn get_tickers(&self, symbols: Option<Vec<String>>) -> Result<Vec<TickerInfo>> {
        use serde::Deserialize;

        #[derive(Deserialize)]
        struct TickerResponse {
            symbol: String,
            #[serde(rename = "lastPrice")]
            last_price: String,
            #[serde(rename = "bidPrice")]
            bid_price: String,
            #[serde(rename = "askPrice")]
            ask_price: String,
            volume: String,
            #[serde(rename = "priceChange")]
            price_change: String,
            #[serde(rename = "priceChangePercent")]
            price_change_percent: String,
            #[serde(rename = "highPrice")]
            high_price: String,
            #[serde(rename = "lowPrice")]
            low_price: String,
            #[serde(rename = "openPrice")]
            open_price: String,
        }

        let client = self.client.clone();
        let response: Vec<TickerResponse> = self.call_api("/api/v3/ticker/24hr", || async {
            client.get_public::<Vec<TickerResponse>>("/api/v3/ticker/24hr", None).await
        }).await?;

        let mut tickers: Vec<TickerInfo> = response
            .into_iter()
            .map(|r| TickerInfo {
                symbol: r.symbol.clone(),
                last_price: parse_f64_or_warn(&r.last_price, "last_price"),
                bid_price: parse_f64_or_warn(&r.bid_price, "bid_price"),
                ask_price: parse_f64_or_warn(&r.ask_price, "ask_price"),
                volume_24h: parse_f64_or_warn(&r.volume, "volume"),
                price_change_24h: parse_f64_or_warn(&r.price_change, "price_change"),
                price_change_pct_24h: parse_f64_or_warn(&r.price_change_percent, "price_change_percent"),
                high_24h: parse_f64_or_warn(&r.high_price, "high_price"),
                low_24h: parse_f64_or_warn(&r.low_price, "low_price"),
                open_price_24h: parse_f64_or_warn(&r.open_price, "open_price"),
                ts_ms: safe_now_millis(),
            })
            .collect();

        if let Some(syms) = symbols {
            let sym_set: std::collections::HashSet<String> = syms
                .into_iter()
                .map(|s| s.replace("/", ""))
                .collect();
            tickers.retain(|t| sym_set.contains(&t.symbol));
        }

        Ok(tickers)
    }

    async fn get_klines(
        &self,
        symbol: &str,
        interval: KlineInterval,
        start_ms: Option<UnixMillis>,
        end_ms: Option<UnixMillis>,
        limit: Option<usize>,
    ) -> Result<Vec<Kline>> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.replace("/", ""));
        params.insert("interval".to_string(), converters::to_binance_interval(interval));

        if let Some(start) = start_ms {
            params.insert("startTime".to_string(), start.to_string());
        }
        if let Some(end) = end_ms {
            params.insert("endTime".to_string(), end.to_string());
        }
        if let Some(lim) = limit {
            params.insert("limit".to_string(), lim.to_string());
        }

        let client = self.client.clone();
        let response: Vec<Vec<serde_json::Value>> = self.call_api("/api/v3/klines", || async {
            client.get_public("/api/v3/klines", Some(params.clone())).await
        }).await?;

        Ok(response
            .into_iter()
            .filter_map(|k| {
                if k.len() < 11 { return None; }

                Some(Kline {
                    symbol: symbol.to_string(),
                    open_ms: k[0].as_u64().unwrap_or(0),
                    close_ms: k[6].as_u64().unwrap_or(0),
                    open: k[1].as_str().map(|s| parse_f64_or_warn(s, "open")).unwrap_or(0.0),
                    high: k[2].as_str().map(|s| parse_f64_or_warn(s, "high")).unwrap_or(0.0),
                    low: k[3].as_str().map(|s| parse_f64_or_warn(s, "low")).unwrap_or(0.0),
                    close: k[4].as_str().map(|s| parse_f64_or_warn(s, "close")).unwrap_or(0.0),
                    volume: k[5].as_str().map(|s| parse_f64_or_warn(s, "volume")).unwrap_or(0.0),
                    quote_volume: k[7].as_str().map(|s| parse_f64_or_warn(s, "quote_volume")).unwrap_or(0.0),
                    trades: k[8].as_u64().unwrap_or(0),
                })
            })
            .collect())
    }
}

// ============================================================================
// SpotWs Trait Implementation
// ============================================================================

#[async_trait::async_trait]
impl SpotWs for BinanceUsSpotAdapter {
    async fn subscribe_user(&self) -> Result<mpsc::Receiver<UserEvent>> {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::connect_async;

        let listen_key = self.create_listen_key().await?;
        *self.listen_key.lock().await = Some(listen_key.clone());

        let ws_url = format!("{}/{}", BINANCE_US_WS_URL, listen_key);
        let (ws_stream, _) = connect_async(&ws_url).await
            .context("Failed to connect to Binance.US WebSocket")?;

        let (mut write, mut read) = ws_stream.split();
        let (tx, rx) = mpsc::channel(1000);

        *self.connection_status.write().await = ConnectionStatus::Connected;

        // Start listen key renewal
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
        *self.shutdown_tx.lock().await = Some(shutdown_tx);
        Self::spawn_listen_key_renewal_task(
            self.listen_key.clone(),
            self.client.clone(),
            shutdown_rx,
        );

        let connection_status = self.connection_status.clone();

        tokio::spawn(async move {
            while let Some(msg) = read.next().await {
                match msg {
                    Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                        if let Ok(data) = serde_json::from_str::<serde_json::Value>(&text) {
                            if let Some(event_type) = data.get("e").and_then(|v| v.as_str()) {
                                let event = match event_type {
                                    "executionReport" => {
                                        parse_execution_report(&data)
                                    }
                                    "outboundAccountPosition" => {
                                        parse_account_update(&data)
                                    }
                                    _ => None,
                                };

                                if let Some(e) = event {
                                    if tx.send(e).await.is_err() {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    Ok(tokio_tungstenite::tungstenite::Message::Ping(data)) => {
                        let _ = write.send(tokio_tungstenite::tungstenite::Message::Pong(data)).await;
                    }
                    Err(e) => {
                        error!("WebSocket error: {}", e);
                        *connection_status.write().await = ConnectionStatus::Error;
                        break;
                    }
                    _ => {}
                }
            }

            *connection_status.write().await = ConnectionStatus::Disconnected;
        });

        Ok(rx)
    }

    async fn subscribe_books(&self, symbols: &[&str]) -> Result<mpsc::Receiver<BookUpdate>> {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::connect_async;

        let streams: Vec<String> = symbols
            .iter()
            .map(|s| format!("{}@depth@100ms", s.to_lowercase().replace("/", "")))
            .collect();

        let ws_url = format!("{}?streams={}", BINANCE_US_WS_URL, streams.join("/"));
        let (ws_stream, _) = connect_async(&ws_url).await
            .context("Failed to connect to Binance.US WebSocket")?;

        let (mut write, mut read) = ws_stream.split();
        let (tx, rx) = mpsc::channel(10000);

        tokio::spawn(async move {
            while let Some(msg) = read.next().await {
                match msg {
                    Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                        if let Ok(data) = serde_json::from_str::<serde_json::Value>(&text) {
                            if let Some(stream_data) = data.get("data") {
                                if let Some(update) = parse_depth_update(stream_data) {
                                    if tx.send(update).await.is_err() {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    Ok(tokio_tungstenite::tungstenite::Message::Ping(data)) => {
                        let _ = write.send(tokio_tungstenite::tungstenite::Message::Pong(data)).await;
                    }
                    Err(e) => {
                        error!("WebSocket error: {}", e);
                        break;
                    }
                    _ => {}
                }
            }
        });

        Ok(rx)
    }

    async fn subscribe_trades(&self, symbols: &[&str]) -> Result<mpsc::Receiver<TradeEvent>> {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::connect_async;

        let streams: Vec<String> = symbols
            .iter()
            .map(|s| format!("{}@trade", s.to_lowercase().replace("/", "")))
            .collect();

        let ws_url = format!("{}?streams={}", BINANCE_US_WS_URL, streams.join("/"));
        let (ws_stream, _) = connect_async(&ws_url).await
            .context("Failed to connect to Binance.US WebSocket")?;

        let (mut write, mut read) = ws_stream.split();
        let (tx, rx) = mpsc::channel(10000);

        tokio::spawn(async move {
            while let Some(msg) = read.next().await {
                match msg {
                    Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                        if let Ok(data) = serde_json::from_str::<serde_json::Value>(&text) {
                            if let Some(stream_data) = data.get("data") {
                                if let Some(trade) = parse_trade_event(stream_data) {
                                    if tx.send(trade).await.is_err() {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    Ok(tokio_tungstenite::tungstenite::Message::Ping(data)) => {
                        let _ = write.send(tokio_tungstenite::tungstenite::Message::Pong(data)).await;
                    }
                    Err(e) => {
                        error!("WebSocket error: {}", e);
                        break;
                    }
                    _ => {}
                }
            }
        });

        Ok(rx)
    }

    async fn health(&self) -> Result<HealthStatus> {
        let status = self.connection_status.read().await.clone();
        Ok(HealthStatus {
            status,
            last_ping_ms: None,
            last_pong_ms: None,
            latency_ms: None,
            reconnect_count: self.reconnect_count.load(Ordering::Relaxed),
            error_msg: None,
        })
    }

    async fn reconnect(&self) -> Result<()> {
        self.reconnect_count.fetch_add(1, Ordering::Relaxed);
        *self.connection_status.write().await = ConnectionStatus::Reconnecting;

        // Recreate listen key
        if let Ok(key) = self.create_listen_key().await {
            *self.listen_key.lock().await = Some(key);
        }

        *self.connection_status.write().await = ConnectionStatus::Connected;
        Ok(())
    }
}

// ============================================================================
// WebSocket Parsing Helpers
// ============================================================================

fn parse_execution_report(data: &serde_json::Value) -> Option<UserEvent> {
    let symbol = data.get("s")?.as_str()?;
    let order_id = data.get("i")?.as_u64()?;
    let client_order_id = data.get("c")?.as_str()?;
    let status = data.get("X")?.as_str()?;
    let side = data.get("S")?.as_str()?;
    let order_type = data.get("o")?.as_str()?;
    let qty = data.get("q")?.as_str()?;
    let price = data.get("p")?.as_str()?;
    let filled = data.get("z")?.as_str()?;
    let time = data.get("T")?.as_u64()?;

    let filled_qty = parse_f64_or_warn(filled, "filled");
    let orig_qty = parse_f64_or_warn(qty, "qty");

    Some(UserEvent::OrderUpdate(Order {
        venue_order_id: order_id.to_string(),
        client_order_id: client_order_id.to_string(),
        symbol: symbol.to_string(),
        ord_type: converters::from_binance_order_type(order_type),
        side: converters::from_binance_side(side),
        qty: orig_qty,
        price: Some(parse_f64_or_warn(price, "price")),
        stop_price: None,
        tif: None,
        status: converters::from_binance_order_status(status),
        filled_qty,
        remaining_qty: orig_qty - filled_qty,
        created_ms: time,
        updated_ms: time,
        recv_ms: safe_now_millis(),
        raw_status: Some(status.to_string()),
    }))
}

fn parse_account_update(data: &serde_json::Value) -> Option<UserEvent> {
    let balances = data.get("B")?.as_array()?;
    let time = data.get("E")?.as_u64()?;

    for balance in balances {
        let asset = balance.get("a")?.as_str()?;
        let free = balance.get("f")?.as_str()?;
        let locked = balance.get("l")?.as_str()?;

        return Some(UserEvent::Balance {
            asset: asset.to_string(),
            free: parse_f64_or_warn(free, "free"),
            locked: parse_f64_or_warn(locked, "locked"),
            ex_ts_ms: time,
            recv_ms: safe_now_millis(),
        });
    }

    None
}

fn parse_depth_update(data: &serde_json::Value) -> Option<BookUpdate> {
    let symbol = data.get("s")?.as_str()?;
    let time = data.get("E")?.as_u64()?;

    let bids: Vec<(f64, f64)> = data.get("b")?
        .as_array()?
        .iter()
        .filter_map(|b| {
            let arr = b.as_array()?;
            let price = arr.get(0)?.as_str()?;
            let qty = arr.get(1)?.as_str()?;
            Some((parse_f64_or_warn(price, "bid_price"), parse_f64_or_warn(qty, "bid_qty")))
        })
        .collect();

    let asks: Vec<(f64, f64)> = data.get("a")?
        .as_array()?
        .iter()
        .filter_map(|a| {
            let arr = a.as_array()?;
            let price = arr.get(0)?.as_str()?;
            let qty = arr.get(1)?.as_str()?;
            Some((parse_f64_or_warn(price, "ask_price"), parse_f64_or_warn(qty, "ask_qty")))
        })
        .collect();

    Some(BookUpdate::DepthDelta {
        symbol: symbol.to_string(),
        bids,
        asks,
        seq: data.get("u").and_then(|v| v.as_u64()).unwrap_or(0),
        prev_seq: data.get("U").and_then(|v| v.as_u64()).unwrap_or(0),
        checksum: None,
        ex_ts_ms: time,
        recv_ms: safe_now_millis(),
    })
}

fn parse_trade_event(data: &serde_json::Value) -> Option<TradeEvent> {
    let symbol = data.get("s")?.as_str()?;
    let price = data.get("p")?.as_str()?;
    let qty = data.get("q")?.as_str()?;
    let time = data.get("T")?.as_u64()?;
    let is_buyer_maker = data.get("m")?.as_bool()?;

    Some(TradeEvent {
        symbol: symbol.to_string(),
        px: parse_f64_or_warn(price, "price"),
        qty: parse_f64_or_warn(qty, "qty"),
        taker_is_buy: !is_buyer_maker,
        ex_ts_ms: time,
        recv_ms: safe_now_millis(),
    })
}
