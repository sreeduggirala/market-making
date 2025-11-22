//! Kalshi Spot Market Adapter
//!
//! Provides REST and WebSocket functionality for Kalshi prediction markets.
//! Kalshi uses event contracts with yes/no outcomes - this adapter maps them to spot trading.
//!
//! # Important Notes
//! - Kalshi uses "tickers" instead of "symbols"
//! - Prices are in cents (1-99), representing probability (0.01-0.99)
//! - Orders have both action (buy/sell) and side (yes/no)
//! - This adapter defaults to "yes" side contracts
//!
//! # API Documentation
//! - REST: <https://docs.kalshi.com/api-reference>
//! - WebSocket: <https://docs.kalshi.com/websockets>

use crate::kalshi::account::{
    converters, KalshiAuth, KalshiRestClient, KALSHI_WS_URL, KALSHI_WS_URL_DEMO,
};
use crate::traits::*;
use crate::utils::{
    CircuitBreaker, CircuitBreakerConfig, HeartbeatConfig, HeartbeatMonitor,
    RateLimiter, RateLimiterConfig, ReconnectConfig, ReconnectStrategy,
};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{debug, error, info, warn};

/// Kalshi Spot adapter combining REST and WebSocket
#[derive(Clone)]
pub struct KalshiSpotAdapter {
    client: KalshiRestClient,
    auth: Option<KalshiAuth>,
    connection_status: Arc<RwLock<ConnectionStatus>>,
    rate_limiter: RateLimiter,
    circuit_breaker: CircuitBreaker,
    reconnect_count: Arc<AtomicU32>,
    shutdown_tx: Arc<Mutex<Option<tokio::sync::broadcast::Sender<()>>>>,
    ws_message_id: Arc<AtomicU64>,
    use_demo: bool,
}

impl KalshiSpotAdapter {
    /// Creates a new Kalshi spot adapter (production)
    pub fn new(key_id: String, private_key_pem: &str) -> Result<Self> {
        let auth = KalshiAuth::new(key_id, private_key_pem)?;
        Ok(Self {
            client: KalshiRestClient::new(Some(auth.clone())),
            auth: Some(auth),
            connection_status: Arc::new(RwLock::new(ConnectionStatus::Disconnected)),
            rate_limiter: RateLimiter::new(RateLimiterConfig::default()),
            circuit_breaker: CircuitBreaker::new("kalshi_spot", CircuitBreakerConfig::production()),
            reconnect_count: Arc::new(AtomicU32::new(0)),
            shutdown_tx: Arc::new(Mutex::new(None)),
            ws_message_id: Arc::new(AtomicU64::new(1)),
            use_demo: false,
        })
    }

    /// Creates a new Kalshi spot adapter for demo environment
    pub fn new_demo(key_id: String, private_key_pem: &str) -> Result<Self> {
        let auth = KalshiAuth::new(key_id, private_key_pem)?;
        Ok(Self {
            client: KalshiRestClient::new_demo(Some(auth.clone())),
            auth: Some(auth),
            connection_status: Arc::new(RwLock::new(ConnectionStatus::Disconnected)),
            rate_limiter: RateLimiter::new(RateLimiterConfig::default()),
            circuit_breaker: CircuitBreaker::new("kalshi_spot_demo", CircuitBreakerConfig::production()),
            reconnect_count: Arc::new(AtomicU32::new(0)),
            shutdown_tx: Arc::new(Mutex::new(None)),
            ws_message_id: Arc::new(AtomicU64::new(1)),
            use_demo: true,
        })
    }

    /// Creates a new adapter without authentication (public endpoints only)
    pub fn new_public() -> Self {
        Self {
            client: KalshiRestClient::new(None),
            auth: None,
            connection_status: Arc::new(RwLock::new(ConnectionStatus::Disconnected)),
            rate_limiter: RateLimiter::new(RateLimiterConfig::default()),
            circuit_breaker: CircuitBreaker::new("kalshi_spot_public", CircuitBreakerConfig::production()),
            reconnect_count: Arc::new(AtomicU32::new(0)),
            shutdown_tx: Arc::new(Mutex::new(None)),
            ws_message_id: Arc::new(AtomicU64::new(1)),
            use_demo: false,
        }
    }

    /// Generates a unique client order ID
    fn generate_client_order_id() -> String {
        format!("mm_{}", uuid::Uuid::new_v4().simple())
    }

    fn now_millis() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    fn next_ws_message_id(&self) -> u64 {
        self.ws_message_id.fetch_add(1, Ordering::Relaxed)
    }

    fn ws_url(&self) -> &'static str {
        if self.use_demo { KALSHI_WS_URL_DEMO } else { KALSHI_WS_URL }
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

        debug!("Calling Kalshi API endpoint: {}", endpoint);

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

    /// Shutdown the adapter
    pub async fn shutdown(&self) {
        info!("Initiating graceful shutdown of Kalshi Spot adapter");
        if let Some(tx) = self.shutdown_tx.lock().await.take() {
            let _ = tx.send(());
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        info!("Kalshi Spot adapter shutdown complete");
    }
}

// =============================================================================
// REST API Response Types
// =============================================================================

#[derive(Debug, Deserialize)]
struct ExchangeStatus {
    exchange_active: bool,
    trading_active: bool,
}

#[derive(Debug, Deserialize)]
struct MarketsResponse {
    markets: Vec<KalshiMarket>,
    cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct KalshiMarket {
    ticker: String,
    event_ticker: String,
    subtitle: Option<String>,
    yes_bid: Option<i32>,
    yes_ask: Option<i32>,
    no_bid: Option<i32>,
    no_ask: Option<i32>,
    last_price: Option<i32>,
    volume: Option<i64>,
    volume_24h: Option<i64>,
    open_interest: Option<i64>,
    status: String,
    result: Option<String>,
    #[serde(default)]
    floor_strike: Option<f64>,
    #[serde(default)]
    cap_strike: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct MarketResponse {
    market: KalshiMarket,
}

#[derive(Debug, Deserialize)]
struct OrderResponse {
    order: KalshiOrder,
}

#[derive(Debug, Deserialize)]
struct OrdersResponse {
    orders: Vec<KalshiOrder>,
    cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct KalshiOrder {
    order_id: String,
    #[serde(default)]
    client_order_id: Option<String>,
    ticker: String,
    action: String, // buy/sell
    side: String,   // yes/no
    #[serde(rename = "type")]
    order_type: String,
    status: String,
    yes_price: Option<i32>,
    no_price: Option<i32>,
    #[serde(default)]
    initial_count: i32,
    #[serde(default)]
    remaining_count: i32,
    #[serde(default)]
    fill_count: i32,
    created_time: Option<String>,
    #[serde(default)]
    last_update_time: Option<String>,
}

#[derive(Debug, Serialize)]
struct CreateOrderRequest {
    ticker: String,
    action: String, // buy/sell
    side: String,   // yes/no (default: yes)
    count: i32,
    #[serde(rename = "type")]
    order_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    yes_price: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    no_price: Option<i32>,
    client_order_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    expiration_ts: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct BalanceResponse {
    balance: i64, // in cents
}

#[derive(Debug, Deserialize)]
struct PositionsResponse {
    market_positions: Vec<KalshiPosition>,
    cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct KalshiPosition {
    ticker: String,
    #[serde(default)]
    position: i32, // positive = yes contracts, negative = no contracts
    #[serde(default)]
    market_exposure: i64,
    #[serde(default)]
    realized_pnl: i64,
}

// =============================================================================
// SpotRest Implementation
// =============================================================================

#[async_trait::async_trait]
impl SpotRest for KalshiSpotAdapter {
    async fn create_order(&self, new: NewOrder) -> Result<Order> {
        let client_order_id = if new.client_order_id.is_empty() {
            Self::generate_client_order_id()
        } else {
            new.client_order_id.clone()
        };

        // Convert price from decimal to cents
        let yes_price = new.price.map(|p| converters::price_to_cents(p));

        let request = CreateOrderRequest {
            ticker: new.symbol.clone(),
            action: converters::to_kalshi_action(new.side).to_string(),
            side: "yes".to_string(), // Default to yes contracts
            count: new.qty as i32,
            order_type: converters::to_kalshi_order_type(new.ord_type).to_string(),
            yes_price,
            no_price: None,
            client_order_id: client_order_id.clone(),
            expiration_ts: None,
        };

        let response: OrderResponse = self
            .call_api("/portfolio/orders", || async {
                self.client
                    .post_private("/portfolio/orders", &request)
                    .await
            })
            .await?;

        let order = response.order;
        let now = Self::now_millis();

        Ok(Order {
            venue_order_id: order.order_id,
            client_order_id,
            symbol: order.ticker,
            ord_type: converters::from_kalshi_order_type(&order.order_type),
            side: converters::from_kalshi_action(&order.action),
            qty: order.initial_count as f64,
            price: order.yes_price.map(|p| converters::cents_to_price(p)),
            stop_price: None,
            tif: Some(TimeInForce::Gtc),
            status: converters::from_kalshi_order_status(&order.status),
            filled_qty: order.fill_count as f64,
            remaining_qty: order.remaining_count as f64,
            created_ms: now,
            updated_ms: now,
            recv_ms: now,
            raw_status: Some(order.status),
        })
    }

    async fn cancel_order(&self, _symbol: &str, venue_order_id: &str) -> Result<bool> {
        let endpoint = format!("/portfolio/orders/{}", venue_order_id);

        let _response: serde_json::Value = self
            .call_api(&endpoint, || async {
                self.client.delete_private(&endpoint).await
            })
            .await?;

        Ok(true)
    }

    async fn cancel_all(&self, symbol: Option<&str>) -> Result<usize> {
        // Kalshi doesn't have a cancel all endpoint, so we need to get open orders and cancel each
        let orders = self.get_open_orders(symbol).await?;
        let mut cancelled = 0;

        for order in orders {
            if self.cancel_order(&order.symbol, &order.venue_order_id).await.is_ok() {
                cancelled += 1;
            }
        }

        Ok(cancelled)
    }

    async fn get_order(&self, _symbol: &str, venue_order_id: &str) -> Result<Order> {
        let endpoint = format!("/portfolio/orders/{}", venue_order_id);

        let response: OrderResponse = self
            .call_api(&endpoint, || async {
                self.client.get_private(&endpoint, None).await
            })
            .await?;

        let order = response.order;
        let now = Self::now_millis();

        Ok(Order {
            venue_order_id: order.order_id,
            client_order_id: order.client_order_id.unwrap_or_default(),
            symbol: order.ticker,
            ord_type: converters::from_kalshi_order_type(&order.order_type),
            side: converters::from_kalshi_action(&order.action),
            qty: order.initial_count as f64,
            price: order.yes_price.map(|p| converters::cents_to_price(p)),
            stop_price: None,
            tif: Some(TimeInForce::Gtc),
            status: converters::from_kalshi_order_status(&order.status),
            filled_qty: order.fill_count as f64,
            remaining_qty: order.remaining_count as f64,
            created_ms: now,
            updated_ms: now,
            recv_ms: now,
            raw_status: Some(order.status),
        })
    }

    async fn get_open_orders(&self, symbol: Option<&str>) -> Result<Vec<Order>> {
        let mut params = HashMap::new();
        params.insert("status".to_string(), "resting".to_string());
        if let Some(s) = symbol {
            params.insert("ticker".to_string(), s.to_string());
        }

        let response: OrdersResponse = self
            .call_api("/portfolio/orders", || async {
                self.client.get_private("/portfolio/orders", Some(params.clone())).await
            })
            .await?;

        let now = Self::now_millis();

        Ok(response
            .orders
            .into_iter()
            .map(|order| Order {
                venue_order_id: order.order_id,
                client_order_id: order.client_order_id.unwrap_or_default(),
                symbol: order.ticker,
                ord_type: converters::from_kalshi_order_type(&order.order_type),
                side: converters::from_kalshi_action(&order.action),
                qty: order.initial_count as f64,
                price: order.yes_price.map(|p| converters::cents_to_price(p)),
                stop_price: None,
                tif: Some(TimeInForce::Gtc),
                status: converters::from_kalshi_order_status(&order.status),
                filled_qty: order.fill_count as f64,
                remaining_qty: order.remaining_count as f64,
                created_ms: now,
                updated_ms: now,
                recv_ms: now,
                raw_status: Some(order.status),
            })
            .collect())
    }

    async fn replace_order(
        &self,
        symbol: &str,
        venue_order_id: &str,
        new_price: Option<Price>,
        new_qty: Option<Quantity>,
        _new_tif: Option<TimeInForce>,
        _post_only: Option<bool>,
    ) -> Result<(Order, bool)> {
        // Kalshi doesn't support order amendment, so cancel and recreate
        let old_order = self.get_order(symbol, venue_order_id).await?;
        self.cancel_order(symbol, venue_order_id).await?;

        let new_order = NewOrder {
            symbol: symbol.to_string(),
            side: old_order.side,
            ord_type: old_order.ord_type,
            qty: new_qty.unwrap_or(old_order.qty),
            price: new_price.or(old_order.price),
            stop_price: None,
            tif: old_order.tif,
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

    async fn get_balances(&self) -> Result<Vec<Balance>> {
        let response: BalanceResponse = self
            .call_api("/portfolio/balance", || async {
                self.client.get_private("/portfolio/balance", None).await
            })
            .await?;

        // Kalshi uses USD cents
        let usd_balance = response.balance as f64 / 100.0;

        Ok(vec![Balance {
            asset: "USD".to_string(),
            free: usd_balance,
            locked: 0.0,
            total: usd_balance,
        }])
    }

    async fn get_account_info(&self) -> Result<AccountInfo> {
        let balances = self.get_balances().await?;
        let now = Self::now_millis();

        // Check exchange status
        let status: ExchangeStatus = self
            .client
            .get_public("/exchange/status", None)
            .await
            .unwrap_or(ExchangeStatus {
                exchange_active: true,
                trading_active: true,
            });

        Ok(AccountInfo {
            balances,
            can_trade: status.trading_active,
            can_withdraw: status.exchange_active,
            can_deposit: status.exchange_active,
            update_ms: now,
        })
    }

    async fn get_market_info(&self, symbol: &str) -> Result<MarketInfo> {
        let endpoint = format!("/markets/{}", symbol);

        let response: MarketResponse = self
            .call_api(&endpoint, || async {
                self.client.get_public(&endpoint, None).await
            })
            .await?;

        let market = response.market;
        let status = match market.status.as_str() {
            "active" | "open" => MarketStatus::Trading,
            "closed" | "settled" => MarketStatus::PostTrading,
            _ => MarketStatus::Halt,
        };

        Ok(MarketInfo {
            symbol: market.ticker,
            base_asset: market.event_ticker.clone(),
            quote_asset: "USD".to_string(),
            status,
            min_qty: 1.0,        // Minimum 1 contract
            max_qty: 100000.0,   // Maximum contracts per order
            step_size: 1.0,      // Contracts are whole numbers
            tick_size: 0.01,     // Price tick is 1 cent
            min_notional: 0.01,  // Minimum 1 cent
            max_leverage: None,
            is_spot: true,
            is_perp: false,
        })
    }

    async fn get_all_markets(&self) -> Result<Vec<MarketInfo>> {
        let response: MarketsResponse = self
            .call_api("/markets", || async {
                self.client.get_public("/markets", None).await
            })
            .await?;

        Ok(response
            .markets
            .into_iter()
            .map(|market| {
                let status = match market.status.as_str() {
                    "active" | "open" => MarketStatus::Trading,
                    "closed" | "settled" => MarketStatus::PostTrading,
                    _ => MarketStatus::Halt,
                };

                MarketInfo {
                    symbol: market.ticker,
                    base_asset: market.event_ticker,
                    quote_asset: "USD".to_string(),
                    status,
                    min_qty: 1.0,
                    max_qty: 100000.0,
                    step_size: 1.0,
                    tick_size: 0.01,
                    min_notional: 0.01,
                    max_leverage: None,
                    is_spot: true,
                    is_perp: false,
                }
            })
            .collect())
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        let endpoint = format!("/markets/{}", symbol);

        let response: MarketResponse = self
            .call_api(&endpoint, || async {
                self.client.get_public(&endpoint, None).await
            })
            .await?;

        let market = response.market;
        let now = Self::now_millis();

        let last_price = market.last_price.map(|p| converters::cents_to_price(p)).unwrap_or(0.0);
        let bid_price = market.yes_bid.map(|p| converters::cents_to_price(p)).unwrap_or(0.0);
        let ask_price = market.yes_ask.map(|p| converters::cents_to_price(p)).unwrap_or(0.0);

        Ok(TickerInfo {
            symbol: market.ticker,
            last_price,
            bid_price,
            ask_price,
            volume_24h: market.volume_24h.unwrap_or(0) as f64,
            price_change_24h: 0.0,     // Not provided by Kalshi
            price_change_pct_24h: 0.0, // Not provided
            high_24h: 0.99,            // Max price for prediction markets
            low_24h: 0.01,             // Min price
            open_price_24h: last_price,
            ts_ms: now,
        })
    }

    async fn get_tickers(&self, symbols: Option<Vec<String>>) -> Result<Vec<TickerInfo>> {
        let markets = self.get_all_markets().await?;
        let now = Self::now_millis();

        let mut tickers: Vec<TickerInfo> = Vec::new();

        for market_info in markets {
            // Get detailed ticker data
            if let Ok(ticker) = self.get_ticker(&market_info.symbol).await {
                tickers.push(ticker);
            }
        }

        if let Some(syms) = symbols {
            let sym_set: std::collections::HashSet<_> = syms.into_iter().collect();
            tickers.retain(|t| sym_set.contains(&t.symbol));
        }

        Ok(tickers)
    }

    async fn get_klines(
        &self,
        _symbol: &str,
        _interval: KlineInterval,
        _start_ms: Option<UnixMillis>,
        _end_ms: Option<UnixMillis>,
        _limit: Option<usize>,
    ) -> Result<Vec<Kline>> {
        // Kalshi doesn't provide kline/candlestick data
        Ok(Vec::new())
    }
}

// =============================================================================
// SpotWs Implementation
// =============================================================================

#[async_trait::async_trait]
impl SpotWs for KalshiSpotAdapter {
    async fn subscribe_user(&self) -> Result<mpsc::Receiver<UserEvent>> {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::tungstenite::Message;

        let (tx, rx) = mpsc::channel(1000);
        let adapter = self.clone();
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel(1);

        {
            let mut guard = adapter.shutdown_tx.lock().await;
            *guard = Some(shutdown_tx);
        }

        let auth = self.auth.clone().context("Authentication required for user stream")?;

        tokio::spawn(async move {
            let reconnect_config = ReconnectConfig::production();
            let mut strategy = ReconnectStrategy::new(reconnect_config);

            'reconnect: loop {
                let reconnect_num = adapter.reconnect_count.load(Ordering::Relaxed);
                info!("Kalshi user stream connecting (reconnect #{})", reconnect_num);

                {
                    let mut status = adapter.connection_status.write().await;
                    *status = ConnectionStatus::Connecting;
                }

                // Build authenticated WebSocket URL
                let timestamp = KalshiAuth::timestamp();
                let signature = match auth.sign_websocket(timestamp) {
                    Ok(s) => s,
                    Err(e) => {
                        error!("Failed to sign WebSocket auth: {}", e);
                        continue 'reconnect;
                    }
                };

                let ws_url = adapter.ws_url();

                // Connect with auth headers
                let request = http::Request::builder()
                    .uri(ws_url)
                    .header("KALSHI-ACCESS-KEY", &auth.key_id)
                    .header("KALSHI-ACCESS-TIMESTAMP", timestamp.to_string())
                    .header("KALSHI-ACCESS-SIGNATURE", signature)
                    .header("Host", if adapter.use_demo { "demo-api.kalshi.co" } else { "api.elections.kalshi.com" })
                    .header("Connection", "Upgrade")
                    .header("Upgrade", "websocket")
                    .header("Sec-WebSocket-Version", "13")
                    .header("Sec-WebSocket-Key", tokio_tungstenite::tungstenite::handshake::client::generate_key())
                    .body(())
                    .unwrap();

                let mut ws = match tokio_tungstenite::connect_async(request).await {
                    Ok((stream, _)) => stream,
                    Err(e) => {
                        error!("Kalshi WebSocket connection failed: {}", e);
                        if !strategy.can_retry() {
                            break 'reconnect;
                        }
                        let delay = strategy.wait_before_retry().await;
                        warn!("Kalshi user stream reconnecting - waiting {}ms", delay);
                        continue 'reconnect;
                    }
                };

                // Subscribe to fill updates
                let sub_msg = serde_json::json!({
                    "id": adapter.next_ws_message_id(),
                    "cmd": "subscribe",
                    "params": {
                        "channels": ["fill"]
                    }
                });

                if ws.send(Message::Text(sub_msg.to_string())).await.is_err() {
                    error!("Kalshi failed to subscribe to user channels");
                    continue 'reconnect;
                }

                strategy.reset();
                adapter.reconnect_count.fetch_add(1, Ordering::Relaxed);
                {
                    let mut status = adapter.connection_status.write().await;
                    *status = ConnectionStatus::Connected;
                }
                info!("Kalshi user WebSocket connected");

                let heartbeat = HeartbeatMonitor::new(HeartbeatConfig::production());

                'message_loop: loop {
                    tokio::select! {
                        _ = shutdown_rx.recv() => {
                            info!("Kalshi user stream shutdown signal received");
                            break 'reconnect;
                        }
                        _ = tokio::time::sleep(tokio::time::Duration::from_secs(30)) => {
                            if !heartbeat.is_alive().await {
                                warn!("Kalshi user stream heartbeat timeout");
                                break 'message_loop;
                            }
                        }
                        msg = ws.next() => {
                            match msg {
                                Some(Ok(Message::Text(text))) => {
                                    heartbeat.record_message_received().await;

                                    if let Ok(event) = parse_kalshi_user_event(&text) {
                                        if tx.send(event).await.is_err() {
                                            info!("Kalshi user stream receiver dropped");
                                            break 'reconnect;
                                        }
                                    }
                                }
                                Some(Ok(Message::Ping(data))) => {
                                    heartbeat.record_pong_received().await;
                                    if ws.send(Message::Pong(data)).await.is_err() {
                                        break 'message_loop;
                                    }
                                }
                                Some(Ok(Message::Pong(_))) => {
                                    heartbeat.record_pong_received().await;
                                }
                                Some(Ok(Message::Close(_))) => {
                                    warn!("Kalshi WebSocket closed by server");
                                    break 'message_loop;
                                }
                                Some(Err(e)) => {
                                    error!("Kalshi WebSocket error: {}", e);
                                    break 'message_loop;
                                }
                                None => {
                                    warn!("Kalshi WebSocket stream ended");
                                    break 'message_loop;
                                }
                                _ => {}
                            }
                        }
                    }
                }

                {
                    let mut status = adapter.connection_status.write().await;
                    *status = ConnectionStatus::Disconnected;
                }

                if !strategy.can_retry() {
                    error!("Kalshi user stream max reconnection attempts reached");
                    break 'reconnect;
                }

                let delay = strategy.wait_before_retry().await;
                warn!("Kalshi user stream reconnecting - waiting {}ms", delay);
            }

            info!("Kalshi user stream task terminated");
        });

        Ok(rx)
    }

    async fn subscribe_books(&self, symbols: &[&str]) -> Result<mpsc::Receiver<BookUpdate>> {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::tungstenite::Message;

        let (tx, rx) = mpsc::channel(1000);
        let adapter = self.clone();
        let symbols_owned: Vec<String> = symbols.iter().map(|s| s.to_string()).collect();
        let mut shutdown_rx = match adapter.shutdown_tx.lock().await.as_ref() {
            Some(tx) => tx.subscribe(),
            None => {
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
                info!("Kalshi books stream connecting (reconnect #{})", reconnect_num);

                let ws_url = adapter.ws_url();

                let mut ws = match tokio_tungstenite::connect_async(ws_url).await {
                    Ok((stream, _)) => stream,
                    Err(e) => {
                        error!("Kalshi books WebSocket connection failed: {}", e);
                        if !strategy.can_retry() {
                            break 'reconnect;
                        }
                        let delay = strategy.wait_before_retry().await;
                        warn!("Kalshi books stream reconnecting - waiting {}ms", delay);
                        continue 'reconnect;
                    }
                };

                // Subscribe to orderbook for all symbols
                let sub_msg = serde_json::json!({
                    "id": adapter.next_ws_message_id(),
                    "cmd": "subscribe",
                    "params": {
                        "channels": ["orderbook_delta"],
                        "market_tickers": symbols_owned
                    }
                });

                if ws.send(Message::Text(sub_msg.to_string())).await.is_err() {
                    warn!("Kalshi failed to subscribe to orderbooks");
                    continue 'reconnect;
                }

                strategy.reset();
                info!("Kalshi books WebSocket connected");

                let heartbeat = HeartbeatMonitor::new(HeartbeatConfig::production());
                let mut seq_map: HashMap<String, u64> = HashMap::new();

                'message_loop: loop {
                    tokio::select! {
                        _ = shutdown_rx.recv() => {
                            info!("Kalshi books stream shutdown signal received");
                            break 'reconnect;
                        }
                        _ = tokio::time::sleep(tokio::time::Duration::from_secs(30)) => {
                            if !heartbeat.is_alive().await {
                                warn!("Kalshi books stream heartbeat timeout");
                                break 'message_loop;
                            }
                        }
                        msg = ws.next() => {
                            match msg {
                                Some(Ok(Message::Text(text))) => {
                                    heartbeat.record_message_received().await;

                                    if let Ok(update) = parse_kalshi_book_update(&text, &mut seq_map) {
                                        if tx.send(update).await.is_err() {
                                            info!("Kalshi books stream receiver dropped");
                                            break 'reconnect;
                                        }
                                    }
                                }
                                Some(Ok(Message::Ping(data))) => {
                                    heartbeat.record_pong_received().await;
                                    if ws.send(Message::Pong(data)).await.is_err() {
                                        break 'message_loop;
                                    }
                                }
                                Some(Ok(Message::Pong(_))) => {
                                    heartbeat.record_pong_received().await;
                                }
                                Some(Ok(Message::Close(_))) => {
                                    warn!("Kalshi books WebSocket closed");
                                    break 'message_loop;
                                }
                                Some(Err(e)) => {
                                    error!("Kalshi books WebSocket error: {}", e);
                                    break 'message_loop;
                                }
                                None => {
                                    warn!("Kalshi books WebSocket stream ended");
                                    break 'message_loop;
                                }
                                _ => {}
                            }
                        }
                    }
                }

                if !strategy.can_retry() {
                    error!("Kalshi books stream max reconnection attempts reached");
                    break 'reconnect;
                }

                let delay = strategy.wait_before_retry().await;
                warn!("Kalshi books stream reconnecting - waiting {}ms", delay);
            }

            info!("Kalshi books stream task terminated");
        });

        Ok(rx)
    }

    async fn subscribe_trades(&self, symbols: &[&str]) -> Result<mpsc::Receiver<TradeEvent>> {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::tungstenite::Message;

        let (tx, rx) = mpsc::channel(1000);
        let adapter = self.clone();
        let symbols_owned: Vec<String> = symbols.iter().map(|s| s.to_string()).collect();
        let mut shutdown_rx = match adapter.shutdown_tx.lock().await.as_ref() {
            Some(tx) => tx.subscribe(),
            None => {
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
                info!("Kalshi trades stream connecting (reconnect #{})", reconnect_num);

                let ws_url = adapter.ws_url();

                let mut ws = match tokio_tungstenite::connect_async(ws_url).await {
                    Ok((stream, _)) => stream,
                    Err(e) => {
                        error!("Kalshi trades WebSocket connection failed: {}", e);
                        if !strategy.can_retry() {
                            break 'reconnect;
                        }
                        let delay = strategy.wait_before_retry().await;
                        warn!("Kalshi trades stream reconnecting - waiting {}ms", delay);
                        continue 'reconnect;
                    }
                };

                // Subscribe to trades for all symbols
                let sub_msg = serde_json::json!({
                    "id": adapter.next_ws_message_id(),
                    "cmd": "subscribe",
                    "params": {
                        "channels": ["trade"],
                        "market_tickers": symbols_owned
                    }
                });

                if ws.send(Message::Text(sub_msg.to_string())).await.is_err() {
                    warn!("Kalshi failed to subscribe to trades");
                    continue 'reconnect;
                }

                strategy.reset();
                info!("Kalshi trades WebSocket connected");

                let heartbeat = HeartbeatMonitor::new(HeartbeatConfig::production());

                'message_loop: loop {
                    tokio::select! {
                        _ = shutdown_rx.recv() => {
                            info!("Kalshi trades stream shutdown signal received");
                            break 'reconnect;
                        }
                        _ = tokio::time::sleep(tokio::time::Duration::from_secs(30)) => {
                            if !heartbeat.is_alive().await {
                                warn!("Kalshi trades stream heartbeat timeout");
                                break 'message_loop;
                            }
                        }
                        msg = ws.next() => {
                            match msg {
                                Some(Ok(Message::Text(text))) => {
                                    heartbeat.record_message_received().await;

                                    if let Ok(events) = parse_kalshi_trade_events(&text) {
                                        for event in events {
                                            if tx.send(event).await.is_err() {
                                                info!("Kalshi trades stream receiver dropped");
                                                break 'reconnect;
                                            }
                                        }
                                    }
                                }
                                Some(Ok(Message::Ping(data))) => {
                                    heartbeat.record_pong_received().await;
                                    if ws.send(Message::Pong(data)).await.is_err() {
                                        break 'message_loop;
                                    }
                                }
                                Some(Ok(Message::Pong(_))) => {
                                    heartbeat.record_pong_received().await;
                                }
                                Some(Ok(Message::Close(_))) => {
                                    warn!("Kalshi trades WebSocket closed");
                                    break 'message_loop;
                                }
                                Some(Err(e)) => {
                                    error!("Kalshi trades WebSocket error: {}", e);
                                    break 'message_loop;
                                }
                                None => {
                                    warn!("Kalshi trades WebSocket stream ended");
                                    break 'message_loop;
                                }
                                _ => {}
                            }
                        }
                    }
                }

                if !strategy.can_retry() {
                    error!("Kalshi trades stream max reconnection attempts reached");
                    break 'reconnect;
                }

                let delay = strategy.wait_before_retry().await;
                warn!("Kalshi trades stream reconnecting - waiting {}ms", delay);
            }

            info!("Kalshi trades stream task terminated");
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
            reconnect_count: self.reconnect_count.load(Ordering::Relaxed),
            error_msg: None,
        })
    }

    async fn reconnect(&self) -> Result<()> {
        {
            let mut status = self.connection_status.write().await;
            *status = ConnectionStatus::Reconnecting;
        }

        self.reconnect_count.fetch_add(1, Ordering::Relaxed);

        {
            let mut status = self.connection_status.write().await;
            *status = ConnectionStatus::Connected;
        }

        Ok(())
    }
}

// =============================================================================
// WebSocket Message Parsers
// =============================================================================

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[derive(Debug, Deserialize)]
struct KalshiWsFill {
    ticker: String,
    order_id: String,
    #[serde(default)]
    client_order_id: Option<String>,
    action: String,
    side: String,
    count: i32,
    yes_price: Option<i32>,
    no_price: Option<i32>,
    #[serde(default)]
    is_taker: bool,
    #[serde(default)]
    trade_id: Option<String>,
    ts: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct KalshiWsMessage {
    #[serde(rename = "type")]
    msg_type: Option<String>,
    msg: Option<KalshiWsMsgContent>,
}

#[derive(Debug, Deserialize)]
struct KalshiWsMsgContent {
    #[serde(default)]
    fills: Vec<KalshiWsFill>,
    #[serde(default)]
    market_ticker: Option<String>,
    #[serde(default)]
    yes: Vec<[i32; 2]>, // [price_cents, quantity]
    #[serde(default)]
    no: Vec<[i32; 2]>,
    #[serde(default)]
    seq: Option<u64>,
    #[serde(default)]
    trades: Vec<KalshiWsTrade>,
}

#[derive(Debug, Deserialize)]
struct KalshiWsTrade {
    ticker: String,
    #[serde(default)]
    count: i32,
    yes_price: Option<i32>,
    #[serde(default)]
    taker_side: Option<String>,
    ts: Option<i64>,
}

fn parse_kalshi_user_event(text: &str) -> Result<UserEvent> {
    let msg: KalshiWsMessage = serde_json::from_str(text)?;

    if let Some(content) = msg.msg {
        if !content.fills.is_empty() {
            let fill = &content.fills[0];
            let now = now_millis();
            let price = fill.yes_price.map(|p| converters::cents_to_price(p)).unwrap_or(0.0);

            return Ok(UserEvent::Fill(Fill {
                venue_order_id: fill.order_id.clone(),
                client_order_id: fill.client_order_id.clone().unwrap_or_default(),
                symbol: fill.ticker.clone(),
                price,
                qty: fill.count as f64,
                fee: 0.0,
                fee_ccy: "USD".to_string(),
                is_maker: !fill.is_taker,
                exec_id: fill.trade_id.clone().unwrap_or_default(),
                ex_ts_ms: fill.ts.unwrap_or(now as i64) as u64,
                recv_ms: now,
            }));
        }
    }

    anyhow::bail!("Unknown or unparseable Kalshi user event")
}

fn parse_kalshi_book_update(text: &str, seq_map: &mut HashMap<String, u64>) -> Result<BookUpdate> {
    let msg: KalshiWsMessage = serde_json::from_str(text)?;

    if let Some(content) = msg.msg {
        if let Some(ticker) = content.market_ticker {
            let prev_seq = seq_map.get(&ticker).copied().unwrap_or(0);
            let new_seq = content.seq.unwrap_or(prev_seq + 1);
            seq_map.insert(ticker.clone(), new_seq);
            let now = now_millis();

            // Convert yes side prices to bids, no side to asks
            let bids: Vec<(Price, Quantity)> = content.yes
                .iter()
                .map(|[price, qty]| (converters::cents_to_price(*price), *qty as f64))
                .collect();

            let asks: Vec<(Price, Quantity)> = content.no
                .iter()
                .map(|[price, qty]| (converters::cents_to_price(*price), *qty as f64))
                .collect();

            return Ok(BookUpdate::DepthDelta {
                symbol: ticker,
                bids,
                asks,
                seq: new_seq,
                prev_seq,
                checksum: None,
                ex_ts_ms: now,
                recv_ms: now,
            });
        }
    }

    anyhow::bail!("Unknown or unparseable Kalshi book update")
}

fn parse_kalshi_trade_events(text: &str) -> Result<Vec<TradeEvent>> {
    let msg: KalshiWsMessage = serde_json::from_str(text)?;

    if let Some(content) = msg.msg {
        let now = now_millis();
        let trades: Vec<TradeEvent> = content.trades
            .into_iter()
            .map(|trade| {
                let price = trade.yes_price.map(|p| converters::cents_to_price(p)).unwrap_or(0.0);
                TradeEvent {
                    symbol: trade.ticker,
                    px: price,
                    qty: trade.count as f64,
                    taker_is_buy: trade.taker_side.as_deref() == Some("yes"),
                    ex_ts_ms: trade.ts.unwrap_or(now as i64) as u64,
                    recv_ms: now,
                }
            })
            .collect();

        if !trades.is_empty() {
            return Ok(trades);
        }
    }

    anyhow::bail!("Unknown or unparseable Kalshi trade event")
}
