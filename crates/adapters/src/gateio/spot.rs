//! Gate.io Spot Trading Adapter
//!
//! Implements SpotRest and SpotWs traits for Gate.io spot trading.

use super::account::{converters, GateioAuth, GateioRestClient, GATEIO_WS_SPOT_URL};
use crate::traits::*;
use crate::utils::{
    CircuitBreaker, CircuitBreakerConfig, HeartbeatConfig, HeartbeatMonitor, RateLimiter,
    RateLimiterConfig, ReconnectConfig, ReconnectStrategy,
};
use anyhow::{Context, Result};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use hmac::Mac;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio_tungstenite::{connect_async, tungstenite::Message};

// =============================================================================
// Adapter
// =============================================================================

/// Gate.io spot trading adapter
pub struct GateioSpotAdapter {
    rest_client: GateioRestClient,
    auth: Option<GateioAuth>,
    rate_limiter: RateLimiter,
    circuit_breaker: CircuitBreaker,
    ws_connected: AtomicBool,
    ws_shutdown: Arc<RwLock<Option<broadcast::Sender<()>>>>,
    heartbeat: HeartbeatMonitor,
    request_id: AtomicU64,
}

impl GateioSpotAdapter {
    /// Creates a new Gate.io spot adapter with authentication
    pub fn new(api_key: String, api_secret: String) -> Self {
        let auth = GateioAuth::new(api_key, api_secret);
        Self {
            rest_client: GateioRestClient::new(Some(auth.clone())),
            auth: Some(auth),
            rate_limiter: RateLimiter::new(RateLimiterConfig {
                requests_per_window: 15,
                window_ms: 1000,
                block_on_limit: true,
                rate_limit_backoff_ms: 2000,
            }),
            circuit_breaker: CircuitBreaker::new("gateio-spot", CircuitBreakerConfig::default()),
            ws_connected: AtomicBool::new(false),
            ws_shutdown: Arc::new(RwLock::new(None)),
            heartbeat: HeartbeatMonitor::new(HeartbeatConfig::default()),
            request_id: AtomicU64::new(1),
        }
    }

    /// Creates a new Gate.io spot adapter without authentication (public endpoints only)
    pub fn public() -> Self {
        Self {
            rest_client: GateioRestClient::new(None),
            auth: None,
            rate_limiter: RateLimiter::new(RateLimiterConfig {
                requests_per_window: 15,
                window_ms: 1000,
                block_on_limit: true,
                rate_limit_backoff_ms: 2000,
            }),
            circuit_breaker: CircuitBreaker::new("gateio-spot-public", CircuitBreakerConfig::default()),
            ws_connected: AtomicBool::new(false),
            ws_shutdown: Arc::new(RwLock::new(None)),
            heartbeat: HeartbeatMonitor::new(HeartbeatConfig::default()),
            request_id: AtomicU64::new(1),
        }
    }

    fn next_request_id(&self) -> u64 {
        self.request_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Converts standard symbol to Gate.io format (BTC/USDT -> BTC_USDT)
    fn to_gateio_symbol(symbol: &str) -> String {
        symbol.replace("/", "_")
    }

    /// Converts Gate.io symbol to standard format (BTC_USDT -> BTC/USDT)
    fn to_standard_symbol(symbol: &str) -> String {
        symbol.replace("_", "/")
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    fn interval_to_str(interval: KlineInterval) -> &'static str {
        match interval {
            KlineInterval::M1 => "1m",
            KlineInterval::M5 => "5m",
            KlineInterval::M15 => "15m",
            KlineInterval::M30 => "30m",
            KlineInterval::H1 => "1h",
            KlineInterval::H4 => "4h",
            KlineInterval::D1 => "1d",
        }
    }
}

// =============================================================================
// REST API Types
// =============================================================================

#[derive(Debug, Serialize)]
struct PlaceOrderRequest {
    currency_pair: String,
    side: String,
    #[serde(rename = "type")]
    order_type: String,
    amount: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    price: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    time_in_force: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OrderResponse {
    id: String,
    text: String,
    currency_pair: String,
    side: String,
    #[serde(rename = "type")]
    order_type: String,
    amount: String,
    price: String,
    #[serde(default)]
    filled_amount: Option<String>,
    #[serde(default)]
    filled_total: Option<String>,
    #[serde(default)]
    avg_deal_price: Option<String>,
    status: String,
    create_time: String,
    update_time: String,
}

#[derive(Debug, Deserialize)]
struct AccountBalance {
    currency: String,
    available: String,
    locked: String,
}

#[derive(Debug, Deserialize)]
struct CurrencyPairInfo {
    id: String,
    base: String,
    quote: String,
    #[allow(dead_code)]
    fee: String,
    min_base_amount: Option<String>,
    min_quote_amount: Option<String>,
    amount_precision: i32,
    precision: i32,
    trade_status: String,
}

#[derive(Debug, Deserialize)]
struct TickerResponse {
    currency_pair: String,
    last: String,
    lowest_ask: String,
    highest_bid: String,
    change_percentage: String,
    base_volume: String,
    #[allow(dead_code)]
    quote_volume: String,
    high_24h: String,
    low_24h: String,
}

#[derive(Debug, Deserialize)]
struct KlineItem(Vec<String>);

// =============================================================================
// SpotRest Implementation
// =============================================================================

#[async_trait]
impl SpotRest for GateioSpotAdapter {
    async fn create_order(&self, order: NewOrder) -> Result<Order> {
        self.rate_limiter.acquire().await;
        let recv_ms = Self::now_ms();

        let request = PlaceOrderRequest {
            currency_pair: Self::to_gateio_symbol(&order.symbol),
            side: converters::to_gateio_side(order.side).to_string(),
            order_type: converters::to_gateio_order_type(order.ord_type).to_string(),
            amount: order.qty.to_string(),
            price: order.price.map(|p| p.to_string()),
            time_in_force: order.tif.map(|t| converters::to_gateio_tif(t).to_string()),
            text: if order.client_order_id.is_empty() {
                Some(format!("t-{}", uuid::Uuid::new_v4()))
            } else {
                Some(format!("t-{}", order.client_order_id))
            },
        };

        let response: OrderResponse = self
            .rest_client
            .post_private("/spot/orders", &request)
            .await?;

        Ok(convert_order_response(&response, recv_ms))
    }

    async fn cancel_order(&self, symbol: &str, order_id: &str) -> Result<bool> {
        self.rate_limiter.acquire().await;
        let gateio_symbol = Self::to_gateio_symbol(symbol);

        let mut params = HashMap::new();
        params.insert("currency_pair".to_string(), gateio_symbol);

        let endpoint = format!("/spot/orders/{}", order_id);
        let _response: OrderResponse = self
            .rest_client
            .delete_private(&endpoint, Some(params))
            .await?;

        Ok(true)
    }

    async fn cancel_all(&self, symbol: Option<&str>) -> Result<usize> {
        self.rate_limiter.acquire().await;

        let mut params = HashMap::new();
        if let Some(s) = symbol {
            params.insert("currency_pair".to_string(), Self::to_gateio_symbol(s));
        }

        let response: Vec<OrderResponse> = self
            .rest_client
            .delete_private("/spot/orders", Some(params))
            .await?;

        Ok(response.len())
    }

    async fn get_order(&self, symbol: &str, order_id: &str) -> Result<Order> {
        self.rate_limiter.acquire().await;
        let recv_ms = Self::now_ms();
        let gateio_symbol = Self::to_gateio_symbol(symbol);

        let mut params = HashMap::new();
        params.insert("currency_pair".to_string(), gateio_symbol);

        let endpoint = format!("/spot/orders/{}", order_id);
        let response: OrderResponse = self
            .rest_client
            .get_private(&endpoint, Some(params))
            .await?;

        Ok(convert_order_response(&response, recv_ms))
    }

    async fn get_open_orders(&self, symbol: Option<&str>) -> Result<Vec<Order>> {
        self.rate_limiter.acquire().await;
        let recv_ms = Self::now_ms();

        let mut params = HashMap::new();
        params.insert("status".to_string(), "open".to_string());
        if let Some(s) = symbol {
            params.insert("currency_pair".to_string(), Self::to_gateio_symbol(s));
        }

        let response: Vec<OrderResponse> = self
            .rest_client
            .get_private("/spot/orders", Some(params))
            .await?;

        Ok(response.iter().map(|r| convert_order_response(r, recv_ms)).collect())
    }

    async fn replace_order(
        &self,
        symbol: &str,
        order_id: &str,
        new_price: Option<Price>,
        new_qty: Option<Quantity>,
        new_tif: Option<TimeInForce>,
        _post_only: Option<bool>,
    ) -> Result<(Order, bool)> {
        // Gate.io doesn't support atomic replace, so cancel and recreate
        let old_order = self.get_order(symbol, order_id).await?;
        self.cancel_order(symbol, order_id).await?;

        let new_order = NewOrder {
            symbol: old_order.symbol.clone(),
            side: old_order.side,
            ord_type: old_order.ord_type,
            qty: new_qty.unwrap_or(old_order.qty),
            price: new_price.or(old_order.price),
            stop_price: old_order.stop_price,
            tif: new_tif.or(old_order.tif),
            post_only: false,
            reduce_only: false,
            client_order_id: old_order.client_order_id,
        };

        let order = self.create_order(new_order).await?;
        Ok((order, false)) // false = not atomic
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
        self.rate_limiter.acquire().await;

        let response: Vec<AccountBalance> = self
            .rest_client
            .get_private("/spot/accounts", None)
            .await?;

        Ok(response
            .iter()
            .map(|b| {
                let free: f64 = b.available.parse().unwrap_or(0.0);
                let locked: f64 = b.locked.parse().unwrap_or(0.0);
                Balance {
                    asset: b.currency.clone(),
                    free,
                    locked,
                    total: free + locked,
                }
            })
            .collect())
    }

    async fn get_account_info(&self) -> Result<AccountInfo> {
        let balances = self.get_balances().await?;
        Ok(AccountInfo {
            balances,
            can_trade: true,
            can_withdraw: true,
            can_deposit: true,
            update_ms: Self::now_ms(),
        })
    }

    async fn get_market_info(&self, symbol: &str) -> Result<MarketInfo> {
        self.rate_limiter.acquire().await;
        let gateio_symbol = Self::to_gateio_symbol(symbol);

        let endpoint = format!("/spot/currency_pairs/{}", gateio_symbol);
        let info: CurrencyPairInfo = self.rest_client.get_public(&endpoint, None).await?;

        let tick_size = 10f64.powi(-(info.precision as i32));
        let step_size = 10f64.powi(-(info.amount_precision as i32));

        Ok(MarketInfo {
            symbol: Self::to_standard_symbol(&info.id),
            base_asset: info.base,
            quote_asset: info.quote,
            status: if info.trade_status == "tradable" {
                MarketStatus::Trading
            } else {
                MarketStatus::Halt
            },
            min_qty: info.min_base_amount.as_deref().unwrap_or("0").parse().unwrap_or(0.0),
            max_qty: f64::MAX,
            step_size,
            tick_size,
            min_notional: info.min_quote_amount.as_deref().and_then(|s| s.parse().ok()).unwrap_or(0.0),
            max_leverage: None,
            is_spot: true,
            is_perp: false,
        })
    }

    async fn get_all_markets(&self) -> Result<Vec<MarketInfo>> {
        self.rate_limiter.acquire().await;

        let response: Vec<CurrencyPairInfo> = self
            .rest_client
            .get_public("/spot/currency_pairs", None)
            .await?;

        Ok(response
            .iter()
            .map(|info| {
                let tick_size = 10f64.powi(-(info.precision as i32));
                let step_size = 10f64.powi(-(info.amount_precision as i32));
                MarketInfo {
                    symbol: Self::to_standard_symbol(&info.id),
                    base_asset: info.base.clone(),
                    quote_asset: info.quote.clone(),
                    status: if info.trade_status == "tradable" {
                        MarketStatus::Trading
                    } else {
                        MarketStatus::Halt
                    },
                    min_qty: info.min_base_amount.as_deref().unwrap_or("0").parse().unwrap_or(0.0),
                    max_qty: f64::MAX,
                    step_size,
                    tick_size,
                    min_notional: info.min_quote_amount.as_deref().and_then(|s| s.parse().ok()).unwrap_or(0.0),
                    max_leverage: None,
                    is_spot: true,
                    is_perp: false,
                }
            })
            .collect())
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        self.rate_limiter.acquire().await;
        let gateio_symbol = Self::to_gateio_symbol(symbol);
        let ts_ms = Self::now_ms();

        let mut params = HashMap::new();
        params.insert("currency_pair".to_string(), gateio_symbol);

        let response: Vec<TickerResponse> = self
            .rest_client
            .get_public("/spot/tickers", Some(params))
            .await?;

        let ticker = response.first().context("No ticker data")?;

        let last_price: f64 = ticker.last.parse().unwrap_or(0.0);
        let change_pct: f64 = ticker.change_percentage.parse().unwrap_or(0.0);
        let open_price = if change_pct != 0.0 {
            last_price / (1.0 + change_pct / 100.0)
        } else {
            last_price
        };

        Ok(TickerInfo {
            symbol: Self::to_standard_symbol(&ticker.currency_pair),
            last_price,
            bid_price: ticker.highest_bid.parse().unwrap_or(0.0),
            ask_price: ticker.lowest_ask.parse().unwrap_or(0.0),
            volume_24h: ticker.base_volume.parse().unwrap_or(0.0),
            price_change_24h: last_price - open_price,
            price_change_pct_24h: change_pct,
            high_24h: ticker.high_24h.parse().unwrap_or(0.0),
            low_24h: ticker.low_24h.parse().unwrap_or(0.0),
            open_price_24h: open_price,
            ts_ms,
        })
    }

    async fn get_tickers(&self, symbols: Option<Vec<String>>) -> Result<Vec<TickerInfo>> {
        self.rate_limiter.acquire().await;
        let ts_ms = Self::now_ms();

        let response: Vec<TickerResponse> = self
            .rest_client
            .get_public("/spot/tickers", None)
            .await?;

        let result: Vec<TickerInfo> = response
            .iter()
            .filter(|t| {
                symbols.as_ref().map_or(true, |syms| {
                    syms.contains(&Self::to_standard_symbol(&t.currency_pair))
                })
            })
            .map(|ticker| {
                let last_price: f64 = ticker.last.parse().unwrap_or(0.0);
                let change_pct: f64 = ticker.change_percentage.parse().unwrap_or(0.0);
                let open_price = if change_pct != 0.0 {
                    last_price / (1.0 + change_pct / 100.0)
                } else {
                    last_price
                };

                TickerInfo {
                    symbol: Self::to_standard_symbol(&ticker.currency_pair),
                    last_price,
                    bid_price: ticker.highest_bid.parse().unwrap_or(0.0),
                    ask_price: ticker.lowest_ask.parse().unwrap_or(0.0),
                    volume_24h: ticker.base_volume.parse().unwrap_or(0.0),
                    price_change_24h: last_price - open_price,
                    price_change_pct_24h: change_pct,
                    high_24h: ticker.high_24h.parse().unwrap_or(0.0),
                    low_24h: ticker.low_24h.parse().unwrap_or(0.0),
                    open_price_24h: open_price,
                    ts_ms,
                }
            })
            .collect();

        Ok(result)
    }

    async fn get_klines(
        &self,
        symbol: &str,
        interval: KlineInterval,
        start_ms: Option<UnixMillis>,
        end_ms: Option<UnixMillis>,
        limit: Option<usize>,
    ) -> Result<Vec<Kline>> {
        self.rate_limiter.acquire().await;
        let gateio_symbol = Self::to_gateio_symbol(symbol);
        let std_symbol = Self::to_standard_symbol(&gateio_symbol);

        let mut params = HashMap::new();
        params.insert("currency_pair".to_string(), gateio_symbol);
        params.insert("interval".to_string(), Self::interval_to_str(interval).to_string());

        if let Some(start) = start_ms {
            params.insert("from".to_string(), (start / 1000).to_string());
        }
        if let Some(end) = end_ms {
            params.insert("to".to_string(), (end / 1000).to_string());
        }
        if let Some(l) = limit {
            params.insert("limit".to_string(), l.to_string());
        }

        let response: Vec<KlineItem> = self
            .rest_client
            .get_public("/spot/candlesticks", Some(params))
            .await?;

        Ok(response
            .iter()
            .filter_map(|k| {
                if k.0.len() >= 6 {
                    let open_ms = k.0[0].parse::<u64>().ok()? * 1000;
                    let volume: f64 = k.0[1].parse().ok()?;
                    let close: f64 = k.0[2].parse().ok()?;
                    let high: f64 = k.0[3].parse().ok()?;
                    let low: f64 = k.0[4].parse().ok()?;
                    let open: f64 = k.0[5].parse().ok()?;
                    let quote_volume = if k.0.len() > 6 {
                        k.0[6].parse().unwrap_or(0.0)
                    } else {
                        volume * close
                    };

                    Some(Kline {
                        symbol: std_symbol.clone(),
                        open_ms,
                        close_ms: open_ms + interval_to_ms(interval),
                        open,
                        high,
                        low,
                        close,
                        volume,
                        quote_volume,
                        trades: 0,
                    })
                } else {
                    None
                }
            })
            .collect())
    }
}

fn interval_to_ms(interval: KlineInterval) -> u64 {
    match interval {
        KlineInterval::M1 => 60_000,
        KlineInterval::M5 => 5 * 60_000,
        KlineInterval::M15 => 15 * 60_000,
        KlineInterval::M30 => 30 * 60_000,
        KlineInterval::H1 => 60 * 60_000,
        KlineInterval::H4 => 4 * 60 * 60_000,
        KlineInterval::D1 => 24 * 60 * 60_000,
    }
}

// =============================================================================
// SpotWs Implementation
// =============================================================================

#[async_trait]
impl SpotWs for GateioSpotAdapter {
    async fn subscribe_user(&self) -> Result<mpsc::Receiver<UserEvent>> {
        let (tx, rx) = mpsc::channel(1000);
        let auth = self.auth.clone().context("Authentication required")?;

        let shutdown = Arc::clone(&self.ws_shutdown);

        tokio::spawn(async move {
            let mut strategy = ReconnectStrategy::new(ReconnectConfig::production());

            'reconnect: loop {
                let ws_result = connect_async(GATEIO_WS_SPOT_URL).await;
                let ws_stream = match ws_result {
                    Ok((stream, _)) => stream,
                    Err(_) => {
                        if !strategy.can_retry() { break 'reconnect; }
                        let _ = strategy.wait_before_retry().await;
                        continue 'reconnect;
                    }
                };
                let (mut write, mut read) = ws_stream.split();

                // Authenticate
                let timestamp = GateioAuth::timestamp();
                let sign_str = format!("channel=spot.orders&event=subscribe&time={}", timestamp);
                let mut mac = hmac::Hmac::<sha2::Sha512>::new_from_slice(auth.api_secret.as_bytes())
                    .expect("HMAC can take key of any size");
                hmac::Mac::update(&mut mac, sign_str.as_bytes());
                let signature = hex::encode(hmac::Mac::finalize(mac).into_bytes());

                let auth_msg = serde_json::json!({
                    "time": timestamp,
                    "channel": "spot.orders",
                    "event": "subscribe",
                    "payload": ["*"],
                    "auth": {
                        "method": "api_key",
                        "KEY": auth.api_key,
                        "SIGN": signature
                    }
                });
                if write.send(Message::Text(auth_msg.to_string())).await.is_err() {
                    continue 'reconnect;
                }

                let (shutdown_tx, mut shutdown_rx) = broadcast::channel(1);
                {
                    let mut guard = shutdown.write().await;
                    *guard = Some(shutdown_tx);
                }

                strategy.reset();
                let mut ping_timer = tokio::time::interval(tokio::time::Duration::from_secs(30));

                'message_loop: loop {
                    tokio::select! {
                        _ = shutdown_rx.recv() => break 'reconnect,
                        _ = ping_timer.tick() => {
                            let ping = serde_json::json!({
                                "time": GateioAuth::timestamp(),
                                "channel": "spot.ping"
                            });
                            if write.send(Message::Text(ping.to_string())).await.is_err() {
                                break 'message_loop;
                            }
                        }
                        Some(msg) = read.next() => {
                            match msg {
                                Ok(Message::Text(text)) => {
                                    if let Ok(data) = serde_json::from_str::<serde_json::Value>(&text) {
                                        if let Some(event) = parse_user_event(&data) {
                                            if tx.send(event).await.is_err() { break 'reconnect; }
                                        }
                                    }
                                }
                                Ok(Message::Close(_)) | Err(_) => break 'message_loop,
                                _ => {}
                            }
                        }
                    }
                }
            }
        });

        self.ws_connected.store(true, Ordering::SeqCst);
        Ok(rx)
    }

    async fn subscribe_books(&self, symbols: &[&str]) -> Result<mpsc::Receiver<BookUpdate>> {
        let (tx, rx) = mpsc::channel(1000);

        let symbols: Vec<String> = symbols.iter().map(|s| Self::to_gateio_symbol(s)).collect();
        let shutdown = Arc::clone(&self.ws_shutdown);

        tokio::spawn(async move {
            let mut strategy = ReconnectStrategy::new(ReconnectConfig::production());
            let mut seq_counter: u64 = 0;

            'reconnect: loop {
                let ws_result = connect_async(GATEIO_WS_SPOT_URL).await;
                let ws_stream = match ws_result {
                    Ok((stream, _)) => stream,
                    Err(_) => {
                        if !strategy.can_retry() { break 'reconnect; }
                        let _ = strategy.wait_before_retry().await;
                        continue 'reconnect;
                    }
                };
                let (mut write, mut read) = ws_stream.split();

                // Subscribe to order book updates
                let sub_msg = serde_json::json!({
                    "time": GateioAuth::timestamp(),
                    "channel": "spot.order_book_update",
                    "event": "subscribe",
                    "payload": symbols.iter().map(|s| format!("{},100ms", s)).collect::<Vec<_>>()
                });
                if write.send(Message::Text(sub_msg.to_string())).await.is_err() {
                    continue 'reconnect;
                }

                let (shutdown_tx, mut shutdown_rx) = broadcast::channel(1);
                {
                    let mut guard = shutdown.write().await;
                    *guard = Some(shutdown_tx);
                }

                strategy.reset();
                let mut ping_timer = tokio::time::interval(tokio::time::Duration::from_secs(30));

                'message_loop: loop {
                    tokio::select! {
                        _ = shutdown_rx.recv() => break 'reconnect,
                        _ = ping_timer.tick() => {
                            let ping = serde_json::json!({
                                "time": GateioAuth::timestamp(),
                                "channel": "spot.ping"
                            });
                            if write.send(Message::Text(ping.to_string())).await.is_err() {
                                break 'message_loop;
                            }
                        }
                        Some(msg) = read.next() => {
                            match msg {
                                Ok(Message::Text(text)) => {
                                    if let Ok(data) = serde_json::from_str::<serde_json::Value>(&text) {
                                        if let Some(update) = parse_book_update(&data, &mut seq_counter) {
                                            if tx.send(update).await.is_err() { break 'reconnect; }
                                        }
                                    }
                                }
                                Ok(Message::Close(_)) | Err(_) => break 'message_loop,
                                _ => {}
                            }
                        }
                    }
                }
            }
        });

        self.ws_connected.store(true, Ordering::SeqCst);
        Ok(rx)
    }

    async fn subscribe_trades(&self, symbols: &[&str]) -> Result<mpsc::Receiver<TradeEvent>> {
        let (tx, rx) = mpsc::channel(1000);

        let symbols: Vec<String> = symbols.iter().map(|s| Self::to_gateio_symbol(s)).collect();
        let shutdown = Arc::clone(&self.ws_shutdown);

        tokio::spawn(async move {
            let mut strategy = ReconnectStrategy::new(ReconnectConfig::production());

            'reconnect: loop {
                let ws_result = connect_async(GATEIO_WS_SPOT_URL).await;
                let ws_stream = match ws_result {
                    Ok((stream, _)) => stream,
                    Err(_) => {
                        if !strategy.can_retry() { break 'reconnect; }
                        let _ = strategy.wait_before_retry().await;
                        continue 'reconnect;
                    }
                };
                let (mut write, mut read) = ws_stream.split();

                // Subscribe to trades
                let sub_msg = serde_json::json!({
                    "time": GateioAuth::timestamp(),
                    "channel": "spot.trades",
                    "event": "subscribe",
                    "payload": symbols
                });
                if write.send(Message::Text(sub_msg.to_string())).await.is_err() {
                    continue 'reconnect;
                }

                let (shutdown_tx, mut shutdown_rx) = broadcast::channel(1);
                {
                    let mut guard = shutdown.write().await;
                    *guard = Some(shutdown_tx);
                }

                strategy.reset();
                let mut ping_timer = tokio::time::interval(tokio::time::Duration::from_secs(30));

                'message_loop: loop {
                    tokio::select! {
                        _ = shutdown_rx.recv() => break 'reconnect,
                        _ = ping_timer.tick() => {
                            let ping = serde_json::json!({
                                "time": GateioAuth::timestamp(),
                                "channel": "spot.ping"
                            });
                            if write.send(Message::Text(ping.to_string())).await.is_err() {
                                break 'message_loop;
                            }
                        }
                        Some(msg) = read.next() => {
                            match msg {
                                Ok(Message::Text(text)) => {
                                    if let Ok(data) = serde_json::from_str::<serde_json::Value>(&text) {
                                        if let Some(trade) = parse_trade(&data) {
                                            if tx.send(trade).await.is_err() { break 'reconnect; }
                                        }
                                    }
                                }
                                Ok(Message::Close(_)) | Err(_) => break 'message_loop,
                                _ => {}
                            }
                        }
                    }
                }
            }
        });

        self.ws_connected.store(true, Ordering::SeqCst);
        Ok(rx)
    }

    async fn health(&self) -> Result<HealthStatus> {
        let is_connected = self.ws_connected.load(Ordering::SeqCst);
        let is_healthy = self.heartbeat.is_alive().await;

        Ok(HealthStatus {
            status: if is_connected && is_healthy {
                ConnectionStatus::Connected
            } else if is_connected {
                ConnectionStatus::Reconnecting
            } else {
                ConnectionStatus::Disconnected
            },
            last_ping_ms: None,
            last_pong_ms: None,
            latency_ms: None,
            reconnect_count: 0,
            error_msg: None,
        })
    }

    async fn reconnect(&self) -> Result<()> {
        if let Some(shutdown) = self.ws_shutdown.read().await.as_ref() {
            let _ = shutdown.send(());
        }
        self.ws_connected.store(false, Ordering::SeqCst);
        Ok(())
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

fn convert_order_response(resp: &OrderResponse, recv_ms: u64) -> Order {
    let qty: f64 = resp.amount.parse().unwrap_or(0.0);
    let filled_qty: f64 = resp.filled_amount.as_deref().unwrap_or("0").parse().unwrap_or(0.0);
    let remaining_qty = qty - filled_qty;

    let status = match resp.status.as_str() {
        "open" => {
            if filled_qty > 0.0 {
                OrderStatus::PartiallyFilled
            } else {
                OrderStatus::New
            }
        }
        "closed" => OrderStatus::Filled,
        "cancelled" => OrderStatus::Canceled,
        _ => OrderStatus::New,
    };

    let created_ms = resp.create_time.parse::<f64>().map(|t| (t * 1000.0) as u64).unwrap_or(0);
    let updated_ms = resp.update_time.parse::<f64>().map(|t| (t * 1000.0) as u64).unwrap_or(0);

    Order {
        venue_order_id: resp.id.clone(),
        client_order_id: resp.text.strip_prefix("t-").unwrap_or(&resp.text).to_string(),
        symbol: GateioSpotAdapter::to_standard_symbol(&resp.currency_pair),
        ord_type: converters::from_gateio_order_type(&resp.order_type),
        side: converters::from_gateio_side(&resp.side),
        qty,
        price: resp.price.parse().ok(),
        stop_price: None,
        tif: None,
        status,
        filled_qty,
        remaining_qty,
        created_ms,
        updated_ms,
        recv_ms,
        raw_status: Some(resp.status.clone()),
    }
}

fn parse_user_event(data: &serde_json::Value) -> Option<UserEvent> {
    let channel = data.get("channel")?.as_str()?;
    let event = data.get("event")?.as_str()?;
    let recv_ms = GateioSpotAdapter::now_ms();

    if channel == "spot.orders" && event == "update" {
        let result = data.get("result")?;

        let qty: f64 = result.get("amount").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0);
        let filled_qty: f64 = result.get("filled_amount").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0);
        let created_ms = result.get("create_time").and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok()).map(|t| (t * 1000.0) as u64).unwrap_or(0);
        let updated_ms = result.get("update_time").and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok()).map(|t| (t * 1000.0) as u64).unwrap_or(0);

        let order = Order {
            venue_order_id: result.get("id")?.as_str()?.to_string(),
            client_order_id: result
                .get("text")
                .and_then(|v| v.as_str())
                .map(|s| s.strip_prefix("t-").unwrap_or(s).to_string())
                .unwrap_or_default(),
            symbol: GateioSpotAdapter::to_standard_symbol(result.get("currency_pair")?.as_str()?),
            ord_type: converters::from_gateio_order_type(result.get("type")?.as_str()?),
            side: converters::from_gateio_side(result.get("side")?.as_str()?),
            qty,
            price: result.get("price").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
            stop_price: None,
            tif: None,
            status: converters::from_gateio_order_status(result.get("event")?.as_str()?),
            filled_qty,
            remaining_qty: qty - filled_qty,
            created_ms,
            updated_ms,
            recv_ms,
            raw_status: result.get("status").and_then(|v| v.as_str()).map(|s| s.to_string()),
        };
        Some(UserEvent::OrderUpdate(order))
    } else {
        None
    }
}

fn parse_book_update(data: &serde_json::Value, seq_counter: &mut u64) -> Option<BookUpdate> {
    let channel = data.get("channel")?.as_str()?;
    let event = data.get("event")?.as_str()?;
    let recv_ms = GateioSpotAdapter::now_ms();

    if channel == "spot.order_book_update" && event == "update" {
        let result = data.get("result")?;
        let symbol = result.get("s")?.as_str()?;

        let bids: Vec<(f64, f64)> = result
            .get("b")
            .and_then(|b| b.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| {
                        let a = v.as_array()?;
                        Some((
                            a.first()?.as_str()?.parse().ok()?,
                            a.get(1)?.as_str()?.parse().ok()?,
                        ))
                    })
                    .collect()
            })
            .unwrap_or_default();

        let asks: Vec<(f64, f64)> = result
            .get("a")
            .and_then(|a| a.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| {
                        let a = v.as_array()?;
                        Some((
                            a.first()?.as_str()?.parse().ok()?,
                            a.get(1)?.as_str()?.parse().ok()?,
                        ))
                    })
                    .collect()
            })
            .unwrap_or_default();

        let prev_seq = *seq_counter;
        *seq_counter += 1;
        let seq = *seq_counter;

        let ex_ts_ms = result.get("t").and_then(|v| v.as_u64()).unwrap_or(recv_ms);

        Some(BookUpdate::DepthDelta {
            symbol: GateioSpotAdapter::to_standard_symbol(symbol),
            bids,
            asks,
            seq,
            prev_seq,
            checksum: None,
            ex_ts_ms,
            recv_ms,
        })
    } else {
        None
    }
}

fn parse_trade(data: &serde_json::Value) -> Option<TradeEvent> {
    let channel = data.get("channel")?.as_str()?;
    let event = data.get("event")?.as_str()?;
    let recv_ms = GateioSpotAdapter::now_ms();

    if channel == "spot.trades" && event == "update" {
        let result = data.get("result")?;

        let ex_ts_ms = result
            .get("create_time")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .map(|t| (t * 1000.0) as u64)
            .unwrap_or(recv_ms);

        Some(TradeEvent {
            symbol: GateioSpotAdapter::to_standard_symbol(result.get("currency_pair")?.as_str()?),
            px: result
                .get("price")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0),
            qty: result
                .get("amount")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0),
            taker_is_buy: result
                .get("side")
                .and_then(|v| v.as_str())
                .map(|s| s == "buy")
                .unwrap_or(false),
            ex_ts_ms,
            recv_ms,
        })
    } else {
        None
    }
}
