//! KuCoin Spot Trading Adapter
//!
//! Implements SpotRest and SpotWs traits for KuCoin spot trading.

use super::account::{converters, KucoinAuth, KucoinResponse, KucoinRestClient};
use crate::traits::*;
use crate::utils::{CircuitBreaker, CircuitBreakerConfig, RateLimiter, RateLimiterConfig};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{debug, warn};

fn parse_f64_or_warn(s: &str, field_name: &str) -> f64 {
    s.parse::<f64>().unwrap_or_else(|e| {
        warn!("Failed to parse {} '{}': {}", field_name, s, e);
        0.0
    })
}

fn safe_now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// KuCoin spot trading adapter
#[derive(Clone)]
pub struct KucoinSpotAdapter {
    client: KucoinRestClient,
    auth: Option<KucoinAuth>,
    connection_status: Arc<RwLock<ConnectionStatus>>,
    rate_limiter: RateLimiter,
    circuit_breaker: CircuitBreaker,
    reconnect_count: Arc<AtomicU32>,
    shutdown_tx: Arc<Mutex<Option<tokio::sync::broadcast::Sender<()>>>>,
}

impl KucoinSpotAdapter {
    pub fn new(api_key: String, api_secret: String, passphrase: String) -> Self {
        let auth = KucoinAuth::new(api_key, api_secret, passphrase);
        Self {
            client: KucoinRestClient::spot(Some(auth.clone())),
            auth: Some(auth),
            connection_status: Arc::new(RwLock::new(ConnectionStatus::Disconnected)),
            rate_limiter: RateLimiter::new(RateLimiterConfig::default()),
            circuit_breaker: CircuitBreaker::new("kucoin_spot", CircuitBreakerConfig::production()),
            reconnect_count: Arc::new(AtomicU32::new(0)),
            shutdown_tx: Arc::new(Mutex::new(None)),
        }
    }

    pub fn public() -> Self {
        Self {
            client: KucoinRestClient::spot(None),
            auth: None,
            connection_status: Arc::new(RwLock::new(ConnectionStatus::Disconnected)),
            rate_limiter: RateLimiter::new(RateLimiterConfig::default()),
            circuit_breaker: CircuitBreaker::new("kucoin_spot", CircuitBreakerConfig::production()),
            reconnect_count: Arc::new(AtomicU32::new(0)),
            shutdown_tx: Arc::new(Mutex::new(None)),
        }
    }

    fn generate_client_id() -> String {
        format!("mm_{}", KucoinAuth::timestamp_ms())
    }

    fn now_millis() -> u64 {
        safe_now_millis()
    }

    async fn call_api<T, F, Fut>(&self, endpoint: &str, f: F) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        if !self.rate_limiter.acquire().await {
            anyhow::bail!("Rate limit reached for endpoint: {}", endpoint);
        }
        debug!("Calling KuCoin Spot API endpoint: {}", endpoint);
        self.circuit_breaker.call(f).await
    }

    fn to_kucoin_symbol(symbol: &str) -> String {
        symbol.replace("/", "-")
    }

    fn to_standard_symbol(symbol: &str) -> String {
        symbol.replace("-", "/")
    }
}

// REST API Types
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PlaceOrderRequest {
    client_oid: String,
    side: String,
    symbol: String,
    #[serde(rename = "type")]
    order_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    price: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    size: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    time_in_force: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlaceOrderResponse {
    order_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderDetail {
    id: String,
    symbol: String,
    side: String,
    #[serde(rename = "type")]
    order_type: String,
    price: String,
    size: String,
    deal_size: String,
    is_active: bool,
    #[serde(default)]
    cancel_exist: bool,
    created_at: u64,
    #[serde(default)]
    client_oid: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrdersResponse {
    items: Vec<OrderDetail>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AccountBalance {
    currency: String,
    #[serde(rename = "type")]
    account_type: String,
    balance: String,
    available: String,
    holds: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SymbolInfo {
    symbol: String,
    base_currency: String,
    quote_currency: String,
    base_min_size: String,
    base_max_size: String,
    base_increment: String,
    price_increment: String,
    enable_trading: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TickerData {
    symbol: String,
    #[serde(default)]
    best_ask: Option<String>,
    #[serde(default)]
    best_bid: Option<String>,
    price: String,
    time: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AllTickersResponse {
    time: u64,
    ticker: Vec<TickerItem>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TickerItem {
    symbol: String,
    #[serde(default)]
    buy: Option<String>,
    #[serde(default)]
    sell: Option<String>,
    last: String,
    vol: String,
    #[serde(rename = "changeRate", default)]
    change_rate: Option<String>,
    #[serde(default)]
    high: Option<String>,
    #[serde(default)]
    low: Option<String>,
    #[serde(rename = "averagePrice", default)]
    average_price: Option<String>,
}

// SpotRest Implementation
#[async_trait::async_trait]
impl SpotRest for KucoinSpotAdapter {
    async fn create_order(&self, new: NewOrder) -> Result<Order> {
        let client_id = if new.client_order_id.is_empty() {
            Self::generate_client_id()
        } else {
            new.client_order_id.clone()
        };

        let request = PlaceOrderRequest {
            client_oid: client_id.clone(),
            side: converters::to_kucoin_side(new.side).to_string(),
            symbol: Self::to_kucoin_symbol(&new.symbol),
            order_type: converters::to_kucoin_order_type(new.ord_type).to_string(),
            price: new.price.map(|p| p.to_string()),
            size: Some(new.qty.to_string()),
            time_in_force: new.tif.map(|t| converters::to_kucoin_tif(t).to_string()),
        };

        let response: KucoinResponse<PlaceOrderResponse> = self
            .call_api("/api/v1/orders", || async {
                self.client.post_private("/api/v1/orders", &request).await
            })
            .await?;

        let data = response.into_result()?;
        let now = Self::now_millis();

        Ok(Order {
            venue_order_id: data.order_id,
            client_order_id: client_id,
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
            raw_status: Some("active".to_string()),
        })
    }

    async fn cancel_order(&self, _symbol: &str, venue_order_id: &str) -> Result<bool> {
        let endpoint = format!("/api/v1/orders/{}", venue_order_id);
        let response: KucoinResponse<serde_json::Value> = self
            .call_api(&endpoint, || async {
                self.client.delete_private(&endpoint, None).await
            })
            .await?;

        Ok(response.is_ok())
    }

    async fn cancel_all(&self, symbol: Option<&str>) -> Result<usize> {
        let mut params = HashMap::new();
        if let Some(s) = symbol {
            params.insert("symbol".to_string(), Self::to_kucoin_symbol(s));
        }

        let response: KucoinResponse<serde_json::Value> = self
            .call_api("/api/v1/orders", || async {
                self.client.delete_private("/api/v1/orders", Some(params.clone())).await
            })
            .await?;

        let data = response.into_result()?;
        Ok(data.get("cancelledOrderIds").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0))
    }

    async fn get_order(&self, _symbol: &str, venue_order_id: &str) -> Result<Order> {
        let endpoint = format!("/api/v1/orders/{}", venue_order_id);
        let response: KucoinResponse<OrderDetail> = self
            .call_api(&endpoint, || async {
                self.client.get_private(&endpoint, None).await
            })
            .await?;

        let order = response.into_result()?;
        let now = Self::now_millis();
        let qty = parse_f64_or_warn(&order.size, "size");
        let filled = parse_f64_or_warn(&order.deal_size, "deal_size");

        let status = if order.cancel_exist {
            OrderStatus::Canceled
        } else if filled >= qty && qty > 0.0 {
            OrderStatus::Filled
        } else if filled > 0.0 {
            OrderStatus::PartiallyFilled
        } else if order.is_active {
            OrderStatus::New
        } else {
            OrderStatus::Filled
        };

        Ok(Order {
            venue_order_id: order.id,
            client_order_id: order.client_oid.unwrap_or_default(),
            symbol: Self::to_standard_symbol(&order.symbol),
            ord_type: converters::from_kucoin_order_type(&order.order_type),
            side: converters::from_kucoin_side(&order.side),
            qty,
            price: order.price.parse().ok(),
            stop_price: None,
            tif: None,
            status,
            filled_qty: filled,
            remaining_qty: qty - filled,
            created_ms: order.created_at,
            updated_ms: order.created_at,
            recv_ms: now,
            raw_status: None,
        })
    }

    async fn get_open_orders(&self, symbol: Option<&str>) -> Result<Vec<Order>> {
        let mut params = HashMap::new();
        params.insert("status".to_string(), "active".to_string());
        if let Some(s) = symbol {
            params.insert("symbol".to_string(), Self::to_kucoin_symbol(s));
        }

        let response: KucoinResponse<OrdersResponse> = self
            .call_api("/api/v1/orders", || async {
                self.client.get_private("/api/v1/orders", Some(params.clone())).await
            })
            .await?;

        let data = response.into_result()?;
        let now = Self::now_millis();

        Ok(data.items.iter().map(|order| {
            let qty = parse_f64_or_warn(&order.size, "size");
            let filled = parse_f64_or_warn(&order.deal_size, "deal_size");
            let status = if order.cancel_exist {
                OrderStatus::Canceled
            } else if filled >= qty && qty > 0.0 {
                OrderStatus::Filled
            } else if filled > 0.0 {
                OrderStatus::PartiallyFilled
            } else if order.is_active {
                OrderStatus::New
            } else {
                OrderStatus::Filled
            };

            Order {
                venue_order_id: order.id.clone(),
                client_order_id: order.client_oid.clone().unwrap_or_default(),
                symbol: Self::to_standard_symbol(&order.symbol),
                ord_type: converters::from_kucoin_order_type(&order.order_type),
                side: converters::from_kucoin_side(&order.side),
                qty,
                price: order.price.parse().ok(),
                stop_price: None,
                tif: None,
                status,
                filled_qty: filled,
                remaining_qty: qty - filled,
                created_ms: order.created_at,
                updated_ms: order.created_at,
                recv_ms: now,
                raw_status: None,
            }
        }).collect())
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
        // KuCoin doesn't support order amendment, so cancel and recreate
        self.cancel_order(symbol, venue_order_id).await?;

        let old_order = self.get_order(symbol, venue_order_id).await.ok();
        let order = self.create_order(NewOrder {
            symbol: symbol.to_string(),
            side: old_order.as_ref().map(|o| o.side).unwrap_or(Side::Buy),
            ord_type: old_order.as_ref().map(|o| o.ord_type).unwrap_or(OrderType::Limit),
            qty: new_qty.unwrap_or(old_order.as_ref().map(|o| o.qty).unwrap_or(0.0)),
            price: new_price.or(old_order.as_ref().and_then(|o| o.price)),
            stop_price: None,
            tif: new_tif.or(old_order.as_ref().and_then(|o| o.tif)),
            post_only: post_only.unwrap_or(false),
            reduce_only: false,
            client_order_id: Self::generate_client_id(),
        }).await?;

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
        let mut params = HashMap::new();
        params.insert("type".to_string(), "trade".to_string());

        let response: KucoinResponse<Vec<AccountBalance>> = self
            .call_api("/api/v1/accounts", || async {
                self.client.get_private("/api/v1/accounts", Some(params.clone())).await
            })
            .await?;

        let data = response.into_result()?;
        Ok(data.iter().map(|b| {
            let free = parse_f64_or_warn(&b.available, "available");
            let locked = parse_f64_or_warn(&b.holds, "holds");
            Balance {
                asset: b.currency.clone(),
                free,
                locked,
                total: free + locked,
            }
        }).collect())
    }

    async fn get_account_info(&self) -> Result<AccountInfo> {
        let balances = self.get_balances().await?;
        Ok(AccountInfo {
            balances,
            can_trade: true,
            can_withdraw: true,
            can_deposit: true,
            update_ms: Self::now_millis(),
        })
    }

    async fn get_market_info(&self, symbol: &str) -> Result<MarketInfo> {
        let kucoin_symbol = Self::to_kucoin_symbol(symbol);
        let response: KucoinResponse<Vec<SymbolInfo>> = self
            .call_api("/api/v2/symbols", || async {
                self.client.get_public("/api/v2/symbols", None).await
            })
            .await?;

        let data = response.into_result()?;
        let info = data.iter().find(|s| s.symbol == kucoin_symbol).context("Symbol not found")?;

        Ok(MarketInfo {
            symbol: Self::to_standard_symbol(&info.symbol),
            base_asset: info.base_currency.clone(),
            quote_asset: info.quote_currency.clone(),
            status: if info.enable_trading { MarketStatus::Trading } else { MarketStatus::Halt },
            min_qty: parse_f64_or_warn(&info.base_min_size, "base_min_size"),
            max_qty: parse_f64_or_warn(&info.base_max_size, "base_max_size"),
            step_size: parse_f64_or_warn(&info.base_increment, "base_increment"),
            tick_size: parse_f64_or_warn(&info.price_increment, "price_increment"),
            min_notional: 0.0,
            max_leverage: None,
            is_spot: true,
            is_perp: false,
        })
    }

    async fn get_all_markets(&self) -> Result<Vec<MarketInfo>> {
        let response: KucoinResponse<Vec<SymbolInfo>> = self
            .call_api("/api/v2/symbols", || async {
                self.client.get_public("/api/v2/symbols", None).await
            })
            .await?;

        let data = response.into_result()?;
        Ok(data.iter().map(|info| MarketInfo {
            symbol: Self::to_standard_symbol(&info.symbol),
            base_asset: info.base_currency.clone(),
            quote_asset: info.quote_currency.clone(),
            status: if info.enable_trading { MarketStatus::Trading } else { MarketStatus::Halt },
            min_qty: parse_f64_or_warn(&info.base_min_size, "base_min_size"),
            max_qty: parse_f64_or_warn(&info.base_max_size, "base_max_size"),
            step_size: parse_f64_or_warn(&info.base_increment, "base_increment"),
            tick_size: parse_f64_or_warn(&info.price_increment, "price_increment"),
            min_notional: 0.0,
            max_leverage: None,
            is_spot: true,
            is_perp: false,
        }).collect())
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        let kucoin_symbol = Self::to_kucoin_symbol(symbol);
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), kucoin_symbol);

        let response: KucoinResponse<TickerData> = self
            .call_api("/api/v1/market/orderbook/level1", || async {
                self.client.get_public("/api/v1/market/orderbook/level1", Some(params.clone())).await
            })
            .await?;

        let data = response.into_result()?;
        let last = parse_f64_or_warn(&data.price, "price");

        Ok(TickerInfo {
            symbol: Self::to_standard_symbol(&data.symbol),
            last_price: last,
            bid_price: data.best_bid.as_deref().map(|s| parse_f64_or_warn(s, "best_bid")).unwrap_or(0.0),
            ask_price: data.best_ask.as_deref().map(|s| parse_f64_or_warn(s, "best_ask")).unwrap_or(0.0),
            volume_24h: 0.0,
            price_change_24h: 0.0,
            price_change_pct_24h: 0.0,
            high_24h: 0.0,
            low_24h: 0.0,
            open_price_24h: 0.0,
            ts_ms: data.time,
        })
    }

    async fn get_tickers(&self, symbols: Option<Vec<String>>) -> Result<Vec<TickerInfo>> {
        let response: KucoinResponse<AllTickersResponse> = self
            .call_api("/api/v1/market/allTickers", || async {
                self.client.get_public("/api/v1/market/allTickers", None).await
            })
            .await?;

        let data = response.into_result()?;
        let ts = data.time;

        let tickers: Vec<TickerInfo> = data.ticker.iter().map(|t| {
            let last = parse_f64_or_warn(&t.last, "last");
            let change_pct = t.change_rate.as_deref().map(|s| parse_f64_or_warn(s, "change_rate") * 100.0).unwrap_or(0.0);

            TickerInfo {
                symbol: Self::to_standard_symbol(&t.symbol),
                last_price: last,
                bid_price: t.buy.as_deref().map(|s| parse_f64_or_warn(s, "buy")).unwrap_or(0.0),
                ask_price: t.sell.as_deref().map(|s| parse_f64_or_warn(s, "sell")).unwrap_or(0.0),
                volume_24h: parse_f64_or_warn(&t.vol, "vol"),
                price_change_24h: 0.0,
                price_change_pct_24h: change_pct,
                high_24h: t.high.as_deref().map(|s| parse_f64_or_warn(s, "high")).unwrap_or(0.0),
                low_24h: t.low.as_deref().map(|s| parse_f64_or_warn(s, "low")).unwrap_or(0.0),
                open_price_24h: t.average_price.as_deref().map(|s| parse_f64_or_warn(s, "avg")).unwrap_or(0.0),
                ts_ms: ts,
            }
        }).collect();

        if let Some(filter) = symbols {
            Ok(tickers.into_iter().filter(|t| filter.contains(&t.symbol)).collect())
        } else {
            Ok(tickers)
        }
    }

    async fn get_klines(
        &self,
        symbol: &str,
        interval: KlineInterval,
        _start_ms: Option<UnixMillis>,
        _end_ms: Option<UnixMillis>,
        limit: Option<usize>,
    ) -> Result<Vec<Kline>> {
        let kucoin_interval = match interval {
            KlineInterval::M1 => "1min",
            KlineInterval::M5 => "5min",
            KlineInterval::M15 => "15min",
            KlineInterval::M30 => "30min",
            KlineInterval::H1 => "1hour",
            KlineInterval::H4 => "4hour",
            KlineInterval::D1 => "1day",
        };

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), Self::to_kucoin_symbol(symbol));
        params.insert("type".to_string(), kucoin_interval.to_string());

        let response: KucoinResponse<Vec<Vec<String>>> = self
            .call_api("/api/v1/market/candles", || async {
                self.client.get_public("/api/v1/market/candles", Some(params.clone())).await
            })
            .await?;

        let data = response.into_result()?;
        let symbol_str = symbol.to_string();
        let limit = limit.unwrap_or(500);

        Ok(data.iter().take(limit).filter_map(|k| {
            if k.len() >= 7 {
                let open_ms = k[0].parse::<u64>().ok()? * 1000;
                Some(Kline {
                    symbol: symbol_str.clone(),
                    open_ms,
                    close_ms: open_ms + interval_to_ms(interval),
                    open: k[1].parse().ok()?,
                    close: k[2].parse().ok()?,
                    high: k[3].parse().ok()?,
                    low: k[4].parse().ok()?,
                    volume: k[5].parse().ok()?,
                    quote_volume: k[6].parse().ok()?,
                    trades: 0,
                })
            } else {
                None
            }
        }).collect())
    }
}

fn interval_to_ms(interval: KlineInterval) -> u64 {
    match interval {
        KlineInterval::M1 => 60_000,
        KlineInterval::M5 => 300_000,
        KlineInterval::M15 => 900_000,
        KlineInterval::M30 => 1_800_000,
        KlineInterval::H1 => 3_600_000,
        KlineInterval::H4 => 14_400_000,
        KlineInterval::D1 => 86_400_000,
    }
}

// SpotWs Implementation
#[async_trait::async_trait]
impl SpotWs for KucoinSpotAdapter {
    async fn subscribe_user(&self) -> Result<mpsc::Receiver<UserEvent>> {
        let (_tx, rx) = mpsc::channel(1000);
        warn!("KuCoin spot user subscription not fully implemented");
        Ok(rx)
    }

    async fn subscribe_books(&self, _symbols: &[&str]) -> Result<mpsc::Receiver<BookUpdate>> {
        let (_tx, rx) = mpsc::channel(1000);
        warn!("KuCoin spot book subscription not fully implemented");
        Ok(rx)
    }

    async fn subscribe_trades(&self, _symbols: &[&str]) -> Result<mpsc::Receiver<TradeEvent>> {
        let (_tx, rx) = mpsc::channel(1000);
        warn!("KuCoin spot trade subscription not fully implemented");
        Ok(rx)
    }

    async fn health(&self) -> Result<HealthStatus> {
        let status = self.connection_status.read().await.clone();
        Ok(HealthStatus {
            status,
            last_ping_ms: None,
            last_pong_ms: None,
            latency_ms: None,
            reconnect_count: self.reconnect_count.load(Ordering::SeqCst),
            error_msg: None,
        })
    }

    async fn reconnect(&self) -> Result<()> {
        *self.connection_status.write().await = ConnectionStatus::Reconnecting;
        self.reconnect_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
