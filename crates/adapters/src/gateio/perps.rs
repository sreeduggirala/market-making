//! Gate.io Perpetual Futures Trading Adapter
//!
//! Implements PerpRest and PerpWs traits for Gate.io futures trading.

use super::account::{converters, GateioAuth, GateioRestClient, GATEIO_WS_FUTURES_URL};
use crate::traits::*;
use crate::utils::{
    CircuitBreaker, CircuitBreakerConfig, HeartbeatConfig, HeartbeatMonitor, RateLimiter,
    RateLimiterConfig, ReconnectConfig, ReconnectStrategy,
};
use anyhow::{Context, Result};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use hmac::Mac;
use rust_decimal::Decimal;
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

/// Gate.io perpetual futures trading adapter
pub struct GateioPerpsAdapter {
    rest_client: GateioRestClient,
    auth: Option<GateioAuth>,
    rate_limiter: RateLimiter,
    circuit_breaker: CircuitBreaker,
    ws_connected: AtomicBool,
    ws_shutdown: Arc<RwLock<Option<broadcast::Sender<()>>>>,
    heartbeat: HeartbeatMonitor,
    request_id: AtomicU64,
    settle: String, // Settlement currency: usdt or btc
}

impl GateioPerpsAdapter {
    /// Creates a new Gate.io perpetual futures adapter with authentication (USDT-settled)
    pub fn new(api_key: String, api_secret: String) -> Self {
        Self::with_settle(api_key, api_secret, "usdt".to_string())
    }

    /// Creates a new Gate.io perpetual futures adapter with custom settle currency
    pub fn with_settle(api_key: String, api_secret: String, settle: String) -> Self {
        let auth = GateioAuth::new(api_key, api_secret);
        Self {
            rest_client: GateioRestClient::new(Some(auth.clone())),
            auth: Some(auth),
            rate_limiter: RateLimiter::new(RateLimiterConfig {
                requests_per_window: 5,
                window_ms: 1000,
                block_on_limit: true,
                rate_limit_backoff_ms: 2000,
            }),
            circuit_breaker: CircuitBreaker::new("gateio-perps", CircuitBreakerConfig::default()),
            ws_connected: AtomicBool::new(false),
            ws_shutdown: Arc::new(RwLock::new(None)),
            heartbeat: HeartbeatMonitor::new(HeartbeatConfig::default()),
            request_id: AtomicU64::new(1),
            settle,
        }
    }

    /// Creates a new Gate.io perpetual futures adapter without authentication (public endpoints only)
    pub fn public() -> Self {
        Self {
            rest_client: GateioRestClient::new(None),
            auth: None,
            rate_limiter: RateLimiter::new(RateLimiterConfig {
                requests_per_window: 5,
                window_ms: 1000,
                block_on_limit: true,
                rate_limit_backoff_ms: 2000,
            }),
            circuit_breaker: CircuitBreaker::new("gateio-perps-public", CircuitBreakerConfig::default()),
            ws_connected: AtomicBool::new(false),
            ws_shutdown: Arc::new(RwLock::new(None)),
            heartbeat: HeartbeatMonitor::new(HeartbeatConfig::default()),
            request_id: AtomicU64::new(1),
            settle: "usdt".to_string(),
        }
    }

    fn next_request_id(&self) -> u64 {
        self.request_id.fetch_add(1, Ordering::SeqCst)
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    /// Converts standard symbol to Gate.io futures format (BTC -> BTC_USDT)
    fn to_gateio_symbol(&self, symbol: &str) -> String {
        let base = symbol
            .replace("/", "")
            .replace("-", "")
            .replace("USDT", "")
            .to_uppercase();
        format!("{}_{}", base, self.settle.to_uppercase())
    }

    /// Converts Gate.io futures symbol to standard format (BTC_USDT -> BTC)
    fn to_standard_symbol(symbol: &str) -> String {
        symbol
            .split('_')
            .next()
            .unwrap_or(symbol)
            .to_uppercase()
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
// REST API Types
// =============================================================================

#[derive(Debug, Serialize)]
struct PlaceOrderRequest {
    contract: String,
    size: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    price: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tif: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reduce_only: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct OrderResponse {
    id: i64,
    #[serde(default)]
    text: Option<String>,
    contract: String,
    size: i64,
    #[serde(default)]
    left: i64,
    price: String,
    #[serde(default)]
    fill_price: Option<String>,
    status: String,
    create_time: f64,
    finish_time: Option<f64>,
    #[serde(default)]
    is_reduce_only: bool,
    #[serde(default)]
    is_close: bool,
}

#[derive(Debug, Deserialize)]
struct GateioAccountInfo {
    total: String,
    #[allow(dead_code)]
    unrealised_pnl: String,
    #[allow(dead_code)]
    position_margin: String,
    #[allow(dead_code)]
    order_margin: String,
    available: String,
    currency: String,
}

#[derive(Debug, Deserialize)]
struct PositionInfo {
    contract: String,
    size: i64,
    leverage: String,
    #[allow(dead_code)]
    risk_limit: String,
    #[allow(dead_code)]
    leverage_max: String,
    #[allow(dead_code)]
    maintenance_rate: String,
    #[allow(dead_code)]
    value: String,
    margin: String,
    entry_price: String,
    liq_price: String,
    mark_price: String,
    unrealised_pnl: String,
    realised_pnl: String,
    mode: String,
    #[allow(dead_code)]
    cross_leverage_limit: String,
    update_time: f64,
}

#[derive(Debug, Deserialize)]
struct ContractInfo {
    name: String,
    #[serde(rename = "type")]
    #[allow(dead_code)]
    contract_type: String,
    #[allow(dead_code)]
    quanto_multiplier: String,
    #[allow(dead_code)]
    leverage_min: String,
    leverage_max: String,
    #[allow(dead_code)]
    maintenance_rate: String,
    #[allow(dead_code)]
    mark_type: String,
    mark_price: String,
    index_price: String,
    last_price: String,
    #[allow(dead_code)]
    maker_fee_rate: String,
    #[allow(dead_code)]
    taker_fee_rate: String,
    order_price_round: String,
    #[allow(dead_code)]
    mark_price_round: String,
    funding_rate: String,
    #[allow(dead_code)]
    funding_interval: i64,
    funding_next_apply: f64,
    #[allow(dead_code)]
    risk_limit_base: String,
    #[allow(dead_code)]
    risk_limit_step: String,
    #[allow(dead_code)]
    risk_limit_max: String,
    order_size_min: i64,
    order_size_max: i64,
    #[allow(dead_code)]
    order_price_deviate: String,
    #[allow(dead_code)]
    ref_discount_rate: String,
    #[allow(dead_code)]
    ref_rebate_rate: String,
    #[allow(dead_code)]
    orderbook_id: i64,
    #[allow(dead_code)]
    trade_id: i64,
    #[allow(dead_code)]
    trade_size: i64,
    #[allow(dead_code)]
    position_size: i64,
    #[allow(dead_code)]
    config_change_time: f64,
    in_delisting: bool,
    #[allow(dead_code)]
    orders_limit: i64,
}

#[derive(Debug, Deserialize)]
struct TickerResponse {
    contract: String,
    last: String,
    low_24h: String,
    high_24h: String,
    change_percentage: String,
    #[allow(dead_code)]
    total_size: String,
    volume_24h: String,
    #[allow(dead_code)]
    volume_24h_btc: String,
    #[allow(dead_code)]
    volume_24h_usd: String,
    #[allow(dead_code)]
    volume_24h_base: String,
    #[allow(dead_code)]
    volume_24h_quote: String,
    #[allow(dead_code)]
    volume_24h_settle: String,
    #[allow(dead_code)]
    mark_price: String,
    #[allow(dead_code)]
    funding_rate: String,
    #[allow(dead_code)]
    funding_rate_indicative: String,
    #[allow(dead_code)]
    index_price: String,
    #[allow(dead_code)]
    quanto_base_rate: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FundingRateItem {
    t: f64,
    r: String,
}

#[derive(Debug, Deserialize)]
struct KlineItem(Vec<serde_json::Value>);

// =============================================================================
// PerpRest Implementation
// =============================================================================

#[async_trait]
impl PerpRest for GateioPerpsAdapter {
    async fn create_order(&self, order: NewOrder) -> Result<Order> {
        self.rate_limiter.acquire().await;
        let recv_ms = Self::now_ms();

        // Gate.io uses signed size for direction
        let size = if order.side == Side::Buy {
            order.qty as i64
        } else {
            -(order.qty as i64)
        };

        let request = PlaceOrderRequest {
            contract: self.to_gateio_symbol(&order.symbol),
            size,
            price: order.price.map(|p| p.to_string()),
            tif: order.tif.map(|t| converters::to_gateio_tif(t).to_string()),
            text: if order.client_order_id.is_empty() {
                Some(format!("t-{}", uuid::Uuid::new_v4()))
            } else {
                Some(format!("t-{}", order.client_order_id))
            },
            reduce_only: if order.reduce_only { Some(true) } else { None },
        };

        let endpoint = format!("/futures/{}/orders", self.settle);
        let response: OrderResponse = self
            .rest_client
            .post_private(&endpoint, &request)
            .await?;

        Ok(convert_order_response(&response, recv_ms))
    }

    async fn cancel_order(&self, symbol: &str, order_id: &str) -> Result<bool> {
        self.rate_limiter.acquire().await;
        let _symbol = self.to_gateio_symbol(symbol);

        let endpoint = format!("/futures/{}/orders/{}", self.settle, order_id);
        let _response: OrderResponse = self
            .rest_client
            .delete_private(&endpoint, None)
            .await?;

        Ok(true)
    }

    async fn cancel_all(&self, symbol: Option<&str>) -> Result<usize> {
        self.rate_limiter.acquire().await;

        let mut params = HashMap::new();
        if let Some(s) = symbol {
            params.insert("contract".to_string(), self.to_gateio_symbol(s));
        }

        let endpoint = format!("/futures/{}/orders", self.settle);
        let response: Vec<OrderResponse> = self
            .rest_client
            .delete_private(&endpoint, Some(params))
            .await?;

        Ok(response.len())
    }

    async fn get_order(&self, _symbol: &str, order_id: &str) -> Result<Order> {
        self.rate_limiter.acquire().await;
        let recv_ms = Self::now_ms();

        let endpoint = format!("/futures/{}/orders/{}", self.settle, order_id);
        let response: OrderResponse = self
            .rest_client
            .get_private(&endpoint, None)
            .await?;

        Ok(convert_order_response(&response, recv_ms))
    }

    async fn get_open_orders(&self, symbol: Option<&str>) -> Result<Vec<Order>> {
        self.rate_limiter.acquire().await;
        let recv_ms = Self::now_ms();

        let mut params = HashMap::new();
        params.insert("status".to_string(), "open".to_string());
        if let Some(s) = symbol {
            params.insert("contract".to_string(), self.to_gateio_symbol(s));
        }

        let endpoint = format!("/futures/{}/orders", self.settle);
        let response: Vec<OrderResponse> = self
            .rest_client
            .get_private(&endpoint, Some(params))
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
        reduce_only: Option<bool>,
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
            reduce_only: reduce_only.unwrap_or(false),
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

    async fn set_leverage(&self, symbol: &str, leverage: Decimal) -> Result<()> {
        self.rate_limiter.acquire().await;
        let gateio_symbol = self.to_gateio_symbol(symbol);

        #[derive(Serialize)]
        struct LeverageRequest {
            leverage: String,
        }

        let request = LeverageRequest {
            leverage: leverage.to_string(),
        };

        let endpoint = format!(
            "/futures/{}/positions/{}/leverage",
            self.settle, gateio_symbol
        );
        let _response: serde_json::Value = self
            .rest_client
            .post_private(&endpoint, &request)
            .await?;

        Ok(())
    }

    async fn set_margin_mode(&self, _symbol: &str, mode: MarginMode) -> Result<()> {
        self.rate_limiter.acquire().await;

        #[derive(Serialize)]
        struct MarginRequest {
            dual_mode: bool,
        }

        let request = MarginRequest {
            dual_mode: mode == MarginMode::Isolated,
        };

        let endpoint = format!("/futures/{}/dual_mode", self.settle);
        let _response: serde_json::Value = self
            .rest_client
            .post_private(&endpoint, &request)
            .await?;

        Ok(())
    }

    async fn get_position(&self, symbol: &str) -> Result<Position> {
        self.rate_limiter.acquire().await;
        let gateio_symbol = self.to_gateio_symbol(symbol);

        let endpoint = format!("/futures/{}/positions/{}", self.settle, gateio_symbol);
        let response: PositionInfo = self
            .rest_client
            .get_private(&endpoint, None)
            .await?;

        Ok(convert_position(&response))
    }

    async fn get_all_positions(&self) -> Result<Vec<Position>> {
        self.rate_limiter.acquire().await;

        let endpoint = format!("/futures/{}/positions", self.settle);
        let response: Vec<PositionInfo> = self
            .rest_client
            .get_private(&endpoint, None)
            .await?;

        Ok(response
            .iter()
            .filter(|p| p.size != 0)
            .map(convert_position)
            .collect())
    }

    async fn get_funding_rate(&self, symbol: &str) -> Result<(Decimal, UnixMillis)> {
        self.rate_limiter.acquire().await;
        let gateio_symbol = self.to_gateio_symbol(symbol);

        let endpoint = format!("/futures/{}/contracts/{}", self.settle, gateio_symbol);
        let response: ContractInfo = self
            .rest_client
            .get_public(&endpoint, None)
            .await?;

        Ok((
            response.funding_rate.parse().unwrap_or_default(),
            (response.funding_next_apply * 1000.0) as u64,
        ))
    }

    async fn get_balances(&self) -> Result<Vec<Balance>> {
        self.rate_limiter.acquire().await;

        let endpoint = format!("/futures/{}/accounts", self.settle);
        let response: GateioAccountInfo = self
            .rest_client
            .get_private(&endpoint, None)
            .await?;

        let total: f64 = response.total.parse().unwrap_or(0.0);
        let available: f64 = response.available.parse().unwrap_or(0.0);

        Ok(vec![Balance {
            asset: response.currency,
            free: available,
            locked: total - available,
            total,
        }])
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
        let gateio_symbol = self.to_gateio_symbol(symbol);

        let endpoint = format!("/futures/{}/contracts/{}", self.settle, gateio_symbol);
        let info: ContractInfo = self.rest_client.get_public(&endpoint, None).await?;

        let tick_size: f64 = info.order_price_round.parse().unwrap_or(0.01);
        let max_leverage: u32 = info.leverage_max.parse().unwrap_or(100);

        Ok(MarketInfo {
            symbol: Self::to_standard_symbol(&info.name),
            base_asset: Self::to_standard_symbol(&info.name),
            quote_asset: self.settle.to_uppercase(),
            status: if info.in_delisting {
                MarketStatus::Delisted
            } else {
                MarketStatus::Trading
            },
            min_qty: info.order_size_min as f64,
            max_qty: info.order_size_max as f64,
            step_size: 1.0, // Contract-based
            tick_size,
            min_notional: 0.0,
            max_leverage: Some(max_leverage),
            is_spot: false,
            is_perp: true,
        })
    }

    async fn get_all_markets(&self) -> Result<Vec<MarketInfo>> {
        self.rate_limiter.acquire().await;

        let endpoint = format!("/futures/{}/contracts", self.settle);
        let response: Vec<ContractInfo> = self
            .rest_client
            .get_public(&endpoint, None)
            .await?;

        Ok(response
            .iter()
            .map(|info| {
                let tick_size: f64 = info.order_price_round.parse().unwrap_or(0.01);
                let max_leverage: u32 = info.leverage_max.parse().unwrap_or(100);
                MarketInfo {
                    symbol: Self::to_standard_symbol(&info.name),
                    base_asset: Self::to_standard_symbol(&info.name),
                    quote_asset: self.settle.to_uppercase(),
                    status: if info.in_delisting {
                        MarketStatus::Delisted
                    } else {
                        MarketStatus::Trading
                    },
                    min_qty: info.order_size_min as f64,
                    max_qty: info.order_size_max as f64,
                    step_size: 1.0,
                    tick_size,
                    min_notional: 0.0,
                    max_leverage: Some(max_leverage),
                    is_spot: false,
                    is_perp: true,
                }
            })
            .collect())
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        self.rate_limiter.acquire().await;
        let gateio_symbol = self.to_gateio_symbol(symbol);
        let ts_ms = Self::now_ms();

        let mut params = HashMap::new();
        params.insert("contract".to_string(), gateio_symbol);

        let endpoint = format!("/futures/{}/tickers", self.settle);
        let response: Vec<TickerResponse> = self
            .rest_client
            .get_public(&endpoint, Some(params))
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
            symbol: Self::to_standard_symbol(&ticker.contract),
            last_price,
            bid_price: 0.0, // Not provided in ticker endpoint
            ask_price: 0.0,
            volume_24h: ticker.volume_24h.parse().unwrap_or(0.0),
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

        let endpoint = format!("/futures/{}/tickers", self.settle);
        let response: Vec<TickerResponse> = self
            .rest_client
            .get_public(&endpoint, None)
            .await?;

        let result: Vec<TickerInfo> = response
            .iter()
            .filter(|t| {
                symbols.as_ref().map_or(true, |syms| {
                    syms.contains(&Self::to_standard_symbol(&t.contract))
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
                    symbol: Self::to_standard_symbol(&ticker.contract),
                    last_price,
                    bid_price: 0.0,
                    ask_price: 0.0,
                    volume_24h: ticker.volume_24h.parse().unwrap_or(0.0),
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

    async fn get_mark_price(&self, symbol: &str) -> Result<(Price, UnixMillis)> {
        self.rate_limiter.acquire().await;
        let gateio_symbol = self.to_gateio_symbol(symbol);

        let endpoint = format!("/futures/{}/contracts/{}", self.settle, gateio_symbol);
        let response: ContractInfo = self
            .rest_client
            .get_public(&endpoint, None)
            .await?;

        Ok((
            response.mark_price.parse().unwrap_or(0.0),
            Self::now_ms(),
        ))
    }

    async fn get_index_price(&self, symbol: &str) -> Result<(Price, UnixMillis)> {
        self.rate_limiter.acquire().await;
        let gateio_symbol = self.to_gateio_symbol(symbol);

        let endpoint = format!("/futures/{}/contracts/{}", self.settle, gateio_symbol);
        let response: ContractInfo = self
            .rest_client
            .get_public(&endpoint, None)
            .await?;

        Ok((
            response.index_price.parse().unwrap_or(0.0),
            Self::now_ms(),
        ))
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
        let gateio_symbol = self.to_gateio_symbol(symbol);
        let std_symbol = Self::to_standard_symbol(&gateio_symbol);

        let mut params = HashMap::new();
        params.insert("contract".to_string(), gateio_symbol);
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

        let endpoint = format!("/futures/{}/candlesticks", self.settle);
        let response: Vec<KlineItem> = self
            .rest_client
            .get_public(&endpoint, Some(params))
            .await?;

        Ok(response
            .iter()
            .filter_map(|k| {
                if k.0.len() >= 6 {
                    let open_ms = k.0[0].as_f64()? as u64 * 1000;
                    let volume: f64 = k.0[1].as_f64()?;
                    let close: f64 = k.0[2].as_str()?.parse().ok()?;
                    let high: f64 = k.0[3].as_str()?.parse().ok()?;
                    let low: f64 = k.0[4].as_str()?.parse().ok()?;
                    let open: f64 = k.0[5].as_str()?.parse().ok()?;

                    Some(Kline {
                        symbol: std_symbol.clone(),
                        open_ms,
                        close_ms: open_ms + interval_to_ms(interval),
                        open,
                        high,
                        low,
                        close,
                        volume,
                        quote_volume: volume * close,
                        trades: 0,
                    })
                } else {
                    None
                }
            })
            .collect())
    }

    async fn get_funding_history(
        &self,
        symbol: &str,
        _start_ms: Option<UnixMillis>,
        _end_ms: Option<UnixMillis>,
        limit: Option<usize>,
    ) -> Result<Vec<FundingRateHistory>> {
        self.rate_limiter.acquire().await;
        let gateio_symbol = self.to_gateio_symbol(symbol);
        let std_symbol = Self::to_standard_symbol(&gateio_symbol);

        let mut params = HashMap::new();
        params.insert("contract".to_string(), gateio_symbol);
        if let Some(l) = limit {
            params.insert("limit".to_string(), l.to_string());
        }

        let endpoint = format!("/futures/{}/funding_rate", self.settle);
        let response: Vec<FundingRateItem> = self
            .rest_client
            .get_public(&endpoint, Some(params))
            .await?;

        Ok(response
            .iter()
            .map(|f| FundingRateHistory {
                symbol: std_symbol.clone(),
                rate: f.r.parse().unwrap_or_default(),
                ts_ms: (f.t * 1000.0) as u64,
            })
            .collect())
    }
}

// =============================================================================
// PerpWs Implementation
// =============================================================================

#[async_trait]
impl PerpWs for GateioPerpsAdapter {
    async fn subscribe_user(&self) -> Result<mpsc::Receiver<UserEvent>> {
        let (tx, rx) = mpsc::channel(1000);
        let auth = self.auth.clone().context("Authentication required")?;

        let shutdown = Arc::clone(&self.ws_shutdown);

        tokio::spawn(async move {
            let mut strategy = ReconnectStrategy::new(ReconnectConfig::production());

            'reconnect: loop {
                let ws_result = connect_async(GATEIO_WS_FUTURES_URL).await;
                let ws_stream = match ws_result {
                    Ok((stream, _)) => stream,
                    Err(_) => {
                        if !strategy.can_retry() { break 'reconnect; }
                        let _ = strategy.wait_before_retry().await;
                        continue 'reconnect;
                    }
                };
                let (mut write, mut read) = ws_stream.split();

                // Authenticate and subscribe
                let timestamp = GateioAuth::timestamp();
                let sign_str = format!("channel=futures.orders&event=subscribe&time={}", timestamp);
                let mut mac = hmac::Hmac::<sha2::Sha512>::new_from_slice(auth.api_secret.as_bytes())
                    .expect("HMAC can take key of any size");
                hmac::Mac::update(&mut mac, sign_str.as_bytes());
                let signature = hex::encode(hmac::Mac::finalize(mac).into_bytes());

                let auth_msg = serde_json::json!({
                    "time": timestamp,
                    "channel": "futures.orders",
                    "event": "subscribe",
                    "payload": ["!all"],
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
                                "channel": "futures.ping"
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

        let symbols: Vec<String> = symbols.iter().map(|s| self.to_gateio_symbol(s)).collect();
        let shutdown = Arc::clone(&self.ws_shutdown);

        tokio::spawn(async move {
            let mut strategy = ReconnectStrategy::new(ReconnectConfig::production());
            let mut seq_counter: u64 = 0;

            'reconnect: loop {
                let ws_result = connect_async(GATEIO_WS_FUTURES_URL).await;
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
                    "channel": "futures.order_book_update",
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
                                "channel": "futures.ping"
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

        let symbols: Vec<String> = symbols.iter().map(|s| self.to_gateio_symbol(s)).collect();
        let shutdown = Arc::clone(&self.ws_shutdown);

        tokio::spawn(async move {
            let mut strategy = ReconnectStrategy::new(ReconnectConfig::production());

            'reconnect: loop {
                let ws_result = connect_async(GATEIO_WS_FUTURES_URL).await;
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
                    "channel": "futures.trades",
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
                                "channel": "futures.ping"
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
    let total_size = resp.size.abs();
    let filled_size = total_size - resp.left.abs();
    let remaining_size = resp.left.abs();

    let status = match resp.status.as_str() {
        "open" => {
            if filled_size > 0 {
                OrderStatus::PartiallyFilled
            } else {
                OrderStatus::New
            }
        }
        "finished" => OrderStatus::Filled,
        "cancelled" => OrderStatus::Canceled,
        _ => OrderStatus::New,
    };

    let side = if resp.size > 0 { Side::Buy } else { Side::Sell };

    let created_ms = (resp.create_time * 1000.0) as u64;
    let updated_ms = resp.finish_time.map(|t| (t * 1000.0) as u64).unwrap_or(created_ms);

    Order {
        venue_order_id: resp.id.to_string(),
        client_order_id: resp
            .text
            .as_deref()
            .map(|s| s.strip_prefix("t-").unwrap_or(s).to_string())
            .unwrap_or_default(),
        symbol: GateioPerpsAdapter::to_standard_symbol(&resp.contract),
        ord_type: OrderType::Limit, // Gate.io futures default
        side,
        qty: total_size as f64,
        price: resp.price.parse().ok(),
        stop_price: None,
        tif: None,
        status,
        filled_qty: filled_size as f64,
        remaining_qty: remaining_size as f64,
        created_ms,
        updated_ms,
        recv_ms,
        raw_status: Some(resp.status.clone()),
    }
}

fn convert_position(pos: &PositionInfo) -> Position {
    let updated_ms = (pos.update_time * 1000.0) as u64;

    Position {
        exchange: Some("gateio".to_string()),
        symbol: GateioPerpsAdapter::to_standard_symbol(&pos.contract),
        qty: pos.size as f64, // Signed: >0 long, <0 short
        entry_px: pos.entry_price.parse().unwrap_or(0.0),
        mark_px: pos.mark_price.parse().ok(),
        liquidation_px: {
            let liq: f64 = pos.liq_price.parse().unwrap_or(0.0);
            if liq > 0.0 { Some(liq) } else { None }
        },
        unrealized_pnl: pos.unrealised_pnl.parse().ok(),
        realized_pnl: pos.realised_pnl.parse().ok(),
        margin: pos.margin.parse().ok(),
        leverage: pos.leverage.parse().ok(),
        opened_ms: None,
        updated_ms,
    }
}

fn parse_user_event(data: &serde_json::Value) -> Option<UserEvent> {
    let channel = data.get("channel")?.as_str()?;
    let event = data.get("event")?.as_str()?;
    let recv_ms = GateioPerpsAdapter::now_ms();

    if channel == "futures.orders" && event == "update" {
        let result = data.get("result")?.as_array()?.first()?;

        let size = result.get("size")?.as_i64()?;
        let left = result.get("left").and_then(|v| v.as_i64()).unwrap_or(0);
        let total_size = size.abs();
        let filled_size = total_size - left.abs();
        let side = if size > 0 { Side::Buy } else { Side::Sell };

        let created_ms = result.get("create_time").and_then(|v| v.as_f64()).map(|t| (t * 1000.0) as u64).unwrap_or(0);
        let updated_ms = result.get("finish_time").and_then(|v| v.as_f64()).map(|t| (t * 1000.0) as u64).unwrap_or(created_ms);

        let order = Order {
            venue_order_id: result.get("id")?.as_i64()?.to_string(),
            client_order_id: result
                .get("text")
                .and_then(|v| v.as_str())
                .map(|s| s.strip_prefix("t-").unwrap_or(s).to_string())
                .unwrap_or_default(),
            symbol: GateioPerpsAdapter::to_standard_symbol(result.get("contract")?.as_str()?),
            ord_type: OrderType::Limit,
            side,
            qty: total_size as f64,
            price: result.get("price").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
            stop_price: None,
            tif: None,
            status: converters::from_gateio_order_status(result.get("status")?.as_str()?),
            filled_qty: filled_size as f64,
            remaining_qty: left.abs() as f64,
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
    let recv_ms = GateioPerpsAdapter::now_ms();

    if channel == "futures.order_book_update" && event == "update" {
        let result = data.get("result")?;
        let symbol = result.get("c")?.as_str()?;

        let bids: Vec<(f64, f64)> = result
            .get("b")
            .and_then(|b| b.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| {
                        let item = v.as_object()?;
                        Some((
                            item.get("p")?.as_str()?.parse().ok()?,
                            item.get("s")?.as_i64()?.abs() as f64,
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
                        let item = v.as_object()?;
                        Some((
                            item.get("p")?.as_str()?.parse().ok()?,
                            item.get("s")?.as_i64()?.abs() as f64,
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
            symbol: GateioPerpsAdapter::to_standard_symbol(symbol),
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
    let recv_ms = GateioPerpsAdapter::now_ms();

    if channel == "futures.trades" && event == "update" {
        let result = data.get("result")?.as_array()?.first()?;

        let size = result.get("size")?.as_i64()?;
        let ex_ts_ms = result.get("create_time").and_then(|v| v.as_f64()).map(|t| (t * 1000.0) as u64).unwrap_or(recv_ms);

        Some(TradeEvent {
            symbol: GateioPerpsAdapter::to_standard_symbol(result.get("contract")?.as_str()?),
            px: result.get("price").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
            qty: size.abs() as f64,
            taker_is_buy: size > 0,
            ex_ts_ms,
            recv_ms,
        })
    } else {
        None
    }
}
