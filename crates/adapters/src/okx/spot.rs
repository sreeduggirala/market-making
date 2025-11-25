//! OKX Spot Market Adapter
//!
//! Provides REST and WebSocket functionality for OKX spot trading.

use crate::okx::account::{converters, OkxAuth, OkxRestClient, OkxResponse, OKX_WS_PUBLIC_URL, OKX_WS_PRIVATE_URL};
use crate::traits::*;
use crate::utils::{
    CircuitBreaker, CircuitBreakerConfig, HeartbeatConfig, HeartbeatMonitor,
    RateLimiter, RateLimiterConfig, ReconnectConfig, ReconnectStrategy,
};
use anyhow::{Context, Result};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{debug, error, info, warn};

/// OKX Spot adapter combining REST and WebSocket
#[derive(Clone)]
pub struct OkxSpotAdapter {
    client: OkxRestClient,
    auth: Option<OkxAuth>,
    connection_status: Arc<RwLock<ConnectionStatus>>,
    rate_limiter: RateLimiter,
    circuit_breaker: CircuitBreaker,
    reconnect_count: Arc<AtomicU32>,
    shutdown_tx: Arc<Mutex<Option<tokio::sync::broadcast::Sender<()>>>>,
}

impl OkxSpotAdapter {
    /// Creates a new OKX spot adapter with authentication
    pub fn new(api_key: String, api_secret: String, passphrase: String) -> Self {
        let auth = OkxAuth::new(api_key, api_secret, passphrase);
        Self {
            client: OkxRestClient::new(Some(auth.clone())),
            auth: Some(auth),
            connection_status: Arc::new(RwLock::new(ConnectionStatus::Disconnected)),
            rate_limiter: RateLimiter::new(RateLimiterConfig::default()),
            circuit_breaker: CircuitBreaker::new("okx_spot", CircuitBreakerConfig::production()),
            reconnect_count: Arc::new(AtomicU32::new(0)),
            shutdown_tx: Arc::new(Mutex::new(None)),
        }
    }

    /// Creates a new OKX spot adapter without authentication
    pub fn new_public() -> Self {
        Self {
            client: OkxRestClient::new(None),
            auth: None,
            connection_status: Arc::new(RwLock::new(ConnectionStatus::Disconnected)),
            rate_limiter: RateLimiter::new(RateLimiterConfig::default()),
            circuit_breaker: CircuitBreaker::new("okx_spot", CircuitBreakerConfig::production()),
            reconnect_count: Arc::new(AtomicU32::new(0)),
            shutdown_tx: Arc::new(Mutex::new(None)),
        }
    }

    fn now_millis() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    fn generate_client_id() -> String {
        format!("cc_{}", uuid::Uuid::new_v4().to_string().replace("-", "")[..16].to_string())
    }

    async fn call_api<T, F, Fut>(&self, endpoint: &str, f: F) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        if !self.rate_limiter.acquire().await {
            anyhow::bail!("Rate limit reached for endpoint: {}", endpoint);
        }
        debug!("Calling OKX Spot API: {}", endpoint);
        match self.circuit_breaker.call(f).await {
            Ok(result) => Ok(result),
            Err(e) => {
                if e.to_string().contains("50011") {
                    self.rate_limiter.handle_rate_limit_error().await;
                }
                Err(e)
            }
        }
    }

    pub async fn shutdown(&self) {
        info!("Shutting down OKX Spot adapter");
        if let Some(tx) = self.shutdown_tx.lock().await.take() {
            let _ = tx.send(());
        }
    }
}

#[async_trait::async_trait]
impl SpotRest for OkxSpotAdapter {
    async fn create_order(&self, new: NewOrder) -> Result<Order> {
        #[derive(Serialize)]
        struct OrderRequest {
            #[serde(rename = "instId")]
            inst_id: String,
            #[serde(rename = "tdMode")]
            td_mode: String,
            side: String,
            #[serde(rename = "ordType")]
            ord_type: String,
            sz: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            px: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            #[serde(rename = "clOrdId")]
            cl_ord_id: Option<String>,
        }

        let cl_ord_id = if new.client_order_id.is_empty() {
            Self::generate_client_id()
        } else {
            new.client_order_id.clone()
        };

        let request = OrderRequest {
            inst_id: new.symbol.clone(),
            td_mode: "cash".to_string(),
            side: converters::to_okx_side(new.side).to_string(),
            ord_type: converters::to_okx_order_type(new.ord_type).to_string(),
            sz: new.qty.to_string(),
            px: new.price.map(|p| p.to_string()),
            cl_ord_id: Some(cl_ord_id.clone()),
        };

        #[derive(serde::Deserialize)]
        struct OrderResult {
            #[serde(rename = "ordId")]
            ord_id: String,
            #[serde(rename = "clOrdId")]
            cl_ord_id: String,
            #[serde(rename = "sCode")]
            s_code: String,
            #[serde(rename = "sMsg")]
            s_msg: String,
        }

        let response: OkxResponse<Vec<OrderResult>> = self
            .call_api("/api/v5/trade/order", || async {
                self.client.post_private("/api/v5/trade/order", &request).await
            })
            .await?;

        let results = response.into_result()?;
        let result = results.first().context("No order result")?;

        if result.s_code != "0" {
            anyhow::bail!("Order failed: {}", result.s_msg);
        }

        let now = Self::now_millis();
        Ok(Order {
            venue_order_id: result.ord_id.clone(),
            client_order_id: result.cl_ord_id.clone(),
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
            raw_status: Some("live".to_string()),
        })
    }

    async fn cancel_order(&self, symbol: &str, venue_order_id: &str) -> Result<bool> {
        #[derive(Serialize)]
        struct CancelRequest {
            #[serde(rename = "instId")]
            inst_id: String,
            #[serde(rename = "ordId")]
            ord_id: String,
        }

        let request = CancelRequest {
            inst_id: symbol.to_string(),
            ord_id: venue_order_id.to_string(),
        };

        let response: OkxResponse<Vec<serde_json::Value>> = self
            .call_api("/api/v5/trade/cancel-order", || async {
                self.client.post_private("/api/v5/trade/cancel-order", &request).await
            })
            .await?;

        Ok(response.is_ok())
    }

    async fn cancel_all(&self, symbol: Option<&str>) -> Result<usize> {
        if let Some(sym) = symbol {
            #[derive(Serialize)]
            struct CancelAllRequest {
                #[serde(rename = "instId")]
                inst_id: String,
            }

            let request = CancelAllRequest {
                inst_id: sym.to_string(),
            };

            let _response: OkxResponse<Vec<serde_json::Value>> = self
                .call_api("/api/v5/trade/cancel-batch-orders", || async {
                    self.client.post_private("/api/v5/trade/cancel-batch-orders", &request).await
                })
                .await?;
        }
        Ok(1)
    }

    async fn get_order(&self, symbol: &str, venue_order_id: &str) -> Result<Order> {
        let mut params = HashMap::new();
        params.insert("instId".to_string(), symbol.to_string());
        params.insert("ordId".to_string(), venue_order_id.to_string());

        #[derive(serde::Deserialize)]
        struct OrderDetail {
            #[serde(rename = "instId")]
            inst_id: String,
            #[serde(rename = "ordId")]
            ord_id: String,
            #[serde(rename = "clOrdId")]
            cl_ord_id: String,
            side: String,
            #[serde(rename = "ordType")]
            ord_type: String,
            sz: String,
            px: String,
            #[serde(rename = "fillSz")]
            fill_sz: String,
            state: String,
            #[serde(rename = "cTime")]
            c_time: String,
            #[serde(rename = "uTime")]
            u_time: String,
        }

        let response: OkxResponse<Vec<OrderDetail>> = self
            .call_api("/api/v5/trade/order", || async {
                self.client.get_private("/api/v5/trade/order", Some(params.clone())).await
            })
            .await?;

        let orders = response.into_result()?;
        let order = orders.first().context("Order not found")?;
        let now = Self::now_millis();

        let qty: f64 = order.sz.parse().unwrap_or(0.0);
        let filled: f64 = order.fill_sz.parse().unwrap_or(0.0);

        Ok(Order {
            venue_order_id: order.ord_id.clone(),
            client_order_id: order.cl_ord_id.clone(),
            symbol: order.inst_id.clone(),
            ord_type: converters::from_okx_order_type(&order.ord_type),
            side: converters::from_okx_side(&order.side),
            qty,
            price: order.px.parse().ok(),
            stop_price: None,
            tif: None,
            status: converters::from_okx_order_status(&order.state),
            filled_qty: filled,
            remaining_qty: qty - filled,
            created_ms: order.c_time.parse().unwrap_or(now),
            updated_ms: order.u_time.parse().unwrap_or(now),
            recv_ms: now,
            raw_status: Some(order.state.clone()),
        })
    }

    async fn get_open_orders(&self, symbol: Option<&str>) -> Result<Vec<Order>> {
        let mut params = HashMap::new();
        params.insert("instType".to_string(), "SPOT".to_string());
        if let Some(s) = symbol {
            params.insert("instId".to_string(), s.to_string());
        }

        #[derive(serde::Deserialize)]
        struct OrderDetail {
            #[serde(rename = "instId")]
            inst_id: String,
            #[serde(rename = "ordId")]
            ord_id: String,
            #[serde(rename = "clOrdId")]
            cl_ord_id: String,
            side: String,
            #[serde(rename = "ordType")]
            ord_type: String,
            sz: String,
            px: String,
            #[serde(rename = "fillSz")]
            fill_sz: String,
            state: String,
            #[serde(rename = "cTime")]
            c_time: String,
            #[serde(rename = "uTime")]
            u_time: String,
        }

        let response: OkxResponse<Vec<OrderDetail>> = self
            .call_api("/api/v5/trade/orders-pending", || async {
                self.client.get_private("/api/v5/trade/orders-pending", Some(params.clone())).await
            })
            .await?;

        let orders = response.into_result()?;
        let now = Self::now_millis();

        Ok(orders
            .into_iter()
            .map(|order| {
                let qty: f64 = order.sz.parse().unwrap_or(0.0);
                let filled: f64 = order.fill_sz.parse().unwrap_or(0.0);
                Order {
                    venue_order_id: order.ord_id,
                    client_order_id: order.cl_ord_id,
                    symbol: order.inst_id,
                    ord_type: converters::from_okx_order_type(&order.ord_type),
                    side: converters::from_okx_side(&order.side),
                    qty,
                    price: order.px.parse().ok(),
                    stop_price: None,
                    tif: None,
                    status: converters::from_okx_order_status(&order.state),
                    filled_qty: filled,
                    remaining_qty: qty - filled,
                    created_ms: order.c_time.parse().unwrap_or(now),
                    updated_ms: order.u_time.parse().unwrap_or(now),
                    recv_ms: now,
                    raw_status: Some(order.state),
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
        _new_tif: Option<TimeInForce>,
        _post_only: Option<bool>,
    ) -> Result<(Order, bool)> {
        #[derive(Serialize)]
        struct AmendRequest {
            #[serde(rename = "instId")]
            inst_id: String,
            #[serde(rename = "ordId")]
            ord_id: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            #[serde(rename = "newSz")]
            new_sz: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            #[serde(rename = "newPx")]
            new_px: Option<String>,
        }

        let request = AmendRequest {
            inst_id: symbol.to_string(),
            ord_id: venue_order_id.to_string(),
            new_sz: new_qty.map(|q| q.to_string()),
            new_px: new_price.map(|p| p.to_string()),
        };

        let _response: OkxResponse<Vec<serde_json::Value>> = self
            .call_api("/api/v5/trade/amend-order", || async {
                self.client.post_private("/api/v5/trade/amend-order", &request).await
            })
            .await?;

        let order = self.get_order(symbol, venue_order_id).await?;
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
        #[derive(serde::Deserialize)]
        struct BalanceData {
            details: Vec<BalanceDetail>,
        }

        #[derive(serde::Deserialize)]
        struct BalanceDetail {
            ccy: String,
            #[serde(rename = "availBal")]
            avail_bal: String,
            #[serde(rename = "frozenBal")]
            frozen_bal: String,
            #[serde(rename = "bal")]
            bal: String,
        }

        let response: OkxResponse<Vec<BalanceData>> = self
            .call_api("/api/v5/account/balance", || async {
                self.client.get_private("/api/v5/account/balance", None).await
            })
            .await?;

        let data = response.into_result()?;
        let balances = data
            .first()
            .map(|d| {
                d.details
                    .iter()
                    .map(|b| {
                        let free: f64 = b.avail_bal.parse().unwrap_or(0.0);
                        let locked: f64 = b.frozen_bal.parse().unwrap_or(0.0);
                        let total: f64 = b.bal.parse().unwrap_or(0.0);
                        Balance {
                            asset: b.ccy.clone(),
                            free,
                            locked,
                            total,
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();

        Ok(balances)
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
        let mut params = HashMap::new();
        params.insert("instType".to_string(), "SPOT".to_string());
        params.insert("instId".to_string(), symbol.to_string());

        #[derive(serde::Deserialize)]
        struct InstrumentInfo {
            #[serde(rename = "instId")]
            inst_id: String,
            #[serde(rename = "baseCcy")]
            base_ccy: String,
            #[serde(rename = "quoteCcy")]
            quote_ccy: String,
            #[serde(rename = "minSz")]
            min_sz: String,
            #[serde(rename = "maxLmtSz")]
            max_lmt_sz: String,
            #[serde(rename = "tickSz")]
            tick_sz: String,
            #[serde(rename = "lotSz")]
            lot_sz: String,
            state: String,
        }

        let response: OkxResponse<Vec<InstrumentInfo>> = self
            .call_api("/api/v5/public/instruments", || async {
                self.client.get_public("/api/v5/public/instruments", Some(params.clone())).await
            })
            .await?;

        let instruments = response.into_result()?;
        let inst = instruments.first().context("Instrument not found")?;

        Ok(MarketInfo {
            symbol: inst.inst_id.clone(),
            base_asset: inst.base_ccy.clone(),
            quote_asset: inst.quote_ccy.clone(),
            status: if inst.state == "live" { MarketStatus::Trading } else { MarketStatus::Halt },
            min_qty: inst.min_sz.parse().unwrap_or(0.0),
            max_qty: inst.max_lmt_sz.parse().unwrap_or(f64::MAX),
            step_size: inst.lot_sz.parse().unwrap_or(0.0),
            tick_size: inst.tick_sz.parse().unwrap_or(0.0),
            min_notional: 0.0,
            max_leverage: None,
            is_spot: true,
            is_perp: false,
        })
    }

    async fn get_all_markets(&self) -> Result<Vec<MarketInfo>> {
        let mut params = HashMap::new();
        params.insert("instType".to_string(), "SPOT".to_string());

        #[derive(serde::Deserialize)]
        struct InstrumentInfo {
            #[serde(rename = "instId")]
            inst_id: String,
            #[serde(rename = "baseCcy")]
            base_ccy: String,
            #[serde(rename = "quoteCcy")]
            quote_ccy: String,
            #[serde(rename = "minSz")]
            min_sz: String,
            #[serde(rename = "maxLmtSz")]
            max_lmt_sz: String,
            #[serde(rename = "tickSz")]
            tick_sz: String,
            #[serde(rename = "lotSz")]
            lot_sz: String,
            state: String,
        }

        let response: OkxResponse<Vec<InstrumentInfo>> = self
            .call_api("/api/v5/public/instruments", || async {
                self.client.get_public("/api/v5/public/instruments", Some(params.clone())).await
            })
            .await?;

        let instruments = response.into_result()?;

        Ok(instruments
            .into_iter()
            .map(|inst| MarketInfo {
                symbol: inst.inst_id,
                base_asset: inst.base_ccy,
                quote_asset: inst.quote_ccy,
                status: if inst.state == "live" { MarketStatus::Trading } else { MarketStatus::Halt },
                min_qty: inst.min_sz.parse().unwrap_or(0.0),
                max_qty: inst.max_lmt_sz.parse().unwrap_or(f64::MAX),
                step_size: inst.lot_sz.parse().unwrap_or(0.0),
                tick_size: inst.tick_sz.parse().unwrap_or(0.0),
                min_notional: 0.0,
                max_leverage: None,
                is_spot: true,
                is_perp: false,
            })
            .collect())
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        let mut params = HashMap::new();
        params.insert("instId".to_string(), symbol.to_string());

        #[derive(serde::Deserialize)]
        struct TickerData {
            #[serde(rename = "instId")]
            inst_id: String,
            last: String,
            #[serde(rename = "bidPx")]
            bid_px: String,
            #[serde(rename = "askPx")]
            ask_px: String,
            #[serde(rename = "vol24h")]
            vol_24h: String,
            #[serde(rename = "high24h")]
            high_24h: String,
            #[serde(rename = "low24h")]
            low_24h: String,
            #[serde(rename = "open24h")]
            open_24h: String,
            ts: String,
        }

        let response: OkxResponse<Vec<TickerData>> = self
            .call_api("/api/v5/market/ticker", || async {
                self.client.get_public("/api/v5/market/ticker", Some(params.clone())).await
            })
            .await?;

        let tickers = response.into_result()?;
        let ticker = tickers.first().context("Ticker not found")?;

        let last: f64 = ticker.last.parse().unwrap_or(0.0);
        let open: f64 = ticker.open_24h.parse().unwrap_or(last);

        Ok(TickerInfo {
            symbol: ticker.inst_id.clone(),
            last_price: last,
            bid_price: ticker.bid_px.parse().unwrap_or(0.0),
            ask_price: ticker.ask_px.parse().unwrap_or(0.0),
            volume_24h: ticker.vol_24h.parse().unwrap_or(0.0),
            price_change_24h: last - open,
            price_change_pct_24h: if open > 0.0 { ((last - open) / open) * 100.0 } else { 0.0 },
            high_24h: ticker.high_24h.parse().unwrap_or(0.0),
            low_24h: ticker.low_24h.parse().unwrap_or(0.0),
            open_price_24h: open,
            ts_ms: ticker.ts.parse().unwrap_or_else(|_| Self::now_millis()),
        })
    }

    async fn get_tickers(&self, symbols: Option<Vec<String>>) -> Result<Vec<TickerInfo>> {
        let mut params = HashMap::new();
        params.insert("instType".to_string(), "SPOT".to_string());

        #[derive(serde::Deserialize)]
        struct TickerData {
            #[serde(rename = "instId")]
            inst_id: String,
            last: String,
            #[serde(rename = "bidPx")]
            bid_px: String,
            #[serde(rename = "askPx")]
            ask_px: String,
            #[serde(rename = "vol24h")]
            vol_24h: String,
            #[serde(rename = "high24h")]
            high_24h: String,
            #[serde(rename = "low24h")]
            low_24h: String,
            #[serde(rename = "open24h")]
            open_24h: String,
            ts: String,
        }

        let response: OkxResponse<Vec<TickerData>> = self
            .call_api("/api/v5/market/tickers", || async {
                self.client.get_public("/api/v5/market/tickers", Some(params.clone())).await
            })
            .await?;

        let tickers = response.into_result()?;
        let now = Self::now_millis();

        let mut result: Vec<TickerInfo> = tickers
            .into_iter()
            .filter(|t| symbols.as_ref().map(|s| s.contains(&t.inst_id)).unwrap_or(true))
            .map(|ticker| {
                let last: f64 = ticker.last.parse().unwrap_or(0.0);
                let open: f64 = ticker.open_24h.parse().unwrap_or(last);
                TickerInfo {
                    symbol: ticker.inst_id,
                    last_price: last,
                    bid_price: ticker.bid_px.parse().unwrap_or(0.0),
                    ask_price: ticker.ask_px.parse().unwrap_or(0.0),
                    volume_24h: ticker.vol_24h.parse().unwrap_or(0.0),
                    price_change_24h: last - open,
                    price_change_pct_24h: if open > 0.0 { ((last - open) / open) * 100.0 } else { 0.0 },
                    high_24h: ticker.high_24h.parse().unwrap_or(0.0),
                    low_24h: ticker.low_24h.parse().unwrap_or(0.0),
                    open_price_24h: open,
                    ts_ms: ticker.ts.parse().unwrap_or(now),
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
        let bar = match interval {
            KlineInterval::M1 => "1m",
            KlineInterval::M5 => "5m",
            KlineInterval::M15 => "15m",
            KlineInterval::M30 => "30m",
            KlineInterval::H1 => "1H",
            KlineInterval::H4 => "4H",
            KlineInterval::D1 => "1D",
        };

        let mut params = HashMap::new();
        params.insert("instId".to_string(), symbol.to_string());
        params.insert("bar".to_string(), bar.to_string());
        if let Some(after) = start_ms {
            params.insert("after".to_string(), after.to_string());
        }
        if let Some(before) = end_ms {
            params.insert("before".to_string(), before.to_string());
        }
        if let Some(l) = limit {
            params.insert("limit".to_string(), l.to_string());
        }

        let response: OkxResponse<Vec<Vec<String>>> = self
            .call_api("/api/v5/market/candles", || async {
                self.client.get_public("/api/v5/market/candles", Some(params.clone())).await
            })
            .await?;

        let candles = response.into_result()?;

        Ok(candles
            .into_iter()
            .filter_map(|c| {
                if c.len() >= 6 {
                    let ts: u64 = c[0].parse().ok()?;
                    Some(Kline {
                        symbol: symbol.to_string(),
                        open_ms: ts,
                        close_ms: ts + interval_to_ms(interval),
                        open: c[1].parse().ok()?,
                        high: c[2].parse().ok()?,
                        low: c[3].parse().ok()?,
                        close: c[4].parse().ok()?,
                        volume: c[5].parse().ok()?,
                        quote_volume: c.get(7).and_then(|v| v.parse().ok()).unwrap_or(0.0),
                        trades: 0,
                    })
                } else {
                    None
                }
            })
            .collect())
    }
}

#[async_trait::async_trait]
impl SpotWs for OkxSpotAdapter {
    async fn subscribe_user(&self) -> Result<mpsc::Receiver<UserEvent>> {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::{connect_async, tungstenite::Message};

        let auth = self.auth.as_ref().context("Authentication required")?;
        let (tx, rx) = mpsc::channel(1000);
        let adapter = self.clone();
        let auth_clone = auth.clone();
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel(1);

        {
            let mut guard = adapter.shutdown_tx.lock().await;
            *guard = Some(shutdown_tx);
        }

        tokio::spawn(async move {
            let mut strategy = ReconnectStrategy::new(ReconnectConfig::production());

            'reconnect: loop {
                info!("OKX Spot user stream connecting");

                let mut ws = match connect_async(OKX_WS_PRIVATE_URL).await {
                    Ok((stream, _)) => stream,
                    Err(e) => {
                        error!("WebSocket connection failed: {}", e);
                        if !strategy.can_retry() { break 'reconnect; }
                        let _ = strategy.wait_before_retry().await;
                        continue 'reconnect;
                    }
                };

                // Login
                let ts = (OkxAuth::timestamp_ms() / 1000).to_string();
                let sign = auth_clone.sign_websocket(&ts);
                let login_msg = serde_json::json!({
                    "op": "login",
                    "args": [{
                        "apiKey": auth_clone.api_key,
                        "passphrase": auth_clone.passphrase,
                        "timestamp": ts,
                        "sign": sign
                    }]
                });

                if ws.send(Message::Text(login_msg.to_string())).await.is_err() {
                    continue 'reconnect;
                }

                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

                // Subscribe
                let sub_msg = serde_json::json!({
                    "op": "subscribe",
                    "args": [
                        {"channel": "orders", "instType": "SPOT"},
                        {"channel": "account"}
                    ]
                });

                if ws.send(Message::Text(sub_msg.to_string())).await.is_err() {
                    continue 'reconnect;
                }

                strategy.reset();
                info!("OKX Spot user WebSocket connected");

                let heartbeat = HeartbeatMonitor::new(HeartbeatConfig::production());
                let mut ping_interval = tokio::time::interval(tokio::time::Duration::from_secs(25));

                'message_loop: loop {
                    tokio::select! {
                        _ = shutdown_rx.recv() => { break 'reconnect; }
                        _ = ping_interval.tick() => {
                            if ws.send(Message::Text("ping".to_string())).await.is_err() {
                                break 'message_loop;
                            }
                            heartbeat.record_ping_sent().await;
                        }
                        msg = ws.next() => {
                            match msg {
                                Some(Ok(Message::Text(text))) => {
                                    heartbeat.record_message_received().await;
                                    if text == "pong" {
                                        heartbeat.record_pong_received().await;
                                        continue;
                                    }
                                    if let Ok(event) = parse_user_event(&text) {
                                        if tx.send(event).await.is_err() { break 'reconnect; }
                                    }
                                }
                                Some(Ok(Message::Ping(data))) => { let _ = ws.send(Message::Pong(data)).await; }
                                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => { break 'message_loop; }
                                _ => {}
                            }
                        }
                    }
                }

                if !strategy.can_retry() { break 'reconnect; }
                let _ = strategy.wait_before_retry().await;
            }
        });

        Ok(rx)
    }

    async fn subscribe_books(&self, symbols: &[&str]) -> Result<mpsc::Receiver<BookUpdate>> {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::{connect_async, tungstenite::Message};

        let (tx, rx) = mpsc::channel(1000);
        let symbols_owned: Vec<String> = symbols.iter().map(|s| s.to_string()).collect();

        tokio::spawn(async move {
            let mut strategy = ReconnectStrategy::new(ReconnectConfig::production());

            'reconnect: loop {
                info!("OKX Spot books stream connecting");

                let mut ws = match connect_async(OKX_WS_PUBLIC_URL).await {
                    Ok((stream, _)) => stream,
                    Err(e) => {
                        error!("WebSocket connection failed: {}", e);
                        if !strategy.can_retry() { break 'reconnect; }
                        let _ = strategy.wait_before_retry().await;
                        continue 'reconnect;
                    }
                };

                let args: Vec<_> = symbols_owned.iter().map(|s| serde_json::json!({"channel": "books5", "instId": s})).collect();
                let sub_msg = serde_json::json!({"op": "subscribe", "args": args});

                if ws.send(Message::Text(sub_msg.to_string())).await.is_err() {
                    continue 'reconnect;
                }

                strategy.reset();
                let heartbeat = HeartbeatMonitor::new(HeartbeatConfig::production());
                let mut seq_map: HashMap<String, u64> = HashMap::new();

                'message_loop: loop {
                    match ws.next().await {
                        Some(Ok(Message::Text(text))) => {
                            heartbeat.record_message_received().await;
                            if let Ok(update) = parse_book_update(&text, &mut seq_map) {
                                if tx.send(update).await.is_err() { break 'reconnect; }
                            }
                        }
                        Some(Ok(Message::Ping(data))) => { let _ = ws.send(Message::Pong(data)).await; }
                        Some(Ok(Message::Close(_))) | Some(Err(_)) | None => { break 'message_loop; }
                        _ => {}
                    }
                }

                if !strategy.can_retry() { break 'reconnect; }
                let _ = strategy.wait_before_retry().await;
            }
        });

        Ok(rx)
    }

    async fn subscribe_trades(&self, symbols: &[&str]) -> Result<mpsc::Receiver<TradeEvent>> {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::{connect_async, tungstenite::Message};

        let (tx, rx) = mpsc::channel(1000);
        let symbols_owned: Vec<String> = symbols.iter().map(|s| s.to_string()).collect();

        tokio::spawn(async move {
            let mut strategy = ReconnectStrategy::new(ReconnectConfig::production());

            'reconnect: loop {
                info!("OKX Spot trades stream connecting");

                let mut ws = match connect_async(OKX_WS_PUBLIC_URL).await {
                    Ok((stream, _)) => stream,
                    Err(e) => {
                        error!("WebSocket connection failed: {}", e);
                        if !strategy.can_retry() { break 'reconnect; }
                        let _ = strategy.wait_before_retry().await;
                        continue 'reconnect;
                    }
                };

                let args: Vec<_> = symbols_owned.iter().map(|s| serde_json::json!({"channel": "trades", "instId": s})).collect();
                let sub_msg = serde_json::json!({"op": "subscribe", "args": args});

                if ws.send(Message::Text(sub_msg.to_string())).await.is_err() {
                    continue 'reconnect;
                }

                strategy.reset();
                let heartbeat = HeartbeatMonitor::new(HeartbeatConfig::production());

                'message_loop: loop {
                    match ws.next().await {
                        Some(Ok(Message::Text(text))) => {
                            heartbeat.record_message_received().await;
                            if let Ok(events) = parse_trade_events(&text) {
                                for event in events {
                                    if tx.send(event).await.is_err() { break 'reconnect; }
                                }
                            }
                        }
                        Some(Ok(Message::Ping(data))) => { let _ = ws.send(Message::Pong(data)).await; }
                        Some(Ok(Message::Close(_))) | Some(Err(_)) | None => { break 'message_loop; }
                        _ => {}
                    }
                }

                if !strategy.can_retry() { break 'reconnect; }
                let _ = strategy.wait_before_retry().await;
            }
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
        self.reconnect_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
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

fn now_millis() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

fn parse_user_event(text: &str) -> Result<UserEvent> {
    let msg: serde_json::Value = serde_json::from_str(text)?;

    if let Some(arg) = msg.get("arg") {
        if let Some(channel) = arg.get("channel").and_then(|c| c.as_str()) {
            if channel == "orders" {
                if let Some(data) = msg.get("data").and_then(|d| d.as_array()) {
                    if let Some(order_data) = data.first() {
                        let now = now_millis();
                        let qty: f64 = order_data.get("sz").and_then(|s| s.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0);
                        let filled: f64 = order_data.get("fillSz").and_then(|s| s.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0);

                        let order = Order {
                            venue_order_id: order_data.get("ordId").and_then(|o| o.as_str()).unwrap_or("").to_string(),
                            client_order_id: order_data.get("clOrdId").and_then(|c| c.as_str()).unwrap_or("").to_string(),
                            symbol: order_data.get("instId").and_then(|i| i.as_str()).unwrap_or("").to_string(),
                            ord_type: converters::from_okx_order_type(order_data.get("ordType").and_then(|o| o.as_str()).unwrap_or("limit")),
                            side: converters::from_okx_side(order_data.get("side").and_then(|s| s.as_str()).unwrap_or("buy")),
                            qty,
                            price: order_data.get("px").and_then(|p| p.as_str()).and_then(|p| p.parse().ok()),
                            stop_price: None,
                            tif: None,
                            status: converters::from_okx_order_status(order_data.get("state").and_then(|s| s.as_str()).unwrap_or("live")),
                            filled_qty: filled,
                            remaining_qty: qty - filled,
                            created_ms: order_data.get("cTime").and_then(|t| t.as_str()).and_then(|t| t.parse().ok()).unwrap_or(now),
                            updated_ms: order_data.get("uTime").and_then(|t| t.as_str()).and_then(|t| t.parse().ok()).unwrap_or(now),
                            recv_ms: now,
                            raw_status: order_data.get("state").and_then(|s| s.as_str()).map(|s| s.to_string()),
                        };
                        return Ok(UserEvent::OrderUpdate(order));
                    }
                }
            }
        }
    }

    anyhow::bail!("Unknown user event")
}

fn parse_book_update(text: &str, seq_map: &mut HashMap<String, u64>) -> Result<BookUpdate> {
    let msg: serde_json::Value = serde_json::from_str(text)?;

    if let Some(arg) = msg.get("arg") {
        if arg.get("channel").and_then(|c| c.as_str()) == Some("books5") {
            if let Some(data) = msg.get("data").and_then(|d| d.as_array()).and_then(|d| d.first()) {
                let symbol = arg.get("instId").and_then(|i| i.as_str()).unwrap_or("");
                let ts: u64 = data.get("ts").and_then(|t| t.as_str()).and_then(|t| t.parse().ok()).unwrap_or(0);

                let prev_seq = seq_map.get(symbol).copied().unwrap_or(0);
                seq_map.insert(symbol.to_string(), ts);

                let bids: Vec<(f64, f64)> = data.get("bids").and_then(|b| b.as_array()).map(|arr| {
                    arr.iter().filter_map(|l| {
                        let a = l.as_array()?;
                        Some((a.first()?.as_str()?.parse().ok()?, a.get(1)?.as_str()?.parse().ok()?))
                    }).collect()
                }).unwrap_or_default();

                let asks: Vec<(f64, f64)> = data.get("asks").and_then(|a| a.as_array()).map(|arr| {
                    arr.iter().filter_map(|l| {
                        let a = l.as_array()?;
                        Some((a.first()?.as_str()?.parse().ok()?, a.get(1)?.as_str()?.parse().ok()?))
                    }).collect()
                }).unwrap_or_default();

                return Ok(BookUpdate::DepthDelta {
                    symbol: symbol.to_string(),
                    bids,
                    asks,
                    seq: ts,
                    prev_seq,
                    checksum: None,
                    ex_ts_ms: ts,
                    recv_ms: now_millis(),
                });
            }
        }
    }

    anyhow::bail!("Unknown book update")
}

fn parse_trade_events(text: &str) -> Result<Vec<TradeEvent>> {
    let msg: serde_json::Value = serde_json::from_str(text)?;

    if let Some(arg) = msg.get("arg") {
        if arg.get("channel").and_then(|c| c.as_str()) == Some("trades") {
            if let Some(data) = msg.get("data").and_then(|d| d.as_array()) {
                let now = now_millis();
                let trades: Vec<TradeEvent> = data.iter().filter_map(|t| {
                    Some(TradeEvent {
                        symbol: t.get("instId")?.as_str()?.to_string(),
                        px: t.get("px")?.as_str()?.parse().ok()?,
                        qty: t.get("sz")?.as_str()?.parse().ok()?,
                        taker_is_buy: t.get("side")?.as_str()? == "buy",
                        ex_ts_ms: t.get("ts")?.as_str()?.parse().ok()?,
                        recv_ms: now,
                    })
                }).collect();
                return Ok(trades);
            }
        }
    }

    anyhow::bail!("Unknown trade event")
}
