//! OKX Perpetuals/Swap Market Adapter

use crate::okx::account::{converters, OkxAuth, OkxRestClient, OkxResponse, OKX_WS_PUBLIC_URL, OKX_WS_PRIVATE_URL};
use crate::traits::*;
use crate::utils::{CircuitBreaker, CircuitBreakerConfig, HeartbeatConfig, HeartbeatMonitor, RateLimiter, RateLimiterConfig, ReconnectConfig, ReconnectStrategy};
use anyhow::{Context, Result};
use rust_decimal::Decimal;
use serde::Serialize;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{debug, error, info, warn};

#[derive(Clone)]
pub struct OkxPerpsAdapter {
    client: OkxRestClient,
    auth: Option<OkxAuth>,
    connection_status: Arc<RwLock<ConnectionStatus>>,
    rate_limiter: RateLimiter,
    circuit_breaker: CircuitBreaker,
    reconnect_count: Arc<AtomicU32>,
    shutdown_tx: Arc<Mutex<Option<tokio::sync::broadcast::Sender<()>>>>,
}

impl OkxPerpsAdapter {
    pub fn new(api_key: String, api_secret: String, passphrase: String) -> Self {
        let auth = OkxAuth::new(api_key, api_secret, passphrase);
        Self {
            client: OkxRestClient::new(Some(auth.clone())),
            auth: Some(auth),
            connection_status: Arc::new(RwLock::new(ConnectionStatus::Disconnected)),
            rate_limiter: RateLimiter::new(RateLimiterConfig::default()),
            circuit_breaker: CircuitBreaker::new("okx_perps", CircuitBreakerConfig::production()),
            reconnect_count: Arc::new(AtomicU32::new(0)),
            shutdown_tx: Arc::new(Mutex::new(None)),
        }
    }

    pub fn new_public() -> Self {
        Self {
            client: OkxRestClient::new(None),
            auth: None,
            connection_status: Arc::new(RwLock::new(ConnectionStatus::Disconnected)),
            rate_limiter: RateLimiter::new(RateLimiterConfig::default()),
            circuit_breaker: CircuitBreaker::new("okx_perps", CircuitBreakerConfig::production()),
            reconnect_count: Arc::new(AtomicU32::new(0)),
            shutdown_tx: Arc::new(Mutex::new(None)),
        }
    }

    fn now_millis() -> u64 { SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0) }
    fn generate_client_id() -> String { format!("cc_{}", &uuid::Uuid::new_v4().to_string().replace("-", "")[..16]) }

    async fn call_api<T, F, Fut>(&self, endpoint: &str, f: F) -> Result<T>
    where F: FnOnce() -> Fut, Fut: std::future::Future<Output = Result<T>> {
        if !self.rate_limiter.acquire().await { anyhow::bail!("Rate limit reached: {}", endpoint); }
        debug!("Calling OKX Perps API: {}", endpoint);
        self.circuit_breaker.call(f).await
    }

    pub async fn shutdown(&self) {
        if let Some(tx) = self.shutdown_tx.lock().await.take() { let _ = tx.send(()); }
    }
}

#[async_trait::async_trait]
impl PerpRest for OkxPerpsAdapter {
    async fn create_order(&self, new: NewOrder) -> Result<Order> {
        #[derive(Serialize)]
        struct OrderRequest { #[serde(rename = "instId")] inst_id: String, #[serde(rename = "tdMode")] td_mode: String, side: String, #[serde(rename = "posSide")] pos_side: String, #[serde(rename = "ordType")] ord_type: String, sz: String, #[serde(skip_serializing_if = "Option::is_none")] px: Option<String>, #[serde(skip_serializing_if = "Option::is_none")] #[serde(rename = "clOrdId")] cl_ord_id: Option<String>, #[serde(skip_serializing_if = "Option::is_none")] #[serde(rename = "reduceOnly")] reduce_only: Option<bool> }

        let cl_ord_id = if new.client_order_id.is_empty() { Self::generate_client_id() } else { new.client_order_id.clone() };
        let request = OrderRequest {
            inst_id: new.symbol.clone(), td_mode: "cross".to_string(),
            side: converters::to_okx_side(new.side).to_string(),
            pos_side: "net".to_string(),
            ord_type: converters::to_okx_order_type(new.ord_type).to_string(),
            sz: new.qty.to_string(), px: new.price.map(|p| p.to_string()),
            cl_ord_id: Some(cl_ord_id.clone()),
            reduce_only: if new.reduce_only { Some(true) } else { None },
        };

        #[derive(serde::Deserialize)]
        struct OrderResult { #[serde(rename = "ordId")] ord_id: String, #[serde(rename = "clOrdId")] cl_ord_id: String, #[serde(rename = "sCode")] s_code: String, #[serde(rename = "sMsg")] s_msg: String }

        let response: OkxResponse<Vec<OrderResult>> = self.call_api("/api/v5/trade/order", || async { self.client.post_private("/api/v5/trade/order", &request).await }).await?;
        let results = response.into_result()?;
        let result = results.first().context("No order result")?;
        if result.s_code != "0" { anyhow::bail!("Order failed: {}", result.s_msg); }

        let now = Self::now_millis();
        Ok(Order { venue_order_id: result.ord_id.clone(), client_order_id: result.cl_ord_id.clone(), symbol: new.symbol, ord_type: new.ord_type, side: new.side, qty: new.qty, price: new.price, stop_price: new.stop_price, tif: new.tif, status: OrderStatus::New, filled_qty: 0.0, remaining_qty: new.qty, created_ms: now, updated_ms: now, recv_ms: now, raw_status: Some("live".to_string()) })
    }

    async fn cancel_order(&self, symbol: &str, venue_order_id: &str) -> Result<bool> {
        #[derive(Serialize)]
        struct CancelRequest { #[serde(rename = "instId")] inst_id: String, #[serde(rename = "ordId")] ord_id: String }
        let request = CancelRequest { inst_id: symbol.to_string(), ord_id: venue_order_id.to_string() };
        let response: OkxResponse<Vec<serde_json::Value>> = self.call_api("/api/v5/trade/cancel-order", || async { self.client.post_private("/api/v5/trade/cancel-order", &request).await }).await?;
        Ok(response.is_ok())
    }

    async fn cancel_all(&self, symbol: Option<&str>) -> Result<usize> {
        if let Some(sym) = symbol {
            #[derive(Serialize)]
            struct Req { #[serde(rename = "instId")] inst_id: String }
            let _: OkxResponse<Vec<serde_json::Value>> = self.call_api("/api/v5/trade/cancel-batch-orders", || async { self.client.post_private("/api/v5/trade/cancel-batch-orders", &Req { inst_id: sym.to_string() }).await }).await?;
        }
        Ok(1)
    }

    async fn get_order(&self, symbol: &str, venue_order_id: &str) -> Result<Order> {
        let mut params = HashMap::new();
        params.insert("instId".to_string(), symbol.to_string());
        params.insert("ordId".to_string(), venue_order_id.to_string());

        #[derive(serde::Deserialize)]
        struct OrderDetail { #[serde(rename = "instId")] inst_id: String, #[serde(rename = "ordId")] ord_id: String, #[serde(rename = "clOrdId")] cl_ord_id: String, side: String, #[serde(rename = "ordType")] ord_type: String, sz: String, px: String, #[serde(rename = "fillSz")] fill_sz: String, state: String, #[serde(rename = "cTime")] c_time: String, #[serde(rename = "uTime")] u_time: String }

        let response: OkxResponse<Vec<OrderDetail>> = self.call_api("/api/v5/trade/order", || async { self.client.get_private("/api/v5/trade/order", Some(params.clone())).await }).await?;
        let orders = response.into_result()?;
        let order = orders.first().context("Order not found")?;
        let now = Self::now_millis();
        let qty: f64 = order.sz.parse().unwrap_or(0.0);
        let filled: f64 = order.fill_sz.parse().unwrap_or(0.0);

        Ok(Order { venue_order_id: order.ord_id.clone(), client_order_id: order.cl_ord_id.clone(), symbol: order.inst_id.clone(), ord_type: converters::from_okx_order_type(&order.ord_type), side: converters::from_okx_side(&order.side), qty, price: order.px.parse().ok(), stop_price: None, tif: None, status: converters::from_okx_order_status(&order.state), filled_qty: filled, remaining_qty: qty - filled, created_ms: order.c_time.parse().unwrap_or(now), updated_ms: order.u_time.parse().unwrap_or(now), recv_ms: now, raw_status: Some(order.state.clone()) })
    }

    async fn get_open_orders(&self, symbol: Option<&str>) -> Result<Vec<Order>> {
        let mut params = HashMap::new();
        params.insert("instType".to_string(), "SWAP".to_string());
        if let Some(s) = symbol { params.insert("instId".to_string(), s.to_string()); }

        #[derive(serde::Deserialize)]
        struct OrderDetail { #[serde(rename = "instId")] inst_id: String, #[serde(rename = "ordId")] ord_id: String, #[serde(rename = "clOrdId")] cl_ord_id: String, side: String, #[serde(rename = "ordType")] ord_type: String, sz: String, px: String, #[serde(rename = "fillSz")] fill_sz: String, state: String, #[serde(rename = "cTime")] c_time: String, #[serde(rename = "uTime")] u_time: String }

        let response: OkxResponse<Vec<OrderDetail>> = self.call_api("/api/v5/trade/orders-pending", || async { self.client.get_private("/api/v5/trade/orders-pending", Some(params.clone())).await }).await?;
        let orders = response.into_result()?;
        let now = Self::now_millis();

        Ok(orders.into_iter().map(|o| { let qty: f64 = o.sz.parse().unwrap_or(0.0); let filled: f64 = o.fill_sz.parse().unwrap_or(0.0); Order { venue_order_id: o.ord_id, client_order_id: o.cl_ord_id, symbol: o.inst_id, ord_type: converters::from_okx_order_type(&o.ord_type), side: converters::from_okx_side(&o.side), qty, price: o.px.parse().ok(), stop_price: None, tif: None, status: converters::from_okx_order_status(&o.state), filled_qty: filled, remaining_qty: qty - filled, created_ms: o.c_time.parse().unwrap_or(now), updated_ms: o.u_time.parse().unwrap_or(now), recv_ms: now, raw_status: Some(o.state) } }).collect())
    }

    async fn replace_order(&self, symbol: &str, venue_order_id: &str, new_price: Option<Price>, new_qty: Option<Quantity>, _: Option<TimeInForce>, _: Option<bool>, _: Option<bool>) -> Result<(Order, bool)> {
        #[derive(Serialize)]
        struct AmendReq { #[serde(rename = "instId")] inst_id: String, #[serde(rename = "ordId")] ord_id: String, #[serde(skip_serializing_if = "Option::is_none")] #[serde(rename = "newSz")] new_sz: Option<String>, #[serde(skip_serializing_if = "Option::is_none")] #[serde(rename = "newPx")] new_px: Option<String> }
        let request = AmendReq { inst_id: symbol.to_string(), ord_id: venue_order_id.to_string(), new_sz: new_qty.map(|q| q.to_string()), new_px: new_price.map(|p| p.to_string()) };
        let _: OkxResponse<Vec<serde_json::Value>> = self.call_api("/api/v5/trade/amend-order", || async { self.client.post_private("/api/v5/trade/amend-order", &request).await }).await?;
        let order = self.get_order(symbol, venue_order_id).await?;
        Ok((order, true))
    }

    async fn create_batch_orders(&self, batch: BatchOrderRequest) -> Result<BatchOrderResult> {
        let mut success = Vec::new(); let mut failed = Vec::new();
        for order in batch.orders { match self.create_order(order.clone()).await { Ok(o) => success.push(o), Err(e) => failed.push((order, e.to_string())) } }
        Ok(BatchOrderResult { success, failed })
    }

    async fn cancel_batch_orders(&self, symbol: &str, order_ids: Vec<String>) -> Result<BatchCancelResult> {
        let mut success = Vec::new(); let mut failed = Vec::new();
        for oid in order_ids { match self.cancel_order(symbol, &oid).await { Ok(_) => success.push(oid), Err(e) => failed.push((oid, e.to_string())) } }
        Ok(BatchCancelResult { success, failed })
    }

    async fn set_leverage(&self, symbol: &str, leverage: Decimal) -> Result<()> {
        #[derive(Serialize)]
        struct LevReq { #[serde(rename = "instId")] inst_id: String, lever: String, #[serde(rename = "mgnMode")] mgn_mode: String }
        let request = LevReq { inst_id: symbol.to_string(), lever: leverage.to_string(), mgn_mode: "cross".to_string() };
        let _: OkxResponse<Vec<serde_json::Value>> = self.call_api("/api/v5/account/set-leverage", || async { self.client.post_private("/api/v5/account/set-leverage", &request).await }).await?;
        Ok(())
    }

    async fn set_margin_mode(&self, _symbol: &str, mode: MarginMode) -> Result<()> {
        #[derive(Serialize)]
        struct ModeReq { #[serde(rename = "posMode")] pos_mode: String }
        let request = ModeReq { pos_mode: if matches!(mode, MarginMode::Isolated) { "long_short_mode" } else { "net_mode" }.to_string() };
        let _: OkxResponse<Vec<serde_json::Value>> = self.call_api("/api/v5/account/set-position-mode", || async { self.client.post_private("/api/v5/account/set-position-mode", &request).await }).await?;
        Ok(())
    }

    async fn get_position(&self, symbol: &str) -> Result<Position> {
        let positions = self.get_all_positions().await?;
        positions.into_iter().find(|p| p.symbol.eq_ignore_ascii_case(symbol)).context(format!("Position not found: {}", symbol))
    }

    async fn get_all_positions(&self) -> Result<Vec<Position>> {
        let mut params = HashMap::new();
        params.insert("instType".to_string(), "SWAP".to_string());

        #[derive(serde::Deserialize)]
        struct PosData { #[serde(rename = "instId")] inst_id: String, pos: String, #[serde(rename = "avgPx")] avg_px: String, #[serde(rename = "markPx")] mark_px: String, #[serde(rename = "liqPx")] liq_px: String, upl: String, lever: String, #[serde(rename = "uTime")] u_time: String }

        let response: OkxResponse<Vec<PosData>> = self.call_api("/api/v5/account/positions", || async { self.client.get_private("/api/v5/account/positions", Some(params.clone())).await }).await?;
        let positions = response.into_result()?;

        Ok(positions.into_iter().filter(|p| p.pos.parse::<f64>().unwrap_or(0.0).abs() > 1e-10).map(|p| {
            Position { exchange: Some("okx".to_string()), symbol: p.inst_id, qty: p.pos.parse().unwrap_or(0.0), entry_px: p.avg_px.parse().unwrap_or(0.0), mark_px: p.mark_px.parse().ok(), liquidation_px: p.liq_px.parse().ok(), unrealized_pnl: p.upl.parse().ok(), realized_pnl: None, margin: None, leverage: p.lever.parse().ok(), opened_ms: None, updated_ms: p.u_time.parse().unwrap_or_else(|_| Self::now_millis()) }
        }).collect())
    }

    async fn get_funding_rate(&self, symbol: &str) -> Result<(Decimal, UnixMillis)> {
        let mut params = HashMap::new();
        params.insert("instId".to_string(), symbol.to_string());

        #[derive(serde::Deserialize)]
        struct FundingData { #[serde(rename = "fundingRate")] funding_rate: String, #[serde(rename = "fundingTime")] funding_time: String }

        let response: OkxResponse<Vec<FundingData>> = self.call_api("/api/v5/public/funding-rate", || async { self.client.get_public("/api/v5/public/funding-rate", Some(params.clone())).await }).await?;
        let data = response.into_result()?;
        let fund = data.first().context("Funding not found")?;
        Ok((Decimal::from_str(&fund.funding_rate).unwrap_or_default(), fund.funding_time.parse().unwrap_or(0)))
    }

    async fn get_balances(&self) -> Result<Vec<Balance>> {
        #[derive(serde::Deserialize)]
        struct BalData { details: Vec<BalDetail> }
        #[derive(serde::Deserialize)]
        struct BalDetail { ccy: String, #[serde(rename = "availBal")] avail_bal: String, #[serde(rename = "frozenBal")] frozen_bal: String, bal: String }

        let response: OkxResponse<Vec<BalData>> = self.call_api("/api/v5/account/balance", || async { self.client.get_private("/api/v5/account/balance", None).await }).await?;
        let data = response.into_result()?;
        Ok(data.first().map(|d| d.details.iter().map(|b| Balance { asset: b.ccy.clone(), free: b.avail_bal.parse().unwrap_or(0.0), locked: b.frozen_bal.parse().unwrap_or(0.0), total: b.bal.parse().unwrap_or(0.0) }).collect()).unwrap_or_default())
    }

    async fn get_account_info(&self) -> Result<AccountInfo> {
        let balances = self.get_balances().await?;
        Ok(AccountInfo { balances, can_trade: true, can_withdraw: true, can_deposit: true, update_ms: Self::now_millis() })
    }

    async fn get_market_info(&self, symbol: &str) -> Result<MarketInfo> {
        let mut params = HashMap::new();
        params.insert("instType".to_string(), "SWAP".to_string());
        params.insert("instId".to_string(), symbol.to_string());

        #[derive(serde::Deserialize)]
        struct InstInfo { #[serde(rename = "instId")] inst_id: String, #[serde(rename = "uly")] uly: String, #[serde(rename = "settleCcy")] settle_ccy: String, #[serde(rename = "minSz")] min_sz: String, #[serde(rename = "tickSz")] tick_sz: String, #[serde(rename = "lotSz")] lot_sz: String, lever: String, state: String }

        let response: OkxResponse<Vec<InstInfo>> = self.call_api("/api/v5/public/instruments", || async { self.client.get_public("/api/v5/public/instruments", Some(params.clone())).await }).await?;
        let instruments = response.into_result()?;
        let inst = instruments.first().context("Instrument not found")?;

        Ok(MarketInfo { symbol: inst.inst_id.clone(), base_asset: inst.uly.clone(), quote_asset: inst.settle_ccy.clone(), status: if inst.state == "live" { MarketStatus::Trading } else { MarketStatus::Halt }, min_qty: inst.min_sz.parse().unwrap_or(0.0), max_qty: f64::MAX, step_size: inst.lot_sz.parse().unwrap_or(0.0), tick_size: inst.tick_sz.parse().unwrap_or(0.0), min_notional: 0.0, max_leverage: inst.lever.parse().ok(), is_spot: false, is_perp: true })
    }

    async fn get_all_markets(&self) -> Result<Vec<MarketInfo>> {
        let mut params = HashMap::new();
        params.insert("instType".to_string(), "SWAP".to_string());

        #[derive(serde::Deserialize)]
        struct InstInfo { #[serde(rename = "instId")] inst_id: String, #[serde(rename = "uly")] uly: String, #[serde(rename = "settleCcy")] settle_ccy: String, #[serde(rename = "minSz")] min_sz: String, #[serde(rename = "tickSz")] tick_sz: String, #[serde(rename = "lotSz")] lot_sz: String, lever: String, state: String }

        let response: OkxResponse<Vec<InstInfo>> = self.call_api("/api/v5/public/instruments", || async { self.client.get_public("/api/v5/public/instruments", Some(params.clone())).await }).await?;
        let instruments = response.into_result()?;

        Ok(instruments.into_iter().map(|i| MarketInfo { symbol: i.inst_id, base_asset: i.uly, quote_asset: i.settle_ccy, status: if i.state == "live" { MarketStatus::Trading } else { MarketStatus::Halt }, min_qty: i.min_sz.parse().unwrap_or(0.0), max_qty: f64::MAX, step_size: i.lot_sz.parse().unwrap_or(0.0), tick_size: i.tick_sz.parse().unwrap_or(0.0), min_notional: 0.0, max_leverage: i.lever.parse().ok(), is_spot: false, is_perp: true }).collect())
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        let mut params = HashMap::new();
        params.insert("instId".to_string(), symbol.to_string());

        #[derive(serde::Deserialize)]
        struct TickerData { #[serde(rename = "instId")] inst_id: String, last: String, #[serde(rename = "bidPx")] bid_px: String, #[serde(rename = "askPx")] ask_px: String, #[serde(rename = "vol24h")] vol_24h: String, #[serde(rename = "high24h")] high_24h: String, #[serde(rename = "low24h")] low_24h: String, #[serde(rename = "open24h")] open_24h: String, ts: String }

        let response: OkxResponse<Vec<TickerData>> = self.call_api("/api/v5/market/ticker", || async { self.client.get_public("/api/v5/market/ticker", Some(params.clone())).await }).await?;
        let tickers = response.into_result()?;
        let t = tickers.first().context("Ticker not found")?;

        let last: f64 = t.last.parse().unwrap_or(0.0);
        let open: f64 = t.open_24h.parse().unwrap_or(last);

        Ok(TickerInfo { symbol: t.inst_id.clone(), last_price: last, bid_price: t.bid_px.parse().unwrap_or(0.0), ask_price: t.ask_px.parse().unwrap_or(0.0), volume_24h: t.vol_24h.parse().unwrap_or(0.0), price_change_24h: last - open, price_change_pct_24h: if open > 0.0 { ((last - open) / open) * 100.0 } else { 0.0 }, high_24h: t.high_24h.parse().unwrap_or(0.0), low_24h: t.low_24h.parse().unwrap_or(0.0), open_price_24h: open, ts_ms: t.ts.parse().unwrap_or_else(|_| Self::now_millis()) })
    }

    async fn get_tickers(&self, symbols: Option<Vec<String>>) -> Result<Vec<TickerInfo>> {
        let mut params = HashMap::new();
        params.insert("instType".to_string(), "SWAP".to_string());

        #[derive(serde::Deserialize)]
        struct TickerData { #[serde(rename = "instId")] inst_id: String, last: String, #[serde(rename = "bidPx")] bid_px: String, #[serde(rename = "askPx")] ask_px: String, #[serde(rename = "vol24h")] vol_24h: String, #[serde(rename = "high24h")] high_24h: String, #[serde(rename = "low24h")] low_24h: String, #[serde(rename = "open24h")] open_24h: String, ts: String }

        let response: OkxResponse<Vec<TickerData>> = self.call_api("/api/v5/market/tickers", || async { self.client.get_public("/api/v5/market/tickers", Some(params.clone())).await }).await?;
        let tickers = response.into_result()?;

        Ok(tickers.into_iter().filter(|t| symbols.as_ref().map(|s| s.contains(&t.inst_id)).unwrap_or(true)).map(|t| { let last: f64 = t.last.parse().unwrap_or(0.0); let open: f64 = t.open_24h.parse().unwrap_or(last); TickerInfo { symbol: t.inst_id, last_price: last, bid_price: t.bid_px.parse().unwrap_or(0.0), ask_price: t.ask_px.parse().unwrap_or(0.0), volume_24h: t.vol_24h.parse().unwrap_or(0.0), price_change_24h: last - open, price_change_pct_24h: if open > 0.0 { ((last - open) / open) * 100.0 } else { 0.0 }, high_24h: t.high_24h.parse().unwrap_or(0.0), low_24h: t.low_24h.parse().unwrap_or(0.0), open_price_24h: open, ts_ms: t.ts.parse().unwrap_or_else(|_| Self::now_millis()) } }).collect())
    }

    async fn get_mark_price(&self, symbol: &str) -> Result<(Price, UnixMillis)> {
        let mut params = HashMap::new();
        params.insert("instId".to_string(), symbol.to_string());
        params.insert("instType".to_string(), "SWAP".to_string());

        #[derive(serde::Deserialize)]
        struct MarkData { #[serde(rename = "markPx")] mark_px: String, ts: String }

        let response: OkxResponse<Vec<MarkData>> = self.call_api("/api/v5/public/mark-price", || async { self.client.get_public("/api/v5/public/mark-price", Some(params.clone())).await }).await?;
        let data = response.into_result()?;
        let mark = data.first().context("Mark price not found")?;
        Ok((mark.mark_px.parse().unwrap_or(0.0), mark.ts.parse().unwrap_or(0)))
    }

    async fn get_index_price(&self, symbol: &str) -> Result<(Price, UnixMillis)> { self.get_mark_price(symbol).await }

    async fn get_klines(&self, symbol: &str, interval: KlineInterval, start_ms: Option<UnixMillis>, end_ms: Option<UnixMillis>, limit: Option<usize>) -> Result<Vec<Kline>> {
        let bar = match interval { KlineInterval::M1 => "1m", KlineInterval::M5 => "5m", KlineInterval::M15 => "15m", KlineInterval::M30 => "30m", KlineInterval::H1 => "1H", KlineInterval::H4 => "4H", KlineInterval::D1 => "1D" };
        let mut params = HashMap::new();
        params.insert("instId".to_string(), symbol.to_string());
        params.insert("bar".to_string(), bar.to_string());
        if let Some(s) = start_ms { params.insert("after".to_string(), s.to_string()); }
        if let Some(e) = end_ms { params.insert("before".to_string(), e.to_string()); }
        if let Some(l) = limit { params.insert("limit".to_string(), l.to_string()); }

        let response: OkxResponse<Vec<Vec<String>>> = self.call_api("/api/v5/market/candles", || async { self.client.get_public("/api/v5/market/candles", Some(params.clone())).await }).await?;
        let candles = response.into_result()?;

        Ok(candles.into_iter().filter_map(|c| if c.len() >= 6 { let ts: u64 = c[0].parse().ok()?; Some(Kline { symbol: symbol.to_string(), open_ms: ts, close_ms: ts + interval_to_ms(interval), open: c[1].parse().ok()?, high: c[2].parse().ok()?, low: c[3].parse().ok()?, close: c[4].parse().ok()?, volume: c[5].parse().ok()?, quote_volume: c.get(7).and_then(|v| v.parse().ok()).unwrap_or(0.0), trades: 0 }) } else { None }).collect())
    }

    async fn get_funding_history(&self, symbol: &str, start_ms: Option<UnixMillis>, _end_ms: Option<UnixMillis>, limit: Option<usize>) -> Result<Vec<FundingRateHistory>> {
        let mut params = HashMap::new();
        params.insert("instId".to_string(), symbol.to_string());
        if let Some(s) = start_ms { params.insert("before".to_string(), s.to_string()); }
        if let Some(l) = limit { params.insert("limit".to_string(), l.to_string()); }

        #[derive(serde::Deserialize)]
        struct FundHist { #[serde(rename = "fundingRate")] funding_rate: String, #[serde(rename = "fundingTime")] funding_time: String }

        let response: OkxResponse<Vec<FundHist>> = self.call_api("/api/v5/public/funding-rate-history", || async { self.client.get_public("/api/v5/public/funding-rate-history", Some(params.clone())).await }).await?;
        let data = response.into_result()?;

        Ok(data.into_iter().map(|f| FundingRateHistory { symbol: symbol.to_string(), rate: Decimal::from_str(&f.funding_rate).unwrap_or_default(), ts_ms: f.funding_time.parse().unwrap_or(0) }).collect())
    }
}

fn interval_to_ms(interval: KlineInterval) -> u64 { match interval { KlineInterval::M1 => 60_000, KlineInterval::M5 => 300_000, KlineInterval::M15 => 900_000, KlineInterval::M30 => 1_800_000, KlineInterval::H1 => 3_600_000, KlineInterval::H4 => 14_400_000, KlineInterval::D1 => 86_400_000 } }
fn now_millis() -> u64 { SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0) }

#[async_trait::async_trait]
impl PerpWs for OkxPerpsAdapter {
    async fn subscribe_user(&self) -> Result<mpsc::Receiver<UserEvent>> {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::{connect_async, tungstenite::Message};

        let auth = self.auth.as_ref().context("Authentication required")?;
        let (tx, rx) = mpsc::channel(1000);
        let auth_clone = auth.clone();
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel(1);
        { let mut guard = self.shutdown_tx.lock().await; *guard = Some(shutdown_tx); }

        tokio::spawn(async move {
            let mut strategy = ReconnectStrategy::new(ReconnectConfig::production());
            'reconnect: loop {
                let mut ws = match connect_async(OKX_WS_PRIVATE_URL).await { Ok((s, _)) => s, Err(_) => { if !strategy.can_retry() { break; } let _ = strategy.wait_before_retry().await; continue; } };

                let ts = (OkxAuth::timestamp_ms() / 1000).to_string();
                let sign = auth_clone.sign_websocket(&ts);
                let login = serde_json::json!({"op": "login", "args": [{"apiKey": auth_clone.api_key, "passphrase": auth_clone.passphrase, "timestamp": ts, "sign": sign}]});
                if ws.send(Message::Text(login.to_string())).await.is_err() { continue 'reconnect; }
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

                let sub = serde_json::json!({"op": "subscribe", "args": [{"channel": "orders", "instType": "SWAP"}, {"channel": "positions", "instType": "SWAP"}]});
                if ws.send(Message::Text(sub.to_string())).await.is_err() { continue 'reconnect; }
                strategy.reset();

                let heartbeat = HeartbeatMonitor::new(HeartbeatConfig::production());
                let mut ping_interval = tokio::time::interval(tokio::time::Duration::from_secs(25));

                loop {
                    tokio::select! {
                        _ = shutdown_rx.recv() => { break 'reconnect; }
                        _ = ping_interval.tick() => { let _ = ws.send(Message::Text("ping".to_string())).await; heartbeat.record_ping_sent().await; }
                        msg = ws.next() => {
                            match msg {
                                Some(Ok(Message::Text(text))) => {
                                    heartbeat.record_message_received().await;
                                    if text == "pong" { heartbeat.record_pong_received().await; continue; }
                                    if let Ok(event) = parse_perps_user_event(&text) { if tx.send(event).await.is_err() { break 'reconnect; } }
                                }
                                Some(Ok(Message::Ping(d))) => { let _ = ws.send(Message::Pong(d)).await; }
                                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => { break; }
                                _ => {}
                            }
                        }
                    }
                }
                if !strategy.can_retry() { break; }
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
                let mut ws = match connect_async(OKX_WS_PUBLIC_URL).await { Ok((s, _)) => s, Err(_) => { if !strategy.can_retry() { break; } let _ = strategy.wait_before_retry().await; continue; } };

                let args: Vec<_> = symbols_owned.iter().map(|s| serde_json::json!({"channel": "books5", "instId": s})).collect();
                if ws.send(Message::Text(serde_json::json!({"op": "subscribe", "args": args}).to_string())).await.is_err() { continue 'reconnect; }
                strategy.reset();

                let mut seq_map: HashMap<String, u64> = HashMap::new();
                loop {
                    match ws.next().await {
                        Some(Ok(Message::Text(text))) => { if let Ok(update) = parse_book_update(&text, &mut seq_map) { if tx.send(update).await.is_err() { break 'reconnect; } } }
                        Some(Ok(Message::Ping(d))) => { let _ = ws.send(Message::Pong(d)).await; }
                        Some(Ok(Message::Close(_))) | Some(Err(_)) | None => { break; }
                        _ => {}
                    }
                }
                if !strategy.can_retry() { break; }
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
                let mut ws = match connect_async(OKX_WS_PUBLIC_URL).await { Ok((s, _)) => s, Err(_) => { if !strategy.can_retry() { break; } let _ = strategy.wait_before_retry().await; continue; } };

                let args: Vec<_> = symbols_owned.iter().map(|s| serde_json::json!({"channel": "trades", "instId": s})).collect();
                if ws.send(Message::Text(serde_json::json!({"op": "subscribe", "args": args}).to_string())).await.is_err() { continue 'reconnect; }
                strategy.reset();

                loop {
                    match ws.next().await {
                        Some(Ok(Message::Text(text))) => { if let Ok(events) = parse_trade_events(&text) { for e in events { if tx.send(e).await.is_err() { break 'reconnect; } } } }
                        Some(Ok(Message::Ping(d))) => { let _ = ws.send(Message::Pong(d)).await; }
                        Some(Ok(Message::Close(_))) | Some(Err(_)) | None => { break; }
                        _ => {}
                    }
                }
                if !strategy.can_retry() { break; }
                let _ = strategy.wait_before_retry().await;
            }
        });
        Ok(rx)
    }

    async fn health(&self) -> Result<HealthStatus> { Ok(HealthStatus { status: *self.connection_status.read().await, last_ping_ms: None, last_pong_ms: None, latency_ms: None, reconnect_count: self.reconnect_count.load(Ordering::Relaxed), error_msg: None }) }
    async fn reconnect(&self) -> Result<()> { self.reconnect_count.fetch_add(1, Ordering::Relaxed); Ok(()) }
}

fn parse_perps_user_event(text: &str) -> Result<UserEvent> {
    let msg: serde_json::Value = serde_json::from_str(text)?;
    if let Some(arg) = msg.get("arg") {
        if let Some(channel) = arg.get("channel").and_then(|c| c.as_str()) {
            if channel == "orders" {
                if let Some(data) = msg.get("data").and_then(|d| d.as_array()).and_then(|d| d.first()) {
                    let qty: f64 = data.get("sz").and_then(|s| s.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0);
                    let filled: f64 = data.get("fillSz").and_then(|s| s.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0);
                    let now = now_millis();
                    return Ok(UserEvent::OrderUpdate(Order {
                        venue_order_id: data.get("ordId").and_then(|o| o.as_str()).unwrap_or("").to_string(),
                        client_order_id: data.get("clOrdId").and_then(|c| c.as_str()).unwrap_or("").to_string(),
                        symbol: data.get("instId").and_then(|i| i.as_str()).unwrap_or("").to_string(),
                        ord_type: converters::from_okx_order_type(data.get("ordType").and_then(|o| o.as_str()).unwrap_or("limit")),
                        side: converters::from_okx_side(data.get("side").and_then(|s| s.as_str()).unwrap_or("buy")),
                        qty, price: data.get("px").and_then(|p| p.as_str()).and_then(|p| p.parse().ok()), stop_price: None, tif: None,
                        status: converters::from_okx_order_status(data.get("state").and_then(|s| s.as_str()).unwrap_or("live")),
                        filled_qty: filled, remaining_qty: qty - filled,
                        created_ms: data.get("cTime").and_then(|t| t.as_str()).and_then(|t| t.parse().ok()).unwrap_or(now),
                        updated_ms: data.get("uTime").and_then(|t| t.as_str()).and_then(|t| t.parse().ok()).unwrap_or(now),
                        recv_ms: now, raw_status: data.get("state").and_then(|s| s.as_str()).map(|s| s.to_string()),
                    }));
                }
            } else if channel == "positions" {
                if let Some(data) = msg.get("data").and_then(|d| d.as_array()).and_then(|d| d.first()) {
                    return Ok(UserEvent::Position(Position {
                        exchange: Some("okx".to_string()),
                        symbol: data.get("instId").and_then(|i| i.as_str()).unwrap_or("").to_string(),
                        qty: data.get("pos").and_then(|p| p.as_str()).and_then(|p| p.parse().ok()).unwrap_or(0.0),
                        entry_px: data.get("avgPx").and_then(|p| p.as_str()).and_then(|p| p.parse().ok()).unwrap_or(0.0),
                        mark_px: data.get("markPx").and_then(|p| p.as_str()).and_then(|p| p.parse().ok()),
                        liquidation_px: data.get("liqPx").and_then(|p| p.as_str()).and_then(|p| p.parse().ok()),
                        unrealized_pnl: data.get("upl").and_then(|u| u.as_str()).and_then(|u| u.parse().ok()),
                        realized_pnl: None, margin: None,
                        leverage: data.get("lever").and_then(|l| l.as_str()).and_then(|l| l.parse().ok()),
                        opened_ms: None, updated_ms: data.get("uTime").and_then(|t| t.as_str()).and_then(|t| t.parse().ok()).unwrap_or_else(now_millis),
                    }));
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

                let bids: Vec<(f64, f64)> = data.get("bids").and_then(|b| b.as_array()).map(|arr| arr.iter().filter_map(|l| { let a = l.as_array()?; Some((a.first()?.as_str()?.parse().ok()?, a.get(1)?.as_str()?.parse().ok()?)) }).collect()).unwrap_or_default();
                let asks: Vec<(f64, f64)> = data.get("asks").and_then(|a| a.as_array()).map(|arr| arr.iter().filter_map(|l| { let a = l.as_array()?; Some((a.first()?.as_str()?.parse().ok()?, a.get(1)?.as_str()?.parse().ok()?)) }).collect()).unwrap_or_default();

                return Ok(BookUpdate::DepthDelta { symbol: symbol.to_string(), bids, asks, seq: ts, prev_seq, checksum: None, ex_ts_ms: ts, recv_ms: now_millis() });
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
                return Ok(data.iter().filter_map(|t| Some(TradeEvent { symbol: t.get("instId")?.as_str()?.to_string(), px: t.get("px")?.as_str()?.parse().ok()?, qty: t.get("sz")?.as_str()?.parse().ok()?, taker_is_buy: t.get("side")?.as_str()? == "buy", ex_ts_ms: t.get("ts")?.as_str()?.parse().ok()?, recv_ms: now })).collect());
            }
        }
    }
    anyhow::bail!("Unknown trade event")
}
