//! KuCoin Perpetual Futures Trading Adapter
//!
//! Implements PerpRest and PerpWs traits for KuCoin futures trading.

use super::account::{converters, KucoinAuth, KucoinResponse, KucoinRestClient};
use crate::traits::{
    AccountInfo, Balance, BatchCancelRequest, BatchCancelResult, BatchOrderRequest,
    BatchOrderResult, BookUpdate, Fill, FundingPayment, Kline, Market, MarginMode, NewOrder, Order,
    OrderStatus, PerpRest, PerpWs, Position, PositionUpdate, Side, Ticker, Trade, UserEvent,
};
use crate::utils::{CircuitBreaker, HeartbeatMonitor, RateLimiter, ReconnectStrategy};
use anyhow::{Context, Result};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio_tungstenite::{connect_async, tungstenite::Message};

// =============================================================================
// Adapter
// =============================================================================

/// KuCoin perpetual futures trading adapter
pub struct KucoinPerpsAdapter {
    rest_client: KucoinRestClient,
    auth: Option<KucoinAuth>,
    rate_limiter: RateLimiter,
    circuit_breaker: CircuitBreaker,
    ws_connected: AtomicBool,
    ws_shutdown: Arc<RwLock<Option<broadcast::Sender<()>>>>,
    heartbeat: HeartbeatMonitor,
    request_id: AtomicU64,
}

impl KucoinPerpsAdapter {
    /// Creates a new KuCoin perpetual futures adapter with authentication
    pub fn new(api_key: String, api_secret: String, passphrase: String) -> Self {
        let auth = KucoinAuth::new(api_key, api_secret, passphrase);
        Self {
            rest_client: KucoinRestClient::futures(Some(auth.clone())),
            auth: Some(auth),
            rate_limiter: RateLimiter::new(30, std::time::Duration::from_secs(3)),
            circuit_breaker: CircuitBreaker::new(5, std::time::Duration::from_secs(60)),
            ws_connected: AtomicBool::new(false),
            ws_shutdown: Arc::new(RwLock::new(None)),
            heartbeat: HeartbeatMonitor::new(std::time::Duration::from_secs(30)),
            request_id: AtomicU64::new(1),
        }
    }

    /// Creates a new KuCoin perpetual futures adapter without authentication (public endpoints only)
    pub fn public() -> Self {
        Self {
            rest_client: KucoinRestClient::futures(None),
            auth: None,
            rate_limiter: RateLimiter::new(30, std::time::Duration::from_secs(3)),
            circuit_breaker: CircuitBreaker::new(5, std::time::Duration::from_secs(60)),
            ws_connected: AtomicBool::new(false),
            ws_shutdown: Arc::new(RwLock::new(None)),
            heartbeat: HeartbeatMonitor::new(std::time::Duration::from_secs(30)),
            request_id: AtomicU64::new(1),
        }
    }

    fn next_request_id(&self) -> u64 {
        self.request_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Converts standard symbol to KuCoin futures format (e.g., BTC -> XBTUSDTM)
    fn to_kucoin_symbol(symbol: &str) -> String {
        // KuCoin futures use XBTUSDTM format for USDT-margined contracts
        let base = symbol.replace("/", "").replace("-", "").replace("USDT", "");
        format!("{}USDTM", base.to_uppercase())
    }

    /// Converts KuCoin futures symbol to standard format
    fn to_standard_symbol(symbol: &str) -> String {
        // Convert XBTUSDTM -> BTC
        symbol
            .replace("USDTM", "")
            .replace("XBT", "BTC")
            .to_uppercase()
    }
}

// =============================================================================
// REST API Types
// =============================================================================

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PlaceOrderRequest {
    client_oid: String,
    side: String,
    symbol: String,
    leverage: String,
    #[serde(rename = "type")]
    order_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    price: Option<String>,
    size: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    time_in_force: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    post_only: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reduce_only: Option<bool>,
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
    size: i64,
    deal_size: i64,
    deal_value: String,
    leverage: String,
    is_active: bool,
    #[serde(default)]
    cancel_exist: bool,
    created_at: u64,
    #[serde(default)]
    client_oid: Option<String>,
    #[serde(default)]
    reduce_only: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrdersResponse {
    items: Vec<OrderDetail>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AccountOverview {
    account_equity: f64,
    unrealised_pnl: f64,
    margin_balance: f64,
    position_margin: f64,
    order_margin: f64,
    frozen_funds: f64,
    available_balance: f64,
    currency: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PositionInfo {
    id: String,
    symbol: String,
    #[serde(default)]
    auto_deposit: bool,
    #[serde(default)]
    maint_margin_req: f64,
    #[serde(default)]
    risk_limit: i64,
    real_leverage: f64,
    cross_mode: bool,
    #[serde(default)]
    deleverage_percentile: Option<f64>,
    opening_timestamp: Option<u64>,
    current_timestamp: u64,
    current_qty: i64,
    current_cost: f64,
    current_comm: f64,
    unrealised_cost: f64,
    realised_gross_cost: f64,
    realised_cost: f64,
    is_open: bool,
    mark_price: f64,
    mark_value: f64,
    pos_cost: f64,
    pos_cross: f64,
    pos_init: f64,
    pos_comm: f64,
    pos_loss: f64,
    pos_margin: f64,
    pos_maint: f64,
    maint_margin: f64,
    realised_gross_pnl: f64,
    realised_pnl: f64,
    unrealised_pnl: f64,
    unrealised_pnl_pcnt: f64,
    unrealised_roe_pcnt: f64,
    avg_entry_price: f64,
    liquidation_price: f64,
    bankrupt_price: f64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ContractInfo {
    symbol: String,
    root_symbol: String,
    #[serde(rename = "type")]
    contract_type: String,
    base_currency: String,
    quote_currency: String,
    settle_currency: String,
    max_order_qty: i64,
    max_price: f64,
    lot_size: i64,
    tick_size: f64,
    index_price_tick_size: f64,
    multiplier: f64,
    initial_margin: f64,
    maint_margin_rate: f64,
    max_risk_limit: i64,
    min_risk_limit: i64,
    risk_step: i64,
    maker_fee_rate: f64,
    taker_fee_rate: f64,
    taker_fix_fee: f64,
    maker_fix_fee: f64,
    settlement_fee: Option<f64>,
    is_deleverage: bool,
    is_quanto: bool,
    is_inverse: bool,
    mark_method: String,
    fair_method: String,
    funding_base_symbol: String,
    funding_quote_symbol: String,
    funding_rate_symbol: String,
    index_symbol: String,
    settlement_symbol: Option<String>,
    status: String,
    funding_fee_rate: f64,
    predicted_funding_fee_rate: f64,
    open_interest: String,
    turnover_of24h: f64,
    volume_of24h: f64,
    mark_price: f64,
    index_price: f64,
    last_trade_price: f64,
    next_funding_rate_time: u64,
    max_leverage: i64,
    source_exchanges: Option<Vec<String>>,
    premium_index_symbol: Option<String>,
    low_price: f64,
    high_price: f64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TickerData {
    symbol: String,
    best_bid_price: f64,
    best_bid_size: i64,
    best_ask_price: f64,
    best_ask_size: i64,
    price: f64,
    size: i64,
    ts: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FundingRateData {
    symbol: String,
    granularity: u64,
    time_point: u64,
    value: f64,
    predicted_value: f64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FundingHistoryItem {
    id: u64,
    symbol: String,
    time_point: u64,
    funding_rate: f64,
    mark_price: f64,
    position_qty: i64,
    position_cost: f64,
    funding: f64,
    settle_currency: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FundingHistoryResponse {
    data_list: Vec<FundingHistoryItem>,
}

#[derive(Debug, Deserialize)]
struct KlineData(Vec<serde_json::Value>);

// =============================================================================
// PerpRest Implementation
// =============================================================================

#[async_trait]
impl PerpRest for KucoinPerpsAdapter {
    async fn create_order(&self, order: NewOrder) -> Result<Order> {
        self.rate_limiter.acquire().await;

        let symbol = Self::to_kucoin_symbol(&order.symbol);

        // Convert qty to contract size (number of contracts)
        let size = order.qty as i64;

        let request = PlaceOrderRequest {
            client_oid: if order.client_order_id.is_empty() {
                uuid::Uuid::new_v4().to_string()
            } else {
                order.client_order_id.clone()
            },
            side: converters::to_kucoin_side(order.side).to_string(),
            symbol: symbol.clone(),
            leverage: "10".to_string(), // Default leverage
            order_type: converters::to_kucoin_order_type(order.ord_type).to_string(),
            price: order.price.map(|p| p.to_string()),
            size,
            time_in_force: order.tif.map(|t| converters::to_kucoin_tif(t).to_string()),
            post_only: if order.post_only { Some(true) } else { None },
            reduce_only: if order.reduce_only { Some(true) } else { None },
        };

        let response: KucoinResponse<PlaceOrderResponse> = self
            .rest_client
            .post_private("/api/v1/orders", &request)
            .await?;

        let data = response.into_result()?;

        Ok(Order {
            venue_order_id: data.order_id,
            client_order_id: order.client_order_id,
            symbol: order.symbol,
            side: order.side,
            ord_type: order.ord_type,
            qty: order.qty,
            filled_qty: 0.0,
            price: order.price,
            avg_price: None,
            status: OrderStatus::New,
            created_at: KucoinAuth::timestamp_ms(),
            updated_at: KucoinAuth::timestamp_ms(),
        })
    }

    async fn cancel_order(&self, symbol: &str, order_id: &str) -> Result<()> {
        self.rate_limiter.acquire().await;
        let _symbol = Self::to_kucoin_symbol(symbol);

        let endpoint = format!("/api/v1/orders/{}", order_id);
        let _response: KucoinResponse<serde_json::Value> = self
            .rest_client
            .delete_private(&endpoint, None)
            .await?;

        Ok(())
    }

    async fn cancel_all(&self, symbol: &str) -> Result<u32> {
        self.rate_limiter.acquire().await;
        let kucoin_symbol = Self::to_kucoin_symbol(symbol);

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), kucoin_symbol);

        let response: KucoinResponse<serde_json::Value> = self
            .rest_client
            .delete_private("/api/v1/orders", Some(params))
            .await?;

        let data = response.into_result()?;
        let cancelled = data
            .get("cancelledOrderIds")
            .and_then(|v| v.as_array())
            .map(|arr| arr.len() as u32)
            .unwrap_or(0);

        Ok(cancelled)
    }

    async fn get_order(&self, symbol: &str, order_id: &str) -> Result<Order> {
        self.rate_limiter.acquire().await;
        let _symbol = Self::to_kucoin_symbol(symbol);

        let endpoint = format!("/api/v1/orders/{}", order_id);
        let response: KucoinResponse<OrderDetail> =
            self.rest_client.get_private(&endpoint, None).await?;

        let data = response.into_result()?;
        Ok(convert_order_detail(&data))
    }

    async fn get_open_orders(&self, symbol: Option<&str>) -> Result<Vec<Order>> {
        self.rate_limiter.acquire().await;

        let mut params = HashMap::new();
        params.insert("status".to_string(), "active".to_string());
        if let Some(s) = symbol {
            params.insert("symbol".to_string(), Self::to_kucoin_symbol(s));
        }

        let response: KucoinResponse<OrdersResponse> = self
            .rest_client
            .get_private("/api/v1/orders", Some(params))
            .await?;

        let data = response.into_result()?;
        Ok(data.items.iter().map(convert_order_detail).collect())
    }

    async fn replace_order(&self, symbol: &str, order_id: &str, order: NewOrder) -> Result<Order> {
        self.cancel_order(symbol, order_id).await?;
        self.create_order(order).await
    }

    async fn create_batch_orders(&self, batch: BatchOrderRequest) -> Result<BatchOrderResult> {
        let mut success = Vec::new();
        let mut failed = Vec::new();

        for order in batch.orders {
            match self.create_order(order.clone()).await {
                Ok(o) => success.push(o),
                Err(e) => failed.push((order.client_order_id.clone(), e.to_string())),
            }
        }

        Ok(BatchOrderResult { success, failed })
    }

    async fn cancel_batch_orders(&self, batch: BatchCancelRequest) -> Result<BatchCancelResult> {
        let mut success = Vec::new();
        let mut failed = Vec::new();

        for (symbol, order_id) in &batch.orders {
            match self.cancel_order(symbol, order_id).await {
                Ok(()) => success.push(order_id.clone()),
                Err(e) => failed.push((order_id.clone(), e.to_string())),
            }
        }

        Ok(BatchCancelResult { success, failed })
    }

    async fn get_balances(&self) -> Result<Vec<Balance>> {
        self.rate_limiter.acquire().await;

        let mut params = HashMap::new();
        params.insert("currency".to_string(), "USDT".to_string());

        let response: KucoinResponse<AccountOverview> = self
            .rest_client
            .get_private("/api/v1/account-overview", Some(params))
            .await?;

        let data = response.into_result()?;

        Ok(vec![Balance {
            asset: data.currency,
            free: data.available_balance,
            locked: data.frozen_funds + data.position_margin + data.order_margin,
            total: data.margin_balance,
        }])
    }

    async fn get_account_info(&self) -> Result<AccountInfo> {
        let balances = self.get_balances().await?;
        Ok(AccountInfo {
            account_id: "kucoin_futures".to_string(),
            balances,
            permissions: vec!["FUTURES".to_string()],
        })
    }

    async fn get_market_info(&self, symbol: &str) -> Result<Market> {
        self.rate_limiter.acquire().await;
        let kucoin_symbol = Self::to_kucoin_symbol(symbol);

        let endpoint = format!("/api/v1/contracts/{}", kucoin_symbol);
        let response: KucoinResponse<ContractInfo> =
            self.rest_client.get_public(&endpoint, None).await?;

        let data = response.into_result()?;

        Ok(Market {
            symbol: Self::to_standard_symbol(&data.symbol),
            base_asset: data.base_currency,
            quote_asset: data.quote_currency,
            price_precision: count_decimals_f64(data.tick_size),
            qty_precision: 0, // Contract-based
            min_qty: data.lot_size as f64,
            max_qty: Some(data.max_order_qty as f64),
            min_notional: None,
            tick_size: data.tick_size,
            lot_size: data.lot_size as f64,
            is_trading: data.status == "Open",
        })
    }

    async fn get_all_markets(&self) -> Result<Vec<Market>> {
        self.rate_limiter.acquire().await;

        let response: KucoinResponse<Vec<ContractInfo>> = self
            .rest_client
            .get_public("/api/v1/contracts/active", None)
            .await?;

        let data = response.into_result()?;

        Ok(data
            .iter()
            .map(|info| Market {
                symbol: Self::to_standard_symbol(&info.symbol),
                base_asset: info.base_currency.clone(),
                quote_asset: info.quote_currency.clone(),
                price_precision: count_decimals_f64(info.tick_size),
                qty_precision: 0,
                min_qty: info.lot_size as f64,
                max_qty: Some(info.max_order_qty as f64),
                min_notional: None,
                tick_size: info.tick_size,
                lot_size: info.lot_size as f64,
                is_trading: info.status == "Open",
            })
            .collect())
    }

    async fn get_ticker(&self, symbol: &str) -> Result<Ticker> {
        self.rate_limiter.acquire().await;
        let kucoin_symbol = Self::to_kucoin_symbol(symbol);

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), kucoin_symbol);

        let response: KucoinResponse<TickerData> = self
            .rest_client
            .get_public("/api/v1/ticker", Some(params))
            .await?;

        let data = response.into_result()?;

        Ok(Ticker {
            symbol: Self::to_standard_symbol(&data.symbol),
            bid_price: data.best_bid_price,
            bid_qty: data.best_bid_size as f64,
            ask_price: data.best_ask_price,
            ask_qty: data.best_ask_size as f64,
            last_price: data.price,
            volume_24h: data.size as f64,
            timestamp: data.ts,
        })
    }

    async fn get_tickers(&self) -> Result<Vec<Ticker>> {
        // KuCoin doesn't have a batch ticker endpoint for futures
        // Would need to fetch contracts and call individual tickers
        let markets = self.get_all_markets().await?;
        let mut tickers = Vec::new();

        for market in markets.iter().take(10) {
            // Limit to avoid rate limiting
            if let Ok(ticker) = self.get_ticker(&market.symbol).await {
                tickers.push(ticker);
            }
        }

        Ok(tickers)
    }

    async fn get_klines(
        &self,
        symbol: &str,
        interval: &str,
        limit: Option<u32>,
    ) -> Result<Vec<Kline>> {
        self.rate_limiter.acquire().await;
        let kucoin_symbol = Self::to_kucoin_symbol(symbol);

        // Convert interval to granularity in minutes
        let granularity = match interval {
            "1m" => 1,
            "5m" => 5,
            "15m" => 15,
            "30m" => 30,
            "1h" | "60m" => 60,
            "2h" => 120,
            "4h" => 240,
            "8h" => 480,
            "12h" => 720,
            "1d" => 1440,
            "1w" => 10080,
            _ => 60,
        };

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), kucoin_symbol);
        params.insert("granularity".to_string(), granularity.to_string());

        let response: KucoinResponse<Vec<KlineData>> = self
            .rest_client
            .get_public("/api/v1/kline/query", Some(params))
            .await?;

        let data = response.into_result()?;
        let limit = limit.unwrap_or(200) as usize;

        Ok(data
            .iter()
            .take(limit)
            .filter_map(|k| {
                if k.0.len() >= 6 {
                    Some(Kline {
                        open_time: k.0[0].as_u64()? * 1000,
                        open: k.0[1].as_f64()?,
                        high: k.0[2].as_f64()?,
                        low: k.0[3].as_f64()?,
                        close: k.0[4].as_f64()?,
                        volume: k.0[5].as_f64()?,
                        close_time: (k.0[0].as_u64()? + (granularity as u64 * 60)) * 1000,
                    })
                } else {
                    None
                }
            })
            .collect())
    }

    // Perp-specific methods

    async fn set_leverage(&self, symbol: &str, leverage: Decimal) -> Result<()> {
        self.rate_limiter.acquire().await;

        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct LeverageRequest {
            symbol: String,
            leverage: String,
        }

        let request = LeverageRequest {
            symbol: Self::to_kucoin_symbol(symbol),
            leverage: leverage.to_string(),
        };

        let _response: KucoinResponse<serde_json::Value> = self
            .rest_client
            .post_private("/api/v1/position/risk-limit-level/change", &request)
            .await?;

        Ok(())
    }

    async fn set_margin_mode(&self, _symbol: &str, _mode: MarginMode) -> Result<()> {
        // KuCoin uses cross/isolated mode at position level
        // Not directly settable via API, determined by auto-deposit setting
        Ok(())
    }

    async fn get_position(&self, symbol: &str) -> Result<Position> {
        self.rate_limiter.acquire().await;
        let kucoin_symbol = Self::to_kucoin_symbol(symbol);

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), kucoin_symbol);

        let response: KucoinResponse<PositionInfo> = self
            .rest_client
            .get_private("/api/v1/position", Some(params))
            .await?;

        let data = response.into_result()?;
        Ok(convert_position(&data))
    }

    async fn get_all_positions(&self) -> Result<Vec<Position>> {
        self.rate_limiter.acquire().await;

        let response: KucoinResponse<Vec<PositionInfo>> = self
            .rest_client
            .get_private("/api/v1/positions", None)
            .await?;

        let data = response.into_result()?;
        Ok(data
            .iter()
            .filter(|p| p.is_open && p.current_qty != 0)
            .map(convert_position)
            .collect())
    }

    async fn get_funding_rate(&self, symbol: &str) -> Result<(Decimal, u64)> {
        self.rate_limiter.acquire().await;
        let kucoin_symbol = Self::to_kucoin_symbol(symbol);

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), kucoin_symbol);

        let response: KucoinResponse<FundingRateData> = self
            .rest_client
            .get_public("/api/v1/funding-rate/{}/current", Some(params))
            .await?;

        let data = response.into_result()?;
        Ok((
            Decimal::try_from(data.value).unwrap_or_default(),
            data.time_point,
        ))
    }

    async fn get_mark_price(&self, symbol: &str) -> Result<(Decimal, u64)> {
        self.rate_limiter.acquire().await;
        let kucoin_symbol = Self::to_kucoin_symbol(symbol);

        let endpoint = format!("/api/v1/contracts/{}", kucoin_symbol);
        let response: KucoinResponse<ContractInfo> =
            self.rest_client.get_public(&endpoint, None).await?;

        let data = response.into_result()?;
        Ok((
            Decimal::try_from(data.mark_price).unwrap_or_default(),
            KucoinAuth::timestamp_ms(),
        ))
    }

    async fn get_index_price(&self, symbol: &str) -> Result<(Decimal, u64)> {
        self.rate_limiter.acquire().await;
        let kucoin_symbol = Self::to_kucoin_symbol(symbol);

        let endpoint = format!("/api/v1/contracts/{}", kucoin_symbol);
        let response: KucoinResponse<ContractInfo> =
            self.rest_client.get_public(&endpoint, None).await?;

        let data = response.into_result()?;
        Ok((
            Decimal::try_from(data.index_price).unwrap_or_default(),
            KucoinAuth::timestamp_ms(),
        ))
    }

    async fn get_funding_history(
        &self,
        symbol: &str,
        limit: Option<u32>,
    ) -> Result<Vec<FundingPayment>> {
        self.rate_limiter.acquire().await;
        let kucoin_symbol = Self::to_kucoin_symbol(symbol);

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), kucoin_symbol);
        if let Some(l) = limit {
            params.insert("maxCount".to_string(), l.to_string());
        }

        let response: KucoinResponse<FundingHistoryResponse> = self
            .rest_client
            .get_private("/api/v1/funding-history", Some(params))
            .await?;

        let data = response.into_result()?;
        Ok(data
            .data_list
            .iter()
            .map(|f| FundingPayment {
                symbol: Self::to_standard_symbol(&f.symbol),
                funding_rate: Decimal::try_from(f.funding_rate).unwrap_or_default(),
                payment: Decimal::try_from(f.funding).unwrap_or_default(),
                timestamp: f.time_point,
            })
            .collect())
    }
}

// =============================================================================
// PerpWs Implementation
// =============================================================================

#[async_trait]
impl PerpWs for KucoinPerpsAdapter {
    async fn subscribe_user(&self) -> Result<mpsc::Receiver<UserEvent>> {
        let (tx, rx) = mpsc::channel(1000);

        let token_response = self.rest_client.get_ws_private_token().await?;
        let ws_url = format!(
            "{}?token={}&connectId={}",
            token_response.endpoint,
            token_response.token,
            self.next_request_id()
        );

        let shutdown = Arc::clone(&self.ws_shutdown);
        let reconnect = ReconnectStrategy::new(3, std::time::Duration::from_secs(5));

        tokio::spawn(async move {
            let mut attempt = 0;
            loop {
                match connect_async(&ws_url).await {
                    Ok((ws_stream, _)) => {
                        attempt = 0;
                        let (mut write, mut read) = ws_stream.split();

                        // Subscribe to private channels
                        let sub_msg = serde_json::json!({
                            "id": uuid::Uuid::new_v4().to_string(),
                            "type": "subscribe",
                            "topic": "/contractMarket/tradeOrders",
                            "privateChannel": true,
                            "response": true
                        });
                        let _ = write.send(Message::Text(sub_msg.to_string())).await;

                        let (shutdown_tx, mut shutdown_rx) = broadcast::channel(1);
                        {
                            let mut guard = shutdown.write().await;
                            *guard = Some(shutdown_tx);
                        }

                        let ping_interval =
                            tokio::time::Duration::from_millis(token_response.ping_interval);
                        let mut ping_timer = tokio::time::interval(ping_interval);

                        loop {
                            tokio::select! {
                                _ = shutdown_rx.recv() => break,
                                _ = ping_timer.tick() => {
                                    let ping = serde_json::json!({
                                        "id": uuid::Uuid::new_v4().to_string(),
                                        "type": "ping"
                                    });
                                    let _ = write.send(Message::Text(ping.to_string())).await;
                                }
                                Some(msg) = read.next() => {
                                    match msg {
                                        Ok(Message::Text(text)) => {
                                            if let Ok(data) = serde_json::from_str::<serde_json::Value>(&text) {
                                                if let Some(event) = parse_user_event(&data) {
                                                    let _ = tx.send(event).await;
                                                }
                                            }
                                        }
                                        Ok(Message::Close(_)) => break,
                                        Err(_) => break,
                                        _ => {}
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => {
                        attempt += 1;
                        if !reconnect.should_retry(attempt) {
                            break;
                        }
                        tokio::time::sleep(reconnect.delay(attempt)).await;
                    }
                }
            }
        });

        self.ws_connected.store(true, Ordering::SeqCst);
        Ok(rx)
    }

    async fn subscribe_books(&self, symbols: &[&str]) -> Result<mpsc::Receiver<BookUpdate>> {
        let (tx, rx) = mpsc::channel(1000);

        let token_response = self.rest_client.get_ws_public_token().await?;
        let ws_url = format!(
            "{}?token={}&connectId={}",
            token_response.endpoint,
            token_response.token,
            self.next_request_id()
        );

        let symbols: Vec<String> = symbols.iter().map(|s| Self::to_kucoin_symbol(s)).collect();
        let shutdown = Arc::clone(&self.ws_shutdown);
        let reconnect = ReconnectStrategy::new(3, std::time::Duration::from_secs(5));

        tokio::spawn(async move {
            let mut attempt = 0;
            loop {
                match connect_async(&ws_url).await {
                    Ok((ws_stream, _)) => {
                        attempt = 0;
                        let (mut write, mut read) = ws_stream.split();

                        // Subscribe to order book channels
                        for symbol in &symbols {
                            let sub_msg = serde_json::json!({
                                "id": uuid::Uuid::new_v4().to_string(),
                                "type": "subscribe",
                                "topic": format!("/contractMarket/level2:{}", symbol),
                                "response": true
                            });
                            let _ = write.send(Message::Text(sub_msg.to_string())).await;
                        }

                        let (shutdown_tx, mut shutdown_rx) = broadcast::channel(1);
                        {
                            let mut guard = shutdown.write().await;
                            *guard = Some(shutdown_tx);
                        }

                        let ping_interval =
                            tokio::time::Duration::from_millis(token_response.ping_interval);
                        let mut ping_timer = tokio::time::interval(ping_interval);

                        loop {
                            tokio::select! {
                                _ = shutdown_rx.recv() => break,
                                _ = ping_timer.tick() => {
                                    let ping = serde_json::json!({
                                        "id": uuid::Uuid::new_v4().to_string(),
                                        "type": "ping"
                                    });
                                    let _ = write.send(Message::Text(ping.to_string())).await;
                                }
                                Some(msg) = read.next() => {
                                    match msg {
                                        Ok(Message::Text(text)) => {
                                            if let Ok(data) = serde_json::from_str::<serde_json::Value>(&text) {
                                                if let Some(update) = parse_book_update(&data) {
                                                    let _ = tx.send(update).await;
                                                }
                                            }
                                        }
                                        Ok(Message::Close(_)) => break,
                                        Err(_) => break,
                                        _ => {}
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => {
                        attempt += 1;
                        if !reconnect.should_retry(attempt) {
                            break;
                        }
                        tokio::time::sleep(reconnect.delay(attempt)).await;
                    }
                }
            }
        });

        self.ws_connected.store(true, Ordering::SeqCst);
        Ok(rx)
    }

    async fn subscribe_trades(&self, symbols: &[&str]) -> Result<mpsc::Receiver<Trade>> {
        let (tx, rx) = mpsc::channel(1000);

        let token_response = self.rest_client.get_ws_public_token().await?;
        let ws_url = format!(
            "{}?token={}&connectId={}",
            token_response.endpoint,
            token_response.token,
            self.next_request_id()
        );

        let symbols: Vec<String> = symbols.iter().map(|s| Self::to_kucoin_symbol(s)).collect();
        let shutdown = Arc::clone(&self.ws_shutdown);
        let reconnect = ReconnectStrategy::new(3, std::time::Duration::from_secs(5));

        tokio::spawn(async move {
            let mut attempt = 0;
            loop {
                match connect_async(&ws_url).await {
                    Ok((ws_stream, _)) => {
                        attempt = 0;
                        let (mut write, mut read) = ws_stream.split();

                        // Subscribe to execution/match channels
                        for symbol in &symbols {
                            let sub_msg = serde_json::json!({
                                "id": uuid::Uuid::new_v4().to_string(),
                                "type": "subscribe",
                                "topic": format!("/contractMarket/execution:{}", symbol),
                                "response": true
                            });
                            let _ = write.send(Message::Text(sub_msg.to_string())).await;
                        }

                        let (shutdown_tx, mut shutdown_rx) = broadcast::channel(1);
                        {
                            let mut guard = shutdown.write().await;
                            *guard = Some(shutdown_tx);
                        }

                        let ping_interval =
                            tokio::time::Duration::from_millis(token_response.ping_interval);
                        let mut ping_timer = tokio::time::interval(ping_interval);

                        loop {
                            tokio::select! {
                                _ = shutdown_rx.recv() => break,
                                _ = ping_timer.tick() => {
                                    let ping = serde_json::json!({
                                        "id": uuid::Uuid::new_v4().to_string(),
                                        "type": "ping"
                                    });
                                    let _ = write.send(Message::Text(ping.to_string())).await;
                                }
                                Some(msg) = read.next() => {
                                    match msg {
                                        Ok(Message::Text(text)) => {
                                            if let Ok(data) = serde_json::from_str::<serde_json::Value>(&text) {
                                                if let Some(trade) = parse_trade(&data) {
                                                    let _ = tx.send(trade).await;
                                                }
                                            }
                                        }
                                        Ok(Message::Close(_)) => break,
                                        Err(_) => break,
                                        _ => {}
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => {
                        attempt += 1;
                        if !reconnect.should_retry(attempt) {
                            break;
                        }
                        tokio::time::sleep(reconnect.delay(attempt)).await;
                    }
                }
            }
        });

        self.ws_connected.store(true, Ordering::SeqCst);
        Ok(rx)
    }

    async fn subscribe_positions(&self) -> Result<mpsc::Receiver<PositionUpdate>> {
        let (tx, rx) = mpsc::channel(1000);

        let token_response = self.rest_client.get_ws_private_token().await?;
        let ws_url = format!(
            "{}?token={}&connectId={}",
            token_response.endpoint,
            token_response.token,
            self.next_request_id()
        );

        let shutdown = Arc::clone(&self.ws_shutdown);
        let reconnect = ReconnectStrategy::new(3, std::time::Duration::from_secs(5));

        tokio::spawn(async move {
            let mut attempt = 0;
            loop {
                match connect_async(&ws_url).await {
                    Ok((ws_stream, _)) => {
                        attempt = 0;
                        let (mut write, mut read) = ws_stream.split();

                        // Subscribe to position changes
                        let sub_msg = serde_json::json!({
                            "id": uuid::Uuid::new_v4().to_string(),
                            "type": "subscribe",
                            "topic": "/contract/position:all",
                            "privateChannel": true,
                            "response": true
                        });
                        let _ = write.send(Message::Text(sub_msg.to_string())).await;

                        let (shutdown_tx, mut shutdown_rx) = broadcast::channel(1);
                        {
                            let mut guard = shutdown.write().await;
                            *guard = Some(shutdown_tx);
                        }

                        let ping_interval =
                            tokio::time::Duration::from_millis(token_response.ping_interval);
                        let mut ping_timer = tokio::time::interval(ping_interval);

                        loop {
                            tokio::select! {
                                _ = shutdown_rx.recv() => break,
                                _ = ping_timer.tick() => {
                                    let ping = serde_json::json!({
                                        "id": uuid::Uuid::new_v4().to_string(),
                                        "type": "ping"
                                    });
                                    let _ = write.send(Message::Text(ping.to_string())).await;
                                }
                                Some(msg) = read.next() => {
                                    match msg {
                                        Ok(Message::Text(text)) => {
                                            if let Ok(data) = serde_json::from_str::<serde_json::Value>(&text) {
                                                if let Some(update) = parse_position_update(&data) {
                                                    let _ = tx.send(update).await;
                                                }
                                            }
                                        }
                                        Ok(Message::Close(_)) => break,
                                        Err(_) => break,
                                        _ => {}
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => {
                        attempt += 1;
                        if !reconnect.should_retry(attempt) {
                            break;
                        }
                        tokio::time::sleep(reconnect.delay(attempt)).await;
                    }
                }
            }
        });

        self.ws_connected.store(true, Ordering::SeqCst);
        Ok(rx)
    }

    async fn health(&self) -> Result<bool> {
        Ok(self.ws_connected.load(Ordering::SeqCst) && self.heartbeat.is_healthy())
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

fn convert_order_detail(detail: &OrderDetail) -> Order {
    let filled_qty = detail.deal_size as f64;
    let total_qty = detail.size as f64;

    let status = if detail.cancel_exist {
        OrderStatus::Canceled
    } else if filled_qty >= total_qty && total_qty > 0.0 {
        OrderStatus::Filled
    } else if filled_qty > 0.0 {
        OrderStatus::PartiallyFilled
    } else if detail.is_active {
        OrderStatus::New
    } else {
        OrderStatus::Filled
    };

    Order {
        venue_order_id: detail.id.clone(),
        client_order_id: detail.client_oid.clone().unwrap_or_default(),
        symbol: KucoinPerpsAdapter::to_standard_symbol(&detail.symbol),
        side: converters::from_kucoin_side(&detail.side),
        ord_type: converters::from_kucoin_order_type(&detail.order_type),
        qty: total_qty,
        filled_qty,
        price: detail.price.parse().ok(),
        avg_price: if filled_qty > 0.0 {
            let deal_value: f64 = detail.deal_value.parse().unwrap_or(0.0);
            Some(deal_value / filled_qty)
        } else {
            None
        },
        status,
        created_at: detail.created_at,
        updated_at: detail.created_at,
    }
}

fn convert_position(pos: &PositionInfo) -> Position {
    let side = if pos.current_qty > 0 {
        Side::Buy
    } else if pos.current_qty < 0 {
        Side::Sell
    } else {
        Side::Buy
    };

    Position {
        symbol: KucoinPerpsAdapter::to_standard_symbol(&pos.symbol),
        side,
        qty: pos.current_qty.abs() as f64,
        entry_px: Decimal::try_from(pos.avg_entry_price).unwrap_or_default(),
        mark_px: Some(Decimal::try_from(pos.mark_price).unwrap_or_default()),
        liquidation_px: if pos.liquidation_price > 0.0 {
            Some(Decimal::try_from(pos.liquidation_price).unwrap_or_default())
        } else {
            None
        },
        unrealized_pnl: Some(Decimal::try_from(pos.unrealised_pnl).unwrap_or_default()),
        realized_pnl: Some(Decimal::try_from(pos.realised_pnl).unwrap_or_default()),
        leverage: Decimal::try_from(pos.real_leverage).unwrap_or_default(),
        margin_mode: if pos.cross_mode {
            MarginMode::Cross
        } else {
            MarginMode::Isolated
        },
        updated_at: pos.current_timestamp,
    }
}

fn count_decimals_f64(val: f64) -> u8 {
    let s = format!("{}", val);
    if let Some(pos) = s.find('.') {
        (s.len() - pos - 1) as u8
    } else {
        0
    }
}

fn parse_user_event(data: &serde_json::Value) -> Option<UserEvent> {
    let topic = data.get("topic")?.as_str()?;
    let subject = data.get("subject")?.as_str()?;
    let msg_data = data.get("data")?;

    if topic == "/contractMarket/tradeOrders" {
        match subject {
            "orderChange" => {
                let order = Order {
                    venue_order_id: msg_data.get("orderId")?.as_str()?.to_string(),
                    client_order_id: msg_data
                        .get("clientOid")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    symbol: KucoinPerpsAdapter::to_standard_symbol(
                        msg_data.get("symbol")?.as_str()?,
                    ),
                    side: converters::from_kucoin_side(msg_data.get("side")?.as_str()?),
                    ord_type: converters::from_kucoin_order_type(
                        msg_data.get("orderType")?.as_str()?,
                    ),
                    qty: msg_data.get("size").and_then(|v| v.as_i64()).unwrap_or(0) as f64,
                    filled_qty: msg_data
                        .get("filledSize")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0) as f64,
                    price: msg_data
                        .get("price")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse().ok()),
                    avg_price: None,
                    status: converters::from_kucoin_order_status(
                        msg_data.get("status")?.as_str()?,
                        msg_data
                            .get("status")
                            .and_then(|v| v.as_str())
                            .map(|s| s == "open" || s == "match")
                            .unwrap_or(false),
                    ),
                    created_at: msg_data.get("orderTime").and_then(|v| v.as_u64()).unwrap_or(0),
                    updated_at: msg_data.get("ts").and_then(|v| v.as_u64()).unwrap_or(0),
                };
                Some(UserEvent::OrderUpdate(order))
            }
            _ => None,
        }
    } else {
        None
    }
}

fn parse_book_update(data: &serde_json::Value) -> Option<BookUpdate> {
    let topic = data.get("topic")?.as_str()?;
    let msg_data = data.get("data")?;

    if topic.starts_with("/contractMarket/level2:") {
        let symbol = topic.strip_prefix("/contractMarket/level2:")?.to_string();

        let change = msg_data.get("change")?.as_str()?;
        let parts: Vec<&str> = change.split(',').collect();

        if parts.len() >= 3 {
            let price: Decimal = parts[0].parse().ok()?;
            let side = parts[1];
            let qty: Decimal = parts[2].parse().ok()?;

            let (bids, asks) = if side == "buy" {
                (vec![(price, qty)], vec![])
            } else {
                (vec![], vec![(price, qty)])
            };

            Some(BookUpdate::DepthDelta {
                symbol: KucoinPerpsAdapter::to_standard_symbol(&symbol),
                bids,
                asks,
                timestamp: msg_data.get("sequence").and_then(|v| v.as_u64()).unwrap_or(0),
            })
        } else {
            None
        }
    } else {
        None
    }
}

fn parse_trade(data: &serde_json::Value) -> Option<Trade> {
    let topic = data.get("topic")?.as_str()?;
    let msg_data = data.get("data")?;

    if topic.starts_with("/contractMarket/execution:") {
        let symbol = topic
            .strip_prefix("/contractMarket/execution:")?
            .to_string();

        Some(Trade {
            symbol: KucoinPerpsAdapter::to_standard_symbol(&symbol),
            trade_id: msg_data.get("tradeId")?.as_str()?.to_string(),
            px: msg_data
                .get("price")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0),
            qty: msg_data.get("size").and_then(|v| v.as_i64()).unwrap_or(0) as f64,
            taker_is_buy: msg_data
                .get("side")
                .and_then(|v| v.as_str())
                .map(|s| s == "buy")
                .unwrap_or(false),
            timestamp: msg_data.get("ts").and_then(|v| v.as_u64()).unwrap_or(0),
        })
    } else {
        None
    }
}

fn parse_position_update(data: &serde_json::Value) -> Option<PositionUpdate> {
    let topic = data.get("topic")?.as_str()?;
    let msg_data = data.get("data")?;

    if topic.starts_with("/contract/position:") {
        let qty = msg_data.get("currentQty").and_then(|v| v.as_i64()).unwrap_or(0);
        let side = if qty > 0 { Side::Buy } else { Side::Sell };

        Some(PositionUpdate {
            symbol: KucoinPerpsAdapter::to_standard_symbol(msg_data.get("symbol")?.as_str()?),
            side,
            qty: qty.abs() as f64,
            entry_px: Decimal::try_from(
                msg_data
                    .get("avgEntryPrice")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0),
            )
            .unwrap_or_default(),
            unrealized_pnl: Decimal::try_from(
                msg_data
                    .get("unrealisedPnl")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0),
            )
            .unwrap_or_default(),
            timestamp: msg_data.get("ts").and_then(|v| v.as_u64()).unwrap_or(0),
        })
    } else {
        None
    }
}
