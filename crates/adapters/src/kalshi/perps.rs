//! Bybit Linear Perpetuals Market Adapter
//!
//! Provides REST and WebSocket functionality for Bybit V5 linear perpetual trading.
//!
//! # Features
//!
//! - **Position Management**: Long/short positions with leverage up to 100x
//! - **Order Management**: Market, limit, stop-loss, take-profit orders
//! - **Margin Modes**: Cross margin and isolated margin support
//! - **Funding Rates**: Query current and historical funding rates
//! - **Real-Time Data**: WebSocket streams for positions, orders, and market data
//!
//! # API Documentation
//!
//! - V5 Linear: <https://bybit-exchange.github.io/docs/v5/linear>

use crate::bybit::account::{
    converters, BybitAuth, BybitRestClient, BybitResponse,
    BYBIT_WS_LINEAR_URL, BYBIT_WS_PRIVATE_URL,
};
use crate::traits::*;
use crate::utils::{
    CircuitBreaker, CircuitBreakerConfig, HeartbeatConfig, HeartbeatMonitor,
    RateLimiter, RateLimiterConfig, ReconnectConfig, ReconnectStrategy,
};
use anyhow::{Context, Result};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{debug, error, info, warn};

/// Bybit Linear Perpetuals adapter combining REST and WebSocket
#[derive(Clone)]
pub struct BybitPerpsAdapter {
    client: BybitRestClient,
    auth: BybitAuth,
    connection_status: Arc<RwLock<ConnectionStatus>>,
    rate_limiter: RateLimiter,
    circuit_breaker: CircuitBreaker,
    reconnect_count: Arc<AtomicU32>,
    shutdown_tx: Arc<Mutex<Option<tokio::sync::broadcast::Sender<()>>>>,
}

impl BybitPerpsAdapter {
    /// Creates a new Bybit linear perpetuals adapter
    pub fn new(api_key: String, api_secret: String) -> Self {
        let auth = BybitAuth::new(api_key, api_secret);
        Self {
            client: BybitRestClient::new(Some(auth.clone())),
            auth,
            connection_status: Arc::new(RwLock::new(ConnectionStatus::Disconnected)),
            rate_limiter: RateLimiter::new(RateLimiterConfig::default()),
            circuit_breaker: CircuitBreaker::new("bybit_perps", CircuitBreakerConfig::production()),
            reconnect_count: Arc::new(AtomicU32::new(0)),
            shutdown_tx: Arc::new(Mutex::new(None)),
        }
    }

    /// Generates client order ID
    fn generate_order_link_id() -> String {
        format!("mm_{}", BybitAuth::timestamp())
    }

    fn now_millis() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
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

        debug!("Calling Bybit Perps API endpoint: {}", endpoint);

        match self.circuit_breaker.call(f).await {
            Ok(result) => Ok(result),
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("10006") || err_str.to_lowercase().contains("rate limit") {
                    warn!("Rate limit error detected on {}", endpoint);
                    self.rate_limiter.handle_rate_limit_error().await;
                }
                Err(e)
            }
        }
    }

    /// Shutdown the adapter
    pub async fn shutdown(&self) {
        info!("Initiating graceful shutdown of Bybit Perps adapter");
        if let Some(tx) = self.shutdown_tx.lock().await.take() {
            let _ = tx.send(());
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        info!("Bybit Perps adapter shutdown complete");
    }
}

// =============================================================================
// REST API Response Types
// =============================================================================

#[derive(Debug, Deserialize)]
struct OrderResult {
    #[serde(rename = "orderId")]
    order_id: String,
    #[serde(rename = "orderLinkId")]
    order_link_id: String,
}

#[derive(Debug, Serialize)]
struct CreateOrderRequest {
    category: String,
    symbol: String,
    side: String,
    #[serde(rename = "orderType")]
    order_type: String,
    qty: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    price: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "timeInForce")]
    time_in_force: Option<String>,
    #[serde(rename = "orderLinkId")]
    order_link_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "reduceOnly")]
    reduce_only: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "positionIdx")]
    position_idx: Option<i32>,
}

#[derive(Debug, Deserialize)]
struct PositionResult {
    list: Vec<PositionItem>,
}

#[derive(Debug, Deserialize)]
struct PositionItem {
    symbol: String,
    side: String,
    size: String,
    #[serde(rename = "avgPrice")]
    avg_price: String,
    #[serde(rename = "markPrice")]
    mark_price: String,
    #[serde(rename = "liqPrice")]
    liq_price: String,
    #[serde(rename = "unrealisedPnl")]
    unrealised_pnl: String,
    #[serde(rename = "cumRealisedPnl")]
    cum_realised_pnl: String,
    #[serde(rename = "positionIM")]
    position_im: String,
    leverage: String,
    #[serde(rename = "updatedTime")]
    updated_time: String,
}

#[derive(Debug, Deserialize)]
struct TickerResult {
    list: Vec<TickerItem>,
}

#[derive(Debug, Deserialize)]
struct TickerItem {
    symbol: String,
    #[serde(rename = "lastPrice")]
    last_price: String,
    #[serde(rename = "bid1Price")]
    bid1_price: String,
    #[serde(rename = "ask1Price")]
    ask1_price: String,
    #[serde(rename = "volume24h")]
    volume_24h: String,
    #[serde(rename = "highPrice24h")]
    high_price_24h: String,
    #[serde(rename = "lowPrice24h")]
    low_price_24h: String,
    #[serde(rename = "prevPrice24h")]
    prev_price_24h: String,
    #[serde(rename = "price24hPcnt")]
    price_24h_pcnt: String,
    #[serde(rename = "markPrice")]
    mark_price: String,
    #[serde(rename = "indexPrice")]
    index_price: String,
    #[serde(rename = "fundingRate")]
    funding_rate: String,
    #[serde(rename = "nextFundingTime")]
    next_funding_time: String,
}

// =============================================================================
// PerpRest Implementation
// =============================================================================

#[async_trait::async_trait]
impl PerpRest for BybitPerpsAdapter {
    async fn create_order(&self, new: NewOrder) -> Result<Order> {
        let order_link_id = if new.client_order_id.is_empty() {
            Self::generate_order_link_id()
        } else {
            new.client_order_id.clone()
        };

        let request = CreateOrderRequest {
            category: "linear".to_string(),
            symbol: new.symbol.clone(),
            side: converters::to_bybit_side(new.side).to_string(),
            order_type: converters::to_bybit_order_type(new.ord_type).to_string(),
            qty: new.qty.to_string(),
            price: new.price.map(|p| p.to_string()),
            time_in_force: new.tif.map(|t| converters::to_bybit_tif(t).to_string()),
            order_link_id: order_link_id.clone(),
            reduce_only: if new.reduce_only { Some(true) } else { None },
            position_idx: Some(0), // One-way mode
        };

        let response: BybitResponse<OrderResult> = self
            .call_api("/v5/order/create", || async {
                self.client
                    .post_private("/v5/order/create", &request)
                    .await
            })
            .await?;

        let result = response.into_result()?;
        let now = Self::now_millis();

        Ok(Order {
            venue_order_id: result.order_id,
            client_order_id: order_link_id,
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
            raw_status: Some("New".to_string()),
        })
    }

    async fn cancel_order(&self, symbol: &str, venue_order_id: &str) -> Result<bool> {
        #[derive(Serialize)]
        struct CancelRequest {
            category: String,
            symbol: String,
            #[serde(rename = "orderId")]
            order_id: String,
        }

        let request = CancelRequest {
            category: "linear".to_string(),
            symbol: symbol.to_string(),
            order_id: venue_order_id.to_string(),
        };

        let response: BybitResponse<serde_json::Value> = self
            .call_api("/v5/order/cancel", || async {
                self.client
                    .post_private("/v5/order/cancel", &request)
                    .await
            })
            .await?;

        Ok(response.ret_code == 0)
    }

    async fn cancel_all(&self, symbol: Option<&str>) -> Result<usize> {
        #[derive(Serialize)]
        struct CancelAllRequest {
            category: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            symbol: Option<String>,
        }

        let request = CancelAllRequest {
            category: "linear".to_string(),
            symbol: symbol.map(|s| s.to_string()),
        };

        let response: BybitResponse<serde_json::Value> = self
            .call_api("/v5/order/cancel-all", || async {
                self.client
                    .post_private("/v5/order/cancel-all", &request)
                    .await
            })
            .await?;

        Ok(if response.ret_code == 0 { 1 } else { 0 })
    }

    async fn get_order(&self, symbol: &str, venue_order_id: &str) -> Result<Order> {
        let mut params = HashMap::new();
        params.insert("category".to_string(), "linear".to_string());
        params.insert("symbol".to_string(), symbol.to_string());
        params.insert("orderId".to_string(), venue_order_id.to_string());

        #[derive(Deserialize)]
        struct OrderListResult {
            list: Vec<OrderDetail>,
        }

        #[derive(Deserialize)]
        struct OrderDetail {
            #[serde(rename = "orderId")]
            order_id: String,
            #[serde(rename = "orderLinkId")]
            order_link_id: String,
            symbol: String,
            side: String,
            #[serde(rename = "orderType")]
            order_type: String,
            price: String,
            qty: String,
            #[serde(rename = "cumExecQty")]
            cum_exec_qty: String,
            #[serde(rename = "orderStatus")]
            order_status: String,
            #[serde(rename = "createdTime")]
            created_time: String,
            #[serde(rename = "updatedTime")]
            updated_time: String,
        }

        let response: BybitResponse<OrderListResult> = self
            .call_api("/v5/order/realtime", || async {
                self.client
                    .get_private("/v5/order/realtime", params.clone())
                    .await
            })
            .await?;

        let result = response.into_result()?;
        let order = result.list.first().context("Order not found")?;
        let now = Self::now_millis();

        let qty: f64 = order.qty.parse().unwrap_or(0.0);
        let filled: f64 = order.cum_exec_qty.parse().unwrap_or(0.0);

        Ok(Order {
            venue_order_id: order.order_id.clone(),
            client_order_id: order.order_link_id.clone(),
            symbol: order.symbol.clone(),
            ord_type: converters::from_bybit_order_type(&order.order_type),
            side: converters::from_bybit_side(&order.side),
            qty,
            price: order.price.parse().ok(),
            stop_price: None,
            tif: None,
            status: converters::from_bybit_order_status(&order.order_status),
            filled_qty: filled,
            remaining_qty: qty - filled,
            created_ms: order.created_time.parse().unwrap_or(now),
            updated_ms: order.updated_time.parse().unwrap_or(now),
            recv_ms: now,
            raw_status: Some(order.order_status.clone()),
        })
    }

    async fn get_open_orders(&self, symbol: Option<&str>) -> Result<Vec<Order>> {
        let mut params = HashMap::new();
        params.insert("category".to_string(), "linear".to_string());
        if let Some(s) = symbol {
            params.insert("symbol".to_string(), s.to_string());
        }

        #[derive(Deserialize)]
        struct OrderListResult {
            list: Vec<OrderDetail>,
        }

        #[derive(Deserialize)]
        struct OrderDetail {
            #[serde(rename = "orderId")]
            order_id: String,
            #[serde(rename = "orderLinkId")]
            order_link_id: String,
            symbol: String,
            side: String,
            #[serde(rename = "orderType")]
            order_type: String,
            price: String,
            qty: String,
            #[serde(rename = "cumExecQty")]
            cum_exec_qty: String,
            #[serde(rename = "orderStatus")]
            order_status: String,
            #[serde(rename = "createdTime")]
            created_time: String,
            #[serde(rename = "updatedTime")]
            updated_time: String,
        }

        let response: BybitResponse<OrderListResult> = self
            .call_api("/v5/order/realtime", || async {
                self.client
                    .get_private("/v5/order/realtime", params.clone())
                    .await
            })
            .await?;

        let result = response.into_result()?;
        let now = Self::now_millis();

        Ok(result
            .list
            .into_iter()
            .map(|order| {
                let qty: f64 = order.qty.parse().unwrap_or(0.0);
                let filled: f64 = order.cum_exec_qty.parse().unwrap_or(0.0);

                Order {
                    venue_order_id: order.order_id,
                    client_order_id: order.order_link_id,
                    symbol: order.symbol,
                    ord_type: converters::from_bybit_order_type(&order.order_type),
                    side: converters::from_bybit_side(&order.side),
                    qty,
                    price: order.price.parse().ok(),
                    stop_price: None,
                    tif: None,
                    status: converters::from_bybit_order_status(&order.order_status),
                    filled_qty: filled,
                    remaining_qty: qty - filled,
                    created_ms: order.created_time.parse().unwrap_or(now),
                    updated_ms: order.updated_time.parse().unwrap_or(now),
                    recv_ms: now,
                    raw_status: Some(order.order_status),
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
        _reduce_only: Option<bool>,
    ) -> Result<(Order, bool)> {
        #[derive(Serialize)]
        struct AmendRequest {
            category: String,
            symbol: String,
            #[serde(rename = "orderId")]
            order_id: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            price: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            qty: Option<String>,
        }

        let request = AmendRequest {
            category: "linear".to_string(),
            symbol: symbol.to_string(),
            order_id: venue_order_id.to_string(),
            price: new_price.map(|p| p.to_string()),
            qty: new_qty.map(|q| q.to_string()),
        };

        let response: BybitResponse<OrderResult> = self
            .call_api("/v5/order/amend", || async {
                self.client
                    .post_private("/v5/order/amend", &request)
                    .await
            })
            .await?;

        let result = response.into_result()?;
        let order = self.get_order(symbol, &result.order_id).await?;
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

    async fn set_leverage(&self, symbol: &str, leverage: Decimal) -> Result<()> {
        #[derive(Serialize)]
        struct LeverageRequest {
            category: String,
            symbol: String,
            #[serde(rename = "buyLeverage")]
            buy_leverage: String,
            #[serde(rename = "sellLeverage")]
            sell_leverage: String,
        }

        let lev_str = leverage.to_string();
        let request = LeverageRequest {
            category: "linear".to_string(),
            symbol: symbol.to_string(),
            buy_leverage: lev_str.clone(),
            sell_leverage: lev_str,
        };

        let response: BybitResponse<serde_json::Value> = self
            .call_api("/v5/position/set-leverage", || async {
                self.client
                    .post_private("/v5/position/set-leverage", &request)
                    .await
            })
            .await?;

        if response.ret_code != 0 && response.ret_code != 110043 {
            // 110043 = leverage not modified (already set)
            anyhow::bail!("Failed to set leverage: {}", response.ret_msg);
        }

        Ok(())
    }

    async fn set_margin_mode(&self, symbol: &str, mode: MarginMode) -> Result<()> {
        #[derive(Serialize)]
        struct MarginModeRequest {
            category: String,
            symbol: String,
            #[serde(rename = "tradeMode")]
            trade_mode: i32,
        }

        let trade_mode = match mode {
            MarginMode::Cross => 0,
            MarginMode::Isolated => 1,
        };

        let request = MarginModeRequest {
            category: "linear".to_string(),
            symbol: symbol.to_string(),
            trade_mode,
        };

        let response: BybitResponse<serde_json::Value> = self
            .call_api("/v5/position/switch-isolated", || async {
                self.client
                    .post_private("/v5/position/switch-isolated", &request)
                    .await
            })
            .await?;

        if response.ret_code != 0 && response.ret_code != 110026 {
            // 110026 = cross/isolated margin mode not changed
            anyhow::bail!("Failed to set margin mode: {}", response.ret_msg);
        }

        Ok(())
    }

    async fn get_position(&self, symbol: &str) -> Result<Position> {
        let mut params = HashMap::new();
        params.insert("category".to_string(), "linear".to_string());
        params.insert("symbol".to_string(), symbol.to_string());

        let response: BybitResponse<PositionResult> = self
            .call_api("/v5/position/list", || async {
                self.client
                    .get_private("/v5/position/list", params.clone())
                    .await
            })
            .await?;

        let result = response.into_result()?;
        let pos = result
            .list
            .first()
            .context("Position not found")?;

        let size: f64 = pos.size.parse().unwrap_or(0.0);
        let qty = if pos.side == "Sell" { -size } else { size };

        Ok(Position {
            exchange: Some("bybit".to_string()),
            symbol: pos.symbol.clone(),
            qty,
            entry_px: pos.avg_price.parse().unwrap_or(0.0),
            mark_px: pos.mark_price.parse().ok(),
            liquidation_px: pos.liq_price.parse().ok(),
            unrealized_pnl: pos.unrealised_pnl.parse().ok(),
            realized_pnl: pos.cum_realised_pnl.parse().ok(),
            margin: pos.position_im.parse().ok(),
            leverage: pos.leverage.parse().ok(),
            opened_ms: None,
            updated_ms: pos.updated_time.parse().unwrap_or_else(|_| Self::now_millis()),
        })
    }

    async fn get_all_positions(&self) -> Result<Vec<Position>> {
        let mut params = HashMap::new();
        params.insert("category".to_string(), "linear".to_string());
        params.insert("settleCoin".to_string(), "USDT".to_string());

        let response: BybitResponse<PositionResult> = self
            .call_api("/v5/position/list", || async {
                self.client
                    .get_private("/v5/position/list", params.clone())
                    .await
            })
            .await?;

        let result = response.into_result()?;

        Ok(result
            .list
            .into_iter()
            .filter(|p| p.size.parse::<f64>().unwrap_or(0.0) != 0.0)
            .map(|pos| {
                let size: f64 = pos.size.parse().unwrap_or(0.0);
                let qty = if pos.side == "Sell" { -size } else { size };

                Position {
                    exchange: Some("bybit".to_string()),
                    symbol: pos.symbol,
                    qty,
                    entry_px: pos.avg_price.parse().unwrap_or(0.0),
                    mark_px: pos.mark_price.parse().ok(),
                    liquidation_px: pos.liq_price.parse().ok(),
                    unrealized_pnl: pos.unrealised_pnl.parse().ok(),
                    realized_pnl: pos.cum_realised_pnl.parse().ok(),
                    margin: pos.position_im.parse().ok(),
                    leverage: pos.leverage.parse().ok(),
                    opened_ms: None,
                    updated_ms: pos.updated_time.parse().unwrap_or_else(|_| Self::now_millis()),
                }
            })
            .collect())
    }

    async fn get_funding_rate(&self, symbol: &str) -> Result<(Decimal, UnixMillis)> {
        let mut params = HashMap::new();
        params.insert("category".to_string(), "linear".to_string());
        params.insert("symbol".to_string(), symbol.to_string());

        let response: BybitResponse<TickerResult> = self
            .call_api("/v5/market/tickers", || async {
                self.client
                    .get_public("/v5/market/tickers", Some(params.clone()))
                    .await
            })
            .await?;

        let result = response.into_result()?;
        let ticker = result.list.first().context("Ticker not found")?;

        let rate = Decimal::from_str(&ticker.funding_rate).unwrap_or_else(|_| Decimal::from(0));
        let next_time: u64 = ticker.next_funding_time.parse().unwrap_or(0);

        Ok((rate, next_time))
    }

    async fn get_balances(&self) -> Result<Vec<Balance>> {
        let mut params = HashMap::new();
        params.insert("accountType".to_string(), "UNIFIED".to_string());

        #[derive(Deserialize)]
        struct WalletResult {
            list: Vec<AccountData>,
        }

        #[derive(Deserialize)]
        struct AccountData {
            coin: Vec<CoinBalance>,
        }

        #[derive(Deserialize)]
        struct CoinBalance {
            coin: String,
            #[serde(rename = "walletBalance")]
            wallet_balance: String,
            #[serde(rename = "availableToWithdraw")]
            available: String,
            locked: String,
        }

        let response: BybitResponse<WalletResult> = self
            .call_api("/v5/account/wallet-balance", || async {
                self.client
                    .get_private("/v5/account/wallet-balance", params.clone())
                    .await
            })
            .await?;

        let result = response.into_result()?;
        let balances = result
            .list
            .first()
            .map(|a| {
                a.coin
                    .iter()
                    .map(|c| {
                        let free: f64 = c.available.parse().unwrap_or(0.0);
                        let locked: f64 = c.locked.parse().unwrap_or(0.0);
                        Balance {
                            asset: c.coin.clone(),
                            free,
                            locked,
                            total: free + locked,
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();

        Ok(balances)
    }

    async fn get_account_info(&self) -> Result<AccountInfo> {
        let balances = self.get_balances().await?;
        let now = Self::now_millis();

        Ok(AccountInfo {
            balances,
            can_trade: true,
            can_withdraw: true,
            can_deposit: true,
            update_ms: now,
        })
    }

    async fn get_market_info(&self, symbol: &str) -> Result<MarketInfo> {
        let mut params = HashMap::new();
        params.insert("category".to_string(), "linear".to_string());
        params.insert("symbol".to_string(), symbol.to_string());

        #[derive(Deserialize)]
        struct InstrumentsResult {
            list: Vec<InstrumentInfo>,
        }

        #[derive(Deserialize)]
        struct InstrumentInfo {
            symbol: String,
            #[serde(rename = "baseCoin")]
            base_coin: String,
            #[serde(rename = "quoteCoin")]
            quote_coin: String,
            #[serde(rename = "lotSizeFilter")]
            lot_size_filter: LotSizeFilter,
            #[serde(rename = "priceFilter")]
            price_filter: PriceFilter,
            #[serde(rename = "leverageFilter")]
            leverage_filter: LeverageFilter,
        }

        #[derive(Deserialize)]
        struct LotSizeFilter {
            #[serde(rename = "minOrderQty")]
            min_order_qty: String,
            #[serde(rename = "maxOrderQty")]
            max_order_qty: String,
            #[serde(rename = "qtyStep")]
            qty_step: String,
        }

        #[derive(Deserialize)]
        struct PriceFilter {
            #[serde(rename = "tickSize")]
            tick_size: String,
        }

        #[derive(Deserialize)]
        struct LeverageFilter {
            #[serde(rename = "maxLeverage")]
            max_leverage: String,
        }

        let response: BybitResponse<InstrumentsResult> = self
            .call_api("/v5/market/instruments-info", || async {
                self.client
                    .get_public("/v5/market/instruments-info", Some(params.clone()))
                    .await
            })
            .await?;

        let result = response.into_result()?;
        let info = result.list.first().context("Instrument not found")?;

        Ok(MarketInfo {
            symbol: info.symbol.clone(),
            base_asset: info.base_coin.clone(),
            quote_asset: info.quote_coin.clone(),
            status: MarketStatus::Trading,
            min_qty: info.lot_size_filter.min_order_qty.parse().unwrap_or(0.0),
            max_qty: info.lot_size_filter.max_order_qty.parse().unwrap_or(f64::MAX),
            step_size: info.lot_size_filter.qty_step.parse().unwrap_or(0.0),
            tick_size: info.price_filter.tick_size.parse().unwrap_or(0.0),
            min_notional: 0.0,
            max_leverage: info.leverage_filter.max_leverage.parse().ok(),
            is_spot: false,
            is_perp: true,
        })
    }

    async fn get_all_markets(&self) -> Result<Vec<MarketInfo>> {
        let mut params = HashMap::new();
        params.insert("category".to_string(), "linear".to_string());

        #[derive(Deserialize)]
        struct InstrumentsResult {
            list: Vec<InstrumentInfo>,
        }

        #[derive(Deserialize)]
        struct InstrumentInfo {
            symbol: String,
            #[serde(rename = "baseCoin")]
            base_coin: String,
            #[serde(rename = "quoteCoin")]
            quote_coin: String,
            status: String,
            #[serde(rename = "lotSizeFilter")]
            lot_size_filter: LotSizeFilter,
            #[serde(rename = "priceFilter")]
            price_filter: PriceFilter,
            #[serde(rename = "leverageFilter")]
            leverage_filter: LeverageFilter,
        }

        #[derive(Deserialize)]
        struct LotSizeFilter {
            #[serde(rename = "minOrderQty")]
            min_order_qty: String,
            #[serde(rename = "maxOrderQty")]
            max_order_qty: String,
            #[serde(rename = "qtyStep")]
            qty_step: String,
        }

        #[derive(Deserialize)]
        struct PriceFilter {
            #[serde(rename = "tickSize")]
            tick_size: String,
        }

        #[derive(Deserialize)]
        struct LeverageFilter {
            #[serde(rename = "maxLeverage")]
            max_leverage: String,
        }

        let response: BybitResponse<InstrumentsResult> = self
            .call_api("/v5/market/instruments-info", || async {
                self.client
                    .get_public("/v5/market/instruments-info", Some(params.clone()))
                    .await
            })
            .await?;

        let result = response.into_result()?;

        Ok(result
            .list
            .into_iter()
            .map(|info| {
                let status = match info.status.as_str() {
                    "Trading" => MarketStatus::Trading,
                    "PreLaunch" => MarketStatus::PreTrading,
                    _ => MarketStatus::Halt,
                };

                MarketInfo {
                    symbol: info.symbol,
                    base_asset: info.base_coin,
                    quote_asset: info.quote_coin,
                    status,
                    min_qty: info.lot_size_filter.min_order_qty.parse().unwrap_or(0.0),
                    max_qty: info.lot_size_filter.max_order_qty.parse().unwrap_or(f64::MAX),
                    step_size: info.lot_size_filter.qty_step.parse().unwrap_or(0.0),
                    tick_size: info.price_filter.tick_size.parse().unwrap_or(0.0),
                    min_notional: 0.0,
                    max_leverage: info.leverage_filter.max_leverage.parse().ok(),
                    is_spot: false,
                    is_perp: true,
                }
            })
            .collect())
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        let mut params = HashMap::new();
        params.insert("category".to_string(), "linear".to_string());
        params.insert("symbol".to_string(), symbol.to_string());

        let response: BybitResponse<TickerResult> = self
            .call_api("/v5/market/tickers", || async {
                self.client
                    .get_public("/v5/market/tickers", Some(params.clone()))
                    .await
            })
            .await?;

        let result = response.into_result()?;
        let ticker = result.list.first().context("Ticker not found")?;
        let now = Self::now_millis();

        let last_price: f64 = ticker.last_price.parse().unwrap_or(0.0);
        let prev_price: f64 = ticker.prev_price_24h.parse().unwrap_or(last_price);
        let change_pct: f64 = ticker.price_24h_pcnt.parse().unwrap_or(0.0) * 100.0;

        Ok(TickerInfo {
            symbol: ticker.symbol.clone(),
            last_price,
            bid_price: ticker.bid1_price.parse().unwrap_or(0.0),
            ask_price: ticker.ask1_price.parse().unwrap_or(0.0),
            volume_24h: ticker.volume_24h.parse().unwrap_or(0.0),
            price_change_24h: last_price - prev_price,
            price_change_pct_24h: change_pct,
            high_24h: ticker.high_price_24h.parse().unwrap_or(0.0),
            low_24h: ticker.low_price_24h.parse().unwrap_or(0.0),
            open_price_24h: prev_price,
            ts_ms: now,
        })
    }

    async fn get_tickers(&self, symbols: Option<Vec<String>>) -> Result<Vec<TickerInfo>> {
        let mut params = HashMap::new();
        params.insert("category".to_string(), "linear".to_string());

        let response: BybitResponse<TickerResult> = self
            .call_api("/v5/market/tickers", || async {
                self.client
                    .get_public("/v5/market/tickers", Some(params.clone()))
                    .await
            })
            .await?;

        let result = response.into_result()?;
        let now = Self::now_millis();

        let mut tickers: Vec<TickerInfo> = result
            .list
            .into_iter()
            .map(|ticker| {
                let last_price: f64 = ticker.last_price.parse().unwrap_or(0.0);
                let prev_price: f64 = ticker.prev_price_24h.parse().unwrap_or(last_price);
                let change_pct: f64 = ticker.price_24h_pcnt.parse().unwrap_or(0.0) * 100.0;

                TickerInfo {
                    symbol: ticker.symbol,
                    last_price,
                    bid_price: ticker.bid1_price.parse().unwrap_or(0.0),
                    ask_price: ticker.ask1_price.parse().unwrap_or(0.0),
                    volume_24h: ticker.volume_24h.parse().unwrap_or(0.0),
                    price_change_24h: last_price - prev_price,
                    price_change_pct_24h: change_pct,
                    high_24h: ticker.high_price_24h.parse().unwrap_or(0.0),
                    low_24h: ticker.low_price_24h.parse().unwrap_or(0.0),
                    open_price_24h: prev_price,
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
        let mut params = HashMap::new();
        params.insert("category".to_string(), "linear".to_string());
        params.insert("symbol".to_string(), symbol.to_string());

        let response: BybitResponse<TickerResult> = self
            .call_api("/v5/market/tickers", || async {
                self.client
                    .get_public("/v5/market/tickers", Some(params.clone()))
                    .await
            })
            .await?;

        let result = response.into_result()?;
        let ticker = result.list.first().context("Ticker not found")?;

        let price: f64 = ticker.mark_price.parse().unwrap_or(0.0);
        Ok((price, Self::now_millis()))
    }

    async fn get_index_price(&self, symbol: &str) -> Result<(Price, UnixMillis)> {
        let mut params = HashMap::new();
        params.insert("category".to_string(), "linear".to_string());
        params.insert("symbol".to_string(), symbol.to_string());

        let response: BybitResponse<TickerResult> = self
            .call_api("/v5/market/tickers", || async {
                self.client
                    .get_public("/v5/market/tickers", Some(params.clone()))
                    .await
            })
            .await?;

        let result = response.into_result()?;
        let ticker = result.list.first().context("Ticker not found")?;

        let price: f64 = ticker.index_price.parse().unwrap_or(0.0);
        Ok((price, Self::now_millis()))
    }

    async fn get_klines(
        &self,
        symbol: &str,
        interval: KlineInterval,
        start_ms: Option<UnixMillis>,
        end_ms: Option<UnixMillis>,
        limit: Option<usize>,
    ) -> Result<Vec<Kline>> {
        let interval_str = match interval {
            KlineInterval::M1 => "1",
            KlineInterval::M5 => "5",
            KlineInterval::M15 => "15",
            KlineInterval::M30 => "30",
            KlineInterval::H1 => "60",
            KlineInterval::H4 => "240",
            KlineInterval::D1 => "D",
        };

        let mut params = HashMap::new();
        params.insert("category".to_string(), "linear".to_string());
        params.insert("symbol".to_string(), symbol.to_string());
        params.insert("interval".to_string(), interval_str.to_string());
        if let Some(start) = start_ms {
            params.insert("start".to_string(), start.to_string());
        }
        if let Some(end) = end_ms {
            params.insert("end".to_string(), end.to_string());
        }
        if let Some(l) = limit {
            params.insert("limit".to_string(), l.to_string());
        }

        #[derive(Deserialize)]
        struct KlineResult {
            list: Vec<Vec<String>>,
        }

        let response: BybitResponse<KlineResult> = self
            .call_api("/v5/market/kline", || async {
                self.client
                    .get_public("/v5/market/kline", Some(params.clone()))
                    .await
            })
            .await?;

        let result = response.into_result()?;
        let klines = result
            .list
            .iter()
            .filter_map(|k| {
                if k.len() >= 7 {
                    let open_ms: u64 = k[0].parse().unwrap_or(0);
                    Some(Kline {
                        symbol: symbol.to_string(),
                        open_ms,
                        close_ms: open_ms + interval_to_ms(interval),
                        open: k[1].parse().unwrap_or(0.0),
                        high: k[2].parse().unwrap_or(0.0),
                        low: k[3].parse().unwrap_or(0.0),
                        close: k[4].parse().unwrap_or(0.0),
                        volume: k[5].parse().unwrap_or(0.0),
                        quote_volume: k[6].parse().unwrap_or(0.0),
                        trades: 0,
                    })
                } else {
                    None
                }
            })
            .collect();

        Ok(klines)
    }

    async fn get_funding_history(
        &self,
        symbol: &str,
        start_ms: Option<UnixMillis>,
        end_ms: Option<UnixMillis>,
        limit: Option<usize>,
    ) -> Result<Vec<FundingRateHistory>> {
        let mut params = HashMap::new();
        params.insert("category".to_string(), "linear".to_string());
        params.insert("symbol".to_string(), symbol.to_string());
        if let Some(start) = start_ms {
            params.insert("startTime".to_string(), start.to_string());
        }
        if let Some(end) = end_ms {
            params.insert("endTime".to_string(), end.to_string());
        }
        if let Some(l) = limit {
            params.insert("limit".to_string(), l.to_string());
        }

        #[derive(Deserialize)]
        struct FundingResult {
            list: Vec<FundingItem>,
        }

        #[derive(Deserialize)]
        struct FundingItem {
            symbol: String,
            #[serde(rename = "fundingRate")]
            funding_rate: String,
            #[serde(rename = "fundingRateTimestamp")]
            funding_rate_timestamp: String,
        }

        let response: BybitResponse<FundingResult> = self
            .call_api("/v5/market/funding/history", || async {
                self.client
                    .get_public("/v5/market/funding/history", Some(params.clone()))
                    .await
            })
            .await?;

        let result = response.into_result()?;

        Ok(result
            .list
            .into_iter()
            .map(|item| FundingRateHistory {
                symbol: item.symbol,
                rate: Decimal::from_str(&item.funding_rate).unwrap_or_else(|_| Decimal::from(0)),
                ts_ms: item.funding_rate_timestamp.parse().unwrap_or(0),
            })
            .collect())
    }
}

// =============================================================================
// PerpWs Implementation
// =============================================================================

#[async_trait::async_trait]
impl PerpWs for BybitPerpsAdapter {
    /// Subscribes to user data stream (order updates, executions, positions)
    ///
    /// Bybit uses a private WebSocket with HMAC-SHA256 authentication.
    async fn subscribe_user(&self) -> Result<mpsc::Receiver<UserEvent>> {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::{connect_async, tungstenite::Message};

        let (tx, rx) = mpsc::channel(1000);
        let adapter = self.clone();
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel(1);

        {
            let mut guard = adapter.shutdown_tx.lock().await;
            *guard = Some(shutdown_tx);
        }

        tokio::spawn(async move {
            let reconnect_config = ReconnectConfig::production();
            let mut strategy = ReconnectStrategy::new(reconnect_config);

            'reconnect: loop {
                let reconnect_num = adapter.reconnect_count.load(Ordering::Relaxed);
                info!("Bybit Perps user stream connecting (reconnect #{})", reconnect_num);

                {
                    let mut status = adapter.connection_status.write().await;
                    *status = ConnectionStatus::Connecting;
                }

                let mut ws = match connect_async(BYBIT_WS_PRIVATE_URL).await {
                    Ok((stream, _)) => stream,
                    Err(e) => {
                        error!("Bybit Perps WebSocket connection failed: {}", e);
                        if !strategy.can_retry() {
                            break 'reconnect;
                        }
                        let delay = strategy.wait_before_retry().await;
                        warn!("Bybit Perps user stream reconnecting - waiting {}ms", delay);
                        continue 'reconnect;
                    }
                };

                // Authenticate
                let expires = BybitAuth::timestamp() + 10000;
                let signature = adapter.auth.sign_websocket(expires);
                let auth_msg = serde_json::json!({
                    "op": "auth",
                    "args": [adapter.auth.api_key.clone(), expires, signature]
                });

                if ws.send(Message::Text(auth_msg.to_string())).await.is_err() {
                    error!("Bybit Perps failed to send auth message");
                    continue 'reconnect;
                }

                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

                // Subscribe to order, execution, and position channels
                let sub_msg = serde_json::json!({
                    "op": "subscribe",
                    "args": ["order", "execution", "position", "wallet"]
                });

                if ws.send(Message::Text(sub_msg.to_string())).await.is_err() {
                    error!("Bybit Perps failed to subscribe to user channels");
                    continue 'reconnect;
                }

                strategy.reset();
                adapter.reconnect_count.fetch_add(1, Ordering::Relaxed);
                {
                    let mut status = adapter.connection_status.write().await;
                    *status = ConnectionStatus::Connected;
                }
                info!("Bybit Perps user WebSocket connected");

                let heartbeat = HeartbeatMonitor::new(HeartbeatConfig::production());
                let mut ping_interval = tokio::time::interval(tokio::time::Duration::from_secs(20));

                'message_loop: loop {
                    tokio::select! {
                        _ = shutdown_rx.recv() => {
                            info!("Bybit Perps user stream shutdown signal received");
                            break 'reconnect;
                        }
                        _ = ping_interval.tick() => {
                            let ping_msg = serde_json::json!({"op": "ping"});
                            if ws.send(Message::Text(ping_msg.to_string())).await.is_err() {
                                warn!("Bybit Perps failed to send ping");
                                break 'message_loop;
                            }
                            heartbeat.record_ping_sent().await;
                        }
                        _ = tokio::time::sleep(tokio::time::Duration::from_secs(30)) => {
                            if !heartbeat.is_alive().await {
                                warn!("Bybit Perps user stream heartbeat timeout");
                                break 'message_loop;
                            }
                        }
                        msg = ws.next() => {
                            match msg {
                                Some(Ok(Message::Text(text))) => {
                                    heartbeat.record_message_received().await;

                                    if text.contains("\"op\":\"pong\"") {
                                        heartbeat.record_pong_received().await;
                                        continue;
                                    }

                                    if let Ok(event) = parse_perps_user_event(&text) {
                                        if tx.send(event).await.is_err() {
                                            info!("Bybit Perps user stream receiver dropped");
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
                                    warn!("Bybit Perps WebSocket closed by server");
                                    break 'message_loop;
                                }
                                Some(Err(e)) => {
                                    error!("Bybit Perps WebSocket error: {}", e);
                                    break 'message_loop;
                                }
                                None => {
                                    warn!("Bybit Perps WebSocket stream ended");
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
                    error!("Bybit Perps user stream max reconnection attempts reached");
                    break 'reconnect;
                }

                let delay = strategy.wait_before_retry().await;
                warn!("Bybit Perps user stream reconnecting - waiting {}ms", delay);
            }

            info!("Bybit Perps user stream task terminated");
        });

        Ok(rx)
    }

    /// Subscribes to order book updates for specified symbols
    ///
    /// Uses public WebSocket: wss://stream.bybit.com/v5/public/linear
    async fn subscribe_books(&self, symbols: &[&str]) -> Result<mpsc::Receiver<BookUpdate>> {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::{connect_async, tungstenite::Message};

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
                info!("Bybit Perps books stream connecting (reconnect #{})", reconnect_num);

                let mut ws = match connect_async(BYBIT_WS_LINEAR_URL).await {
                    Ok((stream, _)) => stream,
                    Err(e) => {
                        error!("Bybit Perps books WebSocket connection failed: {}", e);
                        if !strategy.can_retry() {
                            break 'reconnect;
                        }
                        let delay = strategy.wait_before_retry().await;
                        warn!("Bybit Perps books stream reconnecting - waiting {}ms", delay);
                        continue 'reconnect;
                    }
                };

                // Subscribe to orderbook for all symbols (depth 50)
                let args: Vec<String> = symbols_owned
                    .iter()
                    .map(|s| format!("orderbook.50.{}", s))
                    .collect();
                let sub_msg = serde_json::json!({
                    "op": "subscribe",
                    "args": args
                });

                if ws.send(Message::Text(sub_msg.to_string())).await.is_err() {
                    warn!("Bybit Perps failed to subscribe to orderbooks");
                    continue 'reconnect;
                }

                strategy.reset();
                info!("Bybit Perps books WebSocket connected");

                let heartbeat = HeartbeatMonitor::new(HeartbeatConfig::production());
                let mut ping_interval = tokio::time::interval(tokio::time::Duration::from_secs(20));
                let mut seq_map: HashMap<String, u64> = HashMap::new();

                'message_loop: loop {
                    tokio::select! {
                        _ = shutdown_rx.recv() => {
                            info!("Bybit Perps books stream shutdown signal received");
                            break 'reconnect;
                        }
                        _ = ping_interval.tick() => {
                            let ping_msg = serde_json::json!({"op": "ping"});
                            if ws.send(Message::Text(ping_msg.to_string())).await.is_err() {
                                warn!("Bybit Perps books failed to send ping");
                                break 'message_loop;
                            }
                            heartbeat.record_ping_sent().await;
                        }
                        _ = tokio::time::sleep(tokio::time::Duration::from_secs(30)) => {
                            if !heartbeat.is_alive().await {
                                warn!("Bybit Perps books stream heartbeat timeout");
                                break 'message_loop;
                            }
                        }
                        msg = ws.next() => {
                            match msg {
                                Some(Ok(Message::Text(text))) => {
                                    heartbeat.record_message_received().await;

                                    if text.contains("\"op\":\"pong\"") {
                                        heartbeat.record_pong_received().await;
                                        continue;
                                    }

                                    if let Ok(update) = parse_perps_book_update(&text, &mut seq_map) {
                                        if tx.send(update).await.is_err() {
                                            info!("Bybit Perps books stream receiver dropped");
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
                                    warn!("Bybit Perps books WebSocket closed");
                                    break 'message_loop;
                                }
                                Some(Err(e)) => {
                                    error!("Bybit Perps books WebSocket error: {}", e);
                                    break 'message_loop;
                                }
                                None => {
                                    warn!("Bybit Perps books WebSocket stream ended");
                                    break 'message_loop;
                                }
                                _ => {}
                            }
                        }
                    }
                }

                if !strategy.can_retry() {
                    error!("Bybit Perps books stream max reconnection attempts reached");
                    break 'reconnect;
                }

                let delay = strategy.wait_before_retry().await;
                warn!("Bybit Perps books stream reconnecting - waiting {}ms", delay);
            }

            info!("Bybit Perps books stream task terminated");
        });

        Ok(rx)
    }

    /// Subscribes to trade stream for specified symbols
    async fn subscribe_trades(&self, symbols: &[&str]) -> Result<mpsc::Receiver<TradeEvent>> {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::{connect_async, tungstenite::Message};

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
                info!("Bybit Perps trades stream connecting (reconnect #{})", reconnect_num);

                let mut ws = match connect_async(BYBIT_WS_LINEAR_URL).await {
                    Ok((stream, _)) => stream,
                    Err(e) => {
                        error!("Bybit Perps trades WebSocket connection failed: {}", e);
                        if !strategy.can_retry() {
                            break 'reconnect;
                        }
                        let delay = strategy.wait_before_retry().await;
                        warn!("Bybit Perps trades stream reconnecting - waiting {}ms", delay);
                        continue 'reconnect;
                    }
                };

                // Subscribe to trades for all symbols
                let args: Vec<String> = symbols_owned
                    .iter()
                    .map(|s| format!("publicTrade.{}", s))
                    .collect();
                let sub_msg = serde_json::json!({
                    "op": "subscribe",
                    "args": args
                });

                if ws.send(Message::Text(sub_msg.to_string())).await.is_err() {
                    warn!("Bybit Perps failed to subscribe to trades");
                    continue 'reconnect;
                }

                strategy.reset();
                info!("Bybit Perps trades WebSocket connected");

                let heartbeat = HeartbeatMonitor::new(HeartbeatConfig::production());
                let mut ping_interval = tokio::time::interval(tokio::time::Duration::from_secs(20));

                'message_loop: loop {
                    tokio::select! {
                        _ = shutdown_rx.recv() => {
                            info!("Bybit Perps trades stream shutdown signal received");
                            break 'reconnect;
                        }
                        _ = ping_interval.tick() => {
                            let ping_msg = serde_json::json!({"op": "ping"});
                            if ws.send(Message::Text(ping_msg.to_string())).await.is_err() {
                                warn!("Bybit Perps trades failed to send ping");
                                break 'message_loop;
                            }
                            heartbeat.record_ping_sent().await;
                        }
                        _ = tokio::time::sleep(tokio::time::Duration::from_secs(30)) => {
                            if !heartbeat.is_alive().await {
                                warn!("Bybit Perps trades stream heartbeat timeout");
                                break 'message_loop;
                            }
                        }
                        msg = ws.next() => {
                            match msg {
                                Some(Ok(Message::Text(text))) => {
                                    heartbeat.record_message_received().await;

                                    if text.contains("\"op\":\"pong\"") {
                                        heartbeat.record_pong_received().await;
                                        continue;
                                    }

                                    if let Ok(events) = parse_perps_trade_events(&text) {
                                        for event in events {
                                            if tx.send(event).await.is_err() {
                                                info!("Bybit Perps trades stream receiver dropped");
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
                                    warn!("Bybit Perps trades WebSocket closed");
                                    break 'message_loop;
                                }
                                Some(Err(e)) => {
                                    error!("Bybit Perps trades WebSocket error: {}", e);
                                    break 'message_loop;
                                }
                                None => {
                                    warn!("Bybit Perps trades WebSocket stream ended");
                                    break 'message_loop;
                                }
                                _ => {}
                            }
                        }
                    }
                }

                if !strategy.can_retry() {
                    error!("Bybit Perps trades stream max reconnection attempts reached");
                    break 'reconnect;
                }

                let delay = strategy.wait_before_retry().await;
                warn!("Bybit Perps trades stream reconnecting - waiting {}ms", delay);
            }

            info!("Bybit Perps trades stream task terminated");
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
// Helpers
// =============================================================================

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
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

// =============================================================================
// WebSocket Message Parsers
// =============================================================================

#[derive(Debug, Deserialize)]
struct BybitWsOrderMessage {
    topic: String,
    data: Vec<BybitWsOrder>,
}

#[derive(Debug, Deserialize)]
struct BybitWsOrder {
    symbol: String,
    #[serde(rename = "orderId")]
    order_id: String,
    #[serde(rename = "orderLinkId")]
    order_link_id: String,
    side: String,
    #[serde(rename = "orderType")]
    order_type: String,
    price: String,
    qty: String,
    #[serde(rename = "cumExecQty")]
    cum_exec_qty: String,
    #[serde(rename = "orderStatus")]
    order_status: String,
    #[serde(rename = "createdTime")]
    created_time: String,
    #[serde(rename = "updatedTime")]
    updated_time: String,
}

#[derive(Debug, Deserialize)]
struct BybitWsExecutionMessage {
    topic: String,
    data: Vec<BybitWsExecution>,
}

#[derive(Debug, Deserialize)]
struct BybitWsExecution {
    symbol: String,
    #[serde(rename = "orderId")]
    order_id: String,
    #[serde(rename = "orderLinkId")]
    order_link_id: String,
    side: String,
    #[serde(rename = "execPrice")]
    exec_price: String,
    #[serde(rename = "execQty")]
    exec_qty: String,
    #[serde(rename = "execTime")]
    exec_time: String,
    #[serde(rename = "execId")]
    exec_id: String,
}

#[derive(Debug, Deserialize)]
struct BybitWsBookMessage {
    topic: String,
    #[serde(rename = "type")]
    msg_type: String,
    ts: u64,
    data: BybitWsBookData,
}

#[derive(Debug, Deserialize)]
struct BybitWsBookData {
    s: String,
    b: Vec<[String; 2]>,
    a: Vec<[String; 2]>,
    u: u64,
}

#[derive(Debug, Deserialize)]
struct BybitWsTradeMessage {
    topic: String,
    ts: u64,
    data: Vec<BybitWsTrade>,
}

#[derive(Debug, Deserialize)]
struct BybitWsTrade {
    #[serde(rename = "T")]
    timestamp: u64,
    s: String,
    #[serde(rename = "S")]
    side: String,
    v: String,
    p: String,
    i: String,
}

fn parse_perps_user_event(text: &str) -> Result<UserEvent> {
    if let Ok(msg) = serde_json::from_str::<BybitWsOrderMessage>(text) {
        if msg.topic == "order" {
            if let Some(order) = msg.data.first() {
                let qty: f64 = order.qty.parse().unwrap_or(0.0);
                let filled: f64 = order.cum_exec_qty.parse().unwrap_or(0.0);
                let now = now_millis();

                let order_struct = Order {
                    venue_order_id: order.order_id.clone(),
                    client_order_id: order.order_link_id.clone(),
                    symbol: order.symbol.clone(),
                    ord_type: converters::from_bybit_order_type(&order.order_type),
                    side: converters::from_bybit_side(&order.side),
                    qty,
                    price: order.price.parse().ok(),
                    stop_price: None,
                    tif: None,
                    status: converters::from_bybit_order_status(&order.order_status),
                    filled_qty: filled,
                    remaining_qty: qty - filled,
                    created_ms: order.created_time.parse().unwrap_or(now),
                    updated_ms: order.updated_time.parse().unwrap_or(now),
                    recv_ms: now,
                    raw_status: Some(order.order_status.clone()),
                };

                return Ok(UserEvent::OrderUpdate(order_struct));
            }
        }
    }

    if let Ok(msg) = serde_json::from_str::<BybitWsExecutionMessage>(text) {
        if msg.topic == "execution" {
            if let Some(exec) = msg.data.first() {
                let now = now_millis();
                let fill = Fill {
                    venue_order_id: exec.order_id.clone(),
                    client_order_id: exec.order_link_id.clone(),
                    symbol: exec.symbol.clone(),
                    qty: exec.exec_qty.parse().unwrap_or(0.0),
                    price: exec.exec_price.parse().unwrap_or(0.0),
                    fee: 0.0,
                    fee_ccy: String::new(),
                    is_maker: false,
                    exec_id: exec.exec_id.clone(),
                    ex_ts_ms: exec.exec_time.parse().unwrap_or(now),
                    recv_ms: now,
                };

                return Ok(UserEvent::Fill(fill));
            }
        }
    }

    anyhow::bail!("Unknown or unparseable Bybit Perps user event")
}

fn parse_perps_book_update(text: &str, seq_map: &mut HashMap<String, u64>) -> Result<BookUpdate> {
    let msg: BybitWsBookMessage = serde_json::from_str(text)
        .context("Failed to parse Bybit Perps book message")?;

    let symbol = msg.data.s.clone();
    let prev_seq = seq_map.get(&symbol).copied().unwrap_or(0);
    seq_map.insert(symbol.clone(), msg.data.u);
    let now = now_millis();

    let bids: Vec<(Price, Quantity)> = msg.data.b
        .iter()
        .map(|[price, size]| (price.parse().unwrap_or(0.0), size.parse().unwrap_or(0.0)))
        .collect();

    let asks: Vec<(Price, Quantity)> = msg.data.a
        .iter()
        .map(|[price, size]| (price.parse().unwrap_or(0.0), size.parse().unwrap_or(0.0)))
        .collect();

    Ok(BookUpdate::DepthDelta {
        symbol,
        bids,
        asks,
        seq: msg.data.u,
        prev_seq,
        checksum: None,
        ex_ts_ms: msg.ts,
        recv_ms: now,
    })
}

fn parse_perps_trade_events(text: &str) -> Result<Vec<TradeEvent>> {
    let msg: BybitWsTradeMessage = serde_json::from_str(text)
        .context("Failed to parse Bybit Perps trade message")?;

    let now = now_millis();
    let trades = msg.data
        .into_iter()
        .map(|trade| TradeEvent {
            symbol: trade.s,
            px: trade.p.parse().unwrap_or(0.0),
            qty: trade.v.parse().unwrap_or(0.0),
            taker_is_buy: trade.side == "Buy",
            ex_ts_ms: trade.timestamp,
            recv_ms: now,
        })
        .collect();

    Ok(trades)
}
