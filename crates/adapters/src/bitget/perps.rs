//! Bitget Perpetual Futures Trading Adapter
//!
//! Implements PerpRest and PerpWs traits for Bitget perpetual futures trading.

use super::account::{converters, BitgetAuth, BitgetResponse, BitgetRestClient};
use crate::traits::*;
use crate::utils::{CircuitBreaker, CircuitBreakerConfig, RateLimiter, RateLimiterConfig};
use anyhow::{Context, Result};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::AtomicU32;
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

/// Bitget perpetual futures trading adapter
#[derive(Clone)]
pub struct BitgetPerpsAdapter {
    client: BitgetRestClient,
    #[allow(dead_code)]
    auth: Option<BitgetAuth>,
    #[allow(dead_code)]
    connection_status: Arc<RwLock<ConnectionStatus>>,
    rate_limiter: RateLimiter,
    circuit_breaker: CircuitBreaker,
    #[allow(dead_code)]
    reconnect_count: Arc<AtomicU32>,
    #[allow(dead_code)]
    shutdown_tx: Arc<Mutex<Option<tokio::sync::broadcast::Sender<()>>>>,
    product_type: String, // "USDT-FUTURES" or "COIN-FUTURES"
    margin_coin: String,  // "USDT" for USDT-margined, "BTC"/"ETH"/etc for coin-margined
}

impl BitgetPerpsAdapter {
    /// Creates a new Bitget perpetual futures adapter with authentication (USDT-margined)
    pub fn new(api_key: String, api_secret: String, passphrase: String) -> Self {
        let auth = BitgetAuth::new(api_key, api_secret, passphrase);
        Self {
            client: BitgetRestClient::new(Some(auth.clone())),
            auth: Some(auth),
            connection_status: Arc::new(RwLock::new(ConnectionStatus::Disconnected)),
            rate_limiter: RateLimiter::new(RateLimiterConfig {
                requests_per_window: 10,
                window_ms: 1000,
                block_on_limit: true,
                rate_limit_backoff_ms: 2000,
            }),
            circuit_breaker: CircuitBreaker::new("bitget_perps", CircuitBreakerConfig::production()),
            reconnect_count: Arc::new(AtomicU32::new(0)),
            shutdown_tx: Arc::new(Mutex::new(None)),
            product_type: "USDT-FUTURES".to_string(),
            margin_coin: "USDT".to_string(),
        }
    }

    /// Creates a new Bitget perpetual futures adapter for coin-margined contracts
    /// The margin_coin parameter should be the base currency (e.g., "BTC", "ETH")
    pub fn coin_margined(api_key: String, api_secret: String, passphrase: String, margin_coin: String) -> Self {
        let auth = BitgetAuth::new(api_key, api_secret, passphrase);
        Self {
            client: BitgetRestClient::new(Some(auth.clone())),
            auth: Some(auth),
            connection_status: Arc::new(RwLock::new(ConnectionStatus::Disconnected)),
            rate_limiter: RateLimiter::new(RateLimiterConfig {
                requests_per_window: 10,
                window_ms: 1000,
                block_on_limit: true,
                rate_limit_backoff_ms: 2000,
            }),
            circuit_breaker: CircuitBreaker::new(
                "bitget_perps_coin",
                CircuitBreakerConfig::production(),
            ),
            reconnect_count: Arc::new(AtomicU32::new(0)),
            shutdown_tx: Arc::new(Mutex::new(None)),
            product_type: "COIN-FUTURES".to_string(),
            margin_coin,
        }
    }

    /// Creates a public adapter without authentication
    pub fn public() -> Self {
        Self {
            client: BitgetRestClient::new(None),
            auth: None,
            connection_status: Arc::new(RwLock::new(ConnectionStatus::Disconnected)),
            rate_limiter: RateLimiter::new(RateLimiterConfig {
                requests_per_window: 20,
                window_ms: 1000,
                block_on_limit: true,
                rate_limit_backoff_ms: 2000,
            }),
            circuit_breaker: CircuitBreaker::new(
                "bitget_perps_public",
                CircuitBreakerConfig::production(),
            ),
            reconnect_count: Arc::new(AtomicU32::new(0)),
            shutdown_tx: Arc::new(Mutex::new(None)),
            product_type: "USDT-FUTURES".to_string(),
            margin_coin: "USDT".to_string(),
        }
    }

    fn generate_client_id() -> String {
        format!("cc_{}", BitgetAuth::timestamp_ms())
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
        debug!("Calling Bitget Perps API endpoint: {}", endpoint);
        self.circuit_breaker.call(f).await
    }

    /// Convert standard symbol to Bitget futures format (BTC/USDT -> BTCUSDT)
    fn to_bitget_symbol(symbol: &str) -> String {
        symbol.replace("/", "").replace("-", "")
    }

    /// Convert Bitget symbol to standard format
    fn to_standard_symbol(symbol: &str) -> String {
        for quote in ["USDT", "USD", "PERP"] {
            if symbol.ends_with(quote) {
                let base = &symbol[..symbol.len() - quote.len()];
                return format!("{}/{}", base, quote);
            }
        }
        symbol.to_string()
    }
}

// =============================================================================
// REST API Types
// =============================================================================

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PlaceOrderRequest {
    symbol: String,
    product_type: String,
    margin_mode: String,
    margin_coin: String,
    side: String,
    trade_side: String,
    order_type: String,
    size: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    price: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    force: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_oid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reduce_only: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlaceOrderResponse {
    order_id: String,
    #[serde(default)]
    client_oid: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PositionData {
    symbol: String,
    hold_side: String,
    #[serde(default)]
    total: Option<String>,
    #[serde(default)]
    open_price_avg: Option<String>,
    #[serde(default)]
    leverage: Option<String>,
    #[serde(default)]
    unrealized_pl: Option<String>,
    #[serde(default)]
    realized_pnl: Option<String>,
    #[serde(default)]
    margin_mode: Option<String>,
    #[serde(default)]
    margin: Option<String>,
    #[serde(default)]
    liquidation_price: Option<String>,
    #[serde(default)]
    mark_price: Option<String>,
    #[serde(default)]
    u_time: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FundingRateData {
    symbol: String,
    funding_rate: String,
    #[serde(default)]
    funding_time: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderDetail {
    order_id: String,
    symbol: String,
    side: String,
    #[serde(default)]
    trade_side: Option<String>,
    order_type: String,
    price: String,
    size: String,
    #[serde(default)]
    filled_qty: Option<String>,
    status: String,
    #[serde(default)]
    c_time: Option<String>,
    #[serde(default)]
    u_time: Option<String>,
    #[serde(default)]
    client_oid: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TickerData {
    symbol: String,
    #[serde(default)]
    bid_pr: Option<String>,
    #[serde(default)]
    ask_pr: Option<String>,
    #[serde(default)]
    last_pr: Option<String>,
    #[serde(default)]
    high_24h: Option<String>,
    #[serde(default)]
    low_24h: Option<String>,
    #[serde(default)]
    open_24h: Option<String>,
    #[serde(default)]
    base_volume: Option<String>,
    #[serde(default)]
    quote_volume: Option<String>,
    #[serde(default)]
    ts: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ContractInfo {
    symbol: String,
    base_coin: String,
    quote_coin: String,
    min_trade_num: String,
    #[serde(default)]
    max_trade_num: Option<String>,
    price_precision: String,
    volume_precision: String,
    #[serde(default)]
    max_leverage: Option<String>,
    status: String,
}

// =============================================================================
// PerpRest Implementation
// =============================================================================

#[async_trait::async_trait]
impl PerpRest for BitgetPerpsAdapter {
    async fn create_order(&self, new: NewOrder) -> Result<Order> {
        let client_oid = if new.client_order_id.is_empty() {
            Self::generate_client_id()
        } else {
            new.client_order_id.clone()
        };

        // Determine trade side (open/close)
        let trade_side = if new.reduce_only { "close" } else { "open" };

        let request = PlaceOrderRequest {
            symbol: Self::to_bitget_symbol(&new.symbol),
            product_type: self.product_type.clone(),
            margin_mode: "crossed".to_string(),
            margin_coin: self.margin_coin.clone(),
            side: converters::to_bitget_side(new.side).to_string(),
            trade_side: trade_side.to_string(),
            order_type: converters::to_bitget_order_type(new.ord_type).to_string(),
            size: new.qty.to_string(),
            price: new.price.map(|p| p.to_string()),
            force: new.tif.map(|t| converters::to_bitget_tif(t).to_string()),
            client_oid: Some(client_oid.clone()),
            reduce_only: if new.reduce_only {
                Some("YES".to_string())
            } else {
                None
            },
        };

        let response: BitgetResponse<PlaceOrderResponse> = self
            .call_api("/api/v2/mix/order/place-order", || async {
                self.client
                    .post_private("/api/v2/mix/order/place-order", &request)
                    .await
            })
            .await?;

        let result = response.into_result()?;
        let now = Self::now_millis();

        Ok(Order {
            venue_order_id: result.order_id,
            client_order_id: client_oid,
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
        #[serde(rename_all = "camelCase")]
        struct CancelRequest {
            symbol: String,
            product_type: String,
            order_id: String,
        }

        let request = CancelRequest {
            symbol: Self::to_bitget_symbol(symbol),
            product_type: self.product_type.clone(),
            order_id: venue_order_id.to_string(),
        };

        let response: BitgetResponse<serde_json::Value> = self
            .call_api("/api/v2/mix/order/cancel-order", || async {
                self.client
                    .post_private("/api/v2/mix/order/cancel-order", &request)
                    .await
            })
            .await?;

        Ok(response.is_ok())
    }

    async fn cancel_all(&self, symbol: Option<&str>) -> Result<usize> {
        if let Some(sym) = symbol {
            #[derive(Serialize)]
            #[serde(rename_all = "camelCase")]
            struct CancelAllRequest {
                symbol: String,
                product_type: String,
            }

            let request = CancelAllRequest {
                symbol: Self::to_bitget_symbol(sym),
                product_type: self.product_type.clone(),
            };

            let _response: BitgetResponse<serde_json::Value> = self
                .call_api("/api/v2/mix/order/cancel-all-orders", || async {
                    self.client
                        .post_private("/api/v2/mix/order/cancel-all-orders", &request)
                        .await
                })
                .await?;
        }
        Ok(1)
    }

    async fn get_order(&self, symbol: &str, venue_order_id: &str) -> Result<Order> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), Self::to_bitget_symbol(symbol));
        params.insert("productType".to_string(), self.product_type.clone());
        params.insert("orderId".to_string(), venue_order_id.to_string());

        let response: BitgetResponse<Vec<OrderDetail>> = self
            .call_api("/api/v2/mix/order/detail", || async {
                self.client
                    .get_private("/api/v2/mix/order/detail", Some(params.clone()))
                    .await
            })
            .await?;

        let orders = response.into_result()?;
        let order = orders.first().context("Order not found")?;
        let now = Self::now_millis();

        let qty: f64 = parse_f64_or_warn(&order.size, "size");
        let filled: f64 = order
            .filled_qty
            .as_ref()
            .map(|v| parse_f64_or_warn(v, "filled_qty"))
            .unwrap_or(0.0);

        Ok(Order {
            venue_order_id: order.order_id.clone(),
            client_order_id: order.client_oid.clone().unwrap_or_default(),
            symbol: Self::to_standard_symbol(&order.symbol),
            ord_type: converters::from_bitget_order_type(&order.order_type),
            side: converters::from_bitget_side(&order.side),
            qty,
            price: order.price.parse().ok(),
            stop_price: None,
            tif: None,
            status: converters::from_bitget_order_status(&order.status),
            filled_qty: filled,
            remaining_qty: qty - filled,
            created_ms: order
                .c_time
                .as_ref()
                .and_then(|t| t.parse().ok())
                .unwrap_or(now),
            updated_ms: order
                .u_time
                .as_ref()
                .and_then(|t| t.parse().ok())
                .unwrap_or(now),
            recv_ms: now,
            raw_status: Some(order.status.clone()),
        })
    }

    async fn get_open_orders(&self, symbol: Option<&str>) -> Result<Vec<Order>> {
        let mut params = HashMap::new();
        params.insert("productType".to_string(), self.product_type.clone());
        if let Some(s) = symbol {
            params.insert("symbol".to_string(), Self::to_bitget_symbol(s));
        }

        let response: BitgetResponse<Vec<OrderDetail>> = self
            .call_api("/api/v2/mix/order/orders-pending", || async {
                self.client
                    .get_private("/api/v2/mix/order/orders-pending", Some(params.clone()))
                    .await
            })
            .await?;

        let orders = response.into_result().unwrap_or_default();
        let now = Self::now_millis();

        Ok(orders
            .into_iter()
            .map(|order| {
                let qty: f64 = parse_f64_or_warn(&order.size, "size");
                let filled: f64 = order
                    .filled_qty
                    .as_ref()
                    .map(|v| parse_f64_or_warn(v, "filled_qty"))
                    .unwrap_or(0.0);
                Order {
                    venue_order_id: order.order_id,
                    client_order_id: order.client_oid.unwrap_or_default(),
                    symbol: Self::to_standard_symbol(&order.symbol),
                    ord_type: converters::from_bitget_order_type(&order.order_type),
                    side: converters::from_bitget_side(&order.side),
                    qty,
                    price: order.price.parse().ok(),
                    stop_price: None,
                    tif: None,
                    status: converters::from_bitget_order_status(&order.status),
                    filled_qty: filled,
                    remaining_qty: qty - filled,
                    created_ms: order.c_time.and_then(|t| t.parse().ok()).unwrap_or(now),
                    updated_ms: order.u_time.and_then(|t| t.parse().ok()).unwrap_or(now),
                    recv_ms: now,
                    raw_status: Some(order.status),
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
        // Bitget doesn't support order amendment, cancel and replace
        self.cancel_order(symbol, venue_order_id).await?;

        let original = self.get_order(symbol, venue_order_id).await?;
        let new_order = NewOrder {
            symbol: symbol.to_string(),
            side: original.side,
            ord_type: original.ord_type,
            qty: new_qty.unwrap_or(original.qty),
            price: new_price.or(original.price),
            stop_price: None,
            tif: original.tif,
            post_only: false,
            reduce_only: false,
            client_order_id: Self::generate_client_id(),
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

    async fn set_leverage(&self, symbol: &str, leverage: Decimal) -> Result<()> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct LeverageRequest {
            symbol: String,
            product_type: String,
            margin_coin: String,
            leverage: String,
            hold_side: String,
        }

        // Set leverage for both long and short sides
        for side in ["long", "short"] {
            let request = LeverageRequest {
                symbol: Self::to_bitget_symbol(symbol),
                product_type: self.product_type.clone(),
                margin_coin: self.margin_coin.clone(),
                leverage: leverage.to_string(),
                hold_side: side.to_string(),
            };

            let _response: BitgetResponse<serde_json::Value> = self
                .call_api("/api/v2/mix/account/set-leverage", || async {
                    self.client
                        .post_private("/api/v2/mix/account/set-leverage", &request)
                        .await
                })
                .await?;
        }

        Ok(())
    }

    async fn set_margin_mode(&self, symbol: &str, mode: MarginMode) -> Result<()> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct MarginModeRequest {
            symbol: String,
            product_type: String,
            margin_coin: String,
            margin_mode: String,
        }

        let request = MarginModeRequest {
            symbol: Self::to_bitget_symbol(symbol),
            product_type: self.product_type.clone(),
            margin_coin: self.margin_coin.clone(),
            margin_mode: converters::to_bitget_margin_mode(mode).to_string(),
        };

        let _response: BitgetResponse<serde_json::Value> = self
            .call_api("/api/v2/mix/account/set-margin-mode", || async {
                self.client
                    .post_private("/api/v2/mix/account/set-margin-mode", &request)
                    .await
            })
            .await?;

        Ok(())
    }

    async fn get_position(&self, symbol: &str) -> Result<Position> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), Self::to_bitget_symbol(symbol));
        params.insert("productType".to_string(), self.product_type.clone());

        let response: BitgetResponse<Vec<PositionData>> = self
            .call_api("/api/v2/mix/position/single-position", || async {
                self.client
                    .get_private("/api/v2/mix/position/single-position", Some(params.clone()))
                    .await
            })
            .await?;

        let positions = response.into_result()?;
        let pos = positions.first().context("Position not found")?;
        let now = Self::now_millis();

        let qty = pos
            .total
            .as_ref()
            .map(|s| parse_f64_or_warn(s, "total"))
            .unwrap_or(0.0);
        let entry_px = pos
            .open_price_avg
            .as_ref()
            .map(|s| parse_f64_or_warn(s, "open_price_avg"))
            .unwrap_or(0.0);

        // Convert to signed qty based on side
        let signed_qty = match pos.hold_side.to_lowercase().as_str() {
            "short" => -qty,
            _ => qty,
        };

        Ok(Position {
            exchange: Some("bitget".to_string()),
            symbol: Self::to_standard_symbol(&pos.symbol),
            qty: signed_qty,
            entry_px,
            mark_px: pos.mark_price.as_ref().and_then(|s| s.parse().ok()),
            liquidation_px: pos.liquidation_price.as_ref().and_then(|s| s.parse().ok()),
            unrealized_pnl: pos.unrealized_pl.as_ref().and_then(|s| s.parse().ok()),
            realized_pnl: pos.realized_pnl.as_ref().and_then(|s| s.parse().ok()),
            margin: pos.margin.as_ref().and_then(|s| s.parse().ok()),
            leverage: pos.leverage.as_ref().and_then(|s| s.parse().ok()),
            opened_ms: None,
            updated_ms: pos.u_time.as_ref().and_then(|t| t.parse().ok()).unwrap_or(now),
        })
    }

    async fn get_all_positions(&self) -> Result<Vec<Position>> {
        let mut params = HashMap::new();
        params.insert("productType".to_string(), self.product_type.clone());

        let response: BitgetResponse<Vec<PositionData>> = self
            .call_api("/api/v2/mix/position/all-position", || async {
                self.client
                    .get_private("/api/v2/mix/position/all-position", Some(params.clone()))
                    .await
            })
            .await?;

        let positions = response.into_result().unwrap_or_default();
        let now = Self::now_millis();

        Ok(positions
            .into_iter()
            .filter(|p| {
                p.total
                    .as_ref()
                    .map(|s| parse_f64_or_warn(s, "total") > 0.0)
                    .unwrap_or(false)
            })
            .map(|pos| {
                let qty = pos
                    .total
                    .as_ref()
                    .map(|s| parse_f64_or_warn(s, "total"))
                    .unwrap_or(0.0);
                let entry_px = pos
                    .open_price_avg
                    .as_ref()
                    .map(|s| parse_f64_or_warn(s, "open_price_avg"))
                    .unwrap_or(0.0);

                let signed_qty = match pos.hold_side.to_lowercase().as_str() {
                    "short" => -qty,
                    _ => qty,
                };

                Position {
                    exchange: Some("bitget".to_string()),
                    symbol: Self::to_standard_symbol(&pos.symbol),
                    qty: signed_qty,
                    entry_px,
                    mark_px: pos.mark_price.as_ref().and_then(|s| s.parse().ok()),
                    liquidation_px: pos.liquidation_price.as_ref().and_then(|s| s.parse().ok()),
                    unrealized_pnl: pos.unrealized_pl.as_ref().and_then(|s| s.parse().ok()),
                    realized_pnl: pos.realized_pnl.as_ref().and_then(|s| s.parse().ok()),
                    margin: pos.margin.as_ref().and_then(|s| s.parse().ok()),
                    leverage: pos.leverage.as_ref().and_then(|s| s.parse().ok()),
                    opened_ms: None,
                    updated_ms: pos.u_time.as_ref().and_then(|t| t.parse().ok()).unwrap_or(now),
                }
            })
            .collect())
    }

    async fn get_funding_rate(&self, symbol: &str) -> Result<(Decimal, UnixMillis)> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), Self::to_bitget_symbol(symbol));
        params.insert("productType".to_string(), self.product_type.clone());

        let response: BitgetResponse<Vec<FundingRateData>> = self
            .call_api("/api/v2/mix/market/current-fund-rate", || async {
                self.client
                    .get_public(
                        "/api/v2/mix/market/current-fund-rate",
                        Some(params.clone()),
                    )
                    .await
            })
            .await?;

        let data = response.into_result()?;
        let funding = data.first().context("Funding rate not found")?;

        let rate: Decimal = funding.funding_rate.parse().unwrap_or_default();
        let ts = funding
            .funding_time
            .as_ref()
            .and_then(|t| t.parse().ok())
            .unwrap_or_else(Self::now_millis);

        Ok((rate, ts))
    }

    async fn get_balances(&self) -> Result<Vec<Balance>> {
        let mut params = HashMap::new();
        params.insert("productType".to_string(), self.product_type.clone());

        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct AccountBalance {
            margin_coin: String,
            available: String,
            frozen: String,
        }

        let response: BitgetResponse<Vec<AccountBalance>> = self
            .call_api("/api/v2/mix/account/accounts", || async {
                self.client
                    .get_private("/api/v2/mix/account/accounts", Some(params.clone()))
                    .await
            })
            .await?;

        let balances = response.into_result().unwrap_or_default();

        Ok(balances
            .into_iter()
            .map(|b| {
                let free: f64 = parse_f64_or_warn(&b.available, "available");
                let locked: f64 = parse_f64_or_warn(&b.frozen, "frozen");
                Balance {
                    asset: b.margin_coin,
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
            update_ms: Self::now_millis(),
        })
    }

    async fn get_market_info(&self, symbol: &str) -> Result<MarketInfo> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), Self::to_bitget_symbol(symbol));
        params.insert("productType".to_string(), self.product_type.clone());

        let response: BitgetResponse<Vec<ContractInfo>> = self
            .call_api("/api/v2/mix/market/contracts", || async {
                self.client
                    .get_public("/api/v2/mix/market/contracts", Some(params.clone()))
                    .await
            })
            .await?;

        let contracts = response.into_result()?;
        let info = contracts.first().context("Contract not found")?;

        let price_precision: u32 = info.price_precision.parse().unwrap_or(8);
        let qty_precision: u32 = info.volume_precision.parse().unwrap_or(8);
        let tick_size = 10f64.powi(-(price_precision as i32));
        let step_size = 10f64.powi(-(qty_precision as i32));

        Ok(MarketInfo {
            symbol: Self::to_standard_symbol(&info.symbol),
            base_asset: info.base_coin.clone(),
            quote_asset: info.quote_coin.clone(),
            status: if info.status == "normal" {
                MarketStatus::Trading
            } else {
                MarketStatus::Halt
            },
            min_qty: parse_f64_or_warn(&info.min_trade_num, "min_trade_num"),
            max_qty: info
                .max_trade_num
                .as_ref()
                .map(|s| parse_f64_or_warn(s, "max_trade_num"))
                .unwrap_or(0.0),
            step_size,
            tick_size,
            min_notional: 0.0,
            max_leverage: info.max_leverage.as_ref().and_then(|s| s.parse().ok()),
            is_spot: false,
            is_perp: true,
        })
    }

    async fn get_all_markets(&self) -> Result<Vec<MarketInfo>> {
        let mut params = HashMap::new();
        params.insert("productType".to_string(), self.product_type.clone());

        let response: BitgetResponse<Vec<ContractInfo>> = self
            .call_api("/api/v2/mix/market/contracts", || async {
                self.client
                    .get_public("/api/v2/mix/market/contracts", Some(params.clone()))
                    .await
            })
            .await?;

        let contracts = response.into_result()?;

        Ok(contracts
            .into_iter()
            .map(|info| {
                let price_precision: u32 = info.price_precision.parse().unwrap_or(8);
                let qty_precision: u32 = info.volume_precision.parse().unwrap_or(8);
                let tick_size = 10f64.powi(-(price_precision as i32));
                let step_size = 10f64.powi(-(qty_precision as i32));

                MarketInfo {
                    symbol: Self::to_standard_symbol(&info.symbol),
                    base_asset: info.base_coin,
                    quote_asset: info.quote_coin,
                    status: if info.status == "normal" {
                        MarketStatus::Trading
                    } else {
                        MarketStatus::Halt
                    },
                    min_qty: info.min_trade_num.parse().unwrap_or(0.0),
                    max_qty: info
                        .max_trade_num
                        .as_ref()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0.0),
                    step_size,
                    tick_size,
                    min_notional: 0.0,
                    max_leverage: info.max_leverage.as_ref().and_then(|s| s.parse().ok()),
                    is_spot: false,
                    is_perp: true,
                }
            })
            .collect())
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), Self::to_bitget_symbol(symbol));
        params.insert("productType".to_string(), self.product_type.clone());

        let response: BitgetResponse<Vec<TickerData>> = self
            .call_api("/api/v2/mix/market/ticker", || async {
                self.client
                    .get_public("/api/v2/mix/market/ticker", Some(params.clone()))
                    .await
            })
            .await?;

        let tickers = response.into_result()?;
        let ticker = tickers.first().context("Ticker not found")?;
        let now = Self::now_millis();

        let last_price = ticker
            .last_pr
            .as_ref()
            .map(|s| parse_f64_or_warn(s, "last"))
            .unwrap_or(0.0);
        let open_price = ticker
            .open_24h
            .as_ref()
            .map(|s| parse_f64_or_warn(s, "open"))
            .unwrap_or(0.0);
        let price_change = last_price - open_price;
        let price_change_pct = if open_price > 0.0 {
            (price_change / open_price) * 100.0
        } else {
            0.0
        };

        Ok(TickerInfo {
            symbol: Self::to_standard_symbol(&ticker.symbol),
            last_price,
            bid_price: ticker
                .bid_pr
                .as_ref()
                .map(|s| parse_f64_or_warn(s, "bid"))
                .unwrap_or(0.0),
            ask_price: ticker
                .ask_pr
                .as_ref()
                .map(|s| parse_f64_or_warn(s, "ask"))
                .unwrap_or(0.0),
            volume_24h: ticker
                .base_volume
                .as_ref()
                .map(|s| parse_f64_or_warn(s, "volume"))
                .unwrap_or(0.0),
            price_change_24h: price_change,
            price_change_pct_24h: price_change_pct,
            high_24h: ticker
                .high_24h
                .as_ref()
                .map(|s| parse_f64_or_warn(s, "high"))
                .unwrap_or(0.0),
            low_24h: ticker
                .low_24h
                .as_ref()
                .map(|s| parse_f64_or_warn(s, "low"))
                .unwrap_or(0.0),
            open_price_24h: open_price,
            ts_ms: ticker
                .ts
                .as_ref()
                .and_then(|t| t.parse().ok())
                .unwrap_or(now),
        })
    }

    async fn get_tickers(&self, symbols: Option<Vec<String>>) -> Result<Vec<TickerInfo>> {
        let mut params = HashMap::new();
        params.insert("productType".to_string(), self.product_type.clone());

        let response: BitgetResponse<Vec<TickerData>> = self
            .call_api("/api/v2/mix/market/tickers", || async {
                self.client
                    .get_public("/api/v2/mix/market/tickers", Some(params.clone()))
                    .await
            })
            .await?;

        let tickers = response.into_result()?;
        let now = Self::now_millis();

        let result: Vec<TickerInfo> = tickers
            .into_iter()
            .filter(|t| {
                symbols
                    .as_ref()
                    .map(|s| s.contains(&Self::to_standard_symbol(&t.symbol)))
                    .unwrap_or(true)
            })
            .map(|ticker| {
                let last_price = ticker
                    .last_pr
                    .as_ref()
                    .map(|s| parse_f64_or_warn(s, "last"))
                    .unwrap_or(0.0);
                let open_price = ticker
                    .open_24h
                    .as_ref()
                    .map(|s| parse_f64_or_warn(s, "open"))
                    .unwrap_or(0.0);
                let price_change = last_price - open_price;
                let price_change_pct = if open_price > 0.0 {
                    (price_change / open_price) * 100.0
                } else {
                    0.0
                };

                TickerInfo {
                    symbol: Self::to_standard_symbol(&ticker.symbol),
                    last_price,
                    bid_price: ticker
                        .bid_pr
                        .as_ref()
                        .map(|s| parse_f64_or_warn(s, "bid"))
                        .unwrap_or(0.0),
                    ask_price: ticker
                        .ask_pr
                        .as_ref()
                        .map(|s| parse_f64_or_warn(s, "ask"))
                        .unwrap_or(0.0),
                    volume_24h: ticker
                        .base_volume
                        .as_ref()
                        .map(|s| parse_f64_or_warn(s, "volume"))
                        .unwrap_or(0.0),
                    price_change_24h: price_change,
                    price_change_pct_24h: price_change_pct,
                    high_24h: ticker
                        .high_24h
                        .as_ref()
                        .map(|s| parse_f64_or_warn(s, "high"))
                        .unwrap_or(0.0),
                    low_24h: ticker
                        .low_24h
                        .as_ref()
                        .map(|s| parse_f64_or_warn(s, "low"))
                        .unwrap_or(0.0),
                    open_price_24h: open_price,
                    ts_ms: ticker
                        .ts
                        .as_ref()
                        .and_then(|t| t.parse().ok())
                        .unwrap_or(now),
                }
            })
            .collect();

        Ok(result)
    }

    async fn get_mark_price(&self, symbol: &str) -> Result<(Price, UnixMillis)> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), Self::to_bitget_symbol(symbol));
        params.insert("productType".to_string(), self.product_type.clone());

        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct MarkPriceData {
            mark_price: String,
            ts: String,
        }

        let response: BitgetResponse<Vec<MarkPriceData>> = self
            .call_api("/api/v2/mix/market/mark-price", || async {
                self.client
                    .get_public("/api/v2/mix/market/mark-price", Some(params.clone()))
                    .await
            })
            .await?;

        let data = response.into_result()?;
        let price_data = data.first().context("Mark price not found")?;

        let price: f64 = parse_f64_or_warn(&price_data.mark_price, "mark_price");
        let ts: u64 = price_data.ts.parse().unwrap_or_else(|_| Self::now_millis());

        Ok((price, ts))
    }

    async fn get_index_price(&self, symbol: &str) -> Result<(Price, UnixMillis)> {
        // Bitget uses same endpoint for index price
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), Self::to_bitget_symbol(symbol));
        params.insert("productType".to_string(), self.product_type.clone());

        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct IndexPriceData {
            index_price: String,
            ts: String,
        }

        let response: BitgetResponse<Vec<IndexPriceData>> = self
            .call_api("/api/v2/mix/market/index-price", || async {
                self.client
                    .get_public("/api/v2/mix/market/index-price", Some(params.clone()))
                    .await
            })
            .await?;

        let data = response.into_result()?;
        let price_data = data.first().context("Index price not found")?;

        let price: f64 = parse_f64_or_warn(&price_data.index_price, "index_price");
        let ts: u64 = price_data.ts.parse().unwrap_or_else(|_| Self::now_millis());

        Ok((price, ts))
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
        params.insert("symbol".to_string(), Self::to_bitget_symbol(symbol));
        params.insert("productType".to_string(), self.product_type.clone());
        params.insert(
            "granularity".to_string(),
            match interval {
                KlineInterval::M1 => "1m",
                KlineInterval::M5 => "5m",
                KlineInterval::M15 => "15m",
                KlineInterval::M30 => "30m",
                KlineInterval::H1 => "1H",
                KlineInterval::H4 => "4H",
                KlineInterval::D1 => "1D",
            }
            .to_string(),
        );
        if let Some(st) = start_ms {
            params.insert("startTime".to_string(), st.to_string());
        }
        if let Some(et) = end_ms {
            params.insert("endTime".to_string(), et.to_string());
        }
        if let Some(l) = limit {
            params.insert("limit".to_string(), l.to_string());
        }

        let response: BitgetResponse<Vec<Vec<String>>> = self
            .call_api("/api/v2/mix/market/candles", || async {
                self.client
                    .get_public("/api/v2/mix/market/candles", Some(params.clone()))
                    .await
            })
            .await?;

        let candles = response.into_result().unwrap_or_default();

        Ok(candles
            .into_iter()
            .filter_map(|c| {
                if c.len() >= 6 {
                    let open_ms: u64 = c[0].parse().ok()?;
                    let interval_ms = match interval {
                        KlineInterval::M1 => 60_000,
                        KlineInterval::M5 => 300_000,
                        KlineInterval::M15 => 900_000,
                        KlineInterval::M30 => 1_800_000,
                        KlineInterval::H1 => 3_600_000,
                        KlineInterval::H4 => 14_400_000,
                        KlineInterval::D1 => 86_400_000,
                    };
                    Some(Kline {
                        symbol: symbol.to_string(),
                        open_ms,
                        close_ms: open_ms + interval_ms,
                        open: c[1].parse().ok()?,
                        high: c[2].parse().ok()?,
                        low: c[3].parse().ok()?,
                        close: c[4].parse().ok()?,
                        volume: c[5].parse().ok()?,
                        quote_volume: c.get(6).and_then(|v| v.parse().ok()).unwrap_or(0.0),
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
        start_ms: Option<UnixMillis>,
        end_ms: Option<UnixMillis>,
        limit: Option<usize>,
    ) -> Result<Vec<FundingRateHistory>> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), Self::to_bitget_symbol(symbol));
        params.insert("productType".to_string(), self.product_type.clone());
        if let Some(st) = start_ms {
            params.insert("startTime".to_string(), st.to_string());
        }
        if let Some(et) = end_ms {
            params.insert("endTime".to_string(), et.to_string());
        }
        if let Some(l) = limit {
            params.insert("limit".to_string(), l.to_string());
        }

        let response: BitgetResponse<Vec<FundingRateData>> = self
            .call_api("/api/v2/mix/market/history-fund-rate", || async {
                self.client
                    .get_public(
                        "/api/v2/mix/market/history-fund-rate",
                        Some(params.clone()),
                    )
                    .await
            })
            .await?;

        let data = response.into_result().unwrap_or_default();

        Ok(data
            .into_iter()
            .map(|f| FundingRateHistory {
                symbol: Self::to_standard_symbol(&f.symbol),
                rate: f.funding_rate.parse().unwrap_or_default(),
                ts_ms: f
                    .funding_time
                    .as_ref()
                    .and_then(|t| t.parse().ok())
                    .unwrap_or(0),
            })
            .collect())
    }
}

// =============================================================================
// WebSocket Message Types
// =============================================================================

#[derive(Debug, Deserialize)]
struct BitgetWsMessage {
    #[serde(default)]
    arg: Option<BitgetWsArg>,
    #[serde(default)]
    data: Option<serde_json::Value>,
    #[serde(default)]
    action: Option<String>,
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    msg: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BitgetWsArg {
    #[serde(rename = "instType")]
    inst_type: String,
    channel: String,
    #[serde(rename = "instId")]
    inst_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BitgetWsOrderData {
    inst_id: String,
    order_id: String,
    #[serde(default)]
    client_oid: Option<String>,
    side: String,
    pos_side: String,
    ord_type: String,
    #[serde(default)]
    px: Option<String>,
    sz: String,
    #[serde(default)]
    acc_fill_sz: Option<String>,
    ord_status: String,
    #[serde(default)]
    c_time: Option<String>,
    #[serde(default)]
    u_time: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BitgetWsPositionData {
    inst_id: String,
    pos_side: String,
    #[serde(default)]
    pos: Option<String>,
    #[serde(default)]
    avg_px: Option<String>,
    #[serde(default)]
    lever: Option<String>,
    #[serde(default)]
    unrealized_pl: Option<String>,
    #[serde(default)]
    u_time: Option<String>,
}

// =============================================================================
// PerpWs Implementation
// =============================================================================

#[async_trait::async_trait]
impl PerpWs for BitgetPerpsAdapter {
    async fn subscribe_user(&self) -> Result<mpsc::Receiver<UserEvent>> {
        use futures_util::{SinkExt, StreamExt};
        use std::sync::atomic::Ordering;
        use tokio_tungstenite::{connect_async, tungstenite::Message};
        use tracing::{error, info, warn};
        use crate::utils::{HeartbeatConfig, HeartbeatMonitor, ReconnectConfig, ReconnectStrategy};

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
            let reconnect_config = ReconnectConfig::production();
            let mut strategy = ReconnectStrategy::new(reconnect_config);

            'reconnect: loop {
                let reconnect_num = adapter.reconnect_count.load(Ordering::Relaxed);
                info!("Bitget Perps user stream connecting (reconnect #{})", reconnect_num);

                {
                    let mut status = adapter.connection_status.write().await;
                    *status = ConnectionStatus::Connecting;
                }

                let mut ws = match connect_async(super::account::BITGET_WS_PRIVATE_MIX_URL).await {
                    Ok((stream, _)) => stream,
                    Err(e) => {
                        error!("Bitget Perps WebSocket connection failed: {}", e);
                        if !strategy.can_retry() {
                            break 'reconnect;
                        }
                        let delay = strategy.wait_before_retry().await;
                        warn!("Bitget Perps user stream reconnecting - waiting {}ms", delay);
                        continue 'reconnect;
                    }
                };

                // Authenticate
                let timestamp = BitgetAuth::timestamp();
                let signature = auth_clone.sign_websocket(&timestamp);
                let auth_msg = serde_json::json!({
                    "op": "login",
                    "args": [{
                        "apiKey": auth_clone.api_key,
                        "passphrase": auth_clone.passphrase,
                        "timestamp": timestamp,
                        "sign": signature
                    }]
                });

                if ws.send(Message::Text(auth_msg.to_string())).await.is_err() {
                    error!("Bitget Perps failed to send auth message");
                    continue 'reconnect;
                }

                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

                // Subscribe to order and position updates
                let product_type = adapter.product_type.clone();
                let sub_msg = serde_json::json!({
                    "op": "subscribe",
                    "args": [{
                        "instType": product_type,
                        "channel": "orders",
                        "instId": "default"
                    }, {
                        "instType": product_type,
                        "channel": "positions",
                        "instId": "default"
                    }]
                });

                if ws.send(Message::Text(sub_msg.to_string())).await.is_err() {
                    error!("Bitget Perps failed to subscribe to user channels");
                    continue 'reconnect;
                }

                strategy.reset();
                adapter.reconnect_count.fetch_add(1, Ordering::Relaxed);
                {
                    let mut status = adapter.connection_status.write().await;
                    *status = ConnectionStatus::Connected;
                }
                info!("Bitget Perps user WebSocket connected");

                let heartbeat = HeartbeatMonitor::new(HeartbeatConfig::production());
                let mut ping_interval = tokio::time::interval(tokio::time::Duration::from_secs(30));

                'message_loop: loop {
                    tokio::select! {
                        _ = shutdown_rx.recv() => {
                            info!("Bitget Perps user stream shutdown signal received");
                            break 'reconnect;
                        }
                        _ = ping_interval.tick() => {
                            let ping_msg = Message::Text("ping".to_string());
                            if ws.send(ping_msg).await.is_err() {
                                warn!("Bitget Perps failed to send ping");
                                break 'message_loop;
                            }
                            heartbeat.record_ping_sent().await;
                        }
                        _ = tokio::time::sleep(tokio::time::Duration::from_secs(60)) => {
                            if !heartbeat.is_alive().await {
                                warn!("Bitget Perps user stream heartbeat timeout");
                                break 'message_loop;
                            }
                        }
                        msg = ws.next() => {
                            match msg {
                                Some(Ok(Message::Text(text))) => {
                                    heartbeat.record_message_received().await;

                                    if text == "pong" {
                                        heartbeat.record_pong_received().await;
                                        continue;
                                    }

                                    if let Ok(event) = parse_bitget_perps_user_event(&text) {
                                        if tx.send(event).await.is_err() {
                                            info!("Bitget Perps user stream receiver dropped");
                                            break 'reconnect;
                                        }
                                    }
                                }
                                Some(Ok(Message::Close(_))) => {
                                    warn!("Bitget Perps WebSocket closed by server");
                                    break 'message_loop;
                                }
                                Some(Err(e)) => {
                                    error!("Bitget Perps WebSocket error: {}", e);
                                    break 'message_loop;
                                }
                                None => {
                                    warn!("Bitget Perps WebSocket stream ended");
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
                    break 'reconnect;
                }
                let delay = strategy.wait_before_retry().await;
                warn!("Bitget Perps user stream reconnecting in {}ms", delay);
            }

            info!("Bitget Perps user stream terminated");
        });

        Ok(rx)
    }

    async fn subscribe_books(&self, _symbols: &[&str]) -> Result<mpsc::Receiver<BookUpdate>> {
        let (_tx, rx) = mpsc::channel(100);
        // Book subscriptions not yet implemented
        Ok(rx)
    }

    async fn subscribe_trades(&self, _symbols: &[&str]) -> Result<mpsc::Receiver<TradeEvent>> {
        let (_tx, rx) = mpsc::channel(100);
        // Trade subscriptions not yet implemented
        Ok(rx)
    }

    async fn health(&self) -> Result<HealthStatus> {
        let status = *self.connection_status.read().await;
        let reconnect_count = self.reconnect_count.load(std::sync::atomic::Ordering::Relaxed);

        Ok(HealthStatus {
            status,
            last_ping_ms: None,
            last_pong_ms: None,
            latency_ms: None,
            reconnect_count,
            error_msg: None,
        })
    }

    async fn reconnect(&self) -> Result<()> {
        if let Some(tx) = self.shutdown_tx.lock().await.as_ref() {
            let _ = tx.send(());
        }
        Ok(())
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

fn parse_bitget_perps_user_event(text: &str) -> Result<UserEvent> {
    let msg: BitgetWsMessage = serde_json::from_str(text)?;

    if let Some(data) = msg.data {
        if let Some(arg) = msg.arg {
            // Handle order updates
            if arg.channel == "orders" {
                if let Ok(orders) = serde_json::from_value::<Vec<BitgetWsOrderData>>(data.clone()) {
                    if let Some(order) = orders.first() {
                        let qty = parse_f64_or_warn(&order.sz, "ws_order_qty");
                        let filled = order
                            .acc_fill_sz
                            .as_ref()
                            .map(|s| parse_f64_or_warn(s, "ws_filled_qty"))
                            .unwrap_or(0.0);
                        let now = safe_now_millis();

                        let order_struct = Order {
                            venue_order_id: order.order_id.clone(),
                            client_order_id: order.client_oid.clone().unwrap_or_default(),
                            symbol: BitgetPerpsAdapter::to_standard_symbol(&order.inst_id),
                            ord_type: converters::from_bitget_order_type(&order.ord_type),
                            side: converters::from_bitget_side(&order.side),
                            qty,
                            price: order.px.as_ref().and_then(|p| p.parse().ok()),
                            stop_price: None,
                            tif: None,
                            status: converters::from_bitget_order_status(&order.ord_status),
                            filled_qty: filled,
                            remaining_qty: qty - filled,
                            created_ms: order
                                .c_time
                                .as_ref()
                                .and_then(|t| t.parse().ok())
                                .unwrap_or(now),
                            updated_ms: order
                                .u_time
                                .as_ref()
                                .and_then(|t| t.parse().ok())
                                .unwrap_or(now),
                            recv_ms: now,
                            raw_status: Some(order.ord_status.clone()),
                        };

                        return Ok(UserEvent::OrderUpdate(order_struct));
                    }
                }
            }

            // Handle position updates
            if arg.channel == "positions" {
                if let Ok(positions) = serde_json::from_value::<Vec<BitgetWsPositionData>>(data) {
                    if let Some(pos) = positions.first() {
                        let qty = pos
                            .pos
                            .as_ref()
                            .map(|s| parse_f64_or_warn(s, "ws_pos_qty"))
                            .unwrap_or(0.0);

                        // Convert to signed qty based on side
                        let signed_qty = match pos.pos_side.to_lowercase().as_str() {
                            "short" => -qty,
                            _ => qty,
                        };

                        let now = safe_now_millis();
                        let position = Position {
                            exchange: Some("bitget".to_string()),
                            symbol: BitgetPerpsAdapter::to_standard_symbol(&pos.inst_id),
                            qty: signed_qty,
                            entry_px: pos
                                .avg_px
                                .as_ref()
                                .map(|s| parse_f64_or_warn(s, "ws_avg_px"))
                                .unwrap_or(0.0),
                            mark_px: None,
                            liquidation_px: None,
                            unrealized_pnl: pos.unrealized_pl.as_ref().and_then(|s| s.parse().ok()),
                            realized_pnl: None,
                            margin: None,
                            leverage: pos.lever.as_ref().and_then(|s| s.parse().ok()),
                            opened_ms: None,
                            updated_ms: pos
                                .u_time
                                .as_ref()
                                .and_then(|t| t.parse().ok())
                                .unwrap_or(now),
                        };

                        return Ok(UserEvent::Position(position));
                    }
                }
            }
        }
    }

    anyhow::bail!("Failed to parse Bitget perps user event")
}
