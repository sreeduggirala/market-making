use crate::mexc::common::{converters, MexcAuth, MexcRestClient};
use crate::traits::*;
use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct MexcSpotRest {
    client: MexcRestClient,
}

impl MexcSpotRest {
    pub fn new(api_key: String, api_secret: String) -> Self {
        let auth = MexcAuth::new(api_key, api_secret);
        Self {
            client: MexcRestClient::new_spot(Some(auth)),
        }
    }

    fn generate_client_order_id() -> String {
        format!(
            "mm_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
        )
    }
}

// MEXC-specific response types
#[derive(Debug, Deserialize)]
struct MexcOrderResponse {
    #[serde(rename = "orderId")]
    order_id: String,
    #[serde(rename = "clientOrderId")]
    client_order_id: Option<String>,
    symbol: String,
    status: String,
    side: String,
    #[serde(rename = "type")]
    order_type: String,
    price: Option<String>,
    #[serde(rename = "origQty")]
    orig_qty: String,
    #[serde(rename = "executedQty")]
    executed_qty: String,
    #[serde(rename = "transactTime")]
    #[serde(default)]
    transact_time: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct MexcOrderDetails {
    #[serde(rename = "orderId")]
    order_id: String,
    #[serde(rename = "clientOrderId")]
    client_order_id: Option<String>,
    symbol: String,
    price: String,
    #[serde(rename = "origQty")]
    orig_qty: String,
    #[serde(rename = "executedQty")]
    executed_qty: String,
    side: String,
    #[serde(rename = "type")]
    order_type: String,
    status: String,
    #[serde(rename = "time")]
    time: u64,
    #[serde(rename = "updateTime")]
    update_time: u64,
}

#[derive(Debug, Deserialize)]
struct MexcBalance {
    asset: String,
    free: String,
    locked: String,
}

#[derive(Debug, Deserialize)]
struct MexcAccountInfo {
    balances: Vec<MexcBalance>,
    #[serde(rename = "canTrade")]
    can_trade: bool,
    #[serde(rename = "canWithdraw")]
    can_withdraw: bool,
    #[serde(rename = "canDeposit")]
    can_deposit: bool,
}

#[derive(Debug, Deserialize)]
struct MexcSymbolInfo {
    symbol: String,
    status: String,
    #[serde(rename = "baseAsset")]
    base_asset: String,
    #[serde(rename = "quoteAsset")]
    quote_asset: String,
    #[serde(rename = "baseAssetPrecision")]
    #[allow(dead_code)]
    base_asset_precision: u32,
    #[serde(rename = "quotePrecision")]
    #[allow(dead_code)]
    quote_precision: u32,
    filters: Vec<MexcSymbolFilter>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "filterType")]
enum MexcSymbolFilter {
    #[serde(rename = "PRICE_FILTER")]
    PriceFilter {
        #[serde(rename = "minPrice")]
        #[allow(dead_code)]
        min_price: String,
        #[serde(rename = "maxPrice")]
        #[allow(dead_code)]
        max_price: String,
        #[serde(rename = "tickSize")]
        tick_size: String,
    },
    #[serde(rename = "LOT_SIZE")]
    LotSize {
        #[serde(rename = "minQty")]
        min_qty: String,
        #[serde(rename = "maxQty")]
        max_qty: String,
        #[serde(rename = "stepSize")]
        step_size: String,
    },
    #[serde(rename = "MIN_NOTIONAL")]
    MinNotional {
        #[serde(rename = "minNotional")]
        min_notional: String,
    },
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
struct MexcExchangeInfo {
    symbols: Vec<MexcSymbolInfo>,
}

#[derive(Debug, Deserialize)]
struct MexcTicker {
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

#[derive(Debug, Deserialize)]
struct MexcKline {
    #[serde(rename = "t")]
    open_time: u64,
    #[serde(rename = "o")]
    open: String,
    #[serde(rename = "h")]
    high: String,
    #[serde(rename = "l")]
    low: String,
    #[serde(rename = "c")]
    close: String,
    #[serde(rename = "v")]
    volume: String,
    #[serde(rename = "q")]
    quote_volume: String,
}

// Helper functions
fn parse_mexc_status(status: &str) -> OrderStatus {
    match status {
        "NEW" => OrderStatus::New,
        "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
        "FILLED" => OrderStatus::Filled,
        "CANCELED" | "PENDING_CANCEL" => OrderStatus::Canceled,
        "REJECTED" => OrderStatus::Rejected,
        "EXPIRED" => OrderStatus::Expired,
        _ => OrderStatus::Rejected,
    }
}

fn now_millis() -> UnixMillis {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[async_trait::async_trait]
impl SpotRest for MexcSpotRest {
    async fn create_order(&self, new: NewOrder) -> Result<Order> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), new.symbol.clone());
        params.insert("side".to_string(), converters::to_mexc_side(new.side).to_string());
        params.insert("type".to_string(), converters::to_mexc_order_type(new.ord_type).to_string());
        params.insert("quantity".to_string(), new.qty.to_string());

        if let Some(price) = new.price {
            params.insert("price".to_string(), price.to_string());
        }

        if let Some(tif) = new.tif {
            params.insert("timeInForce".to_string(), converters::to_mexc_tif(tif).to_string());
        }

        params.insert("newClientOrderId".to_string(), new.client_order_id.clone());

        let response: MexcOrderResponse = self
            .client
            .request_private(reqwest::Method::POST, "/api/v3/order", params)
            .await?;

        let now = now_millis();

        Ok(Order {
            venue_order_id: response.order_id,
            client_order_id: response.client_order_id.unwrap_or(new.client_order_id),
            symbol: response.symbol,
            ord_type: converters::from_mexc_order_type(&response.order_type),
            side: converters::from_mexc_side(&response.side),
            qty: response.orig_qty.parse().unwrap_or(0.0),
            price: response.price.and_then(|p| p.parse().ok()),
            stop_price: new.stop_price,
            tif: new.tif,
            status: parse_mexc_status(&response.status),
            filled_qty: response.executed_qty.parse().unwrap_or(0.0),
            remaining_qty: response.orig_qty.parse::<f64>().unwrap_or(0.0)
                - response.executed_qty.parse::<f64>().unwrap_or(0.0),
            created_ms: response.transact_time.unwrap_or(now),
            updated_ms: response.transact_time.unwrap_or(now),
            recv_ms: now,
            raw_status: Some(response.status),
        })
    }

    async fn cancel_order(&self, symbol: &str, venue_order_id: &str) -> Result<bool> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());
        params.insert("orderId".to_string(), venue_order_id.to_string());

        let _response: MexcOrderResponse = self
            .client
            .request_private(reqwest::Method::DELETE, "/api/v3/order", params)
            .await?;

        Ok(true)
    }

    async fn cancel_all(&self, symbol: Option<&str>) -> Result<usize> {
        let orders = self.get_open_orders(symbol).await?;
        let mut cancelled = 0;

        for order in orders {
            if self
                .cancel_order(&order.symbol, &order.venue_order_id)
                .await
                .unwrap_or(false)
            {
                cancelled += 1;
            }
        }

        Ok(cancelled)
    }

    async fn get_order(&self, symbol: &str, venue_order_id: &str) -> Result<Order> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());
        params.insert("orderId".to_string(), venue_order_id.to_string());

        let response: MexcOrderDetails = self
            .client
            .request_private(reqwest::Method::GET, "/api/v3/order", params)
            .await?;

        let now = now_millis();
        let filled = response.executed_qty.parse::<f64>().unwrap_or(0.0);
        let total = response.orig_qty.parse::<f64>().unwrap_or(0.0);

        Ok(Order {
            venue_order_id: response.order_id,
            client_order_id: response.client_order_id.unwrap_or_default(),
            symbol: response.symbol,
            ord_type: converters::from_mexc_order_type(&response.order_type),
            side: converters::from_mexc_side(&response.side),
            qty: total,
            price: Some(response.price.parse().unwrap_or(0.0)),
            stop_price: None,
            tif: None,
            status: parse_mexc_status(&response.status),
            filled_qty: filled,
            remaining_qty: total - filled,
            created_ms: response.time,
            updated_ms: response.update_time,
            recv_ms: now,
            raw_status: Some(response.status),
        })
    }

    async fn get_open_orders(&self, symbol: Option<&str>) -> Result<Vec<Order>> {
        let mut params = HashMap::new();
        if let Some(sym) = symbol {
            params.insert("symbol".to_string(), sym.to_string());
        }

        let response: Vec<MexcOrderDetails> = self
            .client
            .request_private(reqwest::Method::GET, "/api/v3/openOrders", params)
            .await?;

        let now = now_millis();
        let mut orders = Vec::new();

        for details in response {
            let filled = details.executed_qty.parse::<f64>().unwrap_or(0.0);
            let total = details.orig_qty.parse::<f64>().unwrap_or(0.0);

            orders.push(Order {
                venue_order_id: details.order_id,
                client_order_id: details.client_order_id.unwrap_or_default(),
                symbol: details.symbol,
                ord_type: converters::from_mexc_order_type(&details.order_type),
                side: converters::from_mexc_side(&details.side),
                qty: total,
                price: Some(details.price.parse().unwrap_or(0.0)),
                stop_price: None,
                tif: None,
                status: parse_mexc_status(&details.status),
                filled_qty: filled,
                remaining_qty: total - filled,
                created_ms: details.time,
                updated_ms: details.update_time,
                recv_ms: now,
                raw_status: Some(details.status),
            });
        }

        Ok(orders)
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
        // MEXC doesn't have native replace - we cancel and re-create
        let old_order = self.get_order(symbol, venue_order_id).await?;

        let cancelled = self.cancel_order(symbol, venue_order_id).await?;
        if !cancelled {
            anyhow::bail!("Failed to cancel old order");
        }

        let new_order_req = NewOrder {
            symbol: old_order.symbol.clone(),
            side: old_order.side,
            ord_type: old_order.ord_type,
            qty: new_qty.unwrap_or(old_order.qty),
            price: new_price.or(old_order.price),
            stop_price: old_order.stop_price,
            tif: new_tif.or(old_order.tif),
            post_only: post_only.unwrap_or(false),
            reduce_only: false,
            client_order_id: Self::generate_client_order_id(),
        };

        let new_order = self.create_order(new_order_req).await?;
        Ok((new_order, true))
    }

    async fn create_batch_orders(&self, batch: BatchOrderRequest) -> Result<BatchOrderResult> {
        let mut success = Vec::new();
        let mut failed = Vec::new();

        for order in batch.orders {
            match self.create_order(order.clone()).await {
                Ok(created) => success.push(created),
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
                Ok(true) => success.push(order_id),
                Ok(false) => failed.push((order_id.clone(), "Cancellation returned false".to_string())),
                Err(e) => failed.push((order_id, e.to_string())),
            }
        }

        Ok(BatchCancelResult { success, failed })
    }

    async fn get_balances(&self) -> Result<Vec<Balance>> {
        let params = HashMap::new();

        let response: MexcAccountInfo = self
            .client
            .request_private(reqwest::Method::GET, "/api/v3/account", params)
            .await?;

        let mut balances = Vec::new();

        for balance in response.balances {
            let free = balance.free.parse::<f64>().unwrap_or(0.0);
            let locked = balance.locked.parse::<f64>().unwrap_or(0.0);

            if free > 0.0 || locked > 0.0 {
                balances.push(Balance {
                    asset: balance.asset,
                    free,
                    locked,
                    total: free + locked,
                });
            }
        }

        Ok(balances)
    }

    async fn get_account_info(&self) -> Result<AccountInfo> {
        let params = HashMap::new();

        let response: MexcAccountInfo = self
            .client
            .request_private(reqwest::Method::GET, "/api/v3/account", params)
            .await?;

        let mut balances = Vec::new();

        for balance in response.balances {
            let free = balance.free.parse::<f64>().unwrap_or(0.0);
            let locked = balance.locked.parse::<f64>().unwrap_or(0.0);

            if free > 0.0 || locked > 0.0 {
                balances.push(Balance {
                    asset: balance.asset,
                    free,
                    locked,
                    total: free + locked,
                });
            }
        }

        let now = now_millis();

        Ok(AccountInfo {
            balances,
            can_trade: response.can_trade,
            can_withdraw: response.can_withdraw,
            can_deposit: response.can_deposit,
            update_ms: now,
        })
    }

    async fn get_market_info(&self, symbol: &str) -> Result<MarketInfo> {
        let response: MexcExchangeInfo = self
            .client
            .get_public("/api/v3/exchangeInfo", None)
            .await?;

        let symbol_info = response
            .symbols
            .iter()
            .find(|s| s.symbol == symbol)
            .context("Symbol not found")?;

        let status = match symbol_info.status.as_str() {
            "TRADING" => MarketStatus::Trading,
            "PRE_TRADING" => MarketStatus::PreTrading,
            "POST_TRADING" => MarketStatus::PostTrading,
            "HALT" => MarketStatus::Halt,
            _ => MarketStatus::Halt,
        };

        let mut min_qty = 0.0;
        let mut max_qty = f64::MAX;
        let mut step_size = 0.0;
        let mut tick_size = 0.0;
        let mut min_notional = 0.0;

        for filter in &symbol_info.filters {
            match filter {
                MexcSymbolFilter::LotSize {
                    min_qty: mq,
                    max_qty: maxq,
                    step_size: ss,
                } => {
                    min_qty = mq.parse().unwrap_or(0.0);
                    max_qty = maxq.parse().unwrap_or(f64::MAX);
                    step_size = ss.parse().unwrap_or(0.0);
                }
                MexcSymbolFilter::PriceFilter { tick_size: ts, .. } => {
                    tick_size = ts.parse().unwrap_or(0.0);
                }
                MexcSymbolFilter::MinNotional { min_notional: mn } => {
                    min_notional = mn.parse().unwrap_or(0.0);
                }
                _ => {}
            }
        }

        Ok(MarketInfo {
            symbol: symbol_info.symbol.clone(),
            base_asset: symbol_info.base_asset.clone(),
            quote_asset: symbol_info.quote_asset.clone(),
            status,
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
        let response: MexcExchangeInfo = self
            .client
            .get_public("/api/v3/exchangeInfo", None)
            .await?;

        let mut markets = Vec::new();

        for symbol_info in response.symbols {
            let status = match symbol_info.status.as_str() {
                "TRADING" => MarketStatus::Trading,
                _ => MarketStatus::Halt,
            };

            let mut min_qty = 0.0;
            let mut max_qty = f64::MAX;
            let mut step_size = 0.0;
            let mut tick_size = 0.0;
            let mut min_notional = 0.0;

            for filter in &symbol_info.filters {
                match filter {
                    MexcSymbolFilter::LotSize {
                        min_qty: mq,
                        max_qty: maxq,
                        step_size: ss,
                    } => {
                        min_qty = mq.parse().unwrap_or(0.0);
                        max_qty = maxq.parse().unwrap_or(f64::MAX);
                        step_size = ss.parse().unwrap_or(0.0);
                    }
                    MexcSymbolFilter::PriceFilter { tick_size: ts, .. } => {
                        tick_size = ts.parse().unwrap_or(0.0);
                    }
                    MexcSymbolFilter::MinNotional { min_notional: mn } => {
                        min_notional = mn.parse().unwrap_or(0.0);
                    }
                    _ => {}
                }
            }

            markets.push(MarketInfo {
                symbol: symbol_info.symbol,
                base_asset: symbol_info.base_asset,
                quote_asset: symbol_info.quote_asset,
                status,
                min_qty,
                max_qty,
                step_size,
                tick_size,
                min_notional,
                max_leverage: None,
                is_spot: true,
                is_perp: false,
            });
        }

        Ok(markets)
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());

        let response: MexcTicker = self
            .client
            .get_public("/api/v3/ticker/24hr", Some(params))
            .await?;

        Ok(TickerInfo {
            symbol: response.symbol,
            last_price: response.last_price.parse().unwrap_or(0.0),
            bid_price: response.bid_price.parse().unwrap_or(0.0),
            ask_price: response.ask_price.parse().unwrap_or(0.0),
            volume_24h: response.volume.parse().unwrap_or(0.0),
            price_change_24h: response.price_change.parse().unwrap_or(0.0),
            price_change_pct_24h: response.price_change_percent.parse().unwrap_or(0.0),
            high_24h: response.high_price.parse().unwrap_or(0.0),
            low_24h: response.low_price.parse().unwrap_or(0.0),
            open_price_24h: response.open_price.parse().unwrap_or(0.0),
            ts_ms: now_millis(),
        })
    }

    async fn get_tickers(&self, symbols: Option<Vec<String>>) -> Result<Vec<TickerInfo>> {
        let params = symbols.map(|syms| {
            let mut p = HashMap::new();
            p.insert("symbols".to_string(), format!("[\"{}\"]", syms.join("\",\"")));
            p
        });

        let response: Vec<MexcTicker> = self
            .client
            .get_public("/api/v3/ticker/24hr", params)
            .await?;

        let mut tickers = Vec::new();

        for ticker in response {
            tickers.push(TickerInfo {
                symbol: ticker.symbol,
                last_price: ticker.last_price.parse().unwrap_or(0.0),
                bid_price: ticker.bid_price.parse().unwrap_or(0.0),
                ask_price: ticker.ask_price.parse().unwrap_or(0.0),
                volume_24h: ticker.volume.parse().unwrap_or(0.0),
                price_change_24h: ticker.price_change.parse().unwrap_or(0.0),
                price_change_pct_24h: ticker.price_change_percent.parse().unwrap_or(0.0),
                high_24h: ticker.high_price.parse().unwrap_or(0.0),
                low_24h: ticker.low_price.parse().unwrap_or(0.0),
                open_price_24h: ticker.open_price.parse().unwrap_or(0.0),
                ts_ms: now_millis(),
            });
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
        let interval_str = match interval {
            KlineInterval::M1 => "1m",
            KlineInterval::M5 => "5m",
            KlineInterval::M15 => "15m",
            KlineInterval::M30 => "30m",
            KlineInterval::H1 => "1h",
            KlineInterval::H4 => "4h",
            KlineInterval::D1 => "1d",
        };

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());
        params.insert("interval".to_string(), interval_str.to_string());

        if let Some(start) = start_ms {
            params.insert("startTime".to_string(), start.to_string());
        }

        if let Some(end) = end_ms {
            params.insert("endTime".to_string(), end.to_string());
        }

        if let Some(lim) = limit {
            params.insert("limit".to_string(), lim.to_string());
        }

        let response: Vec<MexcKline> = self
            .client
            .get_public("/api/v3/klines", Some(params))
            .await?;

        let interval_ms = match interval {
            KlineInterval::M1 => 60_000,
            KlineInterval::M5 => 300_000,
            KlineInterval::M15 => 900_000,
            KlineInterval::M30 => 1_800_000,
            KlineInterval::H1 => 3_600_000,
            KlineInterval::H4 => 14_400_000,
            KlineInterval::D1 => 86_400_000,
        };

        let mut klines = Vec::new();

        for kline in response {
            klines.push(Kline {
                symbol: symbol.to_string(),
                open_ms: kline.open_time,
                close_ms: kline.open_time + interval_ms,
                open: kline.open.parse().unwrap_or(0.0),
                high: kline.high.parse().unwrap_or(0.0),
                low: kline.low.parse().unwrap_or(0.0),
                close: kline.close.parse().unwrap_or(0.0),
                volume: kline.volume.parse().unwrap_or(0.0),
                quote_volume: kline.quote_volume.parse().unwrap_or(0.0),
                trades: 0, // MEXC doesn't provide this in klines
            });
        }

        Ok(klines)
    }
}
