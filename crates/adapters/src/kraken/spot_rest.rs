use crate::kraken::common::{
    converters, KrakenAuth, KrakenResponse, KrakenRestClient,
};
use crate::traits::*;
use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct KrakenSpotRest {
    client: KrakenRestClient,
}

impl KrakenSpotRest {
    pub fn new(api_key: String, api_secret: String) -> Self {
        let auth = KrakenAuth::new(api_key, api_secret);
        Self {
            client: KrakenRestClient::new_spot(Some(auth)),
        }
    }

    fn generate_client_order_id() -> String {
        format!("mm_{}", SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis())
    }
}

// Kraken-specific response types
#[derive(Debug, Deserialize)]
struct KrakenAddOrderResult {
    txid: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct KrakenCancelOrderResult {
    count: usize,
}

#[derive(Debug, Deserialize)]
struct KrakenOrderInfo {
    #[serde(flatten)]
    orders: HashMap<String, KrakenOrderDetails>,
}

#[derive(Debug, Deserialize)]
struct KrakenOrderDetails {
    status: String,
    #[serde(rename = "type")]
    side: String,
    ordertype: String,
    price: String,
    vol: String,
    vol_exec: String,
    opentm: f64,
    #[serde(default)]
    closetm: Option<f64>,
    #[serde(default)]
    userref: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct KrakenBalanceResult {
    #[serde(flatten)]
    balances: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct KrakenAssetPairsResult {
    #[serde(flatten)]
    pairs: HashMap<String, KrakenAssetPairInfo>,
}

#[derive(Debug, Deserialize)]
struct KrakenAssetPairInfo {
    #[allow(dead_code)]
    altname: String,
    #[allow(dead_code)]
    wsname: String,
    base: String,
    quote: String,
    pair_decimals: u32,
    ordermin: String,
    #[serde(default)]
    costmin: Option<String>,
    status: String,
}

#[derive(Debug, Deserialize)]
struct KrakenTickerResult {
    #[serde(flatten)]
    tickers: HashMap<String, KrakenTickerData>,
}

#[derive(Debug, Deserialize)]
struct KrakenTickerData {
    a: Vec<String>, // ask [price, whole lot volume, lot volume]
    b: Vec<String>, // bid [price, whole lot volume, lot volume]
    c: Vec<String>, // last trade [price, lot volume]
    v: Vec<String>, // volume [today, last 24 hours]
    #[allow(dead_code)]
    p: Vec<String>, // volume weighted average price [today, last 24 hours]
    #[allow(dead_code)]
    t: Vec<u64>,    // number of trades [today, last 24 hours]
    l: Vec<String>, // low [today, last 24 hours]
    h: Vec<String>, // high [today, last 24 hours]
    o: String,      // opening price
}

#[derive(Debug, Deserialize)]
struct KrakenOHLCResult {
    #[serde(flatten)]
    data: HashMap<String, Vec<Vec<serde_json::Value>>>,
}

// Helper functions
fn parse_kraken_status(status: &str) -> OrderStatus {
    match status {
        "pending" => OrderStatus::New,
        "open" => OrderStatus::New,
        "closed" => OrderStatus::Filled,
        "canceled" => OrderStatus::Canceled,
        "expired" => OrderStatus::Expired,
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
impl SpotRest for KrakenSpotRest {
    async fn create_order(&self, new: NewOrder) -> Result<Order> {
        let mut params = HashMap::new();
        params.insert("pair".to_string(), new.symbol.clone());
        params.insert("type".to_string(), converters::to_kraken_side(new.side).to_string());
        params.insert("ordertype".to_string(), converters::to_kraken_order_type(new.ord_type).to_string());
        params.insert("volume".to_string(), new.qty.to_string());

        if let Some(price) = new.price {
            params.insert("price".to_string(), price.to_string());
        }

        if let Some(stop_price) = new.stop_price {
            params.insert("price2".to_string(), stop_price.to_string());
        }

        if let Some(tif) = new.tif {
            params.insert("timeinforce".to_string(), converters::to_kraken_tif(tif).to_string());
        }

        if new.post_only {
            params.insert("oflags".to_string(), "post".to_string());
        }

        if new.reduce_only {
            params.insert("reduce_only".to_string(), "true".to_string());
        }

        params.insert("userref".to_string(), new.client_order_id.clone());

        let response: KrakenResponse<KrakenAddOrderResult> = self.client
            .post_private("/0/private/AddOrder", params)
            .await?;

        let result = response.into_result()?;
        let order_id = result.txid.first()
            .context("No order ID in response")?
            .clone();

        let now = now_millis();

        Ok(Order {
            venue_order_id: order_id,
            client_order_id: new.client_order_id,
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
            raw_status: Some("pending".to_string()),
        })
    }

    async fn cancel_order(&self, _symbol: &str, venue_order_id: &str) -> Result<bool> {
        let mut params = HashMap::new();
        params.insert("txid".to_string(), venue_order_id.to_string());

        let response: KrakenResponse<KrakenCancelOrderResult> = self.client
            .post_private("/0/private/CancelOrder", params)
            .await?;

        let result = response.into_result()?;
        Ok(result.count > 0)
    }

    async fn cancel_all(&self, symbol: Option<&str>) -> Result<usize> {
        // Get all open orders for the symbol
        let orders = self.get_open_orders(symbol).await?;
        let mut cancelled = 0;

        for order in orders {
            if self.cancel_order(&order.symbol, &order.venue_order_id).await.unwrap_or(false) {
                cancelled += 1;
            }
        }

        Ok(cancelled)
    }

    async fn get_order(&self, _symbol: &str, venue_order_id: &str) -> Result<Order> {
        let mut params = HashMap::new();
        params.insert("txid".to_string(), venue_order_id.to_string());
        params.insert("trades".to_string(), "false".to_string());

        let response: KrakenResponse<KrakenOrderInfo> = self.client
            .post_private("/0/private/QueryOrders", params)
            .await?;

        let result = response.into_result()?;
        let (order_id, details) = result.orders.iter().next()
            .context("Order not found")?;

        let now = now_millis();
        let filled = details.vol_exec.parse::<f64>().unwrap_or(0.0);
        let total = details.vol.parse::<f64>().unwrap_or(0.0);

        Ok(Order {
            venue_order_id: order_id.clone(),
            client_order_id: details.userref.map(|u| u.to_string()).unwrap_or_default(),
            symbol: "".to_string(), // Kraken doesn't return symbol in order query
            ord_type: converters::from_kraken_order_type(&details.ordertype),
            side: converters::from_kraken_side(&details.side),
            qty: total,
            price: Some(details.price.parse().unwrap_or(0.0)),
            stop_price: None,
            tif: None,
            status: parse_kraken_status(&details.status),
            filled_qty: filled,
            remaining_qty: total - filled,
            created_ms: (details.opentm * 1000.0) as UnixMillis,
            updated_ms: details.closetm.map(|t| (t * 1000.0) as UnixMillis).unwrap_or(now),
            recv_ms: now,
            raw_status: Some(details.status.clone()),
        })
    }

    async fn get_open_orders(&self, _symbol: Option<&str>) -> Result<Vec<Order>> {
        let params = HashMap::new();

        let response: KrakenResponse<HashMap<String, HashMap<String, KrakenOrderDetails>>> = self.client
            .post_private("/0/private/OpenOrders", params)
            .await?;

        let result = response.into_result()?;
        let orders_map = result.get("open").context("No open orders in response")?;

        let now = now_millis();
        let mut orders = Vec::new();

        for (order_id, details) in orders_map {
            let filled = details.vol_exec.parse::<f64>().unwrap_or(0.0);
            let total = details.vol.parse::<f64>().unwrap_or(0.0);

            orders.push(Order {
                venue_order_id: order_id.clone(),
                client_order_id: details.userref.map(|u| u.to_string()).unwrap_or_default(),
                symbol: "".to_string(),
                ord_type: converters::from_kraken_order_type(&details.ordertype),
                side: converters::from_kraken_side(&details.side),
                qty: total,
                price: Some(details.price.parse().unwrap_or(0.0)),
                stop_price: None,
                tif: None,
                status: parse_kraken_status(&details.status),
                filled_qty: filled,
                remaining_qty: total - filled,
                created_ms: (details.opentm * 1000.0) as UnixMillis,
                updated_ms: details.closetm.map(|t| (t * 1000.0) as UnixMillis).unwrap_or(now),
                recv_ms: now,
                raw_status: Some(details.status.clone()),
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
        // Kraken doesn't have native replace - we cancel and re-create
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

    async fn cancel_batch_orders(&self, _symbol: &str, order_ids: Vec<String>) -> Result<BatchCancelResult> {
        let mut success = Vec::new();
        let mut failed = Vec::new();

        for order_id in order_ids {
            match self.cancel_order("", &order_id).await {
                Ok(true) => success.push(order_id),
                Ok(false) => failed.push((order_id.clone(), "Cancellation returned false".to_string())),
                Err(e) => failed.push((order_id, e.to_string())),
            }
        }

        Ok(BatchCancelResult { success, failed })
    }

    async fn get_balances(&self) -> Result<Vec<Balance>> {
        let params = HashMap::new();

        let response: KrakenResponse<KrakenBalanceResult> = self.client
            .post_private("/0/private/Balance", params)
            .await?;

        let result = response.into_result()?;
        let mut balances = Vec::new();

        for (asset, amount_str) in result.balances {
            let amount = amount_str.parse::<f64>().unwrap_or(0.0);
            balances.push(Balance {
                asset,
                free: amount, // Kraken doesn't distinguish free/locked in Balance endpoint
                locked: 0.0,
                total: amount,
            });
        }

        Ok(balances)
    }

    async fn get_account_info(&self) -> Result<AccountInfo> {
        let balances = self.get_balances().await?;
        let now = now_millis();

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
        params.insert("pair".to_string(), symbol.to_string());

        let response: KrakenResponse<KrakenAssetPairsResult> = self.client
            .get_public("/0/public/AssetPairs", Some(params))
            .await?;

        let result = response.into_result()?;
        let (_, info) = result.pairs.iter().next()
            .context("Symbol not found")?;

        let status = match info.status.as_str() {
            "online" => MarketStatus::Trading,
            "cancel_only" => MarketStatus::PostTrading,
            "post_only" => MarketStatus::PreTrading,
            "limit_only" => MarketStatus::Trading,
            "reduce_only" => MarketStatus::PostTrading,
            _ => MarketStatus::Halt,
        };

        Ok(MarketInfo {
            symbol: symbol.to_string(),
            base_asset: info.base.clone(),
            quote_asset: info.quote.clone(),
            status,
            min_qty: info.ordermin.parse().unwrap_or(0.0),
            max_qty: f64::MAX,
            step_size: 10f64.powi(-(info.pair_decimals as i32)),
            tick_size: 10f64.powi(-(info.pair_decimals as i32)),
            min_notional: info.costmin.as_ref().and_then(|s| s.parse().ok()).unwrap_or(0.0),
            max_leverage: None, // Would need separate API call
            is_spot: true,
            is_perp: false,
        })
    }

    async fn get_all_markets(&self) -> Result<Vec<MarketInfo>> {
        let response: KrakenResponse<KrakenAssetPairsResult> = self.client
            .get_public("/0/public/AssetPairs", None)
            .await?;

        let result = response.into_result()?;
        let mut markets = Vec::new();

        for (symbol, info) in result.pairs {
            let status = match info.status.as_str() {
                "online" => MarketStatus::Trading,
                _ => MarketStatus::Halt,
            };

            markets.push(MarketInfo {
                symbol,
                base_asset: info.base.clone(),
                quote_asset: info.quote.clone(),
                status,
                min_qty: info.ordermin.parse().unwrap_or(0.0),
                max_qty: f64::MAX,
                step_size: 10f64.powi(-(info.pair_decimals as i32)),
                tick_size: 10f64.powi(-(info.pair_decimals as i32)),
                min_notional: info.costmin.as_ref().and_then(|s| s.parse().ok()).unwrap_or(0.0),
                max_leverage: None,
                is_spot: true,
                is_perp: false,
            });
        }

        Ok(markets)
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        let mut params = HashMap::new();
        params.insert("pair".to_string(), symbol.to_string());

        let response: KrakenResponse<KrakenTickerResult> = self.client
            .get_public("/0/public/Ticker", Some(params))
            .await?;

        let result = response.into_result()?;
        let (_, ticker) = result.tickers.iter().next()
            .context("Symbol not found")?;

        let last_price = ticker.c[0].parse().unwrap_or(0.0);
        let open_price = ticker.o.parse().unwrap_or(0.0);
        let price_change = last_price - open_price;
        let price_change_pct = if open_price > 0.0 {
            (price_change / open_price) * 100.0
        } else {
            0.0
        };

        Ok(TickerInfo {
            symbol: symbol.to_string(),
            last_price,
            bid_price: ticker.b[0].parse().unwrap_or(0.0),
            ask_price: ticker.a[0].parse().unwrap_or(0.0),
            volume_24h: ticker.v[1].parse().unwrap_or(0.0),
            price_change_24h: price_change,
            price_change_pct_24h: price_change_pct,
            high_24h: ticker.h[1].parse().unwrap_or(0.0),
            low_24h: ticker.l[1].parse().unwrap_or(0.0),
            open_price_24h: open_price,
            ts_ms: now_millis(),
        })
    }

    async fn get_tickers(&self, symbols: Option<Vec<String>>) -> Result<Vec<TickerInfo>> {
        let params = symbols.map(|syms| {
            let mut p = HashMap::new();
            p.insert("pair".to_string(), syms.join(","));
            p
        });

        let response: KrakenResponse<KrakenTickerResult> = self.client
            .get_public("/0/public/Ticker", params)
            .await?;

        let result = response.into_result()?;
        let mut tickers = Vec::new();

        for (symbol, ticker) in result.tickers {
            let last_price = ticker.c[0].parse().unwrap_or(0.0);
            let open_price = ticker.o.parse().unwrap_or(0.0);
            let price_change = last_price - open_price;
            let price_change_pct = if open_price > 0.0 {
                (price_change / open_price) * 100.0
            } else {
                0.0
            };

            tickers.push(TickerInfo {
                symbol,
                last_price,
                bid_price: ticker.b[0].parse().unwrap_or(0.0),
                ask_price: ticker.a[0].parse().unwrap_or(0.0),
                volume_24h: ticker.v[1].parse().unwrap_or(0.0),
                price_change_24h: price_change,
                price_change_pct_24h: price_change_pct,
                high_24h: ticker.h[1].parse().unwrap_or(0.0),
                low_24h: ticker.l[1].parse().unwrap_or(0.0),
                open_price_24h: open_price,
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
        _end_ms: Option<UnixMillis>,
        _limit: Option<usize>,
    ) -> Result<Vec<Kline>> {
        let interval_mins = match interval {
            KlineInterval::M1 => 1,
            KlineInterval::M5 => 5,
            KlineInterval::M15 => 15,
            KlineInterval::M30 => 30,
            KlineInterval::H1 => 60,
            KlineInterval::H4 => 240,
            KlineInterval::D1 => 1440,
        };

        let mut params = HashMap::new();
        params.insert("pair".to_string(), symbol.to_string());
        params.insert("interval".to_string(), interval_mins.to_string());

        if let Some(since) = start_ms {
            params.insert("since".to_string(), (since / 1000).to_string());
        }

        let response: KrakenResponse<KrakenOHLCResult> = self.client
            .get_public("/0/public/OHLC", Some(params))
            .await?;

        let result = response.into_result()?;
        let (_, ohlc_data) = result.data.iter().next()
            .context("No OHLC data in response")?;

        let mut klines = Vec::new();

        for candle in ohlc_data {
            if candle.len() < 8 {
                continue;
            }

            let open_time = candle[0].as_u64().unwrap_or(0) * 1000;

            klines.push(Kline {
                symbol: symbol.to_string(),
                open_ms: open_time,
                close_ms: open_time + (interval_mins as u64 * 60 * 1000),
                open: candle[1].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0),
                high: candle[2].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0),
                low: candle[3].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0),
                close: candle[4].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0),
                volume: candle[6].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0),
                quote_volume: 0.0, // Kraken doesn't provide this directly
                trades: candle[7].as_u64().unwrap_or(0),
            });
        }

        Ok(klines)
    }
}
