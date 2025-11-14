use crate::kraken::common::{converters, KrakenAuth, KrakenResponse, KrakenRestClient};
use crate::traits::*;
use anyhow::{Context, Result};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::HashMap;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct KrakenPerpsRest {
    client: KrakenRestClient,
}

impl KrakenPerpsRest {
    pub fn new(api_key: String, api_secret: String) -> Self {
        let auth = KrakenAuth::new(api_key, api_secret);
        Self {
            client: KrakenRestClient::new_futures(Some(auth)),
        }
    }

    fn generate_client_order_id() -> String {
        format!("mm_{}", SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis())
    }
}

// Kraken Futures-specific response types
#[derive(Debug, Deserialize)]
struct FuturesOrderResult {
    #[serde(rename = "sendStatus")]
    send_status: SendStatus,
}

#[derive(Debug, Deserialize)]
struct SendStatus {
    order_id: Option<String>,
    status: String,
    #[serde(rename = "orderEvents")]
    order_events: Option<Vec<OrderEvent>>,
}

#[derive(Debug, Deserialize)]
struct OrderEvent {
    order: Option<FuturesOrder>,
}

#[derive(Debug, Deserialize)]
struct FuturesOrder {
    order_id: String,
    #[serde(rename = "cliOrdId")]
    cli_ord_id: Option<String>,
    symbol: String,
    side: String,
    #[serde(rename = "orderType")]
    order_type: String,
    #[serde(rename = "limitPrice")]
    limit_price: Option<f64>,
    #[serde(rename = "stopPrice")]
    stop_price: Option<f64>,
    quantity: f64,
    filled: f64,
    #[serde(rename = "orderStatus")]
    order_status: String,
    timestamp: String,
    #[serde(rename = "lastUpdateTimestamp")]
    last_update_timestamp: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FuturesCancelResult {
    #[serde(rename = "cancelStatus")]
    cancel_status: CancelStatus,
}

#[derive(Debug, Deserialize)]
struct CancelStatus {
    status: String,
    order_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FuturesPosition {
    symbol: String,
    side: String,
    size: f64,
    #[serde(rename = "entryPrice")]
    entry_price: f64,
    #[serde(rename = "markPrice")]
    mark_price: Option<f64>,
    #[serde(rename = "liquidationPrice")]
    liquidation_price: Option<f64>,
    #[serde(rename = "unrealizedPnl")]
    unrealized_pnl: Option<f64>,
    #[serde(rename = "realizedPnl")]
    realized_pnl: Option<f64>,
    margin: Option<f64>,
    leverage: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct FuturesAccountInfo {
    balances: HashMap<String, f64>,
    #[serde(rename = "availableMargin")]
    available_margin: f64,
}

#[derive(Debug, Deserialize)]
struct FuturesInstrument {
    symbol: String,
    #[serde(rename = "type")]
    instrument_type: String,
    #[serde(rename = "underlying")]
    underlying: String,
    #[serde(rename = "tickSize")]
    tick_size: f64,
    #[serde(rename = "contractSize")]
    contract_size: f64,
    #[serde(rename = "minOrderSize")]
    min_order_size: Option<f64>,
    #[serde(rename = "maxOrderSize")]
    max_order_size: Option<f64>,
    #[serde(rename = "maxPositionSize")]
    max_position_size: Option<f64>,
    #[serde(rename = "tradeable")]
    tradeable: bool,
    #[serde(rename = "maxLeverage")]
    max_leverage: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct FuturesTicker {
    symbol: String,
    #[serde(rename = "last")]
    last_price: f64,
    bid: f64,
    ask: f64,
    volume: f64,
    #[serde(rename = "volumeQuote")]
    volume_quote: f64,
    #[serde(rename = "openInterest")]
    open_interest: Option<f64>,
    #[serde(rename = "markPrice")]
    mark_price: Option<f64>,
    #[serde(rename = "indexPrice")]
    index_price: Option<f64>,
    #[serde(rename = "fundingRate")]
    funding_rate: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct FuturesFundingRate {
    symbol: String,
    rate: f64,
    #[serde(rename = "timestamp")]
    timestamp: String,
}

fn now_millis() -> UnixMillis {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn parse_futures_status(status: &str) -> OrderStatus {
    match status.to_lowercase().as_str() {
        "placed" | "open" | "untouched" => OrderStatus::New,
        "partialfill" | "partially_filled" => OrderStatus::PartiallyFilled,
        "filled" => OrderStatus::Filled,
        "cancelled" | "canceled" => OrderStatus::Canceled,
        "rejected" => OrderStatus::Rejected,
        _ => OrderStatus::Rejected,
    }
}

#[async_trait::async_trait]
impl PerpRest for KrakenPerpsRest {
    async fn create_order(&self, new: NewOrder) -> Result<Order> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), new.symbol.clone());
        params.insert("side".to_string(), converters::to_kraken_side(new.side).to_string());
        params.insert("orderType".to_string(), converters::to_kraken_order_type(new.ord_type).to_string());
        params.insert("size".to_string(), new.qty.to_string());

        if let Some(price) = new.price {
            params.insert("limitPrice".to_string(), price.to_string());
        }

        if let Some(stop_price) = new.stop_price {
            params.insert("stopPrice".to_string(), stop_price.to_string());
        }

        if new.reduce_only {
            params.insert("reduceOnly".to_string(), "true".to_string());
        }

        if !new.client_order_id.is_empty() {
            params.insert("cliOrdId".to_string(), new.client_order_id.clone());
        }

        let response: KrakenResponse<FuturesOrderResult> = self.client
            .post_private("/sendorder", params)
            .await?;

        let result = response.into_result()?;
        let order_id = result.send_status.order_id
            .context("No order ID in response")?;

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
            raw_status: Some(result.send_status.status),
        })
    }

    async fn cancel_order(&self, _symbol: &str, venue_order_id: &str) -> Result<bool> {
        let mut params = HashMap::new();
        params.insert("order_id".to_string(), venue_order_id.to_string());

        let response: KrakenResponse<FuturesCancelResult> = self.client
            .post_private("/cancelorder", params)
            .await?;

        let result = response.into_result()?;
        Ok(result.cancel_status.status == "cancelled" || result.cancel_status.status == "canceled")
    }

    async fn cancel_all(&self, symbol: Option<&str>) -> Result<usize> {
        let mut params = HashMap::new();
        if let Some(sym) = symbol {
            params.insert("symbol".to_string(), sym.to_string());
        }

        let response: KrakenResponse<serde_json::Value> = self.client
            .post_private("/cancelallorders", params)
            .await?;

        let result = response.into_result()?;

        // Parse the number of cancelled orders from response
        if let Some(count) = result.get("cancelledOrders").and_then(|c| c.as_array()) {
            Ok(count.len())
        } else {
            Ok(0)
        }
    }

    async fn get_order(&self, _symbol: &str, venue_order_id: &str) -> Result<Order> {
        let mut params = HashMap::new();
        params.insert("order_id".to_string(), venue_order_id.to_string());

        let response: KrakenResponse<Vec<FuturesOrder>> = self.client
            .post_private("/orders", params)
            .await?;

        let orders = response.into_result()?;
        let order = orders.first().context("Order not found")?;

        let now = now_millis();

        Ok(Order {
            venue_order_id: order.order_id.clone(),
            client_order_id: order.cli_ord_id.clone().unwrap_or_default(),
            symbol: order.symbol.clone(),
            ord_type: converters::from_kraken_order_type(&order.order_type),
            side: converters::from_kraken_side(&order.side),
            qty: order.quantity,
            price: order.limit_price,
            stop_price: order.stop_price,
            tif: None,
            status: parse_futures_status(&order.order_status),
            filled_qty: order.filled,
            remaining_qty: order.quantity - order.filled,
            created_ms: now, // Would parse from timestamp in production
            updated_ms: now,
            recv_ms: now,
            raw_status: Some(order.order_status.clone()),
        })
    }

    async fn get_open_orders(&self, symbol: Option<&str>) -> Result<Vec<Order>> {
        let mut params = HashMap::new();
        if let Some(sym) = symbol {
            params.insert("symbol".to_string(), sym.to_string());
        }

        let response: KrakenResponse<Vec<FuturesOrder>> = self.client
            .post_private("/openorders", params)
            .await?;

        let orders = response.into_result()?;
        let now = now_millis();
        let mut result = Vec::new();

        for order in orders {
            result.push(Order {
                venue_order_id: order.order_id,
                client_order_id: order.cli_ord_id.unwrap_or_default(),
                symbol: order.symbol,
                ord_type: converters::from_kraken_order_type(&order.order_type),
                side: converters::from_kraken_side(&order.side),
                qty: order.quantity,
                price: order.limit_price,
                stop_price: order.stop_price,
                tif: None,
                status: parse_futures_status(&order.order_status),
                filled_qty: order.filled,
                remaining_qty: order.quantity - order.filled,
                created_ms: now,
                updated_ms: now,
                recv_ms: now,
                raw_status: Some(order.order_status),
            });
        }

        Ok(result)
    }

    async fn replace_order(
        &self,
        symbol: &str,
        venue_order_id: &str,
        new_price: Option<Price>,
        new_qty: Option<Quantity>,
        new_tif: Option<TimeInForce>,
        post_only: Option<bool>,
        reduce_only: Option<bool>,
    ) -> Result<(Order, bool)> {
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
            reduce_only: reduce_only.unwrap_or(false),
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

    async fn set_leverage(&self, symbol: &str, leverage: Decimal) -> Result<()> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());
        params.insert("leverage".to_string(), leverage.to_string());

        let _response: KrakenResponse<serde_json::Value> = self.client
            .post_private("/leverage/preferences", params)
            .await?;

        Ok(())
    }

    async fn set_margin_mode(&self, _symbol: &str, _mode: MarginMode) -> Result<()> {
        // Kraken Futures uses a different model for margin
        // This would need to be implemented based on their specific API
        Ok(())
    }

    async fn get_position(&self, symbol: &str) -> Result<Position> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());

        let response: KrakenResponse<Vec<FuturesPosition>> = self.client
            .post_private("/openpositions", params)
            .await?;

        let positions = response.into_result()?;
        let position = positions.iter()
            .find(|p| p.symbol == symbol)
            .context("Position not found")?;

        let qty = if position.side.to_lowercase() == "long" {
            position.size
        } else {
            -position.size
        };

        Ok(Position {
            symbol: position.symbol.clone(),
            qty,
            entry_px: position.entry_price,
            mark_px: position.mark_price,
            liquidation_px: position.liquidation_price,
            unrealized_pnl: position.unrealized_pnl,
            realized_pnl: position.realized_pnl,
            margin: position.margin,
            leverage: position.leverage.map(|l| l as u32),
            opened_ms: None,
            updated_ms: now_millis(),
        })
    }

    async fn get_all_positions(&self) -> Result<Vec<Position>> {
        let params = HashMap::new();

        let response: KrakenResponse<Vec<FuturesPosition>> = self.client
            .post_private("/openpositions", params)
            .await?;

        let positions = response.into_result()?;
        let mut result = Vec::new();

        for position in positions {
            let qty = if position.side.to_lowercase() == "long" {
                position.size
            } else {
                -position.size
            };

            result.push(Position {
                symbol: position.symbol.clone(),
                qty,
                entry_px: position.entry_price,
                mark_px: position.mark_price,
                liquidation_px: position.liquidation_price,
                unrealized_pnl: position.unrealized_pnl,
                realized_pnl: position.realized_pnl,
                margin: position.margin,
                leverage: position.leverage.map(|l| l as u32),
                opened_ms: None,
                updated_ms: now_millis(),
            });
        }

        Ok(result)
    }

    async fn get_funding_rate(&self, symbol: &str) -> Result<(Decimal, UnixMillis)> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());

        let response: KrakenResponse<FuturesTicker> = self.client
            .get_public("/tickers", Some(params))
            .await?;

        let ticker = response.into_result()?;
        let rate = ticker.funding_rate.unwrap_or(0.0);
        let rate_decimal = Decimal::from_str(&rate.to_string()).unwrap_or(Decimal::ZERO);

        Ok((rate_decimal, now_millis()))
    }

    async fn get_balances(&self) -> Result<Vec<Balance>> {
        let params = HashMap::new();

        let response: KrakenResponse<FuturesAccountInfo> = self.client
            .post_private("/accounts", params)
            .await?;

        let account = response.into_result()?;
        let mut balances = Vec::new();

        for (asset, amount) in account.balances {
            balances.push(Balance {
                asset,
                free: amount,
                locked: 0.0,
                total: amount,
            });
        }

        Ok(balances)
    }

    async fn get_account_info(&self) -> Result<AccountInfo> {
        let balances = self.get_balances().await?;

        Ok(AccountInfo {
            balances,
            can_trade: true,
            can_withdraw: true,
            can_deposit: true,
            update_ms: now_millis(),
        })
    }

    async fn get_market_info(&self, symbol: &str) -> Result<MarketInfo> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());

        let response: KrakenResponse<Vec<FuturesInstrument>> = self.client
            .get_public("/instruments", Some(params))
            .await?;

        let instruments = response.into_result()?;
        let instrument = instruments.iter()
            .find(|i| i.symbol == symbol)
            .context("Instrument not found")?;

        let status = if instrument.tradeable {
            MarketStatus::Trading
        } else {
            MarketStatus::Halt
        };

        Ok(MarketInfo {
            symbol: instrument.symbol.clone(),
            base_asset: instrument.underlying.clone(),
            quote_asset: "USD".to_string(), // Kraken futures are typically USD-settled
            status,
            min_qty: instrument.min_order_size.unwrap_or(0.0),
            max_qty: instrument.max_order_size.unwrap_or(f64::MAX),
            step_size: instrument.tick_size,
            tick_size: instrument.tick_size,
            min_notional: 0.0,
            max_leverage: instrument.max_leverage.map(|l| l as u32),
            is_spot: false,
            is_perp: instrument.instrument_type.to_lowercase().contains("perpetual"),
        })
    }

    async fn get_all_markets(&self) -> Result<Vec<MarketInfo>> {
        let response: KrakenResponse<Vec<FuturesInstrument>> = self.client
            .get_public("/instruments", None)
            .await?;

        let instruments = response.into_result()?;
        let mut markets = Vec::new();

        for instrument in instruments {
            let status = if instrument.tradeable {
                MarketStatus::Trading
            } else {
                MarketStatus::Halt
            };

            markets.push(MarketInfo {
                symbol: instrument.symbol.clone(),
                base_asset: instrument.underlying.clone(),
                quote_asset: "USD".to_string(),
                status,
                min_qty: instrument.min_order_size.unwrap_or(0.0),
                max_qty: instrument.max_order_size.unwrap_or(f64::MAX),
                step_size: instrument.tick_size,
                tick_size: instrument.tick_size,
                min_notional: 0.0,
                max_leverage: instrument.max_leverage.map(|l| l as u32),
                is_spot: false,
                is_perp: instrument.instrument_type.to_lowercase().contains("perpetual"),
            });
        }

        Ok(markets)
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());

        let response: KrakenResponse<Vec<FuturesTicker>> = self.client
            .get_public("/tickers", Some(params))
            .await?;

        let tickers = response.into_result()?;
        let ticker = tickers.first().context("Ticker not found")?;

        Ok(TickerInfo {
            symbol: ticker.symbol.clone(),
            last_price: ticker.last_price,
            bid_price: ticker.bid,
            ask_price: ticker.ask,
            volume_24h: ticker.volume,
            price_change_24h: 0.0, // Would need historical data
            price_change_pct_24h: 0.0,
            high_24h: 0.0,
            low_24h: 0.0,
            open_price_24h: 0.0,
            ts_ms: now_millis(),
        })
    }

    async fn get_tickers(&self, symbols: Option<Vec<String>>) -> Result<Vec<TickerInfo>> {
        let params = symbols.map(|syms| {
            let mut p = HashMap::new();
            p.insert("symbol".to_string(), syms.join(","));
            p
        });

        let response: KrakenResponse<Vec<FuturesTicker>> = self.client
            .get_public("/tickers", params)
            .await?;

        let tickers = response.into_result()?;
        let mut result = Vec::new();

        for ticker in tickers {
            result.push(TickerInfo {
                symbol: ticker.symbol.clone(),
                last_price: ticker.last_price,
                bid_price: ticker.bid,
                ask_price: ticker.ask,
                volume_24h: ticker.volume,
                price_change_24h: 0.0,
                price_change_pct_24h: 0.0,
                high_24h: 0.0,
                low_24h: 0.0,
                open_price_24h: 0.0,
                ts_ms: now_millis(),
            });
        }

        Ok(result)
    }

    async fn get_mark_price(&self, symbol: &str) -> Result<(Price, UnixMillis)> {
        let ticker = self.get_ticker(symbol).await?;

        // Get mark price from ticker (already includes it)
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());

        let response: KrakenResponse<Vec<FuturesTicker>> = self.client
            .get_public("/tickers", Some(params))
            .await?;

        let tickers = response.into_result()?;
        let ticker_data = tickers.first().context("Ticker not found")?;

        Ok((ticker_data.mark_price.unwrap_or(ticker_data.last_price), now_millis()))
    }

    async fn get_index_price(&self, symbol: &str) -> Result<(Price, UnixMillis)> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());

        let response: KrakenResponse<Vec<FuturesTicker>> = self.client
            .get_public("/tickers", Some(params))
            .await?;

        let tickers = response.into_result()?;
        let ticker = tickers.first().context("Ticker not found")?;

        Ok((ticker.index_price.unwrap_or(ticker.last_price), now_millis()))
    }

    async fn get_klines(
        &self,
        symbol: &str,
        interval: KlineInterval,
        start_ms: Option<UnixMillis>,
        end_ms: Option<UnixMillis>,
        limit: Option<usize>,
    ) -> Result<Vec<Kline>> {
        let resolution = match interval {
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
        params.insert("resolution".to_string(), resolution.to_string());

        if let Some(from) = start_ms {
            params.insert("from".to_string(), (from / 1000).to_string());
        }

        if let Some(to) = end_ms {
            params.insert("to".to_string(), (to / 1000).to_string());
        }

        // Kraken Futures chart endpoint
        let response: KrakenResponse<serde_json::Value> = self.client
            .get_public("/charts", Some(params))
            .await?;

        let result = response.into_result()?;

        // Parse the candles from response
        let mut klines = Vec::new();

        // This is a placeholder - actual parsing would depend on Kraken's response format
        // Would parse OHLCV data similar to spot implementation

        Ok(klines)
    }

    async fn get_funding_history(
        &self,
        symbol: &str,
        start_ms: Option<UnixMillis>,
        end_ms: Option<UnixMillis>,
        _limit: Option<usize>,
    ) -> Result<Vec<FundingRateHistory>> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());

        if let Some(from) = start_ms {
            params.insert("from".to_string(), (from / 1000).to_string());
        }

        if let Some(to) = end_ms {
            params.insert("to".to_string(), (to / 1000).to_string());
        }

        let response: KrakenResponse<Vec<FuturesFundingRate>> = self.client
            .get_public("/fundingrates", Some(params))
            .await?;

        let rates = response.into_result()?;
        let mut history = Vec::new();

        for rate in rates {
            history.push(FundingRateHistory {
                symbol: rate.symbol.clone(),
                rate: Decimal::from_str(&rate.rate.to_string()).unwrap_or(Decimal::ZERO),
                ts_ms: now_millis(), // Would parse from timestamp in production
            });
        }

        Ok(history)
    }
}
