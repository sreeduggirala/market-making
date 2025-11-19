use crate::mexc::common::{converters, MexcAuth, MexcRestClient};
use crate::traits::*;
use anyhow::{Context, Result};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::HashMap;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct MexcPerpsRest {
    client: MexcRestClient,
}

impl MexcPerpsRest {
    pub fn new(api_key: String, api_secret: String) -> Self {
        let auth = MexcAuth::new(api_key, api_secret);
        Self {
            client: MexcRestClient::new_futures(Some(auth)),
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

// MEXC Futures-specific response types
#[derive(Debug, Deserialize)]
struct FuturesOrderResponse {
    #[serde(rename = "orderId")]
    order_id: String,
    #[serde(rename = "clientOrderId")]
    client_order_id: Option<String>,
    symbol: String,
    side: String,
    #[serde(rename = "type")]
    order_type: String,
    #[serde(rename = "origQty")]
    orig_qty: String,
    price: Option<String>,
    status: String,
}

#[derive(Debug, Deserialize)]
struct FuturesOrder {
    #[serde(rename = "orderId")]
    order_id: String,
    #[serde(rename = "clientOrderId")]
    client_order_id: Option<String>,
    symbol: String,
    side: String,
    #[serde(rename = "type")]
    order_type: String,
    price: String,
    #[serde(rename = "origQty")]
    orig_qty: String,
    #[serde(rename = "executedQty")]
    executed_qty: String,
    status: String,
    #[serde(rename = "time")]
    time: u64,
    #[serde(rename = "updateTime")]
    update_time: u64,
}

#[derive(Debug, Deserialize)]
struct FuturesPosition {
    symbol: String,
    #[serde(rename = "positionSide")]
    #[allow(dead_code)]
    position_side: String,
    #[serde(rename = "positionAmt")]
    position_amt: String,
    #[serde(rename = "entryPrice")]
    entry_price: String,
    #[serde(rename = "markPrice")]
    mark_price: Option<String>,
    #[serde(rename = "liquidationPrice")]
    liquidation_price: Option<String>,
    #[serde(rename = "unrealizedProfit")]
    unrealized_profit: Option<String>,
    #[serde(rename = "leverage")]
    leverage: Option<String>,
    #[serde(rename = "initialMargin")]
    initial_margin: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FuturesAccountInfo {
    assets: Vec<FuturesAsset>,
    positions: Vec<FuturesPosition>,
}

#[derive(Debug, Deserialize)]
struct FuturesAsset {
    asset: String,
    #[serde(rename = "walletBalance")]
    wallet_balance: String,
    #[serde(rename = "availableBalance")]
    available_balance: String,
}

#[derive(Debug, Deserialize)]
struct FuturesContract {
    symbol: String,
    #[serde(rename = "contractType")]
    contract_type: String,
    #[serde(rename = "baseCoin")]
    base_coin: String,
    #[serde(rename = "quoteCoin")]
    quote_coin: String,
    #[serde(rename = "minQty")]
    min_qty: String,
    #[serde(rename = "maxQty")]
    max_qty: String,
    #[serde(rename = "tickSize")]
    tick_size: String,
    #[serde(rename = "stepSize")]
    step_size: String,
    #[serde(rename = "maxLeverage")]
    max_leverage: Option<String>,
    state: String,
}

#[derive(Debug, Deserialize)]
struct FuturesTicker {
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
struct FuturesMarkPrice {
    symbol: String,
    #[serde(rename = "markPrice")]
    mark_price: String,
    #[serde(rename = "indexPrice")]
    index_price: String,
    #[serde(rename = "fundingRate")]
    funding_rate: String,
    time: u64,
}

#[derive(Debug, Deserialize)]
struct FuturesKline {
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

fn now_millis() -> UnixMillis {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn parse_futures_status(status: &str) -> OrderStatus {
    match status {
        "NEW" => OrderStatus::New,
        "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
        "FILLED" => OrderStatus::Filled,
        "CANCELED" => OrderStatus::Canceled,
        "REJECTED" => OrderStatus::Rejected,
        "EXPIRED" => OrderStatus::Expired,
        _ => OrderStatus::Rejected,
    }
}

#[async_trait::async_trait]
impl PerpRest for MexcPerpsRest {
    async fn create_order(&self, new: NewOrder) -> Result<Order> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), new.symbol.clone());
        params.insert("side".to_string(), converters::to_mexc_side(new.side).to_string());
        params.insert("type".to_string(), converters::to_mexc_order_type(new.ord_type).to_string());
        params.insert("quantity".to_string(), new.qty.to_string());

        if let Some(price) = new.price {
            params.insert("price".to_string(), price.to_string());
        }

        if new.reduce_only {
            params.insert("reduceOnly".to_string(), "true".to_string());
        }

        if !new.client_order_id.is_empty() {
            params.insert("newClientOrderId".to_string(), new.client_order_id.clone());
        }

        let response: FuturesOrderResponse = self
            .client
            .request_private(reqwest::Method::POST, "/api/v1/private/order/submit", params)
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
            status: parse_futures_status(&response.status),
            filled_qty: 0.0,
            remaining_qty: response.orig_qty.parse::<f64>().unwrap_or(0.0),
            created_ms: now,
            updated_ms: now,
            recv_ms: now,
            raw_status: Some(response.status),
        })
    }

    async fn cancel_order(&self, symbol: &str, venue_order_id: &str) -> Result<bool> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());
        params.insert("orderId".to_string(), venue_order_id.to_string());

        let _response: FuturesOrderResponse = self
            .client
            .request_private(reqwest::Method::DELETE, "/api/v1/private/order/cancel", params)
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

        let response: FuturesOrder = self
            .client
            .request_private(reqwest::Method::GET, "/api/v1/private/order/get", params)
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
            status: parse_futures_status(&response.status),
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

        let response: Vec<FuturesOrder> = self
            .client
            .request_private(reqwest::Method::GET, "/api/v1/private/order/list/open_orders", params)
            .await?;

        let now = now_millis();
        let mut orders = Vec::new();

        for order in response {
            let filled = order.executed_qty.parse::<f64>().unwrap_or(0.0);
            let total = order.orig_qty.parse::<f64>().unwrap_or(0.0);

            orders.push(Order {
                venue_order_id: order.order_id,
                client_order_id: order.client_order_id.unwrap_or_default(),
                symbol: order.symbol,
                ord_type: converters::from_mexc_order_type(&order.order_type),
                side: converters::from_mexc_side(&order.side),
                qty: total,
                price: Some(order.price.parse().unwrap_or(0.0)),
                stop_price: None,
                tif: None,
                status: parse_futures_status(&order.status),
                filled_qty: filled,
                remaining_qty: total - filled,
                created_ms: order.time,
                updated_ms: order.update_time,
                recv_ms: now,
                raw_status: Some(order.status),
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

    async fn set_leverage(&self, symbol: &str, leverage: Decimal) -> Result<()> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());
        params.insert("leverage".to_string(), leverage.to_string());

        let _response: serde_json::Value = self
            .client
            .request_private(reqwest::Method::POST, "/api/v1/private/account/change_leverage", params)
            .await?;

        Ok(())
    }

    async fn set_margin_mode(&self, symbol: &str, mode: MarginMode) -> Result<()> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());
        params.insert(
            "marginMode".to_string(),
            match mode {
                MarginMode::Cross => "CROSSED",
                MarginMode::Isolated => "ISOLATED",
            }
            .to_string(),
        );

        let _response: serde_json::Value = self
            .client
            .request_private(reqwest::Method::POST, "/api/v1/private/account/change_margin_mode", params)
            .await?;

        Ok(())
    }

    async fn get_position(&self, symbol: &str) -> Result<Position> {
        let params = HashMap::new();

        let response: FuturesAccountInfo = self
            .client
            .request_private(reqwest::Method::GET, "/api/v1/private/account/assets", params)
            .await?;

        let position = response
            .positions
            .iter()
            .find(|p| p.symbol == symbol)
            .context("Position not found")?;

        let qty = position.position_amt.parse::<f64>().unwrap_or(0.0);

        Ok(Position {
            symbol: position.symbol.clone(),
            qty,
            entry_px: position.entry_price.parse().unwrap_or(0.0),
            mark_px: position.mark_price.as_ref().and_then(|p| p.parse().ok()),
            liquidation_px: position.liquidation_price.as_ref().and_then(|p| p.parse().ok()),
            unrealized_pnl: position.unrealized_profit.as_ref().and_then(|p| p.parse().ok()),
            realized_pnl: None,
            margin: position.initial_margin.as_ref().and_then(|p| p.parse().ok()),
            leverage: position.leverage.as_ref().and_then(|l| l.parse::<f64>().ok()).map(|l| l as u32),
            opened_ms: None,
            updated_ms: now_millis(),
        })
    }

    async fn get_all_positions(&self) -> Result<Vec<Position>> {
        let params = HashMap::new();

        let response: FuturesAccountInfo = self
            .client
            .request_private(reqwest::Method::GET, "/api/v1/private/account/assets", params)
            .await?;

        let mut result = Vec::new();

        for position in response.positions {
            let qty = position.position_amt.parse::<f64>().unwrap_or(0.0);

            if qty.abs() > 0.0 {
                result.push(Position {
                    symbol: position.symbol.clone(),
                    qty,
                    entry_px: position.entry_price.parse().unwrap_or(0.0),
                    mark_px: position.mark_price.as_ref().and_then(|p| p.parse().ok()),
                    liquidation_px: position.liquidation_price.as_ref().and_then(|p| p.parse().ok()),
                    unrealized_pnl: position.unrealized_profit.as_ref().and_then(|p| p.parse().ok()),
                    realized_pnl: None,
                    margin: position.initial_margin.as_ref().and_then(|p| p.parse().ok()),
                    leverage: position.leverage.as_ref().and_then(|l| l.parse::<f64>().ok()).map(|l| l as u32),
                    opened_ms: None,
                    updated_ms: now_millis(),
                });
            }
        }

        Ok(result)
    }

    async fn get_funding_rate(&self, symbol: &str) -> Result<(Decimal, UnixMillis)> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());

        let response: FuturesMarkPrice = self
            .client
            .get_public("/api/v1/contract/funding_rate", Some(params))
            .await?;

        let rate = response.funding_rate.parse::<f64>().unwrap_or(0.0);
        let rate_decimal = Decimal::from_str(&rate.to_string()).unwrap_or(Decimal::ZERO);

        Ok((rate_decimal, response.time))
    }

    async fn get_balances(&self) -> Result<Vec<Balance>> {
        let params = HashMap::new();

        let response: FuturesAccountInfo = self
            .client
            .request_private(reqwest::Method::GET, "/api/v1/private/account/assets", params)
            .await?;

        let mut balances = Vec::new();

        for asset in response.assets {
            let total = asset.wallet_balance.parse::<f64>().unwrap_or(0.0);
            let free = asset.available_balance.parse::<f64>().unwrap_or(0.0);
            let locked = total - free;

            if total > 0.0 {
                balances.push(Balance {
                    asset: asset.asset,
                    free,
                    locked,
                    total,
                });
            }
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
        let response: Vec<FuturesContract> = self
            .client
            .get_public("/api/v1/contract/detail", None)
            .await?;

        let contract = response
            .iter()
            .find(|c| c.symbol == symbol)
            .context("Contract not found")?;

        let status = match contract.state.as_str() {
            "0" => MarketStatus::Trading,
            _ => MarketStatus::Halt,
        };

        Ok(MarketInfo {
            symbol: contract.symbol.clone(),
            base_asset: contract.base_coin.clone(),
            quote_asset: contract.quote_coin.clone(),
            status,
            min_qty: contract.min_qty.parse().unwrap_or(0.0),
            max_qty: contract.max_qty.parse().unwrap_or(f64::MAX),
            step_size: contract.step_size.parse().unwrap_or(0.0),
            tick_size: contract.tick_size.parse().unwrap_or(0.0),
            min_notional: 0.0,
            max_leverage: contract.max_leverage.as_ref().and_then(|l| l.parse::<f64>().ok()).map(|l| l as u32),
            is_spot: false,
            is_perp: contract.contract_type.to_lowercase().contains("perpetual"),
        })
    }

    async fn get_all_markets(&self) -> Result<Vec<MarketInfo>> {
        let response: Vec<FuturesContract> = self
            .client
            .get_public("/api/v1/contract/detail", None)
            .await?;

        let mut markets = Vec::new();

        for contract in response {
            let status = match contract.state.as_str() {
                "0" => MarketStatus::Trading,
                _ => MarketStatus::Halt,
            };

            markets.push(MarketInfo {
                symbol: contract.symbol.clone(),
                base_asset: contract.base_coin.clone(),
                quote_asset: contract.quote_coin.clone(),
                status,
                min_qty: contract.min_qty.parse().unwrap_or(0.0),
                max_qty: contract.max_qty.parse().unwrap_or(f64::MAX),
                step_size: contract.step_size.parse().unwrap_or(0.0),
                tick_size: contract.tick_size.parse().unwrap_or(0.0),
                min_notional: 0.0,
                max_leverage: contract.max_leverage.as_ref().and_then(|l| l.parse::<f64>().ok()).map(|l| l as u32),
                is_spot: false,
                is_perp: contract.contract_type.to_lowercase().contains("perpetual"),
            });
        }

        Ok(markets)
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());

        let response: FuturesTicker = self
            .client
            .get_public("/api/v1/contract/ticker", Some(params))
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

        let response: Vec<FuturesTicker> = self
            .client
            .get_public("/api/v1/contract/ticker", params)
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

    async fn get_mark_price(&self, symbol: &str) -> Result<(Price, UnixMillis)> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());

        let response: FuturesMarkPrice = self
            .client
            .get_public("/api/v1/contract/funding_rate", Some(params))
            .await?;

        Ok((response.mark_price.parse().unwrap_or(0.0), response.time))
    }

    async fn get_index_price(&self, symbol: &str) -> Result<(Price, UnixMillis)> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());

        let response: FuturesMarkPrice = self
            .client
            .get_public("/api/v1/contract/funding_rate", Some(params))
            .await?;

        Ok((response.index_price.parse().unwrap_or(0.0), response.time))
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
            KlineInterval::M1 => "Min1",
            KlineInterval::M5 => "Min5",
            KlineInterval::M15 => "Min15",
            KlineInterval::M30 => "Min30",
            KlineInterval::H1 => "Hour1",
            KlineInterval::H4 => "Hour4",
            KlineInterval::D1 => "Day1",
        };

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());
        params.insert("interval".to_string(), interval_str.to_string());

        if let Some(start) = start_ms {
            params.insert("start".to_string(), (start / 1000).to_string());
        }

        if let Some(end) = end_ms {
            params.insert("end".to_string(), (end / 1000).to_string());
        }

        if let Some(lim) = limit {
            params.insert("limit".to_string(), lim.to_string());
        }

        let response: Vec<FuturesKline> = self
            .client
            .get_public("/api/v1/contract/kline", Some(params))
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

    async fn get_funding_history(
        &self,
        symbol: &str,
        start_ms: Option<UnixMillis>,
        end_ms: Option<UnixMillis>,
        limit: Option<usize>,
    ) -> Result<Vec<FundingRateHistory>> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), symbol.to_string());

        if let Some(start) = start_ms {
            params.insert("startTime".to_string(), start.to_string());
        }

        if let Some(end) = end_ms {
            params.insert("endTime".to_string(), end.to_string());
        }

        if let Some(lim) = limit {
            params.insert("limit".to_string(), lim.to_string());
        }

        // MEXC API for funding rate history
        let response: Vec<FuturesMarkPrice> = self
            .client
            .get_public("/api/v1/contract/funding_rate/history", Some(params))
            .await?;

        let mut history = Vec::new();

        for rate_data in response {
            let rate = rate_data.funding_rate.parse::<f64>().unwrap_or(0.0);
            history.push(FundingRateHistory {
                symbol: rate_data.symbol.clone(),
                rate: Decimal::from_str(&rate.to_string()).unwrap_or(Decimal::ZERO),
                ts_ms: rate_data.time,
            });
        }

        Ok(history)
    }
}
