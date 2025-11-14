use crate::kraken::common::{converters, KrakenAuth, KRAKEN_FUTURES_WS_URL};
use crate::traits::*;
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use tracing::{debug, error, warn};

type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

pub struct KrakenPerpsWs {
    auth: KrakenAuth,
    user_stream: Arc<Mutex<Option<mpsc::Receiver<UserEvent>>>>,
    book_stream: Arc<Mutex<Option<mpsc::Receiver<BookUpdate>>>>,
    trade_stream: Arc<Mutex<Option<mpsc::Receiver<TradeEvent>>>>,
    connection_status: Arc<RwLock<ConnectionStatus>>,
    health_data: Arc<RwLock<HealthData>>,
}

#[derive(Clone)]
struct HealthData {
    last_ping_ms: Option<UnixMillis>,
    last_pong_ms: Option<UnixMillis>,
    reconnect_count: u32,
    error_msg: Option<String>,
}

impl KrakenPerpsWs {
    pub fn new(api_key: String, api_secret: String) -> Self {
        Self {
            auth: KrakenAuth::new(api_key, api_secret),
            user_stream: Arc::new(Mutex::new(None)),
            book_stream: Arc::new(Mutex::new(None)),
            trade_stream: Arc::new(Mutex::new(None)),
            connection_status: Arc::new(RwLock::new(ConnectionStatus::Disconnected)),
            health_data: Arc::new(RwLock::new(HealthData {
                last_ping_ms: None,
                last_pong_ms: None,
                reconnect_count: 0,
                error_msg: None,
            })),
        }
    }

    async fn connect(&self) -> Result<WsStream> {
        let (ws_stream, _) = connect_async(KRAKEN_FUTURES_WS_URL)
            .await
            .context("Failed to connect to Kraken Futures WebSocket")?;

        debug!("Connected to Kraken Futures WebSocket");
        Ok(ws_stream)
    }

    async fn authenticate(&self, ws: &mut WsStream) -> Result<()> {
        // Generate authentication challenge
        let _challenge = self.generate_auth_challenge();

        let auth_msg = serde_json::json!({
            "event": "challenge",
            "api_key": self.auth.api_key
        });

        ws.send(Message::Text(auth_msg.to_string()))
            .await
            .context("Failed to send auth challenge")?;

        // Wait for challenge response and sign it
        if let Some(Ok(Message::Text(response))) = ws.next().await {
            let resp: Value = serde_json::from_str(&response)?;

            if let Some(challenge_str) = resp.get("message").and_then(|m| m.as_str()) {
                let signature = self.sign_challenge(challenge_str)?;

                let auth_response = serde_json::json!({
                    "event": "subscribe",
                    "feed": "trade",
                    "api_key": self.auth.api_key,
                    "original_challenge": challenge_str,
                    "signed_challenge": signature
                });

                ws.send(Message::Text(auth_response.to_string()))
                    .await
                    .context("Failed to send auth response")?;
            }
        }

        Ok(())
    }

    fn generate_auth_challenge(&self) -> String {
        format!("challenge_{}", Self::now_millis())
    }

    fn sign_challenge(&self, challenge: &str) -> Result<String> {
        use base64::Engine;
        use base64::engine::general_purpose;
        use hmac::{Hmac, Mac};
        use sha2::Sha512;

        let decoded_secret = general_purpose::STANDARD.decode(&self.auth.api_secret)
            .context("Failed to decode API secret")?;

        let mut mac = Hmac::<Sha512>::new_from_slice(&decoded_secret)
            .context("Failed to create HMAC")?;

        mac.update(challenge.as_bytes());
        let signature = mac.finalize().into_bytes();

        Ok(general_purpose::STANDARD.encode(signature))
    }

    fn now_millis() -> UnixMillis {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
}

// Kraken Futures WebSocket message types
#[derive(Debug, Deserialize)]
#[serde(tag = "event")]
enum FuturesWsMessage {
    #[serde(rename = "subscribed")]
    Subscribed { feed: String },
    #[serde(rename = "unsubscribed")]
    Unsubscribed { feed: String },
    #[serde(rename = "alert")]
    Alert { message: String },
    #[serde(rename = "info")]
    Info { version: String },
}

#[derive(Debug, Deserialize)]
struct FuturesOrderUpdate {
    event: String,
    feed: String,
    order_id: String,
    #[serde(rename = "cli_ord_id")]
    cli_ord_id: Option<String>,
    #[serde(rename = "type")]
    order_type: String,
    symbol: String,
    side: String,
    quantity: f64,
    filled: f64,
    #[serde(rename = "limit_price")]
    limit_price: Option<f64>,
    #[serde(rename = "stop_price")]
    stop_price: Option<f64>,
    timestamp: i64,
    #[serde(rename = "last_update_timestamp")]
    last_update_timestamp: Option<i64>,
    #[serde(rename = "order_status")]
    order_status: String,
}

#[derive(Debug, Deserialize)]
struct FuturesFillUpdate {
    event: String,
    feed: String,
    order_id: String,
    fill_id: String,
    symbol: String,
    side: String,
    price: f64,
    quantity: f64,
    #[serde(rename = "order_type")]
    order_type: String,
    timestamp: i64,
}

#[derive(Debug, Deserialize)]
struct FuturesBookSnapshot {
    feed: String,
    product_id: String,
    bids: Vec<BookLevel>,
    asks: Vec<BookLevel>,
    timestamp: i64,
}

#[derive(Debug, Deserialize)]
struct BookLevel {
    price: f64,
    qty: f64,
}

#[derive(Debug, Deserialize)]
struct FuturesTradeUpdate {
    feed: String,
    product_id: String,
    price: f64,
    qty: f64,
    side: String,
    #[serde(rename = "time")]
    timestamp: i64,
    trade_id: String,
}

#[derive(Debug, Deserialize)]
struct FuturesPositionUpdate {
    feed: String,
    symbol: String,
    balance: f64,
    #[serde(rename = "pnl")]
    pnl: f64,
    #[serde(rename = "entry_price")]
    entry_price: Option<f64>,
}

fn parse_futures_order_status(status: &str) -> OrderStatus {
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
impl PerpWs for KrakenPerpsWs {
    async fn subscribe_user(&self) -> Result<mpsc::Receiver<UserEvent>> {
        let (tx, rx) = mpsc::channel(1000);

        let mut ws = self.connect().await?;
        self.authenticate(&mut ws).await?;

        *self.connection_status.write().await = ConnectionStatus::Connected;

        // Subscribe to user feeds
        let subscribe_fills = serde_json::json!({
            "event": "subscribe",
            "feed": "fills"
        });

        let subscribe_orders = serde_json::json!({
            "event": "subscribe",
            "feed": "open_orders"
        });

        ws.send(Message::Text(subscribe_fills.to_string())).await?;
        ws.send(Message::Text(subscribe_orders.to_string())).await?;

        // Spawn task to handle incoming messages
        let tx_clone = tx.clone();
        let connection_status = self.connection_status.clone();
        let health_data = self.health_data.clone();

        tokio::spawn(async move {
            while let Some(msg) = ws.next().await {
                match msg {
                    Ok(Message::Text(text)) => {
                        if let Err(e) = Self::handle_user_message(&text, &tx_clone).await {
                            error!("Error handling user message: {}", e);
                        }
                    }
                    Ok(Message::Ping(_)) => {
                        health_data.write().await.last_ping_ms = Some(Self::now_millis());
                    }
                    Ok(Message::Pong(_)) => {
                        health_data.write().await.last_pong_ms = Some(Self::now_millis());
                    }
                    Ok(Message::Close(_)) => {
                        warn!("WebSocket closed");
                        *connection_status.write().await = ConnectionStatus::Disconnected;
                        break;
                    }
                    Err(e) => {
                        error!("WebSocket error: {}", e);
                        *connection_status.write().await = ConnectionStatus::Error;
                        health_data.write().await.error_msg = Some(e.to_string());
                        break;
                    }
                    _ => {}
                }
            }
        });

        Ok(rx)
    }

    async fn subscribe_books(&self, symbols: &[&str]) -> Result<mpsc::Receiver<BookUpdate>> {
        let (tx, rx) = mpsc::channel(1000);

        let mut ws = self.connect().await?;

        // Subscribe to order books
        for symbol in symbols {
            let subscribe_msg = serde_json::json!({
                "event": "subscribe",
                "feed": "book",
                "product_ids": [symbol]
            });

            ws.send(Message::Text(subscribe_msg.to_string()))
                .await
                .context("Failed to send book subscription")?;
        }

        // Spawn task to handle incoming messages
        let tx_clone = tx.clone();
        tokio::spawn(async move {
            while let Some(msg) = ws.next().await {
                if let Ok(Message::Text(text)) = msg {
                    if let Err(e) = Self::handle_book_message(&text, &tx_clone).await {
                        error!("Error handling book message: {}", e);
                    }
                }
            }
        });

        Ok(rx)
    }

    async fn subscribe_trades(&self, symbols: &[&str]) -> Result<mpsc::Receiver<TradeEvent>> {
        let (tx, rx) = mpsc::channel(1000);

        let mut ws = self.connect().await?;

        // Subscribe to trades
        for symbol in symbols {
            let subscribe_msg = serde_json::json!({
                "event": "subscribe",
                "feed": "trade",
                "product_ids": [symbol]
            });

            ws.send(Message::Text(subscribe_msg.to_string()))
                .await
                .context("Failed to send trade subscription")?;
        }

        // Spawn task to handle incoming messages
        let tx_clone = tx.clone();
        tokio::spawn(async move {
            while let Some(msg) = ws.next().await {
                if let Ok(Message::Text(text)) = msg {
                    if let Err(e) = Self::handle_trade_message(&text, &tx_clone).await {
                        error!("Error handling trade message: {}", e);
                    }
                }
            }
        });

        Ok(rx)
    }

    async fn health(&self) -> Result<HealthStatus> {
        let status = *self.connection_status.read().await;
        let health = self.health_data.read().await;

        let latency_ms = if let (Some(ping), Some(pong)) = (health.last_ping_ms, health.last_pong_ms) {
            if pong > ping {
                Some(pong - ping)
            } else {
                None
            }
        } else {
            None
        };

        Ok(HealthStatus {
            status,
            last_ping_ms: health.last_ping_ms,
            last_pong_ms: health.last_pong_ms,
            latency_ms,
            reconnect_count: health.reconnect_count,
            error_msg: health.error_msg.clone(),
        })
    }

    async fn reconnect(&self) -> Result<()> {
        *self.connection_status.write().await = ConnectionStatus::Reconnecting;
        self.health_data.write().await.reconnect_count += 1;

        // Reconnect logic would go here
        *self.connection_status.write().await = ConnectionStatus::Connecting;

        Ok(())
    }
}

impl KrakenPerpsWs {
    async fn handle_user_message(text: &str, tx: &mpsc::Sender<UserEvent>) -> Result<()> {
        let value: Value = serde_json::from_str(text)?;

        if let Some(feed) = value.get("feed").and_then(|f| f.as_str()) {
            match feed {
                "fills" => {
                    if let Ok(fill) = serde_json::from_value::<FuturesFillUpdate>(value.clone()) {
                        let fill_event = Self::futures_fill_to_event(fill)?;
                        let _ = tx.send(UserEvent::Fill(fill_event)).await;
                    }
                }
                "open_orders" | "open_orders_verbose" => {
                    if let Ok(order) = serde_json::from_value::<FuturesOrderUpdate>(value.clone()) {
                        let order_event = Self::futures_order_to_event(order)?;
                        let _ = tx.send(UserEvent::OrderUpdate(order_event)).await;
                    }
                }
                "account_balances_and_margins" => {
                    // Handle balance updates
                    if let Some(balances) = value.get("balances").and_then(|b| b.as_object()) {
                        for (asset, amount) in balances {
                            if let Some(amt) = amount.as_f64() {
                                let _ = tx.send(UserEvent::Balance {
                                    asset: asset.clone(),
                                    free: amt,
                                    locked: 0.0,
                                    ex_ts_ms: Self::now_millis(),
                                    recv_ms: Self::now_millis(),
                                }).await;
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }

    async fn handle_book_message(text: &str, tx: &mpsc::Sender<BookUpdate>) -> Result<()> {
        let value: Value = serde_json::from_str(text)?;

        if let Some(feed) = value.get("feed").and_then(|f| f.as_str()) {
            if feed == "book" || feed == "book_snapshot" {
                if let Ok(book) = serde_json::from_value::<FuturesBookSnapshot>(value.clone()) {
                    let book_update = Self::futures_book_to_update(book)?;
                    let _ = tx.send(book_update).await;
                }
            }
        }

        Ok(())
    }

    async fn handle_trade_message(text: &str, tx: &mpsc::Sender<TradeEvent>) -> Result<()> {
        let value: Value = serde_json::from_str(text)?;

        if let Some(feed) = value.get("feed").and_then(|f| f.as_str()) {
            if feed == "trade" {
                if let Ok(trade) = serde_json::from_value::<FuturesTradeUpdate>(value.clone()) {
                    let trade_event = Self::futures_trade_to_event(trade)?;
                    let _ = tx.send(trade_event).await;
                }
            }
        }

        Ok(())
    }

    fn futures_order_to_event(order: FuturesOrderUpdate) -> Result<Order> {
        let now = Self::now_millis();

        Ok(Order {
            venue_order_id: order.order_id,
            client_order_id: order.cli_ord_id.unwrap_or_default(),
            symbol: order.symbol,
            ord_type: converters::from_kraken_order_type(&order.order_type),
            side: converters::from_kraken_side(&order.side),
            qty: order.quantity,
            price: order.limit_price,
            stop_price: order.stop_price,
            tif: None,
            status: parse_futures_order_status(&order.order_status),
            filled_qty: order.filled,
            remaining_qty: order.quantity - order.filled,
            created_ms: order.timestamp as UnixMillis,
            updated_ms: order.last_update_timestamp.map(|t| t as UnixMillis).unwrap_or(now),
            recv_ms: now,
            raw_status: Some(order.order_status),
        })
    }

    fn futures_fill_to_event(fill: FuturesFillUpdate) -> Result<Fill> {
        let now = Self::now_millis();

        Ok(Fill {
            venue_order_id: fill.order_id,
            client_order_id: String::new(),
            symbol: fill.symbol,
            price: fill.price,
            qty: fill.quantity,
            fee: 0.0, // Would need to calculate from maker/taker rates
            fee_ccy: "USD".to_string(),
            is_maker: false, // Would need to determine from order type
            exec_id: fill.fill_id,
            ex_ts_ms: fill.timestamp as UnixMillis,
            recv_ms: now,
        })
    }

    fn futures_book_to_update(book: FuturesBookSnapshot) -> Result<BookUpdate> {
        let bids: Vec<(Price, Quantity)> = book.bids
            .iter()
            .map(|level| (level.price, level.qty))
            .collect();

        let asks: Vec<(Price, Quantity)> = book.asks
            .iter()
            .map(|level| (level.price, level.qty))
            .collect();

        let now = Self::now_millis();

        Ok(BookUpdate::DepthDelta {
            symbol: book.product_id,
            bids,
            asks,
            seq: 0,
            prev_seq: 0,
            checksum: None,
            ex_ts_ms: book.timestamp as UnixMillis,
            recv_ms: now,
        })
    }

    fn futures_trade_to_event(trade: FuturesTradeUpdate) -> Result<TradeEvent> {
        let now = Self::now_millis();

        Ok(TradeEvent {
            symbol: trade.product_id,
            px: trade.price,
            qty: trade.qty,
            taker_is_buy: trade.side.to_lowercase() == "buy",
            ex_ts_ms: trade.timestamp as UnixMillis,
            recv_ms: now,
        })
    }
}
