use crate::kraken::common::{converters, KrakenAuth, KRAKEN_SPOT_WS_AUTH_URL, KRAKEN_SPOT_WS_URL};
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

pub struct KrakenSpotWs {
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

impl KrakenSpotWs {
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

    async fn connect_authenticated(&self) -> Result<WsStream> {
        // First get a WebSocket token from REST API
        let token = self.get_ws_token().await?;

        let url = format!("{}?token={}", KRAKEN_SPOT_WS_AUTH_URL, token);
        let (ws_stream, _) = connect_async(&url)
            .await
            .context("Failed to connect to Kraken WebSocket")?;

        debug!("Connected to Kraken authenticated WebSocket");
        Ok(ws_stream)
    }

    async fn connect_public(&self) -> Result<WsStream> {
        let (ws_stream, _) = connect_async(KRAKEN_SPOT_WS_URL)
            .await
            .context("Failed to connect to Kraken WebSocket")?;

        debug!("Connected to Kraken public WebSocket");
        Ok(ws_stream)
    }

    async fn get_ws_token(&self) -> Result<String> {
        // This would require implementing REST call to get token
        // For now, return placeholder - in production, call /0/private/GetWebSocketsToken
        Ok("websocket_token_placeholder".to_string())
    }

    fn now_millis() -> UnixMillis {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
}

// Kraken WebSocket message types
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum KrakenWsMessage {
    Subscription(SubscriptionResponse),
    Channel(ChannelMessage),
    Heartbeat(HeartbeatMessage),
    SystemStatus(SystemStatusMessage),
}

#[derive(Debug, Deserialize)]
struct SubscriptionResponse {
    #[serde(rename = "channelID")]
    channel_id: Option<u64>,
    #[serde(rename = "channelName")]
    channel_name: Option<String>,
    event: String,
    pair: Option<String>,
    subscription: Option<Value>,
    status: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ChannelMessage {
    channel: String,
    #[serde(rename = "type")]
    msg_type: String,
    data: Vec<Value>,
}

#[derive(Debug, Deserialize)]
struct HeartbeatMessage {
    event: String,
}

#[derive(Debug, Deserialize)]
struct SystemStatusMessage {
    event: String,
    status: String,
    version: Option<String>,
}

// Order update from WebSocket
#[derive(Debug, Deserialize)]
struct KrakenWsOrder {
    #[serde(rename = "order_id")]
    order_id: String,
    #[serde(rename = "cl_ord_id")]
    cl_ord_id: Option<String>,
    symbol: String,
    side: String,
    order_type: String,
    order_qty: String,
    limit_price: Option<String>,
    filled_qty: Option<String>,
    order_status: String,
    timestamp: String,
}

// Book update from WebSocket
#[derive(Debug, Deserialize)]
struct KrakenWsBook {
    symbol: String,
    bids: Vec<Vec<String>>,
    asks: Vec<Vec<String>>,
    checksum: Option<u32>,
    timestamp: String,
}

// Trade update from WebSocket
#[derive(Debug, Deserialize)]
struct KrakenWsTrade {
    symbol: String,
    side: String,
    price: String,
    qty: String,
    timestamp: String,
}

#[async_trait::async_trait]
impl SpotWs for KrakenSpotWs {
    async fn subscribe_user(&self) -> Result<mpsc::Receiver<UserEvent>> {
        let (tx, rx) = mpsc::channel(1000);

        let mut ws = self.connect_authenticated().await?;
        *self.connection_status.write().await = ConnectionStatus::Connected;

        // Subscribe to user events
        let subscribe_msg = serde_json::json!({
            "method": "subscribe",
            "params": {
                "channel": "executions",
                "snapshot": false
            }
        });

        ws.send(Message::Text(subscribe_msg.to_string()))
            .await
            .context("Failed to send subscription")?;

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

        let mut ws = self.connect_public().await?;

        // Subscribe to order books
        for symbol in symbols {
            let subscribe_msg = serde_json::json!({
                "method": "subscribe",
                "params": {
                    "channel": "book",
                    "symbol": [symbol],
                    "depth": 10
                }
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

        let mut ws = self.connect_public().await?;

        // Subscribe to trades
        for symbol in symbols {
            let subscribe_msg = serde_json::json!({
                "method": "subscribe",
                "params": {
                    "channel": "trade",
                    "symbol": [symbol]
                }
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
        // For now, just update status
        *self.connection_status.write().await = ConnectionStatus::Connecting;

        Ok(())
    }
}

impl KrakenSpotWs {
    async fn handle_user_message(text: &str, tx: &mpsc::Sender<UserEvent>) -> Result<()> {
        let value: Value = serde_json::from_str(text)?;

        // Check if it's an order update
        if let Some(channel) = value.get("channel").and_then(|c| c.as_str()) {
            match channel {
                "executions" => {
                    if let Some(data) = value.get("data").and_then(|d| d.as_array()) {
                        for item in data {
                            if let Ok(order) = serde_json::from_value::<KrakenWsOrder>(item.clone()) {
                                let user_event = Self::kraken_order_to_user_event(order)?;
                                let _ = tx.send(user_event).await;
                            }
                        }
                    }
                }
                "balances" => {
                    // Handle balance updates
                    if let Some(data) = value.get("data").and_then(|d| d.as_array()) {
                        for item in data {
                            if let (Some(asset), Some(free), Some(locked)) = (
                                item.get("asset").and_then(|a| a.as_str()),
                                item.get("available").and_then(|a| a.as_str()),
                                item.get("hold").and_then(|h| h.as_str()),
                            ) {
                                let _ = tx.send(UserEvent::Balance {
                                    asset: asset.to_string(),
                                    free: free.parse().unwrap_or(0.0),
                                    locked: locked.parse().unwrap_or(0.0),
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

        if let Some(channel) = value.get("channel").and_then(|c| c.as_str()) {
            if channel == "book" {
                if let Some(data) = value.get("data").and_then(|d| d.as_array()) {
                    for item in data {
                        if let Ok(book) = serde_json::from_value::<KrakenWsBook>(item.clone()) {
                            let book_update = Self::kraken_book_to_update(book)?;
                            let _ = tx.send(book_update).await;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn handle_trade_message(text: &str, tx: &mpsc::Sender<TradeEvent>) -> Result<()> {
        let value: Value = serde_json::from_str(text)?;

        if let Some(channel) = value.get("channel").and_then(|c| c.as_str()) {
            if channel == "trade" {
                if let Some(data) = value.get("data").and_then(|d| d.as_array()) {
                    for item in data {
                        if let Ok(trade) = serde_json::from_value::<KrakenWsTrade>(item.clone()) {
                            let trade_event = Self::kraken_trade_to_event(trade)?;
                            let _ = tx.send(trade_event).await;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn kraken_order_to_user_event(order: KrakenWsOrder) -> Result<UserEvent> {
        let filled_qty = order.filled_qty
            .as_ref()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        let total_qty = order.order_qty.parse().unwrap_or(0.0);

        let status = match order.order_status.as_str() {
            "pending" => OrderStatus::New,
            "open" => OrderStatus::New,
            "closed" => OrderStatus::Filled,
            "canceled" => OrderStatus::Canceled,
            "expired" => OrderStatus::Expired,
            _ => OrderStatus::Rejected,
        };

        let now = Self::now_millis();

        let order_obj = Order {
            venue_order_id: order.order_id,
            client_order_id: order.cl_ord_id.unwrap_or_default(),
            symbol: order.symbol,
            ord_type: converters::from_kraken_order_type(&order.order_type),
            side: converters::from_kraken_side(&order.side),
            qty: total_qty,
            price: order.limit_price.and_then(|p| p.parse().ok()),
            stop_price: None,
            tif: None,
            status,
            filled_qty,
            remaining_qty: total_qty - filled_qty,
            created_ms: now,
            updated_ms: now,
            recv_ms: now,
            raw_status: Some(order.order_status),
        };

        Ok(UserEvent::OrderUpdate(order_obj))
    }

    fn kraken_book_to_update(book: KrakenWsBook) -> Result<BookUpdate> {
        let mut bids = Vec::new();
        for bid in book.bids {
            if bid.len() >= 2 {
                let price = bid[0].parse().unwrap_or(0.0);
                let qty = bid[1].parse().unwrap_or(0.0);
                bids.push((price, qty));
            }
        }

        let mut asks = Vec::new();
        for ask in book.asks {
            if ask.len() >= 2 {
                let price = ask[0].parse().unwrap_or(0.0);
                let qty = ask[1].parse().unwrap_or(0.0);
                asks.push((price, qty));
            }
        }

        let now = Self::now_millis();

        Ok(BookUpdate::DepthDelta {
            symbol: book.symbol,
            bids,
            asks,
            seq: 0, // Kraken doesn't provide sequence numbers in this format
            prev_seq: 0,
            checksum: book.checksum,
            ex_ts_ms: now,
            recv_ms: now,
        })
    }

    fn kraken_trade_to_event(trade: KrakenWsTrade) -> Result<TradeEvent> {
        let now = Self::now_millis();

        Ok(TradeEvent {
            symbol: trade.symbol,
            px: trade.price.parse().unwrap_or(0.0),
            qty: trade.qty.parse().unwrap_or(0.0),
            taker_is_buy: trade.side.to_lowercase() == "buy",
            ex_ts_ms: now,
            recv_ms: now,
        })
    }
}
