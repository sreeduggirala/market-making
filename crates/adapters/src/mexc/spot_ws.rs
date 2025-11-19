use crate::mexc::common::{converters, MexcAuth, MEXC_SPOT_WS_URL};
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

#[derive(Clone)]
pub struct MexcSpotWs {
    #[allow(dead_code)]
    auth: MexcAuth,
    #[allow(dead_code)]
    user_stream: Arc<Mutex<Option<mpsc::Receiver<UserEvent>>>>,
    #[allow(dead_code)]
    book_stream: Arc<Mutex<Option<mpsc::Receiver<BookUpdate>>>>,
    #[allow(dead_code)]
    trade_stream: Arc<Mutex<Option<mpsc::Receiver<TradeEvent>>>>,
    connection_status: Arc<RwLock<ConnectionStatus>>,
    health_data: Arc<RwLock<HealthData>>,
    force_disconnect_tx: Arc<Mutex<Option<mpsc::Sender<()>>>>,
}

#[derive(Clone)]
struct HealthData {
    last_ping_ms: Option<UnixMillis>,
    last_pong_ms: Option<UnixMillis>,
    reconnect_count: u32,
    error_msg: Option<String>,
}

impl MexcSpotWs {
    pub fn new(api_key: String, api_secret: String) -> Self {
        Self {
            auth: MexcAuth::new(api_key, api_secret),
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
            force_disconnect_tx: Arc::new(Mutex::new(None)),
        }
    }

    async fn connect_public(&self) -> Result<WsStream> {
        let (ws_stream, _) = connect_async(MEXC_SPOT_WS_URL)
            .await
            .context("Failed to connect to MEXC WebSocket")?;

        debug!("Connected to MEXC public WebSocket");
        Ok(ws_stream)
    }

    fn now_millis() -> UnixMillis {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
}

// MEXC WebSocket message types
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct MexcWsResponse {
    #[serde(rename = "c")]
    channel: Option<String>,
    #[serde(rename = "d")]
    data: Option<Value>,
    #[serde(rename = "s")]
    symbol: Option<String>,
    #[serde(rename = "t")]
    timestamp: Option<u64>,
}

// Order update from WebSocket
#[derive(Debug, Deserialize)]
struct MexcWsOrder {
    #[serde(rename = "i")]
    order_id: String,
    #[serde(rename = "c")]
    client_order_id: Option<String>,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "S")]
    side: String,
    #[serde(rename = "o")]
    order_type: String,
    #[serde(rename = "q")]
    quantity: String,
    #[serde(rename = "p")]
    price: Option<String>,
    #[serde(rename = "z")]
    executed_qty: String,
    #[serde(rename = "X")]
    status: String,
    #[serde(rename = "T")]
    timestamp: u64,
}

// Book update from WebSocket
#[derive(Debug, Deserialize)]
struct MexcWsBookUpdate {
    #[serde(rename = "asks")]
    asks: Vec<MexcBookLevel>,
    #[serde(rename = "bids")]
    bids: Vec<MexcBookLevel>,
}

#[derive(Debug, Deserialize)]
struct MexcBookLevel {
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "v")]
    volume: String,
}

// Trade update from WebSocket
#[derive(Debug, Deserialize)]
struct MexcWsTrade {
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "v")]
    quantity: String,
    #[serde(rename = "S")]
    #[allow(dead_code)]
    side: i32, // 1 = buy, 2 = sell
    #[serde(rename = "t")]
    timestamp: u64,
}

#[async_trait::async_trait]
impl SpotWs for MexcSpotWs {
    async fn subscribe_user(&self) -> Result<mpsc::Receiver<UserEvent>> {
        let (tx, rx) = mpsc::channel(1000);

        let mut ws = self.connect_public().await?;
        *self.connection_status.write().await = ConnectionStatus::Connected;

        // Subscribe to user events (requires listen key - simplified for now)
        let subscribe_msg = serde_json::json!({
            "method": "SUBSCRIPTION",
            "params": ["spot@private.account.v3.api"]
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
        let channels: Vec<String> = symbols
            .iter()
            .map(|s| format!("spot@public.increase.depth.v3.api@{}", s))
            .collect();

        let subscribe_msg = serde_json::json!({
            "method": "SUBSCRIPTION",
            "params": channels
        });

        ws.send(Message::Text(subscribe_msg.to_string()))
            .await
            .context("Failed to send book subscription")?;

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

        // Create channel for force disconnect
        let (disconnect_tx, mut disconnect_rx) = mpsc::channel(1);
        *self.force_disconnect_tx.lock().await = Some(disconnect_tx);

        let symbols_vec: Vec<String> = symbols.iter().map(|s| s.to_string()).collect();
        let connection_status = self.connection_status.clone();
        let health_data = self.health_data.clone();

        // Spawn task with auto-reconnection loop
        let tx_clone = tx.clone();
        tokio::spawn(async move {
            let mut reconnect_count = 0;

            // Outer reconnection loop - runs forever, reconnecting on failures
            loop {
                debug!("Connecting to MEXC trade stream (attempt #{})", reconnect_count + 1);

                // Connect to WebSocket
                let ws_result = connect_async(MEXC_SPOT_WS_URL).await;
                let mut ws = match ws_result {
                    Ok((stream, _)) => {
                        debug!("Connected to MEXC WebSocket for trades");
                        *connection_status.write().await = ConnectionStatus::Connected;
                        stream
                    }
                    Err(e) => {
                        error!("Failed to connect to MEXC WebSocket: {}", e);
                        *connection_status.write().await = ConnectionStatus::Error;
                        health_data.write().await.error_msg = Some(e.to_string());
                        reconnect_count += 1;
                        health_data.write().await.reconnect_count = reconnect_count;

                        // Wait 100ms before retrying
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                        continue;
                    }
                };

                // Subscribe to all symbols
                let channels: Vec<String> = symbols_vec
                    .iter()
                    .map(|s| format!("spot@public.deals.v3.api@{}", s))
                    .collect();

                let subscribe_msg = serde_json::json!({
                    "method": "SUBSCRIPTION",
                    "params": channels
                });

                if let Err(e) = ws.send(Message::Text(subscribe_msg.to_string())).await {
                    error!("Failed to send subscription: {}", e);
                    reconnect_count += 1;
                    health_data.write().await.reconnect_count = reconnect_count;
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    continue;
                }

                // Inner message processing loop
                loop {
                    tokio::select! {
                        // Check for force disconnect signal
                        Some(_) = disconnect_rx.recv() => {
                            warn!("Force disconnect triggered!");
                            *connection_status.write().await = ConnectionStatus::Disconnected;
                            drop(ws);
                            break;
                        }
                        // Process WebSocket messages
                        msg = ws.next() => {
                            match msg {
                                Some(Ok(Message::Text(text))) => {
                                    if let Err(e) = Self::handle_trade_message(&text, &tx_clone).await {
                                        error!("Error handling trade message: {}", e);
                                    }
                                }
                                Some(Ok(Message::Ping(_))) => {
                                    health_data.write().await.last_ping_ms = Some(Self::now_millis());
                                }
                                Some(Ok(Message::Pong(_))) => {
                                    health_data.write().await.last_pong_ms = Some(Self::now_millis());
                                }
                                Some(Ok(Message::Close(frame))) => {
                                    warn!("MEXC WebSocket closed: {:?}", frame);
                                    *connection_status.write().await = ConnectionStatus::Disconnected;
                                    break;
                                }
                                Some(Err(e)) => {
                                    error!("MEXC WebSocket error: {}", e);
                                    *connection_status.write().await = ConnectionStatus::Error;
                                    health_data.write().await.error_msg = Some(e.to_string());
                                    break;
                                }
                                None => {
                                    warn!("MEXC WebSocket stream ended");
                                    *connection_status.write().await = ConnectionStatus::Disconnected;
                                    break;
                                }
                                _ => {}
                            }
                        }
                    }
                }

                // Connection closed, prepare to reconnect
                warn!("MEXC trade stream disconnected. Reconnecting immediately...");
                *connection_status.write().await = ConnectionStatus::Reconnecting;
                reconnect_count += 1;
                health_data.write().await.reconnect_count = reconnect_count;

                // Minimal delay to prevent tight loop
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
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

impl MexcSpotWs {
    async fn handle_user_message(text: &str, tx: &mpsc::Sender<UserEvent>) -> Result<()> {
        let value: Value = serde_json::from_str(text)?;

        // Check if it's an order update
        if let Some(channel) = value.get("c").and_then(|c| c.as_str()) {
            if channel.contains("account") {
                if let Some(data) = value.get("d") {
                    if let Ok(order) = serde_json::from_value::<MexcWsOrder>(data.clone()) {
                        let user_event = Self::mexc_order_to_user_event(order)?;
                        let _ = tx.send(user_event).await;
                    }
                }
            }
        }

        Ok(())
    }

    async fn handle_book_message(text: &str, tx: &mpsc::Sender<BookUpdate>) -> Result<()> {
        let value: Value = serde_json::from_str(text)?;

        if let Some(channel) = value.get("c").and_then(|c| c.as_str()) {
            if channel.contains("depth") {
                if let (Some(data), Some(symbol)) = (value.get("d"), value.get("s").and_then(|s| s.as_str())) {
                    if let Ok(book) = serde_json::from_value::<MexcWsBookUpdate>(data.clone()) {
                        let book_update = Self::mexc_book_to_update(symbol, book)?;
                        let _ = tx.send(book_update).await;
                    }
                }
            }
        }

        Ok(())
    }

    async fn handle_trade_message(text: &str, tx: &mpsc::Sender<TradeEvent>) -> Result<()> {
        let value: Value = serde_json::from_str(text)?;

        if let Some(channel) = value.get("c").and_then(|c| c.as_str()) {
            if channel.contains("deals") {
                if let (Some(data), Some(symbol)) = (value.get("d"), value.get("s").and_then(|s| s.as_str())) {
                    if let Some(trades_array) = data.get("deals").and_then(|d| d.as_array()) {
                        for trade_val in trades_array {
                            if let Ok(trade) = serde_json::from_value::<MexcWsTrade>(trade_val.clone()) {
                                let trade_event = Self::mexc_trade_to_event(symbol, trade)?;
                                let _ = tx.send(trade_event).await;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn mexc_order_to_user_event(order: MexcWsOrder) -> Result<UserEvent> {
        let filled_qty = order.executed_qty.parse().unwrap_or(0.0);
        let total_qty = order.quantity.parse().unwrap_or(0.0);

        let status = match order.status.as_str() {
            "NEW" => OrderStatus::New,
            "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
            "FILLED" => OrderStatus::Filled,
            "CANCELED" => OrderStatus::Canceled,
            "REJECTED" => OrderStatus::Rejected,
            "EXPIRED" => OrderStatus::Expired,
            _ => OrderStatus::Rejected,
        };

        let now = Self::now_millis();

        let order_obj = Order {
            venue_order_id: order.order_id,
            client_order_id: order.client_order_id.unwrap_or_default(),
            symbol: order.symbol,
            ord_type: converters::from_mexc_order_type(&order.order_type),
            side: converters::from_mexc_side(&order.side),
            qty: total_qty,
            price: order.price.and_then(|p| p.parse().ok()),
            stop_price: None,
            tif: None,
            status,
            filled_qty,
            remaining_qty: total_qty - filled_qty,
            created_ms: order.timestamp,
            updated_ms: order.timestamp,
            recv_ms: now,
            raw_status: Some(order.status),
        };

        Ok(UserEvent::OrderUpdate(order_obj))
    }

    fn mexc_book_to_update(symbol: &str, book: MexcWsBookUpdate) -> Result<BookUpdate> {
        let mut bids = Vec::new();
        for bid in book.bids {
            let price = bid.price.parse().unwrap_or(0.0);
            let qty = bid.volume.parse().unwrap_or(0.0);
            bids.push((price, qty));
        }

        let mut asks = Vec::new();
        for ask in book.asks {
            let price = ask.price.parse().unwrap_or(0.0);
            let qty = ask.volume.parse().unwrap_or(0.0);
            asks.push((price, qty));
        }

        let now = Self::now_millis();

        Ok(BookUpdate::DepthDelta {
            symbol: symbol.to_string(),
            bids,
            asks,
            seq: 0,
            prev_seq: 0,
            checksum: None,
            ex_ts_ms: now,
            recv_ms: now,
        })
    }

    fn mexc_trade_to_event(symbol: &str, trade: MexcWsTrade) -> Result<TradeEvent> {
        let now = Self::now_millis();

        Ok(TradeEvent {
            symbol: symbol.to_string(),
            px: trade.price.parse().unwrap_or(0.0),
            qty: trade.quantity.parse().unwrap_or(0.0),
            taker_is_buy: trade.side == 1,
            ex_ts_ms: trade.timestamp,
            recv_ms: now,
        })
    }

    /// Force disconnect the WebSocket (for testing auto-reconnection)
    ///
    /// This method forcefully closes the WebSocket connection, triggering
    /// the automatic reconnection logic. Useful for testing and demonstrations.
    pub async fn force_disconnect(&self) -> Result<()> {
        if let Some(tx) = self.force_disconnect_tx.lock().await.as_ref() {
            let _ = tx.send(()).await;
            Ok(())
        } else {
            Err(anyhow::anyhow!("No active WebSocket connection to disconnect"))
        }
    }
}
