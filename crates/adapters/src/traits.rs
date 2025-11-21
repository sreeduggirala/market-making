use anyhow::Result;
use tokio::sync::mpsc;
use rust_decimal::Decimal;

pub type Price = f64;
pub type Quantity = f64;
pub type UnixMillis = u64;

// ============================================================================
// Balance & Account
// ============================================================================

#[derive(Clone, Debug)]
pub struct Balance {
    pub asset: String,
    pub free: f64,
    pub locked: f64,
    pub total: f64,
}

#[derive(Clone, Debug)]
pub struct AccountInfo {
    pub balances: Vec<Balance>,
    pub can_trade: bool,
    pub can_withdraw: bool,
    pub can_deposit: bool,
    pub update_ms: UnixMillis,
}

// ============================================================================
// Market & Instrument Info
// ============================================================================

#[derive(Clone, Debug)]
pub struct MarketInfo {
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub status: MarketStatus,
    pub min_qty: Quantity,
    pub max_qty: Quantity,
    pub step_size: Quantity,
    pub tick_size: Price,
    pub min_notional: f64,
    pub max_leverage: Option<u32>,
    pub is_spot: bool,
    pub is_perp: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MarketStatus {
    Trading,
    Halt,
    PreTrading,
    PostTrading,
    Delisted,
}

#[derive(Clone, Debug)]
pub struct TickerInfo {
    pub symbol: String,
    pub last_price: Price,
    pub bid_price: Price,
    pub ask_price: Price,
    pub volume_24h: Quantity,
    pub price_change_24h: f64,
    pub price_change_pct_24h: f64,
    pub high_24h: Price,
    pub low_24h: Price,
    pub open_price_24h: Price,
    pub ts_ms: UnixMillis,
}

// ============================================================================
// Historical Data
// ============================================================================

#[derive(Clone, Debug)]
pub struct Kline {
    pub symbol: String,
    pub open_ms: UnixMillis,
    pub close_ms: UnixMillis,
    pub open: Price,
    pub high: Price,
    pub low: Price,
    pub close: Price,
    pub volume: Quantity,
    pub quote_volume: f64,
    pub trades: u64,
}

#[derive(Clone, Copy, Debug)]
pub enum KlineInterval {
    M1,
    M5,
    M15,
    M30,
    H1,
    H4,
    D1,
}

#[derive(Clone, Debug)]
pub struct FundingRateHistory {
    pub symbol: String,
    pub rate: Decimal,
    pub ts_ms: UnixMillis,
}

// ============================================================================
// Orders & Trading
// ============================================================================

#[derive(Clone, Copy, Debug)]
pub enum OrderType {
    Limit,
    Market,
    StopLoss,
    StopLossLimit,
    TakeProfit,
    TakeProfitLimit,
}

#[derive(Clone, Copy, Debug)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Clone, Copy, Debug)]
pub enum TimeInForce {
    Gtc,
    Ioc,
    Fok,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderStatus {
    New,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
    Expired,
}

#[derive(Clone, Debug)]
pub struct NewOrder {
    pub symbol: String,
    pub side: Side,
    pub ord_type: OrderType,
    pub qty: Quantity,
    pub price: Option<Price>,
    pub stop_price: Option<Price>, // trigger price for stop orders
    pub tif: Option<TimeInForce>,
    pub post_only: bool,
    pub reduce_only: bool, // ignored on spot
    pub client_order_id: String, // idempotency
}

#[derive(Clone, Debug)]
pub struct BatchOrderRequest {
    pub orders: Vec<NewOrder>,
}

#[derive(Clone, Debug)]
pub struct Order {
    pub venue_order_id: String,
    pub client_order_id: String,
    pub symbol: String,
    pub ord_type: OrderType,
    pub side: Side,
    pub qty: Quantity,
    pub price: Option<Price>,
    pub stop_price: Option<Price>,
    pub tif: Option<TimeInForce>,
    pub status: OrderStatus,
    pub filled_qty: Quantity,
    pub remaining_qty: Quantity,
    pub created_ms: UnixMillis,
    pub updated_ms: UnixMillis,
    pub recv_ms: UnixMillis,
    pub raw_status: Option<String>,
}

#[derive(Clone, Debug)]
pub struct BatchOrderResult {
    pub success: Vec<Order>,
    pub failed: Vec<(NewOrder, String)>, // failed order + error message
}

#[derive(Clone, Debug)]
pub struct BatchCancelResult {
    pub success: Vec<String>, // venue_order_ids
    pub failed: Vec<(String, String)>, // venue_order_id + error message
}

#[derive(Clone, Debug)]
pub struct Fill {
    pub venue_order_id: String,
    pub client_order_id: String,
    pub symbol: String,
    pub price: Price,
    pub qty: Quantity,
    pub fee: Price,
    pub fee_ccy: String,
    pub is_maker: bool,
    pub exec_id: String,
    pub ex_ts_ms: UnixMillis,
    pub recv_ms: UnixMillis,
}

#[derive(Clone, Debug)]
pub enum PositionSide {
    Long,
    Short,
    Flat,
}

#[derive(Clone, Debug)]
pub struct Position {
    /// Source exchange for this position
    pub exchange: Option<String>,
    pub symbol: String,
    pub qty: Quantity, // signed: >0 long, <0 short
    pub entry_px: Price,
    pub mark_px: Option<Price>,
    pub liquidation_px: Option<Price>,
    pub unrealized_pnl: Option<f64>,
    pub realized_pnl: Option<f64>,
    pub margin: Option<f64>,
    pub leverage: Option<u32>,
    pub opened_ms: Option<UnixMillis>,
    pub updated_ms: UnixMillis,
}

#[derive(Clone, Debug)]
pub enum BookUpdate {
    DepthDelta {
        symbol: String,
        bids: Vec<(Price, Quantity)>,
        asks: Vec<(Price, Quantity)>,
        seq: u64,
        prev_seq: u64,
        checksum: Option<u32>,
        ex_ts_ms: UnixMillis,
        recv_ms: UnixMillis,
    },
    TopOfBook {
        symbol: String,
        bid_px: Price,
        bid_sz: Quantity,
        ask_px: Price,
        ask_sz: Quantity,
        ex_ts_ms: UnixMillis,
        recv_ms: UnixMillis,
    },
}

#[derive(Clone, Debug)]
pub struct TradeEvent {
    pub symbol: String,
    pub px: Price,
    pub qty: Quantity,
    pub taker_is_buy: bool,
    pub ex_ts_ms: UnixMillis,
    pub recv_ms: UnixMillis,
}

#[derive(Clone, Debug)]
pub enum UserEvent {
    OrderUpdate(Order),
    Fill(Fill),
    Position(Position),
    Balance {
        asset: String,
        free: Price,
        locked: Price,
        ex_ts_ms: UnixMillis,
        recv_ms: UnixMillis,
    },
    Funding {
        symbol: String,
        rate: f64,
        ex_ts_ms: UnixMillis,
        recv_ms: UnixMillis,
    },
    Liquidation {
        symbol: String,
        qty: Quantity,
        px: Price,
        side: Side,
        ex_ts_ms: UnixMillis,
        recv_ms: UnixMillis,
    },
}

#[derive(Clone, Copy, Debug)]
pub enum MarginMode {
    Cross,
    Isolated,
}

// ============================================================================
// Connection Health & Status
// ============================================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionStatus {
    Connected,
    Connecting,
    Disconnected,
    Reconnecting,
    Error,
}

#[derive(Clone, Debug)]
pub struct HealthStatus {
    pub status: ConnectionStatus,
    pub last_ping_ms: Option<UnixMillis>,
    pub last_pong_ms: Option<UnixMillis>,
    pub latency_ms: Option<u64>,
    pub reconnect_count: u32,
    pub error_msg: Option<String>,
}

#[async_trait::async_trait]
pub trait SpotRest: Send + Sync {
    // Order Management
    async fn create_order(&self, new: NewOrder) -> Result<Order>;
    async fn cancel_order(&self, symbol: &str, venue_order_id: &str) -> Result<bool>;
    async fn cancel_all(&self, symbol: Option<&str>) -> Result<usize>;
    async fn get_order(&self, symbol: &str, venue_order_id: &str) -> Result<Order>;
    async fn get_open_orders(&self, symbol: Option<&str>) -> Result<Vec<Order>>;
    async fn replace_order(
        &self,
        symbol: &str,
        venue_order_id: &str,
        new_price: Option<Price>,
        new_qty: Option<Quantity>,
        new_tif: Option<TimeInForce>,
        post_only: Option<bool>
    ) -> Result<(Order, bool)>;

    // Batch Operations
    async fn create_batch_orders(&self, batch: BatchOrderRequest) -> Result<BatchOrderResult>;
    async fn cancel_batch_orders(&self, symbol: &str, order_ids: Vec<String>) -> Result<BatchCancelResult>;

    // Account & Balance
    async fn get_balances(&self) -> Result<Vec<Balance>>;
    async fn get_account_info(&self) -> Result<AccountInfo>;

    // Market Data
    async fn get_market_info(&self, symbol: &str) -> Result<MarketInfo>;
    async fn get_all_markets(&self) -> Result<Vec<MarketInfo>>;
    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo>;
    async fn get_tickers(&self, symbols: Option<Vec<String>>) -> Result<Vec<TickerInfo>>;

    // Historical Data
    async fn get_klines(
        &self,
        symbol: &str,
        interval: KlineInterval,
        start_ms: Option<UnixMillis>,
        end_ms: Option<UnixMillis>,
        limit: Option<usize>
    ) -> Result<Vec<Kline>>;
}

#[async_trait::async_trait]
pub trait PerpRest: Send + Sync {
    // Order Management
    async fn create_order(&self, new: NewOrder) -> Result<Order>;
    async fn cancel_order(&self, symbol: &str, venue_order_id: &str) -> Result<bool>;
    async fn cancel_all(&self, symbol: Option<&str>) -> Result<usize>;
    async fn get_order(&self, symbol: &str, venue_order_id: &str) -> Result<Order>;
    async fn get_open_orders(&self, symbol: Option<&str>) -> Result<Vec<Order>>;
    async fn replace_order(
        &self,
        symbol: &str,
        venue_order_id: &str,
        new_price: Option<Price>,
        new_qty: Option<Quantity>,
        new_tif: Option<TimeInForce>,
        post_only: Option<bool>,
        reduce_only: Option<bool>
    ) -> Result<(Order, bool)>;

    // Batch Operations
    async fn create_batch_orders(&self, batch: BatchOrderRequest) -> Result<BatchOrderResult>;
    async fn cancel_batch_orders(&self, symbol: &str, order_ids: Vec<String>) -> Result<BatchCancelResult>;

    // Position & Risk Management
    async fn set_leverage(&self, symbol: &str, leverage: Decimal) -> Result<()>;
    async fn set_margin_mode(&self, symbol: &str, mode: MarginMode) -> Result<()>;
    async fn get_position(&self, symbol: &str) -> Result<Position>;
    async fn get_all_positions(&self) -> Result<Vec<Position>>;
    async fn get_funding_rate(&self, symbol: &str) -> Result<(Decimal, UnixMillis)>;

    // Account & Balance
    async fn get_balances(&self) -> Result<Vec<Balance>>;
    async fn get_account_info(&self) -> Result<AccountInfo>;

    // Market Data
    async fn get_market_info(&self, symbol: &str) -> Result<MarketInfo>;
    async fn get_all_markets(&self) -> Result<Vec<MarketInfo>>;
    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo>;
    async fn get_tickers(&self, symbols: Option<Vec<String>>) -> Result<Vec<TickerInfo>>;
    async fn get_mark_price(&self, symbol: &str) -> Result<(Price, UnixMillis)>;
    async fn get_index_price(&self, symbol: &str) -> Result<(Price, UnixMillis)>;

    // Historical Data
    async fn get_klines(
        &self,
        symbol: &str,
        interval: KlineInterval,
        start_ms: Option<UnixMillis>,
        end_ms: Option<UnixMillis>,
        limit: Option<usize>
    ) -> Result<Vec<Kline>>;
    async fn get_funding_history(
        &self,
        symbol: &str,
        start_ms: Option<UnixMillis>,
        end_ms: Option<UnixMillis>,
        limit: Option<usize>
    ) -> Result<Vec<FundingRateHistory>>;
}

#[async_trait::async_trait]
pub trait SpotWs: Send + Sync {
    async fn subscribe_user(&self) -> Result<mpsc::Receiver<UserEvent>>;
    async fn subscribe_books(&self, symbols: &[&str]) -> Result<mpsc::Receiver<BookUpdate>>;
    async fn subscribe_trades(&self, symbols: &[&str]) -> Result<mpsc::Receiver<TradeEvent>>;

    // Connection Management
    async fn health(&self) -> Result<HealthStatus>;
    async fn reconnect(&self) -> Result<()>;
}

#[async_trait::async_trait]
pub trait PerpWs: Send + Sync {
    async fn subscribe_user(&self) -> Result<mpsc::Receiver<UserEvent>>;
    async fn subscribe_books(&self, symbols: &[&str]) -> Result<mpsc::Receiver<BookUpdate>>;
    async fn subscribe_trades(&self, symbols: &[&str]) -> Result<mpsc::Receiver<TradeEvent>>;

    // Connection Management
    async fn health(&self) -> Result<HealthStatus>;
    async fn reconnect(&self) -> Result<()>;
}
