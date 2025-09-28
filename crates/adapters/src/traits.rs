use anyhow::Result;
use tokio::sync::mpsc;
use rust_decimal::Decimal;

pub type Price = f64;
pub type Quantity = f64;
pub type UnixMillis = u64;

#[derive(Clone, Copy, Debug)]
pub enum OrderType {
    LIMIT,
    MARKET,
}

#[derive(Clone, Copy, Debug)]
pub enum Side {
    BUY,
    SELL,
}

#[derive(Clone, Copy, Debug)]
pub enum TimeInForce {
    GTC,
    IOC,
    FOK,
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
    pub tif: Option<TimeInForce>,
    pub post_only: bool,
    pub reduce_only: bool, // ignored on spot
    pub client_order_id: String, // idempotency
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
    pub symbol: String,
    pub qty: Quantity, // signed: >0 long, <0 short
    pub entry_px: Price,
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

#[async_trait::async_trait]
pub trait SpotRest: Send + Sync {
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
}

#[async_trait::async_trait]
pub trait PerpRest: Send + Sync {
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
    async fn set_leverage(&self, symbol: &str, leverage: Decimal) -> Result<()>;
    async fn set_margin_mode(&self, symbol: &str, mode: MarginMode) -> Result<()>;
    async fn get_position(&self, symbol: &str) -> Result<Position>;
    async fn get_funding_rate(&self, symbol: &str) -> Result<(Decimal, UnixMillis)>;
}

#[async_trait::async_trait]
pub trait SpotWs: Send + Sync {
    async fn subscribe_user(&self) -> Result<mpsc::Receiver<UserEvent>>;
    async fn subscribe_books(&self, symbols: &[&str]) -> Result<mpsc::Receiver<BookUpdate>>;
    async fn subscribe_trades(&self, symbols: &[&str]) -> Result<mpsc::Receiver<TradeEvent>>;
}

#[async_trait::async_trait]
pub trait PerpWs: Send + Sync {
    async fn subscribe_user(&self) -> Result<mpsc::Receiver<UserEvent>>; // <-- was AResult
    async fn subscribe_books(&self, symbols: &[&str]) -> Result<mpsc::Receiver<BookUpdate>>;
    async fn subscribe_trades(&self, symbols: &[&str]) -> Result<mpsc::Receiver<TradeEvent>>;
}
