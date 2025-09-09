use anyhow::Result;

#[derive(Clone, Copy, Debug)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Clone, Copy, Debug)]
pub enum TimeInForce {
    GoodTilCanceled,
    ImmediateOrCancel,
    FillOrKill,
}

#[derive(Clone, Copy, Debug)]
pub enum OrderType {
    Limit,
    Market,
}

#[derive(Clone, Debug)]
pub struct OrderReq {
    pub symbol: String,
    pub side: Side,
    pub ord_type: OrderType, // Limit/Market for now
    pub px: f64, // OMS will tick-round before send
    pub qty: f64, // OMS will lot-round before send
    pub tif: TimeInForce,
    pub post_only: bool, // ignored by venues that don’t support it
    pub reduce_only: bool, // ignored on spot; used on perps
    pub client_order_id: Option<String>, // idempotency / reconciliation
}

#[derive(Clone, Debug)]
pub struct OrderAck {
    pub id: String,
    pub symbol: String,
}

#[derive(Clone, Debug)]
pub struct CancelAck {
    pub id: String,
    pub success: bool,
}

#[derive(Clone, Debug)]
pub struct OrderStatus {
    pub id: String,
    pub symbol: String,
    pub filled_qty: f64,
    pub remaining_qty: f64,
}

// ---------- Spot ----------
#[async_trait::async_trait]
pub trait SpotTrading: Send + Sync {
    async fn open_spot(&self, symbol: &str) -> Result<Vec<OrderStatus>>;
    async fn send_spot(&self, req: OrderReq) -> Result<OrderAck>;
    async fn cancel_spot(&self, order_id: &str, symbol: &str) -> Result<CancelAck>;
    async fn replace_spot(
        &self,
        order_id: &str,
        symbol: &str,
        new_px: f64,
        new_qty: f64
    ) -> Result<OrderAck>;
    async fn cancel_all_spot(&self, symbol: &str) -> Result<usize>; // returns count canceled
}

// ---------- Perps ----------
#[derive(Clone, Debug)]
pub struct FundingInfo {
    pub rate: f64,
    pub next_ts_ms: i64,
}

#[async_trait::async_trait]
pub trait PerpTrading: Send + Sync {
    async fn open_perp(&self, symbol: &str) -> Result<Vec<OrderStatus>>;
    async fn cancel_perp(&self, order_id: &str, symbol: &str) -> Result<CancelAck>;
    async fn replace_perp(
        &self,
        order_id: &str,
        symbol: &str,
        new_px: f64,
        new_qty: f64
    ) -> Result<OrderAck>;
    async fn cancel_all_perp(&self, symbol: &str) -> Result<usize>;

    // Perp controls:
    async fn set_leverage(&self, symbol: &str, x: f64) -> Result<()>;
    async fn set_margin_mode(&self, cross: bool) -> Result<()>;
    async fn funding_info(&self, symbol: &str) -> Result<FundingInfo>;
}

// ---------- Market data ----------
#[derive(Clone, Debug)]
pub struct BookEvent {
    pub symbol: String,
    pub bid_px: f64,
    pub bid_sz: f64,
    pub ask_px: f64,
    pub ask_sz: f64,
    pub ts_ms: i64,
}

#[derive(Clone, Debug)]
pub struct TradeEvent {
    pub symbol: String,
    pub px: f64,
    pub qty: f64,
    pub taker_is_buy: bool,
    pub ts_ms: i64,
}

#[async_trait::async_trait]
pub trait PublicMd: Send + Sync {
    async fn subscribe_books(&self, symbols: &[&str]) -> Result<()>;
    async fn next_book(&self) -> Result<BookEvent>;
    async fn subscribe_trades(&self, symbols: &[&str]) -> Result<()>;
    async fn next_trade(&self) -> Result<TradeEvent>;
}
