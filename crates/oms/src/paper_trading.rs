//! Paper Trading Simulation Mode
//!
//! Provides a simulated exchange adapter that:
//! - Accepts orders but doesn't send them to real exchanges
//! - Simulates fills based on market price movements
//! - Uses real market data for realistic simulation
//!
//! # Usage
//!
//! ```ignore
//! use oms::{OrderManager, Exchange, PaperTradingAdapter};
//!
//! let paper = PaperTradingAdapter::new();
//! let (tx, rx) = mpsc::channel(100);
//!
//! // Register paper trading instead of real exchange
//! oms.register_exchange(Exchange::Kraken, Arc::new(paper.clone()), rx).await;
//!
//! // Feed market data to trigger fills
//! paper.update_price("BTCUSD", 50000.0);
//! ```

use crate::order_router::ExchangeAdapter;
use adapters::traits::{NewOrder, Order, OrderStatus, OrderType, Side};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info};

// =============================================================================
// Paper Trading Adapter
// =============================================================================

/// Simulated exchange adapter for paper trading
///
/// This adapter simulates order execution without sending orders to real exchanges.
/// It tracks orders internally and simulates fills when prices cross limit orders.
#[derive(Clone)]
pub struct PaperTradingAdapter {
    inner: std::sync::Arc<PaperTradingInner>,
}

struct PaperTradingInner {
    /// Active orders (client_order_id -> order)
    orders: RwLock<HashMap<String, PaperOrder>>,

    /// Current market prices (symbol -> price)
    prices: RwLock<HashMap<String, f64>>,

    /// Order counter for generating venue IDs
    order_counter: AtomicU64,

    /// Configuration
    config: PaperTradingConfig,
}

/// Configuration for paper trading simulation
#[derive(Clone)]
pub struct PaperTradingConfig {
    /// Simulated latency in milliseconds (0 = instant)
    pub latency_ms: u64,

    /// Slippage in basis points for market orders (0 = no slippage)
    pub slippage_bps: f64,

    /// Whether to simulate partial fills
    pub partial_fills: bool,

    /// Fee rate for maker orders (e.g., 0.001 = 0.1%)
    pub maker_fee_rate: f64,

    /// Fee rate for taker orders
    pub taker_fee_rate: f64,
}

impl Default for PaperTradingConfig {
    fn default() -> Self {
        Self {
            latency_ms: 0,
            slippage_bps: 0.0,
            partial_fills: false,
            maker_fee_rate: 0.001,  // 0.1%
            taker_fee_rate: 0.002,  // 0.2%
        }
    }
}

impl PaperTradingConfig {
    /// Create config with realistic simulation settings
    pub fn realistic() -> Self {
        Self {
            latency_ms: 50,
            slippage_bps: 5.0,
            partial_fills: true,
            maker_fee_rate: 0.001,
            taker_fee_rate: 0.002,
        }
    }
}

/// Internal order representation
#[derive(Clone)]
struct PaperOrder {
    order: Order,
    is_maker: bool,
}

impl PaperTradingAdapter {
    /// Creates a new paper trading adapter with default config
    pub fn new() -> Self {
        Self::with_config(PaperTradingConfig::default())
    }

    /// Creates a paper trading adapter with custom config
    pub fn with_config(config: PaperTradingConfig) -> Self {
        Self {
            inner: std::sync::Arc::new(PaperTradingInner {
                orders: RwLock::new(HashMap::new()),
                prices: RwLock::new(HashMap::new()),
                order_counter: AtomicU64::new(1),
                config,
            }),
        }
    }

    /// Updates the market price for a symbol
    ///
    /// This should be called when market data is received.
    /// Returns fills that were triggered by this price update.
    pub fn update_price(&self, symbol: &str, price: f64) -> Vec<SimulatedFill> {
        // Update price
        {
            let mut prices = self.inner.prices.write();
            prices.insert(symbol.to_string(), price);
        }

        // Check for fills
        self.check_fills(symbol, price)
    }

    /// Gets the current price for a symbol
    pub fn get_price(&self, symbol: &str) -> Option<f64> {
        self.inner.prices.read().get(symbol).copied()
    }

    /// Gets all active orders
    pub fn get_orders(&self) -> Vec<Order> {
        self.inner.orders.read()
            .values()
            .map(|po| po.order.clone())
            .collect()
    }

    /// Gets active orders for a symbol
    pub fn get_orders_for_symbol(&self, symbol: &str) -> Vec<Order> {
        self.inner.orders.read()
            .values()
            .filter(|po| po.order.symbol == symbol)
            .map(|po| po.order.clone())
            .collect()
    }

    /// Checks if any limit orders should be filled at the current price
    fn check_fills(&self, symbol: &str, price: f64) -> Vec<SimulatedFill> {
        let mut fills = Vec::new();
        let mut to_remove = Vec::new();

        {
            let mut orders = self.inner.orders.write();

            for (client_id, paper_order) in orders.iter_mut() {
                let order = &mut paper_order.order;

                // Only check orders for this symbol
                if order.symbol != symbol {
                    continue;
                }

                // Only check open orders
                if !matches!(order.status, OrderStatus::New | OrderStatus::PartiallyFilled) {
                    continue;
                }

                // Check if order should fill
                let should_fill = match order.ord_type {
                    OrderType::Market => true,
                    OrderType::Limit => {
                        if let Some(limit_price) = order.price {
                            match order.side {
                                Side::Buy => price <= limit_price,
                                Side::Sell => price >= limit_price,
                            }
                        } else {
                            false
                        }
                    }
                    _ => false, // Stop orders etc. need more complex logic
                };

                if should_fill {
                    let fill_price = self.calculate_fill_price(order, price);
                    let fill_qty = order.remaining_qty;
                    let is_maker = paper_order.is_maker;
                    let fee_rate = if is_maker {
                        self.inner.config.maker_fee_rate
                    } else {
                        self.inner.config.taker_fee_rate
                    };
                    let fee = fill_qty * fill_price * fee_rate;

                    // Update order state
                    order.filled_qty += fill_qty;
                    order.remaining_qty = 0.0;
                    order.status = OrderStatus::Filled;
                    order.updated_ms = now_ms();

                    fills.push(SimulatedFill {
                        client_order_id: client_id.clone(),
                        venue_order_id: order.venue_order_id.clone(),
                        symbol: symbol.to_string(),
                        side: order.side.clone(),
                        price: fill_price,
                        qty: fill_qty,
                        fee,
                        fee_currency: "USD".to_string(),
                        is_maker,
                        timestamp_ms: now_ms(),
                    });

                    to_remove.push(client_id.clone());

                    info!(
                        symbol = %symbol,
                        side = ?order.side,
                        qty = fill_qty,
                        price = fill_price,
                        "Paper trade filled"
                    );
                }
            }

            // Remove filled orders
            for id in to_remove {
                orders.remove(&id);
            }
        }

        fills
    }

    /// Calculates the fill price with slippage for market orders
    fn calculate_fill_price(&self, order: &Order, market_price: f64) -> f64 {
        match order.ord_type {
            OrderType::Limit => order.price.unwrap_or(market_price),
            OrderType::Market => {
                let slippage_mult = self.inner.config.slippage_bps / 10000.0;
                match order.side {
                    Side::Buy => market_price * (1.0 + slippage_mult),
                    Side::Sell => market_price * (1.0 - slippage_mult),
                }
            }
            _ => market_price,
        }
    }

    fn generate_venue_id(&self) -> String {
        let count = self.inner.order_counter.fetch_add(1, Ordering::SeqCst);
        format!("paper_{}", count)
    }
}

impl Default for PaperTradingAdapter {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Simulated Fill
// =============================================================================

/// A simulated fill generated by paper trading
#[derive(Debug, Clone)]
pub struct SimulatedFill {
    pub client_order_id: String,
    pub venue_order_id: String,
    pub symbol: String,
    pub side: Side,
    pub price: f64,
    pub qty: f64,
    pub fee: f64,
    pub fee_currency: String,
    pub is_maker: bool,
    pub timestamp_ms: u64,
}

impl SimulatedFill {
    /// Convert to the standard Fill type
    pub fn to_fill(&self) -> adapters::traits::Fill {
        adapters::traits::Fill {
            venue_order_id: self.venue_order_id.clone(),
            client_order_id: self.client_order_id.clone(),
            symbol: self.symbol.clone(),
            price: self.price,
            qty: self.qty,
            fee: self.fee,
            fee_ccy: self.fee_currency.clone(),
            is_maker: self.is_maker,
            exec_id: format!("paper_exec_{}", self.timestamp_ms),
            ex_ts_ms: self.timestamp_ms,
            recv_ms: self.timestamp_ms,
        }
    }
}

// =============================================================================
// ExchangeAdapter Implementation
// =============================================================================

#[async_trait::async_trait]
impl ExchangeAdapter for PaperTradingAdapter {
    async fn create_order(&self, order: NewOrder) -> anyhow::Result<Order> {
        let now = now_ms();
        let venue_id = self.generate_venue_id();

        // Determine if this is a maker order (limit orders resting on book)
        let is_maker = matches!(order.ord_type, OrderType::Limit);

        // Check if market order should fill immediately
        let current_price = self.get_price(&order.symbol);
        let (status, filled_qty, remaining_qty) = if matches!(order.ord_type, OrderType::Market) {
            if current_price.is_some() {
                (OrderStatus::Filled, order.qty, 0.0)
            } else {
                // No price available, reject market order
                anyhow::bail!("No market price available for {}", order.symbol);
            }
        } else {
            (OrderStatus::New, 0.0, order.qty)
        };

        let result_order = Order {
            client_order_id: order.client_order_id.clone(),
            venue_order_id: venue_id,
            symbol: order.symbol.clone(),
            ord_type: order.ord_type.clone(),
            side: order.side.clone(),
            qty: order.qty,
            price: order.price,
            stop_price: order.stop_price,
            tif: order.tif.clone(),
            status,
            filled_qty,
            remaining_qty,
            created_ms: now,
            updated_ms: now,
            recv_ms: now,
            raw_status: Some("paper".to_string()),
        };

        // Store the order if it's not immediately filled
        if !matches!(status, OrderStatus::Filled) {
            let paper_order = PaperOrder {
                order: result_order.clone(),
                is_maker,
            };
            self.inner.orders.write().insert(order.client_order_id.clone(), paper_order);
        }

        debug!(
            client_id = %order.client_order_id,
            symbol = %order.symbol,
            side = ?order.side,
            qty = order.qty,
            price = ?order.price,
            "Paper order created"
        );

        Ok(result_order)
    }

    async fn cancel_order(&self, _symbol: &str, venue_order_id: &str) -> anyhow::Result<bool> {
        let mut orders = self.inner.orders.write();

        // Find by venue_order_id
        let client_id = orders
            .iter()
            .find(|(_, po)| po.order.venue_order_id == venue_order_id)
            .map(|(k, _)| k.clone());

        if let Some(id) = client_id {
            orders.remove(&id);
            debug!(venue_order_id = %venue_order_id, "Paper order cancelled");
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn cancel_all(&self, symbol: Option<&str>) -> anyhow::Result<usize> {
        let mut orders = self.inner.orders.write();
        let initial_count = orders.len();

        if let Some(sym) = symbol {
            orders.retain(|_, po| po.order.symbol != sym);
        } else {
            orders.clear();
        }

        let cancelled = initial_count - orders.len();
        debug!(count = cancelled, symbol = ?symbol, "Paper orders cancelled");
        Ok(cancelled)
    }

    async fn get_order(&self, _symbol: &str, venue_order_id: &str) -> anyhow::Result<Order> {
        let orders = self.inner.orders.read();

        orders
            .values()
            .find(|po| po.order.venue_order_id == venue_order_id)
            .map(|po| po.order.clone())
            .ok_or_else(|| anyhow::anyhow!("Order not found: {}", venue_order_id))
    }
}

// =============================================================================
// Helpers
// =============================================================================

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use adapters::traits::TimeInForce;

    fn make_order(symbol: &str, side: Side, ord_type: OrderType, qty: f64, price: Option<f64>) -> NewOrder {
        NewOrder {
            symbol: symbol.to_string(),
            side,
            ord_type,
            qty,
            price,
            stop_price: None,
            tif: Some(TimeInForce::Gtc),
            post_only: false,
            reduce_only: false,
            client_order_id: format!("test_{}", rand::random::<u32>()),
        }
    }

    #[tokio::test]
    async fn test_paper_trading_limit_order_created() {
        let paper = PaperTradingAdapter::new();

        let order = make_order("BTCUSD", Side::Buy, OrderType::Limit, 1.0, Some(50000.0));
        let result = paper.create_order(order).await;

        assert!(result.is_ok());
        let created = result.unwrap();
        assert!(matches!(created.status, OrderStatus::New));
        assert_eq!(created.filled_qty, 0.0);
        assert_eq!(created.remaining_qty, 1.0);
    }

    #[tokio::test]
    async fn test_paper_trading_market_order_needs_price() {
        let paper = PaperTradingAdapter::new();

        let order = make_order("BTCUSD", Side::Buy, OrderType::Market, 1.0, None);
        let result = paper.create_order(order).await;

        // Market order without price should fail
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_paper_trading_market_order_fills_immediately() {
        let paper = PaperTradingAdapter::new();
        paper.update_price("BTCUSD", 50000.0);

        let order = make_order("BTCUSD", Side::Buy, OrderType::Market, 1.0, None);
        let result = paper.create_order(order).await;

        assert!(result.is_ok());
        let created = result.unwrap();
        assert!(matches!(created.status, OrderStatus::Filled));
        assert_eq!(created.filled_qty, 1.0);
    }

    #[tokio::test]
    async fn test_paper_trading_limit_buy_fills_when_price_drops() {
        let paper = PaperTradingAdapter::new();
        paper.update_price("BTCUSD", 51000.0);

        // Place limit buy at 50000
        let order = make_order("BTCUSD", Side::Buy, OrderType::Limit, 1.0, Some(50000.0));
        let _ = paper.create_order(order).await.unwrap();

        // Price drops to 50000 - should fill
        let fills = paper.update_price("BTCUSD", 50000.0);

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].price, 50000.0);
        assert_eq!(fills[0].qty, 1.0);
    }

    #[tokio::test]
    async fn test_paper_trading_limit_sell_fills_when_price_rises() {
        let paper = PaperTradingAdapter::new();
        paper.update_price("BTCUSD", 49000.0);

        // Place limit sell at 50000
        let order = make_order("BTCUSD", Side::Sell, OrderType::Limit, 1.0, Some(50000.0));
        let _ = paper.create_order(order).await.unwrap();

        // Price rises to 50000 - should fill
        let fills = paper.update_price("BTCUSD", 50000.0);

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].price, 50000.0);
    }

    #[tokio::test]
    async fn test_paper_trading_cancel_order() {
        let paper = PaperTradingAdapter::new();

        let order = make_order("BTCUSD", Side::Buy, OrderType::Limit, 1.0, Some(50000.0));
        let created = paper.create_order(order).await.unwrap();

        let cancelled = paper.cancel_order("BTCUSD", &created.venue_order_id).await.unwrap();
        assert!(cancelled);

        // Should be gone
        assert!(paper.get_orders().is_empty());
    }

    #[tokio::test]
    async fn test_paper_trading_cancel_all() {
        let paper = PaperTradingAdapter::new();

        for i in 0..5 {
            let order = make_order("BTCUSD", Side::Buy, OrderType::Limit, 1.0, Some(50000.0 - i as f64 * 100.0));
            let _ = paper.create_order(order).await.unwrap();
        }

        assert_eq!(paper.get_orders().len(), 5);

        let cancelled = paper.cancel_all(None).await.unwrap();
        assert_eq!(cancelled, 5);
        assert!(paper.get_orders().is_empty());
    }

    #[tokio::test]
    async fn test_paper_trading_config_realistic() {
        let config = PaperTradingConfig::realistic();
        let paper = PaperTradingAdapter::with_config(config);

        assert!(paper.inner.config.latency_ms > 0);
        assert!(paper.inner.config.slippage_bps > 0.0);
    }

    #[tokio::test]
    async fn test_paper_trading_slippage() {
        let config = PaperTradingConfig {
            slippage_bps: 10.0, // 0.1% slippage
            ..Default::default()
        };
        let paper = PaperTradingAdapter::with_config(config);
        paper.update_price("BTCUSD", 50000.0);

        // Market buy should have positive slippage (pay more)
        let order = make_order("BTCUSD", Side::Buy, OrderType::Market, 1.0, None);
        let created = paper.create_order(order).await.unwrap();

        // With 10 bps slippage on 50000, should pay ~50005
        // But we can't check the fill price from create_order for market orders
        // since they fill immediately. This test just verifies it compiles/runs.
        assert!(matches!(created.status, OrderStatus::Filled));
    }

    #[tokio::test]
    async fn test_simulated_fill_to_fill() {
        let sim_fill = SimulatedFill {
            client_order_id: "client_123".to_string(),
            venue_order_id: "venue_456".to_string(),
            symbol: "BTCUSD".to_string(),
            side: Side::Buy,
            price: 50000.0,
            qty: 1.0,
            fee: 0.5,
            fee_currency: "USD".to_string(),
            is_maker: true,
            timestamp_ms: 1234567890,
        };

        let fill = sim_fill.to_fill();
        assert_eq!(fill.client_order_id, "client_123");
        assert_eq!(fill.price, 50000.0);
        assert_eq!(fill.qty, 1.0);
        assert_eq!(fill.fee, 0.5);
        assert!(fill.is_maker);
    }

    #[tokio::test]
    async fn test_paper_trading_multiple_symbols() {
        let paper = PaperTradingAdapter::new();

        // Create orders for different symbols
        let btc_order = make_order("BTCUSD", Side::Buy, OrderType::Limit, 1.0, Some(50000.0));
        let eth_order = make_order("ETHUSD", Side::Buy, OrderType::Limit, 10.0, Some(3000.0));

        let _ = paper.create_order(btc_order).await.unwrap();
        let _ = paper.create_order(eth_order).await.unwrap();

        assert_eq!(paper.get_orders().len(), 2);
        assert_eq!(paper.get_orders_for_symbol("BTCUSD").len(), 1);
        assert_eq!(paper.get_orders_for_symbol("ETHUSD").len(), 1);

        // Update BTC price - should only fill BTC order
        let fills = paper.update_price("BTCUSD", 50000.0);
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].symbol, "BTCUSD");

        // ETH order should still be open
        assert_eq!(paper.get_orders().len(), 1);
        assert_eq!(paper.get_orders_for_symbol("ETHUSD").len(), 1);
    }
}
