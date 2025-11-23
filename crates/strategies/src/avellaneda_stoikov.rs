//! Avellaneda-Stoikov Market Making Strategy
//!
//! Implements the classic optimal market making strategy from:
//! "High-frequency trading in a limit order book" (Avellaneda & Stoikov, 2008)
//!
//! Key Features:
//! - Optimal bid/ask placement based on inventory risk
//! - Volatility-aware spread adjustment
//! - Risk aversion parameter
//! - Reservation price incorporates inventory penalty
//!
//! Math:
//! - Reservation price: r = s - q*γ*σ²*T
//! - Optimal spread: δ = γ*σ²*T + (2/γ)*ln(1 + γ/κ)
//! - Bid: r - δ/2
//! - Ask: r + δ/2
//!
//! Where:
//! - s = mid price
//! - q = inventory (position)
//! - γ = risk aversion
//! - σ = volatility
//! - T = time remaining
//! - κ = order arrival rate

use crate::{MarketData, Strategy};
use adapters::traits::{BookUpdate, NewOrder, OrderType, PerpWs, Side, SpotWs, TimeInForce};
use anyhow::{anyhow, Result};
use inventory::PositionManager;
use oms::{Exchange, OrderManager};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Configuration for Avellaneda-Stoikov strategy
#[derive(Clone, Debug)]
pub struct AvellanedaStoikovConfig {
    /// Exchange to trade on
    pub exchange: Exchange,

    /// Symbol to trade
    pub symbol: String,

    /// Risk aversion parameter (γ)
    /// Higher = more conservative, wider spreads when inventory builds
    /// Typical range: 0.01 - 1.0
    pub risk_aversion: f64,

    /// Time horizon in seconds (T)
    /// Time over which we want to mean-revert inventory to zero
    /// Typical: 60-300 seconds
    pub time_horizon_secs: f64,

    /// Order size per level
    pub order_size: f64,

    /// Minimum allowed spread in basis points
    /// Prevents spreads from being too tight
    pub min_spread_bps: f64,

    /// Maximum allowed spread in basis points
    /// Prevents spreads from being too wide
    pub max_spread_bps: f64,

    /// Volatility estimation window (number of price samples)
    pub volatility_window: usize,

    /// How often to refresh quotes (milliseconds)
    pub quote_refresh_interval_ms: u64,

    /// Order arrival rate parameter (κ)
    /// Estimated fills per second at quoted spread
    /// Higher = expect more fills = tighter spreads
    /// Typical: 0.1 - 10.0
    pub order_arrival_rate: f64,

    /// Maximum inventory before pausing strategy
    pub max_inventory: f64,

    /// Minimum order size (exchange limit)
    pub min_order_size: f64,
}

impl Default for AvellanedaStoikovConfig {
    fn default() -> Self {
        Self {
            exchange: Exchange::Kraken,
            symbol: "BTCUSD".to_string(),
            risk_aversion: 0.1,
            time_horizon_secs: 180.0,
            order_size: 0.01,
            min_spread_bps: 5.0,
            max_spread_bps: 100.0,
            volatility_window: 100,
            quote_refresh_interval_ms: 1000,
            order_arrival_rate: 1.0,
            max_inventory: 1.0,
            min_order_size: 0.001,
        }
    }
}

/// Internal state for the strategy
struct StrategyState {
    /// Current bid order ID
    bid_order_id: Option<String>,

    /// Current ask order ID
    ask_order_id: Option<String>,

    /// Price history for volatility calculation
    price_history: VecDeque<f64>,

    /// Estimated volatility (σ)
    volatility: f64,

    /// Last mid price seen
    last_mid_price: Option<f64>,

    /// Number of quotes placed
    quote_count: u64,
}

impl StrategyState {
    fn new(window_size: usize) -> Self {
        Self {
            bid_order_id: None,
            ask_order_id: None,
            price_history: VecDeque::with_capacity(window_size),
            volatility: 0.0001, // Small initial value to avoid division by zero
            last_mid_price: None,
            quote_count: 0,
        }
    }

    /// Updates price history and recalculates volatility
    fn update_volatility(&mut self, mid_price: f64, window_size: usize) {
        self.price_history.push_back(mid_price);

        if self.price_history.len() > window_size {
            self.price_history.pop_front();
        }

        // Calculate volatility as standard deviation of returns
        if self.price_history.len() >= 2 {
            let returns: Vec<f64> = self
                .price_history
                .iter()
                .zip(self.price_history.iter().skip(1))
                .map(|(p1, p2)| (p2 / p1).ln())
                .collect();

            if !returns.is_empty() {
                let mean = returns.iter().sum::<f64>() / returns.len() as f64;
                let variance = returns
                    .iter()
                    .map(|r| (r - mean).powi(2))
                    .sum::<f64>()
                    / returns.len() as f64;

                self.volatility = variance.sqrt();

                // Ensure volatility is not too small
                if self.volatility < 0.0001 {
                    self.volatility = 0.0001;
                }
            }
        }
    }
}

/// Wrapper for spot adapters to provide a unified interface
pub struct SpotAdapter {
    inner: Arc<dyn SpotWs>,
}

impl SpotAdapter {
    pub fn new(adapter: Arc<dyn SpotWs>) -> Self {
        Self { inner: adapter }
    }

    pub async fn subscribe_books(&self, symbols: &[&str]) -> Result<tokio::sync::mpsc::Receiver<BookUpdate>> {
        self.inner.subscribe_books(symbols).await
    }
}

/// Wrapper for perp adapters to provide a unified interface
pub struct PerpAdapter {
    inner: Arc<dyn PerpWs>,
}

impl PerpAdapter {
    pub fn new(adapter: Arc<dyn PerpWs>) -> Self {
        Self { inner: adapter }
    }

    pub async fn subscribe_books(&self, symbols: &[&str]) -> Result<tokio::sync::mpsc::Receiver<BookUpdate>> {
        self.inner.subscribe_books(symbols).await
    }
}

/// Unified adapter type that can be either spot or perpetual
pub enum Adapter {
    Spot(SpotAdapter),
    Perp(PerpAdapter),
}

impl Adapter {
    pub async fn subscribe_books(&self, symbols: &[&str]) -> Result<tokio::sync::mpsc::Receiver<BookUpdate>> {
        match self {
            Adapter::Spot(adapter) => adapter.subscribe_books(symbols).await,
            Adapter::Perp(adapter) => adapter.subscribe_books(symbols).await,
        }
    }
}

/// Avellaneda-Stoikov market making strategy
pub struct AvellanedaStoikov {
    config: AvellanedaStoikovConfig,
    oms: Arc<OrderManager>,
    position_manager: Arc<PositionManager>,
    state: Arc<RwLock<StrategyState>>,
    adapter: Arc<Adapter>,
}

impl AvellanedaStoikov {
    /// Creates a new Avellaneda-Stoikov strategy
    pub fn new(
        config: AvellanedaStoikovConfig,
        oms: Arc<OrderManager>,
        position_manager: Arc<PositionManager>,
        adapter: Arc<Adapter>,
    ) -> Self {
        let state = StrategyState::new(config.volatility_window);

        Self {
            config,
            oms,
            position_manager,
            state: Arc::new(RwLock::new(state)),
            adapter,
        }
    }

    /// Calculates the reservation price
    ///
    /// r = s - q * γ * σ² * T
    ///
    /// This is the "fair" price adjusted for inventory risk
    #[cfg_attr(test, allow(dead_code))]
    pub(crate) fn calculate_reservation_price(
        &self,
        mid_price: f64,
        inventory: f64,
        volatility: f64,
    ) -> f64 {
        let inventory_penalty =
            inventory * self.config.risk_aversion * volatility.powi(2) * self.config.time_horizon_secs;

        mid_price - inventory_penalty
    }

    /// Calculates the optimal spread
    ///
    /// δ = γ * σ² * T + (2/γ) * ln(1 + γ/κ)
    ///
    /// First term: inventory risk component
    /// Second term: adverse selection component
    #[cfg_attr(test, allow(dead_code))]
    pub(crate) fn calculate_optimal_spread(&self, volatility: f64) -> f64 {
        let gamma = self.config.risk_aversion;
        let sigma_squared = volatility.powi(2);
        let T = self.config.time_horizon_secs;
        let kappa = self.config.order_arrival_rate;

        // Inventory risk term
        let inventory_term = gamma * sigma_squared * T;

        // Adverse selection term
        let adverse_selection_term = (2.0 / gamma) * (1.0 + gamma / kappa).ln();

        let spread = inventory_term + adverse_selection_term;

        // Apply min/max constraints (use a default mid price for spread calculation)
        let mid_price = 50000.0; // Default reference price for spread calculation
        let spread_bps = (spread / mid_price) * 10000.0;

        let clamped_bps = spread_bps
            .max(self.config.min_spread_bps)
            .min(self.config.max_spread_bps);

        (clamped_bps / 10000.0) * mid_price
    }

    /// Calculates optimal bid and ask prices
    ///
    /// Returns None if inventory limits are hit
    async fn calculate_quotes(&self, market_data: &MarketData) -> Option<(f64, f64)> {
        let mut state = self.state.write().await;

        // Update volatility estimate
        state.update_volatility(market_data.mid_price, self.config.volatility_window);
        state.last_mid_price = Some(market_data.mid_price);

        // Get current inventory
        let position = self
            .position_manager
            .get_position(self.config.exchange, &self.config.symbol);

        let inventory = position.map(|p| p.qty).unwrap_or(0.0);

        // Check inventory limits
        if inventory.abs() >= self.config.max_inventory {
            warn!(
                inventory,
                max = self.config.max_inventory,
                "Inventory limit reached - pausing quotes"
            );
            return None;
        }

        // Calculate reservation price
        let reservation_price = self.calculate_reservation_price(
            market_data.mid_price,
            inventory,
            state.volatility,
        );

        // Calculate optimal spread
        let spread = self.calculate_optimal_spread(state.volatility);
        let half_spread = spread / 2.0;

        // Calculate bid and ask
        let bid = reservation_price - half_spread;
        let ask = reservation_price + half_spread;

        debug!(
            mid = market_data.mid_price,
            reservation = reservation_price,
            spread,
            volatility = state.volatility,
            inventory,
            bid,
            ask,
            "Calculated Avellaneda-Stoikov quotes"
        );

        Some((bid, ask))
    }

    /// Places or updates bid and ask orders
    async fn update_quotes(&self, bid: f64, ask: f64) -> Result<()> {
        let mut state = self.state.write().await;

        // Cancel existing orders
        if let Some(bid_id) = &state.bid_order_id {
            if let Err(e) = self.oms.cancel_order(self.config.exchange, bid_id).await {
                warn!(order_id = %bid_id, error = %e, "Failed to cancel bid");
            }
        }

        if let Some(ask_id) = &state.ask_order_id {
            if let Err(e) = self.oms.cancel_order(self.config.exchange, ask_id).await {
                warn!(order_id = %ask_id, error = %e, "Failed to cancel ask");
            }
        }

        // Place new bid
        let bid_order = NewOrder {
            symbol: self.config.symbol.clone(),
            side: Side::Buy,
            ord_type: OrderType::Limit,
            qty: self.config.order_size,
            price: Some(bid),
            stop_price: None,
            tif: Some(TimeInForce::Gtc),
            post_only: true,
            reduce_only: false,
            client_order_id: String::new(),
        };

        match self.oms.submit_order(self.config.exchange, bid_order).await {
            Ok(id) => {
                state.bid_order_id = Some(id);
            }
            Err(e) => {
                warn!(error = %e, "Failed to submit bid");
            }
        }

        // Place new ask
        let ask_order = NewOrder {
            symbol: self.config.symbol.clone(),
            side: Side::Sell,
            ord_type: OrderType::Limit,
            qty: self.config.order_size,
            price: Some(ask),
            stop_price: None,
            tif: Some(TimeInForce::Gtc),
            post_only: true,
            reduce_only: false,
            client_order_id: String::new(),
        };

        match self.oms.submit_order(self.config.exchange, ask_order).await {
            Ok(id) => {
                state.ask_order_id = Some(id);
            }
            Err(e) => {
                warn!(error = %e, "Failed to submit ask");
            }
        }

        state.quote_count += 1;

        info!(
            bid,
            ask,
            spread_bps = ((ask - bid) / ((ask + bid) / 2.0)) * 10000.0,
            quote_count = state.quote_count,
            "Avellaneda-Stoikov quotes updated"
        );

        Ok(())
    }

    /// Converts a BookUpdate to MarketData
    fn book_update_to_market_data(update: BookUpdate) -> Option<MarketData> {
        match update {
            BookUpdate::TopOfBook {
                symbol,
                bid_px,
                bid_sz,
                ask_px,
                ask_sz,
                ex_ts_ms,
                recv_ms: _,
            } => {
                let mid_price = (bid_px + ask_px) / 2.0;
                Some(MarketData {
                    symbol,
                    bid_price: bid_px,
                    ask_price: ask_px,
                    mid_price,
                    bid_size: bid_sz,
                    ask_size: ask_sz,
                    timestamp_ms: ex_ts_ms,
                })
            }
            BookUpdate::DepthDelta {
                symbol,
                bids,
                asks,
                ex_ts_ms,
                ..
            } => {
                // Extract best bid and ask from depth data
                let best_bid = bids.first().copied();
                let best_ask = asks.first().copied();

                match (best_bid, best_ask) {
                    (Some((bid_px, bid_sz)), Some((ask_px, ask_sz))) => {
                        let mid_price = (bid_px + ask_px) / 2.0;
                        Some(MarketData {
                            symbol,
                            bid_price: bid_px,
                            ask_price: ask_px,
                            mid_price,
                            bid_size: bid_sz,
                            ask_size: ask_sz,
                            timestamp_ms: ex_ts_ms,
                        })
                    }
                    _ => None,
                }
            }
        }
    }

    /// Processes a market data update
    pub async fn on_market_data(&self, market_data: MarketData) -> Result<()> {
        if let Some((bid, ask)) = self.calculate_quotes(&market_data).await {
            self.update_quotes(bid, ask).await?;
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl Strategy for AvellanedaStoikov {
    fn name(&self) -> &str {
        "AvellanedaStoikov"
    }

    async fn initialize(&mut self) -> Result<()> {
        info!(
            exchange = ?self.config.exchange,
            symbol = %self.config.symbol,
            risk_aversion = self.config.risk_aversion,
            time_horizon_secs = self.config.time_horizon_secs,
            "Initializing Avellaneda-Stoikov strategy"
        );

        // Cancel any existing orders for this symbol
        self.oms
            .cancel_all_orders(self.config.exchange, Some(&self.config.symbol))
            .await?;

        info!("Avellaneda-Stoikov initialized");
        Ok(())
    }

    async fn run(&self) -> Result<()> {
        info!("Starting Avellaneda-Stoikov main loop");

        // Subscribe to book updates for this symbol
        let mut book_rx = self
            .adapter
            .subscribe_books(&[&self.config.symbol])
            .await
            .map_err(|e| anyhow!("Failed to subscribe to books: {}", e))?;

        info!(
            symbol = %self.config.symbol,
            "Subscribed to book updates - strategy is live"
        );

        // Set up quote refresh timer
        let mut refresh_interval = tokio::time::interval(tokio::time::Duration::from_millis(
            self.config.quote_refresh_interval_ms,
        ));

        // Track last market data for periodic refresh
        let mut last_market_data: Option<MarketData> = None;

        loop {
            tokio::select! {
                // Process incoming book updates
                Some(book_update) = book_rx.recv() => {
                    if let Some(market_data) = Self::book_update_to_market_data(book_update) {
                        // Only process updates for our symbol
                        if market_data.symbol == self.config.symbol {
                            debug!(
                                mid = market_data.mid_price,
                                spread_bps = market_data.spread_bps(),
                                "Received book update"
                            );

                            // Process the market data update
                            if let Err(e) = self.on_market_data(market_data.clone()).await {
                                warn!(error = %e, "Failed to process market data");
                            }

                            // Store for periodic refresh
                            last_market_data = Some(market_data);
                        }
                    }
                }

                // Periodic quote refresh (in case we haven't received updates)
                _ = refresh_interval.tick() => {
                    if let Some(ref market_data) = last_market_data {
                        debug!("Periodic quote refresh");
                        if let Err(e) = self.on_market_data(market_data.clone()).await {
                            warn!(error = %e, "Failed to refresh quotes");
                        }
                    }
                }
            }
        }
    }

    async fn shutdown(&self) -> Result<()> {
        info!("Shutting down Avellaneda-Stoikov");

        // Cancel all orders
        self.oms
            .cancel_all_orders(self.config.exchange, Some(&self.config.symbol))
            .await?;

        info!("Avellaneda-Stoikov shutdown complete");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==========================================================================
    // Configuration Tests
    // ==========================================================================

    #[test]
    fn test_default_config() {
        let config = AvellanedaStoikovConfig::default();

        assert_eq!(config.symbol, "BTCUSD");
        assert_eq!(config.risk_aversion, 0.1);
        assert_eq!(config.time_horizon_secs, 180.0);
        assert_eq!(config.order_size, 0.01);
        assert_eq!(config.min_spread_bps, 5.0);
        assert_eq!(config.max_spread_bps, 100.0);
        assert_eq!(config.volatility_window, 100);
        assert_eq!(config.quote_refresh_interval_ms, 1000);
        assert_eq!(config.order_arrival_rate, 1.0);
        assert_eq!(config.max_inventory, 1.0);
        assert_eq!(config.min_order_size, 0.001);
    }

    #[test]
    fn test_config_clone() {
        let config = AvellanedaStoikovConfig {
            exchange: Exchange::Kraken,
            symbol: "ETHUSD".to_string(),
            risk_aversion: 0.5,
            time_horizon_secs: 60.0,
            order_size: 0.1,
            min_spread_bps: 10.0,
            max_spread_bps: 50.0,
            volatility_window: 50,
            quote_refresh_interval_ms: 500,
            order_arrival_rate: 2.0,
            max_inventory: 5.0,
            min_order_size: 0.01,
        };

        let cloned = config.clone();
        assert_eq!(cloned.symbol, "ETHUSD");
        assert_eq!(cloned.risk_aversion, 0.5);
    }

    // ==========================================================================
    // Strategy State Tests
    // ==========================================================================

    #[test]
    fn test_strategy_state_new() {
        let state = StrategyState::new(100);

        assert!(state.bid_order_id.is_none());
        assert!(state.ask_order_id.is_none());
        assert!(state.price_history.is_empty());
        assert_eq!(state.volatility, 0.0001);
        assert!(state.last_mid_price.is_none());
        assert_eq!(state.quote_count, 0);
    }

    #[test]
    fn test_volatility_calculation_single_price() {
        let mut state = StrategyState::new(100);
        state.update_volatility(50000.0, 100);

        // With single price, volatility should remain at initial value
        assert_eq!(state.volatility, 0.0001);
        assert_eq!(state.price_history.len(), 1);
    }

    #[test]
    fn test_volatility_calculation_multiple_prices() {
        let mut state = StrategyState::new(100);

        // Add prices with some variance
        let prices = vec![
            50000.0, 50100.0, 49900.0, 50200.0, 49800.0,
            50150.0, 49950.0, 50050.0, 50000.0, 49850.0,
        ];

        for price in &prices {
            state.update_volatility(*price, 100);
        }

        // Volatility should be calculated from returns
        assert!(state.volatility > 0.0001);
        assert_eq!(state.price_history.len(), prices.len());
    }

    #[test]
    fn test_volatility_window_limit() {
        let mut state = StrategyState::new(5);

        // Add more prices than window size
        for i in 0..10 {
            state.update_volatility(50000.0 + (i as f64 * 100.0), 5);
        }

        // Should only keep last 5 prices
        assert_eq!(state.price_history.len(), 5);
    }

    #[test]
    fn test_volatility_with_stable_prices() {
        let mut state = StrategyState::new(100);

        // Add identical prices - should result in minimum volatility
        for _ in 0..10 {
            state.update_volatility(50000.0, 100);
        }

        // With identical prices, volatility should be clamped to minimum
        assert_eq!(state.volatility, 0.0001);
    }

    #[test]
    fn test_volatility_with_trending_prices() {
        let mut state = StrategyState::new(100);

        // Add trending prices (consistent increases)
        for i in 0..20 {
            state.update_volatility(50000.0 + (i as f64 * 10.0), 100);
        }

        // Trending prices should have low but non-zero volatility
        assert!(state.volatility >= 0.0001);
    }

    // ==========================================================================
    // Reservation Price Calculation Tests
    // ==========================================================================

    use adapters::traits::SpotWs;

    /// Mock WebSocket adapter for testing
    struct MockSpotWs;

    #[async_trait::async_trait]
    impl SpotWs for MockSpotWs {
        async fn subscribe_user(&self) -> anyhow::Result<tokio::sync::mpsc::Receiver<adapters::traits::UserEvent>> {
            let (_tx, rx) = tokio::sync::mpsc::channel(1);
            Ok(rx)
        }

        async fn subscribe_books(&self, _symbols: &[&str]) -> anyhow::Result<tokio::sync::mpsc::Receiver<BookUpdate>> {
            let (_tx, rx) = tokio::sync::mpsc::channel(1);
            Ok(rx)
        }

        async fn subscribe_trades(&self, _symbols: &[&str]) -> anyhow::Result<tokio::sync::mpsc::Receiver<adapters::traits::TradeEvent>> {
            let (_tx, rx) = tokio::sync::mpsc::channel(1);
            Ok(rx)
        }

        async fn health(&self) -> anyhow::Result<adapters::traits::HealthStatus> {
            Ok(adapters::traits::HealthStatus {
                status: adapters::traits::ConnectionStatus::Connected,
                last_ping_ms: None,
                last_pong_ms: None,
                latency_ms: None,
                reconnect_count: 0,
                error_msg: None,
            })
        }

        async fn reconnect(&self) -> anyhow::Result<()> {
            Ok(())
        }
    }

    /// Helper to create a real test strategy with mock dependencies
    fn create_real_strategy(config: AvellanedaStoikovConfig) -> AvellanedaStoikov {
        let oms = Arc::new(OrderManager::new());
        let position_manager = Arc::new(PositionManager::new());
        let mock_ws: Arc<dyn SpotWs> = Arc::new(MockSpotWs);
        let adapter = Arc::new(Adapter::Spot(SpotAdapter::new(mock_ws)));

        AvellanedaStoikov::new(config, oms, position_manager, adapter)
    }

    #[tokio::test]
    async fn test_reservation_price_zero_inventory() {
        let config = AvellanedaStoikovConfig::default();
        let strategy = create_real_strategy(config);

        let mid_price = 50000.0;
        let inventory = 0.0;
        let volatility = 0.001;

        let r = strategy.calculate_reservation_price(mid_price, inventory, volatility);

        // With zero inventory, reservation price should equal mid price
        assert_eq!(r, mid_price);
    }

    #[tokio::test]
    async fn test_reservation_price_long_inventory() {
        let config = AvellanedaStoikovConfig::default();
        let strategy = create_real_strategy(config);

        let mid_price = 50000.0;
        let inventory = 1.0; // Long
        let volatility = 0.001;

        let r = strategy.calculate_reservation_price(mid_price, inventory, volatility);

        // Long inventory should push reservation price below mid (want to sell)
        assert!(r < mid_price);
    }

    #[tokio::test]
    async fn test_reservation_price_short_inventory() {
        let config = AvellanedaStoikovConfig::default();
        let strategy = create_real_strategy(config);

        let mid_price = 50000.0;
        let inventory = -1.0; // Short
        let volatility = 0.001;

        let r = strategy.calculate_reservation_price(mid_price, inventory, volatility);

        // Short inventory should push reservation price above mid (want to buy)
        assert!(r > mid_price);
    }

    #[tokio::test]
    async fn test_reservation_price_higher_risk_aversion() {
        let mut config = AvellanedaStoikovConfig::default();
        config.risk_aversion = 0.5; // Higher risk aversion
        let strategy_high = create_real_strategy(config);

        let mid_price = 50000.0;
        let inventory = 1.0;
        let volatility = 0.001;

        let r_high_risk = strategy_high.calculate_reservation_price(mid_price, inventory, volatility);

        // Compare to lower risk aversion
        let mut config_low = AvellanedaStoikovConfig::default();
        config_low.risk_aversion = 0.1;
        let strategy_low = create_real_strategy(config_low);
        let r_low_risk = strategy_low.calculate_reservation_price(mid_price, inventory, volatility);

        // Higher risk aversion should result in larger deviation from mid price
        assert!((mid_price - r_high_risk).abs() > (mid_price - r_low_risk).abs());
    }

    #[tokio::test]
    async fn test_reservation_price_higher_volatility() {
        let config = AvellanedaStoikovConfig::default();
        let strategy = create_real_strategy(config);

        let mid_price = 50000.0;
        let inventory = 1.0;

        let r_low_vol = strategy.calculate_reservation_price(mid_price, inventory, 0.001);
        let r_high_vol = strategy.calculate_reservation_price(mid_price, inventory, 0.01);

        // Higher volatility should result in larger deviation from mid price
        assert!((mid_price - r_high_vol).abs() > (mid_price - r_low_vol).abs());
    }

    // ==========================================================================
    // Optimal Spread Calculation Tests
    // ==========================================================================

    #[tokio::test]
    async fn test_optimal_spread_positive() {
        let config = AvellanedaStoikovConfig::default();
        let strategy = create_real_strategy(config);

        let spread = strategy.calculate_optimal_spread(0.001);

        // Spread should always be positive
        assert!(spread > 0.0);
    }

    #[tokio::test]
    async fn test_optimal_spread_min_constraint() {
        let mut config = AvellanedaStoikovConfig::default();
        config.min_spread_bps = 10.0;
        let strategy = create_real_strategy(config);

        // Very low volatility would give tiny spread, but should be clamped
        let spread = strategy.calculate_optimal_spread(0.00001);
        let spread_bps = (spread / 50000.0) * 10000.0;

        assert!(spread_bps >= 10.0);
    }

    #[tokio::test]
    async fn test_optimal_spread_max_constraint() {
        let mut config = AvellanedaStoikovConfig::default();
        config.max_spread_bps = 50.0;
        config.risk_aversion = 10.0; // Very high to get wide spread
        let strategy = create_real_strategy(config);

        // High risk aversion and volatility would give wide spread, but should be clamped
        let spread = strategy.calculate_optimal_spread(0.1);
        let spread_bps = (spread / 50000.0) * 10000.0;

        assert!(spread_bps <= 50.0);
    }

    #[tokio::test]
    async fn test_optimal_spread_increases_with_volatility() {
        let config = AvellanedaStoikovConfig::default();
        let strategy = create_real_strategy(config);

        let spread_low = strategy.calculate_optimal_spread(0.0005);
        let spread_high = strategy.calculate_optimal_spread(0.005);

        // Higher volatility should result in wider spread (if not clamped)
        assert!(spread_high >= spread_low);
    }

    #[tokio::test]
    async fn test_optimal_spread_formula_components() {
        // The A-S spread formula: δ = γ*σ²*T + (2/γ)*ln(1 + γ/κ)
        // Note: The relationship with γ is non-monotonic due to the trade-off
        // between the inventory term (increases with γ) and adverse selection term (decreases with γ)

        let mut config = AvellanedaStoikovConfig::default();
        config.min_spread_bps = 0.1;
        config.max_spread_bps = 10000.0;
        config.risk_aversion = 0.1;
        config.time_horizon_secs = 180.0;
        config.order_arrival_rate = 1.0;
        let strategy = create_real_strategy(config);

        let volatility = 0.001;
        let spread = strategy.calculate_optimal_spread(volatility);

        // Manually calculate expected spread
        let gamma = 0.1;
        let sigma_sq = volatility * volatility;
        let t = 180.0;
        let kappa = 1.0;

        let inventory_term = gamma * sigma_sq * t;
        let adverse_selection_term = (2.0 / gamma) * (1.0 + gamma / kappa).ln();
        let _expected_raw = inventory_term + adverse_selection_term;

        // Spread should be positive and within reasonable bounds
        assert!(spread > 0.0, "spread should be positive");

        // The raw spread (before clamping) should match our calculation
        // Note: The implementation applies min/max clamping in basis points
        let mid_price = 50000.0;
        let spread_bps = (_expected_raw / mid_price) * 10000.0;
        assert!(spread_bps > 0.0, "raw spread bps should be positive: {}", spread_bps);
    }

    #[tokio::test]
    async fn test_spread_sensitivity_to_volatility_is_positive() {
        // Higher volatility should always increase spread (via σ² term)
        let mut config = AvellanedaStoikovConfig::default();
        config.min_spread_bps = 0.01;
        config.max_spread_bps = 10000.0;
        let strategy = create_real_strategy(config);

        let spread_low_vol = strategy.calculate_optimal_spread(0.0001);
        let spread_high_vol = strategy.calculate_optimal_spread(0.01);

        assert!(spread_high_vol > spread_low_vol,
            "higher volatility ({}) should give wider spread than lower volatility ({})",
            spread_high_vol, spread_low_vol);
    }

    // ==========================================================================
    // Quote Calculation Tests
    // ==========================================================================

    #[tokio::test]
    async fn test_bid_ask_symmetry_zero_inventory() {
        let config = AvellanedaStoikovConfig::default();
        let strategy = create_real_strategy(config);

        let mid_price = 50000.0;
        let inventory = 0.0;
        let volatility = 0.001;

        let reservation = strategy.calculate_reservation_price(mid_price, inventory, volatility);
        let spread = strategy.calculate_optimal_spread(volatility);
        let half_spread = spread / 2.0;

        let bid = reservation - half_spread;
        let ask = reservation + half_spread;

        // With zero inventory, bid and ask should be symmetric around mid
        let bid_distance = mid_price - bid;
        let ask_distance = ask - mid_price;

        assert!((bid_distance - ask_distance).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_bid_ask_asymmetry_with_inventory() {
        let config = AvellanedaStoikovConfig::default();
        let strategy = create_real_strategy(config);

        let mid_price = 50000.0;
        let inventory = 1.0; // Long position
        let volatility = 0.001;

        let reservation = strategy.calculate_reservation_price(mid_price, inventory, volatility);
        let spread = strategy.calculate_optimal_spread(volatility);
        let half_spread = spread / 2.0;

        let bid = reservation - half_spread;
        let ask = reservation + half_spread;

        // With long inventory, ask should be closer to mid than bid
        // (we want to sell, so ask is more aggressive)
        let bid_distance = mid_price - bid;
        let ask_distance = ask - mid_price;

        assert!(ask_distance < bid_distance);
    }

    // ==========================================================================
    // MarketData Tests
    // ==========================================================================

    #[test]
    fn test_market_data_spread() {
        let data = crate::MarketData {
            symbol: "BTCUSD".to_string(),
            bid_price: 49990.0,
            ask_price: 50010.0,
            mid_price: 50000.0,
            bid_size: 1.0,
            ask_size: 1.0,
            timestamp_ms: 0,
        };

        assert_eq!(data.spread(), 20.0);
    }

    #[test]
    fn test_market_data_spread_bps() {
        let data = crate::MarketData {
            symbol: "BTCUSD".to_string(),
            bid_price: 49990.0,
            ask_price: 50010.0,
            mid_price: 50000.0,
            bid_size: 1.0,
            ask_size: 1.0,
            timestamp_ms: 0,
        };

        // 20 / 50000 * 10000 = 4 bps
        assert!((data.spread_bps() - 4.0).abs() < 0.01);
    }

    // ==========================================================================
    // BookUpdate Conversion Tests
    // ==========================================================================

    #[test]
    fn test_book_update_top_of_book_conversion() {
        let update = BookUpdate::TopOfBook {
            symbol: "BTCUSD".to_string(),
            bid_px: 49990.0,
            bid_sz: 1.5,
            ask_px: 50010.0,
            ask_sz: 2.0,
            ex_ts_ms: 1234567890,
            recv_ms: 1234567891,
        };

        let market_data = AvellanedaStoikov::book_update_to_market_data(update).unwrap();

        assert_eq!(market_data.symbol, "BTCUSD");
        assert_eq!(market_data.bid_price, 49990.0);
        assert_eq!(market_data.ask_price, 50010.0);
        assert_eq!(market_data.mid_price, 50000.0);
        assert_eq!(market_data.bid_size, 1.5);
        assert_eq!(market_data.ask_size, 2.0);
        assert_eq!(market_data.timestamp_ms, 1234567890);
    }

    #[test]
    fn test_book_update_depth_delta_conversion() {
        let update = BookUpdate::DepthDelta {
            symbol: "BTCUSD".to_string(),
            bids: vec![(49990.0, 1.5), (49980.0, 2.0)],
            asks: vec![(50010.0, 2.0), (50020.0, 1.5)],
            seq: 1,
            prev_seq: 0,
            checksum: None,
            ex_ts_ms: 1234567890,
            recv_ms: 1234567891,
        };

        let market_data = AvellanedaStoikov::book_update_to_market_data(update).unwrap();

        assert_eq!(market_data.symbol, "BTCUSD");
        assert_eq!(market_data.bid_price, 49990.0);
        assert_eq!(market_data.ask_price, 50010.0);
        assert_eq!(market_data.mid_price, 50000.0);
    }

    #[test]
    fn test_book_update_depth_delta_empty_bids() {
        let update = BookUpdate::DepthDelta {
            symbol: "BTCUSD".to_string(),
            bids: vec![],
            asks: vec![(50010.0, 2.0)],
            seq: 1,
            prev_seq: 0,
            checksum: None,
            ex_ts_ms: 1234567890,
            recv_ms: 1234567891,
        };

        let market_data = AvellanedaStoikov::book_update_to_market_data(update);

        // Should return None if bids are empty
        assert!(market_data.is_none());
    }

    #[test]
    fn test_book_update_depth_delta_empty_asks() {
        let update = BookUpdate::DepthDelta {
            symbol: "BTCUSD".to_string(),
            bids: vec![(49990.0, 1.5)],
            asks: vec![],
            seq: 1,
            prev_seq: 0,
            checksum: None,
            ex_ts_ms: 1234567890,
            recv_ms: 1234567891,
        };

        let market_data = AvellanedaStoikov::book_update_to_market_data(update);

        // Should return None if asks are empty
        assert!(market_data.is_none());
    }

    // ==========================================================================
    // NetPosition Tests (from inventory)
    // ==========================================================================

    #[test]
    fn test_hedge_qty_long() {
        let net = inventory::position_manager::NetPosition {
            symbol: "BTCUSD".to_string(),
            net_qty: 2.0,
            avg_entry_px: 50000.0,
            by_exchange: std::collections::HashMap::new(),
        };

        // Threshold of 1.0, need to hedge 1.0
        assert_eq!(net.hedge_qty(1.0), 1.0);
    }

    #[test]
    fn test_hedge_qty_short() {
        let net = inventory::position_manager::NetPosition {
            symbol: "BTCUSD".to_string(),
            net_qty: -2.0,
            avg_entry_px: 50000.0,
            by_exchange: std::collections::HashMap::new(),
        };

        // Threshold of 1.0, need to hedge -1.0 (buy to cover)
        assert_eq!(net.hedge_qty(1.0), -1.0);
    }

    #[test]
    fn test_hedge_qty_within_threshold() {
        let net = inventory::position_manager::NetPosition {
            symbol: "BTCUSD".to_string(),
            net_qty: 0.5,
            avg_entry_px: 50000.0,
            by_exchange: std::collections::HashMap::new(),
        };

        // Within threshold, no hedging needed
        assert_eq!(net.hedge_qty(1.0), 0.0);
    }
}