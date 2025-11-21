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
    fn calculate_reservation_price(
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
    fn calculate_optimal_spread(&self, volatility: f64) -> f64 {
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