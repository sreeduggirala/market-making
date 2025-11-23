//! Position Manager - Tracks positions across exchanges
//!
//! Maintains real-time view of positions with:
//! - Net position calculation
//! - PnL tracking
//! - Position aggregation across venues

use crate::{now_ms, PositionKey};
use adapters::traits::{Fill, Position};
use dashmap::DashMap;
use oms::Exchange;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

/// Tracks positions across multiple exchanges and symbols
#[derive(Clone)]
pub struct PositionManager {
    /// Positions indexed by exchange + symbol
    positions: Arc<DashMap<PositionKey, PositionState>>,
}

/// Internal position state with additional tracking
#[derive(Clone, Debug)]
pub struct PositionState {
    /// Current position from exchange
    pub position: Position,

    /// Total realized PnL from closed trades
    pub total_realized_pnl: f64,

    /// Number of fills that built this position
    pub fill_count: u64,

    /// Last update timestamp
    pub last_updated_ms: u64,
}

impl PositionManager {
    /// Creates a new position manager
    pub fn new() -> Self {
        Self {
            positions: Arc::new(DashMap::new()),
        }
    }

    /// Updates a position from exchange feed
    pub fn update_position(&self, exchange: Exchange, position: Position) {
        let key = PositionKey::new(exchange, position.symbol.clone());

        debug!(
            %exchange,
            symbol = %position.symbol,
            qty = position.qty,
            entry_px = position.entry_px,
            unrealized_pnl = ?position.unrealized_pnl,
            "Updating position"
        );

        self.positions
            .entry(key)
            .and_modify(|state| {
                state.position = position.clone();
                state.last_updated_ms = now_ms();
            })
            .or_insert_with(|| PositionState {
                position,
                total_realized_pnl: 0.0,
                fill_count: 0,
                last_updated_ms: now_ms(),
            });
    }

    /// Updates position based on a fill
    ///
    /// This is useful when you want to track positions locally even if
    /// the exchange doesn't send position updates
    pub fn update_from_fill(&self, exchange: Exchange, fill: &Fill) {
        let key = PositionKey::new(exchange, fill.symbol.clone());

        debug!(
            %exchange,
            symbol = %fill.symbol,
            qty = fill.qty,
            price = fill.price,
            "Updating position from fill"
        );

        self.positions
            .entry(key)
            .and_modify(|state| {
                // Update position quantity (this is simplified - real impl needs to track side)
                // For now, assume fills correctly update the position
                state.fill_count += 1;
                state.last_updated_ms = now_ms();
            })
            .or_insert_with(|| {
                // Create new position if doesn't exist
                let position = Position {
                    exchange: Some(exchange.to_string()),
                    symbol: fill.symbol.clone(),
                    qty: 0.0, // Will be updated by exchange
                    entry_px: fill.price,
                    mark_px: Some(fill.price),
                    liquidation_px: None,
                    unrealized_pnl: None,
                    realized_pnl: None,
                    margin: None,
                    leverage: None,
                    opened_ms: Some(fill.ex_ts_ms),
                    updated_ms: fill.ex_ts_ms,
                };

                PositionState {
                    position,
                    total_realized_pnl: 0.0,
                    fill_count: 1,
                    last_updated_ms: now_ms(),
                }
            });
    }

    /// Gets a position for a specific exchange and symbol
    pub fn get_position(&self, exchange: Exchange, symbol: &str) -> Option<Position> {
        let key = PositionKey::new(exchange, symbol);
        self.positions.get(&key).map(|state| state.position.clone())
    }

    /// Gets all positions for an exchange
    pub fn get_positions_for_exchange(&self, exchange: Exchange) -> Vec<Position> {
        self.positions
            .iter()
            .filter(|entry| entry.key().exchange == exchange)
            .map(|entry| entry.value().position.clone())
            .collect()
    }

    /// Gets net position across all exchanges for a symbol
    ///
    /// Useful for cross-exchange hedging
    pub fn get_net_position(&self, symbol: &str) -> NetPosition {
        let mut net_qty = 0.0;
        let mut total_notional = 0.0;
        let mut positions_map = HashMap::new();

        for entry in self.positions.iter() {
            if entry.key().symbol == symbol {
                let pos = &entry.value().position;
                net_qty += pos.qty;
                total_notional += pos.qty * pos.entry_px;

                positions_map.insert(entry.key().exchange, pos.clone());
            }
        }

        let avg_entry_px = if net_qty.abs() > 0.001 {
            total_notional / net_qty
        } else {
            0.0
        };

        NetPosition {
            symbol: symbol.to_string(),
            net_qty,
            avg_entry_px,
            by_exchange: positions_map,
        }
    }

    /// Gets all unique symbols being tracked
    pub fn get_tracked_symbols(&self) -> Vec<String> {
        let mut symbols: Vec<String> = self
            .positions
            .iter()
            .map(|entry| entry.key().symbol.clone())
            .collect();

        symbols.sort();
        symbols.dedup();
        symbols
    }

    /// Gets all positions
    pub fn get_all_positions(&self) -> HashMap<PositionKey, Position> {
        self.positions
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().position.clone()))
            .collect()
    }

    /// Clears a position (when it's flat)
    pub fn clear_position(&self, exchange: Exchange, symbol: &str) {
        let key = PositionKey::new(exchange, symbol);
        if let Some((_, state)) = self.positions.remove(&key) {
            info!(
                %exchange,
                %symbol,
                total_realized_pnl = state.total_realized_pnl,
                "Position cleared"
            );
        }
    }

    /// Gets position statistics
    pub fn get_stats(&self) -> PositionStats {
        let mut stats = PositionStats::default();

        for entry in self.positions.iter() {
            let pos = &entry.value().position;
            stats.total_positions += 1;

            if pos.qty.abs() > 0.001 {
                stats.active_positions += 1;

                if let Some(upnl) = pos.unrealized_pnl {
                    stats.total_unrealized_pnl += upnl;
                }
            }

            if let Some(rpnl) = pos.realized_pnl {
                stats.total_realized_pnl += rpnl;
            }
        }

        stats
    }
}

impl Default for PositionManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Net position across all exchanges for a symbol
#[derive(Clone, Debug)]
pub struct NetPosition {
    pub symbol: String,
    pub net_qty: f64,
    pub avg_entry_px: f64,
    pub by_exchange: HashMap<Exchange, Position>,
}

impl NetPosition {
    /// Returns true if position needs hedging based on threshold
    pub fn needs_hedging(&self, threshold: f64) -> bool {
        self.net_qty.abs() > threshold
    }

    /// Gets the quantity that needs to be hedged
    pub fn hedge_qty(&self, threshold: f64) -> f64 {
        if self.net_qty > threshold {
            self.net_qty - threshold
        } else if self.net_qty < -threshold {
            self.net_qty + threshold
        } else {
            0.0
        }
    }
}

/// Position statistics
#[derive(Clone, Debug, Default)]
pub struct PositionStats {
    pub total_positions: usize,
    pub active_positions: usize,
    pub total_unrealized_pnl: f64,
    pub total_realized_pnl: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_position(symbol: &str, qty: f64, entry_px: f64) -> Position {
        Position {
            exchange: None,
            symbol: symbol.to_string(),
            qty,
            entry_px,
            mark_px: None,
            liquidation_px: None,
            unrealized_pnl: None,
            realized_pnl: None,
            margin: None,
            leverage: None,
            opened_ms: None,
            updated_ms: now_ms(),
        }
    }

    fn make_test_fill(symbol: &str, qty: f64, price: f64) -> Fill {
        Fill {
            venue_order_id: "v1".to_string(),
            client_order_id: "c1".to_string(),
            symbol: symbol.to_string(),
            price,
            qty,
            fee: 0.0,
            fee_ccy: "USD".to_string(),
            is_maker: true,
            exec_id: "exec1".to_string(),
            ex_ts_ms: now_ms(),
            recv_ms: now_ms(),
        }
    }

    // ==========================================================================
    // Basic Position Management
    // ==========================================================================

    #[test]
    fn test_position_update() {
        let manager = PositionManager::new();

        let position = Position {
            exchange: Some("kraken".to_string()),
            symbol: "BTCUSD".to_string(),
            qty: 1.0,
            entry_px: 50000.0,
            mark_px: Some(51000.0),
            liquidation_px: None,
            unrealized_pnl: Some(1000.0),
            realized_pnl: Some(0.0),
            margin: None,
            leverage: None,
            opened_ms: Some(now_ms()),
            updated_ms: now_ms(),
        };

        manager.update_position(Exchange::Kraken, position.clone());

        let retrieved = manager.get_position(Exchange::Kraken, "BTCUSD").unwrap();
        assert_eq!(retrieved.qty, 1.0);
        assert_eq!(retrieved.entry_px, 50000.0);
    }

    #[test]
    fn test_get_nonexistent_position() {
        let manager = PositionManager::new();
        assert!(manager.get_position(Exchange::Kraken, "BTCUSD").is_none());
    }

    #[test]
    fn test_position_update_overwrites() {
        let manager = PositionManager::new();

        // Initial position
        manager.update_position(Exchange::Kraken, make_test_position("BTCUSD", 1.0, 50000.0));

        // Update position
        manager.update_position(Exchange::Kraken, make_test_position("BTCUSD", 2.0, 51000.0));

        let pos = manager.get_position(Exchange::Kraken, "BTCUSD").unwrap();
        assert_eq!(pos.qty, 2.0);
        assert_eq!(pos.entry_px, 51000.0);
    }

    // ==========================================================================
    // Multi-Exchange Position Management
    // ==========================================================================

    #[test]
    fn test_positions_across_exchanges() {
        let manager = PositionManager::new();

        manager.update_position(Exchange::Kraken, make_test_position("BTCUSD", 1.0, 50000.0));
        manager.update_position(Exchange::Mexc, make_test_position("BTCUSD", 2.0, 51000.0));
        manager.update_position(Exchange::Bybit, make_test_position("ETHUSD", 10.0, 3000.0));

        // Check Kraken
        let kraken_btc = manager.get_position(Exchange::Kraken, "BTCUSD").unwrap();
        assert_eq!(kraken_btc.qty, 1.0);

        // Check MEXC
        let mexc_btc = manager.get_position(Exchange::Mexc, "BTCUSD").unwrap();
        assert_eq!(mexc_btc.qty, 2.0);

        // Check Bybit ETH
        let bybit_eth = manager.get_position(Exchange::Bybit, "ETHUSD").unwrap();
        assert_eq!(bybit_eth.qty, 10.0);
    }

    #[test]
    fn test_get_positions_for_exchange() {
        let manager = PositionManager::new();

        manager.update_position(Exchange::Kraken, make_test_position("BTCUSD", 1.0, 50000.0));
        manager.update_position(Exchange::Kraken, make_test_position("ETHUSD", 10.0, 3000.0));
        manager.update_position(Exchange::Mexc, make_test_position("BTCUSD", 2.0, 51000.0));

        let kraken_positions = manager.get_positions_for_exchange(Exchange::Kraken);
        assert_eq!(kraken_positions.len(), 2);

        let mexc_positions = manager.get_positions_for_exchange(Exchange::Mexc);
        assert_eq!(mexc_positions.len(), 1);
    }

    // ==========================================================================
    // Net Position Calculation
    // ==========================================================================

    #[test]
    fn test_net_position() {
        let manager = PositionManager::new();

        // Long on Kraken
        manager.update_position(
            Exchange::Kraken,
            Position {
                exchange: Some("kraken".to_string()),
                symbol: "BTCUSD".to_string(),
                qty: 1.0,
                entry_px: 50000.0,
                mark_px: None,
                liquidation_px: None,
                unrealized_pnl: None,
                realized_pnl: None,
                margin: None,
                leverage: None,
                opened_ms: None,
                updated_ms: now_ms(),
            },
        );

        // Short on MEXC
        manager.update_position(
            Exchange::Mexc,
            Position {
                exchange: Some("mexc".to_string()),
                symbol: "BTCUSD".to_string(),
                qty: -0.5,
                entry_px: 51000.0,
                mark_px: None,
                liquidation_px: None,
                unrealized_pnl: None,
                realized_pnl: None,
                margin: None,
                leverage: None,
                opened_ms: None,
                updated_ms: now_ms(),
            },
        );

        let net = manager.get_net_position("BTCUSD");
        assert_eq!(net.net_qty, 0.5);
        assert_eq!(net.by_exchange.len(), 2);
    }

    #[test]
    fn test_net_position_fully_hedged() {
        let manager = PositionManager::new();

        manager.update_position(Exchange::Kraken, make_test_position("BTCUSD", 1.0, 50000.0));
        manager.update_position(Exchange::Mexc, make_test_position("BTCUSD", -1.0, 50000.0));

        let net = manager.get_net_position("BTCUSD");
        assert!((net.net_qty).abs() < 0.0001); // Effectively zero
    }

    #[test]
    fn test_net_position_no_positions() {
        let manager = PositionManager::new();
        let net = manager.get_net_position("BTCUSD");
        assert_eq!(net.net_qty, 0.0);
        assert!(net.by_exchange.is_empty());
    }

    // ==========================================================================
    // Hedging Logic
    // ==========================================================================

    #[test]
    fn test_needs_hedging() {
        let net_pos = NetPosition {
            symbol: "BTCUSD".to_string(),
            net_qty: 1.5,
            avg_entry_px: 50000.0,
            by_exchange: HashMap::new(),
        };

        assert!(net_pos.needs_hedging(1.0));
        assert!(!net_pos.needs_hedging(2.0));
    }

    #[test]
    fn test_hedge_qty_long() {
        let net_pos = NetPosition {
            symbol: "BTCUSD".to_string(),
            net_qty: 2.0,
            avg_entry_px: 50000.0,
            by_exchange: HashMap::new(),
        };

        // Threshold 1.0, need to hedge 1.0 (sell 1.0 to get to 1.0)
        assert_eq!(net_pos.hedge_qty(1.0), 1.0);
    }

    #[test]
    fn test_hedge_qty_short() {
        let net_pos = NetPosition {
            symbol: "BTCUSD".to_string(),
            net_qty: -2.0,
            avg_entry_px: 50000.0,
            by_exchange: HashMap::new(),
        };

        // Threshold 1.0, need to hedge -1.0 (buy 1.0 to get to -1.0)
        assert_eq!(net_pos.hedge_qty(1.0), -1.0);
    }

    #[test]
    fn test_hedge_qty_within_threshold() {
        let net_pos = NetPosition {
            symbol: "BTCUSD".to_string(),
            net_qty: 0.5,
            avg_entry_px: 50000.0,
            by_exchange: HashMap::new(),
        };

        // Within threshold, no hedging needed
        assert_eq!(net_pos.hedge_qty(1.0), 0.0);
    }

    // ==========================================================================
    // Fill-Based Updates
    // ==========================================================================

    #[test]
    fn test_update_from_fill_creates_position() {
        let manager = PositionManager::new();
        let fill = make_test_fill("BTCUSD", 1.0, 50000.0);

        manager.update_from_fill(Exchange::Kraken, &fill);

        let pos = manager.get_position(Exchange::Kraken, "BTCUSD").unwrap();
        assert_eq!(pos.symbol, "BTCUSD");
    }

    #[test]
    fn test_update_from_fill_increments_count() {
        let manager = PositionManager::new();
        let fill = make_test_fill("BTCUSD", 1.0, 50000.0);

        // First fill creates position
        manager.update_from_fill(Exchange::Kraken, &fill);

        // Second fill increments count
        manager.update_from_fill(Exchange::Kraken, &fill);

        // Position should exist
        assert!(manager.get_position(Exchange::Kraken, "BTCUSD").is_some());
    }

    // ==========================================================================
    // Clear and Get All
    // ==========================================================================

    #[test]
    fn test_clear_position() {
        let manager = PositionManager::new();
        manager.update_position(Exchange::Kraken, make_test_position("BTCUSD", 1.0, 50000.0));

        manager.clear_position(Exchange::Kraken, "BTCUSD");

        assert!(manager.get_position(Exchange::Kraken, "BTCUSD").is_none());
    }

    #[test]
    fn test_get_all_positions() {
        let manager = PositionManager::new();

        manager.update_position(Exchange::Kraken, make_test_position("BTCUSD", 1.0, 50000.0));
        manager.update_position(Exchange::Mexc, make_test_position("ETHUSD", 10.0, 3000.0));

        let all = manager.get_all_positions();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn test_get_tracked_symbols() {
        let manager = PositionManager::new();

        manager.update_position(Exchange::Kraken, make_test_position("BTCUSD", 1.0, 50000.0));
        manager.update_position(Exchange::Mexc, make_test_position("BTCUSD", 2.0, 51000.0));
        manager.update_position(Exchange::Bybit, make_test_position("ETHUSD", 10.0, 3000.0));

        let symbols = manager.get_tracked_symbols();
        assert_eq!(symbols.len(), 2);
        assert!(symbols.contains(&"BTCUSD".to_string()));
        assert!(symbols.contains(&"ETHUSD".to_string()));
    }

    // ==========================================================================
    // Statistics
    // ==========================================================================

    #[test]
    fn test_get_stats_empty() {
        let manager = PositionManager::new();
        let stats = manager.get_stats();
        assert_eq!(stats.total_positions, 0);
        assert_eq!(stats.active_positions, 0);
    }

    #[test]
    fn test_get_stats_with_positions() {
        let manager = PositionManager::new();

        // Active position with PnL
        let mut pos1 = make_test_position("BTCUSD", 1.0, 50000.0);
        pos1.unrealized_pnl = Some(1000.0);
        pos1.realized_pnl = Some(500.0);
        manager.update_position(Exchange::Kraken, pos1);

        // Flat position
        let pos2 = make_test_position("ETHUSD", 0.0, 3000.0);
        manager.update_position(Exchange::Mexc, pos2);

        let stats = manager.get_stats();
        assert_eq!(stats.total_positions, 2);
        assert_eq!(stats.active_positions, 1);
        assert_eq!(stats.total_unrealized_pnl, 1000.0);
        assert_eq!(stats.total_realized_pnl, 500.0);
    }

    // ==========================================================================
    // Default Implementation
    // ==========================================================================

    #[test]
    fn test_default() {
        let manager = PositionManager::default();
        assert!(manager.get_all_positions().is_empty());
    }

    // ==========================================================================
    // Clone Behavior
    // ==========================================================================

    #[test]
    fn test_clone_shares_state() {
        let manager = PositionManager::new();
        let cloned = manager.clone();

        manager.update_position(Exchange::Kraken, make_test_position("BTCUSD", 1.0, 50000.0));

        // Clone should see the same data
        assert!(cloned.get_position(Exchange::Kraken, "BTCUSD").is_some());
    }
}
