//! In-memory order book for tracking order state
//!
//! Maintains multiple indexes for fast lookups:
//! - By client_order_id (primary key)
//! - By venue_order_id (for exchange callbacks)
//! - By symbol (for queries)
//! - By status (open, filled, etc.)

use adapters::traits::{Order, OrderStatus};
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, warn};

/// Thread-safe order book with multiple indexes
#[derive(Clone)]
pub struct OrderBook {
    /// Primary index: client_order_id -> Order
    by_client_id: Arc<DashMap<String, Order>>,

    /// Secondary index: venue_order_id -> client_order_id
    by_venue_id: Arc<DashMap<String, String>>,

    /// Index by symbol: symbol -> Vec<client_order_id>
    by_symbol: Arc<DashMap<String, Vec<String>>>,

    /// Maximum age for completed orders in milliseconds (default: 24 hours)
    max_order_age_ms: u64,
}

impl OrderBook {
    /// Creates a new empty order book with default 24-hour retention
    pub fn new() -> Self {
        Self::with_retention(24 * 60 * 60 * 1000) // 24 hours in ms
    }

    /// Creates a new order book with custom retention period
    pub fn with_retention(max_age_ms: u64) -> Self {
        Self {
            by_client_id: Arc::new(DashMap::new()),
            by_venue_id: Arc::new(DashMap::new()),
            by_symbol: Arc::new(DashMap::new()),
            max_order_age_ms: max_age_ms,
        }
    }

    /// Inserts a new order into the book
    ///
    /// Returns error if order with same client_order_id already exists
    pub fn insert(&self, order: Order) -> crate::Result<()> {
        let client_id = order.client_order_id.clone();
        let venue_id = order.venue_order_id.clone();
        let symbol = order.symbol.clone();

        // Check for duplicate
        if self.by_client_id.contains_key(&client_id) {
            return Err(crate::OmsError::DuplicateOrder(client_id));
        }

        debug!(
            client_order_id = %client_id,
            venue_order_id = %venue_id,
            symbol = %symbol,
            status = ?order.status,
            "Inserting order into book"
        );

        // Insert into primary index
        self.by_client_id.insert(client_id.clone(), order);

        // Insert into venue index
        self.by_venue_id.insert(venue_id, client_id.clone());

        // Insert into symbol index
        self.by_symbol
            .entry(symbol)
            .or_insert_with(Vec::new)
            .push(client_id);

        Ok(())
    }

    /// Updates an existing order
    ///
    /// Returns error if order doesn't exist
    pub fn update(&self, order: Order) -> crate::Result<()> {
        let client_id = order.client_order_id.clone();

        if !self.by_client_id.contains_key(&client_id) {
            return Err(crate::OmsError::OrderNotFound(client_id));
        }

        debug!(
            client_order_id = %client_id,
            status = ?order.status,
            filled_qty = order.filled_qty,
            "Updating order"
        );

        self.by_client_id.insert(client_id, order);
        Ok(())
    }

    /// Gets an order by client_order_id
    pub fn get_by_client_id(&self, client_id: &str) -> Option<Order> {
        self.by_client_id.get(client_id).map(|r| r.clone())
    }

    /// Gets an order by venue_order_id
    pub fn get_by_venue_id(&self, venue_id: &str) -> Option<Order> {
        self.by_venue_id
            .get(venue_id)
            .and_then(|client_id| self.get_by_client_id(&client_id))
    }

    /// Gets all orders for a symbol
    pub fn get_by_symbol(&self, symbol: &str) -> Vec<Order> {
        self.by_symbol
            .get(symbol)
            .map(|client_ids| {
                client_ids
                    .iter()
                    .filter_map(|id| self.get_by_client_id(id))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Gets all open orders (New or PartiallyFilled)
    pub fn get_open_orders(&self, symbol: Option<&str>) -> Vec<Order> {
        let orders: Vec<Order> = if let Some(sym) = symbol {
            self.get_by_symbol(sym)
        } else {
            self.by_client_id
                .iter()
                .map(|entry| entry.value().clone())
                .collect()
        };

        orders
            .into_iter()
            .filter(|o| {
                matches!(o.status, OrderStatus::New | OrderStatus::PartiallyFilled)
            })
            .collect()
    }

    /// Gets all orders (optionally filtered by symbol)
    pub fn get_all_orders(&self, symbol: Option<&str>) -> Vec<Order> {
        if let Some(sym) = symbol {
            self.get_by_symbol(sym)
        } else {
            self.by_client_id
                .iter()
                .map(|entry| entry.value().clone())
                .collect()
        }
    }

    /// Removes an order from the book (for cleanup of old orders)
    pub fn remove(&self, client_id: &str) -> Option<Order> {
        if let Some((_, order)) = self.by_client_id.remove(client_id) {
            // Clean up secondary indexes
            self.by_venue_id.remove(&order.venue_order_id);

            if let Some(mut ids) = self.by_symbol.get_mut(&order.symbol) {
                ids.retain(|id| id != client_id);
            }

            debug!(client_order_id = %client_id, "Removed order from book");
            Some(order)
        } else {
            None
        }
    }

    /// Returns the total number of orders in the book
    pub fn len(&self) -> usize {
        self.by_client_id.len()
    }

    /// Returns true if the book is empty
    pub fn is_empty(&self) -> bool {
        self.by_client_id.is_empty()
    }

    /// Gets order statistics
    pub fn get_stats(&self) -> OrderBookStats {
        let mut stats = OrderBookStats::default();

        for entry in self.by_client_id.iter() {
            let order = entry.value();
            stats.total_orders += 1;

            match order.status {
                OrderStatus::New => stats.new_orders += 1,
                OrderStatus::PartiallyFilled => stats.partially_filled += 1,
                OrderStatus::Filled => stats.filled_orders += 1,
                OrderStatus::Canceled => stats.canceled_orders += 1,
                OrderStatus::Rejected => stats.rejected_orders += 1,
                OrderStatus::Expired => stats.expired_orders += 1,
            }
        }

        stats
    }

    /// Cleans up old completed orders to prevent memory leak
    ///
    /// Removes orders that are:
    /// - Filled, Canceled, Rejected, or Expired
    /// - Updated more than max_order_age_ms ago
    ///
    /// Returns the number of orders removed
    pub fn cleanup_old_orders(&self) -> usize {
        let cutoff = crate::now_ms() - self.max_order_age_ms;
        let mut removed_count = 0;

        // Collect client IDs to remove (can't modify while iterating)
        let to_remove: Vec<String> = self
            .by_client_id
            .iter()
            .filter_map(|entry| {
                let order = entry.value();
                let is_completed = matches!(
                    order.status,
                    OrderStatus::Filled
                        | OrderStatus::Canceled
                        | OrderStatus::Rejected
                        | OrderStatus::Expired
                );

                if is_completed && order.updated_ms < cutoff {
                    Some(entry.key().clone())
                } else {
                    None
                }
            })
            .collect();

        // Remove old orders
        for client_id in to_remove {
            if let Some((_, order)) = self.by_client_id.remove(&client_id) {
                // Clean up secondary indexes
                self.by_venue_id.remove(&order.venue_order_id);

                if let Some(mut ids) = self.by_symbol.get_mut(&order.symbol) {
                    ids.retain(|id| id != &client_id);
                }

                removed_count += 1;
            }
        }

        if removed_count > 0 {
            debug!(
                removed = removed_count,
                cutoff_age_hours = self.max_order_age_ms / (60 * 60 * 1000),
                "Cleaned up old orders"
            );
        }

        removed_count
    }
}

impl Default for OrderBook {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about the order book
#[derive(Clone, Debug, Default)]
pub struct OrderBookStats {
    pub total_orders: usize,
    pub new_orders: usize,
    pub partially_filled: usize,
    pub filled_orders: usize,
    pub canceled_orders: usize,
    pub rejected_orders: usize,
    pub expired_orders: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use adapters::traits::{OrderType, Side, TimeInForce};

    fn make_test_order(client_id: &str, venue_id: &str, symbol: &str) -> Order {
        Order {
            client_order_id: client_id.to_string(),
            venue_order_id: venue_id.to_string(),
            symbol: symbol.to_string(),
            ord_type: OrderType::Limit,
            side: Side::Buy,
            qty: 1.0,
            price: Some(100.0),
            stop_price: None,
            tif: Some(TimeInForce::Gtc),
            status: OrderStatus::New,
            filled_qty: 0.0,
            remaining_qty: 1.0,
            created_ms: 0,
            updated_ms: 0,
            recv_ms: 0,
            raw_status: None,
        }
    }

    #[test]
    fn test_insert_and_get() {
        let book = OrderBook::new();
        let order = make_test_order("client1", "venue1", "BTCUSD");

        book.insert(order.clone()).unwrap();

        let retrieved = book.get_by_client_id("client1").unwrap();
        assert_eq!(retrieved.client_order_id, "client1");
    }

    #[test]
    fn test_duplicate_insert() {
        let book = OrderBook::new();
        let order = make_test_order("client1", "venue1", "BTCUSD");

        book.insert(order.clone()).unwrap();
        let result = book.insert(order);

        assert!(result.is_err());
    }

    #[test]
    fn test_get_by_venue_id() {
        let book = OrderBook::new();
        let order = make_test_order("client1", "venue1", "BTCUSD");

        book.insert(order).unwrap();

        let retrieved = book.get_by_venue_id("venue1").unwrap();
        assert_eq!(retrieved.client_order_id, "client1");
    }

    #[test]
    fn test_get_by_symbol() {
        let book = OrderBook::new();
        book.insert(make_test_order("c1", "v1", "BTCUSD")).unwrap();
        book.insert(make_test_order("c2", "v2", "BTCUSD")).unwrap();
        book.insert(make_test_order("c3", "v3", "ETHUSD")).unwrap();

        let btc_orders = book.get_by_symbol("BTCUSD");
        assert_eq!(btc_orders.len(), 2);

        let eth_orders = book.get_by_symbol("ETHUSD");
        assert_eq!(eth_orders.len(), 1);
    }

    #[test]
    fn test_get_open_orders() {
        let book = OrderBook::new();

        let mut order1 = make_test_order("c1", "v1", "BTCUSD");
        order1.status = OrderStatus::New;

        let mut order2 = make_test_order("c2", "v2", "BTCUSD");
        order2.status = OrderStatus::Filled;

        book.insert(order1).unwrap();
        book.insert(order2).unwrap();

        let open = book.get_open_orders(None);
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].client_order_id, "c1");
    }
}
