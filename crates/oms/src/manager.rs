//! Order Manager - Main API for the OMS
//!
//! Ties together the order book, event processor, and order router
//! to provide a unified interface for order management.

use crate::{
    event_processor::EventProcessor, order_book::OrderBook, order_router::OrderRouter, Exchange,
    Result,
};
use adapters::traits::{Fill, NewOrder, Order, Position, UserEvent};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, RwLock};
use tracing::info;

#[cfg(feature = "persistence")]
use crate::persistence_handle::PersistenceHandle;

/// Main Order Manager that coordinates all OMS components
pub struct OrderManager {
    /// Order book for state tracking
    order_book: Arc<OrderBook>,

    /// Event processor for handling exchange events
    event_processor: Arc<EventProcessor>,

    /// Order router for submitting orders
    router: Arc<RwLock<OrderRouter>>,

    /// Optional persistence handle for database writes
    #[cfg(feature = "persistence")]
    persistence: Option<PersistenceHandle>,
}

impl OrderManager {
    /// Creates a new OrderManager
    pub fn new() -> Self {
        let order_book = Arc::new(OrderBook::new());
        let event_processor = Arc::new(EventProcessor::new(order_book.clone()));
        let router = Arc::new(RwLock::new(OrderRouter::new(order_book.clone())));

        let manager = Self {
            order_book,
            event_processor,
            router,
            #[cfg(feature = "persistence")]
            persistence: None,
        };

        // Start periodic cleanup task
        manager.start_cleanup_task();

        manager
    }

    /// Creates a new OrderManager with persistence support
    #[cfg(feature = "persistence")]
    pub fn with_persistence(persistence: PersistenceHandle) -> Self {
        let order_book = Arc::new(OrderBook::new());
        let event_processor = Arc::new(
            EventProcessor::new(order_book.clone()).with_persistence(persistence.clone()),
        );
        let router = Arc::new(RwLock::new(
            OrderRouter::new(order_book.clone()).with_persistence(persistence.clone()),
        ));

        let manager = Self {
            order_book,
            event_processor,
            router,
            persistence: Some(persistence),
        };

        // Start periodic cleanup task
        manager.start_cleanup_task();

        manager
    }

    /// Gets a clone of the persistence handle if configured
    #[cfg(feature = "persistence")]
    pub fn persistence(&self) -> Option<PersistenceHandle> {
        self.persistence.clone()
    }

    /// Starts a background task to periodically clean up old orders
    fn start_cleanup_task(&self) {
        let order_book = self.order_book.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(3600)); // Every hour

            loop {
                interval.tick().await;

                let removed = order_book.cleanup_old_orders();
                if removed > 0 {
                    info!(removed, "Periodic cleanup removed old orders");
                }
            }
        });
    }

    /// Registers an exchange adapter and starts processing its user event stream
    ///
    /// # Arguments
    /// * `exchange` - The exchange identifier
    /// * `adapter` - The exchange adapter implementing SpotRest
    /// * `user_stream` - Channel receiving UserEvent updates from the adapter
    pub async fn register_exchange<T>(
        &self,
        exchange: Exchange,
        adapter: Arc<T>,
        user_stream: mpsc::Receiver<UserEvent>,
    ) where
        T: crate::order_router::ExchangeAdapter + 'static,
    {
        info!(%exchange, "Registering exchange with OMS");

        // Register adapter with router
        self.router.write().await.register_adapter(exchange, adapter);

        // Start processing events
        self.event_processor.start_processing(exchange, user_stream);
    }

    // =========================================================================
    // Order Operations
    // =========================================================================

    /// Submits a new order to an exchange
    ///
    /// Returns the client_order_id for tracking
    pub async fn submit_order(&self, exchange: Exchange, order: NewOrder) -> Result<String> {
        let router = self.router.read().await;
        router.submit_order(exchange, order).await
    }

    /// Cancels an order by client_order_id
    pub async fn cancel_order(&self, exchange: Exchange, client_order_id: &str) -> Result<bool> {
        let router = self.router.read().await;
        router.cancel_order(exchange, client_order_id).await
    }

    /// Cancels all orders on an exchange
    ///
    /// Optionally filtered by symbol
    pub async fn cancel_all_orders(
        &self,
        exchange: Exchange,
        symbol: Option<&str>,
    ) -> Result<usize> {
        let router = self.router.read().await;
        router.cancel_all_orders(exchange, symbol).await
    }

    // =========================================================================
    // Query Interface
    // =========================================================================

    /// Gets an order by client_order_id
    pub fn get_order(&self, client_order_id: &str) -> Option<Order> {
        self.order_book.get_by_client_id(client_order_id)
    }

    /// Gets an order by venue_order_id (exchange-assigned ID)
    pub fn get_order_by_venue_id(&self, venue_order_id: &str) -> Option<Order> {
        self.order_book.get_by_venue_id(venue_order_id)
    }

    /// Gets all orders for a symbol
    pub fn get_orders_by_symbol(&self, symbol: &str) -> Vec<Order> {
        self.order_book.get_by_symbol(symbol)
    }

    /// Gets all open orders (New or PartiallyFilled)
    ///
    /// Optionally filtered by symbol
    pub fn get_open_orders(&self, symbol: Option<&str>) -> Vec<Order> {
        self.order_book.get_open_orders(symbol)
    }

    /// Gets all orders
    ///
    /// Optionally filtered by symbol
    pub fn get_all_orders(&self, symbol: Option<&str>) -> Vec<Order> {
        self.order_book.get_all_orders(symbol)
    }

    /// Gets order book statistics
    pub fn get_order_stats(&self) -> crate::order_book::OrderBookStats {
        self.order_book.get_stats()
    }

    // =========================================================================
    // Event Subscriptions
    // =========================================================================

    /// Subscribes to fill events
    ///
    /// Strategies can use this to track executions in real-time
    pub fn subscribe_fills(&self) -> broadcast::Receiver<Fill> {
        self.event_processor.subscribe_fills()
    }

    /// Subscribes to position updates
    ///
    /// Inventory manager uses this to track positions across exchanges
    pub fn subscribe_positions(&self) -> broadcast::Receiver<Position> {
        self.event_processor.subscribe_positions()
    }

    // =========================================================================
    // Maintenance Operations
    // =========================================================================

    /// Removes an order from the book (for cleanup of old filled/canceled orders)
    pub fn remove_order(&self, client_order_id: &str) -> Option<Order> {
        self.order_book.remove(client_order_id)
    }

    /// Gets the number of orders currently tracked
    pub fn order_count(&self) -> usize {
        self.order_book.len()
    }
}

impl Default for OrderManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adapters::traits::{OrderStatus, OrderType, Side, TimeInForce};
    use crate::order_router::ExchangeAdapter;

    struct MockAdapter;

    #[async_trait::async_trait]
    impl ExchangeAdapter for MockAdapter {
        async fn create_order(&self, order: NewOrder) -> anyhow::Result<Order> {
            Ok(Order {
                client_order_id: order.client_order_id,
                venue_order_id: format!("venue_{}", uuid::Uuid::new_v4().simple()),
                symbol: order.symbol,
                ord_type: order.ord_type,
                side: order.side,
                qty: order.qty,
                price: order.price,
                stop_price: order.stop_price,
                tif: order.tif,
                status: OrderStatus::New,
                filled_qty: 0.0,
                remaining_qty: order.qty,
                created_ms: crate::now_ms(),
                updated_ms: crate::now_ms(),
                recv_ms: crate::now_ms(),
                raw_status: None,
            })
        }

        async fn cancel_order(
            &self,
            _symbol: &str,
            _venue_order_id: &str,
        ) -> anyhow::Result<bool> {
            Ok(true)
        }

        async fn cancel_all(&self, _symbol: Option<&str>) -> anyhow::Result<usize> {
            Ok(0)
        }

        async fn get_order(
            &self,
            _symbol: &str,
            _venue_order_id: &str,
        ) -> anyhow::Result<Order> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn test_order_manager_workflow() {
        let manager = OrderManager::new();

        // Register mock adapter
        let (tx, rx) = mpsc::channel(100);
        manager
            .register_exchange(Exchange::Kraken, Arc::new(MockAdapter), rx)
            .await;

        // Submit an order
        let new_order = NewOrder {
            symbol: "BTCUSD".to_string(),
            side: Side::Buy,
            ord_type: OrderType::Limit,
            qty: 1.0,
            price: Some(50000.0),
            stop_price: None,
            tif: Some(TimeInForce::Gtc),
            post_only: false,
            reduce_only: false,
            client_order_id: String::new(),
        };

        let client_id = manager
            .submit_order(Exchange::Kraken, new_order)
            .await
            .unwrap();

        // Query the order
        let order = manager.get_order(&client_id).unwrap();
        assert_eq!(order.symbol, "BTCUSD");
        assert_eq!(order.qty, 1.0);
        assert_eq!(order.status, OrderStatus::New);

        // Check open orders
        let open = manager.get_open_orders(None);
        assert_eq!(open.len(), 1);

        // Cancel the order
        let canceled = manager
            .cancel_order(Exchange::Kraken, &client_id)
            .await
            .unwrap();
        assert!(canceled);
    }

    #[tokio::test]
    async fn test_event_subscriptions() {
        let manager = OrderManager::new();

        // Subscribe to fills
        let mut fill_rx = manager.subscribe_fills();

        // Subscribe to positions
        let mut pos_rx = manager.subscribe_positions();

        // Subscriptions should be ready
        // (actual events would come from exchange adapters)
    }
}
