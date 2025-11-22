//! Order router for submitting orders to exchange adapters
//!
//! Routes orders to the appropriate exchange adapter and handles:
//! - Client order ID generation
//! - Order validation
//! - Adapter selection
//! - Error handling and retries

use crate::{order_book::OrderBook, Exchange, OmsError, Result};
use adapters::traits::{NewOrder, Order, SpotRest};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info};
use uuid::Uuid;

#[cfg(feature = "persistence")]
use crate::persistence_handle::PersistenceHandle;

/// Trait for exchange adapters to enable dynamic dispatch
#[async_trait::async_trait]
pub trait ExchangeAdapter: Send + Sync {
    async fn create_order(&self, order: NewOrder) -> anyhow::Result<Order>;
    async fn cancel_order(&self, symbol: &str, venue_order_id: &str) -> anyhow::Result<bool>;
    async fn cancel_all(&self, symbol: Option<&str>) -> anyhow::Result<usize>;
    async fn get_order(&self, symbol: &str, venue_order_id: &str) -> anyhow::Result<Order>;
}

/// Implementation for any type that implements SpotRest
#[async_trait::async_trait]
impl<T: SpotRest + Send + Sync> ExchangeAdapter for T {
    async fn create_order(&self, order: NewOrder) -> anyhow::Result<Order> {
        SpotRest::create_order(self, order).await
    }

    async fn cancel_order(&self, symbol: &str, venue_order_id: &str) -> anyhow::Result<bool> {
        SpotRest::cancel_order(self, symbol, venue_order_id).await
    }

    async fn cancel_all(&self, symbol: Option<&str>) -> anyhow::Result<usize> {
        SpotRest::cancel_all(self, symbol).await
    }

    async fn get_order(&self, symbol: &str, venue_order_id: &str) -> anyhow::Result<Order> {
        SpotRest::get_order(self, symbol, venue_order_id).await
    }
}

/// Order router that manages order submission to exchanges
pub struct OrderRouter {
    /// Exchange adapters keyed by exchange
    adapters: HashMap<Exchange, Arc<dyn ExchangeAdapter>>,

    /// Reference to order book for state tracking
    order_book: Arc<OrderBook>,

    /// Optional persistence handle for database writes
    #[cfg(feature = "persistence")]
    persistence: Option<PersistenceHandle>,
}

impl OrderRouter {
    /// Creates a new order router
    pub fn new(order_book: Arc<OrderBook>) -> Self {
        Self {
            adapters: HashMap::new(),
            order_book,
            #[cfg(feature = "persistence")]
            persistence: None,
        }
    }

    /// Sets the persistence handle for database writes
    #[cfg(feature = "persistence")]
    pub fn with_persistence(mut self, persistence: PersistenceHandle) -> Self {
        self.persistence = Some(persistence);
        self
    }

    /// Registers an exchange adapter
    pub fn register_adapter(&mut self, exchange: Exchange, adapter: Arc<dyn ExchangeAdapter>) {
        info!(%exchange, "Registering exchange adapter");
        self.adapters.insert(exchange, adapter);
    }

    /// Submits a new order to the specified exchange
    ///
    /// Generates a client_order_id if not provided and tracks the order in the book
    pub async fn submit_order(&self, exchange: Exchange, mut order: NewOrder) -> Result<String> {
        // Get adapter for this exchange
        let adapter = self
            .adapters
            .get(&exchange)
            .ok_or(OmsError::AdapterNotConfigured(exchange))?;

        // Generate client_order_id if not provided
        if order.client_order_id.is_empty() {
            order.client_order_id = generate_client_order_id();
        }

        let client_id = order.client_order_id.clone();

        debug!(
            %exchange,
            client_order_id = %client_id,
            symbol = %order.symbol,
            side = ?order.side,
            qty = order.qty,
            price = ?order.price,
            "Submitting order"
        );

        // Persist order to database before submitting (fire-and-forget)
        #[cfg(feature = "persistence")]
        if let Some(ref p) = self.persistence {
            let db_exchange: persistence::DbExchange = exchange.into();
            let db_side: persistence::DbOrderSide = order.side.into();
            let db_type: persistence::DbOrderType = order.ord_type.into();
            let db_tif: Option<persistence::DbTimeInForce> = order.tif.map(Into::into);

            let db_order = persistence::NewDbOrder {
                client_order_id: order.client_order_id.clone(),
                venue_order_id: None, // Not yet assigned
                exchange: db_exchange,
                symbol: order.symbol.clone(),
                side: db_side,
                order_type: db_type,
                time_in_force: db_tif,
                quantity: rust_decimal::Decimal::try_from(order.qty).unwrap_or_default(),
                price: order.price.map(|p| rust_decimal::Decimal::try_from(p).unwrap_or_default()),
                stop_price: order.stop_price.map(|p| rust_decimal::Decimal::try_from(p).unwrap_or_default()),
                post_only: order.post_only,
                reduce_only: order.reduce_only,
                strategy_id: None, // TODO: pass strategy_id through NewOrder
            };
            p.insert_order(db_order);
        }

        // Submit to exchange
        let result_order = match adapter.create_order(order.clone()).await {
            Ok(o) => o,
            Err(e) => {
                error!(
                    %exchange,
                    client_order_id = %client_id,
                    error = %e,
                    "Order submission failed"
                );

                // Persist rejection status (fire-and-forget)
                #[cfg(feature = "persistence")]
                if let Some(ref p) = self.persistence {
                    let db_exchange: persistence::DbExchange = exchange.into();
                    p.update_order_status(
                        db_exchange,
                        &client_id,
                        persistence::DbOrderStatus::Rejected,
                        None,
                    );
                }

                return Err(OmsError::ExchangeError(e.to_string()));
            }
        };

        // Insert into order book and capture venue_order_id before move
        let venue_order_id = result_order.venue_order_id.clone();
        self.order_book.insert(result_order)?;

        // Update order with venue_order_id (fire-and-forget)
        #[cfg(feature = "persistence")]
        if let Some(ref p) = self.persistence {
            let db_exchange: persistence::DbExchange = exchange.into();
            p.update_order_status(
                db_exchange,
                &client_id,
                persistence::DbOrderStatus::New,
                Some(&venue_order_id),
            );
        }

        info!(
            %exchange,
            client_order_id = %client_id,
            venue_order_id = %venue_order_id,
            "Order submitted successfully"
        );

        Ok(client_id)
    }

    /// Cancels an order by client_order_id
    pub async fn cancel_order(&self, exchange: Exchange, client_order_id: &str) -> Result<bool> {
        let adapter = self
            .adapters
            .get(&exchange)
            .ok_or(OmsError::AdapterNotConfigured(exchange))?;

        // Get order from book to get venue_order_id
        let order = self
            .order_book
            .get_by_client_id(client_order_id)
            .ok_or_else(|| OmsError::OrderNotFound(client_order_id.to_string()))?;

        debug!(
            %exchange,
            client_order_id,
            venue_order_id = %order.venue_order_id,
            symbol = %order.symbol,
            "Canceling order"
        );

        match adapter
            .cancel_order(&order.symbol, &order.venue_order_id)
            .await
        {
            Ok(success) => {
                if success {
                    // Persist cancel status (fire-and-forget)
                    #[cfg(feature = "persistence")]
                    if let Some(ref p) = self.persistence {
                        let db_exchange: persistence::DbExchange = exchange.into();
                        p.cancel_order(db_exchange, client_order_id);
                    }

                    info!(
                        %exchange,
                        client_order_id,
                        "Order canceled successfully"
                    );
                }
                Ok(success)
            }
            Err(e) => {
                error!(
                    %exchange,
                    client_order_id,
                    error = %e,
                    "Order cancellation failed"
                );
                Err(OmsError::ExchangeError(e.to_string()))
            }
        }
    }

    /// Cancels all orders for a symbol (or all symbols if None)
    pub async fn cancel_all_orders(
        &self,
        exchange: Exchange,
        symbol: Option<&str>,
    ) -> Result<usize> {
        let adapter = self
            .adapters
            .get(&exchange)
            .ok_or(OmsError::AdapterNotConfigured(exchange))?;

        debug!(%exchange, ?symbol, "Canceling all orders");

        match adapter.cancel_all(symbol).await {
            Ok(count) => {
                info!(%exchange, ?symbol, count, "Canceled all orders");
                Ok(count)
            }
            Err(e) => {
                error!(%exchange, ?symbol, error = %e, "Cancel all failed");
                Err(OmsError::ExchangeError(e.to_string()))
            }
        }
    }

    /// Gets the adapter for an exchange (for advanced use cases)
    pub fn get_adapter(&self, exchange: Exchange) -> Option<&Arc<dyn ExchangeAdapter>> {
        self.adapters.get(&exchange)
    }
}

/// Generates a unique client order ID
fn generate_client_order_id() -> String {
    format!("oms_{}", Uuid::new_v4().simple())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::now_ms;
    use adapters::traits::{OrderStatus, OrderType, Side, TimeInForce};

    struct MockAdapter;

    #[async_trait::async_trait]
    impl ExchangeAdapter for MockAdapter {
        async fn create_order(&self, order: NewOrder) -> anyhow::Result<Order> {
            Ok(Order {
                client_order_id: order.client_order_id,
                venue_order_id: "mock_venue_123".to_string(),
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
                created_ms: now_ms(),
                updated_ms: now_ms(),
                recv_ms: now_ms(),
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
            Ok(5)
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
    async fn test_submit_order() {
        let order_book = Arc::new(OrderBook::new());
        let mut router = OrderRouter::new(order_book.clone());

        router.register_adapter(Exchange::Kraken, Arc::new(MockAdapter));

        let new_order = NewOrder {
            symbol: "BTCUSD".to_string(),
            side: Side::Buy,
            ord_type: OrderType::Limit,
            qty: 1.0,
            price: Some(100.0),
            stop_price: None,
            tif: Some(TimeInForce::Gtc),
            post_only: false,
            reduce_only: false,
            client_order_id: String::new(), // Will be generated
        };

        let client_id = router.submit_order(Exchange::Kraken, new_order).await.unwrap();

        // Verify order is in book
        let order = order_book.get_by_client_id(&client_id).unwrap();
        assert_eq!(order.symbol, "BTCUSD");
        assert_eq!(order.status, OrderStatus::New);
    }

    #[tokio::test]
    async fn test_cancel_order() {
        let order_book = Arc::new(OrderBook::new());
        let mut router = OrderRouter::new(order_book.clone());

        router.register_adapter(Exchange::Kraken, Arc::new(MockAdapter));

        // Submit order first
        let new_order = NewOrder {
            symbol: "BTCUSD".to_string(),
            side: Side::Buy,
            ord_type: OrderType::Limit,
            qty: 1.0,
            price: Some(100.0),
            stop_price: None,
            tif: Some(TimeInForce::Gtc),
            post_only: false,
            reduce_only: false,
            client_order_id: "test123".to_string(),
        };

        router.submit_order(Exchange::Kraken, new_order).await.unwrap();

        // Cancel it
        let success = router
            .cancel_order(Exchange::Kraken, "test123")
            .await
            .unwrap();

        assert!(success);
    }
}
