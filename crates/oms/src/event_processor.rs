//! Event processor for handling UserEvent streams from exchange adapters
//!
//! Processes order updates, fills, and position changes from WebSocket feeds
//! and updates the order book state accordingly.

use crate::{now_ms, order_book::OrderBook, Exchange, Result};
use adapters::traits::{Fill, Order, OrderStatus, Position, UserEvent};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info, warn};

#[cfg(feature = "persistence")]
use crate::persistence_handle::PersistenceHandle;

/// Event processor that consumes UserEvent streams and updates order state
pub struct EventProcessor {
    /// Reference to the order book
    order_book: Arc<OrderBook>,

    /// Broadcast channel for fill events (strategies can subscribe)
    fill_tx: broadcast::Sender<Fill>,

    /// Broadcast channel for position updates
    position_tx: broadcast::Sender<Position>,

    /// Optional persistence handle for database writes
    #[cfg(feature = "persistence")]
    persistence: Option<PersistenceHandle>,
}

impl EventProcessor {
    /// Creates a new event processor
    pub fn new(order_book: Arc<OrderBook>) -> Self {
        let (fill_tx, _) = broadcast::channel(1000);
        let (position_tx, _) = broadcast::channel(100);

        Self {
            order_book,
            fill_tx,
            position_tx,
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

    /// Gets a clone of the persistence handle if configured
    #[cfg(feature = "persistence")]
    pub fn persistence(&self) -> Option<PersistenceHandle> {
        self.persistence.clone()
    }

    /// Subscribes to fill events
    pub fn subscribe_fills(&self) -> broadcast::Receiver<Fill> {
        self.fill_tx.subscribe()
    }

    /// Subscribes to position updates
    pub fn subscribe_positions(&self) -> broadcast::Receiver<Position> {
        self.position_tx.subscribe()
    }

    /// Starts processing events from an adapter's user stream
    ///
    /// Spawns a background task that processes events until the stream closes
    pub fn start_processing(
        &self,
        exchange: Exchange,
        mut user_stream: mpsc::Receiver<UserEvent>,
    ) {
        let order_book = self.order_book.clone();
        let fill_tx = self.fill_tx.clone();
        let position_tx = self.position_tx.clone();
        #[cfg(feature = "persistence")]
        let persistence = self.persistence.clone();

        tokio::spawn(async move {
            info!(%exchange, "Starting event processor for exchange");

            while let Some(event) = user_stream.recv().await {
                #[cfg(feature = "persistence")]
                let result = Self::process_event(
                    exchange,
                    event,
                    &order_book,
                    &fill_tx,
                    &position_tx,
                    persistence.as_ref(),
                )
                .await;

                #[cfg(not(feature = "persistence"))]
                let result = Self::process_event(
                    exchange,
                    event,
                    &order_book,
                    &fill_tx,
                    &position_tx,
                )
                .await;

                if let Err(e) = result {
                    error!(%exchange, error = %e, "Failed to process event");
                }
            }

            warn!(%exchange, "User event stream ended");
        });
    }

    /// Processes a single user event
    #[cfg(feature = "persistence")]
    async fn process_event(
        exchange: Exchange,
        event: UserEvent,
        order_book: &OrderBook,
        fill_tx: &broadcast::Sender<Fill>,
        position_tx: &broadcast::Sender<Position>,
        persistence: Option<&PersistenceHandle>,
    ) -> Result<()> {
        match event {
            UserEvent::OrderUpdate(order) => {
                Self::handle_order_update(exchange, order, order_book, persistence).await?;
            }
            UserEvent::Fill(fill) => {
                Self::handle_fill(exchange, fill, order_book, fill_tx, persistence).await?;
            }
            UserEvent::Position(position) => {
                Self::handle_position(exchange, position, position_tx, persistence).await?;
            }
            UserEvent::Balance {
                asset,
                free,
                locked,
                ex_ts_ms: _,
                recv_ms: _,
            } => {
                debug!(
                    %exchange,
                    %asset,
                    free,
                    locked,
                    "Balance update received"
                );
            }
            UserEvent::Funding {
                symbol,
                rate,
                ex_ts_ms: _,
                recv_ms: _,
            } => {
                debug!(%exchange, %symbol, rate, "Funding update received");
            }
            UserEvent::Liquidation {
                symbol,
                qty,
                px,
                side,
                ex_ts_ms: _,
                recv_ms: _,
            } => {
                warn!(
                    %exchange,
                    %symbol,
                    qty,
                    px,
                    ?side,
                    "Liquidation event received"
                );
            }
        }

        Ok(())
    }

    /// Processes a single user event (non-persistence version)
    #[cfg(not(feature = "persistence"))]
    async fn process_event(
        exchange: Exchange,
        event: UserEvent,
        order_book: &OrderBook,
        fill_tx: &broadcast::Sender<Fill>,
        position_tx: &broadcast::Sender<Position>,
    ) -> Result<()> {
        match event {
            UserEvent::OrderUpdate(order) => {
                Self::handle_order_update(exchange, order, order_book).await?;
            }
            UserEvent::Fill(fill) => {
                Self::handle_fill(exchange, fill, order_book, fill_tx).await?;
            }
            UserEvent::Position(position) => {
                Self::handle_position(exchange, position, position_tx).await?;
            }
            UserEvent::Balance {
                asset,
                free,
                locked,
                ex_ts_ms: _,
                recv_ms: _,
            } => {
                debug!(
                    %exchange,
                    %asset,
                    free,
                    locked,
                    "Balance update received"
                );
            }
            UserEvent::Funding {
                symbol,
                rate,
                ex_ts_ms: _,
                recv_ms: _,
            } => {
                debug!(%exchange, %symbol, rate, "Funding update received");
            }
            UserEvent::Liquidation {
                symbol,
                qty,
                px,
                side,
                ex_ts_ms: _,
                recv_ms: _,
            } => {
                warn!(
                    %exchange,
                    %symbol,
                    qty,
                    px,
                    ?side,
                    "Liquidation event received"
                );
            }
        }

        Ok(())
    }

    /// Handles order update events (with persistence)
    #[cfg(feature = "persistence")]
    async fn handle_order_update(
        exchange: Exchange,
        mut order: Order,
        order_book: &OrderBook,
        persistence: Option<&PersistenceHandle>,
    ) -> Result<()> {
        debug!(
            %exchange,
            client_order_id = %order.client_order_id,
            venue_order_id = %order.venue_order_id,
            status = ?order.status,
            filled_qty = order.filled_qty,
            "Processing order update"
        );

        // Update recv_ms to track when we received this update
        order.recv_ms = now_ms();

        // Check if order exists in our book
        if let Some(_existing) = order_book.get_by_client_id(&order.client_order_id) {
            // Update existing order
            order_book.update(order.clone())?;
        } else if let Some(existing) = order_book.get_by_venue_id(&order.venue_order_id) {
            // Order found by venue ID - update client ID mapping
            order.client_order_id = existing.client_order_id.clone();
            order_book.update(order.clone())?;
        } else {
            // New order from exchange (possibly from reconnection)
            warn!(
                %exchange,
                client_order_id = %order.client_order_id,
                venue_order_id = %order.venue_order_id,
                "Received order update for unknown order - inserting"
            );
            order_book.insert(order.clone())?;
        }

        // Persist order status update (fire-and-forget)
        if let Some(p) = persistence {
            let db_exchange: persistence::DbExchange = exchange.into();
            let db_status: persistence::DbOrderStatus = order.status.into();
            p.update_order_status(
                db_exchange,
                &order.client_order_id,
                db_status,
                Some(&order.venue_order_id),
            );
        }

        Ok(())
    }

    /// Handles order update events (without persistence)
    #[cfg(not(feature = "persistence"))]
    async fn handle_order_update(
        exchange: Exchange,
        mut order: Order,
        order_book: &OrderBook,
    ) -> Result<()> {
        debug!(
            %exchange,
            client_order_id = %order.client_order_id,
            venue_order_id = %order.venue_order_id,
            status = ?order.status,
            filled_qty = order.filled_qty,
            "Processing order update"
        );

        // Update recv_ms to track when we received this update
        order.recv_ms = now_ms();

        // Check if order exists in our book
        if let Some(_existing) = order_book.get_by_client_id(&order.client_order_id) {
            // Update existing order
            order_book.update(order)?;
        } else if let Some(existing) = order_book.get_by_venue_id(&order.venue_order_id) {
            // Order found by venue ID - update client ID mapping
            order.client_order_id = existing.client_order_id.clone();
            order_book.update(order)?;
        } else {
            // New order from exchange (possibly from reconnection)
            warn!(
                %exchange,
                client_order_id = %order.client_order_id,
                venue_order_id = %order.venue_order_id,
                "Received order update for unknown order - inserting"
            );
            order_book.insert(order)?;
        }

        Ok(())
    }

    /// Handles fill events (with persistence)
    #[cfg(feature = "persistence")]
    async fn handle_fill(
        exchange: Exchange,
        fill: Fill,
        order_book: &OrderBook,
        fill_tx: &broadcast::Sender<Fill>,
        persistence: Option<&PersistenceHandle>,
    ) -> Result<()> {
        debug!(
            %exchange,
            venue_order_id = %fill.venue_order_id,
            client_order_id = %fill.client_order_id,
            qty = fill.qty,
            price = fill.price,
            is_maker = fill.is_maker,
            "Processing fill"
        );

        // Update order filled quantity if order exists
        let order_update = if let Some(mut order) = order_book.get_by_client_id(&fill.client_order_id) {
            let order_side = order.side;
            order.filled_qty += fill.qty;
            order.remaining_qty = order.qty - order.filled_qty;
            order.updated_ms = fill.ex_ts_ms;
            order.recv_ms = now_ms();

            // Update status
            if order.remaining_qty <= 0.0001 {
                // Tolerance for floating point
                order.status = OrderStatus::Filled;
            } else if order.filled_qty > 0.0 {
                order.status = OrderStatus::PartiallyFilled;
            }

            let update = Some((order.filled_qty, order.remaining_qty, order.status, order_side));
            order_book.update(order)?;
            update
        } else {
            warn!(
                %exchange,
                client_order_id = %fill.client_order_id,
                venue_order_id = %fill.venue_order_id,
                "Received fill for unknown order"
            );
            None
        };

        // Persist fill record and order fill update (fire-and-forget)
        if let Some(p) = persistence {
            let db_exchange: persistence::DbExchange = exchange.into();
            // Get side from order if available, otherwise default to Buy
            let db_side: persistence::DbOrderSide = order_update
                .as_ref()
                .map(|(_, _, _, side)| (*side).into())
                .unwrap_or(persistence::DbOrderSide::Buy);

            // Insert fill record
            let db_fill = persistence::NewDbFill {
                order_id: None, // We don't have the UUID, only client_order_id
                venue_order_id: fill.venue_order_id.clone(),
                client_order_id: fill.client_order_id.clone(),
                execution_id: fill.exec_id.clone(),
                exchange: db_exchange,
                symbol: fill.symbol.clone(),
                side: db_side,
                price: rust_decimal::Decimal::try_from(fill.price).unwrap_or_default(),
                quantity: rust_decimal::Decimal::try_from(fill.qty).unwrap_or_default(),
                fee: rust_decimal::Decimal::try_from(fill.fee).unwrap_or_default(),
                fee_currency: Some(fill.fee_ccy.clone()),
                is_maker: fill.is_maker,
                exchange_ts_ms: fill.ex_ts_ms as i64,
                received_ts_ms: fill.recv_ms as i64,
            };
            p.insert_fill(db_fill);

            // Update order with fill info
            if let Some((filled_qty, remaining_qty, status, _side)) = order_update {
                let db_status: persistence::DbOrderStatus = status.into();
                let avg_price = rust_decimal::Decimal::try_from(fill.price).unwrap_or_default();
                p.update_order_fill(
                    db_exchange,
                    &fill.client_order_id,
                    rust_decimal::Decimal::try_from(filled_qty).unwrap_or_default(),
                    rust_decimal::Decimal::try_from(remaining_qty).unwrap_or_default(),
                    avg_price,
                    db_status,
                );
            }
        }

        // Broadcast fill to subscribers (strategies, inventory manager, etc.)
        if let Err(e) = fill_tx.send(fill) {
            warn!(
                %exchange,
                receivers = fill_tx.receiver_count(),
                "Failed to broadcast fill event (no receivers or lagging): {}",
                e
            );
        }

        Ok(())
    }

    /// Handles fill events (without persistence)
    #[cfg(not(feature = "persistence"))]
    async fn handle_fill(
        exchange: Exchange,
        fill: Fill,
        order_book: &OrderBook,
        fill_tx: &broadcast::Sender<Fill>,
    ) -> Result<()> {
        debug!(
            %exchange,
            venue_order_id = %fill.venue_order_id,
            client_order_id = %fill.client_order_id,
            qty = fill.qty,
            price = fill.price,
            is_maker = fill.is_maker,
            "Processing fill"
        );

        // Update order filled quantity if order exists
        if let Some(mut order) = order_book.get_by_client_id(&fill.client_order_id) {
            order.filled_qty += fill.qty;
            order.remaining_qty = order.qty - order.filled_qty;
            order.updated_ms = fill.ex_ts_ms;
            order.recv_ms = now_ms();

            // Update status
            if order.remaining_qty <= 0.0001 {
                // Tolerance for floating point
                order.status = OrderStatus::Filled;
            } else if order.filled_qty > 0.0 {
                order.status = OrderStatus::PartiallyFilled;
            }

            order_book.update(order)?;
        } else {
            warn!(
                %exchange,
                client_order_id = %fill.client_order_id,
                venue_order_id = %fill.venue_order_id,
                "Received fill for unknown order"
            );
        }

        // Broadcast fill to subscribers (strategies, inventory manager, etc.)
        if let Err(e) = fill_tx.send(fill) {
            warn!(
                %exchange,
                receivers = fill_tx.receiver_count(),
                "Failed to broadcast fill event (no receivers or lagging): {}",
                e
            );
        }

        Ok(())
    }

    /// Handles position update events (with persistence)
    #[cfg(feature = "persistence")]
    async fn handle_position(
        exchange: Exchange,
        position: Position,
        position_tx: &broadcast::Sender<Position>,
        persistence: Option<&PersistenceHandle>,
    ) -> Result<()> {
        debug!(
            %exchange,
            symbol = %position.symbol,
            qty = position.qty,
            entry_px = position.entry_px,
            unrealized_pnl = ?position.unrealized_pnl,
            "Processing position update"
        );

        // Persist position update (fire-and-forget)
        if let Some(p) = persistence {
            let db_exchange: persistence::DbExchange = exchange.into();
            let db_position = persistence::NewDbPosition {
                exchange: db_exchange,
                symbol: position.symbol.clone(),
                quantity: rust_decimal::Decimal::try_from(position.qty).unwrap_or_default(),
                entry_price: rust_decimal::Decimal::try_from(position.entry_px).unwrap_or_default(),
                mark_price: position.mark_px.map(|p| rust_decimal::Decimal::try_from(p).unwrap_or_default()),
                liquidation_price: position.liquidation_px.map(|p| rust_decimal::Decimal::try_from(p).unwrap_or_default()),
                margin: position.margin.map(|m| rust_decimal::Decimal::try_from(m).unwrap_or_default()),
                leverage: position.leverage.map(|l| l as i32),
            };
            p.upsert_position(db_position);
        }

        // Broadcast to subscribers (inventory manager will pick this up)
        if let Err(e) = position_tx.send(position) {
            warn!(
                %exchange,
                receivers = position_tx.receiver_count(),
                "Failed to broadcast position event (no receivers or lagging): {}",
                e
            );
        }

        Ok(())
    }

    /// Handles position update events (without persistence)
    #[cfg(not(feature = "persistence"))]
    async fn handle_position(
        exchange: Exchange,
        position: Position,
        position_tx: &broadcast::Sender<Position>,
    ) -> Result<()> {
        debug!(
            %exchange,
            symbol = %position.symbol,
            qty = position.qty,
            entry_px = position.entry_px,
            unrealized_pnl = ?position.unrealized_pnl,
            "Processing position update"
        );

        // Broadcast to subscribers (inventory manager will pick this up)
        if let Err(e) = position_tx.send(position) {
            warn!(
                %exchange,
                receivers = position_tx.receiver_count(),
                "Failed to broadcast position event (no receivers or lagging): {}",
                e
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adapters::traits::{OrderType, Side, TimeInForce};

    #[tokio::test]
    async fn test_order_update_processing() {
        let order_book = Arc::new(OrderBook::new());
        let processor = EventProcessor::new(order_book.clone());

        // Insert initial order
        let order = Order {
            client_order_id: "test1".to_string(),
            venue_order_id: "venue1".to_string(),
            symbol: "BTCUSD".to_string(),
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
        };

        order_book.insert(order.clone()).unwrap();

        // Process fill
        let fill = Fill {
            venue_order_id: "venue1".to_string(),
            client_order_id: "test1".to_string(),
            symbol: "BTCUSD".to_string(),
            price: 100.0,
            qty: 0.5,
            fee: 0.1,
            fee_ccy: "USD".to_string(),
            is_maker: true,
            exec_id: "exec1".to_string(),
            ex_ts_ms: now_ms(),
            recv_ms: now_ms(),
        };

        let (fill_tx, _) = broadcast::channel::<Fill>(10);
        let (_pos_tx, _) = broadcast::channel::<Position>(10);

        #[cfg(feature = "persistence")]
        EventProcessor::handle_fill(Exchange::Kraken, fill, &order_book, &fill_tx, None)
            .await
            .unwrap();

        #[cfg(not(feature = "persistence"))]
        EventProcessor::handle_fill(Exchange::Kraken, fill, &order_book, &fill_tx)
            .await
            .unwrap();

        // Verify order was updated
        let updated = order_book.get_by_client_id("test1").unwrap();
        assert_eq!(updated.filled_qty, 0.5);
        assert_eq!(updated.remaining_qty, 0.5);
        assert_eq!(updated.status, OrderStatus::PartiallyFilled);
    }
}
