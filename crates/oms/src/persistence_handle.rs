//! Persistence handle for database integration
//!
//! Provides fire-and-forget database writes that don't block trading operations.

use crate::Exchange;
use persistence::{
    Database, DbExchange, FillRepository, OrderRepository, PositionRepository, RiskEventRepository,
};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, warn};

// ============================================================================
// Type Conversions from OMS Exchange type to persistence type
// ============================================================================

impl From<Exchange> for DbExchange {
    fn from(e: Exchange) -> Self {
        match e {
            Exchange::Kraken => DbExchange::Kraken,
            Exchange::Mexc => DbExchange::Mexc,
            Exchange::Bybit => DbExchange::Bybit,
            Exchange::Kalshi => DbExchange::Kalshi,
        }
    }
}

impl From<DbExchange> for Exchange {
    fn from(e: DbExchange) -> Self {
        match e {
            DbExchange::Kraken => Exchange::Kraken,
            DbExchange::Mexc => Exchange::Mexc,
            DbExchange::Bybit => Exchange::Bybit,
            DbExchange::Kalshi => Exchange::Kalshi,
        }
    }
}

/// Commands for the persistence worker
#[derive(Debug)]
pub enum PersistenceCommand {
    /// Insert a new order
    InsertOrder(persistence::NewDbOrder),
    /// Update order status
    UpdateOrderStatus {
        exchange: persistence::DbExchange,
        client_order_id: String,
        status: persistence::DbOrderStatus,
        venue_order_id: Option<String>,
    },
    /// Update order with fill information
    UpdateOrderFill {
        exchange: persistence::DbExchange,
        client_order_id: String,
        filled_quantity: rust_decimal::Decimal,
        remaining_quantity: rust_decimal::Decimal,
        average_fill_price: rust_decimal::Decimal,
        status: persistence::DbOrderStatus,
    },
    /// Insert a fill record
    InsertFill(persistence::NewDbFill),
    /// Upsert a position
    UpsertPosition(persistence::NewDbPosition),
    /// Cancel an order
    CancelOrder {
        exchange: persistence::DbExchange,
        client_order_id: String,
    },
    /// Insert a risk event
    InsertRiskEvent(persistence::NewDbRiskEvent),
}

/// Handle for sending persistence commands
///
/// Uses a background worker to perform database writes asynchronously,
/// ensuring trading operations are never blocked by database latency.
#[derive(Clone)]
pub struct PersistenceHandle {
    tx: mpsc::Sender<PersistenceCommand>,
}

impl PersistenceHandle {
    /// Creates a new persistence handle with a background worker
    ///
    /// # Arguments
    /// * `database` - The database connection
    /// * `buffer_size` - Size of the command buffer (default: 10000)
    pub fn new(database: Arc<Database>, buffer_size: usize) -> Self {
        let (tx, rx) = mpsc::channel(buffer_size);

        // Spawn the background worker
        let worker = PersistenceWorker::new(database, rx);
        tokio::spawn(worker.run());

        Self { tx }
    }

    /// Creates a persistence handle with default buffer size
    pub fn with_database(database: Arc<Database>) -> Self {
        Self::new(database, 10000)
    }

    /// Sends a command to the persistence worker (fire-and-forget)
    ///
    /// Returns immediately. If the buffer is full, the command is dropped
    /// and a warning is logged.
    pub fn send(&self, cmd: PersistenceCommand) {
        if let Err(e) = self.tx.try_send(cmd) {
            warn!("Persistence buffer full, dropping command: {:?}", e);
        }
    }

    /// Inserts a new order (fire-and-forget)
    pub fn insert_order(&self, order: persistence::NewDbOrder) {
        debug!(client_order_id = %order.client_order_id, "Queueing order insert");
        self.send(PersistenceCommand::InsertOrder(order));
    }

    /// Updates order status (fire-and-forget)
    pub fn update_order_status(
        &self,
        exchange: persistence::DbExchange,
        client_order_id: &str,
        status: persistence::DbOrderStatus,
        venue_order_id: Option<&str>,
    ) {
        debug!(%client_order_id, ?status, "Queueing order status update");
        self.send(PersistenceCommand::UpdateOrderStatus {
            exchange,
            client_order_id: client_order_id.to_string(),
            status,
            venue_order_id: venue_order_id.map(String::from),
        });
    }

    /// Updates order with fill information (fire-and-forget)
    pub fn update_order_fill(
        &self,
        exchange: persistence::DbExchange,
        client_order_id: &str,
        filled_quantity: rust_decimal::Decimal,
        remaining_quantity: rust_decimal::Decimal,
        average_fill_price: rust_decimal::Decimal,
        status: persistence::DbOrderStatus,
    ) {
        debug!(%client_order_id, %filled_quantity, "Queueing order fill update");
        self.send(PersistenceCommand::UpdateOrderFill {
            exchange,
            client_order_id: client_order_id.to_string(),
            filled_quantity,
            remaining_quantity,
            average_fill_price,
            status,
        });
    }

    /// Inserts a fill record (fire-and-forget)
    pub fn insert_fill(&self, fill: persistence::NewDbFill) {
        debug!(execution_id = %fill.execution_id, "Queueing fill insert");
        self.send(PersistenceCommand::InsertFill(fill));
    }

    /// Upserts a position (fire-and-forget)
    pub fn upsert_position(&self, position: persistence::NewDbPosition) {
        debug!(symbol = %position.symbol, "Queueing position upsert");
        self.send(PersistenceCommand::UpsertPosition(position));
    }

    /// Cancels an order (fire-and-forget)
    pub fn cancel_order(&self, exchange: persistence::DbExchange, client_order_id: &str) {
        debug!(%client_order_id, "Queueing order cancel");
        self.send(PersistenceCommand::CancelOrder {
            exchange,
            client_order_id: client_order_id.to_string(),
        });
    }

    /// Inserts a risk event (fire-and-forget)
    pub fn insert_risk_event(&self, event: persistence::NewDbRiskEvent) {
        debug!(event_type = %event.event_type, "Queueing risk event insert");
        self.send(PersistenceCommand::InsertRiskEvent(event));
    }
}

/// Background worker that processes persistence commands
struct PersistenceWorker {
    order_repo: OrderRepository,
    fill_repo: FillRepository,
    position_repo: PositionRepository,
    risk_repo: RiskEventRepository,
    rx: mpsc::Receiver<PersistenceCommand>,
}

impl PersistenceWorker {
    fn new(database: Arc<Database>, rx: mpsc::Receiver<PersistenceCommand>) -> Self {
        let pool = database.pool().clone();
        Self {
            order_repo: OrderRepository::new(pool.clone()),
            fill_repo: FillRepository::new(pool.clone()),
            position_repo: PositionRepository::new(pool.clone()),
            risk_repo: RiskEventRepository::new(pool),
            rx,
        }
    }

    async fn run(mut self) {
        tracing::info!("Persistence worker started");

        while let Some(cmd) = self.rx.recv().await {
            if let Err(e) = self.process_command(cmd).await {
                error!("Persistence error (non-fatal): {}", e);
            }
        }

        tracing::info!("Persistence worker stopped");
    }

    async fn process_command(&self, cmd: PersistenceCommand) -> persistence::Result<()> {
        match cmd {
            PersistenceCommand::InsertOrder(order) => {
                self.order_repo.insert(&order).await?;
            }
            PersistenceCommand::UpdateOrderStatus {
                exchange,
                client_order_id,
                status,
                venue_order_id,
            } => {
                if let Some(db_order) = self
                    .order_repo
                    .get_by_client_id(exchange, &client_order_id)
                    .await?
                {
                    self.order_repo
                        .update_status(db_order.id, status, venue_order_id.as_deref())
                        .await?;
                }
            }
            PersistenceCommand::UpdateOrderFill {
                exchange,
                client_order_id,
                filled_quantity,
                remaining_quantity,
                average_fill_price,
                status,
            } => {
                if let Some(db_order) = self
                    .order_repo
                    .get_by_client_id(exchange, &client_order_id)
                    .await?
                {
                    self.order_repo
                        .update_fill(
                            db_order.id,
                            filled_quantity,
                            remaining_quantity,
                            average_fill_price,
                            status,
                        )
                        .await?;
                }
            }
            PersistenceCommand::InsertFill(fill) => {
                self.fill_repo.insert(&fill).await?;
            }
            PersistenceCommand::UpsertPosition(position) => {
                self.position_repo.upsert(&position).await?;
            }
            PersistenceCommand::CancelOrder {
                exchange,
                client_order_id,
            } => {
                if let Some(db_order) = self
                    .order_repo
                    .get_by_client_id(exchange, &client_order_id)
                    .await?
                {
                    self.order_repo.cancel(db_order.id).await?;
                }
            }
            PersistenceCommand::InsertRiskEvent(event) => {
                self.risk_repo.insert_event(&event).await?;
            }
        }
        Ok(())
    }
}
