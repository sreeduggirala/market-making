//! Order repository for CRUD operations on orders

use crate::models::{DbExchange, DbOrder, DbOrderStatus, NewDbOrder};
use crate::{PersistenceError, Result};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sqlx::PgPool;
use uuid::Uuid;

/// Repository for order persistence
#[derive(Clone)]
pub struct OrderRepository {
    pool: PgPool,
}

impl OrderRepository {
    /// Creates a new order repository
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Inserts a new order
    pub async fn insert(&self, order: &NewDbOrder) -> Result<DbOrder> {
        let record = sqlx::query_as::<_, DbOrder>(
            r#"
            INSERT INTO orders (
                client_order_id, venue_order_id, exchange, symbol,
                side, order_type, time_in_force, quantity, price, stop_price,
                remaining_quantity, post_only, reduce_only, strategy_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $8, $11, $12, $13)
            RETURNING *
            "#,
        )
        .bind(&order.client_order_id)
        .bind(&order.venue_order_id)
        .bind(&order.exchange)
        .bind(&order.symbol)
        .bind(&order.side)
        .bind(&order.order_type)
        .bind(&order.time_in_force)
        .bind(&order.quantity)
        .bind(&order.price)
        .bind(&order.stop_price)
        .bind(&order.post_only)
        .bind(&order.reduce_only)
        .bind(&order.strategy_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(record)
    }

    /// Gets an order by ID
    pub async fn get_by_id(&self, id: Uuid) -> Result<DbOrder> {
        let record = sqlx::query_as::<_, DbOrder>("SELECT * FROM orders WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| PersistenceError::NotFound(format!("Order {}", id)))?;

        Ok(record)
    }

    /// Gets an order by client order ID and exchange
    pub async fn get_by_client_id(
        &self,
        exchange: DbExchange,
        client_order_id: &str,
    ) -> Result<Option<DbOrder>> {
        let record = sqlx::query_as::<_, DbOrder>(
            "SELECT * FROM orders WHERE exchange = $1 AND client_order_id = $2",
        )
        .bind(exchange)
        .bind(client_order_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(record)
    }

    /// Gets an order by venue order ID
    pub async fn get_by_venue_id(
        &self,
        exchange: DbExchange,
        venue_order_id: &str,
    ) -> Result<Option<DbOrder>> {
        let record = sqlx::query_as::<_, DbOrder>(
            "SELECT * FROM orders WHERE exchange = $1 AND venue_order_id = $2",
        )
        .bind(exchange)
        .bind(venue_order_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(record)
    }

    /// Gets all open orders
    pub async fn get_open_orders(&self) -> Result<Vec<DbOrder>> {
        let records = sqlx::query_as::<_, DbOrder>(
            "SELECT * FROM orders WHERE status IN ('new', 'partially_filled') ORDER BY created_at DESC",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(records)
    }

    /// Gets open orders for a specific exchange and symbol
    pub async fn get_open_orders_for_symbol(
        &self,
        exchange: DbExchange,
        symbol: &str,
    ) -> Result<Vec<DbOrder>> {
        let records = sqlx::query_as::<_, DbOrder>(
            r#"
            SELECT * FROM orders
            WHERE exchange = $1
              AND symbol = $2
              AND status IN ('new', 'partially_filled')
            ORDER BY created_at DESC
            "#,
        )
        .bind(exchange)
        .bind(symbol)
        .fetch_all(&self.pool)
        .await?;

        Ok(records)
    }

    /// Gets orders by strategy ID
    pub async fn get_by_strategy(&self, strategy_id: &str) -> Result<Vec<DbOrder>> {
        let records = sqlx::query_as::<_, DbOrder>(
            "SELECT * FROM orders WHERE strategy_id = $1 ORDER BY created_at DESC",
        )
        .bind(strategy_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(records)
    }

    /// Updates order status
    pub async fn update_status(
        &self,
        id: Uuid,
        status: DbOrderStatus,
        venue_order_id: Option<&str>,
    ) -> Result<DbOrder> {
        let record = sqlx::query_as::<_, DbOrder>(
            r#"
            UPDATE orders
            SET status = $2, venue_order_id = COALESCE($3, venue_order_id)
            WHERE id = $1
            RETURNING *
            "#,
        )
        .bind(id)
        .bind(status)
        .bind(venue_order_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(record)
    }

    /// Updates order with fill information
    pub async fn update_fill(
        &self,
        id: Uuid,
        filled_quantity: Decimal,
        remaining_quantity: Decimal,
        average_fill_price: Decimal,
        status: DbOrderStatus,
    ) -> Result<DbOrder> {
        let filled_at: Option<DateTime<Utc>> = if status == DbOrderStatus::Filled {
            Some(Utc::now())
        } else {
            None
        };

        let record = sqlx::query_as::<_, DbOrder>(
            r#"
            UPDATE orders
            SET filled_quantity = $2,
                remaining_quantity = $3,
                average_fill_price = $4,
                status = $5,
                filled_at = COALESCE($6, filled_at)
            WHERE id = $1
            RETURNING *
            "#,
        )
        .bind(id)
        .bind(filled_quantity)
        .bind(remaining_quantity)
        .bind(average_fill_price)
        .bind(status)
        .bind(filled_at)
        .fetch_one(&self.pool)
        .await?;

        Ok(record)
    }

    /// Marks an order as canceled
    pub async fn cancel(&self, id: Uuid) -> Result<DbOrder> {
        let record = sqlx::query_as::<_, DbOrder>(
            r#"
            UPDATE orders
            SET status = 'canceled', canceled_at = NOW()
            WHERE id = $1
            RETURNING *
            "#,
        )
        .bind(id)
        .fetch_one(&self.pool)
        .await?;

        Ok(record)
    }

    /// Gets order history with pagination
    pub async fn get_history(
        &self,
        exchange: Option<DbExchange>,
        symbol: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<DbOrder>> {
        let records = sqlx::query_as::<_, DbOrder>(
            r#"
            SELECT * FROM orders
            WHERE ($1::exchange_type IS NULL OR exchange = $1)
              AND ($2::text IS NULL OR symbol = $2)
            ORDER BY created_at DESC
            LIMIT $3 OFFSET $4
            "#,
        )
        .bind(exchange)
        .bind(symbol)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;

        Ok(records)
    }

    /// Counts orders by status
    pub async fn count_by_status(&self, status: DbOrderStatus) -> Result<i64> {
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM orders WHERE status = $1")
            .bind(status)
            .fetch_one(&self.pool)
            .await?;

        Ok(count.0)
    }

    /// Gets orders created within a time range
    pub async fn get_by_time_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<DbOrder>> {
        let records = sqlx::query_as::<_, DbOrder>(
            "SELECT * FROM orders WHERE created_at >= $1 AND created_at < $2 ORDER BY created_at",
        )
        .bind(start)
        .bind(end)
        .fetch_all(&self.pool)
        .await?;

        Ok(records)
    }
}
