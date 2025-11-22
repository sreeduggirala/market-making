//! Position repository for position tracking

use crate::models::{DbExchange, DbPosition, NewDbPosition};
use crate::{PersistenceError, Result};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sqlx::PgPool;
use uuid::Uuid;

/// Repository for position persistence
#[derive(Clone)]
pub struct PositionRepository {
    pool: PgPool,
}

impl PositionRepository {
    /// Creates a new position repository
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Inserts or updates a position (upsert)
    pub async fn upsert(&self, position: &NewDbPosition) -> Result<DbPosition> {
        let record = sqlx::query_as::<_, DbPosition>(
            r#"
            INSERT INTO positions (
                exchange, symbol, quantity, entry_price, mark_price,
                liquidation_price, margin, leverage, opened_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
            ON CONFLICT (exchange, symbol)
            DO UPDATE SET
                quantity = EXCLUDED.quantity,
                entry_price = EXCLUDED.entry_price,
                mark_price = EXCLUDED.mark_price,
                liquidation_price = EXCLUDED.liquidation_price,
                margin = EXCLUDED.margin,
                leverage = EXCLUDED.leverage,
                is_open = TRUE,
                closed_at = NULL
            RETURNING *
            "#,
        )
        .bind(&position.exchange)
        .bind(&position.symbol)
        .bind(&position.quantity)
        .bind(&position.entry_price)
        .bind(&position.mark_price)
        .bind(&position.liquidation_price)
        .bind(&position.margin)
        .bind(&position.leverage)
        .fetch_one(&self.pool)
        .await?;

        Ok(record)
    }

    /// Gets a position by ID
    pub async fn get_by_id(&self, id: Uuid) -> Result<DbPosition> {
        let record = sqlx::query_as::<_, DbPosition>("SELECT * FROM positions WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| PersistenceError::NotFound(format!("Position {}", id)))?;

        Ok(record)
    }

    /// Gets an open position for exchange and symbol
    pub async fn get_open_position(
        &self,
        exchange: DbExchange,
        symbol: &str,
    ) -> Result<Option<DbPosition>> {
        let record = sqlx::query_as::<_, DbPosition>(
            "SELECT * FROM positions WHERE exchange = $1 AND symbol = $2 AND is_open = TRUE",
        )
        .bind(exchange)
        .bind(symbol)
        .fetch_optional(&self.pool)
        .await?;

        Ok(record)
    }

    /// Gets all open positions
    pub async fn get_all_open(&self) -> Result<Vec<DbPosition>> {
        let records = sqlx::query_as::<_, DbPosition>(
            "SELECT * FROM positions WHERE is_open = TRUE ORDER BY updated_at DESC",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(records)
    }

    /// Gets all open positions for an exchange
    pub async fn get_open_by_exchange(&self, exchange: DbExchange) -> Result<Vec<DbPosition>> {
        let records = sqlx::query_as::<_, DbPosition>(
            "SELECT * FROM positions WHERE exchange = $1 AND is_open = TRUE ORDER BY symbol",
        )
        .bind(exchange)
        .fetch_all(&self.pool)
        .await?;

        Ok(records)
    }

    /// Updates position quantity and entry price
    pub async fn update_position(
        &self,
        exchange: DbExchange,
        symbol: &str,
        quantity: Decimal,
        entry_price: Decimal,
    ) -> Result<DbPosition> {
        let record = sqlx::query_as::<_, DbPosition>(
            r#"
            UPDATE positions
            SET quantity = $3, entry_price = $4
            WHERE exchange = $1 AND symbol = $2 AND is_open = TRUE
            RETURNING *
            "#,
        )
        .bind(exchange)
        .bind(symbol)
        .bind(quantity)
        .bind(entry_price)
        .fetch_one(&self.pool)
        .await?;

        Ok(record)
    }

    /// Updates mark price and unrealized PnL
    pub async fn update_mark_price(
        &self,
        exchange: DbExchange,
        symbol: &str,
        mark_price: Decimal,
        unrealized_pnl: Decimal,
    ) -> Result<DbPosition> {
        let record = sqlx::query_as::<_, DbPosition>(
            r#"
            UPDATE positions
            SET mark_price = $3, unrealized_pnl = $4
            WHERE exchange = $1 AND symbol = $2 AND is_open = TRUE
            RETURNING *
            "#,
        )
        .bind(exchange)
        .bind(symbol)
        .bind(mark_price)
        .bind(unrealized_pnl)
        .fetch_one(&self.pool)
        .await?;

        Ok(record)
    }

    /// Closes a position
    pub async fn close_position(
        &self,
        exchange: DbExchange,
        symbol: &str,
        realized_pnl: Decimal,
    ) -> Result<DbPosition> {
        let record = sqlx::query_as::<_, DbPosition>(
            r#"
            UPDATE positions
            SET is_open = FALSE,
                closed_at = NOW(),
                quantity = 0,
                realized_pnl = COALESCE(realized_pnl, 0) + $3,
                unrealized_pnl = 0
            WHERE exchange = $1 AND symbol = $2 AND is_open = TRUE
            RETURNING *
            "#,
        )
        .bind(exchange)
        .bind(symbol)
        .bind(realized_pnl)
        .fetch_one(&self.pool)
        .await?;

        Ok(record)
    }

    /// Records a position snapshot to history
    pub async fn record_snapshot(&self, position: &DbPosition) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO position_history (
                position_id, exchange, symbol, quantity, entry_price,
                mark_price, unrealized_pnl, realized_pnl
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(&position.id)
        .bind(&position.exchange)
        .bind(&position.symbol)
        .bind(&position.quantity)
        .bind(&position.entry_price)
        .bind(&position.mark_price)
        .bind(&position.unrealized_pnl)
        .bind(&position.realized_pnl)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Gets position history
    pub async fn get_history(
        &self,
        exchange: DbExchange,
        symbol: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<PositionSnapshot>> {
        let records = sqlx::query_as::<_, PositionSnapshot>(
            r#"
            SELECT id, exchange, symbol, quantity, entry_price, mark_price,
                   unrealized_pnl, realized_pnl, snapshot_at
            FROM position_history
            WHERE exchange = $1 AND symbol = $2
              AND snapshot_at >= $3 AND snapshot_at < $4
            ORDER BY snapshot_at
            "#,
        )
        .bind(exchange)
        .bind(symbol)
        .bind(start)
        .bind(end)
        .fetch_all(&self.pool)
        .await?;

        Ok(records)
    }

    /// Gets total unrealized PnL across all positions
    pub async fn get_total_unrealized_pnl(&self) -> Result<Decimal> {
        let result: (Option<Decimal>,) = sqlx::query_as(
            "SELECT COALESCE(SUM(unrealized_pnl), 0) FROM positions WHERE is_open = TRUE",
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(result.0.unwrap_or_default())
    }

    /// Gets total realized PnL
    pub async fn get_total_realized_pnl(&self) -> Result<Decimal> {
        let result: (Option<Decimal>,) = sqlx::query_as(
            "SELECT COALESCE(SUM(realized_pnl), 0) FROM positions",
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(result.0.unwrap_or_default())
    }
}

/// Position snapshot from history
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PositionSnapshot {
    pub id: Uuid,
    pub exchange: DbExchange,
    pub symbol: String,
    pub quantity: Decimal,
    pub entry_price: Decimal,
    pub mark_price: Option<Decimal>,
    pub unrealized_pnl: Option<Decimal>,
    pub realized_pnl: Option<Decimal>,
    pub snapshot_at: DateTime<Utc>,
}
