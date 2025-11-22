//! Fill repository for trade execution records

use crate::models::{DbExchange, DbFill, NewDbFill};
use crate::{PersistenceError, Result};
use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use sqlx::PgPool;
use uuid::Uuid;

/// Repository for fill persistence
#[derive(Clone)]
pub struct FillRepository {
    pool: PgPool,
}

impl FillRepository {
    /// Creates a new fill repository
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Inserts a new fill
    pub async fn insert(&self, fill: &NewDbFill) -> Result<DbFill> {
        let record = sqlx::query_as::<_, DbFill>(
            r#"
            INSERT INTO fills (
                order_id, venue_order_id, client_order_id, execution_id,
                exchange, symbol, side, price, quantity, fee, fee_currency,
                is_maker, exchange_ts_ms, received_ts_ms
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            RETURNING *
            "#,
        )
        .bind(&fill.order_id)
        .bind(&fill.venue_order_id)
        .bind(&fill.client_order_id)
        .bind(&fill.execution_id)
        .bind(&fill.exchange)
        .bind(&fill.symbol)
        .bind(&fill.side)
        .bind(&fill.price)
        .bind(&fill.quantity)
        .bind(&fill.fee)
        .bind(&fill.fee_currency)
        .bind(&fill.is_maker)
        .bind(&fill.exchange_ts_ms)
        .bind(&fill.received_ts_ms)
        .fetch_one(&self.pool)
        .await?;

        Ok(record)
    }

    /// Gets a fill by ID
    pub async fn get_by_id(&self, id: Uuid) -> Result<DbFill> {
        let record = sqlx::query_as::<_, DbFill>("SELECT * FROM fills WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| PersistenceError::NotFound(format!("Fill {}", id)))?;

        Ok(record)
    }

    /// Gets fills for an order
    pub async fn get_by_order_id(&self, order_id: Uuid) -> Result<Vec<DbFill>> {
        let records = sqlx::query_as::<_, DbFill>(
            "SELECT * FROM fills WHERE order_id = $1 ORDER BY exchange_ts_ms",
        )
        .bind(order_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(records)
    }

    /// Gets fills by execution ID (to check for duplicates)
    pub async fn get_by_execution_id(
        &self,
        exchange: DbExchange,
        execution_id: &str,
    ) -> Result<Option<DbFill>> {
        let record = sqlx::query_as::<_, DbFill>(
            "SELECT * FROM fills WHERE exchange = $1 AND execution_id = $2",
        )
        .bind(exchange)
        .bind(execution_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(record)
    }

    /// Gets fills for an exchange and symbol
    pub async fn get_by_symbol(
        &self,
        exchange: DbExchange,
        symbol: &str,
        limit: i64,
    ) -> Result<Vec<DbFill>> {
        let records = sqlx::query_as::<_, DbFill>(
            r#"
            SELECT * FROM fills
            WHERE exchange = $1 AND symbol = $2
            ORDER BY exchange_ts_ms DESC
            LIMIT $3
            "#,
        )
        .bind(exchange)
        .bind(symbol)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(records)
    }

    /// Gets fills within a time range
    pub async fn get_by_time_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<DbFill>> {
        let records = sqlx::query_as::<_, DbFill>(
            "SELECT * FROM fills WHERE created_at >= $1 AND created_at < $2 ORDER BY exchange_ts_ms",
        )
        .bind(start)
        .bind(end)
        .fetch_all(&self.pool)
        .await?;

        Ok(records)
    }

    /// Gets total volume for a day
    pub async fn get_daily_volume(&self, date: NaiveDate) -> Result<Decimal> {
        let result: (Option<Decimal>,) = sqlx::query_as(
            r#"
            SELECT COALESCE(SUM(notional_value), 0)
            FROM fills
            WHERE DATE(created_at) = $1
            "#,
        )
        .bind(date)
        .fetch_one(&self.pool)
        .await?;

        Ok(result.0.unwrap_or_default())
    }

    /// Gets total fees for a day
    pub async fn get_daily_fees(&self, date: NaiveDate) -> Result<Decimal> {
        let result: (Option<Decimal>,) = sqlx::query_as(
            r#"
            SELECT COALESCE(SUM(fee), 0)
            FROM fills
            WHERE DATE(created_at) = $1
            "#,
        )
        .bind(date)
        .fetch_one(&self.pool)
        .await?;

        Ok(result.0.unwrap_or_default())
    }

    /// Gets fill count for a day
    pub async fn get_daily_count(&self, date: NaiveDate) -> Result<i64> {
        let result: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM fills WHERE DATE(created_at) = $1",
        )
        .bind(date)
        .fetch_one(&self.pool)
        .await?;

        Ok(result.0)
    }

    /// Gets recent fills with pagination
    pub async fn get_recent(&self, limit: i64, offset: i64) -> Result<Vec<DbFill>> {
        let records = sqlx::query_as::<_, DbFill>(
            "SELECT * FROM fills ORDER BY exchange_ts_ms DESC LIMIT $1 OFFSET $2",
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;

        Ok(records)
    }

    /// Gets summary stats for a symbol
    pub async fn get_symbol_stats(
        &self,
        exchange: DbExchange,
        symbol: &str,
    ) -> Result<FillStats> {
        let result: FillStatsRow = sqlx::query_as(
            r#"
            SELECT
                COUNT(*) as count,
                COALESCE(SUM(notional_value), 0) as total_volume,
                COALESCE(SUM(fee), 0) as total_fees,
                COALESCE(AVG(price), 0) as avg_price
            FROM fills
            WHERE exchange = $1 AND symbol = $2
            "#,
        )
        .bind(exchange)
        .bind(symbol)
        .fetch_one(&self.pool)
        .await?;

        Ok(FillStats {
            count: result.count,
            total_volume: result.total_volume,
            total_fees: result.total_fees,
            avg_price: result.avg_price,
        })
    }
}

#[derive(sqlx::FromRow)]
struct FillStatsRow {
    count: i64,
    total_volume: Decimal,
    total_fees: Decimal,
    avg_price: Decimal,
}

/// Fill statistics
#[derive(Debug, Clone)]
pub struct FillStats {
    pub count: i64,
    pub total_volume: Decimal,
    pub total_fees: Decimal,
    pub avg_price: Decimal,
}
