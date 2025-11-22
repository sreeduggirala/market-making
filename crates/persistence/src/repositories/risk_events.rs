//! Risk events repository for audit trail

use crate::models::{DbExchange, DbKillSwitchEvent, DbRiskEvent, DbRiskSeverity, NewDbRiskEvent};
use crate::{PersistenceError, Result};
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

/// Repository for risk event persistence
#[derive(Clone)]
pub struct RiskEventRepository {
    pool: PgPool,
}

impl RiskEventRepository {
    /// Creates a new risk event repository
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    // ========================================================================
    // RISK EVENTS
    // ========================================================================

    /// Inserts a new risk event
    pub async fn insert_event(&self, event: &NewDbRiskEvent) -> Result<DbRiskEvent> {
        let record = sqlx::query_as::<_, DbRiskEvent>(
            r#"
            INSERT INTO risk_events (
                severity, event_type, message, exchange, symbol,
                order_id, violation_details, exchange_ts_ms
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING *
            "#,
        )
        .bind(&event.severity)
        .bind(&event.event_type)
        .bind(&event.message)
        .bind(&event.exchange)
        .bind(&event.symbol)
        .bind(&event.order_id)
        .bind(&event.violation_details)
        .bind(&event.exchange_ts_ms)
        .fetch_one(&self.pool)
        .await?;

        Ok(record)
    }

    /// Gets a risk event by ID
    pub async fn get_event_by_id(&self, id: Uuid) -> Result<DbRiskEvent> {
        let record = sqlx::query_as::<_, DbRiskEvent>("SELECT * FROM risk_events WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| PersistenceError::NotFound(format!("RiskEvent {}", id)))?;

        Ok(record)
    }

    /// Gets recent risk events
    pub async fn get_recent_events(&self, limit: i64) -> Result<Vec<DbRiskEvent>> {
        let records = sqlx::query_as::<_, DbRiskEvent>(
            "SELECT * FROM risk_events ORDER BY created_at DESC LIMIT $1",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(records)
    }

    /// Gets unresolved risk events
    pub async fn get_unresolved_events(&self) -> Result<Vec<DbRiskEvent>> {
        let records = sqlx::query_as::<_, DbRiskEvent>(
            "SELECT * FROM risk_events WHERE resolved_at IS NULL ORDER BY created_at DESC",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(records)
    }

    /// Gets risk events by severity
    pub async fn get_events_by_severity(
        &self,
        severity: DbRiskSeverity,
        limit: i64,
    ) -> Result<Vec<DbRiskEvent>> {
        let records = sqlx::query_as::<_, DbRiskEvent>(
            "SELECT * FROM risk_events WHERE severity = $1 ORDER BY created_at DESC LIMIT $2",
        )
        .bind(severity)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(records)
    }

    /// Gets risk events for a symbol
    pub async fn get_events_by_symbol(
        &self,
        exchange: DbExchange,
        symbol: &str,
        limit: i64,
    ) -> Result<Vec<DbRiskEvent>> {
        let records = sqlx::query_as::<_, DbRiskEvent>(
            r#"
            SELECT * FROM risk_events
            WHERE exchange = $1 AND symbol = $2
            ORDER BY created_at DESC
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

    /// Resolves a risk event
    pub async fn resolve_event(&self, id: Uuid, notes: &str) -> Result<DbRiskEvent> {
        let record = sqlx::query_as::<_, DbRiskEvent>(
            r#"
            UPDATE risk_events
            SET resolved_at = NOW(), resolution_notes = $2
            WHERE id = $1
            RETURNING *
            "#,
        )
        .bind(id)
        .bind(notes)
        .fetch_one(&self.pool)
        .await?;

        Ok(record)
    }

    /// Gets events within a time range
    pub async fn get_events_by_time_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<DbRiskEvent>> {
        let records = sqlx::query_as::<_, DbRiskEvent>(
            "SELECT * FROM risk_events WHERE created_at >= $1 AND created_at < $2 ORDER BY created_at",
        )
        .bind(start)
        .bind(end)
        .fetch_all(&self.pool)
        .await?;

        Ok(records)
    }

    /// Counts events by severity
    pub async fn count_by_severity(&self, severity: DbRiskSeverity) -> Result<i64> {
        let result: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM risk_events WHERE severity = $1",
        )
        .bind(severity)
        .fetch_one(&self.pool)
        .await?;

        Ok(result.0)
    }

    // ========================================================================
    // KILL SWITCH EVENTS
    // ========================================================================

    /// Records a kill switch trigger event
    pub async fn record_kill_switch_trigger(
        &self,
        trigger_type: &str,
        reason: &str,
    ) -> Result<DbKillSwitchEvent> {
        let record = sqlx::query_as::<_, DbKillSwitchEvent>(
            r#"
            INSERT INTO kill_switch_events (trigger_type, reason)
            VALUES ($1, $2)
            RETURNING *
            "#,
        )
        .bind(trigger_type)
        .bind(reason)
        .fetch_one(&self.pool)
        .await?;

        Ok(record)
    }

    /// Records kill switch reset
    pub async fn record_kill_switch_reset(
        &self,
        id: Uuid,
        reset_by: &str,
        orders_canceled: i32,
        positions_flattened: bool,
    ) -> Result<DbKillSwitchEvent> {
        let record = sqlx::query_as::<_, DbKillSwitchEvent>(
            r#"
            UPDATE kill_switch_events
            SET reset_at = NOW(),
                reset_by = $2,
                orders_canceled = $3,
                positions_flattened = $4
            WHERE id = $1
            RETURNING *
            "#,
        )
        .bind(id)
        .bind(reset_by)
        .bind(orders_canceled)
        .bind(positions_flattened)
        .fetch_one(&self.pool)
        .await?;

        Ok(record)
    }

    /// Gets latest kill switch event
    pub async fn get_latest_kill_switch(&self) -> Result<Option<DbKillSwitchEvent>> {
        let record = sqlx::query_as::<_, DbKillSwitchEvent>(
            "SELECT * FROM kill_switch_events ORDER BY triggered_at DESC LIMIT 1",
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(record)
    }

    /// Gets active (unreset) kill switch event
    pub async fn get_active_kill_switch(&self) -> Result<Option<DbKillSwitchEvent>> {
        let record = sqlx::query_as::<_, DbKillSwitchEvent>(
            "SELECT * FROM kill_switch_events WHERE reset_at IS NULL ORDER BY triggered_at DESC LIMIT 1",
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(record)
    }

    /// Gets kill switch history
    pub async fn get_kill_switch_history(&self, limit: i64) -> Result<Vec<DbKillSwitchEvent>> {
        let records = sqlx::query_as::<_, DbKillSwitchEvent>(
            "SELECT * FROM kill_switch_events ORDER BY triggered_at DESC LIMIT $1",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(records)
    }
}
