//! Database models - Rust structs that map to PostgreSQL tables

use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

// ============================================================================
// ENUMS (must match PostgreSQL enum types)
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type, Serialize, Deserialize)]
#[sqlx(type_name = "exchange_type", rename_all = "lowercase")]
pub enum DbExchange {
    Kraken,
    Mexc,
    Bybit,
    Kalshi,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type, Serialize, Deserialize)]
#[sqlx(type_name = "order_side", rename_all = "lowercase")]
pub enum DbOrderSide {
    Buy,
    Sell,
}

impl From<adapters::traits::Side> for DbOrderSide {
    fn from(s: adapters::traits::Side) -> Self {
        match s {
            adapters::traits::Side::Buy => DbOrderSide::Buy,
            adapters::traits::Side::Sell => DbOrderSide::Sell,
        }
    }
}

impl From<DbOrderSide> for adapters::traits::Side {
    fn from(s: DbOrderSide) -> Self {
        match s {
            DbOrderSide::Buy => adapters::traits::Side::Buy,
            DbOrderSide::Sell => adapters::traits::Side::Sell,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type, Serialize, Deserialize)]
#[sqlx(type_name = "order_type", rename_all = "snake_case")]
pub enum DbOrderType {
    Limit,
    Market,
    StopLoss,
    StopLossLimit,
    TakeProfit,
    TakeProfitLimit,
}

impl From<adapters::traits::OrderType> for DbOrderType {
    fn from(t: adapters::traits::OrderType) -> Self {
        match t {
            adapters::traits::OrderType::Limit => DbOrderType::Limit,
            adapters::traits::OrderType::Market => DbOrderType::Market,
            adapters::traits::OrderType::StopLoss => DbOrderType::StopLoss,
            adapters::traits::OrderType::StopLossLimit => DbOrderType::StopLossLimit,
            adapters::traits::OrderType::TakeProfit => DbOrderType::TakeProfit,
            adapters::traits::OrderType::TakeProfitLimit => DbOrderType::TakeProfitLimit,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type, Serialize, Deserialize)]
#[sqlx(type_name = "order_status", rename_all = "snake_case")]
pub enum DbOrderStatus {
    New,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
    Expired,
}

impl From<adapters::traits::OrderStatus> for DbOrderStatus {
    fn from(s: adapters::traits::OrderStatus) -> Self {
        match s {
            adapters::traits::OrderStatus::New => DbOrderStatus::New,
            adapters::traits::OrderStatus::PartiallyFilled => DbOrderStatus::PartiallyFilled,
            adapters::traits::OrderStatus::Filled => DbOrderStatus::Filled,
            adapters::traits::OrderStatus::Canceled => DbOrderStatus::Canceled,
            adapters::traits::OrderStatus::Rejected => DbOrderStatus::Rejected,
            adapters::traits::OrderStatus::Expired => DbOrderStatus::Expired,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type, Serialize, Deserialize)]
#[sqlx(type_name = "time_in_force", rename_all = "lowercase")]
pub enum DbTimeInForce {
    Gtc,
    Ioc,
    Fok,
}

impl From<adapters::traits::TimeInForce> for DbTimeInForce {
    fn from(t: adapters::traits::TimeInForce) -> Self {
        match t {
            adapters::traits::TimeInForce::Gtc => DbTimeInForce::Gtc,
            adapters::traits::TimeInForce::Ioc => DbTimeInForce::Ioc,
            adapters::traits::TimeInForce::Fok => DbTimeInForce::Fok,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type, Serialize, Deserialize)]
#[sqlx(type_name = "risk_severity", rename_all = "lowercase")]
pub enum DbRiskSeverity {
    Info,
    Warning,
    Critical,
    Emergency,
}

// ============================================================================
// ORDER MODEL
// ============================================================================

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct DbOrder {
    pub id: Uuid,
    pub client_order_id: String,
    pub venue_order_id: Option<String>,
    pub exchange: DbExchange,
    pub symbol: String,
    pub side: DbOrderSide,
    pub order_type: DbOrderType,
    pub time_in_force: Option<DbTimeInForce>,
    pub quantity: Decimal,
    pub price: Option<Decimal>,
    pub stop_price: Option<Decimal>,
    pub filled_quantity: Decimal,
    pub remaining_quantity: Decimal,
    pub average_fill_price: Option<Decimal>,
    pub status: DbOrderStatus,
    pub post_only: bool,
    pub reduce_only: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub submitted_at: Option<DateTime<Utc>>,
    pub filled_at: Option<DateTime<Utc>>,
    pub canceled_at: Option<DateTime<Utc>>,
    pub exchange_created_ms: Option<i64>,
    pub exchange_updated_ms: Option<i64>,
    pub strategy_id: Option<String>,
    pub raw_status: Option<String>,
}

/// New order for insertion (without auto-generated fields)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewDbOrder {
    pub client_order_id: String,
    pub venue_order_id: Option<String>,
    pub exchange: DbExchange,
    pub symbol: String,
    pub side: DbOrderSide,
    pub order_type: DbOrderType,
    pub time_in_force: Option<DbTimeInForce>,
    pub quantity: Decimal,
    pub price: Option<Decimal>,
    pub stop_price: Option<Decimal>,
    pub post_only: bool,
    pub reduce_only: bool,
    pub strategy_id: Option<String>,
}

// ============================================================================
// FILL MODEL
// ============================================================================

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct DbFill {
    pub id: Uuid,
    pub order_id: Option<Uuid>,
    pub venue_order_id: String,
    pub client_order_id: String,
    pub execution_id: String,
    pub exchange: DbExchange,
    pub symbol: String,
    pub side: DbOrderSide,
    pub price: Decimal,
    pub quantity: Decimal,
    pub fee: Decimal,
    pub fee_currency: Option<String>,
    pub is_maker: bool,
    pub created_at: DateTime<Utc>,
    pub exchange_ts_ms: i64,
    pub received_ts_ms: i64,
    pub notional_value: Option<Decimal>,
}

/// New fill for insertion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewDbFill {
    pub order_id: Option<Uuid>,
    pub venue_order_id: String,
    pub client_order_id: String,
    pub execution_id: String,
    pub exchange: DbExchange,
    pub symbol: String,
    pub side: DbOrderSide,
    pub price: Decimal,
    pub quantity: Decimal,
    pub fee: Decimal,
    pub fee_currency: Option<String>,
    pub is_maker: bool,
    pub exchange_ts_ms: i64,
    pub received_ts_ms: i64,
}

// ============================================================================
// POSITION MODEL
// ============================================================================

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct DbPosition {
    pub id: Uuid,
    pub exchange: DbExchange,
    pub symbol: String,
    pub quantity: Decimal,
    pub entry_price: Decimal,
    pub mark_price: Option<Decimal>,
    pub liquidation_price: Option<Decimal>,
    pub unrealized_pnl: Option<Decimal>,
    pub realized_pnl: Option<Decimal>,
    pub margin: Option<Decimal>,
    pub leverage: Option<i32>,
    pub opened_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub closed_at: Option<DateTime<Utc>>,
    pub is_open: bool,
}

/// New position for insertion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewDbPosition {
    pub exchange: DbExchange,
    pub symbol: String,
    pub quantity: Decimal,
    pub entry_price: Decimal,
    pub mark_price: Option<Decimal>,
    pub liquidation_price: Option<Decimal>,
    pub margin: Option<Decimal>,
    pub leverage: Option<i32>,
}

// ============================================================================
// PNL DAILY MODEL
// ============================================================================

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct DbPnlDaily {
    pub id: Uuid,
    pub date: NaiveDate,
    pub exchange: Option<DbExchange>,
    pub symbol: Option<String>,
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub fees_paid: Decimal,
    pub volume_traded: Decimal,
    pub num_trades: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// ============================================================================
// RISK EVENT MODEL
// ============================================================================

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct DbRiskEvent {
    pub id: Uuid,
    pub severity: DbRiskSeverity,
    pub event_type: String,
    pub message: String,
    pub exchange: Option<DbExchange>,
    pub symbol: Option<String>,
    pub order_id: Option<Uuid>,
    pub violation_details: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub exchange_ts_ms: Option<i64>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub resolution_notes: Option<String>,
}

/// New risk event for insertion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewDbRiskEvent {
    pub severity: DbRiskSeverity,
    pub event_type: String,
    pub message: String,
    pub exchange: Option<DbExchange>,
    pub symbol: Option<String>,
    pub order_id: Option<Uuid>,
    pub violation_details: Option<serde_json::Value>,
    pub exchange_ts_ms: Option<i64>,
}

// ============================================================================
// KILL SWITCH EVENT MODEL
// ============================================================================

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct DbKillSwitchEvent {
    pub id: Uuid,
    pub trigger_type: String,
    pub reason: String,
    pub triggered_at: DateTime<Utc>,
    pub reset_at: Option<DateTime<Utc>>,
    pub orders_canceled: Option<i32>,
    pub positions_flattened: bool,
    pub reset_by: Option<String>,
}

// ============================================================================
// STRATEGY SESSION MODEL
// ============================================================================

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct DbStrategySession {
    pub id: Uuid,
    pub strategy_name: String,
    pub strategy_id: String,
    pub config: Option<serde_json::Value>,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub total_orders: Option<i32>,
    pub total_fills: Option<i32>,
    pub total_volume: Option<Decimal>,
    pub realized_pnl: Option<Decimal>,
    pub exit_reason: Option<String>,
}
