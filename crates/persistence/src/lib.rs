//! PostgreSQL Persistence Layer
//!
//! Provides durable storage for the market making system:
//!
//! - **Orders**: Full order lifecycle history
//! - **Fills**: Trade execution records
//! - **Positions**: Position snapshots and history
//! - **PnL**: Realized and unrealized PnL tracking
//! - **Risk Events**: Audit trail for risk violations
//!
//! # Example
//!
//! ```ignore
//! use persistence::{Database, OrderRepository};
//!
//! // Connect to database
//! let db = Database::connect("postgres://user:pass@localhost/trading").await?;
//!
//! // Run migrations
//! db.migrate().await?;
//!
//! // Use repositories
//! let order_repo = OrderRepository::new(db.pool());
//! order_repo.insert(&order).await?;
//! ```

pub mod schema;
pub mod models;
pub mod repositories;
pub mod database;

pub use database::Database;
pub use models::*;
pub use repositories::*;

/// Error types for persistence operations
#[derive(Debug, thiserror::Error)]
pub enum PersistenceError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),

    #[error("Record not found: {0}")]
    NotFound(String),

    #[error("Duplicate record: {0}")]
    Duplicate(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, PersistenceError>;

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::*;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    // -------------------------------------------------------------------------
    // Model Conversion Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_db_order_side_from_trait_side() {
        use adapters::traits::Side;

        assert!(matches!(DbOrderSide::from(Side::Buy), DbOrderSide::Buy));
        assert!(matches!(DbOrderSide::from(Side::Sell), DbOrderSide::Sell));
    }

    #[test]
    fn test_trait_side_from_db_order_side() {
        use adapters::traits::Side;

        assert!(matches!(Side::from(DbOrderSide::Buy), Side::Buy));
        assert!(matches!(Side::from(DbOrderSide::Sell), Side::Sell));
    }

    #[test]
    fn test_db_order_type_from_trait_order_type() {
        use adapters::traits::OrderType;

        assert!(matches!(DbOrderType::from(OrderType::Limit), DbOrderType::Limit));
        assert!(matches!(DbOrderType::from(OrderType::Market), DbOrderType::Market));
        assert!(matches!(DbOrderType::from(OrderType::StopLoss), DbOrderType::StopLoss));
        assert!(matches!(DbOrderType::from(OrderType::StopLossLimit), DbOrderType::StopLossLimit));
        assert!(matches!(DbOrderType::from(OrderType::TakeProfit), DbOrderType::TakeProfit));
        assert!(matches!(DbOrderType::from(OrderType::TakeProfitLimit), DbOrderType::TakeProfitLimit));
    }

    #[test]
    fn test_db_order_status_from_trait_order_status() {
        use adapters::traits::OrderStatus;

        assert!(matches!(DbOrderStatus::from(OrderStatus::New), DbOrderStatus::New));
        assert!(matches!(DbOrderStatus::from(OrderStatus::PartiallyFilled), DbOrderStatus::PartiallyFilled));
        assert!(matches!(DbOrderStatus::from(OrderStatus::Filled), DbOrderStatus::Filled));
        assert!(matches!(DbOrderStatus::from(OrderStatus::Canceled), DbOrderStatus::Canceled));
        assert!(matches!(DbOrderStatus::from(OrderStatus::Rejected), DbOrderStatus::Rejected));
        assert!(matches!(DbOrderStatus::from(OrderStatus::Expired), DbOrderStatus::Expired));
    }

    #[test]
    fn test_db_time_in_force_from_trait_tif() {
        use adapters::traits::TimeInForce;

        assert!(matches!(DbTimeInForce::from(TimeInForce::Gtc), DbTimeInForce::Gtc));
        assert!(matches!(DbTimeInForce::from(TimeInForce::Ioc), DbTimeInForce::Ioc));
        assert!(matches!(DbTimeInForce::from(TimeInForce::Fok), DbTimeInForce::Fok));
    }

    // -------------------------------------------------------------------------
    // NewDbOrder Builder Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_new_db_order_creation() {
        let order = NewDbOrder {
            client_order_id: "test-123".to_string(),
            venue_order_id: None,
            exchange: DbExchange::Kraken,
            symbol: "BTC/USD".to_string(),
            side: DbOrderSide::Buy,
            order_type: DbOrderType::Limit,
            time_in_force: Some(DbTimeInForce::Gtc),
            quantity: Decimal::from_str("0.1").unwrap(),
            price: Some(Decimal::from_str("50000.00").unwrap()),
            stop_price: None,
            post_only: true,
            reduce_only: false,
            strategy_id: Some("avellaneda_stoikov".to_string()),
        };

        assert_eq!(order.client_order_id, "test-123");
        assert!(matches!(order.exchange, DbExchange::Kraken));
        assert!(order.post_only);
        assert!(!order.reduce_only);
    }

    #[test]
    fn test_new_db_order_market_order() {
        let order = NewDbOrder {
            client_order_id: "market-456".to_string(),
            venue_order_id: None,
            exchange: DbExchange::Bybit,
            symbol: "BTCUSDT".to_string(),
            side: DbOrderSide::Sell,
            order_type: DbOrderType::Market,
            time_in_force: Some(DbTimeInForce::Ioc),
            quantity: Decimal::from_str("0.5").unwrap(),
            price: None, // Market orders have no price
            stop_price: None,
            post_only: false,
            reduce_only: true,
            strategy_id: None,
        };

        assert!(order.price.is_none());
        assert!(matches!(order.order_type, DbOrderType::Market));
        assert!(order.reduce_only);
    }

    // -------------------------------------------------------------------------
    // NewDbFill Builder Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_new_db_fill_creation() {
        let fill = NewDbFill {
            order_id: Some(uuid::Uuid::new_v4()),
            venue_order_id: "venue-123".to_string(),
            client_order_id: "client-123".to_string(),
            execution_id: "exec-456".to_string(),
            exchange: DbExchange::Mexc,
            symbol: "ETH/USD".to_string(),
            side: DbOrderSide::Buy,
            price: Decimal::from_str("3000.50").unwrap(),
            quantity: Decimal::from_str("1.5").unwrap(),
            fee: Decimal::from_str("0.001").unwrap(),
            fee_currency: Some("ETH".to_string()),
            is_maker: true,
            exchange_ts_ms: 1700000000000,
            received_ts_ms: 1700000000050,
        };

        assert_eq!(fill.execution_id, "exec-456");
        assert!(fill.is_maker);
        assert_eq!(fill.received_ts_ms - fill.exchange_ts_ms, 50); // 50ms latency
    }

    // -------------------------------------------------------------------------
    // NewDbPosition Builder Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_new_db_position_creation() {
        let position = NewDbPosition {
            exchange: DbExchange::Kalshi,
            symbol: "PRES-2024".to_string(),
            quantity: Decimal::from_str("100").unwrap(),
            entry_price: Decimal::from_str("0.55").unwrap(),
            mark_price: Some(Decimal::from_str("0.60").unwrap()),
            liquidation_price: None, // Kalshi doesn't have liquidation
            margin: None,
            leverage: None,
        };

        assert!(matches!(position.exchange, DbExchange::Kalshi));
        assert!(position.liquidation_price.is_none());
    }

    // -------------------------------------------------------------------------
    // NewDbRiskEvent Builder Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_new_db_risk_event_creation() {
        let event = NewDbRiskEvent {
            severity: DbRiskSeverity::Warning,
            event_type: "position_limit_warning".to_string(),
            message: "Position approaching 80% of limit".to_string(),
            exchange: Some(DbExchange::Kraken),
            symbol: Some("BTC/USD".to_string()),
            order_id: None,
            violation_details: Some(serde_json::json!({
                "current_position": 0.08,
                "limit": 0.10,
                "percentage": 80
            })),
            exchange_ts_ms: Some(1700000000000),
        };

        assert!(matches!(event.severity, DbRiskSeverity::Warning));
        assert!(event.violation_details.is_some());
    }

    #[test]
    fn test_new_db_risk_event_emergency() {
        let event = NewDbRiskEvent {
            severity: DbRiskSeverity::Emergency,
            event_type: "kill_switch_triggered".to_string(),
            message: "Maximum loss limit exceeded - all trading halted".to_string(),
            exchange: None, // Affects all exchanges
            symbol: None,
            order_id: None,
            violation_details: Some(serde_json::json!({
                "daily_loss": -5000.0,
                "loss_limit": -2000.0,
                "action": "flatten_all_positions"
            })),
            exchange_ts_ms: None,
        };

        assert!(matches!(event.severity, DbRiskSeverity::Emergency));
    }

    // -------------------------------------------------------------------------
    // Error Type Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_persistence_error_display() {
        let err = PersistenceError::NotFound("Order abc-123".to_string());
        assert!(err.to_string().contains("Order abc-123"));

        let err = PersistenceError::Duplicate("client_order_id".to_string());
        assert!(err.to_string().contains("Duplicate"));

        let err = PersistenceError::Serialization("invalid JSON".to_string());
        assert!(err.to_string().contains("Serialization"));
    }

    // -------------------------------------------------------------------------
    // Database Options Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_database_options_default() {
        let opts = database::DatabaseOptions::default();
        assert_eq!(opts.max_connections, 10);
        assert_eq!(opts.min_connections, 2);
    }

    #[test]
    fn test_database_options_high_performance() {
        let opts = database::DatabaseOptions::high_performance();
        assert_eq!(opts.max_connections, 50);
        assert_eq!(opts.min_connections, 10);
        assert!(opts.acquire_timeout.as_secs() <= 5);
    }

    #[test]
    fn test_database_options_development() {
        let opts = database::DatabaseOptions::development();
        assert_eq!(opts.max_connections, 5);
        assert_eq!(opts.min_connections, 1);
        assert!(opts.max_lifetime.is_none());
    }

    // -------------------------------------------------------------------------
    // Enum Serialization Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_db_exchange_serialization() {
        let exchange = DbExchange::Kraken;
        let json = serde_json::to_string(&exchange).unwrap();
        assert_eq!(json, "\"Kraken\"");

        let parsed: DbExchange = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, DbExchange::Kraken));
    }

    #[test]
    fn test_db_order_status_serialization() {
        let status = DbOrderStatus::PartiallyFilled;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"PartiallyFilled\"");
    }

    #[test]
    fn test_db_risk_severity_serialization() {
        let severity = DbRiskSeverity::Critical;
        let json = serde_json::to_string(&severity).unwrap();
        assert_eq!(json, "\"Critical\"");
    }

    // -------------------------------------------------------------------------
    // Decimal Precision Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_decimal_precision_preserved() {
        let qty = Decimal::from_str("0.00000001").unwrap(); // 1 satoshi
        let price = Decimal::from_str("99999.99999999").unwrap();

        let order = NewDbOrder {
            client_order_id: "precision-test".to_string(),
            venue_order_id: None,
            exchange: DbExchange::Kraken,
            symbol: "BTC/USD".to_string(),
            side: DbOrderSide::Buy,
            order_type: DbOrderType::Limit,
            time_in_force: None,
            quantity: qty,
            price: Some(price),
            stop_price: None,
            post_only: false,
            reduce_only: false,
            strategy_id: None,
        };

        assert_eq!(order.quantity.to_string(), "0.00000001");
        assert_eq!(order.price.unwrap().to_string(), "99999.99999999");
    }
}
