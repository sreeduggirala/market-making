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
