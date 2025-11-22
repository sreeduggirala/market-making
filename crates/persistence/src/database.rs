//! Database connection and migration management

use crate::Result;
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::time::Duration;
use tracing::info;

/// Database connection manager
#[derive(Clone)]
pub struct Database {
    pool: PgPool,
}

impl Database {
    /// Connects to PostgreSQL with default settings
    pub async fn connect(database_url: &str) -> Result<Self> {
        Self::connect_with_options(database_url, DatabaseOptions::default()).await
    }

    /// Connects to PostgreSQL with custom options
    pub async fn connect_with_options(database_url: &str, options: DatabaseOptions) -> Result<Self> {
        info!(
            max_connections = options.max_connections,
            min_connections = options.min_connections,
            "Connecting to PostgreSQL"
        );

        let pool = PgPoolOptions::new()
            .max_connections(options.max_connections)
            .min_connections(options.min_connections)
            .acquire_timeout(options.acquire_timeout)
            .idle_timeout(options.idle_timeout)
            .max_lifetime(options.max_lifetime)
            .connect(database_url)
            .await?;

        info!("Connected to PostgreSQL");

        Ok(Self { pool })
    }

    /// Returns a reference to the connection pool
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Runs database migrations
    pub async fn migrate(&self) -> Result<()> {
        info!("Running database migrations");
        sqlx::migrate!("./migrations").run(&self.pool).await?;
        info!("Migrations complete");
        Ok(())
    }

    /// Checks database connectivity
    pub async fn health_check(&self) -> Result<()> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Closes all connections in the pool
    pub async fn close(&self) {
        info!("Closing database connections");
        self.pool.close().await;
    }
}

/// Database connection options
#[derive(Debug, Clone)]
pub struct DatabaseOptions {
    /// Maximum number of connections in the pool
    pub max_connections: u32,

    /// Minimum number of connections to maintain
    pub min_connections: u32,

    /// Timeout for acquiring a connection from the pool
    pub acquire_timeout: Duration,

    /// Maximum idle time before a connection is closed
    pub idle_timeout: Option<Duration>,

    /// Maximum lifetime of a connection
    pub max_lifetime: Option<Duration>,
}

impl Default for DatabaseOptions {
    fn default() -> Self {
        Self {
            max_connections: 10,
            min_connections: 2,
            acquire_timeout: Duration::from_secs(30),
            idle_timeout: Some(Duration::from_secs(600)),
            max_lifetime: Some(Duration::from_secs(1800)),
        }
    }
}

impl DatabaseOptions {
    /// Creates options optimized for high-frequency trading
    pub fn high_performance() -> Self {
        Self {
            max_connections: 50,
            min_connections: 10,
            acquire_timeout: Duration::from_secs(5),
            idle_timeout: Some(Duration::from_secs(300)),
            max_lifetime: Some(Duration::from_secs(900)),
        }
    }

    /// Creates options for development/testing
    pub fn development() -> Self {
        Self {
            max_connections: 5,
            min_connections: 1,
            acquire_timeout: Duration::from_secs(30),
            idle_timeout: Some(Duration::from_secs(60)),
            max_lifetime: None,
        }
    }
}
