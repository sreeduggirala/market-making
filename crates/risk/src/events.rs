use chrono::{DateTime, Utc};

/// Events emitted by the risk management system
#[derive(Debug, Clone)]
pub enum RiskEvent {
    /// Portfolio value has been updated
    PortfolioUpdate {
        timestamp: DateTime<Utc>,
        total_value: f64,
        initial_capital: f64,
        pnl: f64,
        pnl_percent: f64,
    },
    
    /// Warning threshold has been breached
    WarningTriggered {
        timestamp: DateTime<Utc>,
        loss_percent: f64,
        threshold_percent: f64,
        current_value: f64,
        initial_capital: f64,
    },
    
    /// Circuit breaker has been triggered
    CircuitBreakerTriggered {
        timestamp: DateTime<Utc>,
        loss_percent: f64,
        threshold_percent: f64,
        current_value: f64,
        initial_capital: f64,
        reason: String,
    },
    
    /// Circuit breaker has been reset
    CircuitBreakerReset {
        timestamp: DateTime<Utc>,
    },
    
    /// System shutdown initiated
    SystemShutdown {
        timestamp: DateTime<Utc>,
        reason: String,
    },
}

impl RiskEvent {
    pub fn timestamp(&self) -> DateTime<Utc> {
        match self {
            RiskEvent::PortfolioUpdate { timestamp, .. } => *timestamp,
            RiskEvent::WarningTriggered { timestamp, .. } => *timestamp,
            RiskEvent::CircuitBreakerTriggered { timestamp, .. } => *timestamp,
            RiskEvent::CircuitBreakerReset { timestamp } => *timestamp,
            RiskEvent::SystemShutdown { timestamp, .. } => *timestamp,
        }
    }
}
