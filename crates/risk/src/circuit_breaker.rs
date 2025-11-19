use crate::config::CircuitBreakerConfig;
use crate::events::RiskEvent;
use crate::portfolio::{PortfolioManager, PortfolioSummary};
use chrono::Utc;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;

/// Circuit breaker state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitBreakerState {
    /// Normal operation - trading allowed
    Active,
    
    /// Warning level reached - still trading but at risk
    Warning,
    
    /// Circuit breaker triggered - trading halted
    Triggered,
    
    /// Circuit breaker disabled - no monitoring
    Disabled,
}

/// Circuit breaker that monitors portfolio losses and triggers shutdown
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    state: Arc<RwLock<CircuitBreakerState>>,
    portfolio: Arc<RwLock<PortfolioManager>>,
    event_tx: mpsc::UnboundedSender<RiskEvent>,
    warning_triggered: Arc<RwLock<bool>>,
}

impl CircuitBreaker {
    /// Create a new circuit breaker
    pub fn new(
        config: CircuitBreakerConfig,
        portfolio: Arc<RwLock<PortfolioManager>>,
    ) -> (Self, mpsc::UnboundedReceiver<RiskEvent>) {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        
        let breaker = Self {
            config,
            state: Arc::new(RwLock::new(CircuitBreakerState::Active)),
            portfolio,
            event_tx,
            warning_triggered: Arc::new(RwLock::new(false)),
        };
        
        (breaker, event_rx)
    }
    
    /// Check current portfolio status and update state
    pub fn check(&self) -> Result<CircuitBreakerState, String> {
        let portfolio = self.portfolio.read().map_err(|e| e.to_string())?;
        let summary = portfolio.get_summary();
        
        // Send portfolio update event
        let _ = self.event_tx.send(RiskEvent::PortfolioUpdate {
            timestamp: summary.timestamp,
            total_value: summary.current_value,
            initial_capital: summary.initial_capital,
            pnl: summary.total_pnl,
            pnl_percent: summary.pnl_percent,
        });
        
        let current_state = *self.state.read().map_err(|e| e.to_string())?;
        
        // Don't check if disabled or already triggered
        if current_state == CircuitBreakerState::Disabled 
            || current_state == CircuitBreakerState::Triggered {
            return Ok(current_state);
        }
        
        // Check loss threshold (negative PnL means loss)
        if summary.pnl_percent <= -self.config.loss_threshold_percent {
            self.trigger(summary)?;
            return Ok(CircuitBreakerState::Triggered);
        }
        
        // Check warning threshold
        if let Some(warning_threshold) = self.config.warning_threshold_percent {
            if summary.pnl_percent <= -warning_threshold {
                let mut warning_triggered = self.warning_triggered.write()
                    .map_err(|e| e.to_string())?;
                
                if !*warning_triggered {
                    *warning_triggered = true;
                    let _ = self.event_tx.send(RiskEvent::WarningTriggered {
                        timestamp: Utc::now(),
                        loss_percent: -summary.pnl_percent,
                        threshold_percent: warning_threshold,
                        current_value: summary.current_value,
                        initial_capital: summary.initial_capital,
                    });
                    
                    let mut state = self.state.write().map_err(|e| e.to_string())?;
                    *state = CircuitBreakerState::Warning;
                    return Ok(CircuitBreakerState::Warning);
                }
            } else {
                // Reset warning if we're back above threshold
                let mut warning_triggered = self.warning_triggered.write()
                    .map_err(|e| e.to_string())?;
                if *warning_triggered {
                    *warning_triggered = false;
                    let mut state = self.state.write().map_err(|e| e.to_string())?;
                    *state = CircuitBreakerState::Active;
                }
            }
        }
        
        Ok(current_state)
    }
    
    /// Trigger the circuit breaker
    fn trigger(&self, summary: PortfolioSummary) -> Result<(), String> {
        let mut state = self.state.write().map_err(|e| e.to_string())?;
        
        if *state == CircuitBreakerState::Triggered {
            return Ok(()); // Already triggered
        }
        
        *state = CircuitBreakerState::Triggered;
        
        let reason = format!(
            "Portfolio loss of {:.2}% exceeded threshold of {:.2}%",
            -summary.pnl_percent,
            self.config.loss_threshold_percent
        );
        
        let _ = self.event_tx.send(RiskEvent::CircuitBreakerTriggered {
            timestamp: Utc::now(),
            loss_percent: -summary.pnl_percent,
            threshold_percent: self.config.loss_threshold_percent,
            current_value: summary.current_value,
            initial_capital: summary.initial_capital,
            reason: reason.clone(),
        });
        
        log::error!("🔴 CIRCUIT BREAKER TRIGGERED: {}", reason);
        
        Ok(())
    }
    
    /// Get current state
    pub fn state(&self) -> Result<CircuitBreakerState, String> {
        self.state.read()
            .map(|s| *s)
            .map_err(|e| e.to_string())
    }
    
    /// Check if trading is allowed
    pub fn is_trading_allowed(&self) -> bool {
        matches!(
            self.state().unwrap_or(CircuitBreakerState::Triggered),
            CircuitBreakerState::Active | CircuitBreakerState::Warning
        )
    }
    
    /// Check if circuit breaker is triggered
    pub fn is_triggered(&self) -> bool {
        self.state().unwrap_or(CircuitBreakerState::Triggered) == CircuitBreakerState::Triggered
    }
    
    /// Reset the circuit breaker (manual intervention required)
    pub fn reset(&self) -> Result<(), String> {
        let mut state = self.state.write().map_err(|e| e.to_string())?;
        *state = CircuitBreakerState::Active;
        
        let mut warning_triggered = self.warning_triggered.write()
            .map_err(|e| e.to_string())?;
        *warning_triggered = false;
        
        let _ = self.event_tx.send(RiskEvent::CircuitBreakerReset {
            timestamp: Utc::now(),
        });
        
        log::info!("Circuit breaker reset - trading resumed");
        Ok(())
    }
    
    /// Disable the circuit breaker
    pub fn disable(&self) -> Result<(), String> {
        let mut state = self.state.write().map_err(|e| e.to_string())?;
        *state = CircuitBreakerState::Disabled;
        log::warn!("Circuit breaker disabled");
        Ok(())
    }
    
    /// Enable the circuit breaker
    pub fn enable(&self) -> Result<(), String> {
        let mut state = self.state.write().map_err(|e| e.to_string())?;
        *state = CircuitBreakerState::Active;
        log::info!("Circuit breaker enabled");
        Ok(())
    }
    
    /// Get configuration
    pub fn config(&self) -> &CircuitBreakerConfig {
        &self.config
    }
    
    /// Should auto-close positions when triggered
    pub fn should_auto_close_positions(&self) -> bool {
        self.config.auto_close_positions
    }
}
