use serde::{Deserialize, Serialize};

/// Configuration for the circuit breaker system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    /// Loss threshold as a percentage (e.g., 5.0 for 5%)
    pub loss_threshold_percent: f64,
    
    /// Initial capital in reference currency
    pub initial_capital: f64,
    
    /// Reference currency for portfolio valuation (e.g., "USDT", "USD")
    pub reference_currency: String,
    
    /// Warning threshold as a percentage (optional)
    pub warning_threshold_percent: Option<f64>,
    
    /// Whether to automatically close positions when triggered
    pub auto_close_positions: bool,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            loss_threshold_percent: 5.0,
            initial_capital: 0.0,
            reference_currency: "USDT".to_string(),
            warning_threshold_percent: Some(3.0),
            auto_close_positions: false,
        }
    }
}

impl CircuitBreakerConfig {
    pub fn new(initial_capital: f64, reference_currency: impl Into<String>) -> Self {
        Self {
            initial_capital,
            reference_currency: reference_currency.into(),
            ..Default::default()
        }
    }
    
    pub fn with_threshold(mut self, threshold_percent: f64) -> Self {
        self.loss_threshold_percent = threshold_percent;
        self
    }
    
    pub fn with_warning(mut self, warning_percent: f64) -> Self {
        self.warning_threshold_percent = Some(warning_percent);
        self
    }
    
    pub fn with_auto_close(mut self, auto_close: bool) -> Self {
        self.auto_close_positions = auto_close;
        self
    }
}
