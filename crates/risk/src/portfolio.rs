use adapters::traits::{Balance, Position};
use std::collections::HashMap;
use chrono::Utc;

/// Manages portfolio state across multiple venues and asset types
#[derive(Debug, Clone)]
pub struct PortfolioManager {
    /// Reference currency for valuation (e.g., "USDT")
    reference_currency: String,
    
    /// Current balances across all venues (asset -> total balance)
    balances: HashMap<String, f64>,
    
    /// Current positions across all venues (symbol -> position)
    positions: HashMap<String, PositionSnapshot>,
    
    /// Cached prices for asset conversion (asset -> price in reference currency)
    prices: HashMap<String, f64>,
    
    /// Initial capital in reference currency
    initial_capital: f64,
}

#[derive(Debug, Clone)]
pub struct PositionSnapshot {
    pub symbol: String,
    pub qty: f64,
    pub entry_px: f64,
    pub mark_px: f64,
    pub unrealized_pnl: f64,
    pub realized_pnl: f64,
}

impl PortfolioManager {
    pub fn new(initial_capital: f64, reference_currency: impl Into<String>) -> Self {
        Self {
            reference_currency: reference_currency.into(),
            balances: HashMap::new(),
            positions: HashMap::new(),
            prices: HashMap::new(),
            initial_capital,
        }
    }
    
    /// Update balance for an asset
    pub fn update_balance(&mut self, asset: &str, total: f64) {
        self.balances.insert(asset.to_string(), total);
    }
    
    /// Update balances from account info
    pub fn update_balances(&mut self, balances: &[Balance]) {
        for balance in balances {
            self.balances.insert(balance.asset.clone(), balance.total);
        }
    }
    
    /// Update a position
    pub fn update_position(&mut self, position: &Position) {
        let snapshot = PositionSnapshot {
            symbol: position.symbol.clone(),
            qty: position.qty,
            entry_px: position.entry_px,
            mark_px: position.mark_px.unwrap_or(position.entry_px),
            unrealized_pnl: position.unrealized_pnl.unwrap_or(0.0),
            realized_pnl: position.realized_pnl.unwrap_or(0.0),
        };
        self.positions.insert(position.symbol.clone(), snapshot);
    }
    
    /// Update positions from list
    pub fn update_positions(&mut self, positions: &[Position]) {
        for position in positions {
            self.update_position(position);
        }
    }
    
    /// Update price for an asset in reference currency
    pub fn update_price(&mut self, asset: &str, price: f64) {
        self.prices.insert(asset.to_string(), price);
    }
    
    /// Update multiple prices
    pub fn update_prices(&mut self, prices: HashMap<String, f64>) {
        self.prices.extend(prices);
    }
    
    /// Calculate total portfolio value in reference currency
    pub fn calculate_portfolio_value(&self) -> f64 {
        let mut total_value = 0.0;
        
        // Add balance values converted to reference currency
        for (asset, balance) in &self.balances {
            if asset == &self.reference_currency {
                total_value += balance;
            } else if let Some(&price) = self.prices.get(asset) {
                total_value += balance * price;
            } else {
                log::warn!("No price available for asset: {}", asset);
            }
        }
        
        // Add unrealized PnL from positions (already in reference currency)
        for position in self.positions.values() {
            total_value += position.unrealized_pnl;
        }
        
        total_value
    }
    
    /// Calculate total realized PnL
    pub fn calculate_realized_pnl(&self) -> f64 {
        self.positions.values().map(|p| p.realized_pnl).sum()
    }
    
    /// Calculate total unrealized PnL
    pub fn calculate_unrealized_pnl(&self) -> f64 {
        self.positions.values().map(|p| p.unrealized_pnl).sum()
    }
    
    /// Calculate total PnL (realized + unrealized)
    pub fn calculate_total_pnl(&self) -> f64 {
        let portfolio_value = self.calculate_portfolio_value();
        portfolio_value - self.initial_capital
    }
    
    /// Calculate PnL as a percentage of initial capital
    pub fn calculate_pnl_percent(&self) -> f64 {
        if self.initial_capital == 0.0 {
            return 0.0;
        }
        (self.calculate_total_pnl() / self.initial_capital) * 100.0
    }
    
    /// Get current portfolio summary
    pub fn get_summary(&self) -> PortfolioSummary {
        let portfolio_value = self.calculate_portfolio_value();
        let total_pnl = portfolio_value - self.initial_capital;
        let pnl_percent = if self.initial_capital > 0.0 {
            (total_pnl / self.initial_capital) * 100.0
        } else {
            0.0
        };
        
        PortfolioSummary {
            timestamp: Utc::now(),
            initial_capital: self.initial_capital,
            current_value: portfolio_value,
            total_pnl,
            pnl_percent,
            realized_pnl: self.calculate_realized_pnl(),
            unrealized_pnl: self.calculate_unrealized_pnl(),
            num_positions: self.positions.len(),
            num_assets: self.balances.len(),
        }
    }
    
    /// Get initial capital
    pub fn initial_capital(&self) -> f64 {
        self.initial_capital
    }
    
    /// Get reference currency
    pub fn reference_currency(&self) -> &str {
        &self.reference_currency
    }
    
    /// Get all balances
    pub fn balances(&self) -> &HashMap<String, f64> {
        &self.balances
    }
    
    /// Get all positions
    pub fn positions(&self) -> &HashMap<String, PositionSnapshot> {
        &self.positions
    }
}

#[derive(Debug, Clone)]
pub struct PortfolioSummary {
    pub timestamp: chrono::DateTime<Utc>,
    pub initial_capital: f64,
    pub current_value: f64,
    pub total_pnl: f64,
    pub pnl_percent: f64,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub num_positions: usize,
    pub num_assets: usize,
}
