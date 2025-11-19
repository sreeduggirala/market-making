use anyhow::Result;
use log::{info, warn, error};
use risk::{CircuitBreaker, CircuitBreakerConfig, CircuitBreakerState, PortfolioManager, RiskEvent};
use oms::OrderManager;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::time::sleep;

/// Demo: Circuit breaker that kills the system when portfolio loss exceeds 5%
#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logger
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .init();
    
    info!("🚀 Circuit Breaker Demo Starting");
    info!("============================================================");
    
    // Step 1: Configure initial capital
    let initial_capital = 10_000.0; // $10,000 USDT
    info!("💰 Initial Capital: ${:.2} USDT", initial_capital);
    
    // Step 2: Create portfolio manager
    let portfolio = Arc::new(RwLock::new(
        PortfolioManager::new(initial_capital, "USDT")
    ));
    
    // Step 3: Configure circuit breaker with 5% threshold
    let config = CircuitBreakerConfig::new(initial_capital, "USDT")
        .with_threshold(5.0)  // 5% loss threshold
        .with_warning(3.0)    // 3% warning threshold
        .with_auto_close(false); // Don't auto-close positions in demo
    
    info!("⚙️  Circuit Breaker Config:");
    info!("   - Loss Threshold: {:.1}%", config.loss_threshold_percent);
    info!("   - Warning Threshold: {:.1}%", config.warning_threshold_percent.unwrap_or(0.0));
    info!("   - Auto-close Positions: {}", config.auto_close_positions);
    
    // Step 4: Create circuit breaker
    let (circuit_breaker, mut event_rx) = CircuitBreaker::new(config, portfolio.clone());
    let circuit_breaker = Arc::new(circuit_breaker);
    
    // Step 5: Create order manager (for emergency cancellation)
    let order_manager = Arc::new(OrderManager::new());
    info!("📋 Order Manager: Initialized (no adapters in demo mode)");
    
    // Step 6: Spawn event listener
    let cb_clone = circuit_breaker.clone();
    let om_clone = order_manager.clone();
    tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            handle_risk_event(event, &cb_clone, &om_clone).await;
        }
    });
    
    info!("============================================================");
    info!("📊 Starting Portfolio Simulation");
    info!("============================================================");
    
    // Step 7: Simulate portfolio with initial balance
    {
        let mut port = portfolio.write().unwrap();
        port.update_balance("USDT", initial_capital);
        port.update_price("USDT", 1.0);
    }
    
    // Initial check
    if let Err(e) = circuit_breaker.check() {
        error!("Error checking circuit breaker: {}", e);
    }
    sleep(Duration::from_secs(2)).await;
    
    // Initial check
    if let Err(e) = circuit_breaker.check() {
        error!("Error checking circuit breaker: {}", e);
    }
    sleep(Duration::from_secs(2)).await;
    
    // Step 8: Simulate trading losses
    info!("\n💹 Simulating Trading Scenario...\n");
    
    // Loss 1: -2% (still safe)
    simulate_loss(&portfolio, initial_capital, -2.0, "Bad trade on BTC/USDT").await;
    if let Err(e) = circuit_breaker.check() {
        error!("Error checking circuit breaker: {}", e);
    }
    sleep(Duration::from_secs(2)).await;
    
    // Loss 2: -3.5% total (warning threshold)
    simulate_loss(&portfolio, initial_capital, -3.5, "Stop loss hit on ETH/USDT").await;
    if let Err(e) = circuit_breaker.check() {
        error!("Error checking circuit breaker: {}", e);
    }
    sleep(Duration::from_secs(2)).await;
    
    // Loss 3: -4.2% total (approaching danger)
    simulate_loss(&portfolio, initial_capital, -4.2, "Liquidation on leveraged position").await;
    if let Err(e) = circuit_breaker.check() {
        error!("Error checking circuit breaker: {}", e);
    }
    sleep(Duration::from_secs(2)).await;
    
    // Loss 4: -5.5% total (CIRCUIT BREAKER TRIGGERED!)
    simulate_loss(&portfolio, initial_capital, -5.5, "Market crash - cascade liquidation").await;
    if let Err(e) = circuit_breaker.check() {
        error!("Error checking circuit breaker: {}", e);
    }
    
    // Loss 1: -2% (still safe)
    simulate_loss(&portfolio, initial_capital, -2.0, "Bad trade on BTC/USDT").await;
    if let Err(e) = circuit_breaker.check() {
        error!("Error checking circuit breaker: {}", e);
    }
    sleep(Duration::from_secs(2)).await;
    
    // Loss 2: -3.5% total (warning threshold)
    simulate_loss(&portfolio, initial_capital, -3.5, "Stop loss hit on ETH/USDT").await;
    if let Err(e) = circuit_breaker.check() {
        error!("Error checking circuit breaker: {}", e);
    }
    sleep(Duration::from_secs(2)).await;
    
    // Loss 3: -4.2% total (approaching danger)
    simulate_loss(&portfolio, initial_capital, -4.2, "Liquidation on leveraged position").await;
    if let Err(e) = circuit_breaker.check() {
        error!("Error checking circuit breaker: {}", e);
    }
    sleep(Duration::from_secs(2)).await;
    
    // Loss 4: -5.5% total (CIRCUIT BREAKER TRIGGERED!)
    simulate_loss(&portfolio, initial_capital, -5.5, "Market crash - cascade liquidation").await;
    if let Err(e) = circuit_breaker.check() {
        error!("Error checking circuit breaker: {}", e);
    }
    
    // Step 9: Check if triggered
    if circuit_breaker.is_triggered() {
        error!("\n🔴 CIRCUIT BREAKER TRIGGERED - SYSTEM SHUTDOWN INITIATED");
        error!("============================================================");
        
        // Cancel all orders
        info!("🛑 Cancelling all open orders...");
        order_manager.cancel_all_orders().await?;
        
        // Log final state
        let summary = portfolio.read().unwrap().get_summary();
        error!("📊 Final Portfolio State:");
        error!("   - Initial Capital: ${:.2}", summary.initial_capital);
        error!("   - Current Value: ${:.2}", summary.current_value);
        error!("   - Total Loss: ${:.2}", summary.total_pnl);
        error!("   - Loss Percentage: {:.2}%", summary.pnl_percent);
        
        error!("============================================================");
        error!("💀 SYSTEM TERMINATED - Capital preservation activated");
        
        // Exit with error code
        std::process::exit(1);
    }
    
    Ok(())
}

/// Simulate a portfolio loss
async fn simulate_loss(
    portfolio: &Arc<RwLock<PortfolioManager>>,
    initial_capital: f64,
    loss_percent: f64,
    reason: &str,
) {
    let new_value = initial_capital * (1.0 + loss_percent / 100.0);
    
    info!("📉 Loss Event: {}", reason);
    info!("   Loss: {:.2}% | New Portfolio Value: ${:.2}", loss_percent, new_value);
    
    let mut port = portfolio.write().unwrap();
    port.update_balance("USDT", new_value);
    
    sleep(Duration::from_millis(500)).await;
}

/// Handle risk events
async fn handle_risk_event(
    event: RiskEvent,
    circuit_breaker: &CircuitBreaker,
    order_manager: &OrderManager,
) {
    match event {
        RiskEvent::PortfolioUpdate { 
            total_value, 
            pnl, 
            pnl_percent, 
            .. 
        } => {
            let status_icon = if pnl_percent > 0.0 {
                "✅"
            } else if pnl_percent > -3.0 {
                "⚠️ "
            } else if pnl_percent > -5.0 {
                "🟠"
            } else {
                "🔴"
            };
            
            info!(
                "{} Portfolio: ${:.2} | P&L: ${:.2} ({:.2}%)",
                status_icon, total_value, pnl, pnl_percent
            );
        }
        
        RiskEvent::WarningTriggered {
            loss_percent,
            threshold_percent,
            current_value,
            ..
        } => {
            warn!("\n⚠️  WARNING: Loss threshold reached!");
            warn!("   Current Loss: {:.2}%", loss_percent);
            warn!("   Warning Threshold: {:.2}%", threshold_percent);
            warn!("   Current Value: ${:.2}", current_value);
            warn!("   APPROACHING CIRCUIT BREAKER LIMIT!\n");
        }
        
        RiskEvent::CircuitBreakerTriggered {
            loss_percent,
            threshold_percent,
            current_value,
            initial_capital,
            reason,
            ..
        } => {
            error!("\n🚨 CIRCUIT BREAKER TRIGGERED 🚨");
            error!("============================================================");
            error!("Reason: {}", reason);
            error!("Current Loss: {:.2}% (Threshold: {:.2}%)", loss_percent, threshold_percent);
            error!("Portfolio Value: ${:.2} → ${:.2}", initial_capital, current_value);
            error!("Total Loss: ${:.2}", initial_capital - current_value);
            error!("============================================================");
        }
        
        RiskEvent::CircuitBreakerReset { .. } => {
            info!("🔄 Circuit breaker reset - trading resumed");
        }
        
        RiskEvent::SystemShutdown { reason, .. } => {
            error!("💀 System shutdown: {}", reason);
        }
    }
}
