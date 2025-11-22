//! Real-time risk monitoring
//!
//! Continuously monitors portfolio risk metrics and triggers alerts/actions
//! when thresholds are breached.

use crate::{KillSwitch, KillSwitchTrigger, RiskLimits};
use dashmap::DashMap;
use inventory::PositionManager;
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, warn};

/// Real-time risk monitor
///
/// Tracks PnL, drawdown, and exposure in real-time.
#[derive(Clone)]
pub struct RealTimeMonitor {
    limits: RiskLimits,
    position_manager: Arc<PositionManager>,
    kill_switch: KillSwitch,
    state: Arc<RwLock<MonitorState>>,
    prices: Arc<DashMap<String, f64>>,
}

/// Internal monitoring state
#[derive(Debug)]
struct MonitorState {
    /// Session start timestamp
    session_start_ms: u64,

    /// Day start timestamp (midnight UTC)
    day_start_ms: u64,

    /// Starting equity for session (for drawdown calculation)
    session_start_equity: f64,

    /// Starting equity for day
    day_start_equity: f64,

    /// Peak equity during session (for drawdown calculation)
    peak_equity: f64,

    /// Current equity estimate
    current_equity: f64,

    /// Cumulative realized PnL this session
    session_realized_pnl: f64,

    /// Cumulative realized PnL today
    daily_realized_pnl: f64,

    /// Number of fills this session
    session_fills: u64,

    /// Total traded volume this session (USD)
    session_volume_usd: f64,

    /// Last update timestamp
    last_update_ms: u64,
}

impl MonitorState {
    fn new(starting_equity: f64) -> Self {
        let now = now_ms();
        Self {
            session_start_ms: now,
            day_start_ms: day_start_ms(now),
            session_start_equity: starting_equity,
            day_start_equity: starting_equity,
            peak_equity: starting_equity,
            current_equity: starting_equity,
            session_realized_pnl: 0.0,
            daily_realized_pnl: 0.0,
            session_fills: 0,
            session_volume_usd: 0.0,
            last_update_ms: now,
        }
    }
}

impl RealTimeMonitor {
    /// Creates a new real-time monitor
    pub fn new(
        limits: RiskLimits,
        position_manager: Arc<PositionManager>,
        kill_switch: KillSwitch,
    ) -> Self {
        Self {
            limits,
            position_manager,
            kill_switch,
            state: Arc::new(RwLock::new(MonitorState::new(0.0))),
            prices: Arc::new(DashMap::new()),
        }
    }

    /// Initializes the monitor with starting equity
    pub fn initialize(&self, starting_equity: f64) {
        let mut state = self.state.write();
        *state = MonitorState::new(starting_equity);
        info!(starting_equity, "Risk monitor initialized");
    }

    /// Updates the current price for a symbol
    pub fn update_price(&self, symbol: &str, price: f64) {
        self.prices.insert(symbol.to_string(), price);

        // Recalculate equity when prices change
        self.recalculate_equity();
    }

    /// Records a fill and updates PnL
    pub fn record_fill(&self, symbol: &str, qty: f64, price: f64, realized_pnl: f64) {
        let notional = qty.abs() * price;

        let mut state = self.state.write();
        state.session_fills += 1;
        state.session_volume_usd += notional;
        state.session_realized_pnl += realized_pnl;
        state.daily_realized_pnl += realized_pnl;

        // Update current equity based on realized PnL
        state.current_equity = state.session_start_equity + state.session_realized_pnl;

        // Update peak equity if we have a new high
        if state.current_equity > state.peak_equity {
            state.peak_equity = state.current_equity;
        }

        state.last_update_ms = now_ms();

        debug!(
            symbol,
            qty,
            price,
            realized_pnl,
            session_pnl = state.session_realized_pnl,
            current_equity = state.current_equity,
            peak_equity = state.peak_equity,
            "Fill recorded"
        );

        // Check loss limits after recording
        drop(state);
        self.check_loss_limits();
    }

    /// Recalculates current equity based on positions and prices
    fn recalculate_equity(&self) {
        let positions = self.position_manager.get_all_positions();
        let mut unrealized_pnl = 0.0;

        for (key, position) in positions {
            if let Some(price) = self.prices.get(&key.symbol) {
                let current_value = position.qty * *price;
                let entry_value = position.qty * position.entry_px;
                unrealized_pnl += current_value - entry_value;
            }
        }

        let mut state = self.state.write();
        let realized = state.session_realized_pnl;
        state.current_equity = state.session_start_equity + realized + unrealized_pnl;

        // Update peak equity
        if state.current_equity > state.peak_equity {
            state.peak_equity = state.current_equity;
        }

        state.last_update_ms = now_ms();
    }

    /// Checks if loss limits have been breached
    fn check_loss_limits(&self) {
        let state = self.state.read();

        // Check session loss
        if state.session_realized_pnl < 0.0
            && state.session_realized_pnl.abs() >= self.limits.max_session_loss_usd
        {
            drop(state);
            error!(
                loss = self.get_session_pnl(),
                max = self.limits.max_session_loss_usd,
                "SESSION LOSS LIMIT REACHED"
            );
            self.kill_switch.trigger_with_type(
                &format!("Session loss limit reached: ${:.2}", self.get_session_pnl().abs()),
                KillSwitchTrigger::MaxLoss,
            );
            return;
        }

        // Check daily loss
        if state.daily_realized_pnl < 0.0
            && state.daily_realized_pnl.abs() >= self.limits.max_daily_loss_usd
        {
            drop(state);
            error!(
                loss = self.get_daily_pnl(),
                max = self.limits.max_daily_loss_usd,
                "DAILY LOSS LIMIT REACHED"
            );
            self.kill_switch.trigger_with_type(
                &format!("Daily loss limit reached: ${:.2}", self.get_daily_pnl().abs()),
                KillSwitchTrigger::MaxLoss,
            );
            return;
        }

        // Check drawdown
        let drawdown_pct = self.calculate_drawdown(&state);
        if drawdown_pct >= self.limits.max_drawdown_pct {
            drop(state);
            error!(
                drawdown_pct,
                max = self.limits.max_drawdown_pct,
                "MAX DRAWDOWN REACHED"
            );
            self.kill_switch.trigger_with_type(
                &format!("Max drawdown reached: {:.2}%", drawdown_pct),
                KillSwitchTrigger::MaxDrawdown,
            );
        }
    }

    /// Calculates current drawdown percentage
    fn calculate_drawdown(&self, state: &MonitorState) -> f64 {
        if state.peak_equity <= 0.0 {
            return 0.0;
        }

        let drawdown = state.peak_equity - state.current_equity;
        (drawdown / state.peak_equity) * 100.0
    }

    /// Gets current drawdown percentage
    pub fn get_current_drawdown(&self) -> f64 {
        let state = self.state.read();
        self.calculate_drawdown(&state)
    }

    /// Gets session PnL (realized)
    pub fn get_session_pnl(&self) -> f64 {
        self.state.read().session_realized_pnl
    }

    /// Gets daily PnL (realized)
    pub fn get_daily_pnl(&self) -> f64 {
        self.state.read().daily_realized_pnl
    }

    /// Gets current equity estimate
    pub fn get_current_equity(&self) -> f64 {
        self.state.read().current_equity
    }

    /// Gets peak equity
    pub fn get_peak_equity(&self) -> f64 {
        self.state.read().peak_equity
    }

    /// Gets session statistics
    pub fn get_session_stats(&self) -> SessionStats {
        let state = self.state.read();
        SessionStats {
            session_start_ms: state.session_start_ms,
            session_fills: state.session_fills,
            session_volume_usd: state.session_volume_usd,
            session_realized_pnl: state.session_realized_pnl,
            daily_realized_pnl: state.daily_realized_pnl,
            current_equity: state.current_equity,
            peak_equity: state.peak_equity,
            current_drawdown_pct: self.calculate_drawdown(&state),
        }
    }

    /// Gets total exposure in USD across all positions
    pub fn get_total_exposure(&self) -> f64 {
        let positions = self.position_manager.get_all_positions();
        let mut total = 0.0;

        for (key, position) in positions {
            let price = self.prices.get(&key.symbol).map(|p| *p).unwrap_or(position.entry_px);
            total += position.qty.abs() * price;
        }

        total
    }

    /// Checks if total exposure exceeds limit
    pub fn check_exposure_limit(&self) -> Option<f64> {
        let exposure = self.get_total_exposure();
        if exposure > self.limits.max_total_exposure_usd {
            warn!(
                exposure,
                max = self.limits.max_total_exposure_usd,
                "Exposure limit exceeded"
            );
            Some(exposure)
        } else {
            None
        }
    }

    /// Resets daily counters (call at midnight UTC)
    pub fn reset_daily(&self) {
        let mut state = self.state.write();
        state.day_start_ms = now_ms();
        state.day_start_equity = state.current_equity;
        state.daily_realized_pnl = 0.0;
        info!("Daily risk counters reset");
    }
}

/// Session statistics snapshot
#[derive(Debug, Clone)]
pub struct SessionStats {
    pub session_start_ms: u64,
    pub session_fills: u64,
    pub session_volume_usd: f64,
    pub session_realized_pnl: f64,
    pub daily_realized_pnl: f64,
    pub current_equity: f64,
    pub peak_equity: f64,
    pub current_drawdown_pct: f64,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn day_start_ms(timestamp_ms: u64) -> u64 {
    // Round down to midnight UTC
    let ms_per_day = 24 * 60 * 60 * 1000;
    (timestamp_ms / ms_per_day) * ms_per_day
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_monitor_initialization() {
        let limits = RiskLimits::default();
        let pm = Arc::new(PositionManager::new());
        let ks = KillSwitch::new();
        let monitor = RealTimeMonitor::new(limits, pm, ks);

        monitor.initialize(10_000.0);

        assert_eq!(monitor.get_current_equity(), 10_000.0);
        assert_eq!(monitor.get_peak_equity(), 10_000.0);
        assert_eq!(monitor.get_session_pnl(), 0.0);
    }

    #[test]
    fn test_fill_recording() {
        let limits = RiskLimits::default();
        let pm = Arc::new(PositionManager::new());
        let ks = KillSwitch::new();
        let monitor = RealTimeMonitor::new(limits, pm, ks);

        monitor.initialize(10_000.0);
        monitor.record_fill("BTCUSD", 0.1, 50_000.0, 100.0);

        assert_eq!(monitor.get_session_pnl(), 100.0);

        let stats = monitor.get_session_stats();
        assert_eq!(stats.session_fills, 1);
        assert_eq!(stats.session_volume_usd, 5_000.0);
    }

    #[test]
    fn test_loss_limit_triggers_kill_switch() {
        let limits = RiskLimits::default().with_max_session_loss(100.0);
        let pm = Arc::new(PositionManager::new());
        let ks = KillSwitch::new();
        let monitor = RealTimeMonitor::new(limits, pm, ks.clone());

        monitor.initialize(10_000.0);

        // Record a losing trade
        monitor.record_fill("BTCUSD", 0.1, 50_000.0, -150.0);

        // Kill switch should be triggered
        assert!(ks.is_triggered());
        assert!(ks.get_reason().unwrap().contains("Session loss limit"));
    }

    #[test]
    fn test_drawdown_calculation() {
        let limits = RiskLimits::default();
        let pm = Arc::new(PositionManager::new());
        let ks = KillSwitch::new();
        let monitor = RealTimeMonitor::new(limits, pm, ks);

        monitor.initialize(10_000.0);

        // Simulate profit then loss
        monitor.record_fill("BTCUSD", 0.1, 50_000.0, 500.0); // Up to 10,500
        monitor.record_fill("BTCUSD", 0.1, 50_000.0, -300.0); // Down to 10,200

        let stats = monitor.get_session_stats();
        // Peak was 10,500, current is 10,200
        // Drawdown = (10500 - 10200) / 10500 * 100 = 2.86%
        assert!(stats.current_drawdown_pct > 2.0);
        assert!(stats.current_drawdown_pct < 3.0);
    }
}
