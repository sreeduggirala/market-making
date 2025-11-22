//! Database schema constants and SQL queries

/// Table names
pub mod tables {
    pub const ORDERS: &str = "orders";
    pub const FILLS: &str = "fills";
    pub const POSITIONS: &str = "positions";
    pub const POSITION_HISTORY: &str = "position_history";
    pub const PNL_DAILY: &str = "pnl_daily";
    pub const RISK_EVENTS: &str = "risk_events";
    pub const KILL_SWITCH_EVENTS: &str = "kill_switch_events";
    pub const STRATEGY_SESSIONS: &str = "strategy_sessions";
}
