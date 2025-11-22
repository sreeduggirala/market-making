//! Kill Switch - Emergency shutdown mechanism
//!
//! Provides a global circuit breaker that can halt all trading activity.
//! Can be triggered manually or automatically based on risk conditions.

use crate::KillSwitchTrigger;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{error, info, warn};

/// Kill switch state
#[derive(Debug)]
struct KillSwitchState {
    /// Reason for activation
    reason: Option<String>,

    /// Trigger type
    trigger: Option<KillSwitchTrigger>,

    /// Timestamp when triggered
    triggered_at_ms: Option<u64>,

    /// Number of times triggered in this session
    trigger_count: u32,

    /// Actions to take when triggered
    actions: KillSwitchActions,
}

/// Actions to take when kill switch is triggered
#[derive(Debug, Clone)]
pub struct KillSwitchActions {
    /// Cancel all open orders
    pub cancel_all_orders: bool,

    /// Flatten all positions (market orders to close)
    pub flatten_positions: bool,

    /// Disable new order submission
    pub disable_new_orders: bool,

    /// Disable order modifications
    pub disable_modifications: bool,
}

impl Default for KillSwitchActions {
    fn default() -> Self {
        Self {
            cancel_all_orders: true,
            flatten_positions: false, // Dangerous - requires explicit enable
            disable_new_orders: true,
            disable_modifications: true,
        }
    }
}

impl KillSwitchActions {
    /// Conservative actions - just stop trading
    pub fn conservative() -> Self {
        Self {
            cancel_all_orders: true,
            flatten_positions: false,
            disable_new_orders: true,
            disable_modifications: false, // Allow cancels
        }
    }

    /// Aggressive actions - full shutdown and flatten
    pub fn aggressive() -> Self {
        Self {
            cancel_all_orders: true,
            flatten_positions: true,
            disable_new_orders: true,
            disable_modifications: true,
        }
    }
}

/// Kill switch for emergency trading halt
///
/// Thread-safe and designed to be shared across the trading system.
#[derive(Clone)]
pub struct KillSwitch {
    /// Fast atomic check for triggered state
    triggered: Arc<AtomicBool>,

    /// Detailed state (protected by RwLock for rare writes)
    state: Arc<RwLock<KillSwitchState>>,
}

impl KillSwitch {
    /// Creates a new kill switch (not triggered)
    pub fn new() -> Self {
        Self {
            triggered: Arc::new(AtomicBool::new(false)),
            state: Arc::new(RwLock::new(KillSwitchState {
                reason: None,
                trigger: None,
                triggered_at_ms: None,
                trigger_count: 0,
                actions: KillSwitchActions::default(),
            })),
        }
    }

    /// Creates a kill switch with custom actions
    pub fn with_actions(actions: KillSwitchActions) -> Self {
        Self {
            triggered: Arc::new(AtomicBool::new(false)),
            state: Arc::new(RwLock::new(KillSwitchState {
                reason: None,
                trigger: None,
                triggered_at_ms: None,
                trigger_count: 0,
                actions,
            })),
        }
    }

    /// Fast check if kill switch is triggered
    ///
    /// This is optimized for the hot path (checking before every order).
    #[inline]
    pub fn is_triggered(&self) -> bool {
        self.triggered.load(Ordering::Acquire)
    }

    /// Triggers the kill switch with a reason
    pub fn trigger(&self, reason: &str) {
        self.trigger_with_type(reason, KillSwitchTrigger::Manual);
    }

    /// Triggers the kill switch with a specific trigger type
    pub fn trigger_with_type(&self, reason: &str, trigger: KillSwitchTrigger) {
        // Set atomic flag first for fast path
        let was_triggered = self.triggered.swap(true, Ordering::AcqRel);

        if !was_triggered {
            error!(
                reason = %reason,
                trigger = %trigger,
                "KILL SWITCH TRIGGERED"
            );
        } else {
            warn!(
                reason = %reason,
                trigger = %trigger,
                "Kill switch re-triggered (already active)"
            );
        }

        // Update detailed state
        let mut state = self.state.write();
        state.reason = Some(reason.to_string());
        state.trigger = Some(trigger);
        state.triggered_at_ms = Some(now_ms());
        state.trigger_count += 1;
    }

    /// Resets the kill switch (requires explicit action)
    ///
    /// # Safety
    ///
    /// This should only be called after the operator has reviewed and
    /// addressed the cause of the trigger.
    pub fn reset(&self) {
        let was_triggered = self.triggered.swap(false, Ordering::AcqRel);

        if was_triggered {
            info!("Kill switch RESET by operator");

            let mut state = self.state.write();
            let old_reason = state.reason.take();
            state.trigger = None;
            state.triggered_at_ms = None;

            if let Some(reason) = old_reason {
                info!(previous_reason = %reason, "Previous trigger reason cleared");
            }
        }
    }

    /// Gets the reason for the trigger (if any)
    pub fn get_reason(&self) -> Option<String> {
        self.state.read().reason.clone()
    }

    /// Gets the trigger type (if any)
    pub fn get_trigger_type(&self) -> Option<KillSwitchTrigger> {
        self.state.read().trigger
    }

    /// Gets when the kill switch was triggered
    pub fn triggered_at(&self) -> Option<u64> {
        self.state.read().triggered_at_ms
    }

    /// Gets the total number of triggers in this session
    pub fn trigger_count(&self) -> u32 {
        self.state.read().trigger_count
    }

    /// Gets the configured actions
    pub fn get_actions(&self) -> KillSwitchActions {
        self.state.read().actions.clone()
    }

    /// Checks if orders should be cancelled
    pub fn should_cancel_orders(&self) -> bool {
        self.is_triggered() && self.state.read().actions.cancel_all_orders
    }

    /// Checks if positions should be flattened
    pub fn should_flatten(&self) -> bool {
        self.is_triggered() && self.state.read().actions.flatten_positions
    }

    /// Checks if new orders are disabled
    pub fn orders_disabled(&self) -> bool {
        self.is_triggered() && self.state.read().actions.disable_new_orders
    }

    /// Gets a summary of the kill switch state
    pub fn get_status(&self) -> KillSwitchStatus {
        let state = self.state.read();
        KillSwitchStatus {
            triggered: self.triggered.load(Ordering::Acquire),
            reason: state.reason.clone(),
            trigger: state.trigger,
            triggered_at_ms: state.triggered_at_ms,
            trigger_count: state.trigger_count,
            actions: state.actions.clone(),
        }
    }
}

impl Default for KillSwitch {
    fn default() -> Self {
        Self::new()
    }
}

/// Kill switch status snapshot
#[derive(Debug, Clone)]
pub struct KillSwitchStatus {
    pub triggered: bool,
    pub reason: Option<String>,
    pub trigger: Option<KillSwitchTrigger>,
    pub triggered_at_ms: Option<u64>,
    pub trigger_count: u32,
    pub actions: KillSwitchActions,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kill_switch_default_not_triggered() {
        let ks = KillSwitch::new();
        assert!(!ks.is_triggered());
        assert!(ks.get_reason().is_none());
    }

    #[test]
    fn test_kill_switch_trigger() {
        let ks = KillSwitch::new();
        ks.trigger("Test reason");

        assert!(ks.is_triggered());
        assert_eq!(ks.get_reason(), Some("Test reason".to_string()));
        assert_eq!(ks.trigger_count(), 1);
    }

    #[test]
    fn test_kill_switch_reset() {
        let ks = KillSwitch::new();
        ks.trigger("Test reason");
        assert!(ks.is_triggered());

        ks.reset();
        assert!(!ks.is_triggered());
        assert!(ks.get_reason().is_none());
        // Trigger count should persist
        assert_eq!(ks.trigger_count(), 1);
    }

    #[test]
    fn test_kill_switch_multiple_triggers() {
        let ks = KillSwitch::new();
        ks.trigger("First");
        ks.trigger("Second");
        ks.trigger("Third");

        assert_eq!(ks.trigger_count(), 3);
        assert_eq!(ks.get_reason(), Some("Third".to_string()));
    }

    #[test]
    fn test_kill_switch_trigger_types() {
        let ks = KillSwitch::new();
        ks.trigger_with_type("Max loss reached", KillSwitchTrigger::MaxLoss);

        assert!(ks.is_triggered());
        assert_eq!(ks.get_trigger_type(), Some(KillSwitchTrigger::MaxLoss));
    }

    #[test]
    fn test_kill_switch_clone() {
        let ks1 = KillSwitch::new();
        let ks2 = ks1.clone();

        ks1.trigger("Test");

        // Clone should share state
        assert!(ks2.is_triggered());
        assert_eq!(ks2.get_reason(), Some("Test".to_string()));
    }

    #[test]
    fn test_kill_switch_actions() {
        let actions = KillSwitchActions::aggressive();
        let ks = KillSwitch::with_actions(actions);

        ks.trigger("Test");

        assert!(ks.should_cancel_orders());
        assert!(ks.should_flatten());
        assert!(ks.orders_disabled());
    }
}
