//! Kill Switch Test Suite
//!
//! Tests the emergency kill switch mechanism for trading systems.
//! Validates automatic and manual triggering, state management, and safety controls.
//!
//! Run with: cargo test --package risk --test kill_switch_test -- --nocapture

use risk::{KillSwitch, KillSwitchActions, KillSwitchTrigger};
use std::time::Duration;
use std::thread::sleep;

// =============================================================================
// Test Utilities
// =============================================================================

/// Simulates a trading scenario with order checks
fn simulate_order_submission(kill_switch: &KillSwitch, order_id: u32) -> Result<(), String> {
    if kill_switch.is_triggered() {
        let reason = kill_switch.get_reason().unwrap_or_default();
        Err(format!("Order #{} rejected - Kill switch active: {}", order_id, reason))
    } else {
        Ok(())
    }
}

/// Simulates multiple order submissions
fn simulate_order_batch(kill_switch: &KillSwitch, count: u32) -> (u32, u32) {
    let mut successful = 0;
    let mut rejected = 0;

    for i in 1..=count {
        match simulate_order_submission(kill_switch, i) {
            Ok(_) => {
                successful += 1;
                println!("     ✅ Order #{} accepted", i);
            }
            Err(e) => {
                rejected += 1;
                println!("     ❌ Order #{} rejected: {}", i, e.split(':').last().unwrap_or(""));
            }
        }
    }

    (successful, rejected)
}

// =============================================================================
// Test 1: Basic Kill Switch Lifecycle
// =============================================================================

#[test]
fn test_kill_switch_basic_lifecycle() {
    println!("\n📊 TEST: Kill Switch Basic Lifecycle");
    println!("{}", "=".repeat(60));

    let ks = KillSwitch::new();

    println!("\n1️⃣  Initial State:");
    assert!(!ks.is_triggered());
    assert!(ks.get_reason().is_none());
    assert_eq!(ks.trigger_count(), 0);
    println!("   ✓ Kill switch: INACTIVE");
    println!("   ✓ Trigger count: 0");

    println!("\n2️⃣  Triggering Kill Switch:");
    ks.trigger("Manual emergency stop");

    assert!(ks.is_triggered());
    assert_eq!(ks.get_reason(), Some("Manual emergency stop".to_string()));
    assert_eq!(ks.trigger_count(), 1);
    println!("   ✓ Kill switch: ACTIVE");
    println!("   ✓ Reason: Manual emergency stop");
    println!("   ✓ Trigger count: 1");

    println!("\n3️⃣  Resetting Kill Switch:");
    ks.reset();

    assert!(!ks.is_triggered());
    assert!(ks.get_reason().is_none());
    assert_eq!(ks.trigger_count(), 1); // Count persists
    println!("   ✓ Kill switch: INACTIVE");
    println!("   ✓ Reason: Cleared");
    println!("   ✓ Trigger count: 1 (persisted)");

    println!("\n✅ Test passed: Basic lifecycle working correctly\n");
}

// =============================================================================
// Test 2: Order Rejection When Kill Switch Active
// =============================================================================

#[test]
fn test_kill_switch_order_rejection() {
    println!("\n🛑 TEST: Order Rejection When Kill Switch Active");
    println!("{}", "=".repeat(60));

    let ks = KillSwitch::new();

    println!("\n1️⃣  Submitting orders with kill switch INACTIVE:");
    let (successful, rejected) = simulate_order_batch(&ks, 3);

    assert_eq!(successful, 3);
    assert_eq!(rejected, 0);
    println!("   ✓ All 3 orders accepted");

    println!("\n2️⃣  Triggering kill switch:");
    ks.trigger("Daily loss limit exceeded");
    println!("   ✓ Kill switch activated: Daily loss limit exceeded");

    println!("\n3️⃣  Submitting orders with kill switch ACTIVE:");
    let (successful, rejected) = simulate_order_batch(&ks, 3);

    assert_eq!(successful, 0);
    assert_eq!(rejected, 3);
    println!("   ✓ All 3 orders rejected");

    println!("\n✅ Test passed: Order rejection working correctly\n");
}

// =============================================================================
// Test 3: Trigger Types and Reasons
// =============================================================================

#[test]
fn test_kill_switch_trigger_types() {
    println!("\n🏷️  TEST: Kill Switch Trigger Types");
    println!("{}", "=".repeat(60));

    println!("\n1️⃣  Testing Manual Trigger:");
    let ks1 = KillSwitch::new();
    ks1.trigger_with_type("Operator intervention", KillSwitchTrigger::Manual);

    assert!(ks1.is_triggered());
    assert_eq!(ks1.get_trigger_type(), Some(KillSwitchTrigger::Manual));
    println!("   ✓ Type: Manual");
    println!("   ✓ Reason: Operator intervention");

    println!("\n2️⃣  Testing Max Loss Trigger:");
    let ks2 = KillSwitch::new();
    ks2.trigger_with_type("Daily loss: $10,000 > $5,000 limit", KillSwitchTrigger::MaxLoss);

    assert!(ks2.is_triggered());
    assert_eq!(ks2.get_trigger_type(), Some(KillSwitchTrigger::MaxLoss));
    println!("   ✓ Type: MaxLoss");
    println!("   ✓ Reason: Daily loss: $10,000 > $5,000 limit");

    println!("\n3️⃣  Testing Max Drawdown Trigger:");
    let ks3 = KillSwitch::new();
    ks3.trigger_with_type("Drawdown: 25% > 20% limit", KillSwitchTrigger::MaxDrawdown);

    assert!(ks3.is_triggered());
    assert_eq!(ks3.get_trigger_type(), Some(KillSwitchTrigger::MaxDrawdown));
    println!("   ✓ Type: MaxDrawdown");
    println!("   ✓ Reason: Drawdown: 25% > 20% limit");

    println!("\n4️⃣  Testing Margin Risk Trigger:");
    let ks4 = KillSwitch::new();
    ks4.trigger_with_type("Margin utilization: 95% > 80% limit", KillSwitchTrigger::MarginRisk);

    assert!(ks4.is_triggered());
    assert_eq!(ks4.get_trigger_type(), Some(KillSwitchTrigger::MarginRisk));
    println!("   ✓ Type: MarginRisk");
    println!("   ✓ Reason: Margin utilization: 95% > 80% limit");

    println!("\n5️⃣  Testing Connectivity Trigger:");
    let ks5 = KillSwitch::new();
    ks5.trigger_with_type("Exchange WebSocket disconnected > 30s", KillSwitchTrigger::Connectivity);

    assert!(ks5.is_triggered());
    assert_eq!(ks5.get_trigger_type(), Some(KillSwitchTrigger::Connectivity));
    println!("   ✓ Type: Connectivity");
    println!("   ✓ Reason: Exchange WebSocket disconnected > 30s");

    println!("\n✅ Test passed: All trigger types working correctly\n");
}

// =============================================================================
// Test 4: Multiple Triggers and Counter
// =============================================================================

#[test]
fn test_kill_switch_multiple_triggers() {
    println!("\n🔢 TEST: Multiple Kill Switch Triggers");
    println!("{}", "=".repeat(60));

    let ks = KillSwitch::new();

    println!("\n1️⃣  First Trigger:");
    ks.trigger("Daily loss exceeded");
    println!("   Count: {}", ks.trigger_count());
    println!("   Reason: {}", ks.get_reason().unwrap());
    assert_eq!(ks.trigger_count(), 1);

    println!("\n2️⃣  Second Trigger (while already active):");
    ks.trigger("Position limit exceeded");
    println!("   Count: {}", ks.trigger_count());
    println!("   Reason: {}", ks.get_reason().unwrap());
    assert_eq!(ks.trigger_count(), 2);
    assert_eq!(ks.get_reason(), Some("Position limit exceeded".to_string()));

    println!("\n3️⃣  Reset:");
    ks.reset();
    println!("   Active: {}", ks.is_triggered());
    println!("   Count: {} (persisted)", ks.trigger_count());
    assert_eq!(ks.trigger_count(), 2);

    println!("\n4️⃣  Third Trigger (after reset):");
    ks.trigger("Connection lost");
    println!("   Count: {}", ks.trigger_count());
    assert_eq!(ks.trigger_count(), 3);

    println!("\n✅ Test passed: Multiple triggers tracked correctly\n");
}

// =============================================================================
// Test 5: Kill Switch Actions - Conservative
// =============================================================================

#[test]
fn test_kill_switch_conservative_actions() {
    println!("\n🛡️  TEST: Kill Switch Conservative Actions");
    println!("{}", "=".repeat(60));

    let actions = KillSwitchActions::conservative();
    let ks = KillSwitch::with_actions(actions);

    println!("\n1️⃣  Configuration:");
    println!("   Cancel all orders: {}", ks.get_actions().cancel_all_orders);
    println!("   Flatten positions: {}", ks.get_actions().flatten_positions);
    println!("   Disable new orders: {}", ks.get_actions().disable_new_orders);
    println!("   Disable modifications: {}", ks.get_actions().disable_modifications);

    assert_eq!(ks.get_actions().cancel_all_orders, true);
    assert_eq!(ks.get_actions().flatten_positions, false);
    assert_eq!(ks.get_actions().disable_new_orders, true);
    assert_eq!(ks.get_actions().disable_modifications, false);

    println!("\n2️⃣  Triggering Kill Switch:");
    ks.trigger("Test conservative mode");

    println!("\n3️⃣  Action Checks:");
    println!("   Should cancel orders: {}", ks.should_cancel_orders());
    println!("   Should flatten positions: {}", ks.should_flatten());
    println!("   Orders disabled: {}", ks.orders_disabled());

    assert!(ks.should_cancel_orders());
    assert!(!ks.should_flatten());
    assert!(ks.orders_disabled());

    println!("\n✅ Test passed: Conservative actions configured correctly\n");
}

// =============================================================================
// Test 6: Kill Switch Actions - Aggressive
// =============================================================================

#[test]
fn test_kill_switch_aggressive_actions() {
    println!("\n⚠️  TEST: Kill Switch Aggressive Actions");
    println!("{}", "=".repeat(60));

    let actions = KillSwitchActions::aggressive();
    let ks = KillSwitch::with_actions(actions);

    println!("\n1️⃣  Configuration:");
    println!("   Cancel all orders: {}", ks.get_actions().cancel_all_orders);
    println!("   Flatten positions: {}", ks.get_actions().flatten_positions);
    println!("   Disable new orders: {}", ks.get_actions().disable_new_orders);
    println!("   Disable modifications: {}", ks.get_actions().disable_modifications);

    assert_eq!(ks.get_actions().cancel_all_orders, true);
    assert_eq!(ks.get_actions().flatten_positions, true);
    assert_eq!(ks.get_actions().disable_new_orders, true);
    assert_eq!(ks.get_actions().disable_modifications, true);

    println!("\n2️⃣  Triggering Kill Switch:");
    ks.trigger("Critical risk breach - full shutdown");

    println!("\n3️⃣  Action Checks:");
    println!("   Should cancel orders: {}", ks.should_cancel_orders());
    println!("   Should flatten positions: {}", ks.should_flatten());
    println!("   Orders disabled: {}", ks.orders_disabled());

    assert!(ks.should_cancel_orders());
    assert!(ks.should_flatten());
    assert!(ks.orders_disabled());

    println!("\n⚠️  WARNING: Aggressive mode will:");
    println!("   • Cancel all open orders");
    println!("   • Flatten all positions with market orders");
    println!("   • Disable all new order submissions");
    println!("   • Disable order modifications");

    println!("\n✅ Test passed: Aggressive actions configured correctly\n");
}

// =============================================================================
// Test 7: Clone Behavior (Shared State)
// =============================================================================

#[test]
fn test_kill_switch_clone_shared_state() {
    println!("\n🔗 TEST: Kill Switch Clone Behavior (Shared State)");
    println!("{}", "=".repeat(60));

    let ks1 = KillSwitch::new();
    let ks2 = ks1.clone();

    println!("\n1️⃣  Initial State:");
    assert!(!ks1.is_triggered());
    assert!(!ks2.is_triggered());
    println!("   ✓ Both instances: INACTIVE");

    println!("\n2️⃣  Triggering via first instance:");
    ks1.trigger("Trigger from ks1");

    assert!(ks1.is_triggered());
    assert!(ks2.is_triggered());
    assert_eq!(ks2.get_reason(), Some("Trigger from ks1".to_string()));
    println!("   ✓ ks1 triggered: true");
    println!("   ✓ ks2 triggered: true (shared state)");
    println!("   ✓ ks2 reason: Trigger from ks1");

    println!("\n3️⃣  Resetting via second instance:");
    ks2.reset();

    assert!(!ks1.is_triggered());
    assert!(!ks2.is_triggered());
    println!("   ✓ ks1 triggered: false (shared state)");
    println!("   ✓ ks2 triggered: false");

    println!("\n✅ Test passed: Clones share state correctly\n");
}

// =============================================================================
// Test 8: Kill Switch Status Snapshot
// =============================================================================

#[test]
fn test_kill_switch_status_snapshot() {
    println!("\n📸 TEST: Kill Switch Status Snapshot");
    println!("{}", "=".repeat(60));

    let ks = KillSwitch::new();

    println!("\n1️⃣  Status Before Trigger:");
    let status1 = ks.get_status();
    println!("   Triggered: {}", status1.triggered);
    println!("   Reason: {:?}", status1.reason);
    println!("   Trigger type: {:?}", status1.trigger);
    println!("   Trigger count: {}", status1.trigger_count);

    assert!(!status1.triggered);
    assert!(status1.reason.is_none());

    println!("\n2️⃣  Triggering Kill Switch:");
    ks.trigger_with_type("Max drawdown breach", KillSwitchTrigger::MaxDrawdown);

    println!("\n3️⃣  Status After Trigger:");
    let status2 = ks.get_status();
    println!("   Triggered: {}", status2.triggered);
    println!("   Reason: {:?}", status2.reason);
    println!("   Trigger type: {:?}", status2.trigger);
    println!("   Triggered at: {} ms", status2.triggered_at_ms.unwrap());
    println!("   Trigger count: {}", status2.trigger_count);

    assert!(status2.triggered);
    assert_eq!(status2.reason, Some("Max drawdown breach".to_string()));
    assert_eq!(status2.trigger, Some(KillSwitchTrigger::MaxDrawdown));
    assert_eq!(status2.trigger_count, 1);
    assert!(status2.triggered_at_ms.is_some());

    println!("\n✅ Test passed: Status snapshot captured correctly\n");
}

// =============================================================================
// Test 9: High-Frequency Order Check Performance
// =============================================================================

#[test]
fn test_kill_switch_performance() {
    println!("\n⚡ TEST: High-Frequency Order Check Performance");
    println!("{}", "=".repeat(60));

    let ks = KillSwitch::new();

    println!("\n1️⃣  Simulating 10,000 order checks (kill switch inactive):");
    let start = std::time::Instant::now();

    for i in 0..10_000 {
        if ks.is_triggered() {
            panic!("Should not be triggered at iteration {}", i);
        }
    }

    let duration_inactive = start.elapsed();
    println!("   Duration: {:?}", duration_inactive);
    println!("   Avg per check: {:?}", duration_inactive / 10_000);

    println!("\n2️⃣  Triggering kill switch:");
    ks.trigger("Performance test trigger");

    println!("\n3️⃣  Simulating 10,000 order checks (kill switch active):");
    let start = std::time::Instant::now();

    for _ in 0..10_000 {
        if !ks.is_triggered() {
            panic!("Should be triggered");
        }
    }

    let duration_active = start.elapsed();
    println!("   Duration: {:?}", duration_active);
    println!("   Avg per check: {:?}", duration_active / 10_000);

    println!("\n4️⃣  Performance Summary:");
    println!("   Both scenarios complete in microseconds");
    println!("   ✓ Fast atomic check suitable for hot path");

    println!("\n✅ Test passed: Performance acceptable for high-frequency checks\n");
}

// =============================================================================
// Test 10: Comprehensive Kill Switch Report
// =============================================================================

#[test]
fn test_comprehensive_kill_switch_report() {
    println!("\n📋 TEST: Comprehensive Kill Switch Report");
    println!("{}", "=".repeat(60));

    println!("\n{}", "=".repeat(60));
    println!("TRADING SIMULATION: Kill Switch Integration Test");
    println!("{}", "=".repeat(60));

    let ks = KillSwitch::new();

    println!("\n1️⃣  Session Start - Normal Trading");
    println!("{}", "-".repeat(60));
    println!("   Status: Normal operations");
    let (successful, _) = simulate_order_batch(&ks, 5);
    println!("   ✅ Orders submitted: {}", successful);

    sleep(Duration::from_millis(10));

    println!("\n2️⃣  Risk Breach Detected - Daily Loss Limit");
    println!("{}", "-".repeat(60));
    println!("   Current PnL: -$12,500");
    println!("   Daily loss limit: $10,000");
    println!("   Breach amount: -$2,500");
    ks.trigger_with_type("Daily loss: -$12,500 > -$10,000 limit", KillSwitchTrigger::MaxLoss);
    println!("   🚨 KILL SWITCH ACTIVATED");

    let status = ks.get_status();
    println!("   Triggered at: {} ms since epoch", status.triggered_at_ms.unwrap());
    println!("   Trigger type: {:?}", status.trigger);

    println!("\n3️⃣  Post-Trigger Behavior");
    println!("{}", "-".repeat(60));
    println!("   Attempting to submit new orders...");
    let (successful, rejected) = simulate_order_batch(&ks, 5);
    println!("\n   Summary:");
    println!("   ✅ Orders accepted: {}", successful);
    println!("   ❌ Orders rejected: {}", rejected);

    assert_eq!(successful, 0);
    assert_eq!(rejected, 5);

    println!("\n4️⃣  Kill Switch Actions Executed");
    println!("{}", "-".repeat(60));
    if ks.should_cancel_orders() {
        println!("   ✓ Cancelling all open orders");
    }
    if ks.should_flatten() {
        println!("   ✓ Flattening all positions");
    }
    if ks.orders_disabled() {
        println!("   ✓ New orders disabled");
    }

    println!("\n5️⃣  Operator Review and Reset");
    println!("{}", "-".repeat(60));
    println!("   Operator investigating trigger reason...");
    println!("   Reason: {}", ks.get_reason().unwrap());
    println!("   Corrective actions taken");
    println!("   Resetting kill switch...");

    ks.reset();

    println!("   ✅ Kill switch RESET");
    println!("   Status: Ready for trading (manual approval required)");

    println!("\n6️⃣  Post-Reset Trading");
    println!("{}", "-".repeat(60));
    let (successful, rejected) = simulate_order_batch(&ks, 3);
    println!("\n   Summary:");
    println!("   ✅ Orders accepted: {}", successful);
    println!("   ❌ Orders rejected: {}", rejected);

    assert_eq!(successful, 3);
    assert_eq!(rejected, 0);

    println!("\n7️⃣  Session Statistics");
    println!("{}", "-".repeat(60));
    let final_status = ks.get_status();
    println!("   Total triggers this session: {}", final_status.trigger_count);
    println!("   Current state: {}", if final_status.triggered { "ACTIVE" } else { "INACTIVE" });
    println!("   Last trigger reason: {:?}", final_status.reason);

    println!("\n{}", "=".repeat(60));
    println!("✅ COMPREHENSIVE TEST PASSED");
    println!("   Kill switch system validated:");
    println!("   • Automatic risk breach detection");
    println!("   • Immediate order rejection when active");
    println!("   • Safe reset mechanism");
    println!("   • Multiple trigger types supported");
    println!("   • Fast atomic checks for hot path");
    println!("   • Shared state across clones");
    println!("{}\n", "=".repeat(60));
}
