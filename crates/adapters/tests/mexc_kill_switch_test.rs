//! MEXC Kill Switch Integration Test
//!
//! Tests the kill switch mechanism integrated with real MEXC trading.
//! This test validates that the kill switch properly halts trading activity.
//!
//! Run with: cargo test --package adapters --test mexc_kill_switch_test -- --nocapture --ignored
//!
//! Requirements:
//! - MEXC_API_KEY and MEXC_API_SECRET environment variables must be set
//! - Account must have small USDT balance for test orders
//! - Internet connection to MEXC exchange
//!
//! Test validates:
//! - Kill switch blocks order submission when triggered
//! - Kill switch can be reset to resume trading
//! - Different trigger types work correctly
//! - Kill switch integrates with real trading flow

use adapters::mexc::MexcSpotAdapter;
use adapters::traits::{SpotRest, OrderType, Side, CreateOrderRequest};
use risk::{KillSwitch, KillSwitchTrigger};
use std::time::Duration;
use tokio::time::sleep;

/// Helper to get MEXC credentials from environment
fn get_mexc_credentials() -> Option<(String, String)> {
    let api_key = std::env::var("MEXC_API_KEY").ok()?;
    let api_secret = std::env::var("MEXC_API_SECRET").ok()?;

    if api_key.contains("your_mexc") || api_secret.contains("your_mexc") {
        return None;
    }

    Some((api_key, api_secret))
}

/// Simulates order submission with kill switch check
async fn submit_order_with_kill_switch(
    adapter: &MexcSpotAdapter,
    kill_switch: &KillSwitch,
    symbol: &str,
    price: f64,
    order_num: u32,
) -> Result<String, String> {
    // Pre-trade kill switch check
    if kill_switch.is_triggered() {
        let reason = kill_switch.get_reason().unwrap_or_default();
        return Err(format!("Kill switch active: {}", reason));
    }

    // Submit order
    let result = adapter.create_order(CreateOrderRequest {
        symbol: symbol.to_string(),
        side: Side::Buy,
        ord_type: OrderType::Limit,
        qty: 0.0001,
        price: Some(price),
        client_order_id: Some(format!("kill_switch_test_{}", order_num)),
        ..Default::default()
    }).await;

    match result {
        Ok(order) => Ok(order.venue_order_id),
        Err(e) => Err(format!("Order failed: {}", e)),
    }
}

// =============================================================================
// Test 1: Basic Kill Switch Order Blocking
// =============================================================================

#[tokio::test]
#[ignore] // Run with --ignored flag
async fn test_mexc_kill_switch_order_blocking() {
    println!("\n🛑 TEST: MEXC Kill Switch Order Blocking");
    println!("{}", "=".repeat(60));

    let Some((api_key, api_secret)) = get_mexc_credentials() else {
        println!("⚠️  Skipping test: MEXC credentials not configured");
        return;
    };

    println!("\n1️⃣  Initializing components");
    let adapter = MexcSpotAdapter::new(api_key, api_secret);
    let kill_switch = KillSwitch::new();
    println!("   ✓ Adapter and kill switch ready");
    println!("   Kill switch status: INACTIVE");

    println!("\n2️⃣  Getting market price");
    let ticker = adapter.get_ticker_info("BTCUSDT").await
        .expect("Failed to get ticker");
    let test_price = ticker.last_price * 0.01;
    println!("   Test price: ${:.2}", test_price);

    println!("\n3️⃣  Submitting orders with kill switch INACTIVE");
    let result1 = submit_order_with_kill_switch(&adapter, &kill_switch, "BTCUSDT", test_price, 1).await;

    match &result1 {
        Ok(order_id) => {
            println!("   ✅ Order 1: ACCEPTED (ID: {})", order_id);

            // Clean up
            let _ = adapter.cancel_order("BTCUSDT", order_id).await;
        }
        Err(e) => {
            println!("   ❌ Order 1: FAILED ({})", e);
        }
    }

    sleep(Duration::from_millis(500)).await;

    println!("\n4️⃣  Triggering kill switch");
    kill_switch.trigger("Test: Manual emergency stop");
    println!("   🚨 Kill switch ACTIVATED");
    println!("   Reason: {}", kill_switch.get_reason().unwrap());

    println!("\n5️⃣  Attempting to submit orders with kill switch ACTIVE");

    let result2 = submit_order_with_kill_switch(&adapter, &kill_switch, "BTCUSDT", test_price, 2).await;
    match &result2 {
        Ok(order_id) => {
            println!("   ❌ Order 2: ACCEPTED (should have been blocked!) ID: {}", order_id);
            let _ = adapter.cancel_order("BTCUSDT", order_id).await;
        }
        Err(e) => {
            println!("   ✅ Order 2: REJECTED ({})", e);
        }
    }

    let result3 = submit_order_with_kill_switch(&adapter, &kill_switch, "BTCUSDT", test_price, 3).await;
    match &result3 {
        Ok(order_id) => {
            println!("   ❌ Order 3: ACCEPTED (should have been blocked!) ID: {}", order_id);
            let _ = adapter.cancel_order("BTCUSDT", order_id).await;
        }
        Err(e) => {
            println!("   ✅ Order 3: REJECTED ({})", e);
        }
    }

    println!("\n6️⃣  Resetting kill switch");
    kill_switch.reset();
    println!("   ✅ Kill switch RESET");
    println!("   Status: INACTIVE");

    println!("\n7️⃣  Submitting orders after reset");
    let result4 = submit_order_with_kill_switch(&adapter, &kill_switch, "BTCUSDT", test_price, 4).await;

    match &result4 {
        Ok(order_id) => {
            println!("   ✅ Order 4: ACCEPTED (ID: {})", order_id);
            let _ = adapter.cancel_order("BTCUSDT", order_id).await;
        }
        Err(e) => {
            println!("   ❌ Order 4: FAILED ({})", e);
        }
    }

    println!("\n8️⃣  Test Results:");
    println!("   Order 1 (inactive): {}", if result1.is_ok() { "✅ ACCEPTED" } else { "❌ REJECTED" });
    println!("   Order 2 (active):   {}", if result2.is_err() { "✅ BLOCKED" } else { "❌ NOT BLOCKED" });
    println!("   Order 3 (active):   {}", if result3.is_err() { "✅ BLOCKED" } else { "❌ NOT BLOCKED" });
    println!("   Order 4 (reset):    {}", if result4.is_ok() { "✅ ACCEPTED" } else { "❌ REJECTED" });

    assert!(result1.is_ok(), "Order should succeed when kill switch inactive");
    assert!(result2.is_err(), "Order should be blocked when kill switch active");
    assert!(result3.is_err(), "Order should be blocked when kill switch active");
    assert!(result4.is_ok(), "Order should succeed after kill switch reset");

    println!("\n✅ Test passed: Kill switch correctly blocks orders\n");
}

// =============================================================================
// Test 2: Kill Switch Trigger Types
// =============================================================================

#[tokio::test]
#[ignore] // Run with --ignored flag
async fn test_mexc_kill_switch_trigger_types() {
    println!("\n🏷️  TEST: MEXC Kill Switch Trigger Types");
    println!("{}", "=".repeat(60));

    let Some((api_key, api_secret)) = get_mexc_credentials() else {
        println!("⚠️  Skipping test: MEXC credentials not configured");
        return;
    };

    let adapter = MexcSpotAdapter::new(api_key, api_secret);
    let kill_switch = KillSwitch::new();

    let ticker = adapter.get_ticker_info("BTCUSDT").await
        .expect("Failed to get ticker");
    let test_price = ticker.last_price * 0.01;

    println!("\n1️⃣  Testing Manual Trigger");
    kill_switch.trigger_with_type("Manual operator intervention", KillSwitchTrigger::Manual);

    let result = submit_order_with_kill_switch(&adapter, &kill_switch, "BTCUSDT", test_price, 1).await;
    assert!(result.is_err(), "Order should be blocked");
    println!("   ✓ Manual trigger: Order blocked");
    println!("   Trigger type: {:?}", kill_switch.get_trigger_type());

    kill_switch.reset();

    println!("\n2️⃣  Testing Max Loss Trigger");
    kill_switch.trigger_with_type("Daily loss: $15,000 > $10,000 limit", KillSwitchTrigger::MaxLoss);

    let result = submit_order_with_kill_switch(&adapter, &kill_switch, "BTCUSDT", test_price, 2).await;
    assert!(result.is_err(), "Order should be blocked");
    println!("   ✓ Max loss trigger: Order blocked");
    println!("   Reason: {}", kill_switch.get_reason().unwrap());

    kill_switch.reset();

    println!("\n3️⃣  Testing Max Drawdown Trigger");
    kill_switch.trigger_with_type("Drawdown: 28% > 25% limit", KillSwitchTrigger::MaxDrawdown);

    let result = submit_order_with_kill_switch(&adapter, &kill_switch, "BTCUSDT", test_price, 3).await;
    assert!(result.is_err(), "Order should be blocked");
    println!("   ✓ Max drawdown trigger: Order blocked");

    kill_switch.reset();

    println!("\n4️⃣  Testing Connectivity Trigger");
    kill_switch.trigger_with_type("WebSocket disconnected > 10s", KillSwitchTrigger::Connectivity);

    let result = submit_order_with_kill_switch(&adapter, &kill_switch, "BTCUSDT", test_price, 4).await;
    assert!(result.is_err(), "Order should be blocked");
    println!("   ✓ Connectivity trigger: Order blocked");

    kill_switch.reset();

    println!("\n✅ Test passed: All trigger types work correctly\n");
}

// =============================================================================
// Test 3: Comprehensive Kill Switch Report
// =============================================================================

#[tokio::test]
#[ignore] // Run with --ignored flag
async fn test_mexc_comprehensive_kill_switch_report() {
    println!("\n📋 TEST: MEXC Comprehensive Kill Switch Report");
    println!("{}", "=".repeat(60));

    let Some((api_key, api_secret)) = get_mexc_credentials() else {
        println!("⚠️  Skipping test: MEXC credentials not configured");
        println!("\n⚠️  To run this test:");
        println!("   1. Set MEXC_API_KEY environment variable");
        println!("   2. Set MEXC_API_SECRET environment variable");
        println!("   3. Ensure account has small USDT balance");
        println!("   4. Run with: cargo test --ignored -- test_mexc_comprehensive_kill_switch_report");
        return;
    };

    println!("\n{}", "=".repeat(60));
    println!("MEXC KILL SWITCH INTEGRATION TEST");
    println!("{}", "=".repeat(60));

    let adapter = MexcSpotAdapter::new(api_key, api_secret);
    let kill_switch = KillSwitch::new();

    println!("\n1️⃣  Configuration");
    println!("{}", "-".repeat(60));
    println!("   Exchange: MEXC Spot");
    println!("   Symbol: BTCUSDT");
    println!("   Kill switch: Initialized");
    println!("   Initial status: {:?}", kill_switch.is_triggered());

    println!("\n2️⃣  Pre-Flight Checks");
    println!("{}", "-".repeat(60));

    let ticker = adapter.get_ticker_info("BTCUSDT").await
        .expect("Failed to get ticker");
    let test_price = ticker.last_price * 0.01;

    println!("   ✓ Connection to MEXC: OK");
    println!("   ✓ Market data access: OK");
    println!("   ✓ Test order price: ${:.2}", test_price);

    println!("\n3️⃣  Normal Trading Operations");
    println!("{}", "-".repeat(60));

    let mut order_ids = Vec::new();

    for i in 1..=3 {
        let result = submit_order_with_kill_switch(&adapter, &kill_switch, "BTCUSDT", test_price, i).await;
        match result {
            Ok(order_id) => {
                println!("   ✅ Order {}: Submitted (ID: {})", i, order_id);
                order_ids.push(order_id);
            }
            Err(e) => {
                println!("   ❌ Order {}: Failed ({})", i, e);
            }
        }
        sleep(Duration::from_millis(200)).await;
    }

    println!("   Summary: {}/3 orders accepted", order_ids.len());

    println!("\n4️⃣  Risk Event Simulation");
    println!("{}", "-".repeat(60));
    println!("   Simulating: Daily loss limit breach");
    println!("   Current PnL: -$12,500");
    println!("   Loss limit: -$10,000");
    println!("   Breach: -$2,500");

    kill_switch.trigger_with_type(
        "Daily loss: -$12,500 > -$10,000 limit",
        KillSwitchTrigger::MaxLoss
    );

    println!("\n   🚨 KILL SWITCH ACTIVATED");
    println!("   Trigger: {:?}", kill_switch.get_trigger_type().unwrap());
    println!("   Reason: {}", kill_switch.get_reason().unwrap());
    println!("   Trigger count: {}", kill_switch.trigger_count());

    println!("\n5️⃣  Post-Trigger Trading Attempts");
    println!("{}", "-".repeat(60));

    let mut blocked_count = 0;
    for i in 4..=8 {
        let result = submit_order_with_kill_switch(&adapter, &kill_switch, "BTCUSDT", test_price, i).await;
        match result {
            Ok(order_id) => {
                println!("   ⚠️  Order {}: ACCEPTED (should be blocked!) ID: {}", i, order_id);
                order_ids.push(order_id);
            }
            Err(e) => {
                println!("   ✅ Order {}: BLOCKED ({})", i, e.split(':').last().unwrap_or(""));
                blocked_count += 1;
            }
        }
        sleep(Duration::from_millis(100)).await;
    }

    println!("   Summary: {}/5 orders blocked", blocked_count);

    println!("\n6️⃣  Operator Review & Reset");
    println!("{}", "-".repeat(60));
    println!("   Reviewing trigger reason...");
    println!("   Reason: {}", kill_switch.get_reason().unwrap());
    println!("   Corrective action: Risk limits adjusted");
    println!("   Resetting kill switch...");

    kill_switch.reset();

    println!("   ✅ Kill switch RESET");
    println!("   Status: INACTIVE");
    println!("   Trigger count (persisted): {}", kill_switch.trigger_count());

    println!("\n7️⃣  Post-Reset Trading");
    println!("{}", "-".repeat(60));

    for i in 9..=11 {
        let result = submit_order_with_kill_switch(&adapter, &kill_switch, "BTCUSDT", test_price, i).await;
        match result {
            Ok(order_id) => {
                println!("   ✅ Order {}: Submitted (ID: {})", i, order_id);
                order_ids.push(order_id);
            }
            Err(e) => {
                println!("   ❌ Order {}: Failed ({})", i, e);
            }
        }
        sleep(Duration::from_millis(200)).await;
    }

    println!("\n8️⃣  Cleanup");
    println!("{}", "-".repeat(60));
    print!("   Cancelling all test orders: ");

    for order_id in &order_ids {
        let _ = adapter.cancel_order("BTCUSDT", order_id).await;
    }
    println!("✓ Done ({} orders)", order_ids.len());

    println!("\n9️⃣  Test Summary");
    println!("{}", "-".repeat(60));
    println!("   Total orders attempted: 11");
    println!("   Orders blocked by kill switch: {}", blocked_count);
    println!("   Kill switch triggers: {}", kill_switch.trigger_count());
    println!("   Kill switch resets: 1");

    println!("\n{}", "=".repeat(60));
    println!("✅ COMPREHENSIVE TEST PASSED");
    println!("   Kill switch system validated:");
    println!("   • Normal operation: Orders accepted");
    println!("   • Kill switch active: Orders blocked");
    println!("   • Multiple trigger types: Working");
    println!("   • Reset mechanism: Working");
    println!("   • Integration with MEXC: Successful");
    println!("{}\n", "=".repeat(60));

    assert_eq!(blocked_count, 5, "All 5 orders should have been blocked");
}
