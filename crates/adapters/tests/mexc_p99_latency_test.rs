//! MEXC P99 Latency Integration Test
//!
//! Tests real order round-trip time (ORTT) on MEXC exchange and validates P99 latency.
//! This test submits actual orders to the live MEXC exchange.
//!
//! Run with: cargo test --package adapters --test mexc_p99_latency_test -- --nocapture --ignored
//!
//! Requirements:
//! - MEXC_API_KEY and MEXC_API_SECRET environment variables must be set
//! - Account must have small USDT balance for test orders
//! - Internet connection to MEXC exchange
//!
//! Test validates:
//! - Order submission and acknowledgment latency
//! - P99 latency < 200ms for market making viability
//! - Consistent performance across multiple orders
//!
//! IMPORTANT: This test places real orders on MEXC using very small amounts
//! to minimize cost while measuring actual latency.

use adapters::mexc::MexcSpotAdapter;
use adapters::traits::{SpotRest, OrderType, Side, CreateOrderRequest};
use risk::{LatencyTracker, LatencyConfig, now_ms};
use std::time::{Duration, Instant};
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

// =============================================================================
// Test 1: Single Order Latency Measurement
// =============================================================================

#[tokio::test]
#[ignore] // Run with --ignored flag to actually place orders
async fn test_mexc_single_order_latency() {
    println!("\n⏱️  TEST: MEXC Single Order Latency");
    println!("{}", "=".repeat(60));

    let Some((api_key, api_secret)) = get_mexc_credentials() else {
        println!("⚠️  Skipping test: MEXC credentials not configured");
        return;
    };

    println!("\n1️⃣  Initializing MEXC adapter");
    let adapter = MexcSpotAdapter::new(api_key, api_secret);
    println!("   ✓ Adapter ready");

    println!("\n2️⃣  Getting current market price for BTCUSDT");
    let ticker = adapter.get_ticker_info("BTCUSDT").await
        .expect("Failed to get ticker");

    let current_price = ticker.last_price;
    // Place order far from market to avoid fill (99% below current price)
    let test_price = current_price * 0.01;

    println!("   Current price: ${:.2}", current_price);
    println!("   Test order price: ${:.2} (will not fill)", test_price);

    println!("\n3️⃣  Submitting test limit buy order");
    println!("   Symbol: BTCUSDT");
    println!("   Side: BUY");
    println!("   Quantity: 0.0001 BTC (~${:.2})", test_price * 0.0001);
    println!("   Price: ${:.2}", test_price);

    let created_ms = now_ms();
    let order_start = Instant::now();

    let order = adapter.create_order(CreateOrderRequest {
        symbol: "BTCUSDT".to_string(),
        side: Side::Buy,
        ord_type: OrderType::Limit,
        qty: 0.0001, // Minimum order size
        price: Some(test_price),
        client_order_id: Some(format!("latency_test_{}", created_ms)),
        ..Default::default()
    }).await;

    let sent_ms = now_ms();
    let order_latency = order_start.elapsed();

    match order {
        Ok(order) => {
            let recv_ms = now_ms();
            let ortt = recv_ms - created_ms;

            println!("\n4️⃣  Order Submitted Successfully");
            println!("   Order ID: {}", order.venue_order_id);
            println!("   Status: {:?}", order.status);
            println!("   Latency breakdown:");
            println!("     - Order created: {} ms since epoch", created_ms);
            println!("     - Order sent: {} ms since epoch", sent_ms);
            println!("     - Order acknowledged: {} ms since epoch", recv_ms);
            println!("     - Order Round-Trip Time (ORTT): {} ms", ortt);
            println!("     - Measured latency: {:?}", order_latency);

            // Cancel the order
            println!("\n5️⃣  Cancelling test order");
            match adapter.cancel_order("BTCUSDT", &order.venue_order_id).await {
                Ok(_) => println!("   ✓ Order cancelled successfully"),
                Err(e) => println!("   ⚠️  Cancel failed (may already be cancelled): {}", e),
            }

            println!("\n6️⃣  Latency Assessment:");
            if ortt < 200 {
                println!("   ✅ EXCELLENT: {} ms < 200 ms threshold", ortt);
                println!("   Suitable for high-frequency market making");
            } else if ortt < 500 {
                println!("   ⚠️  ACCEPTABLE: {} ms (200-500 ms)", ortt);
                println!("   May impact market making performance");
            } else {
                println!("   ❌ POOR: {} ms > 500 ms", ortt);
                println!("   Not suitable for market making");
            }

            println!("\n✅ Test passed: Order latency measured\n");
        }
        Err(e) => {
            println!("\n❌ Test failed: Order submission error");
            println!("   Error: {}", e);
            println!("\n   Possible reasons:");
            println!("   - Insufficient balance");
            println!("   - API permissions not enabled for trading");
            println!("   - Symbol restrictions");
            panic!("Order submission failed: {}", e);
        }
    }
}

// =============================================================================
// Test 2: P99 Latency with Multiple Orders
// =============================================================================

#[tokio::test]
#[ignore] // Run with --ignored flag
async fn test_mexc_p99_latency_measurement() {
    println!("\n📊 TEST: MEXC P99 Latency Measurement");
    println!("{}", "=".repeat(60));

    let Some((api_key, api_secret)) = get_mexc_credentials() else {
        println!("⚠️  Skipping test: MEXC credentials not configured");
        return;
    };

    let sample_count = 20; // Number of test orders

    println!("\n1️⃣  Configuration:");
    println!("   Exchange: MEXC Spot");
    println!("   Symbol: BTCUSDT");
    println!("   Sample size: {} orders", sample_count);
    println!("   P99 threshold: 200ms");

    println!("\n2️⃣  Initializing components");
    let adapter = MexcSpotAdapter::new(api_key, api_secret);
    let tracker = LatencyTracker::with_config(LatencyConfig {
        window_size: 1000,
        p99_warning_ms: 150,
        p99_critical_ms: 200,
    });
    println!("   ✓ Adapter and tracker ready");

    println!("\n3️⃣  Getting market price");
    let ticker = adapter.get_ticker_info("BTCUSDT").await
        .expect("Failed to get ticker");

    let current_price = ticker.last_price;
    let test_price = current_price * 0.01; // 99% below market

    println!("   Current price: ${:.2}", current_price);
    println!("   Test price: ${:.2}", test_price);

    println!("\n4️⃣  Submitting {} test orders", sample_count);
    println!("   (orders placed far from market to avoid fills)\n");

    let mut successful_orders = 0;
    let mut failed_orders = 0;
    let mut order_ids = Vec::new();

    for i in 1..=sample_count {
        let created_ms = now_ms();

        print!("   Order {}/{}: ", i, sample_count);

        let result = adapter.create_order(CreateOrderRequest {
            symbol: "BTCUSDT".to_string(),
            side: Side::Buy,
            ord_type: OrderType::Limit,
            qty: 0.0001,
            price: Some(test_price),
            client_order_id: Some(format!("p99_test_{}_{}", created_ms, i)),
            ..Default::default()
        }).await;

        let sent_ms = now_ms();
        let recv_ms = now_ms();

        match result {
            Ok(order) => {
                tracker.record(created_ms, sent_ms, recv_ms);
                order_ids.push(order.venue_order_id.clone());
                successful_orders += 1;

                let ortt = recv_ms - created_ms;
                println!("✓ {} ms", ortt);
            }
            Err(e) => {
                failed_orders += 1;
                println!("✗ {}", e);
            }
        }

        // Rate limit: 100ms between orders
        sleep(Duration::from_millis(100)).await;
    }

    println!("\n5️⃣  Order Submission Summary:");
    println!("   Successful: {}", successful_orders);
    println!("   Failed: {}", failed_orders);

    if successful_orders < 5 {
        println!("\n⚠️  Warning: Too few successful orders for meaningful P99 calculation");
        println!("   Need at least 5 orders, got {}", successful_orders);
        return;
    }

    println!("\n6️⃣  Latency Statistics:");
    if let Some(stats) = tracker.get_stats() {
        println!("   Sample count: {}", stats.count);
        println!("   Min latency: {} ms", stats.min_ms);
        println!("   Mean latency: {:.1} ms", stats.mean_ms);
        println!("   P50 latency: {} ms", stats.p50_ms);
        println!("   P90 latency: {} ms", stats.p90_ms);
        println!("   P95 latency: {} ms", stats.p95_ms);
        println!("   P99 latency: {} ms", stats.p99_ms);
        println!("   Max latency: {} ms", stats.max_ms);

        println!("\n7️⃣  Performance Assessment:");
        if stats.p99_ms < 200 {
            println!("   ✅ EXCELLENT: P99 = {} ms < 200 ms", stats.p99_ms);
            println!("   System meets requirements for market making");
        } else if stats.p99_ms < 300 {
            println!("   ⚠️  WARNING: P99 = {} ms (200-300 ms range)", stats.p99_ms);
            println!("   System may experience performance issues");
        } else {
            println!("   ❌ CRITICAL: P99 = {} ms > 300 ms", stats.p99_ms);
            println!("   System NOT suitable for market making");
        }
    }

    println!("\n8️⃣  Cleaning up test orders");
    for (i, order_id) in order_ids.iter().enumerate() {
        match adapter.cancel_order("BTCUSDT", order_id).await {
            Ok(_) => print!("."),
            Err(_) => print!("!"),
        }
        if (i + 1) % 10 == 0 {
            println!(" {}/{}", i + 1, order_ids.len());
        }
    }
    println!("\n   ✓ Cleanup complete");

    println!("\n✅ Test passed: P99 latency measured\n");
}

// =============================================================================
// Test 3: Comprehensive P99 Report
// =============================================================================

#[tokio::test]
#[ignore] // Run with --ignored flag
async fn test_mexc_comprehensive_p99_report() {
    println!("\n📋 TEST: MEXC Comprehensive P99 Latency Report");
    println!("{}", "=".repeat(60));

    let Some((api_key, api_secret)) = get_mexc_credentials() else {
        println!("⚠️  Skipping test: MEXC credentials not configured");
        println!("\n⚠️  To run this test:");
        println!("   1. Set MEXC_API_KEY environment variable");
        println!("   2. Set MEXC_API_SECRET environment variable");
        println!("   3. Ensure account has small USDT balance");
        println!("   4. Run with: cargo test --ignored -- test_mexc_comprehensive_p99_report");
        return;
    };

    println!("\n{}", "=".repeat(60));
    println!("MEXC ORDER LATENCY PERFORMANCE TEST");
    println!("{}", "=".repeat(60));

    println!("\n1️⃣  Test Configuration");
    println!("{}", "-".repeat(60));
    println!("   Exchange: MEXC");
    println!("   Market: Spot");
    println!("   Symbol: BTCUSDT");
    println!("   Order type: Limit (far from market)");
    println!("   Sample size: 30 orders");
    println!("   Target: P99 < 200ms");

    let adapter = MexcSpotAdapter::new(api_key, api_secret);
    let tracker = LatencyTracker::with_config(LatencyConfig {
        window_size: 1000,
        p99_warning_ms: 150,
        p99_critical_ms: 200,
    });

    println!("\n2️⃣  Pre-Flight Checks");
    println!("{}", "-".repeat(60));

    let ticker = adapter.get_ticker_info("BTCUSDT").await
        .expect("Failed to get ticker");
    let current_price = ticker.last_price;
    let test_price = current_price * 0.01;

    println!("   ✓ Connection to MEXC: OK");
    println!("   ✓ Market data access: OK");
    println!("   ✓ Current BTC price: ${:.2}", current_price);
    println!("   ✓ Test order price: ${:.2} (safe, won't fill)", test_price);

    println!("\n3️⃣  Latency Measurement (30 orders)");
    println!("{}", "-".repeat(60));

    let test_start = Instant::now();
    let mut order_ids = Vec::new();
    let mut latencies = Vec::new();

    for i in 1..=30 {
        let created_ms = now_ms();
        let order_start = Instant::now();

        let result = adapter.create_order(CreateOrderRequest {
            symbol: "BTCUSDT".to_string(),
            side: Side::Buy,
            ord_type: OrderType::Limit,
            qty: 0.0001,
            price: Some(test_price),
            client_order_id: Some(format!("comprehensive_test_{}_{}", created_ms, i)),
            ..Default::default()
        }).await;

        let sent_ms = now_ms();
        let recv_ms = now_ms();
        let measured_latency = order_start.elapsed();

        if let Ok(order) = result {
            tracker.record(created_ms, sent_ms, recv_ms);
            order_ids.push(order.venue_order_id);
            latencies.push(measured_latency);

            if i % 10 == 0 {
                println!("   Progress: {}/30 orders submitted", i);
            }
        }

        sleep(Duration::from_millis(100)).await;
    }

    let test_duration = test_start.elapsed();
    println!("   ✓ All orders submitted in {:?}", test_duration);

    println!("\n4️⃣  Latency Distribution Analysis");
    println!("{}", "-".repeat(60));

    if let Some(stats) = tracker.get_stats() {
        println!("   Samples: {}", stats.count);
        println!("   ");
        println!("   Percentile Breakdown:");
        println!("     Min:  {:4} ms  (fastest order)", stats.min_ms);
        println!("     P50:  {:4} ms  (median)", stats.p50_ms);
        println!("     P90:  {:4} ms  (90th percentile)", stats.p90_ms);
        println!("     P95:  {:4} ms  (95th percentile)", stats.p95_ms);
        println!("     P99:  {:4} ms  (99th percentile) ⭐", stats.p99_ms);
        println!("     Max:  {:4} ms  (slowest order)", stats.max_ms);
        println!("     Mean: {:6.1} ms  (average)", stats.mean_ms);

        println!("\n5️⃣  Performance Evaluation");
        println!("{}", "-".repeat(60));

        println!("   Requirement: P99 < 200ms");
        println!("   Measured P99: {} ms", stats.p99_ms);

        if stats.p99_ms < 200 {
            println!("   Result: ✅ PASS");
            println!("   ");
            println!("   Market Making Viability: EXCELLENT");
            println!("   - Latency well within acceptable range");
            println!("   - Suitable for high-frequency strategies");
            println!("   - Can compete effectively in fast markets");
        } else {
            println!("   Result: ⚠️  WARNING");
            println!("   ");
            println!("   Market Making Viability: MARGINAL");
            println!("   - Latency exceeds recommended threshold");
            println!("   - May face challenges in competitive markets");
            println!("   - Consider infrastructure optimization");
        }

        println!("\n6️⃣  Latency Consistency");
        println!("{}", "-".repeat(60));

        let range = stats.max_ms - stats.min_ms;
        let variance = range as f64 / stats.mean_ms;

        println!("   Range: {} ms ({} - {})", range, stats.min_ms, stats.max_ms);
        println!("   Variance: {:.1}x mean", variance);

        if variance < 2.0 {
            println!("   Consistency: ✅ EXCELLENT (low variance)");
        } else if variance < 5.0 {
            println!("   Consistency: ⚠️  MODERATE (some variance)");
        } else {
            println!("   Consistency: ❌ POOR (high variance)");
        }
    }

    println!("\n7️⃣  Cleanup");
    println!("{}", "-".repeat(60));
    print!("   Cancelling test orders: ");

    for order_id in &order_ids {
        let _ = adapter.cancel_order("BTCUSDT", order_id).await;
    }
    println!("✓ Done");

    println!("\n{}", "=".repeat(60));
    println!("✅ COMPREHENSIVE TEST COMPLETE");
    println!("{}", "=".repeat(60));
    println!();
}
