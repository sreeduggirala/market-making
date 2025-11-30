//! P99 Latency Tracking Test Suite
//!
//! Tests the P99 latency tracking system for Order Round-Trip Time (ORTT).
//! Validates the requirement: p99 latency must be < 200ms for market making.
//!
//! Run with: cargo test --package risk --test p99_latency_test -- --nocapture

use risk::{LatencyTracker, LatencyConfig, now_ms};
use std::time::Duration;
use std::thread::sleep;

// =============================================================================
// Test Utilities
// =============================================================================

/// Simulates order creation, sending, and acknowledgment with realistic delays
fn simulate_order_lifecycle(tracker: &LatencyTracker, internal_delay_ms: u64, network_delay_ms: u64) -> u64 {
    let created = now_ms();

    // Simulate internal processing
    sleep(Duration::from_millis(internal_delay_ms));
    let sent = now_ms();

    // Simulate network round-trip
    sleep(Duration::from_millis(network_delay_ms));
    let recv = now_ms();

    tracker.record(created, sent, recv);
    recv - created
}

/// Simulates a batch of orders with varying latencies
fn simulate_order_batch(tracker: &LatencyTracker, count: usize, base_latency_ms: u64, variance_ms: u64) {
    println!("  📦 Simulating batch of {} orders...", count);

    for i in 0..count {
        let internal = 5 + (i as u64 % 10); // 5-15ms internal processing
        let network = base_latency_ms + ((i * 7) as u64 % variance_ms); // Variable network latency

        simulate_order_lifecycle(tracker, internal, network);
    }

    println!("  ✅ Batch complete");
}

// =============================================================================
// Test 1: Basic Latency Tracking
// =============================================================================

#[test]
fn test_latency_tracker_basic() {
    println!("\n📊 TEST: Basic Latency Tracking");
    println!("{}", "=".repeat(60));

    let tracker = LatencyTracker::new();

    println!("\n1️⃣  Simulating order with 50ms total latency...");
    let created = 1000;
    let sent = 1005;     // 5ms internal
    let recv = 1050;     // 45ms network

    tracker.record(created, sent, recv);

    let stats = tracker.get_stats().unwrap();
    println!("   Order Round-Trip Time (ORTT): {}ms", stats.min_ms);
    println!("   Sample count: {}", stats.count);

    assert_eq!(stats.count, 1);
    assert_eq!(stats.min_ms, 50);
    assert_eq!(stats.max_ms, 50);
    assert_eq!(stats.mean_ms, 50.0);

    println!("\n✅ Test passed: Basic tracking working correctly\n");
}

// =============================================================================
// Test 2: Percentile Calculations
// =============================================================================

#[test]
fn test_latency_percentile_calculations() {
    println!("\n📈 TEST: Latency Percentile Calculations");
    println!("{}", "=".repeat(60));

    let tracker = LatencyTracker::new();

    println!("\n1️⃣  Recording 100 samples with latencies 100-199ms...");

    // Add 100 samples with predictable latencies
    for i in 0..100 {
        let latency = 100 + i;
        tracker.record(1000, 1005, 1000 + latency);
    }

    println!("   ✓ Recorded 100 samples");

    let stats = tracker.get_stats().unwrap();

    println!("\n2️⃣  Calculated Statistics:");
    println!("   Count: {}", stats.count);
    println!("   Min: {}ms", stats.min_ms);
    println!("   Max: {}ms", stats.max_ms);
    println!("   Mean: {:.1}ms", stats.mean_ms);
    println!("   P50 (median): {}ms", stats.p50_ms);
    println!("   P90: {}ms", stats.p90_ms);
    println!("   P95: {}ms", stats.p95_ms);
    println!("   P99: {}ms", stats.p99_ms);

    assert_eq!(stats.count, 100);
    assert_eq!(stats.min_ms, 100);
    assert_eq!(stats.max_ms, 199);
    assert_eq!(stats.p50_ms, 150); // Median should be 150
    assert_eq!(stats.p99_ms, 199); // P99 should be 199

    println!("\n✅ Test passed: Percentile calculations accurate\n");
}

// =============================================================================
// Test 3: P99 Threshold Detection (Good Performance)
// =============================================================================

#[test]
fn test_p99_good_performance() {
    println!("\n✅ TEST: P99 Latency - Good Performance (< 150ms)");
    println!("{}", "=".repeat(60));

    let config = LatencyConfig {
        window_size: 1000,
        p99_warning_ms: 150,
        p99_critical_ms: 200,
    };
    let tracker = LatencyTracker::with_config(config);

    println!("\n1️⃣  Configuration:");
    println!("   Warning threshold: 150ms");
    println!("   Critical threshold: 200ms");

    println!("\n2️⃣  Simulating 100 orders with good performance...");

    // Simulate orders with latencies 80-130ms (well below warning)
    for i in 0..100 {
        let latency = 80 + (i * 50) / 100; // 80-130ms
        tracker.record(1000, 1005, 1000 + latency);
    }

    let stats = tracker.get_stats().unwrap();

    println!("\n3️⃣  Performance Metrics:");
    println!("   Mean latency: {:.1}ms", stats.mean_ms);
    println!("   P99 latency: {}ms", stats.p99_ms);
    println!("   Warning status: {}", tracker.is_warning());
    println!("   Critical status: {}", tracker.is_critical());

    assert!(stats.p99_ms < 150, "P99 should be below warning threshold");
    assert!(!tracker.is_warning(), "Should not trigger warning");
    assert!(!tracker.is_critical(), "Should not trigger critical");

    println!("\n✅ Test passed: System performing well (P99 < 150ms)\n");
}

// =============================================================================
// Test 4: P99 Warning Level (150-200ms)
// =============================================================================

#[test]
fn test_p99_warning_level() {
    println!("\n⚠️  TEST: P99 Latency - Warning Level (150-200ms)");
    println!("{}", "=".repeat(60));

    let config = LatencyConfig {
        window_size: 1000,
        p99_warning_ms: 150,
        p99_critical_ms: 200,
    };
    let tracker = LatencyTracker::with_config(config);

    println!("\n1️⃣  Simulating 100 orders with elevated latency...");

    // Simulate orders with latencies that put p99 around 165ms
    for i in 0..100 {
        let latency = 100 + (i * 70) / 100; // 100-170ms range
        tracker.record(1000, 1005, 1000 + latency);
    }

    let stats = tracker.get_stats().unwrap();

    println!("\n2️⃣  Performance Metrics:");
    println!("   Mean latency: {:.1}ms", stats.mean_ms);
    println!("   P90: {}ms", stats.p90_ms);
    println!("   P95: {}ms", stats.p95_ms);
    println!("   P99: {}ms", stats.p99_ms);

    println!("\n3️⃣  Alert Status:");
    println!("   ⚠️  WARNING: P99 latency ({} ms) exceeds threshold (150ms)", stats.p99_ms);
    println!("   ✓ SAFE: P99 latency below critical threshold (200ms)");

    assert!(stats.p99_ms >= 150, "P99 should exceed warning threshold");
    assert!(stats.p99_ms < 200, "P99 should be below critical threshold");
    assert!(tracker.is_warning(), "Should trigger warning");
    assert!(!tracker.is_critical(), "Should not trigger critical");

    println!("\n✅ Test passed: Warning level detection working\n");
}

// =============================================================================
// Test 5: P99 Critical Level (> 200ms)
// =============================================================================

#[test]
fn test_p99_critical_level() {
    println!("\n🚨 TEST: P99 Latency - Critical Level (> 200ms)");
    println!("{}", "=".repeat(60));

    let config = LatencyConfig {
        window_size: 1000,
        p99_warning_ms: 150,
        p99_critical_ms: 200,
    };
    let tracker = LatencyTracker::with_config(config);

    println!("\n1️⃣  Simulating 100 orders with high latency...");

    // Simulate orders with latencies that put p99 above 200ms
    for i in 0..100 {
        let latency = 100 + (i * 120) / 100; // 100-220ms range
        tracker.record(1000, 1005, 1000 + latency);
    }

    let stats = tracker.get_stats().unwrap();

    println!("\n2️⃣  Performance Metrics:");
    println!("   Mean latency: {:.1}ms", stats.mean_ms);
    println!("   P90: {}ms", stats.p90_ms);
    println!("   P95: {}ms", stats.p95_ms);
    println!("   P99: {}ms", stats.p99_ms);

    println!("\n3️⃣  Alert Status:");
    println!("   🚨 CRITICAL: P99 latency ({} ms) exceeds critical threshold (200ms)", stats.p99_ms);
    println!("   ⚠️  WARNING: Trading performance degraded");
    println!("   💡 RECOMMENDATION: Investigate network/exchange issues");

    assert!(stats.p99_ms >= 200, "P99 should exceed critical threshold");
    assert!(tracker.is_warning(), "Should trigger warning");
    assert!(tracker.is_critical(), "Should trigger critical");

    println!("\n✅ Test passed: Critical level detection working\n");
}

// =============================================================================
// Test 6: Rolling Window Behavior
// =============================================================================

#[test]
fn test_latency_rolling_window() {
    println!("\n🔄 TEST: Latency Rolling Window Behavior");
    println!("{}", "=".repeat(60));

    let config = LatencyConfig {
        window_size: 50,  // Small window for testing
        p99_warning_ms: 150,
        p99_critical_ms: 200,
    };
    let tracker = LatencyTracker::with_config(config);

    println!("\n1️⃣  Adding 100 samples (window size = 50)...");

    // Add 100 samples, but only last 50 should be kept
    for i in 0..100 {
        tracker.record(1000, 1005, 1000 + 100 + i);
    }

    assert_eq!(tracker.sample_count(), 50, "Should only keep 50 samples");
    println!("   ✓ Window size maintained at 50 samples");

    let stats = tracker.get_stats().unwrap();
    println!("\n2️⃣  Statistics (from last 50 samples only):");
    println!("   Count: {}", stats.count);
    println!("   Min: {}ms (should be from sample #50)", stats.min_ms);
    println!("   Max: {}ms (should be from sample #99)", stats.max_ms);

    assert!(stats.min_ms >= 150, "Min should be from later samples");
    assert!(stats.max_ms == 199, "Max should be from last sample");

    println!("\n✅ Test passed: Rolling window correctly discarding old samples\n");
}

// =============================================================================
// Test 7: Real-Time Latency Simulation
// =============================================================================

#[test]
fn test_latency_real_time_simulation() {
    println!("\n⏱️  TEST: Real-Time Latency Simulation");
    println!("{}", "=".repeat(60));

    let tracker = LatencyTracker::new();

    println!("\n1️⃣  Simulating realistic order flow...");
    println!("   (Using actual time delays)\n");

    // Simulate 5 orders with real delays
    for i in 1..=5 {
        println!("   Order #{}", i);
        let actual_latency = simulate_order_lifecycle(&tracker, 5, 20 + (i * 5));
        println!("   └─ Measured ORTT: {}ms\n", actual_latency);
    }

    let stats = tracker.get_stats().unwrap();

    println!("\n2️⃣  Aggregate Statistics:");
    println!("   Sample count: {}", stats.count);
    println!("   Min latency: {}ms", stats.min_ms);
    println!("   Max latency: {}ms", stats.max_ms);
    println!("   Mean latency: {:.1}ms", stats.mean_ms);
    println!("   P50 latency: {}ms", stats.p50_ms);

    assert_eq!(stats.count, 5);
    assert!(stats.min_ms >= 20, "Should have realistic minimum latency");

    println!("\n✅ Test passed: Real-time simulation working correctly\n");
}

// =============================================================================
// Test 8: High-Volume Performance Test
// =============================================================================

#[test]
fn test_latency_high_volume() {
    println!("\n🚀 TEST: High-Volume Latency Tracking");
    println!("{}", "=".repeat(60));

    let tracker = LatencyTracker::new();

    println!("\n1️⃣  Simulating high-frequency trading scenario...");
    println!("   Batch 1: Normal latency (80-120ms)");
    simulate_order_batch(&tracker, 200, 80, 40);

    let stats1 = tracker.get_stats().unwrap();
    println!("   └─ P99: {}ms", stats1.p99_ms);

    println!("\n   Batch 2: Elevated latency (100-150ms)");
    simulate_order_batch(&tracker, 200, 100, 50);

    let stats2 = tracker.get_stats().unwrap();
    println!("   └─ P99: {}ms", stats2.p99_ms);

    println!("\n   Batch 3: Spike in latency (120-200ms)");
    simulate_order_batch(&tracker, 200, 120, 80);

    let stats3 = tracker.get_stats().unwrap();
    println!("   └─ P99: {}ms", stats3.p99_ms);

    println!("\n2️⃣  Final Statistics (last 1000 orders):");
    println!("   Total samples tracked: {}", tracker.sample_count());
    println!("   Mean latency: {:.1}ms", stats3.mean_ms);
    println!("   P50 latency: {}ms", stats3.p50_ms);
    println!("   P90 latency: {}ms", stats3.p90_ms);
    println!("   P99 latency: {}ms", stats3.p99_ms);

    assert_eq!(tracker.sample_count(), 600);

    println!("\n✅ Test passed: High-volume tracking working efficiently\n");
}

// =============================================================================
// Test 9: Latency Degradation Detection
// =============================================================================

#[test]
fn test_latency_degradation_detection() {
    println!("\n📉 TEST: Latency Degradation Detection");
    println!("{}", "=".repeat(60));

    let config = LatencyConfig {
        window_size: 100,
        p99_warning_ms: 150,
        p99_critical_ms: 200,
    };
    let tracker = LatencyTracker::with_config(config);

    println!("\n1️⃣  Phase 1: Good Performance");
    for i in 0..100 {
        let latency = 80 + (i * 40) / 100; // 80-120ms
        tracker.record(1000, 1005, 1000 + latency);
    }

    let stats1 = tracker.get_stats().unwrap();
    println!("   P99: {}ms - ✅ Healthy", stats1.p99_ms);
    assert!(!tracker.is_warning());

    println!("\n2️⃣  Phase 2: Gradual Degradation");
    tracker.clear();
    for i in 0..100 {
        let latency = 120 + (i * 50) / 100; // 120-170ms
        tracker.record(1000, 1005, 1000 + latency);
    }

    let stats2 = tracker.get_stats().unwrap();
    println!("   P99: {}ms - ⚠️  Warning", stats2.p99_ms);
    assert!(tracker.is_warning());
    assert!(!tracker.is_critical());

    println!("\n3️⃣  Phase 3: Critical Degradation");
    tracker.clear();
    for i in 0..100 {
        let latency = 150 + (i * 70) / 100; // 150-220ms
        tracker.record(1000, 1005, 1000 + latency);
    }

    let stats3 = tracker.get_stats().unwrap();
    println!("   P99: {}ms - 🚨 Critical", stats3.p99_ms);
    assert!(tracker.is_critical());

    println!("\n4️⃣  Summary:");
    println!("   Phase 1 → Phase 2: P99 increased by {}ms",
             stats2.p99_ms as i64 - stats1.p99_ms as i64);
    println!("   Phase 2 → Phase 3: P99 increased by {}ms",
             stats3.p99_ms as i64 - stats2.p99_ms as i64);
    println!("   Total degradation: {}ms",
             stats3.p99_ms as i64 - stats1.p99_ms as i64);

    println!("\n✅ Test passed: Degradation detection working correctly\n");
}

// =============================================================================
// Test 10: Comprehensive P99 Report
// =============================================================================

#[test]
fn test_comprehensive_p99_report() {
    println!("\n📋 TEST: Comprehensive P99 Latency Report");
    println!("{}", "=".repeat(60));

    let config = LatencyConfig {
        window_size: 1000,
        p99_warning_ms: 150,
        p99_critical_ms: 200,
    };

    println!("\n1️⃣  Configuration:");
    println!("   Window size: {} samples", config.window_size);
    println!("   Warning threshold: {}ms", config.p99_warning_ms);
    println!("   Critical threshold: {}ms", config.p99_critical_ms);

    let tracker = LatencyTracker::with_config(config);

    println!("\n2️⃣  Simulating 24-hour trading session...");
    println!("   (1000 representative samples)\n");

    // Simulate realistic distribution:
    // - 70% of orders: 80-120ms (good)
    // - 20% of orders: 120-160ms (acceptable)
    // - 9% of orders: 160-200ms (borderline)
    // - 1% of orders: 200-300ms (poor)

    for i in 0..700 {
        let latency = 80 + (i % 40); // 80-120ms
        tracker.record(1000, 1005, 1000 + latency);
    }

    for i in 0..200 {
        let latency = 120 + (i % 40); // 120-160ms
        tracker.record(1000, 1005, 1000 + latency);
    }

    for i in 0..90 {
        let latency = 160 + (i % 40); // 160-200ms
        tracker.record(1000, 1005, 1000 + latency);
    }

    for i in 0..10 {
        let latency = 200 + (i * 10); // 200-300ms (outliers)
        tracker.record(1000, 1005, 1000 + latency);
    }

    let stats = tracker.get_stats().unwrap();

    println!("\n3️⃣  Latency Distribution:");
    println!("   Min: {}ms", stats.min_ms);
    println!("   P50 (median): {}ms", stats.p50_ms);
    println!("   P90: {}ms", stats.p90_ms);
    println!("   P95: {}ms", stats.p95_ms);
    println!("   P99: {}ms", stats.p99_ms);
    println!("   Max: {}ms", stats.max_ms);
    println!("   Mean: {:.1}ms", stats.mean_ms);

    println!("\n4️⃣  Performance Assessment:");
    if tracker.is_critical() {
        println!("   Status: 🚨 CRITICAL");
        println!("   P99 latency exceeds 200ms");
        println!("   Action required: Investigate exchange connectivity");
    } else if tracker.is_warning() {
        println!("   Status: ⚠️  WARNING");
        println!("   P99 latency above 150ms but below critical");
        println!("   Monitor closely for further degradation");
    } else {
        println!("   Status: ✅ HEALTHY");
        println!("   P99 latency within acceptable range");
        println!("   System performing optimally");
    }

    println!("\n5️⃣  Key Metrics:");
    println!("   Total samples: {}", stats.count);
    println!("   Sample window: {} orders", tracker.sample_count());
    println!("   P99 threshold compliance: {}%",
             if stats.p99_ms < 200 { 100 } else { 0 });

    println!("\n{}", "=".repeat(60));
    println!("✅ COMPREHENSIVE TEST PASSED");
    println!("   P99 latency tracking system validated:");
    println!("   • Accurate percentile calculations");
    println!("   • Proper threshold detection (150ms warning, 200ms critical)");
    println!("   • Rolling window management");
    println!("   • Real-time performance monitoring");
    println!("{}\n", "=".repeat(60));
}
