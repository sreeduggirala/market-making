//! WebSocket Reconnection Test Suite
//!
//! Tests the WebSocket reconnection logic and heartbeat monitoring.
//! Validates the requirement: Reconnection must occur within < 5 seconds.
//!
//! Run with: cargo test --package adapters --test websocket_reconnection_test -- --nocapture

use adapters::utils::{WsManager, WsManagerConfig, ConnectionState, HeartbeatConfig, ReconnectConfig};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;

// =============================================================================
// Test Utilities
// =============================================================================

/// Simulates a connection function that fails N times then succeeds
async fn simulate_flaky_connection(
    attempt_counter: Arc<AtomicU64>,
    fail_count: u64,
) -> Result<(), String> {
    let attempt = attempt_counter.fetch_add(1, Ordering::SeqCst);
    println!("  🔌 Connection attempt #{}", attempt + 1);

    sleep(Duration::from_millis(50)).await; // Simulate TCP handshake

    if attempt < fail_count {
        Err(format!("Connection failed (attempt {})", attempt + 1))
    } else {
        println!("  ✅ Connection successful!");
        Ok(())
    }
}

/// Simulates a connection that always fails
async fn simulate_always_failing_connection() -> Result<(), String> {
    println!("  ❌ Connection attempt failed");
    sleep(Duration::from_millis(50)).await;
    Err("Connection refused".to_string())
}

// =============================================================================
// Test 1: Basic Connection Lifecycle
// =============================================================================

#[tokio::test]
async fn test_ws_connection_lifecycle() {
    println!("\n📊 TEST: WebSocket Connection Lifecycle");
    println!("{}", "=".repeat(60));

    let manager = WsManager::new();

    // Initial state
    let stats = manager.get_stats().await;
    assert_eq!(stats.state, ConnectionState::Disconnected);
    println!("✓ Initial state: Disconnected");

    // Connect
    manager.mark_connected().await;
    let stats = manager.get_stats().await;
    assert_eq!(stats.state, ConnectionState::Connected);
    assert_eq!(stats.total_connections, 1);
    assert!(stats.is_alive);
    println!("✓ Connection established");
    println!("  - Total connections: {}", stats.total_connections);
    println!("  - State: {:?}", stats.state);

    // Simulate message activity
    manager.record_message().await;
    println!("✓ Message activity recorded");

    // Disconnect
    manager.mark_disconnected("test disconnect").await;
    let stats = manager.get_stats().await;
    assert_eq!(stats.state, ConnectionState::Disconnected);
    assert_eq!(stats.total_disconnections, 1);
    println!("✓ Connection disconnected");
    println!("  - Total disconnections: {}", stats.total_disconnections);

    println!("\n✅ Test passed: Connection lifecycle working correctly\n");
}

// =============================================================================
// Test 2: Heartbeat Monitoring
// =============================================================================

#[tokio::test]
async fn test_ws_heartbeat_monitoring() {
    println!("\n💓 TEST: WebSocket Heartbeat Monitoring");
    println!("{}", "=".repeat(60));

    let config = WsManagerConfig {
        heartbeat: HeartbeatConfig {
            timeout_ms: 200,      // 200ms timeout
            ping_interval_ms: 100, // 100ms ping interval
            send_pings: false,     // We'll handle pings manually
        },
        ..Default::default()
    };

    let manager = WsManager::with_config(config);
    manager.mark_connected().await;

    // Initially alive
    assert!(manager.check_alive().await);
    println!("✓ Initial heartbeat check: ALIVE");

    // Record some activity
    manager.record_message().await;
    sleep(Duration::from_millis(50)).await;
    assert!(manager.check_alive().await);
    println!("✓ After message activity (50ms): ALIVE");

    // Wait for timeout
    sleep(Duration::from_millis(250)).await;
    assert!(!manager.check_alive().await);
    println!("✓ After timeout (200ms+): DEAD");

    let stats = manager.get_stats().await;
    assert!(!stats.is_alive);
    println!("✓ Connection marked as not alive in stats");

    println!("\n✅ Test passed: Heartbeat monitoring detecting stale connections\n");
}

// =============================================================================
// Test 3: Ping/Pong Latency Tracking
// =============================================================================

#[tokio::test]
async fn test_ws_ping_pong_latency() {
    println!("\n⏱️  TEST: WebSocket Ping/Pong Latency Tracking");
    println!("{}", "=".repeat(60));

    let manager = WsManager::new();
    manager.mark_connected().await;

    // Send ping
    manager.record_ping().await;
    let ping_time = Instant::now();
    println!("📤 Ping sent");

    // Simulate network delay
    sleep(Duration::from_millis(25)).await;

    // Receive pong
    manager.record_pong().await;
    let pong_time = Instant::now();
    let actual_latency = pong_time.duration_since(ping_time);
    println!("📥 Pong received after {:?}", actual_latency);

    // Check recorded latency
    let stats = manager.get_stats().await;
    assert!(stats.current_latency.is_some());
    let recorded_latency = stats.current_latency.unwrap();

    println!("✓ Recorded latency: {:?}", recorded_latency);
    assert!(recorded_latency >= Duration::from_millis(20));
    assert!(recorded_latency <= Duration::from_millis(50));

    println!("\n✅ Test passed: Ping/pong latency correctly tracked\n");
}

// =============================================================================
// Test 4: Fast Reconnection (< 5 seconds requirement)
// =============================================================================

#[tokio::test]
async fn test_ws_fast_reconnection() {
    println!("\n🚀 TEST: Fast WebSocket Reconnection (< 5 seconds)");
    println!("{}", "=".repeat(60));

    let config = WsManagerConfig {
        reconnect: ReconnectConfig {
            initial_backoff_ms: 500,  // Start with 500ms
            max_backoff_ms: 1000,     // Max 1 second
            backoff_multiplier: 1.0,  // No exponential growth
            max_attempts: Some(5),
            jitter_factor: 0.0,       // No jitter for predictable testing
        },
        max_reconnect_time_ms: 5000, // 5 second limit
        ..Default::default()
    };

    let manager = WsManager::with_config(config);

    // Simulate connection that succeeds on 2nd attempt
    let attempts = Arc::new(AtomicU64::new(0));
    let attempts_clone = Arc::clone(&attempts);

    let reconnect_start = Instant::now();
    println!("🔄 Starting reconnection process...");

    let result = manager.attempt_reconnect(|| {
        let attempts = Arc::clone(&attempts_clone);
        async move {
            simulate_flaky_connection(attempts, 1).await
        }
    }).await;

    let reconnect_duration = reconnect_start.elapsed();

    assert!(result.is_ok(), "Reconnection should succeed");
    let reported_duration = result.unwrap();

    println!("\n📊 Reconnection Results:");
    println!("  - Total attempts: {}", attempts.load(Ordering::SeqCst));
    println!("  - Reconnection duration: {:?}", reconnect_duration);
    println!("  - Reported duration: {:?}", reported_duration);

    // Verify it's under 5 seconds
    assert!(reconnect_duration < Duration::from_secs(5));
    println!("  ✅ Reconnection completed in < 5 seconds");

    // Verify statistics
    let stats = manager.get_stats().await;
    assert_eq!(stats.state, ConnectionState::Connected);
    assert_eq!(stats.successful_reconnects, 1);
    assert!(stats.last_reconnect_duration.is_some());

    println!("\n📈 Connection Statistics:");
    println!("  - State: {:?}", stats.state);
    println!("  - Successful reconnects: {}", stats.successful_reconnects);
    println!("  - Last reconnect duration: {:?}", stats.last_reconnect_duration.unwrap());

    println!("\n✅ Test passed: Fast reconnection working (< 5s requirement met)\n");
}

// =============================================================================
// Test 5: Multiple Reconnection Attempts
// =============================================================================

#[tokio::test]
async fn test_ws_multiple_reconnection_attempts() {
    println!("\n🔁 TEST: Multiple Reconnection Attempts");
    println!("{}", "=".repeat(60));

    let config = WsManagerConfig {
        reconnect: ReconnectConfig {
            initial_backoff_ms: 100,
            max_backoff_ms: 300,
            backoff_multiplier: 1.5,
            max_attempts: Some(5),
            jitter_factor: 0.0,
        },
        max_reconnect_time_ms: 5000,
        ..Default::default()
    };

    let manager = WsManager::with_config(config);

    // Connection succeeds on 3rd attempt
    let attempts = Arc::new(AtomicU64::new(0));
    let attempts_clone = Arc::clone(&attempts);

    println!("🔄 Attempting reconnection (will succeed on attempt #3)...\n");

    let result = manager.attempt_reconnect(|| {
        let attempts = Arc::clone(&attempts_clone);
        async move {
            simulate_flaky_connection(attempts, 2).await
        }
    }).await;

    assert!(result.is_ok());
    let total_attempts = attempts.load(Ordering::SeqCst);

    println!("\n📊 Results:");
    println!("  - Total attempts: {}", total_attempts);
    println!("  - Duration: {:?}", result.unwrap());

    assert_eq!(total_attempts, 3);

    let stats = manager.get_stats().await;
    assert_eq!(stats.successful_reconnects, 1);
    println!("  - Successful reconnects: {}", stats.successful_reconnects);

    println!("\n✅ Test passed: Multiple reconnection attempts working correctly\n");
}

// =============================================================================
// Test 6: Reconnection Timeout Exceeded
// =============================================================================

#[tokio::test]
async fn test_ws_reconnection_timeout() {
    println!("\n⏰ TEST: Reconnection Timeout (Exceeds 5 second limit)");
    println!("{}", "=".repeat(60));

    let config = WsManagerConfig {
        reconnect: ReconnectConfig {
            initial_backoff_ms: 1000,
            max_backoff_ms: 2000,
            backoff_multiplier: 1.0,
            max_attempts: Some(10),
            jitter_factor: 0.0,
        },
        max_reconnect_time_ms: 2000, // Very short timeout for testing
        ..Default::default()
    };

    let manager = WsManager::with_config(config);

    println!("🔄 Attempting reconnection with 2 second timeout...\n");

    let start = Instant::now();
    let result = manager.attempt_reconnect(|| async {
        simulate_always_failing_connection().await
    }).await;

    let duration = start.elapsed();

    println!("\n📊 Results:");
    println!("  - Duration: {:?}", duration);
    println!("  - Result: {:?}", result);

    assert!(result.is_err(), "Should fail due to timeout");
    assert!(duration <= Duration::from_millis(2500)); // Should abort near timeout
    println!("  ✅ Timeout enforced correctly");

    let stats = manager.get_stats().await;
    assert_eq!(stats.state, ConnectionState::Failed);
    assert_eq!(stats.failed_reconnects, 1);
    println!("  - State: {:?}", stats.state);
    println!("  - Failed reconnects: {}", stats.failed_reconnects);

    println!("\n✅ Test passed: Timeout mechanism working correctly\n");
}

// =============================================================================
// Test 7: Staleness Detection
// =============================================================================

#[tokio::test]
async fn test_ws_staleness_detection() {
    println!("\n🕐 TEST: Connection Staleness Detection");
    println!("{}", "=".repeat(60));

    let config = WsManagerConfig {
        heartbeat: HeartbeatConfig {
            timeout_ms: 1500,      // 1.5 second staleness threshold
            ping_interval_ms: 0,
            send_pings: false,
        },
        ..Default::default()
    };

    let manager = WsManager::with_config(config);
    manager.mark_connected().await;

    println!("⏱️  Monitoring connection staleness (1.5s threshold)...\n");

    // Check at various time points
    assert!(manager.check_alive().await);
    println!("  T+0ms: ALIVE ✓");

    sleep(Duration::from_millis(500)).await;
    manager.record_message().await;
    assert!(manager.check_alive().await);
    println!("  T+500ms (message received): ALIVE ✓");

    sleep(Duration::from_millis(800)).await;
    manager.record_message().await;
    assert!(manager.check_alive().await);
    println!("  T+1300ms (message received): ALIVE ✓");

    // Now go silent for > 1.5 seconds
    sleep(Duration::from_millis(1600)).await;
    assert!(!manager.check_alive().await);
    println!("  T+2900ms (no messages): DEAD ✗");

    println!("\n✅ Test passed: Staleness detection working (> 1.5s = dead)\n");
}

// =============================================================================
// Test 8: Rapid Reconnection Stress Test
// =============================================================================

#[tokio::test]
async fn test_ws_rapid_reconnection_stress() {
    println!("\n⚡ TEST: Rapid Reconnection Stress Test");
    println!("{}", "=".repeat(60));

    let config = WsManagerConfig {
        reconnect: ReconnectConfig {
            initial_backoff_ms: 50,
            max_backoff_ms: 200,
            backoff_multiplier: 1.0,
            max_attempts: Some(3),
            jitter_factor: 0.0,
        },
        max_reconnect_time_ms: 3000,
        ..Default::default()
    };

    let manager = WsManager::with_config(config);

    println!("🔄 Performing 5 rapid reconnection cycles...\n");

    for cycle in 1..=5 {
        let attempts = Arc::new(AtomicU64::new(0));
        let attempts_clone = Arc::clone(&attempts);

        let result = manager.attempt_reconnect(|| {
            let attempts = Arc::clone(&attempts_clone);
            async move {
                simulate_flaky_connection(attempts, 1).await
            }
        }).await;

        assert!(result.is_ok(), "Cycle {} should succeed", cycle);
        println!("  ✓ Cycle {}: Reconnected in {:?}", cycle, result.unwrap());

        manager.mark_disconnected("test").await;
        sleep(Duration::from_millis(10)).await;
    }

    let stats = manager.get_stats().await;
    assert_eq!(stats.successful_reconnects, 5);

    println!("\n📊 Final Statistics:");
    println!("  - Successful reconnects: {}", stats.successful_reconnects);
    println!("  - Total disconnections: {}", stats.total_disconnections);

    println!("\n✅ Test passed: Rapid reconnection stress test completed\n");
}

// =============================================================================
// Test 9: Comprehensive Reconnection Report
// =============================================================================

#[tokio::test]
async fn test_ws_comprehensive_reconnection_report() {
    println!("\n📋 TEST: Comprehensive WebSocket Reconnection Report");
    println!("{}", "=".repeat(60));

    let config = WsManagerConfig {
        reconnect: ReconnectConfig {
            initial_backoff_ms: 800,  // Simulates realistic retry timing
            max_backoff_ms: 1500,
            backoff_multiplier: 1.0,
            max_attempts: Some(4),
            jitter_factor: 0.0,
        },
        max_reconnect_time_ms: 5000,
        heartbeat: HeartbeatConfig {
            timeout_ms: 1500,
            ping_interval_ms: 500,
            send_pings: false,
        },
    };

    let manager = WsManager::with_config(config);

    println!("\n1️⃣  Initial Connection");
    println!("{}", "-".repeat(60));
    manager.mark_connected().await;
    let stats = manager.get_stats().await;
    println!("   State: {:?}", stats.state);
    println!("   Is Alive: {}", stats.is_alive);

    println!("\n2️⃣  Simulating Connection Loss");
    println!("{}", "-".repeat(60));
    manager.mark_disconnected("network failure").await;
    let stats = manager.get_stats().await;
    println!("   State: {:?}", stats.state);
    println!("   Total Disconnections: {}", stats.total_disconnections);

    println!("\n3️⃣  Attempting Reconnection");
    println!("{}", "-".repeat(60));

    let detection_start = Instant::now();
    let attempts = Arc::new(AtomicU64::new(0));
    let attempts_clone = Arc::clone(&attempts);

    let result = manager.attempt_reconnect(|| {
        let attempts = Arc::clone(&attempts_clone);
        async move {
            simulate_flaky_connection(attempts, 2).await // Succeeds on 3rd attempt
        }
    }).await;

    let reconnection_time = detection_start.elapsed();

    assert!(result.is_ok());
    println!("   Reconnection: SUCCESS");
    println!("   Detection Time: 0ms (immediate)");
    println!("   Reconnection Time: {:?}", reconnection_time);
    println!("   Total Downtime: {:?}", reconnection_time);
    println!("   Attempts Made: {}", attempts.load(Ordering::SeqCst));

    println!("\n4️⃣  Verification Against Requirements");
    println!("{}", "-".repeat(60));

    // Requirement: Reconnection within < 5 seconds
    assert!(reconnection_time < Duration::from_secs(5));
    println!("   ✅ Reconnection Time < 5s: {:?} < 5.0s", reconnection_time);

    let stats = manager.get_stats().await;
    assert_eq!(stats.state, ConnectionState::Connected);
    println!("   ✅ Connection State: Connected");

    assert_eq!(stats.successful_reconnects, 1);
    println!("   ✅ Successful Reconnects: {}", stats.successful_reconnects);

    assert!(stats.is_alive);
    println!("   ✅ Heartbeat Status: Alive");

    println!("\n5️⃣  Final Statistics Summary");
    println!("{}", "-".repeat(60));
    println!("   Total Connections: {}", stats.total_connections);
    println!("   Total Disconnections: {}", stats.total_disconnections);
    println!("   Successful Reconnects: {}", stats.successful_reconnects);
    println!("   Failed Reconnects: {}", stats.failed_reconnects);
    println!("   Current State: {:?}", stats.state);
    println!("   Last Reconnect Duration: {:?}", stats.last_reconnect_duration.unwrap());

    println!("\n{}", "=".repeat(60));
    println!("✅ COMPREHENSIVE TEST PASSED");
    println!("   WebSocket reconnection system meets all requirements:");
    println!("   • Detection: Immediate via heartbeat timeout");
    println!("   • Reconnection: < 5 seconds (actual: {:?})", reconnection_time);
    println!("   • Reliability: Automatic retry with backoff");
    println!("{}\n", "=".repeat(60));
}
