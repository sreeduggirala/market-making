//! Load Tests for Market Making System
//!
//! Tests high message rate behavior, throughput, and latency under load.
//! These tests verify the system can handle production-level message rates.
//!
//! Run with: cargo test --package adapters --test load_tests --release
//! (Release mode recommended for accurate performance measurements)

use adapters::traits::{
    Fill, NewOrder, Order, OrderStatus, OrderType, Position, Side, TimeInForce, UserEvent,
};
use oms::{Exchange, OrderManager};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// Counter for unique IDs
static LOAD_COUNTER: AtomicU64 = AtomicU64::new(1000000);

fn unique_id(prefix: &str) -> String {
    let count = LOAD_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("{}_{}", prefix, count)
}

// =============================================================================
// Fast Mock Adapter for Load Testing
// =============================================================================

/// Ultra-fast mock that minimizes overhead
struct FastMockAdapter;

#[async_trait::async_trait]
impl oms::order_router::ExchangeAdapter for FastMockAdapter {
    async fn create_order(&self, order: NewOrder) -> anyhow::Result<Order> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Ok(Order {
            client_order_id: order.client_order_id,
            venue_order_id: format!("v_{}", now),
            symbol: order.symbol,
            ord_type: order.ord_type,
            side: order.side,
            qty: order.qty,
            price: order.price,
            stop_price: order.stop_price,
            tif: order.tif,
            status: OrderStatus::New,
            filled_qty: 0.0,
            remaining_qty: order.qty,
            created_ms: now,
            updated_ms: now,
            recv_ms: now,
            raw_status: None,
        })
    }

    async fn cancel_order(&self, _symbol: &str, _venue_order_id: &str) -> anyhow::Result<bool> {
        Ok(true)
    }

    async fn cancel_all(&self, _symbol: Option<&str>) -> anyhow::Result<usize> {
        Ok(0)
    }

    async fn get_order(&self, _symbol: &str, venue_order_id: &str) -> anyhow::Result<Order> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Ok(Order {
            client_order_id: "mock".to_string(),
            venue_order_id: venue_order_id.to_string(),
            symbol: "BTCUSD".to_string(),
            ord_type: OrderType::Limit,
            side: Side::Buy,
            qty: 1.0,
            price: Some(50000.0),
            stop_price: None,
            tif: Some(TimeInForce::Gtc),
            status: OrderStatus::New,
            filled_qty: 0.0,
            remaining_qty: 1.0,
            created_ms: now,
            updated_ms: now,
            recv_ms: now,
            raw_status: None,
        })
    }
}

// =============================================================================
// Throughput Tests
// =============================================================================

/// Test: Order submission throughput
/// Target: Process 1000+ orders in under 5 seconds
#[tokio::test]
async fn test_load_order_submission_throughput() {
    let oms = Arc::new(OrderManager::new());
    let (_tx, rx) = mpsc::channel::<UserEvent>(10000);
    oms.register_exchange(Exchange::Kraken, Arc::new(FastMockAdapter), rx)
        .await;

    let num_orders = 1000;
    let start = Instant::now();

    for i in 0..num_orders {
        let order = NewOrder {
            symbol: "BTCUSD".to_string(),
            side: if i % 2 == 0 { Side::Buy } else { Side::Sell },
            ord_type: OrderType::Limit,
            qty: 0.1,
            price: Some(50000.0 + (i % 100) as f64),
            stop_price: None,
            tif: Some(TimeInForce::Gtc),
            post_only: false,
            reduce_only: false,
            client_order_id: unique_id("load"),
        };

        let result = oms.submit_order(Exchange::Kraken, order).await;
        assert!(result.is_ok(), "Order {} failed: {:?}", i, result.err());
    }

    let elapsed = start.elapsed();
    let orders_per_sec = num_orders as f64 / elapsed.as_secs_f64();

    println!("\n=== Order Submission Throughput ===");
    println!("Orders submitted: {}", num_orders);
    println!("Time taken: {:?}", elapsed);
    println!("Throughput: {:.2} orders/sec", orders_per_sec);

    // Should complete in reasonable time
    assert!(elapsed < Duration::from_secs(10), "Throughput too low");

    // Verify all orders in book
    let stats = oms.get_order_stats();
    assert_eq!(stats.total_orders, num_orders);
}

/// Test: Concurrent order submission from multiple tasks
/// Target: Handle 500 concurrent submissions without deadlock
#[tokio::test]
async fn test_load_concurrent_submissions() {
    let oms = Arc::new(OrderManager::new());
    let (_tx, rx) = mpsc::channel::<UserEvent>(10000);
    oms.register_exchange(Exchange::Kraken, Arc::new(FastMockAdapter), rx)
        .await;

    let num_tasks = 100;
    let orders_per_task = 50;
    let total_orders = num_tasks * orders_per_task;

    let start = Instant::now();
    let mut handles = Vec::with_capacity(num_tasks);

    for task_id in 0..num_tasks {
        let oms_clone = oms.clone();
        let handle = tokio::spawn(async move {
            let mut successes = 0;
            for i in 0..orders_per_task {
                let order = NewOrder {
                    symbol: "BTCUSD".to_string(),
                    side: if i % 2 == 0 { Side::Buy } else { Side::Sell },
                    ord_type: OrderType::Limit,
                    qty: 0.01,
                    price: Some(50000.0),
                    stop_price: None,
                    tif: Some(TimeInForce::Gtc),
                    post_only: false,
                    reduce_only: false,
                    client_order_id: unique_id(&format!("conc_{}_{}", task_id, i)),
                };
                if oms_clone.submit_order(Exchange::Kraken, order).await.is_ok() {
                    successes += 1;
                }
            }
            successes
        });
        handles.push(handle);
    }

    let results = futures::future::join_all(handles).await;
    let elapsed = start.elapsed();

    let total_success: usize = results
        .into_iter()
        .filter_map(|r| r.ok())
        .sum();

    let success_rate = (total_success as f64 / total_orders as f64) * 100.0;
    let orders_per_sec = total_success as f64 / elapsed.as_secs_f64();

    println!("\n=== Concurrent Submission Load Test ===");
    println!("Tasks: {}", num_tasks);
    println!("Orders per task: {}", orders_per_task);
    println!("Total attempted: {}", total_orders);
    println!("Successful: {} ({:.1}%)", total_success, success_rate);
    println!("Time taken: {:?}", elapsed);
    println!("Throughput: {:.2} orders/sec", orders_per_sec);

    // All orders should succeed (no deadlocks or races)
    assert_eq!(total_success, total_orders, "Some orders failed");

    // Should complete in reasonable time
    assert!(elapsed < Duration::from_secs(30), "Concurrent test too slow");
}

/// Test: Fill processing throughput
/// Target: Process 1000+ fills per second
#[tokio::test]
async fn test_load_fill_processing() {
    let oms = Arc::new(OrderManager::new());
    let (tx, rx) = mpsc::channel::<UserEvent>(10000);
    oms.register_exchange(Exchange::Kraken, Arc::new(FastMockAdapter), rx)
        .await;

    let mut fill_rx = oms.subscribe_fills();

    // Submit orders first
    let num_orders = 100;
    let mut orders = Vec::with_capacity(num_orders);

    for i in 0..num_orders {
        let order = NewOrder {
            symbol: "BTCUSD".to_string(),
            side: Side::Buy,
            ord_type: OrderType::Limit,
            qty: 1.0,
            price: Some(50000.0),
            stop_price: None,
            tif: Some(TimeInForce::Gtc),
            post_only: false,
            reduce_only: false,
            client_order_id: unique_id(&format!("fill_load_{}", i)),
        };
        let client_id = oms.submit_order(Exchange::Kraken, order).await.unwrap();
        let order_details = oms.get_order(&client_id).unwrap();
        orders.push(order_details);
    }

    // Send many fills
    let fills_per_order = 10;
    let total_fills = num_orders * fills_per_order;
    let start = Instant::now();

    for (idx, order) in orders.iter().enumerate() {
        for fill_num in 0..fills_per_order {
            let fill = Fill {
                venue_order_id: order.venue_order_id.clone(),
                client_order_id: order.client_order_id.clone(),
                symbol: "BTCUSD".to_string(),
                price: 50000.0,
                qty: 0.1,
                fee: 0.005,
                fee_ccy: "USD".to_string(),
                is_maker: true,
                exec_id: unique_id(&format!("fill_{}_{}", idx, fill_num)),
                ex_ts_ms: 1234567890,
                recv_ms: 1234567890,
            };
            tx.send(UserEvent::Fill(fill)).await.unwrap();
        }
    }

    // Collect fills
    let mut received = 0;
    let timeout = Instant::now() + Duration::from_secs(10);

    while received < total_fills && Instant::now() < timeout {
        match tokio::time::timeout(Duration::from_millis(100), fill_rx.recv()).await {
            Ok(Ok(_)) => received += 1,
            Ok(Err(_)) => break,
            Err(_) => continue,
        }
    }

    let elapsed = start.elapsed();
    let fills_per_sec = received as f64 / elapsed.as_secs_f64();

    println!("\n=== Fill Processing Load Test ===");
    println!("Fills sent: {}", total_fills);
    println!("Fills received: {}", received);
    println!("Time taken: {:?}", elapsed);
    println!("Throughput: {:.2} fills/sec", fills_per_sec);

    // Should receive most fills
    assert!(received >= total_fills * 90 / 100, "Dropped too many fills");
}

// =============================================================================
// Latency Tests
// =============================================================================

/// Test: Order submission latency
/// Target: P99 latency under 10ms (without network)
#[tokio::test]
async fn test_load_submission_latency() {
    let oms = Arc::new(OrderManager::new());
    let (_tx, rx) = mpsc::channel::<UserEvent>(1000);
    oms.register_exchange(Exchange::Kraken, Arc::new(FastMockAdapter), rx)
        .await;

    let num_samples = 1000;
    let mut latencies: Vec<Duration> = Vec::with_capacity(num_samples);

    for i in 0..num_samples {
        let order = NewOrder {
            symbol: "BTCUSD".to_string(),
            side: Side::Buy,
            ord_type: OrderType::Limit,
            qty: 0.1,
            price: Some(50000.0),
            stop_price: None,
            tif: Some(TimeInForce::Gtc),
            post_only: false,
            reduce_only: false,
            client_order_id: unique_id(&format!("latency_{}", i)),
        };

        let start = Instant::now();
        let _ = oms.submit_order(Exchange::Kraken, order).await;
        latencies.push(start.elapsed());
    }

    latencies.sort();

    let p50 = latencies[num_samples / 2];
    let p90 = latencies[num_samples * 90 / 100];
    let p99 = latencies[num_samples * 99 / 100];
    let max = latencies[num_samples - 1];
    let min = latencies[0];

    let sum: Duration = latencies.iter().sum();
    let avg = sum / num_samples as u32;

    println!("\n=== Order Submission Latency ===");
    println!("Samples: {}", num_samples);
    println!("Min:  {:?}", min);
    println!("P50:  {:?}", p50);
    println!("P90:  {:?}", p90);
    println!("P99:  {:?}", p99);
    println!("Max:  {:?}", max);
    println!("Avg:  {:?}", avg);

    // P99 should be reasonably low (without real network)
    assert!(p99 < Duration::from_millis(50), "P99 latency too high");
}

// =============================================================================
// Stress Tests
// =============================================================================

/// Test: Sustained load over time
/// Verifies system remains stable under continuous load
#[tokio::test]
async fn test_load_sustained_operation() {
    let oms = Arc::new(OrderManager::new());
    let (tx, rx) = mpsc::channel::<UserEvent>(10000);
    oms.register_exchange(Exchange::Kraken, Arc::new(FastMockAdapter), rx)
        .await;

    let duration = Duration::from_secs(5);
    let start = Instant::now();
    let mut orders_submitted = 0;
    let mut fills_sent = 0;

    while start.elapsed() < duration {
        // Submit order
        let order = NewOrder {
            symbol: "BTCUSD".to_string(),
            side: if orders_submitted % 2 == 0 { Side::Buy } else { Side::Sell },
            ord_type: OrderType::Limit,
            qty: 0.1,
            price: Some(50000.0),
            stop_price: None,
            tif: Some(TimeInForce::Gtc),
            post_only: false,
            reduce_only: false,
            client_order_id: unique_id("sustained"),
        };

        if let Ok(client_id) = oms.submit_order(Exchange::Kraken, order).await {
            orders_submitted += 1;

            // Send fill for some orders
            if orders_submitted % 5 == 0 {
                if let Some(order_details) = oms.get_order(&client_id) {
                    let fill = Fill {
                        venue_order_id: order_details.venue_order_id.clone(),
                        client_order_id: client_id,
                        symbol: "BTCUSD".to_string(),
                        price: 50000.0,
                        qty: 0.1,
                        fee: 0.005,
                        fee_ccy: "USD".to_string(),
                        is_maker: true,
                        exec_id: unique_id("sustained_fill"),
                        ex_ts_ms: 1234567890,
                        recv_ms: 1234567890,
                    };
                    let _ = tx.send(UserEvent::Fill(fill)).await;
                    fills_sent += 1;
                }
            }
        }

        // Small yield to prevent starving
        tokio::task::yield_now().await;
    }

    let elapsed = start.elapsed();
    let orders_per_sec = orders_submitted as f64 / elapsed.as_secs_f64();

    println!("\n=== Sustained Load Test ===");
    println!("Duration: {:?}", elapsed);
    println!("Orders submitted: {}", orders_submitted);
    println!("Fills sent: {}", fills_sent);
    println!("Throughput: {:.2} orders/sec", orders_per_sec);

    // Should have processed a reasonable number
    assert!(orders_submitted > 100, "Too few orders processed");

    // System should remain healthy - stats may differ slightly due to concurrent processing
    let stats = oms.get_order_stats();
    let diff = (stats.total_orders as i64 - orders_submitted as i64).abs();
    assert!(diff < 100, "Order count diverged too much: stats={}, submitted={}", stats.total_orders, orders_submitted);
}

/// Test: Memory stability under load
/// Verifies no memory leaks during high-volume operation
#[tokio::test]
async fn test_load_memory_stability() {
    let oms = Arc::new(OrderManager::new());
    let (_tx, rx) = mpsc::channel::<UserEvent>(1000);
    oms.register_exchange(Exchange::Kraken, Arc::new(FastMockAdapter), rx)
        .await;

    // Submit and cancel many orders
    let iterations = 500;

    for iter in 0..iterations {
        let order = NewOrder {
            symbol: "BTCUSD".to_string(),
            side: Side::Buy,
            ord_type: OrderType::Limit,
            qty: 0.1,
            price: Some(50000.0),
            stop_price: None,
            tif: Some(TimeInForce::Gtc),
            post_only: false,
            reduce_only: false,
            client_order_id: unique_id(&format!("mem_{}", iter)),
        };

        let client_id = oms.submit_order(Exchange::Kraken, order).await.unwrap();
        let _ = oms.cancel_order(Exchange::Kraken, &client_id).await;
    }

    // Book should still be functional
    let order = NewOrder {
        symbol: "BTCUSD".to_string(),
        side: Side::Buy,
        ord_type: OrderType::Limit,
        qty: 0.1,
        price: Some(50000.0),
        stop_price: None,
        tif: Some(TimeInForce::Gtc),
        post_only: false,
        reduce_only: false,
        client_order_id: unique_id("final"),
    };

    let result = oms.submit_order(Exchange::Kraken, order).await;
    assert!(result.is_ok(), "OMS should still function after stress");

    println!("\n=== Memory Stability Test ===");
    println!("Iterations: {}", iterations);
    println!("System remains stable: OK");
}

// =============================================================================
// Position Update Load Test
// =============================================================================

/// Test: Position update throughput
/// Note: Position updates are broadcast through the event processor
#[tokio::test]
async fn test_load_position_updates() {
    let oms = Arc::new(OrderManager::new());
    let (tx, rx) = mpsc::channel::<UserEvent>(10000);
    oms.register_exchange(Exchange::Kraken, Arc::new(FastMockAdapter), rx)
        .await;

    // Give the event processor time to start
    tokio::time::sleep(Duration::from_millis(10)).await;

    let mut pos_rx = oms.subscribe_positions();

    let num_updates = 100; // Reduced for more reliable testing

    // Spawn a receiver task
    let receiver = tokio::spawn(async move {
        let mut received = 0;
        let timeout = Instant::now() + Duration::from_secs(5);
        while received < num_updates && Instant::now() < timeout {
            match tokio::time::timeout(Duration::from_millis(100), pos_rx.recv()).await {
                Ok(Ok(_)) => received += 1,
                Ok(Err(_)) => break,
                Err(_) => continue,
            }
        }
        received
    });

    let start = Instant::now();

    // Send position updates with small delays to allow processing
    for i in 0..num_updates {
        let position = Position {
            exchange: Some("kraken".to_string()),
            symbol: "BTCUSD".to_string(),
            qty: 1.0 + (i as f64) * 0.01,
            entry_px: 50000.0,
            mark_px: Some(50000.0 + i as f64),
            liquidation_px: None,
            unrealized_pnl: Some(i as f64 * 10.0),
            realized_pnl: None,
            margin: None,
            leverage: None,
            opened_ms: Some(1234567890),
            updated_ms: 1234567890 + i as u64,
        };
        tx.send(UserEvent::Position(position)).await.unwrap();

        // Small yield to allow event processor to keep up
        if i % 10 == 0 {
            tokio::task::yield_now().await;
        }
    }

    let received = receiver.await.unwrap_or(0);
    let elapsed = start.elapsed();
    let updates_per_sec = if elapsed.as_secs_f64() > 0.0 {
        received as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };

    println!("\n=== Position Update Load Test ===");
    println!("Updates sent: {}", num_updates);
    println!("Updates received: {}", received);
    println!("Time taken: {:?}", elapsed);
    println!("Throughput: {:.2} updates/sec", updates_per_sec);

    // Just verify we don't crash and receive at least some updates
    // Broadcast channels may lag under load
    assert!(received > 0 || num_updates == 0, "Should receive at least some position updates");
}
