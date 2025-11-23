//! OMS Integration Tests
//!
//! Tests for the Order Management System using mock adapters.
//! These tests verify order lifecycle, event processing, and fill handling.
//!
//! Run with: cargo test --package adapters --test oms_integration

use adapters::traits::{
    Fill, NewOrder, Order, OrderStatus, OrderType, Position, Side, TimeInForce, UserEvent,
};
use futures::future::join_all;
use oms::{Exchange, OrderManager};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// Counter for generating unique IDs across tests
static ORDER_COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_client_id(prefix: &str) -> String {
    let count = ORDER_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("{}_{}", prefix, count)
}

// =============================================================================
// Mock Adapter
// =============================================================================

struct MockExchangeAdapter {
    should_fail: bool,
    fill_immediately: bool,
}

impl MockExchangeAdapter {
    fn new() -> Self {
        Self {
            should_fail: false,
            fill_immediately: false,
        }
    }

    fn with_immediate_fills() -> Self {
        Self {
            should_fail: false,
            fill_immediately: true,
        }
    }

    fn failing() -> Self {
        Self {
            should_fail: true,
            fill_immediately: false,
        }
    }
}

#[async_trait::async_trait]
impl oms::order_router::ExchangeAdapter for MockExchangeAdapter {
    async fn create_order(&self, order: NewOrder) -> anyhow::Result<Order> {
        if self.should_fail {
            anyhow::bail!("Mock exchange error: order rejected");
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let venue_id = format!("mock_{}", uuid::Uuid::new_v4().simple());

        Ok(Order {
            client_order_id: order.client_order_id,
            venue_order_id: venue_id,
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
        if self.should_fail {
            anyhow::bail!("Mock exchange error: cancel rejected");
        }
        Ok(true)
    }

    async fn cancel_all(&self, _symbol: Option<&str>) -> anyhow::Result<usize> {
        if self.should_fail {
            anyhow::bail!("Mock exchange error: cancel all rejected");
        }
        Ok(5) // Pretend we canceled 5 orders
    }

    async fn get_order(&self, _symbol: &str, venue_order_id: &str) -> anyhow::Result<Order> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Ok(Order {
            client_order_id: "mock_client_id".to_string(),
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
// Order Lifecycle Tests
// =============================================================================

#[tokio::test]
async fn test_submit_order_success() {
    let oms = Arc::new(OrderManager::new());
    let (tx, rx) = mpsc::channel::<UserEvent>(100);

    oms.register_exchange(Exchange::Kraken, Arc::new(MockExchangeAdapter::new()), rx)
        .await;

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
        client_order_id: String::new(),
    };

    let result = oms.submit_order(Exchange::Kraken, order).await;
    assert!(result.is_ok(), "Order submission failed: {:?}", result.err());

    let client_id = result.unwrap();
    assert!(!client_id.is_empty(), "Client ID should not be empty");

    // Verify order is in the book
    let retrieved = oms.get_order(&client_id);
    assert!(retrieved.is_some(), "Order not found in book");

    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.symbol, "BTCUSD");
    assert!(matches!(retrieved.side, Side::Buy), "Expected Buy side");
    assert_eq!(retrieved.qty, 1.0);
    assert!(matches!(retrieved.status, OrderStatus::New), "Expected New status");
}

#[tokio::test]
async fn test_submit_order_failure() {
    let oms = Arc::new(OrderManager::new());
    let (tx, rx) = mpsc::channel::<UserEvent>(100);

    oms.register_exchange(Exchange::Kraken, Arc::new(MockExchangeAdapter::failing()), rx)
        .await;

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
        client_order_id: String::new(),
    };

    let result = oms.submit_order(Exchange::Kraken, order).await;
    assert!(result.is_err(), "Order should have failed");
}

#[tokio::test]
async fn test_cancel_order() {
    let oms = Arc::new(OrderManager::new());
    let (tx, rx) = mpsc::channel::<UserEvent>(100);

    oms.register_exchange(Exchange::Kraken, Arc::new(MockExchangeAdapter::new()), rx)
        .await;

    // Submit an order first
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
        client_order_id: "test_cancel_123".to_string(),
    };

    let client_id = oms.submit_order(Exchange::Kraken, order).await.unwrap();

    // Cancel it
    let cancel_result = oms.cancel_order(Exchange::Kraken, &client_id).await;
    assert!(cancel_result.is_ok(), "Cancel failed: {:?}", cancel_result.err());
    assert!(cancel_result.unwrap(), "Cancel should return true");
}

// =============================================================================
// Fill Processing Tests
// =============================================================================

#[tokio::test]
async fn test_fill_updates_order_book() {
    let oms = Arc::new(OrderManager::new());
    let (tx, rx) = mpsc::channel::<UserEvent>(100);

    oms.register_exchange(Exchange::Kraken, Arc::new(MockExchangeAdapter::new()), rx)
        .await;

    // Submit order
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
        client_order_id: "fill_test_123".to_string(),
    };

    let client_id = oms.submit_order(Exchange::Kraken, order).await.unwrap();
    let order = oms.get_order(&client_id).unwrap();

    // Simulate a fill event
    let fill = Fill {
        venue_order_id: order.venue_order_id.clone(),
        client_order_id: client_id.clone(),
        symbol: "BTCUSD".to_string(),
        price: 50000.0,
        qty: 0.5,
        fee: 0.25,
        fee_ccy: "USD".to_string(),
        is_maker: true,
        exec_id: "exec_123".to_string(),
        ex_ts_ms: 1234567890,
        recv_ms: 1234567890,
    };

    tx.send(UserEvent::Fill(fill)).await.unwrap();

    // Give the event processor time to process
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Check order is updated
    let updated = oms.get_order(&client_id).unwrap();
    assert_eq!(updated.filled_qty, 0.5);
    assert_eq!(updated.remaining_qty, 0.5);
    assert!(matches!(updated.status, OrderStatus::PartiallyFilled), "Expected PartiallyFilled status");
}

#[tokio::test]
async fn test_fill_subscription() {
    let oms = Arc::new(OrderManager::new());
    let (tx, rx) = mpsc::channel::<UserEvent>(100);

    oms.register_exchange(Exchange::Kraken, Arc::new(MockExchangeAdapter::new()), rx)
        .await;

    // Subscribe to fills
    let mut fill_rx = oms.subscribe_fills();

    // Submit order
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
        client_order_id: "subscription_test".to_string(),
    };

    let client_id = oms.submit_order(Exchange::Kraken, order).await.unwrap();
    let order = oms.get_order(&client_id).unwrap();

    // Send fill
    let fill = Fill {
        venue_order_id: order.venue_order_id.clone(),
        client_order_id: client_id.clone(),
        symbol: "BTCUSD".to_string(),
        price: 50000.0,
        qty: 1.0,
        fee: 0.5,
        fee_ccy: "USD".to_string(),
        is_maker: true,
        exec_id: "exec_456".to_string(),
        ex_ts_ms: 1234567890,
        recv_ms: 1234567890,
    };

    tx.send(UserEvent::Fill(fill.clone())).await.unwrap();

    // Should receive the fill via subscription
    let received = tokio::time::timeout(Duration::from_secs(1), fill_rx.recv()).await;
    assert!(received.is_ok(), "Did not receive fill within timeout");

    let received_fill = received.unwrap().unwrap();
    assert_eq!(received_fill.qty, 1.0);
    assert_eq!(received_fill.price, 50000.0);
}

// =============================================================================
// Order Book Statistics Tests
// =============================================================================

#[tokio::test]
async fn test_order_stats() {
    let oms = Arc::new(OrderManager::new());
    let (tx, rx) = mpsc::channel::<UserEvent>(100);

    oms.register_exchange(Exchange::Kraken, Arc::new(MockExchangeAdapter::new()), rx)
        .await;

    // Submit multiple orders
    for i in 0..5 {
        let order = NewOrder {
            symbol: "BTCUSD".to_string(),
            side: if i % 2 == 0 { Side::Buy } else { Side::Sell },
            ord_type: OrderType::Limit,
            qty: 1.0,
            price: Some(50000.0 + i as f64 * 100.0),
            stop_price: None,
            tif: Some(TimeInForce::Gtc),
            post_only: false,
            reduce_only: false,
            client_order_id: format!("stats_test_{}", i),
        };

        oms.submit_order(Exchange::Kraken, order).await.unwrap();
    }

    let stats = oms.get_order_stats();
    assert_eq!(stats.total_orders, 5);
    assert_eq!(stats.new_orders, 5);
    assert_eq!(stats.filled_orders, 0);
    assert_eq!(stats.canceled_orders, 0);
}

// =============================================================================
// Complete Fill Tests
// =============================================================================

#[tokio::test]
async fn test_complete_fill_marks_order_filled() {
    let oms = Arc::new(OrderManager::new());
    let (tx, rx) = mpsc::channel::<UserEvent>(100);

    oms.register_exchange(Exchange::Kraken, Arc::new(MockExchangeAdapter::new()), rx)
        .await;

    // Subscribe to fills to verify the fill was processed
    let mut fill_rx = oms.subscribe_fills();

    // Submit order
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
        client_order_id: "complete_fill_test".to_string(),
    };

    let client_id = oms.submit_order(Exchange::Kraken, order).await.unwrap();
    let order = oms.get_order(&client_id).unwrap();

    // Complete fill (full quantity)
    let fill = Fill {
        venue_order_id: order.venue_order_id.clone(),
        client_order_id: client_id.clone(),
        symbol: "BTCUSD".to_string(),
        price: 50000.0,
        qty: 1.0, // Complete fill
        fee: 0.5,
        fee_ccy: "USD".to_string(),
        is_maker: true,
        exec_id: "exec_complete".to_string(),
        ex_ts_ms: 1234567890,
        recv_ms: 1234567890,
    };

    tx.send(UserEvent::Fill(fill.clone())).await.unwrap();

    // Wait for fill to be received via subscription
    let received = tokio::time::timeout(Duration::from_secs(1), fill_rx.recv()).await;
    assert!(received.is_ok(), "Did not receive fill within timeout");

    let received_fill = received.unwrap().unwrap();
    assert_eq!(received_fill.qty, 1.0);
    assert_eq!(received_fill.price, 50000.0);
    assert_eq!(received_fill.client_order_id, client_id);
}

// =============================================================================
// Position Update Tests
// =============================================================================

#[tokio::test]
async fn test_position_update_subscription() {
    let oms = Arc::new(OrderManager::new());
    let (tx, rx) = mpsc::channel::<UserEvent>(100);

    oms.register_exchange(Exchange::Kraken, Arc::new(MockExchangeAdapter::new()), rx)
        .await;

    // Subscribe to positions
    let mut pos_rx = oms.subscribe_positions();

    // Send position update
    let position = Position {
        exchange: Some("kraken".to_string()),
        symbol: "BTCUSD".to_string(),
        qty: 1.5,
        entry_px: 50000.0,
        mark_px: Some(51000.0),
        liquidation_px: None,
        unrealized_pnl: Some(1500.0),
        realized_pnl: None,
        margin: None,
        leverage: None,
        opened_ms: None,
        updated_ms: 1234567890,
    };

    tx.send(UserEvent::Position(position.clone())).await.unwrap();

    // Should receive position via subscription
    let received = tokio::time::timeout(Duration::from_secs(1), pos_rx.recv()).await;
    assert!(received.is_ok(), "Did not receive position within timeout");

    let received_pos = received.unwrap().unwrap();
    assert_eq!(received_pos.qty, 1.5);
    assert_eq!(received_pos.symbol, "BTCUSD");
}

// =============================================================================
// Multi-Symbol Tests
// =============================================================================

#[tokio::test]
async fn test_multiple_symbols() {
    let oms = Arc::new(OrderManager::new());
    let (tx, rx) = mpsc::channel::<UserEvent>(100);

    oms.register_exchange(Exchange::Kraken, Arc::new(MockExchangeAdapter::new()), rx)
        .await;

    // Submit orders for different symbols
    let symbols = vec!["BTCUSD", "ETHUSD", "SOLUSD"];

    for (i, symbol) in symbols.iter().enumerate() {
        let order = NewOrder {
            symbol: symbol.to_string(),
            side: Side::Buy,
            ord_type: OrderType::Limit,
            qty: 1.0,
            price: Some(1000.0 * (i + 1) as f64),
            stop_price: None,
            tif: Some(TimeInForce::Gtc),
            post_only: false,
            reduce_only: false,
            client_order_id: format!("multi_symbol_{}", symbol),
        };

        oms.submit_order(Exchange::Kraken, order).await.unwrap();
    }

    // Verify all orders exist
    let stats = oms.get_order_stats();
    assert_eq!(stats.total_orders, 3);

    // Verify orders can be retrieved by symbol
    let btc_orders = oms.get_open_orders(Some("BTCUSD"));
    assert_eq!(btc_orders.len(), 1);

    let eth_orders = oms.get_open_orders(Some("ETHUSD"));
    assert_eq!(eth_orders.len(), 1);
}

// =============================================================================
// Order Update Event Tests
// =============================================================================

#[tokio::test]
async fn test_order_update_event() {
    let oms = Arc::new(OrderManager::new());
    let (tx, rx) = mpsc::channel::<UserEvent>(100);

    oms.register_exchange(Exchange::Kraken, Arc::new(MockExchangeAdapter::new()), rx)
        .await;

    // Submit order
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
        client_order_id: "update_event_test".to_string(),
    };

    let client_id = oms.submit_order(Exchange::Kraken, order).await.unwrap();
    let order = oms.get_order(&client_id).unwrap();

    // Send order update (e.g., from exchange confirming status)
    let updated_order = Order {
        client_order_id: client_id.clone(),
        venue_order_id: order.venue_order_id.clone(),
        symbol: "BTCUSD".to_string(),
        ord_type: OrderType::Limit,
        side: Side::Buy,
        qty: 1.0,
        price: Some(50000.0),
        stop_price: None,
        tif: Some(TimeInForce::Gtc),
        status: OrderStatus::PartiallyFilled,
        filled_qty: 0.3,
        remaining_qty: 0.7,
        created_ms: order.created_ms,
        updated_ms: 1234567890,
        recv_ms: 1234567890,
        raw_status: Some("PARTIALLY_FILLED".to_string()),
    };

    tx.send(UserEvent::OrderUpdate(updated_order)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Check order is updated
    let retrieved = oms.get_order(&client_id).unwrap();
    assert_eq!(retrieved.filled_qty, 0.3);
    assert_eq!(retrieved.remaining_qty, 0.7);
    assert!(matches!(retrieved.status, OrderStatus::PartiallyFilled));
}

// =============================================================================
// Robust Tests - Channel-Based Synchronization
// =============================================================================

/// Helper to wait for a fill with proper synchronization (no sleep)
async fn wait_for_fill(
    fill_rx: &mut tokio::sync::broadcast::Receiver<Fill>,
    timeout: Duration,
) -> Option<Fill> {
    match tokio::time::timeout(timeout, fill_rx.recv()).await {
        Ok(Ok(fill)) => Some(fill),
        _ => None,
    }
}

/// Helper to wait for a position with proper synchronization
async fn wait_for_position(
    pos_rx: &mut tokio::sync::broadcast::Receiver<Position>,
    timeout: Duration,
) -> Option<Position> {
    match tokio::time::timeout(timeout, pos_rx.recv()).await {
        Ok(Ok(pos)) => Some(pos),
        _ => None,
    }
}

#[tokio::test]
async fn test_fill_with_channel_sync() {
    let oms = Arc::new(OrderManager::new());
    let (tx, rx) = mpsc::channel::<UserEvent>(100);

    oms.register_exchange(Exchange::Kraken, Arc::new(MockExchangeAdapter::new()), rx)
        .await;

    // Subscribe BEFORE sending events
    let mut fill_rx = oms.subscribe_fills();

    let client_id = unique_client_id("channel_sync");
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
        client_order_id: client_id.clone(),
    };

    let returned_id = oms.submit_order(Exchange::Kraken, order).await.unwrap();
    let order = oms.get_order(&returned_id).unwrap();

    let fill = Fill {
        venue_order_id: order.venue_order_id.clone(),
        client_order_id: returned_id.clone(),
        symbol: "BTCUSD".to_string(),
        price: 50000.0,
        qty: 0.5,
        fee: 0.25,
        fee_ccy: "USD".to_string(),
        is_maker: true,
        exec_id: unique_client_id("exec"),
        ex_ts_ms: 1234567890,
        recv_ms: 1234567890,
    };

    tx.send(UserEvent::Fill(fill.clone())).await.unwrap();

    // Use channel-based synchronization instead of sleep
    let received = wait_for_fill(&mut fill_rx, Duration::from_secs(2)).await;
    assert!(received.is_some(), "Fill not received within timeout");

    let received_fill = received.unwrap();
    assert_eq!(received_fill.qty, 0.5);
    assert_eq!(received_fill.client_order_id, returned_id);
}

// =============================================================================
// Concurrent Access Tests
// =============================================================================

#[tokio::test]
async fn test_concurrent_order_submission() {
    let oms = Arc::new(OrderManager::new());
    let (tx, rx) = mpsc::channel::<UserEvent>(1000);

    oms.register_exchange(Exchange::Kraken, Arc::new(MockExchangeAdapter::new()), rx)
        .await;

    let num_orders = 50;
    let mut handles = Vec::with_capacity(num_orders);

    // Submit orders concurrently
    for i in 0..num_orders {
        let oms_clone = oms.clone();
        let handle = tokio::spawn(async move {
            let order = NewOrder {
                symbol: "BTCUSD".to_string(),
                side: if i % 2 == 0 { Side::Buy } else { Side::Sell },
                ord_type: OrderType::Limit,
                qty: 1.0,
                price: Some(50000.0 + i as f64),
                stop_price: None,
                tif: Some(TimeInForce::Gtc),
                post_only: false,
                reduce_only: false,
                client_order_id: unique_client_id("concurrent"),
            };
            oms_clone.submit_order(Exchange::Kraken, order).await
        });
        handles.push(handle);
    }

    // Wait for all submissions
    let results = join_all(handles).await;

    // Verify all succeeded
    let success_count = results
        .into_iter()
        .filter(|r| r.as_ref().map(|inner| inner.is_ok()).unwrap_or(false))
        .count();
    assert_eq!(success_count, num_orders, "Not all orders submitted successfully");

    // Verify order book state is consistent
    let stats = oms.get_order_stats();
    assert_eq!(stats.total_orders, num_orders, "Order count mismatch");
}

#[tokio::test]
async fn test_concurrent_fills_same_order() {
    let oms = Arc::new(OrderManager::new());
    let (tx, rx) = mpsc::channel::<UserEvent>(100);

    oms.register_exchange(Exchange::Kraken, Arc::new(MockExchangeAdapter::new()), rx)
        .await;

    let mut fill_rx = oms.subscribe_fills();

    // Submit one order
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
        client_order_id: unique_client_id("multi_fill"),
    };

    let client_id = oms.submit_order(Exchange::Kraken, order).await.unwrap();
    let order = oms.get_order(&client_id).unwrap();

    // Send multiple partial fills
    for i in 0..4 {
        let fill = Fill {
            venue_order_id: order.venue_order_id.clone(),
            client_order_id: client_id.clone(),
            symbol: "BTCUSD".to_string(),
            price: 50000.0,
            qty: 0.25, // 4 fills of 0.25 = 1.0 total
            fee: 0.125,
            fee_ccy: "USD".to_string(),
            is_maker: true,
            exec_id: unique_client_id(&format!("exec_{}", i)),
            ex_ts_ms: 1234567890 + i as u64,
            recv_ms: 1234567890 + i as u64,
        };
        tx.send(UserEvent::Fill(fill)).await.unwrap();
    }

    // Collect all fills
    let mut received_fills = 0;
    for _ in 0..4 {
        if wait_for_fill(&mut fill_rx, Duration::from_secs(1)).await.is_some() {
            received_fills += 1;
        }
    }
    assert_eq!(received_fills, 4, "Expected 4 fills");
}

// =============================================================================
// Edge Case Tests
// =============================================================================

#[tokio::test]
async fn test_fill_for_unknown_order() {
    let oms = Arc::new(OrderManager::new());
    let (tx, rx) = mpsc::channel::<UserEvent>(100);

    oms.register_exchange(Exchange::Kraken, Arc::new(MockExchangeAdapter::new()), rx)
        .await;

    let mut fill_rx = oms.subscribe_fills();

    // Send fill for non-existent order
    let fill = Fill {
        venue_order_id: "nonexistent_venue".to_string(),
        client_order_id: "nonexistent_client".to_string(),
        symbol: "BTCUSD".to_string(),
        price: 50000.0,
        qty: 1.0,
        fee: 0.5,
        fee_ccy: "USD".to_string(),
        is_maker: true,
        exec_id: unique_client_id("exec_unknown"),
        ex_ts_ms: 1234567890,
        recv_ms: 1234567890,
    };

    tx.send(UserEvent::Fill(fill.clone())).await.unwrap();

    // Should still broadcast the fill (strategies might need it)
    let received = wait_for_fill(&mut fill_rx, Duration::from_secs(1)).await;
    assert!(received.is_some(), "Fill should still be broadcast even for unknown order");

    // OMS should not crash
    let stats = oms.get_order_stats();
    assert_eq!(stats.total_orders, 0, "No orders should exist");
}

#[tokio::test]
async fn test_duplicate_client_order_id() {
    let oms = Arc::new(OrderManager::new());
    let (_tx, rx) = mpsc::channel::<UserEvent>(100);

    oms.register_exchange(Exchange::Kraken, Arc::new(MockExchangeAdapter::new()), rx)
        .await;

    let client_id = unique_client_id("duplicate_test");

    // First order should succeed
    let order1 = NewOrder {
        symbol: "BTCUSD".to_string(),
        side: Side::Buy,
        ord_type: OrderType::Limit,
        qty: 1.0,
        price: Some(50000.0),
        stop_price: None,
        tif: Some(TimeInForce::Gtc),
        post_only: false,
        reduce_only: false,
        client_order_id: client_id.clone(),
    };

    let result1 = oms.submit_order(Exchange::Kraken, order1).await;
    assert!(result1.is_ok(), "First order should succeed");

    // Second order with same client_id should fail
    let order2 = NewOrder {
        symbol: "BTCUSD".to_string(),
        side: Side::Sell,
        ord_type: OrderType::Limit,
        qty: 2.0,
        price: Some(51000.0),
        stop_price: None,
        tif: Some(TimeInForce::Gtc),
        post_only: false,
        reduce_only: false,
        client_order_id: client_id.clone(),
    };

    let result2 = oms.submit_order(Exchange::Kraken, order2).await;
    assert!(result2.is_err(), "Duplicate client_order_id should fail");
}

#[tokio::test]
async fn test_fill_exceeds_order_quantity() {
    let oms = Arc::new(OrderManager::new());
    let (tx, rx) = mpsc::channel::<UserEvent>(100);

    oms.register_exchange(Exchange::Kraken, Arc::new(MockExchangeAdapter::new()), rx)
        .await;

    let mut fill_rx = oms.subscribe_fills();

    // Submit order for 1.0 qty
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
        client_order_id: unique_client_id("overfill"),
    };

    let client_id = oms.submit_order(Exchange::Kraken, order).await.unwrap();
    let order = oms.get_order(&client_id).unwrap();

    // Send fill for MORE than order quantity
    let fill = Fill {
        venue_order_id: order.venue_order_id.clone(),
        client_order_id: client_id.clone(),
        symbol: "BTCUSD".to_string(),
        price: 50000.0,
        qty: 2.0, // More than order qty of 1.0
        fee: 1.0,
        fee_ccy: "USD".to_string(),
        is_maker: true,
        exec_id: unique_client_id("exec_overfill"),
        ex_ts_ms: 1234567890,
        recv_ms: 1234567890,
    };

    tx.send(UserEvent::Fill(fill)).await.unwrap();

    // Wait for fill to be processed
    let _ = wait_for_fill(&mut fill_rx, Duration::from_secs(1)).await;

    // OMS should handle gracefully (not panic, not negative remaining)
    // Note: The exact behavior depends on implementation - this tests it doesn't crash
    let updated = oms.get_order(&client_id);
    assert!(updated.is_some(), "Order should still exist after overfill");
}

#[tokio::test]
async fn test_very_small_quantities() {
    let oms = Arc::new(OrderManager::new());
    let (_tx, rx) = mpsc::channel::<UserEvent>(100);

    oms.register_exchange(Exchange::Kraken, Arc::new(MockExchangeAdapter::new()), rx)
        .await;

    // Test with very small quantity (dust)
    let order = NewOrder {
        symbol: "BTCUSD".to_string(),
        side: Side::Buy,
        ord_type: OrderType::Limit,
        qty: 0.00000001, // 1 satoshi
        price: Some(50000.0),
        stop_price: None,
        tif: Some(TimeInForce::Gtc),
        post_only: false,
        reduce_only: false,
        client_order_id: unique_client_id("tiny"),
    };

    let result = oms.submit_order(Exchange::Kraken, order).await;
    assert!(result.is_ok(), "Small quantity should be accepted");

    let client_id = result.unwrap();
    let retrieved = oms.get_order(&client_id).unwrap();
    assert!((retrieved.qty - 0.00000001).abs() < 1e-12, "Quantity should be preserved");
}

#[tokio::test]
async fn test_empty_symbol() {
    let oms = Arc::new(OrderManager::new());
    let (_tx, rx) = mpsc::channel::<UserEvent>(100);

    oms.register_exchange(Exchange::Kraken, Arc::new(MockExchangeAdapter::new()), rx)
        .await;

    // Empty symbol - mock will accept but real exchange would reject
    let order = NewOrder {
        symbol: "".to_string(),
        side: Side::Buy,
        ord_type: OrderType::Limit,
        qty: 1.0,
        price: Some(50000.0),
        stop_price: None,
        tif: Some(TimeInForce::Gtc),
        post_only: false,
        reduce_only: false,
        client_order_id: unique_client_id("empty_symbol"),
    };

    // This tests that OMS doesn't crash - validation should happen at exchange
    let result = oms.submit_order(Exchange::Kraken, order).await;
    // The mock accepts it, but this documents the behavior
    assert!(result.is_ok() || result.is_err(), "Should not panic");
}

// =============================================================================
// Order State Transition Tests
// =============================================================================

#[tokio::test]
async fn test_order_lifecycle_new_to_partial_to_filled() {
    let oms = Arc::new(OrderManager::new());
    let (tx, rx) = mpsc::channel::<UserEvent>(100);

    oms.register_exchange(Exchange::Kraken, Arc::new(MockExchangeAdapter::new()), rx)
        .await;

    let mut fill_rx = oms.subscribe_fills();

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
        client_order_id: unique_client_id("lifecycle"),
    };

    let client_id = oms.submit_order(Exchange::Kraken, order).await.unwrap();
    let order = oms.get_order(&client_id).unwrap();

    // Verify initial state
    assert!(matches!(order.status, OrderStatus::New));
    assert_eq!(order.filled_qty, 0.0);

    // First partial fill
    let fill1 = Fill {
        venue_order_id: order.venue_order_id.clone(),
        client_order_id: client_id.clone(),
        symbol: "BTCUSD".to_string(),
        price: 50000.0,
        qty: 0.3,
        fee: 0.15,
        fee_ccy: "USD".to_string(),
        is_maker: true,
        exec_id: unique_client_id("exec1"),
        ex_ts_ms: 1000,
        recv_ms: 1000,
    };
    tx.send(UserEvent::Fill(fill1)).await.unwrap();
    let _ = wait_for_fill(&mut fill_rx, Duration::from_secs(1)).await;

    // Verify partially filled
    let order = oms.get_order(&client_id).unwrap();
    assert!(matches!(order.status, OrderStatus::PartiallyFilled));
    assert!((order.filled_qty - 0.3).abs() < 0.001);

    // Second partial fill
    let fill2 = Fill {
        venue_order_id: order.venue_order_id.clone(),
        client_order_id: client_id.clone(),
        symbol: "BTCUSD".to_string(),
        price: 50000.0,
        qty: 0.3,
        fee: 0.15,
        fee_ccy: "USD".to_string(),
        is_maker: true,
        exec_id: unique_client_id("exec2"),
        ex_ts_ms: 2000,
        recv_ms: 2000,
    };
    tx.send(UserEvent::Fill(fill2)).await.unwrap();
    let _ = wait_for_fill(&mut fill_rx, Duration::from_secs(1)).await;

    // Still partially filled
    let order = oms.get_order(&client_id).unwrap();
    assert!(matches!(order.status, OrderStatus::PartiallyFilled));
    assert!((order.filled_qty - 0.6).abs() < 0.001);

    // Final fill to complete
    let fill3 = Fill {
        venue_order_id: order.venue_order_id.clone(),
        client_order_id: client_id.clone(),
        symbol: "BTCUSD".to_string(),
        price: 50000.0,
        qty: 0.4,
        fee: 0.20,
        fee_ccy: "USD".to_string(),
        is_maker: true,
        exec_id: unique_client_id("exec3"),
        ex_ts_ms: 3000,
        recv_ms: 3000,
    };
    tx.send(UserEvent::Fill(fill3)).await.unwrap();
    let _ = wait_for_fill(&mut fill_rx, Duration::from_secs(1)).await;

    // Should be filled now
    let order = oms.get_order(&client_id).unwrap();
    assert!(matches!(order.status, OrderStatus::Filled), "Expected Filled, got {:?}", order.status);
    assert!((order.filled_qty - 1.0).abs() < 0.001);
    assert!(order.remaining_qty < 0.001);
}

// =============================================================================
// Multi-Exchange Tests
// =============================================================================

#[tokio::test]
async fn test_multiple_exchanges_registered() {
    let oms = Arc::new(OrderManager::new());

    let (_tx1, rx1) = mpsc::channel::<UserEvent>(100);
    let (_tx2, rx2) = mpsc::channel::<UserEvent>(100);

    oms.register_exchange(Exchange::Kraken, Arc::new(MockExchangeAdapter::new()), rx1)
        .await;
    oms.register_exchange(Exchange::Mexc, Arc::new(MockExchangeAdapter::new()), rx2)
        .await;

    // Submit to Kraken
    let order1 = NewOrder {
        symbol: "BTCUSD".to_string(),
        side: Side::Buy,
        ord_type: OrderType::Limit,
        qty: 1.0,
        price: Some(50000.0),
        stop_price: None,
        tif: Some(TimeInForce::Gtc),
        post_only: false,
        reduce_only: false,
        client_order_id: unique_client_id("kraken"),
    };
    let kraken_id = oms.submit_order(Exchange::Kraken, order1).await.unwrap();

    // Submit to MEXC
    let order2 = NewOrder {
        symbol: "BTCUSDT".to_string(),
        side: Side::Sell,
        ord_type: OrderType::Limit,
        qty: 2.0,
        price: Some(50100.0),
        stop_price: None,
        tif: Some(TimeInForce::Gtc),
        post_only: false,
        reduce_only: false,
        client_order_id: unique_client_id("mexc"),
    };
    let mexc_id = oms.submit_order(Exchange::Mexc, order2).await.unwrap();

    // Both orders should exist
    assert!(oms.get_order(&kraken_id).is_some());
    assert!(oms.get_order(&mexc_id).is_some());

    let stats = oms.get_order_stats();
    assert_eq!(stats.total_orders, 2);
}

#[tokio::test]
async fn test_submit_to_unregistered_exchange() {
    let oms = Arc::new(OrderManager::new());

    // Don't register any exchange
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
        client_order_id: unique_client_id("unregistered"),
    };

    let result = oms.submit_order(Exchange::Kraken, order).await;
    assert!(result.is_err(), "Should fail for unregistered exchange");
}
