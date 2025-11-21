//! OMS Integration Tests
//!
//! Tests for the Order Management System using mock adapters.
//! These tests verify order lifecycle, event processing, and fill handling.
//!
//! Run with: cargo test --package adapters --test oms_integration

use adapters::traits::{
    Fill, NewOrder, Order, OrderStatus, OrderType, Position, Side, TimeInForce, UserEvent,
};
use oms::{Exchange, OrderManager};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

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
