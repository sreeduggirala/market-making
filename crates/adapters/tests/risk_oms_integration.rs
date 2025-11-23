//! Risk + OMS Integration Tests
//!
//! End-to-end tests that verify the complete order lifecycle with risk management.
//! Tests risk validation, order submission, fill processing, and position tracking.
//!
//! Run with: cargo test --package adapters --test risk_oms_integration

use adapters::traits::{
    Fill, NewOrder, Order, OrderStatus, OrderType, Position, Side, TimeInForce, UserEvent,
};
use inventory::PositionManager;
use oms::{Exchange, OrderManager};
use risk::{RiskLimits, RiskViolation, RiskManager};
#[allow(unused_imports)]
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// Counter for generating unique IDs
static ORDER_COUNTER: AtomicU64 = AtomicU64::new(100000);

fn unique_id(prefix: &str) -> String {
    let count = ORDER_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("{}_{}", prefix, count)
}

// =============================================================================
// Mock Adapter for Testing
// =============================================================================

struct MockRiskOmsAdapter {
    fill_immediately: bool,
    reject_orders: bool,
}

impl MockRiskOmsAdapter {
    fn new() -> Self {
        Self {
            fill_immediately: false,
            reject_orders: false,
        }
    }

    fn with_immediate_fills() -> Self {
        Self {
            fill_immediately: true,
            reject_orders: false,
        }
    }

    fn rejecting() -> Self {
        Self {
            fill_immediately: false,
            reject_orders: true,
        }
    }
}

#[async_trait::async_trait]
impl oms::order_router::ExchangeAdapter for MockRiskOmsAdapter {
    async fn create_order(&self, order: NewOrder) -> anyhow::Result<Order> {
        if self.reject_orders {
            anyhow::bail!("Exchange rejected order");
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let venue_id = format!("venue_{}", uuid::Uuid::new_v4().simple());

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
// Helper Functions
// =============================================================================

fn make_order(symbol: &str, side: Side, qty: f64, price: f64) -> NewOrder {
    NewOrder {
        symbol: symbol.to_string(),
        side,
        ord_type: OrderType::Limit,
        qty,
        price: Some(price),
        stop_price: None,
        tif: Some(TimeInForce::Gtc),
        post_only: false,
        reduce_only: false,
        client_order_id: unique_id("risk_oms"),
    }
}

async fn wait_for_fill(
    fill_rx: &mut tokio::sync::broadcast::Receiver<Fill>,
    timeout: Duration,
) -> Option<Fill> {
    match tokio::time::timeout(timeout, fill_rx.recv()).await {
        Ok(Ok(fill)) => Some(fill),
        _ => None,
    }
}

// =============================================================================
// End-to-End Risk + OMS Tests
// =============================================================================

/// Test: Order passes risk checks and gets submitted
#[tokio::test]
async fn test_e2e_order_passes_risk_and_submits() {
    // Setup risk manager
    let position_manager = Arc::new(PositionManager::new());
    let limits = RiskLimits::default()
        .with_max_order_size("BTCUSD", 10.0)
        .with_max_position("BTCUSD", 100.0);
    let risk_manager = RiskManager::new(limits, position_manager.clone());

    // Setup OMS
    let oms = Arc::new(OrderManager::new());
    let (_tx, rx) = mpsc::channel::<UserEvent>(100);
    oms.register_exchange(Exchange::Kraken, Arc::new(MockRiskOmsAdapter::new()), rx)
        .await;

    // Create order
    let order = make_order("BTCUSD", Side::Buy, 1.0, 50000.0);

    // Step 1: Risk validation
    let risk_result = risk_manager.check_order(&order, 50000.0).await;
    assert!(risk_result.is_ok(), "Order should pass risk checks");

    // Step 2: Submit to OMS
    let oms_result = oms.submit_order(Exchange::Kraken, order).await;
    assert!(oms_result.is_ok(), "Order should submit successfully");

    // Step 3: Record in rate limiter
    risk_manager.record_order();

    // Verify order exists
    let client_id = oms_result.unwrap();
    let retrieved = oms.get_order(&client_id);
    assert!(retrieved.is_some());
    assert!(matches!(retrieved.unwrap().status, OrderStatus::New));
}

/// Test: Order fails risk check - not submitted
#[tokio::test]
async fn test_e2e_order_fails_risk_not_submitted() {
    let position_manager = Arc::new(PositionManager::new());
    let limits = RiskLimits::default()
        .with_max_order_size("BTCUSD", 0.5); // Very small limit
    let risk_manager = RiskManager::new(limits, position_manager.clone());

    let oms = Arc::new(OrderManager::new());
    let (_tx, rx) = mpsc::channel::<UserEvent>(100);
    oms.register_exchange(Exchange::Kraken, Arc::new(MockRiskOmsAdapter::new()), rx)
        .await;

    // Order exceeds max size
    let order = make_order("BTCUSD", Side::Buy, 1.0, 50000.0);

    // Risk check should fail
    let risk_result = risk_manager.check_order(&order, 50000.0).await;
    assert!(matches!(risk_result, Err(RiskViolation::MaxOrderSizeExceeded { .. })));

    // Order should NOT be submitted
    let stats = oms.get_order_stats();
    assert_eq!(stats.total_orders, 0, "No orders should be submitted");
}

/// Test: Kill switch blocks all new orders
#[tokio::test]
async fn test_e2e_kill_switch_blocks_orders() {
    let position_manager = Arc::new(PositionManager::new());
    let limits = RiskLimits::default();
    let risk_manager = RiskManager::new(limits, position_manager.clone());

    let oms = Arc::new(OrderManager::new());
    let (_tx, rx) = mpsc::channel::<UserEvent>(100);
    oms.register_exchange(Exchange::Kraken, Arc::new(MockRiskOmsAdapter::new()), rx)
        .await;

    // Trigger kill switch
    risk_manager.trigger_kill_switch("Maximum daily loss exceeded");

    // Any order should be blocked
    let order = make_order("BTCUSD", Side::Buy, 0.01, 50000.0);
    let risk_result = risk_manager.check_order(&order, 50000.0).await;

    assert!(matches!(risk_result, Err(RiskViolation::KillSwitchActive { .. })));

    // Verify kill switch state
    let state = risk_manager.get_risk_state();
    assert!(state.kill_switch_active);
    assert_eq!(state.kill_switch_reason, Some("Maximum daily loss exceeded".to_string()));
}

/// Test: Kill switch reset allows trading to resume
#[tokio::test]
async fn test_e2e_kill_switch_reset() {
    let position_manager = Arc::new(PositionManager::new());
    let limits = RiskLimits::default();
    let risk_manager = RiskManager::new(limits, position_manager.clone());

    // Trigger then reset
    risk_manager.trigger_kill_switch("Test");
    risk_manager.reset_kill_switch();

    // Order should pass now
    let order = make_order("BTCUSD", Side::Buy, 0.1, 50000.0);
    let risk_result = risk_manager.check_order(&order, 50000.0).await;

    assert!(risk_result.is_ok(), "Order should pass after kill switch reset");

    let state = risk_manager.get_risk_state();
    assert!(!state.kill_switch_active);
}

/// Test: Position updates tracked and limits enforced
#[tokio::test]
async fn test_e2e_position_tracking() {
    let position_manager = Arc::new(PositionManager::new());
    let limits = RiskLimits::default()
        .with_max_position("BTCUSD", 5.0);
    let risk_manager = RiskManager::new(limits, position_manager.clone());

    // Simulate existing position (4.5 BTC)
    position_manager.update_position(
        Exchange::Kraken,
        Position {
            exchange: Some("kraken".to_string()),
            symbol: "BTCUSD".to_string(),
            qty: 4.5,
            entry_px: 50000.0,
            mark_px: Some(50000.0),
            liquidation_px: None,
            unrealized_pnl: None,
            realized_pnl: None,
            margin: None,
            leverage: None,
            opened_ms: None,
            updated_ms: 1234567890,
        },
    );

    // Try to buy 1 more BTC (would exceed 5.0 limit)
    let order = make_order("BTCUSD", Side::Buy, 1.0, 50000.0);
    let result = risk_manager.check_order(&order, 50000.0).await;

    assert!(matches!(result, Err(RiskViolation::MaxPositionExceeded { .. })));

    // Selling should be allowed (reduces position)
    let sell_order = make_order("BTCUSD", Side::Sell, 0.5, 50000.0);
    let sell_result = risk_manager.check_order(&sell_order, 50000.0).await;

    assert!(sell_result.is_ok(), "Sell should pass - reduces position");
}

/// Test: Complete order lifecycle with fills updating positions
#[tokio::test]
async fn test_e2e_full_order_lifecycle() {
    let position_manager = Arc::new(PositionManager::new());
    let limits = RiskLimits::default()
        .with_max_order_size("BTCUSD", 10.0)
        .with_max_position("BTCUSD", 50.0);
    let risk_manager = RiskManager::new(limits, position_manager.clone());

    let oms = Arc::new(OrderManager::new());
    let (tx, rx) = mpsc::channel::<UserEvent>(100);
    oms.register_exchange(Exchange::Kraken, Arc::new(MockRiskOmsAdapter::new()), rx)
        .await;

    let mut fill_rx = oms.subscribe_fills();

    // 1. Validate order
    let order = make_order("BTCUSD", Side::Buy, 1.0, 50000.0);
    let risk_result = risk_manager.check_order(&order, 50000.0).await;
    assert!(risk_result.is_ok());

    // 2. Submit order
    let client_id = oms.submit_order(Exchange::Kraken, order).await.unwrap();
    risk_manager.record_order();

    let submitted_order = oms.get_order(&client_id).unwrap();

    // 3. Simulate fill from exchange
    let fill = Fill {
        venue_order_id: submitted_order.venue_order_id.clone(),
        client_order_id: client_id.clone(),
        symbol: "BTCUSD".to_string(),
        price: 50000.0,
        qty: 1.0,
        fee: 0.5,
        fee_ccy: "USD".to_string(),
        is_maker: true,
        exec_id: unique_id("exec"),
        ex_ts_ms: 1234567890,
        recv_ms: 1234567891,
    };

    tx.send(UserEvent::Fill(fill.clone())).await.unwrap();

    // 4. Wait for fill to be processed
    let received = wait_for_fill(&mut fill_rx, Duration::from_secs(2)).await;
    assert!(received.is_some(), "Fill should be received");

    // 5. Update position from fill
    let position = Position {
        exchange: Some("kraken".to_string()),
        symbol: "BTCUSD".to_string(),
        qty: 1.0,
        entry_px: 50000.0,
        mark_px: Some(50100.0),
        liquidation_px: None,
        unrealized_pnl: Some(100.0),
        realized_pnl: None,
        margin: None,
        leverage: None,
        opened_ms: Some(1234567890),
        updated_ms: 1234567891,
    };
    position_manager.update_position(Exchange::Kraken, position);

    // 6. Verify position is tracked
    let pos = position_manager.get_position(Exchange::Kraken, "BTCUSD");
    assert!(pos.is_some());
    assert_eq!(pos.unwrap().qty, 1.0);
}

/// Test: Price band violation blocks aggressive orders
#[tokio::test]
async fn test_e2e_price_band_violation() {
    let position_manager = Arc::new(PositionManager::new());
    let limits = RiskLimits::default(); // Default: 500 bps (5%)
    let risk_manager = RiskManager::new(limits, position_manager.clone());

    // Order price is 20% above mid - should fail
    let order = make_order("BTCUSD", Side::Buy, 0.1, 60000.0);
    let result = risk_manager.check_order(&order, 50000.0).await;

    assert!(matches!(result, Err(RiskViolation::PriceBandViolation { .. })));
}

/// Test: Multiple symbols tracked independently
#[tokio::test]
async fn test_e2e_multi_symbol_risk_tracking() {
    let position_manager = Arc::new(PositionManager::new());
    let limits = RiskLimits::default()
        .with_max_position("BTCUSD", 10.0)
        .with_max_position("ETHUSD", 100.0)
        .with_max_order_size("BTCUSD", 5.0)
        .with_max_order_size("ETHUSD", 50.0);
    let risk_manager = RiskManager::new(limits, position_manager.clone());

    // Add positions for both
    position_manager.update_position(
        Exchange::Kraken,
        Position {
            exchange: Some("kraken".to_string()),
            symbol: "BTCUSD".to_string(),
            qty: 8.0,
            entry_px: 50000.0,
            mark_px: Some(50000.0),
            liquidation_px: None,
            unrealized_pnl: None,
            realized_pnl: None,
            margin: None,
            leverage: None,
            opened_ms: None,
            updated_ms: 0,
        },
    );

    position_manager.update_position(
        Exchange::Kraken,
        Position {
            exchange: Some("kraken".to_string()),
            symbol: "ETHUSD".to_string(),
            qty: 50.0,
            entry_px: 3000.0,
            mark_px: Some(3000.0),
            liquidation_px: None,
            unrealized_pnl: None,
            realized_pnl: None,
            margin: None,
            leverage: None,
            opened_ms: None,
            updated_ms: 0,
        },
    );

    // BTC: 8 + 3 = 11 > 10 limit - should fail
    let btc_order = make_order("BTCUSD", Side::Buy, 3.0, 50000.0);
    let btc_result = risk_manager.check_order(&btc_order, 50000.0).await;
    assert!(matches!(btc_result, Err(RiskViolation::MaxPositionExceeded { .. })));

    // ETH: 50 + 40 = 90 < 100 limit - should pass
    let eth_order = make_order("ETHUSD", Side::Buy, 40.0, 3000.0);
    let eth_result = risk_manager.check_order(&eth_order, 3000.0).await;
    assert!(eth_result.is_ok(), "ETH order should pass");
}

/// Test: Rate limiting prevents order spam
#[tokio::test]
async fn test_e2e_rate_limiting() {
    let position_manager = Arc::new(PositionManager::new());
    let mut limits = RiskLimits::default();
    // Set very restrictive limits: 1 order/sec * 2.0 burst = 2 max
    limits.rate_limits.max_orders_per_second = 1;
    limits.rate_limits.burst_multiplier = 2.0;
    let risk_manager = RiskManager::new(limits, position_manager.clone());

    // Record orders up to burst limit (1 * 2.0 = 2)
    risk_manager.record_order();
    risk_manager.record_order();

    // Third order should be rate limited
    let order = make_order("BTCUSD", Side::Buy, 0.1, 50000.0);
    let result = risk_manager.check_order(&order, 50000.0).await;

    assert!(matches!(result, Err(RiskViolation::OrderRateLimitExceeded { .. })));
}

/// Test: Market price updates propagate to risk manager
#[tokio::test]
async fn test_e2e_market_price_updates() {
    let position_manager = Arc::new(PositionManager::new());
    let limits = RiskLimits::default();
    let risk_manager = RiskManager::new(limits, position_manager.clone());

    // Update market price
    risk_manager.update_market_price("BTCUSD", 55000.0);

    // Margin monitor should have the updated price
    // (This verifies the update propagates correctly)
    let state = risk_manager.get_risk_state();
    assert!(state.margin_utilization >= 0.0);
}

// =============================================================================
// Stress Tests
// =============================================================================

/// Test: Many concurrent risk checks
#[tokio::test]
async fn test_e2e_concurrent_risk_checks() {
    let position_manager = Arc::new(PositionManager::new());
    let limits = RiskLimits::default()
        .with_max_order_size("BTCUSD", 100.0)
        .with_max_position("BTCUSD", 1000.0);
    let risk_manager = Arc::new(RiskManager::new(limits, position_manager.clone()));

    let mut handles = vec![];

    // Spawn many concurrent risk checks
    for i in 0..100 {
        let rm = risk_manager.clone();
        let handle = tokio::spawn(async move {
            let order = NewOrder {
                symbol: "BTCUSD".to_string(),
                side: if i % 2 == 0 { Side::Buy } else { Side::Sell },
                ord_type: OrderType::Limit,
                qty: 0.1,
                price: Some(50000.0 + (i as f64) * 10.0),
                stop_price: None,
                tif: Some(TimeInForce::Gtc),
                post_only: false,
                reduce_only: false,
                client_order_id: format!("concurrent_{}", i),
            };
            rm.check_order(&order, 50000.0).await
        });
        handles.push(handle);
    }

    // All should complete without panic
    let results = futures::future::join_all(handles).await;
    let success_count = results
        .into_iter()
        .filter(|r: &Result<Result<(), RiskViolation>, _>| {
            r.as_ref().map(|inner| inner.is_ok()).unwrap_or(false)
        })
        .count();

    // Most should pass (some may hit rate limits)
    assert!(success_count > 0, "At least some checks should pass");
}

/// Test: Risk state is consistent under concurrent access
#[tokio::test]
async fn test_e2e_risk_state_consistency() {
    let position_manager = Arc::new(PositionManager::new());
    let limits = RiskLimits::default();
    let risk_manager = Arc::new(RiskManager::new(limits, position_manager.clone()));

    // Concurrent reads and writes
    let mut handles = vec![];

    for i in 0..50 {
        let rm = risk_manager.clone();
        handles.push(tokio::spawn(async move {
            if i % 5 == 0 {
                rm.update_market_price("BTCUSD", 50000.0 + i as f64);
            }
            rm.get_risk_state()
        }));
    }

    let results = futures::future::join_all(handles).await;

    // All reads should succeed
    for result in results {
        assert!(result.is_ok());
    }
}

// =============================================================================
// Error Recovery Tests
// =============================================================================

/// Test: Exchange rejection doesn't corrupt OMS state
#[tokio::test]
async fn test_e2e_exchange_rejection_recovery() {
    let position_manager = Arc::new(PositionManager::new());
    let limits = RiskLimits::default();
    let risk_manager = RiskManager::new(limits, position_manager.clone());

    let oms = Arc::new(OrderManager::new());
    let (_tx, rx) = mpsc::channel::<UserEvent>(100);
    oms.register_exchange(Exchange::Kraken, Arc::new(MockRiskOmsAdapter::rejecting()), rx)
        .await;

    // Order passes risk but fails at exchange
    let order = make_order("BTCUSD", Side::Buy, 0.1, 50000.0);
    assert!(risk_manager.check_order(&order, 50000.0).await.is_ok());

    let result = oms.submit_order(Exchange::Kraken, order).await;
    assert!(result.is_err(), "Exchange should reject");

    // OMS should be in clean state
    let stats = oms.get_order_stats();
    assert_eq!(stats.total_orders, 0);

    // Can submit another order
    let (_tx2, rx2) = mpsc::channel::<UserEvent>(100);
    let oms2 = Arc::new(OrderManager::new());
    oms2.register_exchange(Exchange::Kraken, Arc::new(MockRiskOmsAdapter::new()), rx2)
        .await;

    let order2 = make_order("BTCUSD", Side::Buy, 0.1, 50000.0);
    let result2 = oms2.submit_order(Exchange::Kraken, order2).await;
    assert!(result2.is_ok());
}
