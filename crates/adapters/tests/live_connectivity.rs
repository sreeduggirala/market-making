//! Live connectivity tests for exchange adapters
//!
//! These tests verify that we can connect to real exchanges and receive market data.
//! They require network access but do NOT require API keys for public data.
//!
//! Run with: cargo test --package adapters --test live_connectivity
//!
//! Set SKIP_LIVE_TESTS=1 to skip these tests in CI environments.

use adapters::kraken::spot::KrakenSpotAdapter;
use adapters::mexc::spot::MexcSpotAdapter;
use adapters::traits::{SpotRest, SpotWs};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

fn should_skip_live_tests() -> bool {
    std::env::var("SKIP_LIVE_TESTS").is_ok()
}

// =============================================================================
// Kraken Spot Tests
// =============================================================================

#[tokio::test]
async fn test_kraken_spot_get_ticker() {
    if should_skip_live_tests() {
        println!("Skipping live test (SKIP_LIVE_TESTS set)");
        return;
    }

    let adapter = KrakenSpotAdapter::new(String::new(), String::new());

    let result = timeout(Duration::from_secs(10), adapter.get_ticker("XXBTZUSD")).await;

    assert!(result.is_ok(), "Request timed out");
    let ticker = result.unwrap();
    assert!(ticker.is_ok(), "Failed to get ticker: {:?}", ticker.err());

    let ticker = ticker.unwrap();
    assert_eq!(ticker.symbol, "XXBTZUSD");
    assert!(ticker.last_price > 0.0, "Price should be positive");
    assert!(ticker.bid_price > 0.0, "Bid should be positive");
    assert!(ticker.ask_price > 0.0, "Ask should be positive");

    println!("Kraken BTC/USD: ${:.2}", ticker.last_price);
}

#[tokio::test]
async fn test_kraken_spot_get_market_info() {
    if should_skip_live_tests() {
        println!("Skipping live test (SKIP_LIVE_TESTS set)");
        return;
    }

    let adapter = KrakenSpotAdapter::new(String::new(), String::new());

    let result = timeout(Duration::from_secs(10), adapter.get_market_info("XXBTZUSD")).await;

    assert!(result.is_ok(), "Request timed out");
    let market = result.unwrap();
    assert!(market.is_ok(), "Failed to get market info: {:?}", market.err());

    let market = market.unwrap();
    assert_eq!(market.symbol, "XXBTZUSD");
    assert!(market.min_qty > 0.0, "Min qty should be positive");
    assert!(market.tick_size > 0.0, "Tick size should be positive");
    assert!(market.is_spot, "Should be spot market");

    println!(
        "Kraken XXBTZUSD: min_qty={}, tick_size={}",
        market.min_qty, market.tick_size
    );
}

#[tokio::test]
async fn test_kraken_spot_websocket_trades() {
    if should_skip_live_tests() {
        println!("Skipping live test (SKIP_LIVE_TESTS set)");
        return;
    }

    let adapter = Arc::new(KrakenSpotAdapter::new(String::new(), String::new()));

    // Subscribe to trades
    let subscribe_result = timeout(
        Duration::from_secs(15),
        adapter.subscribe_trades(&["XBT/USD"]),
    )
    .await;

    assert!(subscribe_result.is_ok(), "Subscribe timed out");
    let rx = subscribe_result.unwrap();
    assert!(rx.is_ok(), "Failed to subscribe: {:?}", rx.err());
    let mut rx = rx.unwrap();

    // Wait for at least one trade event (BTC usually has trades within 30s)
    let event_result = timeout(Duration::from_secs(60), rx.recv()).await;

    assert!(event_result.is_ok(), "No trade received within timeout");
    let event = event_result.unwrap();
    assert!(event.is_some(), "Channel closed unexpectedly");

    let trade = event.unwrap();
    println!("Received trade: {:?}", trade);
    assert!(trade.px > 0.0, "Trade price should be positive");

    // Cleanup
    adapter.shutdown().await;
}

// =============================================================================
// MEXC Spot Tests
// =============================================================================

#[tokio::test]
async fn test_mexc_spot_get_ticker() {
    if should_skip_live_tests() {
        println!("Skipping live test (SKIP_LIVE_TESTS set)");
        return;
    }

    let adapter = MexcSpotAdapter::new(String::new(), String::new());

    let result = timeout(Duration::from_secs(10), adapter.get_ticker("BTCUSDT")).await;

    assert!(result.is_ok(), "Request timed out");
    let ticker = result.unwrap();
    assert!(ticker.is_ok(), "Failed to get ticker: {:?}", ticker.err());

    let ticker = ticker.unwrap();
    assert_eq!(ticker.symbol, "BTCUSDT");
    assert!(ticker.last_price > 0.0, "Price should be positive");
    assert!(ticker.bid_price > 0.0, "Bid should be positive");
    assert!(ticker.ask_price > 0.0, "Ask should be positive");

    println!("MEXC BTC/USDT: ${:.2}", ticker.last_price);
}

#[tokio::test]
async fn test_mexc_spot_get_market_info() {
    if should_skip_live_tests() {
        println!("Skipping live test (SKIP_LIVE_TESTS set)");
        return;
    }

    let adapter = MexcSpotAdapter::new(String::new(), String::new());

    let result = timeout(Duration::from_secs(10), adapter.get_market_info("BTCUSDT")).await;

    assert!(result.is_ok(), "Request timed out");
    let market = result.unwrap();
    assert!(market.is_ok(), "Failed to get market info: {:?}", market.err());

    let market = market.unwrap();
    assert_eq!(market.symbol, "BTCUSDT");
    assert!(market.min_qty > 0.0, "Min qty should be positive");
    assert!(market.tick_size > 0.0, "Tick size should be positive");
    assert!(market.is_spot, "Should be spot market");

    println!(
        "MEXC BTCUSDT: min_qty={}, tick_size={}",
        market.min_qty, market.tick_size
    );
}

#[tokio::test]
async fn test_mexc_spot_websocket_trades() {
    if should_skip_live_tests() {
        println!("Skipping live test (SKIP_LIVE_TESTS set)");
        return;
    }

    let adapter = Arc::new(MexcSpotAdapter::new(String::new(), String::new()));

    // Subscribe to trades
    let subscribe_result = timeout(
        Duration::from_secs(15),
        adapter.subscribe_trades(&["BTCUSDT"]),
    )
    .await;

    assert!(subscribe_result.is_ok(), "Subscribe timed out");
    let rx = subscribe_result.unwrap();
    assert!(rx.is_ok(), "Failed to subscribe: {:?}", rx.err());
    let mut rx = rx.unwrap();

    // Wait for at least one trade event
    let event_result = timeout(Duration::from_secs(60), rx.recv()).await;

    assert!(event_result.is_ok(), "No trade received within timeout");
    let event = event_result.unwrap();
    assert!(event.is_some(), "Channel closed unexpectedly");

    let trade = event.unwrap();
    println!("Received trade: {:?}", trade);
    assert!(trade.px > 0.0, "Trade price should be positive");

    // Cleanup
    adapter.shutdown().await;
}

// =============================================================================
// Cross-Exchange Price Comparison
// =============================================================================

#[tokio::test]
async fn test_cross_exchange_btc_price_sanity() {
    if should_skip_live_tests() {
        println!("Skipping live test (SKIP_LIVE_TESTS set)");
        return;
    }

    let kraken = KrakenSpotAdapter::new(String::new(), String::new());
    let mexc = MexcSpotAdapter::new(String::new(), String::new());

    // Get prices from both exchanges
    let (kraken_result, mexc_result) = tokio::join!(
        timeout(Duration::from_secs(10), kraken.get_ticker("XXBTZUSD")),
        timeout(Duration::from_secs(10), mexc.get_ticker("BTCUSDT")),
    );

    let kraken_price = kraken_result
        .expect("Kraken timeout")
        .expect("Kraken error")
        .last_price;

    let mexc_price = mexc_result
        .expect("MEXC timeout")
        .expect("MEXC error")
        .last_price;

    // Prices should be within 1% of each other (reasonable for liquid BTC markets)
    let diff_pct = ((kraken_price - mexc_price) / kraken_price * 100.0).abs();

    println!(
        "Kraken: ${:.2}, MEXC: ${:.2}, Diff: {:.3}%",
        kraken_price, mexc_price, diff_pct
    );

    assert!(
        diff_pct < 1.0,
        "Price difference too large: {:.3}% (Kraken: ${}, MEXC: ${})",
        diff_pct,
        kraken_price,
        mexc_price
    );
}
