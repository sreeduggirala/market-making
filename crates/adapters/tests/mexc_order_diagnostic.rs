//! MEXC Order Placement Diagnostic
//!
//! This diagnostic tool helps identify why orders are failing on MEXC.
//! It provides detailed error information and checks account/symbol configuration.
//!
//! Run with: cargo test --package adapters --test mexc_order_diagnostic -- --nocapture --ignored

use adapters::mexc::MexcSpotAdapter;
use adapters::traits::{SpotRest, OrderType, Side, NewOrder};

/// Helper to get MEXC credentials from environment
fn get_mexc_credentials() -> Option<(String, String)> {
    let api_key = std::env::var("MEXC_API_KEY").ok()?;
    let api_secret = std::env::var("MEXC_API_SECRET").ok()?;

    if api_key.contains("your_mexc") || api_secret.contains("your_mexc") {
        return None;
    }

    Some((api_key, api_secret))
}

#[tokio::test]
#[ignore]
async fn diagnose_order_placement_issues() {
    println!("\n🔍 MEXC ORDER PLACEMENT DIAGNOSTIC");
    println!("{}", "=".repeat(70));

    let Some((api_key, api_secret)) = get_mexc_credentials() else {
        println!("❌ MEXC credentials not configured");
        return;
    };

    println!("\n✓ Credentials loaded");
    println!("  API Key: {}...", &api_key.chars().take(10).collect::<String>());

    let adapter = MexcSpotAdapter::new(api_key, api_secret);

    // Step 1: Check account info
    println!("\n📊 Step 1: Checking Account Information");
    println!("{}", "-".repeat(70));

    match adapter.get_account_info().await {
        Ok(info) => {
            println!("✓ Account accessible");
            println!("  Can trade: {}", info.can_trade);
            println!("  Can withdraw: {}", info.can_withdraw);
            println!("  Can deposit: {}", info.can_deposit);
            println!("  Number of balances: {}", info.balances.len());

            // Check for USDC and ETH balances
            for balance in &info.balances {
                if balance.asset == "USDC" || balance.asset == "USDT" || balance.asset == "ETH" {
                    println!("  {}: free={}, locked={}",
                        balance.asset, balance.free, balance.locked);
                }
            }
        }
        Err(e) => {
            println!("❌ Failed to get account info: {}", e);
            println!("   This suggests API key issues or network problems");
            return;
        }
    }

    // Step 2: Get ticker/market data
    println!("\n📈 Step 2: Checking Market Data for ETHUSDC");
    println!("{}", "-".repeat(70));

    let ticker = match adapter.get_ticker("ETHUSDC").await {
        Ok(t) => {
            println!("✓ Market data accessible");
            println!("  Last price: ${:.2}", t.last_price);
            println!("  Bid price: ${:.2}", t.bid_price);
            println!("  Ask price: ${:.2}", t.ask_price);
            println!("  24h volume: {}", t.volume_24h);
            t
        }
        Err(e) => {
            println!("❌ Failed to get ticker: {}", e);
            println!("   Symbol may not exist or be delisted");
            return;
        }
    };

    // Step 3: Symbol info check
    println!("\n⚙️  Step 3: Symbol Notes");
    println!("{}", "-".repeat(70));
    println!("  Note: Testing will reveal minimum requirements");
    println!("  Common MEXC minimums:");
    println!("    - Min notional: Usually $5-10 USDC");
    println!("    - Min quantity: Varies by symbol");
    println!("  We'll test various sizes to find what works...");

    // Step 4: Try creating a test order with various sizes
    println!("\n🧪 Step 4: Testing Order Placement");
    println!("{}", "-".repeat(70));

    let test_price = ticker.last_price * 0.01; // 99% below market (won't fill)

    // Test different quantities
    let test_quantities = vec![
        ("Tiny", 0.001),
        ("Small", 0.01),
        ("Medium", 0.1),
        ("Min notional $1", 1.0 / test_price),
        ("Min notional $5", 5.0 / test_price),
        ("Min notional $10", 10.0 / test_price),
    ];

    for (label, qty) in test_quantities {
        println!("\n  Testing {} order (qty: {:.4} ETH, ~${:.2}):",
            label, qty, qty * test_price);

        let result = adapter.create_order(NewOrder {
            symbol: "ETHUSDC".to_string(),
            side: Side::Buy,
            ord_type: OrderType::Limit,
            qty,
            price: Some(test_price),
            stop_price: None,
            tif: None,
            post_only: false,
            reduce_only: false,
            client_order_id: format!("diag_{}_{}", label.replace(" ", "_"), chrono::Utc::now().timestamp_millis()),
        }).await;

        match result {
            Ok(order) => {
                println!("  ✅ SUCCESS!");
                println!("     Order ID: {}", order.venue_order_id);
                println!("     Status: {:?}", order.status);

                // Try to cancel it
                println!("     Cancelling...");
                match adapter.cancel_order("ETHUSDC", &order.venue_order_id).await {
                    Ok(_) => println!("     ✓ Cancelled"),
                    Err(e) => println!("     ⚠️  Cancel failed: {}", e),
                }

                // We found a working size, break
                println!("\n✅ Found working order size: {} ({:.4} ETH)", label, qty);
                break;
            }
            Err(e) => {
                println!("  ❌ FAILED");
                println!("     Error: {}", e);

                // Print full error chain
                println!("     Error chain:");
                for (i, cause) in e.chain().enumerate() {
                    println!("       [{}] {}", i, cause);
                }

                // Print debug representation
                println!("     Debug: {:?}", e);
            }
        }
    }

    // Step 5: Summary
    println!("\n📋 DIAGNOSTIC SUMMARY");
    println!("{}", "=".repeat(70));
    println!("\nIf all tests failed, possible issues:");
    println!("  1. Insufficient balance (need USDC in account)");
    println!("  2. Trading not enabled on API key");
    println!("  3. Symbol restrictions or delisted");
    println!("  4. Minimum notional requirements not met");
    println!("  5. Rate limiting from too many requests");
    println!("\nCheck your MEXC account settings and balances.");
}

#[tokio::test]
#[ignore]
async fn test_alternative_symbols() {
    println!("\n🔍 Testing Alternative Trading Pairs");
    println!("{}", "=".repeat(70));

    let Some((api_key, api_secret)) = get_mexc_credentials() else {
        println!("❌ MEXC credentials not configured");
        return;
    };

    let adapter = MexcSpotAdapter::new(api_key, api_secret);

    // Try common pairs
    let test_pairs = vec!["BTCUSDT", "ETHUSDT", "ETHUSDC"];

    for symbol in test_pairs {
        println!("\n📊 Testing {}:", symbol);

        match adapter.get_ticker(symbol).await {
            Ok(ticker) => {
                println!("  ✓ Ticker available");
                println!("    Price: ${:.2}", ticker.last_price);

                // Try a small order
                let test_price = ticker.last_price * 0.01;
                let test_qty = 10.0 / test_price; // ~$10 worth

                println!("  🧪 Attempting test order (~$10 worth)...");

                let result = adapter.create_order(NewOrder {
                    symbol: symbol.to_string(),
                    side: Side::Buy,
                    ord_type: OrderType::Limit,
                    qty: test_qty,
                    price: Some(test_price),
                    stop_price: None,
                    tif: None,
                    post_only: false,
                    reduce_only: false,
                    client_order_id: format!("test_{}_{}", symbol, chrono::Utc::now().timestamp_millis()),
                }).await;

                match result {
                    Ok(order) => {
                        println!("  ✅ Order created successfully!");
                        println!("     ID: {}", order.venue_order_id);

                        // Cancel it
                        let _ = adapter.cancel_order(symbol, &order.venue_order_id).await;
                        println!("     (Cancelled)");
                    }
                    Err(e) => {
                        println!("  ❌ Order failed: {}", e);
                    }
                }
            }
            Err(e) => {
                println!("  ❌ Ticker failed: {}", e);
            }
        }
    }
}
