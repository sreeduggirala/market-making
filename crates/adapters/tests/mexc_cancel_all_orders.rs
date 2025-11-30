//! MEXC Cancel All Orders Utility
//!
//! Cancels all open orders on MEXC to free up locked balances.
//!
//! Run with: cargo test --package adapters --test mexc_cancel_all_orders -- --nocapture --ignored

use adapters::mexc::MexcSpotAdapter;
use adapters::traits::SpotRest;

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
async fn cancel_all_mexc_orders() {
    println!("\n🧹 MEXC - Cancel All Open Orders");
    println!("{}", "=".repeat(70));

    let Some((api_key, api_secret)) = get_mexc_credentials() else {
        println!("❌ MEXC credentials not configured");
        return;
    };

    let adapter = MexcSpotAdapter::new(api_key, api_secret);

    // Check initial balances
    println!("\n📊 Current Account Status:");
    println!("{}", "-".repeat(70));

    match adapter.get_account_info().await {
        Ok(info) => {
            for balance in &info.balances {
                if balance.asset == "USDC" || balance.asset == "USDT" {
                    let total = balance.free + balance.locked;
                    if total > 0.0 {
                        println!("  {}: free=${:.2}, locked=${:.2}, total=${:.2}",
                            balance.asset, balance.free, balance.locked, total);
                    }
                }
            }
        }
        Err(e) => {
            println!("❌ Failed to get account info: {}", e);
            return;
        }
    }

    // Get and cancel all open orders
    println!("\n🔍 Checking for Open Orders:");
    println!("{}", "-".repeat(70));

    let symbols = vec!["ETHUSDC", "ETHUSDT", "BTCUSDT"];
    let mut total_cancelled = 0;

    for symbol in &symbols {
        println!("\n  Checking {}...", symbol);

        match adapter.get_open_orders(Some(symbol)).await {
            Ok(orders) => {
                if orders.is_empty() {
                    println!("    ✓ No open orders");
                } else {
                    println!("    Found {} open orders", orders.len());

                    for order in orders {
                        print!("    Cancelling order {}...", order.venue_order_id);

                        match adapter.cancel_order(symbol, &order.venue_order_id).await {
                            Ok(_) => {
                                println!(" ✅");
                                total_cancelled += 1;
                            }
                            Err(e) => {
                                println!(" ❌ {}", e);
                            }
                        }

                        // Small delay to avoid rate limits
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    }
                }
            }
            Err(e) => {
                println!("    ⚠️  Could not get orders: {}", e);
            }
        }
    }

    // Check final balances
    println!("\n📊 Updated Account Status:");
    println!("{}", "-".repeat(70));

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // Wait for balance update

    match adapter.get_account_info().await {
        Ok(info) => {
            for balance in &info.balances {
                if balance.asset == "USDC" || balance.asset == "USDT" {
                    let total = balance.free + balance.locked;
                    if total > 0.0 {
                        println!("  {}: free=${:.2}, locked=${:.2}, total=${:.2}",
                            balance.asset, balance.free, balance.locked, total);
                    }
                }
            }
        }
        Err(e) => {
            println!("❌ Failed to get updated account info: {}", e);
        }
    }

    println!("\n✅ Summary:");
    println!("   Total orders cancelled: {}", total_cancelled);
    println!("{}", "=".repeat(70));
}
