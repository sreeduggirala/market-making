use adapters::mexc::MexcSpotAdapter;
use adapters::traits::{SpotRest, OrderType, Side, NewOrder};

#[tokio::main]
async fn main() {
    // Enable logging to see all errors
    env_logger::init();

    let api_key = std::env::var("MEXC_API_KEY").expect("MEXC_API_KEY not set");
    let api_secret = std::env::var("MEXC_API_SECRET").expect("MEXC_API_SECRET not set");

    println!("Creating adapter...");
    let adapter = MexcSpotAdapter::new(api_key, api_secret);

    // Test 1: Get account info
    println!("\n1. Testing account access...");
    match adapter.get_account_info().await {
        Ok(info) => println!("✓ Account info retrieved: can_trade={}", info.can_trade),
        Err(e) => {
            println!("✗ Failed to get account info:");
            println!("  Error: {:?}", e);
            return;
        }
    }

    // Test 2: Get ticker
    println!("\n2. Getting BTCUSDT ticker...");
    let ticker = match adapter.get_ticker("BTCUSDT").await {
        Ok(t) => {
            println!("✓ Current price: ${:.2}", t.last_price);
            t
        },
        Err(e) => {
            println!("✗ Failed to get ticker:");
            println!("  Error: {:?}", e);
            return;
        }
    };

    // Test 3: Try to create an order
    println!("\n3. Creating test order...");
    let test_price = ticker.last_price * 0.01; // 99% below market

    let result = adapter.create_order(NewOrder {
        symbol: "BTCUSDT".to_string(),
        side: Side::Buy,
        ord_type: OrderType::Limit,
        qty: 0.0001,
        price: Some(test_price),
        stop_price: None,
        tif: None,
        post_only: false,
        reduce_only: false,
        client_order_id: format!("test_{}", chrono::Utc::now().timestamp_millis()),
    }).await;

    match result {
        Ok(order) => {
            println!("✓ Order created successfully!");
            println!("  Order ID: {}", order.venue_order_id);
            println!("  Status: {:?}", order.status);
        },
        Err(e) => {
            println!("✗ Failed to create order:");
            println!("  Full error chain:");
            for (i, cause) in e.chain().enumerate() {
                println!("  [{}] {}", i, cause);
            }
        }
    }
}
