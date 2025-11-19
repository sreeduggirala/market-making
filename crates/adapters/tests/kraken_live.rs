// Local helpers (avoid shared test-utils crate - keep tests self-contained)
use futures_util::{ StreamExt, SinkExt };

fn live_tests_enabled() -> bool {
    std::env
        ::var("ENABLE_LIVE_TESTS")
        .map(|v| (v == "1" || v.to_lowercase() == "true"))
        .unwrap_or(false)
}

fn long_ws_enabled() -> bool {
    std::env
        ::var("ENABLE_LONG_WS")
        .map(|v| (v == "1" || v.to_lowercase() == "true"))
        .unwrap_or(false)
}

fn load_dotenv() {
    let _ = dotenvy::dotenv();
}

#[tokio::test]
async fn kraken_rest_ping() {
    if !live_tests_enabled() {
        eprintln!("skipping live test");
        return;
    }

    // Load .env (non-fatal) so env vars are available for local runs
    load_dotenv();

    let client = reqwest::Client
        ::builder()
        .user_agent("market-making-tests/0.1")
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .expect("failed to build reqwest client");

    let url = "https://api.kraken.com/0/public/Time";
    let start = std::time::Instant::now();

    let resp = tokio::time
        ::timeout(std::time::Duration::from_secs(12), client.get(url).send()).await
        .expect("request timed out")
        .expect("request failed");

    let latency_ms = start.elapsed().as_millis();
    eprintln!("kraken ping latency_ms={} status={}", latency_ms, resp.status());

    assert!(resp.status().is_success(), "status not success: {}", resp.status());

    let _json: serde_json::Value = resp.json().await.expect("failed to parse json");
}

#[tokio::test]
async fn kraken_ws_short_connect() {
    if !live_tests_enabled() {
        eprintln!("skipping live ws test");
        return;
    }

    // Basic WS connect, subscribe to ticker and receive messages for ~30s
    let url = std::env::var("KRAKEN_WS_URL").unwrap_or_else(|_| "wss://ws.kraken.com".to_string());
    let symbol = std::env::var("KRAKEN_TEST_SYMBOL").unwrap_or_else(|_| "XBT/USD".to_string());
    let short_duration = std::env::var("WS_SHORT_DURATION_SECS").ok()
        .and_then(|s| s.parse().ok()).unwrap_or(35u64);
    let subscribe_timeout = std::time::Duration::from_secs(10);
    let (mut ws, _resp) = tokio::time
        ::timeout(std::time::Duration::from_secs(10), tokio_tungstenite::connect_async(url)).await
        .expect("connect timed out")
        .expect("connect failed");

    let subscribe = serde_json::json!({
        "event": "subscribe",
        "pair": [symbol.clone()],
        "subscription": { "name": "ticker" }
    });

    ws.send(tungstenite::Message::Text(subscribe.to_string())).await.expect("send failed");

    // Wait for a subscription acknowledgement (best-effort generic check)
    let mut got_ack = false;
    match tokio::time::timeout(subscribe_timeout, ws.next()).await {
        Ok(Some(Ok(tungstenite::Message::Text(txt)))) => {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&txt) {
                if v.get("event").and_then(|e| e.as_str()) == Some("subscriptionStatus") {
                    if v.get("status").and_then(|s| s.as_str()) == Some("subscribed") {
                        got_ack = true;
                    }
                } else if v.get("channelName").is_some() || v.get("channel").is_some() {
                    // some responses include channel info
                    got_ack = true;
                }
            }
        }
        _ => { /* ignore - we'll continue and observe messages */ }
    }

    if !got_ack {
        eprintln!("Warning: did not observe explicit subscribe ack for symbol={}", symbol);
    }

    let start = std::time::Instant::now();
    let duration = std::time::Duration::from_secs(short_duration);

    while start.elapsed() < duration {
        match tokio::time::timeout(std::time::Duration::from_secs(10), ws.next()).await {
            Ok(Some(Ok(tungstenite::Message::Text(txt)))) => {
                // Attempt to parse JSON but handle malformed messages gracefully
                match serde_json::from_str::<serde_json::Value>(&txt) {
                    Ok(v) => {
                        // basic semantic check: if it's a ticker channel, it may be an array or object
                        if v.is_object() {
                            // could be heartbeat or subscriptionStatus, ignore
                        }
                    }
                    Err(e) => {
                        eprintln!("kraken: failed to parse ws json: {} text: {}", e, txt);
                    }
                }
            }
            Ok(Some(Ok(tungstenite::Message::Ping(payload)))) => {
                // reply to ping
                let _ = ws.send(tungstenite::Message::Pong(payload)).await;
            }
            Ok(Some(Ok(tungstenite::Message::Pong(_)))) => { /* ignore */ }
            Ok(Some(Ok(_))) => { /* ignore other message types */ }
            Ok(Some(Err(e))) => panic!("ws recv error: {}", e),
            Ok(None) => panic!("ws stream closed unexpectedly"),
            Err(_) => eprintln!("ws message timeout (10s)");
        }
    }

    ws.close(None).await.expect("close failed");
}

// Long-lived WebSocket stability test (5 minutes). Marked `#[ignore]` so it does not run by default.
#[tokio::test]
#[ignore]
async fn kraken_ws_long_stability() {
    if !live_tests_enabled() || !long_ws_enabled() {
        eprintln!("skipping long ws stability test");
        return;
    }

    let url = "wss://ws.kraken.com";
    let (mut ws, _resp) = tokio::time
        ::timeout(std::time::Duration::from_secs(10), tokio_tungstenite::connect_async(url)).await
        .expect("connect timed out")
        .expect("connect failed");

    let subscribe =
        serde_json::json!({
        "event": "subscribe",
        "pair": ["XBT/USD"],
        "subscription": { "name": "ticker" }
    });

    ws.send(tungstenite::Message::Text(subscribe.to_string())).await.expect("send failed");

    let start = std::time::Instant::now();
    let duration = std::time::Duration::from_secs(5 * 60); // 5 minutes
    let mut last_received = std::time::Instant::now();

    while start.elapsed() < duration {
        match tokio::time::timeout(std::time::Duration::from_secs(60), ws.next()).await {
            Ok(Some(Ok(tungstenite::Message::Text(txt)))) => {
                last_received = std::time::Instant::now();
                let _v: serde_json::Value = serde_json::from_str(&txt).unwrap_or_else(|e| {
                    panic!("failed to parse ws json: {} text: {}", e, txt);
                });
            }
            Ok(Some(Ok(_))) => {/* ignore other message types */}
            Ok(Some(Err(e))) => panic!("ws recv error: {}", e),
            Ok(None) => panic!("ws stream closed unexpectedly"),
            Err(_) => panic!("ws message timeout waiting up to 60s"),
        }

        // sanity check: ensure we are receiving at least something every 90s
        if last_received.elapsed() > std::time::Duration::from_secs(90) {
            panic!("no messages for >90s during long-lived WS test");
        }
    }

    ws.close(None).await.expect("close failed");
}
