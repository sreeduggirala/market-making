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
async fn mexc_rest_ping() {
    if !live_tests_enabled() {
        eprintln!("skipping live test");
        return;
    }

    load_dotenv();

    let client = reqwest::Client
        ::builder()
        .user_agent("market-making-tests/0.1")
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .expect("failed to build reqwest client");

    // MEXC public server example
    let url = "https://api.mexc.com/api/v3/time"; // endpoint may vary
    let start = std::time::Instant::now();

    let resp = tokio::time
        ::timeout(std::time::Duration::from_secs(12), client.get(url).send()).await
        .expect("request timed out")
        .expect("request failed");

    let latency_ms = start.elapsed().as_millis();
    eprintln!("mexc ping latency_ms={} status={}", latency_ms, resp.status());

    assert!(resp.status().is_success(), "status not success: {}", resp.status());

    let _json: serde_json::Value = resp.json().await.expect("failed to parse json");
}

#[tokio::test]
async fn mexc_ws_short_connect() {
    if !live_tests_enabled() {
        eprintln!("skipping live ws test");
        return;
    }

    load_dotenv();

    // MEXC websocket url (public). Allow override via env for spot vs contract
    let url = std::env::var("MEXC_WS_URL").unwrap_or_else(|_| "wss://contract.mexc.com/ws".to_string());
    let symbol = std::env::var("MEXC_TEST_SYMBOL").unwrap_or_else(|_| "BTC_USDT".to_string());
    let short_duration = std::env::var("WS_SHORT_DURATION_SECS").ok()
        .and_then(|s| s.parse().ok()).unwrap_or(35u64);

    let (mut ws, _resp) = tokio::time
        ::timeout(std::time::Duration::from_secs(10), tokio_tungstenite::connect_async(url)).await
        .expect("connect timed out")
        .expect("connect failed");

    // Example subscribe message (best-effort spot trade subscription). Adjust if your adapter expects different envelope.
    let subscribe = serde_json::json!({
        "method": "sub.deal",
        "symbol": symbol.clone()
    });

    ws.send(tungstenite::Message::Text(subscribe.to_string())).await.expect("send failed");

    // Wait for a subscription acknowledgement (best-effort generic check)
    let subscribe_timeout = std::time::Duration::from_secs(10);
    let mut got_ack = false;
    if let Ok(Some(Ok(tungstenite::Message::Text(txt)))) = tokio::time::timeout(subscribe_timeout, ws.next()).await {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&txt) {
            // common patterns: code==0, success field, or channel/sub fields
            if v.get("code").and_then(|c| c.as_i64()) == Some(0) {
                got_ack = true;
            }
            if v.get("success").and_then(|s| s.as_bool()) == Some(true) {
                got_ack = true;
            }
            if v.get("channel").is_some() || v.get("method").is_some() {
                got_ack = true;
            }
        }
    }
    if !got_ack {
        eprintln!("Warning: did not observe explicit subscribe ack for symbol={}", symbol);
    }

    let start = std::time::Instant::now();
    let duration = std::time::Duration::from_secs(short_duration);

    while start.elapsed() < duration {
        match tokio::time::timeout(std::time::Duration::from_secs(10), ws.next()).await {
            Ok(Some(Ok(tungstenite::Message::Text(txt)))) => {
                match serde_json::from_str::<serde_json::Value>(&txt) {
                    Ok(v) => {
                        // basic semantic validation: trade messages should include symbol, price or data array
                        if v.get("symbol").is_some() || v.get("data").is_some() {
                            // good
                        }
                    }
                    Err(e) => eprintln!("mexc: failed to parse ws json: {} text: {}", e, txt),
                }
            }
            Ok(Some(Ok(tungstenite::Message::Ping(payload)))) => {
                let _ = ws.send(tungstenite::Message::Pong(payload)).await;
            }
            Ok(Some(Ok(tungstenite::Message::Pong(_)))) => { }
            Ok(Some(Ok(_))) => { }
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
async fn mexc_ws_long_stability() {
    if !live_tests_enabled() || !long_ws_enabled() {
        eprintln!("skipping long ws stability test");
        return;
    }

    load_dotenv();

    let url = "wss://contract.mexc.com/ws";
    let (mut ws, _resp) = tokio::time
        ::timeout(std::time::Duration::from_secs(10), tokio_tungstenite::connect_async(url)).await
        .expect("connect timed out")
        .expect("connect failed");

    let subscribe =
        serde_json::json!({
        "method": "sub.deal",
        "symbol": "BTC_USDT"
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
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(e))) => panic!("ws recv error: {}", e),
            Ok(None) => panic!("ws stream closed unexpectedly"),
            Err(_) => panic!("ws message timeout waiting up to 60s"),
        }

        if last_received.elapsed() > std::time::Duration::from_secs(90) {
            panic!("no messages for >90s during long-lived WS test");
        }
    }

    ws.close(None).await.expect("close failed");
}
