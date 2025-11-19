use serde_json::{ json, Value };

#[test]
fn mexc_subscribe_deal_serializes() -> Result<(), Box<dyn std::error::Error>> {
    let payload =
        json!({
        "method": "sub.deal",
        "params": ["BTC_USDT"],
        "id": 1
    });

    let s = serde_json::to_string(&payload)?;
    let parsed: Value = serde_json::from_str(&s)?;
    assert_eq!(parsed, payload);
    Ok(())
}

#[test]
fn mexc_subscribe_ticker_serializes() -> Result<(), Box<dyn std::error::Error>> {
    let payload =
        json!({
        "method": "sub.ticker",
        "params": ["BTC_USDT"],
        "id": 2
    });

    let s = serde_json::to_string(&payload)?;
    let parsed: Value = serde_json::from_str(&s)?;
    assert_eq!(parsed, payload);
    Ok(())
}
