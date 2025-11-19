use serde_json::json;

#[test]
fn kraken_ws_trade_subscribe_serializes() -> Result<(), Box<dyn std::error::Error>> {
    // Public trade subscription (WebSocket v2 public)
    let payload =
        json!({
        "event": "subscribe",
        "pair": ["XBT/USD"],
        "subscription": { "name": "trade" }
    });

    let s = serde_json::to_string(&payload)?;
    let expected = r#"{"event":"subscribe","pair":["XBT/USD"],"subscription":{"name":"trade"}}"#;
    assert_eq!(s, expected);
    Ok(())
}

#[test]
fn kraken_rest_addorder_param_shape() -> Result<(), Box<dyn std::error::Error>> {
    // Represent the AddOrder parameters as a JSON object for schema validation in tests.
    let params =
        json!({
        "pair": "XBTUSD",
        "type": "buy",
        "ordertype": "limit",
        "price": "50000",
        "volume": "0.01"
    });

    // Ensure required keys exist and are strings
    let obj = params.as_object().expect("params should be an object");
    for key in ["pair", "type", "ordertype", "price", "volume"] {
        assert!(obj.contains_key(key));
        assert!(obj.get(key).unwrap().is_string());
    }

    Ok(())
}
