use serde::Deserialize;

const KRAKEN_TICKER: &str = include_str!("./fixtures/kraken_ticker.json");
const KRAKEN_WS_ORDER: &str = include_str!("./fixtures/kraken_ws_order.json");
const MEXC_TRADE: &str = include_str!("./fixtures/mexc_trade.json");

#[derive(Debug, Deserialize)]
struct MinimalKrakenTicker {
    #[serde(default)]
    a: Vec<String>,
    #[serde(default)]
    b: Vec<String>,
    #[serde(default)]
    c: Vec<String>,
    #[serde(default)]
    o: Option<String>,
}

#[derive(Debug, Deserialize)]
struct KrakenWsOrder {
    order_id: String,
    cl_ord_id: Option<String>,
    symbol: String,
    side: String,
    order_type: String,
    order_qty: String,
    limit_price: Option<String>,
    filled_qty: Option<String>,
    order_status: String,
    timestamp: String,
}

#[derive(Debug, Deserialize)]
struct MexcTrade {
    symbol: String,
    side: String,
    price: String,
    qty: String,
    timestamp: String,
}

#[test]
fn deserialize_kraken_ticker() {
    let t: MinimalKrakenTicker = serde_json::from_str(KRAKEN_TICKER).expect("deserialize ticker");
    // basic sanity checks
    assert!(t.a.len() >= 1);
    assert!(t.b.len() >= 1);
    assert!(t.c.len() >= 1);
}

#[test]
fn deserialize_kraken_ws_order_optional_fields() {
    let o: KrakenWsOrder = serde_json::from_str(KRAKEN_WS_ORDER).expect("deserialize ws order");
    // ensure optionals are handled
    assert_eq!(o.order_id, "O12345");
    assert!(o.cl_ord_id.is_none());
    assert!(o.limit_price.is_none());
}

#[test]
fn deserialize_mexc_trade() {
    let t: MexcTrade = serde_json::from_str(MEXC_TRADE).expect("deserialize mexc trade");
    assert_eq!(t.symbol, "BTC_USDT");
}
