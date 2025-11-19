use std::time::Duration;
use std::sync::Arc;

#[tokio::test]
async fn kraken_reconnect_state_transitions() {
    // Instantiate adapter with dummy creds; reconnect is a no-op that updates status/count
    let adapter = Arc::new(adapters::kraken::KrakenSpotAdapter::new("key".into(), "secret".into()));

    // initial health should be Disconnected
    let h = adapter.health().await.expect("health call");
    assert_eq!(h.status, adapters::traits::ConnectionStatus::Disconnected);

    // Spawn reconnect in background and check transient Reconnecting state
    let adapter_cloned = Arc::clone(&adapter);
    let handle = tokio::spawn(async move {
        adapter_cloned.reconnect().await.expect("reconnect should succeed");
    });

    // give the reconnect task a moment to flip state
    tokio::time::sleep(Duration::from_millis(50)).await;

    let h_mid = adapter.health().await.expect("health mid");
    // during reconnect the adapter should report Reconnecting (or Connecting depending on timing)
    assert!(
        matches!(
            h_mid.status,
            adapters::traits::ConnectionStatus::Reconnecting |
                adapters::traits::ConnectionStatus::Connecting
        )
    );

    // wait for task to finish and then assert final state and reconnect count increment
    let _ = tokio::time
        ::timeout(Duration::from_secs(2), handle).await
        .expect("reconnect task timed out");
    let h_final = adapter.health().await.expect("health final");
    // Kraken reconnect sets status to Connecting at end
    assert!(
        matches!(
            h_final.status,
            adapters::traits::ConnectionStatus::Connecting |
                adapters::traits::ConnectionStatus::Connected
        )
    );
    assert!(h_final.reconnect_count >= 1);
}

#[tokio::test]
async fn mexc_reconnect_sets_reconnecting() {
    // Mexc reconnect performs network calls to create listen key; we test transient state only
    let adapter = Arc::new(adapters::mexc::MexcSpotAdapter::new("key".into(), "secret".into()));

    let h = adapter.health().await.expect("health call");
    assert_eq!(h.status, adapters::traits::ConnectionStatus::Disconnected);

    // Spawn reconnect but do not await full completion (it may attempt network calls)
    let adapter_cloned = Arc::clone(&adapter);
    let handle = tokio::spawn(async move {
        let _ = adapter_cloned.reconnect().await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let h_mid = adapter.health().await.expect("health mid");
    // During reconnect the status should be Reconnecting
    assert_eq!(h_mid.status, adapters::traits::ConnectionStatus::Reconnecting);

    // abort the background task to avoid long network waits in tests
    handle.abort();
}
