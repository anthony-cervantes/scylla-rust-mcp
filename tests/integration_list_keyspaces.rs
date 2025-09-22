#[tokio::test]
#[ignore]
async fn list_keyspaces_live() {
    // Runs only when SCYLLA_URI is set and feature `mcp` is enabled
    if std::env::var("SCYLLA_URI").is_err() {
        eprintln!("SCYLLA_URI not set; skipping live integration test");
        return;
    }
    let ks = scylla_rust_mcp::db::list_keyspaces()
        .await
        .expect("query failed");
    assert!(!ks.is_empty(), "expected at least one keyspace");
}
