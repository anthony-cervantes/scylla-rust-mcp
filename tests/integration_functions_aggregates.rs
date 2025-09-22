#[tokio::test]
#[ignore]
async fn list_functions_aggregates_live() {
    if std::env::var("SCYLLA_URI").is_err() {
        eprintln!("SCYLLA_URI not set; skipping");
        return;
    }
    let ks = std::env::var("TEST_KEYSPACE").unwrap_or_else(|_| "system".into());
    let session = scylla::SessionBuilder::new()
        .known_node(std::env::var("SCYLLA_URI").unwrap())
        .build()
        .await
        .unwrap();
    let _f = scylla_rust_mcp::db::list_functions_with(&session, &ks);
}
