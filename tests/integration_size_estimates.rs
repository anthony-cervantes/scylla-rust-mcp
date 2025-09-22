#[tokio::test]
#[ignore]
async fn size_estimates_live() {
    if std::env::var("SCYLLA_URI").is_err() {
        eprintln!("SCYLLA_URI not set; skipping");
        return;
    }
    let ks = std::env::var("TEST_KEYSPACE").unwrap_or_else(|_| "system_schema".into());
    let tb = std::env::var("TEST_TABLE").unwrap_or_else(|_| "tables".into());
    let sess = scylla::SessionBuilder::new()
        .known_node(std::env::var("SCYLLA_URI").unwrap())
        .build()
        .await
        .unwrap();
    let _ = scylla_rust_mcp::db::size_estimates_with(&sess, &ks, &tb)
        .await
        .expect("size_estimates failed");
}
