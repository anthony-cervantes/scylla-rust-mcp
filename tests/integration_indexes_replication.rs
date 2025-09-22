#[tokio::test]
#[ignore]
async fn list_indexes_live() {
    if std::env::var("SCYLLA_URI").is_err() {
        eprintln!("SCYLLA_URI not set; skipping");
        return;
    }
    // Defaults point at system tables; likely zero indexes, still validates call path
    let ks = std::env::var("TEST_KEYSPACE").unwrap_or_else(|_| "system_schema".into());
    let tb = std::env::var("TEST_TABLE").unwrap_or_else(|_| "tables".into());
    let _ = scylla_rust_mcp::db::list_indexes(&ks, &tb)
        .await
        .expect("list_indexes failed");
}

#[tokio::test]
#[ignore]
async fn keyspace_replication_live() {
    if std::env::var("SCYLLA_URI").is_err() {
        eprintln!("SCYLLA_URI not set; skipping");
        return;
    }
    let ks = std::env::var("TEST_KEYSPACE").unwrap_or_else(|_| "system_schema".into());
    let obj = scylla_rust_mcp::db::keyspace_replication(&ks)
        .await
        .expect("keyspace_replication failed");
    assert!(obj.contains_key("replication"));
}
