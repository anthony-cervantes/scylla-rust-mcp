#[cfg(feature = "mcp")]
#[tokio::test]
#[ignore]
async fn describe_table_live() {
    if std::env::var("SCYLLA_URI").is_err() {
        eprintln!("SCYLLA_URI not set; skipping");
        return;
    }
    let ks = std::env::var("TEST_KEYSPACE").unwrap_or_else(|_| "system_schema".into());
    let tb = std::env::var("TEST_TABLE").unwrap_or_else(|_| "tables".into());
    let meta = scylla_rust_mcp::db::describe_table(&ks, &tb)
        .await
        .expect("describe_table failed");
    assert_eq!(meta.keyspace, ks);
    assert_eq!(meta.table, tb);
    assert!(!meta.columns.is_empty());
}
