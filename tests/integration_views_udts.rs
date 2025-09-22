#[tokio::test]
#[ignore]
async fn list_views_live() {
    if std::env::var("SCYLLA_URI").is_err() {
        eprintln!("SCYLLA_URI not set; skipping");
        return;
    }
    let ks = std::env::var("TEST_KEYSPACE").unwrap_or_else(|_| "system_schema".into());
    let _ = scylla_rust_mcp::db::list_views(&ks)
        .await
        .expect("list_views failed");
}

#[tokio::test]
#[ignore]
async fn list_udts_live() {
    if std::env::var("SCYLLA_URI").is_err() {
        eprintln!("SCYLLA_URI not set; skipping");
        return;
    }
    let ks = std::env::var("TEST_KEYSPACE").unwrap_or_else(|_| "system_schema".into());
    let _ = scylla_rust_mcp::db::list_udts(&ks)
        .await
        .expect("list_udts failed");
}
