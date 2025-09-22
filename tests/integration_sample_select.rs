#[tokio::test]
#[ignore]
async fn sample_rows_live() {
    if std::env::var("SCYLLA_URI").is_err() {
        eprintln!("SCYLLA_URI not set; skipping");
        return;
    }
    let ks = std::env::var("TEST_KEYSPACE").unwrap_or_else(|_| "system_schema".into());
    let tb = std::env::var("TEST_TABLE").unwrap_or_else(|_| "tables".into());
    let rows = scylla_rust_mcp::db::sample_rows(&ks, &tb, 5, None)
        .await
        .expect("sample_rows failed");
    assert!(rows.len() <= 5);
}

#[tokio::test]
#[ignore]
async fn select_columns_live() {
    if std::env::var("SCYLLA_URI").is_err() {
        eprintln!("SCYLLA_URI not set; skipping");
        return;
    }
    let ks = std::env::var("TEST_KEYSPACE").unwrap_or_else(|_| "system_schema".into());
    let tb = std::env::var("TEST_TABLE").unwrap_or_else(|_| "tables".into());
    let cols = vec!["table_name".to_string()];
    let rows = scylla_rust_mcp::db::select_columns(&ks, &tb, &cols, 3, None, None)
        .await
        .expect("select failed");
    assert!(rows.len() <= 3);
}

#[tokio::test]
#[ignore]
async fn cluster_topology_live() {
    if std::env::var("SCYLLA_URI").is_err() {
        eprintln!("SCYLLA_URI not set; skipping");
        return;
    }
    let nodes = scylla_rust_mcp::db::cluster_topology()
        .await
        .expect("topology failed");
    assert!(!nodes.is_empty());
}
