use scylla_rust_mcp::server::{list_tools, server_info};

#[test]
fn server_info_contains_expected_metadata() {
    let info = server_info();
    assert_eq!(info.name, "scylla-rust-mcp");
    assert!(info.instructions.contains("Read-only MCP"));
}

#[test]
fn tools_include_core_read_only_actions() {
    let names: Vec<&str> = list_tools().into_iter().map(|t| t.name).collect();
    let expected = [
        "list_keyspaces",
        "list_tables",
        "describe_table",
        "sample_rows",
        "select",
        "cluster_topology",
    ];
    for e in expected {
        assert!(names.contains(&e), "missing tool {e}");
    }
}
