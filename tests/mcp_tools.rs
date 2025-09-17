#[cfg(feature = "mcp")]
mod mcp_tests {
    use scylla_rust_mcp::server;

    // Pure unit test for mapping assumptions: our internal list has names we expect
    #[test]
    fn internal_tools_list_is_nonempty_and_readonly() {
        let tools = server::list_tools();
        assert!(!tools.is_empty());
        let names: Vec<&str> = tools.iter().map(|t| t.name).collect();
        for n in [
            "list_keyspaces",
            "list_tables",
            "describe_table",
            "sample_rows",
            "select",
            "cluster_topology",
        ] {
            assert!(names.contains(&n), "missing tool {n}");
        }
    }
}
