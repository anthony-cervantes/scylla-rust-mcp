#[cfg(feature = "mcp")]
mod args_tests {
    use serde_json::{Map, Value, json};

    fn extract_key(map: &Map<String, Value>, key: &str) -> Option<String> {
        map.get(key).and_then(|v| v.as_str()).map(|s| s.to_string())
    }

    #[test]
    fn extracts_required_string_arg() {
        let mut m = Map::new();
        m.insert("keyspace".into(), json!("ks1"));
        assert_eq!(extract_key(&m, "keyspace").as_deref(), Some("ks1"));
    }

    #[test]
    fn missing_required_string_arg() {
        let m = Map::new();
        assert!(extract_key(&m, "keyspace").is_none());
    }

    #[test]
    fn extracts_two_required_args() {
        let mut m = Map::new();
        m.insert("keyspace".into(), json!("ks1"));
        m.insert("table".into(), json!("t1"));
        assert_eq!(extract_key(&m, "keyspace").as_deref(), Some("ks1"));
        assert_eq!(extract_key(&m, "table").as_deref(), Some("t1"));
    }
}
