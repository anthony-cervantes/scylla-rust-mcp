use scylla_rust_mcp::schema::AgentValue;

#[test]
fn agent_value_preserves_basic_types() {
    let cases = vec![
        AgentValue::Null,
        AgentValue::Bool(true),
        AgentValue::Int(42),
        AgentValue::Text("hello".to_string()),
    ];
    for c in cases {
        // For now we only validate the type tagging API surface
        let tn = c.type_name();
        match c {
            AgentValue::Null => assert_eq!(tn, "null"),
            AgentValue::Bool(_) => assert_eq!(tn, "bool"),
            AgentValue::Int(_) => assert_eq!(tn, "int"),
            AgentValue::Text(_) => assert_eq!(tn, "text"),
        }
    }
}
