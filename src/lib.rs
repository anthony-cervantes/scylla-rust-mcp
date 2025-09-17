pub mod schema {
    #[derive(Debug, Clone, PartialEq)]
    pub enum AgentValue {
        Null,
        Bool(bool),
        Int(i64),
        Text(String),
        // Extend with more CQL types as needed (e.g., BigInt, Decimal, Uuid, Blob, List, Map, Udt, Tuple, Timestamp, etc.)
    }

    impl AgentValue {
        pub fn type_name(&self) -> &'static str {
            match self {
                AgentValue::Null => "null",
                AgentValue::Bool(_) => "bool",
                AgentValue::Int(_) => "int",
                AgentValue::Text(_) => "text",
            }
        }
    }
}

pub mod server {
    #[derive(Debug, Clone, PartialEq)]
    pub struct ServerInfo {
        pub name: &'static str,
        pub version: &'static str,
        pub instructions: &'static str,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct Tool {
        pub name: &'static str,
        pub description: &'static str,
    }

    pub fn server_info() -> ServerInfo {
        ServerInfo {
            name: "scylla-rust-mcp",
            version: env!("CARGO_PKG_VERSION"),
            instructions: "Read-only MCP server for ScyllaDB",
        }
    }

    pub fn list_tools() -> Vec<Tool> {
        vec![
            Tool {
                name: "list_keyspaces",
                description: "List keyspaces in the cluster",
            },
            Tool {
                name: "list_tables",
                description: "List tables for a keyspace",
            },
            Tool {
                name: "describe_table",
                description: "Describe table schema and types",
            },
            Tool {
                name: "sample_rows",
                description: "Sample rows with LIMIT and filters",
            },
            Tool {
                name: "partition_rows",
                description: "Fetch rows by full partition key (prepared)",
            },
            Tool {
                name: "select",
                description: "Execute read-only SELECT queries",
            },
            Tool {
                name: "paged_select",
                description: "Paged SELECT with cursor support",
            },
            Tool {
                name: "cluster_topology",
                description: "Get cluster nodes, datacenters, racks",
            },
            Tool {
                name: "list_indexes",
                description: "List secondary indexes for a table",
            },
            Tool {
                name: "keyspace_replication",
                description: "Show keyspace replication settings",
            },
            Tool {
                name: "list_views",
                description: "List materialized views in a keyspace",
            },
            Tool {
                name: "list_udts",
                description: "List user-defined types in a keyspace",
            },
            Tool {
                name: "list_functions",
                description: "List user-defined functions in a keyspace",
            },
            Tool {
                name: "list_aggregates",
                description: "List user-defined aggregates in a keyspace",
            },
            Tool {
                name: "size_estimates",
                description: "Approximate size estimates for a table",
            },
            Tool {
                name: "search_schema",
                description: "Search tables, columns, UDTs, views, functions, aggregates by pattern",
            },
        ]
    }
}

#[cfg(feature = "mcp")]
pub mod mcp {
    use rust_mcp_sdk::error::SdkResult;
    use tracing::{Level, info};
    use tracing_subscriber::EnvFilter;

    pub async fn run_stdio_server() -> SdkResult<()> {
        // basic, human-friendly logging for early development
        let _ = tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env().add_directive(Level::INFO.into()))
            .try_init();

        info!("starting MCP stdio server (rust-mcp-sdk)");
        use rust_mcp_schema::{
            Implementation, InitializeResult, LATEST_PROTOCOL_VERSION, ServerCapabilities,
            ServerCapabilitiesTools,
        };
        use rust_mcp_sdk::mcp_server::server_runtime;
        use rust_mcp_sdk::{MCPServer, StdioTransport, TransportOptions};

        let server_details = InitializeResult {
            server_info: Implementation {
                name: "scylla-rust-mcp".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
            },
            capabilities: ServerCapabilities {
                tools: Some(ServerCapabilitiesTools { list_changed: None }),
                ..Default::default()
            },
            meta: None,
            instructions: Some("Read-only ScyllaDB access for AI agents".to_string()),
            protocol_version: LATEST_PROTOCOL_VERSION.to_string(),
        };

        let transport = StdioTransport::new(TransportOptions::default())?;
        // Shared Scylla session for handler state
        let mut sb = scylla::SessionBuilder::new();
        let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
        sb = sb.known_node(uri);
        if let (Ok(user), Ok(pass)) = (std::env::var("SCYLLA_USER"), std::env::var("SCYLLA_PASS")) {
            sb = sb.user(user, pass);
        }
        if std::env::var("SCYLLA_SSL")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
        {
            use openssl::ssl::{SslContext, SslMethod, SslVerifyMode};
            let mut ctx = SslContext::builder(SslMethod::tls()).expect("ssl context");
            if let Ok(ca_file) = std::env::var("SCYLLA_CA_BUNDLE") {
                ctx.set_ca_file(ca_file).expect("set ca file");
            }
            let insecure = std::env::var("SCYLLA_SSL_INSECURE")
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false);
            ctx.set_verify(if insecure {
                SslVerifyMode::NONE
            } else {
                SslVerifyMode::PEER
            });
            let ctx = ctx.build();
            sb = sb.ssl_context(Some(ctx));
        }
        let scy_session = sb.build().await.expect("failed to connect to SCYLLA_URI");
        let handler = MinimalHandler::new(scy_session);
        let server = server_runtime::create_server(server_details, transport, handler);
        server.start().await
    }

    // Basic handler – extend as we implement tools
    use std::collections::HashMap as StdHashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    pub struct MinimalHandler {
        session: scylla::Session,
        schema_cache: Arc<RwLock<StdHashMap<(String, String), crate::db::DescribeTable>>>,
    }

    impl MinimalHandler {
        fn new(session: scylla::Session) -> Self {
            Self {
                session,
                schema_cache: Arc::new(RwLock::new(StdHashMap::new())),
            }
        }

        async fn get_schema(
            &self,
            keyspace: &str,
            table: &str,
        ) -> anyhow::Result<crate::db::DescribeTable> {
            let key = (keyspace.to_string(), table.to_string());
            if let Some(found) = self.schema_cache.read().await.get(&key).cloned() {
                return Ok(found);
            }
            // Use the shared session and add a small retry for transient connection errors
            use tokio::time::{sleep, Duration};
            let mut last_err: Option<anyhow::Error> = None;
            for backoff_ms in [0u64, 50, 150] {
                if backoff_ms > 0 { sleep(Duration::from_millis(backoff_ms)).await; }
                match crate::db::describe_table_with(&self.session, keyspace, table).await {
                    Ok(schema) => {
                        self.schema_cache.write().await.insert(key.clone(), schema.clone());
                        return Ok(schema);
                    }
                    Err(e) => { last_err = Some(e); }
                }
            }
            let err = last_err.unwrap_or_else(|| anyhow::anyhow!("unknown schema error"));
            Err(err)
        }
    }

    #[async_trait::async_trait]
    impl rust_mcp_sdk::mcp_server::ServerHandler for MinimalHandler {
        async fn handle_list_tools_request(
            &self,
            _request: rust_mcp_schema::ListToolsRequest,
            runtime: &dyn rust_mcp_sdk::MCPServer,
        ) -> std::result::Result<rust_mcp_schema::ListToolsResult, rust_mcp_schema::RpcError>
        {
            // Ensure capability exists
            runtime.assert_server_request_capabilities(&"tools/list".to_string())?;

            use rust_mcp_schema::{ListToolsResult, Tool, ToolInputSchema};
            use serde_json::{Map, Value};
            use std::collections::HashMap;
            // Map internal tool list to MCP schema
            let tools = crate::server::list_tools()
                .into_iter()
                .map(|t| {
                    let mut required: Vec<String> = Vec::new();
                    let mut props: HashMap<String, Map<String, Value>> = HashMap::new();
                    if t.name == "list_tables" {
                        required.push("keyspace".into());
                        let mut keyspace_schema = Map::new();
                        keyspace_schema.insert("type".into(), Value::String("string".into()));
                        keyspace_schema
                            .insert("description".into(), Value::String("Keyspace name".into()));
                        props.insert("keyspace".into(), keyspace_schema);
                    } else if t.name == "describe_table" {
                        required.push("keyspace".into());
                        required.push("table".into());
                        let mut ks = Map::new();
                        ks.insert("type".into(), Value::String("string".into()));
                        ks.insert("description".into(), Value::String("Keyspace name".into()));
                        props.insert("keyspace".into(), ks);
                        let mut tb = Map::new();
                        tb.insert("type".into(), Value::String("string".into()));
                        tb.insert("description".into(), Value::String("Table name".into()));
                        props.insert("table".into(), tb);
                    } else if t.name == "sample_rows" {
                        required.extend(["keyspace".into(), "table".into(), "limit".into()]);
                        let mut ks = Map::new();
                        ks.insert("type".into(), Value::String("string".into()));
                        ks.insert("description".into(), Value::String("Keyspace name".into()));
                        props.insert("keyspace".into(), ks);
                        let mut tb = Map::new();
                        tb.insert("type".into(), Value::String("string".into()));
                        tb.insert("description".into(), Value::String("Table name".into()));
                        props.insert("table".into(), tb);
                        let mut lm = Map::new();
                        lm.insert("type".into(), Value::String("integer".into()));
                        lm.insert("minimum".into(), Value::from(1));
                        lm.insert("maximum".into(), Value::from(500));
                        props.insert("limit".into(), lm);
                        // optional filters: object<string, any>
                        let mut fl = Map::new();
                        fl.insert("type".into(), Value::String("object".into()));
                        props.insert("filters".into(), fl);
                    } else if t.name == "partition_rows" {
                        required.extend([
                            "keyspace".into(),
                            "table".into(),
                            "partition".into(),
                            "limit".into(),
                        ]);
                        let mut ks = Map::new();
                        ks.insert("type".into(), Value::String("string".into()));
                        ks.insert("description".into(), Value::String("Keyspace name".into()));
                        props.insert("keyspace".into(), ks);
                        let mut tb = Map::new();
                        tb.insert("type".into(), Value::String("string".into()));
                        tb.insert("description".into(), Value::String("Table name".into()));
                        props.insert("table".into(), tb);
                        let mut pk = Map::new();
                        pk.insert("type".into(), Value::String("object".into()));
                        pk.insert(
                            "description".into(),
                            Value::String("Map of partition key column -> value".into()),
                        );
                        props.insert("partition".into(), pk);
                        let mut lm = Map::new();
                        lm.insert("type".into(), Value::String("integer".into()));
                        lm.insert("minimum".into(), Value::from(1));
                        lm.insert("maximum".into(), Value::from(500));
                        props.insert("limit".into(), lm);
                    } else if t.name == "select" {
                        required.extend([
                            "keyspace".into(),
                            "table".into(),
                            "columns".into(),
                            "limit".into(),
                        ]);
                        let mut ks = Map::new();
                        ks.insert("type".into(), Value::String("string".into()));
                        ks.insert("description".into(), Value::String("Keyspace name".into()));
                        props.insert("keyspace".into(), ks);
                        let mut tb = Map::new();
                        tb.insert("type".into(), Value::String("string".into()));
                        tb.insert("description".into(), Value::String("Table name".into()));
                        props.insert("table".into(), tb);
                        let mut cols = Map::new();
                        cols.insert("type".into(), Value::String("array".into()));
                        props.insert("columns".into(), cols);
                        let mut lm = Map::new();
                        lm.insert("type".into(), Value::String("integer".into()));
                        lm.insert("minimum".into(), Value::from(1));
                        lm.insert("maximum".into(), Value::from(500));
                        props.insert("limit".into(), lm);
                        let mut fl = Map::new();
                        fl.insert("type".into(), Value::String("object".into()));
                        props.insert("filters".into(), fl);
                        // order_by: [{ column: string, direction: string }]
                        let mut ob = Map::new();
                        ob.insert("type".into(), Value::String("array".into()));
                        props.insert("order_by".into(), ob);
                    } else if t.name == "paged_select" {
                        required.extend([
                            "keyspace".into(),
                            "table".into(),
                            "columns".into(),
                            "page_size".into(),
                        ]);
                        let mut ks = Map::new();
                        ks.insert("type".into(), Value::String("string".into()));
                        ks.insert("description".into(), Value::String("Keyspace name".into()));
                        props.insert("keyspace".into(), ks);
                        let mut tb = Map::new();
                        tb.insert("type".into(), Value::String("string".into()));
                        tb.insert("description".into(), Value::String("Table name".into()));
                        props.insert("table".into(), tb);
                        let mut cols = Map::new();
                        cols.insert("type".into(), Value::String("array".into()));
                        props.insert("columns".into(), cols);
                        let mut ps = Map::new();
                        ps.insert("type".into(), Value::String("integer".into()));
                        ps.insert("minimum".into(), Value::from(1));
                        ps.insert("maximum".into(), Value::from(500));
                        props.insert("page_size".into(), ps);
                        let mut fl = Map::new();
                        fl.insert("type".into(), Value::String("object".into()));
                        props.insert("filters".into(), fl);
                        let mut ob = Map::new();
                        ob.insert("type".into(), Value::String("array".into()));
                        props.insert("order_by".into(), ob);
                        let mut cur = Map::new();
                        cur.insert("type".into(), Value::String("string".into()));
                        props.insert("cursor".into(), cur);
                    } else if t.name == "cluster_topology" {
                        // no args
                    } else if t.name == "list_indexes" {
                        required.extend(["keyspace".into(), "table".into()]);
                        let mut ks = Map::new();
                        ks.insert("type".into(), Value::String("string".into()));
                        ks.insert("description".into(), Value::String("Keyspace name".into()));
                        props.insert("keyspace".into(), ks);
                        let mut tb = Map::new();
                        tb.insert("type".into(), Value::String("string".into()));
                        tb.insert("description".into(), Value::String("Table name".into()));
                        props.insert("table".into(), tb);
                    } else if matches!(
                        t.name,
                        "keyspace_replication" | "list_views" | "list_udts" | "list_functions" | "list_aggregates"
                    ) {
                        required.push("keyspace".into());
                        let mut ks = Map::new();
                        ks.insert("type".into(), Value::String("string".into()));
                        ks.insert("description".into(), Value::String("Keyspace name".into()));
                        props.insert("keyspace".into(), ks);
                    } else if t.name == "size_estimates" {
                        required.extend(["keyspace".into(), "table".into()]);
                        let mut ks = Map::new();
                        ks.insert("type".into(), Value::String("string".into()));
                        ks.insert("description".into(), Value::String("Keyspace name".into()));
                        props.insert("keyspace".into(), ks);
                        let mut tb = Map::new();
                        tb.insert("type".into(), Value::String("string".into()));
                        tb.insert("description".into(), Value::String("Table name".into()));
                        props.insert("table".into(), tb);
                    } else if t.name == "search_schema" {
                        required.push("pattern".into());
                        let mut pat = Map::new();
                        pat.insert("type".into(), Value::String("string".into()));
                        pat.insert(
                            "description".into(),
                            Value::String("Substring to search (case-insensitive)".into()),
                        );
                        props.insert("pattern".into(), pat);
                        let mut ks = Map::new();
                        ks.insert("type".into(), Value::String("string".into()));
                        ks.insert(
                            "description".into(),
                            Value::String("Optional keyspace to scope search".into()),
                        );
                        props.insert("keyspace".into(), ks);
                    }
                    Tool {
                        description: Some(t.description.to_string()),
                        input_schema: ToolInputSchema::new(required, Some(props)),
                        name: t.name.to_string(),
                    }
                })
                .collect();

            Ok(ListToolsResult {
                tools,
                meta: None,
                next_cursor: None,
            })
        }

        async fn handle_call_tool_request(
            &self,
            request: rust_mcp_schema::CallToolRequest,
            runtime: &dyn rust_mcp_sdk::MCPServer,
        ) -> std::result::Result<
            rust_mcp_schema::CallToolResult,
            rust_mcp_schema::schema_utils::CallToolError,
        > {
            // Ensure capability exists
            runtime
                .assert_server_request_capabilities(&"tools/call".to_string())
                .map_err(rust_mcp_schema::schema_utils::CallToolError::new)?;

            let name = request.params.name;
            match name.as_str() {
                "list_keyspaces" => {
                    let span = tracing::info_span!("tool", name = "list_keyspaces");
                    let _g = span.enter();
                    match crate::db::list_keyspaces_with(&self.session).await {
                        Ok(list) => {
                            let json = serde_json::to_string(&list).unwrap_or_else(|_| "[]".into());
                            Ok(rust_mcp_schema::CallToolResult::text_content(json, None))
                        }
                        Err(err) => {
                            let msg = format!("list_keyspaces failed: {}", err);
                            Ok(rust_mcp_schema::CallToolResult::text_content(msg, None))
                        }
                    }
                }
                "list_tables" => {
                    // extract keyspace from arguments
                    let ks = request.params.arguments.as_ref().and_then(|m| {
                        m.get("keyspace")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())
                    });
                    if ks.is_none() {
                        let msg = "missing required argument 'keyspace'".to_string();
                        return Ok(rust_mcp_schema::CallToolResult::text_content(msg, None));
                    }
                    let keyspace = ks.unwrap();
                    let span = tracing::info_span!("tool", name = "list_tables", %keyspace);
                    let _g = span.enter();
                    match crate::db::list_tables_with(&self.session, &keyspace).await {
                        Ok(list) => {
                            let json = serde_json::to_string(&list).unwrap_or_else(|_| "[]".into());
                            Ok(rust_mcp_schema::CallToolResult::text_content(json, None))
                        }
                        Err(err) => {
                            let msg = format!("list_tables failed: {}", err);
                            Ok(rust_mcp_schema::CallToolResult::text_content(msg, None))
                        }
                    }
                }
                "describe_table" => {
                    // extract keyspace and table from arguments
                    let args = request.params.arguments.as_ref();
                    let ks = args
                        .and_then(|m| m.get("keyspace").and_then(|v| v.as_str()))
                        .map(|s| s.to_string());
                    let tb = args
                        .and_then(|m| m.get("table").and_then(|v| v.as_str()))
                        .map(|s| s.to_string());
                    if ks.is_none() || tb.is_none() {
                        let msg =
                            "missing required arguments 'keyspace' and/or 'table'".to_string();
                        return Ok(rust_mcp_schema::CallToolResult::text_content(msg, None));
                    }
                    match self
                        .get_schema(&ks.clone().unwrap(), &tb.clone().unwrap())
                        .await
                    {
                        Ok(cols) => {
                            let json = serde_json::to_string(&cols).unwrap_or_else(|_| "{}".into());
                            Ok(rust_mcp_schema::CallToolResult::text_content(json, None))
                        }
                        Err(err) => {
                            let msg = format!("describe_table failed: {}", err);
                            Ok(rust_mcp_schema::CallToolResult::text_content(msg, None))
                        }
                    }
                }
                "sample_rows" => {
                    let args = request.params.arguments.as_ref();
                    let ks = args
                        .and_then(|m| m.get("keyspace").and_then(|v| v.as_str()))
                        .map(|s| s.to_string());
                    let tb = args
                        .and_then(|m| m.get("table").and_then(|v| v.as_str()))
                        .map(|s| s.to_string());
                    let lm = args.and_then(|m| m.get("limit").and_then(|v| v.as_u64()));
                    let filters = args
                        .and_then(|m| m.get("filters"))
                        .and_then(|v| v.as_object());
                    let Some((keyspace, table, limit_u64)) =
                        ks.zip(tb).zip(lm).map(|((a, b), c)| (a, b, c))
                    else {
                        let msg = "missing required arguments 'keyspace', 'table', or 'limit'"
                            .to_string();
                        return Ok(rust_mcp_schema::CallToolResult::text_content(msg, None));
                    };
                    // Validate filter keys exist
                    if let Some(f) = filters {
                        match self.get_schema(&keyspace, &table).await {
                            Ok(schema) => {
                                let available: std::collections::HashSet<String> = schema
                                    .columns
                                    .iter()
                                    .map(|c| c.column_name.clone())
                                    .collect();
                                for col in f.keys() {
                                    if !available.contains(col) {
                                        let msg = format!(
                                            "invalid filter column '{}'; not in table columns",
                                            col
                                        );
                                        return Ok(rust_mcp_schema::CallToolResult::text_content(
                                            msg, None,
                                        ));
                                    }
                                }
                            }
                            Err(err) => {
                                let msg = format!("schema fetch failed: {}", err);
                                return Ok(rust_mcp_schema::CallToolResult::text_content(
                                    msg, None,
                                ));
                            }
                        }
                    }
                    let limit = (limit_u64 as u32).clamp(1, 500);
                    let span = tracing::info_span!("tool", name = "sample_rows", %keyspace, %table, limit = limit as i64);
                    let _g = span.enter();
                    match crate::db::sample_rows_with(
                        &self.session,
                        &keyspace,
                        &table,
                        limit,
                        filters,
                    )
                    .await
                    {
                        Ok(rows) => {
                            let json = serde_json::to_string(&rows).unwrap_or_else(|_| "[]".into());
                            Ok(rust_mcp_schema::CallToolResult::text_content(json, None))
                        }
                        Err(err) => {
                            let msg = format!("sample_rows failed: {}", err);
                            Ok(rust_mcp_schema::CallToolResult::text_content(msg, None))
                        }
                    }
                }
                "select" => {
                    let args = request.params.arguments.as_ref();
                    let ks = args
                        .and_then(|m| m.get("keyspace").and_then(|v| v.as_str()))
                        .map(|s| s.to_string());
                    let tb = args
                        .and_then(|m| m.get("table").and_then(|v| v.as_str()))
                        .map(|s| s.to_string());
                    let cols =
                        args.and_then(|m| m.get("columns").and_then(|v| v.as_array()).cloned());
                    let lm = args.and_then(|m| m.get("limit").and_then(|v| v.as_u64()));
                    let filters = args
                        .and_then(|m| m.get("filters"))
                        .and_then(|v| v.as_object());
                    let order_by = args
                        .and_then(|m| m.get("order_by").or_else(|| m.get("orderBy")))
                        .and_then(|v| v.as_array().cloned());
                    if ks.is_none() || tb.is_none() || cols.is_none() || lm.is_none() {
                        let msg =
                            "missing required arguments 'keyspace', 'table', 'columns', or 'limit'"
                                .to_string();
                        return Ok(rust_mcp_schema::CallToolResult::text_content(msg, None));
                    }
                    let keyspace = ks.unwrap();
                    let table = tb.unwrap();
                    let columns: Vec<String> = cols
                        .unwrap()
                        .iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect();
                    let limit = (lm.unwrap() as u32).clamp(1, 500);
                    // Validate requested columns and filter keys exist
                    match self.get_schema(&keyspace, &table).await {
                        Ok(schema) => {
                            let available: std::collections::HashSet<String> = schema
                                .columns
                                .iter()
                                .map(|c| c.column_name.clone())
                                .collect();
                            for c in columns.iter() {
                                if !available.contains(c) {
                                    let msg = format!(
                                        "invalid column '{}' in select; not in table columns",
                                        c
                                    );
                                    return Ok(rust_mcp_schema::CallToolResult::text_content(
                                        msg, None,
                                    ));
                                }
                            }
                            if let Some(f) = &filters {
                                for col in f.keys() {
                                    if !available.contains(col) {
                                        let msg = format!(
                                            "invalid filter column '{}'; not in table columns",
                                            col
                                        );
                                        return Ok(rust_mcp_schema::CallToolResult::text_content(
                                            msg, None,
                                        ));
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            let msg = format!("schema fetch failed: {}", err);
                            return Ok(rust_mcp_schema::CallToolResult::text_content(msg, None));
                        }
                    }
                    let order_tuples: Option<Vec<(String, String)>> = order_by.map(|arr| {
                        arr.into_iter()
                            .filter_map(|item| item.as_object().cloned())
                            .filter_map(|m| {
                                let col = m.get("column").and_then(|v| v.as_str())?;
                                let dir =
                                    m.get("direction").and_then(|v| v.as_str()).unwrap_or("asc");
                                Some((col.to_string(), dir.to_string()))
                            })
                            .collect()
                    });
                    // Enforce order_by only on clustering keys
                    if let Some(ref ords) = order_tuples {
                        match self.get_schema(&keyspace, &table).await {
                            Ok(schema) => {
                                let allowed: std::collections::HashSet<String> =
                                    schema.clustering_keys.iter().cloned().collect();
                                for (col, _) in ords.iter() {
                                    if !allowed.contains(col) {
                                        let msg = format!(
                                            "invalid order_by column '{}'; only clustering keys are allowed: {:?}",
                                            col, schema.clustering_keys
                                        );
                                        return Ok(rust_mcp_schema::CallToolResult::text_content(
                                            msg, None,
                                        ));
                                    }
                                }
                            }
                            Err(err) => {
                                let msg = format!("schema fetch failed: {}", err);
                                return Ok(rust_mcp_schema::CallToolResult::text_content(
                                    msg, None,
                                ));
                            }
                        }
                    }
                    let span = tracing::info_span!("tool", name = "select", %keyspace, %table, limit = limit as i64);
                    let _g = span.enter();
                    match crate::db::select_columns_with(
                        &self.session,
                        &keyspace,
                        &table,
                        &columns,
                        limit,
                        filters,
                        order_tuples.as_ref(),
                    )
                    .await
                    {
                        Ok(rows) => {
                            let json = serde_json::to_string(&rows).unwrap_or_else(|_| "[]".into());
                            Ok(rust_mcp_schema::CallToolResult::text_content(json, None))
                        }
                        Err(err) => {
                            let msg = format!("select failed: {}", err);
                            Ok(rust_mcp_schema::CallToolResult::text_content(msg, None))
                        }
                    }
                }
                "paged_select" => {
                    let args = request.params.arguments.as_ref();
                    let ks = args
                        .and_then(|m| m.get("keyspace").and_then(|v| v.as_str()))
                        .map(|s| s.to_string());
                    let tb = args
                        .and_then(|m| m.get("table").and_then(|v| v.as_str()))
                        .map(|s| s.to_string());
                    let cols =
                        args.and_then(|m| m.get("columns").and_then(|v| v.as_array()).cloned());
                    let page_size = args.and_then(|m| m.get("page_size").and_then(|v| v.as_u64()));
                    let filters = args
                        .and_then(|m| m.get("filters"))
                        .and_then(|v| v.as_object());
                    let order_by = args
                        .and_then(|m| m.get("order_by").or_else(|| m.get("orderBy")))
                        .and_then(|v| v.as_array().cloned());
                    let cursor = args
                        .and_then(|m| m.get("cursor").and_then(|v| v.as_str()))
                        .map(|s| s.to_string());
                    if ks.is_none() || tb.is_none() || cols.is_none() || page_size.is_none() {
                        let msg = "missing required arguments 'keyspace', 'table', 'columns', or 'page_size'".to_string();
                        return Ok(rust_mcp_schema::CallToolResult::text_content(msg, None));
                    }
                    let keyspace = ks.unwrap();
                    let table = tb.unwrap();
                    let page_size = (page_size.unwrap() as i32).clamp(1, 500);
                    let columns: Vec<String> = cols
                        .unwrap()
                        .iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect();
                    // Validate columns/filters and enforce order_by on clustering keys
                    match self.get_schema(&keyspace, &table).await {
                        Ok(schema) => {
                            let available: std::collections::HashSet<String> = schema
                                .columns
                                .iter()
                                .map(|c| c.column_name.clone())
                                .collect();
                            for c in columns.iter() {
                                if !available.contains(c) {
                                    return Ok(rust_mcp_schema::CallToolResult::text_content(
                                        format!("invalid column '{}'", c),
                                        None,
                                    ));
                                }
                            }
                            if let Some(f) = &filters {
                                for col in f.keys() {
                                    if !available.contains(col) {
                                        return Ok(rust_mcp_schema::CallToolResult::text_content(
                                            format!("invalid filter column '{}'", col),
                                            None,
                                        ));
                                    }
                                }
                            }
                            if let Some(arr) = &order_by {
                                let allowed: std::collections::HashSet<String> =
                                    schema.clustering_keys.iter().cloned().collect();
                                for item in arr.iter().filter_map(|v| v.as_object()) {
                                    if let Some(col) = item.get("column").and_then(|v| v.as_str())
                                        && !allowed.contains(col)
                                    {
                                        return Ok(rust_mcp_schema::CallToolResult::text_content(
                                            format!("invalid order_by column '{}'", col),
                                            None,
                                        ));
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            return Ok(rust_mcp_schema::CallToolResult::text_content(
                                format!("schema fetch failed: {}", err),
                                None,
                            ));
                        }
                    }
                    let order_tuples: Option<Vec<(String, String)>> = order_by.map(|arr| {
                        arr.into_iter()
                            .filter_map(|item| item.as_object().cloned())
                            .filter_map(|m| {
                                let col = m.get("column").and_then(|v| v.as_str())?;
                                let dir =
                                    m.get("direction").and_then(|v| v.as_str()).unwrap_or("asc");
                                Some((col.to_string(), dir.to_string()))
                            })
                            .collect()
                    });
                    let span = tracing::info_span!("tool", name = "paged_select", %keyspace, %table, page_size);
                    let _g = span.enter();
                    match crate::db::paged_select_with(
                        &self.session,
                        &keyspace,
                        &table,
                        &columns,
                        page_size,
                        filters,
                        order_tuples.as_ref(),
                        cursor.as_deref(),
                    )
                    .await
                    {
                        Ok(obj) => {
                            let json = serde_json::to_string(&obj).unwrap_or_else(|_| "{}".into());
                            Ok(rust_mcp_schema::CallToolResult::text_content(json, None))
                        }
                        Err(err) => Ok(rust_mcp_schema::CallToolResult::text_content(
                            format!("paged_select failed: {}", err),
                            None,
                        )),
                    }
                }
                "partition_rows" => {
                    let args = request.params.arguments.as_ref();
                    let ks = args
                        .and_then(|m| m.get("keyspace").and_then(|v| v.as_str()))
                        .map(|s| s.to_string());
                    let tb = args
                        .and_then(|m| m.get("table").and_then(|v| v.as_str()))
                        .map(|s| s.to_string());
                    let part = args
                        .and_then(|m| m.get("partition"))
                        .and_then(|v| v.as_object());
                    let limit = args
                        .and_then(|m| m.get("limit").and_then(|v| v.as_u64()))
                        .map(|n| (n as u32).clamp(1, 500));
                    if ks.is_none() || tb.is_none() || part.is_none() || limit.is_none() {
                        let msg =
                            "missing required arguments 'keyspace','table','partition','limit'"
                                .to_string();
                        return Ok(rust_mcp_schema::CallToolResult::text_content(msg, None));
                    }
                    let keyspace = ks.unwrap();
                    let table = tb.unwrap();
                    let partition = part.unwrap();
                    let limit = limit.unwrap();
                    // Validate exact partition key set against schema (no extras, no missing)
                    match self.get_schema(&keyspace, &table).await {
                        Ok(schema) => {
                            let pk: std::collections::HashSet<String> =
                                schema.partition_keys.iter().cloned().collect();
                            let provided: std::collections::HashSet<String> =
                                partition.keys().cloned().collect();
                            if pk != provided {
                                let msg = format!(
                                    "partition keys mismatch: expected {:?}",
                                    schema.partition_keys
                                );
                                return Ok(rust_mcp_schema::CallToolResult::text_content(
                                    msg, None,
                                ));
                            }
                        }
                        Err(err) => {
                            return Ok(rust_mcp_schema::CallToolResult::text_content(
                                format!("schema fetch failed: {}", err),
                                None,
                            ));
                        }
                    }
                    let span = tracing::info_span!("tool", name = "partition_rows", %keyspace, %table, limit = limit as i64);
                    let _g = span.enter();
                    match crate::db::partition_rows_with(
                        &self.session,
                        &keyspace,
                        &table,
                        partition,
                        limit,
                    )
                    .await
                    {
                        Ok(rows) => {
                            let json = serde_json::to_string(&rows).unwrap_or_else(|_| "[]".into());
                            Ok(rust_mcp_schema::CallToolResult::text_content(json, None))
                        }
                        Err(err) => Ok(rust_mcp_schema::CallToolResult::text_content(
                            format!("partition_rows failed: {}", err),
                            None,
                        )),
                    }
                }
                "cluster_topology" => {
                    let span = tracing::info_span!("tool", name = "cluster_topology");
                    let _g = span.enter();
                    match crate::db::cluster_topology_with(&self.session).await {
                        Ok(nodes) => {
                            let json =
                                serde_json::to_string(&nodes).unwrap_or_else(|_| "[]".into());
                            Ok(rust_mcp_schema::CallToolResult::text_content(json, None))
                        }
                        Err(err) => {
                            let msg = format!("cluster_topology failed: {}", err);
                            Ok(rust_mcp_schema::CallToolResult::text_content(msg, None))
                        }
                    }
                }
                "list_indexes" => {
                    let args = request.params.arguments.as_ref();
                    let ks = args
                        .and_then(|m| m.get("keyspace").and_then(|v| v.as_str()))
                        .map(|s| s.to_string());
                    let tb = args
                        .and_then(|m| m.get("table").and_then(|v| v.as_str()))
                        .map(|s| s.to_string());
                    if ks.is_none() || tb.is_none() {
                        let msg =
                            "missing required arguments 'keyspace' and/or 'table'".to_string();
                        return Ok(rust_mcp_schema::CallToolResult::text_content(msg, None));
                    }
                    let keyspace = ks.unwrap();
                    let table = tb.unwrap();
                    let span =
                        tracing::info_span!("tool", name = "list_indexes", %keyspace, %table);
                    let _g = span.enter();
                    match crate::db::list_indexes_with(&self.session, &keyspace, &table).await {
                        Ok(list) => {
                            let json = serde_json::to_string(&list).unwrap_or_else(|_| "[]".into());
                            Ok(rust_mcp_schema::CallToolResult::text_content(json, None))
                        }
                        Err(err) => {
                            let msg = format!("list_indexes failed: {}", err);
                            Ok(rust_mcp_schema::CallToolResult::text_content(msg, None))
                        }
                    }
                }
                "keyspace_replication" => {
                    let args = request.params.arguments.as_ref();
                    let ks = args
                        .and_then(|m| m.get("keyspace").and_then(|v| v.as_str()))
                        .map(|s| s.to_string());
                    if ks.is_none() {
                        let msg = "missing required argument 'keyspace'".to_string();
                        return Ok(rust_mcp_schema::CallToolResult::text_content(msg, None));
                    }
                    let keyspace = ks.unwrap();
                    let span =
                        tracing::info_span!("tool", name = "keyspace_replication", %keyspace);
                    let _g = span.enter();
                    match crate::db::keyspace_replication_with(&self.session, &keyspace).await {
                        Ok(obj) => {
                            let json = serde_json::to_string(&obj).unwrap_or_else(|_| "{}".into());
                            Ok(rust_mcp_schema::CallToolResult::text_content(json, None))
                        }
                        Err(err) => {
                            let msg = format!("keyspace_replication failed: {}", err);
                            Ok(rust_mcp_schema::CallToolResult::text_content(msg, None))
                        }
                    }
                }
                "list_views" => {
                    let args = request.params.arguments.as_ref();
                    let ks = args
                        .and_then(|m| m.get("keyspace").and_then(|v| v.as_str()))
                        .map(|s| s.to_string());
                    if ks.is_none() {
                        let msg = "missing required argument 'keyspace'".to_string();
                        return Ok(rust_mcp_schema::CallToolResult::text_content(msg, None));
                    }
                    let keyspace = ks.unwrap();
                    let span = tracing::info_span!("tool", name = "list_views", %keyspace);
                    let _g = span.enter();
                    match crate::db::list_views_with(&self.session, &keyspace).await {
                        Ok(list) => {
                            let json = serde_json::to_string(&list).unwrap_or_else(|_| "[]".into());
                            Ok(rust_mcp_schema::CallToolResult::text_content(json, None))
                        }
                        Err(err) => Ok(rust_mcp_schema::CallToolResult::text_content(
                            format!("list_views failed: {}", err),
                            None,
                        )),
                    }
                }
                "list_udts" => {
                    let args = request.params.arguments.as_ref();
                    let ks = args
                        .and_then(|m| m.get("keyspace").and_then(|v| v.as_str()))
                        .map(|s| s.to_string());
                    if ks.is_none() {
                        let msg = "missing required argument 'keyspace'".to_string();
                        return Ok(rust_mcp_schema::CallToolResult::text_content(msg, None));
                    }
                    let keyspace = ks.unwrap();
                    let span = tracing::info_span!("tool", name = "list_udts", %keyspace);
                    let _g = span.enter();
                    match crate::db::list_udts_with(&self.session, &keyspace).await {
                        Ok(list) => {
                            let json = serde_json::to_string(&list).unwrap_or_else(|_| "[]".into());
                            Ok(rust_mcp_schema::CallToolResult::text_content(json, None))
                        }
                        Err(err) => Ok(rust_mcp_schema::CallToolResult::text_content(
                            format!("list_udts failed: {}", err),
                            None,
                        )),
                    }
                }
                "list_functions" => {
                    let args = request.params.arguments.as_ref();
                    let ks = args
                        .and_then(|m| m.get("keyspace").and_then(|v| v.as_str()))
                        .map(|s| s.to_string());
                    if ks.is_none() {
                        let msg = "missing required argument 'keyspace'".to_string();
                        return Ok(rust_mcp_schema::CallToolResult::text_content(msg, None));
                    }
                    let keyspace = ks.unwrap();
                    let span = tracing::info_span!("tool", name = "list_functions", %keyspace);
                    let _g = span.enter();
                    match crate::db::list_functions_with(&self.session, &keyspace).await {
                        Ok(list) => {
                            let json = serde_json::to_string(&list).unwrap_or_else(|_| "[]".into());
                            Ok(rust_mcp_schema::CallToolResult::text_content(json, None))
                        }
                        Err(err) => Ok(rust_mcp_schema::CallToolResult::text_content(
                            format!("list_functions failed: {}", err),
                            None,
                        )),
                    }
                }
                "list_aggregates" => {
                    let args = request.params.arguments.as_ref();
                    let ks = args
                        .and_then(|m| m.get("keyspace").and_then(|v| v.as_str()))
                        .map(|s| s.to_string());
                    if ks.is_none() {
                        let msg = "missing required argument 'keyspace'".to_string();
                        return Ok(rust_mcp_schema::CallToolResult::text_content(msg, None));
                    }
                    let keyspace = ks.unwrap();
                    let span = tracing::info_span!("tool", name = "list_aggregates", %keyspace);
                    let _g = span.enter();
                    match crate::db::list_aggregates_with(&self.session, &keyspace).await {
                        Ok(list) => {
                            let json = serde_json::to_string(&list).unwrap_or_else(|_| "[]".into());
                            Ok(rust_mcp_schema::CallToolResult::text_content(json, None))
                        }
                        Err(err) => Ok(rust_mcp_schema::CallToolResult::text_content(
                            format!("list_aggregates failed: {}", err),
                            None,
                        )),
                    }
                }
                "size_estimates" => {
                    let args = request.params.arguments.as_ref();
                    let ks = args
                        .and_then(|m| m.get("keyspace").and_then(|v| v.as_str()))
                        .map(|s| s.to_string());
                    let tb = args
                        .and_then(|m| m.get("table").and_then(|v| v.as_str()))
                        .map(|s| s.to_string());
                    if ks.is_none() || tb.is_none() {
                        let msg =
                            "missing required arguments 'keyspace' and/or 'table'".to_string();
                        return Ok(rust_mcp_schema::CallToolResult::text_content(msg, None));
                    }
                    let keyspace = ks.unwrap();
                    let table = tb.unwrap();
                    let span =
                        tracing::info_span!("tool", name = "size_estimates", %keyspace, %table);
                    let _g = span.enter();
                    match crate::db::size_estimates_with(&self.session, &keyspace, &table).await {
                        Ok(obj) => {
                            let json = serde_json::to_string(&obj).unwrap_or_else(|_| "[]".into());
                            Ok(rust_mcp_schema::CallToolResult::text_content(json, None))
                        }
                        Err(err) => Ok(rust_mcp_schema::CallToolResult::text_content(
                            format!("size_estimates failed: {}", err),
                            None,
                        )),
                    }
                }
                "search_schema" => {
                    let args = request.params.arguments.as_ref();
                    let pattern = args
                        .and_then(|m| m.get("pattern").and_then(|v| v.as_str()))
                        .map(|s| s.to_string());
                    let keyspace = args
                        .and_then(|m| m.get("keyspace").and_then(|v| v.as_str()))
                        .map(|s| s.to_string());
                    if pattern.is_none() {
                        let msg = "missing required argument 'pattern'".to_string();
                        return Ok(rust_mcp_schema::CallToolResult::text_content(msg, None));
                    }
                    let pat = pattern.unwrap();
                    let span = tracing::info_span!("tool", name = "search_schema", %pat, keyspace = keyspace.as_deref().unwrap_or("<all>"));
                    let _g = span.enter();
                    match crate::db::search_schema_with(&self.session, &pat, keyspace.as_deref())
                        .await
                    {
                        Ok(items) => {
                            let json =
                                serde_json::to_string(&items).unwrap_or_else(|_| "[]".into());
                            Ok(rust_mcp_schema::CallToolResult::text_content(json, None))
                        }
                        Err(err) => Ok(rust_mcp_schema::CallToolResult::text_content(
                            format!("search_schema failed: {}", err),
                            None,
                        )),
                    }
                }
                _ => {
                    let msg = format!("tool '{}' is not yet implemented (read-only phase)", name);
                    Ok(rust_mcp_schema::CallToolResult::text_content(msg, None))
                }
            }
        }
    }
}

#[cfg(feature = "mcp")]
pub mod db {
    use anyhow::Result;
    use base64::Engine;
    use scylla::SessionBuilder;
    use scylla::query::Query;
    use scylla::statement::{PagingState, PagingStateResponse};
    use scylla_cql::frame::response::result::CqlValue;
    use scylla_cql::frame::response::result::Row;
    use serde::Serialize;
    use serde_json::{Map, Value};
    use std::env;
    use tracing::info;

    pub async fn list_keyspaces() -> Result<Vec<String>> {
        let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
        info!(%uri, "connecting to scylla");
        let session = SessionBuilder::new().known_node(uri).build().await?;
        let result = session
            .query_unpaged("SELECT keyspace_name FROM system_schema.keyspaces", &[])
            .await?;
        let mut names = Vec::new();
        for row in result.rows_typed::<(String,)>()? {
            let (name,) = row?;
            names.push(name);
        }
        Ok(names)
    }

    pub async fn list_keyspaces_with(session: &scylla::Session) -> Result<Vec<String>> {
        let result = session
            .query_unpaged("SELECT keyspace_name FROM system_schema.keyspaces", &[])
            .await?;
        let mut names = Vec::new();
        for row in result.rows_typed::<(String,)>()? {
            let (name,) = row?;
            names.push(name);
        }
        Ok(names)
    }

    pub async fn list_tables(keyspace: &str) -> Result<Vec<String>> {
        let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
        info!(%uri, %keyspace, "listing tables");
        let session = SessionBuilder::new().known_node(uri).build().await?;
        let result = session
            .query_unpaged(
                "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?",
                (keyspace.to_string(),),
            )
            .await?;
        let mut names = Vec::new();
        for row in result.rows_typed::<(String,)>()? {
            let (name,) = row?;
            names.push(name);
        }
        Ok(names)
    }

    pub async fn list_tables_with(
        session: &scylla::Session,
        keyspace: &str,
    ) -> Result<Vec<String>> {
        let result = session
            .query_unpaged(
                "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?",
                (keyspace.to_string(),),
            )
            .await?;
        let mut names = Vec::new();
        for row in result.rows_typed::<(String,)>()? {
            let (name,) = row?;
            names.push(name);
        }
        Ok(names)
    }

    #[derive(Debug, Serialize, Clone)]
    pub struct ColumnMeta {
        pub column_name: String,
        pub kind: String,
        pub position: i32,
        pub r#type: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub clustering_order: Option<String>,
    }

    #[derive(Debug, Serialize, Clone)]
    pub struct DescribeTable {
        pub keyspace: String,
        pub table: String,
        pub partition_keys: Vec<String>,
        pub clustering_keys: Vec<String>,
        pub columns: Vec<ColumnMeta>,
    }

    pub async fn describe_table(keyspace: &str, table: &str) -> Result<DescribeTable> {
        let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
        info!(%uri, %keyspace, %table, "describe table");
        let session = SessionBuilder::new().known_node(uri).build().await?;
        describe_table_with(&session, keyspace, table).await
    }

    pub async fn describe_table_with(
        session: &scylla::Session,
        keyspace: &str,
        table: &str,
    ) -> Result<DescribeTable> {
        // Reuse logic by calling the existing function for now
        // (In a later pass, this can be deduplicated to avoid double sessions.)
        // For now, re-implement against provided session to avoid new connections.
        let result = session
            .query_unpaged(
                "SELECT column_name, kind, position, type, clustering_order FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?",
                (keyspace.to_string(), table.to_string()),
            )
            .await?;
        let specs = result.col_specs().to_owned();
        let mut idx_name = None;
        let mut idx_kind = None;
        let mut idx_pos = None;
        let mut idx_type = None;
        let mut idx_order = None;
        for (i, spec) in specs.iter().enumerate() {
            match spec.name.as_str() {
                "column_name" => idx_name = Some(i),
                "kind" => idx_kind = Some(i),
                "position" => idx_pos = Some(i),
                "type" => idx_type = Some(i),
                "clustering_order" => idx_order = Some(i),
                _ => {}
            }
        }
        let rows = result.rows_or_empty();
        let mut cols = Vec::new();
        for row in rows.iter() {
            let name = idx_name
                .and_then(|i| row.columns.get(i))
                .and_then(|o| o.as_ref())
                .and_then(|v| match v {
                    CqlValue::Ascii(s) | CqlValue::Text(s) => Some(s.clone()),
                    _ => None,
                })
                .unwrap_or_default();
            let kind = idx_kind
                .and_then(|i| row.columns.get(i))
                .and_then(|o| o.as_ref())
                .and_then(|v| match v {
                    CqlValue::Ascii(s) | CqlValue::Text(s) => Some(s.clone()),
                    _ => None,
                })
                .unwrap_or_default();
            let pos = idx_pos
                .and_then(|i| row.columns.get(i))
                .and_then(|o| o.as_ref())
                .and_then(|v| match v {
                    CqlValue::Int(i) => Some(*i),
                    _ => None,
                })
                .unwrap_or(0);
            let ty = idx_type
                .and_then(|i| row.columns.get(i))
                .and_then(|o| o.as_ref())
                .and_then(|v| match v {
                    CqlValue::Ascii(s) | CqlValue::Text(s) => Some(s.clone()),
                    _ => None,
                })
                .unwrap_or_default();
            let order = idx_order
                .and_then(|i| row.columns.get(i))
                .and_then(|o| o.as_ref())
                .and_then(|v| match v {
                    CqlValue::Ascii(s) | CqlValue::Text(s) => Some(s.clone()),
                    _ => None,
                });
            cols.push(ColumnMeta {
                column_name: name,
                kind,
                position: pos,
                r#type: ty,
                clustering_order: order,
            });
        }
        let mut part: Vec<(i32, String)> = cols
            .iter()
            .filter(|c| c.kind == "partition_key")
            .map(|c| (c.position, c.column_name.clone()))
            .collect();
        part.sort_by_key(|(pos, _)| *pos);
        let partition_keys: Vec<String> = part.into_iter().map(|(_, n)| n).collect();
        let mut clust: Vec<(i32, String)> = cols
            .iter()
            .filter(|c| c.kind == "clustering")
            .map(|c| (c.position, c.column_name.clone()))
            .collect();
        clust.sort_by_key(|(pos, _)| *pos);
        let clustering_keys: Vec<String> = clust.into_iter().map(|(_, n)| n).collect();
        Ok(DescribeTable {
            keyspace: keyspace.to_string(),
            table: table.to_string(),
            partition_keys,
            clustering_keys,
            columns: cols,
        })
    }

    fn cql_value_to_json(v: &CqlValue) -> Value {
        match v {
            CqlValue::Boolean(b) => Value::Bool(*b),
            CqlValue::Int(i) => Value::from(*i),
            CqlValue::BigInt(i) => Value::from(*i),
            CqlValue::Float(f) => Value::from(*f),
            CqlValue::Double(f) => Value::from(*f),
            CqlValue::Ascii(s) | CqlValue::Text(s) => Value::from(s.clone()),
            CqlValue::Uuid(u) => Value::from(u.to_string()),
            CqlValue::Timeuuid(u) => Value::from(u.to_string()),
            CqlValue::Blob(bytes) => {
                use base64::Engine;
                use base64::engine::general_purpose::STANDARD as B64;
                Value::from(B64.encode(bytes))
            }
            CqlValue::List(items) => {
                Value::Array(items.iter().map(cql_value_to_json).collect())
            }
            CqlValue::Set(items) => {
                Value::Array(items.iter().map(cql_value_to_json).collect())
            }
            CqlValue::Map(entries) => Value::Object(
                entries
                    .iter()
                    .map(|(k, v)| (cql_map_key_to_string(k), cql_value_to_json(v)))
                    .collect(),
            ),
            CqlValue::Tuple(values) => Value::Array(
                values
                    .iter()
                    .map(|opt| opt.as_ref().map(cql_value_to_json).unwrap_or(Value::Null))
                    .collect(),
            ),
            _ => Value::from(format!("{:?}", v)),
        }
    }

    fn cql_map_key_to_string(k: &CqlValue) -> String {
        match k {
            CqlValue::Ascii(s) | CqlValue::Text(s) => s.clone(),
            CqlValue::Uuid(u) => u.to_string(),
            CqlValue::Int(i) => i.to_string(),
            CqlValue::BigInt(i) => i.to_string(),
            _ => format!("{:?}", k),
        }
    }

    pub async fn sample_rows(
        keyspace: &str,
        table: &str,
        limit: u32,
        filters: Option<&Map<String, Value>>,
    ) -> Result<Vec<Map<String, Value>>> {
        let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
        info!(%uri, %keyspace, %table, %limit, "sample rows");
        let session = SessionBuilder::new().known_node(uri).build().await?;
        let (where_clause, bind_values) = build_filters_clause_prepared(filters)?;
        let cql = format!(
            "SELECT * FROM {}.{}{} LIMIT {}",
            keyspace, table, where_clause, limit
        );
        let prepared = session.prepare(cql).await?;
        let result = session.execute_unpaged(&prepared, &bind_values[..]).await?;
        let specs = result.col_specs().to_owned();
        let rows: Vec<Row> = result.rows()?;
        let mut out: Vec<Map<String, Value>> = Vec::with_capacity(rows.len());
        for row in rows.iter() {
            let mut m = Map::new();
            for (i, spec) in specs.iter().enumerate() {
                let name = spec.name.clone();
                let val = row.columns.get(i).and_then(|o| o.as_ref());
                let json = match val {
                    Some(c) => cql_value_to_json(c),
                    None => Value::Null,
                };
                m.insert(name, json);
            }
            out.push(m);
        }
        Ok(out)
    }

    pub async fn sample_rows_with(
        session: &scylla::Session,
        keyspace: &str,
        table: &str,
        limit: u32,
        filters: Option<&Map<String, Value>>,
    ) -> Result<Vec<Map<String, Value>>> {
        let (where_clause, bind_values) = build_filters_clause_prepared(filters)?;
        let cql = format!(
            "SELECT * FROM {}.{}{} LIMIT {}",
            keyspace, table, where_clause, limit
        );
        let prepared = session.prepare(cql).await?;
        let result = session.execute_unpaged(&prepared, &bind_values[..]).await?;
        let specs = result.col_specs().to_owned();
        let rows: Vec<Row> = result.rows()?;
        let mut out: Vec<Map<String, Value>> = Vec::with_capacity(rows.len());
        for row in rows.iter() {
            let mut m = Map::new();
            for (i, spec) in specs.iter().enumerate() {
                let name = spec.name.clone();
                let val = row.columns.get(i).and_then(|o| o.as_ref());
                let json = match val {
                    Some(c) => cql_value_to_json(c),
                    None => Value::Null,
                };
                m.insert(name, json);
            }
            out.push(m);
        }
        Ok(out)
    }

    fn sanitize_ident(ident: &str) -> bool {
        let bytes = ident.as_bytes();
        if bytes.is_empty() {
            return false;
        }
        let first = bytes[0];
        let is_letter = |c: u8| c.is_ascii_alphabetic() || c == b'_';
        let is_alnum = |c: u8| c.is_ascii_alphanumeric() || c == b'_';
        if !is_letter(first) {
            return false;
        }
        bytes.iter().all(|&c| is_alnum(c))
    }

    fn build_filters_clause_prepared(
        filters: Option<&Map<String, Value>>,
    ) -> Result<(String, Vec<CqlValue>)> {
        let mut clause = String::new();
        let mut values: Vec<CqlValue> = Vec::new();
        if let Some(map) = filters {
            let mut first = true;
            for (k, v) in map.iter() {
                if !sanitize_ident(k) {
                    anyhow::bail!("invalid column name in filters");
                }
                clause.push_str(if first { " WHERE " } else { " AND " });
                first = false;
                clause.push_str(k);
                clause.push_str(" = ?");
                let cv = match v {
                    Value::String(s) => CqlValue::Text(s.clone()),
                    Value::Bool(b) => CqlValue::Boolean(*b),
                    Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            CqlValue::BigInt(i)
                        } else if let Some(f) = n.as_f64() {
                            CqlValue::Double(f)
                        } else {
                            anyhow::bail!("unsupported numeric value")
                        }
                    }
                    _ => anyhow::bail!("unsupported filter value type"),
                };
                values.push(cv);
            }
        }
        Ok((clause, values))
    }

    pub async fn select_columns(
        keyspace: &str,
        table: &str,
        columns: &[String],
        limit: u32,
        filters: Option<&Map<String, Value>>,
        order_by: Option<&Vec<(String, String)>>,
    ) -> Result<Vec<Map<String, Value>>> {
        let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
        info!(%uri, %keyspace, %table, %limit, cols=?columns, "select columns");
        // sanitize column idents
        if columns.is_empty() {
            anyhow::bail!("columns must not be empty");
        }
        for c in columns {
            if !sanitize_ident(c) {
                anyhow::bail!("invalid column name");
            }
        }
        let col_list = columns.join(", ");
        let session = SessionBuilder::new().known_node(uri).build().await?;
        let (where_clause, bind_values) = build_filters_clause_prepared(filters)?;
        let order_clause = build_order_by_clause(order_by)?;
        let cql = format!(
            "SELECT {} FROM {}.{}{}{} LIMIT {}",
            col_list, keyspace, table, where_clause, order_clause, limit
        );
        let prepared = session.prepare(cql).await?;
        let result = session.execute_unpaged(&prepared, &bind_values[..]).await?;
        let specs = result.col_specs().to_owned();
        let rows: Vec<Row> = result.rows()?;
        let mut out: Vec<Map<String, Value>> = Vec::with_capacity(rows.len());
        for row in rows.iter() {
            let mut m = Map::new();
            for (i, spec) in specs.iter().enumerate() {
                let name = spec.name.clone();
                let val = row.columns.get(i).and_then(|o| o.as_ref());
                let json = match val {
                    Some(c) => cql_value_to_json(c),
                    None => Value::Null,
                };
                m.insert(name, json);
            }
            out.push(m);
        }
        Ok(out)
    }

    pub async fn select_columns_with(
        session: &scylla::Session,
        keyspace: &str,
        table: &str,
        columns: &[String],
        limit: u32,
        filters: Option<&Map<String, Value>>,
        order_by: Option<&Vec<(String, String)>>,
    ) -> Result<Vec<Map<String, Value>>> {
        if columns.is_empty() {
            anyhow::bail!("columns must not be empty");
        }
        for c in columns {
            if !sanitize_ident(c) {
                anyhow::bail!("invalid column name");
            }
        }
        let col_list = columns.join(", ");
        let (where_clause, bind_values) = build_filters_clause_prepared(filters)?;
        let order_clause = build_order_by_clause(order_by)?;
        let cql = format!(
            "SELECT {} FROM {}.{}{}{} LIMIT {}",
            col_list, keyspace, table, where_clause, order_clause, limit
        );
        let prepared = session.prepare(cql).await?;
        let result = session.execute_unpaged(&prepared, &bind_values[..]).await?;
        let specs = result.col_specs().to_owned();
        let rows: Vec<Row> = result.rows()?;
        let mut out: Vec<Map<String, Value>> = Vec::with_capacity(rows.len());
        for row in rows.iter() {
            let mut m = Map::new();
            for (i, spec) in specs.iter().enumerate() {
                let name = spec.name.clone();
                let val = row.columns.get(i).and_then(|o| o.as_ref());
                let json = match val {
                    Some(c) => cql_value_to_json(c),
                    None => Value::Null,
                };
                m.insert(name, json);
            }
            out.push(m);
        }
        Ok(out)
    }

    /// Fetch rows by full partition key using a strictly prepared statement.
    /// Requires the provided `partition` map to contain exactly the set of partition key columns.
    pub async fn partition_rows_with(
        session: &scylla::Session,
        keyspace: &str,
        table: &str,
        partition: &Map<String, Value>,
        limit: u32,
    ) -> Result<Vec<Map<String, Value>>> {
        // Describe table to determine partition key order
        let meta = crate::db::describe_table_with(session, keyspace, table).await?;
        let expected: std::collections::HashSet<String> =
            meta.partition_keys.iter().cloned().collect();
        let provided: std::collections::HashSet<String> = partition.keys().cloned().collect();
        if expected != provided {
            anyhow::bail!(
                "partition keys mismatch: expected {:?}",
                meta.partition_keys
            );
        }
        // Build WHERE clause in schema PK order and bind values accordingly
        let mut where_clause = String::from(" WHERE ");
        let mut first = true;
        let mut bind_values: Vec<CqlValue> = Vec::with_capacity(meta.partition_keys.len());
        for pk in meta.partition_keys.iter() {
            if !first {
                where_clause.push_str(" AND ");
            } else {
                first = false;
            }
            where_clause.push_str(pk);
            where_clause.push_str(" = ?");
            let v = partition.get(pk).expect("checked equality above");
            let cv = match v {
                Value::String(s) => CqlValue::Text(s.clone()),
                Value::Bool(b) => CqlValue::Boolean(*b),
                Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        CqlValue::BigInt(i)
                    } else if let Some(f) = n.as_f64() {
                        CqlValue::Double(f)
                    } else {
                        anyhow::bail!("unsupported numeric value")
                    }
                }
                _ => anyhow::bail!("unsupported partition value type"),
            };
            bind_values.push(cv);
        }
        let cql = format!(
            "SELECT * FROM {}.{}{} LIMIT {}",
            keyspace, table, where_clause, limit
        );
        let prepared = session.prepare(cql).await?;
        let result = session.execute_unpaged(&prepared, &bind_values[..]).await?;
        let specs = result.col_specs().to_owned();
        let rows: Vec<Row> = result.rows()?;
        let mut out: Vec<Map<String, Value>> = Vec::with_capacity(rows.len());
        for row in rows.iter() {
            let mut m = Map::new();
            for (i, spec) in specs.iter().enumerate() {
                let name = spec.name.clone();
                let val = row.columns.get(i).and_then(|o| o.as_ref());
                let json = match val {
                    Some(c) => cql_value_to_json(c),
                    None => Value::Null,
                };
                m.insert(name, json);
            }
            out.push(m);
        }
        Ok(out)
    }

    /// Wrapper that creates a session, then calls `partition_rows_with`.
    pub async fn partition_rows(
        keyspace: &str,
        table: &str,
        partition: &Map<String, Value>,
        limit: u32,
    ) -> Result<Vec<Map<String, Value>>> {
        let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
        info!(%uri, %keyspace, %table, limit = limit as i64, "partition rows");
        let session = SessionBuilder::new().known_node(uri).build().await?;
        partition_rows_with(&session, keyspace, table, partition, limit).await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn paged_select_with(
        session: &scylla::Session,
        keyspace: &str,
        table: &str,
        columns: &[String],
        page_size: i32,
        filters: Option<&Map<String, Value>>,
        order_by: Option<&Vec<(String, String)>>,
        cursor: Option<&str>,
    ) -> Result<Map<String, Value>> {
        if columns.is_empty() {
            anyhow::bail!("columns must not be empty");
        }
        for c in columns {
            if !sanitize_ident(c) {
                anyhow::bail!("invalid column name");
            }
        }
        let col_list = columns.join(", ");
        let (where_clause, bind_values) = build_filters_clause_prepared(filters)?;
        let order_clause = build_order_by_clause(order_by)?;
        let cql = format!(
            "SELECT {} FROM {}.{}{}{}",
            col_list, keyspace, table, where_clause, order_clause
        );
        let prepared = session
            .prepare(Query::new(cql).with_page_size(page_size))
            .await?;
        let paging_state = match cursor {
            Some(tok) => {
                let bytes = base64::engine::general_purpose::STANDARD.decode(tok.as_bytes())?;
                PagingState::new_from_raw_bytes(bytes)
            }
            None => PagingState::start(),
        };
        let (result, paging_resp) = session
            .execute_single_page(&prepared, &bind_values[..], paging_state)
            .await?;
        let specs = result.col_specs().to_owned();
        let rows: Vec<Row> = result.rows_or_empty();
        let mut items: Vec<Value> = Vec::with_capacity(rows.len());
        for row in rows.iter() {
            let mut m = Map::new();
            for (i, spec) in specs.iter().enumerate() {
                let name = spec.name.clone();
                let val = row.columns.get(i).and_then(|o| o.as_ref());
                let json = match val {
                    Some(c) => cql_value_to_json(c),
                    None => Value::Null,
                };
                m.insert(name, json);
            }
            items.push(Value::Object(m));
        }
        let next_cursor = match paging_resp {
            PagingStateResponse::HasMorePages { state } => state
                .as_bytes_slice()
                .map(|arc| {
                    let slice: &[u8] = arc.as_ref();
                    Value::String(base64::engine::general_purpose::STANDARD.encode(slice))
                })
                .unwrap_or(Value::Null),
            PagingStateResponse::NoMorePages => Value::Null,
        };
        let mut out = Map::new();
        out.insert("items".into(), Value::Array(items));
        out.insert("next_cursor".into(), next_cursor);
        Ok(out)
    }

    fn build_order_by_clause(order_by: Option<&Vec<(String, String)>>) -> Result<String> {
        if let Some(list) = order_by {
            if list.is_empty() {
                return Ok(String::new());
            }
            let mut clause = String::from(" ORDER BY ");
            for (i, (col, dir)) in list.iter().enumerate() {
                if !sanitize_ident(col) {
                    anyhow::bail!("invalid column name in order_by");
                }
                let d = match dir.to_ascii_lowercase().as_str() {
                    "asc" => "ASC",
                    "desc" => "DESC",
                    _ => "ASC",
                };
                if i > 0 {
                    clause.push_str(", ");
                }
                clause.push_str(col);
                clause.push(' ');
                clause.push_str(d);
            }
            Ok(clause)
        } else {
            Ok(String::new())
        }
    }

    pub async fn cluster_topology() -> Result<Vec<Map<String, Value>>> {
        let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
        info!(%uri, "cluster topology");
        let session = SessionBuilder::new().known_node(uri).build().await?;

        // Collect rows from system.local
        let mut out: Vec<Map<String, Value>> = Vec::new();
        let result_local = session
            .query_unpaged(
                "SELECT host_id, data_center, rack, rpc_address FROM system.local",
                &[],
            )
            .await?;
        let specs_local = result_local.col_specs().to_owned();
        let rows_local: Vec<Row> = result_local.rows_or_empty();
        for row in rows_local.iter() {
            let mut m = Map::new();
            for (i, spec) in specs_local.iter().enumerate() {
                let name = spec.name.clone();
                let val = row.columns.get(i).and_then(|o| o.as_ref());
                let json = match val {
                    Some(c) => cql_value_to_json(c),
                    None => Value::Null,
                };
                m.insert(name, json);
            }
            m.insert("source".into(), Value::from("local"));
            out.push(m);
        }

        // Collect rows from system.peers (best-effort, may be empty)
        let result_peers = session
            .query_unpaged(
                "SELECT host_id, data_center, rack, rpc_address FROM system.peers",
                &[],
            )
            .await?;
        let specs_peers = result_peers.col_specs().to_owned();
        let rows_peers: Vec<Row> = result_peers.rows_or_empty();
        for row in rows_peers.iter() {
            let mut m = Map::new();
            for (i, spec) in specs_peers.iter().enumerate() {
                let name = spec.name.clone();
                let val = row.columns.get(i).and_then(|o| o.as_ref());
                let json = match val {
                    Some(c) => cql_value_to_json(c),
                    None => Value::Null,
                };
                m.insert(name, json);
            }
            m.insert("source".into(), Value::from("peers"));
            out.push(m);
        }

        Ok(out)
    }

    pub async fn cluster_topology_with(
        session: &scylla::Session,
    ) -> Result<Vec<Map<String, Value>>> {
        let mut out: Vec<Map<String, Value>> = Vec::new();
        let result_local = session
            .query_unpaged(
                "SELECT host_id, data_center, rack, rpc_address FROM system.local",
                &[],
            )
            .await?;
        let specs_local = result_local.col_specs().to_owned();
        let rows_local: Vec<Row> = result_local.rows_or_empty();
        for row in rows_local.iter() {
            let mut m = Map::new();
            for (i, spec) in specs_local.iter().enumerate() {
                let name = spec.name.clone();
                let val = row.columns.get(i).and_then(|o| o.as_ref());
                let json = match val {
                    Some(c) => cql_value_to_json(c),
                    None => Value::Null,
                };
                m.insert(name, json);
            }
            m.insert("source".into(), Value::from("local"));
            out.push(m);
        }
        let result_peers = session
            .query_unpaged(
                "SELECT host_id, data_center, rack, rpc_address FROM system.peers",
                &[],
            )
            .await?;
        let specs_peers = result_peers.col_specs().to_owned();
        let rows_peers: Vec<Row> = result_peers.rows_or_empty();
        for row in rows_peers.iter() {
            let mut m = Map::new();
            for (i, spec) in specs_peers.iter().enumerate() {
                let name = spec.name.clone();
                let val = row.columns.get(i).and_then(|o| o.as_ref());
                let json = match val {
                    Some(c) => cql_value_to_json(c),
                    None => Value::Null,
                };
                m.insert(name, json);
            }
            m.insert("source".into(), Value::from("peers"));
            out.push(m);
        }
        Ok(out)
    }

    pub async fn list_indexes(keyspace: &str, table: &str) -> Result<Vec<String>> {
        let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
        info!(%uri, %keyspace, %table, "list indexes");
        let session = SessionBuilder::new().known_node(uri).build().await?;
        list_indexes_with(&session, keyspace, table).await
    }

    pub async fn list_indexes_with(
        session: &scylla::Session,
        keyspace: &str,
        table: &str,
    ) -> Result<Vec<String>> {
        let prepared = session
            .prepare("SELECT index_name FROM system_schema.indexes WHERE keyspace_name = ? AND table_name = ?")
            .await?;
        let result = session
            .execute_unpaged(&prepared, &(keyspace.to_string(), table.to_string()))
            .await?;
        let mut names = Vec::new();
        for row in result.rows_typed::<(String,)>()? {
            let (name,) = row?;
            names.push(name);
        }
        Ok(names)
    }

    pub async fn keyspace_replication(keyspace: &str) -> Result<Map<String, Value>> {
        let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
        info!(%uri, %keyspace, "keyspace replication");
        let session = SessionBuilder::new().known_node(uri).build().await?;
        keyspace_replication_with(&session, keyspace).await
    }

    pub async fn keyspace_replication_with(
        session: &scylla::Session,
        keyspace: &str,
    ) -> Result<Map<String, Value>> {
        let prepared = session
            .prepare("SELECT replication, durable_writes FROM system_schema.keyspaces WHERE keyspace_name = ?")
            .await?;
        let result = session
            .execute_unpaged(&prepared, &(keyspace.to_string(),))
            .await?;
        let rows = result.rows_or_empty();
        let mut out = Map::new();
        if let Some(row) = rows.first() {
            if let Some(Some(CqlValue::Map(entries))) = row.columns.first().map(|o| o.as_ref()) {
                let obj: Map<String, Value> = entries
                    .iter()
                    .map(|(k, v)| (cql_map_key_to_string(k), cql_value_to_json(v)))
                    .collect();
                out.insert("replication".into(), Value::Object(obj));
            }
            if let Some(Some(CqlValue::Boolean(b))) = row.columns.get(1).map(|o| o.as_ref()) {
                out.insert("durable_writes".into(), Value::Bool(*b));
            }
        }
        Ok(out)
    }

    pub async fn list_views_with(session: &scylla::Session, keyspace: &str) -> Result<Vec<String>> {
        let prepared = session
            .prepare("SELECT view_name FROM system_schema.views WHERE keyspace_name = ?")
            .await?;
        let result = session
            .execute_unpaged(&prepared, &(keyspace.to_string(),))
            .await?;
        let mut names = Vec::new();
        for row in result.rows_typed::<(String,)>()? {
            let (name,) = row?;
            names.push(name);
        }
        Ok(names)
    }

    pub async fn list_udts_with(session: &scylla::Session, keyspace: &str) -> Result<Vec<String>> {
        let prepared = session
            .prepare("SELECT type_name FROM system_schema.types WHERE keyspace_name = ?")
            .await?;
        let result = session
            .execute_unpaged(&prepared, &(keyspace.to_string(),))
            .await?;
        let mut names = Vec::new();
        for row in result.rows_typed::<(String,)>()? {
            let (name,) = row?;
            names.push(name);
        }
        Ok(names)
    }

    pub async fn list_functions_with(
        session: &scylla::Session,
        keyspace: &str,
    ) -> Result<Vec<Map<String, Value>>> {
        let prepared = session
            .prepare("SELECT function_name, argument_types, return_type FROM system_schema.functions WHERE keyspace_name = ?")
            .await?;
        let result = session
            .execute_unpaged(&prepared, &(keyspace.to_string(),))
            .await?;
        let specs = result.col_specs().to_owned();
        let rows = result.rows_or_empty();
        let mut out = Vec::with_capacity(rows.len());
        for row in rows.iter() {
            let mut m = Map::new();
            for (i, spec) in specs.iter().enumerate() {
                let name = spec.name.clone();
                let val = row.columns.get(i).and_then(|o| o.as_ref());
                let json = match val {
                    Some(c) => cql_value_to_json(c),
                    None => Value::Null,
                };
                m.insert(name, json);
            }
            out.push(m);
        }
        Ok(out)
    }

    pub async fn list_aggregates_with(
        session: &scylla::Session,
        keyspace: &str,
    ) -> Result<Vec<Map<String, Value>>> {
        let prepared = session
            .prepare("SELECT aggregate_name, argument_types, return_type FROM system_schema.aggregates WHERE keyspace_name = ?")
            .await?;
        let result = session
            .execute_unpaged(&prepared, &(keyspace.to_string(),))
            .await?;
        let specs = result.col_specs().to_owned();
        let rows = result.rows_or_empty();
        let mut out = Vec::with_capacity(rows.len());
        for row in rows.iter() {
            let mut m = Map::new();
            for (i, spec) in specs.iter().enumerate() {
                let name = spec.name.clone();
                let val = row.columns.get(i).and_then(|o| o.as_ref());
                let json = match val {
                    Some(c) => cql_value_to_json(c),
                    None => Value::Null,
                };
                m.insert(name, json);
            }
            out.push(m);
        }
        Ok(out)
    }

    pub async fn size_estimates_with(
        session: &scylla::Session,
        keyspace: &str,
        table: &str,
    ) -> Result<Vec<Map<String, Value>>> {
        let prepared = session
            .prepare("SELECT range_start, range_end, mean_partition_size, partitions_count FROM system.size_estimates WHERE keyspace_name = ? AND table_name = ?")
            .await?;
        let result = session
            .execute_unpaged(&prepared, &(keyspace.to_string(), table.to_string()))
            .await?;
        let specs = result.col_specs().to_owned();
        let rows = result.rows_or_empty();
        let mut out: Vec<Map<String, Value>> = Vec::with_capacity(rows.len());
        for row in rows.iter() {
            let mut m = Map::new();
            for (i, spec) in specs.iter().enumerate() {
                let name = spec.name.clone();
                let val = row.columns.get(i).and_then(|o| o.as_ref());
                let json = match val {
                    Some(c) => cql_value_to_json(c),
                    None => Value::Null,
                };
                m.insert(name, json);
            }
            out.push(m);
        }
        Ok(out)
    }

    pub async fn search_schema_with(
        session: &scylla::Session,
        pattern: &str,
        keyspace: Option<&str>,
    ) -> Result<Vec<Map<String, Value>>> {
        let pat_lower = pattern.to_lowercase();
        // Helper to check if s contains pattern (case-insensitive)
        let contains = |s: &str| s.to_lowercase().contains(&pat_lower);
        let mut out: Vec<Map<String, Value>> = Vec::new();

        // Tables
        let q_tables = match keyspace {
            Some(_) => {
                "SELECT keyspace_name, table_name FROM system_schema.tables WHERE keyspace_name = ?"
            }
            None => "SELECT keyspace_name, table_name FROM system_schema.tables",
        };
        let res_tables = match keyspace {
            Some(ks) => session.query_unpaged(q_tables, &(ks.to_string(),)).await?,
            None => session.query_unpaged(q_tables, &[]).await?,
        };
        for row in res_tables.rows_typed::<(String, String)>()? {
            let (ks, tb) = row?;
            if contains(&ks) || contains(&tb) {
                let mut m = Map::new();
                m.insert("kind".into(), Value::String("table".into()));
                m.insert("keyspace".into(), Value::String(ks));
                m.insert("name".into(), Value::String(tb));
                out.push(m);
            }
        }

        // Columns
        let q_cols = match keyspace {
            Some(_) => {
                "SELECT keyspace_name, table_name, column_name FROM system_schema.columns WHERE keyspace_name = ?"
            }
            None => "SELECT keyspace_name, table_name, column_name FROM system_schema.columns",
        };
        let res_cols = match keyspace {
            Some(ks) => session.query_unpaged(q_cols, &(ks.to_string(),)).await?,
            None => session.query_unpaged(q_cols, &[]).await?,
        };
        for row in res_cols.rows_typed::<(String, String, String)>()? {
            let (ks, tb, col) = row?;
            if contains(&col) {
                let mut m = Map::new();
                m.insert("kind".into(), Value::String("column".into()));
                m.insert("keyspace".into(), Value::String(ks));
                m.insert("table".into(), Value::String(tb));
                m.insert("name".into(), Value::String(col));
                out.push(m);
            }
        }

        // UDTs
        let q_types = match keyspace {
            Some(_) => {
                "SELECT keyspace_name, type_name FROM system_schema.types WHERE keyspace_name = ?"
            }
            None => "SELECT keyspace_name, type_name FROM system_schema.types",
        };
        let res_types = match keyspace {
            Some(ks) => session.query_unpaged(q_types, &(ks.to_string(),)).await?,
            None => session.query_unpaged(q_types, &[]).await?,
        };
        for row in res_types.rows_typed::<(String, String)>()? {
            let (ks, ty) = row?;
            if contains(&ty) {
                let mut m = Map::new();
                m.insert("kind".into(), Value::String("udt".into()));
                m.insert("keyspace".into(), Value::String(ks));
                m.insert("name".into(), Value::String(ty));
                out.push(m);
            }
        }

        // Views
        let q_views = match keyspace {
            Some(_) => {
                "SELECT keyspace_name, view_name FROM system_schema.views WHERE keyspace_name = ?"
            }
            None => "SELECT keyspace_name, view_name FROM system_schema.views",
        };
        let res_views = match keyspace {
            Some(ks) => session.query_unpaged(q_views, &(ks.to_string(),)).await?,
            None => session.query_unpaged(q_views, &[]).await?,
        };
        for row in res_views.rows_typed::<(String, String)>()? {
            let (ks, v) = row?;
            if contains(&v) {
                let mut m = Map::new();
                m.insert("kind".into(), Value::String("view".into()));
                m.insert("keyspace".into(), Value::String(ks));
                m.insert("name".into(), Value::String(v));
                out.push(m);
            }
        }

        // Functions
        let q_funcs = match keyspace {
            Some(_) => {
                "SELECT keyspace_name, function_name FROM system_schema.functions WHERE keyspace_name = ?"
            }
            None => "SELECT keyspace_name, function_name FROM system_schema.functions",
        };
        let res_funcs = match keyspace {
            Some(ks) => session.query_unpaged(q_funcs, &(ks.to_string(),)).await?,
            None => session.query_unpaged(q_funcs, &[]).await?,
        };
        for row in res_funcs.rows_typed::<(String, String)>()? {
            let (ks, f) = row?;
            if contains(&f) {
                let mut m = Map::new();
                m.insert("kind".into(), Value::String("function".into()));
                m.insert("keyspace".into(), Value::String(ks));
                m.insert("name".into(), Value::String(f));
                out.push(m);
            }
        }

        // Aggregates
        let q_aggs = match keyspace {
            Some(_) => {
                "SELECT keyspace_name, aggregate_name FROM system_schema.aggregates WHERE keyspace_name = ?"
            }
            None => "SELECT keyspace_name, aggregate_name FROM system_schema.aggregates",
        };
        let res_aggs = match keyspace {
            Some(ks) => session.query_unpaged(q_aggs, &(ks.to_string(),)).await?,
            None => session.query_unpaged(q_aggs, &[]).await?,
        };
        for row in res_aggs.rows_typed::<(String, String)>()? {
            let (ks, a) = row?;
            if contains(&a) {
                let mut m = Map::new();
                m.insert("kind".into(), Value::String("aggregate".into()));
                m.insert("keyspace".into(), Value::String(ks));
                m.insert("name".into(), Value::String(a));
                out.push(m);
            }
        }

        Ok(out)
    }

    // Non-session wrappers for integration tests
    pub async fn list_views(keyspace: &str) -> Result<Vec<String>> {
        let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
        let session = SessionBuilder::new().known_node(uri).build().await?;
        list_views_with(&session, keyspace).await
    }

    pub async fn list_udts(keyspace: &str) -> Result<Vec<String>> {
        let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
        let session = SessionBuilder::new().known_node(uri).build().await?;
        list_udts_with(&session, keyspace).await
    }
}
