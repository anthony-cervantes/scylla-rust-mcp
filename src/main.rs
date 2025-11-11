#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Default to Content-Length framing (RMCP) for compatibility with most MCP clients.
    // Set MCP_FRAMING=newline to use the legacy newline-delimited transport.
    match std::env::var("MCP_FRAMING").as_deref() {
        Ok("newline") => {
            scylla_rust_mcp::mcp::run_stdio_server().await?;
            Ok(())
        }
        _ => {
            scylla_rust_mcp::rmcp_server::run_stdio_server().await?;
            Ok(())
        }
    }
}
