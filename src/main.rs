#![recursion_limit = "256"]

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Default to Content-Length framing for Codex and other MCP clients.
    // Set MCP_FRAMING=newline to use the legacy newline-delimited transport.
    match std::env::var("MCP_FRAMING").as_deref() {
        Ok("newline") => {
            scylla_rust_mcp::rmcp_server::run_stdio_server().await?;
            Ok(())
        }
        _ => {
            scylla_rust_mcp::codex_stdio::run_stdio_server().await?;
            Ok(())
        }
    }
}
