#[tokio::main]
async fn main() -> rust_mcp_sdk::error::SdkResult<()> {
    scylla_rust_mcp::mcp::run_stdio_server().await
}
