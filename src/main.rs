#[cfg(feature = "mcp")]
#[tokio::main]
async fn main() -> rust_mcp_sdk::error::SdkResult<()> {
    scylla_rust_mcp::mcp::run_stdio_server().await
}

#[cfg(not(feature = "mcp"))]
fn main() {
    println!("scylla-rust-mcp: build with --features mcp to run the MCP stdio server");
}
