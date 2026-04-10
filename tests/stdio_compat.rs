use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Command, Stdio};

fn encode_message(value: &Value) -> Vec<u8> {
    let body = serde_json::to_vec(value).expect("serialize request");
    let mut message = format!("Content-Length: {}\r\n\r\n", body.len()).into_bytes();
    message.extend(body);
    message
}

fn read_message(reader: &mut BufReader<impl Read>) -> Value {
    let mut content_length = None;
    loop {
        let mut line = String::new();
        let read = reader.read_line(&mut line).expect("read header line");
        assert!(read > 0, "unexpected EOF while reading headers");
        if line == "\r\n" {
            break;
        }
        let (name, value) = line.split_once(':').expect("header should contain a colon");
        if name.eq_ignore_ascii_case("content-length") {
            content_length = Some(
                value
                    .trim()
                    .parse::<usize>()
                    .expect("content length should be numeric"),
            );
        }
    }

    let len = content_length.expect("content-length header");
    let mut body = vec![0; len];
    reader.read_exact(&mut body).expect("read response body");
    serde_json::from_slice(&body).expect("parse response JSON")
}

#[test]
fn default_stdio_server_speaks_content_length_and_stays_quiet() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_scylla-rust-mcp"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn MCP server");

    let mut stdin = child.stdin.take().expect("stdin");
    let stdout = child.stdout.take().expect("stdout");
    let mut stdout = BufReader::new(stdout);
    let mut stderr = child.stderr.take().expect("stderr");

    stdin
        .write_all(&encode_message(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": { "name": "codex-test", "version": "0" }
            }
        })))
        .expect("send initialize");

    let initialize = read_message(&mut stdout);
    assert_eq!(initialize["jsonrpc"], "2.0");
    assert_eq!(initialize["id"], 1);
    assert_eq!(
        initialize["result"]["serverInfo"]["name"],
        "scylla-rust-mcp"
    );
    assert_eq!(initialize["result"]["protocolVersion"], "2024-11-05");
    assert_eq!(
        initialize["result"]["capabilities"]["tools"]["listChanged"],
        false
    );

    stdin
        .write_all(&encode_message(&json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
            "params": {}
        })))
        .expect("send initialized");

    stdin
        .write_all(&encode_message(&json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/list",
            "params": {}
        })))
        .expect("send tools/list");

    let tools_list = read_message(&mut stdout);
    assert_eq!(tools_list["id"], 2);
    let tools = tools_list["result"]["tools"]
        .as_array()
        .expect("tools array");
    assert!(!tools.is_empty(), "expected at least one tool");
    assert!(
        tools.iter().any(|tool| tool["name"] == "list_keyspaces"),
        "expected list_keyspaces in tools list"
    );

    stdin
        .write_all(&encode_message(&json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "shutdown",
            "params": {}
        })))
        .expect("send shutdown");

    let shutdown = read_message(&mut stdout);
    assert_eq!(shutdown["id"], 3);
    assert_eq!(shutdown["result"], json!({}));

    drop(stdin);

    let status = child.wait().expect("wait for server");
    assert!(status.success(), "server should exit cleanly");

    let mut stderr_output = String::new();
    stderr
        .read_to_string(&mut stderr_output)
        .expect("read stderr");
    assert_eq!(
        stderr_output, "",
        "default startup should be quiet on stderr"
    );
}
