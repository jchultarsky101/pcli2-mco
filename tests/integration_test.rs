use axum::body::Bytes;
use axum::{body::to_bytes, extract::State, http::StatusCode, response::IntoResponse};
use pcli2_mcp::{
    AppState,
    mcp::handle_mcp,
    pcli::{PCLI2_BIN_ENV, run_pcli2_command, run_pcli2_tenant_list, run_pcli2_version},
};
use serde_json::{Value, json};
use std::{
    fs,
    path::PathBuf,
    sync::OnceLock,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;

struct EnvVarGuard {
    key: &'static str,
    original: Option<String>,
}

impl EnvVarGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let original = std::env::var(key).ok();
        unsafe {
            std::env::set_var(key, value);
        }
        Self { key, original }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        unsafe {
            match &self.original {
                Some(value) => std::env::set_var(self.key, value),
                None => std::env::remove_var(self.key),
            }
        }
    }
}

fn test_env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn make_mock_pcli2() -> PathBuf {
    let mut dir = std::env::temp_dir();
    let pid = std::process::id();
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    dir.push(format!("pcli2-mcp-test-{}-{}", pid, ts));
    fs::create_dir_all(&dir).expect("create temp dir");
    let script_path = dir.join("pcli2");
    let script = r#"#!/bin/sh
if [ "$1" = "--version" ]; then
  echo "pcli2 9.9.9"
  exit 0
fi
if [ "$1" = "tenant" ] && [ "$2" = "list" ]; then
  echo "tenant list ok"
  exit 0
fi
echo "unknown args" >&2
exit 1
"#;
    fs::write(&script_path, script).expect("write mock pcli2");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&script_path).expect("metadata").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&script_path, perms).expect("set permissions");
    }
    script_path
}

#[tokio::test]
async fn mock_pcli2_version_and_tenant_list() {
    let _lock = test_env_lock().lock().await;
    let script_path = make_mock_pcli2();
    let _guard = EnvVarGuard::set(PCLI2_BIN_ENV, script_path.to_string_lossy().as_ref());

    let version = run_pcli2_version().await.expect("version");
    assert_eq!(version.trim(), "pcli2 9.9.9");

    let args = json!({
        "format": "json",
        "pretty": false,
        "headers": false
    });
    let list = run_pcli2_tenant_list(args).await.expect("tenant list");
    assert_eq!(list.trim(), "tenant list ok");
}

#[tokio::test]
async fn mock_pcli2_error_includes_label() {
    let _lock = test_env_lock().lock().await;
    let script_path = make_mock_pcli2();
    let _guard = EnvVarGuard::set(PCLI2_BIN_ENV, script_path.to_string_lossy().as_ref());

    let err = run_pcli2_command(vec!["oops".to_string()], "pcli2 oops")
        .await
        .expect_err("expected error");
    assert!(err.contains("pcli2 oops failed"));
}

#[tokio::test]
async fn jsonrpc_parse_error_returns_32700() {
    let state = AppState {
        server_name: "test".to_string(),
        server_version: "0.0.0".to_string(),
    };
    let response = handle_mcp(State(state), Bytes::from("{bad json"))
        .await
        .into_response();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let value: Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(value["error"]["code"], -32700);
}

#[tokio::test]
async fn jsonrpc_invalid_request_returns_32600() {
    let state = AppState {
        server_name: "test".to_string(),
        server_version: "0.0.0".to_string(),
    };
    let response = handle_mcp(State(state), Bytes::from(r#"{"jsonrpc":"2.0","id":1}"#))
        .await
        .into_response();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let value: Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(value["error"]["code"], -32600);
}

#[tokio::test]
async fn jsonrpc_notification_returns_no_content() {
    let state = AppState {
        server_name: "test".to_string(),
        server_version: "0.0.0".to_string(),
    };
    let response = handle_mcp(
        State(state),
        Bytes::from(r#"{"jsonrpc":"2.0","method":"tools/list"}"#),
    )
    .await
    .into_response();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn logging_on_tools_call_outputs_command() {
    let _lock = test_env_lock().lock().await;
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new("debug"))
        .with_writer(std::io::stdout)
        .try_init();

    let script_path = make_mock_pcli2();
    let _guard = EnvVarGuard::set(PCLI2_BIN_ENV, script_path.to_string_lossy().as_ref());

    let state = AppState {
        server_name: "mock".to_string(),
        server_version: "0.0.0".to_string(),
    };

    let request = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": "pcli2_tenant_list",
            "arguments": {}
        }
    });
    let response = handle_mcp(State(state), Bytes::from(request.to_string()))
        .await
        .into_response();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_initialize_method() {
    let state = AppState {
        server_name: "test".to_string(),
        server_version: "0.0.0".to_string(),
    };

    let request = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {}
    });
    let response = handle_mcp(State(state), Bytes::from(request.to_string()))
        .await
        .into_response();
    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let value: Value = serde_json::from_slice(&body).expect("json");

    assert_eq!(value["result"]["protocolVersion"], "2025-03-26");
    assert_eq!(value["result"]["serverInfo"]["name"], "test");
    assert_eq!(value["result"]["serverInfo"]["version"], "0.0.0");
}

#[tokio::test]
async fn test_tools_list_method() {
    let state = AppState {
        server_name: "test".to_string(),
        server_version: "0.0.0".to_string(),
    };

    let request = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/list",
        "params": {}
    });
    let response = handle_mcp(State(state), Bytes::from(request.to_string()))
        .await
        .into_response();
    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let value: Value = serde_json::from_slice(&body).expect("json");

    assert!(value["result"]["tools"].is_array());
    assert!(!value["result"]["tools"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_unknown_method_returns_error() {
    let state = AppState {
        server_name: "test".to_string(),
        server_version: "0.0.0".to_string(),
    };

    let request = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "unknown/method",
        "params": {}
    });
    let response = handle_mcp(State(state), Bytes::from(request.to_string()))
        .await
        .into_response();
    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let value: Value = serde_json::from_slice(&body).expect("json");

    assert_eq!(value["error"]["code"], -32601);
}

#[tokio::test]
async fn test_jsonrpc_wrong_version() {
    let state = AppState {
        server_name: "test".to_string(),
        server_version: "0.0.0".to_string(),
    };

    let request = json!({
        "jsonrpc": "1.0",
        "id": 1,
        "method": "tools/list",
        "params": {}
    });
    let response = handle_mcp(State(state), Bytes::from(request.to_string()))
        .await
        .into_response();
    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let value: Value = serde_json::from_slice(&body).expect("json");

    assert_eq!(value["error"]["code"], -32600);
}
