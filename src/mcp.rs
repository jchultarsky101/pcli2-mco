use crate::{
    AppState,
    cli::{ARG_CLIENT, ARG_HOST, ARG_PORT, CLIENT_CLAUDE, CLIENT_QWEN_AGENT, CLIENT_QWEN_CODE},
    pcli::*,
};
use anyhow::{Result, anyhow};
use axum::{
    body::Bytes,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json},
};
use clap::ArgMatches;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tracing::info;

pub const MCP_SERVER_ALIAS: &str = "pcli2";
pub const MCP_REMOTE_COMMAND: &str = "npx";
pub const MCP_REMOTE_PACKAGE: &str = "mcp-remote";

#[derive(Debug, Deserialize)]
pub struct RpcRequest {
    jsonrpc: Option<String>,
    id: Option<Value>,
    method: Option<String>,
    params: Option<Value>,
}

#[derive(Debug, Serialize)]
pub struct RpcResponse {
    jsonrpc: &'static str,
    id: Value,
    result: Value,
}

#[derive(Debug, Serialize)]
pub struct RpcErrorResponse {
    jsonrpc: &'static str,
    id: Value,
    error: RpcErrorBody,
}

#[derive(Debug, Serialize)]
pub struct RpcErrorBody {
    code: i64,
    message: String,
}

pub fn run_config(matches: &ArgMatches) -> Result<()> {
    let client = matches
        .get_one::<String>(ARG_CLIENT)
        .map(String::as_str)
        .unwrap_or(CLIENT_CLAUDE);
    let host = matches
        .get_one::<String>(ARG_HOST)
        .map(String::as_str)
        .unwrap_or("localhost");
    let port = *matches.get_one::<u16>(ARG_PORT).unwrap_or(&8080);

    let config = build_client_config(client, host, port)?;
    let output = serde_json::to_string_pretty(&config)?;
    println!("{}", output);
    Ok(())
}

fn build_client_config(client: &str, host: &str, port: u16) -> Result<Value> {
    let server_entry = json!({
        MCP_SERVER_ALIAS: {
            "command": MCP_REMOTE_COMMAND,
            "args": [
                "-y",
                MCP_REMOTE_PACKAGE,
                format!("http://{}:{}/mcp", host, port)
            ]
        }
    });

    let config = match client {
        CLIENT_CLAUDE | CLIENT_QWEN_CODE | CLIENT_QWEN_AGENT => {
            json!({ "mcpServers": server_entry })
        }
        _ => return Err(anyhow!("Unsupported client '{}'", client)),
    };

    Ok(config)
}

fn parse_rpc_request(value: Value) -> Result<RpcRequest, String> {
    let obj = value
        .as_object()
        .ok_or_else(|| "Invalid Request: expected a JSON object".to_string())?;
    let jsonrpc = match obj.get("jsonrpc") {
        Some(Value::String(value)) => Some(value.clone()),
        Some(_) => {
            return Err("Invalid Request: 'jsonrpc' must be a string".to_string());
        }
        None => None,
    };
    let id = obj.get("id").cloned();
    let method = match obj.get("method") {
        Some(Value::String(value)) => Some(value.clone()),
        Some(_) => return Err("Invalid Request: 'method' must be a string".to_string()),
        None => None,
    };
    let params = obj.get("params").cloned();
    Ok(RpcRequest {
        jsonrpc,
        id,
        method,
        params,
    })
}

pub async fn handle_mcp(State(state): State<AppState>, bytes: Bytes) -> impl IntoResponse {
    let value: Value = match serde_json::from_slice(&bytes) {
        Ok(value) => value,
        Err(_) => {
            return json_error(Value::Null, -32700, "Parse error: invalid JSON".to_string())
                .into_response();
        }
    };

    let request = match parse_rpc_request(value) {
        Ok(request) => request,
        Err(message) => {
            return json_error(Value::Null, -32600, message).into_response();
        }
    };

    let id = request.id.clone().unwrap_or(Value::Null);
    if let Some(version) = request.jsonrpc.as_deref()
        && version != "2.0"
    {
        return json_error(id, -32600, format!("Invalid jsonrpc version '{}'", version))
            .into_response();
    }
    let method = match request.method.as_deref() {
        Some(method) => method,
        None => {
            return json_error(id, -32600, "Invalid Request: missing 'method'".to_string())
                .into_response();
        }
    };
    if id.is_null() {
        return StatusCode::OK.into_response();
    }

    match method {
        "initialize" => {
            info!("🧩 initialize");
            let result = json!({
                "protocolVersion": "2025-03-26",
                "serverInfo": {
                    "name": state.server_name,
                    "version": state.server_version
                },
                "capabilities": {
                    "tools": {}
                }
            });
            json_ok(id, result).into_response()
        }
        "tools/list" => {
            info!("🔧 tools/list");
            let tools = tool_list();
            let result = json!({ "tools": tools });
            json_ok(id, result).into_response()
        }
        "tools/call" => {
            let params = request.params.unwrap_or_else(|| json!({}));
            let tool_name = params
                .get("name")
                .and_then(|value| value.as_str())
                .unwrap_or("unknown");
            info!("🔧 tools/call name={}", tool_name);
            match call_tool(params, state.thumbnail_cache.as_ref().as_ref()).await {
                Ok(result) => json_ok(id, result).into_response(),
                Err(message) => json_error(id, -32602, message).into_response(),
            }
        }
        _ => json_error(id, -32601, format!("Method '{}' not found", method)).into_response(),
    }
}
pub fn json_ok(id: Value, result: Value) -> Json<RpcResponse> {
    Json(RpcResponse {
        jsonrpc: "2.0",
        id,
        result,
    })
}

pub fn json_error(id: Value, code: i64, message: String) -> Json<RpcErrorResponse> {
    Json(RpcErrorResponse {
        jsonrpc: "2.0",
        id,
        error: RpcErrorBody { code, message },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_build_client_config_claude() {
        let config = build_client_config("claude", "localhost", 8080).unwrap();
        let expected = json!({
            "mcpServers": {
                "pcli2": {
                    "command": "npx",
                    "args": ["-y", "mcp-remote", "http://localhost:8080/mcp"]
                }
            }
        });
        assert_eq!(config, expected);
    }

    #[test]
    fn test_build_client_config_qwen_code() {
        let config = build_client_config("qwen-code", "localhost", 8080).unwrap();
        let expected = json!({
            "mcpServers": {
                "pcli2": {
                    "command": "npx",
                    "args": ["-y", "mcp-remote", "http://localhost:8080/mcp"]
                }
            }
        });
        assert_eq!(config, expected);
    }

    #[test]
    fn test_build_client_config_qwen_agent() {
        let config = build_client_config("qwen-agent", "localhost", 8080).unwrap();
        let expected = json!({
            "mcpServers": {
                "pcli2": {
                    "command": "npx",
                    "args": ["-y", "mcp-remote", "http://localhost:8080/mcp"]
                }
            }
        });
        assert_eq!(config, expected);
    }

    #[test]
    fn test_build_client_config_unsupported() {
        let result = build_client_config("unsupported-client", "localhost", 8080);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Unsupported client")
        );
    }

    #[test]
    fn test_parse_rpc_request_valid() {
        let json_input = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "test_method",
            "params": {"key": "value"}
        });

        let result = parse_rpc_request(json_input);
        assert!(result.is_ok());

        let request = result.unwrap();
        assert_eq!(request.jsonrpc, Some("2.0".to_string()));
        assert_eq!(request.id, Some(json!(1)));
        assert_eq!(request.method, Some("test_method".to_string()));
        assert_eq!(request.params, Some(json!({"key": "value"})));
    }

    #[test]
    fn test_parse_rpc_request_missing_method() {
        let json_input = json!({
            "jsonrpc": "2.0",
            "id": 1
        });

        let result = parse_rpc_request(json_input);
        assert!(result.is_ok());

        let request = result.unwrap();
        assert_eq!(request.method, None);
    }

    #[test]
    fn test_parse_rpc_request_invalid_json() {
        let json_input = json!([]);

        let result = parse_rpc_request(json_input);
        assert!(result.is_err());
    }

    #[test]
    fn test_json_ok() {
        let id = json!(1);
        let result = json!({"test": "value"});
        let response = json_ok(id, result);

        assert_eq!(response.jsonrpc, "2.0");
        assert_eq!(response.id, json!(1));
        assert_eq!(response.result, json!({"test": "value"}));
    }

    #[test]
    fn test_json_error() {
        let id = json!(1);
        let response = json_error(id, -32601, "Method not found".to_string());

        assert_eq!(response.jsonrpc, "2.0");
        assert_eq!(response.id, json!(1));
        assert_eq!(response.error.code, -32601);
        assert_eq!(response.error.message, "Method not found");
    }
}
