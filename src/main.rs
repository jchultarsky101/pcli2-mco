use anyhow::{Result, anyhow};
use axum::{
    BoxError, Json, Router,
    body::Bytes,
    error_handling::HandleErrorLayer,
    extract::{DefaultBodyLimit, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use clap::{Arg, ArgMatches, Command, value_parser};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::env;
use std::fs;
use std::io::IsTerminal;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncRead, AsyncReadExt};
use tower::{ServiceBuilder, timeout::TimeoutLayer};
use tracing::{debug, info};
use tracing_subscriber::{EnvFilter, FmtSubscriber};

const APP_NAME: &str = env!("CARGO_PKG_NAME");
const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
const APP_ABOUT: &str = "A simple MCP server over HTTP";

const SERVER_NAME: &str = "mcp-http-server";

const CMD_SERVE: &str = "serve";
const CMD_CONFIG: &str = "config";
const CMD_HELP: &str = "help";

const ARG_PORT: &str = "port";
const ARG_CLIENT: &str = "client";
const ARG_COMMAND: &str = "command";
const ARG_HOST: &str = "host";
const ARG_LOG_LEVEL: &str = "log_level";

const DEFAULT_PORT_STR: &str = "8080";
const DEFAULT_HOST: &str = "localhost";
const DEFAULT_LOG_LEVEL: &str = "info";

const CLIENT_CLAUDE: &str = "claude";
const CLIENT_QWEN_CODE: &str = "qwen-code";
const CLIENT_QWEN_AGENT: &str = "qwen-agent";

const MCP_SERVER_ALIAS: &str = "pcli2";
const MCP_REMOTE_COMMAND: &str = "npx";
const MCP_REMOTE_PACKAGE: &str = "mcp-remote";

const MAX_REQUEST_BYTES: usize = 1_048_576;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const PCLI2_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const MAX_PCLI2_OUTPUT_BYTES: usize = 200 * 1024 * 1024;
const PCLI2_BIN_ENV: &str = "PCLI2_BIN";

#[derive(Clone)]
struct AppState {
    server_name: String,
    server_version: String,
}

#[derive(Debug, Deserialize)]
struct RpcRequest {
    jsonrpc: Option<String>,
    id: Option<Value>,
    method: Option<String>,
    params: Option<Value>,
}

#[derive(Debug, Serialize)]
struct RpcResponse {
    jsonrpc: &'static str,
    id: Value,
    result: Value,
}

#[derive(Debug, Serialize)]
struct RpcErrorResponse {
    jsonrpc: &'static str,
    id: Value,
    error: RpcErrorBody,
}

#[derive(Debug, Serialize)]
struct RpcErrorBody {
    code: i64,
    message: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let matches = build_cli().get_matches();
    let log_level = matches.subcommand().and_then(|(name, sub_matches)| {
        if name == CMD_SERVE {
            sub_matches
                .get_one::<String>(ARG_LOG_LEVEL)
                .map(|value| value.as_str())
        } else {
            None
        }
    });
    init_logging(log_level);

    match matches.subcommand() {
        Some((CMD_SERVE, sub_matches)) => run_server(sub_matches).await,
        Some((CMD_CONFIG, sub_matches)) => run_config(sub_matches),
        Some((CMD_HELP, sub_matches)) => run_help(sub_matches),
        _ => Ok(()),
    }
}

fn init_logging(level: Option<&str>) {
    if let Some(level) = level
        && std::env::var("RUST_LOG").is_err()
    {
        unsafe {
            std::env::set_var("RUST_LOG", level);
        }
    }
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug")),
        )
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}

fn build_cli() -> Command {
    Command::new(APP_NAME)
        .version(APP_VERSION)
        .about(APP_ABOUT)
        .arg_required_else_help(true)
        .subcommand_required(true)
        .disable_help_subcommand(true)
        .subcommand(serve_command())
        .subcommand(config_command())
        .subcommand(help_command())
}

fn serve_command() -> Command {
    Command::new(CMD_SERVE)
        .about("Run the MCP server")
        .arg(
            Arg::new(ARG_HOST)
                .long("host")
                .value_name("HOST")
                .default_value(DEFAULT_HOST)
                .help("Host to bind to (e.g. localhost or 0.0.0.0)"),
        )
        .arg(
            Arg::new(ARG_PORT)
                .short('p')
                .long("port")
                .value_name("PORT")
                .value_parser(value_parser!(u16))
                .default_value(DEFAULT_PORT_STR)
                .help("Port to listen on"),
        )
        .arg(
            Arg::new(ARG_LOG_LEVEL)
                .long("log-level")
                .value_name("LEVEL")
                .default_value(DEFAULT_LOG_LEVEL)
                .help("Logging level (e.g. trace, debug, info, warn, error)"),
        )
}

fn config_command() -> Command {
    Command::new(CMD_CONFIG)
        .about("Print JSON config for MCP clients")
        .arg(
            Arg::new(ARG_CLIENT)
                .long("client")
                .value_name("CLIENT")
                .value_parser([CLIENT_CLAUDE, CLIENT_QWEN_CODE, CLIENT_QWEN_AGENT])
                .default_value(CLIENT_CLAUDE)
                .help("Target client config to render"),
        )
        .arg(
            Arg::new(ARG_HOST)
                .long("host")
                .value_name("HOST")
                .default_value(DEFAULT_HOST)
                .help("Host for the MCP server URL"),
        )
        .arg(
            Arg::new(ARG_PORT)
                .short('p')
                .long("port")
                .value_name("PORT")
                .value_parser(value_parser!(u16))
                .default_value(DEFAULT_PORT_STR)
                .help("Port the local server will listen on"),
        )
}

fn help_command() -> Command {
    Command::new(CMD_HELP)
        .about("Print help for a command")
        .arg(
            Arg::new(ARG_COMMAND)
                .value_name("COMMAND")
                .required(false)
                .value_parser([CMD_SERVE, CMD_CONFIG, CMD_HELP])
                .help("Command to show help for"),
        )
}

async fn run_server(matches: &ArgMatches) -> Result<()> {
    let host = matches
        .get_one::<String>(ARG_HOST)
        .map(String::as_str)
        .unwrap_or(DEFAULT_HOST);
    let port = *matches
        .get_one::<u16>(ARG_PORT)
        .ok_or_else(|| anyhow!("missing port"))?;

    print_banner();

    let state = AppState {
        server_name: SERVER_NAME.to_string(),
        server_version: APP_VERSION.to_string(),
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/mcp", post(handle_mcp))
        .with_state(state)
        .layer(
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(|error: BoxError| async move {
                    if error.is::<tower::timeout::error::Elapsed>() {
                        (StatusCode::REQUEST_TIMEOUT, "Request timed out")
                    } else {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "Unhandled internal error",
                        )
                    }
                }))
                .layer(TimeoutLayer::new(REQUEST_TIMEOUT))
                .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES)),
        );

    let bind_addr = format!("{host}:{port}");
    info!("listening on http://{}", bind_addr);
    debug!("MCP server bound to {}", bind_addr);

    axum::serve(
        tokio::net::TcpListener::bind(&bind_addr).await?,
        app.into_make_service(),
    )
    .await?;

    Ok(())
}

fn run_config(matches: &ArgMatches) -> Result<()> {
    let client = matches
        .get_one::<String>(ARG_CLIENT)
        .map(String::as_str)
        .unwrap_or(CLIENT_CLAUDE);
    let host = matches
        .get_one::<String>(ARG_HOST)
        .map(String::as_str)
        .unwrap_or(DEFAULT_HOST);
    let port = *matches
        .get_one::<u16>(ARG_PORT)
        .ok_or_else(|| anyhow!("missing port"))?;

    let config = build_client_config(client, host, port)?;
    let output = serde_json::to_string_pretty(&config)?;
    println!("{}", output);
    Ok(())
}

fn run_help(matches: &ArgMatches) -> Result<()> {
    let target = matches.get_one::<String>(ARG_COMMAND).map(String::as_str);
    let mut cmd = build_cli();

    if let Some(name) = target {
        if let Some(sub) = cmd.get_subcommands().find(|sub| sub.get_name() == name) {
            let mut sub_cmd = sub.clone();
            sub_cmd.print_help()?;
            println!();
            return Ok(());
        }
        return Err(anyhow!("Unknown command '{}'", name));
    }

    cmd.print_help()?;
    println!();
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

async fn health() -> impl IntoResponse {
    (StatusCode::OK, "ok")
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

async fn handle_mcp(State(state): State<AppState>, bytes: Bytes) -> impl IntoResponse {
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
            match call_tool(params).await {
                Ok(result) => json_ok(id, result).into_response(),
                Err(message) => json_error(id, -32602, message).into_response(),
            }
        }
        _ => json_error(id, -32601, format!("Method '{}' not found", method)).into_response(),
    }
}

fn json_ok(id: Value, result: Value) -> Json<RpcResponse> {
    Json(RpcResponse {
        jsonrpc: "2.0",
        id,
        result,
    })
}

fn json_error(id: Value, code: i64, message: String) -> Json<RpcErrorResponse> {
    Json(RpcErrorResponse {
        jsonrpc: "2.0",
        id,
        error: RpcErrorBody { code, message },
    })
}

type Props = Map<String, Value>;

fn push_tool(
    tools: &mut Vec<Value>,
    name: &str,
    description: &str,
    properties: Props,
    required: &[&str],
) {
    tools.push(json!({
        "name": name,
        "description": description,
        "inputSchema": {
            "type": "object",
            "properties": properties,
            "required": required
        }
    }));
}

fn define_tool<F>(
    tools: &mut Vec<Value>,
    name: &str,
    description: &str,
    required: &[&str],
    build: F,
) where
    F: FnOnce(&mut Props),
{
    let mut props = Props::new();
    build(&mut props);
    push_tool(tools, name, description, props, required);
}

fn add_prop(props: &mut Props, key: &str, value: Value) {
    props.insert(key.to_string(), value);
}

fn add_tenant(props: &mut Props) {
    add_prop(
        props,
        "tenant",
        json!({ "type": "string", "description": "Tenant ID or alias." }),
    );
}

fn add_headers(props: &mut Props) {
    add_prop(
        props,
        "headers",
        json!({ "type": "boolean", "description": "Include headers in output." }),
    );
}

fn add_pretty(props: &mut Props) {
    add_prop(
        props,
        "pretty",
        json!({ "type": "boolean", "description": "Pretty output." }),
    );
}

fn add_metadata(props: &mut Props) {
    add_prop(
        props,
        "metadata",
        json!({ "type": "boolean", "description": "Include metadata in output." }),
    );
}

fn add_format(props: &mut Props, values: &[&str]) {
    add_prop(
        props,
        "format",
        json!({ "type": "string", "enum": values, "description": "Output format." }),
    );
}

fn add_uuid_path(props: &mut Props) {
    add_prop(
        props,
        "uuid",
        json!({ "type": "string", "description": "Resource UUID." }),
    );
    add_prop(
        props,
        "path",
        json!({ "type": "string", "description": "Resource path, e.g. /Root/Folder/Asset.stl." }),
    );
}

fn add_folder_uuid_path(props: &mut Props) {
    add_prop(
        props,
        "folder_uuid",
        json!({ "type": "string", "description": "Folder UUID." }),
    );
    add_prop(
        props,
        "folder_path",
        json!({ "type": "string", "description": "Folder path, e.g. /Root/Child/Grandchild." }),
    );
}

fn add_folder_path_list(props: &mut Props) {
    add_prop(
        props,
        "folder_path",
        json!({
            "oneOf": [
                { "type": "string" },
                { "type": "array", "items": { "type": "string" } }
            ],
            "description": "Folder path(s) to process."
        }),
    );
}

fn add_threshold(props: &mut Props) {
    add_prop(
        props,
        "threshold",
        json!({ "type": "number", "description": "Similarity threshold (0.00 to 100.00). Default 80.0." }),
    );
}

fn add_exclusive(props: &mut Props) {
    add_prop(
        props,
        "exclusive",
        json!({ "type": "boolean", "description": "Only show matches within the specified paths." }),
    );
}

fn add_progress(props: &mut Props) {
    add_prop(
        props,
        "progress",
        json!({ "type": "boolean", "description": "Display progress bar during processing." }),
    );
}

fn add_concurrent(props: &mut Props) {
    add_prop(
        props,
        "concurrent",
        json!({ "type": "integer", "description": "Maximum number of concurrent operations (1-10)." }),
    );
}

fn add_text(props: &mut Props) {
    add_prop(
        props,
        "text",
        json!({ "type": "string", "description": "Text query to search for in assets." }),
    );
}

fn add_fuzzy(props: &mut Props) {
    add_prop(
        props,
        "fuzzy",
        json!({ "type": "boolean", "description": "Perform fuzzy search instead of exact search." }),
    );
}

fn add_metadata_name_value(props: &mut Props) {
    add_prop(
        props,
        "name",
        json!({ "type": "string", "description": "Metadata property name." }),
    );
    add_prop(
        props,
        "value",
        json!({ "type": "string", "description": "Metadata property value." }),
    );
    add_prop(
        props,
        "type",
        json!({ "type": "string", "enum": ["text", "number", "boolean"], "description": "Metadata field type." }),
    );
}

fn add_metadata_name(props: &mut Props) {
    add_prop(
        props,
        "name",
        json!({
            "oneOf": [
                { "type": "string" },
                { "type": "array", "items": { "type": "string" } }
            ],
            "description": "Metadata property name. Can be a string, comma-separated string, or array."
        }),
    );
}

fn tool_list() -> Vec<Value> {
    debug!("building tool list");
    let mut tools = Vec::new();

    define_tool(
        &mut tools,
        "pcli2",
        "Physna Command Line Interface v2 (PCLI2). Runs `pcli2 folder list` or `pcli2 asset list` with the provided options.",
        &[],
        |props| {
            add_prop(
                props,
                "resource",
                json!({ "type": "string", "enum": ["folder", "asset"], "description": "Resource to list. Defaults to folder." }),
            );
            add_tenant(props);
            add_metadata(props);
            add_headers(props);
            add_pretty(props);
            add_format(props, &["json", "csv", "tree"]);
            add_prop(
                props,
                "folder_uuid",
                json!({ "type": "string", "description": "Folder UUID." }),
            );
            add_prop(
                props,
                "folder_path",
                json!({ "type": "string", "description": "Folder path, e.g. /Root/Child." }),
            );
            add_prop(
                props,
                "reload",
                json!({ "type": "boolean", "description": "Reload folder cache from server." }),
            );
        },
    );

    define_tool(
        &mut tools,
        "pcli2_tenant_list",
        "Runs `pcli2 tenant list`.",
        &[],
        |props| {
            add_headers(props);
            add_pretty(props);
            add_format(props, &["json", "csv"]);
        },
    );

    define_tool(
        &mut tools,
        "pcli2_version",
        "Runs `pcli2 --version`.",
        &[],
        |_| {},
    );

    define_tool(
        &mut tools,
        "pcli2_config_get",
        "Runs `pcli2 config get`.",
        &[],
        |props| {
            add_headers(props);
            add_pretty(props);
            add_format(props, &["json", "csv", "tree"]);
        },
    );

    define_tool(
        &mut tools,
        "pcli2_config_get_path",
        "Runs `pcli2 config get path`.",
        &[],
        |props| {
            add_format(props, &["json", "csv", "tree"]);
        },
    );

    define_tool(
        &mut tools,
        "pcli2_config_environment_list",
        "Runs `pcli2 config environment list`.",
        &[],
        |props| {
            add_headers(props);
            add_pretty(props);
            add_format(props, &["json", "csv"]);
        },
    );

    define_tool(
        &mut tools,
        "pcli2_config_environment_get",
        "Runs `pcli2 config environment get`.",
        &[],
        |props| {
            add_prop(
                props,
                "name",
                json!({ "type": "string", "description": "Environment name (defaults to active environment)." }),
            );
            add_headers(props);
            add_pretty(props);
            add_format(props, &["json", "csv"]);
        },
    );

    define_tool(
        &mut tools,
        "pcli2_tenant_get",
        "Runs `pcli2 tenant get` (current tenant).",
        &[],
        |props| {
            add_headers(props);
            add_pretty(props);
            add_format(props, &["json", "csv", "tree"]);
        },
    );

    define_tool(
        &mut tools,
        "pcli2_tenant_state",
        "Runs `pcli2 tenant state`.",
        &[],
        |props| {
            add_tenant(props);
            add_prop(
                props,
                "type",
                json!({
                    "type": "string",
                    "description": "Filter assets by state.",
                    "enum": [
                        "indexing",
                        "finished",
                        "failed",
                        "unsupported",
                        "no-3d-data",
                        "missing-dependencies"
                    ]
                }),
            );
            add_headers(props);
            add_pretty(props);
            add_format(props, &["json", "csv"]);
        },
    );

    define_tool(
        &mut tools,
        "pcli2_tenant_use",
        "Runs `pcli2 tenant use --name <tenantName>`.",
        &[],
        |props| {
            add_prop(
                props,
                "name",
                json!({ "type": "string", "description": "Tenant short name (as shown in tenant list)." }),
            );
            add_prop(
                props,
                "tenant_name",
                json!({ "type": "string", "description": "Tenant short name (alias for name)." }),
            );
            add_prop(
                props,
                "refresh",
                json!({ "type": "boolean", "description": "Force refresh cache data from API." }),
            );
            add_headers(props);
            add_pretty(props);
            add_format(props, &["json", "csv"]);
        },
    );

    define_tool(
        &mut tools,
        "pcli2_folder_get",
        "Runs `pcli2 folder get`.",
        &[],
        |props| {
            add_tenant(props);
            add_folder_uuid_path(props);
            add_metadata(props);
            add_headers(props);
            add_pretty(props);
            add_format(props, &["json", "csv", "tree"]);
        },
    );

    define_tool(
        &mut tools,
        "pcli2_folder_resolve",
        "Runs `pcli2 folder resolve`.",
        &["folder_path"],
        |props| {
            add_tenant(props);
            add_prop(
                props,
                "folder_path",
                json!({ "type": "string", "description": "Folder path, e.g. /Root/Child/Grandchild." }),
            );
        },
    );

    define_tool(
        &mut tools,
        "pcli2_folder_dependencies",
        "Runs `pcli2 folder dependencies`.",
        &["folder_path"],
        |props| {
            add_tenant(props);
            add_folder_path_list(props);
            add_headers(props);
            add_metadata(props);
            add_pretty(props);
            add_format(props, &["json", "csv", "tree"]);
            add_progress(props);
        },
    );

    define_tool(
        &mut tools,
        "pcli2_folder_geometric_match",
        "Runs `pcli2 folder geometric-match`.",
        &["folder_path"],
        |props| {
            add_tenant(props);
            add_folder_path_list(props);
            add_threshold(props);
            add_exclusive(props);
            add_headers(props);
            add_metadata(props);
            add_pretty(props);
            add_format(props, &["json", "csv"]);
            add_concurrent(props);
            add_progress(props);
        },
    );

    define_tool(
        &mut tools,
        "pcli2_folder_part_match",
        "Runs `pcli2 folder part-match`.",
        &["folder_path"],
        |props| {
            add_tenant(props);
            add_folder_path_list(props);
            add_threshold(props);
            add_exclusive(props);
            add_headers(props);
            add_metadata(props);
            add_pretty(props);
            add_format(props, &["json", "csv"]);
            add_concurrent(props);
            add_progress(props);
        },
    );

    define_tool(
        &mut tools,
        "pcli2_folder_visual_match",
        "Runs `pcli2 folder visual-match`.",
        &["folder_path"],
        |props| {
            add_tenant(props);
            add_folder_path_list(props);
            add_exclusive(props);
            add_headers(props);
            add_metadata(props);
            add_pretty(props);
            add_format(props, &["json", "csv"]);
            add_concurrent(props);
            add_progress(props);
        },
    );

    define_tool(
        &mut tools,
        "pcli2_asset_get",
        "Runs `pcli2 asset get`.",
        &[],
        |props| {
            add_tenant(props);
            add_uuid_path(props);
            add_headers(props);
            add_metadata(props);
            add_pretty(props);
            add_format(props, &["json", "csv"]);
        },
    );

    define_tool(
        &mut tools,
        "pcli2_asset_dependencies",
        "Runs `pcli2 asset dependencies`.",
        &[],
        |props| {
            add_tenant(props);
            add_uuid_path(props);
            add_metadata(props);
            add_headers(props);
            add_pretty(props);
            add_format(props, &["json", "csv", "tree"]);
        },
    );

    define_tool(
        &mut tools,
        "pcli2_asset_thumbnail",
        "Runs `pcli2 asset thumbnail` and returns a base64-encoded PNG.",
        &[],
        |props| {
            add_tenant(props);
            add_uuid_path(props);
        },
    );

    define_tool(
        &mut tools,
        "pcli2_asset_reprocess",
        "Runs `pcli2 asset reprocess`.",
        &[],
        |props| {
            add_tenant(props);
            add_uuid_path(props);
        },
    );

    define_tool(
        &mut tools,
        "pcli2_geometric_match",
        "Physna Command Line Interface v2 (PCLI2). Runs `pcli2 asset geometric-match` with the provided options.",
        &[],
        |props| {
            add_tenant(props);
            add_uuid_path(props);
            add_threshold(props);
            add_headers(props);
            add_metadata(props);
            add_pretty(props);
            add_format(props, &["json", "csv"]);
        },
    );

    define_tool(
        &mut tools,
        "pcli2_asset_part_match",
        "Runs `pcli2 asset part-match`.",
        &[],
        |props| {
            add_tenant(props);
            add_uuid_path(props);
            add_threshold(props);
            add_headers(props);
            add_metadata(props);
            add_pretty(props);
            add_format(props, &["json", "csv"]);
        },
    );

    define_tool(
        &mut tools,
        "pcli2_asset_visual_match",
        "Runs `pcli2 asset visual-match`.",
        &[],
        |props| {
            add_tenant(props);
            add_uuid_path(props);
            add_headers(props);
            add_metadata(props);
            add_pretty(props);
            add_format(props, &["json", "csv"]);
        },
    );

    define_tool(
        &mut tools,
        "pcli2_asset_text_match",
        "Runs `pcli2 asset text-match`.",
        &["text"],
        |props| {
            add_tenant(props);
            add_text(props);
            add_fuzzy(props);
            add_headers(props);
            add_metadata(props);
            add_pretty(props);
            add_format(props, &["json", "csv"]);
        },
    );

    define_tool(
        &mut tools,
        "pcli2_asset_metadata_create",
        "Runs `pcli2 asset metadata create`.",
        &["name", "value"],
        |props| {
            add_tenant(props);
            add_uuid_path(props);
            add_metadata_name_value(props);
        },
    );

    define_tool(
        &mut tools,
        "pcli2_asset_metadata_delete",
        "Runs `pcli2 asset metadata delete`.",
        &["name"],
        |props| {
            add_tenant(props);
            add_uuid_path(props);
            add_metadata_name(props);
            add_format(props, &["json", "csv"]);
        },
    );

    tools
}

async fn call_tool(params: Value) -> Result<Value, String> {
    debug!("call_tool params={}", params);
    let name = params
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Missing tool name".to_string())?;
    let args = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));

    match name {
        "pcli2" => {
            debug!("dispatching pcli2 list");
            let output = run_pcli2_list(args).await?;
            Ok(json!({
                "content": [{
                    "type": "text",
                    "text": output
                }]
            }))
        }
        "pcli2_tenant_list" => {
            run_simple_tool("pcli2 tenant list", run_pcli2_tenant_list(args).await)
        }
        "pcli2_version" => run_simple_tool("pcli2 --version", run_pcli2_version().await),
        "pcli2_config_get" => run_simple_tool("pcli2 config get", run_pcli2_config_get(args).await),
        "pcli2_config_get_path" => run_simple_tool(
            "pcli2 config get path",
            run_pcli2_config_get_path(args).await,
        ),
        "pcli2_config_environment_list" => run_simple_tool(
            "pcli2 config environment list",
            run_pcli2_config_environment_list(args).await,
        ),
        "pcli2_config_environment_get" => run_simple_tool(
            "pcli2 config environment get",
            run_pcli2_config_environment_get(args).await,
        ),
        "pcli2_tenant_get" => run_simple_tool("pcli2 tenant get", run_pcli2_tenant_get(args).await),
        "pcli2_tenant_state" => {
            run_simple_tool("pcli2 tenant state", run_pcli2_tenant_state(args).await)
        }
        "pcli2_tenant_use" => run_simple_tool("pcli2 tenant use", run_pcli2_tenant_use(args).await),
        "pcli2_folder_get" => run_simple_tool("pcli2 folder get", run_pcli2_folder_get(args).await),
        "pcli2_folder_resolve" => {
            run_simple_tool("pcli2 folder resolve", run_pcli2_folder_resolve(args).await)
        }
        "pcli2_folder_dependencies" => run_simple_tool(
            "pcli2 folder dependencies",
            run_pcli2_folder_dependencies(args).await,
        ),
        "pcli2_folder_geometric_match" => run_simple_tool(
            "pcli2 folder geometric-match",
            run_pcli2_folder_geometric_match(args).await,
        ),
        "pcli2_folder_part_match" => run_simple_tool(
            "pcli2 folder part-match",
            run_pcli2_folder_part_match(args).await,
        ),
        "pcli2_folder_visual_match" => run_simple_tool(
            "pcli2 folder visual-match",
            run_pcli2_folder_visual_match(args).await,
        ),
        "pcli2_asset_get" => run_simple_tool("pcli2 asset get", run_pcli2_asset_get(args).await),
        "pcli2_asset_dependencies" => run_simple_tool(
            "pcli2 asset dependencies",
            run_pcli2_asset_dependencies(args).await,
        ),
        "pcli2_asset_thumbnail" => {
            debug!("dispatching pcli2 asset thumbnail");
            let encoded = run_pcli2_asset_thumbnail(args).await?;
            let data_url = format!("data:image/png;base64,{}", encoded);
            Ok(json!({
                "content": [{
                    "type": "image",
                    "mimeType": "image/png",
                    "annotations": {
                        "audience": ["user"]
                    },
                    "data": encoded
                }, {
                    "type": "text",
                    "text": data_url
                }]
            }))
        }
        "pcli2_asset_reprocess" => run_simple_tool(
            "pcli2 asset reprocess",
            run_pcli2_asset_reprocess(args).await,
        ),
        "pcli2_geometric_match" => {
            debug!("dispatching pcli2 asset geometric-match");
            let output = run_pcli2_asset_geometric_match(args).await?;
            Ok(json!({
                "content": [{
                    "type": "text",
                    "text": output
                }]
            }))
        }
        "pcli2_asset_part_match" => run_simple_tool(
            "pcli2 asset part-match",
            run_pcli2_asset_part_match(args).await,
        ),
        "pcli2_asset_visual_match" => run_simple_tool(
            "pcli2 asset visual-match",
            run_pcli2_asset_visual_match(args).await,
        ),
        "pcli2_asset_text_match" => run_simple_tool(
            "pcli2 asset text-match",
            run_pcli2_asset_text_match(args).await,
        ),
        "pcli2_asset_metadata_create" => run_simple_tool(
            "pcli2 asset metadata create",
            run_pcli2_asset_metadata_create(args).await,
        ),
        "pcli2_asset_metadata_delete" => run_simple_tool(
            "pcli2 asset metadata delete",
            run_pcli2_asset_metadata_delete(args).await,
        ),
        _ => Err(format!("Unknown tool '{}'", name)),
    }
}

fn run_simple_tool(label: &str, result: Result<String, String>) -> Result<Value, String> {
    match result {
        Ok(output) => Ok(json!({
            "content": [{
                "type": "text",
                "text": output
            }]
        })),
        Err(message) => Err(format!("{} failed: {}", label, message)),
    }
}

async fn run_pcli2_list(args: Value) -> Result<String, String> {
    debug!("run_pcli2_list args={}", args);
    let resource = args
        .get("resource")
        .and_then(|v| v.as_str())
        .unwrap_or("folder");
    let mut cmd_args: Vec<String> = vec![resource.to_string(), "list".to_string()];

    if let Some(tenant) = args.get("tenant").and_then(|v| v.as_str()) {
        cmd_args.push("-t".to_string());
        cmd_args.push(tenant.to_string());
    }
    if args
        .get("metadata")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        cmd_args.push("--metadata".to_string());
    }
    if args
        .get("headers")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        cmd_args.push("--headers".to_string());
    }
    if args
        .get("pretty")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        cmd_args.push("--pretty".to_string());
    }
    if let Some(format) = args.get("format").and_then(|v| v.as_str()) {
        cmd_args.push("-f".to_string());
        cmd_args.push(format.to_string());
    }
    if let Some(folder_uuid) = args.get("folder_uuid").and_then(|v| v.as_str()) {
        cmd_args.push("--folder-uuid".to_string());
        cmd_args.push(folder_uuid.to_string());
    }
    if let Some(folder_path) = args.get("folder_path").and_then(|v| v.as_str()) {
        cmd_args.push("--folder-path".to_string());
        cmd_args.push(folder_path.to_string());
    }
    if args
        .get("reload")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        cmd_args.push("--reload".to_string());
    }

    run_pcli2_command(cmd_args, &format!("pcli2 {} list", resource)).await
}

async fn run_pcli2_asset_geometric_match(args: Value) -> Result<String, String> {
    debug!("run_pcli2_asset_geometric_match args={}", args);
    validate_range_f64(&args, "threshold", 0.0, 100.0)?;
    let mut cmd_args: Vec<String> = vec!["asset".to_string(), "geometric-match".to_string()];

    if let Some(tenant) = args.get("tenant").and_then(|v| v.as_str()) {
        cmd_args.push("-t".to_string());
        cmd_args.push(tenant.to_string());
    }

    let (uuid, path) = require_uuid_or_path(&args)?;
    push_opt_string(&mut cmd_args, "--uuid", uuid.as_deref());
    push_opt_string(&mut cmd_args, "--path", path.as_deref());
    push_opt_f64(&mut cmd_args, &args, "threshold", "--threshold");
    push_flag_if(&mut cmd_args, &args, "headers", "--headers");
    push_flag_if(&mut cmd_args, &args, "metadata", "--metadata");
    push_flag_if(&mut cmd_args, &args, "pretty", "--pretty");
    push_opt_string(
        &mut cmd_args,
        "-f",
        args.get("format").and_then(|v| v.as_str()),
    );

    run_pcli2_command(cmd_args, "pcli2 asset geometric-match").await
}

async fn run_pcli2_tenant_list(args: Value) -> Result<String, String> {
    debug!("run_pcli2_tenant_list args={}", args);
    let mut cmd_args: Vec<String> = vec!["tenant".to_string(), "list".to_string()];
    push_flag_if(&mut cmd_args, &args, "headers", "--headers");
    push_flag_if(&mut cmd_args, &args, "pretty", "--pretty");
    push_opt_string(
        &mut cmd_args,
        "-f",
        args.get("format").and_then(|v| v.as_str()),
    );
    run_pcli2_command(cmd_args, "pcli2 tenant list").await
}

async fn run_pcli2_version() -> Result<String, String> {
    debug!("run_pcli2_version");
    let cmd_args: Vec<String> = vec!["--version".to_string()];
    run_pcli2_command(cmd_args, "pcli2 --version").await
}

async fn run_pcli2_config_get(args: Value) -> Result<String, String> {
    debug!("run_pcli2_config_get args={}", args);
    let mut cmd_args: Vec<String> = vec!["config".to_string(), "get".to_string()];
    push_flag_if(&mut cmd_args, &args, "headers", "--headers");
    push_flag_if(&mut cmd_args, &args, "pretty", "--pretty");
    push_opt_string(
        &mut cmd_args,
        "-f",
        args.get("format").and_then(|v| v.as_str()),
    );
    run_pcli2_command(cmd_args, "pcli2 config get").await
}

async fn run_pcli2_config_get_path(args: Value) -> Result<String, String> {
    debug!("run_pcli2_config_get_path args={}", args);
    let mut cmd_args: Vec<String> =
        vec!["config".to_string(), "get".to_string(), "path".to_string()];
    push_opt_string(
        &mut cmd_args,
        "-f",
        args.get("format").and_then(|v| v.as_str()),
    );
    run_pcli2_command(cmd_args, "pcli2 config get path").await
}

async fn run_pcli2_config_environment_list(args: Value) -> Result<String, String> {
    debug!("run_pcli2_config_environment_list args={}", args);
    let mut cmd_args: Vec<String> = vec![
        "config".to_string(),
        "environment".to_string(),
        "list".to_string(),
    ];
    push_flag_if(&mut cmd_args, &args, "headers", "--headers");
    push_flag_if(&mut cmd_args, &args, "pretty", "--pretty");
    push_opt_string(
        &mut cmd_args,
        "-f",
        args.get("format").and_then(|v| v.as_str()),
    );
    run_pcli2_command(cmd_args, "pcli2 config environment list").await
}

async fn run_pcli2_config_environment_get(args: Value) -> Result<String, String> {
    debug!("run_pcli2_config_environment_get args={}", args);
    let mut cmd_args: Vec<String> = vec![
        "config".to_string(),
        "environment".to_string(),
        "get".to_string(),
    ];
    push_opt_string(
        &mut cmd_args,
        "-n",
        args.get("name").and_then(|v| v.as_str()),
    );
    push_flag_if(&mut cmd_args, &args, "headers", "--headers");
    push_flag_if(&mut cmd_args, &args, "pretty", "--pretty");
    push_opt_string(
        &mut cmd_args,
        "-f",
        args.get("format").and_then(|v| v.as_str()),
    );
    run_pcli2_command(cmd_args, "pcli2 config environment get").await
}

async fn run_pcli2_tenant_get(args: Value) -> Result<String, String> {
    debug!("run_pcli2_tenant_get args={}", args);
    let mut cmd_args: Vec<String> = vec!["tenant".to_string(), "get".to_string()];
    push_flag_if(&mut cmd_args, &args, "headers", "--headers");
    push_flag_if(&mut cmd_args, &args, "pretty", "--pretty");
    push_opt_string(
        &mut cmd_args,
        "-f",
        args.get("format").and_then(|v| v.as_str()),
    );
    run_pcli2_command(cmd_args, "pcli2 tenant get").await
}

async fn run_pcli2_tenant_state(args: Value) -> Result<String, String> {
    debug!("run_pcli2_tenant_state args={}", args);
    let mut cmd_args: Vec<String> = vec!["tenant".to_string(), "state".to_string()];
    if let Some(tenant) = args.get("tenant").and_then(|v| v.as_str()) {
        cmd_args.push("-t".to_string());
        cmd_args.push(tenant.to_string());
    }
    push_opt_string(
        &mut cmd_args,
        "--type",
        args.get("type").and_then(|v| v.as_str()),
    );
    push_flag_if(&mut cmd_args, &args, "headers", "--headers");
    push_flag_if(&mut cmd_args, &args, "pretty", "--pretty");
    push_opt_string(
        &mut cmd_args,
        "-f",
        args.get("format").and_then(|v| v.as_str()),
    );
    run_pcli2_command(cmd_args, "pcli2 tenant state").await
}

async fn run_pcli2_tenant_use(args: Value) -> Result<String, String> {
    debug!("run_pcli2_tenant_use args={}", args);
    let mut cmd_args: Vec<String> = vec!["tenant".to_string(), "use".to_string()];
    let name = args
        .get("tenant_name")
        .and_then(|v| v.as_str())
        .or_else(|| args.get("name").and_then(|v| v.as_str()))
        .ok_or_else(|| "Missing required argument: provide 'tenant_name' or 'name'".to_string())?;
    cmd_args.push("--name".to_string());
    cmd_args.push(name.to_string());
    push_flag_if(&mut cmd_args, &args, "refresh", "--refresh");
    push_flag_if(&mut cmd_args, &args, "headers", "--headers");
    push_flag_if(&mut cmd_args, &args, "pretty", "--pretty");
    push_opt_string(
        &mut cmd_args,
        "-f",
        args.get("format").and_then(|v| v.as_str()),
    );
    run_pcli2_command(cmd_args, "pcli2 tenant use").await
}

async fn run_pcli2_folder_get(args: Value) -> Result<String, String> {
    debug!("run_pcli2_folder_get args={}", args);
    let mut cmd_args: Vec<String> = vec!["folder".to_string(), "get".to_string()];
    if let Some(tenant) = args.get("tenant").and_then(|v| v.as_str()) {
        cmd_args.push("-t".to_string());
        cmd_args.push(tenant.to_string());
    }
    let (folder_uuid, folder_path) = require_folder_uuid_or_path(&args)?;
    push_opt_string(&mut cmd_args, "--folder-uuid", folder_uuid.as_deref());
    push_opt_string(&mut cmd_args, "--folder-path", folder_path.as_deref());
    push_flag_if(&mut cmd_args, &args, "metadata", "--metadata");
    push_flag_if(&mut cmd_args, &args, "headers", "--headers");
    push_flag_if(&mut cmd_args, &args, "pretty", "--pretty");
    push_opt_string(
        &mut cmd_args,
        "-f",
        args.get("format").and_then(|v| v.as_str()),
    );
    run_pcli2_command(cmd_args, "pcli2 folder get").await
}

async fn run_pcli2_folder_resolve(args: Value) -> Result<String, String> {
    debug!("run_pcli2_folder_resolve args={}", args);
    let mut cmd_args: Vec<String> = vec!["folder".to_string(), "resolve".to_string()];
    if let Some(tenant) = args.get("tenant").and_then(|v| v.as_str()) {
        cmd_args.push("-t".to_string());
        cmd_args.push(tenant.to_string());
    }
    let folder_path = args
        .get("folder_path")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Missing required argument: 'folder_path'".to_string())?;
    cmd_args.push("--folder-path".to_string());
    cmd_args.push(folder_path.to_string());
    run_pcli2_command(cmd_args, "pcli2 folder resolve").await
}

async fn run_pcli2_folder_dependencies(args: Value) -> Result<String, String> {
    debug!("run_pcli2_folder_dependencies args={}", args);
    let mut cmd_args: Vec<String> = vec!["folder".to_string(), "dependencies".to_string()];
    if let Some(tenant) = args.get("tenant").and_then(|v| v.as_str()) {
        cmd_args.push("-t".to_string());
        cmd_args.push(tenant.to_string());
    }
    let folder_paths = parse_string_list(&args, "folder_path");
    if folder_paths.is_empty() {
        return Err("Missing required argument: 'folder_path'".to_string());
    }
    for path in folder_paths {
        cmd_args.push("--folder-path".to_string());
        cmd_args.push(path);
    }
    push_flag_if(&mut cmd_args, &args, "headers", "--headers");
    push_flag_if(&mut cmd_args, &args, "metadata", "--metadata");
    push_flag_if(&mut cmd_args, &args, "pretty", "--pretty");
    push_opt_string(
        &mut cmd_args,
        "-f",
        args.get("format").and_then(|v| v.as_str()),
    );
    push_flag_if(&mut cmd_args, &args, "progress", "--progress");
    run_pcli2_command(cmd_args, "pcli2 folder dependencies").await
}

async fn run_pcli2_folder_geometric_match(args: Value) -> Result<String, String> {
    debug!("run_pcli2_folder_geometric_match args={}", args);
    validate_range_f64(&args, "threshold", 0.0, 100.0)?;
    validate_range_u64(&args, "concurrent", 1, 10)?;
    let mut cmd_args: Vec<String> = vec!["folder".to_string(), "geometric-match".to_string()];
    if let Some(tenant) = args.get("tenant").and_then(|v| v.as_str()) {
        cmd_args.push("-t".to_string());
        cmd_args.push(tenant.to_string());
    }
    let folder_paths = parse_string_list(&args, "folder_path");
    if folder_paths.is_empty() {
        return Err("Missing required argument: 'folder_path'".to_string());
    }
    for path in folder_paths {
        cmd_args.push("--folder-path".to_string());
        cmd_args.push(path);
    }
    push_opt_f64(&mut cmd_args, &args, "threshold", "--threshold");
    push_flag_if(&mut cmd_args, &args, "exclusive", "--exclusive");
    push_flag_if(&mut cmd_args, &args, "headers", "--headers");
    push_flag_if(&mut cmd_args, &args, "metadata", "--metadata");
    push_flag_if(&mut cmd_args, &args, "pretty", "--pretty");
    push_opt_string(
        &mut cmd_args,
        "-f",
        args.get("format").and_then(|v| v.as_str()),
    );
    push_opt_u64(&mut cmd_args, &args, "concurrent", "--concurrent");
    push_flag_if(&mut cmd_args, &args, "progress", "--progress");
    run_pcli2_command(cmd_args, "pcli2 folder geometric-match").await
}

async fn run_pcli2_folder_part_match(args: Value) -> Result<String, String> {
    debug!("run_pcli2_folder_part_match args={}", args);
    validate_range_f64(&args, "threshold", 0.0, 100.0)?;
    validate_range_u64(&args, "concurrent", 1, 10)?;
    let mut cmd_args: Vec<String> = vec!["folder".to_string(), "part-match".to_string()];
    if let Some(tenant) = args.get("tenant").and_then(|v| v.as_str()) {
        cmd_args.push("-t".to_string());
        cmd_args.push(tenant.to_string());
    }
    let folder_paths = parse_string_list(&args, "folder_path");
    if folder_paths.is_empty() {
        return Err("Missing required argument: 'folder_path'".to_string());
    }
    for path in folder_paths {
        cmd_args.push("--folder-path".to_string());
        cmd_args.push(path);
    }
    push_opt_f64(&mut cmd_args, &args, "threshold", "--threshold");
    push_flag_if(&mut cmd_args, &args, "exclusive", "--exclusive");
    push_flag_if(&mut cmd_args, &args, "headers", "--headers");
    push_flag_if(&mut cmd_args, &args, "metadata", "--metadata");
    push_flag_if(&mut cmd_args, &args, "pretty", "--pretty");
    push_opt_string(
        &mut cmd_args,
        "-f",
        args.get("format").and_then(|v| v.as_str()),
    );
    push_opt_u64(&mut cmd_args, &args, "concurrent", "--concurrent");
    push_flag_if(&mut cmd_args, &args, "progress", "--progress");
    run_pcli2_command(cmd_args, "pcli2 folder part-match").await
}

async fn run_pcli2_folder_visual_match(args: Value) -> Result<String, String> {
    debug!("run_pcli2_folder_visual_match args={}", args);
    validate_range_u64(&args, "concurrent", 1, 10)?;
    let mut cmd_args: Vec<String> = vec!["folder".to_string(), "visual-match".to_string()];
    if let Some(tenant) = args.get("tenant").and_then(|v| v.as_str()) {
        cmd_args.push("-t".to_string());
        cmd_args.push(tenant.to_string());
    }
    let folder_paths = parse_string_list(&args, "folder_path");
    if folder_paths.is_empty() {
        return Err("Missing required argument: 'folder_path'".to_string());
    }
    for path in folder_paths {
        cmd_args.push("--folder-path".to_string());
        cmd_args.push(path);
    }
    push_flag_if(&mut cmd_args, &args, "exclusive", "--exclusive");
    push_flag_if(&mut cmd_args, &args, "headers", "--headers");
    push_flag_if(&mut cmd_args, &args, "metadata", "--metadata");
    push_flag_if(&mut cmd_args, &args, "pretty", "--pretty");
    push_opt_string(
        &mut cmd_args,
        "-f",
        args.get("format").and_then(|v| v.as_str()),
    );
    push_opt_u64(&mut cmd_args, &args, "concurrent", "--concurrent");
    push_flag_if(&mut cmd_args, &args, "progress", "--progress");
    run_pcli2_command(cmd_args, "pcli2 folder visual-match").await
}

async fn run_pcli2_asset_get(args: Value) -> Result<String, String> {
    debug!("run_pcli2_asset_get args={}", args);
    let mut cmd_args: Vec<String> = vec!["asset".to_string(), "get".to_string()];
    if let Some(tenant) = args.get("tenant").and_then(|v| v.as_str()) {
        cmd_args.push("-t".to_string());
        cmd_args.push(tenant.to_string());
    }
    let (uuid, path) = require_uuid_or_path(&args)?;
    push_opt_string(&mut cmd_args, "--uuid", uuid.as_deref());
    push_opt_string(&mut cmd_args, "--path", path.as_deref());
    push_flag_if(&mut cmd_args, &args, "headers", "--headers");
    push_flag_if(&mut cmd_args, &args, "metadata", "--metadata");
    push_flag_if(&mut cmd_args, &args, "pretty", "--pretty");
    push_opt_string(
        &mut cmd_args,
        "-f",
        args.get("format").and_then(|v| v.as_str()),
    );
    run_pcli2_command(cmd_args, "pcli2 asset get").await
}

async fn run_pcli2_asset_dependencies(args: Value) -> Result<String, String> {
    debug!("run_pcli2_asset_dependencies args={}", args);
    let mut cmd_args: Vec<String> = vec!["asset".to_string(), "dependencies".to_string()];
    if let Some(tenant) = args.get("tenant").and_then(|v| v.as_str()) {
        cmd_args.push("-t".to_string());
        cmd_args.push(tenant.to_string());
    }
    let (uuid, path) = require_uuid_or_path(&args)?;
    push_opt_string(&mut cmd_args, "--uuid", uuid.as_deref());
    push_opt_string(&mut cmd_args, "--path", path.as_deref());
    push_flag_if(&mut cmd_args, &args, "metadata", "--metadata");
    push_flag_if(&mut cmd_args, &args, "headers", "--headers");
    push_flag_if(&mut cmd_args, &args, "pretty", "--pretty");
    push_opt_string(
        &mut cmd_args,
        "-f",
        args.get("format").and_then(|v| v.as_str()),
    );
    run_pcli2_command(cmd_args, "pcli2 asset dependencies").await
}

async fn run_pcli2_asset_thumbnail(args: Value) -> Result<String, String> {
    debug!("run_pcli2_asset_thumbnail args={}", args);
    let mut cmd_args: Vec<String> = vec!["asset".to_string(), "thumbnail".to_string()];
    if let Some(tenant) = args.get("tenant").and_then(|v| v.as_str()) {
        cmd_args.push("-t".to_string());
        cmd_args.push(tenant.to_string());
    }
    let (uuid, path) = require_uuid_or_path(&args)?;
    push_opt_string(&mut cmd_args, "--uuid", uuid.as_deref());
    push_opt_string(&mut cmd_args, "--path", path.as_deref());
    let temp_path = temp_thumbnail_path()?;
    let temp_path_str = temp_path
        .to_str()
        .ok_or_else(|| "Failed to build temporary thumbnail path".to_string())?;
    push_opt_string(&mut cmd_args, "--file", Some(temp_path_str));
    run_pcli2_command(cmd_args, "pcli2 asset thumbnail").await?;

    let bytes_result =
        fs::read(&temp_path).map_err(|err| format!("Failed to read thumbnail output: {}", err));
    let _ = fs::remove_file(&temp_path);
    let bytes = bytes_result?;
    if !bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        return Err("Thumbnail output was not a valid PNG file.".to_string());
    }
    let encoded = BASE64_STANDARD.encode(bytes);
    Ok(encoded)
}

async fn run_pcli2_asset_reprocess(args: Value) -> Result<String, String> {
    debug!("run_pcli2_asset_reprocess args={}", args);
    let mut cmd_args: Vec<String> = vec!["asset".to_string(), "reprocess".to_string()];
    if let Some(tenant) = args.get("tenant").and_then(|v| v.as_str()) {
        cmd_args.push("-t".to_string());
        cmd_args.push(tenant.to_string());
    }
    let (uuid, path) = require_uuid_or_path(&args)?;
    push_opt_string(&mut cmd_args, "--uuid", uuid.as_deref());
    push_opt_string(&mut cmd_args, "--path", path.as_deref());
    run_pcli2_command(cmd_args, "pcli2 asset reprocess").await
}

async fn run_pcli2_asset_part_match(args: Value) -> Result<String, String> {
    debug!("run_pcli2_asset_part_match args={}", args);
    validate_range_f64(&args, "threshold", 0.0, 100.0)?;
    let mut cmd_args: Vec<String> = vec!["asset".to_string(), "part-match".to_string()];
    if let Some(tenant) = args.get("tenant").and_then(|v| v.as_str()) {
        cmd_args.push("-t".to_string());
        cmd_args.push(tenant.to_string());
    }
    let (uuid, path) = require_uuid_or_path(&args)?;
    push_opt_string(&mut cmd_args, "--uuid", uuid.as_deref());
    push_opt_string(&mut cmd_args, "--path", path.as_deref());
    push_opt_f64(&mut cmd_args, &args, "threshold", "--threshold");
    push_flag_if(&mut cmd_args, &args, "headers", "--headers");
    push_flag_if(&mut cmd_args, &args, "metadata", "--metadata");
    push_flag_if(&mut cmd_args, &args, "pretty", "--pretty");
    push_opt_string(
        &mut cmd_args,
        "-f",
        args.get("format").and_then(|v| v.as_str()),
    );
    run_pcli2_command(cmd_args, "pcli2 asset part-match").await
}

async fn run_pcli2_asset_visual_match(args: Value) -> Result<String, String> {
    debug!("run_pcli2_asset_visual_match args={}", args);
    let mut cmd_args: Vec<String> = vec!["asset".to_string(), "visual-match".to_string()];
    if let Some(tenant) = args.get("tenant").and_then(|v| v.as_str()) {
        cmd_args.push("-t".to_string());
        cmd_args.push(tenant.to_string());
    }
    let (uuid, path) = require_uuid_or_path(&args)?;
    push_opt_string(&mut cmd_args, "--uuid", uuid.as_deref());
    push_opt_string(&mut cmd_args, "--path", path.as_deref());
    push_flag_if(&mut cmd_args, &args, "headers", "--headers");
    push_flag_if(&mut cmd_args, &args, "metadata", "--metadata");
    push_flag_if(&mut cmd_args, &args, "pretty", "--pretty");
    push_opt_string(
        &mut cmd_args,
        "-f",
        args.get("format").and_then(|v| v.as_str()),
    );
    run_pcli2_command(cmd_args, "pcli2 asset visual-match").await
}

async fn run_pcli2_asset_text_match(args: Value) -> Result<String, String> {
    debug!("run_pcli2_asset_text_match args={}", args);
    let mut cmd_args: Vec<String> = vec!["asset".to_string(), "text-match".to_string()];
    if let Some(tenant) = args.get("tenant").and_then(|v| v.as_str()) {
        cmd_args.push("-t".to_string());
        cmd_args.push(tenant.to_string());
    }
    let text = args
        .get("text")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Missing required argument: 'text'".to_string())?;
    cmd_args.push("--text".to_string());
    cmd_args.push(text.to_string());
    push_flag_if(&mut cmd_args, &args, "fuzzy", "--fuzzy");
    push_flag_if(&mut cmd_args, &args, "headers", "--headers");
    push_flag_if(&mut cmd_args, &args, "metadata", "--metadata");
    push_flag_if(&mut cmd_args, &args, "pretty", "--pretty");
    push_opt_string(
        &mut cmd_args,
        "-f",
        args.get("format").and_then(|v| v.as_str()),
    );
    run_pcli2_command(cmd_args, "pcli2 asset text-match").await
}

async fn run_pcli2_asset_metadata_create(args: Value) -> Result<String, String> {
    debug!("run_pcli2_asset_metadata_create args={}", args);
    let mut cmd_args: Vec<String> = vec![
        "asset".to_string(),
        "metadata".to_string(),
        "create".to_string(),
    ];
    if let Some(tenant) = args.get("tenant").and_then(|v| v.as_str()) {
        cmd_args.push("-t".to_string());
        cmd_args.push(tenant.to_string());
    }
    let (uuid, path) = require_uuid_or_path(&args)?;
    push_opt_string(&mut cmd_args, "--uuid", uuid.as_deref());
    push_opt_string(&mut cmd_args, "--path", path.as_deref());

    let name = args
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Missing required argument: 'name'".to_string())?;
    let value = args
        .get("value")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Missing required argument: 'value'".to_string())?;
    cmd_args.push("--name".to_string());
    cmd_args.push(name.to_string());
    cmd_args.push("--value".to_string());
    cmd_args.push(value.to_string());
    push_opt_string(
        &mut cmd_args,
        "--type",
        args.get("type").and_then(|v| v.as_str()),
    );
    run_pcli2_command(cmd_args, "pcli2 asset metadata create").await
}

async fn run_pcli2_asset_metadata_delete(args: Value) -> Result<String, String> {
    debug!("run_pcli2_asset_metadata_delete args={}", args);
    let mut cmd_args: Vec<String> = vec![
        "asset".to_string(),
        "metadata".to_string(),
        "delete".to_string(),
    ];
    if let Some(tenant) = args.get("tenant").and_then(|v| v.as_str()) {
        cmd_args.push("-t".to_string());
        cmd_args.push(tenant.to_string());
    }
    let (uuid, path) = require_uuid_or_path(&args)?;
    push_opt_string(&mut cmd_args, "--uuid", uuid.as_deref());
    push_opt_string(&mut cmd_args, "--path", path.as_deref());
    let names: Vec<String> = match args.get("name") {
        Some(Value::Array(values)) => values
            .iter()
            .filter_map(|value| value.as_str())
            .flat_map(|value| value.split(','))
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .collect(),
        Some(Value::String(value)) => value
            .split(',')
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .collect(),
        _ => Vec::new(),
    };
    if names.is_empty() {
        return Err("Missing required argument: 'name'".to_string());
    }
    for name in names {
        cmd_args.push("--name".to_string());
        cmd_args.push(name);
    }
    push_opt_string(
        &mut cmd_args,
        "-f",
        args.get("format").and_then(|v| v.as_str()),
    );
    run_pcli2_command(cmd_args, "pcli2 asset metadata delete").await
}

fn parse_string_list(args: &Value, key: &str) -> Vec<String> {
    match args.get(key) {
        Some(Value::Array(values)) => values
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect(),
        Some(Value::String(value)) => vec![value.to_string()],
        _ => Vec::new(),
    }
}

fn temp_thumbnail_path() -> Result<PathBuf, String> {
    let mut path = env::temp_dir();
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| format!("Failed to read system time: {}", err))?
        .as_millis();
    let pid = std::process::id();
    path.push(format!("pcli2-thumbnail-{}-{}.png", pid, timestamp));
    Ok(path)
}

fn require_uuid_or_path(args: &Value) -> Result<(Option<String>, Option<String>), String> {
    let uuid = args
        .get("uuid")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let path = args
        .get("path")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    if uuid.is_none() && path.is_none() {
        return Err("Missing required argument: provide either 'uuid' or 'path'".to_string());
    }
    Ok((uuid, path))
}

fn require_folder_uuid_or_path(args: &Value) -> Result<(Option<String>, Option<String>), String> {
    let uuid = args
        .get("folder_uuid")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let path = args
        .get("folder_path")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    if uuid.is_none() && path.is_none() {
        return Err(
            "Missing required argument: provide either 'folder_uuid' or 'folder_path'".to_string(),
        );
    }
    Ok((uuid, path))
}

fn validate_range_f64(args: &Value, key: &str, min: f64, max: f64) -> Result<(), String> {
    if let Some(value) = args.get(key).and_then(|v| v.as_f64())
        && (value < min || value > max)
    {
        return Err(format!(
            "Invalid argument '{}': value {} must be between {} and {}",
            key, value, min, max
        ));
    }
    Ok(())
}

fn validate_range_u64(args: &Value, key: &str, min: u64, max: u64) -> Result<(), String> {
    if let Some(value) = args.get(key).and_then(|v| v.as_u64())
        && (value < min || value > max)
    {
        return Err(format!(
            "Invalid argument '{}': value {} must be between {} and {}",
            key, value, min, max
        ));
    }
    Ok(())
}

fn push_flag_if(cmd_args: &mut Vec<String>, args: &Value, key: &str, flag: &str) {
    if args.get(key).and_then(|v| v.as_bool()).unwrap_or(false) {
        cmd_args.push(flag.to_string());
    }
}

fn push_opt_string(cmd_args: &mut Vec<String>, flag: &str, value: Option<&str>) {
    if let Some(value) = value {
        cmd_args.push(flag.to_string());
        cmd_args.push(value.to_string());
    }
}

fn push_opt_f64(cmd_args: &mut Vec<String>, args: &Value, key: &str, flag: &str) {
    if let Some(value) = args.get(key).and_then(|v| v.as_f64()) {
        cmd_args.push(flag.to_string());
        cmd_args.push(value.to_string());
    }
}

fn push_opt_u64(cmd_args: &mut Vec<String>, args: &Value, key: &str, flag: &str) {
    if let Some(value) = args.get(key).and_then(|v| v.as_u64()) {
        cmd_args.push(flag.to_string());
        cmd_args.push(value.to_string());
    }
}

fn shell_escape_arg(arg: &str) -> String {
    let safe = arg
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | '/' | ':' | '='));
    if safe && !arg.is_empty() {
        return arg.to_string();
    }

    if arg.is_empty() {
        return "''".to_string();
    }

    let mut escaped = String::from("'");
    for ch in arg.chars() {
        if ch == '\'' {
            escaped.push_str("'\"'\"'");
        } else {
            escaped.push(ch);
        }
    }
    escaped.push('\'');
    escaped
}

async fn read_limited<R: AsyncRead + Unpin>(
    mut reader: R,
    limit: usize,
    label: &str,
) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let read = reader
            .read(&mut chunk)
            .await
            .map_err(|err| format!("Failed to read pcli2 {}: {}", label, err))?;
        if read == 0 {
            break;
        }
        if buf.len() + read > limit {
            return Err(format!(
                "pcli2 {} exceeded maximum output size of {} bytes",
                label, limit
            ));
        }
        buf.extend_from_slice(&chunk[..read]);
    }
    Ok(buf)
}

async fn run_pcli2_command(cmd_args: Vec<String>, label: &str) -> Result<String, String> {
    let rendered = cmd_args
        .iter()
        .map(|arg| shell_escape_arg(arg))
        .collect::<Vec<_>>()
        .join(" ");
    info!("▶ pcli2 {}", rendered);
    let mut child = tokio::process::Command::new(pcli2_executable())
        .args(&cmd_args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("Failed to execute pcli2: {}", e))?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "Failed to capture pcli2 stdout".to_string())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "Failed to capture pcli2 stderr".to_string())?;

    let stdout_task = tokio::spawn(read_limited(stdout, MAX_PCLI2_OUTPUT_BYTES, "stdout"));
    let stderr_task = tokio::spawn(read_limited(stderr, MAX_PCLI2_OUTPUT_BYTES, "stderr"));

    let output = tokio::time::timeout(PCLI2_TIMEOUT, async {
        let status = child
            .wait()
            .await
            .map_err(|err| format!("Failed waiting for pcli2: {}", err))?;
        let stdout = stdout_task
            .await
            .map_err(|err| format!("Failed to read pcli2 stdout: {}", err))??;
        let stderr = stderr_task
            .await
            .map_err(|err| format!("Failed to read pcli2 stderr: {}", err))??;
        Ok((status, stdout, stderr))
    })
    .await;

    let (status, stdout, stderr) = match output {
        Ok(Ok(output)) => output,
        Ok(Err(message)) => {
            let _ = child.kill().await;
            return Err(message);
        }
        Err(_) => {
            let _ = child.kill().await;
            return Err(format!(
                "{} failed: timed out after {:?}",
                label, PCLI2_TIMEOUT
            ));
        }
    };

    let stdout = String::from_utf8_lossy(&stdout);
    let stderr = String::from_utf8_lossy(&stderr);

    if status.success() {
        Ok(stdout.trim_end().to_string())
    } else {
        Err(format!(
            "{} failed (code {}):\n{}\n{}",
            label,
            status,
            stdout.trim_end(),
            stderr.trim_end()
        ))
    }
}

fn pcli2_executable() -> String {
    env::var(PCLI2_BIN_ENV).unwrap_or_else(|_| "pcli2".to_string())
}

fn print_banner() {
    let ascii = [
        "██████╗  ██████╗██╗     ██╗██████╗     ███╗   ███╗ ██████╗██████╗ ",
        "██╔══██╗██╔════╝██║     ██║╚════██╗    ████╗ ████║██╔════╝██╔══██╗",
        "██████╔╝██║     ██║     ██║ █████╔╝    ██╔████╔██║██║     ██████╔╝",
        "██╔═══╝ ██║     ██║     ██║██╔═══╝     ██║╚██╔╝██║██║     ██╔═══╝ ",
        "██║     ╚██████╗███████╗██║███████╗    ██║ ╚═╝ ██║╚██████╗██║     ",
        "╚═╝      ╚═════╝╚══════╝╚═╝╚══════╝    ╚═╝     ╚═╝ ╚═════╝╚═╝     ",
    ];
    let use_color = std::io::stdout().is_terminal();

    for line in ascii {
        if use_color {
            println!("{}", gradient_line(line));
        } else {
            println!("{}", line);
        }
    }
    if use_color {
        println!(
            "{}",
            gradient_line("          Model Context Protocol Server for PCLI2           ")
        );
    } else {
        println!("          Model Context Protocol Server for PCLI2           ");
    }
    println!("Version {}", APP_VERSION);
    println!();
}

fn gradient_line(line: &str) -> String {
    let start = (36u8, 144u8, 255u8);
    let end = (255u8, 120u8, 48u8);
    let chars: Vec<char> = line.chars().collect();
    let len = chars.len().max(1);
    let mut out = String::new();

    for (i, ch) in chars.iter().enumerate() {
        let t = if len == 1 {
            0.0
        } else {
            i as f32 / (len - 1) as f32
        };
        let r = lerp(start.0, end.0, t);
        let g = lerp(start.1, end.1, t);
        let b = lerp(start.2, end.2, t);
        out.push_str(&format!("\x1b[38;2;{};{};{}m{}", r, g, b, ch));
    }
    out.push_str("\x1b[0m");
    out
}

fn lerp(a: u8, b: u8, t: f32) -> u8 {
    let af = a as f32;
    let bf = b as f32;
    (af + (bf - af) * t) as u8
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::response::IntoResponse;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::OnceLock;
    use std::time::{SystemTime, UNIX_EPOCH};
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
            .with_env_filter(EnvFilter::new("debug"))
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

    #[test]
    fn validate_threshold_range() {
        let args = json!({ "threshold": 120.0 });
        assert!(validate_range_f64(&args, "threshold", 0.0, 100.0).is_err());
    }

    #[test]
    fn validate_concurrent_range() {
        let args = json!({ "concurrent": 0 });
        assert!(validate_range_u64(&args, "concurrent", 1, 10).is_err());
    }
}
