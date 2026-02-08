use anyhow::{anyhow, Result};
use axum::{
    body::Bytes,
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use clap::{value_parser, Arg, ArgMatches, Command};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::net::SocketAddr;
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

const DEFAULT_PORT_STR: &str = "8080";
const DEFAULT_HOST: &str = "localhost";

const CLIENT_CLAUDE: &str = "claude";
const CLIENT_QWEN_CODE: &str = "qwen-code";
const CLIENT_QWEN_AGENT: &str = "qwen-agent";

const MCP_SERVER_ALIAS: &str = "pcli2";
const MCP_REMOTE_COMMAND: &str = "npx";
const MCP_REMOTE_PACKAGE: &str = "mcp-remote";

#[derive(Clone)]
struct AppState {
    server_name: String,
    server_version: String,
}

#[derive(Debug, Deserialize)]
struct RpcRequest {
    jsonrpc: Option<String>,
    id: Option<Value>,
    method: String,
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
    init_logging();
    let matches = build_cli().get_matches();

    match matches.subcommand() {
        Some((CMD_SERVE, sub_matches)) => run_server(sub_matches).await,
        Some((CMD_CONFIG, sub_matches)) => run_config(sub_matches),
        Some((CMD_HELP, sub_matches)) => run_help(sub_matches),
        _ => Ok(()),
    }
}

fn init_logging() {
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug")),
        )
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");
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
            Arg::new(ARG_PORT)
                .short('p')
                .long("port")
                .value_name("PORT")
                .value_parser(value_parser!(u16))
                .default_value(DEFAULT_PORT_STR)
                .help("Port to listen on"),
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
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("listening on http://{}", addr);

    axum::serve(
        tokio::net::TcpListener::bind(addr).await?,
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
    let port = *matches
        .get_one::<u16>(ARG_PORT)
        .ok_or_else(|| anyhow!("missing port"))?;

    let config = build_client_config(client, port)?;
    let output = serde_json::to_string_pretty(&config)?;
    println!("{}", output);
    Ok(())
}

fn run_help(matches: &ArgMatches) -> Result<()> {
    let target = matches.get_one::<String>(ARG_COMMAND).map(String::as_str);
    let mut cmd = build_cli();

    if let Some(name) = target {
        if let Some(sub) = cmd
            .get_subcommands()
            .find(|sub| sub.get_name() == name)
        {
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

fn build_client_config(client: &str, port: u16) -> Result<Value> {
    let server_entry = json!({
        MCP_SERVER_ALIAS: {
            "command": MCP_REMOTE_COMMAND,
            "args": [
                "-y",
                MCP_REMOTE_PACKAGE,
                format!("http://{}:{}/mcp", DEFAULT_HOST, port)
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

async fn handle_mcp(
    State(state): State<AppState>,
    bytes: Bytes,
) -> impl IntoResponse {
    let request: RpcRequest = match serde_json::from_slice(&bytes) {
        Ok(req) => req,
        Err(_) => {
            return json_error(
                Value::Null,
                -32700,
                "Parse error: invalid JSON".to_string(),
            )
            .into_response();
        }
    };

    let id = request.id.clone().unwrap_or(Value::Null);
    if let Some(version) = request.jsonrpc.as_deref() {
        if version != "2.0" {
            return json_error(
                id,
                -32600,
                format!("Invalid jsonrpc version '{}'", version),
            )
            .into_response();
        }
    }
    if id.is_null() {
        return StatusCode::NO_CONTENT.into_response();
    }

    info!(
        "mcp request: method={} id={}",
        request.method,
        id.to_string()
    );

    match request.method.as_str() {
        "initialize" => {
            debug!("initialize request");
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
            debug!("tools/list request");
            let tools = tool_list();
            let result = json!({ "tools": tools });
            json_ok(id, result).into_response()
        }
        "tools/call" => {
            let params = request.params.unwrap_or_else(|| json!({}));
            debug!("tools/call request params={}", params);
            match call_tool(params).await {
                Ok(result) => json_ok(id, result).into_response(),
                Err(message) => json_error(id, -32602, message).into_response(),
            }
        }
        _ => json_error(
            id,
            -32601,
            format!("Method '{}' not found", request.method),
        )
        .into_response(),
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

fn tool_list() -> Vec<Value> {
    debug!("building tool list");
    let mut tools = Vec::new();

    tools.push(json!({
        "name": "pcli2",
        "description": "Physna Command Line Interface v2 (PCLI2). Runs `pcli2 folder list` or `pcli2 asset list` with the provided options.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "resource": { "type": "string", "enum": ["folder", "asset"], "description": "Resource to list. Defaults to folder." },
                "tenant": { "type": "string", "description": "Tenant ID or alias." },
                "metadata": { "type": "boolean", "description": "Include metadata in output." },
                "headers": { "type": "boolean", "description": "Include headers in output." },
                "pretty": { "type": "boolean", "description": "Pretty output." },
                "format": { "type": "string", "enum": ["json", "csv", "tree"], "description": "Output format." },
                "folder_uuid": { "type": "string", "description": "Folder UUID." },
                "folder_path": { "type": "string", "description": "Folder path, e.g. /Root/Child." },
                "reload": { "type": "boolean", "description": "Reload folder cache from server." }
            },
            "required": []
        }
    }));

    tools.push(json!({
        "name": "pcli2_tenant_list",
        "description": "Runs `pcli2 tenant list`.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "headers": { "type": "boolean", "description": "Include headers in output." },
                "pretty": { "type": "boolean", "description": "Pretty output." },
                "format": { "type": "string", "enum": ["json", "csv"], "description": "Output format." }
            },
            "required": []
        }
    }));

    tools.push(json!({
        "name": "pcli2_version",
        "description": "Runs `pcli2 --version`.",
        "inputSchema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    }));

    tools.push(json!({
        "name": "pcli2_config_get",
        "description": "Runs `pcli2 config get`.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "headers": { "type": "boolean", "description": "Include headers in output." },
                "pretty": { "type": "boolean", "description": "Pretty output." },
                "format": { "type": "string", "enum": ["json", "csv", "tree"], "description": "Output format." }
            },
            "required": []
        }
    }));

    tools.push(json!({
        "name": "pcli2_config_get_path",
        "description": "Runs `pcli2 config get path`.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "format": { "type": "string", "enum": ["json", "csv", "tree"], "description": "Output format." }
            },
            "required": []
        }
    }));

    tools.push(json!({
        "name": "pcli2_config_environment_list",
        "description": "Runs `pcli2 config environment list`.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "headers": { "type": "boolean", "description": "Include headers in output." },
                "pretty": { "type": "boolean", "description": "Pretty output." },
                "format": { "type": "string", "enum": ["json", "csv"], "description": "Output format." }
            },
            "required": []
        }
    }));

    tools.push(json!({
        "name": "pcli2_config_environment_get",
        "description": "Runs `pcli2 config environment get`.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "name": { "type": "string", "description": "Environment name (defaults to active environment)." },
                "headers": { "type": "boolean", "description": "Include headers in output." },
                "pretty": { "type": "boolean", "description": "Pretty output." },
                "format": { "type": "string", "enum": ["json", "csv"], "description": "Output format." }
            },
            "required": []
        }
    }));

    tools.push(json!({
        "name": "pcli2_tenant_get",
        "description": "Runs `pcli2 tenant get` (current tenant).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "headers": { "type": "boolean", "description": "Include headers in output." },
                "pretty": { "type": "boolean", "description": "Pretty output." },
                "format": { "type": "string", "enum": ["json", "csv", "tree"], "description": "Output format." }
            },
            "required": []
        }
    }));

    tools.push(json!({
        "name": "pcli2_tenant_state",
        "description": "Runs `pcli2 tenant state`.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tenant": { "type": "string", "description": "Tenant ID or alias." },
                "headers": { "type": "boolean", "description": "Include headers in output." },
                "pretty": { "type": "boolean", "description": "Pretty output." },
                "format": { "type": "string", "enum": ["json", "csv"], "description": "Output format." }
            },
            "required": []
        }
    }));

    tools.push(json!({
        "name": "pcli2_folder_get",
        "description": "Runs `pcli2 folder get`.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tenant": { "type": "string", "description": "Tenant ID or alias." },
                "folder_uuid": { "type": "string", "description": "Folder UUID." },
                "folder_path": { "type": "string", "description": "Folder path, e.g. /Root/Child/Grandchild." },
                "metadata": { "type": "boolean", "description": "Include metadata in output." },
                "headers": { "type": "boolean", "description": "Include headers in output." },
                "pretty": { "type": "boolean", "description": "Pretty output." },
                "format": { "type": "string", "enum": ["json", "csv", "tree"], "description": "Output format." }
            },
            "required": []
        }
    }));

    tools.push(json!({
        "name": "pcli2_folder_resolve",
        "description": "Runs `pcli2 folder resolve`.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tenant": { "type": "string", "description": "Tenant ID or alias." },
                "folder_path": { "type": "string", "description": "Folder path, e.g. /Root/Child/Grandchild." }
            },
            "required": ["folder_path"]
        }
    }));

    tools.push(json!({
        "name": "pcli2_folder_dependencies",
        "description": "Runs `pcli2 folder dependencies`.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tenant": { "type": "string", "description": "Tenant ID or alias." },
                "folder_path": {
                    "oneOf": [
                        { "type": "string" },
                        { "type": "array", "items": { "type": "string" } }
                    ],
                    "description": "Folder path(s) to process."
                },
                "headers": { "type": "boolean", "description": "Include headers in output." },
                "metadata": { "type": "boolean", "description": "Include metadata in output." },
                "pretty": { "type": "boolean", "description": "Pretty output." },
                "format": { "type": "string", "enum": ["json", "csv", "tree"], "description": "Output format." },
                "progress": { "type": "boolean", "description": "Display progress bar during processing." }
            },
            "required": ["folder_path"]
        }
    }));

    tools.push(json!({
        "name": "pcli2_folder_geometric_match",
        "description": "Runs `pcli2 folder geometric-match`.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tenant": { "type": "string", "description": "Tenant ID or alias." },
                "folder_path": {
                    "oneOf": [
                        { "type": "string" },
                        { "type": "array", "items": { "type": "string" } }
                    ],
                    "description": "Folder path(s) to process."
                },
                "threshold": { "type": "number", "description": "Similarity threshold (0.00 to 100.00). Default 80.0." },
                "exclusive": { "type": "boolean", "description": "Only show matches within the specified paths." },
                "headers": { "type": "boolean", "description": "Include headers in output." },
                "metadata": { "type": "boolean", "description": "Include metadata in output." },
                "pretty": { "type": "boolean", "description": "Pretty output." },
                "format": { "type": "string", "enum": ["json", "csv"], "description": "Output format." },
                "concurrent": { "type": "integer", "description": "Maximum number of concurrent operations (1-10)." },
                "progress": { "type": "boolean", "description": "Display progress bar during processing." }
            },
            "required": ["folder_path"]
        }
    }));

    tools.push(json!({
        "name": "pcli2_folder_part_match",
        "description": "Runs `pcli2 folder part-match`.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tenant": { "type": "string", "description": "Tenant ID or alias." },
                "folder_path": {
                    "oneOf": [
                        { "type": "string" },
                        { "type": "array", "items": { "type": "string" } }
                    ],
                    "description": "Folder path(s) to process."
                },
                "threshold": { "type": "number", "description": "Similarity threshold (0.00 to 100.00). Default 80.0." },
                "exclusive": { "type": "boolean", "description": "Only show matches within the specified paths." },
                "headers": { "type": "boolean", "description": "Include headers in output." },
                "metadata": { "type": "boolean", "description": "Include metadata in output." },
                "pretty": { "type": "boolean", "description": "Pretty output." },
                "format": { "type": "string", "enum": ["json", "csv"], "description": "Output format." },
                "concurrent": { "type": "integer", "description": "Maximum number of concurrent operations (1-10)." },
                "progress": { "type": "boolean", "description": "Display progress bar during processing." }
            },
            "required": ["folder_path"]
        }
    }));

    tools.push(json!({
        "name": "pcli2_folder_visual_match",
        "description": "Runs `pcli2 folder visual-match`.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tenant": { "type": "string", "description": "Tenant ID or alias." },
                "folder_path": {
                    "oneOf": [
                        { "type": "string" },
                        { "type": "array", "items": { "type": "string" } }
                    ],
                    "description": "Folder path(s) to process."
                },
                "exclusive": { "type": "boolean", "description": "Only show matches within the specified paths." },
                "headers": { "type": "boolean", "description": "Include headers in output." },
                "metadata": { "type": "boolean", "description": "Include metadata in output." },
                "pretty": { "type": "boolean", "description": "Pretty output." },
                "format": { "type": "string", "enum": ["json", "csv"], "description": "Output format." },
                "concurrent": { "type": "integer", "description": "Maximum number of concurrent operations (1-10)." },
                "progress": { "type": "boolean", "description": "Display progress bar during processing." }
            },
            "required": ["folder_path"]
        }
    }));

    tools.push(json!({
        "name": "pcli2_folder_thumbnail",
        "description": "Runs `pcli2 folder thumbnail`.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tenant": { "type": "string", "description": "Tenant ID or alias." },
                "folder_uuid": { "type": "string", "description": "Folder UUID." },
                "folder_path": { "type": "string", "description": "Folder path, e.g. /Root/Child/Grandchild." },
                "output": { "type": "string", "description": "Output directory path." },
                "progress": { "type": "boolean", "description": "Display progress bar during download." },
                "concurrent": { "type": "integer", "description": "Maximum number of concurrent downloads (1-10)." },
                "continue_on_error": { "type": "boolean", "description": "Continue downloading other thumbnails if one fails." },
                "delay": { "type": "integer", "description": "Delay in seconds between downloads (0-180)." }
            },
            "required": []
        }
    }));

    tools.push(json!({
        "name": "pcli2_asset_get",
        "description": "Runs `pcli2 asset get`.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tenant": { "type": "string", "description": "Tenant ID or alias." },
                "uuid": { "type": "string", "description": "Resource UUID." },
                "path": { "type": "string", "description": "Resource path, e.g. /Root/Folder/Asset.stl." },
                "headers": { "type": "boolean", "description": "Include headers in output." },
                "metadata": { "type": "boolean", "description": "Include metadata in output." },
                "pretty": { "type": "boolean", "description": "Pretty output." },
                "format": { "type": "string", "enum": ["json", "csv"], "description": "Output format." }
            },
            "required": []
        }
    }));

    tools.push(json!({
        "name": "pcli2_asset_dependencies",
        "description": "Runs `pcli2 asset dependencies`.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tenant": { "type": "string", "description": "Tenant ID or alias." },
                "uuid": { "type": "string", "description": "Resource UUID." },
                "path": { "type": "string", "description": "Resource path, e.g. /Root/Folder/Asset.stl." },
                "headers": { "type": "boolean", "description": "Include headers in output." },
                "metadata": { "type": "boolean", "description": "Include metadata in output." },
                "pretty": { "type": "boolean", "description": "Pretty output." },
                "format": { "type": "string", "enum": ["json", "csv", "tree"], "description": "Output format." }
            },
            "required": []
        }
    }));

    tools.push(json!({
        "name": "pcli2_asset_download",
        "description": "Runs `pcli2 asset download`.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tenant": { "type": "string", "description": "Tenant ID or alias." },
                "uuid": { "type": "string", "description": "Resource UUID." },
                "path": { "type": "string", "description": "Resource path, e.g. /Root/Folder/Asset.stl." },
                "file": { "type": "string", "description": "Output file path." }
            },
            "required": []
        }
    }));

    tools.push(json!({
        "name": "pcli2_asset_thumbnail",
        "description": "Runs `pcli2 asset thumbnail`.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tenant": { "type": "string", "description": "Tenant ID or alias." },
                "uuid": { "type": "string", "description": "Resource UUID." },
                "path": { "type": "string", "description": "Resource path, e.g. /Root/Folder/Asset.stl." },
                "file": { "type": "string", "description": "Output file path." }
            },
            "required": []
        }
    }));

    tools.push(json!({
        "name": "pcli2_geometric_match",
        "description": "Physna Command Line Interface v2 (PCLI2). Runs `pcli2 asset geometric-match` with the provided options.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tenant": { "type": "string", "description": "Tenant ID or alias." },
                "uuid": { "type": "string", "description": "Resource UUID." },
                "path": { "type": "string", "description": "Resource path, e.g. /Root/Folder/Asset.stl." },
                "threshold": { "type": "number", "description": "Similarity threshold (0.00 to 100.00). Default 80.0." },
                "headers": { "type": "boolean", "description": "Include headers in output." },
                "metadata": { "type": "boolean", "description": "Include metadata in output." },
                "pretty": { "type": "boolean", "description": "Pretty output." },
                "format": { "type": "string", "enum": ["json", "csv"], "description": "Output format." }
            },
            "required": []
        }
    }));

    tools.push(json!({
        "name": "pcli2_asset_part_match",
        "description": "Runs `pcli2 asset part-match`.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tenant": { "type": "string", "description": "Tenant ID or alias." },
                "uuid": { "type": "string", "description": "Resource UUID." },
                "path": { "type": "string", "description": "Resource path, e.g. /Root/Folder/Asset.stl." },
                "threshold": { "type": "number", "description": "Similarity threshold (0.00 to 100.00). Default 80.0." },
                "headers": { "type": "boolean", "description": "Include headers in output." },
                "metadata": { "type": "boolean", "description": "Include metadata in output." },
                "pretty": { "type": "boolean", "description": "Pretty output." },
                "format": { "type": "string", "enum": ["json", "csv"], "description": "Output format." }
            },
            "required": []
        }
    }));

    tools.push(json!({
        "name": "pcli2_asset_visual_match",
        "description": "Runs `pcli2 asset visual-match`.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tenant": { "type": "string", "description": "Tenant ID or alias." },
                "uuid": { "type": "string", "description": "Resource UUID." },
                "path": { "type": "string", "description": "Resource path, e.g. /Root/Folder/Asset.stl." },
                "headers": { "type": "boolean", "description": "Include headers in output." },
                "metadata": { "type": "boolean", "description": "Include metadata in output." },
                "pretty": { "type": "boolean", "description": "Pretty output." },
                "format": { "type": "string", "enum": ["json", "csv"], "description": "Output format." }
            },
            "required": []
        }
    }));

    tools.push(json!({
        "name": "pcli2_asset_text_match",
        "description": "Runs `pcli2 asset text-match`.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tenant": { "type": "string", "description": "Tenant ID or alias." },
                "text": { "type": "string", "description": "Text query to search for in assets." },
                "fuzzy": { "type": "boolean", "description": "Perform fuzzy search instead of exact search." },
                "headers": { "type": "boolean", "description": "Include headers in output." },
                "metadata": { "type": "boolean", "description": "Include metadata in output." },
                "pretty": { "type": "boolean", "description": "Pretty output." },
                "format": { "type": "string", "enum": ["json", "csv"], "description": "Output format." }
            },
            "required": ["text"]
        }
    }));

    tools.push(json!({
        "name": "pcli2_asset_metadata_create",
        "description": "Runs `pcli2 asset metadata create`.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tenant": { "type": "string", "description": "Tenant ID or alias." },
                "uuid": { "type": "string", "description": "Resource UUID." },
                "path": { "type": "string", "description": "Resource path, e.g. /Root/Folder/Asset.stl." },
                "name": { "type": "string", "description": "Metadata property name." },
                "value": { "type": "string", "description": "Metadata property value." },
                "type": { "type": "string", "enum": ["text", "number", "boolean"], "description": "Metadata field type." }
            },
            "required": ["name", "value"]
        }
    }));

    tools
}

async fn call_tool(params: Value) -> Result<Value, String> {
    debug!("call_tool params={}", params);
    let name = params
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Missing tool name".to_string())?;
    let args = params.get("arguments").cloned().unwrap_or_else(|| json!({}));

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
        "pcli2_tenant_list" => run_simple_tool("pcli2 tenant list", run_pcli2_tenant_list(args).await),
        "pcli2_version" => run_simple_tool("pcli2 --version", run_pcli2_version().await),
        "pcli2_config_get" => run_simple_tool("pcli2 config get", run_pcli2_config_get(args).await),
        "pcli2_config_get_path" => run_simple_tool("pcli2 config get path", run_pcli2_config_get_path(args).await),
        "pcli2_config_environment_list" => run_simple_tool("pcli2 config environment list", run_pcli2_config_environment_list(args).await),
        "pcli2_config_environment_get" => run_simple_tool("pcli2 config environment get", run_pcli2_config_environment_get(args).await),
        "pcli2_tenant_get" => run_simple_tool("pcli2 tenant get", run_pcli2_tenant_get(args).await),
        "pcli2_tenant_state" => run_simple_tool("pcli2 tenant state", run_pcli2_tenant_state(args).await),
        "pcli2_folder_get" => run_simple_tool("pcli2 folder get", run_pcli2_folder_get(args).await),
        "pcli2_folder_resolve" => run_simple_tool("pcli2 folder resolve", run_pcli2_folder_resolve(args).await),
        "pcli2_folder_dependencies" => run_simple_tool("pcli2 folder dependencies", run_pcli2_folder_dependencies(args).await),
        "pcli2_folder_geometric_match" => run_simple_tool("pcli2 folder geometric-match", run_pcli2_folder_geometric_match(args).await),
        "pcli2_folder_part_match" => run_simple_tool("pcli2 folder part-match", run_pcli2_folder_part_match(args).await),
        "pcli2_folder_visual_match" => run_simple_tool("pcli2 folder visual-match", run_pcli2_folder_visual_match(args).await),
        "pcli2_folder_thumbnail" => run_simple_tool("pcli2 folder thumbnail", run_pcli2_folder_thumbnail(args).await),
        "pcli2_asset_get" => run_simple_tool("pcli2 asset get", run_pcli2_asset_get(args).await),
        "pcli2_asset_dependencies" => run_simple_tool("pcli2 asset dependencies", run_pcli2_asset_dependencies(args).await),
        "pcli2_asset_download" => run_simple_tool("pcli2 asset download", run_pcli2_asset_download(args).await),
        "pcli2_asset_thumbnail" => run_simple_tool("pcli2 asset thumbnail", run_pcli2_asset_thumbnail(args).await),
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
        "pcli2_asset_part_match" => run_simple_tool("pcli2 asset part-match", run_pcli2_asset_part_match(args).await),
        "pcli2_asset_visual_match" => run_simple_tool("pcli2 asset visual-match", run_pcli2_asset_visual_match(args).await),
        "pcli2_asset_text_match" => run_simple_tool("pcli2 asset text-match", run_pcli2_asset_text_match(args).await),
        "pcli2_asset_metadata_create" => run_simple_tool("pcli2 asset metadata create", run_pcli2_asset_metadata_create(args).await),
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
    if args.get("metadata").and_then(|v| v.as_bool()).unwrap_or(false) {
        cmd_args.push("--metadata".to_string());
    }
    if args.get("headers").and_then(|v| v.as_bool()).unwrap_or(false) {
        cmd_args.push("--headers".to_string());
    }
    if args.get("pretty").and_then(|v| v.as_bool()).unwrap_or(false) {
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
    if args.get("reload").and_then(|v| v.as_bool()).unwrap_or(false) {
        cmd_args.push("--reload".to_string());
    }

    run_pcli2_command(cmd_args, &format!("pcli2 {} list", resource)).await
}

async fn run_pcli2_asset_geometric_match(args: Value) -> Result<String, String> {
    debug!("run_pcli2_asset_geometric_match args={}", args);
    let mut cmd_args: Vec<String> = vec![
        "asset".to_string(),
        "geometric-match".to_string(),
    ];

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
    push_opt_string(&mut cmd_args, "-f", args.get("format").and_then(|v| v.as_str()));

    run_pcli2_command(cmd_args, "pcli2 asset geometric-match").await
}

async fn run_pcli2_tenant_list(args: Value) -> Result<String, String> {
    debug!("run_pcli2_tenant_list args={}", args);
    let mut cmd_args: Vec<String> = vec!["tenant".to_string(), "list".to_string()];
    push_flag_if(&mut cmd_args, &args, "headers", "--headers");
    push_flag_if(&mut cmd_args, &args, "pretty", "--pretty");
    push_opt_string(&mut cmd_args, "-f", args.get("format").and_then(|v| v.as_str()));
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
    push_opt_string(&mut cmd_args, "-f", args.get("format").and_then(|v| v.as_str()));
    run_pcli2_command(cmd_args, "pcli2 config get").await
}

async fn run_pcli2_config_get_path(args: Value) -> Result<String, String> {
    debug!("run_pcli2_config_get_path args={}", args);
    let mut cmd_args: Vec<String> = vec!["config".to_string(), "get".to_string(), "path".to_string()];
    push_opt_string(&mut cmd_args, "-f", args.get("format").and_then(|v| v.as_str()));
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
    push_opt_string(&mut cmd_args, "-f", args.get("format").and_then(|v| v.as_str()));
    run_pcli2_command(cmd_args, "pcli2 config environment list").await
}

async fn run_pcli2_config_environment_get(args: Value) -> Result<String, String> {
    debug!("run_pcli2_config_environment_get args={}", args);
    let mut cmd_args: Vec<String> = vec![
        "config".to_string(),
        "environment".to_string(),
        "get".to_string(),
    ];
    push_opt_string(&mut cmd_args, "-n", args.get("name").and_then(|v| v.as_str()));
    push_flag_if(&mut cmd_args, &args, "headers", "--headers");
    push_flag_if(&mut cmd_args, &args, "pretty", "--pretty");
    push_opt_string(&mut cmd_args, "-f", args.get("format").and_then(|v| v.as_str()));
    run_pcli2_command(cmd_args, "pcli2 config environment get").await
}

async fn run_pcli2_tenant_get(args: Value) -> Result<String, String> {
    debug!("run_pcli2_tenant_get args={}", args);
    let mut cmd_args: Vec<String> = vec!["tenant".to_string(), "get".to_string()];
    push_flag_if(&mut cmd_args, &args, "headers", "--headers");
    push_flag_if(&mut cmd_args, &args, "pretty", "--pretty");
    push_opt_string(&mut cmd_args, "-f", args.get("format").and_then(|v| v.as_str()));
    run_pcli2_command(cmd_args, "pcli2 tenant get").await
}

async fn run_pcli2_tenant_state(args: Value) -> Result<String, String> {
    debug!("run_pcli2_tenant_state args={}", args);
    let mut cmd_args: Vec<String> = vec!["tenant".to_string(), "state".to_string()];
    if let Some(tenant) = args.get("tenant").and_then(|v| v.as_str()) {
        cmd_args.push("-t".to_string());
        cmd_args.push(tenant.to_string());
    }
    push_flag_if(&mut cmd_args, &args, "headers", "--headers");
    push_flag_if(&mut cmd_args, &args, "pretty", "--pretty");
    push_opt_string(&mut cmd_args, "-f", args.get("format").and_then(|v| v.as_str()));
    run_pcli2_command(cmd_args, "pcli2 tenant state").await
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
    push_opt_string(&mut cmd_args, "-f", args.get("format").and_then(|v| v.as_str()));
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
    push_opt_string(&mut cmd_args, "-f", args.get("format").and_then(|v| v.as_str()));
    push_flag_if(&mut cmd_args, &args, "progress", "--progress");
    run_pcli2_command(cmd_args, "pcli2 folder dependencies").await
}

async fn run_pcli2_folder_geometric_match(args: Value) -> Result<String, String> {
    debug!("run_pcli2_folder_geometric_match args={}", args);
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
    push_opt_string(&mut cmd_args, "-f", args.get("format").and_then(|v| v.as_str()));
    push_opt_u64(&mut cmd_args, &args, "concurrent", "--concurrent");
    push_flag_if(&mut cmd_args, &args, "progress", "--progress");
    run_pcli2_command(cmd_args, "pcli2 folder geometric-match").await
}

async fn run_pcli2_folder_part_match(args: Value) -> Result<String, String> {
    debug!("run_pcli2_folder_part_match args={}", args);
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
    push_opt_string(&mut cmd_args, "-f", args.get("format").and_then(|v| v.as_str()));
    push_opt_u64(&mut cmd_args, &args, "concurrent", "--concurrent");
    push_flag_if(&mut cmd_args, &args, "progress", "--progress");
    run_pcli2_command(cmd_args, "pcli2 folder part-match").await
}

async fn run_pcli2_folder_visual_match(args: Value) -> Result<String, String> {
    debug!("run_pcli2_folder_visual_match args={}", args);
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
    push_opt_string(&mut cmd_args, "-f", args.get("format").and_then(|v| v.as_str()));
    push_opt_u64(&mut cmd_args, &args, "concurrent", "--concurrent");
    push_flag_if(&mut cmd_args, &args, "progress", "--progress");
    run_pcli2_command(cmd_args, "pcli2 folder visual-match").await
}

async fn run_pcli2_folder_thumbnail(args: Value) -> Result<String, String> {
    debug!("run_pcli2_folder_thumbnail args={}", args);
    let mut cmd_args: Vec<String> = vec!["folder".to_string(), "thumbnail".to_string()];
    if let Some(tenant) = args.get("tenant").and_then(|v| v.as_str()) {
        cmd_args.push("-t".to_string());
        cmd_args.push(tenant.to_string());
    }
    let (folder_uuid, folder_path) = require_folder_uuid_or_path(&args)?;
    push_opt_string(&mut cmd_args, "--folder-uuid", folder_uuid.as_deref());
    push_opt_string(&mut cmd_args, "--folder-path", folder_path.as_deref());
    push_opt_string(&mut cmd_args, "--output", args.get("output").and_then(|v| v.as_str()));
    push_flag_if(&mut cmd_args, &args, "progress", "--progress");
    push_opt_u64(&mut cmd_args, &args, "concurrent", "--concurrent");
    push_flag_if(&mut cmd_args, &args, "continue_on_error", "--continue-on-error");
    push_opt_u64(&mut cmd_args, &args, "delay", "--delay");
    run_pcli2_command(cmd_args, "pcli2 folder thumbnail").await
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
    push_opt_string(&mut cmd_args, "-f", args.get("format").and_then(|v| v.as_str()));
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
    push_opt_string(&mut cmd_args, "-f", args.get("format").and_then(|v| v.as_str()));
    run_pcli2_command(cmd_args, "pcli2 asset dependencies").await
}

async fn run_pcli2_asset_download(args: Value) -> Result<String, String> {
    debug!("run_pcli2_asset_download args={}", args);
    let mut cmd_args: Vec<String> = vec!["asset".to_string(), "download".to_string()];
    if let Some(tenant) = args.get("tenant").and_then(|v| v.as_str()) {
        cmd_args.push("-t".to_string());
        cmd_args.push(tenant.to_string());
    }
    let (uuid, path) = require_uuid_or_path(&args)?;
    push_opt_string(&mut cmd_args, "--uuid", uuid.as_deref());
    push_opt_string(&mut cmd_args, "--path", path.as_deref());
    if let Some(file) = args.get("file").and_then(|v| v.as_str()) {
        cmd_args.push(file.to_string());
    }
    run_pcli2_command(cmd_args, "pcli2 asset download").await
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
    if let Some(file) = args.get("file").and_then(|v| v.as_str()) {
        cmd_args.push(file.to_string());
    }
    run_pcli2_command(cmd_args, "pcli2 asset thumbnail").await
}

async fn run_pcli2_asset_part_match(args: Value) -> Result<String, String> {
    debug!("run_pcli2_asset_part_match args={}", args);
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
    push_opt_string(&mut cmd_args, "-f", args.get("format").and_then(|v| v.as_str()));
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
    push_opt_string(&mut cmd_args, "-f", args.get("format").and_then(|v| v.as_str()));
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
    push_opt_string(&mut cmd_args, "-f", args.get("format").and_then(|v| v.as_str()));
    run_pcli2_command(cmd_args, "pcli2 asset text-match").await
}

async fn run_pcli2_asset_metadata_create(args: Value) -> Result<String, String> {
    debug!("run_pcli2_asset_metadata_create args={}", args);
    let mut cmd_args: Vec<String> = vec!["asset".to_string(), "metadata".to_string(), "create".to_string()];
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
    push_opt_string(&mut cmd_args, "--type", args.get("type").and_then(|v| v.as_str()));
    run_pcli2_command(cmd_args, "pcli2 asset metadata create").await
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

fn require_uuid_or_path(args: &Value) -> Result<(Option<String>, Option<String>), String> {
    let uuid = args.get("uuid").and_then(|v| v.as_str()).map(str::to_string);
    let path = args.get("path").and_then(|v| v.as_str()).map(str::to_string);
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
        return Err("Missing required argument: provide either 'folder_uuid' or 'folder_path'".to_string());
    }
    Ok((uuid, path))
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

async fn run_pcli2_command(cmd_args: Vec<String>, label: &str) -> Result<String, String> {
    info!("executing: pcli2 {}", cmd_args.join(" "));
    let output = tokio::process::Command::new("pcli2")
        .args(&cmd_args)
        .output()
        .await
        .map_err(|e| format!("Failed to execute pcli2: {}", e))?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    if output.status.success() {
        Ok(stdout.trim_end().to_string())
    } else {
        Err(format!(
            "{} failed (code {}):\n{}\n{}",
            label,
            output.status,
            stdout.trim_end(),
            stderr.trim_end()
        ))
    }
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

    for line in ascii {
        println!("{}", gradient_line(line));
    }
    println!("{}", gradient_line("          Model Context Protocol Server for PCLI2           "));
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
    // Tests removed: SQLite support was removed.
}
