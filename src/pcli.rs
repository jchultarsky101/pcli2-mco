use anyhow::Result;
use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use serde_json::{Map, Value, json};
use std::{
    env, fs,
    path::PathBuf,
    process::Stdio,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::info;

use crate::thumbnail::ThumbnailCache;

pub const PCLI2_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30 * 60);
pub const MAX_PCLI2_OUTPUT_BYTES: usize = 200 * 1024 * 1024;
pub const PCLI2_BIN_ENV: &str = "PCLI2_BIN";

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

pub fn tool_list() -> Vec<Value> {
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
        "Runs `pcli2 asset thumbnail` and returns the thumbnail image. Use `response_mode` to control the output format: 'url' returns an HTTP URL (efficient for LLM context, requires HTTP fetch), 'data_url' returns a base64 data URI (self-contained, uses more tokens but renders immediately in markdown).",
        &[],
        |props| {
            add_tenant(props);
            add_uuid_path(props);
            add_prop(
                props,
                "response_mode",
                json!({
                    "type": "string",
                    "enum": ["url", "data_url"],
                    "default": "url",
                    "description": "Output format: 'url' returns an HTTP URL (efficient for LLM context, ~200 tokens), 'data_url' returns a base64 data URI (self-contained image, ~50K tokens but renders immediately in markdown without HTTP fetch). Use 'data_url' when the client cannot make HTTP requests or when you need the image to display immediately."
                }),
            );
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
            add_metadata_name(props);
            add_format(props, &["json", "csv"]);
        },
    );

    define_tool(
        &mut tools,
        "pcli2_thumbnail_cache_cleanup",
        "Removes expired thumbnails from the cache to free up disk space.",
        &[],
        |_| {},
    );

    tools
}

pub async fn call_tool(
    params: Value,
    thumbnail_cache: Option<&ThumbnailCache>,
) -> Result<Value, String> {
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
            let src = run_pcli2_asset_thumbnail(args, thumbnail_cache).await?;
            // Return HTML that embeds the thumbnail image
            // src can be either an HTTP URL (response_mode=url) or a data URI (response_mode=data_url)
            let html = format!(
                r#"<!DOCTYPE html>
<html>
<head><title>Asset Thumbnail</title></head>
<body>
<img src="{}" alt="Asset Thumbnail" style="max-width: 100%; height: auto;">
</body>
</html>"#,
                src
            );
            Ok(json!({
                "content": [{
                    "type": "text",
                    "text": html
                }]
            }))
        }
        "pcli2_asset_reprocess" => run_simple_tool(
            "pcli2 asset reprocess",
            run_pcli2_asset_reprocess(args).await,
        ),
        "pcli2_geometric_match" => {
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
        "pcli2_thumbnail_cache_cleanup" => {
            let Some(cache) = thumbnail_cache else {
                return Ok(json!({
                    "content": [{
                        "type": "text",
                        "text": "Thumbnail cache is not available"
                    }]
                }));
            };
            match cache.cleanup_expired() {
                Ok(count) => Ok(json!({
                    "content": [{
                        "type": "text",
                        "text": format!("Cleaned up {} expired thumbnail(s)", count)
                    }]
                })),
                Err(err) => Err(format!("Thumbnail cache cleanup failed: {}", err)),
            }
        }
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

pub async fn run_pcli2_tenant_list(args: Value) -> Result<String, String> {
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

pub async fn run_pcli2_version() -> Result<String, String> {
    let cmd_args: Vec<String> = vec!["--version".to_string()];
    run_pcli2_command(cmd_args, "pcli2 --version").await
}

async fn run_pcli2_config_get(args: Value) -> Result<String, String> {
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

async fn run_pcli2_asset_thumbnail(
    args: Value,
    thumbnail_cache: Option<&ThumbnailCache>,
) -> Result<String, String> {
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

    // Determine response mode (default to "url" for efficiency)
    let response_mode = args
        .get("response_mode")
        .and_then(|v| v.as_str())
        .unwrap_or("url");

    match response_mode {
        "data_url" => {
            // Return base64-encoded data URI (self-contained, uses more tokens)
            let encoded = BASE64_STANDARD.encode(bytes);
            Ok(format!("data:image/png;base64,{}", encoded))
        }
        "url" => {
            // Save to cache and return HTTP URL (efficient for LLM context)
            if let Some(cache) = thumbnail_cache {
                let source = uuid.or(path).unwrap_or_else(|| "unknown".to_string());
                let (_cache_key, url) = cache.save_thumbnail(&source, &bytes)?;
                Ok(url)
            } else {
                // Fallback to data URL if cache is not available
                let encoded = BASE64_STANDARD.encode(bytes);
                Ok(format!("data:image/png;base64,{}", encoded))
            }
        }
        _ => {
            // Unknown mode, default to URL behavior
            if let Some(cache) = thumbnail_cache {
                let source = uuid.or(path).unwrap_or_else(|| "unknown".to_string());
                let (_cache_key, url) = cache.save_thumbnail(&source, &bytes)?;
                Ok(url)
            } else {
                let encoded = BASE64_STANDARD.encode(bytes);
                Ok(format!("data:image/png;base64,{}", encoded))
            }
        }
    }
}

async fn run_pcli2_asset_reprocess(args: Value) -> Result<String, String> {
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
pub fn shell_escape_arg(arg: &str) -> String {
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

pub async fn read_limited<R: AsyncRead + Unpin>(
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

pub async fn run_pcli2_command(cmd_args: Vec<String>, label: &str) -> Result<String, String> {
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

pub fn pcli2_executable() -> String {
    env::var(PCLI2_BIN_ENV).unwrap_or_else(|_| "pcli2".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_shell_escape_arg() {
        assert_eq!(shell_escape_arg("simple"), "simple");
        assert_eq!(shell_escape_arg(""), "''");
        assert_eq!(shell_escape_arg("with space"), "'with space'");
        assert_eq!(shell_escape_arg("with'quote"), "'with'\"'\"'quote'");
    }

    #[test]
    fn test_push_flag_if() {
        let mut cmd_args = vec![];
        let args = json!({"flag": true});
        push_flag_if(&mut cmd_args, &args, "flag", "--flag");
        assert_eq!(cmd_args, vec!["--flag".to_string()]);
    }

    #[test]
    fn test_push_flag_if_false() {
        let mut cmd_args: Vec<String> = vec![];
        let args = json!({"flag": false});
        push_flag_if(&mut cmd_args, &args, "flag", "--flag");
        let expected: Vec<String> = vec![];
        assert_eq!(cmd_args, expected);
    }

    #[test]
    fn test_push_flag_if_missing() {
        let mut cmd_args: Vec<String> = vec![];
        let args = json!({});
        push_flag_if(&mut cmd_args, &args, "flag", "--flag");
        let expected: Vec<String> = vec![];
        assert_eq!(cmd_args, expected);
    }

    #[test]
    fn test_push_opt_string_some() {
        let mut cmd_args: Vec<String> = vec![];
        push_opt_string(&mut cmd_args, "--opt", Some("value"));
        assert_eq!(cmd_args, vec!["--opt".to_string(), "value".to_string()]);
    }

    #[test]
    fn test_push_opt_string_none() {
        let mut cmd_args: Vec<String> = vec![];
        push_opt_string(&mut cmd_args, "--opt", None);
        let expected: Vec<String> = vec![];
        assert_eq!(cmd_args, expected);
    }

    #[test]
    fn test_push_opt_f64() {
        let mut cmd_args = vec![];
        let args = json!({"threshold": 80.5});
        push_opt_f64(&mut cmd_args, &args, "threshold", "--threshold");
        assert_eq!(
            cmd_args,
            vec!["--threshold".to_string(), "80.5".to_string()]
        );
    }

    #[test]
    fn test_push_opt_f64_missing() {
        let mut cmd_args: Vec<String> = vec![];
        let args = json!({});
        push_opt_f64(&mut cmd_args, &args, "threshold", "--threshold");
        let expected: Vec<String> = vec![];
        assert_eq!(cmd_args, expected);
    }

    #[test]
    fn test_push_opt_u64() {
        let mut cmd_args = vec![];
        let args = json!({"count": 5});
        push_opt_u64(&mut cmd_args, &args, "count", "--count");
        assert_eq!(cmd_args, vec!["--count".to_string(), "5".to_string()]);
    }

    #[test]
    fn test_push_opt_u64_missing() {
        let mut cmd_args: Vec<String> = vec![];
        let args = json!({});
        push_opt_u64(&mut cmd_args, &args, "count", "--count");
        let expected: Vec<String> = vec![];
        assert_eq!(cmd_args, expected);
    }

    #[test]
    fn test_validate_range_f64_valid() {
        let args = json!({"threshold": 80.5});
        let result = validate_range_f64(&args, "threshold", 0.0, 100.0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_range_f64_invalid_low() {
        let args = json!({"threshold": -1.0});
        let result = validate_range_f64(&args, "threshold", 0.0, 100.0);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("must be between"));
    }

    #[test]
    fn test_validate_range_f64_invalid_high() {
        let args = json!({"threshold": 101.0});
        let result = validate_range_f64(&args, "threshold", 0.0, 100.0);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("must be between"));
    }

    #[test]
    fn test_validate_range_u64_valid() {
        let args = json!({"count": 5});
        let result = validate_range_u64(&args, "count", 1, 10);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_range_u64_invalid_low() {
        let args = json!({"count": 0});
        let result = validate_range_u64(&args, "count", 1, 10);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("must be between"));
    }

    #[test]
    fn test_validate_range_u64_invalid_high() {
        let args = json!({"count": 11});
        let result = validate_range_u64(&args, "count", 1, 10);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("must be between"));
    }

    #[test]
    fn test_require_uuid_or_path_both_none() {
        let args = json!({});
        let result = require_uuid_or_path(&args);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("provide either 'uuid' or 'path'")
        );
    }

    #[test]
    fn test_require_uuid_or_path_uuid_only() {
        let args = json!({"uuid": "some-uuid"});
        let result = require_uuid_or_path(&args);
        assert!(result.is_ok());
        let (uuid, path) = result.unwrap();
        assert_eq!(uuid, Some("some-uuid".to_string()));
        assert_eq!(path, None);
    }

    #[test]
    fn test_require_uuid_or_path_path_only() {
        let args = json!({"path": "/some/path"});
        let result = require_uuid_or_path(&args);
        assert!(result.is_ok());
        let (uuid, path) = result.unwrap();
        assert_eq!(uuid, None);
        assert_eq!(path, Some("/some/path".to_string()));
    }

    #[test]
    fn test_require_folder_uuid_or_path_both_none() {
        let args = json!({});
        let result = require_folder_uuid_or_path(&args);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("provide either 'folder_uuid' or 'folder_path'")
        );
    }

    #[test]
    fn test_require_folder_uuid_or_path_uuid_only() {
        let args = json!({"folder_uuid": "some-uuid"});
        let result = require_folder_uuid_or_path(&args);
        assert!(result.is_ok());
        let (uuid, path) = result.unwrap();
        assert_eq!(uuid, Some("some-uuid".to_string()));
        assert_eq!(path, None);
    }

    #[test]
    fn test_require_folder_uuid_or_path_path_only() {
        let args = json!({"folder_path": "/some/path"});
        let result = require_folder_uuid_or_path(&args);
        assert!(result.is_ok());
        let (uuid, path) = result.unwrap();
        assert_eq!(uuid, None);
        assert_eq!(path, Some("/some/path".to_string()));
    }

    #[test]
    fn test_parse_string_list_array() {
        let args = json!({"names": ["item1", "item2", "item3"]});
        let result = parse_string_list(&args, "names");
        assert_eq!(result, vec!["item1", "item2", "item3"]);
    }

    #[test]
    fn test_parse_string_list_single_string() {
        let args = json!({"names": "single-item"});
        let result = parse_string_list(&args, "names");
        assert_eq!(result, vec!["single-item"]);
    }

    #[test]
    fn test_parse_string_list_empty() {
        let args = json!({});
        let result = parse_string_list(&args, "names");
        let expected: Vec<String> = vec![];
        assert_eq!(result, expected);
    }
}
