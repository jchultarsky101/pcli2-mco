# pcli2-mcp

Oranda docs: https://jchultarsky101.github.io/pcli2-mcp/

[![Docs](https://img.shields.io/badge/docs-github%20pages-blue)](https://jchultarsky101.github.io/pcli2-mcp/)
[![License](https://img.shields.io/github/license/jchultarsky101/pcli2-mcp.svg)](LICENSE)
[![CI](https://github.com/jchultarsky101/pcli2-mcp/actions/workflows/ci.yml/badge.svg)](https://github.com/jchultarsky101/pcli2-mcp/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/jchultarsky101/pcli2-mcp)](https://github.com/jchultarsky101/pcli2-mcp/releases)

A lightweight Model Context Protocol (MCP) server over HTTP that wraps the PCLI2 CLI.
It exposes PCLI2 capabilities as MCP tools so LLM clients can list assets/folders
and run geometric match queries through a single JSON-RPC endpoint.

Project links:

- `pcli2-mcp`: https://github.com/jchultarsky101/pcli2-mcp
- `pcli2`: https://github.com/jchultarsky101/pcli2

**Status:** early development (v0.1.3).

## Main Concepts

PCLI2 (Physna Command Line Interface v2) is the official CLI for the Physna public API, focused on 3D geometry search and asset/folder operations. This project is an MCP wrapper around PCLI2: it runs PCLI2 commands behind an MCP JSON-RPC interface so clients like Claude or Qwen can invoke the same capabilities programmatically. For PCLI2 documentation and usage, see the PCLI2 docs site: https://jchultarsky101.github.io/pcli2/ and the repository: https://github.com/jchultarsky101/pcli2.

In short, the flow looks like this:

1. An LLM client (Claude, Qwen, or another MCP-capable app) sends a tool request.
2. `pcli2-mcp` translates that request into a PCLI2 CLI call.
3. PCLI2 talks to the Physna API and returns results.
4. `pcli2-mcp` returns the structured response back to the LLM client.

This keeps your LLM integration stable (MCP over HTTP) while the underlying CLI (PCLI2) remains the single source of truth for Physna API behavior.

```mermaid
flowchart LR
  LLM["LLM Client (Claude, Qwen, etc.)"]
  MCP[pcli2-mcp MCP Server]
  CLI[PCLI2 CLI]
  API[Physna Public API]

  LLM -- MCP tools/list, tools/call --> MCP
  MCP -- spawn CLI commands --> CLI
  CLI -- HTTPS requests --> API
  API -- responses --> CLI
  CLI -- stdout/stderr --> MCP
  MCP -- JSON-RPC response --> LLM
```

## Quick Start

1. Install PCLI2 and authenticate.
   Follow the PCLI2 docs and make sure `pcli2` is on your `PATH`: https://jchultarsky101.github.io/pcli2/
2. Install `pcli2-mcp` (see Installation below).
3. Run the server:

   ```bash
   pcli2-mcp serve --host localhost --port 8080 --log-level info
   ```
4. Verify the server is healthy:

   ```bash
   curl -s http://localhost:8080/health
   ```
5. Validate MCP is responding (list tools):

   ```bash
   curl -s http://localhost:8080/mcp \
     -H "Content-Type: application/json" \
     -d '{
       "jsonrpc": "2.0",
       "id": 1,
       "method": "tools/list",
       "params": {}
     }'
   ```
6. Generate a client config snippet:

   ```bash
   pcli2-mcp config --client claude --host localhost --port 8080
   ```
7. Paste the snippet into your client (see the sections below).

## Installation

Recommended (pre-built binaries):

1. Download the latest release for your platform from:
   https://github.com/jchultarsky101/pcli2-mcp/releases
2. Put the `pcli2-mcp` binary somewhere on your `PATH`.

Build from source:

1. Install the Rust toolchain (edition 2024).
2. Build:

   ```bash
   cargo build --release
   ```

   The binary will be at `target/release/pcli2-mcp`.

## Documentation Site (Oranda)

This repository uses Oranda to render a nicer, hosted version of the README.

Local build:

```bash
oranda build
```

Local preview (auto-rebuilds on changes):

```bash
oranda dev
```

The GitHub Pages workflow (`.github/workflows/docs.yml`) publishes the site from `public/`.

## Features

- MCP over HTTP (`/mcp`) with JSON-RPC 2.0
- Tool wrapper for `pcli2 folder list` and `pcli2 asset list`
- Tool wrapper for `pcli2 asset geometric-match`
- Simple, single-binary Rust server

## Client Setup (Using `config`)

The `config` command prints a ready-to-paste JSON snippet with the MCP server definition:

```bash
pcli2-mcp config --client claude --host localhost --port 8080
```

Use the output in the sections below.

## CLI

Run the server:

```bash
pcli2-mcp serve --host localhost --port 8080 --log-level info
```

Use `--host 0.0.0.0` to listen on all interfaces.

Print client config (pretty JSON):

```bash
pcli2-mcp config --client claude --host localhost --port 8080
```

Command-specific help:

```bash
pcli2-mcp help serve
```

### Claude Desktop

1. Open Claude Desktop and go to Settings > Developer > Edit Config (or open the config file directly).
2. Paste the JSON output under `mcpServers`.
3. Restart Claude Desktop.

Config file locations:

- macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
- Windows: `%APPDATA%\Claude\claude_desktop_config.json`
- Linux: `~/.config/Claude/claude_desktop_config.json`

### Qwen Code

Qwen Code reads MCP servers from `mcpServers` in `settings.json`. You can configure this via:

1. Edit `.qwen/settings.json` in your project, or `~/.qwen/settings.json` for user scope.
2. Paste the JSON output under `mcpServers`.
3. Restart Qwen Code for the settings to load.

Alternatively, you can add a server with the CLI:

```bash
qwen mcp add --transport http pcli2 http://localhost:8080/mcp
```

### Qwen Agent (Python)

Pass an MCP configuration dictionary (including `mcpServers`) when creating the agent:

```python
from qwen_agent.agents import Assistant

mcp_config = {
    "mcpServers": {
        "pcli2": {
            "command": "npx",
            "args": ["-y", "mcp-remote", "http://localhost:8080/mcp"]
        }
    }
}

agent = Assistant(
    llm=llm_cfg,
    function_list=[mcp_config],
)
```

### MCPHost (Local Ollama)

MCPHost can use a local Ollama model and connect to this MCP server over HTTP.

1. Install Ollama, pull a model, and make sure the daemon is running:

   ```bash
   ollama pull mistral
   ollama serve
   ```

2. Install MCPHost:

   ```bash
   go install github.com/mark3labs/mcphost@latest
   ```

3. Create a config file (preferred locations include `~/.mcphost.yml` or `~/.mcphost.json`) and point it at your local MCP server:

   ```yaml
   # ~/.mcphost.yml
   mcpServers:
     pcli2:
       type: "remote"
       url: "http://localhost:8080/mcp"

   # Use a local model with function-calling support
   model: "ollama:mistral"
   ```

4. Run MCPHost:

   ```bash
   mcphost
   ```

### Other MCP Clients

Most MCP-compatible clients accept the same `mcpServers` JSON block. Use the output of `pcli2-mcp config` as the server definition and follow your client’s MCP documentation.

## MCP API

The server implements MCP over HTTP with a JSON-RPC 2.0 interface.

- `POST /mcp`
- Methods: `initialize`, `tools/list`, `tools/call`

Example `tools/list`:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/list",
  "params": {}
}
```

Example `tools/call` (list assets under `/Julian` as CSV):

```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "method": "tools/call",
  "params": {
    "name": "pcli2",
    "arguments": {
      "resource": "asset",
      "folder_path": "/Julian",
      "format": "csv",
      "headers": true
    }
  }
}
```

## Tools

Notes:

- Most asset tools require either `uuid` or `path`.
- Most folder tools require either `folder_uuid` or `folder_path` (or a list of `folder_path`).

| Tool | PCLI2 Command | Required Arguments |
| --- | --- | --- |
| `pcli2` | `pcli2 folder list` / `pcli2 asset list` | none |
| `pcli2_version` | `pcli2 --version` | none |
| `pcli2_tenant_list` | `pcli2 tenant list` | none |
| `pcli2_tenant_get` | `pcli2 tenant get` | none |
| `pcli2_tenant_state` | `pcli2 tenant state` | none |
| `pcli2_tenant_use` | `pcli2 tenant use --name <tenantName>` | `tenant_name` or `name` |
| `pcli2_config_get` | `pcli2 config get` | none |
| `pcli2_config_get_path` | `pcli2 config get path` | none |
| `pcli2_config_environment_list` | `pcli2 config environment list` | none |
| `pcli2_config_environment_get` | `pcli2 config environment get` | none |
| `pcli2_folder_get` | `pcli2 folder get` | `folder_uuid` or `folder_path` |
| `pcli2_folder_resolve` | `pcli2 folder resolve` | `folder_path` |
| `pcli2_folder_dependencies` | `pcli2 folder dependencies` | `folder_path` |
| `pcli2_folder_geometric_match` | `pcli2 folder geometric-match` | `folder_path` |
| `pcli2_folder_part_match` | `pcli2 folder part-match` | `folder_path` |
| `pcli2_folder_visual_match` | `pcli2 folder visual-match` | `folder_path` |
| `pcli2_asset_get` | `pcli2 asset get` | `uuid` or `path` |
| `pcli2_asset_dependencies` | `pcli2 asset dependencies` | `uuid` or `path` |
| `pcli2_asset_thumbnail` | `pcli2 asset thumbnail` | `uuid` or `path` |
| `pcli2_geometric_match` | `pcli2 asset geometric-match` | `uuid` or `path` |
| `pcli2_asset_part_match` | `pcli2 asset part-match` | `uuid` or `path` |
| `pcli2_asset_visual_match` | `pcli2 asset visual-match` | `uuid` or `path` |
| `pcli2_asset_text_match` | `pcli2 asset text-match` | `text` |
| `pcli2_asset_metadata_create` | `pcli2 asset metadata create` | `name`, `value`, plus `uuid` or `path` |
| `pcli2_asset_metadata_delete` | `pcli2 asset metadata delete` | `name`, plus `uuid` or `path` |

Example:

```json
{
  "jsonrpc": "2.0",
  "id": 3,
  "method": "tools/call",
  "params": {
    "name": "pcli2_geometric_match",
    "arguments": {
      "path": "/Root/Folder/Part.stl",
      "threshold": 85,
      "format": "csv",
      "headers": true
    }
  }
}
```

## Configuration

- `--port`: listening port (default: `8080`)
- `--log-level`: logging level for the server (default: `info`)
- `RUST_LOG`: log level (e.g. `info`, `debug`)

## Troubleshooting

- Ensure `pcli2` is installed and reachable via `PATH`.
- If the server returns a non-zero error, check the embedded `pcli2` stdout/stderr in the response.

## Contributing

Issues and pull requests are welcome. If you plan significant changes, open an issue first
so we can discuss scope and approach.

## Getting Help

Open an issue with a clear repro, expected behavior, and logs (set `RUST_LOG=debug` if needed).

## Maintainers

Maintainers are listed in the repository’s contributor/maintainer roster.

## Changelog

See `CHANGELOG.md`.

## License

Apache License 2.0. See `LICENSE` and `NOTICE`.
