# pcli2-mcp

[![License](https://img.shields.io/github/license/jchultarsky101/pcli2-mco.svg)](LICENSE)
[![CI](https://github.com/jchultarsky101/pcli2-mco/actions/workflows/ci.yml/badge.svg)](https://github.com/jchultarsky101/pcli2-mco/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/jchultarsky101/pcli2-mco)](https://github.com/jchultarsky101/pcli2-mco/releases)

A lightweight Model Context Protocol (MCP) server over HTTP that wraps the PCLI2 CLI.
It exposes PCLI2 capabilities as MCP tools so LLM clients can list assets/folders
and run geometric match queries through a single JSON-RPC endpoint.

Project links:

- `pcli2-mcp`: https://github.com/jchultarsky101/pcli2-mco
- `pcli2`: https://github.com/jchultarsky101/pcli2

**Status:** early development (v0.1.0).

## Relationship To PCLI2

PCLI2 (Physna Command Line Interface v2) is the official CLI for the Physna public API, focused on 3D geometry search and asset/folder operations. This project is an MCP wrapper around PCLI2: it runs PCLI2 commands behind an MCP JSON-RPC interface so clients like Claude or Qwen can invoke the same capabilities programmatically. For PCLI2 documentation and usage, see the PCLI2 docs site: https://jchultarsky101.github.io/pcli2/ and the repository: https://github.com/jchultarsky101/pcli2.

## Features

- MCP over HTTP (`/mcp`) with JSON-RPC 2.0
- Tool wrapper for `pcli2 folder list` and `pcli2 asset list`
- Tool wrapper for `pcli2 asset geometric-match`
- Simple, single-binary Rust server

## Requirements

- Rust toolchain (edition 2024)
- `pcli2` installed and available on `PATH`
- Any required PCLI2 auth/config already set up for your environment

## Installation

```bash
cargo build --release
```

The binary will be at `target/release/pcli2-mcp`.

## Run

```bash
cargo run -- serve --port 8080
```

Health check:

```bash
curl -s http://localhost:8080/health
```

## CLI

Run the server:

```bash
pcli2-mcp serve --port 8080
```

Print client config (pretty JSON):

```bash
pcli2-mcp config --client claude --port 8080
```

Command-specific help:

```bash
pcli2-mcp help serve
```

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

### `pcli2`

Runs `pcli2 folder list` (default) or `pcli2 asset list`.

Arguments:

- `resource`: `folder` or `asset` (default: `folder`)
- `tenant`: tenant ID or alias
- `metadata`: include metadata
- `headers`: include headers
- `pretty`: pretty output
- `format`: `json`, `csv`, or `tree`
- `folder_uuid`: folder UUID
- `folder_path`: folder path (e.g. `/Root/Child`)
- `reload`: reload folder cache

### `pcli2_geometric_match`

Runs `pcli2 asset geometric-match`.

Arguments:

- `tenant`: tenant ID or alias
- `uuid`: resource UUID (required if `path` not provided)
- `path`: resource path (required if `uuid` not provided)
- `threshold`: similarity threshold (0.00–100.00)
- `headers`: include headers
- `metadata`: include metadata
- `pretty`: pretty output
- `format`: `json` or `csv`

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
