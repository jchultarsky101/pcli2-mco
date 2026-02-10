# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project adheres to Semantic Versioning.

## [Unreleased]

## [0.1.3] - 2026-02-09

### Added

- Added tenant state filtering via the new `--type` option for `pcli2 tenant state`.
- Streamed asset thumbnails over MCP as base64 image content with fallback data URLs.

## [0.1.2] - 2026-02-09

### Added

- Support for additional PCLI2 tools (tenant, config/environment, folder, asset, metadata).
- CLI improvements: `serve --log-level` and `config --host`.
- Mock `pcli2` integration test for tool wiring.
- Expanded README with client setup instructions and tool reference table.

### Changed

- Tool schema construction refactored for reuse and consistency.
- Removed folder thumbnail tool and asset download support.

## [0.1.1] - 2026-02-08

### Added

- Badges and NOTICE file.
- GitHub Actions CI and cargo-dist release workflow.
- README improvements and client setup instructions.

### Changed

- Remote repository renamed to `pcli2-mcp`.

## [0.1.0] - 2026-02-08

### Added

- MCP-over-HTTP server with JSON-RPC 2.0 interface.
- `pcli2` tool for listing folders or assets.
- `pcli2_geometric_match` tool for asset similarity search.
