# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project adheres to Semantic Versioning.

## [Unreleased]

### Added

- Thumbnail cache for efficient image serving via HTTP URLs (`/thumbnail/:cache_key`)
- New tool `pcli2_thumbnail_cache_cleanup` to remove expired thumbnails
- Disk-based cache at `~/.pcli2-mcp/thumbnails/` with configurable TTL (default 24 hours)
- HTML response for `pcli2_asset_thumbnail` with embedded image URL instead of base64 data

### Changed

- `pcli2_asset_thumbnail` now returns HTML with image URL instead of base64-encoded image data
- AppState extended to include optional thumbnail cache reference

## [0.1.10] - 2026-02-19

### Fixed

- Verified correct MIME type handling for thumbnail downloads (`image/png` with PNG magic byte validation).

## [0.1.9] - 2026-02-12

### Added

- Comprehensive unit and integration tests for all modules (45+ unit tests, 10+ integration tests).
- Server start time indicator in logs for better operational awareness.
- Chrono dependency for time formatting capabilities.

### Changed

- Improved banner formatting with centered version display.
- Changed default log level from debug to info for cleaner output.
- Added clear instruction to press Ctrl+C to stop the server.
- Modularized codebase into separate files (cli, error, mcp, pcli2, server) for better maintainability.
- Fixed tracing initialization to prevent duplicate subscriber errors.

## [0.1.8] - 2026-02-11

### Added

- Request body size limits and timeouts for MCP HTTP requests.
- PCLI2 command timeouts and max output size enforcement.

### Changed

- Stricter JSON-RPC request validation for required fields and types.
- Tool schema construction refactored for consistency and reuse.

## [0.1.7] - 2026-02-11

### Added

- Added MCP tool for `pcli2 asset metadata delete`.

### Changed

- Metadata delete tool now accepts multiple names and optional `format`.
- Banner now displays the running `pcli2-mcp` version.

## [0.1.6] - 2026-02-11

### Changed

- Improved MCP request logging with clearer tool context.

## [0.1.5] - 2026-02-11

### Changed

- Logged PCLI2 commands are now shell-escaped for copy/paste (with a compact emoji prefix).

## [0.1.4] - 2026-02-11

### Added

- Oranda documentation publishing via GitHub Pages.
- Oranda configuration and docs workflow.
- Quick Start validation example for listing MCP tools with curl.

### Changed

- README restructured to introduce core concepts and step-by-step setup.
- Installation guidance now prefers pre-built binaries and references source builds as optional.

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
