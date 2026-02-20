use crate::AppState;
use crate::cli::{ARG_HOST, ARG_PORT, DEFAULT_HOST};
use crate::mcp::handle_mcp;
use crate::thumbnail::{ThumbnailCache, ThumbnailCacheConfig, default_cache_dir};
use anyhow::{Result, anyhow};
use axum::body::Body;
use axum::response::Response;
use axum::{
    BoxError, Router,
    error_handling::HandleErrorLayer,
    extract::{DefaultBodyLimit, Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
};
use chrono::Utc;
use clap::ArgMatches;
use http::header::{CACHE_CONTROL, CONTENT_TYPE};
use std::io::IsTerminal;
use std::sync::Arc;
use std::time::Duration;
use tower::{ServiceBuilder, timeout::TimeoutLayer};
use tracing::{debug, info, warn};

const SERVER_NAME: &str = "mcp-http-server";
const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
const MAX_REQUEST_BYTES: usize = 1_048_576;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const THUMBNAIL_TTL: Duration = Duration::from_secs(24 * 60 * 60); // 24 hours

pub async fn run_server(matches: &ArgMatches) -> Result<()> {
    let host = matches
        .get_one::<String>(ARG_HOST)
        .map(String::as_str)
        .unwrap_or(DEFAULT_HOST);
    let port = *matches
        .get_one::<u16>(ARG_PORT)
        .ok_or_else(|| anyhow!("missing port"))?;

    print_banner();

    // Initialize thumbnail cache
    let thumbnail_cache = match default_cache_dir() {
        Ok(cache_dir) => {
            let config = ThumbnailCacheConfig::new(cache_dir, THUMBNAIL_TTL, host, port);
            match ThumbnailCache::new(config) {
                Ok(cache) => {
                    info!("Thumbnail cache initialized at {:?}", cache.cache_dir());
                    Some(cache)
                }
                Err(err) => {
                    warn!("Failed to initialize thumbnail cache: {}", err);
                    None
                }
            }
        }
        Err(err) => {
            warn!("Could not determine thumbnail cache directory: {}", err);
            None
        }
    };

    let state = AppState {
        server_name: SERVER_NAME.to_string(),
        server_version: APP_VERSION.to_string(),
        thumbnail_cache: Arc::new(thumbnail_cache),
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/mcp", axum::routing::post(handle_mcp))
        .route("/thumbnail/:cache_key", get(serve_thumbnail))
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
    info!("Press Ctrl+C to stop the server");

    // Log the server start time
    info!(
        "Server started at {}",
        Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
    );

    axum::serve(
        tokio::net::TcpListener::bind(&bind_addr).await?,
        app.into_make_service(),
    )
    .await?;

    Ok(())
}

async fn health() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn serve_thumbnail(
    Path(cache_key): Path<String>,
    state: State<AppState>,
) -> impl IntoResponse {
    let Some(cache) = state.thumbnail_cache.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "Thumbnail cache not available",
        )
            .into_response();
    };

    match cache.load_thumbnail(&cache_key) {
        Ok(data) => {
            let mut response = Response::new(Body::from(data));
            response
                .headers_mut()
                .insert(CONTENT_TYPE, "image/png".parse().unwrap());
            response
                .headers_mut()
                .insert(CACHE_CONTROL, "public, max-age=3600".parse().unwrap());
            response
        }
        Err(err) => {
            debug!("Failed to serve thumbnail {}: {}", cache_key, err);
            (
                StatusCode::NOT_FOUND,
                format!("Thumbnail not found: {}", err),
            )
                .into_response()
        }
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

    // Center the version string to match the width of the banner
    let version_text = format!("Version {}", APP_VERSION);
    let banner_width: usize = 67; // Width of the banner lines
    let text_len = version_text.len();
    let padding = if text_len < banner_width {
        (banner_width - text_len) / 2
    } else {
        0
    };
    let centered_version = format!(
        "{}{}{}",
        " ".repeat(padding),
        version_text,
        " ".repeat(banner_width - text_len - padding)
    );

    if use_color {
        println!("{}", gradient_line(&centered_version));
    } else {
        println!("{}", centered_version);
    }
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
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt; // for `oneshot`

    #[tokio::test]
    async fn test_health_endpoint() {
        let app = Router::new().route("/health", get(health));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&body[..], b"ok");
    }

    #[test]
    fn test_lerp() {
        // Test edge cases
        assert_eq!(lerp(0, 100, 0.0), 0);
        assert_eq!(lerp(0, 100, 1.0), 100);
        assert_eq!(lerp(0, 100, 0.5), 50);

        // Test with different values
        assert_eq!(lerp(50, 150, 0.5), 100);
        assert_eq!(lerp(10, 20, 0.3), 13); // Approximation due to integer conversion
    }

    #[test]
    fn test_gradient_line() {
        let input = "test";
        let result = gradient_line(input);
        // Check that the result contains ANSI color codes
        assert!(result.contains("\x1b[38;2;")); // Start of RGB color code
        assert!(result.ends_with("\x1b[0m")); // Reset code at the end
    }
}
