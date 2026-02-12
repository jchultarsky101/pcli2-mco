use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;

pub type Result<T> = std::result::Result<T, AppError>;

#[derive(Debug)]
pub enum AppError {
    Anyhow(anyhow::Error),
    Json(serde_json::Error),
    Rpc(super::mcp::RpcErrorResponse),
}

impl From<anyhow::Error> for AppError {
    fn from(error: anyhow::Error) -> Self {
        AppError::Anyhow(error)
    }
}

impl From<serde_json::Error> for AppError {
    fn from(error: serde_json::Error) -> Self {
        AppError::Json(error)
    }
}

impl From<super::mcp::RpcErrorResponse> for AppError {
    fn from(error: super::mcp::RpcErrorResponse) -> Self {
        AppError::Rpc(error)
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            AppError::Anyhow(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            AppError::Json(e) => (StatusCode::BAD_REQUEST, format!("JSON error: {}", e)),
            AppError::Rpc(e) => {
                return (StatusCode::OK, Json(e)).into_response();
            }
        };

        let body = Json(json!({
            "error": {
                "code": status.as_u16(),
                "message": error_message,
            }
        }));

        (status, body).into_response()
    }
}
