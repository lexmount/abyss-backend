//! Shared error types and their safe HTTP representation.
//!
//! Client-actionable failures retain their messages. Configuration, database,
//! pool, and internal failures are logged server-side and deliberately reduced
//! to a generic response so implementation details and secrets are not exposed.

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;

#[derive(Debug, thiserror::Error)]
/// Error vocabulary shared across configuration, HTTP, and persistence layers.
pub enum AppError {
    /// Invalid process configuration detected during startup.
    #[error("configuration error: {0}")]
    Config(String),
    /// Diesel query or transaction failure.
    #[error("database error: {0}")]
    Database(#[from] diesel::result::Error),
    /// PostgreSQL connection-pool failure.
    #[error("connection pool error: {0}")]
    Pool(#[from] r2d2::Error),
    /// Authenticated resource does not exist.
    #[error("not found: {0}")]
    NotFound(String),
    /// Missing or invalid deployment bearer token.
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    /// Optional dependency is disabled or temporarily unavailable.
    #[error("unavailable: {0}")]
    Unavailable(String),
    /// Request data violates an API contract.
    #[error("validation error: {0}")]
    Validation(String),
    /// Unexpected application invariant or task failure.
    #[error("internal error: {0}")]
    Internal(String),
}

impl AppError {
    /// Constructs a configuration error.
    pub const fn config(message: String) -> Self {
        Self::Config(message)
    }

    /// Constructs a request validation error.
    pub const fn validation(message: String) -> Self {
        Self::Validation(message)
    }

    /// Constructs a resource-not-found error.
    pub const fn not_found(message: String) -> Self {
        Self::NotFound(message)
    }

    /// Constructs an authentication error.
    pub const fn unauthorized(message: String) -> Self {
        Self::Unauthorized(message)
    }

    /// Constructs a dependency-unavailable error.
    pub const fn unavailable(message: String) -> Self {
        Self::Unavailable(message)
    }

    /// Constructs an unexpected internal error.
    pub const fn internal(message: String) -> Self {
        Self::Internal(message)
    }

    const fn status_code(&self) -> StatusCode {
        match self {
            Self::Validation(_) => StatusCode::BAD_REQUEST,
            Self::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            Self::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Config(_) | Self::Database(_) | Self::Pool(_) | Self::Internal(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let error = match &self {
            Self::Config(_) | Self::Database(_) | Self::Pool(_) | Self::Internal(_) => {
                tracing::error!(error = %self, "internal error");
                "internal server error".to_owned()
            }
            Self::NotFound(_)
            | Self::Unauthorized(_)
            | Self::Unavailable(_)
            | Self::Validation(_) => self.to_string(),
        };
        let body = ErrorResponse { error };
        (status, Json(body)).into_response()
    }
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

#[cfg(test)]
mod tests {
    use axum::{body::to_bytes, http::StatusCode, response::IntoResponse};

    use super::AppError;

    async fn response_body(error: AppError) -> (StatusCode, String) {
        let response = error.into_response();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("error response body should be readable");
        let body = String::from_utf8(bytes.to_vec()).expect("error response body should be utf-8");
        (status, body)
    }

    #[tokio::test]
    async fn internal_errors_hide_details_from_clients() {
        let (status, body) = response_body(AppError::internal(
            "driver detail: table llm_usage_events".to_owned(),
        ))
        .await;

        assert_eq!(
            status,
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal errors should return HTTP 500"
        );
        assert_eq!(
            body, r#"{"error":"internal server error"}"#,
            "internal errors should use a generic response body"
        );
    }

    #[tokio::test]
    async fn validation_errors_keep_client_messages() {
        let (status, body) =
            response_body(AppError::validation("bad request body".to_owned())).await;

        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "validation errors should return HTTP 400"
        );
        assert!(
            body.contains("bad request body"),
            "validation errors should keep actionable client messages"
        );
    }
}
