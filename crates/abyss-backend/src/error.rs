//! Shared error types for HTTP and database operations.

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("configuration error: {0}")]
    Config(String),
    #[error("database error: {0}")]
    Database(#[from] diesel::result::Error),
    #[error("connection pool error: {0}")]
    Pool(#[from] r2d2::Error),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    #[error("unavailable: {0}")]
    Unavailable(String),
    #[error("validation error: {0}")]
    Validation(String),
    #[error("internal error: {0}")]
    Internal(String),
}

impl AppError {
    pub const fn config(message: String) -> Self {
        Self::Config(message)
    }

    pub const fn validation(message: String) -> Self {
        Self::Validation(message)
    }

    pub const fn not_found(message: String) -> Self {
        Self::NotFound(message)
    }

    pub const fn unauthorized(message: String) -> Self {
        Self::Unauthorized(message)
    }

    pub const fn unavailable(message: String) -> Self {
        Self::Unavailable(message)
    }

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
