//! HTTP boundary for standalone Agent event ingestion and queries.
//!
//! The health endpoints are public, while every event, attachment, summary,
//! timeline, and search endpoint authenticates the deployment bearer token.
//! Storage implementation details remain behind the configured backend trait.

use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, HeaderValue, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::Serialize;
use tower_http::trace::TraceLayer;
use uuid::Uuid;

use crate::{
    error::AppError,
    identity::IdentityAuthenticator,
    search::{SessionSearchQuery, SessionSearchResponse},
    storage::StorageBackend,
    usage::{
        IngestEventsRequest, IngestEventsResponse, RawEventsQuery, RawEventsResponse,
        SessionTimelineResponse, SummaryFields, SummaryQuery, TokenUsageSummaryResponse,
    },
};

const MAX_INGEST_REQUEST_BODY_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone)]
/// Cloneable dependencies and request limits shared by all Axum handlers.
pub struct AppState {
    /// Deployment label exposed by the informational root endpoint.
    pub environment: String,
    /// Maximum events and diagnostic captures accepted per ingest request.
    pub max_ingest_batch_size: usize,
    /// Default upper bound for summary aggregation rows.
    pub summary_scan_limit: i64,
    /// Default raw-event page size.
    pub default_page_size: i64,
    /// Deployment-wide bearer-token validator.
    pub identity: IdentityAuthenticator,
    /// Compile-time-selected event storage and full-text search implementation.
    pub storage: std::sync::Arc<dyn StorageBackend>,
}

/// Builds the complete HTTP router for one backend process.
///
/// The ingest body limit is intentionally route-local so future endpoints do
/// not inherit a large request allowance by accident.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(root))
        .route("/healthz", get(health))
        .route("/readyz", get(ready))
        .route(
            "/v1/agent-usage/events",
            post(ingest_events)
                .get(raw_events)
                .layer(DefaultBodyLimit::max(MAX_INGEST_REQUEST_BODY_BYTES)),
        )
        .route(
            "/v1/agent-usage/attachments/{attachment_id}",
            get(image_attachment),
        )
        .route("/v1/agent-usage/summary", get(usage_summary))
        .route("/v1/agent-usage/search", get(session_search))
        .route(
            "/v1/agent-usage/sessions/{session_pk}",
            get(session_timeline),
        )
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn root(State(state): State<AppState>) -> Json<RootResponse> {
    Json(RootResponse {
        service: "abyss-backend",
        environment: state.environment,
        status: "ok",
    })
}

async fn health() -> Json<ServiceStatus> {
    Json(ServiceStatus {
        service: "abyss-backend",
        status: "ok",
    })
}

async fn ready(State(state): State<AppState>) -> Result<Json<ServiceStatus>, AppError> {
    // Readiness checks the authoritative database. In the PostgreSQL profile,
    // a temporary failure of the optional search projection must not remove
    // ingestion capacity.
    state.storage.ready().await?;
    Ok(Json(ServiceStatus {
        service: "abyss-backend",
        status: "ok",
    }))
}

async fn ingest_events(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<IngestEventsRequest>,
) -> Result<Json<IngestEventsResponse>, AppError> {
    let user_id = state.identity.authenticate(&headers)?;
    let max_batch_size = state.max_ingest_batch_size;
    let response = state
        .storage
        .ingest_events(user_id, request, max_batch_size)
        .await?;
    Ok(Json(response))
}

async fn usage_summary(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<SummaryQuery>,
) -> Result<Response, AppError> {
    let user_id = state.identity.authenticate(&headers)?;
    let scan_limit = state.summary_scan_limit;
    let fields = query.fields.unwrap_or(SummaryFields::Full);
    let response = state
        .storage
        .usage_summary(user_id, query, scan_limit)
        .await?;
    if fields == SummaryFields::TokenUsage {
        return Ok(Json(TokenUsageSummaryResponse::from_summary(response)).into_response());
    }
    Ok(Json(response).into_response())
}

async fn session_search(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<SessionSearchQuery>,
) -> Result<Json<SessionSearchResponse>, AppError> {
    let user_id = state.identity.authenticate(&headers)?;
    Ok(Json(state.storage.session_search(user_id, query).await?))
}

async fn session_timeline(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_pk): Path<Uuid>,
) -> Result<Json<SessionTimelineResponse>, AppError> {
    let user_id = state.identity.authenticate(&headers)?;
    let response = state.storage.session_timeline(user_id, session_pk).await?;
    Ok(Json(response))
}

async fn image_attachment(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(attachment_id): Path<Uuid>,
) -> Result<Response, AppError> {
    let user_id = state.identity.authenticate(&headers)?;
    let attachment = state
        .storage
        .image_attachment(user_id, attachment_id)
        .await?;

    let mut response = Bytes::from(attachment.content).into_response();
    let response_headers = response.headers_mut();
    response_headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(attachment.media_type.as_str()),
    );
    response_headers.insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!(
            "inline; filename=\"abyss-image.{extension}\"",
            extension = attachment.media_type.file_extension()
        ))
        .map_err(|error| {
            AppError::internal(format!("build image content-disposition header: {error}"))
        })?,
    );
    response_headers.insert(
        header::ETAG,
        HeaderValue::from_str(&format!("\"{}\"", attachment.sha256))
            .map_err(|error| AppError::internal(format!("build image etag header: {error}")))?,
    );
    response_headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-store"),
    );
    // Attachments may contain sensitive conversation context. Disallow MIME
    // sniffing, shared caching, and cross-origin embedding even for valid images.
    response_headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    response_headers.insert(
        "cross-origin-resource-policy",
        HeaderValue::from_static("same-origin"),
    );
    Ok(response)
}

async fn raw_events(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<RawEventsQuery>,
) -> Result<Json<RawEventsResponse>, AppError> {
    let user_id = state.identity.authenticate(&headers)?;
    let default_page_size = state.default_page_size;
    let response = state
        .storage
        .raw_events(user_id, query, default_page_size)
        .await?;
    Ok(Json(response))
}

#[derive(Serialize)]
struct RootResponse {
    service: &'static str,
    environment: String,
    status: &'static str,
}

#[derive(Serialize)]
struct ServiceStatus {
    service: &'static str,
    status: &'static str,
}
