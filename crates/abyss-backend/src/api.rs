//! HTTP boundary for standalone Agent event ingestion and queries.
//!
//! The health endpoints are public, while every event, attachment, summary,
//! timeline, and search endpoint authenticates the deployment bearer token.
//! Diesel is synchronous, so all database access is moved onto Tokio's blocking
//! pool through [`run_db`] rather than running on asynchronous executor threads.

use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, HeaderValue, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use diesel::PgConnection;
use serde::Serialize;
use tokio::task;
use tower_http::trace::TraceLayer;
use uuid::Uuid;

use crate::{
    db::{self, DbPool},
    error::AppError,
    identity::IdentityAuthenticator,
    search::{
        SearchService, SessionSearchQuery, SessionSearchResponse, outbox::SearchOutboxRepository,
    },
    usage::{
        IngestEventsRequest, IngestEventsResponse, RawEventsQuery, RawEventsResponse,
        SessionTimelineResponse, SummaryFields, SummaryQuery, TokenUsageSummaryResponse,
        repository as usage_repository,
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
    /// Optional full-text search service.
    pub search: Option<SearchService>,
    /// PostgreSQL connection pool used by request handlers.
    pub pool: DbPool,
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
    // Readiness checks the source of truth only. Search is an optional derived
    // service and its temporary failure must not remove ingestion capacity.
    run_db(state, db::check_ready).await?;
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
    let response = run_db(state, move |connection| {
        usage_repository::ingest_events(connection, &request, user_id, max_batch_size)
    })
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
    let response = run_db(state, move |connection| {
        usage_repository::usage_summary(connection, &query, user_id, scan_limit)
    })
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
    let search = state
        .search
        .clone()
        .ok_or_else(|| AppError::unavailable("session search is not configured".to_owned()))?;
    let execution = search.search(user_id, query).await?;
    let session_ids = execution.session_ids();
    // Elasticsearch contains only a bounded search projection. Authoritative
    // session/device details are reloaded from PostgreSQL under the owner scope.
    let details = run_db(state, move |connection| {
        SearchOutboxRepository::session_details(connection, user_id, &session_ids)
    })
    .await?;
    Ok(Json(execution.hydrate(details)))
}

async fn session_timeline(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_pk): Path<Uuid>,
) -> Result<Json<SessionTimelineResponse>, AppError> {
    let user_id = state.identity.authenticate(&headers)?;
    let response = run_db(state, move |connection| {
        usage_repository::session_timeline(connection, user_id, session_pk)
    })
    .await?;
    Ok(Json(response))
}

async fn image_attachment(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(attachment_id): Path<Uuid>,
) -> Result<Response, AppError> {
    let user_id = state.identity.authenticate(&headers)?;
    let attachment = run_db(state, move |connection| {
        usage_repository::image_attachment(connection, user_id, attachment_id)
    })
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
    let response = run_db(state, move |connection| {
        usage_repository::raw_events(connection, &query, user_id, default_page_size)
    })
    .await?;
    Ok(Json(response))
}

async fn run_db<T, F>(state: AppState, task_fn: F) -> Result<T, AppError>
where
    T: Send + 'static,
    F: FnOnce(&mut PgConnection) -> Result<T, AppError> + Send + 'static,
{
    // Diesel and r2d2 are blocking APIs. Acquiring the pool connection inside
    // spawn_blocking also keeps pool contention off Tokio worker threads.
    task::spawn_blocking(move || {
        let mut connection = state.pool.get()?;
        task_fn(&mut connection)
    })
    .await
    .map_err(|error| AppError::internal(format!("database task failed: {error}")))?
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
