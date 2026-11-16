//! Standalone operational logging; no product or cloud authentication dependency.

mod json;
#[cfg(test)]
mod tests;

use std::{error::Error, time::Instant};

use axum::{
    extract::{MatchedPath, Request},
    http::HeaderValue,
    middleware::Next,
    response::Response,
};
use tracing::Instrument;
use tracing_subscriber::{EnvFilter, fmt::format::JsonFields};
use uuid::Uuid;

use self::json::JsonFormat;

enum LogFormat {
    Text,
    Json,
}

impl LogFormat {
    fn parse(value: &str) -> Result<Self, &'static str> {
        match value {
            "text" => Ok(Self::Text),
            "json" => Ok(Self::Json),
            _ => Err("ABYSS_LOG_FORMAT must be text or json"),
        }
    }
}

/// Install stdout logging, retaining the existing `RUST_LOG` filter precedence.
///
/// # Errors
/// Returns an error for an invalid format or an already installed subscriber.
pub fn init(
    service: &'static str,
    environment: &str,
    log_level: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let format = match std::env::var("ABYSS_LOG_FORMAT") {
        Ok(value) => LogFormat::parse(&value)?,
        Err(std::env::VarError::NotPresent) => LogFormat::Text,
        Err(error) => return Err(error.into()),
    };
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(log_level));
    let subscriber = tracing_subscriber::fmt().with_env_filter(filter);
    match format {
        LogFormat::Text => subscriber.try_init()?,
        LogFormat::Json => subscriber
            .fmt_fields(JsonFields::new())
            .event_format(JsonFormat::new(service, environment))
            .try_init()?,
    }
    Ok(())
}

/// Correlate HTTP diagnostics without recording credentials or dynamic URL data.
///
/// The response ID is generated locally. `MatchedPath` is the router template,
/// including for capability routes. Duration ends when response headers are ready.
///
/// # Panics
/// Panics only if UUID encoding stops producing a valid HTTP header value.
pub async fn log_request(request: Request, next: Next) -> Response {
    let request_id = Uuid::now_v7().to_string();
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map_or("<unmatched>", MatchedPath::as_str);
    let probe = matches!(
        route,
        "/healthz" | "/readyz" | "/api/healthz" | "/api/readyz"
    );
    // This span emits no event. ERROR keeps diagnostic context available even
    // when the event filter is raised to WARN or ERROR during an incident.
    let span = tracing::error_span!("http_request", %request_id, method = %request.method(), route);
    async move {
        let started = Instant::now();
        let mut response = next.run(request).await;
        let status = response.status().as_u16();
        let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        if status >= 500 {
            tracing::error!(status, duration_ms, "request completed");
        } else if status >= 400 {
            tracing::warn!(status, duration_ms, "request completed");
        } else if !probe {
            tracing::info!(status, duration_ms, "request completed");
        }
        response.headers_mut().insert(
            "x-request-id",
            HeaderValue::from_str(&request_id).expect("UUID header"),
        );
        response
    }
    .instrument(span)
    .await
}
