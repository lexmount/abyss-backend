//! Regression coverage for JSON shape, filter behavior and request isolation.

use std::{
    io::{self, Write},
    sync::{Arc, Mutex},
};

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
    middleware,
    routing::get,
};
use serde_json::Value;
use tower::ServiceExt;
use tracing_subscriber::{EnvFilter, fmt::format::JsonFields};

use super::{LogFormat, json::JsonFormat, log_request};

#[derive(Clone)]
struct Buffer(Arc<Mutex<Vec<u8>>>);

impl Write for Buffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .map_err(|_| io::Error::other("poisoned log buffer"))?
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Buffer {
    fn records(&self) -> Vec<Value> {
        let data = self.0.lock().expect("log buffer");
        String::from_utf8_lossy(&data)
            .lines()
            .map(|line| serde_json::from_str(line).expect("one JSON object per line"))
            .collect()
    }
}

#[test]
fn log_format_is_explicit() {
    assert!(
        matches!(LogFormat::parse("text"), Ok(LogFormat::Text)),
        "text is supported"
    );
    assert!(
        matches!(LogFormat::parse("json"), Ok(LogFormat::Json)),
        "JSON is supported"
    );
    for value in ["", "JSON", "pretty", " json"] {
        assert!(
            LogFormat::parse(value).is_err(),
            "invalid format must fail: {value}"
        );
    }
}

#[test]
fn json_preserves_types_escaping_and_identity() {
    let buffer = Buffer(Arc::default());
    let writer = buffer.clone();
    let subscriber = tracing_subscriber::fmt()
        .fmt_fields(JsonFields::new())
        .event_format(JsonFormat::new("test-service", "test"))
        .with_writer(move || writer.clone())
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        tracing::info!(
            count = 12_u64,
            signed = -2_i64,
            fraction = 1.5_f64,
            enabled = true,
            "line one\nline two"
        );
    });
    let records = buffer.records();
    assert_eq!(
        records.len(),
        1,
        "embedded newlines must stay in one JSON record"
    );
    let record = &records[0];
    assert_eq!(
        record["message"], "line one\nline two",
        "message escaping round trips"
    );
    assert_eq!(record["count"], 12_u64, "counts remain numeric");
    assert_eq!(record["signed"], -2_i64, "signed values remain numeric");
    assert_eq!(
        record["fraction"], 1.5_f64,
        "fractional values remain numeric"
    );
    assert_eq!(record["enabled"], true, "boolean values remain boolean");
    assert_eq!(
        record["service"], "test-service",
        "service is attached to every event"
    );
    assert_eq!(
        record["environment"], "test",
        "environment is attached to every event"
    );
    assert!(record["timestamp"].is_string(), "timestamp is present");
}

#[tokio::test(flavor = "current_thread")]
async fn request_context_survives_filters_without_secrets() {
    for filter in ["info", "warn", "error"] {
        check_filter(filter).await;
    }
}

async fn check_filter(filter: &str) {
    let buffer = Buffer(Arc::default());
    let writer = buffer.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(filter))
        .fmt_fields(JsonFields::new())
        .event_format(JsonFormat::new("test-service", "test"))
        .with_writer(move || writer.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    let app = Router::new()
        .route(
            "/capability/{token}",
            get(|| async {
                tokio::task::yield_now().await;
                tracing::error!(error_kind = "dependency", "safe diagnostic");
                StatusCode::SERVICE_UNAVAILABLE
            }),
        )
        .route("/healthz", get(|| async { StatusCode::OK }))
        .layer(middleware::from_fn(log_request));
    let request = || {
        Request::builder()
            .uri("/capability/secret-path?code=secret-query")
            .header("authorization", "Bearer secret-auth")
            .header("cookie", "session=secret-cookie")
            .header("x-request-id", "secret-client-id")
            .body(Body::empty())
            .expect("request")
    };
    let (first, second) = tokio::join!(
        app.clone().oneshot(request()),
        app.clone().oneshot(request())
    );
    let first = first.expect("first response");
    let second = second.expect("second response");
    let first_id = first.headers()["x-request-id"].to_str().expect("ID");
    let second_id = second.headers()["x-request-id"].to_str().expect("ID");
    assert_ne!(
        first_id, second_id,
        "concurrent requests must have separate IDs"
    );
    assert_request_records(&buffer, [first_id, second_id], filter);
    app.clone()
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .expect("health request"),
        )
        .await
        .expect("health response");
    assert_eq!(
        buffer.records().len(),
        4,
        "successful probes are suppressed"
    );
    app.oneshot(
        Request::builder()
            .uri("/unknown-secret")
            .body(Body::empty())
            .expect("unknown request"),
    )
    .await
    .expect("unknown response");
    let records = buffer.records();
    if filter != "error" {
        assert_eq!(
            records.last().expect("404")["route"],
            "<unmatched>",
            "unknown paths are never recorded"
        );
    }
    assert!(
        !serde_json::to_string(&records)
            .expect("serialized records")
            .contains("secret"),
        "sensitive request data must never enter logs"
    );
}

fn assert_request_records(buffer: &Buffer, ids: [&str; 2], filter: &str) {
    for id in ids {
        let records = buffer.records();
        let matching: Vec<_> = records
            .iter()
            .filter(|record| record["request_id"] == id)
            .collect();
        assert_eq!(
            matching.len(),
            2,
            "inner errors and completion share the ID at {filter}"
        );
        assert!(
            matching
                .iter()
                .all(|record| record["route"] == "/capability/{token}"),
            "only route templates are recorded"
        );
        let complete = matching
            .iter()
            .find(|record| record["message"] == "request completed")
            .expect("completion");
        assert_eq!(complete["status"], 503_u16, "status is numeric");
        assert!(complete["duration_ms"].is_u64(), "duration is numeric");
    }
}
