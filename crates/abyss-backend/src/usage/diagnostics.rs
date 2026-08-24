//! Opaque transport evidence retained for temporary Agent troubleshooting.
//!
//! The Backend deliberately does not understand or validate the captured HTTP
//! or WebSocket payload. It verifies that each capture is correlated with
//! events from the same ingest request; repository-level checks then enforce
//! that those events belong to one user, device, and session.

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;

use crate::error::AppError;

#[derive(Deserialize)]
pub struct IngestDiagnosticCapture {
    pub capture_id: String,
    pub captured_at: DateTime<Utc>,
    pub flow_id: String,
    pub event_ids: Vec<String>,
    pub collector_version: String,
    /// Agent Hook evidence stored without content-level validation.
    pub payload: Value,
}

impl IngestDiagnosticCapture {
    pub fn validate_event_correlation(
        &self,
        request_event_ids: &HashSet<&str>,
    ) -> Result<(), AppError> {
        if self.capture_id.trim().is_empty() {
            return Err(AppError::validation(
                "diagnostic capture_id must not be empty".to_owned(),
            ));
        }
        if self.event_ids.is_empty() {
            return Err(AppError::validation(
                "diagnostic capture must reference at least one event".to_owned(),
            ));
        }

        let mut unique_event_ids = HashSet::with_capacity(self.event_ids.len());
        for event_id in &self.event_ids {
            if !request_event_ids.contains(event_id.as_str()) {
                return Err(AppError::validation(
                    "diagnostic capture event_ids must reference events in the same ingest request"
                        .to_owned(),
                ));
            }
            if !unique_event_ids.insert(event_id.as_str()) {
                return Err(AppError::validation(
                    "diagnostic capture event_ids must be unique".to_owned(),
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use chrono::Utc;
    use serde_json::json;

    use super::IngestDiagnosticCapture;

    #[test]
    fn accepts_opaque_payload_without_content_validation() {
        let capture = capture(
            vec!["evt-test"],
            json!({"arbitrary": [1_i32, 2_i32, 3_i32]}),
        );

        assert!(
            capture
                .validate_event_correlation(&HashSet::from(["evt-test"]))
                .is_ok()
        );
    }

    #[test]
    fn rejects_event_from_outside_the_ingest_request() {
        let capture = capture(vec!["evt-other"], json!(null));

        let error = capture
            .validate_event_correlation(&HashSet::from(["evt-test"]))
            .expect_err("capture must not link to an event outside its ingest request");
        assert!(error.to_string().contains("same ingest request"));
    }

    #[test]
    fn rejects_duplicate_event_links() {
        let capture = capture(vec!["evt-test", "evt-test"], json!("raw evidence"));

        let error = capture
            .validate_event_correlation(&HashSet::from(["evt-test"]))
            .expect_err("capture event links must be unique");
        assert!(error.to_string().contains("must be unique"));
    }

    fn capture(event_ids: Vec<&str>, payload: serde_json::Value) -> IngestDiagnosticCapture {
        IngestDiagnosticCapture {
            capture_id: "diag-test".to_owned(),
            captured_at: Utc::now(),
            flow_id: "flow-test".to_owned(),
            event_ids: event_ids.into_iter().map(str::to_owned).collect(),
            collector_version: "test".to_owned(),
            payload,
        }
    }
}
