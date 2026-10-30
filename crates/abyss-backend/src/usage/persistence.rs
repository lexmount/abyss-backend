//! Backend-independent validation and normalization for persisted usage data.

use std::collections::HashSet;

#[cfg(feature = "postgres-es")]
use chrono::{DateTime, Utc};

use crate::error::AppError;

use super::{
    IngestEventsRequest,
    attachments::{ValidatedImageAttachment, validate_image_attachments},
};

pub fn validate_batch(
    request: &IngestEventsRequest,
    max_batch_size: usize,
) -> Result<Vec<Vec<ValidatedImageAttachment>>, AppError> {
    if request.events.len() > max_batch_size {
        return Err(AppError::validation(format!(
            "events batch size must be <= {max_batch_size}"
        )));
    }

    // Decode potentially expensive attachment content before a transaction and
    // its table locks are acquired by either storage implementation.
    let mut attachments = Vec::with_capacity(request.events.len());
    for event in &request.events {
        attachments.push(validate_image_attachments(&event.attachments)?);
    }
    if request.diagnostic_captures.len() > max_batch_size {
        return Err(AppError::validation(format!(
            "diagnostic captures batch size must be <= {max_batch_size}"
        )));
    }
    let request_event_ids = request
        .events
        .iter()
        .map(|event| event.event_id.as_str())
        .collect::<HashSet<_>>();
    let mut capture_ids = HashSet::with_capacity(request.diagnostic_captures.len());
    for capture in &request.diagnostic_captures {
        capture.validate_event_correlation(&request_event_ids)?;
        if !capture_ids.insert(capture.capture_id.as_str()) {
            return Err(AppError::validation(
                "diagnostic capture ids must be unique within one ingest request".to_owned(),
            ));
        }
    }
    Ok(attachments)
}

pub fn validate_event_type(value: &str) -> Result<String, AppError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "request" => Ok("request".to_owned()),
        "response" => Ok("response".to_owned()),
        _ => Err(AppError::validation(
            "event_type must be request or response".to_owned(),
        )),
    }
}

pub fn parse_group_by(raw: Option<&str>) -> Vec<String> {
    let parsed: Vec<_> = raw
        .unwrap_or("user,agent,provider,model")
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| match value {
            "employee" => "user".to_owned(),
            "llm_provider" => "provider".to_owned(),
            "llm_model" => "model".to_owned(),
            other => other.to_owned(),
        })
        .filter(|value| {
            matches!(
                value.as_str(),
                "day" | "user" | "device" | "agent" | "provider" | "model" | "event_type"
            )
        })
        .collect();

    if parsed.is_empty() {
        vec![
            "user".to_owned(),
            "agent".to_owned(),
            "provider".to_owned(),
            "model".to_owned(),
        ]
    } else {
        parsed
    }
}

/// Trimmed values compared exactly, such as Agent names, providers, models, or
/// session identifiers; empty matches every value.
pub fn exact_values_filter(values: &[String]) -> Vec<String> {
    distinct(
        values
            .iter()
            .filter_map(|value| non_empty(value))
            .map(normalize_text),
    )
}

fn distinct(values: impl Iterator<Item = String>) -> Vec<String> {
    let mut values: Vec<String> = values.collect();
    values.sort_unstable();
    values.dedup();
    values
}

pub fn normalize_text(value: &str) -> String {
    value.trim().to_owned()
}

pub fn non_empty(value: &str) -> Option<&str> {
    (!value.trim().is_empty()).then_some(value)
}

#[cfg(feature = "postgres-es")]
pub fn non_negative_count(value: i64) -> usize {
    usize::try_from(value.max(0_i64)).unwrap_or(usize::MAX)
}

#[cfg(feature = "postgres-es")]
pub const fn unix_epoch() -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp(0_i64, 0_u32).expect("unix epoch must be a valid timestamp")
}

#[cfg(any(feature = "postgres-es", test))]
pub fn escape_like_pattern(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        if matches!(character, '\\' | '%' | '_') {
            escaped.push('\\');
        }
        escaped.push(character);
    }
    escaped
}

pub fn normalize_limit(limit: Option<i64>, fallback: i64, maximum: i64) -> i64 {
    limit.unwrap_or(fallback).clamp(1_i64, maximum)
}

#[cfg(test)]
mod tests {
    use super::{escape_like_pattern, exact_values_filter, parse_group_by};

    #[test]
    fn shared_normalization_is_backend_independent() {
        // Agent names and providers are matched exactly as reported: Claude
        // Desktop stays apart from Claude Code and case is not folded.
        assert_eq!(
            exact_values_filter(&[
                " claude-desktop ".to_owned(),
                "claude-code".to_owned(),
                "Codex".to_owned(),
                "claude-code".to_owned(),
            ]),
            ["Codex", "claude-code", "claude-desktop"]
        );
        assert_eq!(parse_group_by(Some("day,llm_model")), ["day", "model"]);
        assert_eq!(escape_like_pattern(r"a%b_c\d"), r"a\%b\_c\\d");
    }
}
