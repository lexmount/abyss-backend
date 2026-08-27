//! Backend-independent validation and normalization for persisted usage data.

use std::collections::HashSet;

#[cfg(feature = "postgres-es")]
use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};

use crate::error::AppError;

use super::{
    IngestEventsRequest, IngestUsageEvent,
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
        validate_event(event)?;
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

pub fn validate_event(event: &IngestUsageEvent) -> Result<(), AppError> {
    require_non_empty(&event.event_id, "event_id")?;
    require_non_empty(&event.device.host_name, "device.host_name")?;
    require_non_empty(&event.device.platform, "device.platform")?;
    require_non_empty(&event.agent.name, "agent.name")?;
    require_non_empty(&event.session_id, "session_id")?;
    require_non_empty(&event.llm.provider, "llm.provider")?;
    require_non_empty(&event.llm.model, "llm.model")?;
    if event.turn_index < 1_i32 {
        return Err(AppError::validation("turn_index must be >= 1".to_owned()));
    }
    let tokens = normalized_tokens(event)?;
    if tokens.total == 0 {
        return Ok(());
    }
    let visible_tokens = tokens
        .input
        .saturating_add(tokens.output)
        .saturating_add(tokens.reasoning);
    if tokens.total < visible_tokens {
        tracing::debug!(
            event_id = %event.event_id,
            "provider total_tokens is lower than visible token components"
        );
    }
    Ok(())
}

fn require_non_empty(value: &str, field: &str) -> Result<(), AppError> {
    if value.trim().is_empty() {
        return Err(AppError::validation(format!("{field} is required")));
    }
    Ok(())
}

pub struct NormalizedTokens {
    pub input: i64,
    pub output: i64,
    pub cache_read: i64,
    pub cache_write: i64,
    pub reasoning: i64,
    pub total: i64,
}

pub fn normalized_tokens(event: &IngestUsageEvent) -> Result<NormalizedTokens, AppError> {
    let input = normalize_token(event.token_usage.input_tokens, "input_tokens")?;
    let output = normalize_token(event.token_usage.output_tokens, "output_tokens")?;
    let cache_read = normalize_token(event.token_usage.cache_read_tokens, "cache_read_tokens")?;
    let cache_write = normalize_token(event.token_usage.cache_write_tokens, "cache_write_tokens")?;
    let reasoning = normalize_token(event.token_usage.reasoning_tokens, "reasoning_tokens")?;
    // Preserve an explicit provider total because provider accounting may not
    // equal the visible component sum; derive only when the field is absent.
    let total = match event.token_usage.total_tokens {
        Some(value) => normalize_token(Some(value), "total_tokens")?,
        None => input
            .saturating_add(output)
            .saturating_add(cache_read)
            .saturating_add(cache_write)
            .saturating_add(reasoning),
    };

    Ok(NormalizedTokens {
        input,
        output,
        cache_read,
        cache_write,
        reasoning,
        total,
    })
}

fn normalize_token(value: Option<i64>, field: &str) -> Result<i64, AppError> {
    let value = value.unwrap_or(0);
    if value < 0 {
        return Err(AppError::validation(format!("{field} must be >= 0")));
    }
    Ok(value)
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

pub fn normalize_slug(value: &str) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .chars()
        .map(|character| match character {
            '_' | ' ' => '-',
            other => other,
        })
        .collect()
}

pub fn canonical_agent_name(value: &str) -> String {
    let normalized = normalize_slug(value);
    match normalized.as_str() {
        "claude" | "claude-desktop" => "claude-code".to_owned(),
        _ => normalized,
    }
}

pub fn agent_name_filter_values(value: &str) -> Vec<String> {
    let canonical = canonical_agent_name(value);
    if canonical == "claude-code" {
        vec![
            "claude-code".to_owned(),
            "claude".to_owned(),
            "claude-desktop".to_owned(),
        ]
    } else {
        vec![canonical]
    }
}

pub fn normalize_text(value: &str) -> String {
    value.trim().to_owned()
}

pub fn normalized_optional(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
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

pub fn sha256_bytes(value: &str) -> Vec<u8> {
    Sha256::digest(value.as_bytes()).to_vec()
}

pub struct TurnIdentity {
    pub kind: TurnIdentityKind,
    pub value: String,
}

#[derive(Clone, Copy)]
pub enum TurnIdentityKind {
    CodexTurnId,
    ClaudeTurnId,
    ResponseId,
    MessageId,
    RequestHash,
}

impl TurnIdentityKind {
    pub const fn metadata_key(self) -> &'static str {
        match self {
            Self::CodexTurnId => "codex_turn_id",
            Self::ClaudeTurnId => "claude_turn_id",
            Self::ResponseId => "response_id",
            Self::MessageId => "message_id",
            Self::RequestHash => "request_hash",
        }
    }
}

pub fn turn_identity(event: &IngestUsageEvent) -> Option<TurnIdentity> {
    turn_identity_from_metadata(&event.metadata)
}

pub fn turn_identity_from_metadata(metadata: &serde_json::Value) -> Option<TurnIdentity> {
    // Prefer Agent-level identities because one user turn may contain several
    // provider round trips.
    [
        TurnIdentityKind::CodexTurnId,
        TurnIdentityKind::ClaudeTurnId,
        TurnIdentityKind::ResponseId,
        TurnIdentityKind::MessageId,
        TurnIdentityKind::RequestHash,
    ]
    .into_iter()
    .find_map(|kind| {
        metadata_string(metadata, kind.metadata_key()).map(|value| TurnIdentity { kind, value })
    })
}

fn metadata_string(metadata: &serde_json::Value, key: &str) -> Option<String> {
    metadata
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

pub const fn choose_turn_index(
    requested_turn_index: i32,
    existing_turn_index: Option<i32>,
    next_turn_index: i32,
) -> i32 {
    if let Some(existing_turn_index) = existing_turn_index {
        return existing_turn_index;
    }
    if requested_turn_index < next_turn_index {
        next_turn_index
    } else {
        requested_turn_index
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        TurnIdentityKind, canonical_agent_name, choose_turn_index, escape_like_pattern,
        parse_group_by, turn_identity_from_metadata,
    };

    #[test]
    fn shared_normalization_is_backend_independent() {
        assert_eq!(canonical_agent_name(" Claude Desktop "), "claude-code");
        assert_eq!(parse_group_by(Some("day,llm_model")), ["day", "model"]);
        assert_eq!(escape_like_pattern(r"a%b_c\d"), r"a\%b\_c\\d");
    }

    #[test]
    fn turn_identity_and_restart_resolution_are_stable() {
        let identity = turn_identity_from_metadata(&json!({
            "response_id": "response",
            "codex_turn_id": "turn"
        }))
        .expect("turn identity should be extracted");
        assert!(matches!(identity.kind, TurnIdentityKind::CodexTurnId));
        assert_eq!(identity.value, "turn");
        assert_eq!(choose_turn_index(1_i32, None, 4_i32), 4_i32);
        assert_eq!(choose_turn_index(1_i32, Some(2_i32), 4_i32), 2_i32);
    }
}
