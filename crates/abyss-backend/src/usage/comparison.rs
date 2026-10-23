//! Validated, content-free comparison queries with explicit local-time bucket boundaries.

use crate::error::AppError;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonDimension {
    Agent,
    Model,
    Provider,
    Platform,
}

impl ComparisonDimension {
    pub const fn column(self) -> &'static str {
        match self {
            Self::Agent => "e.agent_name",
            Self::Model => "e.llm_model",
            Self::Provider => "e.llm_provider",
            Self::Platform => "d.platform",
        }
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ComparisonFilters {
    pub agent: Vec<String>,
    pub model: Vec<String>,
    pub provider: Vec<String>,
    pub platform: Vec<String>,
    pub session: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComparisonBucket {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComparisonQuery {
    pub group_by: Option<ComparisonDimension>,
    pub buckets: Vec<ComparisonBucket>,
    #[serde(default)]
    pub filters: ComparisonFilters,
}

impl ComparisonQuery {
    pub fn validate(&self) -> Result<(), AppError> {
        let invalid = |message: &str| AppError::validation(message.to_owned());
        let (Some(first), Some(last)) = (self.buckets.first(), self.buckets.last()) else {
            return Err(invalid("at least one time bucket is required"));
        };
        if self.buckets.len() > 400
            || last.end.signed_duration_since(first.start) > Duration::days(367)
        {
            return Err(invalid(
                "comparison range must not exceed 367 days or 400 buckets",
            ));
        }
        if self.buckets.iter().any(|bucket| bucket.start >= bucket.end)
            || self
                .buckets
                .windows(2)
                .any(|pair| pair[0].end != pair[1].start)
        {
            return Err(invalid(
                "time buckets must be ordered, contiguous and non-empty",
            ));
        }
        for values in [
            &self.filters.agent,
            &self.filters.model,
            &self.filters.provider,
            &self.filters.platform,
            &self.filters.session,
        ] {
            if values.len() > 100
                || values
                    .iter()
                    .any(|value| value.trim().is_empty() || value.len() > 4096)
            {
                return Err(invalid(
                    "filters must contain at most 100 non-empty values of at most 4096 bytes",
                ));
            }
        }
        let compared_values = self.group_by.map(|dimension| match dimension {
            ComparisonDimension::Agent => &self.filters.agent,
            ComparisonDimension::Model => &self.filters.model,
            ComparisonDimension::Provider => &self.filters.provider,
            ComparisonDimension::Platform => &self.filters.platform,
        });
        if compared_values.is_some_and(|values| !values.is_empty()) {
            return Err(invalid("a comparison dimension cannot also be a filter"));
        }
        Ok(())
    }

    pub fn encoded_buckets(&self) -> String {
        serde_json::Value::Array(
            self.buckets
                .iter()
                .map(|bucket| {
                    serde_json::json!({
                        "start_us": bucket.start.timestamp_micros(),
                        "end_us": bucket.end.timestamp_micros(),
                    })
                })
                .collect(),
        )
        .to_string()
    }

    pub fn encoded_filters(&self) -> Result<String, AppError> {
        serde_json::to_string(&self.filters)
            .map_err(|error| AppError::internal(format!("serialize comparison filters: {error}")))
    }
}

#[derive(diesel::QueryableByName, Serialize)]
pub struct ComparisonRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub bucket: i64,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub value: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub total_tokens: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub requests: i64,
}

#[derive(diesel::QueryableByName, Serialize)]
pub struct ComparisonOption {
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub dimension: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub value: String,
}

#[derive(Serialize)]
pub struct ComparisonResponse {
    pub rows: Vec<ComparisonRow>,
    pub options: Vec<ComparisonOption>,
}

#[cfg(test)]
mod tests {
    use super::ComparisonQuery;

    fn query(buckets: &serde_json::Value) -> ComparisonQuery {
        serde_json::from_value(serde_json::json!({"group_by":"agent","buckets":buckets}))
            .expect("valid wire query")
    }

    #[test]
    fn validates_dst_length_days_and_rejects_overlap_and_dimension_conflicts() {
        let mut value = query(&serde_json::json!([
            {"start":"2026-11-01T04:00:00Z","end":"2026-11-02T05:00:00Z"},
            {"start":"2026-11-02T05:00:00Z","end":"2026-11-03T05:00:00Z"}
        ]));
        assert!(value.validate().is_ok(), "a local day may be 25 hours");
        value.filters.agent.push("codex".to_owned());
        assert!(
            value.validate().is_err(),
            "comparison and filter are exclusive"
        );
        value.filters.agent.clear();
        value.buckets[1].start = value.buckets[0].start;
        assert!(
            value.validate().is_err(),
            "overlaps would double count events"
        );
        assert!(
            query(&serde_json::json!([])).validate().is_err(),
            "empty ranges are invalid"
        );
    }
}
