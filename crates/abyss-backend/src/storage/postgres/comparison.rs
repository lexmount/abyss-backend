//! Database aggregation for comparison charts; event content never leaves storage.

use crate::{
    error::AppError,
    usage::comparison::{ComparisonOption, ComparisonQuery, ComparisonResponse, ComparisonRow},
};
use diesel::{
    PgConnection, RunQueryDsl, sql_query,
    sql_types::{BigInt, Bool, Text},
};
use uuid::Uuid;

pub(super) fn compare(
    connection: &mut PgConnection,
    query: &ComparisonQuery,
    user_id: Uuid,
) -> Result<ComparisonResponse, AppError> {
    query.validate()?;
    let scope_all = false;
    let first = query
        .buckets
        .first()
        .ok_or_else(|| AppError::validation("missing time range".to_owned()))?;
    let last = query
        .buckets
        .last()
        .ok_or_else(|| AppError::validation("missing time range".to_owned()))?;
    let from = first.start.timestamp_micros();
    let to = last.end.timestamp_micros();
    // Only closed enum variants supply SQL identifiers; all user values are bound.
    let dimension = query.group_by.map_or("'all'", |value| value.column());
    let statement = format!(
        "
WITH buckets AS (SELECT * FROM jsonb_to_recordset(CAST($5 AS jsonb)) AS b(start_us bigint, end_us bigint))
SELECT b.start_us / 1000 AS bucket, {dimension} AS value,
COALESCE(SUM(e.total_tokens), 0)::bigint AS total_tokens,
COUNT(*) FILTER (WHERE e.event_type = 'request')::bigint AS requests
FROM buckets b
JOIN llm_usage_events e ON e.observed_at >= to_timestamp((b.start_us)::double precision / 1000000.0) AND e.observed_at < to_timestamp((b.end_us)::double precision / 1000000.0)
JOIN devices d ON d.id = e.device_context_id
WHERE ($2 OR e.user_id = CAST($1 AS uuid))
AND e.observed_at >= to_timestamp(($3)::double precision / 1000000.0) AND e.observed_at < to_timestamp(($4)::double precision / 1000000.0)
AND (jsonb_array_length(CAST($6 AS jsonb)->'agent') = 0 OR e.agent_name IN (SELECT jsonb_array_elements_text(CAST($6 AS jsonb)->'agent')))
AND (jsonb_array_length(CAST($6 AS jsonb)->'model') = 0 OR e.llm_model IN (SELECT jsonb_array_elements_text(CAST($6 AS jsonb)->'model')))
AND (jsonb_array_length(CAST($6 AS jsonb)->'provider') = 0 OR e.llm_provider IN (SELECT jsonb_array_elements_text(CAST($6 AS jsonb)->'provider')))
AND (jsonb_array_length(CAST($6 AS jsonb)->'platform') = 0 OR d.platform IN (SELECT jsonb_array_elements_text(CAST($6 AS jsonb)->'platform')))
AND (jsonb_array_length(CAST($6 AS jsonb)->'session') = 0 OR e.session_id IN (SELECT jsonb_array_elements_text(CAST($6 AS jsonb)->'session')))
GROUP BY 1, 2
ORDER BY bucket, value
"
    );
    let rows = sql_query(statement)
        .bind::<Text, _>(user_id.to_string())
        .bind::<Bool, _>(scope_all)
        .bind::<BigInt, _>(from)
        .bind::<BigInt, _>(to)
        .bind::<Text, _>(query.encoded_buckets())
        .bind::<Text, _>(query.encoded_filters()?)
        .load::<ComparisonRow>(connection)?;
    // Facets use the same authorized time window, before optional filters, so
    // selecting one value never hides the remaining selectable values.
    let options = sql_query("
WITH source AS (SELECT e.agent_name AS agent, e.llm_model AS model, e.llm_provider AS provider, d.platform AS platform
FROM llm_usage_events e JOIN devices d ON d.id = e.device_context_id
WHERE ($2 OR e.user_id = CAST($1 AS uuid))
AND e.observed_at >= to_timestamp(($3)::double precision / 1000000.0) AND e.observed_at < to_timestamp(($4)::double precision / 1000000.0))
SELECT 'agent' AS dimension, agent AS value FROM source
UNION SELECT 'model', model FROM source
UNION SELECT 'provider', provider FROM source
UNION SELECT 'platform', platform FROM source
ORDER BY dimension, value
")
        .bind::<Text, _>(user_id.to_string())
        .bind::<Bool, _>(scope_all)
        .bind::<BigInt, _>(from)
        .bind::<BigInt, _>(to)
        .load::<ComparisonOption>(connection)?;
    Ok(ComparisonResponse { rows, options })
}
