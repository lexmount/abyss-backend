//! Typed comparison aggregation and authorized filter options; no event content is loaded.

use crate::storage::comparison_buckets::WithBuckets;
use crate::{
    error::AppError,
    storage::sqlite::schema::{devices, llm_usage_events},
    usage::comparison::{
        ComparisonDimension, ComparisonOption, ComparisonQuery, ComparisonResponse, ComparisonRow,
    },
};
use diesel::sqlite::Sqlite;
use diesel::{
    IntoSql, SqliteConnection, define_sql_function,
    dsl::count,
    prelude::*,
    sql_types::{BigInt, Bool, Nullable, Text},
};
use uuid::Uuid;

// A query-local CTE, not a persistent table or migration.
diesel::table! {
    comparison_buckets (bucket) {
        bucket -> BigInt,
        start_at -> BigInt,
        end_at -> BigInt,
    }
}
diesel::allow_tables_to_appear_in_same_query!(comparison_buckets, llm_usage_events);
diesel::allow_tables_to_appear_in_same_query!(comparison_buckets, devices);
diesel::allow_columns_to_appear_in_same_group_by_clause!(
    comparison_buckets::bucket,
    llm_usage_events::agent_name
);
diesel::allow_columns_to_appear_in_same_group_by_clause!(
    comparison_buckets::bucket,
    llm_usage_events::llm_model
);
diesel::allow_columns_to_appear_in_same_group_by_clause!(
    comparison_buckets::bucket,
    llm_usage_events::llm_provider
);
diesel::allow_columns_to_appear_in_same_group_by_clause!(
    comparison_buckets::bucket,
    devices::platform
);

// SQLite SUM over integer tokens returns an integer, unlike PostgreSQL numeric.
define_sql_function! {
    #[aggregate]
    #[sql_name = "sum"]
    fn sum_tokens(value: BigInt) -> Nullable<BigInt>;
}
define_sql_function!(fn coalesce(value: Nullable<BigInt>, fallback: BigInt) -> BigInt);

pub(super) fn compare(
    connection: &mut SqliteConnection,
    query: &ComparisonQuery,
    user_id: Uuid,
) -> Result<ComparisonResponse, AppError> {
    use comparison_buckets as b;
    use llm_usage_events as e;

    query.validate()?;
    let user_id = user_id.to_string();
    let (first, last) = query
        .buckets
        .first()
        .zip(query.buckets.last())
        .ok_or_else(|| AppError::validation("missing time range".to_owned()))?;
    let buckets = query
        .buckets
        .iter()
        .map(|bucket| {
            (
                bucket.start.timestamp_millis(),
                bucket.start.timestamp_micros(),
                bucket.end.timestamp_micros(),
            )
        })
        .collect();
    let source = b::table
        .inner_join(
            e::table.on(e::observed_at
                .ge(b::start_at)
                .and(e::observed_at.lt(b::end_at))),
        )
        .inner_join(devices::table.on(devices::id.eq(e::device_context_id)))
        .filter(e::user_id.eq(&user_id))
        .filter(comparison_filters(
            query,
            first.start.timestamp_micros(),
            last.end.timestamp_micros(),
        ));

    // Each enum arm keeps the selected and grouped column statically identical.
    // Only the WHERE expression is boxed; grouping and result types stay static.
    macro_rules! aggregate_by {
        ($dimension:expr) => {
            aggregate_by!($dimension, (b::bucket, $dimension))
        };
        ($dimension:expr, $grouping:expr) => {{
            let dimension = $dimension;
            let statement = source
                .group_by($grouping)
                .select((
                    b::bucket,
                    dimension,
                    coalesce(sum_tokens(e::total_tokens), 0_i64),
                    count(e::id).aggregate_filter(e::event_type.eq("request")),
                ))
                .order((b::bucket, dimension));
            WithBuckets::<_, _, BigInt>::new(statement, buckets)
                .load::<ComparisonRow>(connection)?
        }};
    }
    let rows = match query.group_by {
        Some(ComparisonDimension::Agent) => aggregate_by!(e::agent_name),
        Some(ComparisonDimension::Model) => aggregate_by!(e::llm_model),
        Some(ComparisonDimension::Provider) => aggregate_by!(e::llm_provider),
        Some(ComparisonDimension::Platform) => aggregate_by!(devices::platform),
        None => aggregate_by!("all".into_sql::<Text>(), b::bucket),
    };

    // Facets retain the authorized time window before optional chart filters.
    let source = e::table
        .inner_join(devices::table)
        .filter(e::user_id.eq(&user_id))
        .filter(e::observed_at.ge(first.start.timestamp_micros()))
        .filter(e::observed_at.lt(last.end.timestamp_micros()));
    let mut options = source
        .select(("agent".into_sql::<Text>(), e::agent_name))
        .union(source.select(("model".into_sql::<Text>(), e::llm_model)))
        .union(source.select(("provider".into_sql::<Text>(), e::llm_provider)))
        .union(source.select(("platform".into_sql::<Text>(), devices::platform)))
        .load::<ComparisonOption>(connection)?;
    options.sort_by(|left, right| {
        (&left.dimension, &left.value).cmp(&(&right.dimension, &right.value))
    });
    Ok(ComparisonResponse { rows, options })
}

// The joined source lets optional predicates remain typed without boxing GROUP BY.
type ComparisonSource = diesel::dsl::InnerJoinQuerySource<
    diesel::dsl::InnerJoinQuerySource<
        comparison_buckets::table,
        llm_usage_events::table,
        diesel::dsl::And<
            diesel::dsl::GtEq<llm_usage_events::observed_at, comparison_buckets::start_at>,
            diesel::dsl::Lt<llm_usage_events::observed_at, comparison_buckets::end_at>,
        >,
    >,
    devices::table,
    diesel::dsl::Eq<devices::id, llm_usage_events::device_context_id>,
>;

type ComparisonPredicate<'a> =
    Box<dyn BoxableExpression<ComparisonSource, Sqlite, SqlType = Bool> + 'a>;

fn comparison_filters(query: &ComparisonQuery, from: i64, to: i64) -> ComparisonPredicate<'_> {
    use llm_usage_events as e;
    let mut predicate: ComparisonPredicate<'_> =
        Box::new(e::observed_at.ge(from).and(e::observed_at.lt(to)));
    if !query.filters.agent.is_empty() {
        predicate = Box::new(predicate.and(e::agent_name.eq_any(&query.filters.agent)));
    }
    if !query.filters.model.is_empty() {
        predicate = Box::new(predicate.and(e::llm_model.eq_any(&query.filters.model)));
    }
    if !query.filters.provider.is_empty() {
        predicate = Box::new(predicate.and(e::llm_provider.eq_any(&query.filters.provider)));
    }
    if !query.filters.platform.is_empty() {
        predicate = Box::new(predicate.and(devices::platform.eq_any(&query.filters.platform)));
    }
    if !query.filters.session.is_empty() {
        predicate = Box::new(predicate.and(e::session_id.eq_any(&query.filters.session)));
    }
    predicate
}
