//! PostgreSQL session index ordered by latest activity.

use chrono::{DateTime, Utc};
use diesel::{
    PgConnection, QueryableByName, RunQueryDsl, sql_query,
    sql_types::{Array, BigInt, Bool, Nullable, Text, Timestamptz, Uuid as SqlUuid},
};
use uuid::Uuid;

use crate::{
    error::AppError,
    usage::session_list::{SessionListItem, SessionListResponse, ValidatedSessionListQuery},
};

/// Per-session activity with the filters that apply before paging.
///
/// Bind order: `$1` owner, `$2`/`$3` agent filter flag and names, `$4`/`$5`
/// lower activity bound, `$6`/`$7` upper start bound, `$8`/`$9` provider and
/// `$10`/`$11` model a session must have used.
const ACTIVITY_CTE: &str = "\
    WITH activity AS ( \
        SELECT \
            s.id AS session_pk, \
            s.session_id AS session_id, \
            s.agent_name AS agent_name, \
            s.agent_version AS agent_version, \
            d.host_name AS host_name, \
            d.platform AS platform, \
            s.started_at AS started_at, \
            s.ended_at AS ended_at, \
            COALESCE(MAX(e.observed_at), s.started_at) AS last_activity_at, \
            (SELECT COUNT(*) FROM agent_turns t WHERE t.session_pk = s.id)::bigint AS turns, \
            COUNT(e.id)::bigint AS events, \
            COALESCE(SUM(e.total_tokens), 0)::bigint AS total_tokens \
        FROM agent_sessions s \
        JOIN devices d ON d.id = s.device_context_id \
        LEFT JOIN llm_usage_events e ON e.session_pk = s.id \
        WHERE s.user_id = $1 \
            AND ($2 = false OR s.agent_name = ANY($3)) \
            AND ($8 = false OR EXISTS ( \
                SELECT 1 FROM llm_usage_events p \
                WHERE p.session_pk = s.id AND p.llm_provider = $9)) \
            AND ($10 = false OR EXISTS ( \
                SELECT 1 FROM llm_usage_events m \
                WHERE m.session_pk = s.id AND m.llm_model = $11)) \
        GROUP BY s.id, d.host_name, d.platform \
    ), \
    filtered AS ( \
        SELECT * FROM activity \
        WHERE ($4 = false OR last_activity_at >= $5) \
            AND ($6 = false OR started_at < $7) \
    ) ";

#[derive(QueryableByName)]
struct SessionListRow {
    #[diesel(sql_type = SqlUuid)]
    session_pk: Uuid,
    #[diesel(sql_type = Text)]
    session_id: String,
    #[diesel(sql_type = Text)]
    agent_name: String,
    #[diesel(sql_type = Nullable<Text>)]
    agent_version: Option<String>,
    #[diesel(sql_type = Text)]
    host_name: String,
    #[diesel(sql_type = Text)]
    platform: String,
    #[diesel(sql_type = Timestamptz)]
    started_at: DateTime<Utc>,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    ended_at: Option<DateTime<Utc>>,
    #[diesel(sql_type = Timestamptz)]
    last_activity_at: DateTime<Utc>,
    #[diesel(sql_type = BigInt)]
    turns: i64,
    #[diesel(sql_type = BigInt)]
    events: i64,
    #[diesel(sql_type = BigInt)]
    total_tokens: i64,
}

#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    total: i64,
}

pub(super) fn list_sessions(
    connection: &mut PgConnection,
    user_id: Uuid,
    query: &ValidatedSessionListQuery,
) -> Result<SessionListResponse, AppError> {
    let has_agents = !query.agent_names.is_empty();
    let from = query.from.unwrap_or(DateTime::UNIX_EPOCH);
    let to = query.to.unwrap_or(DateTime::UNIX_EPOCH);
    let provider = query.llm_provider.as_deref().unwrap_or_default();
    let model = query.llm_model.as_deref().unwrap_or_default();

    let total = sql_query(format!(
        "{ACTIVITY_CTE} SELECT COUNT(*)::bigint AS total FROM filtered"
    ))
    .bind::<SqlUuid, _>(user_id)
    .bind::<Bool, _>(has_agents)
    .bind::<Array<Text>, _>(&query.agent_names)
    .bind::<Bool, _>(query.from.is_some())
    .bind::<Timestamptz, _>(from)
    .bind::<Bool, _>(query.to.is_some())
    .bind::<Timestamptz, _>(to)
    .bind::<Bool, _>(query.llm_provider.is_some())
    .bind::<Text, _>(provider)
    .bind::<Bool, _>(query.llm_model.is_some())
    .bind::<Text, _>(model)
    .get_result::<CountRow>(connection)?
    .total;

    let rows = sql_query(format!(
        "{ACTIVITY_CTE} SELECT * FROM filtered \
         ORDER BY last_activity_at DESC, session_pk DESC LIMIT $12 OFFSET $13"
    ))
    .bind::<SqlUuid, _>(user_id)
    .bind::<Bool, _>(has_agents)
    .bind::<Array<Text>, _>(&query.agent_names)
    .bind::<Bool, _>(query.from.is_some())
    .bind::<Timestamptz, _>(from)
    .bind::<Bool, _>(query.to.is_some())
    .bind::<Timestamptz, _>(to)
    .bind::<Bool, _>(query.llm_provider.is_some())
    .bind::<Text, _>(provider)
    .bind::<Bool, _>(query.llm_model.is_some())
    .bind::<Text, _>(model)
    .bind::<BigInt, _>(i64::from(query.page_size))
    .bind::<BigInt, _>(query.offset)
    .load::<SessionListRow>(connection)?;

    let items = rows
        .into_iter()
        .map(|row| SessionListItem {
            session_pk: row.session_pk,
            session_id: row.session_id,
            agent_name: row.agent_name,
            agent_version: row.agent_version,
            host_name: row.host_name,
            platform: row.platform,
            started_at: row.started_at,
            ended_at: row.ended_at,
            last_activity_at: row.last_activity_at,
            turns: row.turns,
            events: row.events,
            total_tokens: row.total_tokens,
        })
        .collect();
    Ok(SessionListResponse::new(query, total, items))
}
