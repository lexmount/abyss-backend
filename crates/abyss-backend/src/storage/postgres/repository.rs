//! PostgreSQL-backed repository functions for Agent usage APIs.
//!
//! This module is the transactional boundary for the event hierarchy. Ingest
//! validates the complete request before opening a transaction, upserts device,
//! session, and turn aggregates, inserts immutable events idempotently, and
//! enqueues search projection in the same commit. Query functions always apply
//! the authenticated owner before returning conversation or attachment data.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use diesel::{
    Connection, ExpressionMethods, PgConnection, QueryDsl, QueryableByName, RunQueryDsl,
    dsl::{max, sql},
    result::OptionalExtension,
    sql_query,
    sql_types::{
        Array, BigInt, Bool, Integer, Jsonb, Nullable, Text, Timestamptz, Uuid as SqlUuid,
    },
};
use uuid::Uuid;

use crate::{
    db::{
        models::{
            AgentSession, AgentTurn, Device, NewAgentDiagnosticCapture,
            NewAgentDiagnosticCaptureEvent, NewAgentSession, NewAgentTurn, NewDevice,
            NewSearchOutboxTask, NewUsageEvent, NewUsageEventAttachment, UsageEvent,
            UsageEventAttachment,
        },
        schema::{
            agent_diagnostic_capture_events, agent_diagnostic_captures, agent_sessions,
            agent_turns, devices, llm_usage_event_attachments, llm_usage_events, search_outbox,
        },
    },
    error::AppError,
    usage::{
        IngestEventsRequest, IngestEventsResponse, IngestUsageEvent, RawEventsQuery,
        RawEventsResponse, SessionInfo, SessionTimelineResponse, SummaryQuery, SummaryResponse,
        SummaryRow, TurnTimeline, UsageEventResponse,
        attachments::{
            ImageMediaType, StoredImageAttachment, UsageEventAttachmentResponse,
            ValidatedImageAttachment,
        },
        diagnostics::IngestDiagnosticCapture,
        event_order::UsageEventTimelineOrder,
        persistence::{
            TurnIdentity, TurnIdentityKind, agent_name_filter_values, canonical_agent_name,
            choose_turn_index, escape_like_pattern, non_empty, non_negative_count, normalize_limit,
            normalize_slug, normalize_text, normalized_optional, normalized_tokens, parse_group_by,
            sha256_bytes, turn_identity, unix_epoch, validate_batch, validate_event_type,
        },
        session_metadata_from_event,
    },
};

#[cfg(test)]
use crate::usage::persistence::{turn_identity_from_metadata, validate_event};

const SUMMARY_AGGREGATE_SQL: &str = "\
    SELECT \
        CASE WHEN $1 THEN to_char(e.observed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD') END AS day, \
        CASE WHEN $2 THEN e.user_id END AS user_id, \
        CASE WHEN $2 THEN NULLIF(btrim(u.name), '') END AS user_name, \
        CASE WHEN $2 THEN u.email END AS user_email, \
        CASE WHEN $3 THEN d.host_name END AS host_name, \
        CASE WHEN $3 THEN d.platform END AS platform, \
        CASE WHEN $3 THEN d.os_version END AS os_version, \
        CASE \
            WHEN $4 THEN CASE \
                WHEN e.agent_name IN ('claude', 'claude-code', 'claude-desktop') THEN 'claude-code' \
                ELSE e.agent_name \
            END \
        END AS agent_name, \
        CASE WHEN $5 THEN e.llm_provider END AS llm_provider, \
        CASE WHEN $6 THEN e.llm_model END AS llm_model, \
        CASE WHEN $7 THEN e.event_type END AS event_type, \
        COUNT(DISTINCT e.session_pk)::bigint AS sessions, \
        COUNT(DISTINCT e.turn_pk)::bigint AS turns, \
        COUNT(*) FILTER (WHERE e.event_type = 'request')::bigint AS requests, \
        COUNT(*) FILTER (WHERE e.event_type = 'response')::bigint AS responses, \
        COALESCE(SUM(e.input_tokens), 0)::bigint AS input_tokens, \
        COALESCE(SUM(e.output_tokens), 0)::bigint AS output_tokens, \
        COALESCE(SUM(e.cache_read_tokens), 0)::bigint AS cache_read_tokens, \
        COALESCE(SUM(e.cache_write_tokens), 0)::bigint AS cache_write_tokens, \
        COALESCE(SUM(e.reasoning_tokens), 0)::bigint AS reasoning_tokens, \
        COALESCE(SUM(e.total_tokens), 0)::bigint AS total_tokens \
    FROM llm_usage_events e \
    LEFT JOIN app_users u ON u.id = e.user_id \
    LEFT JOIN devices d ON d.id = e.device_context_id \
    WHERE e.user_id = $8 \
        AND ($9 = false OR e.observed_at >= $10) \
        AND ($11 = false OR e.observed_at < $12) \
        AND ( \
            $13 = false \
            OR ($15 = true AND e.user_id = $14) \
            OR ($15 = false AND ( \
                u.email ILIKE $16 ESCAPE '\\' \
                OR COALESCE(u.name, '') ILIKE $16 ESCAPE '\\' \
            )) \
        ) \
        AND ($17 = false OR e.agent_name = ANY($18)) \
        AND ($19 = false OR e.session_id = $20) \
        AND ($21 = false OR e.session_pk = $22) \
        AND ($23 = false OR e.turn_index = $24) \
        AND ($25 = false OR e.llm_provider = $26) \
        AND ($27 = false OR e.llm_model = $28) \
        AND ($29 = false OR e.event_type = $30) \
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 \
    ORDER BY total_tokens DESC, agent_name ASC NULLS LAST, user_email ASC NULLS LAST, day ASC NULLS LAST \
    LIMIT $31";

/// Validates and atomically ingests one batch for the authenticated owner.
///
/// Collector `event_id` and `(user_id, capture_id)` values are idempotency keys;
/// replays are counted as duplicates without changing the original rows.
pub fn ingest_events(
    connection: &mut PgConnection,
    request: &IngestEventsRequest,
    user_id: Uuid,
    max_batch_size: usize,
) -> Result<IngestEventsResponse, AppError> {
    let validated_attachments = validate_batch(request, max_batch_size)?;

    // Process the batch atomically so the device/session/turn hierarchy cannot
    // be partially created without its corresponding usage events.
    let (accepted, duplicates, accepted_diagnostic_captures, duplicate_diagnostic_captures) =
        connection.transaction(|transaction| {
            let mut accepted = 0usize;
            let mut duplicates = 0usize;

            for (event, attachments) in request.events.iter().zip(validated_attachments) {
                let inserted = ingest_one_event(transaction, user_id, event, attachments)?;
                if inserted {
                    accepted = accepted.saturating_add(1);
                } else {
                    duplicates = duplicates.saturating_add(1);
                }
            }

            let mut accepted_diagnostic_captures = 0_usize;
            let mut duplicate_diagnostic_captures = 0_usize;
            for capture in &request.diagnostic_captures {
                if ingest_one_diagnostic_capture(transaction, user_id, capture)? {
                    accepted_diagnostic_captures = accepted_diagnostic_captures.saturating_add(1);
                } else {
                    duplicate_diagnostic_captures = duplicate_diagnostic_captures.saturating_add(1);
                }
            }

            Ok::<_, AppError>((
                accepted,
                duplicates,
                accepted_diagnostic_captures,
                duplicate_diagnostic_captures,
            ))
        })?;

    Ok(IngestEventsResponse {
        accepted,
        duplicates,
        rejected: 0,
        errors: Vec::new(),
        accepted_diagnostic_captures,
        duplicate_diagnostic_captures,
    })
}

fn ingest_one_diagnostic_capture(
    connection: &mut PgConnection,
    user_id: Uuid,
    capture: &IngestDiagnosticCapture,
) -> Result<bool, AppError> {
    // Reload event rows rather than trusting request correlation alone. This
    // establishes authenticated ownership and a single session/device boundary.
    let events = llm_usage_events::table
        .filter(llm_usage_events::user_id.eq(user_id))
        .filter(llm_usage_events::event_id.eq_any(&capture.event_ids))
        .load::<UsageEvent>(connection)?;
    if events.len() != capture.event_ids.len() {
        return Err(AppError::validation(
            "diagnostic capture events were not ingested for the authenticated user".to_owned(),
        ));
    }
    let first_event = events.first().ok_or_else(|| {
        AppError::validation("diagnostic capture must reference at least one event".to_owned())
    })?;
    if events.iter().any(|event| {
        event.session_pk != first_event.session_pk
            || event.device_context_id != first_event.device_context_id
    }) {
        return Err(AppError::validation(
            "diagnostic capture events must belong to one session and device".to_owned(),
        ));
    }

    let capture_pk = Uuid::now_v7();
    let now = Utc::now();
    let inserted = diesel::insert_into(agent_diagnostic_captures::table)
        .values(NewAgentDiagnosticCapture {
            id: capture_pk,
            user_id,
            device_context_id: first_event.device_context_id,
            session_pk: first_event.session_pk,
            capture_id: normalize_text(&capture.capture_id),
            flow_id: normalize_text(&capture.flow_id),
            captured_at: capture.captured_at,
            collector_version: normalize_text(&capture.collector_version),
            payload: capture.payload.clone(),
            created_at: now,
        })
        .on_conflict((
            agent_diagnostic_captures::user_id,
            agent_diagnostic_captures::capture_id,
        ))
        .do_nothing()
        .execute(connection)?;

    if inserted == 1 {
        let event_rows = events
            .into_iter()
            .map(|event| NewAgentDiagnosticCaptureEvent {
                capture_pk,
                event_pk: event.id,
            })
            .collect::<Vec<_>>();
        diesel::insert_into(agent_diagnostic_capture_events::table)
            .values(&event_rows)
            .execute(connection)?;
    }

    Ok(inserted == 1)
}

/// Aggregates owner-scoped event and token counts by requested dimensions.
pub fn usage_summary(
    connection: &mut PgConnection,
    query: &SummaryQuery,
    user_id: Uuid,
    default_limit: i64,
) -> Result<SummaryResponse, AppError> {
    let group_by = parse_group_by(query.group_by.as_deref());
    let limit = normalize_limit(query.limit, default_limit, 100_000);
    let rows = load_filtered_summary_rows(
        connection,
        &EventFilters {
            user_id,
            from: query.from,
            to: query.to,
            user_filter: query.user_filter.as_deref(),
            agent_name: query.agent_name.as_deref(),
            session_id: query.session_id.as_deref(),
            session_pk: None,
            turn_index: None,
            llm_provider: query.llm_provider.as_deref(),
            llm_model: query.llm_model.as_deref(),
            event_type: query.event_type.as_deref(),
            limit,
            offset: 0,
        },
        &group_by,
    )?;

    Ok(SummaryResponse {
        from: query.from,
        to: query.to,
        group_by,
        rows,
        next_page_token: None,
    })
}

fn load_filtered_summary_rows(
    connection: &mut PgConnection,
    filters: &EventFilters<'_>,
    group_by: &[String],
) -> Result<Vec<SummaryRow>, AppError> {
    let dimensions = SummaryDimensions::from_group_by(group_by);
    let sql_filters = SummarySqlFilters::from_event_filters(filters)?;
    let user_id = sql_filters.user_id;
    let has_from = sql_filters.from.is_some();
    let from = sql_filters.from.unwrap_or_else(unix_epoch);
    let has_to = sql_filters.to.is_some();
    let to = sql_filters.to.unwrap_or_else(unix_epoch);
    let user_filter_id = sql_filters.user_filter_id.unwrap_or_else(Uuid::nil);
    let has_user_filter_id = sql_filters.user_filter_id.is_some();
    let user_filter_pattern = sql_filters.user_filter_pattern.unwrap_or_default();
    let has_user_filter = !user_filter_pattern.is_empty();
    let agent_names = sql_filters.agent_names;
    let has_agent_filter = !agent_names.is_empty();
    let session_id = sql_filters.session_id.unwrap_or_default();
    let has_session_id_filter = !session_id.is_empty();
    let session_pk = sql_filters.session_pk.unwrap_or_else(Uuid::nil);
    let has_session_pk_filter = session_pk != Uuid::nil();
    let turn_index = sql_filters.turn_index.unwrap_or_default();
    let has_turn_index_filter = turn_index > 0_i32;
    let provider = sql_filters.provider.unwrap_or_default();
    let has_provider_filter = !provider.is_empty();
    let model = sql_filters.model.unwrap_or_default();
    let has_model_filter = !model.is_empty();
    let event_type = sql_filters.event_type.unwrap_or_default();
    let has_event_type_filter = !event_type.is_empty();

    // The static query uses boolean gates for optional dimensions and filters.
    // This keeps user values in typed bind parameters and avoids dynamic SQL.
    // Bind order intentionally mirrors the numbered placeholders in the query.
    sql_query(SUMMARY_AGGREGATE_SQL)
        .bind::<Bool, _>(dimensions.enabled(SummaryDimension::Day))
        .bind::<Bool, _>(dimensions.enabled(SummaryDimension::User))
        .bind::<Bool, _>(dimensions.enabled(SummaryDimension::Device))
        .bind::<Bool, _>(dimensions.enabled(SummaryDimension::Agent))
        .bind::<Bool, _>(dimensions.enabled(SummaryDimension::Provider))
        .bind::<Bool, _>(dimensions.enabled(SummaryDimension::Model))
        .bind::<Bool, _>(dimensions.enabled(SummaryDimension::EventType))
        .bind::<SqlUuid, _>(user_id)
        .bind::<Bool, _>(has_from)
        .bind::<Timestamptz, _>(from)
        .bind::<Bool, _>(has_to)
        .bind::<Timestamptz, _>(to)
        .bind::<Bool, _>(has_user_filter)
        .bind::<SqlUuid, _>(user_filter_id)
        .bind::<Bool, _>(has_user_filter_id)
        .bind::<Text, _>(&user_filter_pattern)
        .bind::<Bool, _>(has_agent_filter)
        .bind::<Array<Text>, _>(agent_names)
        .bind::<Bool, _>(has_session_id_filter)
        .bind::<Text, _>(&session_id)
        .bind::<Bool, _>(has_session_pk_filter)
        .bind::<SqlUuid, _>(session_pk)
        .bind::<Bool, _>(has_turn_index_filter)
        .bind::<Integer, _>(turn_index)
        .bind::<Bool, _>(has_provider_filter)
        .bind::<Text, _>(&provider)
        .bind::<Bool, _>(has_model_filter)
        .bind::<Text, _>(&model)
        .bind::<Bool, _>(has_event_type_filter)
        .bind::<Text, _>(&event_type)
        .bind::<BigInt, _>(filters.limit)
        .load::<SummaryAggregateRow>(connection)
        .map(|rows| rows.into_iter().map(SummaryRow::from).collect())
        .map_err(AppError::from)
}

/// Loads one owner-scoped session with normalized turns and ordered events.
pub fn session_timeline(
    connection: &mut PgConnection,
    user_id: Uuid,
    session_pk: Uuid,
) -> Result<SessionTimelineResponse, AppError> {
    // Timeline views include prompt/response text, so they are always limited
    // to the owner.
    let session = agent_sessions::table
        .filter(agent_sessions::id.eq(session_pk))
        .filter(agent_sessions::user_id.eq(user_id))
        .first::<AgentSession>(connection)
        .optional()?
        .ok_or_else(|| AppError::not_found(format!("session not found: {session_pk}")))?;
    touch_session_metadata(&session);

    let turns = agent_turns::table
        .filter(agent_turns::session_pk.eq(session.id))
        .order(agent_turns::turn_index.asc())
        .load::<AgentTurn>(connection)?;

    let mut events = llm_usage_events::table
        .filter(llm_usage_events::session_pk.eq(session.id))
        .order((
            llm_usage_events::turn_index.asc(),
            llm_usage_events::observed_at.asc(),
        ))
        .load::<UsageEvent>(connection)?;
    UsageEventTimelineOrder::sort(&mut events);

    let device_contexts = load_device_contexts_for_events(connection, &events)?;
    let mut attachments_by_event = load_attachments_for_events(connection, &events)?;
    let mut events_by_turn: HashMap<Uuid, Vec<UsageEventResponse>> = HashMap::new();
    for event in events {
        let event_pk = event.id;
        let device_context_id = event.device_context_id;
        events_by_turn
            .entry(event.turn_pk)
            .or_default()
            .push(event_response(
                event,
                device_contexts.get(&device_context_id),
                attachments_by_event.remove(&event_pk).unwrap_or_default(),
            ));
    }

    let turn_timelines = turns
        .into_iter()
        .map(|turn| {
            touch_turn_metadata(&turn);
            TurnTimeline {
                turn_pk: turn.id,
                turn_index: turn.turn_index,
                started_at: turn.started_at,
                ended_at: turn.ended_at,
                events: events_by_turn.remove(&turn.id).unwrap_or_default(),
            }
        })
        .collect();

    Ok(SessionTimelineResponse {
        session: SessionInfo {
            session_pk: session.id,
            user_id: session.user_id,
            device_context_id: session.device_context_id,
            agent_name: canonical_agent_name(&session.agent_name),
            agent_version: session.agent_version,
            session_id: session.session_id,
            started_at: session.started_at,
            ended_at: session.ended_at,
            metadata: session.metadata,
        },
        turns: turn_timelines,
    })
}

/// Returns one newest-first page of raw owner-scoped events.
pub fn raw_events(
    connection: &mut PgConnection,
    query: &RawEventsQuery,
    user_id: Uuid,
    default_limit: i64,
) -> Result<RawEventsResponse, AppError> {
    let page_size = normalize_limit(query.limit, default_limit, 1_000);
    let page_size_usize = usize::try_from(page_size)
        .map_err(|error| AppError::internal(format!("invalid raw events page size: {error}")))?;
    let offset = query.offset.unwrap_or(0).max(0);
    // Raw events can contain sensitive conversation context. Keep this endpoint
    // scoped to the deployment owner.
    let mut events = load_filtered_events(
        connection,
        &EventFilters {
            user_id,
            from: query.from,
            to: query.to,
            user_filter: None,
            agent_name: query.agent_name.as_deref(),
            session_id: query.session_id.as_deref(),
            session_pk: query.session_pk,
            turn_index: query.turn_index,
            llm_provider: query.llm_provider.as_deref(),
            llm_model: query.llm_model.as_deref(),
            event_type: query.event_type.as_deref(),
            limit: page_size.saturating_add(1_i64),
            offset,
        },
    )?;
    // Fetch one sentinel row beyond the requested page so no count query is
    // needed merely to determine whether a next offset exists.
    let has_next_page = events.len() > page_size_usize;
    if has_next_page {
        events.truncate(page_size_usize);
    }
    let device_contexts = load_device_contexts_for_events(connection, &events)?;
    let mut attachments_by_event = load_attachments_for_events(connection, &events)?;

    Ok(RawEventsResponse {
        events: events
            .into_iter()
            .map(|event| {
                let event_pk = event.id;
                let device_context_id = event.device_context_id;
                event_response(
                    event,
                    device_contexts.get(&device_context_id),
                    attachments_by_event.remove(&event_pk).unwrap_or_default(),
                )
            })
            .collect(),
        next_page_token: has_next_page.then(|| offset.saturating_add(page_size).to_string()),
    })
}

/// Loads authorized attachment bytes and verifies persisted size metadata.
pub fn image_attachment(
    connection: &mut PgConnection,
    user_id: Uuid,
    attachment_id: Uuid,
) -> Result<StoredImageAttachment, AppError> {
    let attachment = llm_usage_event_attachments::table
        .filter(llm_usage_event_attachments::id.eq(attachment_id))
        .filter(llm_usage_event_attachments::user_id.eq(user_id))
        .first::<UsageEventAttachment>(connection)
        .optional()?
        .ok_or_else(|| {
            AppError::not_found(format!("image attachment not found: {attachment_id}"))
        })?;
    let content = attachment.content.ok_or_else(|| {
        AppError::not_found(format!(
            "image attachment content not found: {attachment_id}"
        ))
    })?;
    if i64::try_from(content.len()).ok() != Some(attachment.byte_size) {
        return Err(AppError::internal(format!(
            "stored image attachment byte size mismatch: {attachment_id}"
        )));
    }
    let media_type = ImageMediaType::from_stored(&attachment.media_type)?;
    // These columns are selected by the reusable Diesel row model but are not
    // part of the download response. Touch them to keep dead-code linting useful
    // without creating a second, nearly identical query model.
    std::hint::black_box((
        attachment.user_id,
        attachment.event_pk,
        attachment.created_at,
    ));
    Ok(StoredImageAttachment {
        media_type,
        sha256: hex::encode(attachment.sha256),
        content,
    })
}

fn ingest_one_event(
    connection: &mut PgConnection,
    user_id: Uuid,
    event: &IngestUsageEvent,
    attachments: Vec<ValidatedImageAttachment>,
) -> Result<bool, AppError> {
    let observed_at = event.observed_at;
    let now = Utc::now();
    let agent_name = canonical_agent_name(&event.agent.name);
    let agent_version = normalized_optional(event.agent.version.as_deref());
    let provider = normalize_slug(&event.llm.provider);
    let model = normalize_text(&event.llm.model);

    let device = upsert_device(connection, user_id, event, observed_at, now)?;
    let session = upsert_session(
        connection,
        UpsertSessionInput {
            device: &device,
            user_id,
            event,
            agent_name: &agent_name,
            agent_version: agent_version.clone(),
            observed_at,
            now,
        },
    )?;
    let turn_index = resolve_turn_index(connection, session.id, event)?;
    let turn = upsert_turn(
        connection,
        user_id,
        session.id,
        turn_index,
        observed_at,
        now,
    )?;
    let tokens = normalized_tokens(event)?;
    let text = normalized_optional(event.text.as_deref());

    // `event_id` is the idempotency key supplied by the collector. Duplicate
    // observations are accepted as no-ops instead of failing the whole batch.
    let usage_event_id = Uuid::now_v7();
    let inserted = diesel::insert_into(llm_usage_events::table)
        .values(NewUsageEvent {
            id: usage_event_id,
            user_id,
            device_context_id: device.id,
            session_pk: session.id,
            turn_pk: turn.id,
            event_id: normalize_text(&event.event_id),
            agent_name,
            agent_version,
            session_id: normalize_text(&event.session_id),
            turn_index,
            llm_provider: provider,
            llm_model: model,
            event_type: event.event_type.as_str().to_owned(),
            text_sha256: text.as_deref().map(sha256_bytes),
            text,
            input_tokens: tokens.input,
            output_tokens: tokens.output,
            cache_read_tokens: tokens.cache_read,
            cache_write_tokens: tokens.cache_write,
            reasoning_tokens: tokens.reasoning,
            total_tokens: tokens.total,
            observed_at,
            metadata: event.metadata.clone(),
            created_at: now,
        })
        .on_conflict(llm_usage_events::event_id)
        .do_nothing()
        .execute(connection)?;

    // Child rows are written only for a newly inserted event. A replay must not
    // replace the attachments associated with the original idempotency key.
    if inserted == 1 && !attachments.is_empty() {
        let attachment_rows = attachments
            .into_iter()
            .map(|attachment| NewUsageEventAttachment {
                id: Uuid::now_v7(),
                user_id,
                event_pk: usage_event_id,
                position: attachment.position,
                media_type: attachment.media_type.as_str().to_owned(),
                byte_size: attachment.byte_size,
                sha256: attachment.sha256,
                content: attachment.content,
                created_at: now,
            })
            .collect::<Vec<_>>();
        diesel::insert_into(llm_usage_event_attachments::table)
            .values(&attachment_rows)
            .execute(connection)?;
    }

    // The enclosing transaction commits the event and its projection task
    // together, providing at-least-once delivery to Elasticsearch.
    if inserted == 1 {
        diesel::insert_into(search_outbox::table)
            .values(NewSearchOutboxTask {
                event_pk: usage_event_id,
                user_id,
                created_at: now,
            })
            .execute(connection)?;
    }

    Ok(inserted == 1)
}

fn upsert_device(
    connection: &mut PgConnection,
    user_id: Uuid,
    event: &IngestUsageEvent,
    observed_at: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<Device, AppError> {
    // We do not require a stable OS-level device id. The current product only
    // needs a user-visible device context, so host/platform identify one user's
    // device bucket and OS version is refreshed opportunistically.
    diesel::insert_into(devices::table)
        .values(NewDevice {
            id: Uuid::now_v7(),
            user_id,
            host_name: normalize_text(&event.device.host_name),
            platform: normalize_slug(&event.device.platform),
            os_version: normalized_optional(event.device.os_version.as_deref()),
            first_seen_at: observed_at,
            last_seen_at: observed_at,
            created_at: now,
            updated_at: now,
        })
        .on_conflict((devices::user_id, devices::host_name, devices::platform))
        .do_update()
        .set((
            devices::os_version.eq(sql::<Nullable<Text>>(
                "COALESCE(EXCLUDED.os_version, devices.os_version)",
            )),
            devices::first_seen_at.eq(sql::<Timestamptz>(
                "LEAST(devices.first_seen_at, EXCLUDED.first_seen_at)",
            )),
            devices::last_seen_at.eq(sql::<Timestamptz>(
                "GREATEST(devices.last_seen_at, EXCLUDED.last_seen_at)",
            )),
            devices::updated_at.eq(now),
        ))
        .get_result::<Device>(connection)
        .map_err(AppError::from)
}

fn upsert_session(
    connection: &mut PgConnection,
    input: UpsertSessionInput<'_>,
) -> Result<AgentSession, AppError> {
    let session_id = normalize_text(&input.event.session_id);
    diesel::insert_into(agent_sessions::table)
        .values(NewAgentSession {
            id: Uuid::now_v7(),
            user_id: input.user_id,
            device_context_id: input.device.id,
            agent_name: input.agent_name.to_owned(),
            agent_version: input.agent_version,
            session_id,
            started_at: input.observed_at,
            ended_at: Some(input.observed_at),
            metadata: session_metadata_from_event(&input.event.metadata),
            created_at: input.now,
            updated_at: input.now,
        })
        .on_conflict((
            agent_sessions::user_id,
            agent_sessions::agent_name,
            agent_sessions::session_id,
        ))
        .do_update()
        .set((
            agent_sessions::device_context_id
                .eq(sql::<diesel::sql_types::Uuid>("EXCLUDED.device_context_id")),
            agent_sessions::agent_version.eq(sql::<Nullable<Text>>(
                "COALESCE(EXCLUDED.agent_version, agent_sessions.agent_version)",
            )),
            agent_sessions::started_at.eq(sql::<Timestamptz>(
                "LEAST(agent_sessions.started_at, EXCLUDED.started_at)",
            )),
            agent_sessions::ended_at.eq(sql::<Nullable<Timestamptz>>(
                "GREATEST(COALESCE(agent_sessions.ended_at, EXCLUDED.ended_at), EXCLUDED.ended_at)",
            )),
            agent_sessions::metadata
                .eq(sql::<Jsonb>("agent_sessions.metadata || EXCLUDED.metadata")),
            agent_sessions::updated_at.eq(input.now),
        ))
        .get_result::<AgentSession>(connection)
        .map_err(AppError::from)
}

fn resolve_turn_index(
    connection: &mut PgConnection,
    session_pk: Uuid,
    event: &IngestUsageEvent,
) -> Result<i32, AppError> {
    // Older collectors without a stable provider identity retain their supplied
    // index. Newer evidence lets restarted collectors avoid merging new work
    // into an already-used local turn number.
    let Some(identity) = turn_identity(event) else {
        return Ok(event.turn_index);
    };
    let existing_turn_index = existing_turn_index_for_identity(connection, session_pk, &identity)?;
    let next_turn_index = next_session_turn_index(connection, session_pk)?;
    Ok(choose_turn_index(
        event.turn_index,
        existing_turn_index,
        next_turn_index,
    ))
}

fn existing_turn_index_for_identity(
    connection: &mut PgConnection,
    session_pk: Uuid,
    identity: &TurnIdentity,
) -> Result<Option<i32>, AppError> {
    let row = match &identity.kind {
        TurnIdentityKind::CodexTurnId => sql_query(
            "SELECT turn_index \
             FROM llm_usage_events \
             WHERE session_pk = $1 AND metadata->>'codex_turn_id' = $2 \
             ORDER BY observed_at ASC \
             LIMIT 1",
        )
        .bind::<SqlUuid, _>(session_pk)
        .bind::<Text, _>(&identity.value)
        .get_result::<TurnIndexRow>(connection),
        TurnIdentityKind::ClaudeTurnId => sql_query(
            "SELECT turn_index \
             FROM llm_usage_events \
             WHERE session_pk = $1 AND metadata->>'claude_turn_id' = $2 \
             ORDER BY observed_at ASC \
             LIMIT 1",
        )
        .bind::<SqlUuid, _>(session_pk)
        .bind::<Text, _>(&identity.value)
        .get_result::<TurnIndexRow>(connection),
        TurnIdentityKind::ResponseId => sql_query(
            "SELECT turn_index \
             FROM llm_usage_events \
             WHERE session_pk = $1 AND metadata->>'response_id' = $2 \
             ORDER BY observed_at ASC \
             LIMIT 1",
        )
        .bind::<SqlUuid, _>(session_pk)
        .bind::<Text, _>(&identity.value)
        .get_result::<TurnIndexRow>(connection),
        TurnIdentityKind::MessageId => sql_query(
            "SELECT turn_index \
             FROM llm_usage_events \
             WHERE session_pk = $1 AND metadata->>'message_id' = $2 \
             ORDER BY observed_at ASC \
             LIMIT 1",
        )
        .bind::<SqlUuid, _>(session_pk)
        .bind::<Text, _>(&identity.value)
        .get_result::<TurnIndexRow>(connection),
        TurnIdentityKind::RequestHash => sql_query(
            "SELECT turn_index \
             FROM llm_usage_events \
             WHERE session_pk = $1 AND metadata->>'request_hash' = $2 \
             ORDER BY observed_at ASC \
             LIMIT 1",
        )
        .bind::<SqlUuid, _>(session_pk)
        .bind::<Text, _>(&identity.value)
        .get_result::<TurnIndexRow>(connection),
    };
    row.optional()
        .map(|row| row.map(|row| row.turn_index))
        .map_err(AppError::from)
}

fn next_session_turn_index(
    connection: &mut PgConnection,
    session_pk: Uuid,
) -> Result<i32, AppError> {
    agent_turns::table
        .filter(agent_turns::session_pk.eq(session_pk))
        .select(max(agent_turns::turn_index))
        .get_result::<Option<i32>>(connection)
        .map(|value| value.unwrap_or(0_i32).saturating_add(1_i32))
        .map_err(AppError::from)
}

fn upsert_turn(
    connection: &mut PgConnection,
    user_id: Uuid,
    session_pk: Uuid,
    turn_index: i32,
    observed_at: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<AgentTurn, AppError> {
    diesel::insert_into(agent_turns::table)
        .values(NewAgentTurn {
            id: Uuid::now_v7(),
            user_id,
            session_pk,
            turn_index,
            started_at: observed_at,
            ended_at: Some(observed_at),
            created_at: now,
            updated_at: now,
        })
        .on_conflict((agent_turns::session_pk, agent_turns::turn_index))
        .do_update()
        .set((
            agent_turns::started_at.eq(sql::<Timestamptz>(
                "LEAST(agent_turns.started_at, EXCLUDED.started_at)",
            )),
            agent_turns::ended_at.eq(sql::<Nullable<Timestamptz>>(
                "GREATEST(COALESCE(agent_turns.ended_at, EXCLUDED.ended_at), EXCLUDED.ended_at)",
            )),
            agent_turns::updated_at.eq(now),
        ))
        .get_result::<AgentTurn>(connection)
        .map_err(AppError::from)
}

fn load_filtered_events(
    connection: &mut PgConnection,
    filters: &EventFilters<'_>,
) -> Result<Vec<UsageEvent>, AppError> {
    let event_type = filters
        .event_type
        .and_then(non_empty)
        .map(validate_event_type)
        .transpose()?;

    let mut query = llm_usage_events::table.into_boxed();

    query = query.filter(llm_usage_events::user_id.eq(filters.user_id));

    if let Some(from) = filters.from {
        query = query.filter(llm_usage_events::observed_at.ge(from));
    }
    if let Some(to) = filters.to {
        query = query.filter(llm_usage_events::observed_at.lt(to));
    }
    if let Some(agent_name) = filters.agent_name.and_then(non_empty) {
        let normalized = agent_name_filter_values(agent_name);
        query = query.filter(llm_usage_events::agent_name.eq_any(normalized));
    }
    if let Some(session_id) = filters.session_id.and_then(non_empty) {
        query = query.filter(llm_usage_events::session_id.eq(session_id));
    }
    if let Some(session_pk) = filters.session_pk {
        query = query.filter(llm_usage_events::session_pk.eq(session_pk));
    }
    if let Some(turn_index) = filters.turn_index {
        query = query.filter(llm_usage_events::turn_index.eq(turn_index));
    }
    if let Some(provider) = filters.llm_provider.and_then(non_empty) {
        let normalized = normalize_slug(provider);
        query = query.filter(llm_usage_events::llm_provider.eq(normalized));
    }
    if let Some(model) = filters.llm_model.and_then(non_empty) {
        query = query.filter(llm_usage_events::llm_model.eq(model.trim()));
    }
    if let Some(event_type) = event_type {
        query = query.filter(llm_usage_events::event_type.eq(event_type));
    }

    query
        .order(llm_usage_events::observed_at.desc())
        .limit(filters.limit)
        .offset(filters.offset)
        .load::<UsageEvent>(connection)
        .map_err(AppError::from)
}

fn load_device_contexts_for_events(
    connection: &mut PgConnection,
    events: &[UsageEvent],
) -> Result<HashMap<Uuid, Device>, AppError> {
    let ids: HashSet<Uuid> = events.iter().map(|event| event.device_context_id).collect();
    if ids.is_empty() {
        return Ok(HashMap::new());
    }

    let ids: Vec<_> = ids.into_iter().collect();
    let devices = devices::table
        .filter(devices::id.eq_any(ids))
        .load::<Device>(connection)?;

    Ok(devices
        .into_iter()
        .map(|device| {
            touch_device_metadata(&device);
            (device.id, device)
        })
        .collect())
}

fn load_attachments_for_events(
    connection: &mut PgConnection,
    events: &[UsageEvent],
) -> Result<HashMap<Uuid, Vec<UsageEventAttachmentResponse>>, AppError> {
    let event_ids = events.iter().map(|event| event.id).collect::<Vec<_>>();
    if event_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let attachments = llm_usage_event_attachments::table
        .filter(llm_usage_event_attachments::event_pk.eq_any(event_ids))
        .order((
            llm_usage_event_attachments::event_pk.asc(),
            llm_usage_event_attachments::position.asc(),
        ))
        .load::<UsageEventAttachment>(connection)?;
    let mut by_event = HashMap::new();
    for attachment in attachments {
        let event_pk = attachment.event_pk;
        // The shared row model includes authorization/audit columns that are not
        // repeated in each event response attachment object.
        std::hint::black_box((attachment.user_id, attachment.created_at));
        by_event
            .entry(event_pk)
            .or_insert_with(Vec::new)
            .push(UsageEventAttachmentResponse {
                id: attachment.id,
                position: attachment.position,
                media_type: ImageMediaType::from_stored(&attachment.media_type)?,
                byte_size: attachment.byte_size,
                sha256: hex::encode(attachment.sha256),
                content_available: attachment.content.is_some(),
            });
    }
    Ok(by_event)
}

fn event_response(
    event: UsageEvent,
    device: Option<&Device>,
    attachments: Vec<UsageEventAttachmentResponse>,
) -> UsageEventResponse {
    // created_at is an internal ingestion timestamp; APIs expose observed_at as
    // the user-facing event time while still selecting a complete Diesel row.
    std::hint::black_box(event.created_at);
    UsageEventResponse {
        id: event.id,
        event_id: event.event_id,
        user_id: event.user_id,
        device_context_id: event.device_context_id,
        host_name: device.map(|value| value.host_name.clone()),
        platform: device.map(|value| value.platform.clone()),
        session_pk: event.session_pk,
        turn_pk: event.turn_pk,
        agent_name: canonical_agent_name(&event.agent_name),
        agent_version: event.agent_version,
        session_id: event.session_id,
        turn_index: event.turn_index,
        llm_provider: event.llm_provider,
        llm_model: event.llm_model,
        event_type: event.event_type,
        text: event.text,
        text_sha256: event.text_sha256.map(hex::encode),
        input_tokens: event.input_tokens,
        output_tokens: event.output_tokens,
        cache_read_tokens: event.cache_read_tokens,
        cache_write_tokens: event.cache_write_tokens,
        reasoning_tokens: event.reasoning_tokens,
        total_tokens: event.total_tokens,
        observed_at: event.observed_at,
        metadata: event.metadata,
        attachments,
    }
}

const fn touch_device_metadata(device: &Device) {
    std::hint::black_box((
        device.user_id,
        &device.host_name,
        &device.platform,
        &device.os_version,
        device.first_seen_at,
        device.last_seen_at,
        device.created_at,
        device.updated_at,
    ));
}

const fn touch_session_metadata(session: &AgentSession) {
    std::hint::black_box((session.created_at, session.updated_at, &session.metadata));
}

const fn touch_turn_metadata(turn: &AgentTurn) {
    std::hint::black_box((
        turn.user_id,
        turn.session_pk,
        turn.created_at,
        turn.updated_at,
    ));
}

#[cfg(test)]
fn latest_optional(
    left: Option<DateTime<Utc>>,
    right: Option<DateTime<Utc>>,
) -> Option<DateTime<Utc>> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

struct EventFilters<'a> {
    user_id: Uuid,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
    user_filter: Option<&'a str>,
    agent_name: Option<&'a str>,
    session_id: Option<&'a str>,
    session_pk: Option<Uuid>,
    turn_index: Option<i32>,
    llm_provider: Option<&'a str>,
    llm_model: Option<&'a str>,
    event_type: Option<&'a str>,
    limit: i64,
    offset: i64,
}

struct SummaryDimensions([bool; SummaryDimension::COUNT]);

#[derive(Clone, Copy)]
enum SummaryDimension {
    Day,
    User,
    Device,
    Agent,
    Provider,
    Model,
    EventType,
}

impl SummaryDimension {
    const COUNT: usize = 7;

    const fn index(self) -> usize {
        match self {
            Self::Day => 0_usize,
            Self::User => 1_usize,
            Self::Device => 2_usize,
            Self::Agent => 3_usize,
            Self::Provider => 4_usize,
            Self::Model => 5_usize,
            Self::EventType => 6_usize,
        }
    }
}

impl SummaryDimensions {
    fn from_group_by(group_by: &[String]) -> Self {
        let includes = |name: &str| group_by.iter().any(|value| value == name);
        Self([
            includes("day"),
            includes("user"),
            includes("device"),
            includes("agent"),
            includes("provider"),
            includes("model"),
            includes("event_type"),
        ])
    }

    const fn enabled(&self, dimension: SummaryDimension) -> bool {
        self.0[dimension.index()]
    }
}

struct SummarySqlFilters {
    user_id: Uuid,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
    user_filter_id: Option<Uuid>,
    user_filter_pattern: Option<String>,
    agent_names: Vec<String>,
    session_id: Option<String>,
    session_pk: Option<Uuid>,
    turn_index: Option<i32>,
    provider: Option<String>,
    model: Option<String>,
    event_type: Option<String>,
}

impl SummarySqlFilters {
    fn from_event_filters(filters: &EventFilters<'_>) -> Result<Self, AppError> {
        let user_filter = filters.user_filter.and_then(non_empty).map(str::trim);
        let user_filter_id = user_filter.and_then(|value| Uuid::parse_str(value).ok());
        let user_filter_pattern =
            user_filter.map(|value| format!("%{}%", escape_like_pattern(value)));
        let agent_names = filters
            .agent_name
            .and_then(non_empty)
            .map(agent_name_filter_values)
            .unwrap_or_default();
        let session_id = filters
            .session_id
            .and_then(non_empty)
            .map(|value| value.trim().to_owned());
        let provider = filters.llm_provider.and_then(non_empty).map(normalize_slug);
        let model = filters
            .llm_model
            .and_then(non_empty)
            .map(|value| value.trim().to_owned());
        let event_type = filters.event_type.map(validate_event_type).transpose()?;

        Ok(Self {
            user_id: filters.user_id,
            from: filters.from,
            to: filters.to,
            user_filter_id,
            user_filter_pattern,
            agent_names,
            session_id,
            session_pk: filters.session_pk,
            turn_index: filters.turn_index,
            provider,
            model,
            event_type,
        })
    }
}

#[derive(QueryableByName)]
struct SummaryAggregateRow {
    #[diesel(sql_type = Nullable<Text>)]
    day: Option<String>,
    #[diesel(sql_type = Nullable<SqlUuid>)]
    user_id: Option<Uuid>,
    #[diesel(sql_type = Nullable<Text>)]
    user_name: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    user_email: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    host_name: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    platform: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    os_version: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    agent_name: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    llm_provider: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    llm_model: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    event_type: Option<String>,
    #[diesel(sql_type = BigInt)]
    sessions: i64,
    #[diesel(sql_type = BigInt)]
    turns: i64,
    #[diesel(sql_type = BigInt)]
    requests: i64,
    #[diesel(sql_type = BigInt)]
    responses: i64,
    #[diesel(sql_type = BigInt)]
    input_tokens: i64,
    #[diesel(sql_type = BigInt)]
    output_tokens: i64,
    #[diesel(sql_type = BigInt)]
    cache_read_tokens: i64,
    #[diesel(sql_type = BigInt)]
    cache_write_tokens: i64,
    #[diesel(sql_type = BigInt)]
    reasoning_tokens: i64,
    #[diesel(sql_type = BigInt)]
    total_tokens: i64,
}

impl From<SummaryAggregateRow> for SummaryRow {
    fn from(row: SummaryAggregateRow) -> Self {
        Self {
            day: row.day,
            user_id: row.user_id,
            user_name: row.user_name,
            user_email: row.user_email,
            host_name: row.host_name,
            platform: row.platform,
            os_version: row.os_version,
            agent_name: row.agent_name,
            llm_provider: row.llm_provider,
            llm_model: row.llm_model,
            event_type: row.event_type,
            sessions: non_negative_count(row.sessions),
            turns: non_negative_count(row.turns),
            requests: row.requests,
            responses: row.responses,
            input_tokens: row.input_tokens,
            output_tokens: row.output_tokens,
            cache_read_tokens: row.cache_read_tokens,
            cache_write_tokens: row.cache_write_tokens,
            reasoning_tokens: row.reasoning_tokens,
            total_tokens: row.total_tokens,
        }
    }
}

#[derive(QueryableByName)]
struct TurnIndexRow {
    #[diesel(sql_type = Integer)]
    turn_index: i32,
}

struct UpsertSessionInput<'a> {
    device: &'a Device,
    user_id: Uuid,
    event: &'a IngestUsageEvent,
    agent_name: &'a str,
    agent_version: Option<String>,
    observed_at: DateTime<Utc>,
    now: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::{
        EventFilters, SummaryDimension, SummaryDimensions, SummarySqlFilters, TurnIdentityKind,
        agent_name_filter_values, canonical_agent_name, choose_turn_index, event_response,
        latest_optional, normalize_limit, normalize_slug, normalized_tokens, parse_group_by,
        turn_identity_from_metadata, validate_batch, validate_event, validate_event_type,
    };
    use chrono::{DateTime, TimeZone, Utc};
    use serde_json::json;
    use uuid::Uuid;

    use crate::{
        db::models::{Device, UsageEvent},
        error::AppError,
        usage::{
            AgentPayload, DevicePayload, IngestEventsRequest, IngestUsageEvent, LlmPayload,
            SummaryRow, TokenUsagePayload, UsageEventType,
        },
    };

    #[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
    struct SummaryKey {
        day: Option<String>,
        user_id: Option<Uuid>,
        user_name: Option<String>,
        user_email: Option<String>,
        host_name: Option<String>,
        platform: Option<String>,
        os_version: Option<String>,
        agent_name: Option<String>,
        llm_provider: Option<String>,
        llm_model: Option<String>,
        event_type: Option<String>,
    }

    #[derive(Default)]
    struct SummaryAccumulator {
        sessions: HashSet<Uuid>,
        turns: HashSet<Uuid>,
        requests: i64,
        responses: i64,
        input_tokens: i64,
        output_tokens: i64,
        cache_read_tokens: i64,
        cache_write_tokens: i64,
        reasoning_tokens: i64,
        total_tokens: i64,
    }

    struct UserSummaryProfile {
        email: String,
        name: Option<String>,
    }

    impl UserSummaryProfile {
        fn name(&self) -> Option<String> {
            self.name
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
        }
    }

    fn aggregate_summary(
        events: &[UsageEvent],
        device_contexts: &HashMap<Uuid, Device>,
        user_contexts: &HashMap<Uuid, UserSummaryProfile>,
        group_by: &[String],
    ) -> Vec<SummaryRow> {
        let mut accumulators: HashMap<SummaryKey, SummaryAccumulator> = HashMap::new();

        for event in events {
            let key = summary_key(event, device_contexts, user_contexts, group_by);
            let accumulator = accumulators.entry(key).or_default();
            accumulator.sessions.insert(event.session_pk);
            accumulator.turns.insert(event.turn_pk);
            if event.event_type == "request" {
                accumulator.requests = accumulator.requests.saturating_add(1);
            }
            if event.event_type == "response" {
                accumulator.responses = accumulator.responses.saturating_add(1);
            }
            accumulator.input_tokens = accumulator.input_tokens.saturating_add(event.input_tokens);
            accumulator.output_tokens = accumulator
                .output_tokens
                .saturating_add(event.output_tokens);
            accumulator.cache_read_tokens = accumulator
                .cache_read_tokens
                .saturating_add(event.cache_read_tokens);
            accumulator.cache_write_tokens = accumulator
                .cache_write_tokens
                .saturating_add(event.cache_write_tokens);
            accumulator.reasoning_tokens = accumulator
                .reasoning_tokens
                .saturating_add(event.reasoning_tokens);
            accumulator.total_tokens = accumulator.total_tokens.saturating_add(event.total_tokens);
        }

        let mut rows: Vec<_> = accumulators
            .into_iter()
            .map(|(key, accumulator)| SummaryRow {
                day: key.day,
                user_id: key.user_id,
                user_name: key.user_name,
                user_email: key.user_email,
                host_name: key.host_name,
                platform: key.platform,
                os_version: key.os_version,
                agent_name: key.agent_name,
                llm_provider: key.llm_provider,
                llm_model: key.llm_model,
                event_type: key.event_type,
                sessions: accumulator.sessions.len(),
                turns: accumulator.turns.len(),
                requests: accumulator.requests,
                responses: accumulator.responses,
                input_tokens: accumulator.input_tokens,
                output_tokens: accumulator.output_tokens,
                cache_read_tokens: accumulator.cache_read_tokens,
                cache_write_tokens: accumulator.cache_write_tokens,
                reasoning_tokens: accumulator.reasoning_tokens,
                total_tokens: accumulator.total_tokens,
            })
            .collect();
        rows.sort_by(|left, right| {
            right
                .total_tokens
                .cmp(&left.total_tokens)
                .then_with(|| left.agent_name.cmp(&right.agent_name))
        });
        rows
    }

    fn summary_key(
        event: &UsageEvent,
        device_contexts: &HashMap<Uuid, Device>,
        user_contexts: &HashMap<Uuid, UserSummaryProfile>,
        group_by: &[String],
    ) -> SummaryKey {
        let includes = |name: &str| group_by.iter().any(|value| value == name);
        let device = device_contexts.get(&event.device_context_id);
        let user = user_contexts.get(&event.user_id);
        SummaryKey {
            day: includes("day").then(|| event.observed_at.date_naive().to_string()),
            user_id: includes("user").then_some(event.user_id),
            user_name: includes("user")
                .then(|| user.and_then(UserSummaryProfile::name))
                .flatten(),
            user_email: includes("user")
                .then(|| user.map(|value| value.email.clone()))
                .flatten(),
            host_name: includes("device")
                .then(|| device.map(|value| value.host_name.clone()))
                .flatten(),
            platform: includes("device")
                .then(|| device.map(|value| value.platform.clone()))
                .flatten(),
            os_version: includes("device")
                .then(|| device.and_then(|value| value.os_version.clone()))
                .flatten(),
            agent_name: includes("agent").then(|| canonical_agent_name(&event.agent_name)),
            llm_provider: includes("provider").then(|| event.llm_provider.clone()),
            llm_model: includes("model").then(|| event.llm_model.clone()),
            event_type: includes("event_type").then(|| event.event_type.clone()),
        }
    }

    #[test]
    fn normalize_slug_converts_agent_names() {
        assert_eq!(
            normalize_slug("Claude Code"),
            "claude-code",
            "agent names should be normalized to lowercase kebab-case"
        );
    }

    #[test]
    fn canonical_agent_name_groups_claude_clients() {
        assert_eq!(canonical_agent_name("Claude"), "claude-code");
        assert_eq!(canonical_agent_name("Claude Code"), "claude-code");
        assert_eq!(canonical_agent_name("claude-desktop"), "claude-code");
        assert_eq!(canonical_agent_name("Codex"), "codex");
    }

    #[test]
    fn agent_name_filter_values_include_historical_claude_slugs() {
        assert_eq!(
            agent_name_filter_values("Claude Code"),
            vec![
                "claude-code".to_owned(),
                "claude".to_owned(),
                "claude-desktop".to_owned(),
            ]
        );
        assert_eq!(agent_name_filter_values("Codex"), vec!["codex".to_owned()]);
    }

    #[test]
    fn latest_optional_keeps_the_newer_timestamp() {
        let early = Utc
            .with_ymd_and_hms(2026, 5, 20, 1, 0, 0)
            .single()
            .expect("valid timestamp");
        let late = Utc
            .with_ymd_and_hms(2026, 5, 20, 2, 0, 0)
            .single()
            .expect("valid timestamp");

        assert_eq!(
            latest_optional(Some(early), Some(late)),
            Some(late),
            "latest_optional should keep the later timestamp"
        );
    }

    #[test]
    fn parse_group_by_uses_default_when_empty() {
        assert_eq!(
            parse_group_by(Some("")),
            vec!["user", "agent", "provider", "model"],
            "empty group_by should fall back to default dimensions"
        );
    }

    #[test]
    fn parse_group_by_keeps_employee_alias_for_older_clients() {
        assert_eq!(
            parse_group_by(Some("employee,device,llm_provider,llm_model")),
            vec!["user", "device", "provider", "model"],
            "employee and llm aliases should map to current summary dimensions"
        );
    }

    #[test]
    fn summary_dimensions_follow_requested_group_by_values() {
        let dimensions =
            SummaryDimensions::from_group_by(&parse_group_by(Some("user,agent,event_type")));

        assert!(dimensions.enabled(SummaryDimension::User));
        assert!(dimensions.enabled(SummaryDimension::Agent));
        assert!(dimensions.enabled(SummaryDimension::EventType));
        assert!(!dimensions.enabled(SummaryDimension::Day));
        assert!(!dimensions.enabled(SummaryDimension::Device));
        assert!(!dimensions.enabled(SummaryDimension::Provider));
        assert!(!dimensions.enabled(SummaryDimension::Model));
    }

    #[test]
    fn summary_sql_filters_normalize_runtime_filters() {
        let filters = EventFilters {
            user_id: fixed_uuid(42_u128),
            from: Some(timestamp(2_u32, 0_u32, 0_u32)),
            to: None,
            user_filter: Some(" Alice_% "),
            agent_name: Some("Claude Desktop"),
            session_id: Some(" session-1 "),
            session_pk: None,
            turn_index: None,
            llm_provider: Some("OpenAI"),
            llm_model: Some(" gpt-5.5 "),
            event_type: Some(" Response "),
            limit: 50_i64,
            offset: 0_i64,
        };

        let sql_filters =
            SummarySqlFilters::from_event_filters(&filters).expect("filters should normalize");

        assert_eq!(sql_filters.user_id, fixed_uuid(42_u128));
        assert!(sql_filters.from.is_some());
        assert!(sql_filters.to.is_none());
        assert_eq!(sql_filters.user_filter_id, None);
        assert_eq!(
            sql_filters.user_filter_pattern.as_deref(),
            Some("%Alice\\_\\%%")
        );
        assert_eq!(
            sql_filters.agent_names,
            vec![
                "claude-code".to_owned(),
                "claude".to_owned(),
                "claude-desktop".to_owned(),
            ]
        );
        assert_eq!(sql_filters.session_id.as_deref(), Some("session-1"));
        assert_eq!(sql_filters.session_pk, None);
        assert_eq!(sql_filters.turn_index, None);
        assert_eq!(sql_filters.provider.as_deref(), Some("openai"));
        assert_eq!(sql_filters.model.as_deref(), Some("gpt-5.5"));
        assert_eq!(sql_filters.event_type.as_deref(), Some("response"));
    }

    #[test]
    fn validate_event_rejects_zero_turn_index() {
        let mut event = sample_event();
        event.turn_index = 0_i32;

        let error = validate_event(&event).expect_err("zero turn_index should be invalid");
        assert!(
            matches!(&error, AppError::Validation(message) if message.contains("turn_index")),
            "zero turn_index should return a validation error"
        );
    }

    #[test]
    fn validate_batch_rejects_oversized_batches() {
        let request = IngestEventsRequest {
            events: vec![sample_event()],
            diagnostic_captures: Vec::new(),
        };

        let error =
            validate_batch(&request, 0_usize).expect_err("oversized batch should be invalid");
        assert!(
            matches!(&error, AppError::Validation(message) if message.contains("batch size")),
            "oversized batch should return a validation error"
        );
    }

    #[test]
    fn normalized_tokens_derives_total_from_components() {
        let mut event = sample_event();
        event.token_usage = TokenUsagePayload {
            input_tokens: Some(11_i64),
            output_tokens: Some(13_i64),
            cache_read_tokens: Some(2_i64),
            cache_write_tokens: Some(3_i64),
            reasoning_tokens: Some(5_i64),
            total_tokens: None,
        };

        let tokens = normalized_tokens(&event).expect("token usage should normalize");
        assert_eq!(
            tokens.total, 34_i64,
            "missing total_tokens should be derived from all token components"
        );
    }

    #[test]
    fn normalized_tokens_rejects_negative_components() {
        let mut event = sample_event();
        event.token_usage = TokenUsagePayload {
            input_tokens: Some(-1_i64),
            ..TokenUsagePayload::default()
        };

        let Err(error) = normalized_tokens(&event) else {
            panic!("negative token usage should be invalid");
        };
        assert!(
            matches!(&error, AppError::Validation(message) if message.contains("input_tokens")),
            "negative input_tokens should return a validation error"
        );
    }

    #[test]
    fn validate_event_type_normalizes_known_values() {
        assert_eq!(
            validate_event_type(" Request ").expect("request event type should be valid"),
            "request",
            "event_type should be trimmed and lowercased"
        );
        assert_eq!(
            validate_event_type("RESPONSE").expect("response event type should be valid"),
            "response",
            "event_type should accept uppercase response values"
        );
    }

    #[test]
    fn validate_event_type_rejects_unknown_values() {
        let error =
            validate_event_type("tool_call").expect_err("unknown event type should be invalid");
        assert!(
            matches!(&error, AppError::Validation(message) if message.contains("request or response")),
            "unknown event_type should return a validation error"
        );
    }

    #[test]
    fn choose_turn_index_reuses_existing_provider_identity() {
        assert_eq!(
            choose_turn_index(9_i32, Some(1_i32), 5_i32),
            1_i32,
            "request and response events for the same provider response should share one turn"
        );
    }

    #[test]
    fn choose_turn_index_allocates_after_conflicting_restarted_counter() {
        assert_eq!(
            choose_turn_index(1_i32, None, 5_i32),
            5_i32,
            "a restarted collector must not merge a new provider response into an old turn"
        );
    }

    #[test]
    fn choose_turn_index_allocates_after_restarted_counter_gap() {
        assert_eq!(
            choose_turn_index(2_i32, None, 5_i32),
            5_i32,
            "a restarted collector should append even if earlier filtered turns left a gap"
        );
    }

    #[test]
    fn choose_turn_index_keeps_requested_gap_when_there_is_no_collision() {
        assert_eq!(
            choose_turn_index(4_i32, None, 2_i32),
            4_i32,
            "non-conflicting collector indexes should remain stable"
        );
    }

    #[test]
    fn turn_identity_prefers_codex_turn_id_before_provider_response_id() {
        let identity = turn_identity_from_metadata(&json!({
            "codex_turn_id": "turn-a",
            "response_id": "resp-a",
            "request_hash": "hash-a"
        }))
        .expect("codex_turn_id should produce a turn identity");

        assert!(
            matches!(identity.kind, TurnIdentityKind::CodexTurnId),
            "one Codex turn can span multiple provider response ids"
        );
        assert_eq!(identity.value, "turn-a");
    }

    #[test]
    fn turn_identity_uses_claude_turn_id_before_provider_message_id() {
        let identity = turn_identity_from_metadata(&json!({
            "claude_turn_id": "claude-turn-a",
            "message_id": "msg-a",
            "request_hash": "hash-a"
        }))
        .expect("claude_turn_id should produce a turn identity");

        assert!(
            matches!(identity.kind, TurnIdentityKind::ClaudeTurnId),
            "one Claude user turn can span multiple provider message ids"
        );
        assert_eq!(identity.value, "claude-turn-a");
    }

    #[test]
    fn normalize_limit_uses_fallback_and_clamps_bounds() {
        assert_eq!(
            normalize_limit(None, 50_i64, 100_i64),
            50_i64,
            "missing limit should use the fallback"
        );
        assert_eq!(
            normalize_limit(Some(0_i64), 50_i64, 100_i64),
            1_i64,
            "limit should clamp to the minimum page size"
        );
        assert_eq!(
            normalize_limit(Some(500_i64), 50_i64, 100_i64),
            100_i64,
            "limit should clamp to the configured maximum page size"
        );
    }

    #[test]
    fn aggregate_summary_counts_sessions_turns_events_and_tokens() {
        let device_context_id = fixed_uuid(1_u128);
        let events = vec![
            usage_event(
                "evt-summary-request",
                "request",
                10_i64,
                0_i64,
                0_i64,
                10_i64,
            ),
            usage_event(
                "evt-summary-response",
                "response",
                0_i64,
                12_i64,
                3_i64,
                15_i64,
            ),
        ];
        let device_contexts =
            HashMap::from([(device_context_id, device_context(device_context_id))]);
        let group_by = vec![
            "day".to_owned(),
            "user".to_owned(),
            "device".to_owned(),
            "agent".to_owned(),
            "provider".to_owned(),
            "model".to_owned(),
        ];

        let user_contexts = HashMap::from([(
            fixed_uuid(100_u128),
            UserSummaryProfile {
                email: "alice@example.invalid".to_owned(),
                name: Some("Alice".to_owned()),
            },
        )]);
        let rows = aggregate_summary(&events, &device_contexts, &user_contexts, &group_by);

        assert_eq!(
            rows.len(),
            1_usize,
            "request and response events in the same dimensions should share one summary row"
        );
        let row = rows.first().expect("summary should contain one row");
        assert_summary_dimensions(row);
        assert_eq!(
            row.sessions, 1_usize,
            "summary row should count distinct sessions"
        );
        assert_eq!(
            row.turns, 1_usize,
            "summary row should count distinct turns"
        );
        assert_eq!(
            row.requests, 1_i64,
            "summary row should count request events"
        );
        assert_eq!(
            row.responses, 1_i64,
            "summary row should count response events"
        );
        assert_eq!(
            row.total_tokens, 25_i64,
            "summary row should sum total tokens across request and response events"
        );
    }

    #[test]
    fn aggregate_summary_groups_historical_claude_agent_slugs() {
        let device_context_id = fixed_uuid(1_u128);
        let mut desktop_event = usage_event(
            "evt-claude-desktop-request",
            "request",
            8_i64,
            0_i64,
            0_i64,
            8_i64,
        );
        desktop_event.agent_name = "claude-desktop".to_owned();
        let mut cli_event = usage_event(
            "evt-claude-code-response",
            "response",
            0_i64,
            12_i64,
            0_i64,
            12_i64,
        );
        cli_event.agent_name = "claude-code".to_owned();
        let device_contexts =
            HashMap::from([(device_context_id, device_context(device_context_id))]);
        let group_by = vec![
            "agent".to_owned(),
            "provider".to_owned(),
            "model".to_owned(),
        ];
        let rows = aggregate_summary(
            &[desktop_event, cli_event],
            &device_contexts,
            &HashMap::new(),
            &group_by,
        );

        assert_eq!(
            rows.len(),
            1_usize,
            "Claude CLI and Desktop rows should aggregate as one product"
        );
        let row = rows.first().expect("summary should contain one row");
        assert_eq!(row.agent_name.as_deref(), Some("claude-code"));
        assert_eq!(row.requests, 1_i64);
        assert_eq!(row.responses, 1_i64);
        assert_eq!(row.total_tokens, 20_i64);
    }

    #[test]
    fn event_response_includes_device_context_labels() {
        let device_context_id = fixed_uuid(1_u128);
        let device = device_context(device_context_id);
        let response = event_response(
            usage_event(
                "evt-device-response",
                "response",
                0_i64,
                12_i64,
                0_i64,
                12_i64,
            ),
            Some(&device),
            Vec::new(),
        );

        assert_eq!(response.host_name.as_deref(), Some("alice-mbp"));
        assert_eq!(response.platform.as_deref(), Some("macos"));
    }

    fn assert_summary_dimensions(row: &SummaryRow) {
        assert_eq!(row.day.as_deref(), Some("2026-05-20"));
        assert_eq!(row.user_id, Some(fixed_uuid(100_u128)));
        assert_eq!(row.user_name.as_deref(), Some("Alice"));
        assert_eq!(row.user_email.as_deref(), Some("alice@example.invalid"));
        assert_eq!(row.host_name.as_deref(), Some("alice-mbp"));
        assert_eq!(row.platform.as_deref(), Some("macos"));
        assert_eq!(row.os_version.as_deref(), Some("15.5"));
        assert_eq!(row.agent_name.as_deref(), Some("codex"));
        assert_eq!(row.llm_provider.as_deref(), Some("openai"));
        assert_eq!(row.llm_model.as_deref(), Some("gpt-5.5"));
    }

    fn sample_event() -> IngestUsageEvent {
        IngestUsageEvent {
            event_id: "evt-sample".to_owned(),
            observed_at: timestamp(1_u32, 0_u32, 0_u32),
            device: DevicePayload {
                host_name: "alice-mbp".to_owned(),
                platform: "macos".to_owned(),
                os_version: Some("15.5".to_owned()),
            },
            agent: AgentPayload {
                name: "Codex".to_owned(),
                version: Some("1.0.0".to_owned()),
            },
            session_id: "session-001".to_owned(),
            turn_index: 1_i32,
            llm: LlmPayload {
                provider: "OpenAI".to_owned(),
                model: "gpt-5.5".to_owned(),
            },
            event_type: UsageEventType::Request,
            text: Some("hello".to_owned()),
            token_usage: TokenUsagePayload {
                input_tokens: Some(5_i64),
                output_tokens: Some(7_i64),
                cache_read_tokens: None,
                cache_write_tokens: None,
                reasoning_tokens: None,
                total_tokens: None,
            },
            metadata: json!({}),
            attachments: Vec::new(),
        }
    }

    fn usage_event(
        event_id: &str,
        event_type: &str,
        input_tokens: i64,
        output_tokens: i64,
        reasoning_tokens: i64,
        total_tokens: i64,
    ) -> UsageEvent {
        UsageEvent {
            id: fixed_uuid(10_u128),
            user_id: fixed_uuid(100_u128),
            device_context_id: fixed_uuid(1_u128),
            session_pk: fixed_uuid(2_u128),
            turn_pk: fixed_uuid(3_u128),
            event_id: event_id.to_owned(),
            agent_name: "codex".to_owned(),
            agent_version: Some("1.0.0".to_owned()),
            session_id: "session-001".to_owned(),
            turn_index: 1_i32,
            llm_provider: "openai".to_owned(),
            llm_model: "gpt-5.5".to_owned(),
            event_type: event_type.to_owned(),
            text: None,
            text_sha256: None,
            input_tokens,
            output_tokens,
            cache_read_tokens: 0_i64,
            cache_write_tokens: 0_i64,
            reasoning_tokens,
            total_tokens,
            observed_at: timestamp(1_u32, 0_u32, 0_u32),
            metadata: json!({}),
            created_at: timestamp(1_u32, 0_u32, 1_u32),
        }
    }

    fn device_context(id: Uuid) -> Device {
        Device {
            id,
            user_id: fixed_uuid(100_u128),
            host_name: "alice-mbp".to_owned(),
            platform: "macos".to_owned(),
            os_version: Some("15.5".to_owned()),
            first_seen_at: timestamp(1_u32, 0_u32, 0_u32),
            last_seen_at: timestamp(1_u32, 0_u32, 1_u32),
            created_at: timestamp(1_u32, 0_u32, 0_u32),
            updated_at: timestamp(1_u32, 0_u32, 1_u32),
        }
    }

    fn timestamp(hour: u32, minute: u32, second: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 5, 20, hour, minute, second)
            .single()
            .expect("valid timestamp")
    }

    const fn fixed_uuid(value: u128) -> Uuid {
        Uuid::from_u128(value)
    }
}
