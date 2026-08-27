//! Transactional SQLite implementation of usage ingest and relational queries.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use rusqlite::{
    Connection, OptionalExtension as _, Row, Transaction, TransactionBehavior, params,
    params_from_iter,
    types::{Type, Value as SqlValue},
};
use uuid::Uuid;

use crate::{
    error::AppError,
    search::projection::SearchProjection,
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
            TurnIdentity, agent_name_filter_values, canonical_agent_name, choose_turn_index,
            escape_like_pattern, non_empty, non_negative_count, normalize_limit, normalize_slug,
            normalize_text, normalized_optional, normalized_tokens, parse_group_by, sha256_bytes,
            turn_identity, validate_batch, validate_event_type,
        },
        session_metadata_from_event,
    },
};

use super::models::{AttachmentRecord, DeviceRecord, EventRecord, SessionRecord, TurnRecord};

pub(super) fn ingest_events(
    connection: &mut Connection,
    request: &IngestEventsRequest,
    user_id: Uuid,
    max_batch_size: usize,
) -> Result<IngestEventsResponse, AppError> {
    let validated_attachments = validate_batch(request, max_batch_size)?;
    // IMMEDIATE obtains the single SQLite write reservation before turn indexes
    // are allocated, preventing two local writers from merging distinct turns.
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let mut accepted = 0_usize;
    let mut duplicates = 0_usize;
    for (event, attachments) in request.events.iter().zip(validated_attachments) {
        if ingest_one_event(&transaction, user_id, event, attachments)? {
            accepted = accepted.saturating_add(1);
        } else {
            duplicates = duplicates.saturating_add(1);
        }
    }

    let mut accepted_diagnostic_captures = 0_usize;
    let mut duplicate_diagnostic_captures = 0_usize;
    for capture in &request.diagnostic_captures {
        if ingest_one_diagnostic_capture(&transaction, user_id, capture)? {
            accepted_diagnostic_captures = accepted_diagnostic_captures.saturating_add(1);
        } else {
            duplicate_diagnostic_captures = duplicate_diagnostic_captures.saturating_add(1);
        }
    }
    transaction.commit()?;

    Ok(IngestEventsResponse {
        accepted,
        duplicates,
        rejected: 0,
        errors: Vec::new(),
        accepted_diagnostic_captures,
        duplicate_diagnostic_captures,
    })
}

#[expect(
    clippy::too_many_lines,
    reason = "One linear function keeps the event, attachments, and FTS projection visibly in one transaction."
)]
fn ingest_one_event(
    transaction: &Transaction<'_>,
    user_id: Uuid,
    event: &IngestUsageEvent,
    attachments: Vec<ValidatedImageAttachment>,
) -> Result<bool, AppError> {
    let event_id = normalize_text(&event.event_id);
    let exists = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM llm_usage_events WHERE event_id = ?1)",
        [&event_id],
        |row| row.get::<_, bool>(0),
    )?;
    if exists {
        return Ok(false);
    }

    let observed_at = timestamp_to_micros(event.observed_at);
    let now = timestamp_to_micros(Utc::now());
    let agent_name = canonical_agent_name(&event.agent.name);
    let agent_version = normalized_optional(event.agent.version.as_deref());
    let provider = normalize_slug(&event.llm.provider);
    let model = normalize_text(&event.llm.model);
    let device_id = upsert_device(transaction, user_id, event, observed_at, now)?;
    let session_id = upsert_session(
        transaction,
        user_id,
        device_id,
        event,
        &agent_name,
        agent_version.as_deref(),
        observed_at,
        now,
    )?;
    let turn_index = resolve_turn_index(transaction, session_id, event)?;
    let turn_id = upsert_turn(
        transaction,
        user_id,
        session_id,
        turn_index,
        observed_at,
        now,
    )?;
    let tokens = normalized_tokens(event)?;
    let text = normalized_optional(event.text.as_deref());
    let event_pk = Uuid::now_v7();
    let metadata = serde_json::to_string(&event.metadata)
        .map_err(|error| AppError::internal(format!("serialize event metadata: {error}")))?;

    let inserted = transaction.execute(
        "INSERT INTO llm_usage_events (
             id, user_id, device_context_id, session_pk, turn_pk, event_id,
             agent_name, agent_version, session_id, turn_index, llm_provider,
             llm_model, event_type, text, text_sha256, input_tokens,
             output_tokens, cache_read_tokens, cache_write_tokens,
             reasoning_tokens, total_tokens, observed_at, metadata, created_at
         ) VALUES (
             ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12,
             ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24
         ) ON CONFLICT(event_id) DO NOTHING",
        params![
            event_pk.to_string(),
            user_id.to_string(),
            device_id.to_string(),
            session_id.to_string(),
            turn_id.to_string(),
            event_id,
            agent_name,
            agent_version,
            normalize_text(&event.session_id),
            turn_index,
            provider,
            model,
            event.event_type.as_str(),
            text,
            text.as_deref().map(sha256_bytes),
            tokens.input,
            tokens.output,
            tokens.cache_read,
            tokens.cache_write,
            tokens.reasoning,
            tokens.total,
            observed_at,
            metadata,
            now,
        ],
    )?;
    if inserted != 1 {
        return Ok(false);
    }

    for attachment in attachments {
        transaction.execute(
            "INSERT INTO llm_usage_event_attachments (
                 id, user_id, event_pk, position, media_type, byte_size,
                 sha256, content, created_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                Uuid::now_v7().to_string(),
                user_id.to_string(),
                event_pk.to_string(),
                attachment.position,
                attachment.media_type.as_str(),
                attachment.byte_size,
                attachment.sha256,
                attachment.content,
                now,
            ],
        )?;
    }

    insert_search_projection(transaction, event_pk, user_id, session_id, event)?;
    Ok(true)
}

fn upsert_device(
    transaction: &Transaction<'_>,
    user_id: Uuid,
    event: &IngestUsageEvent,
    observed_at: i64,
    now: i64,
) -> Result<Uuid, AppError> {
    let id = transaction.query_row(
        "INSERT INTO devices (
             id, user_id, host_name, platform, os_version, first_seen_at,
             last_seen_at, created_at, updated_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6, ?7, ?7)
         ON CONFLICT(user_id, host_name, platform) DO UPDATE SET
             os_version = COALESCE(excluded.os_version, devices.os_version),
             first_seen_at = MIN(devices.first_seen_at, excluded.first_seen_at),
             last_seen_at = MAX(devices.last_seen_at, excluded.last_seen_at),
             updated_at = excluded.updated_at
         RETURNING id",
        params![
            Uuid::now_v7().to_string(),
            user_id.to_string(),
            normalize_text(&event.device.host_name),
            normalize_slug(&event.device.platform),
            normalized_optional(event.device.os_version.as_deref()),
            observed_at,
            now,
        ],
        |row| row.get::<_, String>(0),
    )?;
    parse_uuid(&id, "device id")
}

#[expect(
    clippy::too_many_arguments,
    reason = "The session upsert receives one explicit value for every evolving boundary."
)]
fn upsert_session(
    transaction: &Transaction<'_>,
    user_id: Uuid,
    device_id: Uuid,
    event: &IngestUsageEvent,
    agent_name: &str,
    agent_version: Option<&str>,
    observed_at: i64,
    now: i64,
) -> Result<Uuid, AppError> {
    let metadata = serde_json::to_string(&session_metadata_from_event(&event.metadata))
        .map_err(|error| AppError::internal(format!("serialize session metadata: {error}")))?;
    let id = transaction.query_row(
        "INSERT INTO agent_sessions (
             id, user_id, device_context_id, agent_name, agent_version,
             session_id, started_at, ended_at, metadata, created_at, updated_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7, ?8, ?9, ?9)
         ON CONFLICT(user_id, agent_name, session_id) DO UPDATE SET
             device_context_id = excluded.device_context_id,
             agent_version = COALESCE(excluded.agent_version, agent_sessions.agent_version),
             started_at = MIN(agent_sessions.started_at, excluded.started_at),
             ended_at = MAX(COALESCE(agent_sessions.ended_at, excluded.ended_at), excluded.ended_at),
             metadata = json_patch(agent_sessions.metadata, excluded.metadata),
             updated_at = excluded.updated_at
         RETURNING id",
        params![
            Uuid::now_v7().to_string(),
            user_id.to_string(),
            device_id.to_string(),
            agent_name,
            agent_version,
            normalize_text(&event.session_id),
            observed_at,
            metadata,
            now,
        ],
        |row| row.get::<_, String>(0),
    )?;
    parse_uuid(&id, "session id")
}

fn resolve_turn_index(
    transaction: &Transaction<'_>,
    session_pk: Uuid,
    event: &IngestUsageEvent,
) -> Result<i32, AppError> {
    let Some(identity) = turn_identity(event) else {
        return Ok(event.turn_index);
    };
    let existing = existing_turn_index_for_identity(transaction, session_pk, &identity)?;
    let next = transaction.query_row(
        "SELECT COALESCE(MAX(turn_index), 0) + 1 FROM agent_turns WHERE session_pk = ?1",
        [session_pk.to_string()],
        |row| row.get::<_, i32>(0),
    )?;
    Ok(choose_turn_index(event.turn_index, existing, next))
}

fn existing_turn_index_for_identity(
    transaction: &Transaction<'_>,
    session_pk: Uuid,
    identity: &TurnIdentity,
) -> Result<Option<i32>, AppError> {
    // Metadata keys come from the closed TurnIdentityKind enum rather than user
    // input, so selecting a fixed JSON path here cannot alter SQL structure.
    let path = format!("$.{}", identity.kind.metadata_key());
    transaction
        .query_row(
            "SELECT turn_index
             FROM llm_usage_events
             WHERE session_pk = ?1 AND json_extract(metadata, ?2) = ?3
             ORDER BY observed_at ASC, id ASC
             LIMIT 1",
            params![session_pk.to_string(), path, identity.value],
            |row| row.get::<_, i32>(0),
        )
        .optional()
        .map_err(AppError::from)
}

fn upsert_turn(
    transaction: &Transaction<'_>,
    user_id: Uuid,
    session_pk: Uuid,
    turn_index: i32,
    observed_at: i64,
    now: i64,
) -> Result<Uuid, AppError> {
    let id = transaction.query_row(
        "INSERT INTO agent_turns (
             id, user_id, session_pk, turn_index, started_at, ended_at,
             created_at, updated_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?5, ?6, ?6)
         ON CONFLICT(session_pk, turn_index) DO UPDATE SET
             started_at = MIN(agent_turns.started_at, excluded.started_at),
             ended_at = MAX(COALESCE(agent_turns.ended_at, excluded.ended_at), excluded.ended_at),
             updated_at = excluded.updated_at
         RETURNING id",
        params![
            Uuid::now_v7().to_string(),
            user_id.to_string(),
            session_pk.to_string(),
            turn_index,
            observed_at,
            now,
        ],
        |row| row.get::<_, String>(0),
    )?;
    parse_uuid(&id, "turn id")
}

fn insert_search_projection(
    transaction: &Transaction<'_>,
    event_pk: Uuid,
    user_id: Uuid,
    session_pk: Uuid,
    event: &IngestUsageEvent,
) -> Result<(), AppError> {
    let projection =
        SearchProjection::from_source(normalized_optional(event.text.as_deref()), &event.metadata);
    transaction.execute(
        "INSERT INTO usage_events_fts (
             event_pk, user_id, session_pk, session_id, content, tool_names,
             tool_content, commands, file_paths
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            event_pk.to_string(),
            user_id.to_string(),
            session_pk.to_string(),
            normalize_text(&event.session_id),
            projection.content,
            projection.tool_names.join("\n"),
            projection.tool_content.join("\n"),
            projection.commands.join("\n"),
            projection.file_paths.join("\n"),
        ],
    )?;
    Ok(())
}

fn ingest_one_diagnostic_capture(
    transaction: &Transaction<'_>,
    user_id: Uuid,
    capture: &IngestDiagnosticCapture,
) -> Result<bool, AppError> {
    let mut events = Vec::with_capacity(capture.event_ids.len());
    for event_id in &capture.event_ids {
        let event = transaction
            .query_row(
                "SELECT id, device_context_id, session_pk
                 FROM llm_usage_events
                 WHERE user_id = ?1 AND event_id = ?2",
                params![user_id.to_string(), event_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()?;
        let Some((event_pk, device_id, session_pk)) = event else {
            return Err(AppError::validation(
                "diagnostic capture events were not ingested for the authenticated user".to_owned(),
            ));
        };
        events.push((
            parse_uuid(&event_pk, "event id")?,
            parse_uuid(&device_id, "device id")?,
            parse_uuid(&session_pk, "session id")?,
        ));
    }
    let Some((_, device_id, session_pk)) = events.first().copied() else {
        return Err(AppError::validation(
            "diagnostic capture must reference at least one event".to_owned(),
        ));
    };
    if events
        .iter()
        .any(|(_, device, session)| *device != device_id || *session != session_pk)
    {
        return Err(AppError::validation(
            "diagnostic capture events must belong to one session and device".to_owned(),
        ));
    }

    let capture_pk = Uuid::now_v7();
    let payload = serde_json::to_string(&capture.payload)
        .map_err(|error| AppError::internal(format!("serialize diagnostic payload: {error}")))?;
    let inserted = transaction.execute(
        "INSERT INTO agent_diagnostic_captures (
             id, user_id, device_context_id, session_pk, capture_id, flow_id,
             captured_at, collector_version, payload, created_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
         ON CONFLICT(user_id, capture_id) DO NOTHING",
        params![
            capture_pk.to_string(),
            user_id.to_string(),
            device_id.to_string(),
            session_pk.to_string(),
            normalize_text(&capture.capture_id),
            normalize_text(&capture.flow_id),
            timestamp_to_micros(capture.captured_at),
            normalize_text(&capture.collector_version),
            payload,
            timestamp_to_micros(Utc::now()),
        ],
    )?;
    if inserted == 1 {
        for (event_pk, _, _) in events {
            transaction.execute(
                "INSERT INTO agent_diagnostic_capture_events (capture_pk, event_pk)
                 VALUES (?1, ?2)",
                params![capture_pk.to_string(), event_pk.to_string()],
            )?;
        }
    }
    Ok(inserted == 1)
}

pub(super) fn raw_events(
    connection: &Connection,
    query: &RawEventsQuery,
    user_id: Uuid,
    default_limit: i64,
) -> Result<RawEventsResponse, AppError> {
    let page_size = normalize_limit(query.limit, default_limit, 1_000);
    let page_size_usize = usize::try_from(page_size)
        .map_err(|error| AppError::internal(format!("invalid raw events page size: {error}")))?;
    let offset = query.offset.unwrap_or(0).max(0);
    let mut events = load_filtered_events(
        connection,
        &EventFilters {
            user_id,
            from: query.from,
            to: query.to,
            agent_name: query.agent_name.as_deref(),
            session_id: query.session_id.as_deref(),
            session_pk: query.session_pk,
            turn_index: query.turn_index,
            llm_provider: query.llm_provider.as_deref(),
            llm_model: query.llm_model.as_deref(),
            event_type: query.event_type.as_deref(),
            limit: page_size.saturating_add(1),
            offset,
        },
    )?;
    let has_next_page = events.len() > page_size_usize;
    if has_next_page {
        events.truncate(page_size_usize);
    }
    let devices = load_device_contexts(connection, &events)?;
    let mut attachments = load_attachments(connection, &events)?;
    let responses = events
        .into_iter()
        .map(|event| {
            let event_pk = event.id;
            let device_id = event.device_context_id;
            event_response(
                event,
                devices.get(&device_id),
                attachments.remove(&event_pk).unwrap_or_default(),
            )
        })
        .collect();
    Ok(RawEventsResponse {
        events: responses,
        next_page_token: has_next_page.then(|| offset.saturating_add(page_size).to_string()),
    })
}

struct EventFilters<'a> {
    user_id: Uuid,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
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

fn load_filtered_events(
    connection: &Connection,
    filters: &EventFilters<'_>,
) -> Result<Vec<EventRecord>, AppError> {
    let mut sql = String::from(EVENT_SELECT_SQL);
    sql.push_str(" WHERE user_id = ?");
    let mut values = vec![SqlValue::Text(filters.user_id.to_string())];
    if let Some(from) = filters.from {
        sql.push_str(" AND observed_at >= ?");
        values.push(SqlValue::Integer(timestamp_to_micros(from)));
    }
    if let Some(to) = filters.to {
        sql.push_str(" AND observed_at < ?");
        values.push(SqlValue::Integer(timestamp_to_micros(to)));
    }
    if let Some(agent_name) = filters.agent_name.and_then(non_empty) {
        let names = agent_name_filter_values(agent_name);
        sql.push_str(" AND agent_name IN (");
        sql.push_str(&placeholders(names.len()));
        sql.push(')');
        values.extend(names.into_iter().map(SqlValue::Text));
    }
    if let Some(session_id) = filters.session_id.and_then(non_empty) {
        sql.push_str(" AND session_id = ?");
        values.push(SqlValue::Text(session_id.trim().to_owned()));
    }
    if let Some(session_pk) = filters.session_pk {
        sql.push_str(" AND session_pk = ?");
        values.push(SqlValue::Text(session_pk.to_string()));
    }
    if let Some(turn_index) = filters.turn_index {
        sql.push_str(" AND turn_index = ?");
        values.push(SqlValue::Integer(i64::from(turn_index)));
    }
    if let Some(provider) = filters.llm_provider.and_then(non_empty) {
        sql.push_str(" AND llm_provider = ?");
        values.push(SqlValue::Text(normalize_slug(provider)));
    }
    if let Some(model) = filters.llm_model.and_then(non_empty) {
        sql.push_str(" AND llm_model = ?");
        values.push(SqlValue::Text(model.trim().to_owned()));
    }
    if let Some(event_type) = filters.event_type.and_then(non_empty) {
        sql.push_str(" AND event_type = ?");
        values.push(SqlValue::Text(validate_event_type(event_type)?));
    }
    sql.push_str(" ORDER BY observed_at DESC, id DESC LIMIT ? OFFSET ?");
    values.push(SqlValue::Integer(filters.limit));
    values.push(SqlValue::Integer(filters.offset));

    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map(params_from_iter(values.iter()), map_event)?;
    rows.collect::<Result<Vec<_>, _>>().map_err(AppError::from)
}

pub(super) fn session_timeline(
    connection: &Connection,
    user_id: Uuid,
    session_pk: Uuid,
) -> Result<SessionTimelineResponse, AppError> {
    let session = connection
        .query_row(
            "SELECT id, user_id, device_context_id, agent_name, agent_version,
                    session_id, started_at, ended_at, metadata
             FROM agent_sessions
             WHERE id = ?1 AND user_id = ?2",
            params![session_pk.to_string(), user_id.to_string()],
            map_session,
        )
        .optional()?
        .ok_or_else(|| AppError::not_found(format!("session not found: {session_pk}")))?;

    let mut turn_statement = connection.prepare(
        "SELECT id, turn_index, started_at, ended_at
         FROM agent_turns WHERE session_pk = ?1 ORDER BY turn_index ASC",
    )?;
    let turns = turn_statement
        .query_map([session.id.to_string()], map_turn)?
        .collect::<Result<Vec<_>, _>>()?;

    let mut event_statement = connection.prepare(&format!(
        "{EVENT_SELECT_SQL} WHERE session_pk = ?1 ORDER BY turn_index, observed_at, id"
    ))?;
    let mut events = event_statement
        .query_map([session.id.to_string()], map_event)?
        .collect::<Result<Vec<_>, _>>()?;
    UsageEventTimelineOrder::sort(&mut events);
    let devices = load_device_contexts(connection, &events)?;
    let mut attachments = load_attachments(connection, &events)?;
    let mut events_by_turn: HashMap<Uuid, Vec<UsageEventResponse>> = HashMap::new();
    for event in events {
        let event_pk = event.id;
        let device_id = event.device_context_id;
        events_by_turn
            .entry(event.turn_pk)
            .or_default()
            .push(event_response(
                event,
                devices.get(&device_id),
                attachments.remove(&event_pk).unwrap_or_default(),
            ));
    }
    let turn_timelines = turns
        .into_iter()
        .map(|turn| TurnTimeline {
            turn_pk: turn.id,
            turn_index: turn.turn_index,
            started_at: turn.started_at,
            ended_at: turn.ended_at,
            events: events_by_turn.remove(&turn.id).unwrap_or_default(),
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

pub(super) fn image_attachment(
    connection: &Connection,
    user_id: Uuid,
    attachment_id: Uuid,
) -> Result<StoredImageAttachment, AppError> {
    let row = connection
        .query_row(
            "SELECT id, event_pk, position, media_type, byte_size, sha256, content
             FROM llm_usage_event_attachments
             WHERE id = ?1 AND user_id = ?2",
            params![attachment_id.to_string(), user_id.to_string()],
            map_attachment,
        )
        .optional()?
        .ok_or_else(|| {
            AppError::not_found(format!("image attachment not found: {attachment_id}"))
        })?;
    let content = row.content.ok_or_else(|| {
        AppError::not_found(format!(
            "image attachment content not found: {attachment_id}"
        ))
    })?;
    if i64::try_from(content.len()).ok() != Some(row.byte_size) {
        return Err(AppError::internal(format!(
            "stored image attachment byte size mismatch: {attachment_id}"
        )));
    }
    Ok(StoredImageAttachment {
        media_type: ImageMediaType::from_stored(&row.media_type)?,
        sha256: hex::encode(row.sha256),
        content,
    })
}

pub(super) fn usage_summary(
    connection: &Connection,
    query: &SummaryQuery,
    user_id: Uuid,
    default_limit: i64,
) -> Result<SummaryResponse, AppError> {
    let group_by = parse_group_by(query.group_by.as_deref());
    let dimensions = SummaryDimensions::from_group_by(&group_by);
    let limit = normalize_limit(query.limit, default_limit, 100_000);
    let mut sql = summary_select_sql(&dimensions);
    let mut values = vec![SqlValue::Text(user_id.to_string())];
    if let Some(from) = query.from {
        sql.push_str(" AND e.observed_at >= ?");
        values.push(SqlValue::Integer(timestamp_to_micros(from)));
    }
    if let Some(to) = query.to {
        sql.push_str(" AND e.observed_at < ?");
        values.push(SqlValue::Integer(timestamp_to_micros(to)));
    }
    if let Some(user_filter) = query.user_filter.as_deref().and_then(non_empty) {
        if let Ok(user_filter_id) = Uuid::parse_str(user_filter.trim()) {
            sql.push_str(" AND e.user_id = ?");
            values.push(SqlValue::Text(user_filter_id.to_string()));
        } else {
            sql.push_str(
                " AND (lower(u.email) LIKE lower(?) ESCAPE '\\'
                        OR lower(COALESCE(u.name, '')) LIKE lower(?) ESCAPE '\\')",
            );
            let pattern = format!("%{}%", escape_like_pattern(user_filter.trim()));
            values.push(SqlValue::Text(pattern.clone()));
            values.push(SqlValue::Text(pattern));
        }
    }
    if let Some(agent_name) = query.agent_name.as_deref().and_then(non_empty) {
        let names = agent_name_filter_values(agent_name);
        sql.push_str(" AND e.agent_name IN (");
        sql.push_str(&placeholders(names.len()));
        sql.push(')');
        values.extend(names.into_iter().map(SqlValue::Text));
    }
    if let Some(session_id) = query.session_id.as_deref().and_then(non_empty) {
        sql.push_str(" AND e.session_id = ?");
        values.push(SqlValue::Text(session_id.trim().to_owned()));
    }
    if let Some(provider) = query.llm_provider.as_deref().and_then(non_empty) {
        sql.push_str(" AND e.llm_provider = ?");
        values.push(SqlValue::Text(normalize_slug(provider)));
    }
    if let Some(model) = query.llm_model.as_deref().and_then(non_empty) {
        sql.push_str(" AND e.llm_model = ?");
        values.push(SqlValue::Text(model.trim().to_owned()));
    }
    if let Some(event_type) = query.event_type.as_deref().and_then(non_empty) {
        sql.push_str(" AND e.event_type = ?");
        values.push(SqlValue::Text(validate_event_type(event_type)?));
    }
    sql.push_str(
        " GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
          ORDER BY total_tokens DESC, agent_name ASC, user_email ASC, day ASC
          LIMIT ?",
    );
    values.push(SqlValue::Integer(limit));
    let mut statement = connection.prepare(&sql)?;
    let rows = statement
        .query_map(params_from_iter(values.iter()), map_summary_row)?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(SummaryResponse {
        from: query.from,
        to: query.to,
        group_by,
        rows,
        next_page_token: None,
    })
}

struct SummaryDimensions([bool; 7]);

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
    const fn index(self) -> usize {
        match self {
            Self::Day => 0,
            Self::User => 1,
            Self::Device => 2,
            Self::Agent => 3,
            Self::Provider => 4,
            Self::Model => 5,
            Self::EventType => 6,
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

fn summary_select_sql(dimensions: &SummaryDimensions) -> String {
    let selected = |enabled: bool, expression: &str| {
        if enabled {
            expression.to_owned()
        } else {
            "NULL".to_owned()
        }
    };
    format!(
        "SELECT
             {} AS day,
             {} AS user_id,
             {} AS user_name,
             {} AS user_email,
             {} AS host_name,
             {} AS platform,
             {} AS os_version,
             {} AS agent_name,
             {} AS llm_provider,
             {} AS llm_model,
             {} AS event_type,
             COUNT(DISTINCT e.session_pk) AS sessions,
             COUNT(DISTINCT e.turn_pk) AS turns,
             SUM(CASE WHEN e.event_type = 'request' THEN 1 ELSE 0 END) AS requests,
             SUM(CASE WHEN e.event_type = 'response' THEN 1 ELSE 0 END) AS responses,
             COALESCE(SUM(e.input_tokens), 0) AS input_tokens,
             COALESCE(SUM(e.output_tokens), 0) AS output_tokens,
             COALESCE(SUM(e.cache_read_tokens), 0) AS cache_read_tokens,
             COALESCE(SUM(e.cache_write_tokens), 0) AS cache_write_tokens,
             COALESCE(SUM(e.reasoning_tokens), 0) AS reasoning_tokens,
             COALESCE(SUM(e.total_tokens), 0) AS total_tokens
         FROM llm_usage_events e
         LEFT JOIN app_users u ON u.id = e.user_id
         LEFT JOIN devices d ON d.id = e.device_context_id
         WHERE e.user_id = ?",
        selected(
            dimensions.enabled(SummaryDimension::Day),
            "strftime('%Y-%m-%d', e.observed_at / 1000000, 'unixepoch')"
        ),
        selected(dimensions.enabled(SummaryDimension::User), "e.user_id"),
        selected(
            dimensions.enabled(SummaryDimension::User),
            "NULLIF(trim(u.name), '')"
        ),
        selected(dimensions.enabled(SummaryDimension::User), "u.email"),
        selected(dimensions.enabled(SummaryDimension::Device), "d.host_name"),
        selected(dimensions.enabled(SummaryDimension::Device), "d.platform"),
        selected(dimensions.enabled(SummaryDimension::Device), "d.os_version"),
        selected(
            dimensions.enabled(SummaryDimension::Agent),
            "CASE WHEN e.agent_name IN ('claude', 'claude-code', 'claude-desktop')
                  THEN 'claude-code' ELSE e.agent_name END"
        ),
        selected(
            dimensions.enabled(SummaryDimension::Provider),
            "e.llm_provider"
        ),
        selected(dimensions.enabled(SummaryDimension::Model), "e.llm_model"),
        selected(
            dimensions.enabled(SummaryDimension::EventType),
            "e.event_type"
        ),
    )
}

fn load_device_contexts(
    connection: &Connection,
    events: &[EventRecord],
) -> Result<HashMap<Uuid, DeviceRecord>, AppError> {
    let ids = events
        .iter()
        .map(|event| event.device_context_id)
        .collect::<HashSet<_>>();
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    let mut sql = String::from("SELECT id, host_name, platform FROM devices WHERE id IN (");
    sql.push_str(&placeholders(ids.len()));
    sql.push(')');
    let values = ids
        .into_iter()
        .map(|id| SqlValue::Text(id.to_string()))
        .collect::<Vec<_>>();
    let mut statement = connection.prepare(&sql)?;
    let devices = statement
        .query_map(params_from_iter(values.iter()), |row| {
            Ok(DeviceRecord {
                id: uuid_at(row, 0)?,
                host_name: row.get(1)?,
                platform: row.get(2)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(devices
        .into_iter()
        .map(|device| (device.id, device))
        .collect())
}

fn load_attachments(
    connection: &Connection,
    events: &[EventRecord],
) -> Result<HashMap<Uuid, Vec<UsageEventAttachmentResponse>>, AppError> {
    if events.is_empty() {
        return Ok(HashMap::new());
    }
    let mut sql = String::from(
        "SELECT id, event_pk, position, media_type, byte_size, sha256, content
         FROM llm_usage_event_attachments WHERE event_pk IN (",
    );
    sql.push_str(&placeholders(events.len()));
    sql.push_str(") ORDER BY event_pk, position");
    let values = events
        .iter()
        .map(|event| SqlValue::Text(event.id.to_string()))
        .collect::<Vec<_>>();
    let mut statement = connection.prepare(&sql)?;
    let rows = statement
        .query_map(params_from_iter(values.iter()), map_attachment)?
        .collect::<Result<Vec<_>, _>>()?;
    let mut by_event = HashMap::new();
    for row in rows {
        by_event
            .entry(row.event_pk)
            .or_insert_with(Vec::new)
            .push(UsageEventAttachmentResponse {
                id: row.id,
                position: row.position,
                media_type: ImageMediaType::from_stored(&row.media_type)?,
                byte_size: row.byte_size,
                sha256: hex::encode(row.sha256),
                content_available: row.content.is_some(),
            });
    }
    Ok(by_event)
}

fn event_response(
    event: EventRecord,
    device: Option<&DeviceRecord>,
    attachments: Vec<UsageEventAttachmentResponse>,
) -> UsageEventResponse {
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

const EVENT_SELECT_SQL: &str = "SELECT
    id, user_id, device_context_id, session_pk, turn_pk, event_id,
    agent_name, agent_version, session_id, turn_index, llm_provider,
    llm_model, event_type, text, text_sha256, input_tokens,
    output_tokens, cache_read_tokens, cache_write_tokens, reasoning_tokens,
    total_tokens, observed_at, metadata
FROM llm_usage_events";

fn map_event(row: &Row<'_>) -> rusqlite::Result<EventRecord> {
    Ok(EventRecord {
        id: uuid_at(row, 0)?,
        user_id: uuid_at(row, 1)?,
        device_context_id: uuid_at(row, 2)?,
        session_pk: uuid_at(row, 3)?,
        turn_pk: uuid_at(row, 4)?,
        event_id: row.get(5)?,
        agent_name: row.get(6)?,
        agent_version: row.get(7)?,
        session_id: row.get(8)?,
        turn_index: row.get(9)?,
        llm_provider: row.get(10)?,
        llm_model: row.get(11)?,
        event_type: row.get(12)?,
        text: row.get(13)?,
        text_sha256: row.get(14)?,
        input_tokens: row.get(15)?,
        output_tokens: row.get(16)?,
        cache_read_tokens: row.get(17)?,
        cache_write_tokens: row.get(18)?,
        reasoning_tokens: row.get(19)?,
        total_tokens: row.get(20)?,
        observed_at: timestamp_at(row, 21)?,
        metadata: json_at(row, 22)?,
    })
}

fn map_session(row: &Row<'_>) -> rusqlite::Result<SessionRecord> {
    Ok(SessionRecord {
        id: uuid_at(row, 0)?,
        user_id: uuid_at(row, 1)?,
        device_context_id: uuid_at(row, 2)?,
        agent_name: row.get(3)?,
        agent_version: row.get(4)?,
        session_id: row.get(5)?,
        started_at: timestamp_at(row, 6)?,
        ended_at: optional_timestamp_at(row, 7)?,
        metadata: json_at(row, 8)?,
    })
}

fn map_turn(row: &Row<'_>) -> rusqlite::Result<TurnRecord> {
    Ok(TurnRecord {
        id: uuid_at(row, 0)?,
        turn_index: row.get(1)?,
        started_at: timestamp_at(row, 2)?,
        ended_at: optional_timestamp_at(row, 3)?,
    })
}

fn map_attachment(row: &Row<'_>) -> rusqlite::Result<AttachmentRecord> {
    Ok(AttachmentRecord {
        id: uuid_at(row, 0)?,
        event_pk: uuid_at(row, 1)?,
        position: row.get(2)?,
        media_type: row.get(3)?,
        byte_size: row.get(4)?,
        sha256: row.get(5)?,
        content: row.get(6)?,
    })
}

fn map_summary_row(row: &Row<'_>) -> rusqlite::Result<SummaryRow> {
    let user_id = row
        .get::<_, Option<String>>(1)?
        .map(|value| parse_uuid_value(&value, 1))
        .transpose()?;
    Ok(SummaryRow {
        day: row.get(0)?,
        user_id,
        user_name: row.get(2)?,
        user_email: row.get(3)?,
        host_name: row.get(4)?,
        platform: row.get(5)?,
        os_version: row.get(6)?,
        agent_name: row.get(7)?,
        llm_provider: row.get(8)?,
        llm_model: row.get(9)?,
        event_type: row.get(10)?,
        sessions: non_negative_count(row.get(11)?),
        turns: non_negative_count(row.get(12)?),
        requests: row.get(13)?,
        responses: row.get(14)?,
        input_tokens: row.get(15)?,
        output_tokens: row.get(16)?,
        cache_read_tokens: row.get(17)?,
        cache_write_tokens: row.get(18)?,
        reasoning_tokens: row.get(19)?,
        total_tokens: row.get(20)?,
    })
}

fn uuid_at(row: &Row<'_>, index: usize) -> rusqlite::Result<Uuid> {
    let value = row.get::<_, String>(index)?;
    parse_uuid_value(&value, index)
}

pub(super) fn parse_uuid_value(value: &str, index: usize) -> rusqlite::Result<Uuid> {
    Uuid::parse_str(value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(index, Type::Text, Box::new(error))
    })
}

fn parse_uuid(value: &str, field: &str) -> Result<Uuid, AppError> {
    Uuid::parse_str(value)
        .map_err(|error| AppError::internal(format!("stored {field} is not a UUID: {error}")))
}

fn json_at(row: &Row<'_>, index: usize) -> rusqlite::Result<serde_json::Value> {
    let value = row.get::<_, String>(index)?;
    serde_json::from_str(&value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(index, Type::Text, Box::new(error))
    })
}

fn timestamp_at(row: &Row<'_>, index: usize) -> rusqlite::Result<DateTime<Utc>> {
    timestamp_from_micros(row.get(index)?, index)
}

fn optional_timestamp_at(row: &Row<'_>, index: usize) -> rusqlite::Result<Option<DateTime<Utc>>> {
    row.get::<_, Option<i64>>(index)?
        .map(|value| timestamp_from_micros(value, index))
        .transpose()
}

pub(super) fn timestamp_from_micros(value: i64, index: usize) -> rusqlite::Result<DateTime<Utc>> {
    DateTime::from_timestamp_micros(value).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            Type::Integer,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("timestamp microseconds are out of range: {value}"),
            )),
        )
    })
}

pub(super) const fn timestamp_to_micros(value: DateTime<Utc>) -> i64 {
    value.timestamp_micros()
}

fn placeholders(count: usize) -> String {
    std::iter::repeat_n("?", count)
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    use serde_json::json;
    use uuid::Uuid;

    use crate::{search::SessionSearchQuery, usage::IngestEventsRequest};

    use super::{super::migrations, super::search, ingest_events};

    const OWNER_ID: Uuid = Uuid::from_u128(1);

    #[test]
    fn ingest_is_idempotent_and_searchable_in_the_same_transaction() {
        let mut database = Connection::open_in_memory().expect("SQLite should open");
        migrations::run(&mut database).expect("migration should succeed");
        let request = request_with_text("sqlite searchable response");

        let first = ingest_events(&mut database, &request, OWNER_ID, 100)
            .expect("first ingest should succeed");
        assert_eq!(first.accepted, 1);
        assert_eq!(first.duplicates, 0);
        let duplicate = ingest_events(&mut database, &request, OWNER_ID, 100)
            .expect("duplicate ingest should succeed");
        assert_eq!(duplicate.accepted, 0);
        assert_eq!(duplicate.duplicates, 1);

        let projection_count = database
            .query_row("SELECT count(*) FROM usage_events_fts", [], |row| {
                row.get::<_, i64>(0)
            })
            .expect("FTS projection should be queryable");
        assert_eq!(projection_count, 1);
        let results = search::session_search(
            &database,
            OWNER_ID,
            SessionSearchQuery {
                q: "searchable response".to_owned(),
                from: None,
                to: None,
                agent_name: Some("codex".to_owned()),
                llm_provider: None,
                llm_model: None,
                event_type: None,
                page: None,
                page_size: None,
            },
        )
        .expect("committed event should be searchable");
        assert_eq!(results.total_sessions, 1);
        assert_eq!(results.items.len(), 1);
        assert_eq!(results.items[0].match_count, 1);
        assert!(
            results.items[0].matches[0]
                .fragments
                .iter()
                .flat_map(|fragment| &fragment.segments)
                .any(|segment| segment.highlighted),
            "FTS result should contain a structured highlight"
        );
    }

    #[test]
    fn validation_failure_leaves_relational_and_fts_tables_empty() {
        let mut database = Connection::open_in_memory().expect("SQLite should open");
        migrations::run(&mut database).expect("migration should succeed");
        let mut request = request_with_text("invalid event");
        request.events[0].token_usage.input_tokens = Some(-1);

        ingest_events(&mut database, &request, OWNER_ID, 100)
            .expect_err("negative tokens should reject the batch");
        for table in ["llm_usage_events", "usage_events_fts"] {
            let count = database
                .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                    row.get::<_, i64>(0)
                })
                .expect("table should be queryable");
            assert_eq!(count, 0, "{table} should remain empty");
        }
    }

    fn request_with_text(text: &str) -> IngestEventsRequest {
        serde_json::from_value(json!({
            "events": [{
                "event_id": "sqlite-event",
                "observed_at": "2026-08-27T00:00:00Z",
                "device": {"host_name": "local", "platform": "macos"},
                "agent": {"name": "Codex"},
                "session_id": "sqlite-session",
                "turn_index": 1,
                "llm": {"provider": "OpenAI", "model": "gpt-5"},
                "event_type": "response",
                "text": text,
                "token_usage": {"output_tokens": 3},
                "metadata": {"response_id": "sqlite-response"}
            }]
        }))
        .expect("test ingest request should deserialize")
    }
}
