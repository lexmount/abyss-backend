//! Diesel ORM implementation of SQLite usage ingest and relational queries.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use diesel::{
    ExpressionMethods, QueryDsl, RunQueryDsl, SelectableHelper, SqliteConnection, dsl::max,
    result::OptionalExtension, sql_query, sql_types::Text,
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
            non_empty, normalize_limit, normalize_slug, normalize_text, normalized_optional,
            normalized_tokens, parse_group_by, sha256_bytes, turn_identity, validate_batch,
            validate_event_type,
        },
        session_metadata_from_event,
    },
};

use super::{
    models::{
        AttachmentRow, DeviceRecord, DeviceRow, EventRecord, EventRow, NewAttachment, NewDevice,
        NewDiagnosticCapture, NewDiagnosticCaptureEvent, NewEvent, NewSession, NewTurn, SessionRow,
        TurnRow, parse_uuid,
    },
    schema::{
        agent_diagnostic_capture_events, agent_diagnostic_captures, agent_sessions, agent_turns,
        app_users, devices, llm_usage_event_attachments, llm_usage_events,
    },
};

pub(super) fn ingest_events(
    connection: &mut SqliteConnection,
    request: &IngestEventsRequest,
    user_id: Uuid,
    max_batch_size: usize,
) -> Result<IngestEventsResponse, AppError> {
    let validated_attachments = validate_batch(request, max_batch_size)?;
    // Reserve the single SQLite writer before allocating turn indexes.
    let (accepted, duplicates, accepted_captures, duplicate_captures) = connection
        .immediate_transaction(|transaction| {
            let mut accepted = 0_usize;
            let mut duplicates = 0_usize;
            for (event, attachments) in request.events.iter().zip(validated_attachments) {
                if ingest_one_event(transaction, user_id, event, attachments)? {
                    accepted = accepted.saturating_add(1);
                } else {
                    duplicates = duplicates.saturating_add(1);
                }
            }

            let mut accepted_captures = 0_usize;
            let mut duplicate_captures = 0_usize;
            for capture in &request.diagnostic_captures {
                if ingest_one_diagnostic_capture(transaction, user_id, capture)? {
                    accepted_captures = accepted_captures.saturating_add(1);
                } else {
                    duplicate_captures = duplicate_captures.saturating_add(1);
                }
            }
            Ok::<_, AppError>((accepted, duplicates, accepted_captures, duplicate_captures))
        })?;

    Ok(IngestEventsResponse {
        accepted,
        duplicates,
        rejected: 0,
        errors: Vec::new(),
        accepted_diagnostic_captures: accepted_captures,
        duplicate_diagnostic_captures: duplicate_captures,
    })
}

fn ingest_one_event(
    connection: &mut SqliteConnection,
    user_id: Uuid,
    event: &IngestUsageEvent,
    attachments: Vec<ValidatedImageAttachment>,
) -> Result<bool, AppError> {
    let event_id = normalize_text(&event.event_id);
    let exists = llm_usage_events::table
        .filter(llm_usage_events::event_id.eq(&event_id))
        .select(llm_usage_events::id)
        .first::<String>(connection)
        .optional()?
        .is_some();
    if exists {
        return Ok(false);
    }

    let observed_at = timestamp_to_micros(event.observed_at);
    let now = timestamp_to_micros(Utc::now());
    let agent_name = canonical_agent_name(&event.agent.name);
    let agent_version = normalized_optional(event.agent.version.as_deref());
    let provider = normalize_slug(&event.llm.provider);
    let model = normalize_text(&event.llm.model);
    let device_id = upsert_device(connection, user_id, event, observed_at, now)?;
    let session_pk = upsert_session(
        connection,
        user_id,
        device_id,
        event,
        &agent_name,
        agent_version.as_deref(),
        observed_at,
        now,
    )?;
    let turn_index = resolve_turn_index(connection, session_pk, event)?;
    let turn_pk = upsert_turn(
        connection,
        user_id,
        session_pk,
        turn_index,
        observed_at,
        now,
    )?;
    let tokens = normalized_tokens(event)?;
    let text = normalized_optional(event.text.as_deref());
    let event_pk = Uuid::now_v7();
    let metadata = serialize_json(&event.metadata, "event metadata")?;

    let inserted = diesel::insert_into(llm_usage_events::table)
        .values(NewEvent {
            id: event_pk.to_string(),
            user_id: user_id.to_string(),
            device_context_id: device_id.to_string(),
            session_pk: session_pk.to_string(),
            turn_pk: turn_pk.to_string(),
            event_id,
            agent_name,
            agent_version,
            session_id: normalize_text(&event.session_id),
            turn_index,
            llm_provider: provider,
            llm_model: model,
            event_type: event.event_type.as_str().to_owned(),
            text: text.clone(),
            text_sha256: text.as_deref().map(sha256_bytes),
            input_tokens: tokens.input,
            output_tokens: tokens.output,
            cache_read_tokens: tokens.cache_read,
            cache_write_tokens: tokens.cache_write,
            reasoning_tokens: tokens.reasoning,
            total_tokens: tokens.total,
            observed_at,
            metadata,
            created_at: now,
        })
        .on_conflict(llm_usage_events::event_id)
        .do_nothing()
        .execute(connection)?;
    if inserted != 1 {
        return Ok(false);
    }

    let attachment_rows = attachments
        .into_iter()
        .map(|attachment| NewAttachment {
            id: Uuid::now_v7().to_string(),
            user_id: user_id.to_string(),
            event_pk: event_pk.to_string(),
            position: attachment.position,
            media_type: attachment.media_type.as_str().to_owned(),
            byte_size: attachment.byte_size,
            sha256: attachment.sha256,
            content: attachment.content,
            created_at: now,
        })
        .collect::<Vec<_>>();
    if !attachment_rows.is_empty() {
        diesel::insert_into(llm_usage_event_attachments::table)
            .values(&attachment_rows)
            .execute(connection)?;
    }

    insert_search_projection(connection, event_pk, user_id, session_pk, event)?;
    Ok(true)
}

fn upsert_device(
    connection: &mut SqliteConnection,
    user_id: Uuid,
    event: &IngestUsageEvent,
    observed_at: i64,
    now: i64,
) -> Result<Uuid, AppError> {
    let owner = user_id.to_string();
    let host_name = normalize_text(&event.device.host_name);
    let platform = normalize_slug(&event.device.platform);
    let os_version = normalized_optional(event.device.os_version.as_deref());
    let existing = devices::table
        .filter(devices::user_id.eq(&owner))
        .filter(devices::host_name.eq(&host_name))
        .filter(devices::platform.eq(&platform))
        .select(DeviceRow::as_select())
        .first::<DeviceRow>(connection)
        .optional()?;
    if let Some(existing) = existing {
        let id = parse_uuid(&existing.id, "device id")?;
        diesel::update(devices::table.find(existing.id))
            .set((
                devices::os_version.eq(os_version.or(existing.os_version)),
                devices::first_seen_at.eq(existing.first_seen_at.min(observed_at)),
                devices::last_seen_at.eq(existing.last_seen_at.max(observed_at)),
                devices::updated_at.eq(now),
            ))
            .execute(connection)?;
        return Ok(id);
    }

    let id = Uuid::now_v7();
    diesel::insert_into(devices::table)
        .values(NewDevice {
            id: id.to_string(),
            user_id: owner,
            host_name,
            platform,
            os_version,
            first_seen_at: observed_at,
            last_seen_at: observed_at,
            created_at: now,
            updated_at: now,
        })
        .execute(connection)?;
    Ok(id)
}

#[expect(
    clippy::too_many_arguments,
    reason = "The session upsert receives one explicit value for every evolving boundary."
)]
fn upsert_session(
    connection: &mut SqliteConnection,
    user_id: Uuid,
    device_id: Uuid,
    event: &IngestUsageEvent,
    agent_name: &str,
    agent_version: Option<&str>,
    observed_at: i64,
    now: i64,
) -> Result<Uuid, AppError> {
    let owner = user_id.to_string();
    let session_id = normalize_text(&event.session_id);
    let existing = agent_sessions::table
        .filter(agent_sessions::user_id.eq(&owner))
        .filter(agent_sessions::agent_name.eq(agent_name))
        .filter(agent_sessions::session_id.eq(&session_id))
        .select(SessionRow::as_select())
        .first::<SessionRow>(connection)
        .optional()?;
    let incoming_metadata = session_metadata_from_event(&event.metadata);
    if let Some(existing) = existing {
        let id = parse_uuid(&existing.id, "session id")?;
        let metadata = merge_json(&existing.metadata, &incoming_metadata, "session metadata")?;
        diesel::update(agent_sessions::table.find(existing.id))
            .set((
                agent_sessions::device_context_id.eq(device_id.to_string()),
                agent_sessions::agent_version
                    .eq(agent_version.map(str::to_owned).or(existing.agent_version)),
                agent_sessions::started_at.eq(existing.started_at.min(observed_at)),
                agent_sessions::ended_at.eq(Some(
                    existing.ended_at.unwrap_or(observed_at).max(observed_at),
                )),
                agent_sessions::metadata.eq(metadata),
                agent_sessions::updated_at.eq(now),
            ))
            .execute(connection)?;
        return Ok(id);
    }

    let id = Uuid::now_v7();
    diesel::insert_into(agent_sessions::table)
        .values(NewSession {
            id: id.to_string(),
            user_id: owner,
            device_context_id: device_id.to_string(),
            agent_name: agent_name.to_owned(),
            agent_version: agent_version.map(str::to_owned),
            session_id,
            started_at: observed_at,
            ended_at: Some(observed_at),
            metadata: serialize_json(&incoming_metadata, "session metadata")?,
            created_at: now,
            updated_at: now,
        })
        .execute(connection)?;
    Ok(id)
}

fn resolve_turn_index(
    connection: &mut SqliteConnection,
    session_pk: Uuid,
    event: &IngestUsageEvent,
) -> Result<i32, AppError> {
    let Some(identity) = turn_identity(event) else {
        return Ok(event.turn_index);
    };
    let existing = existing_turn_index_for_identity(connection, session_pk, &identity)?;
    let next = agent_turns::table
        .filter(agent_turns::session_pk.eq(session_pk.to_string()))
        .select(max(agent_turns::turn_index))
        .first::<Option<i32>>(connection)?
        .unwrap_or(0_i32)
        .saturating_add(1);
    Ok(choose_turn_index(event.turn_index, existing, next))
}

fn existing_turn_index_for_identity(
    connection: &mut SqliteConnection,
    session_pk: Uuid,
    identity: &TurnIdentity,
) -> Result<Option<i32>, AppError> {
    let rows = llm_usage_events::table
        .filter(llm_usage_events::session_pk.eq(session_pk.to_string()))
        .order_by((
            llm_usage_events::observed_at.asc(),
            llm_usage_events::id.asc(),
        ))
        .select((llm_usage_events::turn_index, llm_usage_events::metadata))
        .load::<(i32, String)>(connection)?;
    for (turn_index, metadata) in rows {
        let metadata = serde_json::from_str::<serde_json::Value>(&metadata).map_err(|error| {
            AppError::internal(format!("stored event metadata is invalid JSON: {error}"))
        })?;
        if metadata
            .get(identity.kind.metadata_key())
            .and_then(serde_json::Value::as_str)
            == Some(identity.value.as_str())
        {
            return Ok(Some(turn_index));
        }
    }
    Ok(None)
}

fn upsert_turn(
    connection: &mut SqliteConnection,
    user_id: Uuid,
    session_pk: Uuid,
    turn_index: i32,
    observed_at: i64,
    now: i64,
) -> Result<Uuid, AppError> {
    let session = session_pk.to_string();
    let existing = agent_turns::table
        .filter(agent_turns::session_pk.eq(&session))
        .filter(agent_turns::turn_index.eq(turn_index))
        .select(TurnRow::as_select())
        .first::<TurnRow>(connection)
        .optional()?;
    if let Some(existing) = existing {
        let id = parse_uuid(&existing.id, "turn id")?;
        diesel::update(agent_turns::table.find(existing.id))
            .set((
                agent_turns::started_at.eq(existing.started_at.min(observed_at)),
                agent_turns::ended_at.eq(Some(
                    existing.ended_at.unwrap_or(observed_at).max(observed_at),
                )),
                agent_turns::updated_at.eq(now),
            ))
            .execute(connection)?;
        return Ok(id);
    }

    let id = Uuid::now_v7();
    diesel::insert_into(agent_turns::table)
        .values(NewTurn {
            id: id.to_string(),
            user_id: user_id.to_string(),
            session_pk: session,
            turn_index,
            started_at: observed_at,
            ended_at: Some(observed_at),
            created_at: now,
            updated_at: now,
        })
        .execute(connection)?;
    Ok(id)
}

/// Writes the SQLite FTS5 projection. Virtual tables are intentionally outside
/// Diesel's relational schema and use a fully bound raw statement.
fn insert_search_projection(
    connection: &mut SqliteConnection,
    event_pk: Uuid,
    user_id: Uuid,
    session_pk: Uuid,
    event: &IngestUsageEvent,
) -> Result<(), AppError> {
    let projection =
        SearchProjection::from_source(normalized_optional(event.text.as_deref()), &event.metadata);
    sql_query(
        "INSERT INTO usage_events_fts (
             event_pk, user_id, session_pk, session_id, content, tool_names,
             tool_content, commands, file_paths
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind::<Text, _>(event_pk.to_string())
    .bind::<Text, _>(user_id.to_string())
    .bind::<Text, _>(session_pk.to_string())
    .bind::<Text, _>(normalize_text(&event.session_id))
    .bind::<diesel::sql_types::Nullable<Text>, _>(projection.content)
    .bind::<Text, _>(projection.tool_names.join("\n"))
    .bind::<Text, _>(projection.tool_content.join("\n"))
    .bind::<Text, _>(projection.commands.join("\n"))
    .bind::<Text, _>(projection.file_paths.join("\n"))
    .execute(connection)?;
    Ok(())
}

fn ingest_one_diagnostic_capture(
    connection: &mut SqliteConnection,
    user_id: Uuid,
    capture: &IngestDiagnosticCapture,
) -> Result<bool, AppError> {
    let event_ids = capture
        .event_ids
        .iter()
        .map(|event_id| normalize_text(event_id))
        .collect::<Vec<_>>();
    let events = llm_usage_events::table
        .filter(llm_usage_events::user_id.eq(user_id.to_string()))
        .filter(llm_usage_events::event_id.eq_any(&event_ids))
        .select((
            llm_usage_events::id,
            llm_usage_events::device_context_id,
            llm_usage_events::session_pk,
        ))
        .load::<(String, String, String)>(connection)?;
    if events.len() != capture.event_ids.len() {
        return Err(AppError::validation(
            "diagnostic capture events were not ingested for the authenticated user".to_owned(),
        ));
    }
    let Some((_, device_id, session_pk)) = events.first() else {
        return Err(AppError::validation(
            "diagnostic capture must reference at least one event".to_owned(),
        ));
    };
    if events
        .iter()
        .any(|(_, device, session)| device != device_id || session != session_pk)
    {
        return Err(AppError::validation(
            "diagnostic capture events must belong to one session and device".to_owned(),
        ));
    }

    let capture_pk = Uuid::now_v7().to_string();
    let inserted = diesel::insert_into(agent_diagnostic_captures::table)
        .values(NewDiagnosticCapture {
            id: capture_pk.clone(),
            user_id: user_id.to_string(),
            device_context_id: device_id.clone(),
            session_pk: session_pk.clone(),
            capture_id: normalize_text(&capture.capture_id),
            flow_id: normalize_text(&capture.flow_id),
            captured_at: timestamp_to_micros(capture.captured_at),
            collector_version: normalize_text(&capture.collector_version),
            payload: serialize_json(&capture.payload, "diagnostic payload")?,
            created_at: timestamp_to_micros(Utc::now()),
        })
        .on_conflict((
            agent_diagnostic_captures::user_id,
            agent_diagnostic_captures::capture_id,
        ))
        .do_nothing()
        .execute(connection)?;
    if inserted == 1 {
        let links = events
            .into_iter()
            .map(|(event_pk, _, _)| NewDiagnosticCaptureEvent {
                capture_pk: capture_pk.clone(),
                event_pk,
            })
            .collect::<Vec<_>>();
        diesel::insert_into(agent_diagnostic_capture_events::table)
            .values(&links)
            .execute(connection)?;
    }
    Ok(inserted == 1)
}

pub(super) fn raw_events(
    connection: &mut SqliteConnection,
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
    connection: &mut SqliteConnection,
    filters: &EventFilters<'_>,
) -> Result<Vec<EventRecord>, AppError> {
    let mut query = llm_usage_events::table
        .filter(llm_usage_events::user_id.eq(filters.user_id.to_string()))
        .into_boxed::<diesel::sqlite::Sqlite>();
    if let Some(from) = filters.from {
        query = query.filter(llm_usage_events::observed_at.ge(timestamp_to_micros(from)));
    }
    if let Some(to) = filters.to {
        query = query.filter(llm_usage_events::observed_at.lt(timestamp_to_micros(to)));
    }
    if let Some(agent_name) = filters.agent_name.and_then(non_empty) {
        query =
            query.filter(llm_usage_events::agent_name.eq_any(agent_name_filter_values(agent_name)));
    }
    if let Some(session_id) = filters.session_id.and_then(non_empty) {
        query = query.filter(llm_usage_events::session_id.eq(session_id.trim()));
    }
    if let Some(session_pk) = filters.session_pk {
        query = query.filter(llm_usage_events::session_pk.eq(session_pk.to_string()));
    }
    if let Some(turn_index) = filters.turn_index {
        query = query.filter(llm_usage_events::turn_index.eq(turn_index));
    }
    if let Some(provider) = filters.llm_provider.and_then(non_empty) {
        query = query.filter(llm_usage_events::llm_provider.eq(normalize_slug(provider)));
    }
    if let Some(model) = filters.llm_model.and_then(non_empty) {
        query = query.filter(llm_usage_events::llm_model.eq(model.trim()));
    }
    if let Some(event_type) = filters.event_type.and_then(non_empty) {
        query = query.filter(llm_usage_events::event_type.eq(validate_event_type(event_type)?));
    }
    query
        .order_by((
            llm_usage_events::observed_at.desc(),
            llm_usage_events::id.desc(),
        ))
        .limit(filters.limit)
        .offset(filters.offset)
        .select(EventRow::as_select())
        .load::<EventRow>(connection)?
        .into_iter()
        .map(EventRow::into_record)
        .collect()
}

pub(super) fn session_timeline(
    connection: &mut SqliteConnection,
    user_id: Uuid,
    session_pk: Uuid,
) -> Result<SessionTimelineResponse, AppError> {
    let session = agent_sessions::table
        .filter(agent_sessions::id.eq(session_pk.to_string()))
        .filter(agent_sessions::user_id.eq(user_id.to_string()))
        .select(SessionRow::as_select())
        .first::<SessionRow>(connection)
        .optional()?
        .ok_or_else(|| AppError::not_found(format!("session not found: {session_pk}")))?
        .into_record()?;
    let turns = agent_turns::table
        .filter(agent_turns::session_pk.eq(session.id.to_string()))
        .order_by(agent_turns::turn_index.asc())
        .select(TurnRow::as_select())
        .load::<TurnRow>(connection)?
        .into_iter()
        .map(TurnRow::into_record)
        .collect::<Result<Vec<_>, _>>()?;
    let mut events = llm_usage_events::table
        .filter(llm_usage_events::session_pk.eq(session.id.to_string()))
        .order_by((
            llm_usage_events::turn_index.asc(),
            llm_usage_events::observed_at.asc(),
            llm_usage_events::id.asc(),
        ))
        .select(EventRow::as_select())
        .load::<EventRow>(connection)?
        .into_iter()
        .map(EventRow::into_record)
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
    connection: &mut SqliteConnection,
    user_id: Uuid,
    attachment_id: Uuid,
) -> Result<StoredImageAttachment, AppError> {
    let row = llm_usage_event_attachments::table
        .filter(llm_usage_event_attachments::id.eq(attachment_id.to_string()))
        .filter(llm_usage_event_attachments::user_id.eq(user_id.to_string()))
        .select(AttachmentRow::as_select())
        .first::<AttachmentRow>(connection)
        .optional()?
        .ok_or_else(|| AppError::not_found(format!("image attachment not found: {attachment_id}")))?
        .into_record()?;
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
    connection: &mut SqliteConnection,
    query: &SummaryQuery,
    user_id: Uuid,
    scan_limit: i64,
) -> Result<SummaryResponse, AppError> {
    let group_by = parse_group_by(query.group_by.as_deref());
    let dimensions = SummaryDimensions::from_group_by(&group_by);
    let owner = load_owner(connection, user_id)?;
    if !owner_matches_filter(user_id, &owner, query.user_filter.as_deref()) {
        return Ok(empty_summary(query, group_by));
    }
    let events = load_filtered_events(
        connection,
        &EventFilters {
            user_id,
            from: query.from,
            to: query.to,
            agent_name: query.agent_name.as_deref(),
            session_id: query.session_id.as_deref(),
            session_pk: None,
            turn_index: None,
            llm_provider: query.llm_provider.as_deref(),
            llm_model: query.llm_model.as_deref(),
            event_type: query.event_type.as_deref(),
            limit: scan_limit,
            offset: 0,
        },
    )?;
    let devices = load_device_contexts(connection, &events)?;
    let mut buckets: HashMap<SummaryKey, SummaryBucket> = HashMap::new();
    for event in events {
        let device = devices.get(&event.device_context_id);
        let key = SummaryKey::new(&event, device, user_id, &owner, &dimensions);
        buckets.entry(key).or_default().add(&event);
    }
    let mut rows = buckets
        .into_iter()
        .map(|(key, bucket)| bucket.into_row(key))
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        right
            .total_tokens
            .cmp(&left.total_tokens)
            .then_with(|| left.agent_name.cmp(&right.agent_name))
            .then_with(|| left.user_email.cmp(&right.user_email))
            .then_with(|| left.day.cmp(&right.day))
    });
    let row_limit = usize::try_from(normalize_limit(query.limit, scan_limit, 100_000))
        .map_err(|error| AppError::internal(format!("invalid summary row limit: {error}")))?;
    rows.truncate(row_limit);
    Ok(SummaryResponse {
        from: query.from,
        to: query.to,
        group_by,
        rows,
        next_page_token: None,
    })
}

struct OwnerRecord {
    email: String,
    name: Option<String>,
}

fn load_owner(connection: &mut SqliteConnection, user_id: Uuid) -> Result<OwnerRecord, AppError> {
    let (email, name) = app_users::table
        .find(user_id.to_string())
        .select((app_users::email, app_users::name))
        .first::<(String, Option<String>)>(connection)?;
    Ok(OwnerRecord { email, name })
}

fn owner_matches_filter(user_id: Uuid, owner: &OwnerRecord, user_filter: Option<&str>) -> bool {
    let Some(filter) = user_filter.and_then(non_empty) else {
        return true;
    };
    if let Ok(filter_id) = Uuid::parse_str(filter.trim()) {
        return filter_id == user_id;
    }
    let filter = filter.trim().to_lowercase();
    owner.email.to_lowercase().contains(&filter)
        || owner
            .name
            .as_deref()
            .unwrap_or_default()
            .to_lowercase()
            .contains(&filter)
}

const fn empty_summary(query: &SummaryQuery, group_by: Vec<String>) -> SummaryResponse {
    SummaryResponse {
        from: query.from,
        to: query.to,
        group_by,
        rows: Vec::new(),
        next_page_token: None,
    }
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

#[derive(Hash, Eq, PartialEq)]
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

impl SummaryKey {
    fn new(
        event: &EventRecord,
        device: Option<&DeviceRecord>,
        user_id: Uuid,
        owner: &OwnerRecord,
        dimensions: &SummaryDimensions,
    ) -> Self {
        let include_user = dimensions.enabled(SummaryDimension::User);
        let include_device = dimensions.enabled(SummaryDimension::Device);
        Self {
            day: dimensions
                .enabled(SummaryDimension::Day)
                .then(|| event.observed_at.format("%Y-%m-%d").to_string()),
            user_id: include_user.then_some(user_id),
            user_name: include_user.then(|| owner.name.clone()).flatten(),
            user_email: include_user.then(|| owner.email.clone()),
            host_name: include_device
                .then(|| device.map(|value| value.host_name.clone()))
                .flatten(),
            platform: include_device
                .then(|| device.map(|value| value.platform.clone()))
                .flatten(),
            os_version: include_device
                .then(|| device.and_then(|value| value.os_version.clone()))
                .flatten(),
            agent_name: dimensions
                .enabled(SummaryDimension::Agent)
                .then(|| canonical_agent_name(&event.agent_name)),
            llm_provider: dimensions
                .enabled(SummaryDimension::Provider)
                .then(|| event.llm_provider.clone()),
            llm_model: dimensions
                .enabled(SummaryDimension::Model)
                .then(|| event.llm_model.clone()),
            event_type: dimensions
                .enabled(SummaryDimension::EventType)
                .then(|| event.event_type.clone()),
        }
    }
}

#[derive(Default)]
struct SummaryBucket {
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

impl SummaryBucket {
    fn add(&mut self, event: &EventRecord) {
        self.sessions.insert(event.session_pk);
        self.turns.insert(event.turn_pk);
        self.requests = self
            .requests
            .saturating_add(i64::from(event.event_type == "request"));
        self.responses = self
            .responses
            .saturating_add(i64::from(event.event_type == "response"));
        self.input_tokens = self.input_tokens.saturating_add(event.input_tokens);
        self.output_tokens = self.output_tokens.saturating_add(event.output_tokens);
        self.cache_read_tokens = self
            .cache_read_tokens
            .saturating_add(event.cache_read_tokens);
        self.cache_write_tokens = self
            .cache_write_tokens
            .saturating_add(event.cache_write_tokens);
        self.reasoning_tokens = self.reasoning_tokens.saturating_add(event.reasoning_tokens);
        self.total_tokens = self.total_tokens.saturating_add(event.total_tokens);
    }

    fn into_row(self, key: SummaryKey) -> SummaryRow {
        SummaryRow {
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
            sessions: self.sessions.len(),
            turns: self.turns.len(),
            requests: self.requests,
            responses: self.responses,
            input_tokens: self.input_tokens,
            output_tokens: self.output_tokens,
            cache_read_tokens: self.cache_read_tokens,
            cache_write_tokens: self.cache_write_tokens,
            reasoning_tokens: self.reasoning_tokens,
            total_tokens: self.total_tokens,
        }
    }
}

fn load_device_contexts(
    connection: &mut SqliteConnection,
    events: &[EventRecord],
) -> Result<HashMap<Uuid, DeviceRecord>, AppError> {
    let ids = events
        .iter()
        .map(|event| event.device_context_id.to_string())
        .collect::<HashSet<_>>();
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    let devices = devices::table
        .filter(devices::id.eq_any(ids))
        .select(DeviceRow::as_select())
        .load::<DeviceRow>(connection)?
        .into_iter()
        .map(DeviceRow::into_record)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(devices
        .into_iter()
        .map(|device| (device.id, device))
        .collect())
}

fn load_attachments(
    connection: &mut SqliteConnection,
    events: &[EventRecord],
) -> Result<HashMap<Uuid, Vec<UsageEventAttachmentResponse>>, AppError> {
    if events.is_empty() {
        return Ok(HashMap::new());
    }
    let event_ids = events
        .iter()
        .map(|event| event.id.to_string())
        .collect::<Vec<_>>();
    let rows = llm_usage_event_attachments::table
        .filter(llm_usage_event_attachments::event_pk.eq_any(event_ids))
        .order_by((
            llm_usage_event_attachments::event_pk.asc(),
            llm_usage_event_attachments::position.asc(),
        ))
        .select(AttachmentRow::as_select())
        .load::<AttachmentRow>(connection)?
        .into_iter()
        .map(AttachmentRow::into_record)
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

fn merge_json(stored: &str, incoming: &serde_json::Value, field: &str) -> Result<String, AppError> {
    let mut stored = serde_json::from_str::<serde_json::Value>(stored)
        .map_err(|error| AppError::internal(format!("stored {field} is invalid JSON: {error}")))?;
    if let (Some(stored), Some(incoming)) = (stored.as_object_mut(), incoming.as_object()) {
        stored.extend(incoming.clone());
    }
    serialize_json(&stored, field)
}

fn serialize_json(value: &serde_json::Value, field: &str) -> Result<String, AppError> {
    serde_json::to_string(value)
        .map_err(|error| AppError::internal(format!("serialize {field}: {error}")))
}

pub(super) const fn timestamp_to_micros(value: DateTime<Utc>) -> i64 {
    value.timestamp_micros()
}

#[cfg(test)]
mod tests {
    use diesel::{Connection, QueryDsl, RunQueryDsl, SqliteConnection, dsl::count_star, sql_query};
    use serde_json::json;
    use uuid::Uuid;

    use crate::{search::SessionSearchQuery, usage::IngestEventsRequest};

    use super::{
        super::{migrations, schema::llm_usage_events, search},
        ingest_events,
    };

    const OWNER_ID: Uuid = Uuid::from_u128(1);

    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        count: i64,
    }

    fn database() -> SqliteConnection {
        let mut database = SqliteConnection::establish(":memory:").expect("SQLite should open");
        migrations::run(&mut database).expect("migration should succeed");
        database
    }

    #[test]
    fn ingest_is_idempotent_and_searchable_in_the_same_transaction() {
        let mut database = database();
        let request = request_with_text("sqlite searchable response");

        let first = ingest_events(&mut database, &request, OWNER_ID, 100)
            .expect("first ingest should succeed");
        assert_eq!(first.accepted, 1);
        assert_eq!(first.duplicates, 0);
        let duplicate = ingest_events(&mut database, &request, OWNER_ID, 100)
            .expect("duplicate ingest should succeed");
        assert_eq!(duplicate.accepted, 0);
        assert_eq!(duplicate.duplicates, 1);

        let projection_count = sql_query("SELECT count(*) AS count FROM usage_events_fts")
            .get_result::<Count>(&mut database)
            .expect("FTS projection should be queryable")
            .count;
        assert_eq!(projection_count, 1);
        let results = search::session_search(
            &mut database,
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
        let mut database = database();
        let mut request = request_with_text("invalid event");
        request.events[0].token_usage.input_tokens = Some(-1);

        ingest_events(&mut database, &request, OWNER_ID, 100)
            .expect_err("negative tokens should reject the batch");
        let relational_count = llm_usage_events::table
            .select(count_star())
            .first::<i64>(&mut database)
            .expect("relational table should be queryable");
        assert_eq!(relational_count, 0);

        let fts_count = sql_query("SELECT count(*) AS count FROM usage_events_fts")
            .get_result::<Count>(&mut database)
            .expect("FTS table should be queryable")
            .count;
        assert_eq!(fts_count, 0);
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
