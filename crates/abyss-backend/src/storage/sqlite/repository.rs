//! Diesel ORM implementation of SQLite usage ingest and relational queries.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use diesel::{
    ExpressionMethods, QueryDsl, QueryableByName, RunQueryDsl, SelectableHelper, SqliteConnection,
    dsl::{count_star, max, sql},
    result::OptionalExtension,
    sql_query,
    sql_types::{BigInt, Bool, Nullable, Text},
};
use uuid::Uuid;

use crate::{
    error::AppError,
    search::projection::SearchProjection,
    usage::{
        IngestEventsRequest, IngestEventsResponse, IngestUsageEvent, OUTLINE_PREVIEW_CHARS,
        RawEventsQuery, RawEventsResponse, SessionInfo, SessionOutlineResponse,
        SessionTimelineResponse, SummaryQuery, SummaryResponse, SummaryRow, TurnTimeline,
        UsageEventOutline, UsageEventResponse,
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
        NewDiagnosticCapture, NewDiagnosticCaptureEvent, NewEvent, NewSession, NewTurn,
        SessionRecord, SessionRow, TurnRecord, TurnRow, parse_uuid, timestamp_from_micros,
    },
    schema::{
        agent_diagnostic_capture_events, agent_diagnostic_captures, agent_sessions, agent_turns,
        app_users, devices, llm_usage_event_attachments, llm_usage_events,
    },
    session_titles,
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
            agent_name: agent_name.clone(),
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

    insert_attachments(connection, user_id, event_pk, attachments, now)?;
    insert_search_projection(connection, event_pk, user_id, session_pk, event)?;
    session_titles::capture(
        connection,
        &session_titles::CapturedEvent {
            user_id,
            session_pk,
            event_pk,
            turn_index,
            agent_name: &agent_name,
            event_type: event.event_type.as_str(),
            text: text.as_deref(),
            metadata: &event.metadata,
            observed_at,
        },
    )?;
    Ok(true)
}

fn insert_attachments(
    connection: &mut SqliteConnection,
    user_id: Uuid,
    event_pk: Uuid,
    attachments: Vec<ValidatedImageAttachment>,
    now: i64,
) -> Result<(), AppError> {
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

    Ok(())
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
    let (session, turns) = load_session_turns(connection, user_id, session_pk)?;
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
    let mut timeline = assemble_timeline(session, turns, events_by_turn);
    session_titles::fill(connection, &mut timeline.session)?;
    Ok(timeline)
}

type OutlineRow = (
    String,
    String,
    String,
    i32,
    String,
    String,
    String,
    i64,
    i64,
    i64,
    i64,
    i64,
    i64,
    bool,
    i64,
    i64,
    Option<String>,
    Option<String>,
    Option<String>,
    bool,
);

pub(super) fn session_outline(
    connection: &mut SqliteConnection,
    user_id: Uuid,
    session_pk: Uuid,
) -> Result<SessionOutlineResponse, AppError> {
    let (session, turns) = load_session_turns(connection, user_id, session_pk)?;
    // Only fixed-size columns and SQL-computed summaries are selected, so the
    // outline stays small no matter how large event text or metadata grows.
    let rows = llm_usage_events::table
        .filter(llm_usage_events::session_pk.eq(session.id.to_string()))
        .select((
            llm_usage_events::id,
            llm_usage_events::event_id,
            llm_usage_events::turn_pk,
            llm_usage_events::turn_index,
            llm_usage_events::llm_provider,
            llm_usage_events::llm_model,
            llm_usage_events::event_type,
            llm_usage_events::input_tokens,
            llm_usage_events::output_tokens,
            llm_usage_events::cache_read_tokens,
            llm_usage_events::cache_write_tokens,
            llm_usage_events::reasoning_tokens,
            llm_usage_events::total_tokens,
            sql::<Bool>("json_type(metadata, '$.token_usage_estimated') IS 'true'"),
            llm_usage_events::observed_at,
            sql::<BigInt>("coalesce(length(text), 0)"),
            sql::<Nullable<Text>>(&format!(
                "CASE WHEN event_type = 'request' THEN substr(text, -{OUTLINE_PREVIEW_CHARS}) \
                 ELSE substr(text, 1, {OUTLINE_PREVIEW_CHARS}) END"
            )),
            sql::<Nullable<Text>>(
                "CASE WHEN json_type(metadata, '$.response_id') = 'text' \
                 THEN json_extract(metadata, '$.response_id') END",
            ),
            sql::<Nullable<Text>>(
                "CASE WHEN json_type(metadata, '$.previous_response_id') = 'text' \
                 THEN json_extract(metadata, '$.previous_response_id') END",
            ),
            // SQLite text functions stop at the first NUL character, so the
            // length and preview above are wrong for text containing NUL.
            sql::<Bool>("coalesce(instr(CAST(text AS BLOB), x'00') > 0, 0)"),
        ))
        .load::<OutlineRow>(connection)?;
    let nul_text_ids = rows
        .iter()
        .filter(|row| row.19)
        .map(|row| row.0.clone())
        .collect::<Vec<_>>();
    let nul_texts = if nul_text_ids.is_empty() {
        HashMap::new()
    } else {
        llm_usage_events::table
            .filter(llm_usage_events::id.eq_any(nul_text_ids))
            .select((llm_usage_events::id, llm_usage_events::text))
            .load::<(String, Option<String>)>(connection)?
            .into_iter()
            .filter_map(|(id, text)| text.map(|text| (id, text)))
            .collect::<HashMap<_, _>>()
    };
    let event_ids = rows.iter().map(|row| row.0.clone()).collect::<Vec<_>>();
    let attachment_counts = llm_usage_event_attachments::table
        .filter(llm_usage_event_attachments::event_pk.eq_any(event_ids))
        .group_by(llm_usage_event_attachments::event_pk)
        .select((llm_usage_event_attachments::event_pk, count_star()))
        .load::<(String, i64)>(connection)?
        .into_iter()
        .collect::<HashMap<_, _>>();
    let mut events = rows
        .into_iter()
        .map(|mut row| {
            let attachment_count = attachment_counts.get(&row.0).copied().unwrap_or(0);
            if let Some(text) = nul_texts.get(&row.0) {
                (row.15, row.16) = text_outline(text, &row.6)?;
            }
            Ok(UsageEventOutline {
                id: parse_uuid(&row.0, "event id")?,
                event_id: row.1,
                turn_pk: parse_uuid(&row.2, "event turn id")?,
                turn_index: row.3,
                llm_provider: row.4,
                llm_model: row.5,
                event_type: row.6,
                input_tokens: row.7,
                output_tokens: row.8,
                cache_read_tokens: row.9,
                cache_write_tokens: row.10,
                reasoning_tokens: row.11,
                total_tokens: row.12,
                token_usage_estimated: row.13,
                observed_at: timestamp_from_micros(row.14)?,
                text_chars: row.15,
                preview: row.16,
                attachment_count,
                ordering_metadata: UsageEventOutline::ordering_metadata(row.17, row.18),
            })
        })
        .collect::<Result<Vec<_>, AppError>>()?;
    UsageEventTimelineOrder::sort(&mut events);
    let mut events_by_turn: HashMap<Uuid, Vec<UsageEventOutline>> = HashMap::new();
    for event in events {
        events_by_turn.entry(event.turn_pk).or_default().push(event);
    }
    let mut timeline = assemble_timeline(session, turns, events_by_turn);
    session_titles::fill(connection, &mut timeline.session)?;
    Ok(timeline)
}

/// Computes the outline length and preview in Rust for text SQLite cannot measure.
fn text_outline(text: &str, event_type: &str) -> Result<(i64, Option<String>), AppError> {
    let chars = text.chars().count();
    let preview_chars = usize::try_from(OUTLINE_PREVIEW_CHARS)
        .map_err(|error| AppError::internal(format!("invalid outline preview size: {error}")))?;
    let preview = if event_type == "request" {
        text.chars()
            .skip(chars.saturating_sub(preview_chars))
            .collect()
    } else {
        text.chars().take(preview_chars).collect()
    };
    let chars = i64::try_from(chars)
        .map_err(|error| AppError::internal(format!("invalid event text length: {error}")))?;
    Ok((chars, Some(preview)))
}

fn load_session_turns(
    connection: &mut SqliteConnection,
    user_id: Uuid,
    session_pk: Uuid,
) -> Result<(SessionRecord, Vec<TurnRecord>), AppError> {
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
    Ok((session, turns))
}

fn assemble_timeline<E>(
    session: SessionRecord,
    turns: Vec<TurnRecord>,
    mut events_by_turn: HashMap<Uuid, Vec<E>>,
) -> SessionTimelineResponse<E> {
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
    SessionTimelineResponse {
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
            title: None,
            purpose: None,
            parent_session_pk: None,
        },
        turns: turn_timelines,
    }
}

pub(super) fn usage_event(
    connection: &mut SqliteConnection,
    user_id: Uuid,
    event_pk: Uuid,
) -> Result<UsageEventResponse, AppError> {
    let event = llm_usage_events::table
        .filter(llm_usage_events::id.eq(event_pk.to_string()))
        .filter(llm_usage_events::user_id.eq(user_id.to_string()))
        .select(EventRow::as_select())
        .first::<EventRow>(connection)
        .optional()?
        .ok_or_else(|| AppError::not_found(format!("usage event not found: {event_pk}")))?
        .into_record()?;
    let events = [event];
    let devices = load_device_contexts(connection, &events)?;
    let mut attachments = load_attachments(connection, &events)?;
    let [event] = events;
    let event_pk = event.id;
    let device_id = event.device_context_id;
    Ok(event_response(
        event,
        devices.get(&device_id),
        attachments.remove(&event_pk).unwrap_or_default(),
    ))
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
    default_limit: i64,
) -> Result<SummaryResponse, AppError> {
    let group_by = parse_group_by(query.group_by.as_deref());
    let dimensions = SummaryDimensions::from_group_by(&group_by);
    let owner = load_owner(connection, user_id)?;
    if !owner_matches_filter(user_id, &owner, query.user_filter.as_deref()) {
        return Ok(empty_summary(query, group_by));
    }
    let agent_names = query
        .agent_name
        .as_deref()
        .and_then(non_empty)
        .map(agent_name_filter_values)
        .unwrap_or_default();
    let agent_names_json = serde_json::to_string(&agent_names)
        .map_err(|error| AppError::internal(format!("serialize agent filter: {error}")))?;
    let session_id = query
        .session_id
        .as_deref()
        .and_then(non_empty)
        .map(str::trim);
    let provider = query
        .llm_provider
        .as_deref()
        .and_then(non_empty)
        .map(normalize_slug);
    let model = query
        .llm_model
        .as_deref()
        .and_then(non_empty)
        .map(str::trim);
    let event_type = query
        .event_type
        .as_deref()
        .and_then(non_empty)
        .map(validate_event_type)
        .transpose()?;
    let limit = normalize_limit(query.limit, default_limit, 100_000);

    // Bind order mirrors the numbered placeholders in the query.
    let rows = sql_query(SUMMARY_AGGREGATE_SQL)
        .bind::<Bool, _>(dimensions.enabled(SummaryDimension::Day))
        .bind::<Bool, _>(dimensions.enabled(SummaryDimension::Device))
        .bind::<Bool, _>(dimensions.enabled(SummaryDimension::Agent))
        .bind::<Bool, _>(dimensions.enabled(SummaryDimension::Provider))
        .bind::<Bool, _>(dimensions.enabled(SummaryDimension::Model))
        .bind::<Bool, _>(dimensions.enabled(SummaryDimension::EventType))
        .bind::<Text, _>(user_id.to_string())
        .bind::<BigInt, _>(query.from.map_or(i64::MIN, timestamp_to_micros))
        .bind::<BigInt, _>(query.to.map_or(i64::MAX, timestamp_to_micros))
        .bind::<Bool, _>(!agent_names.is_empty())
        .bind::<Text, _>(&agent_names_json)
        .bind::<Bool, _>(session_id.is_some())
        .bind::<Text, _>(session_id.unwrap_or_default())
        .bind::<Bool, _>(provider.is_some())
        .bind::<Text, _>(provider.as_deref().unwrap_or_default())
        .bind::<Bool, _>(model.is_some())
        .bind::<Text, _>(model.unwrap_or_default())
        .bind::<Bool, _>(event_type.is_some())
        .bind::<Text, _>(event_type.unwrap_or_default())
        .bind::<BigInt, _>(limit)
        .load::<SummaryAggregateRow>(connection)?;
    let include_user = dimensions.enabled(SummaryDimension::User);
    let rows = rows
        .into_iter()
        .map(|row| row.into_summary_row(include_user.then_some((user_id, &owner))))
        .collect::<Result<Vec<_>, AppError>>()?;
    Ok(SummaryResponse {
        from: query.from,
        to: query.to,
        group_by,
        rows,
        next_page_token: None,
    })
}

/// Usage totals grouped in SQL, reading only the columns a summary reports.
///
/// Event text and metadata hold whole request bodies, so loading rows to sum
/// them in memory read gigabytes for a week of usage. The owner is the single
/// standalone user, so the user dimension is filled in afterwards.
///
/// Bind order: `?1`..`?6` day, device, Agent, provider, model, and event-type
/// dimensions; `?7` owner; `?8`/`?9` observation bounds in microseconds, kept
/// unconditional so the owner-time index bounds the scan; `?10`/`?11` Agent
/// filter flag and JSON name list; `?12`..`?19` session, provider, model, and
/// event-type filter flags and values; `?20` row limit.
const SUMMARY_AGGREGATE_SQL: &str = "\
    SELECT \
        CASE WHEN ?1 THEN strftime('%Y-%m-%d', e.observed_at / 1000000, 'unixepoch') END AS day, \
        CASE WHEN ?2 THEN d.host_name END AS host_name, \
        CASE WHEN ?2 THEN d.platform END AS platform, \
        CASE WHEN ?2 THEN d.os_version END AS os_version, \
        CASE \
            WHEN ?3 THEN CASE \
                WHEN e.agent_name IN ('claude', 'claude-desktop') THEN 'claude-code' \
                ELSE e.agent_name \
            END \
        END AS agent_name, \
        CASE WHEN ?4 THEN e.llm_provider END AS llm_provider, \
        CASE WHEN ?5 THEN e.llm_model END AS llm_model, \
        CASE WHEN ?6 THEN e.event_type END AS event_type, \
        COUNT(DISTINCT e.session_pk) AS sessions, \
        COUNT(DISTINCT e.turn_pk) AS turns, \
        COUNT(*) FILTER (WHERE e.event_type = 'request') AS requests, \
        COUNT(*) FILTER (WHERE e.event_type = 'response') AS responses, \
        COALESCE(SUM(e.input_tokens), 0) AS input_tokens, \
        COALESCE(SUM(e.output_tokens), 0) AS output_tokens, \
        COALESCE(SUM(e.cache_read_tokens), 0) AS cache_read_tokens, \
        COALESCE(SUM(e.cache_write_tokens), 0) AS cache_write_tokens, \
        COALESCE(SUM(e.reasoning_tokens), 0) AS reasoning_tokens, \
        COALESCE(SUM(e.total_tokens), 0) AS total_tokens \
    FROM llm_usage_events e \
    LEFT JOIN devices d ON ?2 AND d.id = e.device_context_id \
    WHERE e.user_id = ?7 \
        AND e.observed_at >= ?8 \
        AND e.observed_at < ?9 \
        AND (?10 = 0 OR e.agent_name IN (SELECT value FROM json_each(?11))) \
        AND (?12 = 0 OR e.session_id = ?13) \
        AND (?14 = 0 OR e.llm_provider = ?15) \
        AND (?16 = 0 OR e.llm_model = ?17) \
        AND (?18 = 0 OR e.event_type = ?19) \
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8 \
    ORDER BY total_tokens DESC, agent_name ASC, day ASC \
    LIMIT ?20";

#[derive(QueryableByName)]
struct SummaryAggregateRow {
    #[diesel(sql_type = Nullable<Text>)]
    day: Option<String>,
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

impl SummaryAggregateRow {
    fn into_summary_row(self, user: Option<(Uuid, &OwnerRecord)>) -> Result<SummaryRow, AppError> {
        let count = |value: i64, name: &str| {
            usize::try_from(value)
                .map_err(|error| AppError::internal(format!("invalid summary {name}: {error}")))
        };
        Ok(SummaryRow {
            day: self.day,
            user_id: user.map(|(user_id, _)| user_id),
            user_name: user.and_then(|(_, owner)| owner.name.clone()),
            user_email: user.map(|(_, owner)| owner.email.clone()),
            host_name: self.host_name,
            platform: self.platform,
            os_version: self.os_version,
            agent_name: self.agent_name,
            llm_provider: self.llm_provider,
            llm_model: self.llm_model,
            event_type: self.event_type,
            sessions: count(self.sessions, "sessions")?,
            turns: count(self.turns, "turns")?,
            requests: self.requests,
            responses: self.responses,
            input_tokens: self.input_tokens,
            output_tokens: self.output_tokens,
            cache_read_tokens: self.cache_read_tokens,
            cache_write_tokens: self.cache_write_tokens,
            reasoning_tokens: self.reasoning_tokens,
            total_tokens: self.total_tokens,
        })
    }
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

    use crate::{
        search::SessionSearchQuery,
        usage::{IngestEventsRequest, SummaryQuery},
    };

    use super::{
        super::{migrations, schema::llm_usage_events, search},
        ingest_events, session_outline, usage_summary,
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

    #[test]
    fn outline_measures_text_containing_nul_characters() {
        let mut database = database();
        let text = format!("ab\0{}", "c".repeat(300));
        ingest_events(&mut database, &request_with_text(&text), OWNER_ID, 100)
            .expect("ingest should succeed");
        let session_pk = llm_usage_events::table
            .select(llm_usage_events::session_pk)
            .first::<String>(&mut database)
            .expect("event should exist");
        let session_pk = Uuid::parse_str(&session_pk).expect("session pk should be a UUID");

        let outline =
            session_outline(&mut database, OWNER_ID, session_pk).expect("outline should load");

        let event = &outline.turns[0].events[0];
        assert_eq!(event.text_chars, 303);
        assert_eq!(
            event.preview.as_deref(),
            Some(format!("ab\0{}", "c".repeat(197)).as_str())
        );
    }

    #[test]
    fn raw_body_archive_survives_ingest_retries_and_trajectory_projection() {
        use base64::{Engine as _, engine::general_purpose::STANDARD};
        use sha2::Digest as _;
        let original =
            b" {\"input\":[{\"role\":\"user\",\"content\":\"hello\"}],\"unknown\": [1,1]} \r\n";
        let mut database = database();
        let mut request = request_with_text("hello");
        request.events[0].event_type = crate::usage::UsageEventType::Request;
        request.events[0].metadata["raw_body"] = json!({"transport":"http", "parts":[{
            "content_base64": STANDARD.encode(original), "byte_size": original.len(), "truncated": false,
            "sha256": hex::encode(sha2::Sha256::digest(original))
        }]});
        let expected = request.events[0].metadata.clone();
        ingest_events(&mut database, &request, OWNER_ID, 100).unwrap();
        ingest_events(&mut database, &request, OWNER_ID, 100).unwrap();
        let session_pk = llm_usage_events::table
            .select(llm_usage_events::session_pk)
            .first::<String>(&mut database)
            .unwrap();
        let session_pk = Uuid::parse_str(&session_pk).unwrap();
        let timeline = super::session_timeline(&mut database, OWNER_ID, session_pk).unwrap();
        let stored = &timeline.turns[0].events[0].metadata;
        assert_eq!(*stored, expected);
        assert_eq!(
            STANDARD
                .decode(
                    stored["raw_body"]["parts"][0]["content_base64"]
                        .as_str()
                        .unwrap()
                )
                .unwrap(),
            original
        );
        let trajectory = crate::usage::trajectory::build(timeline);
        assert_eq!(trajectory.items[0].text.as_deref(), Some("hello"));
        let after = super::session_timeline(&mut database, OWNER_ID, session_pk).unwrap();
        assert_eq!(after.turns[0].events.len(), 1);
        assert_eq!(after.turns[0].events[0].metadata, expected);
    }

    fn summary_seeded() -> SqliteConnection {
        let mut database = database();
        let event = |id: &str, agent: &str, session: &str, turn: i32, observed_at: &str| {
            let (provider, model) = if agent == "Codex" {
                ("OpenAI", "gpt-5")
            } else {
                ("Anthropic", "claude-x")
            };
            json!({
                "event_id": id,
                "observed_at": observed_at,
                "device": {"host_name": "alice-mbp", "platform": "macos", "os_version": "15.1"},
                "agent": {"name": agent},
                "session_id": session,
                "turn_index": turn,
                "llm": {"provider": provider, "model": model},
                "event_type": if id.ends_with("req") { "request" } else { "response" },
                "text": "body",
                "token_usage": if id.ends_with("req") {
                    json!({"input_tokens": 10_i64, "cache_read_tokens": 4_i64})
                } else {
                    json!({"output_tokens": 5_i64})
                },
                "metadata": {"raw_body": "large"}
            })
        };
        let request: IngestEventsRequest = serde_json::from_value(json!({
            "events": [
                event("a-req", "Codex", "alpha", 1_i32, "2026-09-01T23:00:00Z"),
                event("a-res", "Codex", "alpha", 1_i32, "2026-09-01T23:30:00Z"),
                event("b-req", "claude-desktop", "beta", 1_i32, "2026-09-02T01:00:00Z"),
                event("c-res", "claude-code", "gamma", 1_i32, "2026-09-02T02:00:00Z"),
                event("a2-req", "Codex", "alpha", 2_i32, "2026-09-03T00:00:00Z"),
            ]
        }))
        .expect("request should deserialize");
        ingest_events(&mut database, &request, OWNER_ID, 100).expect("ingest should succeed");
        database
    }

    fn summary(database: &mut SqliteConnection, query: serde_json::Value) -> serde_json::Value {
        let query: SummaryQuery = serde_json::from_value(query).expect("query should deserialize");
        let response =
            usage_summary(database, &query, OWNER_ID, 500).expect("summary should succeed");
        serde_json::to_value(response.rows).expect("rows should serialize")
    }

    #[test]
    fn summary_groups_in_sql_with_canonical_agents_and_distinct_counts() {
        let mut database = summary_seeded();

        let rows = summary(
            &mut database,
            json!({"group_by": "agent,provider,model", "to": "2026-09-03T00:00:00Z"}),
        );

        assert_eq!(
            rows,
            json!([
                // Equal totals fall back to the Agent name.
                {
                    "agent_name": "claude-code", "llm_provider": "anthropic",
                    "llm_model": "claude-x",
                    "sessions": 2_i64, "turns": 2_i64, "requests": 1_i64, "responses": 1_i64,
                    "input_tokens": 10_i64, "output_tokens": 5_i64, "cache_read_tokens": 4_i64,
                    "cache_write_tokens": 0_i64, "reasoning_tokens": 0_i64, "total_tokens": 19_i64
                },
                {
                    "agent_name": "codex", "llm_provider": "openai", "llm_model": "gpt-5",
                    "sessions": 1_i64, "turns": 1_i64, "requests": 1_i64, "responses": 1_i64,
                    "input_tokens": 10_i64, "output_tokens": 5_i64, "cache_read_tokens": 4_i64,
                    "cache_write_tokens": 0_i64, "reasoning_tokens": 0_i64, "total_tokens": 19_i64
                }
            ])
        );
    }

    #[test]
    fn summary_filters_and_groups_by_utc_day_and_event_type() {
        let mut database = summary_seeded();

        let rows = summary(
            &mut database,
            json!({"group_by": "day,event_type", "agent_name": "claude", "limit": 10_i64}),
        );

        let mut buckets = rows
            .as_array()
            .expect("rows should be an array")
            .iter()
            .map(|row| {
                (
                    row["day"].clone(),
                    row["event_type"].clone(),
                    row["total_tokens"].clone(),
                )
            })
            .collect::<Vec<_>>();
        buckets.sort_by_key(|bucket| bucket.1.to_string());
        assert_eq!(
            buckets,
            vec![
                (json!("2026-09-02"), json!("request"), json!(14_i64)),
                (json!("2026-09-02"), json!("response"), json!(5_i64)),
            ]
        );
        let codex_requests = summary(
            &mut database,
            json!({"group_by": "event_type", "agent_name": "codex", "event_type": "request"}),
        );
        assert_eq!(codex_requests[0]["requests"], json!(2_i64));
        assert_eq!(codex_requests[0]["turns"], json!(2_i64));
        assert_eq!(codex_requests.as_array().map(Vec::len), Some(1));
    }

    #[test]
    fn summary_fills_owner_and_device_dimensions() {
        let mut database = summary_seeded();

        let rows = summary(&mut database, json!({"group_by": "user,device"}));

        assert_eq!(rows.as_array().map(Vec::len), Some(1));
        assert_eq!(rows[0]["user_id"], json!(OWNER_ID));
        assert!(rows[0]["user_email"].is_string());
        assert_eq!(rows[0]["host_name"], json!("alice-mbp"));
        assert_eq!(rows[0]["platform"], json!("macos"));
        assert_eq!(rows[0]["os_version"], json!("15.1"));
        assert_eq!(rows[0]["sessions"], json!(3_i64));
        assert!(rows[0].get("agent_name").is_none());
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
