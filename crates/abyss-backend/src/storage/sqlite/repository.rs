//! Diesel ORM implementation of SQLite usage ingest and relational queries.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use diesel::{
    ExpressionMethods, IntoSql, NullableExpressionMethods, QueryDsl, QueryableByName, RunQueryDsl,
    SelectableHelper, SqliteConnection,
    dsl::{max, min},
    result::OptionalExtension,
    sql_query,
    sql_types::{BigInt, Bool, Nullable, Text},
};
use uuid::Uuid;

use crate::{
    error::AppError,
    storage::stream_cache::StreamCache,
    usage::{
        IngestEventsRequest, IngestEventsResponse, IngestUsageEvent, RawEventsQuery,
        RawEventsResponse, SummaryQuery, SummaryResponse, SummaryRow, UsageEventResponse,
        attachments::{
            ImageMediaType, StoredImageAttachment, UsageEventAttachmentResponse,
            ValidatedImageAttachment,
        },
        diagnostics::IngestDiagnosticCapture,
        persistence::{
            exact_values_filter, non_empty, normalize_limit, normalize_text, parse_group_by,
            validate_batch, validate_event_type,
        },
        preview::{ConversationPreview, Window, preview},
        segments::{
            SegmentResponse, TokenCounts, ValidatedSegmentQuery, run_event_pks, segment_response,
        },
        session_header::{HeaderEvent, SessionHeaderResponse, session_header as header_of},
        streams::{
            EventLocationResponse, SessionStreamsResponse, SessionStructure, SessionTurnsResponse,
            StreamEvent,
        },
        time_zone::parse_time_zone,
    },
};

use super::{
    day_offsets::day_offsets_json,
    models::{
        AttachmentRow, DeviceRecord, DeviceRow, EventRecord, EventRow, NewAttachment, NewDevice,
        NewDiagnosticCapture, NewDiagnosticCaptureEvent, NewEvent, NewSession, NewStreamFacts,
        SessionRow, StreamEventRow, parse_uuid, timestamp_from_micros,
    },
    schema::{
        agent_diagnostic_capture_events, agent_diagnostic_captures, agent_sessions, app_users,
        devices, llm_usage_event_attachments, llm_usage_event_stream_facts, llm_usage_events,
    },
    search::index_event,
    session_titles,
};

pub(super) fn ingest_events(
    connection: &mut SqliteConnection,
    request: &IngestEventsRequest,
    user_id: Uuid,
    max_batch_size: usize,
) -> Result<IngestEventsResponse, AppError> {
    let validated_attachments = validate_batch(request, max_batch_size)?;
    // Reserve the single SQLite writer before updating session metadata.
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
    let event_id = event.event_id.clone();
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
    let agent_name = event.agent.name.clone();
    let agent_version = event.agent.version.clone();
    let provider = event.llm.provider.clone();
    let model = event.llm.model.clone();
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
    let tokens = &event.token_usage;
    let event_pk = Uuid::now_v7();
    let metadata = serialize_json(&event.metadata, "event metadata")?;

    let inserted = diesel::insert_into(llm_usage_events::table)
        .values(NewEvent {
            id: event_pk.to_string(),
            user_id: user_id.to_string(),
            device_context_id: device_id.to_string(),
            session_pk: session_pk.to_string(),
            event_id,
            agent_name: agent_name.clone(),
            agent_version,
            session_id: event.session_id.clone(),
            llm_provider: provider,
            llm_model: model,
            event_type: event.event_type.as_str().to_owned(),
            input_tokens: tokens.input_tokens.unwrap_or_default(),
            output_tokens: tokens.output_tokens.unwrap_or_default(),
            cache_read_tokens: tokens.cache_read_tokens.unwrap_or_default(),
            cache_write_tokens: tokens.cache_write_tokens.unwrap_or_default(),
            reasoning_tokens: tokens.reasoning_tokens.unwrap_or_default(),
            total_tokens: tokens.total_tokens.unwrap_or_default(),
            observed_at,
            metadata,
            created_at: now,
            stream_id: event.stream_id.clone(),
            response_id: event.response_id.clone(),
            previous_response_id: event.previous_response_id.clone(),
        })
        .on_conflict(llm_usage_events::event_id)
        .do_nothing()
        .execute(connection)?;
    if inserted != 1 {
        return Ok(false);
    }

    insert_stream_facts(connection, event_pk, event)?;
    insert_attachments(connection, user_id, event_pk, attachments, now)?;
    index_event(
        connection,
        &user_id.to_string(),
        &event_pk.to_string(),
        &event.metadata,
    )?;
    session_titles::capture(
        connection,
        &session_titles::CapturedEvent {
            user_id,
            session_pk,
            event_pk,
            event_type: event.event_type.as_str(),
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
    let host_name = event.device.host_name.clone();
    let platform = event.device.platform.clone();
    let os_version = event.device.os_version.clone();
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
#[expect(
    clippy::arithmetic_side_effects,
    reason = "Diesel builds an atomic SQL increment, not a Rust integer addition."
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
    let session_id = event.session_id.clone();
    let existing = agent_sessions::table
        .filter(agent_sessions::user_id.eq(&owner))
        .filter(agent_sessions::agent_name.eq(agent_name))
        .filter(agent_sessions::session_id.eq(&session_id))
        .select(SessionRow::as_select())
        .first::<SessionRow>(connection)
        .optional()?;
    if let Some(existing) = existing {
        let id = parse_uuid(&existing.id, "session id")?;
        diesel::update(agent_sessions::table.find(existing.id))
            .set((
                agent_sessions::device_context_id.eq(device_id.to_string()),
                agent_sessions::agent_version
                    .eq(agent_version.map(str::to_owned).or(existing.agent_version)),
                agent_sessions::started_at.eq(existing.started_at.min(observed_at)),
                agent_sessions::ended_at.eq(Some(
                    existing.ended_at.unwrap_or(observed_at).max(observed_at),
                )),
                agent_sessions::updated_at.eq(now),
                agent_sessions::structure_revision.eq(agent_sessions::structure_revision + 1_i64),
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
            metadata: "{}".to_owned(),
            created_at: now,
            updated_at: now,
            structure_revision: 1,
        })
        .execute(connection)?;
    Ok(id)
}

/// Derives the streams of one session's events from their stream facts,
/// without reading any request body.
pub(super) fn session_streams(
    connection: &mut SqliteConnection,
    user_id: Uuid,
    session_pk: Uuid,
    cache: &StreamCache,
) -> Result<SessionStreamsResponse, AppError> {
    let structure = session_structure(connection, user_id, session_pk, cache)?;
    Ok(structure.session_streams(session_pk))
}

/// Derives the turn view of one session from its stream facts, without
/// reading any request body.
pub(super) fn session_turns(
    connection: &mut SqliteConnection,
    user_id: Uuid,
    session_pk: Uuid,
    cache: &StreamCache,
) -> Result<SessionTurnsResponse, AppError> {
    let structure = session_structure(connection, user_id, session_pk, cache)?;
    Ok(structure.session_turns(session_pk))
}

/// Uses the caller's read transaction so the revision and facts are one snapshot.
/// A cache hit reads only the session revision; it does not load the event facts.
pub(super) fn session_structure(
    connection: &mut SqliteConnection,
    user_id: Uuid,
    session_pk: Uuid,
    cache: &StreamCache,
) -> Result<Arc<SessionStructure>, AppError> {
    let revision = agent_sessions::table
        .filter(agent_sessions::user_id.eq(user_id.to_string()))
        .filter(agent_sessions::id.eq(session_pk.to_string()))
        .select(agent_sessions::structure_revision)
        .first::<i64>(connection)
        .optional()?
        .ok_or_else(|| AppError::not_found(format!("session not found: {session_pk}")))?;
    cache.get_or_load(user_id, session_pk, revision, || {
        let events = stream_events(connection, user_id, session_pk)?;
        if events.is_empty() {
            return Err(AppError::not_found(format!(
                "session not found: {session_pk}"
            )));
        }
        Ok(events)
    })
}

/// Resolves an authorized event without reading its body or attachments.
pub(super) fn event_location(
    connection: &mut SqliteConnection,
    user_id: Uuid,
    event_pk: Uuid,
    cache: &StreamCache,
) -> Result<EventLocationResponse, AppError> {
    let session_pk = llm_usage_events::table
        .filter(llm_usage_events::user_id.eq(user_id.to_string()))
        .filter(llm_usage_events::id.eq(event_pk.to_string()))
        .select(llm_usage_events::session_pk)
        .first::<String>(connection)
        .optional()?
        .ok_or_else(|| AppError::not_found(format!("event not found: {event_pk}")))?;
    let session_pk = parse_uuid(&session_pk, "session primary key")?;
    session_structure(connection, user_id, session_pk, cache)?
        .event_location(session_pk, event_pk)
        .ok_or_else(|| AppError::not_found(format!("event not found: {event_pk}")))
}

/// Columns of one session event the header reads.
type HeaderColumns = (
    String,
    String,
    Option<String>,
    String,
    String,
    i64,
    i64,
    i64,
    i64,
    i64,
    i64,
    i64,
);

/// The header of an owner-scoped session.
pub(super) fn session_header(
    connection: &mut SqliteConnection,
    user_id: Uuid,
    session_pk: Uuid,
    cache: &StreamCache,
) -> Result<SessionHeaderResponse, AppError> {
    let events = llm_usage_events::table
        .filter(llm_usage_events::user_id.eq(user_id.to_string()))
        .filter(llm_usage_events::session_pk.eq(session_pk.to_string()))
        .select((
            llm_usage_events::session_id,
            llm_usage_events::agent_name,
            llm_usage_events::agent_version,
            llm_usage_events::llm_provider,
            llm_usage_events::llm_model,
            llm_usage_events::observed_at,
            llm_usage_events::input_tokens,
            llm_usage_events::output_tokens,
            llm_usage_events::cache_read_tokens,
            llm_usage_events::cache_write_tokens,
            llm_usage_events::reasoning_tokens,
            llm_usage_events::total_tokens,
        ))
        .load::<HeaderColumns>(connection)?
        .into_iter()
        .map(header_event)
        .collect::<Result<Vec<_>, AppError>>()?;
    if events.is_empty() {
        return Err(AppError::not_found(format!(
            "session not found: {session_pk}"
        )));
    }
    let summary = session_structure(connection, user_id, session_pk, cache)?.summary();
    let title = session_titles::title(connection, session_pk)?;
    Ok(header_of(session_pk, events, &summary, title))
}

fn header_event(
    (
        session_id,
        agent_name,
        agent_version,
        provider,
        model,
        observed_at,
        input,
        output,
        cache_read,
        cache_write,
        reasoning,
        total,
    ): HeaderColumns,
) -> Result<HeaderEvent, AppError> {
    Ok(HeaderEvent {
        session_id,
        agent_name,
        agent_version,
        provider,
        model,
        observed_at: timestamp_from_micros(observed_at)?,
        tokens: TokenCounts {
            input,
            output,
            cache_read,
            cache_write,
            reasoning,
            total,
        },
    })
}

/// The exchanges of the segment holding exchange `head`, from it to the
/// segment end, with a preview of each event.
pub(super) fn session_segment(
    connection: &mut SqliteConnection,
    user_id: Uuid,
    session_pk: Uuid,
    head: &str,
    query: &ValidatedSegmentQuery,
    cache: &StreamCache,
) -> Result<SegmentResponse, AppError> {
    let structure = session_structure(connection, user_id, session_pk, cache)?;
    let run = structure.segment_run(head).ok_or_else(|| {
        AppError::not_found(format!(
            "exchange not found in session {session_pk}: {head}"
        ))
    })?;
    let events = usage_events(connection, user_id, &run_event_pks(&run))?;
    Ok(segment_response(session_pk, run, events, &query.window))
}

/// The conversation of one owner-scoped event through `window`.
pub(super) fn event_conversation(
    connection: &mut SqliteConnection,
    user_id: Uuid,
    event_pk: Uuid,
    window: &Window,
) -> Result<ConversationPreview, AppError> {
    usage_event(connection, user_id, event_pk).map(|event| preview(event, window))
}

/// Owner-scoped events by primary key, with their device labels and
/// attachments.
fn usage_events(
    connection: &mut SqliteConnection,
    user_id: Uuid,
    pks: &[Uuid],
) -> Result<HashMap<Uuid, UsageEventResponse>, AppError> {
    let events = llm_usage_events::table
        .filter(llm_usage_events::id.eq_any(pks.iter().map(Uuid::to_string)))
        .filter(llm_usage_events::user_id.eq(user_id.to_string()))
        .select(EventRow::as_select())
        .load::<EventRow>(connection)?
        .into_iter()
        .map(EventRow::into_record)
        .collect::<Result<Vec<_>, _>>()?;
    let devices = load_device_contexts(connection, &events)?;
    let mut attachments = load_attachments(connection, &events)?;
    Ok(events
        .into_iter()
        .map(|event| {
            let (event_pk, device_id) = (event.id, event.device_context_id);
            let attachments = attachments.remove(&event_pk).unwrap_or_default();
            (
                event_pk,
                event_response(event, devices.get(&device_id), attachments),
            )
        })
        .collect())
}

/// The events of one session in session order, with their stream facts and
/// without any request body.
pub(super) fn stream_events(
    connection: &mut SqliteConnection,
    user_id: Uuid,
    session_pk: Uuid,
) -> Result<Vec<StreamEvent>, AppError> {
    llm_usage_events::table
        .left_join(llm_usage_event_stream_facts::table)
        .filter(llm_usage_events::user_id.eq(user_id.to_string()))
        .filter(llm_usage_events::session_pk.eq(session_pk.to_string()))
        .order_by((
            llm_usage_events::observed_at.asc(),
            llm_usage_events::event_type.asc(),
            llm_usage_events::id.asc(),
        ))
        .select((
            llm_usage_events::id,
            llm_usage_events::event_id,
            llm_usage_events::event_type,
            llm_usage_events::observed_at,
            llm_usage_events::response_id,
            llm_usage_events::previous_response_id,
            llm_usage_event_stream_facts::new_from.nullable(),
            llm_usage_event_stream_facts::starts_turn.nullable(),
            llm_usage_event_stream_facts::last_identity.nullable(),
            llm_usage_event_stream_facts::trailing.nullable(),
            llm_usage_event_stream_facts::result_ids.nullable(),
            llm_usage_event_stream_facts::emitted_ids.nullable(),
            llm_usage_event_stream_facts::output_skeleton.nullable(),
            llm_usage_event_stream_facts::label_index.nullable(),
            llm_usage_event_stream_facts::label_text.nullable(),
            llm_usage_event_stream_facts::label_length.nullable(),
            llm_usage_event_stream_facts::subagent_id.nullable(),
            llm_usage_event_stream_facts::parent_subagent_id.nullable(),
            llm_usage_event_stream_facts::turn_id.nullable(),
        ))
        .load::<StreamEventRow>(connection)?
        .into_iter()
        .map(StreamEventRow::into_stream_event)
        .collect()
}

/// Stores the stream facts read from one new event's own entries.
fn insert_stream_facts(
    connection: &mut SqliteConnection,
    event_pk: Uuid,
    event: &IngestUsageEvent,
) -> Result<(), AppError> {
    let facts = NewStreamFacts::read(
        event_pk.to_string(),
        event.event_type.as_str(),
        &event.metadata,
        event.previous_response_id.is_some(),
    )?;
    let facts = NewStreamFacts {
        subagent_id: event.subagent_id.clone(),
        parent_subagent_id: event.parent_subagent_id.clone(),
        turn_id: event.turn_id.clone(),
        ..facts
    };
    diesel::insert_into(llm_usage_event_stream_facts::table)
        .values(facts)
        .execute(connection)?;
    Ok(())
}

fn ingest_one_diagnostic_capture(
    connection: &mut SqliteConnection,
    user_id: Uuid,
    capture: &IngestDiagnosticCapture,
) -> Result<bool, AppError> {
    let events = llm_usage_events::table
        .filter(llm_usage_events::user_id.eq(user_id.to_string()))
        .filter(llm_usage_events::event_id.eq_any(&capture.event_ids))
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
            agent_names: &query.agent_name,
            session_ids: &query.session_id,
            session_pk: query.session_pk,
            llm_providers: &query.llm_provider,
            llm_models: &query.llm_model,
            event_type: query.event_type.as_deref(),
            limit: page_size.saturating_add(1),
            offset,
        },
        query.includes_content(),
    )?;
    let has_next_page = events.len() > page_size_usize;
    if has_next_page {
        events.truncate(page_size_usize);
    }
    let devices = load_device_contexts(connection, &events)?;
    let mut attachments = if query.includes_content() {
        load_attachments(connection, &events)?
    } else {
        HashMap::new()
    };
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
    /// Each list matches any of its values; an empty list matches everything.
    agent_names: &'a [String],
    session_ids: &'a [String],
    session_pk: Option<Uuid>,
    llm_providers: &'a [String],
    llm_models: &'a [String],
    event_type: Option<&'a str>,
    limit: i64,
    offset: i64,
}

/// Loads matching events newest first. Without content, text and metadata are
/// not read: they hold whole request bodies, and an event list never shows them.
fn load_filtered_events(
    connection: &mut SqliteConnection,
    filters: &EventFilters<'_>,
    include_content: bool,
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
    let agent_names = exact_values_filter(filters.agent_names);
    if !agent_names.is_empty() {
        query = query.filter(llm_usage_events::agent_name.eq_any(agent_names));
    }
    let session_ids = exact_values_filter(filters.session_ids);
    if !session_ids.is_empty() {
        query = query.filter(llm_usage_events::session_id.eq_any(session_ids));
    }
    if let Some(session_pk) = filters.session_pk {
        query = query.filter(llm_usage_events::session_pk.eq(session_pk.to_string()));
    }
    let providers = exact_values_filter(filters.llm_providers);
    if !providers.is_empty() {
        query = query.filter(llm_usage_events::llm_provider.eq_any(providers));
    }
    let models = exact_values_filter(filters.llm_models);
    if !models.is_empty() {
        query = query.filter(llm_usage_events::llm_model.eq_any(models));
    }
    if let Some(event_type) = filters.event_type.and_then(non_empty) {
        query = query.filter(llm_usage_events::event_type.eq(validate_event_type(event_type)?));
    }
    let query = query
        .order_by((
            llm_usage_events::observed_at.desc(),
            llm_usage_events::event_type.asc(),
            llm_usage_events::id.desc(),
        ))
        .limit(filters.limit)
        .offset(filters.offset);
    let rows = if include_content {
        query
            .select(EventRow::as_select())
            .load::<EventRow>(connection)?
    } else {
        query
            .select((
                llm_usage_events::id,
                llm_usage_events::user_id,
                llm_usage_events::device_context_id,
                llm_usage_events::session_pk,
                llm_usage_events::event_id,
                llm_usage_events::agent_name,
                llm_usage_events::agent_version,
                llm_usage_events::session_id,
                llm_usage_events::llm_provider,
                llm_usage_events::llm_model,
                llm_usage_events::event_type,
                llm_usage_events::input_tokens,
                llm_usage_events::output_tokens,
                llm_usage_events::cache_read_tokens,
                llm_usage_events::cache_write_tokens,
                llm_usage_events::reasoning_tokens,
                llm_usage_events::total_tokens,
                llm_usage_events::observed_at,
                "{}".into_sql::<Text>(),
                llm_usage_events::created_at,
                llm_usage_events::stream_id,
                llm_usage_events::response_id,
                llm_usage_events::previous_response_id,
            ))
            .load::<EventRow>(connection)?
    };
    rows.into_iter().map(EventRow::into_record).collect()
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
    let zone = parse_time_zone(query.tz.as_deref())?;
    let owner = load_owner(connection, user_id)?;
    if !owner_matches_filter(user_id, &owner, query.user_filter.as_deref()) {
        return Ok(empty_summary(query, group_by));
    }
    // Each list matches any of its values; an empty list matches everything.
    let filter_list = |values: Vec<String>| {
        serde_json::to_string(&values)
            .map(|json| (!values.is_empty(), json))
            .map_err(|error| AppError::internal(format!("serialize summary filter: {error}")))
    };
    let (has_agents, agent_names) = filter_list(exact_values_filter(&query.agent_name))?;
    let (has_sessions, session_ids) = filter_list(exact_values_filter(&query.session_id))?;
    let (has_providers, providers) = filter_list(exact_values_filter(&query.llm_provider))?;
    let (has_models, models) = filter_list(exact_values_filter(&query.llm_model))?;
    let event_type = query
        .event_type
        .as_deref()
        .and_then(non_empty)
        .map(validate_event_type)
        .transpose()?;
    let limit = normalize_limit(query.limit, default_limit, 100_000);
    let from = query.from.map_or(i64::MIN, timestamp_to_micros);
    let to = query.to.map_or(i64::MAX, timestamp_to_micros);
    let day_offsets = if dimensions.enabled(SummaryDimension::Day) {
        day_offsets(connection, user_id, zone, from, to)?
    } else {
        "[]".to_owned()
    };

    // Bind order mirrors the numbered placeholders in the query.
    let rows = sql_query(SUMMARY_AGGREGATE_SQL)
        .bind::<Bool, _>(dimensions.enabled(SummaryDimension::Day))
        .bind::<Bool, _>(dimensions.enabled(SummaryDimension::Device))
        .bind::<Bool, _>(dimensions.enabled(SummaryDimension::Agent))
        .bind::<Bool, _>(dimensions.enabled(SummaryDimension::Provider))
        .bind::<Bool, _>(dimensions.enabled(SummaryDimension::Model))
        .bind::<Bool, _>(dimensions.enabled(SummaryDimension::EventType))
        .bind::<Text, _>(user_id.to_string())
        .bind::<BigInt, _>(from)
        .bind::<BigInt, _>(to)
        .bind::<Bool, _>(has_agents)
        .bind::<Text, _>(&agent_names)
        .bind::<Bool, _>(has_sessions)
        .bind::<Text, _>(&session_ids)
        .bind::<Bool, _>(has_providers)
        .bind::<Text, _>(&providers)
        .bind::<Bool, _>(has_models)
        .bind::<Text, _>(&models)
        .bind::<Bool, _>(event_type.is_some())
        .bind::<Text, _>(event_type.unwrap_or_default())
        .bind::<BigInt, _>(limit)
        .bind::<Text, _>(&day_offsets)
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

/// Day offsets for `zone` across the owner's events observed in `[from, to)`;
/// see [`day_offsets_json`].
fn day_offsets(
    connection: &mut SqliteConnection,
    user_id: Uuid,
    zone: Tz,
    from: i64,
    to: i64,
) -> Result<String, AppError> {
    if zone == Tz::UTC {
        return day_offsets_json(zone, None);
    }
    let (first, last) = llm_usage_events::table
        .filter(llm_usage_events::user_id.eq(user_id.to_string()))
        .filter(llm_usage_events::observed_at.ge(from))
        .filter(llm_usage_events::observed_at.lt(to))
        .select((
            min(llm_usage_events::observed_at),
            max(llm_usage_events::observed_at),
        ))
        .first::<(Option<i64>, Option<i64>)>(connection)?;
    day_offsets_json(zone, first.zip(last))
}

/// Usage totals grouped in SQL, reading only the columns a summary reports.
///
/// Event metadata holds each event's conversation, so reading event rows
/// reads a page per event; the summary cover index carries every event column
/// used here, `id` included for the stream facts join. The owner is the single
/// standalone user, so the user dimension is filled in afterwards.
///
/// Turns are counted per session within each group, then summed: the
/// distinct declared `turn_id` values when the session declares any turn,
/// else the requests that open a turn.
///
/// Bind order: `?1`..`?6` day, device, Agent, provider, model, and event-type
/// dimensions; `?7` owner; `?8`/`?9` observation bounds in microseconds, kept
/// unconditional so the owner-time index bounds the scan; `?10`..`?17` flags
/// and JSON value lists for the Agent, session, provider, and model filters,
/// each matching any listed value; `?18`/`?19` event-type filter; `?20` row
/// limit; `?21` the day offsets from [`day_offsets`], read only for the day
/// dimension.
const SUMMARY_AGGREGATE_SQL: &str = "\
    WITH day_offsets(start_us, offset_us) AS MATERIALIZED ( \
        SELECT json_extract(value, '$[0]'), json_extract(value, '$[1]') FROM json_each(?21) \
    ), \
    first_openers AS ( \
        SELECT id FROM ( \
            SELECT fe.id, ROW_NUMBER() OVER ( \
                PARTITION BY fe.session_pk ORDER BY fe.observed_at, fe.id \
            ) AS n \
            FROM llm_usage_events fe \
            JOIN llm_usage_event_stream_facts ff ON ff.event_pk = fe.id \
            WHERE ff.starts_turn = 1 \
        ) firsts WHERE n = 1 \
    ), \
    by_session AS ( \
        SELECT \
            CASE WHEN ?1 THEN strftime( \
                '%Y-%m-%d', \
                (e.observed_at + ( \
                    SELECT o.offset_us FROM day_offsets o WHERE o.start_us <= e.observed_at \
                    ORDER BY o.start_us DESC LIMIT 1 \
                )) / 1000000, \
                'unixepoch' \
            ) END AS day, \
            CASE WHEN ?2 THEN d.host_name END AS host_name, \
            CASE WHEN ?2 THEN d.platform END AS platform, \
            CASE WHEN ?2 THEN d.os_version END AS os_version, \
            CASE WHEN ?3 THEN e.agent_name END AS agent_name, \
            CASE WHEN ?4 THEN e.llm_provider END AS llm_provider, \
            CASE WHEN ?5 THEN e.llm_model END AS llm_model, \
            CASE WHEN ?6 THEN e.event_type END AS event_type, \
            e.session_pk AS session_pk, \
            EXISTS ( \
                SELECT 1 FROM llm_usage_events de \
                JOIN llm_usage_event_stream_facts df ON df.event_pk = de.id \
                WHERE de.session_pk = e.session_pk AND df.turn_id IS NOT NULL \
            ) AS declares_turns, \
            COUNT(DISTINCT f.turn_id) AS declared_turns, \
            COUNT(*) FILTER (WHERE f.starts_turn = 1 \
                AND (f.continues_context = 1 OR e.id IN first_openers)) AS opened_turns, \
            COUNT(*) FILTER (WHERE e.event_type = 'request') AS requests, \
            COUNT(*) FILTER (WHERE e.event_type = 'response') AS responses, \
            COALESCE(SUM(e.input_tokens), 0) AS input_tokens, \
            COALESCE(SUM(e.output_tokens), 0) AS output_tokens, \
            COALESCE(SUM(e.cache_read_tokens), 0) AS cache_read_tokens, \
            COALESCE(SUM(e.cache_write_tokens), 0) AS cache_write_tokens, \
            COALESCE(SUM(e.reasoning_tokens), 0) AS reasoning_tokens, \
            COALESCE(SUM(e.total_tokens), 0) AS total_tokens \
        FROM llm_usage_events e \
        LEFT JOIN llm_usage_event_stream_facts f ON f.event_pk = e.id \
        LEFT JOIN devices d ON ?2 AND d.id = e.device_context_id \
        WHERE e.user_id = ?7 \
            AND e.observed_at >= ?8 \
            AND e.observed_at < ?9 \
            AND (?10 = 0 OR e.agent_name IN (SELECT value FROM json_each(?11))) \
            AND (?12 = 0 OR e.session_id IN (SELECT value FROM json_each(?13))) \
            AND (?14 = 0 OR e.llm_provider IN (SELECT value FROM json_each(?15))) \
            AND (?16 = 0 OR e.llm_model IN (SELECT value FROM json_each(?17))) \
            AND (?18 = 0 OR e.event_type = ?19) \
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9 \
    ) \
    SELECT \
        day, host_name, platform, os_version, agent_name, llm_provider, llm_model, event_type, \
        COUNT(*) AS sessions, \
        COALESCE(SUM(CASE WHEN declares_turns THEN declared_turns ELSE opened_turns END), 0) AS turns, \
        COALESCE(SUM(requests), 0) AS requests, \
        COALESCE(SUM(responses), 0) AS responses, \
        COALESCE(SUM(input_tokens), 0) AS input_tokens, \
        COALESCE(SUM(output_tokens), 0) AS output_tokens, \
        COALESCE(SUM(cache_read_tokens), 0) AS cache_read_tokens, \
        COALESCE(SUM(cache_write_tokens), 0) AS cache_write_tokens, \
        COALESCE(SUM(reasoning_tokens), 0) AS reasoning_tokens, \
        COALESCE(SUM(total_tokens), 0) AS total_tokens \
    FROM by_session g \
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
        location: None,
        agent_name: event.agent_name,
        agent_version: event.agent_version,
        session_id: event.session_id,
        llm_provider: event.llm_provider,
        llm_model: event.llm_model,
        event_type: event.event_type,
        input_tokens: event.input_tokens,
        output_tokens: event.output_tokens,
        cache_read_tokens: event.cache_read_tokens,
        cache_write_tokens: event.cache_write_tokens,
        reasoning_tokens: event.reasoning_tokens,
        total_tokens: event.total_tokens,
        observed_at: event.observed_at,
        stream_id: event.stream_id,
        response_id: event.response_id,
        previous_response_id: event.previous_response_id,
        metadata: event.metadata,
        attachments,
    }
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
    use crate::storage::stream_cache::StreamCache;
    use diesel::{
        Connection, ExpressionMethods, QueryDsl, RunQueryDsl, SqliteConnection, dsl::count_star,
        sql_query,
    };
    use serde_json::json;
    use uuid::Uuid;

    use crate::{
        search::SessionSearchQuery,
        usage::{
            IngestEventsRequest, RawEventsQuery, SummaryQuery,
            preview::{PreviewQuery, Window},
            segments::SegmentQuery,
        },
    };

    use super::{
        super::{
            migrations,
            schema::{agent_sessions, llm_usage_event_stream_facts, llm_usage_events},
            search,
        },
        event_conversation, event_location, ingest_events, raw_events, session_header,
        session_segment, session_streams, usage_summary,
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

        let reference_count = sql_query("SELECT count(*) AS count FROM search_refs")
            .get_result::<Count>(&mut database)
            .expect("search references should be queryable")
            .count;
        assert_eq!(reference_count, 1, "one piece, indexed once");
        let results = search::session_search(
            &mut database,
            OWNER_ID,
            SessionSearchQuery {
                q: "searchable response".to_owned(),
                from: None,
                to: None,
                agent_name: vec!["claude-code".to_owned(), "codex".to_owned()],
                llm_provider: Vec::new(),
                llm_model: Vec::new(),
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
            "a search result should contain a structured highlight"
        );
    }

    #[test]
    fn validation_failure_leaves_relational_and_search_tables_empty() {
        let mut database = database();
        let request = request_with_text("invalid event");

        ingest_events(&mut database, &request, OWNER_ID, 0)
            .expect_err("a batch over the size limit should be rejected");
        let relational_count = llm_usage_events::table
            .select(count_star())
            .first::<i64>(&mut database)
            .expect("relational table should be queryable");
        assert_eq!(relational_count, 0);

        let search_count = sql_query(
            "SELECT (SELECT count(*) FROM search_texts) + (SELECT count(*) FROM search_refs) \
             AS count",
        )
        .get_result::<Count>(&mut database)
        .expect("search tables should be queryable")
        .count;
        assert_eq!(search_count, 0);
    }

    #[test]
    fn raw_body_archive_survives_ingest_retries_and_preview_projection() {
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
        let event_pk = llm_usage_events::table
            .select(llm_usage_events::id)
            .first::<String>(&mut database)
            .unwrap();
        let event_pk = Uuid::parse_str(&event_pk).unwrap();
        let event = super::usage_event(&mut database, OWNER_ID, event_pk).unwrap();
        let stored = &event.metadata;
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
        let preview =
            event_conversation(&mut database, OWNER_ID, event_pk, &Window::default()).unwrap();
        assert_eq!(preview.chunks[0].entries[0]["text"].as_str(), Some("hello"));
        let after = super::usage_event(&mut database, OWNER_ID, event_pk).unwrap();
        assert_eq!(after.metadata, expected);
    }

    #[test]
    fn ingest_stores_reported_values_unchanged() {
        let mut database = database();
        let mut request = request_with_text("  hello \n");
        let event = &mut request.events[0];
        event.session_id = " Session ".to_owned();
        event.llm.provider = "Gateway_Host".to_owned();
        event.llm.model = " Model ".to_owned();
        event.device.platform = "macOS".to_owned();
        ingest_events(&mut database, &request, OWNER_ID, 100).expect("ingest should succeed");
        let query: RawEventsQuery =
            serde_json::from_value(json!({})).expect("query should deserialize");
        let events = raw_events(&mut database, &query, OWNER_ID, 50)
            .expect("events should load")
            .events;
        let stored = events
            .iter()
            .find(|event| event.event_id == "sqlite-event")
            .expect("the first event should be stored");
        assert_eq!(stored.session_id, " Session ");
        assert_eq!(stored.llm_provider, "Gateway_Host");
        assert_eq!(stored.llm_model, " Model ");
        assert_eq!(stored.platform.as_deref(), Some("macOS"));
        assert_eq!(
            stored.metadata["conversation"]["entries"][0]["text"],
            json!("  hello \n")
        );
    }

    #[test]
    fn ingest_stores_the_stream_facts_of_each_new_event_beside_it() {
        type Facts = (
            String,
            Option<i32>,
            Option<bool>,
            Option<String>,
            Option<String>,
            Option<String>,
        );
        let mut database = database();
        let event = |id: &str, event_type: &str, entries: serde_json::Value| {
            json!({
                "event_id": id,
                "observed_at": "2026-10-19T00:00:00Z",
                "device": {"host_name": "local", "platform": "macos"},
                "agent": {"name": "codex"},
                "session_id": "sqlite-session",
                "llm": {"provider": "openai", "model": "gpt-5"},
                "event_type": event_type,
                "metadata": {"conversation": {"entries": entries, "completeness": "complete"}}
            })
        };
        let request: IngestEventsRequest = serde_json::from_value(json!({"events": [
            event("ask", "request", json!([
                {"role": "user", "text": "fix it"},
                {"role": "tool_call", "text": "{}", "call_id": "a", "item_id": "fc_1"},
                {"role": "tool_result", "text": "ok", "call_id": "a"}
            ])),
            event("answer", "response", json!([
                {"role": "assistant", "text": "done", "item_id": "msg_1"}
            ]))
        ]}))
        .expect("test ingest request should deserialize");
        for _ in 0_u8..2 {
            ingest_events(&mut database, &request, OWNER_ID, 100).expect("ingest should succeed");
        }

        let facts = llm_usage_event_stream_facts::table
            .inner_join(llm_usage_events::table)
            .order(llm_usage_events::event_id.desc())
            .select((
                llm_usage_events::event_id,
                llm_usage_event_stream_facts::new_from,
                llm_usage_event_stream_facts::starts_turn,
                llm_usage_event_stream_facts::last_identity,
                llm_usage_event_stream_facts::trailing,
                llm_usage_event_stream_facts::emitted_ids,
            ))
            .load::<Facts>(&mut database)
            .expect("stream facts should load");
        assert_eq!(
            facts,
            [
                (
                    "ask".to_owned(),
                    Some(2_i32),
                    Some(false),
                    Some("a".to_owned()),
                    Some("[]".to_owned()),
                    None
                ),
                (
                    "answer".to_owned(),
                    None,
                    None,
                    None,
                    None,
                    Some(r#"["item:msg_1"]"#.to_owned())
                ),
            ],
            "one row per event, written once, the other side's columns NULL"
        );
    }

    /// A database holding session `streams-session`: exchange r1 asks and
    /// calls c1, exchange r2 sends its result back and gets an answer.
    fn streams_database() -> (SqliteConnection, Uuid) {
        let mut database = database();
        let event = |id: &str, second: u8, event_type: &str, entries: serde_json::Value| {
            let response = if second < 3 { "r1" } else { "r2" };
            json!({
                "event_id": id,
                "observed_at": format!("2026-10-20T00:00:0{second}Z"),
                "device": {"host_name": "local", "platform": "macos"},
                "agent": {"name": "codex"},
                "session_id": "streams-session",
                "llm": {"provider": "openai", "model": "gpt-5"},
                "event_type": event_type,
                "response_id": response,
                "metadata": {"conversation": {"entries": entries, "completeness": "complete"}}
            })
        };
        let request: IngestEventsRequest = serde_json::from_value(json!({"events": [
            event("a", 1, "request", json!([{"role": "user", "text": "fix the bug"}])),
            event("ab", 2, "response", json!([
                {"role": "tool_call", "text": "{}", "call_id": "c1"}
            ])),
            event("abc", 3, "request", json!([
                {"role": "user", "text": "fix the bug"},
                {"role": "tool_call", "text": "{}", "call_id": "c1"},
                {"role": "tool_result", "text": "ok", "call_id": "c1"}
            ])),
            event("abcd", 4, "response", json!([{"role": "assistant", "text": "fixed"}]))
        ]}))
        .expect("test ingest request should deserialize");
        ingest_events(&mut database, &request, OWNER_ID, 100).expect("ingest should succeed");
        let session_pk = agent_sessions::table
            .select(agent_sessions::id)
            .first::<String>(&mut database)
            .expect("the session is stored");
        let session_pk = Uuid::parse_str(&session_pk).expect("a UUID");
        (database, session_pk)
    }

    #[test]
    fn session_streams_link_stored_exchanges() {
        let (mut database, session_pk) = streams_database();
        let streams = session_streams(&mut database, OWNER_ID, session_pk, &StreamCache::default())
            .expect("the session's streams derive");
        let segments = streams
            .segments
            .iter()
            .map(|segment| (segment.head.as_str(), segment.exchange_count))
            .collect::<Vec<_>>();
        assert_eq!(segments, [("r1", 2)], "r2 continues r1 by its call");
        let label = streams.segments[0]
            .label
            .as_ref()
            .expect("r1 asks in words");
        assert_eq!(
            (label.entry_index, label.text.as_str(), label.length),
            (0_i32, "fix the bug", 11_i32)
        );
        assert!(
            session_streams(
                &mut database,
                OWNER_ID,
                Uuid::nil(),
                &StreamCache::default()
            )
            .is_err(),
            "a session without events is not found"
        );
    }

    #[test]
    fn declared_sub_agent_ids_are_stored_and_group_the_sub_agent_segment() {
        let mut database = database();
        let event = |id: &str, second: u8, event_type: &str, declared: serde_json::Value| {
            let mut event = json!({
                "event_id": id,
                "observed_at": format!("2026-10-20T00:00:0{second}Z"),
                "device": {"host_name": "local", "platform": "macos"},
                "agent": {"name": "claude-code"},
                "session_id": "declared-session",
                "llm": {"provider": "anthropic", "model": "claude"},
                "event_type": event_type,
                "response_id": &id[..1],
                "metadata": {"conversation": {
                    "entries": [{"role": "user", "text": id}],
                    "completeness": "complete"
                }}
            });
            for (key, value) in declared.as_object().into_iter().flatten() {
                event[key] = value.clone();
            }
            event
        };
        let main = json!({"turn_id": "p1"});
        let sub = json!({"subagent_id": "a1", "turn_id": "p1"});
        let request: IngestEventsRequest = serde_json::from_value(json!({"events": [
            event("m-req", 1, "request", main.clone()),
            event("m-res", 5, "response", main),
            event("s-req", 2, "request", sub.clone()),
            event("s-res", 3, "response", sub),
            // Without a request, the response's declared ids stand for the
            // exchange; with no turn it attaches by time.
            event("o-res", 4, "response", json!({"subagent_id": "a2", "parent_subagent_id": null})),
        ]}))
        .expect("test ingest request should deserialize");
        ingest_events(&mut database, &request, OWNER_ID, 100).expect("ingest should succeed");

        let stored = llm_usage_event_stream_facts::table
            .inner_join(llm_usage_events::table)
            .filter(llm_usage_events::event_id.eq("s-res"))
            .select((
                llm_usage_event_stream_facts::subagent_id,
                llm_usage_event_stream_facts::parent_subagent_id,
                llm_usage_event_stream_facts::turn_id,
            ))
            .first::<(Option<String>, Option<String>, Option<String>)>(&mut database)
            .expect("the response's facts");
        assert_eq!(stored, (Some("a1".to_owned()), None, Some("p1".to_owned())));

        let session_pk = agent_sessions::table
            .select(agent_sessions::id)
            .first::<String>(&mut database)
            .expect("the session is stored");
        let session_pk = Uuid::parse_str(&session_pk).expect("a UUID");
        let streams = session_streams(&mut database, OWNER_ID, session_pk, &StreamCache::default())
            .expect("the session's streams derive");
        let attachments = streams
            .segments
            .iter()
            .map(|segment| {
                (
                    segment.head.as_str(),
                    segment.subagent_id.as_deref(),
                    segment.attached_to.as_deref(),
                    segment.attached_by,
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            attachments,
            [
                ("m", None, None, None),
                // Sub-agent segments are never attached.
                ("s", Some("a1"), None, None),
                ("o", Some("a2"), None, None),
            ]
        );
    }

    /// One exchange's events: `(name, session, agent, second, declared ids,
    /// request entries, response entries)`.
    type TurnExchange = (
        &'static str,
        &'static str,
        &'static str,
        u8,
        serde_json::Value,
        serde_json::Value,
        serde_json::Value,
    );

    fn turn_events(exchanges: &[TurnExchange]) -> IngestEventsRequest {
        let mut events = Vec::new();
        for (name, session, agent, second, declared, request, response) in exchanges {
            for (side, entries, at) in [("request", request, "0"), ("response", response, "5")] {
                let mut event = json!({
                    "event_id": format!("{name}-{side}"),
                    "observed_at": format!("2026-10-22T00:00:{second:02}.{at}Z"),
                    "device": {"host_name": "local", "platform": "macos"},
                    "agent": {"name": agent},
                    "session_id": session,
                    "llm": {"provider": "anthropic", "model": "claude"},
                    "event_type": side,
                    "response_id": name,
                    "metadata": {"conversation": {"entries": entries, "completeness": "complete"}}
                });
                for (key, value) in declared.as_object().into_iter().flatten() {
                    event[key] = value.clone();
                }
                events.push(event);
            }
        }
        serde_json::from_value(json!({ "events": events })).expect("a valid ingest request")
    }

    /// A Claude Code session `declared` with two declared turns, and a Codex
    /// session `plain` without declarations whose three requests open turns.
    fn turn_database() -> SqliteConnection {
        let mut database = database();
        let user = |text: &str| json!({"role": "user", "text": text});
        let call = |id: &str| json!({"role": "tool_call", "text": "{}", "call_id": id});
        let result = |id: &str| json!({"role": "tool_result", "text": "ok", "call_id": id});
        let answer = |text: &str| json!({"role": "assistant", "text": text});
        let said = |text: &str| json!([answer(text)]);
        let (cc, codex) = ("claude-code", "codex");
        let none = json!({});
        let request = turn_events(&[
            // An auxiliary request before the first prompt belongs to no turn.
            (
                "x",
                "declared",
                cc,
                1,
                none.clone(),
                json!([user("quota")]),
                said("ok"),
            ),
            (
                "m",
                "declared",
                cc,
                2,
                json!({"turn_id": "p1"}),
                json!([user("fix")]),
                json!([call("c1")]),
            ),
            (
                "s",
                "declared",
                cc,
                3,
                json!({"subagent_id": "a1", "turn_id": "p1"}),
                json!([user("explore")]),
                said("found"),
            ),
            // A title request declares nothing and joins p1.
            (
                "t",
                "declared",
                cc,
                4,
                none.clone(),
                json!([user("name it")]),
                said("Fix"),
            ),
            (
                "m2",
                "declared",
                cc,
                5,
                json!({"turn_id": "p1"}),
                json!([user("fix"), call("c1"), result("c1")]),
                said("fixed"),
            ),
            (
                "i",
                "declared",
                cc,
                6,
                json!({"turn_id": "p2"}),
                json!([user("next")]),
                said("ok"),
            ),
            (
                "u1",
                "plain",
                codex,
                7,
                none.clone(),
                json!([user("one")]),
                said("1"),
            ),
            (
                "u2",
                "plain",
                codex,
                8,
                none.clone(),
                // A plain chat carries its history: each later question
                // follows an answer, so each opens a turn.
                json!([user("one"), answer("1"), user("two")]),
                said("2"),
            ),
            (
                "u3",
                "plain",
                codex,
                9,
                none,
                json!([
                    user("one"),
                    answer("1"),
                    user("two"),
                    answer("2"),
                    user("three")
                ]),
                said("3"),
            ),
        ]);
        ingest_events(&mut database, &request, OWNER_ID, 100).expect("ingest should succeed");
        database
    }

    #[test]
    fn turns_follow_declarations_or_turn_opening_requests_everywhere() {
        let mut database = turn_database();
        let stored = llm_usage_event_stream_facts::table
            .inner_join(llm_usage_events::table)
            .filter(llm_usage_events::event_id.eq("i-response"))
            .select(llm_usage_event_stream_facts::turn_id)
            .first::<Option<String>>(&mut database)
            .expect("the response's facts");
        assert_eq!(stored.as_deref(), Some("p2"));

        let query = crate::usage::session_list::SessionListQuery::default()
            .validate()
            .expect("valid");
        let listed = super::super::session_list::list_sessions(&mut database, OWNER_ID, &query)
            .expect("sessions list")
            .items
            .into_iter()
            .map(|item| (item.session_id, (item.session_pk, item.turns)))
            .collect::<std::collections::HashMap<_, _>>();
        let cache = StreamCache::default();
        for (session, turns) in [("declared", 2_i64), ("plain", 3_i64)] {
            let (session_pk, listed_turns) = listed[session];
            assert_eq!(listed_turns, turns, "{session}: listed turns");
            let header =
                session_header(&mut database, OWNER_ID, session_pk, &cache).expect("the header");
            assert_eq!(
                i64::try_from(header.turn_count).expect("a small count"),
                turns,
                "{session}: the header counts as the list does"
            );
        }

        let (declared_pk, _) = listed["declared"];
        let streams =
            session_streams(&mut database, OWNER_ID, declared_pk, &cache).expect("streams");
        let segments = streams
            .segments
            .iter()
            .map(|segment| {
                (
                    segment.head.as_str(),
                    segment.turn_id.as_deref(),
                    segment.turn,
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            segments,
            [
                ("x", None, None),
                ("m", Some("p1"), Some(1)),
                ("s", Some("p1"), Some(1)),
                ("t", Some("p1"), Some(1)),
                ("i", Some("p2"), Some(2)),
            ]
        );
        let event_pk = |database: &mut SqliteConnection, event_id: &str| {
            let pk = llm_usage_events::table
                .filter(llm_usage_events::event_id.eq(event_id))
                .select(llm_usage_events::id)
                .first::<String>(database)
                .expect("the event is stored");
            Uuid::parse_str(&pk).expect("a UUID")
        };
        for (event_id, turn) in [
            ("x-request", None),
            ("t-response", Some(1)),
            ("i-request", Some(2)),
        ] {
            let pk = event_pk(&mut database, event_id);
            let location = event_location(&mut database, OWNER_ID, pk, &cache).expect("located");
            assert_eq!(location.location.turn, turn, "{event_id}");
        }

        let rows = summary(&mut database, json!({"group_by": "agent"}));
        let turns = rows
            .as_array()
            .expect("rows")
            .iter()
            .map(|row| {
                (
                    row["agent_name"].clone(),
                    row["turns"].clone(),
                    row["sessions"].clone(),
                )
            })
            .collect::<Vec<_>>();
        assert!(
            turns.contains(&(json!("claude-code"), json!(2_i64), json!(1_i64))),
            "{turns:?}"
        );
        assert!(
            turns.contains(&(json!("codex"), json!(3_i64), json!(1_i64))),
            "{turns:?}"
        );
        let all = summary(&mut database, json!({"group_by": "day"}));
        assert_eq!(
            (all[0]["turns"].clone(), all[0]["sessions"].clone()),
            (json!(5_i64), json!(2_i64))
        );
    }

    #[test]
    fn segments_and_conversations_preview_stored_events() {
        let (mut database, session_pk) = streams_database();
        let query = SegmentQuery {
            entries: Some("0".to_owned()),
            text_preview: Some(3),
        }
        .validate()
        .expect("a valid query");
        let segment = session_segment(
            &mut database,
            OWNER_ID,
            session_pk,
            "r1",
            &query,
            &StreamCache::default(),
        )
        .expect("the whole segment");
        assert_eq!(segment.segment, "r1");
        let r2 = &segment.exchanges[1];
        assert_eq!(
            (r2.exchange.base.as_deref(), r2.model.as_deref()),
            (Some("r1"), Some("gpt-5"))
        );
        let request = r2.request.as_ref().expect("a request preview");
        assert_eq!(
            (
                request.entries_total,
                request.omitted_entries,
                request.chunks.len()
            ),
            (3_usize, 2_usize, 1_usize)
        );
        assert_eq!(
            request.chunks[0].entries[0],
            json!({"role": "user", "text": "fix", "text_length": 11_usize})
        );
        assert!(
            session_segment(
                &mut database,
                OWNER_ID,
                session_pk,
                "zz",
                &query,
                &StreamCache::default()
            )
            .is_err()
        );

        let window = Window::parse(&PreviewQuery {
            entries: Some("-1".to_owned()),
            text_preview: None,
        })
        .expect("a valid window");
        let conversation = event_conversation(&mut database, OWNER_ID, request.event_pk, &window)
            .expect("the event's conversation");
        assert_eq!(
            (
                conversation.chunks[0].from,
                conversation.chunks[0].entries[0]["role"].clone()
            ),
            (2, json!("tool_result"))
        );
    }

    #[test]
    fn the_session_header_sums_its_events() {
        let (mut database, session_pk) = streams_database();
        let header = session_header(&mut database, OWNER_ID, session_pk, &StreamCache::default())
            .expect("the header");
        assert_eq!(
            (
                header.event_count,
                header.exchange_count,
                header.turn_count,
                header.segment_count
            ),
            (4, 2, 1, 1)
        );
        assert_eq!(
            (header.agents[0].name.as_str(), header.models.as_slice()),
            ("codex", ["gpt-5".to_owned()].as_slice())
        );
        assert_eq!(
            header.end.signed_duration_since(header.start),
            chrono::TimeDelta::seconds(3)
        );
        assert_eq!(
            header.title.as_ref().map(|label| label.text.as_str()),
            Some("fix the bug"),
            "the title is the first request label"
        );
        assert!(
            session_header(
                &mut database,
                OWNER_ID,
                Uuid::nil(),
                &StreamCache::default()
            )
            .is_err()
        );
    }

    #[test]
    fn event_list_leaves_out_content_but_keeps_what_a_list_shows() {
        let mut database = summary_seeded();
        let page = |database: &mut SqliteConnection, query: serde_json::Value| {
            let query: RawEventsQuery =
                serde_json::from_value(query).expect("query should deserialize");
            raw_events(database, &query, OWNER_ID, 50).expect("events should load")
        };

        let list = page(&mut database, json!({"fields": "list", "limit": 2_i64}));
        let full = page(&mut database, json!({"limit": 2_i64}));

        assert_eq!(list.next_page_token.as_deref(), Some("2"));
        assert_eq!(list.events.len(), 2);
        for (listed, complete) in list.events.iter().zip(&full.events) {
            assert_eq!(listed.id, complete.id);
            assert_eq!(listed.total_tokens, complete.total_tokens);
            assert_eq!(listed.host_name.as_deref(), Some("alice-mbp"));
            assert_eq!(listed.metadata, json!({}));
            assert!(listed.attachments.is_empty());
            assert_eq!(complete.metadata["raw_body"], json!("large"));
        }
    }

    fn summary_seeded() -> SqliteConnection {
        let mut database = database();
        let event = |id: &str, agent: &str, session: &str, observed_at: &str| {
            let (provider, model) = if agent == "codex" {
                ("openai", "gpt-5")
            } else {
                ("anthropic", "claude-x")
            };
            json!({
                "event_id": id,
                "observed_at": observed_at,
                "device": {"host_name": "alice-mbp", "platform": "macos", "os_version": "15.1"},
                "agent": {"name": agent},
                "session_id": session,
                "llm": {"provider": provider, "model": model},
                "event_type": if id.ends_with("req") { "request" } else { "response" },
                "token_usage": if id.ends_with("req") {
                    json!({"input_tokens": 10_i64, "output_tokens": 0_i64, "cache_read_tokens": 4_i64, "cache_write_tokens": 0_i64, "reasoning_tokens": 0_i64, "total_tokens": 14_i64})
                } else {
                    json!({"input_tokens": 0_i64, "output_tokens": 5_i64, "cache_read_tokens": 0_i64, "cache_write_tokens": 0_i64, "reasoning_tokens": 0_i64, "total_tokens": 5_i64})
                },
                // A request asking in words after an answer opens a turn; a
                // response opens none.
                "metadata": if id.ends_with("req") {
                    json!({"raw_body": "large", "conversation": {
                        "entries": [
                            {"role": "assistant", "text": "earlier"},
                            {"role": "user", "text": "ask"}
                        ],
                        "completeness": "complete"
                    }})
                } else {
                    json!({"raw_body": "large"})
                }
            })
        };
        let request: IngestEventsRequest = serde_json::from_value(json!({
            "events": [
                event("a-req", "codex", "alpha", "2026-09-01T23:00:00Z"),
                event("a-res", "codex", "alpha", "2026-09-01T23:30:00Z"),
                event("b-req", "claude-desktop", "beta", "2026-09-02T01:00:00Z"),
                event("c-res", "claude-code", "gamma", "2026-09-02T02:00:00Z"),
                event("a2-req", "codex", "alpha", "2026-09-03T00:00:00Z"),
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
    fn summary_reads_events_only_from_covering_indexes() {
        #[derive(diesel::QueryableByName)]
        struct PlanStep {
            #[diesel(sql_type = diesel::sql_types::Text)]
            detail: String,
        }
        let mut database = summary_seeded();

        let plan = diesel::sql_query(format!(
            "EXPLAIN QUERY PLAN {}",
            super::SUMMARY_AGGREGATE_SQL
        ))
        .load::<PlanStep>(&mut database)
        .expect("summary should plan");

        // Each event row holds its conversation, so reading rows costs a page
        // per event; `e` and the turn check's `de` are both events.
        let event_reads = plan
            .iter()
            .map(|step| step.detail.as_str())
            .filter(|detail| {
                let mut words = detail.split_whitespace();
                matches!(words.next(), Some("SEARCH" | "SCAN"))
                    && matches!(words.next(), Some("e" | "de"))
            })
            .collect::<Vec<_>>();
        assert!(
            !event_reads.is_empty()
                && event_reads
                    .iter()
                    .all(|detail| detail.contains("USING COVERING INDEX")),
            "summary should not read event rows: {event_reads:?}"
        );
    }

    #[test]
    fn summary_groups_in_sql_by_reported_agent_and_distinct_counts() {
        let mut database = summary_seeded();

        let rows = summary(
            &mut database,
            json!({"group_by": "agent,provider,model", "to": "2026-09-03T00:00:00Z"}),
        );

        // Claude Desktop is its own Agent, not an alias of Claude Code.
        assert_eq!(
            rows,
            json!([
                {
                    "agent_name": "codex", "llm_provider": "openai", "llm_model": "gpt-5",
                    "sessions": 1_i64, "turns": 1_i64, "requests": 1_i64, "responses": 1_i64,
                    "input_tokens": 10_i64, "output_tokens": 5_i64, "cache_read_tokens": 4_i64,
                    "cache_write_tokens": 0_i64, "reasoning_tokens": 0_i64, "total_tokens": 19_i64
                },
                {
                    "agent_name": "claude-desktop", "llm_provider": "anthropic",
                    "llm_model": "claude-x",
                    "sessions": 1_i64, "turns": 1_i64, "requests": 1_i64, "responses": 0_i64,
                    "input_tokens": 10_i64, "output_tokens": 0_i64, "cache_read_tokens": 4_i64,
                    "cache_write_tokens": 0_i64, "reasoning_tokens": 0_i64, "total_tokens": 14_i64
                },
                {
                    "agent_name": "claude-code", "llm_provider": "anthropic",
                    "llm_model": "claude-x",
                    "sessions": 1_i64, "turns": 0_i64, "requests": 0_i64, "responses": 1_i64,
                    "input_tokens": 0_i64, "output_tokens": 5_i64, "cache_read_tokens": 0_i64,
                    "cache_write_tokens": 0_i64, "reasoning_tokens": 0_i64, "total_tokens": 5_i64
                }
            ])
        );
    }

    #[test]
    fn summary_filters_and_groups_by_utc_day_and_event_type() {
        let mut database = summary_seeded();

        let rows = summary(
            &mut database,
            json!({
                "group_by": "day,event_type",
                "agent_name": ["claude-code", "claude-desktop"],
                "limit": 10_i64,
            }),
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
            json!({"group_by": "event_type", "agent_name": ["codex"], "event_type": "request"}),
        );
        assert_eq!(codex_requests[0]["requests"], json!(2_i64));
        assert_eq!(codex_requests[0]["turns"], json!(2_i64));
        assert_eq!(codex_requests.as_array().map(Vec::len), Some(1));
        // Agent names match exactly: no alias expansion and no case folding.
        for agent in ["claude", "Codex"] {
            let rows = summary(
                &mut database,
                json!({"group_by": "agent", "agent_name": [agent]}),
            );
            assert_eq!(rows, json!([]), "{agent} must not match another Agent");
        }
        // Providers match exactly as reported, without case folding.
        let folded = summary(
            &mut database,
            json!({"group_by": "agent", "llm_provider": ["OpenAI"]}),
        );
        assert_eq!(
            folded,
            json!([]),
            "OpenAI must not match the reported openai"
        );
        // A list matches any of its values.
        let either = summary(
            &mut database,
            json!({
                "group_by": "agent",
                "llm_provider": ["openai", "anthropic"],
                "session_id": ["alpha", "beta"],
            }),
        );
        let mut agents = either
            .as_array()
            .expect("rows should be an array")
            .iter()
            .map(|row| (row["agent_name"].clone(), row["sessions"].clone()))
            .collect::<Vec<_>>();
        agents.sort_by_key(|agent| agent.0.to_string());
        assert_eq!(
            agents,
            vec![
                (json!("claude-desktop"), json!(1_i64)),
                (json!("codex"), json!(1_i64)),
            ]
        );
    }

    /// Token totals of each `day` row, in day order.
    fn day_totals(rows: &serde_json::Value) -> Vec<(serde_json::Value, serde_json::Value)> {
        let mut days = rows
            .as_array()
            .expect("rows should be an array")
            .iter()
            .map(|row| (row["day"].clone(), row["total_tokens"].clone()))
            .collect::<Vec<_>>();
        days.sort_by_key(|day| day.0.to_string());
        days
    }

    #[test]
    fn summary_groups_days_in_the_requested_time_zone() {
        let mut database = summary_seeded();

        let utc = summary(&mut database, json!({"group_by": "day"}));
        let shanghai = summary(
            &mut database,
            json!({"group_by": "day", "tz": "Asia/Shanghai"}),
        );

        assert_eq!(
            day_totals(&utc),
            vec![
                (json!("2026-09-01"), json!(19_i64)),
                (json!("2026-09-02"), json!(19_i64)),
                (json!("2026-09-03"), json!(14_i64)),
            ]
        );
        assert_eq!(
            day_totals(&shanghai),
            vec![
                (json!("2026-09-02"), json!(38_i64)),
                (json!("2026-09-03"), json!(14_i64)),
            ]
        );
        let query: SummaryQuery = serde_json::from_value(json!({"group_by": "day", "tz": "UTC+8"}))
            .expect("query should deserialize");
        assert!(usage_summary(&mut database, &query, OWNER_ID, 500).is_err());
    }

    #[test]
    fn summary_days_follow_daylight_saving_changes() {
        let mut database = database();
        let event = |id: &str, observed_at: &str| {
            json!({
                "event_id": id,
                "observed_at": observed_at,
                "device": {"host_name": "alice-mbp", "platform": "macos", "os_version": "15.1"},
                "agent": {"name": "Codex"},
                "session_id": "dst",
                "llm": {"provider": "OpenAI", "model": "gpt-5"},
                "event_type": "request",
                "token_usage": {"input_tokens": 1_i64, "total_tokens": 1_i64},
                "metadata": {"turn_id": id}
            })
        };
        // Los Angeles moves from PST (-8) to PDT (-7) at 2026-03-08T10:00Z.
        let request: IngestEventsRequest = serde_json::from_value(json!({
            "events": [
                event("before", "2026-03-08T07:30:00Z"), // 03-07 23:30 PST
                event("late", "2026-03-09T06:30:00Z"),   // 03-08 23:30 PDT
                event("after", "2026-03-09T07:30:00Z"),  // 03-09 00:30 PDT
            ]
        }))
        .expect("request should deserialize");
        ingest_events(&mut database, &request, OWNER_ID, 100).expect("ingest should succeed");

        let rows = summary(
            &mut database,
            json!({"group_by": "day", "tz": "America/Los_Angeles"}),
        );

        assert_eq!(
            day_totals(&rows),
            vec![
                (json!("2026-03-07"), json!(1_i64)),
                (json!("2026-03-08"), json!(1_i64)),
                (json!("2026-03-09"), json!(1_i64)),
            ]
        );
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

    /// One request observed `second` seconds into a fixed minute, working for
    /// `turn` when it names one.
    fn request_with_text(text: &str) -> IngestEventsRequest {
        serde_json::from_value(json!({
            "events": [{
                "event_id": "sqlite-event",
                "observed_at": "2026-08-27T00:00:00Z",
                "device": {"host_name": "local", "platform": "macos"},
                "agent": {"name": "codex"},
                "session_id": "sqlite-session",
                "llm": {"provider": "openai", "model": "gpt-5"},
                "event_type": "response",
                "token_usage": {"input_tokens": 0_i64, "output_tokens": 3_i64, "cache_read_tokens": 0_i64, "cache_write_tokens": 0_i64, "reasoning_tokens": 0_i64, "total_tokens": 3_i64},
                "metadata": {"conversation": {
                    "entries": [{"role": "assistant", "text": text}],
                    "completeness": "complete"
                }}
            }]
        }))
        .expect("test ingest request should deserialize")
    }
}
