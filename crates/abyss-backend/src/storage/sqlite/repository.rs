//! Diesel ORM implementation of SQLite usage ingest and relational queries.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use diesel::{
    ExpressionMethods, IntoSql, JoinOnDsl, QueryDsl, QueryableByName, RunQueryDsl,
    SelectableHelper, SqliteConnection,
    dsl::{count_star, max, sql},
    result::OptionalExtension,
    sql_query,
    sql_types::{BigInt, Binary, Bool, Nullable, Text},
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
            exact_values_filter, non_empty, normalize_limit, normalize_text, parse_group_by,
            sha256_bytes, validate_batch, validate_event_type,
        },
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
    let (turn_pk, turn_index) = place_in_turn(
        connection,
        user_id,
        session_pk,
        event.turn_id.as_deref(),
        event.response_id.as_deref(),
        observed_at,
        now,
    )?;
    let tokens = &event.token_usage;
    let text = event.text.clone();
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
            session_id: event.session_id.clone(),
            turn_index,
            llm_provider: provider,
            llm_model: model,
            event_type: event.event_type.as_str().to_owned(),
            text: text.clone(),
            text_sha256: text.as_deref().map(sha256_bytes),
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

    insert_attachments(connection, user_id, event_pk, attachments, now)?;
    insert_search_projection(connection, event_pk, user_id, session_pk, event)?;
    session_titles::capture(
        connection,
        &session_titles::CapturedEvent {
            user_id,
            session_pk,
            event_pk,
            event_type: event.event_type.as_str(),
            metadata: &event.metadata,
            naming_thread: event.naming_thread,
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
        })
        .execute(connection)?;
    Ok(id)
}

/// Places an event in a turn of its session and returns the turn's id and
/// position.
///
/// Explicit turn ids take priority; otherwise each response id gets its own
/// fallback turn. Events with neither id join the turn in progress.
/// Turns are numbered by when they started, so a turn that arrives late
/// moves the turns after it.
fn place_in_turn(
    connection: &mut SqliteConnection,
    user_id: Uuid,
    session_pk: Uuid,
    turn_id: Option<&str>,
    response_id: Option<&str>,
    observed_at: i64,
    now: i64,
) -> Result<(Uuid, i32), AppError> {
    let session = session_pk.to_string();
    let found = match (turn_id, response_id) {
        (Some(key), _) => named_turn(connection, &session, key)?,
        (None, Some(key)) => response_turn(connection, &session, key)?,
        (None, None) => turn_in_progress(connection, &session, observed_at)?,
    };
    let (turn_pk, turn_index, reordered) = if let Some(turn) = found {
        diesel::update(agent_turns::table.find(&turn.id))
            .set((
                agent_turns::started_at.eq(turn.started_at.min(observed_at)),
                agent_turns::ended_at
                    .eq(Some(turn.ended_at.unwrap_or(observed_at).max(observed_at))),
                agent_turns::updated_at.eq(now),
            ))
            .execute(connection)?;
        (turn.id, turn.turn_index, observed_at < turn.started_at)
    } else {
        let last = agent_turns::table
            .filter(agent_turns::session_pk.eq(&session))
            .select(max(agent_turns::turn_index))
            .first::<Option<i32>>(connection)?;
        let id = Uuid::now_v7().to_string();
        let turn_index = last.unwrap_or(0_i32).saturating_add(1_i32);
        diesel::insert_into(agent_turns::table)
            .values(NewTurn {
                id: id.clone(),
                user_id: user_id.to_string(),
                session_pk: session.clone(),
                turn_index,
                turn_key: turn_id.map(str::to_owned),
                started_at: observed_at,
                ended_at: Some(observed_at),
                created_at: now,
                updated_at: now,
            })
            .execute(connection)?;
        (id, turn_index, last.is_some())
    };
    let turn_index = if reordered && number_turns(connection, &session)? {
        agent_turns::table
            .find(&turn_pk)
            .select(agent_turns::turn_index)
            .first::<i32>(connection)?
    } else {
        turn_index
    };
    Ok((parse_uuid(&turn_pk, "turn id")?, turn_index))
}

/// The turn named `key` in a session. The first named turn adopts the turn
/// that events with neither turn nor response id opened at the start of the
/// session. A response fallback turn keeps its separate identity.
fn named_turn(
    connection: &mut SqliteConnection,
    session: &str,
    key: &str,
) -> Result<Option<TurnRow>, AppError> {
    let named = agent_turns::table
        .filter(agent_turns::session_pk.eq(session))
        .filter(agent_turns::turn_key.eq(key))
        .select(TurnRow::as_select())
        .first::<TurnRow>(connection)
        .optional()?;
    if named.is_some() {
        return Ok(named);
    }
    let mut turns = agent_turns::table
        .filter(agent_turns::session_pk.eq(session))
        .select(TurnRow::as_select())
        .limit(2_i64)
        .load::<TurnRow>(connection)?;
    let opening = match turns.as_slice() {
        [turn] if turn.turn_key.is_none() => turns.pop(),
        _ => None,
    };
    if let Some(turn) = &opening {
        let has_response = llm_usage_events::table
            .filter(llm_usage_events::turn_pk.eq(&turn.id))
            .filter(llm_usage_events::response_id.is_not_null())
            .select(llm_usage_events::id)
            .first::<String>(connection)
            .optional()?
            .is_some();
        if has_response {
            return Ok(None);
        }
        diesel::update(agent_turns::table.find(&turn.id))
            .set(agent_turns::turn_key.eq(key))
            .execute(connection)?;
    }
    Ok(opening)
}

/// Reuses the fallback turn for a model response within this session.
/// Explicit turn keys and response ids occupy separate identity domains.
fn response_turn(
    connection: &mut SqliteConnection,
    session: &str,
    response_id: &str,
) -> Result<Option<TurnRow>, AppError> {
    agent_turns::table
        .inner_join(llm_usage_events::table.on(llm_usage_events::turn_pk.eq(agent_turns::id)))
        .filter(llm_usage_events::session_pk.eq(session))
        .filter(llm_usage_events::response_id.eq(response_id))
        .filter(agent_turns::turn_key.is_null())
        .select(TurnRow::as_select())
        .first::<TurnRow>(connection)
        .optional()
        .map_err(AppError::from)
}

/// The turn in progress when an event naming no turn was observed: the latest
/// turn started by then, else the first turn of the session.
fn turn_in_progress(
    connection: &mut SqliteConnection,
    session: &str,
    observed_at: i64,
) -> Result<Option<TurnRow>, AppError> {
    let started = agent_turns::table
        .filter(agent_turns::session_pk.eq(session))
        .filter(agent_turns::started_at.le(observed_at))
        .order_by((agent_turns::started_at.desc(), agent_turns::id.desc()))
        .select(TurnRow::as_select())
        .first::<TurnRow>(connection)
        .optional()?;
    if started.is_some() {
        return Ok(started);
    }
    agent_turns::table
        .filter(agent_turns::session_pk.eq(session))
        .order_by((agent_turns::started_at.asc(), agent_turns::id.asc()))
        .select(TurnRow::as_select())
        .first::<TurnRow>(connection)
        .optional()
        .map_err(AppError::from)
}

/// Numbers the turns of a session, and their events, by when the turns
/// started. Writes nothing and returns `false` while the numbers already
/// follow that order.
fn number_turns(connection: &mut SqliteConnection, session: &str) -> Result<bool, AppError> {
    let turns = agent_turns::table
        .filter(agent_turns::session_pk.eq(session))
        .order_by((agent_turns::started_at.asc(), agent_turns::id.asc()))
        .select((agent_turns::id, agent_turns::turn_index))
        .load::<(String, i32)>(connection)?;
    let moved = turns
        .iter()
        .zip(1_i32..)
        .filter(|((_, index), position)| index != position)
        .collect::<Vec<_>>();
    if moved.is_empty() {
        return Ok(false);
    }
    // Moved turns first step above every current number, so no single-row
    // update collides with another turn's number or goes below one.
    let above = turns.iter().map(|(_, index)| *index).max().unwrap_or(0_i32);
    for ((id, _), position) in &moved {
        diesel::update(agent_turns::table.find(id))
            .set(agent_turns::turn_index.eq(above.saturating_add(*position)))
            .execute(connection)?;
    }
    for ((id, _), position) in moved {
        diesel::update(agent_turns::table.find(id))
            .set(agent_turns::turn_index.eq(position))
            .execute(connection)?;
        diesel::update(llm_usage_events::table.filter(llm_usage_events::turn_pk.eq(id)))
            .set(llm_usage_events::turn_index.eq(position))
            .execute(connection)?;
    }
    Ok(true)
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
    let projection = SearchProjection::from_source(event.text.clone(), &event.metadata);
    sql_query(
        "INSERT INTO usage_events_fts (
             event_pk, user_id, session_pk, session_id, content, tool_names,
             tool_content
         ) VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind::<Text, _>(event_pk.to_string())
    .bind::<Text, _>(user_id.to_string())
    .bind::<Text, _>(session_pk.to_string())
    .bind::<Text, _>(&event.session_id)
    .bind::<diesel::sql_types::Nullable<Text>, _>(projection.content)
    .bind::<Text, _>(projection.tool_names.join("\n"))
    .bind::<Text, _>(projection.tool_content.join("\n"))
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
            turn_index: query.turn_index,
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
    turn_index: Option<i32>,
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
    if let Some(turn_index) = filters.turn_index {
        query = query.filter(llm_usage_events::turn_index.eq(turn_index));
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
                llm_usage_events::turn_pk,
                llm_usage_events::event_id,
                llm_usage_events::agent_name,
                llm_usage_events::agent_version,
                llm_usage_events::session_id,
                llm_usage_events::turn_index,
                llm_usage_events::llm_provider,
                llm_usage_events::llm_model,
                llm_usage_events::event_type,
                None::<String>.into_sql::<Nullable<Text>>(),
                None::<Vec<u8>>.into_sql::<Nullable<Binary>>(),
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
    i64,
    i64,
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
            llm_usage_events::observed_at,
            sql::<BigInt>("coalesce(length(text), 0)"),
            sql::<Nullable<Text>>(&format!(
                "CASE WHEN event_type = 'request' THEN substr(text, -{OUTLINE_PREVIEW_CHARS}) \
                 ELSE substr(text, 1, {OUTLINE_PREVIEW_CHARS}) END"
            )),
            // SQLite text functions stop at the first NUL character, so the
            // length and preview above are wrong for text containing NUL.
            sql::<Bool>("coalesce(instr(CAST(text AS BLOB), x'00') > 0, 0)"),
        ))
        .load::<OutlineRow>(connection)?;
    let nul_text_ids = rows
        .iter()
        .filter(|row| row.16)
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
                (row.14, row.15) = text_outline(text, &row.6)?;
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
                observed_at: timestamp_from_micros(row.13)?,
                text_chars: row.14,
                preview: row.15,
                attachment_count,
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
            agent_name: session.agent_name,
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
/// unconditional so the owner-time index bounds the scan; `?10`..`?17` flags
/// and JSON value lists for the Agent, session, provider, and model filters,
/// each matching any listed value; `?18`/`?19` event-type filter; `?20` row
/// limit.
const SUMMARY_AGGREGATE_SQL: &str = "\
    SELECT \
        CASE WHEN ?1 THEN strftime('%Y-%m-%d', e.observed_at / 1000000, 'unixepoch') END AS day, \
        CASE WHEN ?2 THEN d.host_name END AS host_name, \
        CASE WHEN ?2 THEN d.platform END AS platform, \
        CASE WHEN ?2 THEN d.os_version END AS os_version, \
        CASE WHEN ?3 THEN e.agent_name END AS agent_name, \
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
        AND (?12 = 0 OR e.session_id IN (SELECT value FROM json_each(?13))) \
        AND (?14 = 0 OR e.llm_provider IN (SELECT value FROM json_each(?15))) \
        AND (?16 = 0 OR e.llm_model IN (SELECT value FROM json_each(?17))) \
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
        agent_name: event.agent_name,
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
    use diesel::{
        Connection, ExpressionMethods, QueryDsl, RunQueryDsl, SqliteConnection, dsl::count_star,
        sql_query,
    };
    use serde_json::json;
    use uuid::Uuid;

    use crate::{
        search::SessionSearchQuery,
        usage::{IngestEventsRequest, RawEventsQuery, SummaryQuery},
    };

    use super::{
        super::{
            migrations,
            schema::{agent_turns, llm_usage_events},
            search,
        },
        ingest_events, raw_events, session_outline, usage_summary,
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
            "FTS result should contain a structured highlight"
        );
    }

    #[test]
    fn validation_failure_leaves_relational_and_fts_tables_empty() {
        let mut database = database();
        let request = request_with_text("invalid event");

        ingest_events(&mut database, &request, OWNER_ID, 0)
            .expect_err("a batch over the size limit should be rejected");
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
        let mut empty = request_with_text("");
        empty.events[0].event_id = "sqlite-empty-text".to_owned();
        ingest_events(&mut database, &empty, OWNER_ID, 100).expect("ingest should succeed");

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
        assert_eq!(stored.text.as_deref(), Some("  hello \n"));
        let empty = events
            .iter()
            .find(|event| event.event_id == "sqlite-empty-text")
            .expect("the empty-text event should be stored");
        assert_eq!(empty.text.as_deref(), Some(""));
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
            assert_eq!(listed.text, None);
            assert_eq!(listed.metadata, json!({}));
            assert!(listed.attachments.is_empty());
            assert_eq!(complete.text.as_deref(), Some("body"));
            assert_eq!(complete.metadata["raw_body"], json!("large"));
        }
    }

    fn summary_seeded() -> SqliteConnection {
        let mut database = database();
        let event = |id: &str, agent: &str, session: &str, turn: i32, observed_at: &str| {
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
                "turn_id": format!("{session}-{turn}"),
                "llm": {"provider": provider, "model": model},
                "event_type": if id.ends_with("req") { "request" } else { "response" },
                "text": "body",
                "token_usage": if id.ends_with("req") {
                    json!({"input_tokens": 10_i64, "output_tokens": 0_i64, "cache_read_tokens": 4_i64, "cache_write_tokens": 0_i64, "reasoning_tokens": 0_i64, "total_tokens": 14_i64})
                } else {
                    json!({"input_tokens": 0_i64, "output_tokens": 5_i64, "cache_read_tokens": 0_i64, "cache_write_tokens": 0_i64, "reasoning_tokens": 0_i64, "total_tokens": 5_i64})
                },
                "metadata": {"raw_body": "large"}
            })
        };
        let request: IngestEventsRequest = serde_json::from_value(json!({
            "events": [
                event("a-req", "codex", "alpha", 1_i32, "2026-09-01T23:00:00Z"),
                event("a-res", "codex", "alpha", 1_i32, "2026-09-01T23:30:00Z"),
                event("b-req", "claude-desktop", "beta", 1_i32, "2026-09-02T01:00:00Z"),
                event("c-res", "claude-code", "gamma", 1_i32, "2026-09-02T02:00:00Z"),
                event("a2-req", "codex", "alpha", 2_i32, "2026-09-03T00:00:00Z"),
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
                    "sessions": 1_i64, "turns": 1_i64, "requests": 0_i64, "responses": 1_i64,
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
    fn turn_event(id: &str, agent: &str, second: u32, turn: Option<&str>) -> serde_json::Value {
        json!({
            "event_id": id,
            "observed_at": format!("2026-10-08T00:00:{second:02}Z"),
            "device": {"host_name": "local", "platform": "macos"},
            "agent": {"name": agent},
            "session_id": "thread-session",
            "turn_id": turn,
            "llm": {"provider": "Anthropic", "model": "claude-x"},
            "event_type": "request",
            "token_usage": {"input_tokens": 1_i64, "output_tokens": 0_i64, "cache_read_tokens": 0_i64, "cache_write_tokens": 0_i64, "reasoning_tokens": 0_i64, "total_tokens": 1_i64},
            "metadata": {}
        })
    }

    fn ingest_turn_events(database: &mut SqliteConnection, events: Vec<serde_json::Value>) {
        let request: IngestEventsRequest =
            serde_json::from_value(json!({"events": serde_json::Value::Array(events)}))
                .expect("request should deserialize");
        ingest_events(database, &request, OWNER_ID, 100).expect("ingest should succeed");
    }

    fn turn_of(database: &mut SqliteConnection, event_id: &str) -> i32 {
        llm_usage_events::table
            .filter(llm_usage_events::event_id.eq(event_id))
            .select(llm_usage_events::turn_index)
            .first::<i32>(database)
            .expect("event should be stored")
    }

    /// Stored turns in number order: key, number, start and end second.
    fn turns(database: &mut SqliteConnection) -> Vec<(Option<String>, i32, i64, Option<i64>)> {
        agent_turns::table
            .order_by(agent_turns::turn_index.asc())
            .select((
                agent_turns::turn_key,
                agent_turns::turn_index,
                agent_turns::started_at,
                agent_turns::ended_at,
            ))
            .load::<(Option<String>, i32, i64, Option<i64>)>(database)
            .expect("turns should load")
    }

    fn second(value: i64) -> i64 {
        super::timestamp_to_micros(
            chrono::DateTime::parse_from_rfc3339(&format!("2026-10-08T00:00:{value:02}Z"))
                .expect("timestamp should parse")
                .into(),
        )
    }

    #[test]
    fn events_naming_a_turn_share_it_and_others_join_the_turn_in_progress() {
        let mut database = database();
        ingest_turn_events(
            &mut database,
            vec![
                turn_event("main-1", "codex", 10, Some("p1")),
                turn_event("subagent-1", "codex", 12, Some("p1")),
                turn_event("recap", "codex", 20, None),
                turn_event("main-2", "codex", 30, Some("p2")),
                turn_event("subagent-2", "codex", 40, Some("p2")),
            ],
        );
        // Delivered late: a request naming no turn observed during turn 2, and
        // a background agent of turn 1 still working during turn 2.
        ingest_turn_events(
            &mut database,
            vec![
                turn_event("late-title", "codex", 32, None),
                turn_event("late-subagent", "codex", 45, Some("p1")),
            ],
        );

        for (event, turn) in [
            ("main-1", 1_i32),
            ("subagent-1", 1_i32),
            ("recap", 1_i32),
            ("late-subagent", 1_i32),
            ("main-2", 2_i32),
            ("subagent-2", 2_i32),
            ("late-title", 2_i32),
        ] {
            assert_eq!(turn_of(&mut database, event), turn, "{event}");
        }
        assert_eq!(
            turns(&mut database),
            vec![
                (Some("p1".to_owned()), 1_i32, second(10), Some(second(45))),
                (Some("p2".to_owned()), 2_i32, second(30), Some(second(40))),
            ]
        );
    }

    #[test]
    fn the_first_named_turn_adopts_the_turn_opened_without_one() {
        let mut database = database();
        ingest_turn_events(
            &mut database,
            vec![turn_event("probe", "claude-code", 5, None)],
        );
        ingest_turn_events(
            &mut database,
            vec![
                turn_event("main-1", "claude-code", 10, Some("p1")),
                turn_event("main-2", "claude-code", 30, Some("p2")),
                // Earlier than every turn: joins the first one.
                turn_event("early", "claude-code", 1, None),
            ],
        );

        assert_eq!(turn_of(&mut database, "probe"), 1_i32);
        assert_eq!(turn_of(&mut database, "main-1"), 1_i32);
        assert_eq!(turn_of(&mut database, "early"), 1_i32);
        assert_eq!(turn_of(&mut database, "main-2"), 2_i32);
        assert_eq!(
            turns(&mut database),
            vec![
                (Some("p1".to_owned()), 1_i32, second(1), Some(second(10))),
                (Some("p2".to_owned()), 2_i32, second(30), Some(second(30))),
            ]
        );
    }

    #[test]
    fn turns_are_numbered_by_start_time_whatever_order_they_arrive_in() {
        let mut database = database();
        ingest_turn_events(
            &mut database,
            vec![
                turn_event("b", "codex", 30, Some("b")),
                turn_event("b-next", "codex", 31, Some("b")),
                turn_event("c", "codex", 50, Some("c")),
            ],
        );
        assert_eq!(turn_of(&mut database, "c"), 2_i32);

        // A turn delivered late started first.
        ingest_turn_events(&mut database, vec![turn_event("a", "codex", 10, Some("a"))]);
        for (event, turn) in [("a", 1_i32), ("b", 2_i32), ("b-next", 2_i32), ("c", 3_i32)] {
            assert_eq!(turn_of(&mut database, event), turn, "{event}");
        }

        // A late event of a known turn moves its start before every other turn.
        ingest_turn_events(
            &mut database,
            vec![turn_event("c-early", "codex", 5, Some("c"))],
        );
        for (event, turn) in [
            ("c-early", 1_i32),
            ("c", 1_i32),
            ("a", 2_i32),
            ("b", 3_i32),
            ("b-next", 3_i32),
        ] {
            assert_eq!(turn_of(&mut database, event), turn, "{event}");
        }
        assert_eq!(
            turns(&mut database)
                .into_iter()
                .map(|(key, index, ..)| (key, index))
                .collect::<Vec<_>>(),
            vec![
                (Some("c".to_owned()), 1_i32),
                (Some("a".to_owned()), 2_i32),
                (Some("b".to_owned()), 3_i32),
            ]
        );
    }

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
                "text": text,
                "token_usage": {"input_tokens": 0_i64, "output_tokens": 3_i64, "cache_read_tokens": 0_i64, "cache_write_tokens": 0_i64, "reasoning_tokens": 0_i64, "total_tokens": 3_i64},
                "metadata": {}
            }]
        }))
        .expect("test ingest request should deserialize")
    }
}
