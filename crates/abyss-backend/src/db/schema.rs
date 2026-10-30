//! Diesel schema for the standalone PostgreSQL event store.

diesel::table! {
    use diesel::sql_types::{Nullable, Text, Timestamptz, Uuid};

    app_users (id) {
        id -> Uuid,
        email -> Text,
        name -> Nullable<Text>,
        created_at -> Timestamptz,
    }
}

diesel::table! {
    use diesel::sql_types::{Nullable, Text, Timestamptz, Uuid};

    devices (id) {
        id -> Uuid,
        user_id -> Uuid,
        host_name -> Text,
        platform -> Text,
        os_version -> Nullable<Text>,
        first_seen_at -> Timestamptz,
        last_seen_at -> Timestamptz,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}

diesel::table! {
    use diesel::sql_types::{BigInt, Jsonb, Nullable, Text, Timestamptz, Uuid};

    agent_sessions (id) {
        id -> Uuid,
        user_id -> Uuid,
        device_context_id -> Uuid,
        agent_name -> Text,
        agent_version -> Nullable<Text>,
        session_id -> Text,
        started_at -> Timestamptz,
        ended_at -> Nullable<Timestamptz>,
        metadata -> Jsonb,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
        structure_revision -> BigInt,
    }
}

diesel::table! {
    use diesel::sql_types::{BigInt, Bytea, Integer, Jsonb, Nullable, Text, Timestamptz, Uuid};

    llm_usage_events (id) {
        id -> Uuid,
        user_id -> Uuid,
        device_context_id -> Uuid,
        session_pk -> Uuid,
        event_id -> Text,
        agent_name -> Text,
        agent_version -> Nullable<Text>,
        session_id -> Text,
        llm_provider -> Text,
        llm_model -> Text,
        event_type -> Text,
        text -> Nullable<Text>,
        text_sha256 -> Nullable<Bytea>,
        input_tokens -> BigInt,
        output_tokens -> BigInt,
        cache_read_tokens -> BigInt,
        cache_write_tokens -> BigInt,
        reasoning_tokens -> BigInt,
        total_tokens -> BigInt,
        observed_at -> Timestamptz,
        metadata -> Jsonb,
        created_at -> Timestamptz,
        stream_id -> Nullable<Text>,
        response_id -> Nullable<Text>,
        previous_response_id -> Nullable<Text>,
        conversation -> Nullable<Jsonb>,
    }
}

diesel::table! {
    use diesel::sql_types::{Bool, Integer, Jsonb, Nullable, Text, Uuid};

    llm_usage_event_stream_facts (event_pk) {
        event_pk -> Uuid,
        new_from -> Nullable<Integer>,
        starts_turn -> Nullable<Bool>,
        last_identity -> Nullable<Text>,
        trailing -> Nullable<Jsonb>,
        result_ids -> Nullable<Jsonb>,
        emitted_ids -> Nullable<Jsonb>,
        output_skeleton -> Nullable<Jsonb>,
        label_index -> Nullable<Integer>,
        label_text -> Nullable<Text>,
        label_length -> Nullable<Integer>,
        subagent_id -> Nullable<Text>,
        parent_subagent_id -> Nullable<Text>,
        turn_id -> Nullable<Text>,
        continues_context -> Nullable<Bool>,
    }
}

diesel::table! {
    use diesel::sql_types::{BigInt, Bytea, Integer, Nullable, Text, Timestamptz, Uuid};

    llm_usage_event_attachments (id) {
        id -> Uuid,
        user_id -> Uuid,
        event_pk -> Uuid,
        position -> Integer,
        media_type -> Text,
        byte_size -> BigInt,
        sha256 -> Bytea,
        content -> Nullable<Bytea>,
        created_at -> Timestamptz,
    }
}

diesel::table! {
    use diesel::sql_types::{Jsonb, Text, Timestamptz, Uuid};

    agent_diagnostic_captures (id) {
        id -> Uuid,
        user_id -> Uuid,
        device_context_id -> Uuid,
        session_pk -> Uuid,
        capture_id -> Text,
        flow_id -> Text,
        captured_at -> Timestamptz,
        collector_version -> Text,
        payload -> Jsonb,
        created_at -> Timestamptz,
    }
}

diesel::table! {
    use diesel::sql_types::Uuid;

    agent_diagnostic_capture_events (capture_pk, event_pk) {
        capture_pk -> Uuid,
        event_pk -> Uuid,
    }
}

diesel::table! {
    use diesel::sql_types::{BigInt, Integer, Nullable, Text, Timestamptz, Uuid};

    search_outbox (id) {
        id -> BigInt,
        event_pk -> Uuid,
        user_id -> Uuid,
        operation -> Text,
        attempt_count -> Integer,
        available_at -> Timestamptz,
        claimed_at -> Nullable<Timestamptz>,
        claimed_by -> Nullable<Text>,
        processed_at -> Nullable<Timestamptz>,
        dead_lettered_at -> Nullable<Timestamptz>,
        last_error -> Nullable<Text>,
        created_at -> Timestamptz,
    }
}

diesel::table! {
    use diesel::sql_types::{Nullable, Text, Timestamptz, Uuid};

    session_titles (session_pk) {
        session_pk -> Uuid,
        user_id -> Uuid,
        title -> Nullable<Text>,
        title_event_pk -> Nullable<Uuid>,
        title_observed_at -> Nullable<Timestamptz>,
        purpose -> Nullable<Text>,
        updated_at -> Timestamptz,
        parent_session_pk -> Nullable<Uuid>,
    }
}

diesel::joinable!(agent_sessions -> app_users (user_id));
diesel::joinable!(agent_sessions -> devices (device_context_id));
diesel::joinable!(devices -> app_users (user_id));
diesel::joinable!(llm_usage_events -> agent_sessions (session_pk));
diesel::joinable!(llm_usage_events -> app_users (user_id));
diesel::joinable!(llm_usage_events -> devices (device_context_id));
diesel::joinable!(llm_usage_event_attachments -> app_users (user_id));
diesel::joinable!(llm_usage_event_attachments -> llm_usage_events (event_pk));
diesel::joinable!(llm_usage_event_stream_facts -> llm_usage_events (event_pk));
diesel::joinable!(agent_diagnostic_capture_events -> agent_diagnostic_captures (capture_pk));
diesel::joinable!(agent_diagnostic_capture_events -> llm_usage_events (event_pk));
diesel::joinable!(agent_diagnostic_captures -> agent_sessions (session_pk));
diesel::joinable!(agent_diagnostic_captures -> app_users (user_id));
diesel::joinable!(agent_diagnostic_captures -> devices (device_context_id));
diesel::joinable!(session_titles -> agent_sessions (session_pk));

diesel::allow_tables_to_appear_in_same_query!(
    agent_diagnostic_capture_events,
    agent_diagnostic_captures,
    agent_sessions,
    app_users,
    devices,
    llm_usage_events,
    llm_usage_event_attachments,
    llm_usage_event_stream_facts,
    search_outbox,
    session_titles,
);
