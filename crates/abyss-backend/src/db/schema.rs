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
    use diesel::sql_types::{Jsonb, Nullable, Text, Timestamptz, Uuid};

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
    }
}

diesel::table! {
    use diesel::sql_types::{Integer, Nullable, Timestamptz, Uuid};

    agent_turns (id) {
        id -> Uuid,
        user_id -> Uuid,
        session_pk -> Uuid,
        turn_index -> Integer,
        started_at -> Timestamptz,
        ended_at -> Nullable<Timestamptz>,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}

diesel::table! {
    use diesel::sql_types::{BigInt, Bytea, Integer, Jsonb, Nullable, Text, Timestamptz, Uuid};

    llm_usage_events (id) {
        id -> Uuid,
        user_id -> Uuid,
        device_context_id -> Uuid,
        session_pk -> Uuid,
        turn_pk -> Uuid,
        event_id -> Text,
        agent_name -> Text,
        agent_version -> Nullable<Text>,
        session_id -> Text,
        turn_index -> Integer,
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

diesel::joinable!(agent_sessions -> app_users (user_id));
diesel::joinable!(agent_sessions -> devices (device_context_id));
diesel::joinable!(agent_turns -> agent_sessions (session_pk));
diesel::joinable!(agent_turns -> app_users (user_id));
diesel::joinable!(devices -> app_users (user_id));
diesel::joinable!(llm_usage_events -> agent_sessions (session_pk));
diesel::joinable!(llm_usage_events -> agent_turns (turn_pk));
diesel::joinable!(llm_usage_events -> app_users (user_id));
diesel::joinable!(llm_usage_events -> devices (device_context_id));
diesel::joinable!(llm_usage_event_attachments -> app_users (user_id));
diesel::joinable!(llm_usage_event_attachments -> llm_usage_events (event_pk));
diesel::joinable!(agent_diagnostic_capture_events -> agent_diagnostic_captures (capture_pk));
diesel::joinable!(agent_diagnostic_capture_events -> llm_usage_events (event_pk));
diesel::joinable!(agent_diagnostic_captures -> agent_sessions (session_pk));
diesel::joinable!(agent_diagnostic_captures -> app_users (user_id));
diesel::joinable!(agent_diagnostic_captures -> devices (device_context_id));

diesel::allow_tables_to_appear_in_same_query!(
    agent_diagnostic_capture_events,
    agent_diagnostic_captures,
    agent_sessions,
    agent_turns,
    app_users,
    devices,
    llm_usage_events,
    llm_usage_event_attachments,
    search_outbox,
);
