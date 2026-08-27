//! Diesel schema for SQLite relational tables; the FTS5 virtual table stays raw SQL.

diesel::table! {
    use diesel::sql_types::{BigInt, Nullable, Text};

    app_users (id) {
        id -> Text,
        email -> Text,
        name -> Nullable<Text>,
        created_at -> BigInt,
    }
}

diesel::table! {
    use diesel::sql_types::{BigInt, Nullable, Text};

    devices (id) {
        id -> Text,
        user_id -> Text,
        host_name -> Text,
        platform -> Text,
        os_version -> Nullable<Text>,
        first_seen_at -> BigInt,
        last_seen_at -> BigInt,
        created_at -> BigInt,
        updated_at -> BigInt,
    }
}

diesel::table! {
    use diesel::sql_types::{BigInt, Nullable, Text};

    agent_sessions (id) {
        id -> Text,
        user_id -> Text,
        device_context_id -> Text,
        agent_name -> Text,
        agent_version -> Nullable<Text>,
        session_id -> Text,
        started_at -> BigInt,
        ended_at -> Nullable<BigInt>,
        metadata -> Text,
        created_at -> BigInt,
        updated_at -> BigInt,
    }
}

diesel::table! {
    use diesel::sql_types::{BigInt, Integer, Nullable, Text};

    agent_turns (id) {
        id -> Text,
        user_id -> Text,
        session_pk -> Text,
        turn_index -> Integer,
        started_at -> BigInt,
        ended_at -> Nullable<BigInt>,
        created_at -> BigInt,
        updated_at -> BigInt,
    }
}

diesel::table! {
    use diesel::sql_types::{BigInt, Binary, Integer, Nullable, Text};

    llm_usage_events (id) {
        id -> Text,
        user_id -> Text,
        device_context_id -> Text,
        session_pk -> Text,
        turn_pk -> Text,
        event_id -> Text,
        agent_name -> Text,
        agent_version -> Nullable<Text>,
        session_id -> Text,
        turn_index -> Integer,
        llm_provider -> Text,
        llm_model -> Text,
        event_type -> Text,
        text -> Nullable<Text>,
        text_sha256 -> Nullable<Binary>,
        input_tokens -> BigInt,
        output_tokens -> BigInt,
        cache_read_tokens -> BigInt,
        cache_write_tokens -> BigInt,
        reasoning_tokens -> BigInt,
        total_tokens -> BigInt,
        observed_at -> BigInt,
        metadata -> Text,
        created_at -> BigInt,
    }
}

diesel::table! {
    use diesel::sql_types::{BigInt, Binary, Integer, Nullable, Text};

    llm_usage_event_attachments (id) {
        id -> Text,
        user_id -> Text,
        event_pk -> Text,
        position -> Integer,
        media_type -> Text,
        byte_size -> BigInt,
        sha256 -> Binary,
        content -> Nullable<Binary>,
        created_at -> BigInt,
    }
}

diesel::table! {
    use diesel::sql_types::{BigInt, Text};

    agent_diagnostic_captures (id) {
        id -> Text,
        user_id -> Text,
        device_context_id -> Text,
        session_pk -> Text,
        capture_id -> Text,
        flow_id -> Text,
        captured_at -> BigInt,
        collector_version -> Text,
        payload -> Text,
        created_at -> BigInt,
    }
}

diesel::table! {
    use diesel::sql_types::Text;

    agent_diagnostic_capture_events (capture_pk, event_pk) {
        capture_pk -> Text,
        event_pk -> Text,
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
);
