//! Diesel schema for SQLite relational tables; the FTS5 trigram index is a
//! virtual table outside it.

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
        structure_revision -> BigInt,
    }
}

diesel::table! {
    use diesel::sql_types::{BigInt, Binary, Integer, Nullable, Text};

    llm_usage_events (id) {
        id -> Text,
        user_id -> Text,
        device_context_id -> Text,
        session_pk -> Text,
        event_id -> Text,
        agent_name -> Text,
        agent_version -> Nullable<Text>,
        session_id -> Text,
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
        stream_id -> Nullable<Text>,
        response_id -> Nullable<Text>,
        previous_response_id -> Nullable<Text>,
    }
}

diesel::table! {
    use diesel::sql_types::{Bool, Integer, Nullable, Text};

    llm_usage_event_stream_facts (event_pk) {
        event_pk -> Text,
        new_from -> Nullable<Integer>,
        starts_turn -> Nullable<Bool>,
        last_identity -> Nullable<Text>,
        trailing -> Nullable<Text>,
        result_ids -> Nullable<Text>,
        emitted_ids -> Nullable<Text>,
        output_skeleton -> Nullable<Text>,
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

diesel::table! {
    use diesel::sql_types::{BigInt, Nullable, Text};

    session_titles (session_pk) {
        session_pk -> Text,
        user_id -> Text,
        title -> Nullable<Text>,
        title_event_pk -> Nullable<Text>,
        title_observed_at -> Nullable<BigInt>,
        purpose -> Nullable<Text>,
        updated_at -> BigInt,
        parent_session_pk -> Nullable<Text>,
    }
}

diesel::table! {
    use diesel::sql_types::{BigInt, Binary, Text};

    search_texts (id) {
        id -> BigInt,
        user_id -> Text,
        sha256 -> Binary,
        text -> Text,
    }
}

diesel::table! {
    use diesel::sql_types::{BigInt, Integer, Text};

    search_refs (event_pk, entry_index, field) {
        event_pk -> Text,
        entry_index -> Integer,
        field -> Text,
        text_id -> BigInt,
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
    search_refs,
    search_texts,
    session_titles,
);
