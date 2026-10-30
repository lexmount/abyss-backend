CREATE TABLE app_users (
    id TEXT PRIMARY KEY,
    email TEXT NOT NULL,
    name TEXT,
    created_at INTEGER NOT NULL
) STRICT;

INSERT INTO app_users (id, email, name, created_at)
VALUES (
    '00000000-0000-0000-0000-000000000001',
    'owner@abyss.local',
    'Abyss Owner',
    CAST(unixepoch('subsec') * 1000000 AS INTEGER)
);

CREATE TABLE devices (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    host_name TEXT NOT NULL,
    platform TEXT NOT NULL,
    os_version TEXT,
    first_seen_at INTEGER NOT NULL,
    last_seen_at INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE (user_id, host_name, platform)
) STRICT;

CREATE INDEX devices_user_seen_idx
    ON devices (user_id, last_seen_at DESC);

CREATE TABLE agent_sessions (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    device_context_id TEXT NOT NULL REFERENCES devices(id),
    agent_name TEXT NOT NULL,
    agent_version TEXT,
    session_id TEXT NOT NULL,
    started_at INTEGER NOT NULL,
    ended_at INTEGER,
    metadata TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(metadata)),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    -- Advances on event ingestion so derived stream caches share one revision.
    structure_revision INTEGER NOT NULL DEFAULT 0,
    UNIQUE (user_id, agent_name, session_id)
) STRICT;

CREATE INDEX agent_sessions_user_time_idx
    ON agent_sessions (user_id, started_at DESC);

CREATE TABLE llm_usage_events (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    device_context_id TEXT NOT NULL REFERENCES devices(id),
    session_pk TEXT NOT NULL REFERENCES agent_sessions(id) ON DELETE CASCADE,
    event_id TEXT NOT NULL UNIQUE,
    agent_name TEXT NOT NULL,
    agent_version TEXT,
    session_id TEXT NOT NULL,
    llm_provider TEXT NOT NULL,
    llm_model TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN ('request', 'response')),
    input_tokens INTEGER NOT NULL DEFAULT 0 CHECK (input_tokens >= 0),
    output_tokens INTEGER NOT NULL DEFAULT 0 CHECK (output_tokens >= 0),
    cache_read_tokens INTEGER NOT NULL DEFAULT 0 CHECK (cache_read_tokens >= 0),
    cache_write_tokens INTEGER NOT NULL DEFAULT 0 CHECK (cache_write_tokens >= 0),
    reasoning_tokens INTEGER NOT NULL DEFAULT 0 CHECK (reasoning_tokens >= 0),
    total_tokens INTEGER NOT NULL DEFAULT 0 CHECK (total_tokens >= 0),
    observed_at INTEGER NOT NULL,
    -- Agent-declared thread of the exchange; equal values continue one conversation.
    stream_id TEXT,
    -- Provider identity of the response the exchange produced.
    response_id TEXT,
    -- Response whose conversation the exchange's request continues.
    previous_response_id TEXT,
    metadata TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(metadata)),
    created_at INTEGER NOT NULL
) STRICT;

CREATE INDEX llm_usage_events_user_time_idx
    ON llm_usage_events (user_id, observed_at DESC, id DESC);

CREATE INDEX llm_usage_events_session_order_idx
    ON llm_usage_events (session_pk, observed_at, event_type, id);

CREATE INDEX llm_usage_events_agent_model_time_idx
    ON llm_usage_events (agent_name, llm_provider, llm_model, observed_at DESC);

CREATE TABLE llm_usage_event_attachments (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    event_pk TEXT NOT NULL REFERENCES llm_usage_events(id) ON DELETE CASCADE,
    position INTEGER NOT NULL CHECK (position >= 0),
    media_type TEXT NOT NULL CHECK (
        media_type IN ('image/png', 'image/jpeg', 'image/webp', 'image/gif')
    ),
    byte_size INTEGER NOT NULL CHECK (byte_size > 0 AND byte_size <= 8388608),
    sha256 BLOB NOT NULL CHECK (length(sha256) = 32),
    content BLOB,
    created_at INTEGER NOT NULL,
    UNIQUE (event_pk, position),
    CHECK (content IS NULL OR length(content) = byte_size)
) STRICT;

CREATE INDEX llm_usage_event_attachments_user_event_idx
    ON llm_usage_event_attachments (user_id, event_pk, position);

CREATE TABLE agent_diagnostic_captures (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    device_context_id TEXT NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
    session_pk TEXT NOT NULL REFERENCES agent_sessions(id) ON DELETE CASCADE,
    capture_id TEXT NOT NULL,
    flow_id TEXT NOT NULL,
    captured_at INTEGER NOT NULL,
    collector_version TEXT NOT NULL,
    payload TEXT NOT NULL CHECK (json_valid(payload)),
    created_at INTEGER NOT NULL,
    UNIQUE (user_id, capture_id)
) STRICT;

CREATE INDEX agent_diagnostic_captures_session_time_idx
    ON agent_diagnostic_captures (session_pk, captured_at, id);

CREATE INDEX agent_diagnostic_captures_flow_idx
    ON agent_diagnostic_captures (user_id, flow_id, captured_at);

CREATE TABLE agent_diagnostic_capture_events (
    capture_pk TEXT NOT NULL REFERENCES agent_diagnostic_captures(id) ON DELETE CASCADE,
    event_pk TEXT NOT NULL REFERENCES llm_usage_events(id) ON DELETE CASCADE,
    PRIMARY KEY (capture_pk, event_pk)
) STRICT;

CREATE INDEX agent_diagnostic_capture_events_event_idx
    ON agent_diagnostic_capture_events (event_pk);

-- The earliest request is the title anchor, including requests without a label.
-- API titles read this event's stream facts rather than the title column.
-- purpose and parent_session_pk are separate session metadata.
CREATE TABLE session_titles (
    session_pk TEXT PRIMARY KEY REFERENCES agent_sessions(id) ON DELETE CASCADE,
    user_id TEXT NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    title TEXT,
    title_event_pk TEXT REFERENCES llm_usage_events(id) ON DELETE SET NULL,
    title_observed_at INTEGER,
    purpose TEXT,
    parent_session_pk TEXT REFERENCES agent_sessions(id) ON DELETE SET NULL,
    updated_at INTEGER NOT NULL
) STRICT;

-- Event rows hold each event's conversation, tens of kilobytes, so a leaf
-- page holds about one row and reading any column from the row reads a page
-- per event. These indexes carry every column the usage summary and the
-- session list read, letting both answer from the index alone; the summary
-- joins stream facts by id.
CREATE INDEX llm_usage_events_summary_cover_idx
    ON llm_usage_events (
        user_id, observed_at, agent_name, llm_provider, llm_model, event_type,
        device_context_id, session_pk, session_id,
        input_tokens, output_tokens, cache_read_tokens, cache_write_tokens,
        reasoning_tokens, total_tokens, id
    );
CREATE INDEX llm_usage_events_session_activity_idx
    ON llm_usage_events (session_pk, observed_at, total_tokens);
