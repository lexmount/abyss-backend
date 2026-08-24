CREATE TABLE app_users (
    id uuid PRIMARY KEY,
    email text NOT NULL,
    name text,
    created_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO app_users (id, email, name)
VALUES (
    '00000000-0000-0000-0000-000000000001',
    'owner@abyss.local',
    'Abyss Owner'
);

CREATE TABLE devices (
    id uuid PRIMARY KEY,
    user_id uuid NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    host_name text NOT NULL,
    platform text NOT NULL,
    os_version text,
    first_seen_at timestamptz NOT NULL,
    last_seen_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (user_id, host_name, platform)
);

CREATE INDEX devices_user_seen_idx
    ON devices (user_id, last_seen_at DESC);

CREATE TABLE agent_sessions (
    id uuid PRIMARY KEY,
    user_id uuid NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    device_context_id uuid NOT NULL REFERENCES devices(id),
    agent_name text NOT NULL,
    agent_version text,
    session_id text NOT NULL,
    started_at timestamptz NOT NULL,
    ended_at timestamptz,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (user_id, agent_name, session_id)
);

CREATE INDEX agent_sessions_user_time_idx
    ON agent_sessions (user_id, started_at DESC);

CREATE TABLE agent_turns (
    id uuid PRIMARY KEY,
    user_id uuid NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    session_pk uuid NOT NULL REFERENCES agent_sessions(id) ON DELETE CASCADE,
    turn_index integer NOT NULL CHECK (turn_index >= 1),
    started_at timestamptz NOT NULL,
    ended_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (session_pk, turn_index)
);

CREATE INDEX agent_turns_user_session_idx
    ON agent_turns (user_id, session_pk, turn_index);

CREATE TABLE llm_usage_events (
    id uuid PRIMARY KEY,
    user_id uuid NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    device_context_id uuid NOT NULL REFERENCES devices(id),
    session_pk uuid NOT NULL REFERENCES agent_sessions(id) ON DELETE CASCADE,
    turn_pk uuid NOT NULL REFERENCES agent_turns(id) ON DELETE CASCADE,
    event_id text NOT NULL UNIQUE,
    agent_name text NOT NULL,
    agent_version text,
    session_id text NOT NULL,
    turn_index integer NOT NULL CHECK (turn_index >= 1),
    llm_provider text NOT NULL,
    llm_model text NOT NULL,
    event_type text NOT NULL CHECK (event_type IN ('request', 'response')),
    text text,
    text_sha256 bytea,
    input_tokens bigint NOT NULL DEFAULT 0 CHECK (input_tokens >= 0),
    output_tokens bigint NOT NULL DEFAULT 0 CHECK (output_tokens >= 0),
    cache_read_tokens bigint NOT NULL DEFAULT 0 CHECK (cache_read_tokens >= 0),
    cache_write_tokens bigint NOT NULL DEFAULT 0 CHECK (cache_write_tokens >= 0),
    reasoning_tokens bigint NOT NULL DEFAULT 0 CHECK (reasoning_tokens >= 0),
    total_tokens bigint NOT NULL DEFAULT 0 CHECK (total_tokens >= 0),
    observed_at timestamptz NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX llm_usage_events_user_time_idx
    ON llm_usage_events (user_id, observed_at DESC);

CREATE INDEX llm_usage_events_observed_at_idx
    ON llm_usage_events (observed_at DESC);

CREATE INDEX llm_usage_events_session_turn_idx
    ON llm_usage_events (session_pk, turn_index, observed_at);

CREATE INDEX llm_usage_events_agent_model_time_idx
    ON llm_usage_events (agent_name, llm_provider, llm_model, observed_at DESC);

CREATE TABLE llm_usage_event_attachments (
    id uuid PRIMARY KEY,
    user_id uuid NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    event_pk uuid NOT NULL REFERENCES llm_usage_events(id) ON DELETE CASCADE,
    position integer NOT NULL CHECK (position >= 0),
    media_type text NOT NULL CHECK (
        media_type IN ('image/png', 'image/jpeg', 'image/webp', 'image/gif')
    ),
    byte_size bigint NOT NULL CHECK (byte_size > 0 AND byte_size <= 8388608),
    sha256 bytea NOT NULL CHECK (octet_length(sha256) = 32),
    content bytea,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (event_pk, position),
    CHECK (content IS NULL OR octet_length(content) = byte_size)
);

CREATE INDEX llm_usage_event_attachments_user_event_idx
    ON llm_usage_event_attachments (user_id, event_pk, position);

CREATE TABLE agent_diagnostic_captures (
    id uuid PRIMARY KEY,
    user_id uuid NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    device_context_id uuid NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
    session_pk uuid NOT NULL REFERENCES agent_sessions(id) ON DELETE CASCADE,
    capture_id text NOT NULL,
    flow_id text NOT NULL,
    captured_at timestamptz NOT NULL,
    collector_version text NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL,
    UNIQUE (user_id, capture_id)
);

CREATE INDEX agent_diagnostic_captures_session_time_idx
    ON agent_diagnostic_captures (session_pk, captured_at, id);

CREATE INDEX agent_diagnostic_captures_flow_idx
    ON agent_diagnostic_captures (user_id, flow_id, captured_at);

CREATE TABLE agent_diagnostic_capture_events (
    capture_pk uuid NOT NULL REFERENCES agent_diagnostic_captures(id) ON DELETE CASCADE,
    event_pk uuid NOT NULL REFERENCES llm_usage_events(id) ON DELETE CASCADE,
    PRIMARY KEY (capture_pk, event_pk)
);

CREATE INDEX agent_diagnostic_capture_events_event_idx
    ON agent_diagnostic_capture_events (event_pk);

CREATE TABLE search_outbox (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_pk uuid NOT NULL,
    user_id uuid NOT NULL,
    operation text NOT NULL DEFAULT 'upsert' CHECK (operation IN ('upsert', 'delete')),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    available_at timestamptz NOT NULL DEFAULT now(),
    claimed_at timestamptz,
    claimed_by text,
    processed_at timestamptz,
    dead_lettered_at timestamptz,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (event_pk, operation),
    CHECK ((claimed_at IS NULL) = (claimed_by IS NULL))
);

CREATE INDEX search_outbox_pending_idx
    ON search_outbox (available_at, id)
    WHERE processed_at IS NULL AND dead_lettered_at IS NULL;
