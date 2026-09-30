-- Session names read back from the Agents' own title-generation calls, kept
-- apart from agent_sessions so session rows keep their shape. purpose marks
-- sessions that exist only to generate a title, such as Codex naming threads.
CREATE TABLE session_titles (
    session_pk uuid PRIMARY KEY REFERENCES agent_sessions(id) ON DELETE CASCADE,
    user_id uuid NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    title text,
    title_event_pk uuid REFERENCES llm_usage_events(id) ON DELETE SET NULL,
    title_observed_at timestamptz,
    purpose text,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- One-time data backfills that run in application code after migrations.
CREATE TABLE schema_backfills (
    name text PRIMARY KEY,
    completed_at timestamptz NOT NULL DEFAULT now()
);
