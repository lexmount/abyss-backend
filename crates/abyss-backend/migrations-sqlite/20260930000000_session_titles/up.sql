-- Session names read back from the Agents' own title-generation calls, kept
-- apart from agent_sessions so session rows keep their shape. purpose marks
-- sessions that exist only to generate a title, such as Codex naming threads.
CREATE TABLE session_titles (
    session_pk TEXT PRIMARY KEY REFERENCES agent_sessions(id) ON DELETE CASCADE,
    user_id TEXT NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    title TEXT,
    title_event_pk TEXT REFERENCES llm_usage_events(id) ON DELETE SET NULL,
    title_observed_at INTEGER,
    purpose TEXT,
    updated_at INTEGER NOT NULL
) STRICT;

-- One-time data backfills that run in application code after migrations.
CREATE TABLE schema_backfills (
    name TEXT PRIMARY KEY,
    completed_at INTEGER NOT NULL
) STRICT;
