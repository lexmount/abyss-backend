-- Codex names a task from a separate naming thread. naming_turn_id and
-- naming_prompt keep what that thread's request shows about the task it names,
-- and parent_session_pk records the session it was matched to.
ALTER TABLE session_titles
    ADD COLUMN naming_turn_id text,
    ADD COLUMN naming_prompt text,
    ADD COLUMN parent_session_pk uuid REFERENCES agent_sessions(id) ON DELETE SET NULL;
