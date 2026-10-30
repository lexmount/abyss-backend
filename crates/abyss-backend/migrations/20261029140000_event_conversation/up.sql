-- The AgentEvent's conversation, stored as received; NULL when the event
-- carries none. Metadata no longer holds it: stored conversations move here.
ALTER TABLE llm_usage_events ADD COLUMN conversation jsonb;

UPDATE llm_usage_events
SET conversation = NULLIF(metadata -> 'conversation', 'null'::jsonb),
    metadata = metadata - 'conversation'
WHERE metadata ? 'conversation';
