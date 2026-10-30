-- The AgentEvent's conversation, stored as received; NULL when the event
-- carries none. Metadata no longer holds it: stored conversations move here.
ALTER TABLE llm_usage_events ADD COLUMN conversation TEXT CHECK (json_valid(conversation));

UPDATE llm_usage_events
SET conversation = NULLIF(metadata -> '$.conversation', 'null'),
    metadata = json_remove(metadata, '$.conversation')
WHERE json_type(metadata, '$.conversation') IS NOT NULL;
