UPDATE llm_usage_events
SET metadata = json_set(metadata, '$.conversation', json(conversation))
WHERE conversation IS NOT NULL;

ALTER TABLE llm_usage_events DROP COLUMN conversation;
