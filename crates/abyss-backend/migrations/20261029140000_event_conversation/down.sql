UPDATE llm_usage_events
SET metadata = jsonb_set(metadata, '{conversation}', conversation)
WHERE conversation IS NOT NULL;

ALTER TABLE llm_usage_events DROP COLUMN conversation;
