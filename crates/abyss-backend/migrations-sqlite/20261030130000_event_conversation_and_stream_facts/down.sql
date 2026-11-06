DROP INDEX search_refs_text;
DROP TABLE search_refs;
DROP TABLE search_texts_trigram;
DROP TABLE search_texts;

DROP TABLE llm_usage_event_stream_facts;

ALTER TABLE llm_usage_events DROP COLUMN conversation;
