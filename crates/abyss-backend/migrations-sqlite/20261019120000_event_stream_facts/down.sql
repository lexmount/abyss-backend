DROP INDEX search_refs_text;
DROP TABLE search_refs;
DROP TABLE search_texts_trigram;
DROP TABLE search_texts;

-- The previous projection comes back empty; nothing rebuilds it.
CREATE VIRTUAL TABLE usage_events_fts USING fts5(
    event_pk UNINDEXED,
    user_id UNINDEXED,
    session_pk UNINDEXED,
    session_id,
    content,
    tool_names,
    tool_content,
    tokenize = 'unicode61'
);

DROP TABLE IF EXISTS llm_usage_event_stream_facts;
