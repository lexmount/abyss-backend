-- Event rows hold whole request bodies in text and metadata, so reading a
-- token column from the row walks through megabytes of overflow pages. These
-- indexes carry every column the usage summary and the session list read,
-- letting both answer from the index alone.
CREATE INDEX llm_usage_events_summary_cover_idx
    ON llm_usage_events (
        user_id, observed_at, agent_name, llm_provider, llm_model, event_type,
        device_context_id, session_pk, turn_pk, session_id,
        input_tokens, output_tokens, cache_read_tokens, cache_write_tokens,
        reasoning_tokens, total_tokens
    );
CREATE INDEX llm_usage_events_session_activity_idx
    ON llm_usage_events (session_pk, observed_at, total_tokens);
