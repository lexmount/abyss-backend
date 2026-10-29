-- Whether a request continues a context: it holds model output (its new
-- part begins after the first entry) or names the response it continues in
-- previous_response_id, as a request sending only what is new does. In a
-- session without declared turns, a turn-opening request opens a turn only
-- when it continues a context, or when no turn began before it. NULL for a
-- response.
ALTER TABLE llm_usage_event_stream_facts ADD COLUMN continues_context boolean;

UPDATE llm_usage_event_stream_facts f
SET continues_context = (f.new_from > 0 OR e.previous_response_id IS NOT NULL)
FROM llm_usage_events e
WHERE e.id = f.event_pk AND f.new_from IS NOT NULL;
