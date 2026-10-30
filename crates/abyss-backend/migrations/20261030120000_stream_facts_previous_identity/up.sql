-- The identity before a request's last identity, repeats of it skipped: it
-- tells apart the responses that emitted the same last identity, such as an
-- answer given word for word in every round. NULL when nothing before the last
-- identity has one, and for a response. Only an event's conversation holds it,
-- so in the same transaction that records this migration the backend reads the
-- stream facts of every stored event again.
ALTER TABLE llm_usage_event_stream_facts ADD COLUMN previous_identity text;
