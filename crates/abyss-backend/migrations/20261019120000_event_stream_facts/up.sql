-- Stream facts read at ingest from one event's own conversation entries,
-- kept beside the event so a session's streams read them without touching
-- event metadata. A request has new_from, starts_turn, last_identity,
-- trailing, result_ids, and its label; a response has emitted_ids and
-- output_skeleton; the other side's columns stay NULL. The label is the last
-- user entry with text in the request's new part: its index, its first 50
-- UTF-16 code units, and its whole length in them. trailing is a JSON array of roles, id
-- lists JSON arrays of strings, and output_skeleton a JSON array of
-- [role, key] pairs, the key being the call id, else the item id. Events
-- stored before this migration have no facts row.
-- subagent_id, parent_subagent_id, and root_turn_id are the ids the client
-- declared on the event, kept as sent and NULL when absent; they only
-- decide where a sub-agent's segment shows.
CREATE TABLE llm_usage_event_stream_facts (
    event_pk uuid PRIMARY KEY REFERENCES llm_usage_events(id) ON DELETE CASCADE,
    new_from integer,
    starts_turn boolean,
    last_identity text,
    trailing jsonb,
    result_ids jsonb,
    emitted_ids jsonb,
    output_skeleton jsonb,
    label_index integer,
    label_text text,
    label_length integer,
    subagent_id text,
    parent_subagent_id text,
    root_turn_id text
);

-- The backend also registers each session's earliest request in session_titles,
-- in the same transaction that records this migration. It reads no old event
-- bodies; the title remains absent unless that request already has label facts.
