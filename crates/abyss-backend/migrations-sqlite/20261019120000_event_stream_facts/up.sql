-- Stream facts read at ingest from one event's own conversation entries,
-- kept beside the event so a session's streams read them without walking
-- the overflow pages of event metadata. A request has new_from,
-- starts_turn, last_identity, trailing, and result_ids; a response has
-- emitted_ids and output_skeleton; the other side's columns stay NULL.
-- trailing is a JSON array of roles, id lists JSON arrays of strings, and
-- output_skeleton a JSON array of [role, key] pairs, the key being the call
-- id, else the item id. Events stored before this migration have no facts
-- row.
CREATE TABLE llm_usage_event_stream_facts (
    event_pk TEXT PRIMARY KEY REFERENCES llm_usage_events(id) ON DELETE CASCADE,
    new_from INTEGER,
    starts_turn INTEGER,
    last_identity TEXT,
    trailing TEXT CHECK (json_valid(trailing)),
    result_ids TEXT CHECK (json_valid(result_ids)),
    emitted_ids TEXT CHECK (json_valid(emitted_ids)),
    output_skeleton TEXT CHECK (json_valid(output_skeleton))
) STRICT;
