-- The AgentEvent's conversation, stored as received; NULL when the event
-- carries none.
ALTER TABLE llm_usage_events ADD COLUMN conversation TEXT CHECK (json_valid(conversation));

-- Stream facts read at ingest from one event's own conversation entries,
-- kept beside the event so a session's streams read them without walking
-- the overflow pages of the conversation. A request has new_from,
-- continues_context, starts_turn, last_identity, previous_identity,
-- trailing, result_ids, and its label; a response has emitted_ids and
-- output_skeleton; the other side's columns stay NULL.
-- continues_context: the request holds model output (its new part begins
-- after the first entry) or names the response it continues in
-- previous_response_id, as a request sending only what is new does. In a
-- session without declared turns, a turn-opening request opens a turn only
-- when it continues a context, or when no turn began before it.
-- previous_identity: the identity before the request's last identity,
-- repeats of it skipped; it tells apart the responses that emitted the same
-- last identity, such as an answer given word for word in every round. NULL
-- when nothing before the last identity has one.
-- The label is the last user entry with text in the request's new part: its
-- index, its first 50 UTF-16 code units, and its whole length in them.
-- trailing is a JSON array of roles, id lists JSON arrays of strings, and
-- output_skeleton a JSON array of [role, key] pairs, the key being the call
-- id, else the item id.
-- subagent_id, parent_subagent_id, and turn_id are the ids the client
-- declared on the event, kept as sent and NULL when absent. turn_id names
-- the user input the event works for; it decides the event's turn and
-- where a sub-agent's segment shows.
CREATE TABLE llm_usage_event_stream_facts (
    event_pk TEXT PRIMARY KEY REFERENCES llm_usage_events(id) ON DELETE CASCADE,
    new_from INTEGER,
    continues_context INTEGER,
    starts_turn INTEGER,
    last_identity TEXT,
    previous_identity TEXT,
    trailing TEXT CHECK (json_valid(trailing)),
    result_ids TEXT CHECK (json_valid(result_ids)),
    emitted_ids TEXT CHECK (json_valid(emitted_ids)),
    output_skeleton TEXT CHECK (json_valid(output_skeleton)),
    label_index INTEGER,
    label_text TEXT,
    label_length INTEGER,
    subagent_id TEXT,
    parent_subagent_id TEXT,
    turn_id TEXT
) STRICT;

-- Search reads the text and the name of every conversation entry. Each
-- distinct piece of text is stored once per owner, and every piece of an
-- event refers to its text; replayed history therefore adds references, not
-- text. A trigram index over the folded texts, without positions, narrows
-- the candidates the backend verifies.

CREATE TABLE search_texts (
    id INTEGER PRIMARY KEY,
    user_id TEXT NOT NULL,
    -- SHA-256 of the text's UTF-8 bytes.
    sha256 BLOB NOT NULL,
    text TEXT NOT NULL,
    UNIQUE (user_id, sha256)
) STRICT;

-- Contentless and without positions; it indexes the folded text of each
-- row of search_texts under that row's id.
CREATE VIRTUAL TABLE search_texts_trigram USING fts5(
    folded,
    content = '',
    detail = none,
    tokenize = 'trigram case_sensitive 1'
);

-- One row per nonempty `text` or `name` of an entry of an event.
CREATE TABLE search_refs (
    event_pk TEXT NOT NULL,
    entry_index INTEGER NOT NULL,
    field TEXT NOT NULL CHECK (field IN ('text', 'name')),
    text_id INTEGER NOT NULL,
    PRIMARY KEY (event_pk, entry_index, field)
) STRICT, WITHOUT ROWID;

CREATE INDEX search_refs_text ON search_refs (text_id);
