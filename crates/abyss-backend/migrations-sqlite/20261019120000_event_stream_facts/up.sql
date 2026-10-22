-- Stream facts read at ingest from one event's own conversation entries,
-- kept beside the event so a session's streams read them without walking
-- the overflow pages of event metadata. A request has new_from,
-- starts_turn, last_identity, trailing, result_ids, and its label; a
-- response has emitted_ids and output_skeleton; the other side's columns
-- stay NULL. The label is the last user entry with text in the request's new
-- part: its index, its first 50 UTF-16 code units, and its whole length in
-- them.
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
    output_skeleton TEXT CHECK (json_valid(output_skeleton)),
    label_index INTEGER,
    label_text TEXT,
    label_length INTEGER
) STRICT;

-- Search reads the text and the name of every conversation entry. Each
-- distinct piece of text is stored once per owner, and every piece of an
-- event refers to its text; replayed history therefore adds references, not
-- text. A trigram index over the folded texts, without positions, narrows
-- the candidates the backend verifies.
DROP TABLE usage_events_fts;

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
