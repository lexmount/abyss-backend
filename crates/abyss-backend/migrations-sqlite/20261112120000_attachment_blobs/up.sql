-- Image bytes are stored once per user and content, keyed by the owner and the
-- bytes' SHA-256; an event's attachments refer to them. Blobs are never shared
-- between users, so an upload reveals nothing about another user's images.
CREATE TABLE attachment_blobs (
    user_id TEXT NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    sha256 BLOB NOT NULL CHECK (length(sha256) = 32),
    media_type TEXT NOT NULL CHECK (
        media_type IN ('image/png', 'image/jpeg', 'image/webp', 'image/gif')
    ),
    byte_size INTEGER NOT NULL CHECK (byte_size > 0 AND byte_size <= 8388608),
    content BLOB,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (user_id, sha256),
    CHECK (content IS NULL OR length(content) = byte_size)
) STRICT;

-- Rows that carry bytes come first, so a digest stored both with and without
-- bytes keeps them.
INSERT INTO attachment_blobs (user_id, sha256, media_type, byte_size, content, created_at)
SELECT user_id, sha256, media_type, byte_size, content, created_at
FROM llm_usage_event_attachments
WHERE true
ORDER BY content IS NULL, created_at, id
ON CONFLICT (user_id, sha256) DO NOTHING;

CREATE TABLE llm_usage_event_attachments_by_digest (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    event_pk TEXT NOT NULL REFERENCES llm_usage_events(id) ON DELETE CASCADE,
    position INTEGER NOT NULL CHECK (position >= 0),
    sha256 BLOB NOT NULL,
    created_at INTEGER NOT NULL,
    UNIQUE (event_pk, position),
    FOREIGN KEY (user_id, sha256) REFERENCES attachment_blobs(user_id, sha256)
) STRICT;

INSERT INTO llm_usage_event_attachments_by_digest
    (id, user_id, event_pk, position, sha256, created_at)
SELECT id, user_id, event_pk, position, sha256, created_at
FROM llm_usage_event_attachments;

DROP TABLE llm_usage_event_attachments;

ALTER TABLE llm_usage_event_attachments_by_digest RENAME TO llm_usage_event_attachments;

CREATE INDEX llm_usage_event_attachments_user_event_idx
    ON llm_usage_event_attachments (user_id, event_pk, position);
