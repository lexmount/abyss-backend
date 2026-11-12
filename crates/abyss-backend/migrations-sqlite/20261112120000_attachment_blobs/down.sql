CREATE TABLE llm_usage_event_attachments_with_content (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    event_pk TEXT NOT NULL REFERENCES llm_usage_events(id) ON DELETE CASCADE,
    position INTEGER NOT NULL CHECK (position >= 0),
    media_type TEXT NOT NULL CHECK (
        media_type IN ('image/png', 'image/jpeg', 'image/webp', 'image/gif')
    ),
    byte_size INTEGER NOT NULL CHECK (byte_size > 0 AND byte_size <= 8388608),
    sha256 BLOB NOT NULL CHECK (length(sha256) = 32),
    content BLOB,
    created_at INTEGER NOT NULL,
    UNIQUE (event_pk, position),
    CHECK (content IS NULL OR length(content) = byte_size)
) STRICT;

INSERT INTO llm_usage_event_attachments_with_content
    (id, user_id, event_pk, position, media_type, byte_size, sha256, content, created_at)
SELECT a.id, a.user_id, a.event_pk, a.position, b.media_type, b.byte_size, a.sha256,
    b.content, a.created_at
FROM llm_usage_event_attachments a
JOIN attachment_blobs b ON b.user_id = a.user_id AND b.sha256 = a.sha256;

DROP TABLE llm_usage_event_attachments;
DROP TABLE attachment_blobs;

ALTER TABLE llm_usage_event_attachments_with_content RENAME TO llm_usage_event_attachments;

CREATE INDEX llm_usage_event_attachments_user_event_idx
    ON llm_usage_event_attachments (user_id, event_pk, position);
