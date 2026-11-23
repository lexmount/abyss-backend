-- Image bytes are stored once per user and content, keyed by the owner and the
-- bytes' SHA-256; an event's attachments refer to them. Blobs are never shared
-- between users, so an upload reveals nothing about another user's images.
CREATE TABLE attachment_blobs (
    user_id uuid NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    sha256 bytea NOT NULL CHECK (octet_length(sha256) = 32),
    media_type text NOT NULL CHECK (
        media_type IN ('image/png', 'image/jpeg', 'image/webp', 'image/gif')
    ),
    byte_size bigint NOT NULL CHECK (byte_size > 0 AND byte_size <= 8388608),
    content bytea,
    created_at timestamptz NOT NULL,
    PRIMARY KEY (user_id, sha256),
    CHECK (content IS NULL OR octet_length(content) = byte_size)
);

-- Rows that carry bytes come first, so a digest stored both with and without
-- bytes keeps them.
INSERT INTO attachment_blobs (user_id, sha256, media_type, byte_size, content, created_at)
SELECT DISTINCT ON (user_id, sha256) user_id, sha256, media_type, byte_size, content, created_at
FROM llm_usage_event_attachments
ORDER BY user_id, sha256, content IS NULL, created_at, id;

ALTER TABLE llm_usage_event_attachments
    DROP COLUMN media_type,
    DROP COLUMN byte_size,
    DROP COLUMN content,
    ADD FOREIGN KEY (user_id, sha256) REFERENCES attachment_blobs(user_id, sha256);
