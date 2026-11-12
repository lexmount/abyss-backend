ALTER TABLE llm_usage_event_attachments
    DROP CONSTRAINT llm_usage_event_attachments_user_id_sha256_fkey,
    ADD COLUMN media_type text,
    ADD COLUMN byte_size bigint,
    ADD COLUMN content bytea;

UPDATE llm_usage_event_attachments a
SET media_type = b.media_type, byte_size = b.byte_size, content = b.content
FROM attachment_blobs b
WHERE b.user_id = a.user_id AND b.sha256 = a.sha256;

ALTER TABLE llm_usage_event_attachments
    ALTER COLUMN media_type SET NOT NULL,
    ALTER COLUMN byte_size SET NOT NULL,
    ADD CHECK (media_type IN ('image/png', 'image/jpeg', 'image/webp', 'image/gif')),
    ADD CHECK (byte_size > 0 AND byte_size <= 8388608),
    ADD CHECK (content IS NULL OR octet_length(content) = byte_size);

DROP TABLE attachment_blobs;
