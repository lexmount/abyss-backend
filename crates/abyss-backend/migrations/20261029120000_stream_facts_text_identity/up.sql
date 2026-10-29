-- Model text without a call id or item id now has an identity: the SHA-256
-- of its text. The schema does not change. In the same transaction that
-- records this migration, the backend reads the stream facts of every stored
-- event again from its metadata, including events stored before facts were
-- read, keeps the ids the client declared, and bumps the structure revision
-- of each session whose facts changed.
-- PostgreSQL refuses an empty query, so the migration runs one that does
-- nothing.
SELECT 1;
