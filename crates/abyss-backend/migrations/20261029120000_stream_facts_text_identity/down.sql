-- Reverting keeps the rebuilt stream facts. PostgreSQL refuses an empty
-- query, so the migration runs one that does nothing.
SELECT 1;
