-- Older collectors counted OpenAI-style cached input inside input_tokens and
-- set request totals to input_tokens alone, dropping Anthropic cache reads and
-- writes. Rewrite stored rows to disjoint input, cache read, cache write, and
-- output counters with total_tokens as their sum.
--
-- Anthropic Messages already reports cache counters outside input_tokens.
-- Provider names cannot identify the protocol because gateway traffic records
-- the gateway host, so Anthropic harnesses are excluded by agent name as well,
-- and rows whose input is smaller than their cached input are left untouched.
UPDATE llm_usage_events
SET input_tokens = input_tokens - cache_read_tokens - cache_write_tokens
WHERE llm_provider <> 'anthropic'
  AND agent_name NOT IN ('claude-code', 'claude-desktop')
  AND cache_read_tokens + cache_write_tokens > 0
  AND input_tokens >= cache_read_tokens + cache_write_tokens;

UPDATE llm_usage_events
SET total_tokens = input_tokens + cache_read_tokens + cache_write_tokens + output_tokens;
