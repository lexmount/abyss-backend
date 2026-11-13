# Session search ordered by recency

Task: `fix-search-sorting.md`.

Session searches must show the sessions with the most recent matching activity first, including across page boundaries. The Elasticsearch-backed APIs currently omit an explicit sort and therefore return relevance order. Both web applications render the server page as received. The standalone SQLite implementation already orders sessions by their latest matching event.

## Implementation

Sort Elasticsearch outer hits by `observed_at` descending, then `session_id.keyword` and `session_pk` ascending. Collapse on `session_pk` selects the newest matching event for each session before offset pagination. The two tie breakers match the existing SQLite session ordering. These fields already have sortable mappings, so no migration or reindex is required.

Keep match eligibility, authenticated owner filters, observation-time filters, page size and result-window limits, hydration, and relevance-ranked preview snippets unchanged. Recency refers to matching activity within the requested filters, not unrelated later events or the session start time. The API shape is unchanged. Pagination remains offset-based and is stable for a fixed indexed dataset; ongoing ingestion can shift pages as before.

The coordinated service change uses the same sort in both standalone PostgreSQL/Elasticsearch and product Agent Data. The dashboard and frontend document and regression-test the server-owned ordering. Deploy the API changes to enable the behavior; the web changes do not require a new shared UI package.

The card timestamp continues to show session start time. It is informational and does not determine result order: an older session with a newer matching event can lead the page. Ordinary session-list ordering is unchanged.

## Validation

Unit coverage checks the Elasticsearch sort, collapse, and offset contract. A reusable HTTP black-box test seeds more than one page in deliberately conflicting relevance and ingestion order, same-time sessions, a resumed old session, and an unrelated recent event. It verifies global ordering, four filter scenarios, repeated pages, highlights, and empty results. A late earlier nonmatching event must not affect ranking; a separate 1,005-session fixture verifies pagination on larger result sets. Run it against real PostgreSQL/Elasticsearch in both stacks and standalone SQLite. Run each repository's required unit, black-box, formatting, lint, and CI checks.

## Review evidence

The persistent HTML report is `artifacts/fix-search-sorting/test-report.html` in the coordinator workspace. It records tested revisions, commands, results, review URLs, and supporting logs. See the companion designs: [abyss-services](https://github.com/lexmount/abyss-services/blob/task/fix-search-sorting/docs/fix-search-sorting.md), [abyss-dashboard](https://github.com/lexmount/abyss-dashboard/blob/task/fix-search-sorting/docs/fix-search-sorting.md), [abyss-frontend](https://github.com/lexmount/abyss-frontend/blob/task/fix-search-sorting/docs/fix-search-sorting.md).

Elasticsearch's [collapse contract](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/collapse-search-results) selects the top sorted document per collapse key.

## CI toolchain compatibility

Rust 1.100 in CI newly flags existing `Option::map(...).unwrap_or_default()` expressions. The affected projections and session reconstruction helpers now use equivalent `map_or_else(Default::default, ...)` expressions, which also compile with the locally supported Rust 1.98 toolchain. These changes preserve empty defaults and successful values; full unit and storage black-box suites cover the touched paths.
