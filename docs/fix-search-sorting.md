# Session search ordered by session start time

Task: `fix-search-sorting.md`.

Search results must descend by the session start time shown on each card. A session that began in 2025 stays below a session that began in 2026 even when the older session has newer matching events. Event observation times determine filter eligibility, not session ordering.

## Implementation

The PostgreSQL-backed APIs enumerate matching session keys through an owner-scoped Elasticsearch composite aggregation, in batches of 1,000. PostgreSQL rechecks ownership and existing session membership, counts the matches, and orders by `started_at DESC, session_id ASC, id ASC` before applying the requested offset and limit. Elasticsearch then fetches bounded matching previews only for the selected page; the API restores PostgreSQL page order after that fetch. All keyword and event filters apply to both Elasticsearch phases. No index mapping change, data migration, or reindex is needed. Reading authoritative start times also handles an earlier, nonmatching event delivered late without rewriting existing search documents.

SQLite joins each matching event to its authoritative session start time and applies the same ordering before pagination. Both web applications preserve the API page order and continue displaying `started_at`; they do not sort only the visible page. Preview selection, matching counts, authentication, response shape, and observation-time filter semantics remain compatible.

Broad searches require enumerating matching session keys rather than only fetching one collapsed page. Each Elasticsearch response and each preview fetch are bounded; application memory and SQL key filtering scale with matching sessions. Validation includes a query spanning multiple 1,000-key aggregation batches. Offset pagination can still shift during concurrent ingestion; it does not promise a cross-request snapshot.

Deploy the two service changes to enable the behavior. Web changes document and test the contract and need no shared UI package release. Each stack can deploy independently; all four PRs form the coordinated task.

## Validation

Unit tests cover aggregation continuation and selected-page query scoping, SQLite start-time ranking, ties, and frontend page navigation. HTTP black-box checks compare every returned `started_at` with the expected descending sequence across pages and filters. Fixtures cover resumed older sessions, cross-year dates, equal start times, late nonmatching earlier events, empty results, and exact total counts. Run both real PostgreSQL/Elasticsearch stacks, standalone SQLite, and browser checks of displayed card order. Run the required repository quality, packaging, and CI checks on the submitted revisions.

## Review evidence

The persistent HTML report is `artifacts/fix-search-sorting/test-report.html` in the coordinator workspace. It records tested revisions, commands, results, review URLs, and supporting logs. See the companion designs: [abyss-services](https://github.com/lexmount/abyss-services/blob/task/fix-search-sorting/docs/fix-search-sorting.md), [abyss-dashboard](https://github.com/lexmount/abyss-dashboard/blob/task/fix-search-sorting/docs/fix-search-sorting.md), [abyss-frontend](https://github.com/lexmount/abyss-frontend/blob/task/fix-search-sorting/docs/fix-search-sorting.md).

Elasticsearch's [composite aggregation](https://www.elastic.co/docs/reference/aggregations/search-aggregations-bucket-composite-aggregation) pages matching session keys without the collapsed-hit result window.

## CI toolchain compatibility

Rust 1.100 in CI newly flags existing `Option::map(...).unwrap_or_default()` expressions. The affected projections and session reconstruction helpers now use equivalent `map_or_else(Default::default, ...)` expressions, which also compile with the locally supported Rust 1.98 toolchain. These changes preserve empty defaults and successful values; full unit and storage black-box suites cover the touched paths.
