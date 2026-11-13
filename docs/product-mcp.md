# Revision-checked session reading

Memory and Transfer require the same recorded history: a directory for targeted
reads and a bounded, resumable traversal for full handoff. The standalone API
adds `/v1/agent-usage/sessions/{session_pk}/directory` and `/read`, matching the
Agent Data domain contract in the companion `abyss-services` change. The MCP
transport itself belongs only to the product services.

Both storage profiles load an owner-scoped, body-free event index ordered by
observation time, event side and primary key within a read transaction. Its SHA-256
revision binds the ordered event identifiers. A paginated directory exposes the
index; reads load one selected event and return a bounded UTF-8 slice of its
serialized recorded JSON with source identifiers, SHA-256 digest, byte offsets
and a continuation token. Tokens bind session, revision, event index and offset;
the content digest detects changes while reading one event. Invalid positions
and stale revisions fail explicitly. No persistent cursor or migration is needed.

The response preserves conversation, tool activity, omissions, completeness and
attachment metadata. End-of-scope means all stored events have been traversed,
not that all provider traffic was captured. Device labels are current metadata,
and content digest checks prevent mixing two event representations. Live ingestion
can invalidate traversal, requiring a new directory; durable snapshots and
exports are deferred. Existing public interfaces remain unchanged.

Validation includes shared domain unit cases and real ingestion/read HTTP tests
against PostgreSQL and SQLite, including Unicode and oversized content, precise
reconstruction, interruption, malformed cursors, late events and foreign session
IDs. Both local review stacks, formatting, both Clippy profiles, workspace tests
and existing black-box suites must pass. The services PR owns MCP authentication
and protocol validation. These changes have no merge-order dependency.

The persistent HTML report and companion PR are linked below.

## Review evidence

The task report is retained at
`/Users/ccc/repo/lexmount/abyss-workspace/artifacts/product-mcp/report/report.html`
and served locally at `http://127.0.0.1:25223/report.html`. The standalone review
URL is `http://127.0.0.1:25221`; product login is
`http://127.0.0.1:25222/review/login`.
The [companion product design](https://github.com/lexmount/abyss-services/blob/task/product-mcp/docs/product-mcp.md)
describes the shared MCP transport and Identity boundary.

Companion implementation: [abyss-services PR](https://github.com/lexmount/abyss-services/pull/27).

The product companion now also implements browser-based MCP OAuth authorization
in Identity and Agent Data. Standalone keeps its configured deployment bearer
token and the same recorded-session read contract; it does not acquire product
SSO, OAuth grant storage or an MCP transport. See the
[OAuth design](https://github.com/lexmount/abyss-services/blob/task/product-mcp/docs/product-mcp-oauth.md).

Validation uses Rust 1.100, matching CI. Equivalent optional-value defaults and
unused dependency declarations were cleaned up for its stricter checks while
retaining compatibility with the existing Rust 1.98 build.
