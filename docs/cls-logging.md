# Standalone structured operational logging

Task: `cls.md`.

## Problem and scope

The standalone backend needs the same structured diagnostics and safe request
correlation as the product Agent Data service. It remains a self-contained public
backend: its `telemetry` module owns formatting and request middleware without
linking private services or cloud authentication. JSON is opt-in for standalone
operators. PostgreSQL/Elasticsearch and SQLite/FTS keep identical HTTP behavior.

## Runtime contract

`ABYSS_LOG_FORMAT=json` enables one JSON event per stdout line. Unset or `text`
preserves the previous text format; invalid values fail startup. Existing service
log-level settings and `RUST_LOG` precedence are unchanged. JSON includes UTC
`timestamp`, severity, target, service, environment and workspace package version.
Kubernetes collector metadata identifies the actual pod and immutable image.

HTTP middleware replaces the default URI-bearing `TraceLayer`. It creates a UUID,
returns it in `x-request-id`, and records method, matched route template, status
and `duration_ms` until response construction. Dynamic paths, query strings,
headers and bodies are not recorded by the middleware; unknown routes use
`<unmatched>`. Successful health/readiness probes produce no completion event.
Other success responses emit INFO, 4xx WARN, and 5xx ERROR.

The JSON formatter promotes only request_id/method/route from span context to
flat, indexable keys. The context span emits no event and has ERROR level so
raising event filters to WARN/ERROR does not remove diagnostic context. Request
context remains distinct across concurrent futures. IDs are per-service requests;
this does not implement cross-service distributed tracing or body-stream timing.

HTTP internal errors and Elasticsearch failures log stable categories instead of
raw database errors or remote response bodies, which can contain customer data.
Identity HTTP errors omit URLs and OIDC failures omit remote error bodies. This
trades raw diagnostic detail for a safe category, operation message, source target
and request ID. Existing error responses and stored outbox errors are unchanged.
Process startup failures/panics remain legacy stderr; the collector's parse_failed
fallback retains these lines rather than silently dropping them. Operators should
keep production at the default INFO level and treat raw stderr as sensitive.

No database migration or API payload change is required. Existing response headers
remain compatible, with x-request-id added. No cloud credentials or upload client
are linked into applications. Collection runs independently of the request path.
Container rotation and finite collector buffers can lose unshipped logs during
long outages; this is operational logging, not a durable audit ledger.

## Validation and evidence

The telemetry unit tests cover typed JSON, newline escaping, concurrent request
isolation, probe suppression, unknown routes and INFO/WARN/ERROR filtering.
`scripts/tests/logging_blackbox.py` launches real binaries with disposable stores,
sends sensitive marker values through paths/queries/headers/bodies, and verifies
JSON fields, IDs, text compatibility and invalid-format failure. Agent Data also
exercises real Identity connection failure and its correlated 503 record.

Existing full unit, database/API black-box, formatting and clippy checks remain
required. Both local review stacks run the task binaries with seeded data and
real gateway/proxy routing. Persistent coordinator evidence lives at
`artifacts/cls/report/test-report.html`, with source identities, commands, logs,
review URLs and explicit cloud gaps. Cloud ingestion is not established by local
stdout tests; it is verified after merge, publication and authorized rollout.

The standalone checks include both storage feature profiles and their full API
black-box suites. The SQLite black-box entry point also runs the new process-level
logging suite, so CI verifies the installed logging behavior rather than only a
formatter unit test.

Companion: [product CLS design](https://github.com/lexmount/abyss-services/blob/task/cls/docs/cls-logging.md).
The product owns regional collection manifests and the tccli rollout runbook.
The PRs can merge independently; standalone users are not required to configure
CLS. No dashboard changes or package release dependency is introduced.
