# Enterprise routing observation compatibility

Task: `lexmount-vpn.md`. Baseline: `b9035b9cf6d25bb268e90b8656e0701403e71153`.

Enterprise model routing belongs to the endpoint and a separate product service.
The standalone backend retains its existing bearer authentication, event schema,
storage, search and query responsibilities. No database migration or provider
credential store is introduced.

## Capture-to-query verification

`scripts/verify_capture_delivery.py` compares normalized broker events with records
read through the authenticated event API after the actual delivery plugin uploads
them. It requires every captured event ID, compares session and response identity,
harness, provider/model, conversation and all token counters, and checks sensitive
header redaction. Optional properties represented as null by the read API are
normalized to the capture protocol's omission semantics. Missing events, changed
fields, duplicate capture IDs and unredacted credentials fail verification.

The verifier is intentionally read-only. It does not insert data or reimplement
uploading. Credentials are read from a private file, and output contains only
counts and token totals. Unit tests cover field preservation, missing records,
changed model/content/counters and leaked headers. The same contract verifier and
regressions are included in the companion services repository.

## Evidence and deployment

Local workspace tests and clippy passed for both `postgres-es` and `sqlite-fts`.
Both complete backend black-box suites passed. Installed Codex 0.167.0, Claude
Code 2.1.206 and DSH 0.2.0-rc.2 ran through the enterprise plugin and local provider.
Two actual delivery workers uploaded the observed events to this backend and to
product Agent Data. Ten events, including readiness traffic, retained all compared
fields and 85 input / 15 output tokens. Provider credentials were redacted.

The review deployment uses task-only PostgreSQL, the actual standalone backend,
and the built dashboard served by its production Node proxy on port 15181. The
product counterpart runs Identity, Agent Data, search indexer, PostgreSQL,
Elasticsearch, Dex and an Nginx gateway on port 15182. Build outputs, private seed
credentials and the reproducible `review_stack.py` live outside reusable checkouts
under coordinator `artifacts/lexmount-vpn`. The HTML report records exact final
revisions, commands, screenshots, runtime availability and outstanding checks.

Companion designs are `docs/lexmount-vpn.md` in abyss-services and AbyssApp.
The generic endpoint change is [abyss-rs PR 85](https://github.com/lexmount/abyss-rs/pull/85).
This verifier can merge independently; it requires no schema or API rollout.
Production provider and residential egress validation remains an operator
integration step and is not inferred from local fixtures.

## Coordinated review

- [Endpoint PR 85](https://github.com/lexmount/abyss-rs/pull/85) and [runtime design](https://github.com/lexmount/abyss-rs/blob/task/lexmount-vpn/docs/lexmount-vpn.md).
- [Desktop PR 86](https://github.com/lexmount/AbyssApp/pull/86) and [product design](https://github.com/lexmount/AbyssApp/blob/task/lexmount-vpn/docs/lexmount-vpn.md).
- [Services PR 25](https://github.com/lexmount/abyss-services/pull/25) and [service design](https://github.com/lexmount/abyss-services/blob/task/lexmount-vpn/docs/lexmount-vpn.md).
- [Standalone PR 16](https://github.com/lexmount/abyss-backend/pull/16) and [persistence design](https://github.com/lexmount/abyss-backend/blob/task/lexmount-vpn/docs/lexmount-vpn.md).

The standalone verifier is independent. Integrate the public runtime primitives
before the product composition. The enterprise service is opt-in; production
packaging additionally requires a published endpoint release and normal release
pins. Review packages consume explicit immutable development artifacts only.
