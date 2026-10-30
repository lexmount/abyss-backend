# abyss-backend

`abyss-backend` is the open-source, self-hostable Agent event store for Abyss.
It accepts normalized Agent events and exposes APIs for event history,
summaries, session streams, attachments, and session search.

This repository is intentionally focused on standalone event storage, queries,
and search.

## Capabilities

- Idempotent Agent event ingestion and raw event queries.
- Token-usage summaries and derived session streams.
- Validated image attachment storage and authorized download.
- Session search through Elasticsearch or an embedded SQLite trigram index,
  and, with SQLite, search within one session.
- Health and storage-readiness probes.
- Embedded migrations for both supported storage profiles.

Every session view uses the same title: the label of the session's earliest
request, ordered by observation time and event primary key, even when that
request has no label. A request's label is the last `user` entry with text in
the request's new part, as `{"event_pk", "entry_index", "text", "length"}`:
`text` is the entry's text cut to its first 50 UTF-16 code units, unformatted,
and `length` is the whole text's length in UTF-16 code units. A session whose
earliest request has no label has a `null` title, and later delivery of an
earlier request corrects it. A segment label is the label of the segment's first
request; it does not name the session.

`20261014150000_initial_schema` creates the owner, device, session, event,
attachment, diagnostic, and session title tables, and in PostgreSQL the search
outbox. `20261030130000_event_conversation_and_stream_facts` adds the event
`conversation` column and the stream facts table, and in SQLite the search text
tables. Neither migration moves or rebuilds stored data: databases created by
earlier releases are not upgraded and must be recreated.

Exactly one storage profile is compiled into a binary:

- `postgres-es` uses PostgreSQL as the source of truth and optionally projects
  search documents to Elasticsearch. It is the default profile.
- `sqlite-fts` stores both authoritative data and its search index, written in
  the same transactions, in one local SQLite file through Diesel, with FTS5
  operations expressed as SQL. It does not compile PostgreSQL, reqwest, or Elasticsearch worker code
  into the binary.

## Authentication

Standalone deployments use one bearer token mapped to a fixed deployment
owner. Configure only its SHA-256 digest; clients send the original token in
the standard `Authorization: Bearer` header.

```bash
export ABYSS_API_TOKEN="$(openssl rand -hex 32)"
export ABYSS_BACKEND_API_TOKEN_SHA256="$(printf '%s' "${ABYSS_API_TOKEN}" | openssl dgst -sha256 -r | cut -d' ' -f1)"
```

The plaintext token is never stored by `abyss-backend`. Put it in the Agent
collector configuration and keep the digest in the backend secret store.

## Run locally with SQLite

The SQLite profile has no external service dependency:

```bash
export ABYSS_BACKEND_DATABASE_URL='./data/abyss.sqlite'
cargo run --locked --package abyss-backend \
  --no-default-features --features sqlite-fts
```

The backend creates the parent directory, database, schema, and search index
on startup. A `sqlite://` prefix is also accepted, for example
`sqlite://./data/abyss.sqlite`.

## Run with PostgreSQL

Start PostgreSQL, then configure the required environment variables:

```bash
export ABYSS_BACKEND_DATABASE_URL='postgres://abyss:abyss@127.0.0.1:5432/abyss?sslmode=disable'
cargo run --locked --package abyss-backend
```

Both profiles listen on `0.0.0.0:8080` and run their embedded migrations by
default. Verify the selected store with:

```bash
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/readyz
```

An authenticated request uses the plaintext token:

```bash
curl \
  -H "Authorization: Bearer ${ABYSS_API_TOKEN}" \
  'http://127.0.0.1:8080/v1/agent-usage/events?limit=20'
```

## API surface

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/healthz` | Process liveness. |
| `GET` | `/readyz` | Selected storage readiness. |
| `POST` | `/v1/agent-usage/events` | Ingest events and correlated diagnostic captures. |
| `GET` | `/v1/agent-usage/events` | Query raw events. |
| `GET` | `/v1/agent-usage/events/{event_pk}` | Read one event. |
| `GET` | `/v1/agent-usage/events/{event_pk}/conversation` | Read one event's conversation, all of it or the asked entries. |
| `GET` | `/v1/agent-usage/events/{event_pk}/location` | Resolve an event to its current session, segment, and exchange. |
| `GET` | `/v1/agent-usage/attachments/{id}` | Download stored image content. |
| `GET` | `/v1/agent-usage/summary` | Aggregate event and token usage. |
| `GET` | `/v1/agent-usage/sessions` | List sessions with their titles, counts, and token totals. |
| `GET` | `/v1/agent-usage/sessions/{session_pk}/header` | Session title, counts, and token totals. |
| `GET` | `/v1/agent-usage/sessions/{session_pk}/streams` | Read the session’s derived segments. |
| `GET` | `/v1/agent-usage/sessions/{session_pk}/turns` | One node per turn: its executors' main-line and side segments. |
| `GET` | `/v1/agent-usage/sessions/{session_pk}/segments/{head}` | Read one segment’s exchanges. |
| `POST` | `/v1/agent-usage/comparison` | Compare token totals and daily activity across dimensions. |
| `GET` | `/v1/agent-usage/search` | Search sessions through Elasticsearch or SQLite. |
| `GET` | `/v1/agent-usage/sessions/{session_pk}/search` | Every hit of a query in one session (SQLite). |

A turn is one user input and all the work it causes; the backend reads only
the `turn_id` an event declares. Turn counts (session list, summary `turns`,
header `turn_count`) are, per session, the number of distinct `turn_id` values
when the session declares any, else the number of requests whose persisted
`starts_turn` fact is true; a summary row applies the same rule to the events
of its group.

Every `/v1/agent-usage/*` endpoint requires the bearer token. Health and
readiness endpoints are unauthenticated for orchestrator probes.

Comparison requests contain `group_by` (`agent`, `model`, `provider`, `platform`,
or `null` for combined activity), contiguous `buckets` of ISO `start`/`end`
timestamps, and optional `filters` arrays for `agent`, `model`, `provider`,
`platform`, and `session`. Values within a filter are ORed; dimensions are ANDed.
A comparison dimension cannot also be filtered. Ranges are start-inclusive and
end-exclusive, limited to 367 days and 400 buckets. Results contain bucket-start
epoch milliseconds, dimension values, summed `total_tokens`, and request counts;
filter options use the same authorized time range before optional filters.

Comparison joins, filters, aggregates, and facet queries use Diesel's typed query
builder for both storage backends. A small `QueryFragment` extension supplies the
bound `VALUES` CTE for time buckets, which Diesel does not provide as a built-in
query source. Bucket values remain bound and result types remain checked.

### Session structure and event links

The header, streams, turns, segment details, in-session search, and event location
share one immutable derived structure for an owner/session/revision. The
process retains up to 128 idle revisions with LRU eviction. Concurrent reads
of the same revision share the load and derivation; a cache hit reads the
session revision without loading stream facts again. Event bodies are not
stored in this cache.

Ingestion advances `agent_sessions.structure_revision` within its existing
session update and event transaction. Structure readers load the revision and
facts from the same database snapshot (a read transaction in SQLite, read-only
repeatable read in PostgreSQL). On a new revision or after eviction/restart,
the first read derives the full structure again. This is a shared query cache,
not an incremental derivation at ingest time.

A request's base is the exchange that emitted the last identity in its
history, when exactly one other exchange did and the model output after that
identity was all said by it. Identities come from the conversation entries:
an entry's `item_id` (`item:<id>`), a `tool_call` entry's `call_id`, and for an
`assistant` or `reasoning` entry with neither and non-blank text, a digest of
that exact text (`text:<role>:<sha256 hex>`). Text identities let a reply that
the protocol gives no id, such as a Chat Completions answer or an Anthropic text
block without thinking, link the next turn; rewritten text links nothing, and
text emitted by more than one exchange is ambiguous.

Stable links retain `event_pk`. The location endpoint returns
`{"session_pk", "event_pk", "segment", "turn", "segment_kind", "exchange"}` using the current structure,
and returns 404 for an event outside the authenticated owner's data. A later
fork can change an event's segment, so links resolve the event when opened.
Segment IDs remain their first exchange IDs; a segment is not a turn number.

`GET /events?locate=true` includes a `location` object on each returned event;
the list projection also preserves it. Session-search matches and the single
event endpoint include the same object: `{segment, turn, segment_kind,
exchange}`. `turn` is the one-based number of the turn of the event's exchange,
in the order the session's turns first appear, or null when the exchange belongs
to no turn; it is for display only. A response page looks up each session structure
once, sharing the cache with the detail view. The dashboard uses these values
in its segment column, search-hit labels, and inspector properties without
sending a separate location request for every row.

### Search

In the SQLite profile, search reads the pieces of each event's
`conversation.entries`: the `text` of every entry, whatever its
role, and its `name`, the tool name of a tool call. Entries are never joined,
an empty value is not a piece, and nothing else is searched. The query is one
literal substring, matched after folding every character to its lowercase
form when that is exactly one character. A hit is a piece of an event whose
folded text holds the folded query, so each event that replays history hits
again. A hit is new when its event is a response, or when its entry index is
at least the request's `new_from`; otherwise it is in the inherited context.

`GET /v1/agent-usage/search?q=…` also takes `from`, `to`, `agent_name`,
`llm_provider`, `llm_model`, `event_type`, `page` (from 1), and `page_size`
(at most 50). Sessions are ordered by their latest matching event, newest
first. Each item carries `match_count`, its matching events,
`new_match_count`, those with a new hit, and up to three `matches`: the events
with a new hit, earliest first, then the earliest events whose hits are all
inherited. A match says whether it is `new` and carries up to two `fragments`
of distinct matching texts, new hits first, each about 240 characters around
the first occurrence, `…` on a cut side, as `{"text", "highlighted"}`
segments.

`GET /v1/agent-usage/sessions/{session_pk}/search?q=…` returns every hit in
one session as `hits`, each `{"event_pk", "entry_index", "field", "new",
"text_id", "segment"}` with `field` `text` or `name` and `segment` the head of
the segment holding the event's exchange, as `/streams` gives it, or null. Hits
are in observation order, then by event, entry, and `name` before `text`.
`texts` lists each named text once as `{"id", "ranges"}`, the non-overlapping
occurrences as half-open ranges of the original text's UTF-16 code units. At
most 100,000 hits are returned; `truncated` says whether more were left out.

The PostgreSQL profile searches through Elasticsearch, requires every
whitespace-separated term to match, counts every match as new, and does not
support search within a session.

## Configuration

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `ABYSS_BACKEND_DATABASE_URL` | yes | — | PostgreSQL URL or SQLite file path, depending on the compiled profile. |
| `ABYSS_BACKEND_API_TOKEN_SHA256` | yes | — | Lowercase, 64-character SHA-256 bearer-token digest. |
| `ABYSS_BACKEND_ADDR` | no | `0.0.0.0:8080` | HTTP listen address. |
| `ABYSS_BACKEND_ENV` | no | `local` | Environment label reported at `/`. |
| `ABYSS_BACKEND_LOG_LEVEL` | no | `info` | Default tracing filter. |
| `ABYSS_BACKEND_DATABASE_POOL_SIZE` | no | `10` | Selected database pool size. |
| `ABYSS_BACKEND_RUN_MIGRATIONS` | no | `true` | Run embedded migrations during startup. |
| `ABYSS_BACKEND_MAX_INGEST_BATCH_SIZE` | no | `1000` | Maximum event count per ingest request. |
| `ABYSS_BACKEND_SUMMARY_SCAN_LIMIT` | no | `100000` | Maximum source rows scanned by a summary query. |
| `ABYSS_BACKEND_DEFAULT_PAGE_SIZE` | no | `100` | Default raw event page size. |
| `ABYSS_BACKEND_ELASTICSEARCH_URL` | no | — | Enables search and the outbox indexer. |
| `ABYSS_BACKEND_ELASTICSEARCH_USERNAME` | no | — | Elasticsearch basic-auth username. |
| `ABYSS_BACKEND_ELASTICSEARCH_PASSWORD` | no | — | Elasticsearch basic-auth password. |
| `ABYSS_BACKEND_SEARCH_REQUEST_TIMEOUT_SECONDS` | no | `10` | Elasticsearch request timeout. |
| `ABYSS_BACKEND_SEARCH_POLL_INTERVAL_MILLISECONDS` | no | `500` | Search outbox polling interval. |
| `ABYSS_BACKEND_SEARCH_BATCH_SIZE` | no | `100` | Search outbox batch size. |

Elasticsearch settings exist only in `postgres-es` builds. Username and
password must be provided together. Search remains disabled in that profile
when no URL is configured; event storage and queries continue to work. The
`sqlite-fts` profile always provides search through its local index.

## Native releases

Version tags publish checksummed `sqlite-fts` executables on the repository's
GitHub Release. The current native targets are:

- `aarch64-apple-darwin` for macOS ARM64.
- `x86_64-unknown-linux-musl` for Linux x86_64.

Each filename contains the tag and Rust target, for example
`abyss-backend-v1.0.0-aarch64-apple-darwin`. `SHA256SUMS` in the same release
authenticates the downloaded bytes. These artifacts are consumed by
`abyss deploy-local`; they do not require Docker, PostgreSQL, Elasticsearch, or
a Rust toolchain on the destination machine.

## Containers and Kubernetes

### Docker

Successful `main` builds publish the public
`docker.io/lexmount/abyss-backend` image. Every build receives an immutable
`sha-<full-git-sha>` tag, while the newest successful `main` build also receives
`latest`. Version tags such as `v1.2.3` additionally publish `v1.2.3`, `1.2.3`,
`1.2`, and `1` tags. Automated deployments should pin the published digest
instead of a mutable tag:

```bash
docker pull 'lexmount/abyss-backend@sha256:<digest>'
```

The following example starts PostgreSQL and `abyss-backend` on a private Docker
network. PostgreSQL data is retained in a named volume, while Elasticsearch
remains disabled.

```bash
export ABYSS_API_TOKEN="$(openssl rand -hex 32)"
export ABYSS_BACKEND_API_TOKEN_SHA256="$(printf '%s' "${ABYSS_API_TOKEN}" | openssl dgst -sha256 -r | cut -d' ' -f1)"

docker network inspect abyss-local >/dev/null 2>&1 || docker network create abyss-local
docker volume inspect abyss-postgres-data >/dev/null 2>&1 || docker volume create abyss-postgres-data

docker run --detach \
  --name abyss-postgres \
  --network abyss-local \
  --restart unless-stopped \
  --env POSTGRES_USER=abyss \
  --env POSTGRES_PASSWORD=abyss \
  --env POSTGRES_DB=abyss \
  --volume abyss-postgres-data:/var/lib/postgresql/data \
  --health-cmd='pg_isready -U abyss -d abyss' \
  --health-interval=2s \
  --health-timeout=5s \
  --health-retries=30 \
  postgres:16

until [ "$(docker inspect --format='{{.State.Health.Status}}' abyss-postgres)" = healthy ]; do
  sleep 1
done

docker build -t abyss-backend:local .

docker run --detach \
  --name abyss-backend \
  --network abyss-local \
  --restart unless-stopped \
  --publish 127.0.0.1:8080:8080 \
  --env ABYSS_BACKEND_DATABASE_URL='postgres://abyss:abyss@abyss-postgres:5432/abyss?sslmode=disable' \
  --env ABYSS_BACKEND_API_TOKEN_SHA256 \
  abyss-backend:local
```

The backend runs its embedded migration during startup. Confirm that both the
process and PostgreSQL are ready:

```bash
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/readyz
```

Keep `ABYSS_API_TOKEN` for Agent configuration. The example database password
is intended only for a host-local deployment; use managed secrets and TLS when
the service is reachable from other machines.

#### Enable search

Session search is optional. For a host-local deployment, start a single-node
Elasticsearch container on the same Docker network before starting the backend:

```bash
docker volume inspect abyss-elasticsearch-data >/dev/null 2>&1 || docker volume create abyss-elasticsearch-data

docker run --detach \
  --name abyss-elasticsearch \
  --network abyss-local \
  --restart unless-stopped \
  --memory=1536m \
  --env discovery.type=single-node \
  --env xpack.security.enabled=false \
  --env ES_JAVA_OPTS='-Xms512m -Xmx512m' \
  --volume abyss-elasticsearch-data:/usr/share/elasticsearch/data \
  docker.elastic.co/elasticsearch/elasticsearch:8.17.0
```

Add the following option to the `docker run` command for `abyss-backend`:

```bash
--env ABYSS_BACKEND_ELASTICSEARCH_URL='http://abyss-elasticsearch:9200'
```

The backend creates the search index and projects existing and newly ingested
events asynchronously. The disabled Elasticsearch security setting above is
only for a host-local Docker network. Private deployments should use HTTPS with
a certificate trusted by the backend image and configure
`ABYSS_BACKEND_ELASTICSEARCH_USERNAME` and
`ABYSS_BACKEND_ELASTICSEARCH_PASSWORD` when authentication is enabled.

### Kubernetes

The `k8s/` Kustomize base expects a Secret named `abyss-backend-secret` with
the two required variables. Create it before applying the manifests:

```bash
kubectl create secret generic abyss-backend-secret \
  --from-literal=ABYSS_BACKEND_DATABASE_URL='postgres://user:password@postgres:5432/abyss' \
  --from-literal=ABYSS_BACKEND_API_TOKEN_SHA256="${ABYSS_BACKEND_API_TOKEN_SHA256}"
kubectl apply -k k8s
```

Patch the image in an environment overlay or with Kustomize rather than using
the example `:latest` reference for production promotion.

## Development

```bash
make check
make test-blackbox
make test-blackbox-sqlite
make docker-build
make k8s-render
```

`make test-blackbox` runs the same API contract against both storage profiles.
The PostgreSQL case starts an ephemeral container and an Elasticsearch contract
double; the SQLite case uses only a temporary local database file. Both verify
schema creation, authentication, event ingestion, attachment retrieval, raw
diagnostic retention, summary, streams, and search behavior.
