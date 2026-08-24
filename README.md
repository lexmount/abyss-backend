# abyss-backend

`abyss-backend` is the open-source, self-hostable Agent event store for Abyss.
It accepts normalized Agent events, stores them in PostgreSQL, and exposes APIs
for event history, summaries, session timelines, attachments, and optional
full-text session search.

This repository is intentionally focused on standalone event storage, queries,
and optional full-text search.

## Capabilities

- Idempotent Agent event ingestion and raw event queries.
- Token-usage summaries and ordered session timelines.
- Validated image attachment storage and authorized download.
- Optional asynchronous Elasticsearch projection and session search.
- Health and PostgreSQL readiness probes.
- A consolidated PostgreSQL schema for new standalone deployments.

PostgreSQL is the only supported database in this migration step. SQLite is a
future addition and is not implemented here.

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

## Run locally

Start PostgreSQL, then configure the required environment variables:

```bash
export ABYSS_BACKEND_DATABASE_URL='postgres://abyss:abyss@127.0.0.1:5432/abyss?sslmode=disable'
cargo run --locked --package abyss-backend
```

The service listens on `0.0.0.0:8080` and runs its embedded migration by
default. Verify it with:

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
| `GET` | `/readyz` | PostgreSQL readiness. |
| `POST` | `/v1/agent-usage/events` | Ingest events and correlated diagnostic captures. |
| `GET` | `/v1/agent-usage/events` | Query raw events. |
| `GET` | `/v1/agent-usage/attachments/{id}` | Download stored image content. |
| `GET` | `/v1/agent-usage/summary` | Aggregate event and token usage. |
| `GET` | `/v1/agent-usage/sessions/{id}` | Read an ordered session timeline. |
| `GET` | `/v1/agent-usage/search` | Search sessions when Elasticsearch is configured. |

Every `/v1/agent-usage/*` endpoint requires the bearer token. Health and
readiness endpoints are unauthenticated for orchestrator probes.

## Configuration

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `ABYSS_BACKEND_DATABASE_URL` | yes | — | PostgreSQL connection URL. |
| `ABYSS_BACKEND_API_TOKEN_SHA256` | yes | — | Lowercase, 64-character SHA-256 bearer-token digest. |
| `ABYSS_BACKEND_ADDR` | no | `0.0.0.0:8080` | HTTP listen address. |
| `ABYSS_BACKEND_ENV` | no | `local` | Environment label reported at `/`. |
| `ABYSS_BACKEND_LOG_LEVEL` | no | `info` | Default tracing filter. |
| `ABYSS_BACKEND_DATABASE_POOL_SIZE` | no | `10` | PostgreSQL pool size. |
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

Elasticsearch username and password must be provided together. Search remains
disabled when no URL is configured; event storage and queries continue to
work.

## Containers and Kubernetes

### Docker

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
make docker-build
make k8s-render
```

The black-box test starts an ephemeral PostgreSQL container and an
Elasticsearch contract double, then verifies the consolidated schema,
authentication, event ingestion, attachment retrieval, raw diagnostic
retention, summary, timeline, and search behavior.
