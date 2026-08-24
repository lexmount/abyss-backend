#!/usr/bin/env bash

set -euo pipefail

for command_name in cargo curl python3; do
  command -v "${command_name}" >/dev/null || {
    echo "${command_name} is required for the abyss-backend black-box test." >&2
    exit 2
  }
done

if [[ -n "${ABYSS_BACKEND_BLACKBOX_ADDR:-}" ]]; then
  LISTEN_ADDR="${ABYSS_BACKEND_BLACKBOX_ADDR}"
else
  BACKEND_PORT="$(python3 -c 'import socket; server = socket.socket(); server.bind(("127.0.0.1", 0)); print(server.getsockname()[1]); server.close()')"
  LISTEN_ADDR="127.0.0.1:${BACKEND_PORT}"
fi
BASE_URL="http://${LISTEN_ADDR}"
RUN_ID="$(date -u +%Y%m%d%H%M%S)-$$"
API_TOKEN="blackbox-token-${RUN_ID}"
API_TOKEN_HASH="$(python3 -c 'import hashlib, sys; print(hashlib.sha256(sys.argv[1].encode()).hexdigest())' "${API_TOKEN}")"
TEMP_DIR="$(mktemp -d -t abyss-backend-blackbox.XXXXXX)"
SERVER_LOG="${TEMP_DIR}/backend.log"
ELASTICSEARCH_LOG="${TEMP_DIR}/elasticsearch.log"
ELASTICSEARCH_READY_FILE="${TEMP_DIR}/elasticsearch.ready"
CARGO_FEATURES="${ABYSS_BACKEND_BLACKBOX_CARGO_FEATURES:-blackbox-bundled-pq}"
SERVER_PID=""
ELASTICSEARCH_PID=""
POSTGRES_CONTAINER=""

print_logs() {
  if [[ -f "${SERVER_LOG}" ]]; then
    echo "abyss-backend log:" >&2
    tail -n 200 "${SERVER_LOG}" >&2 || true
  fi
  if [[ -f "${ELASTICSEARCH_LOG}" ]]; then
    echo "Elasticsearch contract-double log:" >&2
    tail -n 200 "${ELASTICSEARCH_LOG}" >&2 || true
  fi
  if [[ -n "${POSTGRES_CONTAINER}" ]]; then
    echo "PostgreSQL container log:" >&2
    docker logs --tail 200 "${POSTGRES_CONTAINER}" >&2 || true
  fi
}

cleanup() {
  local status=$?
  if [[ "${status}" -ne 0 ]]; then
    print_logs
  fi
  if [[ -n "${SERVER_PID}" ]]; then
    kill "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  if [[ -n "${ELASTICSEARCH_PID}" ]]; then
    kill "${ELASTICSEARCH_PID}" 2>/dev/null || true
    wait "${ELASTICSEARCH_PID}" 2>/dev/null || true
  fi
  if [[ -n "${POSTGRES_CONTAINER}" ]]; then
    docker rm -f "${POSTGRES_CONTAINER}" >/dev/null 2>&1 || true
  fi
  rm -r "${TEMP_DIR}"
}
trap cleanup EXIT

fail() {
  echo "blackbox: $*" >&2
  exit 1
}

start_postgres_if_needed() {
  if [[ -n "${ABYSS_BACKEND_DATABASE_URL:-}" ]]; then
    command -v psql >/dev/null || fail "psql is required with an external ABYSS_BACKEND_DATABASE_URL"
    return
  fi

  command -v docker >/dev/null || fail "docker is required when ABYSS_BACKEND_DATABASE_URL is not set"
  docker info >/dev/null 2>&1 || fail "the Docker daemon is not available"

  local image="${ABYSS_BACKEND_BLACKBOX_POSTGRES_IMAGE:-postgres:16}"
  local postgres_port=""
  POSTGRES_CONTAINER="abyss-backend-blackbox-postgres-${RUN_ID}"
  docker run --rm \
    --name "${POSTGRES_CONTAINER}" \
    -e POSTGRES_USER=abyss \
    -e POSTGRES_PASSWORD=abyss \
    -e POSTGRES_DB=abyss \
    -p 127.0.0.1::5432 \
    -d "${image}" >/dev/null || fail "could not start PostgreSQL"

  for _attempt in $(seq 1 60); do
    if docker exec "${POSTGRES_CONTAINER}" pg_isready -U abyss -d abyss >/dev/null 2>&1; then
      postgres_port="$(docker port "${POSTGRES_CONTAINER}" 5432/tcp | sed -n 's/.*:\([0-9][0-9]*\)$/\1/p' | head -n 1)"
      break
    fi
    sleep 1
  done
  [[ -n "${postgres_port}" ]] || fail "PostgreSQL did not become ready"
  export ABYSS_BACKEND_DATABASE_URL="postgres://abyss:abyss@127.0.0.1:${postgres_port}/abyss?sslmode=disable"
}

query_database() {
  local sql=$1
  if [[ -n "${POSTGRES_CONTAINER}" ]]; then
    docker exec "${POSTGRES_CONTAINER}" psql -U abyss -d abyss -Atq -v ON_ERROR_STOP=1 -c "${sql}"
  else
    psql "${ABYSS_BACKEND_DATABASE_URL}" -Atq -v ON_ERROR_STOP=1 -c "${sql}"
  fi
}

wait_for_file() {
  local path=$1
  local process_id=$2
  for _attempt in $(seq 1 100); do
    [[ -s "${path}" ]] && return
    kill -0 "${process_id}" 2>/dev/null || fail "process ${process_id} exited before writing ${path}"
    sleep 0.05
  done
  fail "timed out waiting for ${path}"
}

wait_for_backend() {
  for _attempt in $(seq 1 180); do
    if curl -fsS "${BASE_URL}/readyz" >/dev/null 2>&1; then
      return
    fi
    kill -0 "${SERVER_PID}" 2>/dev/null || fail "abyss-backend exited before becoming ready"
    sleep 1
  done
  fail "abyss-backend did not become ready at ${BASE_URL}"
}

start_postgres_if_needed

python3 scripts/tests/mock_elasticsearch.py \
  --listen 127.0.0.1:0 \
  --ready-file "${ELASTICSEARCH_READY_FILE}" \
  >"${ELASTICSEARCH_LOG}" 2>&1 &
ELASTICSEARCH_PID=$!
wait_for_file "${ELASTICSEARCH_READY_FILE}" "${ELASTICSEARCH_PID}"
ELASTICSEARCH_URL="$(<"${ELASTICSEARCH_READY_FILE}")"

CARGO_RUN_ARGS=(run --locked --package abyss-backend --quiet)
if [[ -n "${CARGO_FEATURES}" ]]; then
  CARGO_RUN_ARGS+=(--features "${CARGO_FEATURES}")
fi

ABYSS_BACKEND_ADDR="${LISTEN_ADDR}" \
ABYSS_BACKEND_ENV="blackbox" \
ABYSS_BACKEND_API_TOKEN_SHA256="${API_TOKEN_HASH}" \
ABYSS_BACKEND_ELASTICSEARCH_URL="${ELASTICSEARCH_URL}" \
ABYSS_BACKEND_SEARCH_POLL_INTERVAL_MILLISECONDS="50" \
ABYSS_BACKEND_RUN_MIGRATIONS="true" \
cargo "${CARGO_RUN_ARGS[@]}" >"${SERVER_LOG}" 2>&1 &
SERVER_PID=$!
wait_for_backend

EXPECTED_TABLES="$(query_database "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('app_users', 'devices', 'agent_sessions', 'agent_turns', 'llm_usage_events', 'llm_usage_event_attachments', 'agent_diagnostic_captures', 'agent_diagnostic_capture_events', 'search_outbox');")"
[[ "${EXPECTED_TABLES}" == "9" ]] || fail "the consolidated migration did not create all event-store tables"

PRODUCT_TABLES="$(query_database "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('sso_identities', 'auth_sessions', 'oauth_login_requests', 'native_auth_sessions', 'terminal_login_attempts', 'context_handoffs', 'session_shares', 'shared_session_handoffs', 'update_release_manifests');")"
[[ "${PRODUCT_TABLES}" == "0" ]] || fail "the consolidated migration created retired product tables"

python3 scripts/tests/blackbox_api.py \
  --base-url "${BASE_URL}" \
  --token "${API_TOKEN}" \
  --run-id "${RUN_ID}"

CAPTURE_COUNT="$(query_database "SELECT count(*) FROM agent_diagnostic_captures WHERE capture_id = 'capture-${RUN_ID}' AND payload ->> 'request_plaintext' = 'diagnostic request' AND payload ->> 'response_plaintext' = 'diagnostic response';")"
[[ "${CAPTURE_COUNT}" == "1" ]] || fail "diagnostic capture payload was not retained"

CAPTURE_LINK_COUNT="$(query_database "SELECT count(*) FROM agent_diagnostic_capture_events links JOIN agent_diagnostic_captures captures ON captures.id = links.capture_pk WHERE captures.capture_id = 'capture-${RUN_ID}';")"
[[ "${CAPTURE_LINK_COUNT}" == "2" ]] || fail "diagnostic capture was not linked to both events"

echo "blackbox: standalone PostgreSQL, HTTP, attachment, diagnostics, and search contracts passed"
