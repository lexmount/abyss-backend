#!/usr/bin/env bash

set -euo pipefail

for command_name in cargo curl python3; do
  command -v "${command_name}" >/dev/null || {
    echo "${command_name} is required for the SQLite black-box test." >&2
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
TEMP_DIR="$(mktemp -d -t abyss-backend-sqlite-blackbox.XXXXXX)"
DATABASE_PATH="${TEMP_DIR}/abyss.sqlite"
SERVER_LOG="${TEMP_DIR}/backend.log"
SERVER_PID=""

cleanup() {
  local status=$?
  if [[ "${status}" -ne 0 && -f "${SERVER_LOG}" ]]; then
    echo "abyss-backend SQLite log:" >&2
    tail -n 200 "${SERVER_LOG}" >&2 || true
  fi
  if [[ -n "${SERVER_PID}" ]]; then
    kill "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  rm -r "${TEMP_DIR}"
}
trap cleanup EXIT

fail() {
  echo "blackbox SQLite: $*" >&2
  exit 1
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

ABYSS_BACKEND_ADDR="${LISTEN_ADDR}" \
ABYSS_BACKEND_ENV="blackbox" \
ABYSS_BACKEND_API_TOKEN_SHA256="${API_TOKEN_HASH}" \
ABYSS_BACKEND_DATABASE_URL="${DATABASE_PATH}" \
ABYSS_BACKEND_RUN_MIGRATIONS="true" \
cargo run --locked --package abyss-backend --no-default-features --features sqlite-fts --quiet \
  >"${SERVER_LOG}" 2>&1 &
SERVER_PID=$!
wait_for_backend

python3 - "${DATABASE_PATH}" <<'PY'
import sqlite3
import sys

database = sqlite3.connect(sys.argv[1])
tables = {
    row[0]
    for row in database.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
    )
}
expected = {
    "app_users",
    "devices",
    "agent_sessions",
    "agent_turns",
    "llm_usage_events",
    "llm_usage_event_attachments",
    "agent_diagnostic_captures",
    "agent_diagnostic_capture_events",
    "usage_events_fts",
}
missing = expected - tables
if missing:
    raise SystemExit(f"SQLite migration is missing tables: {sorted(missing)}")
PY

python3 scripts/tests/blackbox_api.py \
  --base-url "${BASE_URL}" \
  --token "${API_TOKEN}" \
  --run-id "${RUN_ID}"

python3 - "${DATABASE_PATH}" "capture-${RUN_ID}" <<'PY'
import sqlite3
import sys

database = sqlite3.connect(sys.argv[1])
capture_id = sys.argv[2]
capture_count = database.execute(
    """SELECT count(*)
       FROM agent_diagnostic_captures
       WHERE capture_id = ?
         AND json_extract(payload, '$.request_plaintext') = 'diagnostic request'
         AND json_extract(payload, '$.response_plaintext') = 'diagnostic response'""",
    (capture_id,),
).fetchone()[0]
if capture_count != 1:
    raise SystemExit("diagnostic capture payload was not retained")
link_count = database.execute(
    """SELECT count(*)
       FROM agent_diagnostic_capture_events links
       JOIN agent_diagnostic_captures captures ON captures.id = links.capture_pk
       WHERE captures.capture_id = ?""",
    (capture_id,),
).fetchone()[0]
if link_count != 2:
    raise SystemExit("diagnostic capture was not linked to both events")
PY

echo "blackbox: standalone SQLite, HTTP, attachment, diagnostics, and FTS contracts passed"
