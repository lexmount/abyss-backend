#!/usr/bin/env python3
"""Verify structured stdout through real service processes and HTTP interfaces.

Build the selected binary first. PostgreSQL runs against a disposable database
created below TEST_POSTGRES_ADMIN_URL; --sqlite uses a temporary SQLite file.
No internal implementation or HTTP handler is mocked.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid


def request(base, path, method="GET", body=None):
    headers = {"Authorization": "Bearer SECRET_AUTH", "Cookie": "abyss_session=SECRET_COOKIE", "x-request-id": "SECRET_CLIENT", "Content-Type": "application/json"}
    req = urllib.request.Request(base + path, body, headers, method=method)
    try:
        response = urllib.request.urlopen(req, timeout=10)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        response.read()
        return response.status, response.headers.get("x-request-id")


def check_process(binary, service, env, folder, level, format_value):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    prefix = {"abyss-identity-service": "ABYSS_IDENTITY", "abyss-agent-data-service": "ABYSS_AGENT_DATA", "abyss-update-service": "ABYSS_UPDATE", "abyss-backend": "ABYSS_BACKEND"}[service]
    env = dict(env, **{prefix + "_ADDR": f"127.0.0.1:{port}", "RUST_LOG": level})
    env.pop("ABYSS_LOG_FORMAT", None)
    if format_value is not None:
        env["ABYSS_LOG_FORMAT"] = format_value
    log = folder / f"{level}-{format_value}.log"
    with log.open("w") as output:
        process = subprocess.Popen([str(binary)], env=env, stdout=output, stderr=subprocess.STDOUT)
        try:
            if format_value == "invalid":
                assert process.wait(timeout=15) != 0, "invalid log format must fail startup"
                assert "ABYSS_LOG_FORMAT must be text or json" in log.read_text()
                return
            base = f"http://127.0.0.1:{port}"
            for _ in range(300):
                if process.poll() is not None:
                    raise AssertionError("service failed to start: " + log.read_text())
                try:
                    if request(base, "/healthz")[0] == 200:
                        break
                except OSError:
                    pass
                time.sleep(.1)
            else:
                raise AssertionError("service startup timed out")
            observed = []
            with ThreadPoolExecutor(max_workers=8) as pool:
                observed.extend(pool.map(lambda _: request(base, "/SECRET_PATH?code=SECRET_QUERY"), range(8)))
            observed.append(request(base, "/", "POST", b'{"secret":"SECRET_BODY"}'))
            if service == "abyss-agent-data-service":
                observed.append(request(base, "/api/v1/agent-usage/handoffs/SECRET_CAPABILITY/consume", "POST", b"{}"))
            elif service == "abyss-update-service":
                observed.append(request(base, "/api/v1/updates/releases", "POST", b"{}"))
            elif service == "abyss-backend":
                observed.append(request(base, "/v1/agent-usage/events?token=SECRET_QUERY"))
            else:
                observed.append(request(base, "/api/auth/session?code=SECRET_QUERY"))
            root_status, root_id = request(base, "/")
            observed.append((root_status, root_id))
            assert root_status == 200
            assert all(value == 404 for value, _ in observed[:8])
            assert len({ident for _, ident in observed}) == len(observed), "IDs must be unique"
            for _, ident in observed:
                uuid.UUID(ident)
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
                process.wait(timeout=10)
    text = log.read_text()
    assert "SECRET_" not in text, "raw request data leaked into logs"
    if format_value != "json":
        assert "request completed" in text, "text mode remains supported"
        return
    records = [json.loads(line) for line in text.splitlines()]
    assert records, "logs must not be empty"
    assert all(record["service"] == service and record["environment"] == "test" for record in records)
    completions = {record["request_id"]: record for record in records if record.get("message") == "request completed"}
    for status, ident in observed:
        if level == "error" and status < 500:
            assert ident not in completions
            continue
        if level == "warn" and status < 400:
            assert ident not in completions
            continue
        event = completions[ident]
        assert event["status"] == status
        assert isinstance(event["duration_ms"], int)
        assert event["method"] in ("GET", "POST")
    assert not any(record.get("route") in ("/healthz", "/readyz") for record in records)
    assert all(completions[ident]["route"] == "<unmatched>" for _, ident in observed[:8] if ident in completions)
    if service == "abyss-agent-data-service":
        status, ident = observed[-2]
        assert status == 503, "real Identity connection failure returns 503"
        linked = [record for record in records if record.get("request_id") == ident]
        assert any(record["route"].endswith("/{handoff_token}/consume") for record in linked)
    print(service, level, format_value, "passed", len(records), "records")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--service", required=True)
    parser.add_argument("--sqlite", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    binary = root / "target/debug" / args.service
    admin = os.environ.get("TEST_POSTGRES_ADMIN_URL")
    db = "cls_test_" + uuid.uuid4().hex
    with tempfile.TemporaryDirectory(prefix="cls-logging-") as folder:
        folder = Path(folder)
        if args.sqlite:
            database = str(folder / "backend.sqlite")
        else:
            if not admin:
                parser.error("TEST_POSTGRES_ADMIN_URL is required without --sqlite")
            subprocess.run(["psql", admin, "-v", "ON_ERROR_STOP=1", "-qc", "CREATE DATABASE " + db], check=True)
            database = admin.rsplit("/", 1)[0] + "/" + db
        env = {key: value for key, value in os.environ.items() if not key.startswith("ABYSS_")}
        for prefix in ("ABYSS_IDENTITY", "ABYSS_AGENT_DATA", "ABYSS_UPDATE", "ABYSS_BACKEND"):
            env.update({prefix + "_DATABASE_URL": database, prefix + "_ENV": "test", prefix + "_RUN_MIGRATIONS": "true"})
        env["ABYSS_AGENT_DATA_ELASTICSEARCH_URL"] = "http://127.0.0.1:9"
        env.update({"ABYSS_IDENTITY_SSO_REQUIRED": "false", "ABYSS_IDENTITY_INTERNAL_SERVICE_TOKEN": "logging-test-service-token-0123456789", "ABYSS_BACKEND_API_TOKEN_SHA256": hashlib.sha256(b"test-token").hexdigest()})
        for prefix in ("ABYSS_AGENT_DATA", "ABYSS_UPDATE"):
            env[prefix + "_IDENTITY_BASE_URL"] = "http://127.0.0.1:9"
            env[prefix + "_IDENTITY_SERVICE_TOKEN"] = "logging-test-service-token-0123456789"
        try:
            for level in ("info", "warn"):
                check_process(binary, args.service, env, folder, level, "json")
            if args.service == "abyss-agent-data-service":
                check_process(binary, args.service, env, folder, "error", "json")
            for value in (None, "text", "invalid"):
                check_process(binary, args.service, env, folder, "info", value)
        finally:
            if not args.sqlite:
                subprocess.run(["psql", admin, "-v", "ON_ERROR_STOP=1", "-qc", "DROP DATABASE " + db + " WITH (FORCE)"], check=True)
    print(args.service, "logging process black-box passed")


if __name__ == "__main__":
    main()
