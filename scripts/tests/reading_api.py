#!/usr/bin/env python3
"""Real HTTP regression contract for revision-checked session reading.

Set READING_TEST_URL to the agent-usage API prefix and READING_TEST_TOKEN to an
owner bearer credential. The test ingests isolated fixture sessions; no database
reads or internal application mocks are used.
"""
import hashlib
import json
import os
import urllib.error
import urllib.parse
import urllib.request
import uuid


def request(url, token, body=None, status=200, headers=None, method=None):
    outgoing = {"Accept": "application/json, text/event-stream"}
    if token:
        outgoing["Authorization"] = "Bearer " + token
    if body is not None:
        outgoing["Content-Type"] = "application/json"
    outgoing.update(headers or {})
    data = body if isinstance(body, bytes) else json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, headers=outgoing, method=method)
    try:
        response = urllib.request.urlopen(req, timeout=30)
    except urllib.error.HTTPError as error:
        response = error
    payload = response.read()
    assert response.status == status, (url, response.status, status, payload[:300])
    if status >= 400 and "application/json" not in response.headers.get("Content-Type", ""):
        return {"http_error": payload.decode(errors="replace")}
    return json.loads(payload) if payload else None


def fixture(session, key, second, side, conversation):
    value = {
        "event_id": session + key, "session_id": session,
        "observed_at": f"2026-11-01T00:00:{second:02d}Z",
        "device": {"host_name": "mcp-review", "platform": "linux", "os_version": "test"},
        "agent": {"name": "codex", "version": "reading-test"},
        "llm": {"provider": "openai", "model": "reading-test"},
        "event_type": side, "response_id": session + key,
        "token_usage": {"input_tokens": 4, "output_tokens": 2},
        "metadata": {"omitted": ["images"], "path": "/v1/responses", "purpose": "agent"},
    }
    if conversation is not None:
        value["conversation"] = conversation
    return value


def run(base, token):
    session = "productmcpreview-" + uuid.uuid4().hex
    conversation = {"completeness": "partial", "entries": [
        {"role": "user", "text": session + " decision history"},
        {"role": "tool_call", "name": "shell", "call_id": "call-1", "text": "echo test", "arguments": {"path": "a/b"}},
        {"role": "tool_result", "call_id": "call-1", "text": "a🦀é\n\"\\" * 8000},
    ]}
    events = [fixture(session, "-later", 20, "response", None), fixture(session, "-first", 10, "request", conversation)]
    accepted = request(base + "/events", token, {"events": events})
    assert accepted["accepted"] == 2 and accepted["rejected"] == 0
    listing = request(base + "/sessions?page_size=100", token)
    stored = next(item for item in listing["items"] if item["session_id"] == session)
    session_pk = stored["session_pk"]
    path = base + "/sessions/" + session_pk
    directory = request(path + "/directory?limit=1", token)
    assert directory["total_events"] == 2 and directory["events"][0]["event_id"] == session + "-first"
    second = request(path + "/directory?" + urllib.parse.urlencode({"offset": 1, "revision": directory["revision"]}), token)
    assert second["events"][0]["event_id"] == session + "-later" and second["next_offset"] is None
    query = {"revision": directory["revision"], "max_bytes": 4097}
    first_page = request(path + "/read?" + urllib.parse.urlencode(query), token)
    assert not first_page["event_complete"] and first_page["capture"]["omitted"] == ["images"]
    accumulated, reconstructed, chunks = "", [], 0
    while True:
        page = request(path + "/read?" + urllib.parse.urlencode(query), token)
        assert page["byte_offset"] == len(accumulated.encode())
        assert len(page["content_json"].encode()) <= 4097
        accumulated += page["content_json"]
        chunks += 1
        if page["event_complete"]:
            assert len(accumulated.encode()) == page["content_bytes"]
            assert hashlib.sha256(accumulated.encode()).hexdigest() == page["content_sha256"]
            event = json.loads(accumulated)
            assert event["id"] == page["source"]["event_pk"]
            reconstructed.append(event)
            accumulated = ""
        if page["session_complete"]:
            assert page["next_cursor"] is None
            break
        assert page["next_cursor"]
        query = {"cursor": page["next_cursor"], "max_bytes": 4097}
    assert len(reconstructed) == 2
    assert reconstructed[0]["conversation"] == conversation
    assert "conversation" not in reconstructed[1]
    assert page["capture"]["conversation_present"] is False
    for suffix in ("/directory?limit=0", "/directory?offset=3", "/read?max_bytes=3", "/read?event_index=2", "/read?cursor=bad"):
        request(path + suffix, token, status=400)
    request(base + "/sessions/" + str(uuid.uuid4()) + "/directory", token, status=404)
    request(path + "/directory", None, status=401)
    late = fixture(session, "-inserted", 5, "request", {"entries": [], "completeness": "complete"})
    request(base + "/events", token, {"events": [late]})
    stale = request(path + "/read?" + urllib.parse.urlencode({"cursor": first_page["next_cursor"]}), token, status=400)
    assert "session_changed" in stale["error"]
    request(path + "/directory?" + urllib.parse.urlencode({"revision": directory["revision"]}), token, status=400)
    print(json.dumps({"suite": "reading-http", "session_pk": session_pk, "chunks": chunks, "result": "passed"}))
    return session_pk, session


if __name__ == "__main__":
    run(os.environ["READING_TEST_URL"], os.environ["READING_TEST_TOKEN"])
