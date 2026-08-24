#!/usr/bin/env python3
"""Exercise the standalone abyss-backend HTTP contract against real storage."""

from __future__ import annotations

import argparse
import base64
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


OWNER_ID = "00000000-0000-0000-0000-000000000001"
IMAGE_BASE64 = "iVBORw0KGgpibGFja2JveC1pbWFnZQ=="
IMAGE_SHA256 = "db2c0374aa140d829781122a7dc3434cfb1ee17de1f57738e14656d633747b4a"


@dataclass(frozen=True)
class HttpResponse:
    status: int
    headers: Any
    body: bytes

    def json(self) -> dict[str, Any]:
        value = json.loads(self.body)
        require(isinstance(value, dict), "HTTP response must contain a JSON object")
        return value


def request(
    base_url: str,
    method: str,
    path: str,
    *,
    token: str | None = None,
    json_body: dict[str, Any] | None = None,
) -> HttpResponse:
    headers = {}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    data = None
    if json_body is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(json_body, separators=(",", ":")).encode()
    http_request = urllib.request.Request(
        f"{base_url}{path}", data=data, headers=headers, method=method
    )
    try:
        with urllib.request.urlopen(http_request, timeout=10) as response:
            return HttpResponse(response.status, response.headers, response.read())
    except urllib.error.HTTPError as error:
        return HttpResponse(error.code, error.headers, error.read())


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def require_status(response: HttpResponse, expected: int, context: str) -> None:
    require(
        response.status == expected,
        f"{context}: expected HTTP {expected}, got {response.status}: {response.body!r}",
    )


def event(
    event_id: str,
    session_id: str,
    event_type: str,
    text: str,
    token_usage: dict[str, int],
    run_id: str,
) -> dict[str, Any]:
    value: dict[str, Any] = {
        "event_id": event_id,
        "observed_at": "2026-08-24T08:00:00Z",
        "device": {
            "host_name": "blackbox-host",
            "platform": "linux",
            "os_version": "blackbox",
        },
        "agent": {"name": "Codex", "version": "blackbox"},
        "session_id": session_id,
        "turn_index": 1,
        "llm": {"provider": "OpenAI", "model": "gpt-5.5"},
        "event_type": event_type,
        "text": text,
        "token_usage": token_usage,
        "metadata": {
            "test_run": run_id,
            "response_id": f"response-{run_id}",
            "previous_response_id": None,
            "provider_call_index": 1,
        },
    }
    if event_type == "request":
        value["attachments"] = [
            {
                "position": 0,
                "media_type": "image/png",
                "byte_size": 22,
                "sha256": IMAGE_SHA256,
                "content_base64": IMAGE_BASE64,
            }
        ]
    return value


def run(base_url: str, token: str, run_id: str) -> None:
    session_id = f"session-blackbox-{run_id}"
    request_event_id = f"event-request-{run_id}"
    response_event_id = f"event-response-{run_id}"
    capture_id = f"capture-{run_id}"

    health = request(base_url, "GET", "/healthz")
    require_status(health, 200, "health check")
    require(health.json()["status"] == "ok", "health check must report ok")

    unauthenticated = request(base_url, "GET", "/v1/agent-usage/events")
    require_status(unauthenticated, 401, "unauthenticated event query")
    wrong_token = request(
        base_url, "GET", "/v1/agent-usage/events", token="wrong-token"
    )
    require_status(wrong_token, 401, "incorrect bearer token")

    retired_routes = (
        ("GET", "/auth/login"),
        ("GET", "/v1/updates/check"),
        (
            "POST",
            "/v1/agent-usage/sessions/00000000-0000-0000-0000-000000000001/shares",
        ),
        ("POST", "/v1/agent-usage/handoffs/00000000-0000-0000-0000-000000000001/consume"),
    )
    for method, path in retired_routes:
        require_status(
            request(base_url, method, path, token=token),
            404,
            f"retired route {method} {path}",
        )

    ingest_payload = {
        "events": [
            event(
                request_event_id,
                session_id,
                "request",
                "blackbox request",
                {"input_tokens": 12, "cache_read_tokens": 3},
                run_id,
            ),
            event(
                response_event_id,
                session_id,
                "response",
                "blackbox response",
                {"output_tokens": 21, "reasoning_tokens": 4, "total_tokens": 25},
                run_id,
            ),
        ],
        "diagnostic_captures": [
            {
                "capture_id": capture_id,
                "captured_at": "2026-08-24T08:00:00Z",
                "flow_id": f"flow-{run_id}",
                "event_ids": [request_event_id, response_event_id],
                "collector_version": "blackbox",
                "payload": {
                    "transport": "http_exchange",
                    "request_plaintext": "diagnostic request",
                    "response_plaintext": "diagnostic response",
                },
            }
        ],
    }
    first_ingest = request(
        base_url,
        "POST",
        "/v1/agent-usage/events",
        token=token,
        json_body=ingest_payload,
    )
    require_status(first_ingest, 200, "first event ingest")
    first_result = first_ingest.json()
    require(first_result["accepted"] == 2, "first ingest must accept both events")
    require(first_result["duplicates"] == 0, "first ingest must have no duplicates")
    require(
        first_result["accepted_diagnostic_captures"] == 1,
        "first ingest must persist the diagnostic capture",
    )

    duplicate_ingest = request(
        base_url,
        "POST",
        "/v1/agent-usage/events",
        token=token,
        json_body=ingest_payload,
    )
    require_status(duplicate_ingest, 200, "duplicate event ingest")
    duplicate_result = duplicate_ingest.json()
    require(duplicate_result["accepted"] == 0, "duplicate ingest must accept no events")
    require(duplicate_result["duplicates"] == 2, "duplicate ingest must report both events")
    require(
        duplicate_result["duplicate_diagnostic_captures"] == 1,
        "duplicate ingest must report the diagnostic capture",
    )

    raw_path = "/v1/agent-usage/events?" + urllib.parse.urlencode(
        {"session_id": session_id, "limit": 10}
    )
    raw_response = request(base_url, "GET", raw_path, token=token)
    require_status(raw_response, 200, "raw event query")
    raw_events = raw_response.json()["events"]
    require(len(raw_events) == 2, "raw event query must return two events")
    by_event_id = {item["event_id"]: item for item in raw_events}
    require(
        set(by_event_id) == {request_event_id, response_event_id},
        "raw event query returned unexpected events",
    )
    require(
        all(item["user_id"] == OWNER_ID for item in raw_events),
        "events must belong to the standalone owner",
    )
    require(
        IMAGE_BASE64.encode() not in raw_response.body,
        "raw event responses must not expose attachment content",
    )

    attachments = by_event_id[request_event_id]["attachments"]
    require(len(attachments) == 1, "request event must expose one attachment record")
    require(attachments[0]["content_available"], "attachment bytes must be persisted")
    attachment = request(
        base_url,
        "GET",
        f"/v1/agent-usage/attachments/{attachments[0]['id']}",
        token=token,
    )
    require_status(attachment, 200, "attachment download")
    require(
        attachment.body == base64.b64decode(IMAGE_BASE64),
        "downloaded attachment bytes must match the ingest payload",
    )
    require(
        attachment.headers.get_content_type() == "image/png",
        "attachment media type must be preserved",
    )
    require(
        attachment.headers.get("Cache-Control") == "private, no-store",
        "attachment response must disable shared caching",
    )

    summary_path = "/v1/agent-usage/summary?" + urllib.parse.urlencode(
        {"group_by": "user,device,agent,provider,model", "session_id": session_id}
    )
    summary_response = request(base_url, "GET", summary_path, token=token)
    require_status(summary_response, 200, "usage summary")
    rows = summary_response.json()["rows"]
    require(len(rows) == 1, "usage summary must contain one aggregate row")
    summary = rows[0]
    require(summary["user_id"] == OWNER_ID, "summary must belong to the owner")
    require(summary["agent_name"] == "codex", "summary must normalize Agent names")
    require(summary["llm_provider"] == "openai", "summary must normalize providers")
    require(summary["requests"] == 1 and summary["responses"] == 1, "summary event counts are incorrect")
    require(summary["total_tokens"] == 40, "summary token total is incorrect")

    session_pk = raw_events[0]["session_pk"]
    timeline = request(
        base_url,
        "GET",
        f"/v1/agent-usage/sessions/{session_pk}",
        token=token,
    )
    require_status(timeline, 200, "session timeline")
    timeline_events = [
        item
        for turn in timeline.json()["turns"]
        for item in turn["events"]
    ]
    require(
        [item["event_id"] for item in timeline_events]
        == [request_event_id, response_event_id],
        "session timeline must preserve request-before-response ordering",
    )

    search_path = "/v1/agent-usage/search?" + urllib.parse.urlencode(
        {"q": "blackbox response", "agent_name": "codex"}
    )
    search_result: dict[str, Any] = {}
    for _attempt in range(100):
        search_response = request(base_url, "GET", search_path, token=token)
        require_status(search_response, 200, "session search")
        search_result = search_response.json()
        if any(item["session_pk"] == session_pk for item in search_result["items"]):
            break
        time.sleep(0.1)
    else:
        raise AssertionError("session search did not index the ingested events")
    require(search_result["total_sessions"] == 1, "search must return one session")
    segments = [
        segment
        for item in search_result["items"]
        for match in item["matches"]
        for fragment in match["fragments"]
        for segment in fragment["segments"]
    ]
    require(
        any(segment["highlighted"] for segment in segments),
        "search must return structured highlights",
    )

    print(f"blackbox API contract passed for {session_id}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    run(args.base_url.rstrip("/"), args.token, args.run_id)


if __name__ == "__main__":
    main()
