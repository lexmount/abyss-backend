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
import uuid
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


def token_usage(
    input_tokens: int = 0,
    output_tokens: int = 0,
    cache_read_tokens: int = 0,
    cache_write_tokens: int = 0,
    reasoning_tokens: int = 0,
) -> dict[str, int]:
    """AgentEvent token counters; the total is the sum of the disjoint four."""
    return {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cache_read_tokens": cache_read_tokens,
        "cache_write_tokens": cache_write_tokens,
        "reasoning_tokens": reasoning_tokens,
        "total_tokens": input_tokens + output_tokens + cache_read_tokens + cache_write_tokens,
    }


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
        "agent": {"name": "codex", "version": "blackbox"},
        "session_id": session_id,
        "stream_id": f"stream-{run_id}",
        "response_id": f"response-{run_id}",
        "llm": {"provider": "openai", "model": "gpt-5.5"},
        "event_type": event_type,
        "token_usage": token_usage,
        "metadata": {
            "test_run": run_id,
            "conversation": {
                "entries": [{"role": "user" if event_type == "request" else "assistant", "text": text}],
                "completeness": "complete",
            },
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


def run(base_url: str, token: str, run_id: str, in_session: bool) -> None:
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

    ingest_payload = {
        "events": [
            event(
                request_event_id,
                session_id,
                "request",
                "blackbox request",
                token_usage(input_tokens=12, cache_read_tokens=3),
                run_id,
            ),
            event(
                response_event_id,
                session_id,
                "response",
                "blackbox response",
                token_usage(output_tokens=21, reasoning_tokens=4),
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
    require(
        all(
            item.get("stream_id") == f"stream-{run_id}"
            and item.get("response_id") == f"response-{run_id}"
            and item.get("previous_response_id") is None
            for item in raw_events
        ),
        "events must return the exchange identities they were ingested with",
    )

    # The list projection carries what an event list shows and no content.
    list_path = "/v1/agent-usage/events?" + urllib.parse.urlencode(
        {"session_id": session_id, "limit": 10, "fields": "list"}
    )
    list_response = request(base_url, "GET", list_path, token=token)
    require_status(list_response, 200, "event list query")
    listed_events = list_response.json()["events"]
    require(
        [item["id"] for item in listed_events] == [item["id"] for item in raw_events],
        "event list must return the same events in the same order",
    )
    content_fields = {"metadata", "attachments"}
    require(
        all(content_fields.isdisjoint(item) for item in listed_events),
        "event list items must leave out metadata and attachments",
    )
    require(
        all(
            item["total_tokens"] == by_event_id[item["event_id"]]["total_tokens"]
            and item["host_name"] == by_event_id[item["event_id"]]["host_name"]
            for item in listed_events
        ),
        "event list items must keep token counts and device labels",
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
    require(summary["agent_name"] == "codex", "summary must report the Agent name as received")
    require(summary["llm_provider"] == "openai", "summary must report the provider as received")
    require(summary["requests"] == 1 and summary["responses"] == 1, "summary event counts are incorrect")
    require(summary["total_tokens"] == 36, "summary token total is incorrect")

    # Repeating a filter key matches any of its values; filters still combine
    # with each other.
    other_session_id = f"{session_id}-absent"
    any_of_path = "/v1/agent-usage/events?" + urllib.parse.urlencode(
        [
            ("session_id", other_session_id),
            ("session_id", session_id),
            ("agent_name", "claude-code"),
            ("agent_name", "codex"),
            ("llm_provider", "anthropic"),
            ("llm_provider", "openai"),
            ("llm_model", "gpt-5.5"),
            ("llm_model", "absent-model"),
            ("limit", 10),
        ]
    )
    any_of_response = request(base_url, "GET", any_of_path, token=token)
    require_status(any_of_response, 200, "raw event query with repeated filters")
    require(
        len(any_of_response.json()["events"]) == 2,
        "repeated filter values must match any of the values",
    )
    # Agent names match exactly as reported: no case folding or aliases.
    for agent in ["Codex", "claude-desktop"]:
        exact_agent_path = "/v1/agent-usage/events?" + urllib.parse.urlencode(
            [("session_id", session_id), ("agent_name", agent)]
        )
        exact_agent_response = request(base_url, "GET", exact_agent_path, token=token)
        require_status(exact_agent_response, 200, "raw event query with another Agent")
        require(
            exact_agent_response.json()["events"] == [],
            f"agent_name={agent} must not match codex events",
        )
    # Providers also match exactly as reported.
    folded_provider_path = "/v1/agent-usage/events?" + urllib.parse.urlencode(
        [("session_id", session_id), ("llm_provider", "OpenAI")]
    )
    folded_provider_response = request(base_url, "GET", folded_provider_path, token=token)
    require_status(folded_provider_response, 200, "raw event query with a case-folded provider")
    require(
        folded_provider_response.json()["events"] == [],
        "llm_provider=OpenAI must not match the reported openai",
    )
    none_of_path = "/v1/agent-usage/events?" + urllib.parse.urlencode(
        [("session_id", session_id), ("llm_model", "absent-model"), ("llm_model", "other-model")]
    )
    none_of_response = request(base_url, "GET", none_of_path, token=token)
    require_status(none_of_response, 200, "raw event query with unmatched models")
    require(
        none_of_response.json()["events"] == [],
        "filters must still combine across dimensions",
    )
    any_of_summary_path = "/v1/agent-usage/summary?" + urllib.parse.urlencode(
        [
            ("group_by", "model"),
            ("session_id", session_id),
            ("session_id", other_session_id),
            ("llm_model", "absent-model"),
            ("llm_model", "gpt-5.5"),
        ]
    )
    any_of_summary = request(base_url, "GET", any_of_summary_path, token=token)
    require_status(any_of_summary, 200, "usage summary with repeated filters")
    any_of_rows = any_of_summary.json()["rows"]
    require(
        [(row["llm_model"], row["total_tokens"]) for row in any_of_rows] == [("gpt-5.5", 36)],
        "summary must aggregate every repeated filter value",
    )

    # Days follow the requested time zone; both events are at 08:00 UTC.
    def day_totals(time_zone: str | None) -> list[tuple[str, int]]:
        params = [("group_by", "day"), ("session_id", session_id)]
        if time_zone is not None:
            params.append(("tz", time_zone))
        response = request(
            base_url, "GET", "/v1/agent-usage/summary?" + urllib.parse.urlencode(params), token=token
        )
        require_status(response, 200, f"usage summary by day in {time_zone or 'UTC'}")
        return [(row["day"], row["total_tokens"]) for row in response.json()["rows"]]

    require(day_totals(None) == [("2026-08-24", 36)], "summary days must default to UTC")
    require(
        day_totals("Pacific/Honolulu") == [("2026-08-23", 36)],
        "summary days must follow the requested time zone",
    )
    invalid_zone = request(
        base_url,
        "GET",
        "/v1/agent-usage/summary?" + urllib.parse.urlencode({"group_by": "day", "tz": "UTC+8"}),
        token=token,
    )
    require_status(invalid_zone, 400, "usage summary with an unknown time zone")
    any_of_sessions_path = "/v1/agent-usage/sessions?" + urllib.parse.urlencode(
        [("llm_model", "absent-model"), ("llm_model", "gpt-5.5"), ("page_size", 100)]
    )
    any_of_sessions = request(base_url, "GET", any_of_sessions_path, token=token)
    require_status(any_of_sessions, 200, "session list with repeated filters")
    require(
        session_id in {item["session_id"] for item in any_of_sessions.json()["items"]},
        "session list must keep sessions that used any selected model",
    )

    session_pk = raw_events[0]["session_pk"]
    session_events = sorted(
        raw_events,
        key=lambda item: (item["observed_at"], item["event_type"], item["id"]),
    )
    for session_event in session_events:
        single = request(
            base_url,
            "GET",
            f"/v1/agent-usage/events/{session_event['id']}",
            token=token,
        )
        require_status(single, 200, "single event query")
        # A single event also says where it currently is in its session.
        single_event = single.json()
        location = single_event.pop("location", None)
        require(
            single_event == session_event,
            "single event query must match the raw event exactly",
        )
        require(
            isinstance(location, dict) and location.get("segment"),
            "single event query must carry the event's current segment",
        )
        require(
            IMAGE_BASE64.encode() not in single.body,
            "single event responses must not expose attachment content",
        )
    missing_event = request(
        base_url,
        "GET",
        f"/v1/agent-usage/events/{uuid.uuid4()}",
        token=token,
    )
    require_status(missing_event, 404, "unknown single event query")
    unauthenticated_event = request(
        base_url,
        "GET",
        f"/v1/agent-usage/events/{session_events[0]['id']}",
    )
    require_status(unauthenticated_event, 401, "unauthenticated single event query")

    search_path = "/v1/agent-usage/search?" + urllib.parse.urlencode(
        [
            ("q", "blackbox response"),
            ("agent_name", "claude-code"),
            ("agent_name", "codex"),
            ("llm_model", "absent-model"),
            ("llm_model", "gpt-5.5"),
        ]
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
    found = next(item for item in search_result["items"] if item["session_pk"] == session_pk)
    require(
        (found["match_count"], found["new_match_count"]) == (1, 1),
        f"the response matches once, outside any inherited context: {found}",
    )
    require(
        all(
            match["new"] is True and match["event_pk"] == session_events[1]["id"]
            for match in found["matches"]
        ),
        f"each match says whether it is new: {found['matches']}",
    )
    run_in_session_search_contract(
        base_url, token, session_pk, [item["id"] for item in session_events], in_session
    )

    from session_titles_api import run_title_contract

    run_title_contract(base_url, token, run_id)

    print(f"blackbox API contract passed for {session_id}")


def in_session_search_path(session_pk: str, query: str) -> str:
    return (
        f"/v1/agent-usage/sessions/{session_pk}/search?"
        + urllib.parse.urlencode([("q", query)])
    )


def run_in_session_search_contract(
    base_url: str, token: str, session_pk: str, event_pks: list[str], supported: bool
) -> None:
    """Every hit of a query in one session, with UTF-16 occurrence ranges and
    the segment of each hit's event."""
    path = in_session_search_path(session_pk, "BLACKBOX")
    if not supported:
        response = request(base_url, "GET", path, token=token)
        require_status(response, 503, "search within a session is not supported")
        return
    response = request(base_url, "GET", path, token=token)
    require_status(response, 200, "search within a session")
    found = response.json()
    require(
        set(found) == {"query", "texts", "hits", "truncated"} and found["truncated"] is False,
        f"in-session search response shape: {found}",
    )
    streams = request(
        base_url, "GET", f"/v1/agent-usage/sessions/{session_pk}/streams", token=token
    )
    require_status(streams, 200, "session streams")
    heads = [segment["head"] for segment in streams.json()["segments"]]
    require(len(heads) == 1, f"the session is one segment: {heads}")
    hits = [
        (hit["event_pk"], hit["entry_index"], hit["field"], hit["new"], hit["segment"])
        for hit in found["hits"]
    ]
    require(
        hits
        == [
            (event_pks[0], 0, "text", True, heads[0]),
            (event_pks[1], 0, "text", True, heads[0]),
        ],
        f"hits of every piece in session order, in their segment: {hits}",
    )
    texts = {text["id"]: text["ranges"] for text in found["texts"]}
    require(
        set(texts) == {hit["text_id"] for hit in found["hits"]} and len(texts) == 2,
        f"each distinct text once: {found['texts']}",
    )
    require(
        all(ranges == [[0, len("blackbox")]] for ranges in texts.values()),
        f"ranges are UTF-16 offsets of the original text: {texts}",
    )
    missing = request(
        base_url, "GET", in_session_search_path(str(uuid.uuid4()), "blackbox"), token=token
    )
    require_status(missing, 404, "search within an unknown session")
    empty = request(base_url, "GET", in_session_search_path(session_pk, " "), token=token)
    require_status(empty, 400, "search within a session without a query")
    require_status(request(base_url, "GET", path), 401, "unauthenticated in-session search")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument(
        "--in-session-search",
        action="store_true",
        help="the storage profile supports search within a session",
    )
    args = parser.parse_args()
    run(args.base_url.rstrip("/"), args.token, args.run_id, args.in_session_search)


if __name__ == "__main__":
    main()
