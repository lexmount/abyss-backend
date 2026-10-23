#!/usr/bin/env python3
"""Exercise comparison aggregation against a running service and real storage."""

import copy
import json
import urllib.error
import urllib.request
import uuid


def request(base, path, token, body):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    req = urllib.request.Request(
        base + path, json.dumps(body).encode(), headers, method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode()


def run(base, token, prefix="/v1/agent-usage", other_token=None, admin_token=None):
    key = "comparison-" + str(uuid.uuid4())
    events = []
    fixtures = [
        ("09:59:59", "codex", "macos", 999, "request"),
        ("10:00:00", "codex", "macos", 10, "request"),
        ("10:30:00", "codex", "macos", 20, "response"),
        ("11:00:00", "claude-code", "macos", 30, "request"),
        ("11:30:00", "codex", "linux", 40, "request"),
        ("12:00:00", "codex", "macos", 999, "request"),
    ]
    for index, (time, agent, platform, tokens, kind) in enumerate(fixtures):
        events.append(
            {
                "event_id": key + "-" + str(index),
                "session_id": key,
                "observed_at": "2026-01-01T" + time + "Z",
                "device": {"host_name": key + platform, "platform": platform},
                "agent": {"name": agent},
                "llm": {
                    "provider": "openai" if agent == "codex" else "anthropic",
                    "model": "alpha" if agent == "codex" else "beta",
                },
                "event_type": kind,
                "token_usage": {"input_tokens": tokens, "total_tokens": tokens},
            }
        )
    code, _ = request(base, prefix + "/events", token, {"events": events})
    assert code == 200, "ingestion must succeed"
    # A duplicate batch must not change the aggregation.
    assert request(base, prefix + "/events", token, {"events": events})[0] == 200
    query = {
        "group_by": "agent",
        "buckets": [
            {"start": "2026-01-01T10:00:00Z", "end": "2026-01-01T11:00:00Z"},
            {"start": "2026-01-01T11:00:00Z", "end": "2026-01-01T12:00:00Z"},
        ],
        "filters": {"session": [key]},
    }

    def compare(value, credential=token, suffix=""):
        status, data = request(base, prefix + "/comparison" + suffix, credential, value)
        assert status == 200, (status, data)
        return data

    for dimension, expected in [
        ("agent", {"codex": 70, "claude-code": 30}),
        ("model", {"alpha": 70, "beta": 30}),
        ("provider", {"openai": 70, "anthropic": 30}),
        ("platform", {"macos": 60, "linux": 40}),
        (None, {"all": 100}),
    ]:
        query["group_by"] = dimension
        data = compare(query)
        totals = {}
        for row in data["rows"]:
            totals[row["value"]] = totals.get(row["value"], 0) + row["total_tokens"]
        assert totals == expected, (dimension, totals)
        assert sum(row["requests"] for row in data["rows"]) == 3
        assert {option["dimension"] for option in data["options"]} == {
            "agent",
            "model",
            "provider",
            "platform",
        }
    query["group_by"] = "agent"
    query["filters"].update({"platform": ["macos"], "model": ["alpha"]})
    assert sum(row["total_tokens"] for row in compare(query)["rows"]) == 30
    query["filters"]["model"] = ["alpha", "beta"]
    assert sum(row["total_tokens"] for row in compare(query)["rows"]) == 60
    query["filters"] = {"session": [key]}
    assert request(base, prefix + "/comparison", None, query)[0] == 401
    invalid = copy.deepcopy(query)
    invalid["filters"]["agent"] = ["codex"]
    assert request(base, prefix + "/comparison", token, invalid)[0] == 400
    invalid = copy.deepcopy(query)
    invalid["buckets"][1]["start"] = invalid["buckets"][0]["start"]
    assert request(base, prefix + "/comparison", token, invalid)[0] == 400
    invalid = copy.deepcopy(query)
    invalid["buckets"] = []
    assert request(base, prefix + "/comparison", token, invalid)[0] == 400
    invalid = copy.deepcopy(query)
    invalid["group_by"] = "session"
    assert request(base, prefix + "/comparison", token, invalid)[0] == 422
    if other_token:
        other = compare(query, other_token)
        assert (
            other["rows"] == [] and other["options"] == []
        ), "another user's data must remain private"
        assert (
            request(base, prefix + "/comparison?scope=global", token, query)[0] == 403
        )
        assert (
            sum(
                row["total_tokens"]
                for row in compare(query, admin_token, "?scope=global")["rows"]
            )
            == 100
        )
    print(
        "comparison API: dimensions, filters, time boundaries, idempotency and authorization passed"
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--token", required=True)
    args = parser.parse_args()
    run(args.base_url, args.token)
