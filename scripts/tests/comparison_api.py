#!/usr/bin/env python3
"""Exercise comparison aggregation against a running service and real storage."""

import copy
from datetime import datetime, timedelta, timezone
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
    check_bucket_boundaries(base, prefix, token, key)
    print(
        "comparison API: dimensions, filters, time boundaries, idempotency and authorization passed"
    )


def check_bucket_boundaries(base, prefix, token, key):
    """Exercise bound timestamps, quoted filter values and maximum-size bucket sets."""
    session = key + "-'quoted"
    model = "model-'quoted"
    provider = "provider-'quoted"

    def ingest(samples, suffix):
        events = [
            {
                "event_id": session + suffix + str(index),
                "session_id": session + suffix,
                "observed_at": observed_at,
                "device": {"host_name": session, "platform": "linux"},
                "agent": {"name": "codex"},
                "llm": {"provider": provider, "model": model},
                "event_type": "request",
                "token_usage": {"input_tokens": tokens, "total_tokens": tokens},
            }
            for index, (observed_at, tokens) in enumerate(samples)
        ]
        status, data = request(base, prefix + "/events", token, {"events": events})
        assert status == 200, (status, data)

    def compare(query):
        status, data = request(base, prefix + "/comparison", token, query)
        assert status == 200, (status, data)
        return data

    def millis(value):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        delta = value - datetime(1970, 1, 1, tzinfo=timezone.utc)
        return (delta.days * 86400 + delta.seconds) * 1000 + delta.microseconds // 1000

    ingest(
        [
            ("2030-03-01T00:00:00.000000Z", 999),
            ("2030-03-01T00:00:00.000001Z", 5_000_000_000),
            ("2030-03-01T01:00:00.000000Z", 7),
            ("2030-03-01T01:00:00.000001Z", 11),
            ("2030-03-01T02:00:00.000001Z", 999),
        ],
        "",
    )
    query = {
        "group_by": None,
        "buckets": [
            {
                "start": "2030-03-01T00:00:00.000001Z",
                "end": "2030-03-01T01:00:00.000001Z",
            },
            {
                "start": "2030-03-01T01:00:00.000001Z",
                "end": "2030-03-01T02:00:00.000001Z",
            },
        ],
        "filters": {
            "session": [session],
            "agent": ["codex"],
            "model": [model],
            "provider": [provider],
            "platform": ["linux"],
        },
    }
    data = compare(query)
    assert data["rows"] == [
        {
            "bucket": millis(query["buckets"][0]["start"]),
            "value": "all",
            "total_tokens": 5_000_000_007,
            "requests": 2,
        },
        {
            "bucket": millis(query["buckets"][1]["start"]),
            "value": "all",
            "total_tokens": 11,
            "requests": 1,
        },
    ], data
    query["filters"]["model"] = ["does-not-exist-' OR 1=1 --"]
    empty = compare(query)
    assert empty["rows"] == [], "filter values must remain literal parameters"
    assert empty["options"] == data["options"], "chart filters must not narrow facets"
    query["filters"]["model"] = [model]
    start = datetime(2030, 3, 1, microsecond=1, tzinfo=timezone.utc)
    query["buckets"] = [
        {
            "start": (start + timedelta(minutes=index)).isoformat(),
            "end": (start + timedelta(minutes=index + 1)).isoformat(),
        }
        for index in range(400)
    ]
    maximum = compare(query)
    assert len(maximum["rows"]) == 4, maximum
    assert sum(row["total_tokens"] for row in maximum["rows"]) == 5_000_001_017
    assert sum(row["requests"] for row in maximum["rows"]) == 4

    ingest(
        [
            ("2026-11-01T04:00:00Z", 7),
            ("2026-11-02T04:59:59.999999Z", 11),
            ("2026-11-02T05:00:00Z", 13),
            ("2026-11-03T05:00:00Z", 999),
        ],
        "-dst",
    )
    query["filters"]["session"] = [session + "-dst"]
    query["buckets"] = [
        {"start": "2026-11-01T04:00:00Z", "end": "2026-11-02T05:00:00Z"},
        {"start": "2026-11-02T05:00:00Z", "end": "2026-11-03T05:00:00Z"},
    ]
    dst = compare(query)
    assert [
        (row["bucket"], row["total_tokens"], row["requests"]) for row in dst["rows"]
    ] == [
        (millis(query["buckets"][0]["start"]), 18, 2),
        (millis(query["buckets"][1]["start"]), 13, 1),
    ], dst
    print(
        "comparison boundaries: microseconds, quoted filters, empty matches, 400 buckets and DST passed"
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--token", required=True)
    args = parser.parse_args()
    run(args.base_url, args.token)
