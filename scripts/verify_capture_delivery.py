#!/usr/bin/env python3
"""Compare captured broker events with records uploaded through the real API.

This read-only verifier owns no provider policy or authentication logic. Pass a
private deployment bearer file and normalized events from an isolated workload.
Only event counts and token totals are printed, never credentials or messages.
"""
import argparse
from collections import Counter
import json
from pathlib import Path
import time
import urllib.request


def canonical(value):
    """The read API represents omitted optional properties as explicit nulls."""
    if isinstance(value, dict):
        return {key: canonical(item) for key, item in value.items() if item is not None}
    if isinstance(value, list):
        return [canonical(item) for item in value]
    return value


def compare(captured, stored):
    """Require field-preserving delivery, credential redaction, and conversation data."""
    assert captured, "capture is empty"
    expected = {event["event_id"]: event for event in captured}
    assert len(expected) == len(captured), "capture has duplicate event identifiers"
    actual = {event["event_id"]: event for event in stored}
    assert expected.keys() <= actual.keys(), "captured events have not all been persisted"
    counts, usage = Counter(), Counter()
    for event_id, source in expected.items():
        record = actual[event_id]
        for field in ("session_id", "response_id", "agent", "llm", "token_usage", "conversation"):
            assert canonical(record.get(field)) == canonical(source.get(field)), f"persisted {field} differs for {event_id}"
        assert record["event_type"] == source["side"], "event side changed"
        assert record["http"]["path"] == source["path"], "request path changed"
        for header in record["http"]["headers"]:
            if header["name"].lower() in ("authorization", "x-api-key", "api-key", "proxy-authorization"):
                assert header["value"] == "<redacted>", "credential header is not redacted"
        counts[source["agent"]["name"]] += 1
        usage.update(source["token_usage"])
    return {"events": len(expected), "agents": dict(counts), "token_usage": dict(usage)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True, help="Base URL ending in /v1/agent-usage")
    parser.add_argument("--token-file", required=True, type=Path)
    parser.add_argument("--captured-events", required=True, type=Path)
    args = parser.parse_args()
    captured = json.loads(args.captured_events.read_text())
    request = urllib.request.Request(args.endpoint.rstrip("/") + "/events?limit=1000",
        headers={"Authorization": "Bearer " + args.token_file.read_text().strip()})
    for attempt in range(50):
        with urllib.request.urlopen(request, timeout=5) as response:
            records = json.load(response)["events"]
        try:
            result = compare(captured, records)
            break
        except AssertionError:
            if attempt == 49:
                raise
            time.sleep(.1)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
