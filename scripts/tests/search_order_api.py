#!/usr/bin/env python3
"""Seed and verify recency ordering through a real session-search HTTP API.

The API base includes /v1/agent-usage (standalone) or /api/v1/agent-usage
(product). Supply a token file or a cookie file; credentials are never printed.
The fixed dataset is isolated by a run-specific query and agent version.
"""
import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import time
import urllib.parse
import urllib.request
import uuid


def run(base, credential, *, cookie=False, run_id=None, output=None, scan_sessions=0):
    suffix = run_id or uuid.uuid4().hex
    marker = "recencysorting" + suffix
    session_prefix = "sorting-" + suffix
    headers = {"Content-Type": "application/json"}
    headers["Cookie" if cookie else "Authorization"] = (
        "abyss_session=" + credential if cookie else "Bearer " + credential
    )

    def request(path, payload=None):
        data = None if payload is None else json.dumps(payload).encode()
        with urllib.request.urlopen(urllib.request.Request(
            base.rstrip("/") + path, data=data, headers=headers
        ), timeout=15) as response:
            return json.load(response)

    def event(session, stamp, text, provider="openai"):
        return {
            "event_id": str(uuid.uuid5(uuid.NAMESPACE_URL, marker + session + stamp)),
            "observed_at": stamp,
            "device": {"host_name": "sorting-review", "platform": "linux", "os_version": "test"},
            "agent": {"name": "codex", "version": "sorting-review"},
            "session_id": session_prefix + "-" + session,
            "response_id": marker + session + stamp,
            "event_type": "response",
            "llm": {"provider": provider, "model": "sorting-review"},
            "conversation": {"entries": [{"role": "assistant", "text": text}], "completeness": "complete"},
            "token_usage": {"output_tokens": 1, "total_tokens": 1},
            "metadata": {},
        }

    if scan_sessions:
        assert scan_sessions > 1000, "exercise at least two aggregation batches"
        origin = datetime(2026, 1, 1, tzinfo=timezone.utc)
        expected_ids = []
        batch = []
        for i in range(scan_sessions):
            stamp = (origin + timedelta(minutes=i)).isoformat().replace("+00:00", "Z")
            item = event(f"scan-{i:05}", stamp, marker)
            expected_ids.insert(0, item["session_id"])
            batch.append(item)
            if len(batch) == 100 or i == scan_sessions - 1:
                request("/events", {"events": batch})
                batch = []
        deadline = time.monotonic() + 120
        while True:
            result = request("/search?" + urllib.parse.urlencode({"q": marker, "page_size": 50}))
            if result["total_sessions"] == scan_sessions:
                break
            if time.monotonic() > deadline:
                raise AssertionError("large scan seed was not indexed")
            time.sleep(.2)
        begun = time.monotonic()
        for page in [1, 20, 21, 22]:
            result = request("/search?" + urllib.parse.urlencode({"q": marker, "page_size": 50, "page": page}))
            assert result["total_sessions"] == scan_sessions
            assert [item["session_id"] for item in result["items"]] == expected_ids[(page-1)*50:page*50]
            assert result["has_more"] == (page*50 < scan_sessions)
        result = {"passed": True, "sessions": scan_sessions, "pages": [1, 20, 21, 22], "seconds": time.monotonic()-begun}
        if output:
            Path(output).write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result))
        return result

    origin = datetime(2026, 10, 1, tzinfo=timezone.utc)
    events = []
    for i in range(25):
        stamp = (origin + timedelta(days=i)).isoformat().replace("+00:00", "Z")
        # Oldest content has the strongest relevance; newer content is diluted.
        text = marker if i == 0 else marker + " background" * (i + 1)
        events.append(event(f"session-{i:02}", stamp, text, "openai" if i % 2 else "anthropic"))
    events.extend([
        event("tie-b", "2026-10-13T00:00:00Z", marker),
        event("tie-a", "2026-10-13T00:00:00Z", marker),
        event("resumed", "2025-01-01T00:00:00Z", marker),
        event("resumed", "2026-11-01T00:00:00Z", marker + " background" * 100),
        event("session-00", "2026-11-02T00:00:00Z", "unrelated latest activity"),
    ])
    request("/events", {"events": events})

    def search(**filters):
        return request("/search?" + urllib.parse.urlencode({"q": marker, **filters}))

    # Wait only for asynchronous indexing completeness, not for the asserted order.
    deadline = time.monotonic() + 60
    while True:
        all_results = search(page_size=50)
        if all_results["total_sessions"] == 28 and sum(x["match_count"] for x in all_results["items"]) == 29:
            break
        if time.monotonic() > deadline:
            raise AssertionError("seeded matching events were not fully indexed")
        time.sleep(.2)

    def expected(**filters):
        starts = {}
        eligible = set()
        for item in events:
            sid, stamp = item["session_id"], item["observed_at"]
            starts[sid] = min(stamp, starts.get(sid, stamp))
            if marker not in item["conversation"]["entries"][0]["text"]:
                continue
            if filters.get("to") and stamp >= filters["to"]:
                continue
            if filters.get("from") and stamp < filters["from"]:
                continue
            if filters.get("llm_provider") and item["llm"]["provider"] != filters["llm_provider"]:
                continue
            eligible.add(sid)
        return sorted(sorted(eligible), key=lambda sid: starts[sid], reverse=True)

    late = event("session-24", "2025-06-01T00:00:00Z", "earlier unrelated event")
    already_delivered = any(item["session_id"] == late["session_id"] and item["started_at"].startswith("2025-06-01")
                            for item in all_results["items"])
    if already_delivered:
        events.append(late)
    actual_ids = [item["session_id"] for item in all_results["items"]]
    assert actual_ids == expected(), {"expected": expected(), "actual": actual_ids}
    # An earlier nonmatching event changes the card time immediately, including
    # when existing matching documents were indexed before its arrival.
    request("/events", {"events": [late]})
    if not already_delivered:
        events.append(late)

    evidence = []
    for filters in ({}, {"llm_provider": "openai"}, {"to": "2026-10-20T00:00:00Z"}, {"from": "2026-10-20T00:00:00Z"}):
        wanted = expected(**filters)
        actual = []
        starts = []
        for page in range(1, (len(wanted) + 19) // 20 + 1):
            result = search(page=page, page_size=20, **filters)
            ids = [item["session_id"] for item in result["items"]]
            assert result["total_sessions"] == len(wanted), result
            assert ids == wanted[(page - 1) * 20:page * 20], {"expected": wanted, "actual": ids, "page": page}
            assert result["has_more"] == (page * 20 < len(wanted)), result
            assert all(item["matches"] for item in result["items"]), result
            actual.extend(ids)
            starts.extend(item["started_at"] for item in result["items"])
        assert starts == sorted(starts, reverse=True), starts
        assert len(set(actual)) == len(wanted)
        evidence.append({"filters": filters, "sessions": len(actual), "ordered_ids": actual, "started_at": starts})
    assert [x["session_id"] for x in search(page_size=20)["items"]] == expected()[:20]
    assert search(page=3, page_size=20)["items"] == []
    empty = request("/search?" + urllib.parse.urlencode({"q": marker + "nomatch"}))
    assert empty["total_sessions"] == 0 and empty["items"] == []
    assert any(fragment["segments"] and any(s["highlighted"] for s in fragment["segments"])
               for item in all_results["items"] for match in item["matches"] for fragment in match["fragments"])
    result = {"passed": True, "marker": marker, "base_url": base, "scenarios": evidence,
              "session_keys": {item["session_id"]: item["session_pk"] for item in all_results["items"]}}
    if output:
        Path(output).write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"passed": True, "sessions": 28, "scenarios": len(evidence), "marker": marker}))
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--credential-file", type=Path, required=True)
    parser.add_argument("--cookie", action="store_true")
    parser.add_argument("--run-id")
    parser.add_argument("--output")
    parser.add_argument("--scan-sessions", type=int, default=0)
    args = parser.parse_args()
    run(args.base_url, args.credential_file.read_text().strip(), cookie=args.cookie,
        run_id=args.run_id, output=args.output, scan_sessions=args.scan_sessions)
