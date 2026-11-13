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


def run(base, credential, *, cookie=False, run_id=None, output=None):
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
        event("resumed", "2026-01-01T00:00:00Z", marker),
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
        latest = {}
        for item in events:
            if marker not in item["conversation"]["entries"][0]["text"]:
                continue
            stamp = item["observed_at"]
            if filters.get("to") and stamp >= filters["to"]:
                continue
            if filters.get("llm_provider") and item["llm"]["provider"] != filters["llm_provider"]:
                continue
            latest[item["session_id"]] = max(stamp, latest.get(item["session_id"], ""))
        # Stable lexical tie-breaks after descending observation time.
        return sorted(sorted(latest), key=lambda sid: latest[sid], reverse=True)

    evidence = []
    for filters in ({}, {"llm_provider": "openai"}, {"to": "2026-10-20T00:00:00Z"}):
        wanted = expected(**filters)
        actual = []
        for page in range(1, (len(wanted) + 19) // 20 + 1):
            result = search(page=page, page_size=20, **filters)
            ids = [item["session_id"] for item in result["items"]]
            assert result["total_sessions"] == len(wanted), result
            assert ids == wanted[(page - 1) * 20:page * 20], {"expected": wanted, "actual": ids, "page": page}
            assert result["has_more"] == (page * 20 < len(wanted)), result
            assert all(item["matches"] for item in result["items"]), result
            actual.extend(ids)
        assert len(set(actual)) == len(wanted)
        evidence.append({"filters": filters, "sessions": len(actual), "ordered_ids": actual})
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
    args = parser.parse_args()
    run(args.base_url, args.credential_file.read_text().strip(), cookie=args.cookie,
        run_id=args.run_id, output=args.output)
