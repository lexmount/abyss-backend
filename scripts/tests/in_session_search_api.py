#!/usr/bin/env python3
"""Shared HTTP search contract for SQLite, PostgreSQL and Agent Data.

Keep this fixture identical in abyss-backend and abyss-services. It ingests
events through the public API and checks positions against their original text.
"""

import argparse
import json
import urllib.error
import urllib.parse
import urllib.request
import uuid


def run(base_url, prefix, headers, other_headers=None):
    def request(path, body=None, *, status=200, auth=None):
        req = urllib.request.Request(
            base_url + prefix + path,
            data=None if body is None else json.dumps(body).encode(),
            headers={"Content-Type": "application/json", **(headers if auth is None else auth)},
        )
        try:
            response = urllib.request.urlopen(req, timeout=60)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            data = response.read()
            assert response.code == status, (path, response.code, status, data[:500])
            return json.loads(data) if response.headers.get("Content-Type", "").startswith("application/json") else data.decode()

    run_id = uuid.uuid4().hex
    session_id = f"in-session-{run_id}"

    def event(key, second, side, entries, response, session=session_id):
        value = {
            "event_id": f"{run_id}-{key}",
            "observed_at": f"2026-11-09T00:00:{second:02d}Z",
            "device": {"host_name": "in-session-contract", "platform": "linux"},
            "agent": {"name": "codex"},
            "session_id": session,
            "response_id": response,
            "event_type": side,
            "llm": {"provider": "openai", "model": "search-contract"},
            "token_usage": {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
            "metadata": {"headers": [{"name": "X-Test", "value": "metadata-only-secret"}]},
        }
        if entries is not None:
            value["conversation"] = {"completeness": "complete", "entries": entries}
        return value

    def ingest(events):
        result = request("/events", {"events": events})
        assert result["accepted"] == len(events) and result["rejected"] == 0, result

    def stored(session):
        return request("/events?" + urllib.parse.urlencode({"session_id": session, "limit": 100}))["events"]

    def search(pk, query, **kwargs):
        return request(f"/sessions/{pk}/search?" + urllib.parse.urlencode({"q": query}), **kwargs)

    first = [{"role": "system", "text": "rules"}, {"role": "user", "text": "😀 NIMBUS nimbus"}]
    answer = [
        {"role": "assistant", "text": "nimbus nimbus", "item_id": "answer-1"},
        {"role": "tool_call", "name": "Nimbus", "text": "Nimbus", "call_id": "tool-1"},
    ]
    continuation = first + answer + [{"role": "tool_result", "text": "搜索🔎 Nimbus", "call_id": "tool-1"}]
    special = [
        {"role": "assistant", "text": "CARGO TEST / aaaaa / 😀搜索 🔎搜索 / K k İ i Straße STRASSE Ａ abc % _ \"", "item_id": "answer-2"},
        {"role": "user", "text": "cross-boundary-left"},
        {"role": "user", "text": "right-boundary"},
    ]
    fixtures = [
        event("r1", 1, "request", first, "search-1"),
        event("p1", 2, "response", answer, "search-1"),
        event("r2", 3, "request", continuation, "search-2"),
        event("p2", 3, "response", special, "search-2"),
        event("empty", 4, "response", None, "empty"),
    ]
    # Reverse arrival order and equal observation times must not change hit order.
    ingest(list(reversed(fixtures)))
    ingest([event("elsewhere", 0, "request", first, "elsewhere", session_id + "-other")])
    rows = stored(session_id)
    pk = rows[0]["session_pk"]
    pks = {row["event_id"]: row["id"] for row in rows}
    new_from = {fixtures[2]["event_id"]: 4}
    ordered = sorted(fixtures, key=lambda row: (row["observed_at"], pks[row["event_id"]]))
    segments = {
        row["id"]: request(f"/events/{row['id']}/location")["segment"]
        for row in rows
    }

    def fold(text):
        return "".join(char.lower() if len(char.lower()) == 1 else char for char in text)

    def reference(query):
        query = fold(query.strip())
        hits = []
        for value in ordered:
            for index, entry in enumerate(value.get("conversation", {}).get("entries", [])):
                for field in ("name", "text"):
                    text = entry.get(field)
                    if not isinstance(text, str) or not text:
                        continue
                    folded, offset, ranges = fold(text), 0, []
                    while (start := folded.find(query, offset)) >= 0:
                        end = start + len(query)
                        ranges.append([len(text[:start].encode("utf-16-le")) // 2,
                                       len(text[:end].encode("utf-16-le")) // 2])
                        offset = end
                    if ranges:
                        event_pk = pks[value["event_id"]]
                        new = value["event_type"] == "response" or index >= new_from.get(value["event_id"], 0)
                        hits.append((event_pk, index, field, new, segments[event_pk], ranges, text))
        return hits

    for query in [" NIMBUS ", "搜索", "搜", "😀", "cargo test", "aa", "k", "İ", "STRASSE",
                  "Ａ", "%", "_", '"', "left right", "metadata-only-secret", "absent"]:
        result = search(pk, query)
        assert set(result) == {"query", "hits", "texts", "truncated"}, result
        assert result["query"] == query.strip() and result["truncated"] is False, result
        ranges = {item["id"]: item["ranges"] for item in result["texts"]}
        assert len(ranges) == len(result["texts"])
        assert set(ranges) == {hit["text_id"] for hit in result["hits"]}
        actual = [(h["event_pk"], h["entry_index"], h["field"], h["new"], h["segment"], ranges[h["text_id"]])
                  for h in result["hits"]]
        expected = reference(query)
        assert actual == [hit[:-1] for hit in expected], (query, actual, expected)
        text_ids = {}
        for hit, expected_hit in zip(result["hits"], expected):
            previous = text_ids.setdefault(expected_hit[-1], hit["text_id"])
            assert previous == hit["text_id"], "identical texts must share ranges"
        assert len(set(text_ids.values())) == len(text_ids), "different original texts need distinct ids"

    search(pk, " ", status=400)
    search(pk, "x" * 257, status=400)
    request(f"/sessions/{pk}/search", status=400)
    search(pk, "nimbus", status=401, auth={})
    search(str(uuid.uuid4()), "nimbus", status=404)
    if other_headers is not None:
        search(pk, "nimbus", status=404, auth=other_headers)
        search(pk, "absent", status=404, auth=other_headers)

    # Read-your-writes: a late event changes the next search without indexing.
    late = event("late", 0, "request", [{"role": "user", "text": "late-arrival"}], "late")
    ingest([late])
    result = search(pk, "late-arrival")
    assert len(result["hits"]) == 1 and result["hits"][0]["new"] is True, result

    # Exactly the cap is complete; one more matching piece signals truncation.
    cap_session = session_id + "-cap"
    cap_event = event("cap", 5, "request", [{"role": "user", "text": "capword"}] * 100_000, "cap", cap_session)
    ingest([cap_event])
    cap_pk = stored(cap_session)[0]["session_pk"]
    at_cap = search(cap_pk, "capword")
    assert len(at_cap["hits"]) == 100_000 and not at_cap["truncated"]
    assert len(at_cap["texts"]) == 1 and at_cap["texts"][0]["ranges"] == [[0, 7]]
    ingest([event("over-cap", 6, "response", [{"role": "assistant", "text": "capword excess"}], "cap", cap_session)])
    over_cap = search(cap_pk, "capword")
    assert over_cap["hits"] == at_cap["hits"] and over_cap["texts"] == at_cap["texts"]
    assert over_cap["truncated"] is True
    print("in-session search: literal matching, UTF-16 ranges, history, ordering, scoping and cap passed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--token", required=True)
    args = parser.parse_args()
    run(args.base_url.rstrip("/"), "/v1/agent-usage", {"Authorization": f"Bearer {args.token}"})
