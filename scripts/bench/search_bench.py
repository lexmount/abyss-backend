#!/usr/bin/env python3
"""Benchmark GET /v1/agent-usage/search against a running backend.

The script starts nothing. Point it at a backend with --base-url and
--token-file; it runs every query in the set, records the first (cold) and
median warm latency, the session total, and a hash of the canonical response
body so two runs can be compared for identical results.

    python3 scripts/bench/search_bench.py \
        --base-url http://127.0.0.1:8080 --token-file run/token \
        --out run/baseline.json
    python3 scripts/bench/search_bench.py --compare run/baseline.json run/after.json
"""

import argparse
import hashlib
import json
import statistics
import sys
import time
import urllib.parse
import urllib.request

# Terms range from matching nothing to matching most events.
QUERIES = [
    {"name": "missing", "q": "qqvvxxnone7"},
    {"name": "rare", "q": "bm25"},
    {"name": "chinese_run", "q": "帮你尝试不同的思路"},
    {"name": "medium_timeline", "q": "timeline"},
    {"name": "medium_useQuery", "q": "useQuery"},
    {"name": "common_entry", "q": "entry"},
    {"name": "common_dashboard", "q": "dashboard"},
    {"name": "very_common_the", "q": "the"},
    {"name": "and_two_terms", "q": "cargo sqlite"},
    {"name": "and_three_terms", "q": "git status branch"},
    {"name": "entry_page2", "q": "entry", "page": 2},
    {"name": "entry_page5", "q": "entry", "page": 5},
    {"name": "entry_page_size50", "q": "entry", "page_size": 50},
    {"name": "entry_past_end", "q": "entry", "page": 40},
    {"name": "entry_codex", "q": "entry", "agent_name": ["codex"]},
    {"name": "entry_two_agents", "q": "entry", "agent_name": ["codex", "claude-code"]},
    {"name": "entry_model", "q": "entry", "llm_model": ["claude-opus-5-5"]},
    {"name": "entry_requests", "q": "entry", "event_type": "request"},
    {"name": "entry_responses", "q": "entry", "event_type": "response"},
    {"name": "entry_week", "q": "entry", "from": "2026-10-01T07:00:00Z", "to": "2026-10-08T07:00:00Z"},
    {"name": "the_week_codex", "q": "the", "from": "2026-10-01T07:00:00Z", "agent_name": ["codex"]},
]


def request(base_url, token, query):
    params = []
    for key, value in query.items():
        if key == "name":
            continue
        for item in value if isinstance(value, list) else [value]:
            params.append((key, str(item)))
    url = f"{base_url}/v1/agent-usage/search?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    started = time.perf_counter()
    with urllib.request.urlopen(req, timeout=600) as response:
        body = response.read()
    return time.perf_counter() - started, body


def canonical_hash(body):
    payload = json.loads(body)
    return hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def run(args):
    token = open(args.token_file, encoding="utf-8").read().strip()
    results = []
    for query in QUERIES:
        cold, body = request(args.base_url, token, query)
        warm = []
        for _ in range(args.repeat):
            elapsed, warm_body = request(args.base_url, token, query)
            warm.append(elapsed)
            if canonical_hash(warm_body) != canonical_hash(body):
                print(f"warning: {query['name']} answered differently between runs", file=sys.stderr)
        payload = json.loads(body)
        result = {
            "name": query["name"],
            "query": {k: v for k, v in query.items() if k != "name"},
            "cold_s": round(cold, 4),
            "median_s": round(statistics.median(warm), 4),
            "total_sessions": payload["total_sessions"],
            "items": len(payload["items"]),
            "hash": canonical_hash(body),
        }
        results.append(result)
        print(f"{result['name']:20} cold {cold:7.3f}s  median {result['median_s']:7.3f}s  "
              f"sessions {result['total_sessions']:5}  items {result['items']}")
    with open(args.out, "w", encoding="utf-8") as handle:
        json.dump(results, handle, ensure_ascii=False, indent=2)


def compare(before_path, after_path):
    before = {r["name"]: r for r in json.load(open(before_path, encoding="utf-8"))}
    after = {r["name"]: r for r in json.load(open(after_path, encoding="utf-8"))}
    print(f"{'query':20} {'before':>9} {'after':>9} {'speedup':>8}  same")
    for name, old in before.items():
        new = after.get(name)
        if new is None:
            continue
        speedup = old["median_s"] / new["median_s"] if new["median_s"] else float("inf")
        same = "yes" if old["hash"] == new["hash"] else "NO"
        print(f"{name:20} {old['median_s']:8.3f}s {new['median_s']:8.3f}s {speedup:7.1f}x  {same}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base-url")
    parser.add_argument("--token-file")
    parser.add_argument("--out")
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--compare", nargs=2, metavar=("BEFORE", "AFTER"))
    args = parser.parse_args()
    if args.compare:
        compare(*args.compare)
    elif args.base_url and args.token_file and args.out:
        run(args)
    else:
        parser.error("pass --base-url, --token-file and --out, or --compare")


if __name__ == "__main__":
    main()
