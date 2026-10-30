# Search benchmark

`search_bench.py` times `GET /v1/agent-usage/search` on a running backend
against a set of queries covering missing, rare, Chinese, common, multi-term,
paged, and filtered searches. It records the cold first run, the median of
three warm runs, the session total, and a hash of the canonical response, so
two builds on the same database can be compared for speed and identical
results:

```bash
python3 scripts/bench/search_bench.py --base-url http://127.0.0.1:PORT \
    --token-file TOKEN --out before.json
python3 scripts/bench/search_bench.py --compare before.json after.json
```

Run benchmarks against a copy, never a live database.
