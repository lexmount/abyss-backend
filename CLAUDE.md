# abyss-backend Development Guide

`abyss-backend` is the standalone open-source Agent event service. It owns
event ingestion, PostgreSQL persistence, event queries, optional Elasticsearch
projection, and the HTTP API that exposes those capabilities.

Keep the repository focused on these public event-service responsibilities.

## Architecture

- `api` owns HTTP routing and response boundaries.
- `identity` validates the deployment bearer token and returns the stable
  standalone owner identifier.
- `usage` owns event request types, validation, ingestion, and queries.
- `search` owns the Elasticsearch projection and search worker.
- `db` owns PostgreSQL pooling, the consolidated migration, and Diesel models.

Keep modules focused and start new Rust module files with a `//!` responsibility
comment. Keep the implementation self-contained within this repository.

## Commands

```bash
cargo +nightly fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
make test-blackbox
```
