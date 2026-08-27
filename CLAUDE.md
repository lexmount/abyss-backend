# abyss-backend Development Guide

`abyss-backend` is the standalone open-source Agent event service. It owns
event ingestion, compile-time-selected persistence and search, event queries,
and the HTTP API that exposes those capabilities.

Keep the repository focused on these public event-service responsibilities.

## Architecture

- `api` owns HTTP routing and response boundaries.
- `identity` validates the deployment bearer token and returns the stable
  standalone owner identifier.
- `usage` owns event contracts plus backend-independent validation and ordering.
- `search` owns shared search contracts and safe projection extraction.
- `storage` is the `dyn StorageBackend` compatibility boundary and contains the
  conditionally compiled PostgreSQL/Elasticsearch and SQLite/FTS5 backends.
- `db` owns PostgreSQL-only pooling, migrations, and Diesel models.

Keep modules focused and start new Rust module files with a `//!` responsibility
comment. Keep the implementation self-contained within this repository.

## Commands

```bash
cargo +nightly fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy --no-default-features --features sqlite-fts --workspace --all-targets -- -D warnings
cargo test --workspace
cargo test --no-default-features --features sqlite-fts --workspace
make test-blackbox
```
