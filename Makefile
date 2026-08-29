.PHONY: check fmt clippy test test-blackbox test-blackbox-postgres test-blackbox-sqlite docker-build k8s-render

check: fmt clippy test

fmt:
	cargo +nightly fmt --all -- --check

clippy:
	cargo clippy --locked --workspace --all-targets -- -D warnings
	cargo clippy --locked --no-default-features --features sqlite-fts --workspace --all-targets -- -D warnings

test:
	cargo test --locked --workspace
	cargo test --locked --no-default-features --features sqlite-fts --workspace
	python3 scripts/tests/test_native_release_contract.py

test-blackbox: test-blackbox-postgres test-blackbox-sqlite

test-blackbox-postgres:
	bash scripts/blackbox_abyss_backend.sh

test-blackbox-sqlite:
	bash scripts/blackbox_sqlite_backend.sh

docker-build:
	docker build -t abyss-backend:local .

k8s-render:
	kubectl kustomize k8s
