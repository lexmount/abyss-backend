.PHONY: check fmt clippy test test-blackbox docker-build k8s-render

check: fmt clippy test

fmt:
	cargo +nightly fmt --all -- --check

clippy:
	cargo clippy --locked --workspace --all-targets -- -D warnings

test:
	cargo test --locked --workspace

test-blackbox:
	bash scripts/blackbox_abyss_backend.sh

docker-build:
	docker build -t abyss-backend:local .

k8s-render:
	kubectl kustomize k8s
