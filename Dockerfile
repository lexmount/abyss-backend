FROM rust:1-bookworm AS builder

WORKDIR /src

RUN apt-get update \
    && apt-get install -y --no-install-recommends libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN cargo build --locked --release --package abyss-backend

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libpq5 tzdata \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 abyss

COPY --from=builder /src/target/release/abyss-backend /usr/local/bin/abyss-backend

USER 10001:10001
ENV ABYSS_BACKEND_ADDR=0.0.0.0:8080
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/abyss-backend"]
