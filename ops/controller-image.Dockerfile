# syntax=docker/dockerfile:1.7
FROM rust:1.96-bookworm AS builder
WORKDIR /src
RUN apt-get update && apt-get upgrade -y && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --locked --release --bin flash-controller

FROM debian:bookworm-slim
RUN apt-get update && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 65532 --home-dir /nonexistent --shell /usr/sbin/nologin flash
COPY --from=builder /src/target/release/flash-controller /usr/local/bin/flash-controller
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/flash-controller"]
