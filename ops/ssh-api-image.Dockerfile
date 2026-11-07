# Build only the API for a provider-only rollout. Other Flash components and
# workload helper images keep their independently pinned production images.
FROM rust:1.96.1-bookworm AS builder
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --locked --release --bin flash-api && cp target/release/flash-api /tmp/flash-api

FROM debian:bookworm-slim
RUN apt-get update && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 65532 --home-dir /nonexistent --shell /usr/sbin/nologin flash
COPY --from=builder /src/Cargo.lock /usr/share/heterocloud/Cargo.lock
COPY --from=builder /tmp/flash-api /usr/local/bin/flash-api
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/flash-api"]
