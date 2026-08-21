# syntax=docker/dockerfile:1.7
FROM rust:1.96-bookworm AS builder
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --locked --release --bins

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 65532 --home-dir /nonexistent --shell /usr/sbin/nologin flash
COPY --from=builder /src/target/release/flash-api /usr/local/bin/flash-api
COPY --from=builder /src/target/release/flash-controller /usr/local/bin/flash-controller
COPY --from=builder /src/target/release/flashctl /usr/local/bin/flashctl
COPY --from=builder /src/target/release/flash-udp-echo /usr/local/bin/flash-udp-echo
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/flash-api"]

