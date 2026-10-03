# syntax=docker/dockerfile:1.7
FROM rust:1.96-bookworm AS builder
WORKDIR /src
RUN apt-get update \
    && apt-get install -y --no-install-recommends musl-tools \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --locked --release --bins
RUN musl-gcc -O2 -static -Wall -Wextra -Werror -o /usr/local/bin/flash-secret-env-launcher src/flash-secret-env-launcher.c

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 65532 --home-dir /nonexistent --shell /usr/sbin/nologin flash
COPY --from=builder /src/target/release/flash-vpc-ready /usr/local/bin/flash-vpc-ready
COPY --from=builder /src/target/release/flash-api /usr/local/bin/flash-api
COPY --from=builder /src/target/release/flash-controller /usr/local/bin/flash-controller
COPY --from=builder /src/target/release/flash-activator /usr/local/bin/flash-activator
COPY --from=builder /src/target/release/flashctl /usr/local/bin/flashctl
COPY --from=builder /src/target/release/flash-udp-echo /usr/local/bin/flash-udp-echo
COPY --from=builder /usr/local/bin/flash-secret-env-launcher /usr/local/bin/flash-secret-env-launcher
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/flash-api"]
