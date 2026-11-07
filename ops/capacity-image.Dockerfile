FROM ghcr.io/ipa-cyberlab/ipa-rs-heterocloud-flash:0.1.56-observability.2@sha256:312aeebd0f08a8c6ca663dbf2e29a388ae76d17b0235224c30c0bb66cdc43c32
COPY --chown=65532:65532 ops/capacity.py /usr/local/lib/flash-ops/capacity.py
USER 65532:65532
