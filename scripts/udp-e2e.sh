#!/usr/bin/env bash
set -Eeuo pipefail

if ! command -v python3 >/dev/null 2>&1; then
  printf '%s\n' 'udp-e2e: python3 is required' >&2
  exit 127
fi

exec python3 - <<'PY'
import math
import os
import secrets
import socket
import sys
import time


def configuration_error(message):
    print(f"udp-e2e: invalid configuration: {message}", file=sys.stderr)
    raise SystemExit(2)


def positive_integer(name, default, maximum=None):
    raw = os.environ.get(name, default)
    try:
        value = int(raw, 10)
    except ValueError:
        configuration_error(f"{name} must be an integer, got {raw!r}")
    if value < 1 or (maximum is not None and value > maximum):
        expected = f"1..{maximum}" if maximum is not None else "at least 1"
        configuration_error(f"{name} must be {expected}, got {value}")
    return value


host = os.environ.get("HOST", "127.0.0.1")
if not host:
    configuration_error("HOST must not be empty")

port = positive_integer("PORT", "7777", 65535)
attempts = positive_integer("ATTEMPTS", "5")

timeout_raw = os.environ.get("TIMEOUT", "2.0")
try:
    timeout = float(timeout_raw)
except ValueError:
    configuration_error(f"TIMEOUT must be a number, got {timeout_raw!r}")
if not math.isfinite(timeout) or timeout <= 0:
    configuration_error(f"TIMEOUT must be a finite number greater than zero, got {timeout_raw!r}")

try:
    resolved = socket.getaddrinfo(host, port, type=socket.SOCK_DGRAM)
except socket.gaierror as error:
    print(f"udp-e2e: failed to resolve {host}:{port}: {error}", file=sys.stderr)
    raise SystemExit(1)

targets = []
seen = set()
for family, socktype, protocol, _canonical_name, address in resolved:
    key = (family, socktype, protocol, address)
    if key not in seen:
        seen.add(key)
        targets.append((family, socktype, protocol, address))

if not targets:
    print(f"udp-e2e: no UDP addresses resolved for {host}:{port}", file=sys.stderr)
    raise SystemExit(1)

failures = []
for attempt in range(1, attempts + 1):
    family, socktype, protocol, address = targets[(attempt - 1) % len(targets)]
    payload = b"heterocloud-flash-udp-e2e:" + secrets.token_hex(24).encode("ascii")
    started = time.monotonic()

    try:
        with socket.socket(family, socktype, protocol) as client:
            client.settimeout(timeout)
            client.connect(address)
            sent = client.send(payload)
            if sent != len(payload):
                raise RuntimeError(f"sent {sent} of {len(payload)} bytes")
            response = client.recv(65_535)

        if response != payload:
            raise RuntimeError(
                f"payload mismatch: expected {len(payload)} bytes, received {len(response)} bytes"
            )

        elapsed_ms = (time.monotonic() - started) * 1000
        print(
            f"udp-e2e: PASS endpoint={address!r} bytes={len(payload)} "
            f"attempt={attempt}/{attempts} rtt_ms={elapsed_ms:.2f}"
        )
        raise SystemExit(0)
    except (OSError, RuntimeError) as error:
        failures.append(f"attempt {attempt}/{attempts} endpoint={address!r}: {error}")
        if attempt < attempts:
            time.sleep(min(0.1 * attempt, 1.0))

print(
    f"udp-e2e: FAIL host={host!r} port={port} after {attempts} attempts",
    file=sys.stderr,
)
for failure in failures:
    print(f"udp-e2e: {failure}", file=sys.stderr)
raise SystemExit(1)
PY
