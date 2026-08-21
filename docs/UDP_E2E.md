# HeteroCloud Flash UDP Echo E2E

This check verifies that a UDP datagram can reach a HeteroCloud Flash
workload and that the response returns to the original client unchanged. It
uses only the Rust and Python standard libraries.

## Echo server

`flash-udp-echo` binds to `FLASH_ECHO_LISTEN`, receives one UDP datagram at a
time, and sends the exact payload back to its source address. The default is
`0.0.0.0:7777`.

The listen value must be a numeric IP socket address. IPv6 addresses must use
brackets, for example `[::]:7777`.

```bash
FLASH_ECHO_LISTEN=0.0.0.0:7777 cargo run --bin flash-udp-echo
```

The process uses the operating system's default signal handling, so SIGTERM
terminates it without an application-level shutdown sequence.

## E2E client

Run the client from the network location whose UDP path needs verification:

```bash
HOST=127.0.0.1 PORT=7777 TIMEOUT=2 ATTEMPTS=5 ./scripts/udp-e2e.sh
```

| Variable | Default | Meaning |
| --- | --- | --- |
| `HOST` | `127.0.0.1` | Echo service hostname or IP address |
| `PORT` | `7777` | UDP destination port, from 1 through 65535 |
| `TIMEOUT` | `2.0` | Per-attempt timeout in seconds |
| `ATTEMPTS` | `5` | Maximum number of send/receive attempts |

The client resolves all available IPv4 and IPv6 UDP addresses and rotates
through them across retries. Each attempt sends a fresh random nonce over a
connected UDP socket. A check passes only when the received byte sequence is
exactly equal to that attempt's payload.

A successful run prints one line similar to:

```text
udp-e2e: PASS endpoint=('127.0.0.1', 7777) bytes=74 attempt=1/5 rtt_ms=0.31
```

Exit status `0` means the echo matched, `1` means resolution or all network
attempts failed, and `2` means an environment variable was invalid. Exit
status `127` means Python 3 was unavailable.

## External UDP validation

For a gVisor workload exposed through HeteroCloud Flash, set `HOST` to the
externally routed hostname or IP and `PORT` to the published UDP port. Run the
client outside the cluster or NAT boundary being tested. A local-cluster test
alone does not verify public routing, firewall, NAT, or return-path behavior.
