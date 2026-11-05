#!/usr/bin/env python3
"""Destination-scoped HTTPS CONNECT proxy for customer S3 endpoints.

The caller receives a host-bound HMAC credential; each DNS answer is checked
and the validated address is used directly, preventing DNS-rebinding access
to cluster, host and metadata networks. The proxy never terminates TLS.
"""
import argparse
import base64
import hashlib
import hmac
import http.server
import ipaddress
from pathlib import Path
import re
import select
import socket
import socketserver
import threading
import urllib.parse

IDENTITY = re.compile(r'^[0-9a-f]{32}\.[0-9a-f]{32}$')
DNS_NAME = re.compile(r'^[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?$')
TOKEN_DOMAIN = b'heterocloud-backup-s3-proxy-v1\x00'


def authority(value):
    if not isinstance(value, str) or not value or len(value) > 300 or any(c in value for c in '/?#@%\\\r\n'):
        raise ValueError('invalid destination authority')
    try:
        parsed = urllib.parse.urlsplit('//' + value)
        host, port = parsed.hostname, parsed.port
    except ValueError:
        raise ValueError('invalid destination authority') from None
    if not host or not port or not 1 <= port <= 65535:
        raise ValueError('invalid destination authority')
    host = host.lower().rstrip('.')
    try:
        ipaddress.ip_address(host)
    except ValueError:
        if not DNS_NAME.fullmatch(host) or '..' in host or host == 'localhost' or host.endswith('.localhost'):
            raise ValueError('invalid destination hostname') from None
    return host, port


def token_for(key, identity, host, port):
    if len(key) < 32 or not IDENTITY.fullmatch(identity):
        raise ValueError('invalid proxy credential parameters')
    host, port = authority(('[' + host + ']' if ':' in host else host) + ':' + str(port))
    message = TOKEN_DOMAIN + identity.encode() + b'\x00' + host.encode() + b'\x00' + str(port).encode()
    return hmac.new(key, message, hashlib.sha256).hexdigest()


def public_ip(value):
    try:
        address = ipaddress.ip_address(value)
        if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
            address = address.ipv4_mapped
        return address.is_global and not any((address.is_multicast, address.is_unspecified,
                                              address.is_loopback, address.is_link_local, address.is_reserved))
    except ValueError:
        return False


def system_resolver(host, port):
    return list(dict.fromkeys(item[4][0] for item in socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)))


def system_connector(address, port):
    return socket.create_connection((address, port), timeout=10)


class Handler(http.server.BaseHTTPRequestHandler):
    server_version = 'HeteroBackupProxy'
    sys_version = ''

    def log_message(self, *_):
        pass  # Never log proxy credentials or destination paths.

    def do_GET(self):
        self.send_response(200 if self.path == '/healthz' else 405)
        self.send_header('Content-Length', '0')
        self.end_headers()

    def do_CONNECT(self):
        if not self.server.slots.acquire(blocking=False):
            self.send_error(503)
            return
        upstream = None
        try:
            try:
                host, port = authority(self.path)
                header = self.headers.get('Proxy-Authorization', '')
                if not header.startswith('Basic ') or len(header) > 512:
                    raise ValueError('invalid credential')
                encoded = header.removeprefix('Basic ')
                raw = base64.b64decode(encoded, validate=True).decode('ascii')
                identity, separator, provided = raw.partition(':')
                expected = token_for(self.server.key, identity, host, port)
                if not separator or not hmac.compare_digest(provided, expected):
                    raise ValueError('invalid credential')
                addresses = self.server.resolver(host, port)
                if not addresses or any(not public_ip(address) for address in addresses):
                    raise ValueError('destination is not public')
            except (ValueError, UnicodeError, OSError):
                self.send_error(403)
                return
            try:
                for address in addresses:
                    try:
                        upstream = self.server.connector(address, port)
                        break
                    except OSError:
                        continue
                if upstream is None:
                    self.send_error(502)
                    return
                self.send_response(200, 'Connection Established')
                self.end_headers()
                self.connection.settimeout(None)
                upstream.settimeout(None)
                connections = [self.connection, upstream]
                while True:
                    ready, _, _ = select.select(connections, [], [], self.server.idle_timeout)
                    if not ready:
                        break
                    for source in ready:
                        data = source.recv(65536)
                        if not data:
                            return
                        (upstream if source is self.connection else self.connection).sendall(data)
            except (OSError, ValueError):
                pass
        finally:
            if upstream is not None:
                upstream.close()
            self.server.slots.release()
            self.close_connection = True


class Server(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address, key, resolver=system_resolver, connector=system_connector,
                 max_connections=32, idle_timeout=60):
        if len(key) < 32 or max_connections < 1:
            raise ValueError('invalid proxy server configuration')
        self.key, self.resolver, self.connector = key, resolver, connector
        self.slots, self.idle_timeout = threading.BoundedSemaphore(max_connections), idle_timeout
        super().__init__(address, Handler)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--key-file', type=Path, required=True)
    parser.add_argument('--listen', default='0.0.0.0')
    parser.add_argument('--port', type=int, default=8080)
    args = parser.parse_args()
    key = args.key_file.read_bytes()
    with Server((args.listen, args.port), key) as server:
        server.serve_forever(poll_interval=.5)


if __name__ == '__main__':
    main()
