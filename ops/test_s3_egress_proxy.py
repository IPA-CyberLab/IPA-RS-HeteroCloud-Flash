import base64
import ipaddress
import socket
import socketserver
import threading
import unittest

import s3_egress_proxy as proxy


KEY = b'test-only-proxy-signing-key-32-bytes'
IDENTITY = 'a' * 32 + '.' + 'b' * 32


class Echo(socketserver.BaseRequestHandler):
    def handle(self):
        while data := self.request.recv(1024):
            self.request.sendall(data)


class ProxyTests(unittest.TestCase):
    def setUp(self):
        self.echo = socketserver.ThreadingTCPServer(('127.0.0.1', 0), Echo)
        self.echo.daemon_threads = True
        self.lookups, self.connections = [], []

        def resolve(host, port):
            self.lookups.append((host, port))
            if host == 'private.example': return ['169.254.169.254']
            if host == 'mixed.example': return ['93.184.216.34', '10.0.0.2']
            return ['93.184.216.34']

        def connect(ip, port):
            self.connections.append((ip, port))
            return socket.create_connection(self.echo.server_address, timeout=2)

        self.server = proxy.Server(('127.0.0.1', 0), KEY, resolver=resolve, connector=connect, idle_timeout=2)
        for instance in (self.echo, self.server):
            threading.Thread(target=instance.serve_forever, daemon=True).start()

    def tearDown(self):
        for instance in (self.server, self.echo):
            instance.shutdown()
            instance.server_close()

    def request(self, target, credential=None):
        sock = socket.create_connection(self.server.server_address, timeout=2)
        auth = '' if credential is None else 'Proxy-Authorization: Basic ' + base64.b64encode(credential.encode()).decode() + '\r\n'
        sock.sendall(('CONNECT ' + target + ' HTTP/1.1\r\nHost: ' + target + '\r\n' + auth + '\r\n').encode())
        response = b''
        while b'\r\n\r\n' not in response:
            chunk = sock.recv(4096)
            if not chunk: break
            response += chunk
        return sock, response.split(b'\r\n', 1)[0]

    def credential(self, host, port=443):
        return IDENTITY + ':' + proxy.token_for(KEY, IDENTITY, host, port)

    def test_authorized_public_target_uses_the_validated_address(self):
        client, status = self.request('storage.example:443', self.credential('storage.example'))
        try:
            self.assertIn(b' 200 ', status)
            client.sendall(b'fixture payload')
            self.assertEqual(client.recv(100), b'fixture payload')
            self.assertEqual(self.lookups, [('storage.example', 443)])
            self.assertEqual(self.connections, [('93.184.216.34', 443)])
        finally:
            client.close()

    def test_different_host_or_missing_credential_is_denied_before_dns(self):
        for credential in (None, self.credential('other.example')):
            client, status = self.request('storage.example:443', credential)
            client.close()
            self.assertIn(b' 403 ', status)
        self.assertEqual(self.lookups, [])
        self.assertEqual(self.connections, [])

    def test_private_or_mixed_dns_answer_cannot_reach_internal_networks(self):
        for host in ('private.example', 'mixed.example'):
            client, status = self.request(host + ':443', self.credential(host))
            client.close()
            self.assertIn(b' 403 ', status)
        self.assertEqual(self.connections, [])

    def test_ipv4_mapped_and_reserved_addresses_are_rejected(self):
        for address in ('127.0.0.1', '169.254.169.254', '10.1.2.3', '100.64.0.1',
                        '224.0.0.1', '::1', '::ffff:127.0.0.1', 'fc00::1'):
            with self.subTest(address=address): self.assertFalse(proxy.public_ip(address))
        self.assertTrue(proxy.public_ip('1.1.1.1'))

    def test_public_cluster_addresses_are_blocked_after_dns_resolution(self):
        self.server.blocked_networks = (ipaddress.ip_network('93.184.216.0/24'),)
        client, status = self.request('storage.example:443', self.credential('storage.example'))
        client.close()
        self.assertIn(b' 403 ', status)
        self.assertEqual(self.connections, [])

    def test_authority_rejects_credentials_and_missing_ports(self):
        for value in ('localhost:443', 'foo.localhost:443', 'user@host:443', 'host',
                      'host:443/path', 'host:0', 'host:65536', 'host%00:443'):
            with self.subTest(value=value), self.assertRaises(ValueError): proxy.authority(value)


if __name__ == '__main__': unittest.main()
