#!/usr/bin/env python3
"""Exercise the SSH provider against an explicitly selected running Flash pod.

Runs only printf/cat and an optional HTTP health probe; never changes the pod,
service or its files. The provider runs on loopback with a disposable signing
key, so no production credentials or private keys are exported.
"""
import argparse
import asyncio
import base64
import json
import os
import socket
import subprocess
import time
import uuid

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
import websockets


def resource(namespace, kind, name):
    result = subprocess.run(["kubectl", "-n", namespace, "get", kind, name, "-o", "json"],
                            check=True, capture_output=True)
    return json.loads(result.stdout)


def b64(value):
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def sign(private, claims):
    header = b64(json.dumps({"alg":"EdDSA", "kid":"ssh-test"}, separators=(",", ":")).encode())
    payload = b64(json.dumps(claims, separators=(",", ":")).encode())
    message = f"{header}.{payload}".encode()
    return f"{header}.{payload}.{b64(private.sign(message))}"


async def connect(base, service, pod, generation, token, route, extra=""):
    url = f"{base}/internal/v1/service-instances/{service}/{route}?generation={generation}&pod={pod}{extra}"
    return await websockets.connect(url, additional_headers={"Authorization": f"Bearer {token}"},
                                    proxy=None, max_size=65536, open_timeout=15)


async def command(base, service, pod, generation, token, cmd, tty=False, data=None):
    async with await connect(base, service, pod, generation, token, "ssh-exec") as ws:
        await ws.send(json.dumps({"type":"start", "command":cmd, "tty":tty, "cols":100, "rows":30}))
        started = json.loads(await asyncio.wait_for(ws.recv(), 60))
        assert started == {"type":"started"}, started
        if data is not None:
            await ws.send(data)
            await ws.send(json.dumps({"type":"eof"}))
        stdout = bytearray()
        stderr = bytearray()
        while True:
            message = await asyncio.wait_for(ws.recv(), 60)
            if isinstance(message, bytes):
                assert message and message[0] in (1, 2)
                (stdout if message[0] == 1 else stderr).extend(message[1:])
            else:
                info = json.loads(message)
                if info.get("type") == "exit":
                    return bytes(stdout), bytes(stderr), info["code"]


async def exercise(args, base, spec, private):
    def claims():
        now = int(time.time())
        return {"iss":"ssh-test", "aud":"ssh-test", "sub":str(uuid.uuid4()),
                "organization_id":spec["organization_id"], "project_id":spec["project_id"],
                "service_instance_id":args.service, "action":"flash.exec",
                "generation":spec["desired_generation"], "jti":str(uuid.uuid4()),
                "iat":now, "nbf":now - 5, "exp":now + 45}
    generation = spec["desired_generation"]
    if not args.only_forward:
        out, err, code = await command(base, args.service, args.pod, generation, sign(private, claims()),
                                       "printf ssh-stdout; printf ssh-stderr >&2; exit 42")
        assert (out, err, code) == (b"ssh-stdout", b"ssh-stderr", 42), (out, err, code)
        out, err, code = await command(base, args.service, args.pod, generation, sign(private, claims()),
                                       "printf ssh-tty", tty=True)
        assert out == b"ssh-tty" and err == b"" and code == 0, (out, err, code)
        out, err, code = await command(base, args.service, args.pod, generation, sign(private, claims()),
                                       "cat", data=b"ssh-stdin-eof")
        assert (out, err, code) == (b"ssh-stdin-eof", b"", 0), (out, err, code)
    if args.forward_port:
        async with await connect(base, args.service, args.pod, generation, sign(private, claims()), "ssh-forward",
                                 f"&port={args.forward_port}") as ws:
            assert json.loads(await asyncio.wait_for(ws.recv(), 60)) == {"type":"started"}
            await ws.send(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            output = bytearray()
            while b"\r\n\r\n" not in output and len(output) < 65536:
                message = await asyncio.wait_for(ws.recv(), 60)
                if isinstance(message, bytes): output.extend(message)
            assert output.startswith(b"HTTP/1.1 200"), output[:120]
    wrong = dict(claims(), organization_id=str(uuid.uuid4()))
    try:
        async with await connect(base, args.service, args.pod, generation, sign(private, wrong), "ssh-exec"):
            raise AssertionError("cross-organization token was accepted")
    except websockets.exceptions.InvalidStatus as error:
        assert error.response.status_code == 403, error.response.status_code
    print(json.dumps({"stdout_stderr_exit":"not_requested" if args.only_forward else "passed", "pty":"not_requested" if args.only_forward else "passed", "stdin_eof":"not_requested" if args.only_forward else "passed",
                      "port_forward":"passed" if args.forward_port else "not_requested", "cross_org":"rejected"}))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider-binary", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--service", type=lambda value: str(uuid.UUID(value)), required=True)
    parser.add_argument("--pod", required=True)
    parser.add_argument("--forward-port", type=int)
    parser.add_argument("--only-forward", action="store_true")
    args = parser.parse_args()
    if args.forward_port is not None and not 1 <= args.forward_port <= 65535:
        parser.error("invalid forward port")
    if args.only_forward and args.forward_port is None:
        parser.error("--only-forward requires --forward-port")
    service = resource(args.namespace, "flashservices.flash.heterocloud.io", f"flash-{args.service}")
    pod = resource(args.namespace, "pod", args.pod)
    assert pod["metadata"].get("labels", {}).get("flash.heterocloud.io/instance") == args.service
    assert pod["status"].get("phase") == "Running"
    spec = service["spec"]
    private = Ed25519PrivateKey.generate()
    public = private.public_key().public_bytes(Encoding.PEM, PublicFormat.SubjectPublicKeyInfo).decode()
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    env = os.environ.copy()
    env.update({"FLASH_API_BIND_ADDR":f"127.0.0.1:{port}", "FLASH_WORKLOAD_NAMESPACE":args.namespace,
                "HETEROCLOUD_PROVIDER_ISSUER":"ssh-test", "HETEROCLOUD_PROVIDER_AUDIENCE":"ssh-test",
                "HETEROCLOUD_PROVIDER_PUBLIC_KEYS_JSON":json.dumps({"ssh-test":public})})
    provider = subprocess.Popen([args.provider_binary], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(150):
            if provider.poll() is not None: raise RuntimeError("local test provider exited")
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=1): break
            except OSError: time.sleep(0.1)
        else: raise RuntimeError("local test provider did not start")
        asyncio.run(exercise(args, f"ws://127.0.0.1:{port}", spec, private))
    finally:
        provider.terminate()
        try: provider.wait(timeout=10)
        except subprocess.TimeoutExpired: provider.kill(); provider.wait(timeout=5)


if __name__ == "__main__":
    main()
