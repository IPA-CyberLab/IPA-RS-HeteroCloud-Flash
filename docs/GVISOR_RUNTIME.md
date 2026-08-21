# HeteroCloud Flash gVisor runtime

HeteroCloud Flash is the sandboxed container service in HeteroCloud. It is
positioned alongside HeteroCloud Flow: Flow provides real-time communication,
while Flash runs tenant workloads with a stronger isolation boundary than a
host-native OCI runtime.

The host bootstrap is paired with the `FlashService` operator, private provider
API, HeteroCloud management API, and console. Tenant input never selects the
runtime handler: the operator always sets `runtimeClassName: gvisor`.

## Supported hosts

The installer supports:

- Ubuntu on `amd64` and `arm64`
- containerd 1.x with config version 2
- containerd 2.x with config version 3
- containerd 2.x retaining a backwards-compatible version 2 config
- systemd-managed `containerd.service`

The host must already have containerd installed and must be able to reach the
Ubuntu and official gVisor package repositories. Run the installer as root:

```bash
sudo ./scripts/install-gvisor.sh
```

The installer converges the following state:

1. Installs repository prerequisites and `runsc` from the official gVisor
   `release` APT repository.
2. Stores the repository key in
   `/usr/share/keyrings/gvisor-archive-keyring.gpg` and restricts that repository
   with `signed-by`.
3. Preserves `/etc/containerd/config.toml` and adds only the top-level import
   for `/etc/containerd/conf.d/*.toml` when it is absent.
4. Writes the `runsc` runtime handler to
   `/etc/containerd/conf.d/50-heterocloud-flash-runsc.toml` using the plugin ID
   required by config version 2 or 3.
5. Asks containerd to parse the complete candidate configuration before it is
   activated.
6. Restarts containerd only when either containerd configuration file changed.
   If validation or restart fails, it exits nonzero and restores the previous
   files.

Repeated successful runs with the same package and configuration state do not
rewrite the containerd files and do not restart containerd. The scripts do not
enable shell tracing or print containerd configuration, APT output, registry
credentials, or temporary file contents.

Symbolic-link configuration files are rejected rather than replaced because
they normally indicate that another system owns the containerd configuration.
Config versions other than 2 and 3 are also rejected explicitly.

## Verification

Run the read-only host check after installation:

```bash
sudo ./scripts/check-gvisor.sh
```

When SSH sudo is intentionally unavailable but Kubernetes administration is
available, the same host installer can be applied one node at a time through a
temporary privileged bootstrap Pod:

```bash
sudo ./scripts/rollout-gvisor-kubernetes.sh node-a node-b
```

The helper waits for one node to pass the full host check before moving to the
next node, labels only successful nodes with
`flash.heterocloud.io/gvisor-ready=true`, and removes its Pod and ConfigMap.
The privilege is limited to the explicitly named bootstrap namespace and is
not used by tenant workloads.

After host installation, verify the CRI path itself on every node. This starts
one restricted Pod per node, requires the sandbox kernel banner, and removes
the scheduling label again if any node fails:

```bash
sudo ./scripts/verify-gvisor-kubernetes.sh node-a node-b
```

It verifies Ubuntu and CPU support, the signed official repository, installed
gVisor binaries, the containerd version, the drop-in import, the effective
`runsc` handler, and the active containerd service. It exits nonzero if any
check fails. Effective containerd configuration is held only in a mode-0700
temporary directory and is never printed.

The registered CRI runtime handler is named `runsc`. A cluster layer can map it
to a Kubernetes RuntimeClass as follows:

```yaml
apiVersion: node.k8s.io/v1
kind: RuntimeClass
metadata:
  name: gvisor
handler: runsc
scheduling:
  nodeSelector:
    flash.heterocloud.io/gvisor-ready: "true"
```

Flash workloads then select it with:

```yaml
spec:
  runtimeClassName: gvisor
```

Installing the host runtime does not make `runsc` the default runtime, so
existing workloads continue using their existing handler.

## UDP networking

HeteroCloud Flash uses gVisor's default sandbox networking. No
`--network=host` override is installed. TCP and UDP are processed by gVisor's
userspace **netstack** inside the sandbox and pass through the pod network's
virtual interface.

UDP therefore remains available to Flash containers without bypassing gVisor.
Applications must still listen on the intended UDP socket, declare the UDP
container and Service ports, and satisfy the cluster's CNI, NetworkPolicy,
firewall, NAT, and load-balancer rules. Netstack does not itself publish a UDP
port or override those controls.

Host networking should not be enabled merely to make UDP work. It trades away
netstack's network isolation and is outside the Flash runtime profile.

## References

- [gVisor installation](https://gvisor.dev/docs/user_guide/install/)
- [gVisor networking](https://gvisor.dev/docs/user_guide/networking/)
- [containerd CRI runtime classes](https://github.com/containerd/containerd/blob/main/docs/cri/config.md#runtime-classes)
- [containerd configuration imports](https://github.com/containerd/containerd/blob/main/docs/man/containerd-config.toml.5.md)
