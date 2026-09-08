# Autoscaling and domain endpoints

These are optional workload fields; existing requests remain fixed-replica with
`exposure.endpoint_mode: "ip"` by default. No release or deployment is implied.

```json
{
  "replicas": 3,
  "autoscaling": {
    "min_replicas": 2,
    "max_replicas": 10,
    "target_cpu_utilization_percent": 65,
    "target_memory_utilization_percent": 80
  },
  "exposure": {
    "type": "public",
    "traffic_mode": "forwarded",
    "endpoint_mode": "load_balancer"
  }
}
```

This fragment belongs inside an otherwise complete Flash spec. Each target is
optional independently, but at least one is required. Targets are integers from
1 through 100, measured against per-Pod resource requests. Bounds must satisfy
`1 <= min_replicas <= replicas <= max_replicas <= 100000`.

## Autoscaling

The controller manages an owned `autoscaling/v2` HorizontalPodAutoscaler targeting
the workload Deployment. CPU-only, memory-only, and combined targets are supported.
With both targets Kubernetes selects the larger replica recommendation, so either
resource can trigger scale-up. Scale-down stabilization is 300 seconds. A working
resource metrics API (normally metrics-server) is a platform prerequisite; missing
metrics can prevent scaling. Flash does not install metrics-server or reserve quota.
HeteroCloud must reserve maximum capacity before issuing provider commands, including
its policy for the existing one-Pod rolling-update surge. Existing resource requests,
limits, gVisor isolation, scheduling, storage, and network policies remain unchanged.
Persistent `/root` storage remains a shared ReadWriteMany volume, not one PVC per Pod.
Autoscaling runs more copies of the same application. Existing admin S3 mounts/PVCs
are shared across those copies; applications must coordinate concurrent writes,
locking, and durable state themselves. There is no implicit session or state
synchronization between Pods. Enabling this provider code does not require a Pod
template change or restart for unchanged fixed-replica workloads.

The initial count uses `replicas`. For existing Deployments, the live count is
preserved during handoff. A dedicated replica field manager shares that count
before the workload manager omits `spec.replicas`; it remains dormant under HPA.
Resource-version preconditions prevent a stale handoff from overwriting an HPA
update. On removal of `autoscaling`, the controller waits for HPA deletion before
reclaiming the requested fixed count. Terminal image rejection also removes the HPA
before suspending the Deployment. Status uses the Deployment's live desired count,
not the original seed. Workload readiness does not certify metrics availability.

See Kubernetes documentation on [multiple metrics](https://kubernetes.io/docs/concepts/workloads/autoscaling/horizontal-pod-autoscale/)
and [SSA replica ownership transfer](https://kubernetes.io/docs/reference/using-api/server-side-apply/).

## Domain endpoints

Only public, forwarded exposure supports `load_balancer`. The provider Helm value
`publicDomain` maps to controller environment variable `FLASH_PUBLIC_DOMAIN`.
For example, `flash.heterocloud.mizuame.app` produces exactly
`f-<service_instance_id>.flash.heterocloud.mizuame.app`. The ID is a UUID; the suffix
is trusted operator configuration. Display names and tenant metadata cannot select
a hostname. Missing configuration produces an error status with no endpoints and
does not provision a new domain Service. Invalid suffixes stop controller startup.
Existing workloads are not deleted when configuration is missing.

This is L4 load balancing: connect using the declared TCP or UDP `hostname:port`.
It does not provision HTTP routing, certificates, TLS termination, or implicit HTTPS.
The existing `heteronetwork.io/public` class and forwarded `Cluster` traffic policy
balance across selected workload Pods. Client source allow/deny ranges remain on
the Service for pre-SNAT enforcement; Pod policies continue to allow only declared
ports, public source blocks, and assigned forwarder addresses. No extra tenant or
infrastructure access is granted by enabling domains or autoscaling.

Domain-mode Services alone receive:

- Label `dns.heterocloud.io/publish: "true"`.
- Annotation `external-dns.alpha.kubernetes.io/hostname` with the derived hostname.
- Annotation `external-dns.alpha.kubernetes.io/ttl: "60"`.
- Annotation `external-dns.alpha.kubernetes.io/cloudflare-proxied: "false"`.

ExternalDNS must watch Services with that label and include the suffix in its managed
zone/domain filters. HCloud currently manages A records only, so usable IPv4 LB ingress
is required for DNS publication; IPv6-only or hostname-only ingress is insufficient
for that configuration. Cloudflare remains DNS-only for arbitrary TCP/UDP ports.

Switching to IP mode omits the publication label and all three DNS annotations from
the same SSA manager, removing its previously owned fields while preserving annotations
owned by the network controller. DNS record deletion still depends on ExternalDNS's
registry ownership and sync policy (upsert-only will not delete old records).
Helm does not automatically upgrade installed CRDs from `crds/`; operators must arrange
the CRD update through their normal infrastructure workflow before using new fields.

Status exposes only the derived hostname for domain mode, never ingress IPs. Endpoints
are withheld until LB ingress is allocated. Ready means Pods and LB allocation are
ready, not DNS verification; the status message explicitly states that DNS publication
and resolution are unverified. ExternalDNS propagation is asynchronous.

## Verification

Run `cargo test --all-targets`, `cargo check --all-targets`, and `cargo fmt --all --check`.
Unit and request-level tests cover target validation, independent metrics, handoff
ordering, fixed/HPA transitions, hostname derivation, DNS-field omission, status,
and unchanged source filtering/isolation. They do not emulate the API server's SSA
engine or establish live metrics, DNS propagation, or network dataplane behavior.
No tenant runtime or cluster needs to be contacted for these tests.
