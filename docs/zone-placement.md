# Zone placement

The controller adds a zone spread constraint to new multi-replica stateless
service generations, including autoscaled services whose maximum exceeds one.
Operators must first declare truthful `topology.kubernetes.io/region` and
`topology.kubernetes.io/zone` labels on eligible nodes. Missing labels do not
prove that nodes are independent. The existing hostname spread remains enabled.
Taints and node affinity are honored when deciding which zones are eligible, so
dedicated control-plane nodes without matching tolerations do not count.

Set `controller.zoneSpread` in the Helm values to:

- `preferred` (default): favor different zones but retain availability in one.
- `required`: require two eligible zones and a maximum skew of one. If fewer
  than two zones are eligible, excess replicas stay pending. This intentionally
  trades replica availability for a strict failure-domain requirement.
- `disabled`: retain hostname-only placement.

A controller upgrade or policy change does not restart an existing service
generation. The new placement policy applies when a service creates its next
generation. Kubernetes does not automatically move existing Pods when a failed
zone recovers; new scheduling decisions use the current policy.

One persistent home is a ReadWriteOnce volume. The controller does not apply
the zone-spread policy to replicas sharing that home, and a volume with copies
in two zones is not an application-level multi-writer or an offsite backup.
One replica, a single shared database, one power supply or a common network can
remain a single point of failure even if scheduling labels differ. See the
platform's storage and control-plane quorum status before promising site
survival.

`scripts/zone-placement-kind-e2e.py` consumes real controller-generated Pod
constraints in a disposable two-worker kind cluster. It taints one worker to
model one unavailable site, checks strict pending versus preferred recovery,
and restores the worker. This validates Kubernetes scheduling behavior; it
does not emulate WAN packet loss, storage replication or independent power.
