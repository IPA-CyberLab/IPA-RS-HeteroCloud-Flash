# GPU inventory and scheduling

Flash users select a GPU type. They never select a node, PCI address, NVIDIA
UUID, or a particular card. A typed request allocates one GPU to one Flash VM:

```json
{"gpu_type": "nvidia-geforce-gtx-1080-ti"}
```

Omitting `gpu_type` creates a CPU VM. The legacy `gpu_count` field remains
accepted during migration, with a maximum value of one, but new clients omit
it. GPU services use one replica; scale-to-zero may use `min_replicas: 0`, but
`max_replicas` remains one.

## Inventory contract

Each physical GPU is a cluster-scoped `FlashGpuDevice` in
`flash.heterocloud.io/v1alpha1`. The resource name is an opaque management ID;
the infrastructure convention is `gpu-` followed by the first 16 lowercase
hex characters of SHA-256 over the NVIDIA UUID. The raw UUID is stored only in
`spec.physical_id` and is not returned by either catalog.

Hardware automation owns only these fields with the
`heteronetwork-gpu-inventory` server-side-apply manager:

```yaml
spec:
  node_name: uc-k8sp5
  physical_id: GPU-...
  gpu_type: nvidia-geforce-gtx-1080-ti
  model: NVIDIA GeForce GTX 1080 Ti
  memory_mib: 11264
```

`spec.visibility` defaults to `open` and `spec.private_assignments` defaults to
an empty list. The owner API manages those access fields separately, so a
hardware refresh cannot undo console changes. Open devices are visible to every
authenticated subject; private devices are visible only to assigned subjects.
A private device with no assignments is intentionally invisible to everyone
and can be used for isolation or maintenance.

The owner API force-applies only `visibility` and `private_assignments` with
field manager `heterocloud-owner-api`. This explicitly transfers those two
fields away from any manager that owned CRD defaults during the first hardware
apply. Later `heteronetwork-gpu-inventory` applies omit them and therefore
cannot reset console changes.

The scheduler derives status health from the Kubernetes Node `Ready`
condition and both labels:

```text
flash.heterocloud.io/gpu-ready=true
flash.heterocloud.io/gpu-type=nvidia-geforce-gtx-1080-ti
```

Install the Helm release before applying inventory, and wait for discovery:

```bash
kubectl wait --for=condition=Established --timeout=120s \
  crd/flashgpudevices.flash.heterocloud.io \
  crd/flashgpujobs.flash.heterocloud.io
kubectl apply --server-side \
  --field-manager=heteronetwork-gpu-inventory \
  -f flash-gpu-inventory.yaml
```

## Scheduling lifecycle

`FlashGpuJob` is a namespaced internal queue record owned by a `FlashService`.
The queue serves the least recently allocated organization first, then the
least recently allocated user within that organization. Each user contributes
only their oldest compatible job, ordered by `queued_at` and resource name.
Within a type, devices are ordered by their last allocation and opaque
inventory name. These stable tie-breakers make placement deterministic while
rotating capacity across organizations, users, and devices.

The scheduler filters inventory by authenticated subject, requested type,
health, and active reservation. It reserves capacity with a Kubernetes status
replacement carrying the current `resourceVersion`; simultaneous attempts
therefore produce one winner and an HTTP 409 retry for the loser. Reservations
use a 90-second lease, renew every 30 seconds, and are reclaimed after expiry.
Job deletion releases its reservation through a finalizer. Lost leases return
the job to the queue.

Cold or weekly-quota-suspended services release their job and run zero Pods. A
cold-start request recreates the job. A zero remaining weekly GPU limit is
rejected before placement. The shared weekly CPU, memory, and GPU allocation
meter remains the source of the GPU quota value copied into a job.

The Pod requests and limits `nvidia.com/gpu: 1`, uses the `nvidia`
RuntimeClass, and has required node affinity for the selected node and GPU type.
The NVIDIA device plugin chooses the final UUID on that node. For multiple
identical cards, each inventory record is a logical capacity slot; the UUID
selected by the plugin need not match that slot's `physical_id`. This is safe
because eligible slots on that node have the same canonical type and Kubernetes
atomically accounts `nvidia.com/gpu` capacity.

## Provider APIs

The user type catalog aggregates accessible inventory and returns only
`gpu_type`, `display_name`, `access`, `total`, and `available`. Inventory with
different model names under one canonical type is marked unhealthy and rejected
from catalog synchronization. Management synchronization uses:

- `GET /internal/v1/gpus`, action `flash.gpus.catalog.list`
- `PUT /internal/v1/gpus/access`, action `flash.gpus.access.update`

Both require an owner command with nil subject, organization, project, and
service UUIDs and generation 1. Management responses contain `management_id`,
`gpu_type`, `display_name`, `visibility`, `assigned_user_ids`, and dynamic
`available`; they never contain the physical ID or node. Access update requests
omit `available` because it is scheduler-owned state.

Tenant provider JWTs keep PrincipalId in `sub` for command authentication and
carry the resolved owner account UserId in optional `user_id`. GPU visibility
and job ownership use `user_id`. Tokens issued before this claim existed see
open inventory only; a PrincipalId is never compared with private UserId
assignments.
