# 2026-10-04 Flash: image update blocked and container diagnostics unavailable

Status: incident remediation in progress. All times below are UTC.

## Impact and evidence

The requested update of `kabubot-monitor`
(`01a0ff77-c342-7a80-b596-85bb3c792c89`) was accepted at **10:11:06** as
generation 4, but the old container continued running. The provider's desired
generation was 4, observed generation was 3, and the Deployment template still
used generation 2. Container discovery and WebSocket exec through HeteroCloud
returned 503. Direct Kubernetes exec into the existing container succeeded.

The reporter also observed a container-list 500 and intermittent 503 for
`kabubot-runner` (`01a0ff76-0c51-7752-b251-792c215ff47d`). Both existing bot Pods
were Running/Ready with zero restarts during investigation. Runner's database
generation, provider desired generation, and observed generation were all 2;
a signed provider container-list request returned 200 during investigation.
Do not classify the runner's earlier 503 as the same generation mismatch
without additional evidence.

There is no evidence of persistent-volume deletion or bot data loss in this
incident. Successful infrastructure recovery does not establish that the bot
sent a Discord notification; that is a separate application-level check.

## Confirmed causes

### Image updates recomputed and attempted to shrink an existing PVC

The controller divided the writable disk budget (configured disk limit minus
image size) between the container filesystem and `/root` on every reconcile.
It did not read the existing persistent-volume allocation before applying a
new PVC request. An image-size change could therefore produce a smaller PVC
request, even though the user had not requested a disk shrink.

The existing monitor PVC requested **4,623,292,749 bytes**, with provisioned
capacity **4410Mi = 4,624,220,160 bytes**. The controller repeatedly received
Kubernetes HTTP 422:

```text
spec.resources.requests.storage: Forbidden: field can not be less than status.capacity
```

The error occurred before the Deployment update and status refresh. Consequently
the service remained `updating`, and the visible message retained an earlier
OCI image-inspection network error rather than the current PVC failure.
[Kubernetes does not support shrinking a volume below its current capacity.](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#expanding-persistent-volumes-claims)

### Diagnostics incorrectly required successful reconciliation

`validate_resource_access` required `status.observed_generation` to equal the
current signed generation, in addition to checking identity and desired
generation. A failed update therefore disabled list/exec for the still-running
old container. A signed request reproduced provider HTTP 503 with
`operation_in_progress`; HeteroCloud translated this into
`flash_provider_unavailable`.

This coupled the recovery tool to the operation it was needed to diagnose.
Status freshness is relevant to reporting update completion, not to authorizing
access to an already-owned, running container.

## Concurrent failure with an incomplete causal chain

At **10:11:50.403**, HeteroCloud API logs on `ichikawap1` recorded a database
connection closing without TLS `close_notify`, then an unexpected EOF and HTTP
500. This establishes a database transport failure; it does **not** establish
whether a proxy, network interruption, or database event closed the connection.
The primary's postmaster start time predates this incident (2026-10-01).

The database autopilot on that node also logged `invalid database member count`
and an invalid proxy bundle roughly every 30 seconds. A causal link between
these messages, the EOF, and runner's intermittent 503 has not been established.
Do not mark these failures fixed merely because requests later succeed.

## Fix

Release **0.1.42**:

- Read the existing PVC before computing an update. Retain its requested
  allocation, including any pending expansion; never reduce the persistent
  allocation to accommodate an image change.
- For an automatic split, use the remaining writable budget for the container
  filesystem while preserving its 64 MiB minimum. An explicit rootfs allocation
  is not silently reduced. If the existing disk and requested filesystem do not
  fit, report an actionable error and preserve the running Deployment and PVC.
- Apply a PVC storage change only when growth exceeds both the existing request
  and actual capacity. CSI capacity rounding must not turn an image update into
  an invalid resize. Quota allocation uses requested bytes; provisioner rounding
  remains visible in PVC capacity. Unchanged allocations stay unchanged.
- Allow diagnostics with an authenticated, correctly scoped current command
  even if status is missing or stale. Preserve organization, project, service,
  desired-generation, deletion, Pod ownership, and running-container checks.
  The separate status endpoint continues enforcing status freshness.
- Allow an independently pinned `workloadHelperImage` in Helm, so this provider
  hotfix can keep the unchanged secret-launcher image and avoid rolling unrelated
  workloads solely because the provider version changed.

## Why existing tests missed it; prevention

Storage tests covered initial creation and a split within the disk budget, but
did not supply an existing PVC during an image update. Diagnostics tests covered
non-ready phases with the *same* observed generation. The retained-Pod test
checked Pod selectors and exec capability without exercising resource access.

Added regressions exercise:

1. Image-update allocation with an existing disk, insufficient remaining space,
   explicit filesystem size, and stable allocations for unchanged workloads.
2. Rounded capacity and pending expansion; no shrink or redundant resize.
3. Exact fixed-point storage quantities, fractional-byte rounding, invalid data,
   and overflow.
4. Signed HTTP container discovery with stale or absent status and a retained
   previous-generation Pod, plus the shared exec authorization checks.
5. Existing identity/deletion/generation rejection and status-freshness tests
   remain in the suite.

Local validation: 95 Rust tests, clippy with warnings denied, 11 release-artifact
tests, secret-launcher checks, and Helm lint passed. Release CI and live
verification results will be appended after deployment. A passing unit suite is
not a substitute for verifying the accepted generation, actual running image,
retained PVC identity, and list/exec on both affected services.

## Deployment and recovery evidence

Pending: immutable release digest, GitOps revision, Argo synchronization,
monitor generation 4 readiness and image digest, authenticated list/exec results,
runner continuity, and PVC UID preservation. No recovery timestamp is asserted
until those checks have completed.
