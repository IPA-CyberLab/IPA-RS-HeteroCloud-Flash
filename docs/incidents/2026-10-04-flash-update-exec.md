# 2026-10-04 Flash: image update blocked and container diagnostics unavailable

Status: recovered on 2026-10-04. Monitor generation 4 became `ready` at
**10:58:07**; authenticated public list/exec verification completed at
**11:01:24**. All times below are UTC.

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

## Database transport failure: cause established during investigation

At **10:11:50.403**, HeteroCloud API logs on `ichikawap1` recorded a database
connection closing without TLS `close_notify`, then an unexpected EOF and HTTP
500. At **10:11:50.396701**, the same node's HAProxy marked backend `db-e`
DOWN and closed 18 sessions under `on-marked-down shutdown-sessions`, immediately
preceding the API error. The primary's postmaster start time predates this
incident (2026-10-01).

The database autopilot on that node also logged `invalid database member count`
and an invalid proxy bundle roughly every 30 seconds. Two health frontends were
bound to the same member overlay address/port: one forwarded to the current
primary, and the other to the local replica. Requests to the replica's overlay
health endpoint returned 7 successes and 13 HTTP 503 responses in 20 attempts.
This ambiguous listener caused downstream failure detection and connection
termination. The proxy reconciler additionally rejected the valid recovered
two-database/three-DCS-voter topology, preventing automatic repair.

The network repository records the [database root cause, fixes, and IaC
recovery](https://github.com/IPA-CyberLab/IPA-RS-HeteroNetwork/blob/master/docs/incidents/2026-10-04-db-proxy.md).
No complete causal trace has been established for runner's originally reported
503. Both bots later passed authenticated public container listing and
WebSocket exec even before the Flash rollout; monitor's observed generation
had advanced to 4, while its old image still ran. This shows why successful
exec alone does not establish update completion.

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
tests, secret-launcher checks, and Helm lint passed. The release workflow also
passed verification and both architecture builds. A passing unit suite is not a
substitute for verifying the accepted generation, actual running image,
retained PVC identity, and list/exec on both affected services. The live rollout
also exposed pending CSI expansion/rounding affecting other workloads, which
the stable-allocation unit cases did not model; that impact is recorded below.

## Deployment and recovery evidence

Flash source fix: [`efdf87c`](https://github.com/IPA-CyberLab/IPA-RS-HeteroCloud-Flash/commit/efdf87c53873f36d3c520299bbcf3ad205ede06e).
[Release v0.1.42](https://github.com/IPA-CyberLab/IPA-RS-HeteroCloud-Flash/releases/tag/v0.1.42)
uses immutable image index
`sha256:04a873a32b794cf85e40b3be523e9a73b129403ec7a40e61f1797a3750d98757`.
The [release CI](https://github.com/IPA-CyberLab/IPA-RS-HeteroCloud-Flash/actions/runs/37196041550)
passed. Network GitOps commit
[`2960c050`](https://github.com/IPA-CyberLab/IPA-RS-HeteroNetwork/commit/2960c050df19bce9127fdda2253c8305ab91fea3)
pins this image and the unchanged v0.1.41 workload helper. The targeted Terraform
apply completed; Argo CD reported **Synced / Healthy** at the release revision.
Live CRD validation and secret environment injection/removal acceptance passed;
the disposable acceptance workload was cleaned up.

| Time | Evidence |
| --- | --- |
| 10:11:06 | Monitor update accepted as generation 4. |
| 10:11:50 | Database proxy closed 18 sessions; API logged EOF/500. |
| 10:48:57–10:55:07 | One database session completed 180 queries over 370 seconds with no errors or session replacement after the proxy fix. |
| 10:54:09 | New monitor workload started with the requested image digest. |
| 10:55:39 | Monitor CRD was ready at generation 4; PVC identity and request were retained. |
| 10:58:07 | Existing outbox retry completed and the public service state became `ready`, without resubmitting the update. |
| 11:01:24 | Final authenticated public list/exec checks passed for monitor and runner. |

Monitor's Pod image and runtime image ID both match
`ghcr.io/mizuamedesu/kabubot-monitor@sha256:a639ce0691346e6976f53407f991e20c305b9b36a986cab127ff3de965f2ae47`.
The PVC retained UID `3c4d0cf4-d45a-440a-847d-f7253cd26a87` and its original
4,623,292,749-byte request. The 437,226,477-byte rootfs allocation plus that
request and the 308,189,894-byte image equals the configured 5 GiB budget.
Monitor required the expected Pod replacement to run its new image.

The first post-rollout API check correctly failed the overall acceptance
because the service record still said `updating`, despite successful list/exec
and a ready new Pod. The existing reconciliation event was waiting for its
backoff deadline, 10:58:07. Its eighteenth attempt delivered successfully; no
generation was incremented or database state manually marked ready.

Final public verification made **60/60 successful HTTP container-list requests**
using new connections and executed a harmless `printf` through each service's
WebSocket, checking the returned marker. Both services reported `ready` at
their original requested generations (monitor 4, runner 2). Temporary IAM
credentials were scoped to these two services and revoked after the probe.
All three API replicas' logs contained no matching TLS EOF, database connection,
or container-diagnostic failures from 10:48 through the 11:01 observation.

The final [GitHub runner VPN + Chromium workflow](https://github.com/IPA-CyberLab/IPA-RS-HeteroNetwork/actions/runs/37196780031)
passed. An [earlier run during recovery](https://github.com/IPA-CyberLab/IPA-RS-HeteroNetwork/actions/runs/37196428630)
failed the console's three-second navigation limit; it is not counted as a pass.

### Additional rollout impact

The broad check that every other workload remained on the same Pod **failed**:
nginx and whisper also rolled while their pending volume expansions and CSI
rounding settled. Their application images and the whisper secret helper image
did not change. The retained PVC requests increased to rounded allocations,
and the controller reduced automatic rootfs allocation by the same number of
bytes to stay within each service's disk budget:

| Service | PVC request before → after (bytes) | Rootfs before → after (bytes) |
| --- | --- | --- |
| nginx | 945,303,070 → 945,815,552 | 105,033,674 → 104,521,192 |
| whisper | 18,079,871,244 → 18,081,644,544 | 1,073,741,824 → 1,071,968,524 |

Both workloads were ready by the final observation. All five service PVC UIDs
were retained. Runner and the development service
`01a101e7-5acd-78a0-b8e9-88950db3ecad` retained the same Pod UID, container ID,
and restart count; the development container's earlier OOM restart is separate
from this incident. There was no PostgreSQL restart. These checks establish
observed continuity, not an application data-integrity audit or proof of zero
interruption for the two additionally replaced Pods.

The [sanitized verification record](2026-10-04-flash-update-exec-verification.json)
includes successful recovery checks, the earlier pending-state result, the
failed broad continuity check, and CI links. Discord delivery remains unverified.
