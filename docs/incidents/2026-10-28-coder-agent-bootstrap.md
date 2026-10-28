# Coder workspace agent never connects (2026-10-28)

## Impact and evidence

The new `ramen` Big workspace reported `One or more workspace agents need
attention`. Coder's Terraform build completed, but Flash remained in image
inspection before a workload Pod existed. The Coder agent had never connected;
this was not a Docker daemon error inside the workspace.

The configured Harbor manifest endpoint returned HTTP 502. The registry data
volume was detached/faulted, with its only registered storage replica on a
retired worker. A successful `/v2/` authentication challenge did not establish
that manifest or blob reads worked. Existing running workspaces had the exact
immutable Coder image cached locally and could keep running.

The independent second blocker was storage admission. Longhorn desired three
replicas and already allowed degraded volume creation, while only two eligible
storage workers were online. The Flash webhook required all three nodes anyway,
so a new home remained blocked despite sufficient physical headroom on both
live storage nodes.

## Immediate recovery

For the affected workspace, the operator verified the immutable OCI index,
platform manifest, configuration and cached layers against their SHA-256
digests, then restored image inspection metadata. The empty new home was
provisioned under the existing storage system's degraded-creation policy,
preserving the desired three replicas and physical-space reserves on both live
storage workers. Existing user volumes and running workspaces were retained.

`ramen` reached one ready Flash replica and a connected/ready Coder agent.
Chromium verified the VS Code workbench and Explorer, bidirectional WebSocket
traffic, five seconds of connection stability, and a successful page reload.

## Provider correction

- Registry availability errors may reuse complete provider-recorded metadata
  for the same organization's exact repository and immutable SHA-256 image.
  Authentication, missing/invalid content, mutable tags and other tenants are
  excluded; destination disk space is recalculated.
- Storage admission reads the actual storage-class/native replica policy and
  honors explicit degraded creation with one missing replica, at least two
  healthy independent storage workers and full physical-space reservation.
  It leaves the desired replica count unchanged and fails closed when policy
  cannot be read.
- The capacity service account can read only the two required Longhorn
  settings. Provider rollouts retain the existing workload helper image to
  avoid restarting user containers for a controller-only correction.

Regression tests cover tenant/digest isolation, invalid metadata, registry
availability versus authentication/content failures, two-worker recovery,
single-copy rejection, native per-engine replica defaults, unavailable policy,
pending reservations and physical-space exhaustion.

## Approved registry reset

The owner subsequently approved discarding the lost registry data. A new
32 GiB registry volume was created with two healthy copies on independent
workers. New storage defaults to two replicas and disallows initial creation
with missing replicas. The old registry volume and its orphaned data were
deleted; user workspace homes were retained.

Lost artifact records and stale blob-descriptor cache entries were cleared.
The cached Linux Coder platform image was verified and republished, with every
blob read back through the public registry and checked against its digest.
Lost attestations were not fabricated: future Standard Develop and Big builds
use the verified platform digest in a newly validated template version.
Other discarded images require a fresh push.

The provider chart also sets an explicit one-at-a-time rollout strategy for
capacity and verification replicas. The default percentage rounds to zero
unavailable replicas for a two-replica deployment, which deadlocks against
required placement on distinct hosts when only two workers exist. Production
API replicas were aligned to the available two workers, retaining the separately
pinned workload helper image. Full recovery evidence is recorded in the
[infrastructure incident report](https://github.com/IPA-CyberLab/IPA-RS-HeteroNetwork/blob/master/docs/incidents/registry-reset-2026-10-28.md).
