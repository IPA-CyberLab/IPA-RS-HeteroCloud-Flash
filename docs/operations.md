# Capacity admission and backup operations

Enable `capacity.enabled` only after cert-manager is available. Two HTTPS webhook
replicas use a resourceVersion-fenced ConfigMap reservation ledger. Admission is
limited to managed Flash Pods/PVCs in the workload namespace, including disposable
verification disks. Other namespaces and unrelated Pods are unaffected.

New Pods require recent kubelet nodefs/imagefs metrics, CPU/memory/request space,
inodes, runtime readiness, affinity/taint compatibility, image expansion and
remaining writable-layer budgets. Defaults leave 25% and at least 10 GiB free;
uncached images reserve six times compressed size, at least 1 GiB. In-flight
reservations prevent simultaneous admissions spending the same headroom.
Requests fail closed when metrics are unknown. Existing Pods are not evicted by
this webhook. Controller preflight preserves the prior deployment if a new image
or capacity cannot be validated. Autoscaling, replacement and verification Pods
also pass admission before image pull.

Longhorn create/grow preflight accounts for the full new replica delta on each
distinct eligible storage node. This is a physical-space guard for new changes,
not a retroactive conversion of legacy thin provisioning to thick provisioning.
Existing thin-provisioned volumes can still grow, caches can expand, and host
system processes can consume space. Continue monitoring host and Longhorn free
space; this guard does not guarantee capacity under every future write pattern.

## Disposable restore verification

Set `backups.snapshotClass`, `backups.verification.enabled` and optionally
`backups.verification.nodeTag`. A one-replica, Delete-reclaim storage class limits
verification space independently of production's replicated retained homes. The
cluster-wide verifier lease permits only one restore at a time. Verification
requires an immutable provider image including Python. The image is controlled
by the platform, never by the backup owner. Network access is denied and the
restored filesystem is mounted read-only. All temporary resource identities and
owner references are checked before deletion. Success includes verified cleanup.

## Independent S3 copy

`backups.offsite.enabled` adds a UTC CronJob (default 03:17). It requires
verification enabled for the restricted service account. The source Secret
`flash-backup-export-source` contains `config.json` with a bucket-scoped read-only
credential. The separate destination Secret `flash-backup-offsite` contains
`config.json` and `catalog.key` (at least 32 random bytes).

```json
{"endpoint":"https://ACCOUNT.r2.cloudflarestorage.com","bucket":"PRIVATE_BUCKET","region":"auto","access_key":"...","secret_key":"..."}
```

Use an independent private bucket outside the production cluster. Do not use a
public bucket or public website endpoint. Credentials belong in a protected
Secret, never Git, Terraform outputs/state or CLI arguments. Back up `catalog.key`
separately in an access-controlled recovery location; losing it prevents catalog
authentication. R2 public access remains disabled. The copier uses HTTPS and
rejects redirects before forwarding authorization headers. At-rest encryption is
provided by the destination storage service; this is not client-side encryption.

The copier deduplicates blocks by SHA-256, verifies new and reused objects, and
commits HMAC-authenticated catalogs only after all reads succeed. A Kubernetes
lease prevents overlapping exports. Per-backup locks delay deletion while an
export reads immutable ready backups. Catalogs include CSI snapshot metadata, not
Pod environment values. Keep seven catalogs by default, preserving active restore
leases; unreferenced blocks are removed only after a two-day grace period.
Limits default to 40 GiB of source objects and 512 GiB of destination objects.
The independent bucket must also have a provider-enforced spending/storage cap
appropriate to the deployment. Do not mistake a second bucket in the same
cluster for protection against cluster loss.

## Recovery to a replacement backup store

Run `offsite.py list` against the independent copy, then select an explicit
committed catalog. Run `restore` with the independent bucket as source and a
**new empty private S3 bucket** as destination:

```sh
python3 -B /usr/local/lib/flash-ops/offsite.py restore \
  --source-config /protected/offsite.json \
  --destination-config /protected/replacement.json \
  --signing-key-file /protected/catalog.key \
  --catalog catalogs/SELECTED.json \
  --metadata-output /protected/recovered-csi-metadata.json
```

It refuses a nonempty destination, checks the authenticated catalog and all block
hashes, verifies copied objects and holds a renewable source lease against
retention. Configure the replacement Longhorn backup target and recreate/import
the recovered CSI catalog under the platform's disaster-recovery procedure.
This restores backup-store data; it does not replace control-plane PostgreSQL,
IAM or Secret Manager backups. Test that wider recovery procedure separately.

## Checks

```sh
python3 -B -m unittest discover -s ops -p 'test_*.py' -v
cargo test --all-targets --locked
helm template heterocloud-flash deploy/helm/heterocloud-flash \
  --set providerAuth.existingSecret=validation --set capacity.enabled=true \
  --set backups.snapshotClass=flash-disk-backup \
  --set backups.verification.enabled=true --set backups.offsite.enabled=true
```
