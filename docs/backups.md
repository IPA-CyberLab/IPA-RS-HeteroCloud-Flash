# Durable disk backups

Users operate backups through HeteroCloud's console, API and `flash backups`
CLI commands. This provider exposes only signed internal commands scoped to
organization, project, source service and action. No storage credential or
backend path is returned to users.

Enable `backups.snapshotClass` only after installing snapshot CRDs/controller,
configuring the Longhorn default BackupTarget and validating its private S3
bucket. The class must use `driver.longhorn.io`, `parameters.type: bak`, and
`deletionPolicy: Delete`. `snap` is a local snapshot and is rejected. Set
`backups.storageNamespace` to the Longhorn namespace (default `longhorn-system`).
The chart grants read-only access to Longhorn volume restore status there.

`FlashBackup` owns the CSI VolumeSnapshot and has no source service owner.
Deleting a source service therefore preserves backups. Its protection finalizer
waits for active restores and CSI storage deletion. The CRD is kept during Helm
uninstall or Argo pruning; decommissioning backups is an explicit operator action.

Snapshot `readyToUse` confirms backup upload. Restore creates a different PVC
with a snapshot dataSource and keeps the new service stopped. Source UID,
organization, immutable restore origin and existing claim ownership are checked.
An existing disk is never replaced. Resizing source disks are rejected.

**PVC Bound does not mean restoration is finished:** Longhorn's CreateVolume
returns before copying backup data. Completion requires `restoreInitiated=true`
and `restoreRequired=false` on the new Longhorn volume. Both user-visible
readiness and backup deletion wait for that condition. Once copied, restoring
services remain independent of their backup's metadata and lifetime.

The infrastructure repository provides `scripts/flash_backup_storage_e2e.py`.
It uploads a real backup, changes and deletes only its own isolated source
volume, restores and verifies file hashes, tests backup deletion, and cleans up
its uniquely labelled resources. Run it with an explicit kubeconfig, fixture
image digest and report path. It never accesses existing user volumes.

Backups are crash-consistent. Stop or quiesce applications requiring transaction
consistency. Only `/root` is captured; container image layers, temporary storage,
secret/environment bindings and additional admin mounts are outside this scope.
The cluster's snapshot metadata and its S3 target must both remain available.
