#!/usr/bin/env python3
"""Restore a disposable read-only clone, verify readability, and remove it.

The application is never started and the source volume is never mounted. A
single resourceVersion-fenced lease bounds concurrent restores across replicas.
Only objects with this backup's UID and verification label can be removed.
"""
import datetime as dt
import json
import os
import time
import urllib.error
from capacity import Kube, NS, STORAGE_NS, SYSTEM_NS

BASE = f"/apis/flash.heterocloud.io/v1alpha1/namespaces/{NS}/flashbackups"
REQUEST = "flash.heterocloud.io/verify-request"
RESULT = "flash.heterocloud.io/restore-verification"
LABEL = "flash.heterocloud.io/backup-verification"
LEASE = f"/apis/coordination.k8s.io/v1/namespaces/{SYSTEM_NS}/leases/flash-backup-verifier"
HOLDER = os.environ.get("HOSTNAME", "verifier")
STORAGE_CLASS = os.environ.get("FLASH_VERIFICATION_STORAGE_CLASS", "flash-backup-verification")
IMAGE = os.environ.get("FLASH_VERIFICATION_IMAGE", "")

VERIFY = r'''
import hashlib,json,os,stat
digest=hashlib.sha256();count=0;size=0;limit=128*1024*1024
for root,dirs,files in os.walk('/data',followlinks=False):
 dirs[:]=sorted(d for d in dirs if not os.path.islink(os.path.join(root,d)))
 for name in sorted(files):
  path=os.path.join(root,name)
  info=os.lstat(path)
  if not stat.S_ISREG(info.st_mode):continue
  digest.update(os.path.relpath(path,'/data').encode('utf8','surrogateescape'))
  digest.update(str(info.st_size).encode())
  with open(path,'rb') as f:
   data=f.read(min(1024*1024,limit-size));digest.update(data);size+=len(data)
  count+=1
  if size>=limit or count>=1024:break
 if size>=limit or count>=1024:break
print(json.dumps({'phase':'passed','sampled_files':count,'sampled_bytes':size,'sha256':digest.hexdigest()}))
'''


def stamp():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def get(kube, path):
    try:
        return kube.call(path)
    except urllib.error.HTTPError as error:
        if error.code != 404:
            raise
        return None


def acquire(kube):
    value = get(kube, LEASE)
    now = dt.datetime.now(dt.timezone.utc)
    if value is None:
        value = {"apiVersion": "coordination.k8s.io/v1", "kind": "Lease", "metadata": {"name": "flash-backup-verifier", "namespace": SYSTEM_NS}, "spec": {}}
    spec = value.setdefault("spec", {})
    old = dt.datetime.fromisoformat(spec.get("renewTime", "1970-01-01T00:00:00Z").replace("Z", "+00:00"))
    if spec.get("holderIdentity") != HOLDER and (now-old).total_seconds() < spec.get("leaseDurationSeconds", 120):
        return False
    spec.update(holderIdentity=HOLDER, leaseDurationSeconds=120, renewTime=now.isoformat())
    try:
        kube.call(LEASE if value["metadata"].get("resourceVersion") else LEASE.rsplit("/", 1)[0], "PUT" if value["metadata"].get("resourceVersion") else "POST", value)
        return True
    except urllib.error.HTTPError as error:
        if error.code != 409:
            raise
        return False


def result(kube, backup, value):
    current = kube.call(BASE + "/" + backup["metadata"]["name"])
    if current["metadata"]["uid"] != backup["metadata"]["uid"]:
        raise RuntimeError("backup_identity_changed")
    current["metadata"].setdefault("annotations", {})[RESULT] = json.dumps(value, separators=(",", ":"))
    kube.call(BASE + "/" + current["metadata"]["name"], "PUT", current)


def names(backup):
    name = "verify-" + backup["spec"]["id"].replace("-", "")
    return name, f"/api/v1/namespaces/{NS}/persistentvolumeclaims/{name}", f"/apis/batch/v1/namespaces/{NS}/jobs/{name}"


def delete_owned(kube, path, backup):
    value = get(kube, path)
    if value is None:
        return True
    if value["metadata"].get("labels", {}).get(LABEL) != backup["spec"]["id"] or not any(o["uid"] == backup["metadata"]["uid"] for o in value["metadata"].get("ownerReferences", [])):
        raise RuntimeError("verification_cleanup_owner_mismatch")
    if not value["metadata"].get("deletionTimestamp"):
        kube.call(path, "DELETE", {"apiVersion": "v1", "kind": "DeleteOptions", "propagationPolicy": "Foreground", "preconditions": {"uid": value["metadata"]["uid"]}})
    return False


def restore_complete(kube, claim):
    if claim.get("status", {}).get("phase") != "Bound":
        return False
    pv = kube.call("/api/v1/persistentvolumes/" + claim["spec"]["volumeName"])
    csi = pv["spec"]["csi"]
    if csi["driver"] != "driver.longhorn.io":
        raise RuntimeError("verification_driver_mismatch")
    volume = kube.call(f"/apis/longhorn.io/v1beta2/namespaces/{STORAGE_NS}/volumes/" + csi["volumeHandle"])
    status = volume.get("status", {})
    return status.get("restoreInitiated") is True and status.get("restoreRequired") is False


def step(kube, backup):
    annotations = backup["metadata"].get("annotations", {})
    if not annotations.get(REQUEST):
        return
    existing = json.loads(annotations.get(RESULT, "{}"))
    name, pvc_path, job_path = names(backup)
    # Cleanup is part of success, not a best-effort task after reporting it.
    if backup["metadata"].get("deletionTimestamp") or existing.get("phase") in ("passed", "failed", "cleaning"):
        if delete_owned(kube, job_path, backup) and delete_owned(kube, pvc_path, backup):
            if existing.get("volume_name") and get(kube, "/api/v1/persistentvolumes/" + existing["volume_name"]):
                return
            if existing.get("volume_handle") and get(kube, f"/apis/longhorn.io/v1beta2/namespaces/{STORAGE_NS}/volumes/" + existing["volume_handle"]):
                return
            if existing.get("phase") == "cleaning":
                existing["phase"] = existing.pop("outcome", "failed")
                existing["cleanup_verified"] = True
                existing["completed_at"] = stamp()
                result(kube, backup, existing)
        return
    if backup.get("status", {}).get("phase") != "ready":
        return
    started = dt.datetime.fromisoformat(existing.get("started_at", stamp()).replace("Z", "+00:00"))
    if (dt.datetime.now(dt.timezone.utc)-started).total_seconds() > 1800:
        result(kube, backup, dict(existing, phase="cleaning", outcome="failed", message="restore_verification_timeout"))
        return
    if not existing:
        result(kube, backup, {"phase": "restoring", "started_at": stamp()})
    owner = {"apiVersion": "flash.heterocloud.io/v1alpha1", "kind": "FlashBackup", "name": backup["metadata"]["name"], "uid": backup["metadata"]["uid"]}
    labels = {LABEL: backup["spec"]["id"], "flash.heterocloud.io/instance": backup["spec"]["service_instance_id"]}
    metadata = {"name": name, "namespace": NS, "labels": labels, "ownerReferences": [owner]}
    claim = get(kube, pvc_path)
    if claim is None:
        kube.call(pvc_path.rsplit("/", 1)[0], "POST", {"apiVersion": "v1", "kind": "PersistentVolumeClaim", "metadata": metadata,
            "spec": {"storageClassName": STORAGE_CLASS, "accessModes": ["ReadWriteOnce"], "volumeMode": "Filesystem",
                     "resources": {"requests": {"storage": str(backup["spec"]["volume_size_bytes"])}},
                     "dataSource": {"apiGroup": "snapshot.storage.k8s.io", "kind": "VolumeSnapshot", "name": backup["metadata"]["name"]}}})
        return
    if claim["metadata"].get("labels", {}).get(LABEL) != backup["spec"]["id"]:
        raise RuntimeError("verification_claim_owner_mismatch")
    if not restore_complete(kube, claim):
        return
    if not existing.get("volume_name"):
        existing.update(phase="restoring", volume_name=claim["spec"]["volumeName"])
        pv = kube.call("/api/v1/persistentvolumes/" + existing["volume_name"])
        existing["volume_handle"] = pv["spec"]["csi"]["volumeHandle"]
        existing.setdefault("started_at", stamp())
        result(kube, backup, existing)
    job = get(kube, job_path)
    if job is not None and any("DAC_READ_SEARCH" in c.get("securityContext", {}).get("capabilities", {}).get("add", [])
                               for c in job.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])):
        # Replace only our own earlier verification Job rejected by baseline
        # Pod security. The restored clone and original disk remain untouched.
        delete_owned(kube, job_path, backup)
        return
    if job is None:
        if not IMAGE or "@sha256:" not in IMAGE:
            raise RuntimeError("verification_image_must_be_pinned")
        kube.call(job_path.rsplit("/", 1)[0], "POST", {"apiVersion": "batch/v1", "kind": "Job", "metadata": metadata,
            "spec": {"activeDeadlineSeconds": 600, "backoffLimit": 0,
                     "template": {"metadata": {"labels": {LABEL: backup["spec"]["id"]}, "annotations": {"flash.heterocloud.io/image-size-bytes": str(256*1024**2)}},
                                  "spec": {"runtimeClassName": "gvisor", "restartPolicy": "Never", "automountServiceAccountToken": False,
                                           "containers": [{"name": "workload", "image": IMAGE, "imagePullPolicy": "IfNotPresent", "command": ["python3", "-B", "-c", VERIFY],
                                                           "resources": {"requests": {"cpu": "50m", "memory": "64Mi", "ephemeral-storage": "64Mi"}, "limits": {"cpu": "250m", "memory": "128Mi", "ephemeral-storage": "64Mi"}},
                                                           "securityContext": {"runAsUser": 0, "readOnlyRootFilesystem": True, "allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"], "add": ["DAC_OVERRIDE"]}},
                                                           "volumeMounts": [{"name": "restore", "mountPath": "/data", "readOnly": True}]}],
                                           "volumes": [{"name": "restore", "persistentVolumeClaim": {"claimName": name, "readOnly": True}}]}}}})
        return
    if job.get("status", {}).get("succeeded"):
        pods = kube.call(f"/api/v1/namespaces/{NS}/pods?labelSelector=job-name%3D{name}")["items"]
        if len(pods) != 1:
            raise RuntimeError("verification_job_pod_missing")
        # The API client exposes bounded raw logs separately; only our trusted
        # verifier emits them, never the user's image or application.
        value = kube.call(f"/api/v1/namespaces/{NS}/pods/{pods[0]['metadata']['name']}/log?container=workload&limitBytes=2048")
        if value.get("phase") != "passed" or len(value.get("sha256", "")) != 64:
            raise RuntimeError("verification_result_invalid")
        value.update(phase="cleaning", outcome="passed", started_at=existing.get("started_at", stamp()),volume_name=existing["volume_name"],volume_handle=existing["volume_handle"])
        result(kube, backup, value)
    elif job.get("status", {}).get("failed"):
        result(kube, backup, dict(existing,phase="cleaning",outcome="failed",message="restore_verification_failed"))


def run():
    kube = Kube()
    while True:
        try:
            if acquire(kube):
                for backup in kube.call(BASE)["items"]:
                    annotations = backup["metadata"].get("annotations", {})
                    state = json.loads(annotations.get(RESULT, "{}"))
                    if annotations.get(REQUEST) and not state.get("cleanup_verified"):
                        step(kube, backup)
                        break  # At most one restore/verification clone in flight.
        except (OSError, ValueError, KeyError, RuntimeError):
            print(json.dumps({"event": "backup_verification_retry"}), flush=True)
        time.sleep(5)


if __name__ == "__main__":
    run()
