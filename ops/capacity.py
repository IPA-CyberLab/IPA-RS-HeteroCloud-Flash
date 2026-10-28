#!/usr/bin/env python3
"""Flash Pod/PVC admission and deployment preflight. Standard library only.

Only new objects are gated. Existing workloads are never evicted by this service.
A resourceVersion-fenced ledger serializes reservations across webhook replicas.
The Kubernetes scheduler still enforces affinity, taints, and resource requests.
"""
import base64
import copy
import datetime as dt
import decimal
import hashlib
import http.server
import json
import os
import pathlib
import re
import ssl
import threading
import time
import urllib.error
import urllib.request
import urllib.parse
import uuid

GIB = 1024**3
INSTANCE = "flash.heterocloud.io/instance"
RESERVATION = "flash.heterocloud.io/capacity-reservation"
IMAGE_BYTES = "flash.heterocloud.io/image-size-bytes"
NS = os.environ.get("FLASH_WORKLOAD_NAMESPACE", "heterocloud-flash-workloads")
SYSTEM_NS = os.environ.get("POD_NAMESPACE", "heterocloud-flash")
STORAGE_NS = os.environ.get("FLASH_BACKUP_STORAGE_NAMESPACE", "longhorn-system")
LEDGER = "flash-capacity-reservations"


class Unavailable(Exception):
    pass


def quantity(value):
    value = str(value)
    units = {"Ki": 1024, "Mi": 1024**2, "Gi": GIB, "Ti": 1024**4,
             "m": decimal.Decimal(".001"), "k": 1000, "M": 1000**2, "G": 1000**3}
    for unit, multiplier in units.items():
        if value.endswith(unit):
            return decimal.Decimal(value[:-len(unit)]) * multiplier
    return decimal.Decimal(value)


def fresh(value, now, age=90):
    try:
        stamp = dt.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        return -10 <= now - stamp <= age
    except (TypeError, ValueError, AttributeError):
        return False


def alive(pod):
    return pod.get("status", {}).get("phase") not in ("Failed", "Succeeded")


def image_identity(value):
    if "@" not in value:return value
    repository,digest=value.split("@",1)
    if ":" in repository.rsplit("/",1)[-1]:repository=repository.rsplit(":",1)[0]
    return repository+"@"+digest


def requests(pod):
    """Kubernetes regular/init maximum plus runtime overhead, in base units."""
    total, init, sidecars = {}, {}, {}
    for c in pod.get("spec", {}).get("containers", []):
        for k, v in c.get("resources", {}).get("requests", {}).items():
            total[k] = total.get(k, 0) + quantity(v)
    for c in pod.get("spec", {}).get("initContainers", []):
        req = {k: quantity(v) for k, v in c.get("resources", {}).get("requests", {}).items()}
        if c.get("restartPolicy") == "Always":
            for k, v in req.items():
                sidecars[k] = sidecars.get(k, 0) + v
        for k in req.keys() | sidecars.keys():
            init[k] = max(init.get(k, 0), req.get(k, 0) + sidecars.get(k, 0)
                          - (req.get(k, 0) if c.get("restartPolicy") == "Always" else 0))
    for k in total.keys() | sidecars.keys() | init.keys():
        total[k] = max(total.get(k, 0) + sidecars.get(k, 0), init.get(k, 0))
    for k, v in pod.get("spec", {}).get("overhead", {}).items():
        total[k] = total.get(k, 0) + quantity(v)
    return total


def matches_node(pod, node):
    spec, meta = pod.get("spec", {}), node["metadata"]
    labels = meta.get("labels", {})
    if labels.get("heteronetwork.io/control-plane-only") == "true":
        return False
    if node.get("spec", {}).get("unschedulable"):
        return False
    conditions = {c["type"]: c["status"] for c in node.get("status", {}).get("conditions", [])}
    if conditions.get("Ready") != "True" or any(conditions.get(k) == "True" for k in
                                               ("DiskPressure", "MemoryPressure", "PIDPressure")):
        return False
    if spec.get("nodeName") and spec["nodeName"] != meta["name"]:
        return False
    if any(labels.get(k) != v for k, v in spec.get("nodeSelector", {}).items()):
        return False
    # RuntimeClass scheduling selectors are merged by Kubernetes admission.
    if spec.get("runtimeClassName") == "gvisor" and labels.get("flash.heterocloud.io/gvisor-ready") != "true":
        return False
    for taint in node.get("spec", {}).get("taints", []):
        if taint.get("effect") not in ("NoSchedule", "NoExecute"):
            continue
        if not any((not t.get("effect") or t["effect"] == taint["effect"])
                   and (not t.get("key") or t["key"] == taint["key"])
                   and (t.get("operator", "Equal") == "Exists" or t.get("value", "") == taint.get("value", ""))
                   for t in spec.get("tolerations", [])):
            return False
    terms = spec.get("affinity", {}).get("nodeAffinity", {}).get(
        "requiredDuringSchedulingIgnoredDuringExecution", {}).get("nodeSelectorTerms")
    if terms is not None:
        def term_matches(term):
            if not term.get("matchExpressions") and not term.get("matchFields"):
                return False
            for expr in term.get("matchExpressions", []) + term.get("matchFields", []):
                k, op, values = expr["key"], expr["operator"], expr.get("values", [])
                value = meta["name"] if k == "metadata.name" else labels.get(k)
                if op == "In" and value not in values or op == "NotIn" and value in values:
                    return False
                if op == "Exists" and value is None or op == "DoesNotExist" and value is not None:
                    return False
                if op in ("Gt", "Lt"):
                    try:
                        if not (int(value) > int(values[0]) if op == "Gt" else int(value) < int(values[0])):
                            return False
                    except (TypeError, ValueError, IndexError):
                        return False
                if op not in ("In", "NotIn", "Exists", "DoesNotExist", "Gt", "Lt"):
                    return False
            return True
        if not any(term_matches(t) for t in terms):
            return False
    return True


def filesystem_budget(fs, need, now, policy):
    if not fs or not fresh(fs.get("time"), now):
        return "capacity_metrics_unavailable"
    capacity, available = fs.get("capacityBytes", 0), fs.get("availableBytes", 0)
    if not capacity or available - need < max(policy["min_free_bytes"], capacity * policy["free_percent"] // 100):
        return "insufficient_disk_capacity"
    if not fs.get("inodes") or fs.get("inodesFree", 0) < max(100_000, fs["inodes"] // 10):
        return "insufficient_inodes"
    return None


def select_node(pod, nodes, summaries, pods, reservations, now, policy, image_bytes):
    need = requests(pod)
    reasons, candidates = set(), []
    used_by_uid = {p.get("podRef", {}).get("uid"): p for s in summaries.values() for p in s.get("pods", [])}
    image = next((c["image"] for c in pod["spec"]["containers"] if c["name"] == "workload"), "")
    for node in nodes:
        if not matches_node(pod, node):
            continue
        name = node["metadata"]["name"]
        summary = summaries.get(name, {})
        bound = [p for p in pods if alive(p) and p.get("spec", {}).get("nodeName") == name]
        used = {}
        for other in bound:
            for k, v in requests(other).items():
                used[k] = used.get(k, 0) + v
        pending = [r for r in reservations.values() if r.get("kind") == "Pod" and r.get("node") == name
                   and not any(p.get("metadata", {}).get("annotations", {}).get(RESERVATION) == r["id"] for p in bound)]
        for r in pending:
            for k, v in r["requests"].items():
                used[k] = used.get(k, 0) + decimal.Decimal(v)
        alloc = node.get("status", {}).get("allocatable", {})
        if any(used.get(k, 0) + v > quantity(alloc.get(k, 0)) for k, v in need.items()):
            reasons.add("insufficient_compute_capacity")
            continue
        if len(bound) + len(pending) + 1 > int(alloc.get("pods", 0)):
            reasons.add("insufficient_pod_capacity")
            continue
        cached = any(image_identity(image) == image_identity(value) for entry in node.get("status", {}).get("images", []) for value in entry.get("names",[]))
        # Preserved legacy ReplicaSets may predate image-size annotations. If
        # inspection is unavailable, an immutable image can still restart from
        # this node's cache. Admission forces Never so a cache eviction cannot
        # turn this decision into an unbudgeted pull.
        if image_bytes is None and (not re.fullmatch(r".+@sha256:[a-f0-9]{64}", image) or not cached):
            reasons.add("image_capacity_unknown")
            continue
        # Keep room for writable layers already promised but not yet consumed.
        rootfs_remaining = 0
        for other in bound:
            if not any(k in other.get("metadata", {}).get("labels", {}) for k in (INSTANCE, "flash.heterocloud.io/backup-verification")):
                continue
            limit = requests(other).get("ephemeral-storage", 0)
            measured = used_by_uid.get(other["metadata"].get("uid"), {}).get("ephemeral-storage", {}).get("usedBytes", 0)
            rootfs_remaining += max(0, int(limit) - measured)
        peak = 0 if cached else max(GIB, image_bytes * policy["image_expansion_factor"])
        # Trusted injected helpers also use the host image store. Unknown helper
        # sizes receive a conservative reservation; native scheduling still sees
        # their final resource requests after all admission mutations.
        cached_images={image_identity(image) for entry in node.get("status",{}).get("images",[]) for image in entry.get("names",[])}
        helpers={container.get("image") for container in pod["spec"].get("initContainers",[])+pod["spec"].get("containers",[])
                 if container.get("image") and container.get("name")!="workload"}
        peak+=sum(GIB*policy["image_expansion_factor"] for helper in helpers if image_identity(helper) not in cached_images)
        disk_need = int(need.get("ephemeral-storage", 0)) + rootfs_remaining + peak
        disk_need += sum(r["disk_bytes"] for r in pending)
        # Bound pulls may not be reflected in the last kubelet sample yet.
        disk_need += sum(r.get("peak_bytes", 0) for r in reservations.values()
                         if r.get("kind") == "Pod" and r.get("node") == name and r not in pending)
        fs_reason = filesystem_budget(summary.get("node", {}).get("fs"), disk_need, now, policy)
        fs_reason = fs_reason or filesystem_budget(summary.get("node", {}).get("runtime", {}).get("imageFs"), disk_need, now, policy)
        if fs_reason:
            reasons.add(fs_reason)
            continue
        candidates.append((cached, int(summary["node"]["fs"]["availableBytes"]) - disk_need, name, peak))
    if not candidates:
        return {"allowed": False, "reason": sorted(reasons)[0] if reasons else "no_eligible_node"}
    cached, _, name, peak = max(candidates)
    return {"allowed": True, "node": name, "peak_bytes": peak,
            "cached_image_only": image_bytes is None,
            "disk_bytes": int(need.get("ephemeral-storage", 0)) + peak,
            "requests": {k: str(v) for k, v in need.items()}}


class Kube:
    def __init__(self):
        self.origin = "https://" + os.environ.get("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc") + ":" + os.environ.get("KUBERNETES_SERVICE_PORT", "443")
        self.sa = pathlib.Path("/var/run/secrets/kubernetes.io/serviceaccount")
        self.context = ssl.create_default_context(cafile=str(self.sa / "ca.crt"))

    def call(self, path, method="GET", value=None):
        req = urllib.request.Request(self.origin + path, data=None if value is None else json.dumps(value).encode(), method=method,
                                     headers={"Authorization": "Bearer " + (self.sa / "token").read_text().strip(), "Content-Type": "application/json"})
        with urllib.request.urlopen(req, context=self.context, timeout=4) as response:
            return json.load(response)


class Guard:
    def __init__(self, kube, policy=None):
        self.kube = kube
        self.policy = policy or {"min_free_bytes": 10 * GIB, "free_percent": 25, "image_expansion_factor": 6}
        self.lock = threading.Lock()
        self.snapshot_lock = threading.Lock()
        self.cache = None
        self.cache_at = 0

    def snapshot(self):
        # Controllers request usage concurrently for every service. Only one
        # refresh may retain API responses at a time; readers reuse the result.
        with self.snapshot_lock:
            return self._snapshot()

    def _snapshot(self):
        now = time.time()
        if self.cache and now - self.cache_at < 5:
            return self.cache
        nodes = self.kube.call("/api/v1/nodes")["items"]
        pods = []
        continuation = None
        while True:
            query = {"limit": "100"}
            if continuation:
                query["continue"] = continuation
            page = self.kube.call("/api/v1/pods?" + urllib.parse.urlencode(query))
            for pod in page["items"]:
                # Scheduling never needs env values, mounts, logs or managedFields.
                # Compact each page before requesting the next one.
                meta, spec = pod.get("metadata", {}), pod.get("spec", {})
                compact = {"metadata": {k: meta[k] for k in ("name", "namespace", "uid", "labels", "annotations") if k in meta},
                           "spec": {k: spec[k] for k in ("nodeName", "overhead") if k in spec},
                           "status": {"phase": pod.get("status", {}).get("phase")}}
                for field in ("containers", "initContainers"):
                    compact["spec"][field] = [{k: c[k] for k in ("name", "image", "resources", "restartPolicy") if k in c} for c in spec.get(field, [])]
                compact["metadata"]["annotations"] = {k: v for k, v in meta.get("annotations", {}).items() if k in (RESERVATION, IMAGE_BYTES)}
                pods.append(compact)
            continuation = page.get("metadata", {}).get("continue")
            if not continuation:
                break
        summaries = {}
        for node in nodes:
            name = node["metadata"]["name"]
            if node["metadata"].get("labels", {}).get("heteronetwork.io/control-plane-only") == "true":
                continue
            try:
                raw = self.kube.call(f"/api/v1/nodes/{name}/proxy/stats/summary")
                summaries[name] = {"node": {"fs": raw.get("node", {}).get("fs"), "runtime": {"imageFs": raw.get("node", {}).get("runtime", {}).get("imageFs")}},
                                   "pods": [{k: p[k] for k in ("podRef", "ephemeral-storage", "volume") if k in p} for p in raw.get("pods", [])]}
            except (OSError, ValueError):
                pass  # This node fails closed; other healthy nodes remain usable.
        self.cache, self.cache_at = (nodes, pods, summaries), time.time()
        return self.cache

    def ledger(self, pods):
        path = f"/api/v1/namespaces/{SYSTEM_NS}/configmaps/{LEDGER}"
        value = self.kube.call(path)
        records = json.loads(value.get("data", {}).get("reservations", "{}"))
        now = time.time()
        live = {p.get("metadata", {}).get("annotations", {}).get(RESERVATION): p for p in pods if alive(p)}
        # Keep admitted-but-unseen requests for two minutes, pending image pulls
        # until running, and running pulls until fresh kubelet accounting catches up.
        for key, r in list(records.items()):
            pod = live.get(key)
            if r["kind"] == "Pod":
                if pod and pod.get("status", {}).get("phase") == "Running":
                    if not r.get("running_at"):
                        r["running_at"] = now
                    if now - r["running_at"] > 120:
                        records.pop(key)
                elif not pod and now - r["created_at"] > 120:
                    records.pop(key)
            elif now - r["created_at"] > 300:
                records.pop(key)
        return path, value, records

    def storage(self, pvc, records):
        requested = int(quantity(pvc["spec"]["resources"]["requests"]["storage"]))
        name = pvc["metadata"]["name"]
        old = 0
        try:
            prior = self.kube.call(f"/api/v1/namespaces/{NS}/persistentvolumeclaims/{name}")
            old = int(quantity(prior["spec"]["resources"]["requests"]["storage"]))
        except urllib.error.HTTPError as exc:
            if exc.code != 404:
                raise
        delta = max(0, requested - old)
        if not delta:
            return {"allowed": True, "storage_nodes": [], "delta": 0}
        sc = self.kube.call("/apis/storage.k8s.io/v1/storageclasses/" + pvc["spec"]["storageClassName"])
        if sc.get("provisioner") != "driver.longhorn.io":
            return {"allowed": False, "reason": "unsupported_storage_capacity"}
        parameters = sc.get("parameters", {})
        try:
            replica_setting = parameters.get("numberOfReplicas")
            if not replica_setting:
                replica_setting = self.kube.call(
                    f"/apis/longhorn.io/v1beta2/namespaces/{STORAGE_NS}/settings/default-replica-count")["value"]
                # Recent Longhorn versions keep one default per data engine.
                if str(replica_setting).startswith('{'):
                    replica_setting = json.loads(replica_setting)[parameters.get("dataEngine", "v1")]
            replicas = int(replica_setting)
            if replicas < 1:
                raise ValueError("invalid replica count")
        except (urllib.error.URLError, ValueError, TypeError, KeyError):
            return {"allowed": False, "reason": "storage_replica_policy_unavailable"}
        node_tags = set(filter(None, sc.get("parameters", {}).get("nodeSelector", "").split(',')))
        disk_tags = set(filter(None, sc.get("parameters", {}).get("diskSelector", "").split(',')))
        eligible = []
        for node in self.kube.call(f"/apis/longhorn.io/v1beta2/namespaces/{STORAGE_NS}/nodes")["items"]:
            name = node["metadata"]["name"]
            if not node.get("spec", {}).get("allowScheduling"):
                continue
            if not node_tags.issubset(node.get("spec", {}).get("tags", [])):
                continue
            for key, status in node.get("status", {}).get("diskStatus", {}).items():
                spec = node["spec"].get("disks", {}).get(key, {})
                cond = {c["type"]: c["status"] for c in status.get("conditions", [])}
                if not spec.get("allowScheduling") or cond.get("Ready") != "True" or cond.get("Schedulable") != "True":
                    continue
                if not disk_tags.issubset(spec.get("tags", [])):
                    continue
                capacity, free = status.get("storageMaximum", 0), status.get("storageAvailable", 0)
                held = sum(r.get("delta", 0) for r in records.values() if name in r.get("storage_nodes", []))
                # Full new replica size is charged on every storage node. Existing
                # thin provisioned allocations are exposed by monitoring separately.
                if free - held - delta >= max(self.policy["min_free_bytes"], capacity * self.policy["free_percent"] // 100):
                    eligible.append((free - held, name))
                    break
        selected = [name for _, name in sorted(eligible, reverse=True)[:replicas]]
        allowed = len(selected) == replicas
        if not allowed and len(selected) >= max(2, replicas - 1):
            try:
                degraded = self.kube.call(
                    f"/apis/longhorn.io/v1beta2/namespaces/{STORAGE_NS}/settings/allow-volume-creation-with-degraded-availability")
                # Honor the storage system's recovery policy without changing the
                # desired replica count or ever admitting a single-copy volume.
                allowed = degraded.get("value") == "true"
            except urllib.error.URLError:
                pass  # Missing settings permissions must not relax admission.
        return {"allowed": allowed, "reason": "insufficient_replicated_storage",
                "storage_nodes": selected, "delta": delta,
                "desired_storage_replicas": replicas, "degraded_storage": allowed and len(selected) < replicas}

    def evaluate(self, kind, obj, identity, dry_run=False, replaces=None):
        with self.lock:
            nodes, pods, summaries = self.snapshot()
            for _ in range(8):
                path, ledger, records = self.ledger(pods)
                fingerprint=hashlib.sha256(json.dumps({"spec":obj.get("spec"),"kind":kind},sort_keys=True).encode()).hexdigest()
                if identity in records:
                    if records[identity].get("fingerprint")==fingerprint:
                        return dict(records[identity], allowed=True)
                    records.pop(identity)
                if kind == "PersistentVolumeClaim":
                    result = self.storage(obj, records)
                else:
                    image_bytes = obj.get("metadata", {}).get("annotations", {}).get(IMAGE_BYTES)
                    if image_bytes is None:
                        sid = obj.get("metadata", {}).get("labels", {}).get(INSTANCE)
                        flash = self.kube.call(f"/apis/flash.heterocloud.io/v1alpha1/namespaces/{NS}/flashservices/flash-{sid}")
                        status = flash.get("status", {})
                        image = next((c["image"] for c in obj["spec"]["containers"] if c["name"] == "workload"), "")
                        if image_identity(status.get("resolved_image") or "") == image_identity(image):
                            image_bytes = status.get("image_size_bytes")
                    if image_bytes is not None and not 0 < int(image_bytes) < 1024**5:
                        return {"allowed": False, "reason": "image_capacity_unknown"}
                    candidates = [p for p in pods if not (dry_run and replaces and p.get("metadata", {}).get("labels", {}).get(INSTANCE) == replaces)]
                    result = select_node(obj, nodes, summaries, candidates, records, time.time(), self.policy, None if image_bytes is None else int(image_bytes))
                if not result["allowed"] or dry_run:
                    return result
                result.update(id=identity, kind=kind, created_at=time.time(),fingerprint=fingerprint)
                records[identity] = result
                ledger["data"] = {"reservations": json.dumps(records, separators=(",", ":"))}
                try:
                    self.kube.call(path, "PUT", ledger)
                    return result
                except urllib.error.HTTPError as exc:
                    if exc.code != 409:
                        raise
            raise Unavailable("capacity_reservation_busy")


def admission(guard, review):
    request = review["request"]
    response = {"uid": request["uid"], "allowed": True}
    obj = copy.deepcopy(request["object"])
    kind = request["kind"]["kind"]
    if request.get("namespace") != NS or not any(k in obj.get("metadata", {}).get("labels", {}) for k in (INSTANCE, "flash.heterocloud.io/backup-verification")):
        return {"apiVersion": "admission.k8s.io/v1", "kind": "AdmissionReview", "response": response}
    identity=obj.get("metadata",{}).get("annotations",{}).get(RESERVATION,request["uid"]) if kind=="Pod" else request["uid"]
    result = guard.evaluate(kind, obj, identity, bool(request.get("dryRun")))
    if not result["allowed"]:
        response.update(allowed=False, status={"code": 429, "reason": "InsufficientCapacity", "message": "Flash capacity: " + result["reason"]})
    elif kind == "Pod":
        spec = obj["spec"]
        affinity = spec.setdefault("affinity", {})
        required = affinity.setdefault("nodeAffinity", {}).setdefault("requiredDuringSchedulingIgnoredDuringExecution", {})
        terms = required.setdefault("nodeSelectorTerms", [{"matchExpressions": []}])
        for term in terms:
            expression={"key": "metadata.name", "operator": "In", "values": [result["node"]]}
            if expression not in term.setdefault("matchFields", []):term["matchFields"].append(expression)
        annotations = obj["metadata"].setdefault("annotations", {})
        annotations[RESERVATION] = identity
        patch = [{"op": "add", "path": "/spec/affinity", "value": affinity},
                 {"op": "add", "path": "/metadata/annotations", "value": annotations}]
        if result.get("cached_image_only"):
            index = next(i for i, container in enumerate(spec["containers"]) if container["name"] == "workload")
            patch.append({"op": "add", "path": f"/spec/containers/{index}/imagePullPolicy", "value": "Never"})
        response.update(patchType="JSONPatch", patch=base64.b64encode(json.dumps(patch).encode()).decode())
    return {"apiVersion": "admission.k8s.io/v1", "kind": "AdmissionReview", "response": response}


class BoundedServer(http.server.ThreadingHTTPServer):
    """Bound request memory, including clients waiting on a metrics refresh."""
    request_queue_size = 64

    def __init__(self, *args, **kwargs):
        self.slots = threading.BoundedSemaphore(16)
        super().__init__(*args, **kwargs)

    def handle_error(self, *_):
        print(json.dumps({"event": "capacity_client_request_failed"}), flush=True)

    def process_request(self, request, address):
        request.settimeout(15)
        if not self.slots.acquire(blocking=False):
            try:
                request.sendall(b"HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Length: 0\r\n\r\n")
            finally:
                self.shutdown_request(request)
            return
        try:
            super().process_request(request, address)
        except Exception:
            self.slots.release()
            raise

    def process_request_thread(self, request, address):
        try:
            super().process_request_thread(request, address)
        finally:
            self.slots.release()


def handler(guard):

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass  # Never log admission bodies, image credentials, or environment.

        def send(self, code, value):
            body = json.dumps(value).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            self.send(200 if urllib.parse.urlsplit(self.path).path == "/healthz" else 404, {"ready": True})

        def do_POST(self):
            try:
                size = int(self.headers.get("Content-Length", "0"))
                if not 0 < size <= 2 * 1024**2:
                    return self.send(413, {"error": "request_size"})
                value = json.loads(self.rfile.read(size))
                route = urllib.parse.urlsplit(self.path).path
                if route == "/admit":
                    self.send(200, admission(guard, value))
                elif route == "/preflight":
                    replacing = value["object"].get("metadata", {}).get("labels", {}).get(INSTANCE) if value.get("recreate") else None
                    result = guard.evaluate(value["kind"], value["object"], "preflight", True, replacing)
                    # Physical node names and reservations are platform-private.
                    self.send(200, {"allowed": result["allowed"], "reason": result.get("reason")})
                elif route == "/usage":
                    sid = str(uuid.UUID(value["service_instance_id"]))
                    _, _, summaries = guard.snapshot()
                    volumes = [v for summary in summaries.values() for pod in summary.get("pods", [])
                               for v in pod.get("volume", []) if v.get("pvcRef", {}).get("namespace") == NS
                               and v.get("pvcRef", {}).get("name") == f"flash-{sid}-home"
                               and fresh(v.get("time"), time.time()) and v.get("capacityBytes", 0) > 0]
                    if not volumes:
                        return self.send(503, {"error": "disk_usage_unavailable"})
                    volume = max(volumes, key=lambda v: v["time"])
                    self.send(200, {"capacity_bytes": volume["capacityBytes"], "used_bytes": volume.get("usedBytes", 0),
                                    "available_bytes": volume.get("availableBytes", 0), "measured_at": volume["time"]})
                else:
                    self.send(404, {"error": "not_found"})
            except (OSError, ValueError, KeyError, decimal.InvalidOperation, Unavailable):
                self.send(503, {"error": "capacity_metrics_unavailable"})

    return Handler


def serve():
    guard = Guard(Kube(), {"min_free_bytes": int(os.environ.get("CAPACITY_MIN_FREE_GIB", "10")) * GIB,
                          "free_percent": int(os.environ.get("CAPACITY_FREE_PERCENT", "25")),
                          "image_expansion_factor": int(os.environ.get("CAPACITY_IMAGE_FACTOR", "6"))})
    server = BoundedServer(("0.0.0.0", 9443), handler(guard))
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain("/tls/tls.crt", "/tls/tls.key")
    server.socket = context.wrap_socket(server.socket, server_side=True)
    def reload_certificate():
        previous = None
        while True:
            try:
                current = pathlib.Path("/tls/tls.crt").read_bytes()
                if current != previous:
                    context.load_cert_chain("/tls/tls.crt", "/tls/tls.key")
                    previous = current
            except OSError:
                pass  # Retain the current certificate during an atomic Secret rotation.
            time.sleep(30)
    threading.Thread(target=reload_certificate, daemon=True).start()
    server.serve_forever()


if __name__ == "__main__":
    serve()
