import base64
import copy
import datetime as dt
import importlib.util
import json
import pathlib
import threading
import time
import unittest
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

spec = importlib.util.spec_from_file_location("capacity", pathlib.Path(__file__).with_name("capacity.py"))
c = importlib.util.module_from_spec(spec)
spec.loader.exec_module(c)
NOW = dt.datetime.now(dt.timezone.utc).timestamp()
STAMP = dt.datetime.fromtimestamp(NOW, dt.timezone.utc).isoformat()
POLICY = {"min_free_bytes": 10*c.GIB, "free_percent": 25, "image_expansion_factor": 6}


def node(name="worker"):
    return {"metadata": {"name": name, "labels": {"flash.heterocloud.io/gvisor-ready": "true"}},
            "status": {"conditions": [{"type": "Ready", "status": "True"}],
                       "allocatable": {"cpu": "16", "memory": "32Gi", "ephemeral-storage": "200Gi", "pods": "110"},
                       "images": [{"names": ["test@sha256:abc"]}]}}


def pod():
    return {"metadata": {"name": "new", "labels": {c.INSTANCE: "00000000-0000-0000-0000-000000000001"}, "annotations": {c.IMAGE_BYTES: str(c.GIB)}},
            "spec": {"runtimeClassName": "gvisor", "containers": [{"name": "workload", "image": "test@sha256:abc", "resources": {"requests": {"cpu": "1", "memory": "1Gi", "ephemeral-storage": "1Gi"}}}]}}


def summary(free=100*c.GIB):
    fs = {"time": STAMP, "capacityBytes": 200*c.GIB, "availableBytes": free, "inodes": 1_000_000, "inodesFree": 900_000}
    return {"node": {"fs": fs, "runtime": {"imageFs": dict(fs)}}, "pods": []}


def choose(p=None, n=None, s=None, others=None, ledger=None):
    return c.select_node(p or pod(), [n or node()], {"worker": s or summary()}, others or [], ledger or {}, NOW, POLICY, c.GIB)


class CapacityTests(unittest.TestCase):
    def test_disk_pressure_incident_denies_before_image_pull(self):
        p = pod(); p["spec"]["containers"][0]["image"] = "uncached"
        self.assertEqual(choose(p=p, s=summary(55*c.GIB))["reason"], "insufficient_disk_capacity")
        self.assertTrue(choose(p=p, s=summary(70*c.GIB))["allowed"])

    def test_imagefs_independently_checked(self):
        s = summary(); s["node"]["runtime"]["imageFs"]["availableBytes"] = c.GIB
        self.assertFalse(choose(s=s)["allowed"])

    def test_metrics_missing_stale_future_and_inode_exhaustion_fail_closed(self):
        for stamp in (None, "2020-01-01T00:00:00Z", "2100-01-01T00:00:00Z"):
            s = summary(); s["node"]["fs"]["time"] = stamp
            self.assertEqual(choose(s=s)["reason"], "capacity_metrics_unavailable")
        s = summary(); s["node"]["fs"]["inodesFree"] = 1
        self.assertEqual(choose(s=s)["reason"], "insufficient_inodes")

    def test_dedicated_masters_taints_and_gpu_affinity_are_not_bypassed(self):
        n = node(); n["metadata"]["labels"]["heteronetwork.io/control-plane-only"] = "true"
        self.assertFalse(choose(n=n)["allowed"])
        n = node(); n["spec"] = {"taints": [{"key": "dedicated", "effect": "NoSchedule"}]}
        self.assertFalse(choose(n=n)["allowed"])
        p = pod(); p["spec"]["affinity"] = {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [{"matchFields": [{"key": "metadata.name", "operator": "In", "values": ["gpu-host"]}]}]}}}
        self.assertFalse(choose(p=p)["allowed"])

    def test_cpu_memory_requests_include_inflight_and_init_sidecars(self):
        p = pod(); p["spec"]["initContainers"] = [{"name": "init", "resources": {"requests": {"memory": "33Gi"}}}]
        self.assertEqual(choose(p=p)["reason"], "insufficient_compute_capacity")
        ledger = {"first": {"id": "first", "kind": "Pod", "node": "worker", "requests": {"memory": str(32*c.GIB)}, "disk_bytes": 0}}
        self.assertFalse(choose(ledger=ledger)["allowed"])

    def test_remaining_existing_writable_budget_is_reserved(self):
        other = pod(); other["metadata"]["uid"] = "existing"; other["spec"]["nodeName"] = "worker"
        other["spec"]["containers"][0]["resources"]["requests"]["ephemeral-storage"] = "60Gi"
        self.assertFalse(choose(others=[other])["allowed"])
        other["status"] = {"phase": "Failed"}
        self.assertTrue(choose(others=[other])["allowed"])

    def test_admission_preserves_existing_affinity_and_dry_run(self):
        class Stub:
            def evaluate(self, kind, obj, identity, dry):
                self.dry = dry
                return {"allowed": True, "node": "worker"}
        stub = Stub(); p = pod()
        p["spec"]["affinity"] = {"podAntiAffinity": {"preferredDuringSchedulingIgnoredDuringExecution": []}}
        review = {"request": {"uid": "one", "kind": {"kind": "Pod"}, "namespace": c.NS, "dryRun": True, "object": p}}
        result = c.admission(stub, review)["response"]
        patch = json.loads(base64.b64decode(result["patch"]))
        self.assertTrue(stub.dry)
        self.assertIn("podAntiAffinity", patch[0]["value"])
        self.assertEqual(patch[0]["value"]["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]["nodeSelectorTerms"][0]["matchFields"][0]["values"], ["worker"])
        self.assertNotIn("affinity", pod()["spec"])

    def test_other_namespaces_and_system_pods_are_unaffected(self):
        class Never:
            def evaluate(self, *_): raise AssertionError("not a Flash admission")
        review = {"request": {"uid": "one", "kind": {"kind": "Pod"}, "namespace": "kube-system", "object": pod()}}
        self.assertTrue(c.admission(Never(), review)["response"]["allowed"])

    def test_reinvocation_is_idempotent(self):
        class Stub:
            def evaluate(self, *args): return {"allowed": True, "node": "worker"}
        review = {"request": {"uid": "one", "kind": {"kind": "Pod"}, "namespace": c.NS, "object": pod()}}
        first = c.admission(Stub(), review)["response"]
        for patch in json.loads(base64.b64decode(first["patch"])):
            parts = patch["path"].strip('/').split('/')
            review["request"]["object"][parts[0]][parts[1]] = patch["value"]
        second = c.admission(Stub(), review)["response"]
        self.assertEqual(first["patch"], second["patch"])


class StorageTests(unittest.TestCase):
    def test_every_replica_needs_independent_physical_headroom(self):
        class StorageKube:
            def call(self, path, *args):
                if "/persistentvolumeclaims/" in path:
                    raise urllib.error.HTTPError(path, 404, "missing", None, None)
                if "/storageclasses/" in path:
                    return {"provisioner": "driver.longhorn.io", "parameters": {"numberOfReplicas": "3"}}
                return {"items": [{"metadata": {"name": name}, "spec": {"allowScheduling": True, "disks": {"data": {"allowScheduling": True}}},
                                  "status": {"diskStatus": {"data": {"storageMaximum": 200*c.GIB, "storageAvailable": free*c.GIB,
                                      "conditions": [{"type": "Ready", "status": "True"}, {"type": "Schedulable", "status": "True"}]}}}}
                                  for name, free in [("one",100),("two",100),("three",60)]]}
        claim = {"metadata": {"name": "new"}, "spec": {"storageClassName": "longhorn", "resources": {"requests": {"storage": "20Gi"}}}}
        guard = c.Guard(StorageKube(), POLICY)
        self.assertFalse(guard.storage(claim,{})["allowed"])
        claim["spec"]["resources"]["requests"]["storage"] = "5Gi"
        self.assertTrue(guard.storage(claim,{})["allowed"])
        self.assertFalse(guard.storage(claim,{"pending":{"delta":10*c.GIB,"storage_nodes":["three"]}})["allowed"])


class HTTPTests(unittest.TestCase):
    def test_kubernetes_timeout_query_uses_the_admission_endpoint(self):
        class Guard:
            def evaluate(self, *_):
                return {"allowed": False,"reason":"insufficient_disk_capacity"}
        server=c.BoundedServer(("127.0.0.1",0),c.handler(Guard()))
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            origin="http://127.0.0.1:"+str(server.server_port)
            review={"request":{"uid":"real-http-query","namespace":c.NS,"kind":{"kind":"Pod"},"object":pod()}}
            request=urllib.request.Request(origin+"/admit?timeout=15s",data=json.dumps(review).encode(),headers={"Content-Type":"application/json"})
            with urllib.request.urlopen(request,timeout=5) as response:
                result=json.load(response)
            self.assertFalse(result["response"]["allowed"])
            self.assertEqual(result["response"]["uid"],review["request"]["uid"])
            self.assertEqual(result["response"]["status"]["reason"],"InsufficientCapacity")
            with urllib.request.urlopen(origin+"/healthz?probe=liveness",timeout=5) as response:
                self.assertTrue(json.load(response)["ready"])
        finally:
            server.shutdown();server.server_close();thread.join(timeout=5)


class FakeKube:
    def __init__(self):
        self.lock = threading.Lock()
        self.value = {"metadata": {"resourceVersion": "1"}, "data": {"reservations": "{}"}}

    def call(self, path, method="GET", value=None):
        if path == "/api/v1/nodes": return {"items": [node()]}
        if path.startswith("/api/v1/pods?"): return {"items": []}
        if "/proxy/stats/summary" in path: return summary(55*c.GIB)
        if "/configmaps/" in path:
            with self.lock:
                if method == "PUT":
                    if value["metadata"]["resourceVersion"] != self.value["metadata"]["resourceVersion"]:
                        raise urllib.error.HTTPError(path, 409, "conflict", None, None)
                    self.value = copy.deepcopy(value)
                    self.value["metadata"]["resourceVersion"] = str(int(self.value["metadata"]["resourceVersion"]) + 1)
                return copy.deepcopy(self.value)
        raise AssertionError(path)


class ReservationTests(unittest.TestCase):
    def test_concurrent_usage_refreshes_share_one_compact_snapshot(self):
        class Counted(FakeKube):
            calls = 0
            def call(self, path, method="GET", value=None):
                if path.startswith("/api/v1/pods?"):
                    self.calls += 1
                    time.sleep(.02)
                    value = pod()
                    value["spec"]["containers"][0]["env"] = [{"name": "TOKEN", "value": "never-retain"}]
                    return {"items": [value]}
                return super().call(path, method, value)
        kube = Counted(); guard = c.Guard(kube, POLICY)
        with ThreadPoolExecutor(max_workers=32) as pool:
            snapshots = list(pool.map(lambda _: guard.snapshot(), range(64)))
        self.assertEqual(kube.calls, 1)
        self.assertTrue(all(s is snapshots[0] for s in snapshots))
        self.assertNotIn("never-retain", json.dumps(snapshots[0]))

    def test_two_replicas_cannot_spend_the_same_headroom(self):
        kube = FakeKube()
        guards = [c.Guard(kube, POLICY), c.Guard(kube, POLICY)]
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda i: guards[i].evaluate("Pod", pod(), str(i)), range(2)))
        self.assertTrue(all(r["allowed"] for r in results))
        results += [guards[0].evaluate("Pod", pod(), str(i)) for i in range(2, 10)]
        self.assertEqual(sum(r["allowed"] for r in results), 5)
        self.assertEqual(len(json.loads(kube.value["data"]["reservations"])), 5)

    def test_dry_run_does_not_mutate_ledger(self):
        kube = FakeKube(); before = copy.deepcopy(kube.value)
        self.assertTrue(c.Guard(kube, POLICY).evaluate("Pod", pod(), "dry", True)["allowed"])
        self.assertEqual(kube.value, before)


if __name__ == "__main__":
    unittest.main()
