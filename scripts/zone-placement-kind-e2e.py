#!/usr/bin/env python3
"""Exercise generated placement policies on an explicitly named disposable kind cluster."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time

CONTEXT = "kind-flash-zone-e2e"
NAMESPACE = "flash-zone-e2e"
WORKERS = ("flash-zone-e2e-worker", "flash-zone-e2e-worker2")
CONTROL = "flash-zone-e2e-control-plane"


def kubectl(*args, input_data=None, check=True):
    return subprocess.run(["kubectl", "--context", CONTEXT, *args], input=input_data,
                          text=True, capture_output=True, check=check, timeout=30)


def wait(description, predicate, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(2)
    raise RuntimeError(f"timed out waiting for {description}")


def pods(name):
    response = kubectl("-n", NAMESPACE, "get", "pods", "-l", f"app={name}", "-o", "json")
    return json.loads(response.stdout)["items"]


def ready(pod):
    return any(condition.get("type") == "Ready" and condition.get("status") == "True"
               for condition in pod.get("status", {}).get("conditions", []))


def ready_nodes(name):
    return [pod["spec"].get("nodeName") for pod in pods(name) if ready(pod)]


def deployment(name, fixture):
    source = json.loads(fixture.read_text())
    constraints = source["spec"]["template"]["spec"]["topologySpreadConstraints"]
    assert len(constraints) == 2 and constraints[1]["topologyKey"] == "topology.kubernetes.io/zone"
    for constraint in constraints:
        constraint["labelSelector"] = {"matchLabels": {"app": name}}
    return {
        "apiVersion": "apps/v1", "kind": "Deployment",
        "metadata": {"name": name, "namespace": NAMESPACE},
        "spec": {"replicas": 2, "selector": {"matchLabels": {"app": name}},
                 "template": {"metadata": {"labels": {"app": name}},
                              "spec": {"topologySpreadConstraints": constraints,
                                       "containers": [{"name": "pause", "image": "registry.k8s.io/pause:3.10"}]}}},
    }


def apply(value):
    kubectl("apply", "-f", "-", input_data=json.dumps(value))


def main():
    if os.environ.get("FLASH_ZONE_E2E") != "kind-only" or len(sys.argv) != 2:
        raise RuntimeError("requires FLASH_ZONE_E2E=kind-only and a fixture directory")
    items = json.loads(kubectl("get", "nodes", "-o", "json").stdout)["items"]
    names = {item["metadata"]["name"] for item in items}
    if names != {CONTROL, *WORKERS}:
        raise RuntimeError("refusing to test a cluster without the exact disposable kind node names")
    fixtures = Path(sys.argv[1])
    for node in (CONTROL, *WORKERS):
        kubectl("patch", "node", node, "--type=merge", "-p", json.dumps({
            "metadata": {"labels": {"topology.kubernetes.io/region": None,
                                     "topology.kubernetes.io/zone": None}}}))
    kubectl("create", "namespace", NAMESPACE)
    apply(deployment("unknown-preferred", fixtures / "preferred.json"))
    wait("preferred placement with unknown zones", lambda: len(ready_nodes("unknown-preferred")) == 2)
    kubectl("-n", NAMESPACE, "delete", "deployment", "unknown-preferred", "--wait=true", "--timeout=90s")
    apply(deployment("unknown-required", fixtures / "required.json"))
    wait("strict placement to reject unknown zones", lambda: len(pods("unknown-required")) == 2 and all(
        any(condition.get("type") == "PodScheduled" and condition.get("reason") == "Unschedulable"
            for condition in pod.get("status", {}).get("conditions", []))
        for pod in pods("unknown-required")))
    if ready_nodes("unknown-required"):
        raise RuntimeError("strict placement accepted nodes without known zones")
    kubectl("-n", NAMESPACE, "delete", "deployment", "unknown-required", "--wait=true", "--timeout=90s")
    for node, zone in ((CONTROL, "control-only"), (WORKERS[0], "site-a"), (WORKERS[1], "site-b")):
        kubectl("label", "node", node, "topology.kubernetes.io/region=region-a",
                f"topology.kubernetes.io/zone={zone}", "--overwrite")

    apply(deployment("required", fixtures / "required.json"))
    wait("required replicas in distinct zones", lambda: set(ready_nodes("required")) == set(WORKERS))
    kubectl("taint", "node", WORKERS[1], "flash-zone-e2e=offline:NoExecute")
    wait("strict placement to retain one ready and one pending", lambda: (
        ready_nodes("required") == [WORKERS[0]] and any(
            pod.get("status", {}).get("phase") == "Pending" for pod in pods("required"))))
    time.sleep(10)
    if ready_nodes("required") != [WORKERS[0]]:
        raise RuntimeError("strict placement scheduled extra replicas without two eligible zones")
    kubectl("taint", "node", WORKERS[1], "flash-zone-e2e=offline:NoExecute-")
    wait("strict placement to recover across zones", lambda: set(ready_nodes("required")) == set(WORKERS))
    kubectl("-n", NAMESPACE, "delete", "deployment", "required", "--wait=true", "--timeout=90s")

    apply(deployment("preferred", fixtures / "preferred.json"))
    wait("preferred replicas in distinct zones", lambda: set(ready_nodes("preferred")) == set(WORKERS))
    kubectl("taint", "node", WORKERS[1], "flash-zone-e2e=offline:NoExecute")
    wait("preferred placement to use the surviving zone", lambda: ready_nodes("preferred").count(WORKERS[0]) == 2)
    kubectl("taint", "node", WORKERS[1], "flash-zone-e2e=offline:NoExecute-")
    kubectl("-n", NAMESPACE, "scale", "deployment", "preferred", "--replicas=3")
    wait("new preferred replica to use the recovered zone", lambda: (
        len(ready_nodes("preferred")) == 3 and WORKERS[1] in ready_nodes("preferred")))
    print("zone scheduler E2E passed: strict pending/recovery and preferred availability/recovery")


if __name__ == "__main__":
    main()
