#!/usr/bin/env bash
set -Eeuo pipefail

if (($# == 0)); then
  printf 'usage: %s NODE [NODE ...]\n' "$0" >&2
  exit 2
fi

command -v kubectl >/dev/null 2>&1 || {
  printf 'verify-gvisor: kubectl is required\n' >&2
  exit 127
}

namespace="${FLASH_SMOKE_NAMESPACE:-heterocloud-flash-bootstrap}"
kubectl create namespace "${namespace}" --dry-run=client -o yaml | kubectl apply -f -
kubectl label namespace "${namespace}" pod-security.kubernetes.io/enforce=restricted --overwrite
kubectl apply -f - <<'YAML'
apiVersion: node.k8s.io/v1
kind: RuntimeClass
metadata:
  name: gvisor
handler: runsc
scheduling:
  nodeSelector:
    flash.heterocloud.io/gvisor-ready: "true"
YAML

for node in "$@"; do
  if [[ ! "${node}" =~ ^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$ ]]; then
    printf 'verify-gvisor: invalid Kubernetes node name %q\n' "${node}" >&2
    exit 2
  fi
  pod="gvisor-smoke-${node//./-}"
  kubectl label node "${node}" flash.heterocloud.io/gvisor-ready=true --overwrite
  kubectl -n "${namespace}" delete pod "${pod}" --ignore-not-found=true --wait=true
  if ! kubectl -n "${namespace}" run "${pod}" \
    --image=alpine:3.22 \
    --restart=Never \
    --overrides="$(printf '{\"spec\":{\"runtimeClassName\":\"gvisor\",\"nodeSelector\":{\"kubernetes.io/hostname\":\"%s\"},\"automountServiceAccountToken\":false,\"securityContext\":{\"runAsNonRoot\":true,\"seccompProfile\":{\"type\":\"RuntimeDefault\"}},\"containers\":[{\"name\":\"%s\",\"image\":\"alpine:3.22\",\"command\":[\"sh\",\"-c\",\"dmesg | grep -q \\\"Starting gVisor\\\" && echo GVISOR_SMOKE_OK\"],\"securityContext\":{\"allowPrivilegeEscalation\":false,\"runAsNonRoot\":true,\"runAsUser\":65532,\"capabilities\":{\"drop\":[\"ALL\"]}}}]}}' "${node}" "${pod}")"; then
    kubectl label node "${node}" flash.heterocloud.io/gvisor-ready- || true
    exit 1
  fi
  if ! kubectl -n "${namespace}" wait \
    --for=jsonpath='{.status.phase}'=Succeeded "pod/${pod}" --timeout=3m; then
    kubectl -n "${namespace}" describe pod "${pod}" >&2 || true
    kubectl -n "${namespace}" logs "${pod}" >&2 || true
    kubectl label node "${node}" flash.heterocloud.io/gvisor-ready- || true
    exit 1
  fi
  output="$(kubectl -n "${namespace}" logs "${pod}")"
  if [[ "${output}" != "GVISOR_SMOKE_OK" ]]; then
    printf 'verify-gvisor: %s did not execute under gVisor\n' "${node}" >&2
    kubectl label node "${node}" flash.heterocloud.io/gvisor-ready- || true
    exit 1
  fi
  printf 'verify-gvisor: %s passed\n' "${node}"
  kubectl -n "${namespace}" delete pod "${pod}" --wait=true
done

