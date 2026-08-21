#!/usr/bin/env bash
set -Eeuo pipefail

if (($# == 0)); then
  printf 'usage: %s NODE [NODE ...]\n' "$0" >&2
  exit 2
fi

for command in kubectl sed; do
  command -v "${command}" >/dev/null 2>&1 || {
    printf 'rollout-gvisor: %s is required\n' "${command}" >&2
    exit 127
  }
done

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
namespace="${FLASH_BOOTSTRAP_NAMESPACE:-heterocloud-flash-bootstrap}"
config_map="heterocloud-flash-gvisor-installer"

kubectl create namespace "${namespace}" --dry-run=client -o yaml | kubectl apply -f -
kubectl label namespace "${namespace}" pod-security.kubernetes.io/enforce=privileged --overwrite
kubectl -n "${namespace}" create configmap "${config_map}" \
  --from-file=install-gvisor.sh="${repo_root}/scripts/install-gvisor.sh" \
  --from-file=check-gvisor.sh="${repo_root}/scripts/check-gvisor.sh" \
  --dry-run=client -o yaml | kubectl apply -f -

for node in "$@"; do
  if [[ ! "${node}" =~ ^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$ ]]; then
    printf 'rollout-gvisor: invalid Kubernetes node name %q\n' "${node}" >&2
    exit 2
  fi
  pod="gvisor-bootstrap-${node//./-}"
  kubectl -n "${namespace}" delete pod "${pod}" --ignore-not-found=true --wait=true
  sed "s/__NODE__/${node}/g; s/__POD__/${pod}/g; s/__CONFIG_MAP__/${config_map}/g" <<'YAML' \
    | kubectl -n "${namespace}" apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: __POD__
  labels:
    app.kubernetes.io/name: heterocloud-flash-gvisor-bootstrap
spec:
  nodeName: __NODE__
  hostPID: true
  restartPolicy: Never
  tolerations:
    - operator: Exists
  containers:
    - name: bootstrap
      image: ubuntu:24.04
      command:
        - /bin/bash
        - -ec
        - |
          install -m 0700 /scripts/install-gvisor.sh /host/tmp/install-gvisor.sh
          install -m 0700 /scripts/check-gvisor.sh /host/tmp/check-gvisor.sh
          chroot /host /bin/bash /tmp/install-gvisor.sh
          chroot /host /bin/bash /tmp/check-gvisor.sh
      securityContext:
        privileged: true
      volumeMounts:
        - name: host-root
          mountPath: /host
        - name: installer
          mountPath: /scripts
          readOnly: true
  volumes:
    - name: host-root
      hostPath:
        path: /
        type: Directory
    - name: installer
      configMap:
        name: __CONFIG_MAP__
        defaultMode: 0555
YAML
  if ! kubectl -n "${namespace}" wait \
    --for=jsonpath='{.status.phase}'=Succeeded "pod/${pod}" --timeout=15m; then
    kubectl -n "${namespace}" describe pod "${pod}" >&2 || true
    kubectl -n "${namespace}" logs "${pod}" >&2 || true
    exit 1
  fi
  kubectl -n "${namespace}" logs "${pod}"
  kubectl label node "${node}" flash.heterocloud.io/gvisor-ready=true --overwrite
  kubectl -n "${namespace}" delete pod "${pod}" --wait=true
done

kubectl -n "${namespace}" delete configmap "${config_map}"
