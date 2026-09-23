#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
declare -A outputs=(
  [service]="flashservices.yaml"
  [gpu-device]="flashgpudevices.yaml"
  [gpu-job]="flashgpujobs.yaml"
  [usage-record]="flashusagerecords.yaml"
)
temporary_directory="$(mktemp -d)"
trap 'rm -rf -- "${temporary_directory}"' EXIT

for kind in service gpu-device gpu-job usage-record; do
  temporary="${temporary_directory}/${outputs[$kind]}"
  cargo run --quiet --manifest-path "${repo_root}/Cargo.toml" --bin flash-crdgen -- "$kind" >"${temporary}"
  test -s "${temporary}"
done
for kind in service gpu-device gpu-job usage-record; do
  mv -- "${temporary_directory}/${outputs[$kind]}" \
    "${repo_root}/deploy/helm/heterocloud-flash/crds/${outputs[$kind]}"
done
