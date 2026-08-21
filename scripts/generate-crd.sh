#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
output="${repo_root}/deploy/helm/heterocloud-flash/crds/flashservices.yaml"
temporary="$(mktemp "${output}.XXXXXX")"
trap 'rm -f -- "${temporary}"' EXIT

cargo run --quiet --manifest-path "${repo_root}/Cargo.toml" --bin flash-crdgen >"${temporary}"
test -s "${temporary}"
mv -- "${temporary}" "${output}"

