#!/usr/bin/env bash

set -uo pipefail
umask 077

readonly CONFIG_FILE="/etc/containerd/config.toml"
readonly DROPIN_DIR="/etc/containerd/conf.d"
readonly DROPIN_FILE="${DROPIN_DIR}/50-heterocloud-flash-runsc.toml"
readonly DROPIN_GLOB="${DROPIN_DIR}/*.toml"
readonly KEYRING_FILE="/usr/share/keyrings/gvisor-archive-keyring.gpg"
readonly SOURCE_FILE="/etc/apt/sources.list.d/gvisor.list"
readonly GVISOR_REPOSITORY="https://storage.googleapis.com/gvisor/releases"

failures=0
work_dir=""

cleanup() {
  if [[ -n "${work_dir}" && -d "${work_dir}" ]]; then
    rm -rf "${work_dir}"
  fi
}
trap cleanup EXIT

ok() {
  printf '[OK] %s\n' "$*"
}

not_ok() {
  printf '[FAIL] %s\n' "$*" >&2
  failures=$((failures + 1))
}

os_id=""
if [[ -r /etc/os-release ]]; then
  while IFS='=' read -r key value; do
    if [[ "${key}" == "ID" ]]; then
      os_id="${value%\"}"
      os_id="${os_id#\"}"
      break
    fi
  done </etc/os-release
fi
if [[ "${os_id}" == "ubuntu" ]]; then
  ok "operating system is Ubuntu"
else
  not_ok "operating system is not supported Ubuntu"
fi

architecture="unknown"
if command -v dpkg >/dev/null 2>&1; then
  architecture="$(dpkg --print-architecture 2>/dev/null || printf unknown)"
fi
case "${architecture}" in
  amd64 | arm64) ok "architecture is ${architecture}" ;;
  *) not_ok "architecture '${architecture}' is unsupported; expected amd64 or arm64" ;;
esac

if command -v dpkg-query >/dev/null 2>&1 \
  && dpkg-query -W -f='${Status}' runsc 2>/dev/null | grep -qx 'install ok installed'; then
  package_version="$(dpkg-query -W -f='${Version}' runsc 2>/dev/null || printf unknown)"
  ok "runsc package is installed (${package_version})"
else
  not_ok "runsc package is not installed"
fi

if command -v runsc >/dev/null 2>&1; then
  ok "runsc executable is available"
else
  not_ok "runsc executable is unavailable"
fi
if command -v containerd-shim-runsc-v1 >/dev/null 2>&1; then
  ok "containerd-shim-runsc-v1 executable is available"
else
  not_ok "containerd-shim-runsc-v1 executable is unavailable"
fi

expected_source="deb [arch=${architecture} signed-by=${KEYRING_FILE}] ${GVISOR_REPOSITORY} release main"
if [[ -r "${SOURCE_FILE}" ]] && grep -Fxq "${expected_source}" "${SOURCE_FILE}"; then
  ok "official gVisor release repository is configured"
else
  not_ok "official gVisor release repository configuration is missing or incorrect"
fi
if [[ -s "${KEYRING_FILE}" ]] && command -v gpg >/dev/null 2>&1 \
  && gpg --batch --show-keys "${KEYRING_FILE}" >/dev/null 2>&1; then
  ok "gVisor repository keyring is readable"
else
  not_ok "gVisor repository keyring is missing or invalid"
fi

containerd_major=""
if command -v containerd >/dev/null 2>&1; then
  containerd_version="$(containerd --version 2>/dev/null || true)"
  if [[ "${containerd_version}" =~ ([0-9]+)\.([0-9]+)\.([0-9]+) ]]; then
    containerd_major="${BASH_REMATCH[1]}"
    containerd_semver="${BASH_REMATCH[1]}.${BASH_REMATCH[2]}.${BASH_REMATCH[3]}"
    case "${containerd_major}" in
      1 | 2) ok "containerd ${containerd_semver} is supported" ;;
      *) not_ok "containerd major version ${containerd_major} is unsupported" ;;
    esac
  else
    not_ok "containerd version could not be determined"
  fi
else
  not_ok "containerd executable is unavailable"
fi

config_version=""
if [[ -r "${CONFIG_FILE}" ]] && command -v python3 >/dev/null 2>&1; then
  config_version="$(python3 - "${CONFIG_FILE}" <<'PY' 2>/dev/null || true
import re
import sys
from pathlib import Path

text = Path(sys.argv[1]).read_text(encoding="utf-8")
table = re.search(r"(?m)^[ \t]*\[", text)
root = text[:table.start()] if table else text
match = re.search(r"(?m)^[ \t]*version[ \t]*=[ \t]*([0-9]+)[ \t]*(?:#.*)?$", root)
if match:
    print(match.group(1))
PY
)"
  case "${config_version}" in
    2 | 3) ok "containerd config version is ${config_version}" ;;
    *) not_ok "containerd config version is not supported version 2 or 3" ;;
  esac
else
  not_ok "${CONFIG_FILE} is unreadable or python3 is unavailable"
fi

if [[ -r "${CONFIG_FILE}" ]] && command -v python3 >/dev/null 2>&1 \
  && python3 - "${CONFIG_FILE}" "${DROPIN_GLOB}" <<'PY' >/dev/null 2>&1
import re
import sys
from pathlib import Path

text = Path(sys.argv[1]).read_text(encoding="utf-8")
wanted = sys.argv[2]
table = re.search(r"(?m)^[ \t]*\[", text)
root = text[:table.start()] if table else text
match = re.search(r"(?ms)^[ \t]*imports[ \t]*=[ \t]*\[(.*?)\]", root)
if not match:
    raise SystemExit(1)
fragment = match.group(1)
if f'"{wanted}"' not in fragment and f"'{wanted}'" not in fragment:
    raise SystemExit(1)
PY
then
  ok "containerd imports ${DROPIN_GLOB}"
else
  not_ok "containerd does not import ${DROPIN_GLOB}"
fi

runtime_plugin=""
case "${config_version}" in
  2) runtime_plugin="io.containerd.grpc.v1.cri" ;;
  3) runtime_plugin="io.containerd.cri.v1.runtime" ;;
esac
if [[ -n "${runtime_plugin}" ]]; then
  expected_dropin="$(printf 'version = %s\n\n[plugins.\"%s\".containerd.runtimes.runsc]\n  runtime_type = \"io.containerd.runsc.v1\"\n' \
    "${config_version}" "${runtime_plugin}")"
  if [[ -r "${DROPIN_FILE}" ]] && [[ "$(cat "${DROPIN_FILE}" 2>/dev/null)" == "${expected_dropin}" ]]; then
    ok "runsc runtime drop-in matches config version ${config_version}"
  else
    not_ok "runsc runtime drop-in is missing or does not match config version ${config_version}"
  fi
fi

if command -v mktemp >/dev/null 2>&1; then
  work_dir="$(mktemp -d /tmp/heterocloud-flash-gvisor-check.XXXXXX 2>/dev/null || true)"
fi
if [[ -n "${work_dir}" && -r "${CONFIG_FILE}" && -x "$(command -v containerd 2>/dev/null || true)" ]]; then
  if containerd --config "${CONFIG_FILE}" config dump \
    >"${work_dir}/effective-config.toml" 2>"${work_dir}/containerd-config.log"; then
    if grep -Eq '^[[:space:]]*runtime_type[[:space:]]*=.*io\.containerd\.runsc\.v1' \
      "${work_dir}/effective-config.toml"; then
      ok "effective containerd configuration exposes the runsc handler"
    else
      not_ok "effective containerd configuration does not expose the runsc handler"
    fi
  else
    not_ok "containerd rejected its effective configuration"
  fi
else
  not_ok "effective containerd configuration could not be inspected"
fi

if command -v systemctl >/dev/null 2>&1 && systemctl is-active --quiet containerd.service; then
  ok "containerd service is active"
else
  not_ok "containerd service is not active"
fi

if ((failures > 0)); then
  printf 'HeteroCloud Flash gVisor check failed: %d problem(s).\n' "${failures}" >&2
  exit 1
fi

printf 'HeteroCloud Flash gVisor host prerequisites are ready.\n'
