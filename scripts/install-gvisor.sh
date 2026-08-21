#!/usr/bin/env bash

set -Eeuo pipefail
umask 077

readonly PRODUCT_NAME="HeteroCloud Flash"
readonly CONFIG_FILE="/etc/containerd/config.toml"
readonly DROPIN_DIR="/etc/containerd/conf.d"
readonly DROPIN_FILE="${DROPIN_DIR}/50-heterocloud-flash-runsc.toml"
readonly DROPIN_GLOB="${DROPIN_DIR}/*.toml"
readonly KEYRING_FILE="/usr/share/keyrings/gvisor-archive-keyring.gpg"
readonly SOURCE_FILE="/etc/apt/sources.list.d/gvisor.list"
readonly GVISOR_KEY_URL="https://gvisor.dev/archive.key"
readonly GVISOR_REPOSITORY="https://storage.googleapis.com/gvisor/releases"

STEP="initialization"
WORK_DIR=""
CONFIG_CANDIDATE=""
TRANSACTION_ACTIVE=0
RESTART_ATTEMPTED=0
ORIGINAL_CONFIG_PRESENT=0
ORIGINAL_DROPIN_PRESENT=0

log() {
  printf '%s\n' "${PRODUCT_NAME}: $*"
}

fail() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

restore_files() {
  ((TRANSACTION_ACTIVE == 1)) || return 0

  if ((ORIGINAL_CONFIG_PRESENT == 1)); then
    cp --preserve=all "${WORK_DIR}/config.toml.original" "${CONFIG_FILE}"
  else
    rm -f "${CONFIG_FILE}"
  fi

  if ((ORIGINAL_DROPIN_PRESENT == 1)); then
    cp --preserve=all "${WORK_DIR}/runsc.toml.original" "${DROPIN_FILE}"
  else
    rm -f "${DROPIN_FILE}"
  fi

  if ((RESTART_ATTEMPTED == 1)); then
    systemctl restart containerd.service >"${WORK_DIR}/containerd-rollback.log" 2>&1 || true
  fi

  TRANSACTION_ACTIVE=0
}

cleanup() {
  if [[ -n "${CONFIG_CANDIDATE}" && -e "${CONFIG_CANDIDATE}" ]]; then
    rm -f "${CONFIG_CANDIDATE}"
  fi
  if [[ -n "${WORK_DIR}" && -d "${WORK_DIR}" ]]; then
    rm -rf "${WORK_DIR}"
  fi
}

on_error() {
  local status=$?
  trap - ERR
  restore_files || true
  printf 'ERROR: %s failed during %s (exit %d).\n' "${PRODUCT_NAME}" "${STEP}" "${status}" >&2
  exit "${status}"
}

trap on_error ERR
trap cleanup EXIT

[[ ${EUID} -eq 0 ]] || fail "run this installer as root (for example: sudo $0)."
[[ -r /etc/os-release ]] || fail "/etc/os-release is unavailable; Ubuntu is required."

os_id=""
while IFS='=' read -r key value; do
  if [[ "${key}" == "ID" ]]; then
    os_id="${value%\"}"
    os_id="${os_id#\"}"
    break
  fi
done </etc/os-release
[[ "${os_id}" == "ubuntu" ]] || fail "unsupported operating system '${os_id:-unknown}'; Ubuntu is required."

for command in apt-get cat chmod cmp containerd cp dirname dpkg dpkg-query flock grep install mkdir mktemp mv rm systemctl; do
  command -v "${command}" >/dev/null 2>&1 || fail "required command '${command}' was not found."
done

architecture="$(dpkg --print-architecture)"
case "${architecture}" in
  amd64 | arm64) ;;
  *) fail "unsupported architecture '${architecture}'; expected amd64 or arm64." ;;
esac

containerd_version="$(containerd --version 2>/dev/null)" || fail "containerd version detection failed."
if [[ "${containerd_version}" =~ ([0-9]+)\.([0-9]+)\.([0-9]+) ]]; then
  containerd_major="${BASH_REMATCH[1]}"
else
  fail "containerd returned an unrecognized version string."
fi
case "${containerd_major}" in
  1) default_config_version=2 ;;
  2) default_config_version=3 ;;
  *) fail "unsupported containerd major version '${containerd_major}'; expected 1.x or 2.x." ;;
esac

mkdir -p /run/lock
exec 9>/run/lock/heterocloud-flash-gvisor.lock
flock -x 9

WORK_DIR="$(mktemp -d /tmp/heterocloud-flash-gvisor.XXXXXX)"

run_apt() {
  local description=$1
  shift
  STEP="${description}"
  if ! DEBIAN_FRONTEND=noninteractive apt-get "$@" >"${WORK_DIR}/apt.log" 2>&1; then
    fail "${description} failed; inspect the host's APT configuration and network access."
  fi
  : >"${WORK_DIR}/apt.log"
}

run_apt "updating Ubuntu package indexes" update
run_apt "installing gVisor repository prerequisites" install -y --no-install-recommends \
  apt-transport-https ca-certificates curl gnupg python3-minimal

for command in curl gpg python3; do
  command -v "${command}" >/dev/null 2>&1 || fail "required command '${command}' was not installed."
done

STEP="configuring the official gVisor APT repository"
if ! curl --fail --silent --show-error --location "${GVISOR_KEY_URL}" \
  --output "${WORK_DIR}/archive.key" 2>"${WORK_DIR}/curl.log"; then
  fail "downloading the official gVisor archive key failed."
fi
if ! gpg --batch --yes --dearmor --output "${WORK_DIR}/archive-keyring.gpg" \
  "${WORK_DIR}/archive.key" >"${WORK_DIR}/gpg.log" 2>&1; then
  fail "decoding the official gVisor archive key failed."
fi
gpg --batch --show-keys "${WORK_DIR}/archive-keyring.gpg" >/dev/null 2>&1 \
  || fail "the downloaded gVisor archive key is invalid."

[[ ! -L "${KEYRING_FILE}" ]] || fail "${KEYRING_FILE} is a symbolic link; refusing to replace it."
[[ ! -L "${SOURCE_FILE}" ]] || fail "${SOURCE_FILE} is a symbolic link; refusing to replace it."
install -d -m 0755 "$(dirname "${KEYRING_FILE}")" "$(dirname "${SOURCE_FILE}")"
if [[ ! -f "${KEYRING_FILE}" ]] || ! cmp -s "${WORK_DIR}/archive-keyring.gpg" "${KEYRING_FILE}"; then
  install -m 0644 "${WORK_DIR}/archive-keyring.gpg" "${KEYRING_FILE}"
fi

repository_line="deb [arch=${architecture} signed-by=${KEYRING_FILE}] ${GVISOR_REPOSITORY} release main"
printf '%s\n' "${repository_line}" >"${WORK_DIR}/gvisor.list"
if [[ ! -f "${SOURCE_FILE}" ]] || ! cmp -s "${WORK_DIR}/gvisor.list" "${SOURCE_FILE}"; then
  install -m 0644 "${WORK_DIR}/gvisor.list" "${SOURCE_FILE}"
fi

run_apt "refreshing package indexes with the gVisor repository" update
run_apt "installing runsc from the official gVisor repository" install -y --no-install-recommends runsc

dpkg-query -W -f='${Status}' runsc 2>/dev/null | grep -qx 'install ok installed' \
  || fail "the runsc package is not installed."
command -v runsc >/dev/null 2>&1 || fail "the runsc executable is unavailable after package installation."
command -v containerd-shim-runsc-v1 >/dev/null 2>&1 \
  || fail "the containerd-shim-runsc-v1 executable is unavailable after package installation."

STEP="preparing the containerd configuration"
install -d -m 0755 "$(dirname "${CONFIG_FILE}")" "${DROPIN_DIR}"

if [[ -L "${CONFIG_FILE}" ]]; then
  fail "${CONFIG_FILE} is a symbolic link; refusing to replace a configuration managed elsewhere."
fi
if [[ -e "${CONFIG_FILE}" && ! -f "${CONFIG_FILE}" ]]; then
  fail "${CONFIG_FILE} is not a regular file."
fi
if [[ -L "${DROPIN_FILE}" ]]; then
  fail "${DROPIN_FILE} is a symbolic link; refusing to replace it."
fi
if [[ -e "${DROPIN_FILE}" && ! -f "${DROPIN_FILE}" ]]; then
  fail "${DROPIN_FILE} is not a regular file."
fi

CONFIG_CANDIDATE="$(mktemp "$(dirname "${CONFIG_FILE}")/.config.toml.flash.XXXXXX")"
if [[ -f "${CONFIG_FILE}" ]]; then
  cp --preserve=all "${CONFIG_FILE}" "${CONFIG_CANDIDATE}"
else
  printf 'version = %d\n' "${default_config_version}" >"${CONFIG_CANDIDATE}"
  chmod 0644 "${CONFIG_CANDIDATE}"
fi

config_version="$({ python3 - "${CONFIG_CANDIDATE}" "${DROPIN_GLOB}" <<'PY'
import json
import re
import sys
from pathlib import Path

path = Path(sys.argv[1])
wanted = sys.argv[2]

try:
    text = path.read_text(encoding="utf-8")
except (OSError, UnicodeError):
    raise SystemExit("ERROR: containerd config is not a readable UTF-8 TOML file.")

table = re.search(r"(?m)^[ \t]*\[", text)
root_end = table.start() if table else len(text)
root = text[:root_end]

version_matches = list(re.finditer(
    r"(?m)^[ \t]*version[ \t]*=[ \t]*([0-9]+)[ \t]*(?:#.*)?$", root
))
if len(version_matches) != 1:
    raise SystemExit("ERROR: containerd config must contain exactly one top-level numeric version.")
version = int(version_matches[0].group(1))
if version not in (2, 3):
    raise SystemExit(f"ERROR: unsupported containerd config version {version}; expected 2 or 3.")

if re.search(r"(?m)^[ \t]*[\"']imports[\"'][ \t]*=", root):
    raise SystemExit("ERROR: quoted top-level imports keys are not supported for safe in-place editing.")

imports_matches = list(re.finditer(r"(?m)^[ \t]*imports[ \t]*=", root))
if len(imports_matches) > 1:
    raise SystemExit("ERROR: containerd config contains multiple top-level imports assignments.")

if not imports_matches:
    insertion = version_matches[0].end()
    text = text[:insertion] + f'\nimports = [{json.dumps(wanted)}]' + text[insertion:]
else:
    assignment = imports_matches[0]
    opening = text.find("[", assignment.end(), root_end)
    if opening < 0:
        raise SystemExit("ERROR: containerd imports must be a TOML array.")

    values = []
    index = opening + 1
    closing = -1
    while index < len(text):
        char = text[index]
        if char == "]":
            closing = index
            break
        if char == "#":
            newline = text.find("\n", index)
            index = len(text) if newline < 0 else newline + 1
            continue
        if char in ("'", '"'):
            quote = char
            if text.startswith(quote * 3, index):
                raise SystemExit("ERROR: multiline strings in containerd imports are not supported.")
            index += 1
            value = []
            while index < len(text):
                char = text[index]
                if char == quote:
                    break
                if quote == '"' and char == "\\":
                    if index + 1 >= len(text):
                        raise SystemExit("ERROR: malformed escape in containerd imports.")
                    value.extend((char, text[index + 1]))
                    index += 2
                    continue
                value.append(char)
                index += 1
            if index >= len(text):
                raise SystemExit("ERROR: unterminated string in containerd imports.")
            raw = "".join(value)
            if quote == '"':
                try:
                    raw = json.loads('"' + raw + '"')
                except json.JSONDecodeError:
                    pass
            values.append(raw)
            index += 1
            continue
        index += 1

    if closing < 0:
        raise SystemExit("ERROR: unterminated containerd imports array.")
    if wanted not in values:
        separator = ", " if values else " "
        text = text[:opening + 1] + json.dumps(wanted) + separator + text[opening + 1:]

try:
    path.write_text(text, encoding="utf-8")
except OSError:
    raise SystemExit("ERROR: failed to write the containerd config candidate.")

print(version)
PY
} 2>&1)" || fail "the existing containerd configuration cannot be updated safely."

case "${config_version}" in
  2) runtime_plugin="io.containerd.grpc.v1.cri" ;;
  3) runtime_plugin="io.containerd.cri.v1.runtime" ;;
  *) fail "unsupported containerd config version '${config_version}'." ;;
esac
if [[ "${containerd_major}" == "1" && "${config_version}" != "2" ]]; then
  fail "containerd 1.x requires config version 2 for the runsc handler."
fi

cat >"${WORK_DIR}/runsc.toml" <<EOF
version = ${config_version}

[plugins."${runtime_plugin}".containerd.runtimes.runsc]
  runtime_type = "io.containerd.runsc.v1"
EOF
chmod 0644 "${WORK_DIR}/runsc.toml"

root_config_changed=0
dropin_changed=0
if [[ ! -f "${CONFIG_FILE}" ]] || ! cmp -s "${CONFIG_CANDIDATE}" "${CONFIG_FILE}"; then
  root_config_changed=1
fi
if [[ ! -f "${DROPIN_FILE}" ]] || ! cmp -s "${WORK_DIR}/runsc.toml" "${DROPIN_FILE}"; then
  dropin_changed=1
fi
config_changed=$((root_config_changed || dropin_changed))

if [[ -f "${CONFIG_FILE}" ]]; then
  cp --preserve=all "${CONFIG_FILE}" "${WORK_DIR}/config.toml.original"
  ORIGINAL_CONFIG_PRESENT=1
fi
if [[ -f "${DROPIN_FILE}" ]]; then
  cp --preserve=all "${DROPIN_FILE}" "${WORK_DIR}/runsc.toml.original"
  ORIGINAL_DROPIN_PRESENT=1
fi
TRANSACTION_ACTIVE=1

# The final drop-in must exist while containerd resolves the candidate's absolute import.
if ((dropin_changed == 1)); then
  install -m 0644 "${WORK_DIR}/runsc.toml" "${DROPIN_FILE}"
fi

STEP="validating the effective containerd configuration"
if ! containerd --config "${CONFIG_CANDIDATE}" config dump \
  >"${WORK_DIR}/effective-config.toml" 2>"${WORK_DIR}/containerd-config.log"; then
  restore_files
  fail "containerd rejected the proposed configuration; no configuration change was retained."
fi
if ! grep -Eq '^[[:space:]]*runtime_type[[:space:]]*=.*io\.containerd\.runsc\.v1' \
  "${WORK_DIR}/effective-config.toml"; then
  restore_files
  fail "the proposed containerd configuration does not expose the runsc runtime handler."
fi

if ((config_changed == 0)); then
  TRANSACTION_ACTIVE=0
  rm -f "${CONFIG_CANDIDATE}"
  CONFIG_CANDIDATE=""
  log "runsc and the containerd runtime handler are already configured; containerd was not restarted."
  exit 0
fi

STEP="installing the containerd configuration"
mv -f "${CONFIG_CANDIDATE}" "${CONFIG_FILE}"
CONFIG_CANDIDATE=""

STEP="restarting containerd after a configuration change"
RESTART_ATTEMPTED=1
if ! systemctl restart containerd.service >"${WORK_DIR}/containerd-restart.log" 2>&1; then
  restore_files
  fail "containerd restart failed; the previous configuration was restored."
fi
if ! systemctl is-active --quiet containerd.service; then
  restore_files
  fail "containerd is not active after restart; the previous configuration was restored."
fi

TRANSACTION_ACTIVE=0
log "installed the runsc handler and restarted containerd because its configuration changed."
