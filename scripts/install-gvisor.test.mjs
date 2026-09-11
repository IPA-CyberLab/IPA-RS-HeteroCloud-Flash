import assert from 'node:assert/strict';
import { test } from 'node:test';
import { spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';

const source = readFileSync(new URL('./install-gvisor.sh', import.meta.url), 'utf8');
function fragment(start, end) {
  const from = source.indexOf(start);
  const to = source.indexOf(end, from);
  assert.ok(from >= 0 && to > from, `missing fragment: ${start}`);
  return source.slice(from, to);
}

// Execute only pin selection and package verification, never the installer.
const selection = fragment('runsc_package="runsc"', '[[ ${EUID}');
const installation = fragment('run_apt "installing runsc from', 'command -v runsc');
function check(version, installed = version, queryStatus = 0) {
  const env = { ...process.env, INSTALLED_VERSION: installed ?? '', QUERY_STATUS: String(queryStatus) };
  delete env.GVISOR_VERSION;
  if (version !== undefined) env.GVISOR_VERSION = version;
  return spawnSync('bash', ['-c', `
set -Eeuo pipefail
fail() { printf '%s\\n' "$*" >&2; exit 1; }
run_apt() { printf 'APT'; printf ' <%s>' "$@"; printf '\\n'; }
dpkg-query() {
  [[ "$1" == '-W' && "$3" == 'runsc' && "$#" == 3 ]] || exit 90
  case "$2" in
    '-f=\${Status}') printf 'install ok installed' ;;
    '-f=\${Version}') printf 'VERSION_QUERY\\n' >&2; printf '%s' "$INSTALLED_VERSION"; return "$QUERY_STATUS" ;;
    *) exit 91 ;;
  esac
}
${selection}
${installation}
printf 'VERIFIED\\n'
`], { env, encoding: 'utf8' });
}

test('pin validation precedes all host mutations', () => {
  assert.ok(source.indexOf(selection) < source.indexOf('mkdir -p /run/lock'));
  assert.ok(source.indexOf(selection) < source.indexOf('run_apt "updating Ubuntu'));
  assert.ok(source.indexOf(installation) < source.indexOf('STEP="preparing the containerd configuration"'));
});

test('unset pin retains unversioned install without querying version', () => {
  const result = check(undefined);
  assert.equal(result.status, 0, result.stderr);
  assert.match(result.stdout, /<install> <-y> <--no-install-recommends> <runsc>\nVERIFIED\n$/);
  assert.equal(result.stderr, '');
});

test('safe Debian versions install exactly and verify the dpkg version', () => {
  for (const version of ['20260907.0', '1:20260907.0-1', '0.0~rc1+dev-2', '1.0-dev-1']) {
    const result = check(version);
    assert.equal(result.status, 0, result.stderr);
    assert.ok(result.stdout.endsWith(`<install> <-y> <--no-install-recommends> <runsc=${version}>\nVERIFIED\n`));
    assert.equal(result.stderr, 'VERSION_QUERY\n');
  }
});

test('empty, malformed and unsafe pins fail before package commands', () => {
  for (const version of ['', 'latest', '-1', '1-', 'x:1', '1/2', '1*', '1=2', '1 2', '1\n', '1\t2', '1;id', '$(id)', '1_2', '1:']) {
    const result = check(version);
    assert.notEqual(result.status, 0, version);
    assert.equal(result.stdout, '', version);
    assert.match(result.stderr, /GVISOR_VERSION must be/);
    assert.doesNotMatch(result.stderr, /VERSION_QUERY/);
  }
});

test('version mismatch and failed dpkg query stop installation', () => {
  for (const [installed, status, message] of [
    ['20260906.0', 0, /does not match GVISOR_VERSION/],
    ['', 0, /does not match GVISOR_VERSION/],
    ['20260907.0', 1, /could not query/],
  ]) {
    const result = check('20260907.0', installed, status);
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, message);
    assert.doesNotMatch(result.stdout, /VERIFIED/);
  }
});
