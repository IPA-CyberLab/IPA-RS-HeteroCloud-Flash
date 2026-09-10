import assert from 'node:assert/strict';
import { test } from 'node:test';
import { execFileSync } from 'node:child_process';
import { checkedOutRelease } from './release-artifact-manifest.mjs';
import { indexDigest } from './release-artifact-manifest.mjs';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { validateRelease, createManifest, assertAssetsAbsent, image, writeManifest, component, repository } from './release-artifact-manifest.mjs';
const sha = 'a'.repeat(40);
const digest = 'sha256:' + 'b'.repeat(64);
const release = { tag: 'v1.2.3-dev.2', commit: sha, head: sha, tagCommit: sha };
test('final artifact uses pushed index descriptor, not architecture or config digest', () => {
  const metadata = { 'containerimage.descriptor': { mediaType: 'application/vnd.oci.image.index.v1+json', digest },
    'containerimage.config.digest': 'sha256:' + 'c'.repeat(64) };
  assert.equal(indexDigest(metadata), digest);
  for (const value of [null, {}, { 'containerimage.descriptor': { mediaType: 'application/vnd.oci.image.manifest.v1+json', digest } },
    { 'containerimage.descriptor': { mediaType: 'application/vnd.oci.image.index.v1+json', digest: 'latest' } }])
    assert.throws(() => indexDigest(value));
});
test('multiarch workflow consumes both digest artifacts and captures create metadata', () => {
  const workflow = readFileSync(new URL('../.github/workflows/release.yml', import.meta.url), 'utf8');
  for (const arch of ['amd64', 'arm64']) {
    assert.ok(workflow.includes('digest-input digests/' + arch));
    assert.ok(workflow.includes('name: flash-digest-' + arch + '-'));
  }
  assert.ok(workflow.includes('--metadata-file index-metadata.json'));
  assert.ok(workflow.includes('"$amd64" "$arm64"'));
  assert.ok(workflow.includes('generate-index flash-release-artifact.json index-metadata.json'));
});
test('prerelease preserves version and full commit with immutable images', () => {
  const value = createManifest(release, digest);
  assert.deepEqual(value, { schema_version: 1, component, version: '1.2.3-dev.2', commit: sha, image: repository + '@' + digest,
    ...(component === 'flow' ? { companions: { livekit: { image: repository + '-livekit@' + digest } } } : {}) });
});
test('stable version accepted', () => assert.equal(validateRelease({ ...release, tag: 'v1.2.3' }), '1.2.3'));
test('rejects event, HEAD and exact tag mismatch', () => {
  for (const key of ['commit', 'head', 'tagCommit']) assert.throws(() => validateRelease({ ...release, [key]: 'c'.repeat(40) }));
});
test('rejects short commits and unsafe tags', () => {
  assert.throws(() => validateRelease({ ...release, commit: 'abcdef0' }));
  for (const tag of ['latest', '../v1.2.3', '-v1.2.3', 'v1.2.3\n', 'v1.2.3;' ])
    assert.throws(() => validateRelease({ ...release, tag }));
});
test('rejects missing, malformed or tagged digests', () => {
  for (const value of [undefined, '', 'latest', digest + '\n', 'sha256:abc', repository + '@' + digest])
    assert.throws(() => image(value));
  if (component === 'flow') assert.throws(() => createManifest(release, digest));
});
test('release asset guard fails closed', () => {
  assertAssetsAbsent([], ['flow-release-artifact.json']);
  assert.throws(() => assertAssetsAbsent([{ name: 'flow-release-artifact.json' }], ['flow-release-artifact.json']));
  for (const value of [null, {}, [{}]]) assert.throws(() => assertAssetsAbsent(value, ['x']));
});
test('local artifact cannot overwrite existing output', () => {
  const dir = mkdtempSync(join(tmpdir(), 'release-manifest-test-'));
  try {
    const path = join(dir, 'artifact.json');
    const manifest = createManifest(release, digest);
    writeManifest(path, manifest);
    assert.throws(() => writeManifest(path, manifest), { code: 'EEXIST' });
    assert.deepEqual(JSON.parse(readFileSync(path, 'utf8')), manifest);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
test('real Git checkout validates annotated tags and rejects missing or moved identity', () => {
  const dir = mkdtempSync(join(tmpdir(), 'release-git-test-'));
  const git = (...args) => execFileSync('git', args, { cwd: dir, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim();
  try {
    git('init');
    git('-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '--allow-empty', '-m', 'fixture');
    const commit = git('rev-parse', 'HEAD');
    git('-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', 'tag', '-a', 'v1.2.3', '-m', 'fixture');
    assert.equal(checkedOutRelease({ RELEASE_TAG: 'v1.2.3', GITHUB_SHA: commit }, dir).head, commit);
    assert.throws(() => checkedOutRelease({ RELEASE_TAG: 'v1.2.4', GITHUB_SHA: commit }, dir));
    assert.throws(() => checkedOutRelease({ RELEASE_TAG: 'v1.2.3', GITHUB_SHA: 'f'.repeat(40) }, dir));
    git('-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '--allow-empty', '-m', 'next');
    assert.throws(() => checkedOutRelease({ RELEASE_TAG: 'v1.2.3', GITHUB_SHA: git('rev-parse', 'HEAD') }, dir));
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
test('workflow publishes only explicit version/full commit tags and guards before build', () => {
  const workflow = readFileSync(new URL('../.github/workflows/release.yml', import.meta.url), 'utf8');
  assert.match(workflow, /types: \[published\]/);
  assert.doesNotMatch(workflow, /metadata-action|value=latest|--clobber|format=short/);
  assert.match(workflow, /node --test scripts\/release-artifact-manifest\.test\.mjs/);
  assert.match(workflow, /ref: \$\{\{ github\.sha \}\}/);
  assert.ok(workflow.indexOf('guard ' + component + '-release-artifact.json') < workflow.indexOf('docker/build-push-action'));
  assert.match(workflow, /gh release upload "\$RELEASE_TAG"/);
});
