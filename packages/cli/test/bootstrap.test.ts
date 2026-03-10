import assert from 'node:assert/strict';
import test from 'node:test';

import {
  buildLocalFullArgs,
  mergeCargoConfig,
  mergeDockerDaemonJson,
  mergeMavenSettings,
  mergeNpmrc,
  resolveAvailablePortWithProbe,
} from '../src/setup/bootstrap.ts';

test('buildLocalFullArgs composes default local-full flags', () => {
  const args = buildLocalFullArgs({ registryPort: 5000, gitPort: 7000 }, true, false);
  assert.deepEqual(args, [
    '--registry',
    'full',
    '--git',
    '--default-tenant',
    'local',
    '--registry-port',
    '5000',
    '--git-port',
    '7000',
    '--embedded-agent',
  ]);
});

test('resolveAvailablePortWithProbe moves forward when preferred port is unavailable', async () => {
  const busy = new Set([5000, 5001, 5002]);
  const resolved = await resolveAvailablePortWithProbe(5000, async port => !busy.has(port));
  assert.equal(resolved, 5003);
});

test('mergeDockerDaemonJson appends insecure registry idempotently', () => {
  const merged1 = mergeDockerDaemonJson('{}', 'local.localhost:5000');
  const merged2 = mergeDockerDaemonJson(merged1, 'local.localhost:5000');
  const parsed = JSON.parse(merged2);
  assert.deepEqual(parsed['insecure-registries'], ['local.localhost:5000']);
});

test('mergeNpmrc injects registry and auth token lines', () => {
  const next = mergeNpmrc('', 'local.localhost', 5000, 'tok123');
  assert.match(next, /registry=http:\/\/local\.localhost:5000\/-\/npm\//);
  assert.match(next, /_authToken=tok123/);
});

test('mergeCargoConfig creates muli registry section', () => {
  const next = mergeCargoConfig('', 'local.localhost', 5000, 'tok123');
  assert.match(next, /\[registries\.muli\]/);
  assert.match(next, /sparse\+http:\/\/local\.localhost:5000\/index\//);
  assert.match(next, /token = "tok123"/);
});

test('mergeMavenSettings inserts muli server entry', () => {
  const next = mergeMavenSettings('<settings></settings>', 'tok123');
  assert.match(next, /<id>muli<\/id>/);
  assert.match(next, /<password>tok123<\/password>/);
});
