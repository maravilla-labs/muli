import assert from 'node:assert/strict';
import test from 'node:test';

import {
  checksumsAssetName,
  compareVersions,
  normalizeVersion,
  serverAssetName,
} from '../src/server/manager.ts';

test('normalizeVersion strips v prefix', () => {
  assert.equal(normalizeVersion('v1.2.3'), '1.2.3');
  assert.equal(normalizeVersion('1.2.3'), '1.2.3');
});

test('normalizeVersion rejects invalid values', () => {
  assert.throws(() => normalizeVersion('latest'));
  assert.throws(() => normalizeVersion('1.2'));
});

test('compareVersions compares semver core correctly', () => {
  assert.equal(compareVersions('1.2.3', '1.2.3'), 0);
  assert.equal(compareVersions('1.2.4', '1.2.3'), 1);
  assert.equal(compareVersions('1.1.9', '1.2.0'), -1);
});

test('asset naming follows release contract', () => {
  assert.equal(
    serverAssetName('1.2.3', 'linux-x86_64', ''),
    'muli-server-1.2.3-linux-x86_64',
  );
  assert.equal(
    serverAssetName('v1.2.3', 'windows-x86_64', '.exe'),
    'muli-server-1.2.3-windows-x86_64.exe',
  );
  assert.equal(checksumsAssetName('v1.2.3'), 'checksums-1.2.3.txt');
});
