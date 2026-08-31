// @zakkster/lite-query -- public-surface drift guard.
//
// Keeps three things in agreement so a doc can never silently drift from code:
//   1. every runtime export of each entry point is documented in llms.txt AND
//      typed in that entry's .d.ts;
//   2. every @zakkster/lite-* name declared as a peer in llms.txt actually
//      exists in package.json peerDependencies.
// Pure node:test + node:fs, zero new dependencies. The relative dynamic imports
// resolve directly under `node --test` from the repo root.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const HERE = dirname(fileURLToPath(import.meta.url));
const ROOT = join(HERE, '..');

const llms = readFileSync(join(ROOT, 'llms.txt'), 'utf8');
const pkg = JSON.parse(readFileSync(join(ROOT, 'package.json'), 'utf8'));

// Each entry point paired with the .d.ts that must type its exports.
const ENTRIES = [
  { mod: '../Query.js', dts: 'Query.d.ts' },
  { mod: '../StreamQuery.js', dts: 'StreamQuery.d.ts' },
  { mod: '../Awaitable.js', dts: 'Awaitable.d.ts' },
];

// A name "appears in" a body when it is present as a whole identifier token.
function mentions(body, name) {
  return new RegExp('\\b' + name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '\\b').test(body);
}

// The peer block: the `## Peer dependencies` section, restricted to lines that
// declare a peer as `@zakkster/lite-<name> ^<version>`. The trailing caret is
// what separates a real peer declaration (lite-signal/lite-stream/lite-await)
// from a non-peer "Optional companion" mention in the same section.
function peerNamesFromLlms(body) {
  const start = body.indexOf('## Peer dependencies');
  assert.notEqual(start, -1, 'llms.txt must have a "## Peer dependencies" section');
  let end = body.indexOf('\n## ', start + 1);
  if (end === -1) end = body.length;
  const section = body.slice(start, end);
  const names = new Set();
  const re = /(@zakkster\/lite-[a-z-]+)\s+\^/g;
  let m;
  while ((m = re.exec(section)) !== null) names.add(m[1]);
  return names;
}

test('surface-guard: every export is documented in llms.txt and typed in its .d.ts', async () => {
  for (const { mod, dts } of ENTRIES) {
    const ns = await import(mod);
    const dtsBody = readFileSync(join(ROOT, dts), 'utf8');
    const names = Object.keys(ns);
    assert.ok(names.length > 0, mod + ' must export at least one name');
    for (const name of names) {
      assert.ok(mentions(llms, name), 'export ' + name + ' (' + mod + ') missing from llms.txt');
      assert.ok(mentions(dtsBody, name), 'export ' + name + ' (' + mod + ') missing from ' + dts);
    }
  }

  // Reverse: every peer named in llms.txt exists in package.json peerDependencies.
  const peers = pkg.peerDependencies || {};
  for (const name of peerNamesFromLlms(llms)) {
    assert.ok(name in peers, 'llms.txt peer ' + name + ' absent from package.json peerDependencies');
  }
});

test('surface-guard: control -- an llms body missing an export name fails', () => {
  // A fixture llms.txt that documents every real export except one. The guard
  // must flag the omission; if it did not, the drift check would be worthless.
  const realExports = ['mutation', 'query', 'queryClient'];
  const omitted = 'queryClient';
  const fixtureLlms = realExports.filter((n) => n !== omitted).join(' ');
  const missing = realExports.filter((n) => !mentions(fixtureLlms, n));
  assert.deepEqual(missing, [omitted], 'exactly the omitted name must be flagged');
});
