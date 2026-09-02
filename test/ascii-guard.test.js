// @zakkster/lite-query -- ASCII-law drift guard.
//
// The source law is ASCII-only across every shipped and tooling file. This
// suite freezes that: it walks the package.json files[] scope (parsed at
// runtime, so a future files[] change is covered automatically) plus the
// test and bench trees, and asserts no byte falls outside printable ASCII + LF.
//
// The allowlist mechanism exists for the two source-law exceptions (U+00D7 and
// U+00B5). It is EMPTY today -- no shipped file needs it -- and the third test
// proves the escape hatch works without any file relying on it.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, readdirSync, statSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join, relative } from 'node:path';

const HERE = dirname(fileURLToPath(import.meta.url));
const ROOT = join(HERE, '..');

// Repo docs that are NOT in package.json files[] (so the files[] walk misses
// them) but are still source-law ASCII-only. Listed explicitly and statSync'd
// so a missing entry fails the walk closed rather than being skipped.
const ROOT_DOCS = ['INCONCLUSIVE.md'];

// Printable ASCII (0x20-0x7E) plus LF (0x0A). Anything else is a violation
// unless its codepoint is in `allow`. Operates on a Buffer so the failure-path
// controls below can exercise it without mutating a repo file.
function scanAscii(buf, allow = new Set()) {
  const s = buf.toString('utf8');
  const bad = [];
  let index = 0;
  for (const ch of s) {
    const cp = ch.codePointAt(0);
    if (cp === 0x0a || (cp >= 0x20 && cp <= 0x7e) || allow.has(cp)) {
      index++;
      continue;
    }
    bad.push({ index, codePoint: cp });
    index++;
  }
  return bad;
}

// Collect every regular file under an absolute path (recursing directories),
// so a files[] entry that later becomes a directory is still fully covered.
function collectFiles(abs, out) {
  const st = statSync(abs); // throws loudly if the path does not exist
  if (st.isDirectory()) {
    for (const name of readdirSync(abs).sort()) {
      collectFiles(join(abs, name), out);
    }
  } else {
    out.push(abs);
  }
}

function collectByExt(dirAbs, ext, out) {
  for (const name of readdirSync(dirAbs).sort()) {
    const abs = join(dirAbs, name);
    const st = statSync(abs);
    if (st.isDirectory()) {
      collectByExt(abs, ext, out);
    } else if (name.endsWith(ext)) {
      out.push(abs);
    }
  }
}

function buildWalkList() {
  const pkg = JSON.parse(readFileSync(join(ROOT, 'package.json'), 'utf8'));
  const files = Array.isArray(pkg.files) ? pkg.files : [];
  const out = [];
  // files[] scope. A listed entry that does not exist IS a bug: fail closed
  // by letting statSync throw rather than silently skipping it.
  for (const entry of files) {
    collectFiles(join(ROOT, entry), out);
  }
  // test/*.js -- includes the guards themselves, so they self-police.
  collectByExt(join(ROOT, 'test'), '.js', out);
  // test/**/*.mjs -- the torture-law entry and its phase-C control fixtures.
  collectByExt(join(ROOT, 'test'), '.mjs', out);
  // bench/**/*.mjs -- the whole bench tree, benchmark + torture harnesses.
  collectByExt(join(ROOT, 'bench'), '.mjs', out);
  // ROOT_DOCS -- repo docs outside files[]. statSync via collectFiles fails
  // closed if a listed doc is missing.
  for (const doc of ROOT_DOCS) {
    collectFiles(join(ROOT, doc), out);
  }
  return [...new Set(out)].sort();
}

test('ascii-guard: every walked file is printable ASCII + LF only', () => {
  const files = buildWalkList();
  assert.ok(files.length > 0, 'walk list must not be empty');
  for (const abs of files) {
    const bad = scanAscii(readFileSync(abs));
    if (bad.length > 0) {
      const first = bad[0];
      const cp = 'U+' + first.codePoint.toString(16).toUpperCase().padStart(4, '0');
      assert.fail(
        relative(ROOT, abs) + ': ' + bad.length + ' non-ASCII byte(s); first ' +
        cp + ' at codepoint index ' + first.index
      );
    }
  }
});

test('ascii-guard: control -- a fixture with one U+2014 fails the predicate', () => {
  // The em dash is built from its codepoint so this guard file stays ASCII and
  // passes its own walk in the test above.
  const fixture = Buffer.from('valid ASCII then em dash ' + String.fromCodePoint(0x2014) + ' here', 'utf8');
  const bad = scanAscii(fixture);
  assert.equal(bad.length, 1, 'exactly one non-ASCII codepoint expected');
  assert.equal(bad[0].codePoint, 0x2014, 'the flagged codepoint is U+2014');
});

test('ascii-guard: control -- allowlist admits a listed codepoint (escape hatch)', () => {
  const fixture = Buffer.from('multiply sign ' + String.fromCodePoint(0x00d7) + ' present', 'utf8');
  // Without the allowlist the U+00D7 is a violation...
  assert.equal(scanAscii(fixture).length, 1, 'U+00D7 flagged when not allowed');
  // ...and with it in the allowlist the same buffer passes clean.
  assert.equal(
    scanAscii(fixture, new Set([0x00d7])).length,
    0,
    'U+00D7 admitted when allowlisted'
  );
});
