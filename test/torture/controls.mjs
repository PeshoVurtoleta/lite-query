// test/torture/controls.mjs -- phase C break-mode controls (cold path).
//
// Reached ONLY by `await import()` from test/torture.mjs when
// QUERY_TORTURE_BREAK is set, so the plain torture run pays zero bytes for any
// of this. Each control targets one gate clause and reuses the module tracker,
// RULES and the SAME evaluateGate passed in via ctx -- a control that trips a
// lookalike gate proves nothing. A gate that cannot fail is decorative; these
// prove the phase-H gate can.
//
//   C-alloc  -> the checkNoGc(RULES) clause: a retain-per-iteration twin of the
//               warm loop forces major GC (report.ok === false).
//   C-detach -> the live === 0 clause: 64 churn cycles tracked OUTSIDE any owner
//               (no auto-untrack) and never disposed, so tracker.size() === 128.
//   C-fuzz   -> phase P's echo oracle: spawns cache-fuzzer.mjs with the break
//               var, which injects one phantom re-broadcast; child exit 1.
//
// The findings.length === 0 clause has NO control: owner-cascade audit() fires
// only when a lite-signal owner tree breaks under a still-tracked handle, and
// lite-query's public surface never leaves that state (clean owner disposal
// auto-untracks). Confirmed empirically this session; recorded in
// INCONCLUSIVE.md and carried to Q5. OR-1 forbids the runtime edit that would
// stage it.

import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

import { GcProfiler, checkNoGc } from '@zakkster/lite-gc-profiler';
import { queryClient, query } from '../../Query.js';
import { streamQuery } from '../../StreamQuery.js';
import { effect } from '@zakkster/lite-signal';

const HERE = dirname(fileURLToPath(import.meta.url));
const FUZZER = join(HERE, '..', '..', 'bench', 'torture', 'cache-fuzzer.mjs');
const DETACH_CYCLES = 64;

const settle = () => new Promise((r) => setTimeout(r, 50));

// C-alloc: same 200000-iteration shape as phase H's warm loop, same RULES, same
// evaluateGate -- one difference: a retained object per iteration. ~200k live
// objects force old-space growth and major collections; the seed's loop retains
// nothing. Trips report.ok === false, violation maxMajor limit=0 actual>=1.
async function ctlAlloc(ctx) {
  const { RULES, evaluateGate, FETCHER, tick, HOT } = ctx;
  const qc = queryClient({ defaultStaleTime: 0 });
  const warm = query(qc, { key: ['warm'], fetcher: FETCHER });
  const stopWarm = effect(() => warm.data());
  await tick();
  const keep = [];
  let sink = 0;
  const gc = new GcProfiler().start();
  for (let i = 0; i < HOT; i++) {
    const dv = warm.data();
    sink += (dv | 0);
    keep.push({ i, pad: new Array(16).fill(i) });   // the one difference: retained
    if ((i & 8191) === 0) {
      gc.sampleHeap(performance.now(), process.memoryUsage().heapUsed);
    }
  }
  await settle();
  const s = gc.summary();
  const report = checkNoGc(s, RULES);
  gc.stop();
  stopWarm(); warm.dispose();
  if (sink === Number.MIN_SAFE_INTEGER || keep.length < 0) console.log('unreachable');
  const gate = evaluateGate({ live: 0, findings: [], leaks: [], warns: [], summary: s, report, censusOk: true });
  let stderrText = '';
  for (const v of report.violations) {
    stderrText += '  violation ' + v.metric + ' limit=' + v.limit + ' actual=' + v.actual + '\n';
  }
  return { id: 'alloc', tripped: !gate.ok, clause: 'gc', stderrText };
}

// C-detach: 64 churn cycles with two deliberate omissions -- track() is called
// OUTSIDE any owner (so lite-leak's onCleanup(untrack) auto-untrack never
// registers) and handles are retained instead of disposed, with no qc.clear().
// Held-value contract preserved: NOOP_RELEASE and the tag close over nothing.
// Trips the live === 0 clause: tracker.size() === 128.
async function ctlDetach(ctx) {
  const { tracker, RULES, evaluateGate, FETCHER, NOOP_RELEASE, makeController } = ctx;
  const RETAIN = [];
  const censusRefs = [];
  const qc = queryClient({ defaultStaleTime: 0 });
  for (let i = 0; i < DETACH_CYCLES; i++) {
    const c = makeController();
    const q = query(qc, { key: ['q', i], fetcher: FETCHER });
    q.status();
    tracker.track(q, NOOP_RELEASE, 'query', { audit: true });        // no owner -> no auto-untrack
    RETAIN.push(q);
    censusRefs.push(new WeakRef(q));
    const sq = streamQuery(qc, { key: ['s', i], stream: c.factory, mode: 'buffer', maxBuffer: 4 });
    sq.data();
    tracker.track(sq, NOOP_RELEASE, 'streamQuery', { audit: true });
    RETAIN.push(sq);
    censusRefs.push(new WeakRef(sq));
    // deliberate: no dispose(), no qc.clear()
  }
  globalThis.gc?.();
  await settle();
  const live = tracker.size();
  const findings = tracker.audit();
  // a passing gc window, so the gc clause stays green (the leak + census
  // clauses are the ones under test here, not the budget).
  const gc = new GcProfiler().start();
  gc.sampleHeap(performance.now(), process.memoryUsage().heapUsed);
  gc.sampleHeap(performance.now(), process.memoryUsage().heapUsed);
  await new Promise((r) => setTimeout(r, 20));
  const s = gc.summary();
  const report = checkNoGc(s, RULES);
  gc.stop();
  // Census over the strongly-held RETAIN handles: same settle shape as phase H
  // (8 gc + macrotask cycles). Because RETAIN pins every handle, the whole
  // sample comes back live -- the census clause's all-live trip signature.
  const sample = censusRefs.slice(-256);
  for (let cyc = 0; cyc < 8; cyc++) {
    globalThis.gc?.();
    await new Promise((r) => setTimeout(r, 0));
  }
  let sampledLive = 0;
  for (const ref of sample) if (ref.deref() !== undefined) sampledLive++;
  // Same all-live signature phase H gates on ("nothing collected"), sized to the
  // control's 128 pinned handles instead of 256: fail iff the whole sample lives.
  const censusOk = !(sample.length > 0 && sampledLive === sample.length);
  if (RETAIN.length < 0) console.log('unreachable');
  const gate = evaluateGate({ live, findings, leaks: [], warns: [], summary: s, report, censusOk });
  const clause = censusOk ? 'leak' : 'leak+census';
  const stderrText = 'FAIL H leak=size ' + live + '/0\n' +
    'FAIL H census ' + sampledLive + '/' + sample.length + ' live\n';
  return { id: 'detach', tripped: !gate.ok, clause, stderrText };
}

// C-fuzz: spawn cache-fuzzer.mjs WITH the break var. Its one env-gated line
// snapshots broadcasts+1 -- a phantom re-broadcast, exactly the echo regression
// the oracle exists to catch. Child exit 1 with the echo-storm failure text.
function ctlFuzz() {
  const res = spawnSync(process.execPath, [FUZZER], {
    stdio: 'pipe',
    env: { ...process.env, QUERY_TORTURE_BREAK: 'fuzz', TORTURE_SECONDS: '1' },
  });
  const out = (res.stdout ? res.stdout.toString() : '') + (res.stderr ? res.stderr.toString() : '');
  const tripped = res.status === 1 &&
    out.includes('FAIL: echo storm -- expected') &&
    out.includes('(re-broadcast leak)');
  return { id: 'fuzz', tripped, clause: 'fuzz-oracle', stderrText: tripped ? '' : ('  fuzz child status=' + res.status + '\n' + out) };
}

export async function runControls(mode, ctx) {
  const ids = mode === '1' ? ['alloc', 'detach', 'fuzz'] : [mode];
  const results = [];
  for (const id of ids) {
    if (id === 'alloc') results.push(await ctlAlloc(ctx));
    else if (id === 'detach') results.push(await ctlDetach(ctx));
    else if (id === 'fuzz') results.push(ctlFuzz());
    // Unknown value: fail loudly (ambiguous state is not "off"; note "0" is
    // truthy in JS, so it reaches here rather than running the plain gate).
    else results.push({ id, unknown: true, tripped: false, stderrText: '  unknown QUERY_TORTURE_BREAK=' + id + '\n' });
  }
  let tripped = 0;
  for (const r of results) {
    if (r.unknown) {
      console.log('CONTROL unknown QUERY_TORTURE_BREAK value "' + r.id + '" -- valid: 1|alloc|detach|fuzz');
    } else if (r.tripped) {
      tripped++;
      console.log('CONTROL ' + r.id + ' tripped: ' + r.clause);
    } else {
      // reserved for a genuine control that ran and failed to trip.
      console.log('CONTROL ' + r.id + ' DID-NOT-TRIP -- gate is decorative');
    }
  }
  console.log('CONTROL SUMMARY tripped=' + tripped + '/' + ids.length);
  for (const r of results) if (r.stderrText) process.stderr.write(r.stderrText);
  return { tripped, total: ids.length };
}
