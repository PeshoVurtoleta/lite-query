// test/torture.mjs -- node --expose-gc test/torture.mjs
//
// THE single suite-law torture entry for @zakkster/lite-query (npm run
// torture:law). Three phases, one tracker, one frozen rules object, one gate
// evaluator, run in order:
//
//   phase P (pool)  -- spawnSync the three bench/torture/*.mjs soaks as child
//                      processes and assert exit 0 on each (env-bleed guard:
//                      QUERY_TORTURE_BREAK deleted from the child env). Child
//                      output is buffered and printed only on failure or under
//                      QUERY_TORTURE_VERBOSE=1.
//   phase H (heap)  -- the 1.2.0 seed's two gates, bodies unchanged: 4096-cycle
//                      lifecycle churn (tracker.size() back to 0) + 200000 warm
//                      reads under GcProfiler + checkNoGc, plus a one-sided
//                      WeakRef census beside tracker.size() (lite-leak 1.10.0:
//                      size() counts registrations, not reachability).
//   phase C (controls) -- cold path; runs iff QUERY_TORTURE_BREAK is set. Bodies
//                      live in test/torture/controls.mjs, reached by dynamic
//                      import only, so the plain run pays zero bytes for them.
//                      In break mode phases P and H are skipped, so exactly one
//                      profiler is ever in flight.
//
// Retention + GC-budget proof wired to the real entry points: query()
// (warm-read hot path) and streamQuery() in buffer mode (the surface the C4
// collapse touched -- onValue-driven status/count and the live droppedCount
// getter). The package's broader soak gate is the phase-P children
// (bench/torture/*.mjs, lite-signal pool introspection); this harness adds the
// lite-leak / lite-gc-profiler retention + budget proof.
//
// The lite-leak / lite-gc-profiler deps are dev-only diagnostics; nothing here
// ships (test/ is not in files[]).

import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

import { GcProfiler, checkNoGc } from '@zakkster/lite-gc-profiler';
import {
  createLeakTracker,
  createOwnerCascadeOrphanKernel,
  createObserverOrphanKernel,
  createAsyncRetentionKernel,
} from '@zakkster/lite-leak';
import { createRoot, effect } from '@zakkster/lite-signal';

// >>> WIRE 1: the package under test
import { queryClient, query } from '../Query.js';
import { streamQuery } from '../StreamQuery.js';

const CYCLES = 4096;
const HOT = 200000;
const leaks = [];
const warns = [];

// Frozen; checkNoGc lane only. checkNoGc accepts ONLY the six rules it
// evaluates (lite-gc-profiler 1.16.0, OR-3): maxMajor/maxMinor/maxPauseMs/
// maxTotalMs/maxAllocRate/maxArrayBuffersGrowth. A per-op/per-frame key throws.
const RULES = Object.freeze({ maxMajor: 0, maxPauseMs: 4 });

const VERBOSE = process.env.QUERY_TORTURE_VERBOSE === '1';

const HERE = dirname(fileURLToPath(import.meta.url));
const BENCH = join(HERE, '..', 'bench', 'torture');
const P_SCRIPTS = ['query-soak.mjs', 'cache-fuzzer.mjs', 'shared-fetch-soak.mjs'];

// Module-scope release / fetcher -- must NOT close over any tracked target
// (the held-value contract: capturing target defeats finalization).
const NOOP_RELEASE = () => {};
const FETCHER = async () => 1;

// A minimal manually-driven async iterator: yields queued values, done on
// return() (abort-on-detach). No wall-clock timers.
function makeController() {
  const c = { _results: [], _waiter: null, closed: false };
  const pump = () => {
    if (c._waiter && c._results.length) {
      const w = c._waiter; c._waiter = null;
      const d = c._results.shift();
      w.resolve(d.type === 'done' ? { done: true, value: undefined } : { done: false, value: d.value });
    }
  };
  const iterator = {
    next() {
      if (c._results.length) {
        const d = c._results.shift();
        return Promise.resolve(d.type === 'done' ? { done: true, value: undefined } : { done: false, value: d.value });
      }
      if (c.closed) return Promise.resolve({ done: true, value: undefined });
      return new Promise((resolve) => { c._waiter = { resolve }; });
    },
    return() { c.closed = true; if (c._waiter) { const w = c._waiter; c._waiter = null; w.resolve({ done: true, value: undefined }); } return Promise.resolve({ done: true, value: undefined }); },
  };
  c.factory = () => iterator;
  c.push = (v) => { c._results.push({ type: 'value', value: v }); pump(); };
  return c;
}

const tick = () => new Promise((r) => setTimeout(r, 0));

const tracker = createLeakTracker({
  name: 'torture',
  onLeak: (r) => leaks.push(r.kind + ':' + String(r.tag)),
  onWarning: (w) => warns.push(w.kind + ':' + w.reason),
});
// Kernels for the surfaces actually in play: lite-signal's owner tree and
// observer graph, and async fetch retention. lite-query patches no global
// timer/listener surface (its GC timers are injectable), so those kernels are
// omitted per the harness guidance (register only patched surfaces).
tracker.registerKernel(createOwnerCascadeOrphanKernel());
tracker.registerKernel(createObserverOrphanKernel());
tracker.registerKernel(createAsyncRetentionKernel());

// ---- gate evaluator -------------------------------------------------------
// The seed's boolean and the seed's GATE string, moved not rewritten (OR-2).
// ADDITIVE clause, fail-closed per profiler 1.16.0 three-state law: an
// inconclusive verdict is not a pass, and the one-sided census folds in.
// allowInconclusive is NEVER set anywhere in this file.
function evaluateGate({ live, findings, leaks: lk, warns: wn, summary, report, censusOk }) {
  const ok = report.ok && report.verdict === 'pass' && live === 0 &&
    lk.length === 0 && findings.length === 0 && censusOk;
  const line =
    'GATE leak=size ' + live + '/0 findings=' + findings.length +
    ' warnings=' + wn.length +
    ' | gc major=' + summary.gc.major + ' minor=' + summary.gc.minor +
    ' maxMs=' + summary.gc.maxMs.toFixed(2) +
    ' | ' + (ok ? 'ok' : 'FAIL');
  return { ok, line };
}

// ---- phase P: exec the three bench/torture soaks --------------------------
function runPhaseP() {
  const env = { ...process.env };
  delete env.QUERY_TORTURE_BREAK; // env-bleed guard: children never see the break switch
  const failures = [];
  for (const script of P_SCRIPTS) {
    const res = spawnSync(process.execPath, [join(BENCH, script)], { stdio: 'pipe', env });
    const out = (res.stdout ? res.stdout.toString() : '') + (res.stderr ? res.stderr.toString() : '');
    if (VERBOSE) process.stdout.write('--- ' + script + ' ---\n' + out);
    if (res.status !== 0) {
      failures.push(script + ' status=' + res.status);
      if (!VERBOSE) process.stderr.write('--- ' + script + ' (FAILED) ---\n' + out);
    }
  }
  return failures;
}

// ---- phase H: retention + GC torture --------------------------------------
async function runPhaseH() {
  const censusRefs = [];

  // ---- phase 1: retention torture ----------------------------------------
  // Churn query() and streamQuery() handles: subscribe inside an owned effect,
  // track the handle, dispose the owner (cascade detach) + the handle (release
  // its pooled signal node). A properly-used-and-disposed handle must finalize.
  for (let i = 0; i < CYCLES; i++) {
    const qc = queryClient({ defaultStaleTime: 0 });
    const c = makeController();
    let qHandle = null;
    let sHandle = null;
    const stop = createRoot(() => effect(() => {
      const q = query(qc, { key: ['q'], fetcher: FETCHER });
      q.status();                                     // subscribe (attach)
      qHandle = q;
      tracker.track(q, NOOP_RELEASE, 'query', { audit: true });

      const sq = streamQuery(qc, { key: ['s'], stream: c.factory, mode: 'buffer', maxBuffer: 4 });
      sq.data();                                      // subscribe (attach + start pump)
      sHandle = sq;
      tracker.track(sq, NOOP_RELEASE, 'streamQuery', { audit: true });
    }));
    stop();                                           // dispose the effect -> detach
    // Census sample: weak-reference the churn handles before release, so the
    // one-sided reachability check below can prove they were collectable.
    if (qHandle) censusRefs.push(new WeakRef(qHandle));
    if (sHandle) censusRefs.push(new WeakRef(sHandle));
    if (qHandle) { qHandle.dispose(); qHandle = null; }
    if (sHandle) { sHandle.dispose(); sHandle = null; }
    qc.clear();                                        // disposeEntry -> release entry signal nodes
  }

  globalThis.gc?.();
  await new Promise((r) => setTimeout(r, 50));

  const live = tracker.size();
  const findings = tracker.audit();

  // ---- phase 2: allocation + GC torture ----------------------------------
  // Steady-state handles allocated OUTSIDE the loop; the loop only reads warm
  // accessors. query() warm read + streamQuery() buffer-mode data()/droppedCount
  // (the live overflow getter the C4 collapse routes through). Zero major GC.
  const qc2 = queryClient({ defaultStaleTime: 0 });
  const warm = query(qc2, { key: ['warm'], fetcher: FETCHER });
  const stopWarm = effect(() => warm.data());
  const c2 = makeController();
  const buf = streamQuery(qc2, { key: ['buf'], stream: c2.factory, mode: 'buffer', maxBuffer: 4 });
  const stopBuf = effect(() => buf.data());
  await tick();
  for (let k = 0; k < 20; k++) { c2.push(k); await tick(); }   // drive drops (droppedCount -> 16)

  let sink = 0;
  const gc = new GcProfiler().start();
  for (let i = 0; i < HOT; i++) {
    const dv = warm.data();
    sink += (dv | 0) + buf.count() + buf.droppedCount() + (Array.isArray(buf.data()) ? buf.data().length : 0);
    if ((i & 8191) === 0) {
      gc.sampleHeap(performance.now(), process.memoryUsage().heapUsed);
    }
  }

  await new Promise((r) => setTimeout(r, 50));
  const s = gc.summary();
  const report = checkNoGc(s, RULES);
  gc.stop();
  stopWarm(); stopBuf(); warm.dispose(); buf.dispose();
  if (sink === Number.MIN_SAFE_INTEGER) console.log('unreachable');   // keep sink live

  // ---- phase H census (additive, non-flaky) ------------------------------
  // tracker.size() is live REGISTRATIONS, not a reachability census (lite-leak
  // 1.10.0). Pair it with a WeakRef census: 256 handles, 8 settle cycles of
  // gc() + macrotask yield. ONE-SIDED gate -- fail iff nothing at all was
  // collected (sampledLive === 256, a real retention signature); partial
  // survival is noise and never a failure.
  const sample = censusRefs.slice(-256);
  for (let cyc = 0; cyc < 8; cyc++) {
    globalThis.gc?.();
    await new Promise((r) => setTimeout(r, 0));
  }
  let sampledLive = 0;
  for (const ref of sample) if (ref.deref() !== undefined) sampledLive++;
  const censusOk = !(sample.length === 256 && sampledLive === 256);
  if (VERBOSE) process.stderr.write('census ' + sampledLive + '/' + sample.length + ' live\n');

  return { live, findings, summary: s, report, censusOk, sampledLive, sampleSize: sample.length };
}

// ---- orchestration --------------------------------------------------------
// Fail closed on a missing --expose-gc BEFORE any phase runs: without it the
// census settle cycles cannot collect and phase H reports a leak-shaped
// 256/256, which would send a human chasing a phantom retention bug. Name the
// fix instead of failing incidentally.
if (typeof globalThis.gc !== 'function') {
  process.stderr.write('FAIL missing --expose-gc; run: node --expose-gc test/torture.mjs\n');
  process.exit(1);
}

// Break switch: "set" means NON-EMPTY. QUERY_TORTURE_BREAK= (empty string) is
// falsy and runs the plain phase P + H gate; a non-empty value selects phase C.
const BREAK = process.env.QUERY_TORTURE_BREAK;
if (BREAK) {
  // Phase C only (cold path): phases P and H are skipped so exactly one
  // profiler is ever in flight and no phase-P child can fail for the wrong
  // reason. Controls reuse the module tracker, RULES and the SAME evaluateGate
  // -- a control that trips a lookalike gate proves nothing.
  const mod = await import('./torture/controls.mjs');
  await mod.runControls(BREAK, {
    tracker, RULES, evaluateGate, leaks, warns,
    makeController, FETCHER, NOOP_RELEASE, tick, HOT,
  });
  process.exitCode = 1; // break mode exits non-zero always
} else {
  const pFailures = runPhaseP();
  const h = await runPhaseH();
  const gate = evaluateGate({
    live: h.live, findings: h.findings, leaks, warns,
    summary: h.summary, report: h.report, censusOk: h.censusOk,
  });
  console.log(gate.line);
  const ok = gate.ok && pFailures.length === 0;
  if (ok) {
    console.log('ok');
  } else {
    for (const f of pFailures) process.stderr.write('FAIL P ' + f + '\n');
    if (!gate.ok) {
      if (h.live !== 0 || leaks.length > 0) process.stderr.write('FAIL H leak=size ' + h.live + '/0\n');
      if (!h.censusOk) process.stderr.write('FAIL H census ' + h.sampledLive + '/' + h.sampleSize + ' live\n');
      if (h.report.verdict !== 'pass') process.stderr.write('FAIL H gc verdict=' + h.report.verdict + ' -- see INCONCLUSIVE.md\n');
      for (const v of h.report.violations) process.stderr.write('  violation ' + v.metric + ' limit=' + v.limit + ' actual=' + v.actual + '\n');
      for (const f of h.findings) process.stderr.write('  finding ' + f.kind + ':' + f.reason + '\n');
      for (const l of leaks) process.stderr.write('  leak ' + l + '\n');
    }
    process.exitCode = 1;
  }
}
