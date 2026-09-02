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
import { queryClient, query, infiniteQuery, persistQueryClient } from '../Query.js';
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
// Infinite-query page fetcher + cursor -- module scope so they close over no
// tracked target (held-value contract). Each page is a two-item array; the
// cursor is the page index, exhausting after four pages.
const PAGE_FETCHER = async ({ cursor }) => { const b = (cursor == null ? 0 : cursor) * 2; return [b, b + 1]; };
const GET_NEXT = (lastPage, allPages) => (allPages.length < 4 ? allPages.length : null);
// Persistence churn support (phase 1b): a no-op save (closes over no tracked
// target) and a minimal per-cycle mock clock whose timers are created + cleared
// inside the cycle and never advanced, so no wall-clock timer is ever armed.
const NOOP_SAVE = () => {};
function makePersistClock() {
  let id = 1;
  const timers = new Map();
  return {
    now: () => 0,
    setTimeout: (fn, ms) => { const t = id++; timers.set(t, fn); return t; },
    clearTimeout: (t) => { timers.delete(t); },
  };
}

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
    let iHandle = null;
    const stop = createRoot(() => effect(() => {
      const q = query(qc, { key: ['q'], fetcher: FETCHER });
      q.status();                                     // subscribe (attach)
      qHandle = q;
      tracker.track(q, NOOP_RELEASE, 'query', { audit: true });

      const sq = streamQuery(qc, { key: ['s'], stream: c.factory, mode: 'buffer', maxBuffer: 4 });
      sq.data();                                      // subscribe (attach + start pump)
      sHandle = sq;
      tracker.track(sq, NOOP_RELEASE, 'streamQuery', { audit: true });

      const iq = infiniteQuery(qc, { key: ['i'], fetcher: PAGE_FETCHER, getNextCursor: GET_NEXT });
      iq.data();                                      // subscribe (attach + fetch page one)
      iHandle = iq;
      tracker.track(iq, NOOP_RELEASE, 'infiniteQuery', { audit: true });
    }));
    stop();                                           // dispose the effect -> detach
    // Census sample: weak-reference the churn handles before release, so the
    // one-sided reachability check below can prove they were collectable.
    if (qHandle) censusRefs.push(new WeakRef(qHandle));
    if (sHandle) censusRefs.push(new WeakRef(sHandle));
    if (iHandle) censusRefs.push(new WeakRef(iHandle));
    if (qHandle) { qHandle.dispose(); qHandle = null; }
    if (sHandle) { sHandle.dispose(); sHandle = null; }
    if (iHandle) { iHandle.dispose(); iHandle = null; }
    qc.clear();                                        // disposeEntry -> release entry signal nodes
  }

  // ---- phase 1b: persistence churn (dehydrate/hydrate/teardown) -----------
  // Serialization is the classic accidental-retention factory: a payload that
  // closes over the cache, or a persister that outlives stop(), pins the whole
  // entry map. Each cycle snapshots a small cache, restores it into a fresh
  // client THROUGH THE ADAPTER, then tears both down -- install -> restored ->
  // a write that opens a pending throttled save -> stop() (FLUSH-ON-STOP, which
  // fires the pending save and uninstalls). The payload is the retention
  // subject; it is WeakRef-sampled into the census below (not tracker.track'd:
  // lite-leak 1.10.0 size() counts live REGISTRATIONS, and a payload has no
  // reactive owner to auto-untrack, so a WeakRef census -- not a registration
  // -- is the right reachability tool for it). The OR-11 findings-clause attempt
  // (a payload tracked with { audit: true } carried across stop()) was run
  // against this exact path and did NOT fire; recorded verbatim in
  // INCONCLUSIVE.md (Attempt C). Runs before the gc() settle below, so the
  // census (8 cycles) proves the payloads collect.
  for (let i = 0; i < CYCLES; i++) {
    const clock = makePersistClock();
    const src = queryClient({ now: clock.now, setTimeout: clock.setTimeout, clearTimeout: clock.clearTimeout, defaultStaleTime: 0 });
    src.setQueryData(['p', 0], i);
    src.setQueryData(['p', 1], { v: i });
    const payload = { version: 'v1', state: src.dehydrate() };
    src.clear();
    src.dispose();

    const dst = queryClient({ now: clock.now, setTimeout: clock.setTimeout, clearTimeout: clock.clearTimeout, defaultStaleTime: 0 });
    const handle = persistQueryClient(dst, { save: NOOP_SAVE, load: () => payload, version: 'v1', throttle: 1000 });
    censusRefs.push(new WeakRef(payload));
    await handle.restored;                 // arm the write hook (drain the load microtask)
    dst.setQueryData(['w'], i);            // opens a pending throttled save window
    handle.stop();                         // FLUSH-ON-STOP: fires the pending save, then teardown
    dst.clear();
    dst.dispose();
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
  // Prefilled infinite handle: four pages accumulated, then read WARM inside the
  // hot loop. pages() returns the stored array, data() the live flat view,
  // hasNextPage() a bool -- all zero-allocation on an attached observer. Pages
  // are injected synchronously via setQueryData (the whole-list rebuild path)
  // rather than three imperative fetchNextPage() awaits: an un-owned async
  // fetch would trip the async-retention kernel's no-owner-set warning and move
  // the GATE line off byte-identical.
  const inf = infiniteQuery(qc2, { key: ['inf'], fetcher: PAGE_FETCHER, getNextCursor: GET_NEXT });
  const stopInf = effect(() => inf.data());
  await tick();                                       // attach + configure infinite
  qc2.setQueryData(['inf'], [[0, 1], [2, 3], [4, 5], [6, 7]]);   // four pages, flat rebuilt
  for (let k = 0; k < 20; k++) { c2.push(k); await tick(); }   // drive drops (droppedCount -> 16)

  let sink = 0;
  const gc = new GcProfiler().start();
  for (let i = 0; i < HOT; i++) {
    const dv = warm.data();
    sink += (dv | 0) + buf.count() + buf.droppedCount() + (Array.isArray(buf.data()) ? buf.data().length : 0)
      + inf.pages().length + inf.data().length + (inf.hasNextPage() ? 1 : 0);
    if ((i & 8191) === 0) {
      gc.sampleHeap(performance.now(), process.memoryUsage().heapUsed);
    }
  }

  await new Promise((r) => setTimeout(r, 50));
  const s = gc.summary();
  const report = checkNoGc(s, RULES);
  gc.stop();
  stopWarm(); stopBuf(); stopInf(); warm.dispose(); buf.dispose(); inf.dispose();
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

// ---- phase H provenance: the feed double-run (ON-2, Q7) --------------------
// Runs AFTER the frozen gate evaluation and prints its OWN lines -- it NEVER
// touches the byte-frozen GATE line (option a). Three configs share one warm body
// (200000 reads) plus a 20000-write sub-loop that REWRITES a pre-created 64-key
// space (bounded retention, so the feed's per-event cost is isolated from entry
// growth), each under its own GcProfiler gated by RULES:
//   absent          -- no hook. Proves G3: zero added allocation (B/op 0.00).
//   installed-pooled -- candidate (c): a no-op COPYING hook that reads pooled
//                       fields; 0 B allocated per event. The T3 recommendation.
//   installed-fresh  -- candidate (b) simulated: the hook allocates a fresh
//                       3-field object per event and discards it.
// heapDelta is bracketed by gc() so it reports NET RETAINED (~0 for all three:
// pooled retains nothing, fresh's per-event objects are transient); the pooled-
// vs-fresh difference shows in the minor-GC column. Both installed runs must hold
// maxMajor 0 (G3). Recorded as a 3-row table in the CHANGELOG [1.5.0] head.
const PROV_HOT = 200000;
const PROV_WRITES = 20000;
let provSink = 0;
let provKeep = null;
async function provRun(cfg) {
  const qc = queryClient({ defaultStaleTime: 0 });
  const warm = query(qc, { key: ['warm'], fetcher: FETCHER });
  const stopWarm = effect(() => warm.data());
  const c = makeController();
  const buf = streamQuery(qc, { key: ['buf'], stream: c.factory, mode: 'buffer', maxBuffer: 4 });
  const stopBuf = effect(() => buf.data());
  await tick();
  for (let k = 0; k < 64; k++) qc.setQueryData(['pw', k], 0);   // pre-create -> writes only rewrite
  let hookCount = 0;
  let uninstall = null;
  if (cfg === 'installed-pooled') {
    uninstall = qc.inspect((e) => { hookCount++; provSink = e.type.length + e.count; });
  } else if (cfg === 'installed-fresh') {
    uninstall = qc.inspect((e) => { hookCount++; provKeep = { type: e.type, ts: e.ts, keyHash: e.keyHash }; });
  }
  // The absent baseline is the zero-alloc WARM READ path (V7): 200000 reads, no
  // writes -> a true 0.00 B/op, the same allocation profile the frozen GATE
  // proves. Runs 2 and 3 ADDITIONALLY drive the 20000-write sub-loop to produce
  // emits (the warm read loop performs no status writes, so it would not exercise
  // an installed hook otherwise). Both installed runs must still hold maxMajor 0.
  const doWrites = cfg !== 'absent';
  // Warmup pass (unmeasured): grow lite-signal's node/link pools to steady state
  // so the MEASURED window rewrites into already-sized pools and retains ~0.
  for (let i = 0; i < PROV_HOT; i++) {
    provSink += (warm.data() | 0) + buf.count() + buf.droppedCount();
    if (doWrites && i < PROV_WRITES) qc.setQueryData(['pw', i & 63], i);
  }
  hookCount = 0;                                      // count only the MEASURED window's emits
  globalThis.gc?.();
  await new Promise((r) => setTimeout(r, 20));
  const before = process.memoryUsage().heapUsed;     // pre-loop, post-gc (outside the window)
  const gcp = new GcProfiler().start();
  for (let i = 0; i < PROV_HOT; i++) {
    provSink += (warm.data() | 0) + buf.count() + buf.droppedCount();
    if (doWrites && i < PROV_WRITES) qc.setQueryData(['pw', i & 63], i);   // rewrite -> emits, no new entry
    if ((i & 8191) === 0) gcp.sampleHeap(performance.now(), process.memoryUsage().heapUsed);
  }
  await new Promise((r) => setTimeout(r, 50));
  const s = gcp.summary();                            // window = the loop only (no forced gc)
  const report = checkNoGc(s, RULES);
  gcp.stop();
  globalThis.gc?.();                                  // net-retained bracket, OUTSIDE the window
  await new Promise((r) => setTimeout(r, 20));
  const after = process.memoryUsage().heapUsed;
  stopWarm(); stopBuf(); if (uninstall) uninstall();
  warm.dispose(); buf.dispose(); qc.dispose();
  if (provSink === Number.MIN_SAFE_INTEGER) console.log('unreachable', provKeep);
  const ops = PROV_HOT + PROV_WRITES;
  const heapDelta = after - before;
  return {
    cfg, major: s.gc.major, minor: s.gc.minor, maxMs: s.gc.maxMs,
    heapDelta, perOp: heapDelta / ops, verdict: report.verdict, ok: report.ok, hookCount,
  };
}

async function runProvenance() {
  const results = [];
  for (const cfg of ['absent', 'installed-pooled', 'installed-fresh']) {
    results.push(await provRun(cfg));
  }
  console.log('phase H provenance (ON-2; outside the frozen GATE window):');
  console.log('  run              | major | minor | maxMs | heapDelta B |  B/op | emits');
  for (const r of results) {
    console.log('  ' + r.cfg.padEnd(16) + ' | ' + String(r.major).padStart(5) + ' | ' +
      String(r.minor).padStart(5) + ' | ' + r.maxMs.toFixed(2).padStart(5) + ' | ' +
      String(r.heapDelta).padStart(11) + ' | ' + r.perOp.toFixed(2).padStart(5) + ' | ' + r.hookCount);
  }
  const absent = results.find((r) => r.cfg === 'absent');
  const pooled = results.find((r) => r.cfg === 'installed-pooled');
  let provOk = true;
  // G3, zero added allocation: the absent warm-read window is byte-identical in
  // GC PROFILE to the frozen GATE line (major=0, minor=0, maxMs=0.00) -- zero
  // collectible allocation on the hot read path (V7). The GC-event count is the
  // true signal: a window that triggers no minor GC allocated nothing. The
  // heapDelta byte count is V8 heap bookkeeping (sample arrays, async timer
  // state) even at minor=0, so it is reported but not gated.
  if (!(absent.major === 0 && absent.minor === 0 && absent.maxMs < 0.005)) {
    process.stderr.write('  FAIL provenance: absent GC profile major=' + absent.major +
      ' minor=' + absent.minor + ' maxMs=' + absent.maxMs.toFixed(2) + ' (expected 0/0/0.00)\n');
    provOk = false;
  }
  // The installed candidate (c) must still hold maxMajor 0 under the emit load.
  if (!(pooled.ok && pooled.verdict === 'pass' && pooled.major === 0)) {
    process.stderr.write('  FAIL provenance: installed-pooled major=' + pooled.major +
      ' verdict=' + pooled.verdict + '\n');
    provOk = false;
  }
  console.log(provOk
    ? '  PASS: absent GC profile == frozen GATE (0/0/0.00); installed-pooled holds maxMajor 0 (G3)'
    : '  provenance FAILED');
  if (!provOk) process.exitCode = 1;
}

// ---- OR-10 attempt D: inspect install/uninstall retention (NOT a gate) -----
// 4096 cycles: a hook closure tracked { audit: true } OUTSIDE any owner and
// carried across the inspect() install/uninstall boundary while the client's
// owner tree is torn down (clear + dispose). Honest pass-or-fail; the outcome is
// recorded verbatim in INCONCLUSIVE.md as Attempt D. It is NOT a gate clause
// (ON-3): a control that cannot trip is decorative.
let dSink = 0;
function makeInspectHook() { return (e) => { dSink = e.type.length; }; }
async function runAttemptD() {
  const beforeSize = tracker.size();
  const CYC = 4096;
  for (let i = 0; i < CYC; i++) {
    const qc = queryClient({ defaultStaleTime: 0 });
    const hookClosure = makeInspectHook();       // closes over dSink (module) only, not qc
    tracker.track(hookClosure, NOOP_RELEASE, 'inspect-hook', { audit: true });
    const uninstall = qc.inspect(hookClosure);
    qc.setQueryData(['d', i & 15], i);           // drive events through the hook
    uninstall();                                 // carried across the install/uninstall boundary
    qc.clear();
    qc.dispose();                                // tear down the client owner tree
  }
  globalThis.gc?.();
  await new Promise((r) => setTimeout(r, 50));
  const liveD = tracker.size() - beforeSize;
  const findingsD = tracker.audit();
  const fired = liveD !== 0 || findingsD.length !== 0;
  if (dSink === Number.MIN_SAFE_INTEGER) console.log('unreachable');
  console.log('phase H OR-10 attempt D (inspect install/uninstall retention; NOT a gate clause):');
  console.log('  cycles=' + CYC + ' tracked-hook live-delta=' + liveD + ' audit-findings=' +
    findingsD.length + ' -> ' + (fired ? 'FIRED' : 'DID NOT FIRE (carried; recorded verbatim in INCONCLUSIVE.md)'));
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
  // C7 (ON-2): the feed provenance double-run + OR-10 attempt D run AFTER the
  // frozen gate evaluation and print their own lines -- they never alter the GATE
  // line above. The provenance can independently fail the process (G3); attempt D
  // is recorded only (never a gate clause).
  await runProvenance();
  await runAttemptD();
}
