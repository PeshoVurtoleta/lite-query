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
import { createRoot, effect, signal } from '@zakkster/lite-signal';

// >>> WIRE 1: the package under test
import { queryClient, query, infiniteQuery, persistQueryClient, mutation } from '../Query.js';
import { streamQuery } from '../StreamQuery.js';
import { createMockClock, createMockBroadcastChannel } from './harness.js';

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
  // QD-1: heapDelta is bookkeeping noise (informational), but hold it under a hard
  // 1.0 B/op bound so a real per-op retention regression on the warm read path
  // still trips even if it does not perturb the GC-event count.
  if (!(Math.abs(absent.perOp) < 1.0)) {
    process.stderr.write('  FAIL provenance: absent B/op=' + absent.perOp.toFixed(2) + ' (bound < 1.0)\n');
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

// ---- C11 shared-stream soak (Q8): 5-tab N-tab soak + leader-failover -------
// Runs AFTER the frozen gate evaluation and prints its OWN lines -- it never
// touches the byte-frozen GATE line (ON-2 precedent). Five tabs share ONE
// upstream connection through leader churn (killed every k ops); the mock
// source counts live connections, and every follower records its projected
// frames so any duplicate or reorder is a strictly-non-increasing step. Also
// asserts G5: the leader keeps ZERO per-follower state, so its entries.size is
// identical whether 1 or 4 followers are stalled (a prohibition, not a bound).
function makeCountedStreamSource() {
  let live = 0, opened = 0;
  const conns = new Set();
  const factory = (ctx) => {
    opened++;
    const conn = { buf: [], wake: null, aborted: false, closed: false };
    conn.k = () => { if (conn.wake) { const r = conn.wake; conn.wake = null; r(); } };
    const close = () => { if (!conn.closed) { conn.closed = true; live--; conns.delete(conn); } };
    conns.add(conn); live++;
    if (ctx && ctx.signal) ctx.signal.addEventListener('abort', () => { conn.aborted = true; conn.k(); close(); });
    return { async *[Symbol.asyncIterator]() {
      try {
        while (true) {
          if (conn.buf.length) { yield conn.buf.shift(); continue; }
          if (conn.aborted) return;
          await new Promise((r) => { conn.wake = r; });
        }
      } finally { close(); }
    } };
  };
  return { factory, pushAll(v) { for (const c of conns) { c.buf.push(v); c.k(); } },
    get live() { return live; }, get opened() { return opened; } };
}

async function soakDrain(n) { for (let i = 0; i < n; i++) await Promise.resolve(); }

// A source that stamps every connection with a distinct owner id and pushes a
// DISTINCT per-connection value each round ({ owner, s }), so a follower can
// prove the per-(epoch,owner)-segment subsequence law when multiple owners are
// live at once. Tracks the max simultaneous connection count (the concurrency
// window the reviewer proved the plain soak never entered).
function makeStampedSource() {
  let live = 0, nextId = 0, maxLive = 0;
  const conns = new Set();
  const factory = (ctx) => {
    const conn = { id: nextId++, s: 0, buf: [], wake: null, aborted: false, closed: false };
    conn.k = () => { if (conn.wake) { const r = conn.wake; conn.wake = null; r(); } };
    const close = () => { if (!conn.closed) { conn.closed = true; live--; conns.delete(conn); } };
    conns.add(conn); live++; if (live > maxLive) maxLive = live;
    if (ctx && ctx.signal) ctx.signal.addEventListener('abort', () => { conn.aborted = true; conn.k(); close(); });
    return { async *[Symbol.asyncIterator]() {
      try {
        while (true) {
          if (conn.buf.length) { yield conn.buf.shift(); continue; }
          if (conn.aborted) return;
          await new Promise((r) => { conn.wake = r; });
        }
      } finally { close(); }
    } };
  };
  return { factory, pushRound() { for (const c of conns) { c.s++; c.buf.push({ owner: c.id, s: c.s }); c.k(); } },
    get live() { return live; }, get maxLive() { return maxLive; } };
}

// Concurrency phase: force TWO+ owners live at once (kill the leader, let the
// racers' watchdogs fire, push from every owner BEFORE convergence) and prove a
// PURE follower (a tab whose watchdog never fires this window) projects a stream
// where every owner-run is strictly increasing (dup=0, reorder=0) and no owner
// re-appears after convergence -- the QD-1 law under real concurrency.
async function runStreamConcurrencySoak() {
  const clock = createMockClock();
  const bc = createMockBroadcastChannel();
  const src = makeStampedSource();
  // tab 0: initial leader; tabs 1-3: racers (short idle); tab 4: pure observer
  // (huge idle -> never self-connects this window, stays a follower).
  const mk = (idx, idle, lead) => queryClient({
    crossTab: true, broadcastChannel: bc.BroadcastChannel, crossTabChannel: 'conc',
    sharedStream: true, isLeader: () => lead,
    now: clock.now, setTimeout: clock.setTimeout, clearTimeout: clock.clearTimeout,
    streamIdleTimeout: idle,
  });
  const tabs = [mk(0, 1000, true), mk(1, 1000, false), mk(2, 1000, false), mk(3, 1000, false), mk(4, 1e9, false)];
  const obs = [];   // pure follower's projected frames
  const handles = tabs.map((t, i) => {
    const h = streamQuery(t, { key: ['c'], stream: src.factory });
    const d = createRoot(() => effect(() => { const v = h.data(); if (i === 4 && v && typeof v === 'object') obs.push(v); }));
    return { h, d };
  });
  await soakDrain(20);

  // leader streams a few rounds; the observer projects them
  for (let r = 0; r < 5; r++) { src.pushRound(); await soakDrain(3); }

  // KILL the leader (abort its connection, NO stream-end -> watchdog only).
  const lead = tabs[0]._internal.entries.get(JSON.stringify(['c']));
  if (lead && lead.streamStop) { try { lead.streamStop(); } catch {} lead.streamStop = null; lead.streamOwner = false; }
  // fire the racers' watchdogs together -> several promote CONCURRENTLY
  clock.advance(1000);
  // push from EVERY live owner while they are still racing (minimal drain, so
  // the opens have not yet crossed and multiple owners are broadcasting).
  await soakDrain(2);
  for (let r = 0; r < 4; r++) { src.pushRound(); await soakDrain(1); }
  // now let it converge
  await soakDrain(40);
  for (let r = 0; r < 4; r++) { src.pushRound(); await soakDrain(3); }
  await soakDrain(20);

  // -- assert the QD-1 law on the pure follower's projected stream --
  let dup = 0, reorder = 0;
  const seenVal = new Set();
  const lastSByOwner = new Map();
  const ownerOrder = [];
  for (const v of obs) {
    const key = v.owner + ':' + v.s;
    if (seenVal.has(key)) dup++; else seenVal.add(key);
    if (lastSByOwner.has(v.owner) && v.s <= lastSByOwner.get(v.owner)) reorder++;
    lastSByOwner.set(v.owner, v.s);
    if (ownerOrder.length === 0 || ownerOrder[ownerOrder.length - 1] !== v.owner) ownerOrder.push(v.owner);
  }
  // no owner re-appears in a non-adjacent run (converged, not oscillating)
  let reappear = 0;
  const runOwners = new Set();
  for (const o of ownerOrder) { if (runOwners.has(o)) reappear++; runOwners.add(o); }
  const concurrentOwners = src.maxLive;
  const tailConverged = obs.length >= 2 && obs[obs.length - 1].owner === obs[obs.length - 2].owner;

  handles.forEach((x) => { x.d(); x.h.dispose(); });
  await soakDrain(10);
  tabs.forEach((t) => t.dispose());
  await soakDrain(10);

  const ok = dup === 0 && reorder === 0 && reappear === 0 && concurrentOwners > 1 && tailConverged;
  console.log('phase H shared-stream CONCURRENCY soak (Q8 QD-1/QD-2/QD-3; outside the frozen GATE window):');
  console.log('  observer-frames=' + obs.length + ' owner-runs=' + ownerOrder.length +
    ' concurrent-owners(maxLive)=' + concurrentOwners +
    ' | dup=' + dup + ' reorder=' + reorder + ' owner-reappear=' + reappear + ' tailConverged=' + tailConverged);
  console.log(ok
    ? '  PASS: >1 owner live simultaneously; every owner-run strictly increasing, no dup/reorder/oscillation (QD-1 under real concurrency)'
    : '  FAIL: concurrency-phase projection law broken');
  if (!ok) process.exitCode = 1;
}

async function runStreamSoak() {
  const N = 5, KILL_EVERY = 500, FRAMES = 50000;
  const clock = createMockClock();
  const bc = createMockBroadcastChannel();
  const src = makeCountedStreamSource();
  let leaderTrue = 0;                                   // rotating "leader" hint (oracle only hints)
  const mkTab = (idx) => queryClient({
    crossTab: true, broadcastChannel: bc.BroadcastChannel, crossTabChannel: 'soak',
    sharedStream: true, isLeader: () => idx === leaderTrue,
    now: clock.now, setTimeout: clock.setTimeout, clearTimeout: clock.clearTimeout,
    streamIdleTimeout: 1000,
  });
  const tabs = Array.from({ length: N }, (_, i) => mkTab(i));
  const recs = tabs.map(() => []);
  const stops = tabs.map((t, i) => {
    const h = streamQuery(t, { key: ['soak'], stream: src.factory });
    const d = createRoot(() => effect(() => { const v = h.data(); if (typeof v === 'number') recs[i].push(v); }));
    return { h, d };
  });
  await soakDrain(20);

  let dupReorder = 0, maxLive = 0, drainMax = 0, killCount = 0;
  const seen = tabs.map(() => -Infinity);
  for (let f = 1; f <= FRAMES; f++) {
    src.pushAll(f);
    if ((f & 63) === 0) await soakDrain(2);
    if (src.live > maxLive) maxLive = src.live;
    if (f % KILL_EVERY === 0) {
      // kill the current leader owner: abort its connection + rotate the hint;
      // the surviving tabs' watchdogs/race converge to one connection.
      const owner = tabs.findIndex((t) => { const e = t._internal.entries.get(JSON.stringify(['soak'])); return e && e.streamOwner; });
      leaderTrue = (leaderTrue + 1) % N;
      if (owner >= 0) {
        const e = tabs[owner]._internal.entries.get(JSON.stringify(['soak']));
        if (e && e.streamStop) { try { e.streamStop(); } catch {} e.streamStop = null; e.streamOwner = false; }
      }
      killCount++;
      clock.advance(1000);                            // let the watchdog self-connect
      await soakDrain(20);
      if (src.live > drainMax) drainMax = src.live;
    }
  }
  await soakDrain(40);
  // dup/reorder detector: each tab's recorded stream must be strictly increasing.
  for (let i = 0; i < N; i++) {
    for (let j = 0; j < recs[i].length; j++) {
      if (recs[i][j] <= seen[i]) dupReorder++;
      seen[i] = recs[i][j];
    }
  }
  const drainLive = src.live;                          // connections after messages drain

  // G5: leader keeps zero per-follower state -- entries.size identical for 1 vs 4 followers.
  const g5 = (stalled) => {
    const c2 = createMockClock(); const b2 = createMockBroadcastChannel(); const s2 = makeCountedStreamSource();
    const L = queryClient({ crossTab: true, broadcastChannel: b2.BroadcastChannel, crossTabChannel: 'g5', sharedStream: true, isLeader: () => true, now: c2.now, setTimeout: c2.setTimeout, clearTimeout: c2.clearTimeout });
    const F = Array.from({ length: stalled }, () => queryClient({ crossTab: true, broadcastChannel: b2.BroadcastChannel, crossTabChannel: 'g5', sharedStream: true, isLeader: () => false, now: c2.now, setTimeout: c2.setTimeout, clearTimeout: c2.clearTimeout, streamIdleTimeout: 1e9 }));
    const lh = streamQuery(L, { key: ['g'], stream: s2.factory });
    const ld = createRoot(() => effect(() => lh.data()));
    const fds = F.map((t) => { const h = streamQuery(t, { key: ['g'], stream: s2.factory }); return createRoot(() => effect(() => h.data())); });
    return { L, F, lh, ld, fds, c2, s2 };
  };
  const g1 = g5(1); const g4 = g5(4);
  await soakDrain(20);
  for (let f = 1; f <= 50000; f++) { g1.s2.pushAll(f); g4.s2.pushAll(f); if ((f & 255) === 0) await soakDrain(1); }
  await soakDrain(20);
  const size1 = g1.L._internal.entries.size;
  const size4 = g4.L._internal.entries.size;
  g1.ld(); g4.ld(); g1.fds.forEach((d) => d()); g4.fds.forEach((d) => d());
  g1.lh.dispose(); g4.lh.dispose(); g1.L.dispose(); g4.L.dispose(); g1.F.forEach((t) => t.dispose()); g4.F.forEach((t) => t.dispose());

  // teardown the main soak; entries must drain, pool to baseline.
  stops.forEach((s) => { s.d(); s.h.dispose(); });
  await soakDrain(20);
  clock.advance(2000); await soakDrain(20);
  tabs.forEach((t) => t.dispose());
  await soakDrain(20);
  const entriesLeft = tabs.reduce((a, t) => a + t._internal.entries.size, 0);

  const ok = dupReorder === 0 && drainLive <= 1 && maxLive <= N && size1 === size4 && entriesLeft === 0;
  console.log('phase H shared-stream soak (Q8 G4/A1/A2/A4/G5; outside the frozen GATE window):');
  console.log('  tabs=' + N + ' frames=' + FRAMES + ' leader-kills=' + killCount +
    ' | dup/reorder=' + dupReorder + ' conn@drain=' + drainLive + ' maxLiveTransient=' + maxLive);
  console.log('  G5 leader entries.size 1-follower=' + size1 + ' 4-followers=' + size4 +
    ' (identical=' + (size1 === size4) + ', zero per-follower state) | entries-after-teardown=' + entriesLeft);
  console.log(ok
    ? '  PASS: one connection converged under churn, zero dup/reorder in 5 tabs, G5 prohibition holds (A1/A2/A4/G5)'
    : '  FAIL: shared-stream soak invariant broken');
  if (!ok) process.exitCode = 1;
}

// ---- OR-10 attempt E: leader teardown with live follower projection buffers -
// The carry_from_q7 attempt through the NEW Q8 surface. The honest control is a
// projection slot -- specifically the streamPromote closure, which captures the
// /stream body's scope, plus the buffer window array -- deliberately carried
// past disposeEntry with the follower still projecting. If C8's releaseProjection
// leaves either reachable, the owner-cascade / async-retention kernels trip.
// Runs AFTER the frozen gate (ON-2); NOT a gate clause (ON-3). Honest pass-or-
// fail; the outcome is recorded verbatim in INCONCLUSIVE.md as Attempt E.
async function runAttemptE() {
  const beforeSize = tracker.size();
  const CYC = 2048;
  const bc = createMockBroadcastChannel();
  let eSink = 0;
  for (let i = 0; i < CYC; i++) {
    const clock = createMockClock();
    const qc = queryClient({
      crossTab: true, broadcastChannel: bc.BroadcastChannel, crossTabChannel: 'attemptE',
      sharedStream: true, isLeader: () => false,
      now: clock.now, setTimeout: clock.setTimeout, clearTimeout: clock.clearTimeout,
      streamIdleTimeout: 1e9,
    });
    const h = streamQuery(qc, { key: ['e'], mode: 'buffer', maxBuffer: 4, stream: () => ({ async *[Symbol.asyncIterator]() {} }) });
    const stop = createRoot(() => effect(() => { const v = h.data(); eSink = Array.isArray(v) ? v.length : 0; }));
    const peer = new bc.BroadcastChannel('attemptE');
    peer.postMessage({ type: 'stream-frame', key: ['e'], epochSeq: 1, clientId: 'L', seq: 1, value: { p: i } });
    peer.postMessage({ type: 'stream-frame', key: ['e'], epochSeq: 1, clientId: 'L', seq: 2, value: { p: i } });
    await Promise.resolve(); await Promise.resolve();
    // Track the streamPromote closure (captures the /stream scope) OUTSIDE any
    // lite-signal owner; neither the tag nor the release closes over it.
    const entry = qc._internal.entries.get(JSON.stringify(['e']));
    if (entry && entry.streamPromote) tracker.track(entry.streamPromote, NOOP_RELEASE, 'stream-promote', { audit: true });
    // Tear down the follower + client WHILE it was projecting; releaseProjection
    // must null the window + promote closure at detach and disposeEntry.
    stop();
    h.dispose();
    qc.removeQueries(['e']);
    qc.dispose();
    peer.close();
  }
  globalThis.gc?.();
  await new Promise((r) => setTimeout(r, 50));
  const liveE = tracker.size() - beforeSize;
  const findingsE = tracker.audit();
  const fired = liveE !== 0 || findingsE.length !== 0;
  if (eSink === Number.MIN_SAFE_INTEGER) console.log('unreachable');
  console.log('phase H OR-10 attempt E (leader teardown w/ live follower projection; NOT a gate clause):');
  console.log('  cycles=' + CYC + ' tracked-promote live-delta=' + liveE + ' audit-findings=' +
    findingsE.length + ' -> ' + (fired ? 'FIRED' : 'DID NOT FIRE (carried; recorded verbatim in INCONCLUSIVE.md)'));
}

// ---- Q9 airplane-mode replay soak (T5/G3) ---------------------------------
// The offline queue's headline scenario, run AFTER the frozen gate (ON-2): a tab
// goes offline, enqueues N mutations, persists them, and closes; a FRESH client
// then restores the durable queue and replays it on reconnect. 200 reload cycles.
//
// GATE (the soak's real contract, T5): the queue RESTORES N and drains to 0 every
// cycle (nothing stranded across a reload) AND the retention baseline is flat
// (tracker delta 0 -- no client, entry, or record outlives its cycle). enqueue and
// replay are COLD paths that legitimately ALLOCATE their records/promises (the
// planner's own words), so the transient GC profile is REPORTED here, not gated:
// gating maxMajor 0 on 5000 cold record allocations would be a budget the path
// never promised. The zero-alloc guarantee lives on the WARM projection/read path,
// proven maxMajor 0 by the frozen phase-H gate above (byte-untouched). Prints its
// own line; a stranded item or a retention leak fails the process, never the GATE.
async function runAirplaneModeSoak() {
  const AM_CYCLES = 200;
  const N = 25;
  const beforeSize = tracker.size();
  const gc = new GcProfiler().start();
  let allDrained = true;
  let durable = null;
  for (let c = 0; c < AM_CYCLES; c++) {
    // The offline tab: seed entries, enqueue N mutations while offline, persist.
    const A = queryClient();
    const pa = persistQueryClient(A, {
      save() {}, load: () => null, version: 'v1', throttle: 0,
      queueSave: (env) => { durable = env; }, queueLoad: () => null,
    });
    await pa.queueRestored;
    for (let i = 0; i < N; i++) {
      A.setQueryData(['todo', i], { seeded: true });
      const m = mutation(A, { fn: async () => 'x', queue: true, offline: () => true, name: 'save', queueKey: ['todo', i] });
      await m.mutate({ i });
      m.dispose();
    }
    pa.stop();
    A.dispose();
    // The reload: a fresh client restores the durable queue and replays on
    // reconnect (offline() would now report false, but replay is caller-driven).
    const B = queryClient();
    const pb = persistQueryClient(B, {
      save() {}, load: () => null, version: 'v1', throttle: 0,
      queueSave: () => {}, queueLoad: () => durable,
    });
    const outcome = await pb.queueRestored;
    for (let i = 0; i < N; i++) B.setQueryData(['todo', i], { seeded: true });
    await B.replayQueue(() => async () => 'server-ok');
    if (outcome.status !== 'restored' || outcome.count !== N || B.queueSize() !== 0) allDrained = false;
    pb.stop();
    B.dispose();
    durable = null;
    if ((c & 31) === 0) gc.sampleHeap(performance.now(), process.memoryUsage().heapUsed);
  }
  globalThis.gc?.();
  await new Promise((r) => setTimeout(r, 50));
  const s = gc.summary();
  gc.stop();
  const liveDelta = tracker.size() - beforeSize;
  const findingsAM = tracker.audit();
  const ok = allDrained && liveDelta === 0 && findingsAM.length === 0;
  console.log('phase H Q9 airplane-mode replay soak (T5/G3; outside the frozen GATE window):');
  console.log('  cycles=' + AM_CYCLES + ' enqueue-per-cycle=' + N + ' | queueSize->0 every cycle=' + allDrained +
    ' pool-baseline-delta=' + liveDelta + ' audit-findings=' + findingsAM.length +
    ' | gc(reported, cold) major=' + s.gc.major + ' minor=' + s.gc.minor + ' maxMs=' + s.gc.maxMs.toFixed(2));
  console.log(ok
    ? '  PASS: every reload cycle restores N and drains to 0; retention baseline flat (T5)'
    : '  FAIL: a reload stranded an item or leaked a cycle -- see delta above');
  if (!ok) process.exitCode = 1;
}

// ---- OR-9 attempt F: replay teardown with an in-flight replay handle -------
// The carry_from_q8 findings-clause attempt (Q5 A/B, Q6 C, Q7 D, Q8 E; five
// verbatim in INCONCLUSIVE.md), now riding the queue's replay teardown (OR-9).
// The honest control: a replayQueue whose handler is still in flight when the
// CLIENT is disposed. The in-flight replay promise + its handler closure are the
// carried surface; if disposing the client mid-replay leaves them reachable, the
// async-retention kernel trips. Runs AFTER the frozen gate; NOT a gate clause.
// Honest pass-or-fail, recorded verbatim in INCONCLUSIVE.md as Attempt F.
async function runAttemptF() {
  const beforeSize = tracker.size();
  const CYC = 2048;
  let fSink = 0;
  for (let i = 0; i < CYC; i++) {
    const qc = queryClient();
    qc.setQueryData(['f'], { seeded: true });
    const m = mutation(qc, { fn: async () => 'x', queue: true, offline: () => true, name: 'save', queueKey: ['f'] });
    await m.mutate({ i });
    m.dispose();
    // Start a replay whose handler blocks on a gate (in flight), then dispose the
    // client BEFORE the handler resolves. Neither the tag nor the release closes
    // over the tracked promise.
    let releaseHandler;
    const gate = new Promise((res) => { releaseHandler = res; });
    const replayPromise = qc.replayQueue(() => async () => { await gate; return 'ok'; });
    await Promise.resolve();
    tracker.track(replayPromise, NOOP_RELEASE, 'replay-inflight', { audit: true });
    qc.dispose();                 // teardown WHILE the replay is in flight
    releaseHandler();             // handler resolves after disposal
    await replayPromise.catch(() => {});
    fSink += 1;
  }
  globalThis.gc?.();
  await new Promise((r) => setTimeout(r, 50));
  const liveF = tracker.size() - beforeSize;
  const findingsF = tracker.audit();
  const fired = liveF !== 0 || findingsF.length !== 0;
  if (fSink === Number.MIN_SAFE_INTEGER) console.log('unreachable');
  console.log('phase H OR-9 attempt F (replay teardown w/ in-flight replay handle; NOT a gate clause):');
  console.log('  cycles=' + CYC + ' tracked-replay live-delta=' + liveF + ' audit-findings=' +
    findingsF.length + ' -> ' + (fired ? 'FIRED' : 'DID NOT FIRE (carried; recorded verbatim in INCONCLUSIVE.md)'));
}

// ---- Q10 (C8): refetchInterval churn soak (T5/G3) --------------------------
// The interval scanner's headline retention gate, run AFTER the frozen gate
// (ON-2/ON-3): 10000 arm/disarm cycles under observer churn on the mock clock
// (no real sleeps), then a WARM-read window with a poll registered but not
// firing. The soak's real contract: ZERO dangling mock-clock timers at drain
// (every armed poll torn down), the retention baseline flat (tracker delta 0),
// AND the warm read path byte-identical to a non-polling query (maxMajor 0,
// < 8 B/op -- a registered interval never touches the read path). Prints its own
// line; a dangling timer / retained cycle / warm-path regression fails the
// process, never the GATE.
async function runIntervalSoak() {
  const beforeSize = tracker.size();
  const CYC = 10000;
  // Phase A: arm/disarm churn. defaultCacheTime Infinity -> no GC timer armed,
  // so the ONLY mock-clock timers are poll timers; pendingCount at drain is the
  // dangling-poll-timer count directly.
  const clock = createMockClock();
  const qc = queryClient({ now: clock.now, setTimeout: clock.setTimeout, clearTimeout: clock.clearTimeout, defaultStaleTime: 0, defaultCacheTime: Infinity });
  for (let i = 0; i < CYC; i++) {
    const q = query(qc, { key: ['iv', i & 7], fetcher: FETCHER, refetchInterval: 1000 });
    const stop = createRoot(() => effect(() => q.status()));   // attach -> register + arm
    clock.advance(1000);                                        // fire one poll tick
    await Promise.resolve();
    // QD-1: rotate the teardown path so the entry-removal disarm (clear /
    // removeQueries) stays covered as a CLASS, not just detach/dispose. Each
    // variant must leave zero dangling poll timers at the next drain check.
    const variant = i % 3;
    if (variant === 0) {
      stop();                                                   // detach -> unregister
      q.dispose();
      qc.removeQueries(['iv', i & 7], { exact: true });
    } else if (variant === 1) {
      qc.removeQueries(['iv', i & 7], { exact: true });         // remove a still-mounted polled entry
      stop();
      q.dispose();
    } else {
      qc.clear();                                               // clear with a still-mounted polled query
      stop();
      q.dispose();
    }
    await Promise.resolve();
  }
  const pendingAtDrain = clock.pendingCount;                    // must be 0: nothing armed
  const pollCountAtDrain = qc._internal.pollCount;              // 0 (list drained) -- not -1 (never used)
  qc.dispose();

  // Phase B: warm-read B/op with a poll registered but never firing. The read
  // path is the OFF-path data() (placeholderSig null), so this must match the
  // frozen phase-H warm read: maxMajor 0, ~0 B/op.
  const clock2 = createMockClock();
  const qc2 = queryClient({ now: clock2.now, setTimeout: clock2.setTimeout, clearTimeout: clock2.clearTimeout, defaultStaleTime: Infinity, defaultCacheTime: Infinity });
  const warm = query(qc2, { key: ['warm'], fetcher: FETCHER, refetchInterval: 1e9 });
  const stopW = createRoot(() => effect(() => warm.data()));
  await tick();
  let sink = 0;
  globalThis.gc?.();
  await new Promise((r) => setTimeout(r, 20));
  const before = process.memoryUsage().heapUsed;
  const gc = new GcProfiler().start();
  for (let i = 0; i < HOT; i++) {
    sink += (warm.data() | 0);
    if ((i & 8191) === 0) gc.sampleHeap(performance.now(), process.memoryUsage().heapUsed);
  }
  await new Promise((r) => setTimeout(r, 50));
  const s = gc.summary();
  gc.stop();
  globalThis.gc?.();
  await new Promise((r) => setTimeout(r, 20));
  const bpop = (process.memoryUsage().heapUsed - before) / HOT;
  stopW(); warm.dispose(); qc2.dispose();
  if (sink === Number.MIN_SAFE_INTEGER) console.log('unreachable');

  globalThis.gc?.();
  await new Promise((r) => setTimeout(r, 50));
  const liveDelta = tracker.size() - beforeSize;
  const findings = tracker.audit();
  const ok = pendingAtDrain === 0 && pollCountAtDrain === 0 && liveDelta === 0 &&
    findings.length === 0 && s.gc.major === 0 && Math.abs(bpop) < 8;
  console.log('phase H Q10 refetchInterval churn soak (C8 T5/G3; outside the frozen GATE window):');
  console.log('  cycles=' + CYC + ' | dangling-timers@drain=' + pendingAtDrain + ' pollCount@drain=' + pollCountAtDrain +
    ' retain-delta=' + liveDelta + ' audit=' + findings.length +
    ' | warm gc major=' + s.gc.major + ' minor=' + s.gc.minor + ' B/op=' + bpop.toFixed(2));
  console.log(ok
    ? '  PASS: every armed poll torn down (zero dangling), retention flat, warm read byte-identical (< 8 B/op, maxMajor 0)'
    : '  FAIL: a poll timer dangled, a cycle leaked, or the warm read regressed');
  if (!ok) process.exitCode = 1;
}

// ---- Q10 (C8): keepPreviousData placeholder-swap churn ---------------------
// 10000 reactive key swaps on a keepPreviousData query: each swap captures the
// previous entry's data into the handle-level hold, presents it while the new
// entry loads, then clears on the new value. Retention contract: dispose returns
// the placeholder signal node to the pool and drops heldData -- tracker delta 0
// across the churn (no per-swap accumulation of held payloads or signal nodes).
async function runPlaceholderSoak() {
  const beforeSize = tracker.size();
  const CYC = 10000;
  let sink = 0;
  // ONE key signal, reused across cycles (a fresh signal per cycle would never be
  // disposed and would exhaust the node pool -- the churn subject is the per-cycle
  // query + placeholder hold, not the driving signal). Three distinct key values
  // per cycle force two real entry swaps.
  const id = signal(0);
  for (let i = 0; i < CYC; i++) {
    const base = i * 3;
    id.set(base);
    const qc = queryClient({ defaultStaleTime: Infinity, defaultCacheTime: Infinity });
    const q = query(qc, { key: () => ['pp', id()], fetcher: FETCHER, keepPreviousData: true });
    const stop = createRoot(() => effect(() => { sink += (q.data() | 0) + (q.isPlaceholder() ? 1 : 0); }));
    await Promise.resolve();
    id.set(base + 1); await Promise.resolve();                  // swap -> hold previous
    id.set(base + 2); await Promise.resolve();                  // swap again -> hold, then clear
    stop();
    q.dispose();
    qc.clear();
    qc.dispose();
  }
  globalThis.gc?.();
  await new Promise((r) => setTimeout(r, 50));
  const liveDelta = tracker.size() - beforeSize;
  const findings = tracker.audit();
  if (sink === Number.MIN_SAFE_INTEGER) console.log('unreachable');
  const ok = liveDelta === 0 && findings.length === 0;
  console.log('phase H Q10 keepPreviousData placeholder-swap churn (C8 T5/G5; outside the frozen GATE window):');
  console.log('  cycles=' + CYC + ' swaps-per-cycle=2 | retain-delta=' + liveDelta + ' audit=' + findings.length);
  console.log(ok
    ? '  PASS: the placeholder hold and its signal node are released on dispose; retention baseline flat'
    : '  FAIL: a placeholder hold or its signal node outlived its query');
  if (!ok) process.exitCode = 1;
}

// ---- OR-9 attempt G: teardown with a live armed recurring poll timer -------
// The carry_from_q8 findings-clause attempt (Q5 A/B, Q6 C, Q7 D, Q8 E, Q9 F; six
// verbatim in INCONCLUSIVE.md), now riding the NEW Q10 interval surface (OR-9):
// a recurring poll timer left ARMED at teardown. The honest control: mount a
// polling query, let it arm its scanner timer, track the live entry with
// { audit: true }, then dispose the CLIENT while the timer is still armed and
// drop every local reference. If disposing the client failed to disarm the timer,
// the mock clock's pending closure would pin pollList -> the entry, and the
// owner-cascade / async-retention kernels would trip. Runs AFTER the frozen gate;
// NOT a gate clause (ON-3). Honest pass-or-fail, recorded verbatim in
// INCONCLUSIVE.md as Attempt G.
async function runAttemptG() {
  const beforeSize = tracker.size();
  const CYC = 2048;
  let gSink = 0;
  for (let i = 0; i < CYC; i++) {
    const clock = createMockClock();
    const qc = queryClient({ now: clock.now, setTimeout: clock.setTimeout, clearTimeout: clock.clearTimeout, defaultStaleTime: 0, defaultCacheTime: Infinity });
    const q = query(qc, { key: ['g'], fetcher: FETCHER, refetchInterval: 1000 });
    const stop = createRoot(() => effect(() => { gSink += q.status().length; }));
    await Promise.resolve();
    // The poll scanner is now armed on the mock clock (a live recurring timer).
    // Track the entry OUTSIDE any lite-signal owner; neither the tag nor the
    // release closes over it.
    const entry = qc._internal.entries.get(JSON.stringify(['g']));
    if (entry) tracker.track(entry, NOOP_RELEASE, 'poll-entry', { audit: true });
    // Tear down the CLIENT while the poll timer is still armed (no q.dispose /
    // stop first) -- the adversarial ordering.
    qc.dispose();
    stop();
    q.dispose();
  }
  // Multiple settle cycles (as the census does): FinalizationRegistry callbacks
  // arrive across GC rounds, so a single round can leave the last registration
  // pending and read a phantom +1.
  for (let s = 0; s < 8; s++) { globalThis.gc?.(); await new Promise((r) => setTimeout(r, 10)); }
  const liveG = tracker.size() - beforeSize;
  const findingsG = tracker.audit();
  const fired = liveG !== 0 || findingsG.length !== 0;
  if (gSink === Number.MIN_SAFE_INTEGER) console.log('unreachable');
  console.log('phase H OR-9 attempt G (client teardown w/ a live armed recurring poll timer; NOT a gate clause):');
  console.log('  cycles=' + CYC + ' tracked-entry live-delta=' + liveG + ' audit-findings=' +
    findingsG.length + ' -> ' + (fired ? 'FIRED' : 'DID NOT FIRE (carried; recorded verbatim in INCONCLUSIVE.md)'));
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
  // C11 (Q8): the 5-tab shared-stream soak + leader-failover churn + G5, all
  // AFTER the frozen gate evaluation, printing their own lines (never the GATE).
  await runStreamSoak();
  // QD-3(b): the concurrency phase that actually enters the multi-owner window.
  await runStreamConcurrencySoak();
  // C12 (OR-10): attempt E through the new follower-projection teardown surface.
  await runAttemptE();
  // Q9 (C8): the airplane-mode replay soak (a gate clause, G3) + attempt F, both
  // AFTER the frozen gate evaluation, printing their own lines (never the GATE).
  await runAirplaneModeSoak();
  await runAttemptF();
  // Q10 (C8): the refetchInterval churn soak + keepPreviousData placeholder-swap
  // churn (gate clauses, T5/G3/G5) + attempt G, all AFTER the frozen gate
  // evaluation, printing their own lines (never the GATE).
  await runIntervalSoak();
  await runPlaceholderSoak();
  await runAttemptG();
}
