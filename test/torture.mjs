// test/torture.mjs -- node --expose-gc test/torture.mjs
//
// Retention + GC-budget torture for @zakkster/lite-query, wired to the real
// entry points: query() (warm-read hot path) and streamQuery() in buffer mode
// (the surface the C4 collapse touched -- onValue-driven status/count and the
// live droppedCount getter). The package's broader soak gate is
// `npm run torture` (bench/torture/*.mjs, lite-signal pool introspection);
// this harness adds the lite-leak / lite-gc-profiler retention + budget proof.
//
// The lite-leak / lite-gc-profiler deps are dev-only diagnostics (installed
// --no-save for this run); nothing here ships (test/ is not in files[]).

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

// ---- phase 1: retention torture ------------------------------------------
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
  if (qHandle) { qHandle.dispose(); qHandle = null; }
  if (sHandle) { sHandle.dispose(); sHandle = null; }
  qc.clear();                                        // disposeEntry -> release entry signal nodes
}

globalThis.gc?.();
await new Promise((r) => setTimeout(r, 50));

const live = tracker.size();
const findings = tracker.audit();

// ---- phase 2: allocation + GC torture ------------------------------------
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
const report = checkNoGc(s, { maxMajor: 0, maxPauseMs: 4 });
gc.stop();
stopWarm(); stopBuf(); warm.dispose(); buf.dispose();
if (sink === Number.MIN_SAFE_INTEGER) console.log('unreachable');   // keep sink live

const ok = report.ok && live === 0 && leaks.length === 0 && findings.length === 0;
console.log(
  'GATE leak=size ' + live + '/0 findings=' + findings.length +
  ' warnings=' + warns.length +
  ' | gc major=' + s.gc.major + ' minor=' + s.gc.minor +
  ' maxMs=' + s.gc.maxMs.toFixed(2) +
  ' | ' + (ok ? 'ok' : 'FAIL')
);
if (!ok) {
  for (const v of report.violations) {
    console.error('  violation ' + v.metric + ' limit=' + v.limit + ' actual=' + v.actual);
  }
  for (const f of findings) console.error('  finding ' + f.kind + ':' + f.reason);
  for (const l of leaks) console.error('  leak ' + l);
  process.exitCode = 1;
}
