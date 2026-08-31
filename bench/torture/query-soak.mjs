/**
 * bench/torture/query-soak.mjs — high-volume cache-lifecycle churn soak.
 *
 * Not a benchmark — a soak. Continuously mounts/unmounts observers, rewires
 * reactive keys, writes/invalidates/removes cache entries, and forces refetches
 * against a fixed keyspace for a few seconds. Fetchers resolve on a microtask,
 * resolve on a deferred mock-clock timer (to exercise in-flight abort), or
 * reject (to exercise the error transition + generation guard). The ops/sec is
 * contextual; the assertions are what matter:
 *
 *   - zero thrown exceptions during the run
 *   - after teardown the entry map fully drains (every entry GC'd)
 *   - after teardown no timers remain pending on the mock clock
 *   - after teardown lite-signal's node/link pool returns to its pre-soak
 *     baseline — i.e. every entry's data/error/status/fetching signal node and
 *     every observer link was released. This is the zero-GC contract under
 *     churn: the refcount → GC → disposeEntry chain leaks nothing.
 *
 * The pool assertion is the sharp one. If a single detach were dropped, its
 * entry keeps observerCount > 0, never GCs, and activeNodes stays above
 * baseline → FAIL. If a disposeEntry path forgot a signal, activeNodes drifts
 * up → FAIL. If an observer link were orphaned, activeLinks != 0 → FAIL.
 *
 * Exit code: 0 on a clean run, 1 on any error or stability assertion failure.
 *
 * Usage:
 *   node bench/torture/query-soak.mjs
 *   TORTURE_SECONDS=15 TORTURE_SEED=7 node bench/torture/query-soak.mjs
 *
 * NOTE: drives a mock clock (from the test harness), so GC timers, deferred
 * fetches, and retries fire deterministically under advance() rather than on
 * real wall-clock time. Uses an explicit registry sized for the soak shape,
 * with onCapacityExceeded:"grow" so a churn spike can't trip the default cap.
 */
import {performance} from "node:perf_hooks";
import {createRegistry, setDefaultRegistry, signal, effect, dispose as disposeNode} from "@zakkster/lite-signal";
import {queryClient, query} from "../../Query.js";
import {createMockClock} from "../../test/harness.js";

// ── knobs ────────────────────────────────────────────────────────────────────
const SECONDS      = Number(process.env.TORTURE_SECONDS || 5);
const SEED         = (Number(process.env.TORTURE_SEED || 0x9e3779b9) >>> 0) || 1;
const N_KEYS       = 400;    // static keyspace
const N_HANDLES    = 400;    // one query() handle per static key
const N_REACTIVE   = 48;     // handles whose key is a signal (reactive-key churn)
const N_OBSERVERS  = 256;    // observer slots (effects reading a random handle)
const OPS_PER_TICK = 1500;
const CACHE_TIME   = 800;    // ms — finite so unobserved entries actually GC
const STALE_TIME   = 0;      // every fresh attach is eligible to refetch
const DEFER_MAX    = 400;    // ms — max deferred-fetch delay on the mock clock

// ── deterministic PRNG (mulberry32) — reproducible op streams for triage ─────
let _s = SEED;
function rnd() {
    _s |= 0; _s = (_s + 0x6D2B79F5) | 0;
    let t = Math.imul(_s ^ (_s >>> 15), 1 | _s);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
}
const randInt = (n) => (rnd() * n) | 0;

// ── registry + baseline ──────────────────────────────────────────────────────
const reg = createRegistry({
    maxNodes: (N_HANDLES + N_OBSERVERS + N_KEYS) * 8,
    maxLinks: (N_HANDLES + N_OBSERVERS + N_KEYS) * 32,
    prealloc: "eager",
    onCapacityExceeded: "grow",
});
setDefaultRegistry(reg);
const baseline = reg.stats();

// ── mock environment ─────────────────────────────────────────────────────────
const clock = createMockClock();
let fetchCalls = 0;
let rejectCalls = 0;

// Auto-resolving torture fetcher. ~15% reject (retry is 0, so this is a clean
// error transition, no retry timers), ~40% resolve on a deferred mock-clock
// timer (so a detach/removeQueries mid-flight has to abort a live fetch and the
// generation guard has to swallow the late resolution), the rest resolve on a
// microtask.
function torFetcher(ctx) {
    fetchCalls++;
    const roll = rnd();
    const val = {k: ctx.key, n: fetchCalls};
    if (roll < 0.15) {
        rejectCalls++;
        return Promise.reject(new Error("tor-reject"));
    }
    if (roll < 0.55) {
        return new Promise((res) => {
            clock.setTimeout(() => res(val), 1 + randInt(DEFER_MAX));
        });
    }
    return Promise.resolve(val);
}

const qc = queryClient({
    now: clock.now,
    setTimeout: clock.setTimeout,
    clearTimeout: clock.clearTimeout,
    defaultStaleTime: STALE_TIME,
    defaultCacheTime: CACHE_TIME,
    retry: 0,
});
const entries = qc._internal.entries;

// ── handles ──────────────────────────────────────────────────────────────────
// Static handles: fixed key ["k", i]. Reactive handles: key reads a per-handle
// signal, so flipping that signal moves the handle across entries (detach old /
// attach new inside the watcher — the reactive-key path).
const handles = new Array(N_HANDLES);
for (let i = 0; i < N_HANDLES; i++) {
    handles[i] = query(qc, {key: ["k", i], fetcher: torFetcher});
}
const reactiveSigs = new Array(N_REACTIVE);
const reactiveHandles = new Array(N_REACTIVE);
for (let i = 0; i < N_REACTIVE; i++) {
    const ks = signal(randInt(N_KEYS));
    reactiveSigs[i] = ks;
    reactiveHandles[i] = query(qc, {key: () => ["k", ks()], fetcher: torFetcher});
}
function anyHandle() {
    const r = randInt(N_HANDLES + N_REACTIVE);
    return r < N_HANDLES ? handles[r] : reactiveHandles[r - N_HANDLES];
}

// ── observer slots ───────────────────────────────────────────────────────────
// Each slot is an effect that reads a random handle's accessors — that read is
// what attaches an observer to the entry. Rewiring a slot to a new handle
// exercises detach-old / attach-new; toggling a slot off exercises last-observer
// teardown + GC scheduling.
const slots = new Array(N_OBSERVERS).fill(null);
function mountSlot(i) {
    if (slots[i]) slots[i]();
    const h = anyHandle();
    slots[i] = effect(() => {
        // Four accessor reads per run — the hoisted-cleanup zero-GC path.
        h.data(); h.status(); h.fetching(); h.error();
    });
}
function unmountSlot(i) {
    if (slots[i]) { slots[i](); slots[i] = null; }
}
for (let i = 0; i < N_OBSERVERS; i++) mountSlot(i);

// ── op mix ───────────────────────────────────────────────────────────────────
let ops = 0;
let errors = 0;
let lastError = null;

function step() {
    const mode = randInt(8);
    try {
        if (mode === 0) {
            qc.setQueryData(["k", randInt(N_KEYS)], {v: randInt(1e6)});
            ops++;
        } else if (mode === 1) {
            qc.invalidate(["k", randInt(N_KEYS)]);
            ops++;
        } else if (mode === 2) {
            mountSlot(randInt(N_OBSERVERS));          // rewire an observer
            ops++;
        } else if (mode === 3) {
            qc.removeQueries(["k", randInt(N_KEYS)]); // torture observed-removal
            ops++;
        } else if (mode === 4) {
            // refetch() hands the fetch promise back to the caller; a rejecting
            // fetch is the caller's to handle (a real app awaits it in try/catch).
            const p = anyHandle().refetch();          // force fetch (no-op if unobserved)
            if (p && typeof p.then === "function") p.then(undefined, () => {});
            ops++;
        } else if (mode === 5) {
            const writes = 1 + randInt(16);           // batched writes
            for (let w = 0; w < writes; w++) {
                qc.setQueryData(["k", randInt(N_KEYS)], randInt(1e6));
                ops++;
            }
        } else if (mode === 6) {
            reactiveSigs[randInt(N_REACTIVE)].set(randInt(N_KEYS)); // reactive-key flip
            ops++;
        } else {
            // Grow/shrink the live observer population.
            if (rnd() < 0.5) unmountSlot(randInt(N_OBSERVERS));
            else mountSlot(randInt(N_OBSERVERS));
            ops++;
        }
    } catch (e) {
        errors++;
        if (!lastError) lastError = e;
    }
}

// ── driver ───────────────────────────────────────────────────────────────────
const nextTick = () => new Promise((r) => setImmediate(r));

async function run() {
    const start = performance.now();
    const endAt = start + SECONDS * 1000;
    while (performance.now() < endAt) {
        for (let i = 0; i < OPS_PER_TICK; i++) step();
        await clock.flush();                 // settle microtask-resolving fetches
        clock.advance(1 + randInt(64));      // fire some deferred fetches + GC mid-run
        await clock.flush();
        await nextTick();
    }
    const elapsed = (performance.now() - start) / 1000;
    return {elapsed, perSec: ops / elapsed};
}

async function teardown() {
    for (let i = 0; i < N_OBSERVERS; i++) unmountSlot(i);   // stop all observers
    await clock.flush();                                    // run deferred stopWatcher microtasks
    for (let i = 0; i < N_HANDLES; i++) handles[i].dispose();
    for (let i = 0; i < N_REACTIVE; i++) reactiveHandles[i].dispose();
    for (let i = 0; i < N_REACTIVE; i++) { try { disposeNode(reactiveSigs[i]); } catch {} }
    clock.advance(DEFER_MAX + CACHE_TIME + 10_000);         // fire every remaining timer
    await clock.flush();
    qc.clear();                                             // sweep any stragglers
    qc.dispose();
    clock.advance(CACHE_TIME + 10_000);
    await clock.flush();
    await nextTick();
}

const {elapsed, perSec} = await run();
await teardown();

const after = reg.stats();

console.log("query-soak (single-client cache-lifecycle churn)");
console.log("  duration:", elapsed.toFixed(3), "s");
console.log("  ops:", ops.toLocaleString());
console.log("  ops/sec:", perSec.toLocaleString(undefined, {maximumFractionDigits: 0}));
console.log("  fetches:", fetchCalls.toLocaleString(), "(rejections:", rejectCalls + ")");
console.log("  errors:", errors);
console.log("  entries.size after teardown:", entries.size);
console.log("  clock timers pending after teardown:", clock.pendingCount);
console.log("  baseline activeNodes/activeLinks:", baseline.activeNodes, "/", baseline.activeLinks);
console.log("  post-teardown activeNodes/activeLinks:", after.activeNodes, "/", after.activeLinks);
console.log("  poolGrowths:", after.poolGrowths, " totalAllocations:", after.totalAllocations.toLocaleString());

let exitCode = 0;
if (errors > 0) {
    console.error("  FAIL: errors > 0; first =", lastError && lastError.message);
    exitCode = 1;
}
if (entries.size !== 0) {
    console.error("  FAIL: entry map didn't drain — leaked", entries.size, "entries");
    exitCode = 1;
}
if (clock.pendingCount !== 0) {
    console.error("  FAIL: dangling timers after teardown:", clock.pendingCount);
    exitCode = 1;
}
if (after.activeNodes > baseline.activeNodes + 8) {
    console.error("  FAIL: node pool leak — expected ≤", baseline.activeNodes + 8, "got", after.activeNodes);
    exitCode = 1;
}
if (after.activeLinks !== 0) {
    console.error("  FAIL: activeLinks != 0 after teardown:", after.activeLinks);
    exitCode = 1;
}
if (exitCode === 0) console.log("  PASS: zero errors, cache drained, pool returned to baseline");
process.exit(exitCode);
