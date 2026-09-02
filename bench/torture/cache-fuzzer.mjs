/**
 * bench/torture/cache-fuzzer.mjs -- two-tab cross-tab coherence fuzzer.
 *
 * Not a benchmark -- a CRASH + COHERENCE soak. Two queryClients ("tabs") share
 * one mock BroadcastChannel with crossTab:true, and both tabs fuzz the same
 * cache concurrently: setQueryData / invalidate / removeQueries / clear, plus
 * observed queries and streamQueries that mount, restart, and tear down under
 * the churn. The ops/sec is contextual; the assertions are what matter:
 *
 *   - zero thrown exceptions across both tabs
 *   - NO ECHO STORM. Every locally-issued mutation broadcasts exactly once; a
 *     remotely-applied mutation broadcasts zero times (the processingRemote
 *     guard suppresses the echo). So total postMessages MUST equal the number
 *     of mutations we issued. An echo-loop regression makes this strictly
 *     greater -- and, being self-amplifying, runs away fast.
 *   - CONVERGENCE. After the fuzz, tab A writes a sentinel to each "sync" key;
 *     once messages drain, BOTH tabs read that sentinel back. Cross-tab
 *     propagation of writes still works after all the churn.
 *   - the entry map on both tabs fully drains after teardown, no timers dangle,
 *     and the shared lite-signal pool returns to its pre-fuzz baseline (streams
 *     aborted, every entry's signal nodes released).
 *
 * Exit code: 0 on a clean run, 1 on any error or assertion failure.
 *
 * Usage:
 *   node bench/torture/cache-fuzzer.mjs
 *   TORTURE_SECONDS=15 TORTURE_SEED=3 node bench/torture/cache-fuzzer.mjs
 *
 * Cross-tab delivery is via the harness mock channel (queueMicrotask, matching
 * the spec's async delivery), so the fuzzer drains with microtask flushes; the
 * mock clock is advanced only to age out work-key GC. sharedFetch is OFF here
 * (its fetch-req broadcasts are a separate protocol tortured in
 * shared-fetch-soak.mjs) so the echo count stays exact.
 */
import {performance} from "node:perf_hooks";
import {createRegistry, setDefaultRegistry, effect} from "@zakkster/lite-signal";
import {queryClient, query} from "../../Query.js";
import {streamQuery} from "../../StreamQuery.js";
import {createMockClock, createMockBroadcastChannel} from "../../test/harness.js";

// -- knobs --------------------------------------------------------------------
const SECONDS      = Number(process.env.TORTURE_SECONDS || 5);
const SEED         = (Number(process.env.TORTURE_SEED || 0x1234567) >>> 0) || 1;
const N_WORK       = 160;   // shared work keyspace (both tabs mutate)
const N_SYNC       = 24;    // sync keyspace (only tab A writes; used for convergence)
const N_OBS_PER    = 48;    // observer slots per tab
const N_STREAM_PER = 4;     // stream handles per tab
const OPS_PER_TICK = 900;
const CACHE_TIME   = 600;
// Phase-C control hook (test/torture.mjs C-fuzz): when set, inject ONE phantom
// re-broadcast into the echo-oracle snapshot below -- exactly the echo-storm
// regression the invariant exists to catch. Unset on every plain run.
const BREAK = process.env.QUERY_TORTURE_BREAK === "1" || process.env.QUERY_TORTURE_BREAK === "fuzz";

// -- deterministic PRNG -------------------------------------------------------
let _s = SEED;
function rnd() {
    _s |= 0; _s = (_s + 0x6D2B79F5) | 0;
    let t = Math.imul(_s ^ (_s >>> 15), 1 | _s);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
}
const randInt = (n) => (rnd() * n) | 0;

// -- registry + baseline ------------------------------------------------------
const reg = createRegistry({
    maxNodes: (N_WORK + N_SYNC + (N_OBS_PER + N_STREAM_PER) * 2) * 16,
    maxLinks: (N_WORK + N_SYNC + (N_OBS_PER + N_STREAM_PER) * 2) * 48,
    prealloc: "eager",
    onCapacityExceeded: "grow",
});
setDefaultRegistry(reg);
const baseline = reg.stats();

// -- shared mock channel, instrumented to count every real broadcast ----------
const {BroadcastChannel: BaseBC} = createMockBroadcastChannel();
let broadcasts = 0;
class CountingBC extends BaseBC {
    postMessage(data) { broadcasts++; return super.postMessage(data); }
}

const clock = createMockClock();
function makeTab() {
    return queryClient({
        now: clock.now,
        setTimeout: clock.setTimeout,
        clearTimeout: clock.clearTimeout,
        broadcastChannel: CountingBC,
        crossTab: true,
        crossTabChannel: "tor",
        defaultStaleTime: 0,
        defaultCacheTime: CACHE_TIME,
        retry: 0,
    });
}
const tabA = makeTab();
const tabB = makeTab();
const tabs = [tabA, tabB];

// -- fetchers + stream source -------------------------------------------------
// Each tab fetches independently (results do NOT cross tabs -- by design).
let fetchCalls = 0;
function torFetcher(ctx) {
    fetchCalls++;
    return Promise.resolve({k: ctx.key, n: fetchCalls});
}
// Bounded, abort-aware async source: yields a few values over microtasks then
// completes (status -> success). Aborts promptly on detach / restart / removal.
function makeStreamSource() {
    const n = 2 + randInt(6);
    return async function* torStream({signal}) {
        for (let i = 0; i < n; i++) {
            if (signal && signal.aborted) return;
            await Promise.resolve();
            if (signal && signal.aborted) return;
            yield {i};
        }
    };
}

// -- per-tab handles + observer slots -----------------------------------------
function tabState(qc) {
    const handles = new Array(N_WORK);
    for (let i = 0; i < N_WORK; i++) handles[i] = query(qc, {key: ["w", i], fetcher: torFetcher});
    const streams = new Array(N_STREAM_PER);
    for (let i = 0; i < N_STREAM_PER; i++) {
        streams[i] = streamQuery(qc, {key: ["w", randInt(N_WORK)], stream: makeStreamSource(), mode: "latest"});
    }
    const slots = new Array(N_OBS_PER).fill(null);
    return {qc, handles, streams, slots};
}
const A = tabState(tabA);
const B = tabState(tabB);

function mountObs(st, i) {
    if (st.slots[i]) st.slots[i]();
    const useStream = rnd() < 0.25;
    const h = useStream ? st.streams[randInt(N_STREAM_PER)] : st.handles[randInt(N_WORK)];
    st.slots[i] = effect(() => {
        h.data(); h.status();
        if (useStream) { h.done(); h.count(); } else { h.fetching(); h.error(); }
    });
}
function unmountObs(st, i) { if (st.slots[i]) { st.slots[i](); st.slots[i] = null; } }
for (const st of [A, B]) for (let i = 0; i < N_OBS_PER; i++) mountObs(st, i);

// -- op mix -------------------------------------------------------------------
let ops = 0;
let localMutations = 0;   // setData + invalidate + remove + clear we ISSUE (each broadcasts once)
let errors = 0;
let lastError = null;
const stOf = (qc) => (qc === tabA ? A : B);

function step() {
    const qc = tabs[randInt(2)];
    const st = stOf(qc);
    const mode = randInt(9);
    try {
        if (mode === 0) {
            qc.setQueryData(["w", randInt(N_WORK)], {v: randInt(1e6)});
            localMutations++; ops++;
        } else if (mode === 1) {
            qc.invalidate(["w", randInt(N_WORK)]);
            localMutations++; ops++;
        } else if (mode === 2) {
            qc.removeQueries(["w", randInt(N_WORK)]);
            localMutations++; ops++;
        } else if (mode === 3) {
            mountObs(st, randInt(N_OBS_PER));
            ops++;
        } else if (mode === 4) {
            unmountObs(st, randInt(N_OBS_PER));
            ops++;
        } else if (mode === 5) {
            st.streams[randInt(N_STREAM_PER)].restart();   // abort + re-establish a stream
            ops++;
        } else if (mode === 6) {
            // Only tab A owns the sync keyspace (single writer keeps convergence
            // unambiguous); tab B spends this op on the work space instead.
            if (qc === tabA) { qc.setQueryData(["s", randInt(N_SYNC)], randInt(1e6)); localMutations++; }
            else             { qc.invalidate(["w", randInt(N_WORK)]); localMutations++; }
            ops++;
        } else if (mode === 7) {
            const p = st.handles[randInt(N_WORK)].refetch();
            if (p && typeof p.then === "function") p.then(undefined, () => {});
            ops++;
        } else {
            if (rnd() < 0.04) { qc.clear(); localMutations++; }   // rare big hammer
            ops++;
        }
    } catch (e) {
        errors++;
        if (!lastError) lastError = e;
    }
}

// -- driver -------------------------------------------------------------------
const nextTick = () => new Promise((r) => setImmediate(r));
async function drain(rounds = 24) { for (let i = 0; i < rounds; i++) await Promise.resolve(); }

async function run() {
    const start = performance.now();
    const endAt = start + SECONDS * 1000;
    while (performance.now() < endAt) {
        for (let i = 0; i < OPS_PER_TICK; i++) step();
        await drain();                    // deliver cross-tab messages + settle fetches/streams
        clock.advance(1 + randInt(48));   // age out some work-key GC mid-run
        await drain();
        await nextTick();
    }
    return (performance.now() - start) / 1000;
}

// -- convergence probe: tab A stamps a sentinel per sync key, both tabs must see it
const SENTINEL = 0xC0FFEE;
async function convergeCheck() {
    for (let j = 0; j < N_SYNC; j++) { tabA.setQueryData(["s", j], SENTINEL); localMutations++; }
    await drain(64);                      // NB: no clock.advance -- sync entries must not GC before we read
    let mismatches = 0;
    for (let j = 0; j < N_SYNC; j++) {
        if (tabA.getQueryData(["s", j]) !== SENTINEL) mismatches++;
        if (tabB.getQueryData(["s", j]) !== SENTINEL) mismatches++;
    }
    return mismatches;
}

async function teardown() {
    for (const st of [A, B]) {
        for (let i = 0; i < N_OBS_PER; i++) unmountObs(st, i);
    }
    await drain(64);                                  // deferred stopWatcher microtasks
    for (const st of [A, B]) {
        for (let i = 0; i < N_WORK; i++) st.handles[i].dispose();
        for (let i = 0; i < N_STREAM_PER; i++) st.streams[i].dispose();
    }
    clock.advance(CACHE_TIME + 10_000);               // fire all GC timers
    await drain(64);
    tabA.dispose();
    tabB.dispose();
    clock.advance(CACHE_TIME + 10_000);
    await drain(64);
    await nextTick();
}

const elapsed = await run();
const mismatches = await convergeCheck();
// Snapshot before teardown: qc.dispose() calls clear(), which legitimately
// broadcasts one {clear} per tab -- not an echo, so it's outside the invariant.
const broadcastsDuringFuzz = broadcasts + (BREAK ? 1 : 0);
await teardown();

const after = reg.stats();
const entriesA = tabA._internal.entries.size;
const entriesB = tabB._internal.entries.size;

console.log("cache-fuzzer (two-tab cross-tab coherence)");
console.log("  duration:", elapsed.toFixed(3), "s");
console.log("  ops:", ops.toLocaleString());
console.log("  ops/sec:", (ops / elapsed).toLocaleString(undefined, {maximumFractionDigits: 0}));
console.log("  fetches:", fetchCalls.toLocaleString());
console.log("  local mutations issued:", localMutations.toLocaleString());
console.log("  broadcasts during fuzz:", broadcastsDuringFuzz.toLocaleString());
console.log("  sync-key mismatches after converge:", mismatches);
console.log("  errors:", errors);
console.log("  entries.size after teardown  A/B:", entriesA, "/", entriesB);
console.log("  clock timers pending after teardown:", clock.pendingCount);
console.log("  baseline activeNodes/activeLinks:", baseline.activeNodes, "/", baseline.activeLinks);
console.log("  post-teardown activeNodes/activeLinks:", after.activeNodes, "/", after.activeLinks);
console.log("  poolGrowths:", after.poolGrowths, " totalAllocations:", after.totalAllocations.toLocaleString());

let exitCode = 0;
if (errors > 0) {
    console.error("  FAIL: errors > 0; first =", lastError && lastError.message);
    exitCode = 1;
}
if (broadcastsDuringFuzz !== localMutations) {
    console.error("  FAIL: echo storm -- expected", localMutations, "broadcasts, got", broadcastsDuringFuzz,
        broadcastsDuringFuzz > localMutations ? "(re-broadcast leak)" : "(dropped a broadcast)");
    exitCode = 1;
}
if (mismatches !== 0) {
    console.error("  FAIL: cross-tab convergence -- ", mismatches, "sync reads didn't match the sentinel");
    exitCode = 1;
}
if (entriesA !== 0 || entriesB !== 0) {
    console.error("  FAIL: entry maps didn't drain -- A:", entriesA, "B:", entriesB);
    exitCode = 1;
}
if (clock.pendingCount !== 0) {
    console.error("  FAIL: dangling timers after teardown:", clock.pendingCount);
    exitCode = 1;
}
if (after.activeNodes > baseline.activeNodes + 8) {
    console.error("  FAIL: node pool leak -- expected <=", baseline.activeNodes + 8, "got", after.activeNodes);
    exitCode = 1;
}
if (after.activeLinks !== 0) {
    console.error("  FAIL: activeLinks != 0 after teardown:", after.activeLinks);
    exitCode = 1;
}
if (exitCode === 0) console.log("  PASS: no echo storm, tabs converged, caches drained, pool at baseline");
process.exit(exitCode);
