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

// -- feed assertion mode (A-5) ------------------------------------------------
// The observed qc.inspect() stream must form a CONSISTENT lifecycle balance and
// cover every class the fuzzer provokes (G4). ON by default; TORTURE_FEED=0
// disables. The C-feed control (QUERY_TORTURE_BREAK=feed) skips exactly ONE
// entry:attach bookkeeping update -> a guaranteed detach-without-attach.
//
// The gated invariants are BALANCES, not per-keyHash sequence epochs (QD-2). A
// keyHash is NOT a stable identity under this fuzzer: it deliberately
// removeQueries()es OBSERVED entries and then restart()/refetch()es the now-
// stranded handles, so a removed-and-disposed entry OBJECT and a freshly-created
// one legitimately emit on the SAME keyHash at the same time. A per-keyHash
// SEQUENCE machine cannot separate those instances and would false-flag -- and
// the 10-key vocabulary is frozen, so no entry-identity discriminator can be
// added. Hence the sequence-order rules (create-over-live, event-after-remove,
// status-from-mismatch, the per-hash stream ladder) STAY DROPPED (operator ruling
// QD-2): they are unresolvable without entry identity.
//
// What DOES hold under aliasing is additive balance -- the sum of two balanced
// streams is balanced -- so these are gated:
//   1. attach/detach: cumulative entry:attach - entry:detach == the sum of live
//      observerCounts, >= 0 at every instant (each event fires AFTER its ++/--
//      commit, sites 2/3, once per transition). A detach below zero is
//      detach-without-attach -- the C-feed control's signature.
//   2. fetch dispatch/resolve: after teardown drains, fetch:dispatch count ==
//      fetch:settle + fetch:abort count, PER-HASH and GLOBAL, strict equality.
//      Each dispatched controller resolves EXACTLY once (settle OR the first
//      abort); QD-2's `was = signal.aborted` guard makes a re-aborted controller
//      emit no second fetch:abort, so the balance is exact. (No verified
//      counter-example forces a tolerance; if one ever does it is recorded here
//      verbatim, never silently loosened.)
// tab:send/receive are counted and bounded (receives <= sends * (tabs-1), else an
// echo loop shows as receive-overflow). shared:*/mutation:*/persist:* are not
// fuzz-reachable here (sharedFetch off, no mutations/persisters) -- excluded from
// the coverage set (G4).
const FEED_ASSERT = process.env.TORTURE_FEED !== "0";
const FEED_BREAK = process.env.QUERY_TORTURE_BREAK === "feed";
const feedViolations = [];
const feedCoverage = new Set();
let tabSends = 0, tabReceives = 0;
let feedBreakArmed = FEED_BREAK;
let atCount = 0, dtCount = 0;          // cumulative attach / detach (global, >= 0)
let dispatchTotal = 0, resolveTotal = 0;   // cumulative fetch dispatch / (settle+abort)
const fetchByHash = new Map();        // keyHash -> { dispatch, resolve }
const EXPECTED_TYPES = [
    "entry:create", "entry:attach", "entry:detach", "entry:gc", "entry:remove",
    "entry:status", "entry:stale", "fetch:dispatch", "fetch:settle", "fetch:abort",
    "tab:send", "tab:receive", "stream:start", "stream:value",
];
function feedViolate(rule, keyHash, detail) {
    if (feedViolations.length < 10000) feedViolations.push({ rule, keyHash, detail: detail || null });
}
function fetchSlot(kh) {
    let f = fetchByHash.get(kh);
    if (f === undefined) { f = { dispatch: 0, resolve: 0 }; fetchByHash.set(kh, f); }
    return f;
}
function feedStep(e) {
    const t = e.type;
    feedCoverage.add(t);
    switch (t) {
        case "tab:send": tabSends++; break;
        case "tab:receive": tabReceives++; break;
        case "entry:attach":
            if (feedBreakArmed) { feedBreakArmed = false; break; }   // C-feed: drop one attach
            atCount++;
            break;
        case "entry:detach":
            dtCount++;
            if (dtCount > atCount) feedViolate("detach-without-attach", e.keyHash);
            break;
        case "fetch:dispatch":
            dispatchTotal++;
            fetchSlot(e.keyHash).dispatch++;
            break;
        case "fetch:settle":
        case "fetch:abort":
            resolveTotal++;
            fetchSlot(e.keyHash).resolve++;
            break;
    }
}

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

// Install the feed observers BEFORE any entry is created, so no lifecycle event
// escapes the running balances. Both tabs feed the same global counters.
if (FEED_ASSERT) {
    tabA.inspect(feedStep);
    tabB.inspect(feedStep);
}

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

// -- feed state-machine + coverage gate (A-5 / G4) ---------------------------
if (FEED_ASSERT) {
    for (const type of EXPECTED_TYPES) {
        if (!feedCoverage.has(type)) feedViolate("uncovered-event-class", null, type);
    }
    // Echo bound: with N tabs a locally-issued mutation reaches at most N-1 peers.
    if (tabReceives > tabSends * (tabs.length - 1)) {
        feedViolate("receive-overflow", null, "recv " + tabReceives + " sends " + tabSends);
    }
    // Fetch dispatch/resolve balance (QD-2), evaluated after the teardown drain so
    // every in-flight fetch has settled or aborted: global AND per-hash strict
    // equality. Each dispatched controller resolves exactly once.
    if (dispatchTotal !== resolveTotal) {
        feedViolate("fetch-balance-global", null, "dispatch " + dispatchTotal + " resolve " + resolveTotal);
    }
    for (const [kh, f] of fetchByHash) {
        if (f.dispatch !== f.resolve) {
            feedViolate("fetch-balance-hash", kh, "dispatch " + f.dispatch + " resolve " + f.resolve);
        }
    }
    console.log("  feed classes covered:", feedCoverage.size, " tab send/recv:", tabSends, "/", tabReceives,
        " fetch d/r:", dispatchTotal, "/", resolveTotal);
    if (feedViolations.length > 0) {
        const shown = Math.min(10, feedViolations.length);
        for (let i = 0; i < shown; i++) {
            const v = feedViolations[i];
            console.error("  feed violation " + v.rule + " keyHash=" + v.keyHash + (v.detail ? " " + v.detail : ""));
        }
        console.error("FAIL: feed state machine -- " + feedViolations.length + " violations");
        exitCode = 1;
    } else {
        console.log("  PASS: feed state machine consistent across " + feedCoverage.size + " event classes");
    }
}

process.exit(exitCode);
