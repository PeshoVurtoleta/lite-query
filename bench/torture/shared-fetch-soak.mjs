/**
 * bench/torture/shared-fetch-soak.mjs -- leader/follower shared-fetch dedup soak.
 *
 * Not a benchmark -- a coherence soak for the cross-tab fetch-dedup protocol.
 * One leader tab and four follower tabs share a mock BroadcastChannel with
 * sharedFetch:true. Followers mount/unmount observers on shared keys while the
 * leader invalidates and writes them, for a few seconds. The ops/sec is
 * contextual; the assertions are what matter:
 *
 *   - zero thrown exceptions across all five tabs
 *   - THE DEDUP INVARIANT: follower tabs issue ZERO local fetches for shared
 *     keys. A follower needing data broadcasts a fetch-req and defers to the
 *     leader; as long as the leader answers before the fallback timer fires
 *     (it always does -- the leader resolves on a microtask, the fallback is a
 *     mock-clock timer we never advance past mid-run), the follower's own
 *     fetcher is never called. Each follower fetcher is a spy; the sum of their
 *     call counts must be 0.
 *   - LIVENESS: the leader actually fetched (leaderFetches > 0), and a follower
 *     ends up holding the leader's fetched value for a probe key it never
 *     fetched itself -- proving the result travelled leader -> follower over the
 *     channel, not via a local network call.
 *   - every tab's entry map drains after teardown, no timers dangle, and the
 *     shared lite-signal pool returns to its pre-soak baseline.
 *
 * Exit code: 0 on a clean run, 1 on any error or assertion failure.
 *
 * Usage:
 *   node bench/torture/shared-fetch-soak.mjs
 *   TORTURE_SECONDS=15 TORTURE_SEED=5 FOLLOWERS=8 node bench/torture/shared-fetch-soak.mjs
 *
 * The leader keeps a permanent observer on every shared key, so a follower's
 * fetch-req always finds a live leader entry to fulfill. sharedFetchTimeout is
 * set far beyond any mid-run clock advance, so a follower fallback self-fetch
 * (the one path that would break the invariant) is structurally impossible
 * until teardown -- by which point all observers are detached and their fallback
 * timers cleared.
 */
import {performance} from "node:perf_hooks";
import {createRegistry, setDefaultRegistry, effect} from "@zakkster/lite-signal";
import {queryClient, query, infiniteQuery} from "../../Query.js";
import {createMockClock, createMockBroadcastChannel} from "../../test/harness.js";

// -- knobs --------------------------------------------------------------------
const SECONDS       = Number(process.env.TORTURE_SECONDS || 5);
const SEED          = (Number(process.env.TORTURE_SEED || 0xABCDEF) >>> 0) || 1;
const N_FOLLOWERS   = Number(process.env.FOLLOWERS || 4);
const N_SHARED      = 96;    // shared keyspace, all observed by the leader
const N_OBS_PER_FOL = 40;    // follower observer slots
const OPS_PER_TICK  = 700;
const CACHE_TIME    = 10_000;      // long; entries persist through the run, drain at teardown
const SHARED_TO     = 1_000_000;   // sharedFetchTimeout -- never advanced past mid-run
const PROBE_KEY     = ["w", N_SHARED];   // a key outside the churned range, for the liveness probe

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
    maxNodes: (N_SHARED * (N_FOLLOWERS + 2) + N_OBS_PER_FOL * N_FOLLOWERS) * 16,
    maxLinks: (N_SHARED * (N_FOLLOWERS + 2) + N_OBS_PER_FOL * N_FOLLOWERS) * 48,
    prealloc: "eager",
    onCapacityExceeded: "grow",
});
setDefaultRegistry(reg);
const baseline = reg.stats();

// -- shared channel -----------------------------------------------------------
const {BroadcastChannel: BC} = createMockBroadcastChannel();
const clock = createMockClock();

function makeTab(isLeaderTab) {
    return queryClient({
        now: clock.now,
        setTimeout: clock.setTimeout,
        clearTimeout: clock.clearTimeout,
        broadcastChannel: BC,
        crossTab: true,
        crossTabChannel: "sf",
        sharedFetch: true,
        isLeader: () => isLeaderTab,
        sharedFetchTimeout: SHARED_TO,
        defaultStaleTime: 0,
        defaultCacheTime: CACHE_TIME,
        retry: 0,
    });
}

// -- leader: real fetcher + permanent observer on every shared key ------------
let leaderFetches = 0;
function leaderFetcher(ctx) {
    leaderFetches++;
    return Promise.resolve({k: ctx.key, n: leaderFetches});
}
const leader = makeTab(true);
const leaderHandles = new Array(N_SHARED + 1);
const leaderObservers = new Array(N_SHARED + 1);
for (let i = 0; i <= N_SHARED; i++) {   // includes PROBE_KEY at index N_SHARED
    const h = query(leader, {key: ["w", i], fetcher: leaderFetcher});
    leaderHandles[i] = h;
    leaderObservers[i] = effect(() => { h.data(); h.status(); });   // permanent -- leader always holds the entry
}

// -- followers: spy fetchers that MUST never fire for shared keys -------------
const followerFetches = new Array(N_FOLLOWERS).fill(0);
function makeFollower(fi) {
    const qc = makeTab(false);
    const fetcher = (ctx) => {                 // if this ever runs, the invariant is broken
        followerFetches[fi]++;
        return Promise.resolve({k: ctx.key, self: true});
    };
    const handles = new Array(N_SHARED);
    for (let i = 0; i < N_SHARED; i++) handles[i] = query(qc, {key: ["w", i], fetcher});
    const probe = query(qc, {key: PROBE_KEY, fetcher});
    const slots = new Array(N_OBS_PER_FOL).fill(null);
    return {qc, fetcher, handles, probe, slots};
}
const followers = Array.from({length: N_FOLLOWERS}, (_, i) => makeFollower(i));

function mountFol(f, i) {
    if (f.slots[i]) f.slots[i]();
    const h = f.handles[randInt(N_SHARED)];
    f.slots[i] = effect(() => { h.data(); h.status(); h.fetching(); });
}
function unmountFol(f, i) { if (f.slots[i]) { f.slots[i](); f.slots[i] = null; } }
for (const f of followers) for (let i = 0; i < N_OBS_PER_FOL; i++) mountFol(f, i);

// -- op mix -------------------------------------------------------------------
let ops = 0;
let errors = 0;
let lastError = null;

function step() {
    const mode = randInt(6);
    try {
        if (mode === 0) {
            leader.invalidate(["w", randInt(N_SHARED)]);   // drives shared refetch across followers
            ops++;
        } else if (mode === 1) {
            leader.setQueryData(["w", randInt(N_SHARED)], {v: randInt(1e6)});  // broadcast a value
            ops++;
        } else if (mode <= 3) {
            const f = followers[randInt(N_FOLLOWERS)];
            mountFol(f, randInt(N_OBS_PER_FOL));           // follower attaches -> defer to leader
            ops++;
        } else {
            const f = followers[randInt(N_FOLLOWERS)];
            unmountFol(f, randInt(N_OBS_PER_FOL));
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
        await drain();                                     // deliver fetch-req / setData; settle leader fetches
        clock.advance(1 + randInt(200));                   // stays far below SHARED_TO and CACHE_TIME
        await drain();
        await nextTick();
    }
    return (performance.now() - start) / 1000;
}

// -- liveness probe: a follower must hold the leader's fetched value for a key
//    it never fetched itself -------------------------------------------------
async function livenessProbe() {
    const f0 = followers[0];
    const s = effect(() => { f0.probe.data(); f0.probe.status(); });   // follower observes PROBE_KEY
    await drain(48);
    const leaderVal = leader.getQueryData(PROBE_KEY);
    const followerVal = f0.qc.getQueryData(PROBE_KEY);
    s();
    await drain(24);
    return {
        leaderVal,
        followerVal,
        ok: !!followerVal && !!leaderVal && followerVal.k !== undefined && followerVal.self === undefined,
    };
}

// -- ON-2: two-tab infinite page-storm oracle --------------------------------
// One shared infinite key. A single follower storms fetchNextPage(); each call
// broadcasts a fetch-req the leader fulfils, dedups per page (entry.promise),
// and broadcasts the whole pages array back. Assert the follower converges to
// the leader's exact pages array AND the leader fetched each page exactly once
// (per-page dedup). The follower's own infinite fetcher is a spy that must
// never run. QUERY_TORTURE_BREAK=storm corrupts the follower's final list to
// prove the convergence oracle is able to fail.
const STORM_BREAK = process.env.QUERY_TORTURE_BREAK === "storm";
async function pageStormOracle() {
    const INF_KEY = ["inf-storm"];
    const PAGES = 5;
    const perPageFetch = new Map();      // cursor -> leader fetch count
    const getNext = (last, all) => (all.length < PAGES ? all.length : null);
    const leaderInfFetcher = (ctx) => {
        const k = ctx.cursor == null ? 0 : ctx.cursor;
        perPageFetch.set(k, (perPageFetch.get(k) || 0) + 1);
        return Promise.resolve([k * 10, k * 10 + 1]);
    };
    let folSelfFetches = 0;
    const folInfFetcher = () => { folSelfFetches++; return Promise.resolve([-1]); };  // must never run

    const lInf = infiniteQuery(leader, {key: INF_KEY, fetcher: leaderInfFetcher, getNextCursor: getNext});
    const lStop = effect(() => { lInf.pages(); });
    const f0 = followers[0];
    const fInf = infiniteQuery(f0.qc, {key: INF_KEY, fetcher: folInfFetcher, getNextCursor: getNext});
    const fStop = effect(() => { fInf.pages(); });
    await drain(48);                     // leader + follower page one (via fetch-req)

    for (let round = 0; round < PAGES + 3; round++) {
        fInf.fetchNextPage();
        fInf.fetchNextPage();            // concurrent storm -- dedups to one leader fetch
        await drain(32);
    }
    await drain(48);

    if (STORM_BREAK) {
        // Corrupt the FOLLOWER'S entry data directly (not via setQueryData,
        // which would broadcast the divergence back to the leader and re-
        // converge them). A fresh array replaces the follower's page list so
        // only the follower diverges -- the convergence oracle must catch it.
        for (const e of f0.qc._internal.entries.values()) {
            if (e.isInfinite) { e.data.set([[999]]); break; }
        }
        await drain(24);
    }

    const leaderPages = leader.getQueryData(INF_KEY);
    const followerPages = f0.qc.getQueryData(INF_KEY);
    lStop(); fStop(); lInf.dispose(); fInf.dispose();
    await drain(24);

    const converged =
        Array.isArray(leaderPages) && Array.isArray(followerPages) &&
        JSON.stringify(leaderPages) === JSON.stringify(followerPages);
    let eachOnce = perPageFetch.size > 0;
    for (const [, c] of perPageFetch) if (c !== 1) eachOnce = false;

    return {
        converged, eachOnce, folSelfFetches,
        pageCount: Array.isArray(leaderPages) ? leaderPages.length : 0,
        perPage: [...perPageFetch.entries()].sort((a, b) => a[0] - b[0]),
        leaderPages, followerPages,
    };
}

async function teardown() {
    for (const f of followers) for (let i = 0; i < N_OBS_PER_FOL; i++) unmountFol(f, i);
    for (let i = 0; i <= N_SHARED; i++) leaderObservers[i]();     // stop permanent leader observers
    await drain(64);                                             // deferred stopWatcher + clearSharedTimer
    for (let i = 0; i <= N_SHARED; i++) leaderHandles[i].dispose();
    for (const f of followers) {
        for (let i = 0; i < N_SHARED; i++) f.handles[i].dispose();
        f.probe.dispose();
    }
    clock.advance(SHARED_TO + CACHE_TIME + 10_000);             // fire every remaining timer
    await drain(64);
    leader.dispose();
    for (const f of followers) f.qc.dispose();
    clock.advance(CACHE_TIME + 10_000);
    await drain(64);
    await nextTick();
}

const elapsed = await run();
const probe = await livenessProbe();
const storm = await pageStormOracle();
await teardown();

const after = reg.stats();
const followerTotal = followerFetches.reduce((a, b) => a + b, 0);
const leftoverEntries =
    leader._internal.entries.size + followers.reduce((a, f) => a + f.qc._internal.entries.size, 0);

console.log("shared-fetch-soak (leader/follower fetch dedup)");
console.log("  duration:", elapsed.toFixed(3), "s");
console.log("  followers:", N_FOLLOWERS);
console.log("  ops:", ops.toLocaleString());
console.log("  ops/sec:", (ops / elapsed).toLocaleString(undefined, {maximumFractionDigits: 0}));
console.log("  leader fetches:", leaderFetches.toLocaleString());
console.log("  follower fetches (must be 0):", followerTotal, "  per-tab:", followerFetches.join(","));
console.log("  liveness probe: leaderVal", probe.leaderVal ? "set" : "unset",
    "/ followerVal", probe.followerVal ? "set" : "unset", "/ ok:", probe.ok);
console.log("  page-storm: pages", storm.pageCount, "/ converged", storm.converged,
    "/ leader per-page", JSON.stringify(storm.perPage), "/ eachOnce", storm.eachOnce,
    "/ follower self-fetches", storm.folSelfFetches);
console.log("  errors:", errors);
console.log("  entries across all tabs after teardown:", leftoverEntries);
console.log("  clock timers pending after teardown:", clock.pendingCount);
console.log("  baseline activeNodes/activeLinks:", baseline.activeNodes, "/", baseline.activeLinks);
console.log("  post-teardown activeNodes/activeLinks:", after.activeNodes, "/", after.activeLinks);
console.log("  poolGrowths:", after.poolGrowths, " totalAllocations:", after.totalAllocations.toLocaleString());

let exitCode = 0;
if (errors > 0) {
    console.error("  FAIL: errors > 0; first =", lastError && lastError.message);
    exitCode = 1;
}
if (followerTotal !== 0) {
    console.error("  FAIL: dedup broken -- followers self-fetched", followerTotal, "times:", followerFetches.join(","));
    exitCode = 1;
}
if (leaderFetches === 0) {
    console.error("  FAIL: leader never fetched -- soak did no shared-fetch work");
    exitCode = 1;
}
if (!probe.ok) {
    console.error("  FAIL: liveness -- a follower did not receive the leader's fetched value for the probe key");
    exitCode = 1;
}
if (!storm.converged) {
    console.error("  FAIL: page-storm divergence -- follower", JSON.stringify(storm.followerPages),
        "!= leader", JSON.stringify(storm.leaderPages));
    exitCode = 1;
}
if (!storm.eachOnce) {
    console.error("  FAIL: page-storm dedup -- leader fetched a page more than once:", JSON.stringify(storm.perPage));
    exitCode = 1;
}
if (storm.folSelfFetches !== 0) {
    console.error("  FAIL: page-storm -- follower self-fetched", storm.folSelfFetches, "times");
    exitCode = 1;
}
if (leftoverEntries !== 0) {
    console.error("  FAIL: entry maps didn't drain -- leftover:", leftoverEntries);
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
if (exitCode === 0) console.log("  PASS: followers never self-fetched, leader served every request, pool at baseline");
process.exit(exitCode);
