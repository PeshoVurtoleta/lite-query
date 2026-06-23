// @zakkster/lite-query — comparative benchmark vs @tanstack/query-core.
//
// Run: node --expose-gc bench/bench.mjs
//
// Why no SWR? SWR is React-coupled — it has no framework-agnostic core to
// compare apples-to-apples against. (Its primitive is `useSWR(key, fetcher)`
// which depends on React hooks.) TanStack's `@tanstack/query-core` IS
// framework-agnostic, which makes it the right baseline for a signals-based
// alternative.
//
// Scenarios measure the same workload on each library so the numbers are
// directly comparable. Where possible we use the same fetcher fn, the same
// keys, and the same observer count to keep the comparison fair.
import {queryClient as liteClient, query as liteQuery, mutation as liteMutation} from "../Query.js";
import {
    QueryClient as TanstackQC,
    QueryObserver as TanstackObs,
    MutationObserver as TanstackMut
} from "@tanstack/query-core";
import {effect, createRegistry, setDefaultRegistry} from "@zakkster/lite-signal";

// Scenario E mounts 1000 concurrent queries; at peak that's ~5000 active
// lite-signal nodes (4 per cache entry + 1 per query observer), which
// exceeds the default registry's 1024-node cap. Install a grow-policy
// registry once at startup. Scenarios A–D stay well under 1024 and don't
// need this; it's only for the high-concurrency stress case.
setDefaultRegistry(createRegistry({
    maxNodes: 8192,
    onCapacityExceeded: "grow",
}));

const N_COLD = 5_000;
const N_WARM = 50_000;
const N_INVALIDATE = 2_000;
const N_MUTATE = 5_000;
const N_PARALLEL = 1_000;
const WARMUP_RATIO = 0.05;

// ── Memory + timing helpers ─────────────────────────────────────────────
function gc() {
    if (global.gc) global.gc();
}

function mem() {
    return process.memoryUsage().heapUsed;
}

function fmtBytes(n) {
    if (!isFinite(n)) return "—";
    const a = Math.abs(n);
    if (a >= 1_000_000) return (n / 1_000_000).toFixed(2) + " MB";
    if (a >= 1_000) return (n / 1_000).toFixed(2) + " KB";
    return n.toFixed(0) + " B";
}

function fmtMs(n) {
    return n.toFixed(1).padStart(8) + " ms";
}

function fmtOps(n) {
    return Math.round(n).toLocaleString("en-US").padStart(13);
}

function pad(s, w) {
    return String(s).padEnd(w);
}

// Synchronous-resolution fetcher pair — same observable behavior, both libs.
let counter = 0;

function makeFetcher() {
    counter = 0;
    return () => Promise.resolve({n: ++counter});
}

async function measure(label, N, setup) {
    const warm = Math.max(1, Math.floor(N * WARMUP_RATIO));
    const out = await setup();
    const tick = typeof out === "function" ? out : out.tick;
    const teardown = typeof out === "function" ? null : out.teardown;
    for (let i = 0; i < warm; i++) await tick(i);
    gc();
    const memStart = mem();
    const t0 = process.hrtime.bigint();
    for (let i = 0; i < N; i++) await tick(i);
    const t1 = process.hrtime.bigint();
    const transient = mem() - memStart;
    gc();
    const retained = mem() - memStart;
    const ms = Number(t1 - t0) / 1e6;
    const ops = (N * 1000) / ms;
    if (teardown) await teardown();
    return {label, N, ms, ops, transient: transient / N, retained: retained / N};
}

function reportRow(r) {
    console.log(
        pad(r.label, 60),
        pad(r.N.toLocaleString(), 8),
        fmtMs(r.ms),
        fmtOps(r.ops),
        fmtBytes(r.transient).padStart(13),
        fmtBytes(r.retained).padStart(13),
    );
}

function reportPair(name, lite, tan) {
    const speedup = lite.ops / tan.ops;
    const allocRatio = lite.transient / Math.max(1, tan.transient);
    console.log("");
    console.log(`▶ ${name}`);
    console.log(`  lite-query:    ${fmtOps(lite.ops).trim()} ops/sec, ${fmtBytes(lite.transient)}/op transient, ${fmtBytes(lite.retained)}/op retained`);
    console.log(`  query-core:    ${fmtOps(tan.ops).trim()} ops/sec, ${fmtBytes(tan.transient)}/op transient, ${fmtBytes(tan.retained)}/op retained`);
    console.log(`  lite is ${speedup.toFixed(2)}× ${speedup > 1 ? "FASTER" : "slower"}, allocates ${allocRatio.toFixed(2)}× ${allocRatio < 1 ? "LESS" : "more"} transient memory`);
}

// ─── A) Cold attach → resolve → dispose ──────────────────────────────────
// The full mount/unmount cycle on a unique key each iteration. Measures the
// cost of allocating an entry, attaching an observer, awaiting resolution,
// detaching, and scheduling GC.

async function liteColdScenario() {
    const qc = liteClient({});
    const f = makeFetcher();
    return {
        tick: async (i) => {
            const key = ["cold", i];
            const q = liteQuery(qc, {key, fetcher: f});
            const stop = effect(() => {
                q.data();
            });
            await Promise.resolve();
            await Promise.resolve();
            stop();
            q.dispose();
            // Synchronous cleanup — disposes entry signals back to the pool.
            // Mirrors a typical "route unmount" cleanup in real apps; also
            // keeps the bench independent of macrotask-based GC timing.
            qc.removeQueries(key, {exact: true});
        },
        teardown: async () => {
            qc.dispose();
        },
    };
}

async function tanstackColdScenario() {
    const qc = new TanstackQC();
    const f = makeFetcher();
    return {
        tick: async (i) => {
            const obs = new TanstackObs(qc, {queryKey: ["cold", i], queryFn: f});
            const unsub = obs.subscribe(() => {
            });
            await Promise.resolve();
            await Promise.resolve();
            unsub();
            obs.destroy();
            qc.removeQueries({queryKey: ["cold", i], exact: true});
        },
        teardown: async () => {
            qc.clear();
        },
    };
}

// ─── B) Warm cache hit ───────────────────────────────────────────────────
// Pre-resolve a single key; then repeatedly attach a fresh observer and read
// the cached value. Measures the read-through fast path.

async function liteWarmScenario() {
    const qc = liteClient({defaultStaleTime: Infinity, defaultCacheTime: Infinity});
    const f = makeFetcher();
    // Seed: attach once, resolve, detach.
    const seed = liteQuery(qc, {key: ["warm"], fetcher: f});
    const stopSeed = effect(() => {
        seed.data();
    });
    await Promise.resolve();
    await Promise.resolve();
    stopSeed();
    seed.dispose();
    return {
        tick: async () => {
            const q = liteQuery(qc, {key: ["warm"], fetcher: f, staleTime: Infinity});
            const stop = effect(() => {
                q.data();
            });   // attaches and reads cached value
            stop();
            q.dispose();
        },
        teardown: async () => {
            qc.dispose();
        },
    };
}

async function tanstackWarmScenario() {
    const qc = new TanstackQC({defaultOptions: {queries: {staleTime: Infinity, gcTime: Infinity}}});
    const f = makeFetcher();
    const seed = new TanstackObs(qc, {queryKey: ["warm"], queryFn: f});
    const seedUnsub = seed.subscribe(() => {
    });
    await Promise.resolve();
    await Promise.resolve();
    seedUnsub();
    seed.destroy();
    return {
        tick: async () => {
            const obs = new TanstackObs(qc, {queryKey: ["warm"], queryFn: f, staleTime: Infinity});
            const unsub = obs.subscribe(() => {
            });
            unsub();
            obs.destroy();
        },
        teardown: async () => {
            qc.clear();
        },
    };
}

// ─── C) Invalidation + refetch on N observed queries ─────────────────────
// Pre-attach M observers on distinct keys. Call invalidate() on all.
// Each iteration measures one full invalidate+refetch sweep.

async function liteInvalidateScenario() {
    const M = 50;
    const qc = liteClient({defaultStaleTime: 0, defaultCacheTime: Infinity});
    const f = makeFetcher();
    const queries = [];
    const stops = [];
    for (let i = 0; i < M; i++) {
        const q = liteQuery(qc, {key: ["inv", i], fetcher: f, staleTime: 0});
        queries.push(q);
        stops.push(effect(() => {
            q.data();
        }));
    }
    await Promise.resolve();
    await Promise.resolve();
    return {
        tick: async () => {
            qc.invalidate(["inv"]);     // prefix match → all M re-fetch
            await Promise.resolve();
            await Promise.resolve();
        },
        teardown: async () => {
            for (const s of stops) s();
            for (const q of queries) q.dispose();
            qc.dispose();
        },
    };
}

async function tanstackInvalidateScenario() {
    const M = 50;
    const qc = new TanstackQC({defaultOptions: {queries: {staleTime: 0, gcTime: Infinity}}});
    const f = makeFetcher();
    const observers = [];
    const unsubs = [];
    for (let i = 0; i < M; i++) {
        const obs = new TanstackObs(qc, {queryKey: ["inv", i], queryFn: f, staleTime: 0});
        observers.push(obs);
        unsubs.push(obs.subscribe(() => {
        }));
    }
    await Promise.resolve();
    await Promise.resolve();
    return {
        tick: async () => {
            await qc.invalidateQueries({queryKey: ["inv"]});
        },
        teardown: async () => {
            for (const u of unsubs) u();
            for (const o of observers) o.destroy();
            qc.clear();
        },
    };
}

// ─── D) Mutation with optimistic update + rollback ───────────────────────
// onMutate snapshots + writes, fn rejects, onError rolls back. Full pipeline.

async function liteMutationScenario() {
    const qc = liteClient({});
    qc.setQueryData(["todos"], [{id: 1, done: false}]);
    const m = liteMutation(qc, {
        fn: () => Promise.reject(new Error("nope")),
        onMutate: () => {
            const prev = qc.getQueryData(["todos"]);
            qc.setQueryData(["todos"], (old) => [...old, {id: 2, done: false, _opt: true}]);
            return {prev};
        },
        onError: (err, vars, ctx) => qc.setQueryData(["todos"], ctx.prev),
    });
    return {
        tick: async () => {
            try {
                await m.mutate({});
            } catch (_) { /* expected */
            }
        },
        teardown: async () => {
            qc.dispose();
        },
    };
}

async function tanstackMutationScenario() {
    const qc = new TanstackQC();
    qc.setQueryData(["todos"], [{id: 1, done: false}]);
    const obs = new TanstackMut(qc, {
        mutationFn: () => Promise.reject(new Error("nope")),
        onMutate: () => {
            const prev = qc.getQueryData(["todos"]);
            qc.setQueryData(["todos"], (old) => [...old, {id: 2, done: false, _opt: true}]);
            return {prev};
        },
        onError: (err, vars, ctx) => qc.setQueryData(["todos"], ctx.prev),
    });
    return {
        tick: async () => {
            try {
                await obs.mutate({});
            } catch (_) { /* expected */
            }
        },
        teardown: async () => {
            obs.reset();
            qc.clear();
        },
    };
}

// ─── E) Many parallel queries — cache-map scaling ────────────────────────

async function liteParallelScenario() {
    return {
        tick: async () => {
            const qc = liteClient({});
            const f = makeFetcher();
            const queries = [], stops = [];
            for (let i = 0; i < N_PARALLEL; i++) {
                const q = liteQuery(qc, {key: ["p", i], fetcher: f});
                queries.push(q);
                stops.push(effect(() => {
                    q.data();
                }));
            }
            await Promise.resolve();
            await Promise.resolve();
            for (const s of stops) s();
            for (const q of queries) q.dispose();
            qc.dispose();
        },
    };
}

async function tanstackParallelScenario() {
    return {
        tick: async () => {
            const qc = new TanstackQC();
            const f = makeFetcher();
            const observers = [], unsubs = [];
            for (let i = 0; i < N_PARALLEL; i++) {
                const obs = new TanstackObs(qc, {queryKey: ["p", i], queryFn: f});
                observers.push(obs);
                unsubs.push(obs.subscribe(() => {
                }));
            }
            await Promise.resolve();
            await Promise.resolve();
            for (const u of unsubs) u();
            for (const o of observers) o.destroy();
            qc.clear();
        },
    };
}

// ─── Main ───────────────────────────────────────────────────────────────
console.log("");
console.log("@zakkster/lite-query  vs  @tanstack/query-core");
console.log(`Node: ${process.version} · ${new Date().toISOString()}`);
console.log("");
console.log(
    pad("scenario", 60),
    pad("N", 8),
    "      ms total",
    "        ops/sec",
    "  transient/op",
    "    retained/op",
);
console.log("─".repeat(125));

const rows = {};
rows.coldL = await measure("A) cold attach → resolve → dispose  [lite-query]", N_COLD, liteColdScenario);
reportRow(rows.coldL);
rows.coldT = await measure("A) cold attach → resolve → dispose  [query-core]", N_COLD, tanstackColdScenario);
reportRow(rows.coldT);

rows.warmL = await measure("B) warm cache hit (already resolved) [lite-query]", N_WARM, liteWarmScenario);
reportRow(rows.warmL);
rows.warmT = await measure("B) warm cache hit (already resolved) [query-core]", N_WARM, tanstackWarmScenario);
reportRow(rows.warmT);

rows.invL = await measure("C) invalidate 50 observed queries    [lite-query]", N_INVALIDATE, liteInvalidateScenario);
reportRow(rows.invL);
rows.invT = await measure("C) invalidate 50 observed queries    [query-core]", N_INVALIDATE, tanstackInvalidateScenario);
reportRow(rows.invT);

rows.mutL = await measure("D) mutation w/ optimistic + rollback [lite-query]", N_MUTATE, liteMutationScenario);
reportRow(rows.mutL);
rows.mutT = await measure("D) mutation w/ optimistic + rollback [query-core]", N_MUTATE, tanstackMutationScenario);
reportRow(rows.mutT);

rows.parL = await measure("E) 1000 parallel queries per cycle   [lite-query]", 50, liteParallelScenario);
reportRow(rows.parL);
rows.parT = await measure("E) 1000 parallel queries per cycle   [query-core]", 50, tanstackParallelScenario);
reportRow(rows.parT);

console.log("");
console.log("─".repeat(125));
console.log("Pairwise comparison:");
reportPair("A) cold attach → resolve → dispose", rows.coldL, rows.coldT);
reportPair("B) warm cache hit", rows.warmL, rows.warmT);
reportPair("C) invalidate 50 observed queries", rows.invL, rows.invT);
reportPair("D) mutation w/ optimistic + rollback", rows.mutL, rows.mutT);
reportPair("E) 1000 parallel queries per cycle", rows.parL, rows.parT);

console.log("");
console.log("Notes:");
console.log("  • SWR is React-coupled (no framework-agnostic core); excluded for honest apples-to-apples.");
console.log("  • TanStack version: " + JSON.parse(await import("node:fs").then(f => f.promises.readFile("./node_modules/@tanstack/query-core/package.json", "utf8"))).version);
console.log("  • Same fetcher, same keys, same observer pattern. lite-query uses lite-signal effect, query-core uses observer.subscribe.");
console.log("  • Both libraries run their cache lookups, observer machinery, and cleanup paths in full — no skipping or stubbing.");
console.log("");
