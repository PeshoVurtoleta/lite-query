/**
 * @zakkster/lite-query — test suite (the implementation spec).
 *
 * Patterns adopted from prior art:
 *   - TanStack Query: dedup, stale-time semantics, cache-time GC, retry with
 *     backoff, optimistic updates with rollback, mutation invalidation cascade.
 *   - SWR: background revalidation, no-revalidate-when-fresh.
 *   - Apollo: prefix invalidation (without the normalization machinery).
 *
 * What this suite intentionally does NOT cover in v1:
 *   - Focus / reconnect revalidation (the browser-event-driven refetch
 *     triggers from SWR/TanStack). Punted to 1.x as injectable triggers.
 *   - Suspense / throw-promise integration. Framework-agnostic; just signals.
 *   - Infinite queries / pagination. Separate concern; deserves its own pass.
 *   - Persistence to storage. Use @zakkster/lite-persist on snapshots.
 */

import { test, beforeEach } from "node:test";
import assert from "node:assert/strict";

import {
    signal, effect, computed,
    stats, createRegistry, setDefaultRegistry,
} from "@zakkster/lite-signal";

import { queryClient, query, mutation } from "../Query.js";

import {
    createControlledFetcher,
    createQueuedFetcher,
    createMockClock,
    createMockBroadcastChannel,
    setupMockEnv,
} from "./harness.js";

// Roomy registry for the long-running suite
beforeEach(() => setDefaultRegistry(createRegistry({ maxNodes: 16384 })));

// Helper: flush microtasks so awaited fetcher resolutions chain out.
const tick = () => new Promise((r) => queueMicrotask(r));

// ─────────────────────────────────────────────────────────────────────────────
// Section 1 — queryClient: construction & defaults
// ─────────────────────────────────────────────────────────────────────────────

test("queryClient: creates a client with sensible defaults", () => {
    const qc = queryClient();
    assert.equal(typeof qc.getQueryData, "function");
    assert.equal(typeof qc.setQueryData, "function");
    assert.equal(typeof qc.invalidate, "function");
    assert.equal(typeof qc.removeQueries, "function");
    assert.equal(typeof qc.clear, "function");
});

test("queryClient: accepts default stale/cache times", () => {
    const qc = queryClient({ defaultStaleTime: 60_000, defaultCacheTime: 600_000 });
    assert.equal(qc.options.defaultStaleTime, 60_000);
    assert.equal(qc.options.defaultCacheTime, 600_000);
});

test("queryClient: crossTab defaults to false (no BroadcastChannel created)", () => {
    let bcInstances = 0;
    class TrackedBC { constructor() { bcInstances++; } postMessage() {} addEventListener() {} close() {} }
    queryClient({ broadcastChannel: TrackedBC });
    assert.equal(bcInstances, 0, "no channel until crossTab: true");
});

test("queryClient: crossTab:true instantiates exactly one BroadcastChannel", () => {
    let bcInstances = 0;
    let lastName = null;
    class TrackedBC {
        constructor(name) { bcInstances++; lastName = name; }
        postMessage() {} addEventListener() {} close() {}
    }
    queryClient({ crossTab: true, crossTabChannel: "my-app", broadcastChannel: TrackedBC });
    assert.equal(bcInstances, 1);
    assert.equal(lastName, "my-app");
});

test("queryClient: clear() releases all entries", () => {
    const qc = queryClient();
    qc.setQueryData(["a"], 1);
    qc.setQueryData(["b"], 2);
    qc.clear();
    assert.equal(qc.getQueryData(["a"]), undefined);
    assert.equal(qc.getQueryData(["b"]), undefined);
});

// ─────────────────────────────────────────────────────────────────────────────
// Section 2 — query: basic lifecycle
// ─────────────────────────────────────────────────────────────────────────────

test("query: fires fetcher on first observer attach", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createControlledFetcher();
    const q = query(qc, { key: ["todos"], fetcher: f.fetcher });
    effect(() => { q.data(); });                                // observe
    await tick();
    assert.equal(f.callCount, 1);
    q.dispose();
});

test("query: loading is true before resolution, false after", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createControlledFetcher();
    const q = query(qc, { key: ["todos"], fetcher: f.fetcher });
    effect(() => { q.data(); q.loading(); });
    await tick();
    assert.equal(q.loading(), true, "loading while pending");
    f.resolve(["a", "b"]);
    await tick();
    assert.equal(q.loading(), false);
    assert.deepEqual(q.data(), ["a", "b"]);
    q.dispose();
});

test("query: status transitions idle → pending → success", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createControlledFetcher();
    const seen = [];
    const q = query(qc, { key: ["k"], fetcher: f.fetcher });
    effect(() => { seen.push(q.status()); });
    await tick();
    f.resolve("ok");
    await tick();
    assert.ok(seen.includes("pending"));
    assert.equal(seen[seen.length - 1], "success");
    q.dispose();
});

test("query: error path sets error and status='error'", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createControlledFetcher();
    const q = query(qc, { key: ["k"], fetcher: f.fetcher });
    effect(() => { q.data(); q.error(); });
    await tick();
    f.reject(new Error("boom"));
    await tick();
    assert.equal(q.status(), "error");
    assert.equal(q.error().message, "boom");
    assert.equal(q.data(), undefined);
    q.dispose();
});

test("query: subsequent observers don't trigger duplicate fetches", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createControlledFetcher();
    const q = query(qc, { key: ["k"], fetcher: f.fetcher });
    effect(() => q.data());
    effect(() => q.data());
    effect(() => q.data());
    await tick();
    assert.equal(f.callCount, 1, "three observers, one fetch");
    q.dispose();
});

test("query: data() returns undefined until first resolve", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createControlledFetcher();
    const q = query(qc, { key: ["k"], fetcher: f.fetcher });
    effect(() => q.data());
    assert.equal(q.data(), undefined);
    await tick();
    assert.equal(q.data(), undefined);
    f.resolve(42);
    await tick();
    assert.equal(q.data(), 42);
    q.dispose();
});

test("query: fetcher receives { key, signal } argument", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createControlledFetcher();
    const q = query(qc, { key: ["users", 5], fetcher: f.fetcher });
    effect(() => q.data());
    await tick();
    assert.ok(f.lastCall);
    assert.deepEqual(f.lastCall.key, ["users", 5]);
    assert.ok(f.lastCall.signal instanceof AbortSignal);
    q.dispose();
});

test("query: no observers → no fetch", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createControlledFetcher();
    query(qc, { key: ["k"], fetcher: f.fetcher });               // no effect attached
    await tick();
    assert.equal(f.callCount, 0);
});

test("query: fetching() is true on initial AND on background revalidations", async () => {
    const { qc } = setupMockEnv(queryClient, { defaultStaleTime: 0 });
    const f = createQueuedFetcher();
    const q = query(qc, { key: ["k"], fetcher: f.fetcher });
    effect(() => { q.data(); q.fetching(); });

    await tick();
    assert.equal(q.fetching(), true, "fetching during initial");
    f.resolveNth(0, "first");
    await tick();
    assert.equal(q.fetching(), false, "settled");

    q.refetch();                                              // background revalidation — don't await
    await tick();
    assert.equal(q.fetching(), true, "fetching during background revalidation");
    f.resolveNth(1, "second");
    await tick();
    assert.equal(q.fetching(), false, "settled again");
    q.dispose();
});

test("query: data persists across observer reattach within cacheTime", async () => {
    const { qc, clock } = setupMockEnv(queryClient, { defaultCacheTime: 10_000, defaultStaleTime: 100_000 });
    const f = createControlledFetcher();
    const q1 = query(qc, { key: ["k"], fetcher: f.fetcher });
    const stop1 = effect(() => q1.data());
    await tick();
    f.resolve("cached");
    await tick();
    stop1();
    q1.dispose();

    // Within cacheTime AND staleTime — new observer reads from cache, no new fetch
    clock.advance(5_000);
    const q2 = query(qc, { key: ["k"], fetcher: f.fetcher });
    effect(() => q2.data());
    await tick();
    assert.equal(q2.data(), "cached", "served from cache");
    assert.equal(f.callCount, 1, "no second fetch (still fresh per staleTime)");
    q2.dispose();
});

// ─────────────────────────────────────────────────────────────────────────────
// Section 3 — query: dedup (concurrent fetch sharing)
// ─────────────────────────────────────────────────────────────────────────────

test("dedup: two queries with the same key share the same in-flight fetch", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createControlledFetcher();
    const q1 = query(qc, { key: ["users"], fetcher: f.fetcher });
    const q2 = query(qc, { key: ["users"], fetcher: f.fetcher });
    effect(() => q1.data());
    effect(() => q2.data());
    await tick();
    assert.equal(f.callCount, 1, "single fetch for two observers");
    f.resolve(["alice"]);
    await tick();
    assert.deepEqual(q1.data(), ["alice"]);
    assert.deepEqual(q2.data(), ["alice"]);
    q1.dispose(); q2.dispose();
});

test("dedup: queries with different keys fetch independently", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createQueuedFetcher();
    const a = query(qc, { key: ["a"], fetcher: f.fetcher });
    const b = query(qc, { key: ["b"], fetcher: f.fetcher });
    effect(() => a.data());
    effect(() => b.data());
    await tick();
    assert.equal(f.callCount, 2);
    a.dispose(); b.dispose();
});

test("dedup: structurally-equal keys are treated as the same key", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createControlledFetcher();
    const q1 = query(qc, { key: ["users", { filter: "all" }], fetcher: f.fetcher });
    const q2 = query(qc, { key: ["users", { filter: "all" }], fetcher: f.fetcher });
    effect(() => q1.data());
    effect(() => q2.data());
    await tick();
    assert.equal(f.callCount, 1, "structural dedup");
    q1.dispose(); q2.dispose();
});

test("dedup: object key order doesn't affect dedup (stable hashing)", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createControlledFetcher();
    const q1 = query(qc, { key: ["users", { a: 1, b: 2 }], fetcher: f.fetcher });
    const q2 = query(qc, { key: ["users", { b: 2, a: 1 }], fetcher: f.fetcher });
    effect(() => q1.data());
    effect(() => q2.data());
    await tick();
    assert.equal(f.callCount, 1, "key order does not change hash");
    q1.dispose(); q2.dispose();
});

// ─────────────────────────────────────────────────────────────────────────────
// Section 4 — query: reactive key
// ─────────────────────────────────────────────────────────────────────────────

test("reactive key: key change triggers a new fetch", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createQueuedFetcher();
    const userId = signal(1);
    const q = query(qc, {
        key: () => ["user", userId()],
        fetcher: f.fetcher,
    });
    effect(() => q.data());
    await tick();
    f.resolveNth(0, "user1");
    await tick();

    userId.set(2);
    await tick();
    assert.equal(f.callCount, 2);
    assert.equal(f.calls[1].key[1], 2);
    f.resolveNth(1, "user2");
    await tick();
    assert.equal(q.data(), "user2");
    q.dispose();
});

test("reactive key: identical keys (no change) do NOT refetch", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createControlledFetcher();
    const flip = signal(1);
    const q = query(qc, {
        key: () => ["k", flip() * 0],                           // always [k, 0]
        fetcher: f.fetcher,
    });
    effect(() => q.data());
    await tick();
    f.resolve("v");
    await tick();
    flip.set(2);
    await tick();
    assert.equal(f.callCount, 1, "key didn't actually change");
    q.dispose();
});

test("reactive key: key change while fetch in flight aborts the previous fetch", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createQueuedFetcher();
    const userId = signal(1);
    const q = query(qc, {
        key: () => ["user", userId()],
        fetcher: f.fetcher,
    });
    effect(() => q.data());
    await tick();
    const firstSignal = f.calls[0].signal;
    assert.equal(firstSignal.aborted, false);

    userId.set(2);                                              // change before resolve
    await tick();
    assert.equal(firstSignal.aborted, true, "previous fetch aborted");
    q.dispose();
});

test("reactive key: data from old key not reflected when new key resolves", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createQueuedFetcher();
    const userId = signal(1);
    const q = query(qc, { key: () => ["user", userId()], fetcher: f.fetcher });
    effect(() => q.data());
    await tick();
    userId.set(2);
    await tick();

    // Resolve the OLD fetch — should be discarded (it was aborted)
    f.resolveNth(0, "stale");
    f.resolveNth(1, "fresh");
    await tick();
    assert.equal(q.data(), "fresh");
    q.dispose();
});

test("reactive key: data() reflects the cache entry for the *current* key", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createQueuedFetcher();
    const userId = signal(1);
    const q = query(qc, { key: () => ["user", userId()], fetcher: f.fetcher });
    effect(() => q.data());
    await tick();
    f.resolveNth(0, "u1");
    await tick();
    assert.equal(q.data(), "u1");

    userId.set(2);
    await tick();
    assert.equal(q.data(), undefined, "no data yet for the new key");
    f.resolveNth(1, "u2");
    await tick();
    assert.equal(q.data(), "u2");

    userId.set(1);                                              // back to cached key
    await tick();
    assert.equal(q.data(), "u1", "cache hit on previous key");
    q.dispose();
});

// ─────────────────────────────────────────────────────────────────────────────
// Section 5 — query: stale time
// ─────────────────────────────────────────────────────────────────────────────

test("staleTime: fresh data does NOT trigger refetch on reobserve", async () => {
    const { qc, clock } = setupMockEnv(queryClient, { defaultStaleTime: 10_000 });
    const f = createControlledFetcher();
    const q1 = query(qc, { key: ["k"], fetcher: f.fetcher });
    const stop1 = effect(() => q1.data());
    await tick();
    f.resolve("v");
    await tick();
    stop1(); q1.dispose();

    clock.advance(5_000);                                       // still fresh
    const q2 = query(qc, { key: ["k"], fetcher: f.fetcher });
    effect(() => q2.data());
    await tick();
    assert.equal(f.callCount, 1, "no refetch — data is fresh");
    q2.dispose();
});

test("staleTime: stale data triggers a refetch on reobserve (stale-while-revalidate)", async () => {
    const { qc, clock } = setupMockEnv(queryClient, { defaultStaleTime: 10_000 });
    const f = createQueuedFetcher();
    const q1 = query(qc, { key: ["k"], fetcher: f.fetcher });
    const stop1 = effect(() => q1.data());
    await tick();
    f.resolveNth(0, "stale");
    await tick();
    stop1(); q1.dispose();

    clock.advance(15_000);                                      // past staleTime
    const q2 = query(qc, { key: ["k"], fetcher: f.fetcher });
    effect(() => q2.data());
    await tick();
    assert.equal(q2.data(), "stale", "stale data returned immediately");
    assert.equal(f.callCount, 2, "background refetch triggered");
    q2.dispose();
});

test("staleTime: per-query override beats the client default", async () => {
    const { qc, clock } = setupMockEnv(queryClient, { defaultStaleTime: 1000 });
    const f = createControlledFetcher();
    const q1 = query(qc, { key: ["k"], staleTime: 100_000, fetcher: f.fetcher });
    const stop1 = effect(() => q1.data());
    await tick();
    f.resolve("v");
    await tick();
    stop1(); q1.dispose();

    clock.advance(50_000);                                      // past default, within per-query
    const q2 = query(qc, { key: ["k"], staleTime: 100_000, fetcher: f.fetcher });
    effect(() => q2.data());
    await tick();
    assert.equal(f.callCount, 1, "per-query staleTime honored");
    q2.dispose();
});

test("staleTime: staleTime: 0 means always-refetch on attach", async () => {
    const { qc, clock } = setupMockEnv(queryClient, { defaultStaleTime: 0 });
    const f = createQueuedFetcher();
    const q1 = query(qc, { key: ["k"], fetcher: f.fetcher });
    const stop1 = effect(() => q1.data());
    await tick();
    f.resolveNth(0, "v");
    await tick();
    stop1(); q1.dispose();

    // Re-attach: should refetch even with no time advance
    const q2 = query(qc, { key: ["k"], fetcher: f.fetcher });
    effect(() => q2.data());
    await tick();
    assert.equal(f.callCount, 2);
    q2.dispose();
});

test("staleTime: Infinity means never refetch from staleness", async () => {
    const { qc, clock } = setupMockEnv(queryClient, { defaultStaleTime: Infinity, defaultCacheTime: Infinity });
    const f = createControlledFetcher();
    const q1 = query(qc, { key: ["k"], fetcher: f.fetcher });
    const stop1 = effect(() => q1.data());
    await tick();
    f.resolve("v");
    await tick();
    stop1(); q1.dispose();

    clock.advance(365 * 24 * 60 * 60 * 1000);                  // a year
    const q2 = query(qc, { key: ["k"], fetcher: f.fetcher });
    effect(() => q2.data());
    await tick();
    assert.equal(f.callCount, 1, "Infinity means never stale");
    q2.dispose();
});

// ─────────────────────────────────────────────────────────────────────────────
// Section 6 — query: cache time / GC
// ─────────────────────────────────────────────────────────────────────────────

test("cacheTime: entry survives observer detach within cacheTime", async () => {
    const { qc, clock } = setupMockEnv(queryClient, { defaultCacheTime: 10_000 });
    const f = createControlledFetcher();
    const q = query(qc, { key: ["k"], fetcher: f.fetcher });
    const stop = effect(() => q.data());
    await tick();
    f.resolve("v");
    await tick();
    stop(); q.dispose();

    clock.advance(5_000);
    assert.equal(qc.getQueryData(["k"]), "v", "entry survives within cacheTime");
});

test("cacheTime: entry removed after cacheTime with no observers", async () => {
    const { qc, clock } = setupMockEnv(queryClient, { defaultCacheTime: 10_000 });
    const f = createControlledFetcher();
    const q = query(qc, { key: ["k"], fetcher: f.fetcher });
    const stop = effect(() => q.data());
    await tick();
    f.resolve("v");
    await tick();
    stop(); q.dispose();

    clock.advance(11_000);
    assert.equal(qc.getQueryData(["k"]), undefined, "entry GC'd past cacheTime");
});

test("cacheTime: reattaching within window cancels the GC timer", async () => {
    const { qc, clock } = setupMockEnv(queryClient, { defaultCacheTime: 10_000 });
    const f = createControlledFetcher();
    const q1 = query(qc, { key: ["k"], fetcher: f.fetcher });
    const stop1 = effect(() => q1.data());
    await tick();
    f.resolve("v");
    await tick();
    stop1(); q1.dispose();

    clock.advance(5_000);
    const q2 = query(qc, { key: ["k"], fetcher: f.fetcher });
    const stop2 = effect(() => q2.data());
    await tick();
    clock.advance(20_000);                                       // past original GC
    assert.equal(q2.data(), "v", "GC was cancelled by reattach");
    stop2(); q2.dispose();
});

test("cacheTime: per-query override beats the client default", async () => {
    const { qc, clock } = setupMockEnv(queryClient, { defaultCacheTime: 1000 });
    const f = createControlledFetcher();
    const q = query(qc, { key: ["k"], cacheTime: 60_000, fetcher: f.fetcher });
    const stop = effect(() => q.data());
    await tick();
    f.resolve("v");
    await tick();
    stop(); q.dispose();

    clock.advance(30_000);
    assert.equal(qc.getQueryData(["k"]), "v", "per-query cacheTime honored");
});

test("cacheTime: cacheTime: Infinity pins the entry forever", async () => {
    const { qc, clock } = setupMockEnv(queryClient, { defaultCacheTime: Infinity });
    const f = createControlledFetcher();
    const q = query(qc, { key: ["k"], fetcher: f.fetcher });
    const stop = effect(() => q.data());
    await tick();
    f.resolve("v");
    await tick();
    stop(); q.dispose();
    clock.advance(365 * 24 * 60 * 60 * 1000);                   // a year
    assert.equal(qc.getQueryData(["k"]), "v");
});

// ─────────────────────────────────────────────────────────────────────────────
// Section 7 — query: enabled flag
// ─────────────────────────────────────────────────────────────────────────────

test("enabled: false skips the initial fetch", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createControlledFetcher();
    const q = query(qc, { key: ["k"], fetcher: f.fetcher, enabled: false });
    effect(() => q.data());
    await tick();
    assert.equal(f.callCount, 0);
    q.dispose();
});

test("enabled: transitioning false → true triggers a fetch", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createControlledFetcher();
    const en = signal(false);
    const q = query(qc, { key: ["k"], fetcher: f.fetcher, enabled: () => en() });
    effect(() => q.data());
    await tick();
    assert.equal(f.callCount, 0);
    en.set(true);
    await tick();
    assert.equal(f.callCount, 1);
    q.dispose();
});

test("enabled: while disabled, status stays 'idle'", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createControlledFetcher();
    const q = query(qc, { key: ["k"], fetcher: f.fetcher, enabled: false });
    effect(() => q.status());
    await tick();
    assert.equal(q.status(), "idle");
    q.dispose();
});

test("enabled: transitioning true → false aborts in-flight fetch and reverts to idle", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createControlledFetcher();
    const en = signal(true);
    const q = query(qc, { key: ["k"], fetcher: f.fetcher, enabled: () => en() });
    effect(() => { q.data(); q.status(); });
    await tick();
    assert.equal(f.callCount, 1, "fetched while enabled");
    const ac = f.lastCall.signal;
    en.set(false);
    await tick();
    assert.equal(ac.aborted, true, "fetch aborted on enabled→false");
    assert.equal(q.status(), "idle", "reverts to idle, not 'error'");
    q.dispose();
});

// ─────────────────────────────────────────────────────────────────────────────
// Section 8 — query: retry & error
// ─────────────────────────────────────────────────────────────────────────────

test("retry: failed fetch retries up to N times then errors", async () => {
    const { qc, clock } = setupMockEnv(queryClient, { retry: 2, retryDelay: () => 100 });
    const f = createQueuedFetcher();
    const q = query(qc, { key: ["k"], fetcher: f.fetcher });
    effect(() => { q.data(); q.error(); });
    await tick();
    f.rejectNth(0, new Error("e1"));
    await tick();
    clock.advance(100);
    await tick();
    f.rejectNth(1, new Error("e2"));
    await tick();
    clock.advance(100);
    await tick();
    f.rejectNth(2, new Error("e3"));
    await tick();
    assert.equal(f.callCount, 3, "1 initial + 2 retries");
    assert.equal(q.status(), "error");
    assert.equal(q.error().message, "e3");
    q.dispose();
});

test("retry: success on retry clears error and resolves", async () => {
    const { qc, clock } = setupMockEnv(queryClient, { retry: 2, retryDelay: () => 100 });
    const f = createQueuedFetcher();
    const q = query(qc, { key: ["k"], fetcher: f.fetcher });
    effect(() => { q.data(); q.error(); });
    await tick();
    f.rejectNth(0, new Error("transient"));
    await tick();
    clock.advance(100);
    await tick();
    f.resolveNth(1, "recovered");
    await tick();
    assert.equal(q.status(), "success");
    assert.equal(q.error(), undefined);
    assert.equal(q.data(), "recovered");
    q.dispose();
});

test("retry: retryDelay is called per attempt and may return varying delays", async () => {
    const { qc, clock } = setupMockEnv(queryClient, {
        retry: 3,
        retryDelay: (attempt) => attempt * 1000,                // 0ms, 1000ms, 2000ms, 3000ms
    });
    const f = createQueuedFetcher();
    const q = query(qc, { key: ["k"], fetcher: f.fetcher });
    effect(() => q.data());
    await tick();
    f.rejectNth(0, new Error("e1"));
    await tick();
    clock.advance(999); await tick();
    assert.equal(f.callCount, 1, "still waiting on first delay");
    clock.advance(1); await tick();
    assert.equal(f.callCount, 2);
    q.dispose();
});

test("retry: per-query retry override beats client default", async () => {
    const { qc, clock } = setupMockEnv(queryClient, { retry: 3 });
    const f = createControlledFetcher();
    const q = query(qc, { key: ["k"], fetcher: f.fetcher, retry: 0 });
    effect(() => q.data());
    await tick();
    f.reject(new Error("nope"));
    await tick();
    assert.equal(f.callCount, 1, "no retries");
    assert.equal(q.status(), "error");
    q.dispose();
});

test("retry: function form can return false to abort retries", async () => {
    const { qc } = setupMockEnv(queryClient, {
        retry: (attempt, err) => err.message !== "fatal",
        retryDelay: () => 0,
    });
    const f = createControlledFetcher();
    const q = query(qc, { key: ["k"], fetcher: f.fetcher });
    effect(() => q.data());
    await tick();
    f.reject(new Error("fatal"));
    await tick();
    assert.equal(f.callCount, 1, "fatal errors don't retry");
    q.dispose();
});

// ─────────────────────────────────────────────────────────────────────────────
// Section 9 — query: abort & race safety
// ─────────────────────────────────────────────────────────────────────────────

test("abort: dispose aborts in-flight fetch", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createControlledFetcher();
    const q = query(qc, { key: ["k"], fetcher: f.fetcher });
    effect(() => q.data());
    await tick();
    const ac = f.lastCall.signal;
    q.dispose();
    assert.equal(ac.aborted, true);
});

test("abort: refetch while pending aborts the previous fetch", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createQueuedFetcher();
    const q = query(qc, { key: ["k"], fetcher: f.fetcher });
    effect(() => q.data());
    await tick();
    const firstSignal = f.calls[0].signal;
    q.refetch();                                                // forces new fetch
    await tick();
    assert.equal(firstSignal.aborted, true);
    q.dispose();
});

test("abort: aborted fetch's eventual resolution is ignored", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createQueuedFetcher();
    const q = query(qc, { key: ["k"], fetcher: f.fetcher });
    effect(() => q.data());
    await tick();
    q.refetch();
    await tick();
    f.resolveNth(0, "stale");                                   // aborted fetch resolves
    f.resolveNth(1, "fresh");
    await tick();
    assert.equal(q.data(), "fresh", "stale resolution ignored");
    q.dispose();
});

// ─────────────────────────────────────────────────────────────────────────────
// Section 10 — query: refetch
// ─────────────────────────────────────────────────────────────────────────────

test("refetch: returns a promise that resolves with the new data", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createQueuedFetcher();
    const q = query(qc, { key: ["k"], fetcher: f.fetcher });
    effect(() => q.data());
    await tick();
    f.resolveNth(0, "first");
    await tick();
    const p = q.refetch();
    await tick();
    f.resolveNth(1, "second");
    const result = await p;
    assert.equal(result, "second");
    q.dispose();
});

test("refetch: ignores staleTime — always fetches", async () => {
    const { qc } = setupMockEnv(queryClient, { defaultStaleTime: 100_000 });
    const f = createQueuedFetcher();
    const q = query(qc, { key: ["k"], fetcher: f.fetcher });
    effect(() => q.data());
    await tick();
    f.resolveNth(0, "v");
    await tick();
    q.refetch();
    await tick();
    assert.equal(f.callCount, 2);
    q.dispose();
});

test("refetch: rejects with the error if fetch fails", async () => {
    const { qc } = setupMockEnv(queryClient, { retry: 0 });
    const f = createQueuedFetcher();
    const q = query(qc, { key: ["k"], fetcher: f.fetcher });
    effect(() => q.data());
    await tick();
    f.resolveNth(0, "v");
    await tick();
    const p = q.refetch();
    await tick();
    f.rejectNth(1, new Error("nope"));
    await assert.rejects(p, /nope/);
    q.dispose();
});

// ─────────────────────────────────────────────────────────────────────────────
// Section 11 — query: dispose
// ─────────────────────────────────────────────────────────────────────────────

test("dispose: detaches observers; observer count drops to 0", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createControlledFetcher();
    const q = query(qc, { key: ["k"], fetcher: f.fetcher });
    const stop = effect(() => q.data());
    await tick();
    stop();
    q.dispose();
    f.resolve("v");
    await tick();
    // No effect re-fires; this is structural — q.dispose() is idempotent
    assert.doesNotThrow(() => q.dispose());
});

test("dispose: pending fetch is aborted", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createControlledFetcher();
    const q = query(qc, { key: ["k"], fetcher: f.fetcher });
    effect(() => q.data());
    await tick();
    const ac = f.lastCall.signal;
    q.dispose();
    assert.equal(ac.aborted, true);
});

test("dispose: with shared observers, disposing one does NOT abort the fetch", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createControlledFetcher();
    const q1 = query(qc, { key: ["shared"], fetcher: f.fetcher });
    const q2 = query(qc, { key: ["shared"], fetcher: f.fetcher });
    effect(() => q1.data());
    effect(() => q2.data());
    await tick();
    assert.equal(f.callCount, 1, "dedup'd: one fetch for two observers");
    const ac = f.lastCall.signal;
    q1.dispose();
    await tick();
    assert.equal(ac.aborted, false, "fetch alive while q2 still observing");
    f.resolve("v");
    await tick();
    assert.equal(q2.data(), "v", "remaining observer received the data");
    q2.dispose();
});

test("refcount: GC timer does NOT start while any observer is still active", async () => {
    const { qc, clock } = setupMockEnv(queryClient, { defaultCacheTime: 1000 });
    const f = createControlledFetcher();
    const q1 = query(qc, { key: ["k"], fetcher: f.fetcher });
    const q2 = query(qc, { key: ["k"], fetcher: f.fetcher });
    const stop1 = effect(() => q1.data());
    const stop2 = effect(() => q2.data());
    await tick();
    f.resolve("v");
    await tick();

    stop1(); q1.dispose();                                      // count: 2 → 1
    clock.advance(5000);                                        // way past cacheTime
    assert.equal(qc.getQueryData(["k"]), "v", "entry alive while q2 observing");

    stop2(); q2.dispose();                                      // count: 1 → 0; timer starts
    clock.advance(1100);
    assert.equal(qc.getQueryData(["k"]), undefined, "GC fired only after last observer left");
});

// ─────────────────────────────────────────────────────────────────────────────
// Section 11.5 — structural sharing / zero-GC assertions
// ─────────────────────────────────────────────────────────────────────────────

test("structural sharing: refetch returning the cached reference does not re-fire observers", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createQueuedFetcher();
    const q = query(qc, { key: ["k"], fetcher: f.fetcher });
    let runs = 0;
    effect(() => { runs++; q.data(); });
    await tick();
    const value = { items: [1, 2, 3] };
    f.resolveNth(0, value);
    await tick();
    const after = runs;

    q.refetch();
    await tick();
    f.resolveNth(1, value);                                     // SAME reference
    await tick();
    assert.equal(runs, after, "no re-fire — value is referentially equal (Object.is)");
    q.dispose();
});

test("structural sharing: `equals` opt-in skips re-fire on structurally-equal data", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createQueuedFetcher();
    const q = query(qc, {
        key: ["k"],
        fetcher: f.fetcher,
        equals: (a, b) => JSON.stringify(a) === JSON.stringify(b),
    });
    let runs = 0;
    effect(() => { runs++; q.data(); });
    await tick();
    f.resolveNth(0, { items: [1, 2, 3] });
    await tick();
    const after = runs;

    q.refetch();
    await tick();
    f.resolveNth(1, { items: [1, 2, 3] });                      // structurally equal, NEW reference
    await tick();
    assert.equal(runs, after, "no re-fire — opt-in `equals` matched");
    q.dispose();
});

test("structural sharing: signal count does not inflate across redundant refetches", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createQueuedFetcher();
    const q = query(qc, { key: ["k"], fetcher: f.fetcher });
    effect(() => q.data());
    await tick();
    const sharedValue = "v";
    f.resolveNth(0, sharedValue);
    await tick();
    const baseline = stats().signals;

    for (let i = 1; i <= 5; i++) {
        q.refetch();
        await tick();
        f.resolveNth(i, sharedValue);                           // identical reference each time
        await tick();
    }
    assert.equal(stats().signals, baseline, "no signal allocation from redundant refetches");
    q.dispose();
});

// ─────────────────────────────────────────────────────────────────────────────
// Section 12 — cache: getQueryData / setQueryData
// ─────────────────────────────────────────────────────────────────────────────

test("setQueryData: writes data accessible via getQueryData", () => {
    const { qc } = setupMockEnv(queryClient);
    qc.setQueryData(["k"], "value");
    assert.equal(qc.getQueryData(["k"]), "value");
});

test("setQueryData: updater fn receives previous value", () => {
    const { qc } = setupMockEnv(queryClient);
    qc.setQueryData(["count"], 5);
    qc.setQueryData(["count"], (prev) => prev + 1);
    assert.equal(qc.getQueryData(["count"]), 6);
});

test("setQueryData: updater fn called with undefined for nonexistent key", () => {
    const { qc } = setupMockEnv(queryClient);
    qc.setQueryData(["new"], (prev) => prev ?? "default");
    assert.equal(qc.getQueryData(["new"]), "default");
});

test("setQueryData: notifies observers of matching active queries", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createControlledFetcher();
    const q = query(qc, { key: ["k"], fetcher: f.fetcher });
    let observed;
    effect(() => { observed = q.data(); });
    await tick();
    f.resolve("a");
    await tick();
    qc.setQueryData(["k"], "b");                                // direct cache write
    await tick();
    assert.equal(observed, "b", "active observer saw the cache update");
    q.dispose();
});

test("getQueryData: returns undefined for unknown keys", () => {
    const { qc } = setupMockEnv(queryClient);
    assert.equal(qc.getQueryData(["never-set"]), undefined);
});

// ─────────────────────────────────────────────────────────────────────────────
// Section 13 — cache: invalidate
// ─────────────────────────────────────────────────────────────────────────────

test("invalidate: exact match triggers refetch on active queries", async () => {
    const { qc } = setupMockEnv(queryClient, { defaultStaleTime: 100_000 });
    const f = createQueuedFetcher();
    const q = query(qc, { key: ["k"], fetcher: f.fetcher });
    effect(() => q.data());
    await tick();
    f.resolveNth(0, "v");
    await tick();
    qc.invalidate(["k"]);
    await tick();
    assert.equal(f.callCount, 2);
    q.dispose();
});

test("invalidate: prefix match invalidates all matching keys (default behavior)", async () => {
    const { qc } = setupMockEnv(queryClient, { defaultStaleTime: 100_000 });
    const f = createQueuedFetcher();
    const a = query(qc, { key: ["users", 1], fetcher: f.fetcher });
    const b = query(qc, { key: ["users", 2], fetcher: f.fetcher });
    const c = query(qc, { key: ["posts"], fetcher: f.fetcher });
    effect(() => a.data());
    effect(() => b.data());
    effect(() => c.data());
    await tick();
    f.resolveNth(0, "a"); f.resolveNth(1, "b"); f.resolveNth(2, "c");
    await tick();
    qc.invalidate(["users"]);                                   // prefix match
    await tick();
    assert.equal(f.callCount, 5, "users/1 and users/2 refetched; posts untouched");
    a.dispose(); b.dispose(); c.dispose();
});

test("invalidate: exact: true skips prefix matches", async () => {
    const { qc } = setupMockEnv(queryClient, { defaultStaleTime: 100_000 });
    const f = createQueuedFetcher();
    const a = query(qc, { key: ["users"], fetcher: f.fetcher });
    const b = query(qc, { key: ["users", 1], fetcher: f.fetcher });
    effect(() => a.data());
    effect(() => b.data());
    await tick();
    f.resolveNth(0, "a"); f.resolveNth(1, "b");
    await tick();
    qc.invalidate(["users"], { exact: true });
    await tick();
    assert.equal(f.callCount, 3, "only ['users'] refetched");
    a.dispose(); b.dispose();
});

test("invalidate: marks data stale even without active observers", async () => {
    const { qc, clock } = setupMockEnv(queryClient, { defaultStaleTime: 100_000 });
    const f = createQueuedFetcher();
    const q1 = query(qc, { key: ["k"], fetcher: f.fetcher });
    const stop1 = effect(() => q1.data());
    await tick();
    f.resolveNth(0, "v");
    await tick();
    stop1(); q1.dispose();

    qc.invalidate(["k"]);                                       // no active observers
    // Reattach: should refetch because entry is marked stale
    const q2 = query(qc, { key: ["k"], fetcher: f.fetcher });
    effect(() => q2.data());
    await tick();
    assert.equal(f.callCount, 2);
    q2.dispose();
});

test("invalidate: while a fetch is in flight, queues a follow-up refetch after it settles", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createQueuedFetcher();
    const q = query(qc, { key: ["k"], fetcher: f.fetcher });
    effect(() => q.data());
    await tick();
    qc.invalidate(["k"]);                                       // invalidate mid-flight
    f.resolveNth(0, "first");
    await tick();
    // The invalidation queues a refetch that fires AFTER the in-flight settles.
    // Locked-in semantic: option (b) — let finish + refetch. Not abort+restart.
    assert.equal(f.callCount, 2, "exactly two fetches: in-flight + follow-up");
    q.dispose();
});

test("invalidate: no matching keys is a silent no-op", () => {
    const { qc } = setupMockEnv(queryClient);
    assert.doesNotThrow(() => qc.invalidate(["nonexistent"]));
});

// ─────────────────────────────────────────────────────────────────────────────
// Section 14 — cache: removeQueries / clear
// ─────────────────────────────────────────────────────────────────────────────

test("removeQueries: prefix match drops entries from the cache", async () => {
    const { qc } = setupMockEnv(queryClient);
    qc.setQueryData(["users", 1], "alice");
    qc.setQueryData(["users", 2], "bob");
    qc.setQueryData(["posts"], ["p1"]);
    qc.removeQueries(["users"]);
    assert.equal(qc.getQueryData(["users", 1]), undefined);
    assert.equal(qc.getQueryData(["users", 2]), undefined);
    assert.deepEqual(qc.getQueryData(["posts"]), ["p1"]);
});

test("removeQueries: exact: true drops only the precise key", () => {
    const { qc } = setupMockEnv(queryClient);
    qc.setQueryData(["users"], ["all"]);
    qc.setQueryData(["users", 1], "alice");
    qc.removeQueries(["users"], { exact: true });
    assert.equal(qc.getQueryData(["users"]), undefined);
    assert.equal(qc.getQueryData(["users", 1]), "alice");
});

test("clear: nukes every cache entry", () => {
    const { qc } = setupMockEnv(queryClient);
    qc.setQueryData(["a"], 1);
    qc.setQueryData(["b"], 2);
    qc.setQueryData(["nested", "deep"], 3);
    qc.clear();
    assert.equal(qc.getQueryData(["a"]), undefined);
    assert.equal(qc.getQueryData(["b"]), undefined);
    assert.equal(qc.getQueryData(["nested", "deep"]), undefined);
});

// ─────────────────────────────────────────────────────────────────────────────
// Section 15 — mutation: basic flow
// ─────────────────────────────────────────────────────────────────────────────

test("mutation: calling .mutate() triggers fn with the vars", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createControlledFetcher();
    const m = mutation(qc, { fn: (vars) => f.fetcher({ key: ["mut"], vars }) });
    const p = m.mutate({ text: "hi" });
    await tick();
    assert.equal(f.callCount, 1);
    assert.deepEqual(f.lastCall.vars, { text: "hi" });
    f.resolve("ok");
    await p;
});

test("mutation: status transitions idle → pending → success", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createControlledFetcher();
    const m = mutation(qc, { fn: () => f.fetcher({ key: ["m"] }) });
    const seen = [];
    effect(() => { seen.push(m.status()); });
    const p = m.mutate();
    await tick();
    f.resolve("ok");
    await p;
    assert.ok(seen.includes("idle"));
    assert.ok(seen.includes("pending"));
    assert.equal(seen[seen.length - 1], "success");
});

test("mutation: error path exposes error", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createControlledFetcher();
    const m = mutation(qc, { fn: () => f.fetcher({ key: ["m"] }) });
    const p = m.mutate().catch(() => {});
    await tick();
    f.reject(new Error("nope"));
    await p;
    assert.equal(m.status(), "error");
    assert.equal(m.error().message, "nope");
});

test("mutation: .mutate() returns the resolved value as the promise's value", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createControlledFetcher();
    const m = mutation(qc, { fn: () => f.fetcher({ key: ["m"] }) });
    const p = m.mutate({ x: 1 });
    await tick();
    f.resolve({ id: 42 });
    const result = await p;
    assert.deepEqual(result, { id: 42 });
});

test("mutation: reset() clears data, error, and status", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createControlledFetcher();
    const m = mutation(qc, { fn: () => f.fetcher({ key: ["m"] }) });
    const p = m.mutate();
    await tick();
    f.resolve("ok");
    await p;
    assert.equal(m.data(), "ok");
    m.reset();
    assert.equal(m.data(), undefined);
    assert.equal(m.error(), undefined);
    assert.equal(m.status(), "idle");
});

// ─────────────────────────────────────────────────────────────────────────────
// Section 16 — mutation: callbacks
// ─────────────────────────────────────────────────────────────────────────────

test("mutation: onMutate fires before fn with the vars", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createControlledFetcher();
    const events = [];
    const m = mutation(qc, {
        fn: (vars) => { events.push(["fn", vars]); return f.fetcher({}); },
        onMutate: (vars) => { events.push(["onMutate", vars]); },
    });
    const p = m.mutate({ text: "x" });
    await tick();
    f.resolve("ok");
    await p;
    assert.equal(events[0][0], "onMutate");
    assert.equal(events[1][0], "fn");
});

test("mutation: onMutate's returned context is passed to onSuccess / onError", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createControlledFetcher();
    let receivedCtx = null;
    const m = mutation(qc, {
        fn: () => f.fetcher({}),
        onMutate: () => ({ rollback: "saved-state" }),
        onSuccess: (data, vars, ctx) => { receivedCtx = ctx; },
    });
    const p = m.mutate({});
    await tick();
    f.resolve("ok");
    await p;
    assert.deepEqual(receivedCtx, { rollback: "saved-state" });
});

test("mutation: onError receives error, vars, and onMutate context", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createControlledFetcher();
    const captured = {};
    const m = mutation(qc, {
        fn: () => f.fetcher({}),
        onMutate: (vars) => ({ pre: vars.x }),
        onError: (err, vars, ctx) => { captured.err = err; captured.vars = vars; captured.ctx = ctx; },
    });
    const p = m.mutate({ x: 42 }).catch(() => {});
    await tick();
    f.reject(new Error("boom"));
    await p;
    assert.equal(captured.err.message, "boom");
    assert.deepEqual(captured.vars, { x: 42 });
    assert.deepEqual(captured.ctx, { pre: 42 });
});

test("mutation: onSettled fires after both success and error paths", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f1 = createControlledFetcher();
    let settledTimes = 0;
    const m = mutation(qc, {
        fn: () => f1.fetcher({}),
        onSettled: () => { settledTimes++; },
    });
    const p1 = m.mutate();
    await tick();
    f1.resolve("ok");
    await p1;
    assert.equal(settledTimes, 1);

    const f2 = createControlledFetcher();
    const m2 = mutation(qc, {
        fn: () => f2.fetcher({}),
        onSettled: () => { settledTimes++; },
    });
    const p2 = m2.mutate().catch(() => {});
    await tick();
    f2.reject(new Error("e"));
    await p2;
    assert.equal(settledTimes, 2, "onSettled fires on errors too");
});

// ─────────────────────────────────────────────────────────────────────────────
// Section 17 — mutation: optimistic updates + rollback
// ─────────────────────────────────────────────────────────────────────────────

test("optimistic: onMutate's setQueryData is visible immediately", async () => {
    const { qc } = setupMockEnv(queryClient);
    qc.setQueryData(["todos"], [{ id: 1, text: "a" }]);
    const f = createControlledFetcher();
    const m = mutation(qc, {
        fn: (vars) => f.fetcher({ vars }),
        onMutate: (vars) => {
            qc.setQueryData(["todos"], (prev) => [...prev, { id: -1, text: vars.text, optimistic: true }]);
        },
    });
    m.mutate({ text: "b" });
    await tick();
    const data = qc.getQueryData(["todos"]);
    assert.equal(data.length, 2);
    assert.equal(data[1].optimistic, true);
    f.resolve({ id: 2, text: "b" });
});

test("optimistic: onError can roll back using context from onMutate", async () => {
    const { qc } = setupMockEnv(queryClient);
    qc.setQueryData(["todos"], ["a"]);
    const f = createControlledFetcher();
    const m = mutation(qc, {
        fn: () => f.fetcher({}),
        onMutate: (vars) => {
            const prev = qc.getQueryData(["todos"]);
            qc.setQueryData(["todos"], [...prev, "optimistic"]);
            return { prev };
        },
        onError: (err, vars, ctx) => {
            qc.setQueryData(["todos"], ctx.prev);                // rollback
        },
    });
    const p = m.mutate({}).catch(() => {});
    await tick();
    assert.deepEqual(qc.getQueryData(["todos"]), ["a", "optimistic"]);
    f.reject(new Error("nope"));
    await p;
    assert.deepEqual(qc.getQueryData(["todos"]), ["a"], "rolled back");
});

test("optimistic: invalidate in onSuccess triggers refetch", async () => {
    const { qc } = setupMockEnv(queryClient, { defaultStaleTime: 100_000 });
    const queryFetcher = createQueuedFetcher();
    const q = query(qc, { key: ["todos"], fetcher: queryFetcher.fetcher });
    effect(() => q.data());
    await tick();
    queryFetcher.resolveNth(0, ["a"]);
    await tick();

    const mutFetcher = createControlledFetcher();
    const m = mutation(qc, {
        fn: () => mutFetcher.fetcher({}),
        onSuccess: () => { qc.invalidate(["todos"]); },
    });
    const p = m.mutate({});
    await tick();
    mutFetcher.resolve("done");
    await p;
    await tick();
    assert.equal(queryFetcher.callCount, 2, "query refetched after mutation invalidated");
    q.dispose();
});

// ─────────────────────────────────────────────────────────────────────────────
// Section 18 — cross-tab: opt-in
// ─────────────────────────────────────────────────────────────────────────────

test("crossTab: off by default — no BroadcastChannel created", () => {
    let bcInstances = 0;
    class TBC { constructor() { bcInstances++; } postMessage() {} addEventListener() {} close() {} }
    queryClient({ broadcastChannel: TBC });
    assert.equal(bcInstances, 0);
});

test("crossTab: on — exactly one channel per client instance", () => {
    let bcInstances = 0;
    class TBC { constructor() { bcInstances++; } postMessage() {} addEventListener() {} close() {} }
    queryClient({ crossTab: true, broadcastChannel: TBC });
    queryClient({ crossTab: true, broadcastChannel: TBC });
    assert.equal(bcInstances, 2, "one channel per client");
});

// ─────────────────────────────────────────────────────────────────────────────
// Section 19 — cross-tab: invalidate broadcasts
// ─────────────────────────────────────────────────────────────────────────────

test("crossTab: invalidate in tab A causes tab B to refetch matching queries", async () => {
    const mockBC = createMockBroadcastChannel();
    const opts = { crossTab: true, broadcastChannel: mockBC.BroadcastChannel, crossTabChannel: "x", defaultStaleTime: 100_000 };

    const qcA = queryClient(opts);
    const qcB = queryClient(opts);

    const fB = createQueuedFetcher();
    const qB = query(qcB, { key: ["users"], fetcher: fB.fetcher });
    effect(() => qB.data());
    await tick();
    fB.resolveNth(0, ["alice"]);
    await tick();

    qcA.invalidate(["users"]);
    await tick();
    await tick();                                                // BroadcastChannel delivers async
    assert.equal(fB.callCount, 2, "tab B refetched after tab A invalidated");
    qB.dispose();
});

test("crossTab: invalidate prefix-match propagates to tab B", async () => {
    const mockBC = createMockBroadcastChannel();
    const opts = { crossTab: true, broadcastChannel: mockBC.BroadcastChannel, crossTabChannel: "x", defaultStaleTime: 100_000 };
    const qcA = queryClient(opts);
    const qcB = queryClient(opts);

    const fB = createQueuedFetcher();
    const q1 = query(qcB, { key: ["users", 1], fetcher: fB.fetcher });
    const q2 = query(qcB, { key: ["users", 2], fetcher: fB.fetcher });
    effect(() => q1.data());
    effect(() => q2.data());
    await tick();
    fB.resolveNth(0, "a"); fB.resolveNth(1, "b");
    await tick();

    qcA.invalidate(["users"]);                                   // prefix
    await tick(); await tick();
    assert.equal(fB.callCount, 4);
    q1.dispose(); q2.dispose();
});

test("crossTab: setQueryData in tab A updates tab B's cache", async () => {
    const mockBC = createMockBroadcastChannel();
    const opts = { crossTab: true, broadcastChannel: mockBC.BroadcastChannel, crossTabChannel: "x" };
    const qcA = queryClient(opts);
    const qcB = queryClient(opts);

    qcA.setQueryData(["users"], ["alice"]);
    await tick(); await tick();
    assert.deepEqual(qcB.getQueryData(["users"]), ["alice"]);
});

test("crossTab: removeQueries in tab A removes from tab B", async () => {
    const mockBC = createMockBroadcastChannel();
    const opts = { crossTab: true, broadcastChannel: mockBC.BroadcastChannel, crossTabChannel: "x" };
    const qcA = queryClient(opts);
    const qcB = queryClient(opts);

    qcB.setQueryData(["users"], ["alice"]);
    qcA.removeQueries(["users"]);
    await tick(); await tick();
    assert.equal(qcB.getQueryData(["users"]), undefined);
});

test("crossTab: messages from tab A don't cause tab A to loop back", async () => {
    const mockBC = createMockBroadcastChannel();
    const opts = { crossTab: true, broadcastChannel: mockBC.BroadcastChannel, crossTabChannel: "x", defaultStaleTime: 100_000 };
    const qcA = queryClient(opts);

    const fA = createQueuedFetcher();
    const q = query(qcA, { key: ["k"], fetcher: fA.fetcher });
    effect(() => q.data());
    await tick();
    fA.resolveNth(0, "v");
    await tick();

    qcA.invalidate(["k"]);                                       // local invalidate
    await tick(); await tick();
    // Should refetch exactly once (local), NOT twice (local + echo from broadcast)
    assert.equal(fA.callCount, 2);
    q.dispose();
});

test("crossTab: different channel names don't cross-fire", async () => {
    const mockBC = createMockBroadcastChannel();
    const optsA = { crossTab: true, broadcastChannel: mockBC.BroadcastChannel, crossTabChannel: "alpha", defaultStaleTime: 100_000 };
    const optsB = { crossTab: true, broadcastChannel: mockBC.BroadcastChannel, crossTabChannel: "beta",  defaultStaleTime: 100_000 };
    const qcA = queryClient(optsA);
    const qcB = queryClient(optsB);

    const fB = createControlledFetcher();
    const qB = query(qcB, { key: ["k"], fetcher: fB.fetcher });
    effect(() => qB.data());
    await tick();
    fB.resolve("v");
    await tick();

    qcA.invalidate(["k"]);
    await tick(); await tick();
    assert.equal(fB.callCount, 1, "different channels are isolated");
    qB.dispose();
});

test("crossTab: simultaneous fetch in two tabs resolves last-wins; no broadcast loop", async () => {
    const mockBC = createMockBroadcastChannel();
    const opts = { crossTab: true, broadcastChannel: mockBC.BroadcastChannel, crossTabChannel: "x" };
    const qcA = queryClient(opts);
    const qcB = queryClient(opts);

    // Both tabs mount the same key — independent caches, each fetches separately
    const fA = createControlledFetcher();
    const fB = createControlledFetcher();
    const qA = query(qcA, { key: ["k"], fetcher: fA.fetcher });
    const qB = query(qcB, { key: ["k"], fetcher: fB.fetcher });
    effect(() => qA.data());
    effect(() => qB.data());
    await tick();

    assert.equal(fA.callCount, 1, "each tab fetches independently");
    assert.equal(fB.callCount, 1);

    // Resolve A first, then B — neither broadcasts background fetch results,
    // so each tab's local cache holds its own value (no last-wins overwrite for
    // background fetches — that's the locked-in semantic).
    fA.resolve("from-A");
    await tick(); await tick();
    fB.resolve("from-B");
    await tick(); await tick();

    assert.equal(qA.data(), "from-A");
    assert.equal(qB.data(), "from-B");

    // Now explicit setQueryData DOES propagate. Tab A writes; B should see it.
    qcA.setQueryData(["k"], "explicit-A");
    await tick(); await tick();
    assert.equal(qcB.getQueryData(["k"]), "explicit-A", "explicit set propagates");

    // And no infinite loop: tab B's local cache update from the broadcast
    // did NOT re-broadcast. We assert by ensuring tab A's value is still
    // what A wrote, not something B might have echoed back.
    assert.equal(qcA.getQueryData(["k"]), "explicit-A");
    qA.dispose(); qB.dispose();
});

// ─────────────────────────────────────────────────────────────────────────────
// Section 20 — sanity & integration
// ─────────────────────────────────────────────────────────────────────────────

test("mutation: concurrent mutate() calls — latest wins on state, but each promise reflects its own outcome", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createQueuedFetcher();
    const m = mutation(qc, { fn: (vars) => f.fetcher({ vars }) });

    const p1 = m.mutate({ id: 1 }).catch((e) => ({ err: e }));
    await tick();
    const p2 = m.mutate({ id: 2 });
    await tick();
    assert.equal(f.callCount, 2, "both fetcher calls fired");

    // Resolve p2 FIRST (the latest mutation), then p1 LATER
    f.resolveNth(1, "second");
    await tick();
    assert.equal(m.data(), "second");
    assert.equal(m.status(), "success");

    // Now resolve the older one — its result is for p1's promise, but state
    // must NOT regress to it.
    f.resolveNth(0, "first");
    const r1 = await p1;
    const r2 = await p2;

    assert.equal(r2, "second", "p2 resolves with its own value");
    assert.equal(r1, "first",  "p1 resolves with its own value");
    assert.equal(m.data(), "second", "state still shows the latest mutation's data");
    assert.equal(m.status(), "success");
});

test("mutation: concurrent mutate() — fast success then slow error: state stays success", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createQueuedFetcher();
    const m = mutation(qc, { fn: () => f.fetcher({}) });

    const p1 = m.mutate({ x: 1 }).catch(() => {});           // will reject later
    await tick();
    const p2 = m.mutate({ x: 2 });
    await tick();

    f.resolveNth(1, "ok");                                   // p2 succeeds first
    await p2;
    assert.equal(m.status(), "success");
    assert.equal(m.data(), "ok");

    // Now p1 fails (stale, slow)
    f.rejectNth(0, new Error("stale failure"));
    await p1;
    assert.equal(m.status(), "success", "stale error did NOT overwrite latest success");
    assert.equal(m.error(), undefined);
});

test("mutation: onSuccess throw does NOT flip status to error, onSettled still fires", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createControlledFetcher();
    let settledFired = false;
    const m = mutation(qc, {
        fn: () => f.fetcher({}),
        onSuccess: () => { throw new Error("user bug in onSuccess"); },
        onSettled: () => { settledFired = true; },
    });
    const p = m.mutate({});
    await tick();
    f.resolve("data");
    await p;                                                 // resolves successfully — callback errors contained
    assert.equal(m.status(), "success", "status preserved despite onSuccess throw");
    assert.equal(m.data(), "data");
    assert.equal(settledFired, true, "onSettled fires even when onSuccess threw");
});

test("mutation: onError throw still allows onSettled to fire", async () => {
    const { qc } = setupMockEnv(queryClient);
    const f = createControlledFetcher();
    let settledArgs = null;
    const m = mutation(qc, {
        fn: () => f.fetcher({}),
        onError: () => { throw new Error("bug in onError"); },
        onSettled: (data, err) => { settledArgs = { data, err }; },
    });
    const p = m.mutate({}).catch(() => {});
    await tick();
    f.reject(new Error("real failure"));
    await p;
    assert.ok(settledArgs, "onSettled fired");
    assert.equal(settledArgs.err.message, "real failure");
    assert.equal(settledArgs.data, undefined);
});

test("timeout: per-query timeout aborts the fetch with ABORT_REASON.TIMEOUT on signal.reason", async () => {
    const { qc, clock } = setupMockEnv(queryClient, { retry: 0 });
    const f = createControlledFetcher();
    let abortReason = null;
    const fetcher = (ctx) => {
        ctx.signal.addEventListener("abort", () => { abortReason = ctx.signal.reason; });
        return f.fetcher(ctx);
    };
    const q = query(qc, { key: ["k"], fetcher, timeout: 5000 });
    effect(() => { q.data(); q.error(); });
    await tick();
    assert.equal(q.fetching(), true);

    clock.advance(5000);                                     // exceed timeout
    await tick();
    assert.equal(abortReason, "lite-query:timeout", "signal.reason carries timeout marker");
    q.dispose();
});

test("timeout: client default timeout applies when no per-query override", async () => {
    const { qc, clock } = setupMockEnv(queryClient, { defaultTimeout: 1000, retry: 0 });
    const f = createControlledFetcher();
    let abortReason = null;
    const fetcher = (ctx) => {
        ctx.signal.addEventListener("abort", () => { abortReason = ctx.signal.reason; });
        return f.fetcher(ctx);
    };
    const q = query(qc, { key: ["k"], fetcher });            // no timeout — inherits 1000
    effect(() => q.data());
    await tick();
    clock.advance(1000);
    await tick();
    assert.equal(abortReason, "lite-query:timeout");
    q.dispose();
});

test("abort reasons: dispose carries DETACH, refetch carries REFETCH, removeQueries carries REMOVED", async () => {
    const { qc } = setupMockEnv(queryClient);
    const detachReasons = [];
    const refetchReasons = [];
    const removeReasons = [];

    // DETACH path
    {
        const f = createControlledFetcher();
        const fetcher = (ctx) => {
            ctx.signal.addEventListener("abort", () => { detachReasons.push(ctx.signal.reason); });
            return f.fetcher(ctx);
        };
        const q = query(qc, { key: ["a"], fetcher });
        effect(() => q.data());
        await tick();
        q.dispose();
        await tick();
    }
    assert.equal(detachReasons[0], "lite-query:detach");

    // REFETCH path
    {
        const f = createQueuedFetcher();
        const fetcher = (ctx) => {
            ctx.signal.addEventListener("abort", () => { refetchReasons.push(ctx.signal.reason); });
            return f.fetcher(ctx);
        };
        const q = query(qc, { key: ["b"], fetcher });
        effect(() => q.data());
        await tick();
        q.refetch().catch(() => {});                         // aborts the first fetch
        await tick();
        q.dispose();
    }
    assert.equal(refetchReasons[0], "lite-query:refetch");

    // REMOVED path
    {
        const f = createControlledFetcher();
        const fetcher = (ctx) => {
            ctx.signal.addEventListener("abort", () => { removeReasons.push(ctx.signal.reason); });
            return f.fetcher(ctx);
        };
        const q = query(qc, { key: ["c"], fetcher });
        effect(() => q.data());
        await tick();
        qc.removeQueries(["c"]);
        await tick();
        q.dispose();
    }
    assert.equal(removeReasons[0], "lite-query:removed");
});

test("queryClient.dispose(): clears cache and detaches BroadcastChannel listener", async () => {
    let lastListener = null;
    let removed = false;
    let closed = false;
    class TrackedBC {
        constructor(name) { this.name = name; }
        postMessage() {}
        addEventListener(_evt, fn) { lastListener = fn; }
        removeEventListener(_evt, fn) {
            if (fn === lastListener) removed = true;
        }
        close() { closed = true; }
    }

    const qc = queryClient({
        crossTab: true,
        broadcastChannel: TrackedBC,
        crossTabChannel: "x",
    });
    qc.setQueryData(["k"], "v");
    assert.equal(qc.getQueryData(["k"]), "v");

    qc.dispose();
    assert.equal(qc.getQueryData(["k"]), undefined, "cache cleared on dispose");
    assert.equal(removed, true, "listener removed from channel");
    assert.equal(closed, true, "channel closed");
});

// ─────────────────────────────────────────────────────────────────────────────
// Section 21 — cross-tab fetch deduplication (sharedFetch + leader election)
// ─────────────────────────────────────────────────────────────────────────────

function setupSharedFetch({ leaderHasQuery = true } = {}) {
    const mockBC = createMockBroadcastChannel();
    const clock = createMockClock();
    const base = {
        crossTab: true,
        sharedFetch: true,
        broadcastChannel: mockBC.BroadcastChannel,
        crossTabChannel: "shared",
        now: clock.now,
        setTimeout: clock.setTimeout,
        clearTimeout: clock.clearTimeout,
        sharedFetchTimeout: 3000,
        defaultStaleTime: 100_000,
    };
    const qcLeader = queryClient({ ...base, isLeader: () => true });
    const qcFollower = queryClient({ ...base, isLeader: () => false });
    return { qcLeader, qcFollower, mockBC, clock };
}

test("sharedFetch: follower does NOT call its own fetcher — receives the leader's broadcast", async () => {
    const { qcLeader, qcFollower } = setupSharedFetch();
    const leaderF = createControlledFetcher();
    const followerF = createControlledFetcher();

    // Both tabs observe the same key. Leader has a real fetcher; follower's
    // fetcher should never be invoked.
    const qL = query(qcLeader,  { key: ["data"], fetcher: leaderF.fetcher });
    const qF = query(qcFollower, { key: ["data"], fetcher: followerF.fetcher });
    effect(() => qL.data());
    effect(() => qF.data());
    await tick();

    // Leader fetched; follower issued a fetch-req instead of fetching.
    assert.equal(leaderF.callCount, 1, "leader fetched once");
    assert.equal(followerF.callCount, 0, "follower did NOT fetch");

    // Leader resolves → broadcasts result → follower receives it.
    leaderF.resolve({ value: 42 });
    await tick(); await tick();

    assert.deepEqual(qF.data(), { value: 42 }, "follower got the leader's data");
    assert.equal(followerF.callCount, 0, "follower still never fetched");
    qL.dispose(); qF.dispose();
});

test("sharedFetch: follower request is fulfilled by leader that isn't currently observing", async () => {
    const { qcLeader, qcFollower } = setupSharedFetch();
    const leaderF = createControlledFetcher();
    const followerF = createControlledFetcher();

    // Leader has the query DEFINED (so it has a fetcher in the entry) but we
    // dispose its observer so it isn't actively watching. The follower's
    // fetch-req should still be fulfilled because the entry survives within
    // cacheTime.
    const qL = query(qcLeader, { key: ["data"], fetcher: leaderF.fetcher });
    const stopL = effect(() => qL.data());
    await tick();
    leaderF.resolve({ v: 1 });
    await tick();
    stopL();                                                 // leader stops observing (entry stays cached)

    // Follower now mounts and needs data
    const qF = query(qcFollower, { key: ["data"], fetcher: followerF.fetcher });
    effect(() => qF.data());
    await tick(); await tick();

    // The leader fulfilled the request from its cached entry's fetcher
    assert.equal(followerF.callCount, 0, "follower didn't self-fetch");
    assert.ok(leaderF.callCount >= 1, "leader did the fetching");
    qL.dispose(); qF.dispose();
});

test("sharedFetch: follower self-fetches (fallback) when no leader can fulfill", async () => {
    // Only a follower exists — no leader on the channel at all.
    const mockBC = createMockBroadcastChannel();
    const clock = createMockClock();
    const qcFollower = queryClient({
        crossTab: true,
        sharedFetch: true,
        broadcastChannel: mockBC.BroadcastChannel,
        crossTabChannel: "shared",
        now: clock.now,
        setTimeout: clock.setTimeout,
        clearTimeout: clock.clearTimeout,
        sharedFetchTimeout: 3000,
        isLeader: () => false,                               // perpetual follower
    });
    const followerF = createControlledFetcher();
    const q = query(qcFollower, { key: ["data"], fetcher: followerF.fetcher });
    effect(() => q.data());
    await tick();

    assert.equal(followerF.callCount, 0, "no immediate self-fetch — waiting for leader");

    // No leader responds. Advance past the fallback timeout.
    clock.advance(3000);
    await tick();
    assert.equal(followerF.callCount, 1, "fallback self-fetch fired after timeout");

    followerF.resolve({ v: 'self' });
    await tick();
    assert.deepEqual(q.data(), { v: 'self' });
    q.dispose();
});

test("sharedFetch: leader's own observed fetch broadcasts to followers (no fetch-req needed)", async () => {
    const { qcLeader, qcFollower } = setupSharedFetch();
    const leaderF = createQueuedFetcher();
    const followerF = createControlledFetcher();

    // Leader mounts first and fetches on its own
    const qL = query(qcLeader, { key: ["live"], fetcher: leaderF.fetcher });
    effect(() => qL.data());
    await tick();
    leaderF.resolveNth(0, "leader-value");
    await tick(); await tick();

    // Follower mounts later; its key already has data in the leader. The
    // follower requests, leader is fresh (staleTime 100s) so it returns
    // cached value via broadcast rather than refetching.
    const qF = query(qcFollower, { key: ["live"], fetcher: followerF.fetcher });
    effect(() => qF.data());
    await tick(); await tick();

    assert.equal(followerF.callCount, 0, "follower never fetched");
    assert.equal(qF.data(), "leader-value", "follower received leader's cached value");
    qL.dispose(); qF.dispose();
});

test("sharedFetch: disabled when no isLeader supplied — each tab fetches independently", async () => {
    const mockBC = createMockBroadcastChannel();
    const clock = createMockClock();
    const base = {
        crossTab: true,
        sharedFetch: true,                                   // on, but...
        broadcastChannel: mockBC.BroadcastChannel,
        crossTabChannel: "shared",
        now: clock.now,
        setTimeout: clock.setTimeout,
        clearTimeout: clock.clearTimeout,
        // ...no isLeader → sharedFetch is inert, falls back to per-tab fetch
    };
    const qcA = queryClient(base);
    const qcB = queryClient(base);
    const fA = createControlledFetcher();
    const fB = createControlledFetcher();

    const qA = query(qcA, { key: ["data"], fetcher: fA.fetcher });
    const qB = query(qcB, { key: ["data"], fetcher: fB.fetcher });
    effect(() => qA.data());
    effect(() => qB.data());
    await tick();

    assert.equal(fA.callCount, 1, "tab A fetched (no leader coordination)");
    assert.equal(fB.callCount, 1, "tab B fetched (no leader coordination)");
    qA.dispose(); qB.dispose();
});

test("sharedFetch: follower.refetch() defers to the leader (no local network call)", async () => {
    const { qcLeader, qcFollower } = setupSharedFetch();
    const leaderF = createQueuedFetcher();
    const followerF = createControlledFetcher();

    const qL = query(qcLeader,  { key: ["m"], fetcher: leaderF.fetcher });
    const qF = query(qcFollower, { key: ["m"], fetcher: followerF.fetcher });
    effect(() => qL.data());
    effect(() => qF.data());
    await tick();
    // Leader serves the initial mount
    leaderF.resolveNth(0, "v1");
    await tick(); await tick();
    assert.equal(qF.data(), "v1");
    assert.equal(followerF.callCount, 0);

    // Follower explicitly refetches — must go through the leader, not locally.
    qF.refetch();
    await tick();
    assert.equal(followerF.callCount, 0, "follower.refetch did NOT self-fetch");
    // Leader fulfills the fetch-req
    leaderF.resolveNth(1, "v2");
    await tick(); await tick();
    assert.equal(qF.data(), "v2", "follower received refreshed value from leader");
    assert.equal(followerF.callCount, 0, "follower still never fetched");
    qL.dispose(); qF.dispose();
});

test("integration: full flow — fetch, mutate, optimistic, rollback, invalidate", async () => {
    const { qc } = setupMockEnv(queryClient, { defaultStaleTime: 100_000 });
    const qf = createQueuedFetcher();
    const mf = createControlledFetcher();

    const todos = query(qc, { key: ["todos"], fetcher: qf.fetcher });
    effect(() => todos.data());
    await tick();
    qf.resolveNth(0, ["a", "b"]);
    await tick();
    assert.deepEqual(todos.data(), ["a", "b"]);

    const add = mutation(qc, {
        fn: () => mf.fetcher({}),
        onMutate: () => {
            const prev = qc.getQueryData(["todos"]);
            qc.setQueryData(["todos"], [...prev, "optimistic"]);
            return { prev };
        },
        onError: (err, vars, ctx) => qc.setQueryData(["todos"], ctx.prev),
        onSuccess: () => qc.invalidate(["todos"]),
    });

    const p = add.mutate({ text: "c" });
    await tick();
    assert.deepEqual(qc.getQueryData(["todos"]), ["a", "b", "optimistic"]);
    mf.resolve({ id: 3 });
    await p;
    await tick();
    assert.equal(qf.callCount, 2, "invalidate triggered refetch");

    qf.resolveNth(1, ["a", "b", "c"]);
    await tick();
    assert.deepEqual(todos.data(), ["a", "b", "c"]);
    todos.dispose();
});
