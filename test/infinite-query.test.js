/**
 * @zakkster/lite-query -- infiniteQuery() + qc.prefetch() suite.
 *
 * infiniteQuery is the cursor-paginated sibling of query(): one cache entry
 * accumulates a list of pages. These tests pin the accumulation order, the flat
 * view, cursor exhaustion, in-flight dedup, the invalidate/refetch replace flow
 * with its generation guard, mid-page abort on detach / key-change, the error
 * ladder, the enabled gate, cross-tab page sync, and qc.prefetch adoption.
 */

import { test, beforeEach } from "node:test";
import assert from "node:assert/strict";

import { signal, effect, createRegistry, setDefaultRegistry } from "@zakkster/lite-signal";

import { queryClient, query, infiniteQuery } from "../Query.js";
import {
    createControlledFetcher,
    createQueuedFetcher,
    createMockClock,
    createMockBroadcastChannel,
} from "./harness.js";

beforeEach(() => setDefaultRegistry(createRegistry({ maxNodes: 16384 })));

const tick = () => new Promise((r) => queueMicrotask(r));

// Cursor is the page index; exhausts after `max` pages.
const cursorUpTo = (max) => (lastPage, allPages) => (allPages.length < max ? allPages.length : null);

// -----------------------------------------------------------------------------
// C1 -- runtime core
// -----------------------------------------------------------------------------

test("infiniteQuery: accumulates pages in fetch order", async () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    const src = [[1, 2, 3], [4, 5, 6], [7, 8, 9]];
    const q = infiniteQuery(qc, {
        key: ["feed"],
        fetcher: ({ cursor }) => Promise.resolve(src[cursor == null ? 0 : cursor]),
        getNextCursor: cursorUpTo(3),
    });
    const stop = effect(() => q.pages());
    await tick();
    assert.deepEqual(q.pages(), [[1, 2, 3]]);
    await q.fetchNextPage();
    await q.fetchNextPage();
    assert.deepEqual(q.pages(), [[1, 2, 3], [4, 5, 6], [7, 8, 9]]);
    stop();
    qc.dispose();
});

test("infiniteQuery: data() is the flattened accumulation in order", async () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    const src = [["a"], ["b", "c"], ["d"]];
    const q = infiniteQuery(qc, {
        key: ["flat"],
        fetcher: ({ cursor }) => Promise.resolve(src[cursor == null ? 0 : cursor]),
        getNextCursor: cursorUpTo(3),
    });
    const stop = effect(() => q.data());
    await tick();
    await q.fetchNextPage();
    await q.fetchNextPage();
    assert.deepEqual(q.data(), ["a", "b", "c", "d"]);
    stop();
    qc.dispose();
});

test("infiniteQuery: hasNextPage is true before the first page resolves", async () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    const f = createControlledFetcher();
    const q = infiniteQuery(qc, { key: ["h"], fetcher: f.fetcher, getNextCursor: () => null });
    const stop = effect(() => q.hasNextPage());
    await tick();
    assert.equal(q.hasNextPage(), true, "hasNextPage armed before first page");
    assert.equal(q.status(), "pending");
    stop();
    qc.dispose();
});

test("infiniteQuery: hasNextPage flips false when getNextCursor returns null", async () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    const q = infiniteQuery(qc, {
        key: ["end"],
        fetcher: () => Promise.resolve([1]),
        getNextCursor: () => null,
    });
    const stop = effect(() => q.data());
    await tick();
    assert.equal(q.hasNextPage(), false);
    assert.deepEqual(q.data(), [1]);
    stop();
    qc.dispose();
});

test("infiniteQuery: concurrent fetchNextPage dedups on the in-flight page", async () => {
    const qc = queryClient({ defaultStaleTime: 0, retry: 0 });
    const f = createQueuedFetcher();
    const q = infiniteQuery(qc, { key: ["d"], fetcher: f.fetcher, getNextCursor: cursorUpTo(3) });
    const stop = effect(() => q.data());
    await tick();
    f.resolveNth(0, [1]);
    await tick();
    const p1 = q.fetchNextPage();
    const p2 = q.fetchNextPage();
    assert.equal(f.callCount, 2, "page one + exactly one next-page fetch (deduped)");
    f.resolveNth(1, [2]);
    await Promise.all([p1, p2]);
    await tick();
    assert.deepEqual(q.pages(), [[1], [2]]);
    stop();
    qc.dispose();
});

test("infiniteQuery: fetchNextPage after exhaustion is a no-op", async () => {
    const qc = queryClient({ defaultStaleTime: 0, retry: 0 });
    const f = createQueuedFetcher();
    const q = infiniteQuery(qc, { key: ["x"], fetcher: f.fetcher, getNextCursor: cursorUpTo(1) });
    const stop = effect(() => q.data());
    await tick();
    f.resolveNth(0, [1]);
    await tick();
    assert.equal(q.hasNextPage(), false);
    const before = f.callCount;
    await q.fetchNextPage();
    assert.equal(f.callCount, before, "no fetch issued past exhaustion");
    stop();
    qc.dispose();
});

test("infiniteQuery: invalidate refetches from page one and replaces the array", async () => {
    const qc = queryClient({ defaultStaleTime: 0, retry: 0 });
    let gen = 0;
    const q = infiniteQuery(qc, {
        key: ["inv"],
        fetcher: ({ cursor }) => Promise.resolve([gen * 10 + (cursor == null ? 0 : cursor)]),
        getNextCursor: cursorUpTo(2),
    });
    const stop = effect(() => q.data());
    await tick();
    await q.fetchNextPage();
    assert.deepEqual(q.data(), [0, 1]);
    gen = 1;
    qc.invalidate(["inv"]);
    await tick();
    assert.deepEqual(q.pages(), [[10]], "old array replaced with fresh page one");
    assert.deepEqual(q.data(), [10]);
    stop();
    qc.dispose();
});

test("infiniteQuery: a late page from a superseded generation is swallowed", async () => {
    const qc = queryClient({ defaultStaleTime: 0, retry: 0 });
    const f = createQueuedFetcher();
    const q = infiniteQuery(qc, { key: ["gen"], fetcher: f.fetcher, getNextCursor: cursorUpTo(5) });
    const stop = effect(() => q.data());
    await tick();
    f.resolveNth(0, ["a"]);
    await tick();
    const p = q.fetchNextPage();          // call 1 -- next page, in flight
    await tick();
    qc.invalidate(["gen"]);               // bumps pageGen, arms page-one refetch
    f.resolveNth(1, ["late"]);            // resolves into the dead generation
    await tick();
    f.resolveNth(2, ["a2"]);              // the page-one refetch
    await p.catch(() => {});
    await tick();
    assert.deepEqual(q.pages(), [["a2"]], "page-one refetch wins; late page dropped");
    assert.ok(!q.data().includes("late"), "the superseded page never entered flat");
    stop();
    qc.dispose();
});

test("infiniteQuery: detach aborts an in-flight page fetch", async () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    const f = createControlledFetcher();
    const q = infiniteQuery(qc, { key: ["ab"], fetcher: f.fetcher, getNextCursor: () => null });
    const stop = effect(() => q.data());
    await tick();
    const sig = f.lastCall.signal;
    assert.equal(sig.aborted, false);
    stop();
    await tick();                         // flush deferred stopWatcher microtask
    assert.equal(sig.aborted, true);
    assert.equal(sig.reason, "lite-query:detach");
    qc.dispose();
});

test("infiniteQuery: a reactive key change aborts the in-flight page", async () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    const k = signal(1);
    const f = createControlledFetcher();
    const q = infiniteQuery(qc, { key: () => ["k", k()], fetcher: f.fetcher, getNextCursor: () => null });
    const stop = effect(() => q.data());
    await tick();
    const sig = f.lastCall.signal;
    k.set(2);
    await tick();
    assert.equal(sig.aborted, true, "old-key page aborted on key change");
    stop();
    qc.dispose();
});

test("infiniteQuery: an error preserves the already-accumulated pages", async () => {
    const qc = queryClient({ defaultStaleTime: 0, retry: 0 });
    const f = createQueuedFetcher();
    const q = infiniteQuery(qc, { key: ["err"], fetcher: f.fetcher, getNextCursor: cursorUpTo(5) });
    const stop = effect(() => q.data());
    await tick();
    f.resolveNth(0, ["a"]);
    await tick();
    const p = q.fetchNextPage();
    await tick();
    f.rejectNth(1, new Error("boom"));
    await p.catch(() => {});
    await tick();
    assert.equal(q.status(), "error");
    assert.deepEqual(q.pages(), [["a"]], "pages intact through the error");
    assert.deepEqual(q.data(), ["a"]);
    stop();
    qc.dispose();
});

test("infiniteQuery: refetch forces page one and replaces", async () => {
    const qc = queryClient({ defaultStaleTime: 0, retry: 0 });
    let gen = 0;
    const q = infiniteQuery(qc, {
        key: ["rf"],
        fetcher: ({ cursor }) => Promise.resolve([gen * 10 + (cursor == null ? 0 : cursor)]),
        getNextCursor: cursorUpTo(2),
    });
    const stop = effect(() => q.data());
    await tick();
    await q.fetchNextPage();
    assert.deepEqual(q.data(), [0, 1]);
    gen = 1;
    await q.refetch();
    await tick();
    assert.deepEqual(q.pages(), [[10]]);
    stop();
    qc.dispose();
});

test("infiniteQuery: the enabled gate holds the query idle until flipped", async () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    const en = signal(false);
    const f = createControlledFetcher();
    const q = infiniteQuery(qc, {
        key: ["gate"], fetcher: f.fetcher, getNextCursor: () => null, enabled: () => en(),
    });
    const stop = effect(() => q.status());
    await tick();
    assert.equal(q.status(), "idle");
    assert.equal(f.callCount, 0);
    en.set(true);
    await tick();
    assert.equal(f.callCount, 1, "flipping enabled fetches page one");
    stop();
    qc.dispose();
});

test("infiniteQuery: cross-tab page write syncs the whole list to a peer entry", async () => {
    const { BroadcastChannel: BC, reset } = createMockBroadcastChannel();
    const A = queryClient({ broadcastChannel: BC, crossTab: true, crossTabChannel: "pg", defaultStaleTime: 0 });
    const B = queryClient({ broadcastChannel: BC, crossTab: true, crossTabChannel: "pg", defaultStaleTime: 0 });
    // B observes an infinite entry; its fetcher never resolves so the only way
    // pages appear is the cross-tab whole-list write from A.
    const qB = infiniteQuery(B, {
        key: ["feed"],
        fetcher: () => new Promise(() => {}),
        getNextCursor: (lastPage, allPages) => allPages.length,
    });
    const stop = effect(() => qB.data());
    await tick();
    A.setQueryData(["feed"], [["x", "y"], ["z"]]);
    for (let i = 0; i < 4; i++) await tick();
    assert.deepEqual(qB.pages(), [["x", "y"], ["z"]], "peer adopts the pages array");
    assert.deepEqual(qB.data(), ["x", "y", "z"], "peer rebuilds the flat view");
    stop();
    A.dispose();
    B.dispose();
    reset();
});

// -----------------------------------------------------------------------------
// C2 -- qc.prefetch
// -----------------------------------------------------------------------------

test("prefetch: a later query() adopts the entry with zero refetch", async () => {
    const qc = queryClient({ defaultStaleTime: 60_000 });
    let calls = 0;
    const fetcher = () => { calls++; return Promise.resolve("v"); };
    await qc.prefetch(["u", 1], fetcher);
    assert.equal(calls, 1);
    const q = query(qc, { key: ["u", 1], fetcher });
    const stop = effect(() => q.data());
    await tick();
    assert.equal(q.data(), "v");
    assert.equal(calls, 1, "adoption served the prefetched entry, no refetch");
    stop();
    qc.dispose();
});

test("prefetch: prefetching an already-fresh entry is a no-op", async () => {
    const qc = queryClient({ defaultStaleTime: 60_000 });
    let calls = 0;
    const fetcher = () => { calls++; return Promise.resolve(1); };
    await qc.prefetch(["k"], fetcher);
    assert.equal(calls, 1);
    await qc.prefetch(["k"], fetcher);
    assert.equal(calls, 1, "fresh entry: no second fetch");
    qc.dispose();
});

test("prefetch: an unobserved prefetched entry GCs at cacheTime", async () => {
    const clock = createMockClock();
    const qc = queryClient({
        now: clock.now, setTimeout: clock.setTimeout, clearTimeout: clock.clearTimeout,
        defaultCacheTime: 1000, defaultStaleTime: 0,
    });
    await qc.prefetch(["g"], () => Promise.resolve(42));
    await tick();
    assert.equal(qc.getQueryData(["g"]), 42);
    clock.advance(1001);
    assert.equal(qc.getQueryData(["g"]), undefined, "no observer ever attached -> GC removes it");
    qc.dispose();
});

// -----------------------------------------------------------------------------
// QA regressions -- D1 / D2 / D3 contract fixes
// -----------------------------------------------------------------------------

test("infiniteQuery: a throwing getNextCursor routes to the error ladder + re-attempts cleanly (D1)", async () => {
    const qc = queryClient({ defaultStaleTime: 0, retry: 0 });
    let shouldThrow = true;
    const q = infiniteQuery(qc, {
        key: ["d1"],
        fetcher: ({ cursor }) => Promise.resolve(cursor == null ? [1] : [2]),
        getNextCursor: (lastPage, allPages) => {
            if (allPages.length === 1) return 1;           // page one -> a next cursor
            if (shouldThrow) throw new Error("boom");       // page two -> throw
            return null;
        },
    });
    const stop = effect(() => q.data());
    await tick();
    assert.deepEqual(q.pages(), [[1]], "page one committed");
    const p = q.fetchNextPage();
    await tick();
    await p.catch(() => {});
    await tick();
    assert.equal(q.status(), "error", "throw -> error status, not a wedge");
    assert.equal(q.error().message, "boom");
    assert.equal(q.fetching(), false, "fetching cleared");
    assert.deepEqual(q.pages(), [[1]], "committed page preserved through the throw");
    assert.deepEqual(q.data(), [1]);
    // clean re-attempt: no dead-promise reuse, same cursor re-tried
    shouldThrow = false;
    const p2 = q.fetchNextPage();
    await tick();
    await p2.catch(() => {});
    await tick();
    assert.equal(q.status(), "success");
    assert.deepEqual(q.pages(), [[1], [2]], "re-attempt appends the page cleanly");
    stop();
    qc.dispose();
});

test("setQueryData: a non-array on an infinite entry throws TypeError locally (D2)", async () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    const q = infiniteQuery(qc, { key: ["d2a"], fetcher: () => Promise.resolve([1]), getNextCursor: () => null });
    const stop = effect(() => q.data());
    await tick();
    assert.throws(
        () => qc.setQueryData(["d2a"], { not: "an array" }),
        /infinite entries accept an array of pages/,
    );
    qc.setQueryData(["d2a"], [[9]]);                        // array still works
    assert.deepEqual(q.pages(), [[9]]);
    stop();
    qc.dispose();
});

test("setQueryData: a remote non-array on an infinite entry drops silently, state coherent (D2)", async () => {
    const { BroadcastChannel: BC, reset } = createMockBroadcastChannel();
    const A = queryClient({ broadcastChannel: BC, crossTab: true, crossTabChannel: "d2", defaultStaleTime: 0 });
    const B = queryClient({ broadcastChannel: BC, crossTab: true, crossTabChannel: "d2", defaultStaleTime: 0 });
    const qB = infiniteQuery(B, {
        key: ["d2b"], fetcher: () => new Promise(() => {}), getNextCursor: (lastPage, allPages) => allPages.length,
    });
    const stop = effect(() => qB.data());
    await tick();
    A.setQueryData(["d2b"], [[1], [2]]);                    // valid array write -> B rebuilds
    for (let i = 0; i < 4; i++) await tick();
    assert.deepEqual(qB.pages(), [[1], [2]]);
    // A's entry is plain (no infinite handle) so the non-array write is legal on A
    // and broadcasts; B (infinite) must DROP it -- cannot throw across tabs.
    A.setQueryData(["d2b"], { rogue: true });
    for (let i = 0; i < 4; i++) await tick();
    assert.deepEqual(qB.pages(), [[1], [2]], "remote non-array dropped; infinite state untouched");
    assert.equal(qB.status(), "success", "coherence preserved");
    stop();
    A.dispose();
    B.dispose();
    reset();
});

test("prefetch: a strict no-op on an active infinite entry -- never advances pagination (D3)", async () => {
    const qc = queryClient({ defaultStaleTime: 0, retry: 0 });
    let calls = 0;
    const q = infiniteQuery(qc, {
        key: ["d3"],
        fetcher: ({ cursor }) => { calls++; return Promise.resolve([cursor == null ? 0 : cursor]); },
        getNextCursor: (lastPage, allPages) => (allPages.length < 5 ? allPages.length : null),
    });
    const stop = effect(() => q.data());
    await tick();
    assert.equal(calls, 1, "page one only");
    assert.deepEqual(q.pages(), [[0]]);
    let otherCalled = 0;
    await qc.prefetch(["d3"], () => { otherCalled++; return Promise.resolve([999]); });
    await tick();
    assert.equal(otherCalled, 0, "supplied fetcher never called");
    assert.equal(calls, 1, "no new page fetched");
    assert.deepEqual(q.pages(), [[0]], "pagination unchanged");
    stop();
    qc.dispose();
});
