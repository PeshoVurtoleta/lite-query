/**
 * @zakkster/lite-query -- persistence primitive + adapter suite.
 *
 * Covers qc.dehydrate / qc.hydrate (boot-only, all-or-nothing, fail-closed),
 * the infinite round-trip (pages restore + cursor recompute at first attach),
 * the private cache-write hook (six commit sites), and persistQueryClient (the
 * storage-agnostic adapter -- install validation, restore ladder, throttle,
 * flush/stop). Mock clock + mock BroadcastChannel from harness.js; the mock
 * channel delivers via queueMicrotask (V1).
 */

import { test } from "node:test";
import assert from "node:assert/strict";

import { effect } from "@zakkster/lite-signal";

import { queryClient, query, infiniteQuery } from "../Query.js";
import { streamQuery } from "../StreamQuery.js";
import {
    createControlledFetcher,
    createMockClock,
    createMockBroadcastChannel,
} from "./harness.js";

const tick = () => new Promise((r) => queueMicrotask(r));
const rec = (key, data, dataUpdatedAt, infinite) => ({ key, data, dataUpdatedAt, infinite });

// -----------------------------------------------------------------------------
// C1 -- dehydrate + hydrate (plain entries)
// -----------------------------------------------------------------------------

test("dehydrate: a success cache round-trips through JSON with identical getQueryData for every key", () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    qc.setQueryData(["a"], 1);
    qc.setQueryData(["b"], { x: 2, y: [3, 4] });
    qc.setQueryData(["c", 7], "seven");
    const wire = JSON.parse(JSON.stringify(qc.dehydrate()));
    qc.dispose();

    const qc2 = queryClient({ defaultStaleTime: 0 });
    const res = qc2.hydrate(wire);
    assert.equal(res.ok, true);
    assert.equal(res.count, 3);
    assert.equal(res.reason, null);
    assert.deepEqual(qc2.getQueryData(["a"]), 1);
    assert.deepEqual(qc2.getQueryData(["b"]), { x: 2, y: [3, 4] });
    assert.deepEqual(qc2.getQueryData(["c", 7]), "seven");
    qc2.dispose();
});

test("dehydrate: pending entries are never serialized (a promise is not data)", async () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    const cf = createControlledFetcher();
    const q = query(qc, { key: ["p"], fetcher: cf.fetcher });
    const stop = effect(() => q.status());
    await tick();
    assert.equal(q.status(), "pending");
    assert.equal(qc.dehydrate().entries.length, 0, "pending excluded");
    stop(); q.dispose(); qc.dispose();
});

test("dehydrate: error entries are never serialized", async () => {
    const qc = queryClient({ defaultStaleTime: 0, retry: 0 });
    const cf = createControlledFetcher();
    const q = query(qc, { key: ["e"], fetcher: cf.fetcher });
    const stop = effect(() => q.status());
    await tick();
    cf.reject(new Error("boom"));
    await tick();
    assert.equal(q.status(), "error");
    assert.equal(qc.dehydrate().entries.length, 0, "error excluded");
    stop(); q.dispose(); qc.dispose();
});

test("dehydrate: stream entries are never serialized (a connection is not data)", async () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    let resolveNext = null;
    const factory = () => ({
        next() { return new Promise((r) => { resolveNext = r; }); },
        return() { return Promise.resolve({ done: true, value: undefined }); },
    });
    const sq = streamQuery(qc, { key: ["s"], stream: factory, mode: "latest" });
    const stop = effect(() => sq.data());
    await tick();
    // deliver one value so the entry reaches a non-idle, data-bearing state
    if (resolveNext) resolveNext({ done: false, value: 42 });
    await tick();
    assert.equal(qc.dehydrate().entries.length, 0, "stream entry excluded regardless of status");
    stop(); sq.dispose(); qc.dispose();
});

test("dehydrate: an infinite entry is marked and carries a COPY of its pages array", async () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    const q = infiniteQuery(qc, {
        key: ["f"],
        fetcher: ({ cursor }) => Promise.resolve([cursor == null ? 0 : cursor]),
        getNextCursor: (lastPage, allPages) => (allPages.length < 3 ? allPages.length : null),
    });
    const stop = effect(() => q.data());
    await tick();
    assert.deepEqual(q.pages(), [[0]], "page one committed");
    const state = qc.dehydrate();
    assert.equal(state.entries.length, 1);
    const r = state.entries[0];
    assert.equal(r.infinite, true);
    assert.deepEqual(r.data, [[0]]);
    // COPY: appending a page to the live entry does not grow the snapshot.
    await q.fetchNextPage();
    await tick();
    assert.deepEqual(q.pages(), [[0], [1]], "live entry grew");
    assert.equal(r.data.length, 1, "snapshot is a shallow copy, unaffected");
    stop(); q.dispose(); qc.dispose();
});

test("dehydrate: the emitted state has no version field (the adapter stamps it, OR-6)", () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    qc.setQueryData(["a"], 1);
    const state = qc.dehydrate();
    assert.deepEqual(Object.keys(state), ["entries"], "exactly one own key");
    assert.equal("version" in state, false);
    for (const r of state.entries) {
        assert.deepEqual(Object.keys(r).sort(), ["data", "dataUpdatedAt", "infinite", "key"]);
    }
    qc.dispose();
});

test("hydrate: seeded entries are stale-aware -- 29999ms no refetch, 30000ms exactly one (mock clock)", async () => {
    const srcClock = createMockClock();
    const src = queryClient({ now: srcClock.now, setTimeout: srcClock.setTimeout, clearTimeout: srcClock.clearTimeout, defaultStaleTime: 30_000 });
    src.setQueryData(["u"], "cached");
    const wire = JSON.parse(JSON.stringify(src.dehydrate()));
    src.dispose();

    const clock = createMockClock();
    const qc = queryClient({ now: clock.now, setTimeout: clock.setTimeout, clearTimeout: clock.clearTimeout, defaultStaleTime: 30_000 });
    qc.hydrate(wire);
    const cf = createControlledFetcher();

    // attach #1 at t=0 -- fresh from cache, no fetch
    let q = query(qc, { key: ["u"], fetcher: cf.fetcher, staleTime: 30_000 });
    let stop = effect(() => q.data());
    await tick();
    assert.equal(cf.callCount, 0, "restored entry is fresh at boot");
    assert.equal(q.data(), "cached");
    stop(); q.dispose();

    // re-attach at 29999 -- still fresh
    clock.advance(29_999);
    q = query(qc, { key: ["u"], fetcher: cf.fetcher, staleTime: 30_000 });
    stop = effect(() => q.data());
    await tick();
    assert.equal(cf.callCount, 0, "still fresh at 29999ms");
    stop(); q.dispose();

    // re-attach at 30000 -- stale, exactly one fetch
    clock.advance(1);
    q = query(qc, { key: ["u"], fetcher: cf.fetcher, staleTime: 30_000 });
    stop = effect(() => q.data());
    await tick();
    assert.equal(cf.callCount, 1, "stale at exactly 30000ms -> one refetch");
    stop(); q.dispose(); qc.dispose();
});

test("hydrate: THROWS on a non-empty cache, and the message names the empty-cache precondition (OR-2)", () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    qc.setQueryData(["x"], 1);
    assert.throws(
        () => qc.hydrate({ entries: [] }),
        /hydrate requires an empty cache/,
    );
    qc.dispose();
});

test("hydrate: throws even when the existing entry has zero observers (entry count, not observer count)", () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    qc.setQueryData(["x"], 1); // creates an entry with ZERO observers
    assert.throws(
        () => qc.hydrate({ entries: [rec(["y"], 2, 0, false)] }),
        /empty cache \(1 entries present\)/,
    );
    qc.dispose();
});

test("hydrate: never broadcasts -- a peer tab sees zero messages after seeding (mock BroadcastChannel)", async () => {
    const { BroadcastChannel: BC, reset } = createMockBroadcastChannel();
    const A = queryClient({ broadcastChannel: BC, crossTab: true, crossTabChannel: "h", defaultStaleTime: 0 });
    const B = queryClient({ broadcastChannel: BC, crossTab: true, crossTabChannel: "h", defaultStaleTime: 0 });
    A.hydrate({ entries: [rec(["k"], 1, 0, false)] });
    for (let i = 0; i < 4; i++) await tick();
    assert.equal(B.getQueryData(["k"]), undefined, "hydrate never echoes across tabs");
    A.dispose(); B.dispose(); reset();
});

test("hydrate: a same-tick boot (create qc -> hydrate) cannot observe a queued remote setData (V1)", async () => {
    const { BroadcastChannel: BC, reset } = createMockBroadcastChannel();
    const A = queryClient({ broadcastChannel: BC, crossTab: true, crossTabChannel: "v1", defaultStaleTime: 0 });
    const B = queryClient({ broadcastChannel: BC, crossTab: true, crossTabChannel: "v1", defaultStaleTime: 0 });
    A.setQueryData(["k"], "remote");       // queues a microtask delivery to B
    // same tick, no await between construction/subscription and hydrate:
    const res = B.hydrate({ entries: [rec(["boot"], "local", 0, false)] });
    assert.equal(res.ok, true, "cache is empty at the synchronous hydrate");
    assert.equal(B.getQueryData(["boot"]), "local");
    await tick();                          // now the queued remote lands
    assert.equal(B.getQueryData(["k"]), "remote");
    A.dispose(); B.dispose(); reset();
});

test("hydrate: an awaited boot on a crossTab client that received a remote setData drops as cache-not-empty (V1c)", async () => {
    const { BroadcastChannel: BC, reset } = createMockBroadcastChannel();
    const A = queryClient({ broadcastChannel: BC, crossTab: true, crossTabChannel: "v1c", defaultStaleTime: 0 });
    const B = queryClient({ broadcastChannel: BC, crossTab: true, crossTabChannel: "v1c", defaultStaleTime: 0 });
    A.setQueryData(["k"], "remote");
    await tick();                          // the awaited boot: the remote lands first
    assert.equal(B.getQueryData(["k"]), "remote");
    assert.throws(
        () => B.hydrate({ entries: [rec(["boot"], "local", 0, false)] }),
        /empty cache/,
    );
    A.dispose(); B.dispose(); reset();
});

test("hydrate: a future dataUpdatedAt is clamped to now() -- restored entry is not immortally fresh", async () => {
    const clock = createMockClock(1000);
    const qc = queryClient({ now: clock.now, setTimeout: clock.setTimeout, clearTimeout: clock.clearTimeout, defaultStaleTime: 5000 });
    qc.hydrate({ entries: [rec(["k"], 1, 999_999, false)] }); // far-future timestamp
    const cf = createControlledFetcher();

    // at now=1000 -- fresh (clamped lastCompletedAt === 1000)
    let q = query(qc, { key: ["k"], fetcher: cf.fetcher, staleTime: 5000 });
    let stop = effect(() => q.data());
    await tick();
    assert.equal(cf.callCount, 0, "fresh at boot");
    stop(); q.dispose();

    // advance one staleTime -- if the future stamp had survived, this would
    // still read fresh; the clamp makes it stale exactly here.
    clock.advance(5000); // now 6000
    q = query(qc, { key: ["k"], fetcher: cf.fetcher, staleTime: 5000 });
    stop = effect(() => q.data());
    await tick();
    assert.equal(cf.callCount, 1, "clamped to now -> stale a staleTime later");
    stop(); q.dispose(); qc.dispose();
});

test("hydrate: seeded entries GC at cacheTime like any unobserved entry", () => {
    const clock = createMockClock();
    const qc = queryClient({ now: clock.now, setTimeout: clock.setTimeout, clearTimeout: clock.clearTimeout, defaultCacheTime: 1000 });
    qc.hydrate({ entries: [rec(["k"], 1, 0, false)] });
    assert.equal(qc.getQueryData(["k"]), 1, "restored and readable");
    clock.advance(1001);
    assert.equal(qc.getQueryData(["k"]), undefined, "unobserved seeded entry GCs at cacheTime");
    qc.dispose();
});
