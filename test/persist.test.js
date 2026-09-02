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

import { queryClient, query, infiniteQuery, persistQueryClient } from "../Query.js";
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

// -----------------------------------------------------------------------------
// C2 -- infinite round-trip
// -----------------------------------------------------------------------------

const cursorUpTo4 = (lastPage, allPages) => (allPages.length < 4 ? allPages.length : null);

// Build a source client, accumulate `pages` pages on an infinite entry, and
// return a JSON-round-tripped dehydrated wire snapshot.
async function dehydratedFeed(pages) {
    const src = queryClient({ defaultStaleTime: 0 });
    const q = infiniteQuery(src, {
        key: ["feed"],
        fetcher: ({ cursor }) => Promise.resolve([cursor == null ? 0 : cursor]),
        getNextCursor: cursorUpTo4,
    });
    const stop = effect(() => q.data());
    await tick();
    for (let i = 1; i < pages; i++) { await q.fetchNextPage(); await tick(); }
    const wire = JSON.parse(JSON.stringify(src.dehydrate()));
    stop(); q.dispose(); src.dispose();
    return wire;
}

test("infinite: a restored list continues paginating -- fetchNextPage appends page N+1 exactly once (G6)", async () => {
    const wire = await dehydratedFeed(3);           // [[0],[1],[2]], next cursor 3

    const qc = queryClient({ defaultStaleTime: 30_000 });
    assert.equal(qc.hydrate(wire).ok, true);
    const calls = [];
    const q = infiniteQuery(qc, {
        key: ["feed"],
        fetcher: ({ cursor }) => { calls.push(cursor); return Promise.resolve([cursor]); },
        getNextCursor: cursorUpTo4,
        staleTime: 30_000,
    });
    const stop = effect(() => q.data());
    await tick();
    assert.deepEqual(q.pages(), [[0], [1], [2]], "3 dehydrated pages restored");
    assert.deepEqual(q.data(), [0, 1, 2], "flat view restored");
    assert.equal(q.hasNextPage(), true, "cursor recomputed at first attach");
    assert.equal(calls.length, 0, "a fresh restored list does not refetch on attach");

    await q.fetchNextPage();
    await tick();
    assert.equal(calls.length, 1, "exactly one page fetch");
    assert.equal(calls[0], 3, "fetch used the recomputed cursor 3, not null");
    assert.equal(q.pages().length, 4);
    assert.deepEqual(q.pages()[0], [0], "page one unchanged at index 0");
    assert.deepEqual(q.pages()[3], [3], "appended page four");
    stop(); q.dispose(); qc.dispose();
});

test("infinite: hasNextPage is false before attach and correct after the first attach (fail closed)", async () => {
    const wire = await dehydratedFeed(2);           // [[0],[1]], next cursor 2

    const qc = queryClient({ defaultStaleTime: 30_000 });
    qc.hydrate(wire);
    const e = [...qc._internal.entries.values()][0];
    assert.equal(e.isInfinite, true, "seeded as infinite");
    assert.equal(e.hasNext, false, "fail closed: no cursor known before attach");
    assert.equal(e.getNextCursor, null, "cursor function not installed until attach");

    const q = infiniteQuery(qc, {
        key: ["feed"],
        fetcher: ({ cursor }) => Promise.resolve([cursor]),
        getNextCursor: cursorUpTo4,
        staleTime: 30_000,
    });
    const stop = effect(() => q.data());
    await tick();
    assert.equal(q.hasNextPage(), true, "cursor recomputed at first attach -> hasNext correct");
    stop(); q.dispose(); qc.dispose();

    // Exhausted list: after attach hasNextPage must read false, not re-open.
    const wire4 = await dehydratedFeed(4);          // [[0]..[3]], cursor exhausted
    const qc2 = queryClient({ defaultStaleTime: 30_000 });
    qc2.hydrate(wire4);
    const q2 = infiniteQuery(qc2, {
        key: ["feed"],
        fetcher: ({ cursor }) => Promise.resolve([cursor]),
        getNextCursor: cursorUpTo4,
        staleTime: 30_000,
    });
    const stop2 = effect(() => q2.data());
    await tick();
    assert.equal(q2.hasNextPage(), false, "restored exhausted list stays exhausted after attach");
    stop2(); q2.dispose(); qc2.dispose();
});

test("infinite: a getNextCursor that throws at adoption resets to a clean page-one fetch, never wedges", async () => {
    const wire = await dehydratedFeed(2);           // [[0],[1]]

    const qc = queryClient({ defaultStaleTime: 30_000 });
    qc.hydrate(wire);
    let firstCall = true;
    const calls = [];
    const q = infiniteQuery(qc, {
        key: ["feed"],
        fetcher: ({ cursor }) => { calls.push(cursor); return Promise.resolve([cursor == null ? 100 : cursor]); },
        getNextCursor: (lastPage, allPages) => {
            if (firstCall) { firstCall = false; throw new Error("adoption boom"); }
            return allPages.length < 4 ? allPages.length : null;
        },
        staleTime: 30_000,
    });
    const stop = effect(() => q.data());
    await tick(); await tick(); await tick();
    assert.equal(calls.length, 1, "exactly one page-one fetch after the reset");
    assert.equal(calls[0], null, "clean page-one restart, cursor null (not wedged)");
    assert.equal(q.status(), "success");
    assert.deepEqual(q.pages(), [[100]], "list rebuilt from page one");
    stop(); q.dispose(); qc.dispose();
});

// -----------------------------------------------------------------------------
// C3 -- the private write seam (six V4 hook sites)
// -----------------------------------------------------------------------------

test("write hook: fires on setData / fetch settle / commitPage throw / removeQueries / clear / GC expiry (6 sites)", async () => {
    // site 1: setQueryData (covers local writes AND remote applies)
    {
        const qc = queryClient({ defaultStaleTime: 0 });
        let hits = 0;
        qc._internal.installPersistHook(() => hits++);
        qc.setQueryData(["a"], 1);
        assert.equal(hits, 1, "site 1: setQueryData");
        qc.dispose();
    }
    // site 2: fetch settle (success), no hook while pending
    {
        const qc = queryClient({ defaultStaleTime: 0, retry: 0 });
        let hits = 0;
        qc._internal.installPersistHook(() => hits++);
        const cf = createControlledFetcher();
        const q = query(qc, { key: ["b"], fetcher: cf.fetcher });
        const stop = effect(() => q.data());
        await tick();
        assert.equal(hits, 0, "no hook while a fetch is pending");
        cf.resolve("ok");
        await tick();
        assert.equal(hits, 1, "site 2: fetch settle");
        stop(); q.dispose(); qc.dispose();
    }
    // site 3: commitPage-throw branch (returns before site 2, so exactly one)
    {
        const qc = queryClient({ defaultStaleTime: 0, retry: 0 });
        let hits = 0;
        qc._internal.installPersistHook(() => hits++);
        const iq = infiniteQuery(qc, {
            key: ["c"], fetcher: () => Promise.resolve([1]),
            getNextCursor: () => { throw new Error("commit boom"); },
        });
        const stop = effect(() => iq.data());
        await tick(); await tick();
        assert.equal(iq.status(), "error");
        assert.equal(hits, 1, "site 3: commitPage-throw branch fires exactly once");
        stop(); iq.dispose(); qc.dispose();
    }
    // site 4: removeQueries, only when >= 1 entry matched
    {
        const qc = queryClient({ defaultStaleTime: 0 });
        qc.setQueryData(["d"], 1);
        let hits = 0;
        qc._internal.installPersistHook(() => hits++);
        qc.removeQueries(["nope"]);
        assert.equal(hits, 0, "no match -> no hook");
        qc.removeQueries(["d"]);
        assert.equal(hits, 1, "site 4: removeQueries matched");
        qc.dispose();
    }
    // site 5: clear, only when the map was non-empty
    {
        const qc = queryClient({ defaultStaleTime: 0 });
        let hits = 0;
        qc._internal.installPersistHook(() => hits++);
        qc.clear();
        assert.equal(hits, 0, "empty clear -> no hook");
        qc.setQueryData(["e"], 1);
        assert.equal(hits, 1, "setData fired site 1");
        qc.clear();
        assert.equal(hits, 2, "site 5: clear on a non-empty map persists emptiness");
        qc.dispose();
    }
    // site 6: cacheTime GC expiry of an unobserved entry
    {
        const clock = createMockClock();
        const qc = queryClient({ now: clock.now, setTimeout: clock.setTimeout, clearTimeout: clock.clearTimeout, defaultCacheTime: 1000 });
        qc.setQueryData(["g"], 5);   // before install, so site 1 is not counted
        let hits = 0;
        qc._internal.installPersistHook(() => hits++);
        clock.advance(1001);
        assert.equal(hits, 1, "site 6: an expired entry leaves the snapshot");
        qc.dispose();
    }
});

test("write hook: does NOT fire on invalidate, attach, detach, or hydrate seeding", async () => {
    // invalidate: no content change; invalidatedSinceCompletion is not serialized
    {
        const qc = queryClient({ defaultStaleTime: 0, retry: 0 });
        qc.setQueryData(["a"], 1);
        let hits = 0;
        qc._internal.installPersistHook(() => hits++);
        qc.invalidate(["a"]);
        assert.equal(hits, 0, "invalidate does not persist");
        qc.dispose();
    }
    // attach + detach on a fresh entry: neither is a cache write
    {
        const qc = queryClient({ defaultStaleTime: 30_000 });
        qc.setQueryData(["b"], 1);
        let hits = 0;
        qc._internal.installPersistHook(() => hits++);
        const q = query(qc, { key: ["b"], fetcher: () => Promise.resolve(2), staleTime: 30_000 });
        const stop = effect(() => q.data());
        await tick();
        assert.equal(hits, 0, "attach on a fresh entry does not persist");
        stop(); q.dispose();
        await tick();
        assert.equal(hits, 0, "detach does not persist");
        qc.dispose();
    }
    // hydrate seeding: writing what we just loaded must not re-persist it
    {
        const qc = queryClient({ defaultStaleTime: 0 });
        let hits = 0;
        qc._internal.installPersistHook(() => hits++);
        qc.hydrate({ entries: [rec(["h"], 1, 0, false), rec(["h2"], 2, 0, false)] });
        assert.equal(hits, 0, "hydrate seeding does not fire the write hook");
        qc.dispose();
    }
});

test("write hook: zero calls with no adapter installed; a second install throws", () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    // no hook installed: every write path is a silent no-op on the seam
    qc.setQueryData(["a"], 1);
    qc.removeQueries(["a"]);
    qc.setQueryData(["b"], 2);
    qc.clear();
    const uninstall = qc._internal.installPersistHook(() => {});
    assert.throws(() => qc._internal.installPersistHook(() => {}), /single-slot/);
    uninstall();
    const u2 = qc._internal.installPersistHook(() => {}); // re-install after uninstall succeeds
    assert.equal(typeof u2, "function");
    u2();
    qc.dispose();
});

// -----------------------------------------------------------------------------
// C4 -- the adapter (persistQueryClient)
// -----------------------------------------------------------------------------

test("adapter: version is REQUIRED -- no default, install throws without it (OR-6)", () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    assert.throws(() => persistQueryClient(qc, { save: () => {}, load: () => null }), /`version` is required/);
    assert.throws(() => persistQueryClient(qc, { save: () => {}, load: () => null, version: {} }), /must be a string or number/);
    assert.throws(() => persistQueryClient(qc, { save: "x", load: () => null, version: "v1" }), /`save` must be a function/);
    assert.throws(() => persistQueryClient(qc, { save: () => {}, load: () => null, version: "v1", throttle: -1 }), /`throttle` must be a finite number/);
    qc.dispose();
});

test("adapter: load() -> null is an EMPTY store, status \"empty\", not an error (OR-3)", async () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    const p = persistQueryClient(qc, { save: () => {}, load: () => null, version: "v1" });
    assert.deepEqual(await p.restored, { status: "empty", count: 0, reason: null });
    p.stop(); qc.dispose();
});

test("adapter: a throwing/rejecting load() resolves { status: \"dropped\", reason: \"load-threw\" } -- never unhandled", async () => {
    const qc1 = queryClient({ defaultStaleTime: 0 });
    const p1 = persistQueryClient(qc1, { save: () => {}, load: () => { throw new Error("io"); }, version: "v1" });
    assert.deepEqual(await p1.restored, { status: "dropped", count: 0, reason: "load-threw" });
    p1.stop(); qc1.dispose();

    const qc2 = queryClient({ defaultStaleTime: 0 });
    const p2 = persistQueryClient(qc2, { save: () => {}, load: () => Promise.reject(new Error("io")), version: "v1" });
    assert.equal((await p2.restored).reason, "load-threw");
    p2.stop(); qc2.dispose();
});

test("adapter: version mismatch drops 100% of entries and fetches fresh", async () => {
    const src = queryClient({ defaultStaleTime: 0 });
    src.setQueryData(["a"], 1);
    src.setQueryData(["b"], 2);
    const stored = { version: "v1", state: JSON.parse(JSON.stringify(src.dehydrate())) };
    src.dispose();

    const qc = queryClient({ defaultStaleTime: 0 });
    const p = persistQueryClient(qc, { save: () => {}, load: () => stored, version: "v2" });
    assert.deepEqual(await p.restored, { status: "dropped", count: 0, reason: "version-mismatch" });
    assert.equal(qc.getQueryData(["a"]), undefined, "0 of N hydrated");
    assert.equal(qc.getQueryData(["b"]), undefined);
    p.stop(); qc.dispose();
});

test("adapter: throttle coalesces N writes in a window into exactly ONE save (mock clock, trailing edge)", async () => {
    const clock = createMockClock();
    const qc = queryClient({ now: clock.now, setTimeout: clock.setTimeout, clearTimeout: clock.clearTimeout, defaultStaleTime: 0 });
    let saves = 0; let last = null;
    const p = persistQueryClient(qc, { save: (env) => { saves++; last = env; }, load: () => null, version: "v1", throttle: 1000 });
    await p.restored;
    qc.setQueryData(["a"], 1);
    qc.setQueryData(["b"], 2);
    qc.setQueryData(["c"], 3);
    assert.equal(saves, 0, "no save yet -- window open");
    clock.advance(999);
    assert.equal(saves, 0);
    clock.advance(1);
    assert.equal(saves, 1, "exactly one save coalesced from three writes");
    assert.equal(last.version, "v1");
    assert.equal(last.state.entries.length, 3, "the ONE save carries all three writes");
    p.stop(); qc.dispose();
});

test("adapter: stop() uninstalls the hook and clears the pending timer; further writes save nothing", async () => {
    const clock = createMockClock();
    const qc = queryClient({ now: clock.now, setTimeout: clock.setTimeout, clearTimeout: clock.clearTimeout, defaultStaleTime: 0 });
    let saves = 0;
    const p = persistQueryClient(qc, { save: () => saves++, load: () => null, version: "v1", throttle: 1000 });
    await p.restored;
    qc.setQueryData(["a"], 1);
    clock.advance(1000);
    assert.equal(saves, 1);
    p.stop();
    assert.equal(saves, 1, "stop with no pending timer saves nothing extra");
    qc.setQueryData(["b"], 2);
    clock.advance(5000);
    assert.equal(saves, 1, "hook uninstalled: further writes save nothing");
    qc.dispose();
});

test("adapter: stop() FLUSHES a pending save (OR-7/ON-3c decision); clear() + stop() persists emptiness", async () => {
    const clock = createMockClock();
    const qc = queryClient({ now: clock.now, setTimeout: clock.setTimeout, clearTimeout: clock.clearTimeout, defaultStaleTime: 0 });
    let saves = 0; let last = null;
    const p = persistQueryClient(qc, { save: (env) => { saves++; last = env; }, load: () => null, version: "v1", throttle: 1000 });
    await p.restored;
    qc.setQueryData(["a"], 1);          // window open, timer pending
    assert.equal(saves, 0);
    p.stop();                            // flush-on-stop
    assert.equal(saves, 1, "pending save flushed at stop");
    assert.equal(last.state.entries.length, 1);
    qc.dispose();

    // logout path: clear() then stop() persists emptiness
    const clock2 = createMockClock();
    const qc2 = queryClient({ now: clock2.now, setTimeout: clock2.setTimeout, clearTimeout: clock2.clearTimeout, defaultStaleTime: 0 });
    let saved = null;
    const p2 = persistQueryClient(qc2, { save: (env) => { saved = env; }, load: () => null, version: "v1", throttle: 1000 });
    await p2.restored;
    qc2.setQueryData(["u"], 1);
    clock2.advance(1000);
    assert.equal(saved.state.entries.length, 1, "one entry persisted");
    qc2.clear();                         // hook site 5 opens a fresh window (empty snapshot)
    p2.stop();                           // flush-on-stop -> persists emptiness
    assert.equal(saved.state.entries.length, 0, "clear() + stop() persists emptiness");
    qc2.dispose();
});

test("adapter: a rejecting save() is contained -- the adapter keeps running, no unhandled rejection", async () => {
    const clock = createMockClock();
    const qc = queryClient({ now: clock.now, setTimeout: clock.setTimeout, clearTimeout: clock.clearTimeout, defaultStaleTime: 0 });
    let attempts = 0;
    const p = persistQueryClient(qc, {
        save: () => { attempts++; return Promise.reject(new Error("disk full")); },
        load: () => null, version: "v1", throttle: 1000,
    });
    await p.restored;
    qc.setQueryData(["a"], 1);
    clock.advance(1000);
    assert.equal(attempts, 1, "save attempted");
    qc.setQueryData(["b"], 2);
    clock.advance(1000);
    assert.equal(attempts, 2, "adapter kept running after a rejected save");
    p.stop(); qc.dispose();
});

test("adapter: the restore outcome is observable before the first observer attaches", async () => {
    const src = queryClient({ defaultStaleTime: 0 });
    src.setQueryData(["a"], 41);
    const stored = { version: "v1", state: JSON.parse(JSON.stringify(src.dehydrate())) };
    src.dispose();

    const qc = queryClient({ defaultStaleTime: 30_000 });
    const p = persistQueryClient(qc, { save: () => {}, load: () => stored, version: "v1" });
    const outcome = await p.restored;
    assert.equal(outcome.status, "restored");
    assert.equal(outcome.count, 1);
    assert.equal(qc.getQueryData(["a"]), 41, "readable with no observer ever attached");
    p.stop(); qc.dispose();
});
