// @zakkster/lite-query -- devtools feed (qc.inspect) behavioural suite (Q7).
//
// The feed is a push-mode stream of cache truth a panel renders. This suite
// pins the seam contract (single slot, two throw shapes, idempotent uninstall)
// and, as the emit sites land rung by rung, the event vocabulary itself: entry
// lifecycle, status/staleness, fetch dispatch/settle/abort, cross-tab and
// shared-fetch roles, and mutation lifecycle. Shape/identity live in
// inspect-shape.test.js; stream events in inspect-stream.test.js; the two-seam
// independence + throwing-hook containment in inspect-seams.test.js.

import { test } from "node:test";
import assert from "node:assert/strict";
import { queryClient, query } from "../Query.js";
import { effect, createRoot } from "@zakkster/lite-signal";
import { createMockClock, createMockBroadcastChannel, createControlledFetcher } from "./harness.js";

const noop = () => {};

const tick = () => new Promise((r) => setTimeout(r, 0));

// A mock-clock client so cacheTime GC is driven deterministically by advance().
function clockClient(opts = {}) {
    const clock = createMockClock();
    const qc = queryClient({
        now: clock.now, setTimeout: clock.setTimeout, clearTimeout: clock.clearTimeout,
        ...opts,
    });
    return { qc, clock };
}

// Collect every event a hook sees into a plain array; return { qc, events, stop }.
function withFeed(opts = {}) {
    const qc = queryClient(opts);
    const events = [];
    const stop = qc.inspect((e) => {
        // The hook MUST copy what it keeps -- events are pooled + overwritten.
        events.push({ ...e });
    });
    return { qc, events, stop };
}

// -- seam contract (C1) ------------------------------------------------------

test("inspect: returns an idempotent uninstall function", () => {
    const qc = queryClient();
    const stop = qc.inspect(noop);
    assert.equal(typeof stop, "function", "inspect returns an uninstall thunk");
    stop();
    stop();                                 // idempotent: a second call is a no-op
    // The slot is free again -- a fresh install must succeed.
    const stop2 = qc.inspect(noop);
    assert.equal(typeof stop2, "function");
    stop2();
});

test("inspect: a second install while one is live throws Error (single slot)", () => {
    const qc = queryClient();
    const stop = qc.inspect(noop);
    assert.throws(() => qc.inspect(noop), (err) =>
        err instanceof Error && !(err instanceof TypeError) &&
        /already installed/.test(err.message));
    stop();
});

test("inspect: uninstall frees the slot for a later install", () => {
    const qc = queryClient();
    const stopA = qc.inspect(noop);
    stopA();
    // A different hook installs cleanly after uninstall (no throw = slot free);
    // and while it holds the slot, a third install throws (slot taken again).
    const stopB = qc.inspect(noop);
    assert.throws(() => qc.inspect(noop), (err) =>
        err instanceof Error && /already installed/.test(err.message));
    stopB();
});

test("inspect: a stale uninstall thunk does not evict a newer hook", () => {
    const qc = queryClient();
    const hookA = () => {};
    const hookB = () => {};
    const stopA = qc.inspect(hookA);
    stopA();
    const stopB = qc.inspect(hookB);
    stopA();                                 // the OLD thunk must be a no-op now
    // If stopA had evicted B, this install would succeed; it must throw because
    // B still holds the single slot.
    assert.throws(() => qc.inspect(noop), (err) =>
        err instanceof Error && /already installed/.test(err.message));
    stopB();
});

for (const bad of [null, undefined, [], {}]) {
    test("inspect: rejects a non-function argument (" +
        (Array.isArray(bad) ? "array" : bad === null ? "null" :
            bad === undefined ? "undefined" : "object") + ") with TypeError", () => {
        const qc = queryClient();
        assert.throws(() => qc.inspect(bad), (err) =>
            err instanceof TypeError && /requires a function/.test(err.message));
    });
}

// -- entry lifecycle (C2) ----------------------------------------------------

const only = (events, type) => events.filter((e) => e.type === type);

test("entry:create fires once on the first ensureEntry for a key", () => {
    const { qc, events, stop } = withFeed();
    const KEY = ["c", 1];
    qc.setQueryData(KEY, 7);
    const created = only(events, "entry:create");
    assert.equal(created.length, 1, "one create for a new key");
    assert.deepEqual(created[0].key, KEY);
    assert.equal(created[0].keyHash, JSON.stringify(KEY));
    assert.equal(created[0].count, 0);
    assert.equal(created[0].reason, null);
    // A second write to the same key does NOT re-create.
    qc.setQueryData(KEY, 8);
    assert.equal(only(events, "entry:create").length, 1, "no re-create for an existing key");
    stop();
});

test("entry:attach / entry:detach carry the observerCount after the change", () => {
    const { qc, events, stop } = withFeed({ defaultStaleTime: 60_000 });
    const KEY = ["a"];
    qc.setQueryData(KEY, 1);                         // fresh success -> mount won't fetch
    const q = query(qc, { key: KEY, fetcher: async () => 1 });
    const stopEffect = createRoot(() => effect(() => q.status()));
    const att = only(events, "entry:attach");
    assert.equal(att.length, 1, "one attach on first observed read");
    assert.equal(att[0].count, 1, "observerCount after attach is 1");
    assert.deepEqual(att[0].key, KEY);
    stopEffect();
    q.dispose();                                     // synchronous stopWatcher -> detach
    const det = only(events, "entry:detach");
    assert.equal(det.length, 1, "one detach on teardown");
    assert.equal(det[0].count, 0, "observerCount after detach is 0");
    stop();
});

test("entry:gc fires when an unobserved entry expires", () => {
    const { qc, clock } = clockClient({ defaultCacheTime: 1000 });
    const events = [];
    const stop = qc.inspect((e) => events.push({ ...e }));
    qc.setQueryData(["g"], 1);                       // entry created, GC armed at t=1000
    clock.advance(1001);                             // fire the cacheTime timer
    const gced = only(events, "entry:gc");
    assert.equal(gced.length, 1, "one gc on expiry");
    assert.deepEqual(gced[0].key, ["g"]);
    assert.equal(gced[0].count, 0);
    stop();
});

test("entry:remove reason 'remove' via removeQueries", () => {
    const { qc, events, stop } = withFeed();
    qc.setQueryData(["r", 1], 1);
    qc.removeQueries(["r", 1]);
    const rem = only(events, "entry:remove");
    assert.equal(rem.length, 1);
    assert.equal(rem[0].reason, "remove");
    assert.deepEqual(rem[0].key, ["r", 1]);
    stop();
});

test("entry:remove reason 'clear' via clear", () => {
    const { qc, events, stop } = withFeed();
    qc.setQueryData(["k1"], 1);
    qc.setQueryData(["k2"], 2);
    qc.clear();
    const rem = only(events, "entry:remove");
    assert.equal(rem.length, 2, "one remove per cleared entry");
    assert.ok(rem.every((e) => e.reason === "clear"));
    stop();
});

test("entry:remove reason 'hydrate-rollback' when seeding throws mid-payload", () => {
    const qc = queryClient();
    const events = [];
    const stop = qc.inspect((e) => events.push({ ...e }));
    // A pages array that passes Array.isArray (proxy of a real array) but throws
    // on element access -- validation only type-checks the array, seedInfinite
    // iterates it and throws, forcing the all-or-nothing rollback of the first,
    // already-seeded record.
    const throwingPages = new Proxy([1], {
        get(t, p) { if (p === "0") throw new Error("boom"); return t[p]; },
    });
    const state = { entries: [
        { key: ["ok"], data: 1, dataUpdatedAt: 0, infinite: false },
        { key: ["bad"], data: throwingPages, dataUpdatedAt: 0, infinite: true },
    ] };
    const res = qc.hydrate(state);
    assert.equal(res.ok, false, "malformed payload drops");
    const rem = only(events, "entry:remove");
    assert.ok(rem.length >= 1, "rollback emits at least one remove");
    assert.ok(rem.every((e) => e.reason === "hydrate-rollback"));
    stop();
});

// -- status + staleness (C2) -------------------------------------------------

test("entry:status carries from/to and fires even when from === to", () => {
    const { qc, events, stop } = withFeed();
    const KEY = ["s"];
    qc.setQueryData(KEY, 1);                         // idle -> success
    let st = only(events, "entry:status");
    assert.equal(st.length, 1);
    assert.equal(st[0].from, "idle");
    assert.equal(st[0].to, "success");
    assert.deepEqual(st[0].key, KEY);
    qc.setQueryData(KEY, 2);                         // success -> success (a real write)
    st = only(events, "entry:status");
    assert.equal(st.length, 2, "a same-status re-write still emits");
    assert.equal(st[1].from, "success");
    assert.equal(st[1].to, "success");
    stop();
});

test("entry:status count/ok/value are the N/A defaults (0/false/null)", () => {
    const { qc, events, stop } = withFeed();
    qc.setQueryData(["s2"], 1);
    const st = only(events, "entry:status")[0];
    assert.equal(st.count, 0);
    assert.equal(st.ok, false);
    assert.equal(st.value, null);
    assert.equal(st.reason, null);
    stop();
});

test("entry:stale fires per matched entry with reason 'invalidate'", () => {
    const { qc, events, stop } = withFeed();
    qc.setQueryData(["inv", 1], 1);
    qc.setQueryData(["inv", 2], 2);
    qc.invalidate(["inv"]);                          // prefix match -> both entries
    const stale = only(events, "entry:stale");
    assert.equal(stale.length, 2);
    assert.ok(stale.every((e) => e.reason === "invalidate"));
    assert.ok(stale.every((e) => e.count === 0));
    stop();
});

// -- fetch dispatch / settle / abort (C3) ------------------------------------

test("fetch:dispatch + fetch:settle (success) carry gen, cursor null, and data", async () => {
    const { qc, events, stop } = withFeed({ defaultStaleTime: 0 });
    const q = query(qc, { key: ["f"], fetcher: async () => 42 });
    const stopE = createRoot(() => effect(() => q.data()));
    await tick();
    const disp = only(events, "fetch:dispatch");
    assert.equal(disp.length, 1);
    assert.equal(disp[0].count, 1, "count is the fetch generation");
    assert.equal(disp[0].value, null, "plain query: cursor is null");
    assert.equal(disp[0].reason, null, "unforced dispatch has null reason");
    assert.equal(disp[0].ok, false);
    const settle = only(events, "fetch:settle");
    assert.equal(settle.length, 1);
    assert.equal(settle[0].ok, true);
    assert.equal(settle[0].value, 42, "settle value is the fetched data");
    assert.equal(settle[0].count, 1);
    assert.equal(settle[0].reason, null);
    stopE(); q.dispose();
    stop();
});

test("fetch:settle (error) reports ok=false and the error value", async () => {
    const { qc, events, stop } = withFeed({ defaultStaleTime: 0, retry: 0 });
    const boom = new Error("nope");
    const q = query(qc, { key: ["fe"], fetcher: async () => { throw boom; } });
    const stopE = createRoot(() => effect(() => q.data()));
    await tick();
    const settle = only(events, "fetch:settle");
    assert.equal(settle.length, 1);
    assert.equal(settle[0].ok, false);
    assert.equal(settle[0].value, boom, "settle value is the thrown error");
    stopE(); q.dispose();
    stop();
});

test("fetch:abort reason 'lite-query:detach' when the last observer leaves in-flight", async () => {
    const { qc, events, stop } = withFeed({ defaultStaleTime: 0 });
    const cf = createControlledFetcher();
    const q = query(qc, { key: ["ad"], fetcher: cf.fetcher });
    const stopE = createRoot(() => effect(() => q.data()));
    await tick();                                    // fetch dispatched, in-flight
    stopE(); q.dispose();                            // detach -> abort in-flight
    const ab = only(events, "fetch:abort");
    assert.equal(ab.length, 1);
    assert.equal(ab[0].reason, "lite-query:detach");
    assert.equal(ab[0].count, 1);
    stop();
});

test("fetch:abort reason 'lite-query:refetch' when a forced refetch supersedes", async () => {
    const { qc, events, stop } = withFeed({ defaultStaleTime: 0 });
    const cf = createControlledFetcher();
    const q = query(qc, { key: ["ar"], fetcher: cf.fetcher });
    const stopE = createRoot(() => effect(() => q.data()));
    await tick();                                    // gen 1 in-flight
    q.refetch();                                     // force -> aborts gen 1
    const ab = only(events, "fetch:abort").filter((e) => e.reason === "lite-query:refetch");
    assert.equal(ab.length, 1);
    stopE(); q.dispose();
    stop();
});

test("fetch:abort reason 'lite-query:removed' when removeQueries evicts an in-flight entry", async () => {
    const { qc, events, stop } = withFeed({ defaultStaleTime: 0 });
    const cf = createControlledFetcher();
    const q = query(qc, { key: ["arm"], fetcher: cf.fetcher });
    const stopE = createRoot(() => effect(() => q.data()));
    await tick();
    qc.removeQueries(["arm"]);
    const ab = only(events, "fetch:abort").filter((e) => e.reason === "lite-query:removed");
    assert.equal(ab.length, 1);
    stopE(); q.dispose();
    stop();
});

test("fetch:abort reason 'lite-query:timeout' when the per-query timeout fires", async () => {
    const { qc, clock } = clockClient({ defaultStaleTime: 0, defaultTimeout: 500 });
    const events = [];
    const stop = qc.inspect((e) => events.push({ ...e }));
    const cf = createControlledFetcher();
    const q = query(qc, { key: ["at"], fetcher: cf.fetcher });
    const stopE = createRoot(() => effect(() => q.data()));
    await tick();                                    // dispatched, timeout timer armed
    clock.advance(501);                              // fire the timeout -> abort
    const ab = only(events, "fetch:abort").filter((e) => e.reason === "lite-query:timeout");
    assert.equal(ab.length, 1);
    stopE(); q.dispose();
    stop();
});

// -- cross-tab + shared-fetch (C3) -------------------------------------------

test("tab:send (issuer) and tab:receive (peer) mirror the message type", async () => {
    const { BroadcastChannel } = createMockBroadcastChannel();
    const mk = () => queryClient({ crossTab: true, broadcastChannel: BroadcastChannel,
        crossTabChannel: "ct", defaultStaleTime: 0 });
    const tabA = mk(), tabB = mk();
    const evA = [], evB = [];
    const sA = tabA.inspect((e) => evA.push({ ...e }));
    const sB = tabB.inspect((e) => evB.push({ ...e }));
    tabA.setQueryData(["x"], 1);
    const send = only(evA, "tab:send");
    assert.ok(send.some((e) => e.reason === "setData" && e.ok === true), "issuer sees tab:send ok");
    await tick();                                    // mock channel delivers via microtask
    const recv = only(evB, "tab:receive");
    assert.ok(recv.some((e) => e.reason === "setData"), "peer sees tab:receive");
    sA(); sB(); tabA.dispose(); tabB.dispose();
});

test("shared:request (follower) then shared:fallback on leader-timeout", () => {
    const clock = createMockClock();
    const { BroadcastChannel } = createMockBroadcastChannel();
    const qc = queryClient({ crossTab: true, sharedFetch: true, isLeader: () => false,
        broadcastChannel: BroadcastChannel, crossTabChannel: "sh",
        now: clock.now, setTimeout: clock.setTimeout, clearTimeout: clock.clearTimeout,
        defaultStaleTime: 0, sharedFetchTimeout: 3000 });
    const events = [];
    const stop = qc.inspect((e) => events.push({ ...e }));
    const q = query(qc, { key: ["sh"], fetcher: async () => 1 });
    const stopE = createRoot(() => effect(() => q.data()));
    const req = only(events, "shared:request");
    assert.ok(req.some((e) => e.reason === "follower"), "follower emits shared:request");
    clock.advance(3001);                             // no leader replied -> fallback self-fetch
    const fb = only(events, "shared:fallback");
    assert.ok(fb.some((e) => e.reason === "follower-timeout"), "timeout emits shared:fallback");
    stopE(); q.dispose(); stop(); qc.dispose();
});

test("shared:serve (leader) when a follower's fetch-req reaches a leader that owns the entry", async () => {
    const { BroadcastChannel } = createMockBroadcastChannel();
    const common = { crossTab: true, sharedFetch: true, broadcastChannel: BroadcastChannel,
        crossTabChannel: "srv", defaultStaleTime: 0 };
    const leader = queryClient({ ...common, isLeader: () => true });
    const follower = queryClient({ ...common, isLeader: () => false });
    const evL = [];
    const sL = leader.inspect((e) => evL.push({ ...e }));
    const lq = query(leader, { key: ["k"], fetcher: async () => 7 });
    const stopL = createRoot(() => effect(() => lq.data()));
    await tick();                                    // leader has the entry alive with a fetcher
    const fq = query(follower, { key: ["k"], fetcher: async () => 9 });
    const stopF = createRoot(() => effect(() => fq.data()));
    await tick();                                    // follower broadcasts fetch-req; leader serves
    const serve = only(evL, "shared:serve");
    assert.ok(serve.some((e) => e.reason === "leader"), "leader emits shared:serve");
    stopL(); stopF(); lq.dispose(); fq.dispose();
    sL(); leader.dispose(); follower.dispose();
});
