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
import { createMockClock } from "./harness.js";

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
