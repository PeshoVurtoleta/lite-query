// Tests for @zakkster/lite-query/stream (Phase 2).
//
// streamQuery against a manually-driven async iterator, exercising both modes,
// all three termination paths, lazy subscription, abort-on-detach (iterator
// .return()), reactive-key restart, enabled gate, shared observers, cache
// interop (getQueryData / invalidate / removeQueries), and signal disposal.
//
// Run: node --test test/stream-query.test.js

import { test } from "node:test";
import assert from "node:assert/strict";

import { signal, effect } from "@zakkster/lite-signal";
import { queryClient } from "../Query.js";
import { streamQuery } from "../StreamQuery.js";

// A manually-driven async iterator. push() a value, complete() to finish,
// fail(err) to throw. `closed` flips true when the consumer calls return()
// (which is how lite-stream signals abort-on-detach). `starts` counts how many
// times the factory was invoked (for laziness / shared-observer assertions).
function makeController() {
    const c = {
        starts: 0,
        closed: false,
        _results: [],          // queued { type, value|err }
        _waiter: null,         // { resolve, reject } awaiting a value
    };
    function pump() {
        if (c._waiter && c._results.length) {
            const w = c._waiter; c._waiter = null;
            const d = c._results.shift();
            if (d.type === "error") w.reject(d.err);
            else if (d.type === "done") w.resolve({ done: true, value: undefined });
            else w.resolve({ done: false, value: d.value });
        }
    }
    const iterator = {
        next() {
            if (c._results.length) {
                const d = c._results.shift();
                if (d.type === "error") return Promise.reject(d.err);
                if (d.type === "done") return Promise.resolve({ done: true, value: undefined });
                return Promise.resolve({ done: false, value: d.value });
            }
            if (c.closed) return Promise.resolve({ done: true, value: undefined });
            return new Promise((resolve, reject) => { c._waiter = { resolve, reject }; });
        },
        return(v) {
            c.closed = true;
            if (c._waiter) { const w = c._waiter; c._waiter = null; w.resolve({ done: true, value: v }); }
            return Promise.resolve({ done: true, value: v });
        },
    };
    c.factory = () => { c.starts++; return iterator; };
    c.push = (value) => { c._results.push({ type: "value", value }); pump(); };
    c.complete = () => { c._results.push({ type: "done" }); pump(); };
    c.fail = (err) => { c._results.push({ type: "error", err }); pump(); };
    return c;
}

function tick() { return new Promise((r) => setTimeout(r, 0)); }

// ---------------------------------------------------------------------------
// latest mode
// ---------------------------------------------------------------------------

test("latest: values flow into data(); status pending -> streaming; count increments", async () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    const c = makeController();
    const sq = streamQuery(qc, { key: ["feed"], stream: c.factory });

    let seen;
    const stop = effect(() => { seen = sq.data(); });
    await tick();
    assert.equal(sq.status(), "pending");           // subscribed, no value yet
    assert.equal(sq.loading(), true);
    assert.equal(c.starts, 1);

    c.push(10);
    await tick();
    assert.equal(seen, 10);
    assert.equal(sq.status(), "streaming");
    assert.equal(sq.loading(), false);
    assert.equal(sq.count(), 1);

    c.push(20);
    await tick();
    assert.equal(seen, 20);
    assert.equal(sq.count(), 2);
    stop(); sq.dispose();
});

test("latest: natural done -> status success, done() true", async () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    const c = makeController();
    const sq = streamQuery(qc, { key: ["feed"], stream: c.factory });
    const stop = effect(() => sq.data());
    await tick();
    c.push(1); await tick();
    c.complete(); await tick();
    assert.equal(sq.status(), "success");
    assert.equal(sq.done(), true);
    stop(); sq.dispose();
});

test("latest: iterator error -> status error, error() set", async () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    const c = makeController();
    const sq = streamQuery(qc, { key: ["feed"], stream: c.factory });
    const stop = effect(() => sq.data());
    await tick();
    const boom = new Error("stream broke");
    c.fail(boom); await tick();
    assert.equal(sq.status(), "error");
    assert.equal(sq.error(), boom);
    stop(); sq.dispose();
});

// ---------------------------------------------------------------------------
// lifecycle: lazy, abort-on-detach, shared observers
// ---------------------------------------------------------------------------

test("lazy: the stream factory is not called until an accessor is read in an effect", async () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    const c = makeController();
    const sq = streamQuery(qc, { key: ["feed"], stream: c.factory });
    await tick();
    assert.equal(c.starts, 0, "no observers -> factory never called");
    // reading outside an effect does not subscribe
    assert.equal(sq.data(), undefined);
    await tick();
    assert.equal(c.starts, 0);
    const stop = effect(() => sq.data());
    await tick();
    assert.equal(c.starts, 1, "reading inside an effect starts the stream");
    stop(); sq.dispose();
});

test("abort-on-detach: last observer leaving closes the iterator (return())", async () => {
    const qc = queryClient({ defaultStaleTime: 60_000 });
    const c = makeController();
    const sq = streamQuery(qc, { key: ["feed"], stream: c.factory });
    const stop = effect(() => sq.data());
    await tick();
    c.push(1); await tick();
    assert.equal(c.closed, false);
    stop();                                          // dispose the consumer effect
    await tick();
    assert.equal(c.closed, true, "iterator.return() was called on detach");
    assert.equal(sq.status(), "idle", "active stream status reset on detach");
    sq.dispose();
});

test("shared observers: two handles on one key share a single stream", async () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    const c = makeController();
    const a = streamQuery(qc, { key: ["feed"], stream: c.factory });
    const b = streamQuery(qc, { key: ["feed"], stream: c.factory });
    let av, bv;
    const sa = effect(() => { av = a.data(); });
    const sb = effect(() => { bv = b.data(); });
    await tick();
    assert.equal(c.starts, 1, "only one stream pump for the shared key");
    c.push(42); await tick();
    assert.equal(av, 42);
    assert.equal(bv, 42);
    sa(); sb(); a.dispose(); b.dispose();
});

// ---------------------------------------------------------------------------
// reactive key + enabled
// ---------------------------------------------------------------------------

test("reactive key: a key change aborts the old stream and starts a new one", async () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    const channel = signal("a");
    const controllers = {};
    const sq = streamQuery(qc, {
        key: () => ["feed", channel()],
        stream: ({ key }) => {
            const c = makeController();
            controllers[key[1]] = c;
            return c.factory();
        },
    });
    let seen;
    const stop = effect(() => { seen = sq.data(); });
    await tick();
    controllers["a"].push("a1"); await tick();
    assert.equal(seen, "a1");

    channel.set("b");
    await tick();
    assert.equal(controllers["a"].closed, true, "old stream aborted on key change");
    controllers["b"].push("b1"); await tick();
    assert.equal(seen, "b1");
    stop(); sq.dispose();
});

test("enabled: false suppresses the stream; flipping true starts it", async () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    const on = signal(false);
    const c = makeController();
    const sq = streamQuery(qc, { key: ["feed"], stream: c.factory, enabled: () => on() });
    const stop = effect(() => sq.data());
    await tick();
    assert.equal(c.starts, 0, "disabled -> no stream");
    on.set(true);
    await tick();
    assert.equal(c.starts, 1, "enabled -> stream starts");
    stop(); sq.dispose();
});

// ---------------------------------------------------------------------------
// buffer mode
// ---------------------------------------------------------------------------

test("buffer: data() is a bounded window; droppedCount tracks overflow", async () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    const c = makeController();
    const sq = streamQuery(qc, { key: ["log"], stream: c.factory, mode: "buffer", maxBuffer: 3 });
    let seen;
    const stop = effect(() => { seen = sq.data(); });
    await tick();
    for (const v of [1, 2, 3, 4, 5]) { c.push(v); await tick(); }
    assert.deepEqual(seen, [3, 4, 5], "window holds the last maxBuffer values");
    assert.equal(sq.count(), 5);
    assert.equal(sq.droppedCount(), 2);
    stop(); sq.dispose();
});

test("buffer: rejects a missing/invalid maxBuffer", () => {
    const qc = queryClient({});
    const c = makeController();
    assert.throws(() => streamQuery(qc, { key: ["x"], stream: c.factory, mode: "buffer" }), TypeError);
    assert.throws(() => streamQuery(qc, { key: ["x"], stream: c.factory, mode: "buffer", maxBuffer: 0 }), TypeError);
});

// ---------------------------------------------------------------------------
// cache interop
// ---------------------------------------------------------------------------

test("getQueryData returns the current stream value (latest) and window (buffer)", async () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    const cl = makeController();
    const latest = streamQuery(qc, { key: ["latest"], stream: cl.factory });
    const s1 = effect(() => latest.data());
    await tick();
    cl.push("v1"); await tick();
    assert.equal(qc.getQueryData(["latest"]), "v1");

    const cb = makeController();
    const buf = streamQuery(qc, { key: ["buf"], stream: cb.factory, mode: "buffer", maxBuffer: 2 });
    const s2 = effect(() => buf.data());
    await tick();
    cb.push("a"); await tick(); cb.push("b"); await tick();
    assert.deepEqual(qc.getQueryData(["buf"]), ["a", "b"]);
    s1(); s2(); latest.dispose(); buf.dispose();
});

test("invalidate restarts the stream (new connection, count resets)", async () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    let made = 0;
    const controllers = [];
    const sq = streamQuery(qc, {
        key: ["feed"],
        stream: () => { const c = makeController(); controllers.push(c); made++; return c.factory(); },
    });
    const stop = effect(() => sq.data());
    await tick();
    controllers[0].push(1); await tick();
    assert.equal(sq.count(), 1);
    assert.equal(made, 1);

    qc.invalidate(["feed"]);
    await tick();
    assert.equal(controllers[0].closed, true, "old stream aborted by invalidate");
    assert.equal(made, 2, "a fresh stream was established");
    assert.equal(sq.count(), 0, "count reset on restart");
    controllers[1].push(99); await tick();
    assert.equal(sq.data(), 99);
    stop(); sq.dispose();
});

test("removeQueries aborts the stream and drops the entry", async () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    const c = makeController();
    const sq = streamQuery(qc, { key: ["feed"], stream: c.factory });
    const stop = effect(() => sq.data());
    await tick();
    c.push(1); await tick();
    qc.removeQueries(["feed"]);
    await tick();
    assert.equal(c.closed, true, "stream aborted on removeQueries");
    assert.equal(qc.getQueryData(["feed"]), undefined, "entry dropped");
    stop(); sq.dispose();
});

test("restart() imperatively re-establishes the stream", async () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    const controllers = [];
    const sq = streamQuery(qc, {
        key: ["feed"],
        stream: () => { const c = makeController(); controllers.push(c); return c.factory(); },
    });
    const stop = effect(() => sq.data());
    await tick();
    controllers[0].push(1); await tick();
    sq.restart();
    await tick();
    assert.equal(controllers[0].closed, true);
    assert.equal(controllers.length, 2);
    controllers[1].push(2); await tick();
    assert.equal(sq.data(), 2);
    stop(); sq.dispose();
});

// ---------------------------------------------------------------------------
// coexistence with query()
// ---------------------------------------------------------------------------

test("a streamQuery and a query coexist in one client without interference", async () => {
    const { query } = await import("../Query.js");
    const qc = queryClient({ defaultStaleTime: 0 });

    const q = query(qc, { key: ["q"], fetcher: async () => "fetched" });
    const c = makeController();
    const sq = streamQuery(qc, { key: ["s"], stream: c.factory });

    let qv, sv;
    const sqStop = effect(() => { sv = sq.data(); });
    const qStop = effect(() => { qv = q.data(); });
    await tick();
    c.push("streamed"); await tick();

    assert.equal(qv, "fetched");
    assert.equal(sv, "streamed");
    assert.equal(qc.getQueryData(["q"]), "fetched");
    assert.equal(qc.getQueryData(["s"]), "streamed");
    qStop(); sqStop(); q.dispose(); sq.dispose();
});
