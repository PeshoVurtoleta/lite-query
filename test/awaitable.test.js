// Tests for @zakkster/lite-query/await (Phase 1).
//
// Exercises the whenQuery / whenAllQueries bridges against mock query handles
// backed by REAL lite-signal signals (so whenSignal's reactive teardown runs
// for real), plus a smoke check that the lite-await primitives are re-exported.
//
// Run: node --test test/awaitable.test.js

import { test } from "node:test";
import assert from "node:assert/strict";

import { signal } from "@zakkster/lite-signal";
import {
    whenQuery,
    whenAllQueries,
    whenSignal,
    whenTruthy,
    whenEquals,
    allOf,
    anyOf,
    raceOf,
    withTimeout,
    withAbort,
    fromPromise,
    TimeoutError
} from "../Awaitable.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// A mock query handle backed by real signals. status()/data()/error() read
// reactively; the _set* helpers drive transitions from the test.
function makeQuery(initialStatus) {
    const status = signal(initialStatus !== undefined ? initialStatus : "pending");
    const data = signal(undefined);
    const error = signal(undefined);
    return {
        status: function () { return status(); },
        data: function () { return data(); },
        error: function () { return error(); },
        _status: function (s) { status.set(s); },
        _data: function (d) { data.set(d); },
        _error: function (e) { error.set(e); }
    };
}

// Resolve once the microtask queue (and any 0ms timers) drains.
function tick() {
    return new Promise(function (r) { setTimeout(r, 0); });
}

// ---------------------------------------------------------------------------
// whenQuery
// ---------------------------------------------------------------------------

test("whenQuery: resolves with data() when status reaches success", async () => {
    const q = makeQuery("pending");
    const p = whenQuery(q);
    q._data({ id: 1, name: "Ada" });
    q._status("success");
    const result = await p;
    assert.deepEqual(result, { id: 1, name: "Ada" });
});

test("whenQuery: rejects with error() when status reaches error", async () => {
    const q = makeQuery("pending");
    const boom = new Error("fetch failed");
    const p = whenQuery(q);
    q._error(boom);
    q._status("error");
    await assert.rejects(p, function (e) { return e === boom; });
});

test("whenQuery: resolves immediately when already success", async () => {
    const q = makeQuery("success");
    q._data(42);
    const result = await whenQuery(q);
    assert.equal(result, 42);
});

test("whenQuery: rejects immediately when already error", async () => {
    const q = makeQuery("error");
    const boom = new Error("already broken");
    q._error(boom);
    await assert.rejects(whenQuery(q), function (e) { return e === boom; });
});

test("whenQuery: honors a custom predicate (waits for streaming)", async () => {
    const q = makeQuery("pending");
    const p = whenQuery(q, function (s) { return s === "streaming"; });
    q._data(["chunk-1"]);
    q._status("streaming");
    const result = await p;
    assert.deepEqual(result, ["chunk-1"]);
});

test("whenQuery: custom predicate still rejects on error status", async () => {
    const q = makeQuery("pending");
    const boom = new Error("died mid-stream");
    const p = whenQuery(q, function (s) { return s === "streaming"; });
    q._error(boom);
    q._status("error");
    await assert.rejects(p, function (e) { return e === boom; });
});

test("whenQuery: honors timeout when the query never settles", async () => {
    const q = makeQuery("pending");
    await assert.rejects(
        whenQuery(q, undefined, { timeout: 30 }),
        function (e) { return e instanceof TimeoutError; }
    );
});

test("whenQuery: whenQuery(q, opts) overload treats 2nd arg as options", async () => {
    const q = makeQuery("pending");
    await assert.rejects(
        whenQuery(q, { timeout: 30 }),
        function (e) { return e instanceof TimeoutError; }
    );
});

test("whenQuery: honors an AbortSignal", async () => {
    const q = makeQuery("pending");
    const ac = new AbortController();
    const p = whenQuery(q, undefined, { signal: ac.signal });
    ac.abort();
    await assert.rejects(p);
});

test("whenQuery: rejects a non-handle argument", async () => {
    await assert.rejects(
        whenQuery({ status: "not a function" }),
        function (e) { return e instanceof TypeError; }
    );
});

// ---------------------------------------------------------------------------
// whenAllQueries
// ---------------------------------------------------------------------------

test("whenAllQueries: resolves with data array in input order", async () => {
    const a = makeQuery("pending");
    const b = makeQuery("pending");
    const c = makeQuery("pending");
    const p = whenAllQueries([a, b, c]);

    a._data("A"); a._status("success");
    c._data("C"); c._status("success");
    b._data("B"); b._status("success");

    const result = await p;
    assert.deepEqual(result, ["A", "B", "C"]);
});

test("whenAllQueries: rejects on the first query to error", async () => {
    const a = makeQuery("pending");
    const b = makeQuery("pending");
    const boom = new Error("b failed");
    const p = whenAllQueries([a, b]);

    a._data("A"); a._status("success");
    b._error(boom); b._status("error");

    await assert.rejects(p, function (e) { return e === boom; });
});

test("whenAllQueries: empty array resolves to []", async () => {
    const result = await whenAllQueries([]);
    assert.deepEqual(result, []);
});

test("whenAllQueries: resolves immediately when all already success", async () => {
    const a = makeQuery("success"); a._data(1);
    const b = makeQuery("success"); b._data(2);
    const result = await whenAllQueries([a, b]);
    assert.deepEqual(result, [1, 2]);
});

test("whenAllQueries: honors timeout when one query hangs", async () => {
    const a = makeQuery("success"); a._data(1);
    const b = makeQuery("pending");                 // never settles
    await assert.rejects(
        whenAllQueries([a, b], { timeout: 30 }),
        function (e) { return e instanceof TimeoutError; }
    );
});

test("whenAllQueries: rejects a non-array argument", async () => {
    await assert.rejects(
        whenAllQueries("nope"),
        function (e) { return e instanceof TypeError; }
    );
});

// ---------------------------------------------------------------------------
// Re-export smoke
// ---------------------------------------------------------------------------

test("re-exports: lite-await primitives are present and callable", () => {
    for (const fn of [whenSignal, whenTruthy, whenEquals, allOf, anyOf, raceOf, withTimeout, withAbort, fromPromise]) {
        assert.equal(typeof fn, "function");
    }
    assert.equal(typeof TimeoutError, "function");
    // TimeoutError is a real Error subclass.
    const e = new TimeoutError(100);
    assert.ok(e instanceof Error);
    assert.equal(e.name, "TimeoutError");
});

test("re-exports: fromPromise projects a promise into signal state", async () => {
    const sig = fromPromise(Promise.resolve("done"), "initial");
    // pending synchronously, resolved after the promise settles
    assert.equal(sig.peek().status, "pending");
    assert.equal(sig.peek().data, "initial");
    await tick();
    assert.equal(sig.peek().status, "resolved");
    assert.equal(sig.peek().data, "done");
});
