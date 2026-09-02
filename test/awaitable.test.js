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

// Whole-namespace imports for the 18-name parity / identity block. The subpath
// re-exports must be the SAME bindings as upstream, not copies.
import * as subpath from "../Awaitable.js";
import * as upstream from "@zakkster/lite-await";
import { VERSION as queryVersion } from "../Query.js";
import {
    allSettledOf,
    withResolvers,
    tryFn,
    delay,
    withRetry,
    mapLimit,
    whenStatechart,
    createAwaitScope
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

// ---------------------------------------------------------------------------
// 18-name parity with lite-await 1.3.0 (Q-07). The eight new re-exports must
// be the IDENTICAL bindings upstream ships -- single source of truth, zero
// wrapping (OR-4). Each identity test asserts reference equality to upstream.
// ---------------------------------------------------------------------------

test("identity: allSettledOf is upstream's binding", () => {
    assert.equal(allSettledOf, upstream.allSettledOf);
});
test("identity: withResolvers is upstream's binding", () => {
    assert.equal(withResolvers, upstream.withResolvers);
});
test("identity: tryFn is upstream's binding", () => {
    assert.equal(tryFn, upstream.tryFn);
});
test("identity: delay is upstream's binding", () => {
    assert.equal(delay, upstream.delay);
});
test("identity: withRetry is upstream's binding", () => {
    assert.equal(withRetry, upstream.withRetry);
});
test("identity: mapLimit is upstream's binding", () => {
    assert.equal(mapLimit, upstream.mapLimit);
});
test("identity: whenStatechart is upstream's binding", () => {
    assert.equal(whenStatechart, upstream.whenStatechart);
});
test("identity: createAwaitScope is upstream's binding", () => {
    assert.equal(createAwaitScope, upstream.createAwaitScope);
});

test("VERSION on /await is lite-query's own, not lite-await's", () => {
    // The subpath exposes lite-query's OWN VERSION (Query.js, re-exported by
    // this entry in C6). lite-await's VERSION is deliberately NOT imported, so
    // it never leaks through -- the two differ (lite-query 1.1.x vs lite-await
    // 1.3.x). version-sync.test.js pins VERSION === package.json.
    assert.equal(subpath.VERSION, queryVersion);
    assert.notEqual(subpath.VERSION, upstream.VERSION);
});

test("subpath named surface == upstream minus VERSION plus the two bridges", () => {
    // The re-exported lite-await surface is EXACTLY every upstream name except
    // VERSION (lite-query does not re-export the sibling's VERSION), plus
    // lite-query's two native bridges. VERSION is filtered from `actual` too
    // because the VERSION that IS present is lite-query's own, asserted above.
    // A set compare catches any drift in either direction.
    const expected = new Set(Object.keys(upstream).filter((n) => n !== "VERSION"));
    expected.add("whenQuery");
    expected.add("whenAllQueries");
    const actual = new Set(Object.keys(subpath).filter((n) => n !== "VERSION"));
    assert.deepEqual([...actual].sort(), [...expected].sort());
});

test("withRetry: succeeds on the 3rd attempt after two transient failures", async () => {
    let attempt = 0;
    const result = await withRetry(
        function () {
            attempt = (attempt + 1) | 0;
            if (attempt < 3) return Promise.reject(new Error("transient " + attempt));
            return Promise.resolve("ok@" + attempt);
        },
        { attempts: 5, baseMs: 0 }
    );
    assert.equal(attempt, 3, "the factory ran exactly three times");
    assert.equal(result, "ok@3");
});

test("mapLimit: never exceeds the concurrency ceiling", async () => {
    let inFlight = 0;
    let peak = 0;
    const items = [0, 1, 2, 3, 4, 5, 6, 7];
    const out = await mapLimit(
        items,
        function (item) {
            inFlight = (inFlight + 1) | 0;
            if (inFlight > peak) peak = inFlight;
            return new Promise(function (resolve) {
                setTimeout(function () {
                    inFlight = (inFlight - 1) | 0;
                    resolve(item * 2);
                }, 5);
            });
        },
        3
    );
    assert.ok(peak <= 3, "in-flight count never exceeded the limit of 3 (peak " + peak + ")");
    assert.ok(peak >= 2, "the cap was actually exercised (peak " + peak + ")");
    assert.deepEqual(out, [0, 2, 4, 6, 8, 10, 12, 14], "results in input order");
});

test("createAwaitScope: scope.abort settles a pre-bound whenSignal and tears down", async () => {
    const scope = createAwaitScope();                 // owns a fresh controller
    const src = signal("waiting");
    // A predicate that never becomes true -- the only way this settles is abort.
    const p = scope.whenSignal(function () { return src(); }, function () { return false; });
    assert.equal(scope.aborted, false);
    const reason = new Error("scope closed");
    scope.abort(reason);
    // Reason-preserving: the pre-bound awaiter rejects with the scope's own
    // abort reason (upstream decisions/0007), and the scope signal reflects it.
    await assert.rejects(p, function (e) { return e === reason; });
    assert.equal(scope.aborted, true, "the scope signal reflects the abort (teardown observed)");
});
