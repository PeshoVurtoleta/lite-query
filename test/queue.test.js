// @zakkster/lite-query -- offline mutation queue: core store + opt-in dispatch
// ladder (Q9, T1/OR-3/OR-4). The queue is durable per-client state that a
// mutation lands in ONLY under an explicit per-mutation opt-in when the caller's
// offline() oracle reports the transport down. Fresh clients hold no queue (null
// is not an empty array); maxQueue is validated at construction (fail closed).

import { test } from "node:test";
import assert from "node:assert/strict";
import { queryClient, mutation } from "../Query.js";

// -- C2: core store --------------------------------------------------------

test("queue: a fresh client reports queueSize() === 0 (queueStore null, not [])", () => {
    const qc = queryClient();
    // queueStore starts null -- no queue is not an empty queue (OR-3). queueSize()
    // reads 0 for the null store; the first enqueue lazily creates the backing
    // array, so a client that never queued retains zero queue objects.
    assert.equal(qc.queueSize(), 0, "fresh client has an empty queue count");
    qc.dispose();
});

test("queue: maxQueue is a positive integer, validated at construction (fail closed)", () => {
    // A non-integer, zero, negative, or NaN cap each throw a TypeError at
    // construction -- a caller who wrote a bad cap must not silently get 100.
    for (const bad of [1.5, 0, -1, NaN, "10", null]) {
        assert.throws(() => queryClient({ maxQueue: bad }), TypeError,
            "maxQueue " + String(bad) + " must throw at construction");
    }
    // A valid positive integer constructs cleanly; undefined defaults to 100.
    const qc = queryClient({ maxQueue: 5 });
    assert.equal(qc.queueSize(), 0);
    qc.dispose();
    const dflt = queryClient();
    assert.equal(dflt.queueSize(), 0);
    dflt.dispose();
});

// -- C3: opt-in shape validation at construction (ON-3) --------------------

test("queue: opt-in shape is validated at mutation() construction, fail closed (ON-3)", () => {
    const qc = queryClient();
    const fn = async () => "ok";
    // `queue` present and not a boolean -> a caller who wrote queue:1 believes
    // they opted in; silently taking the normal path is fail-open. TypeError.
    assert.throws(() => mutation(qc, { fn, queue: 1 }), TypeError, "queue:1 throws");
    assert.throws(() => mutation(qc, { fn, queue: "yes" }), TypeError, "queue:'yes' throws");
    // queue:true requires offline (function), name (string), queueKey (array).
    assert.throws(() => mutation(qc, { fn, queue: true, name: "m", queueKey: ["k"] }), TypeError, "missing offline throws");
    assert.throws(() => mutation(qc, { fn, queue: true, offline: () => true, queueKey: ["k"] }), TypeError, "missing name throws");
    assert.throws(() => mutation(qc, { fn, queue: true, offline: () => true, name: "m" }), TypeError, "missing queueKey throws");
    assert.throws(() => mutation(qc, { fn, queue: true, offline: () => true, name: 7, queueKey: ["k"] }), TypeError, "non-string name throws");
    assert.throws(() => mutation(qc, { fn, queue: true, offline: () => true, name: "m", queueKey: "k" }), TypeError, "non-array queueKey throws");
    // A well-formed opt-in constructs cleanly; queue:false is a no-op opt-out.
    assert.doesNotThrow(() => mutation(qc, { fn, queue: true, offline: () => false, name: "m", queueKey: ["k"] }));
    assert.doesNotThrow(() => mutation(qc, { fn, queue: false }));
    qc.dispose();
});

test("queue: a mutation that never opts in takes the unchanged 2.0.0 path", async () => {
    const qc = queryClient();
    let calls = 0;
    const m = mutation(qc, { fn: async (v) => { calls++; return v * 2; } });
    const out = await m.mutate(21);
    assert.equal(out, 42, "fn ran and resolved normally");
    assert.equal(calls, 1, "fn called exactly once");
    assert.equal(m.status(), "success");
    assert.equal(qc.queueSize(), 0, "nothing queued on the no-opt-in path");
    m.dispose();
    qc.dispose();
});

// -- C3: runtime oracle outcomes (fail closed) -----------------------------

test("queue: a throwing offline() rejects LQ_OFFLINE_ORACLE, nothing dispatched or queued", async () => {
    const qc = queryClient();
    let calls = 0;
    const m = mutation(qc, {
        fn: async () => { calls++; return "ok"; },
        queue: true, offline: () => { throw new Error("oracle boom"); },
        name: "m", queueKey: ["k"],
    });
    await assert.rejects(m.mutate({ n: 1 }), (e) => e.code === "LQ_OFFLINE_ORACLE");
    assert.equal(m.status(), "error", "status settles error on an unverified oracle");
    assert.equal(calls, 0, "fn never ran (nothing dispatched)");
    assert.equal(qc.queueSize(), 0, "nothing queued");
    m.dispose();
    qc.dispose();
});

test("queue: a non-boolean offline() return rejects LQ_OFFLINE_ORACLE (null is not zero)", async () => {
    const qc = queryClient();
    const m = mutation(qc, {
        fn: async () => "ok",
        queue: true, offline: () => 1,   // truthy non-boolean -- unverified, fail closed
        name: "m", queueKey: ["k"],
    });
    await assert.rejects(m.mutate({ n: 1 }), (e) => e.code === "LQ_OFFLINE_ORACLE");
    assert.equal(qc.queueSize(), 0, "nothing queued");
    m.dispose();
    qc.dispose();
});

test("queue: offline() === true enqueues and resolves { queued: true, id }", async () => {
    const qc = queryClient();
    let fnCalls = 0;
    let settledCalls = 0;
    let settledArgs = null;
    const m = mutation(qc, {
        fn: async () => { fnCalls++; return "ok"; },
        queue: true, offline: () => true, name: "saveTodo", queueKey: ["todo", 1],
        onSettled: (data, err, vars, ctx) => { settledCalls++; settledArgs = [data, err, vars, ctx]; },
    });
    const receipt = await m.mutate({ title: "x" });
    assert.deepEqual(Object.keys(receipt), ["queued", "id"], "receipt is exactly { queued, id }");
    assert.equal(receipt.queued, true);
    assert.equal(typeof receipt.id, "string");
    assert.equal(m.status(), "queued", "status settles the new 'queued' value");
    assert.equal(m.loading(), false, "a queued mutation is not loading");
    assert.equal(fnCalls, 0, "fn never ran");
    assert.equal(settledCalls, 1, "onSettled ran exactly once (Phase-4 law)");
    assert.deepEqual(settledArgs, [undefined, undefined, { title: "x" }, undefined], "onSettled args are (undefined, undefined, vars, undefined)");
    assert.equal(qc.queueSize(), 1, "the item is durably enqueued");
    m.dispose();
    qc.dispose();
});

test("queue: enqueue against a full queue rejects LQ_QUEUE_FULL, size unchanged (OR-3)", async () => {
    const qc = queryClient({ maxQueue: 2 });
    const mk = () => mutation(qc, {
        fn: async () => "ok",
        queue: true, offline: () => true, name: "m", queueKey: ["k"],
    });
    await mk().mutate({ n: 1 });
    await mk().mutate({ n: 2 });
    assert.equal(qc.queueSize(), 2, "queue filled to the cap");
    await assert.rejects(mk().mutate({ n: 3 }), (e) => e.code === "LQ_QUEUE_FULL");
    assert.equal(qc.queueSize(), 2, "a rejected enqueue leaves the queue unchanged");
    qc.dispose();
});
