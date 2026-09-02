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
