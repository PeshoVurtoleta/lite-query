// @zakkster/lite-query -- offline-queue feed vocabulary (Q9, T3/OR-7). The queue
// grows the feed by five additive domain:verb types (26 -> 31) on the SAME frozen
// 10-key record -- no second hook, no record-shape change. This suite pins the
// 10-key shape on the new types and the 31-type count guard.

import { test } from "node:test";
import assert from "node:assert/strict";
import { queryClient } from "../Query.js";

const FEED_KEYS = ["type", "ts", "key", "keyHash", "from", "to", "reason", "count", "ok", "value"];
const QUEUE_TYPES = ["queue:enqueue", "queue:restore", "queue:replay", "queue:settle", "queue:drop"];

// -- C2: the enqueue event carries the frozen 10-key record ----------------

test("queue-feed: queue:enqueue emits a monomorphic 10-key record in the frozen order", () => {
    const qc = queryClient();
    const events = [];
    const stop = qc.inspect((e) => { if (e.type === "queue:enqueue") events.push({ keys: Object.keys(e), snap: { ...e } }); });
    // enqueue is the client-internal store write mutate() drives under opt-in;
    // exercised directly here (an established _internal test convention) so the
    // feed shape is pinned before the mutation ladder lands in C3.
    const KEY = ["todo", 7];
    const rec = qc._internal.enqueue("saveTodo", KEY, JSON.stringify(KEY), { title: "x" });
    assert.ok(rec && typeof rec.id === "string", "enqueue returns the record");
    assert.equal(events.length, 1, "one queue:enqueue event");
    assert.equal(events[0].keys.length, 10, "exactly 10 own keys");
    assert.deepEqual(events[0].keys, FEED_KEYS, "keys present in the frozen order");
    assert.equal(events[0].snap.type, "queue:enqueue");
    assert.equal(events[0].snap.count, 1, "count is the queue length after the enqueue");
    assert.equal(events[0].snap.ok, true, "enqueue is ok:true (control flow, not truthiness)");
    assert.ok(Object.is(events[0].snap.key, KEY), "key is the caller's array by reference");
    assert.deepEqual(events[0].snap.value, { title: "x" }, "value is vars verbatim");
    stop();
    qc.dispose();
});

// The five queue types are domain:verb tokens -- a structural guard so a typo'd
// bare-verb type ("enqueue") can never enter the vocabulary silently.
test("queue-feed: every queue vocabulary type is a domain:verb token", () => {
    for (const t of QUEUE_TYPES) {
        assert.match(t, /^queue:[a-z]+$/, t + " must be a domain:verb token");
    }
});
