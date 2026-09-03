// @zakkster/lite-query -- offline-queue replay surface + drop lever (Q9, T3/
// OR-4/ON-4). replayQueue(resolve) is caller-triggered, sequential FIFO,
// single-flight; per-item results are surfaced; undispatchable records DROP,
// rejections KEEP the item (tries++), resolutions REMOVE it. dropQueued(id) is
// the poison-item exit.

import { test } from "node:test";
import assert from "node:assert/strict";
import { queryClient, mutation } from "../Query.js";

const RESULT_KEYS = ["id", "key", "status", "value", "reason"];

// Seed a cache entry for `key` so replay's entry-lookup succeeds, then enqueue a
// mutation against it while offline. Returns the enqueue receipt.
async function enqueueOffline(qc, name, key, vars) {
    qc.setQueryData(key, { seeded: true });
    const m = mutation(qc, { fn: async () => "live", queue: true, offline: () => true, name, queueKey: key });
    const receipt = await m.mutate(vars);
    m.dispose();
    return receipt;
}

// -- C4: FIFO order + exactly-once dispatch --------------------------------

test("queue-replay: 5 items across 2 keys replay in enqueue order, each fn once", async () => {
    const qc = queryClient();
    const enqOrder = [];
    // Interleave two keys: k1, k2, k1, k2, k1.
    enqOrder.push((await enqueueOffline(qc, "a", ["k1"], { n: 1 })).id);
    enqOrder.push((await enqueueOffline(qc, "b", ["k2"], { n: 2 })).id);
    enqOrder.push((await enqueueOffline(qc, "a", ["k1"], { n: 3 })).id);
    enqOrder.push((await enqueueOffline(qc, "b", ["k2"], { n: 4 })).id);
    enqOrder.push((await enqueueOffline(qc, "a", ["k1"], { n: 5 })).id);
    assert.equal(qc.queueSize(), 5);

    const dispatched = [];
    const callCounts = new Map();
    const resolve = (rec) => (vars) => {
        dispatched.push(rec.id);
        callCounts.set(rec.id, (callCounts.get(rec.id) || 0) + 1);
        return "server-ok";
    };
    const result = await qc.replayQueue(resolve);

    assert.deepEqual(dispatched, enqOrder, "dispatch order deep-equals global enqueue order");
    for (const c of callCounts.values()) assert.equal(c, 1, "each handler fired exactly once");
    assert.equal(result.status, "done");
    assert.equal(result.replayed, 5);
    assert.equal(result.failed, 0);
    assert.equal(result.dropped, 0);
    assert.equal(qc.queueSize(), 0, "resolved items are removed");
    qc.dispose();
});

test("queue-replay: an empty/absent queue resolves the empty result shape", async () => {
    const qc = queryClient();
    const r = await qc.replayQueue(() => () => "x");
    assert.deepEqual(r, { status: "empty", total: 0, replayed: 0, failed: 0, dropped: 0, items: [] });
    qc.dispose();
});

test("queue-replay: replayQueue requires a resolver function (synchronous TypeError)", () => {
    const qc = queryClient();
    assert.throws(() => qc.replayQueue(null), TypeError);
    assert.throws(() => qc.replayQueue(123), TypeError);
    qc.dispose();
});

// -- C4: single-flight -----------------------------------------------------

test("queue-replay: a concurrent replay rejects LQ_REPLAY_BUSY while one is in flight", async () => {
    const qc = queryClient();
    await enqueueOffline(qc, "a", ["k1"], { n: 1 });
    let release;
    const gate = new Promise((res) => { release = res; });
    const resolve = () => async () => { await gate; return "ok"; };
    const first = qc.replayQueue(resolve);          // starts, awaits the gate
    await Promise.resolve();                          // let the loop reach the await
    await assert.rejects(qc.replayQueue(resolve), (e) => e.code === "LQ_REPLAY_BUSY");
    release();
    await first;
    qc.dispose();
});

// -- C4: per-item dispositions ---------------------------------------------

test("queue-replay: a handler rejecting with null settles error, item stays queued (tries 1)", async () => {
    const qc = queryClient();
    await enqueueOffline(qc, "a", ["k1"], { n: 1 });
    // Rejection tracked by CONTROL FLOW: a falsy (null) rejection is still error.
    const result = await qc.replayQueue(() => () => Promise.reject(null));
    assert.equal(result.status, "done");
    assert.equal(result.failed, 1);
    assert.equal(result.replayed, 0);
    assert.equal(result.items[0].status, "error");
    assert.equal(result.items[0].value, null, "error value is the rejection verbatim (falsy included)");
    assert.equal(qc.queueSize(), 1, "an errored item remains durable for the next call");

    // The item carries tries === 1 after one dispatch; a second replay makes it 2.
    let seenTries = null;
    await qc.replayQueue((rec) => { seenTries = rec.tries; return () => "ok"; });
    assert.equal(seenTries, 1, "tries reflects the prior dispatch attempt");
    assert.equal(qc.queueSize(), 0, "the retried item finally resolved and was removed");
    qc.dispose();
});

test("queue-replay: a record whose entry no longer exists is dropped (entry-missing), never dispatched", async () => {
    const qc = queryClient();
    await enqueueOffline(qc, "a", ["k1"], { n: 1 });
    qc.removeQueries(["k1"]);                          // the entry is gone
    let handlerCalls = 0;
    const result = await qc.replayQueue(() => () => { handlerCalls++; return "ok"; });
    assert.equal(handlerCalls, 0, "handler never called for a missing entry");
    assert.equal(result.dropped, 1);
    assert.equal(result.items[0].status, "dropped");
    assert.equal(result.items[0].reason, "entry-missing");
    assert.equal(qc.queueSize(), 0, "the dropped item is removed, never silently retried");
    qc.dispose();
});

test("queue-replay: unresolved and throwing resolvers drop the item with the honest reason", async () => {
    const qc = queryClient();
    await enqueueOffline(qc, "a", ["k1"], { n: 1 });
    // resolve returns null -> handler-unresolved.
    let r = await qc.replayQueue(() => null);
    assert.equal(r.items[0].status, "dropped");
    assert.equal(r.items[0].reason, "handler-unresolved");
    assert.equal(qc.queueSize(), 0);

    await enqueueOffline(qc, "a", ["k1"], { n: 2 });
    // resolve throws -> resolver-threw.
    r = await qc.replayQueue(() => { throw new Error("resolver boom"); });
    assert.equal(r.items[0].status, "dropped");
    assert.equal(r.items[0].reason, "resolver-threw");
    assert.equal(qc.queueSize(), 0);
    qc.dispose();
});

test("queue-replay: every per-item result has exactly 5 keys and the return has 6", async () => {
    const qc = queryClient();
    await enqueueOffline(qc, "a", ["k1"], { n: 1 });
    const r = await qc.replayQueue(() => () => "ok");
    assert.deepEqual(Object.keys(r), ["status", "total", "replayed", "failed", "dropped", "items"]);
    assert.equal(r.items.length, 1);
    assert.deepEqual(Object.keys(r.items[0]), RESULT_KEYS, "per-item result is the 5-key shape in order");
    qc.dispose();
});

// -- C4: dropQueued poison-item exit (ON-4) --------------------------------

test("queue-replay: dropQueued(knownId) removes the record and fires one queue:drop", async () => {
    const qc = queryClient();
    const receipt = await enqueueOffline(qc, "a", ["k1"], { n: 1 });
    await enqueueOffline(qc, "a", ["k1"], { n: 2 });
    assert.equal(qc.queueSize(), 2);

    const drops = [];
    const stop = qc.inspect((e) => { if (e.type === "queue:drop") drops.push({ ...e }); });
    const ok = qc.dropQueued(receipt.id);
    assert.equal(ok, true, "known id returns true");
    assert.equal(qc.queueSize(), 1, "queueSize decremented");
    assert.equal(drops.length, 1, "exactly one queue:drop event");
    assert.equal(drops[0].reason, "caller-dropped");
    assert.equal(drops[0].count, 1, "count is the queue length after the drop");
    stop();
    qc.dispose();
});

test("queue-replay: dropQueued of an unknown id (or empty queue) returns false, no event", async () => {
    const qc = queryClient();
    // Empty queue -> false, no throw.
    let events = 0;
    const stop = qc.inspect((e) => { if (e.type === "queue:drop") events++; });
    assert.equal(qc.dropQueued("nope"), false, "absent id on a null store is an answer, not an error");
    await enqueueOffline(qc, "a", ["k1"], { n: 1 });
    assert.equal(qc.dropQueued("also-nope"), false, "absent id on a live store returns false");
    assert.equal(events, 0, "no queue:drop event for an absent id");
    assert.equal(qc.queueSize(), 1, "the queue is unchanged");
    stop();
    qc.dispose();
});

// -- QD-3: dropQueued mid-replay makes the poison-item exit actually exit ----

test("queue-replay: dropQueued fired mid-replay evicts a not-yet-dispatched item (QD-3)", async () => {
    const qc = queryClient();
    const ids = [];
    ids.push((await enqueueOffline(qc, "a", ["k1"], { n: 1 })).id);
    ids.push((await enqueueOffline(qc, "a", ["k1"], { n: 2 })).id);
    ids.push((await enqueueOffline(qc, "a", ["k1"], { n: 3 })).id);  // the poison item
    assert.equal(qc.queueSize(), 3);

    const drops = [];
    const stop = qc.inspect((e) => { if (e.type === "queue:drop") drops.push({ ...e }); });

    // The 2nd item's handler drops the 3rd (still queued, not yet dispatched).
    const dispatched = [];
    const result = await qc.replayQueue((rec) => (vars) => {
        dispatched.push(rec.id);
        if (rec.id === ids[1]) qc.dropQueued(ids[2]);   // poison-item exit mid-run
        return "server-ok";
    });

    // The 3rd handler is NEVER called; the item exits as dropped/caller-dropped.
    assert.deepEqual(dispatched, [ids[0], ids[1]], "the dropped item never dispatched");
    const third = result.items.find((r) => r.id === ids[2]);
    assert.equal(third.status, "dropped", "the mid-run eviction is a dropped result");
    assert.equal(third.reason, "caller-dropped");
    assert.equal(result.replayed, 2);
    assert.equal(result.dropped, 1);
    assert.equal(result.failed, 0);
    assert.equal(result.total, 3, "counts are coherent with the 3 snapshot records");
    assert.equal(qc.queueSize(), 0, "nothing stranded");
    // Exactly ONE queue:drop for that id -- from the dropQueued call, not a second
    // emit from the replay loop.
    assert.equal(drops.length, 1, "exactly one queue:drop total");
    assert.equal(drops[0].reason, "caller-dropped");
    stop();
    qc.dispose();
});
