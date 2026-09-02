// @zakkster/lite-query -- offline-queue crash boundary (Q9, OR-6/G9). The RULED
// semantics is AT-LEAST-ONCE: a record leaves the durable queue only AFTER its
// terminal per-item result exists AND the removal has been handed to queueSave.
// A crash between dispatch and that persisted removal replays the item on the
// next call (a double-fire is caller-addressable via the record's stable id).

import { test } from "node:test";
import assert from "node:assert/strict";
import { queryClient, mutation, persistQueryClient } from "../Query.js";

async function enqueueOffline(qc, key, vars) {
    qc.setQueryData(key, { seeded: true });
    const m = mutation(qc, { fn: async () => "live", queue: true, offline: () => true, name: "saveTodo", queueKey: key });
    const r = await m.mutate(vars);
    m.dispose();
    return r;
}

// -- C6: removal ordering (fn -> terminal -> queueSave -> splice) -----------

test("queue-crash: the removal is persisted (item absent) only after the terminal result exists", async () => {
    const qc = queryClient();
    const log = [];
    let durable = null;
    const p = persistQueryClient(qc, {
        save: () => {}, load: () => null, version: "v1", throttle: 0,
        queueSave: (env) => { log.push("save:len=" + env.queue.length); durable = env; },
        queueLoad: () => null,
    });
    await p.queueRestored;
    await enqueueOffline(qc, ["todo", 1], { n: 1 });
    assert.deepEqual(log, ["save:len=1"], "the enqueue persisted the item (len 1)");

    const result = await qc.replayQueue(() => async () => { log.push("dispatch"); return "server-ok"; });
    // Order proves at-least-once: the handler dispatched (terminal result exists)
    // BEFORE the removal write, and that removal write reflects the item's absence
    // (len 0), i.e. no record is removed before its terminal result exists.
    assert.deepEqual(log, ["save:len=1", "dispatch", "save:len=0"],
        "fn -> record terminal -> queueSave(removal, item absent) -> splice");
    assert.equal(result.replayed, 1);
    assert.equal(qc.queueSize(), 0, "the resolved item was spliced after the durable removal");
    p.stop(); qc.dispose();
});

// -- C6/G9: a crash between dispatch and the persisted removal replays -------

test("queue: a crash between dispatch and the persisted removal replays the item (at-least-once, G9)", async () => {
    // The durable store. The removal write (queue length 0) is where the "crash"
    // happens -- it throws before durable is overwritten, so the durable copy
    // keeps the item exactly as the enqueue persisted it.
    let durable = null;
    const queueSaveA = (env) => {
        if (env.queue.length === 0) throw new Error("crash: removal write never reached disk");
        durable = env;
    };

    const A = queryClient();
    const pa = persistQueryClient(A, { save: () => {}, load: () => null, version: "v1", throttle: 0,
        queueSave: queueSaveA, queueLoad: () => null });
    await pa.queueRestored;
    const receipt = await enqueueOffline(A, ["todo", 1], { n: 1 });
    const origId = receipt.id;
    assert.ok(durable && durable.queue.length === 1, "the enqueue reached the durable store");

    // Dispatch resolves (the mutation DID run), but the removal write crashes.
    const resA = await A.replayQueue(() => async () => "server-ok");
    assert.equal(resA.replayed, 1, "the item dispatched exactly once in-run");
    assert.equal(A.queueSize(), 1, "the removal-write crash leaves the item durable in memory too");
    assert.deepEqual(durable.queue[0].id, origId, "the durable copy still holds the item with its id");

    // The reload: a FRESH client loads the last durable payload (item present).
    const B = queryClient();
    const dispatched = [];
    const pb = persistQueryClient(B, { save: () => {}, load: () => null, version: "v1", throttle: 0,
        queueSave: () => {}, queueLoad: () => durable });
    const outcome = await pb.queueRestored;
    assert.equal(outcome.status, "restored");
    assert.equal(B.queueSize(), 1, "the item survived the crash and restored");
    // The reloaded tab has its cache entry back (restored / re-fetched), so the
    // record resolves to a live entry rather than dropping as entry-missing.
    B.setQueryData(["todo", 1], { seeded: true });
    await B.replayQueue((r) => async () => { dispatched.push(r.id); return "server-ok"; });
    assert.deepEqual(dispatched, [origId], "the item re-dispatches with the IDENTICAL id (idempotency key)");
    assert.equal(B.queueSize(), 0, "the second dispatch resolved and removed it");

    pa.stop(); pb.stop(); A.dispose(); B.dispose();
});
