// @zakkster/lite-query -- offline-queue persistence seam (Q9, T2/OR-5). The
// queue rides SIBLING queueSave/queueLoad thunks on the same adapter (the frozen
// envelope/state length gates forbid riding inside them). The adapter stamps the
// version; a mismatch or ANY corruption drops the WHOLE queue fail-closed and
// surfaces the drop twice (queueRestored + a queue:drop feed event). Only
// enqueued records are ever persisted.

import { test } from "node:test";
import assert from "node:assert/strict";
import { queryClient, mutation, persistQueryClient } from "../Query.js";

// A valid persisted record (the 7 frozen keys in order).
function rec(id, key) {
    return { id, name: "saveTodo", key, keyHash: JSON.stringify(key), vars: { n: 1 }, at: 1, tries: 0 };
}

// Enqueue an offline mutation against a seeded entry (so the record exists).
async function enqueueOffline(qc, key, vars) {
    qc.setQueryData(key, { seeded: true });
    const m = mutation(qc, { fn: async () => "live", queue: true, offline: () => true, name: "saveTodo", queueKey: key });
    const r = await m.mutate(vars);
    m.dispose();
    return r;
}

// -- C5: install-time both-or-neither validation ---------------------------

test("queue-persist: queueSave without queueLoad (or vice versa) throws at install (fail closed)", () => {
    const qc = queryClient();
    assert.throws(() => persistQueryClient(qc, { save: () => {}, load: () => null, version: "v1", queueSave: () => {} }),
        /`queueLoad` must be a function/, "queueSave alone is a half-durable queue");
    const qc2 = queryClient();
    assert.throws(() => persistQueryClient(qc2, { save: () => {}, load: () => null, version: "v1", queueLoad: () => null }),
        /`queueSave` must be a function/, "queueLoad alone is a half-durable queue");
    qc.dispose(); qc2.dispose();
});

// -- C5: empty-is-not-error + round-trip restore ---------------------------

test("queue-persist: queueLoad() -> null resolves empty; a valid queue round-trips", async () => {
    // Empty (null) queue section is "no queue", never an error (OR-3).
    const qc = queryClient();
    const p = persistQueryClient(qc, { save: () => {}, load: () => null, version: "v1", throttle: 0,
        queueSave: () => {}, queueLoad: () => null });
    assert.deepEqual(await p.queueRestored, { status: "empty", count: 0, reason: null });
    p.stop(); qc.dispose();

    // Round trip: enqueue offline in A -> the queueSave payload restores into B.
    let durable = null;
    const A = queryClient();
    const pa = persistQueryClient(A, { save: () => {}, load: () => null, version: "v1", throttle: 0,
        queueSave: (env) => { durable = env; }, queueLoad: () => null });
    await pa.queueRestored;
    await enqueueOffline(A, ["todo", 1], { title: "x" });
    assert.ok(durable && durable.queue.length === 1, "enqueue persisted through queueSave");

    const B = queryClient();
    const pb = persistQueryClient(B, { save: () => {}, load: () => null, version: "v1", throttle: 0,
        queueSave: () => {}, queueLoad: () => durable });
    const outcome = await pb.queueRestored;
    assert.equal(outcome.status, "restored");
    assert.equal(outcome.count, 1);
    assert.equal(B.queueSize(), 1, "the restored item is durably present in B");
    pa.stop(); pb.stop(); A.dispose(); B.dispose();
});

// -- C5: malformed envelope drops whole + surfaces the drop ----------------

test("queue-persist: a 3-key envelope drops the whole queue (malformed-envelope) + one queue:drop", async () => {
    const qc = queryClient();
    const drops = [];
    const stopInspect = qc.inspect((e) => { if (e.type === "queue:drop") drops.push({ ...e }); });
    const p = persistQueryClient(qc, { save: () => {}, load: () => null, version: "v1", throttle: 0,
        queueSave: () => {}, queueLoad: () => ({ version: "v1", queue: [], extra: 1 }) });
    const outcome = await p.queueRestored;
    assert.equal(outcome.status, "dropped");
    assert.equal(outcome.reason, "malformed-envelope");
    assert.equal(outcome.count, 0);
    assert.equal(qc.queueSize(), 0, "nothing restored");
    assert.equal(drops.length, 1, "the drop is surfaced on the feed exactly once");
    assert.equal(drops[0].reason, "malformed-envelope");
    stopInspect(); p.stop(); qc.dispose();
});

// -- C5: strict version mismatch drops whole -------------------------------

test("queue-persist: version '2' against version 2 is a strict mismatch, whole queue drops", async () => {
    const qc = queryClient();
    const drops = [];
    const stopInspect = qc.inspect((e) => { if (e.type === "queue:drop") drops.push({ ...e }); });
    const p = persistQueryClient(qc, { save: () => {}, load: () => null, version: 2, throttle: 0,
        queueSave: () => {}, queueLoad: () => ({ version: "2", queue: [rec("c1:1", ["todo", 1])] }) });
    const outcome = await p.queueRestored;
    assert.equal(outcome.status, "dropped");
    assert.equal(outcome.reason, "version-mismatch", "string '2' !== number 2 (no coercion)");
    assert.equal(qc.queueSize(), 0);
    assert.equal(drops.length, 1, "exactly one queue:drop event");
    stopInspect(); p.stop(); qc.dispose();
});

// -- C5: one corrupt record drops all + in-flight exclusion ----------------

test("queue-persist: one corrupt record drops all four; a pending non-queued mutation is never persisted", async () => {
    // Whole-drop: 3 valid records + 1 corrupt (tries a string) -> nothing restores.
    const qc = queryClient();
    const bad = rec("c1:4", ["todo", 4]); bad.tries = "nope";
    const payload = { version: "v1", queue: [rec("c1:1", ["todo", 1]), rec("c1:2", ["todo", 2]), rec("c1:3", ["todo", 3]), bad] };
    const p = persistQueryClient(qc, { save: () => {}, load: () => null, version: "v1", throttle: 0,
        queueSave: () => {}, queueLoad: () => payload });
    const outcome = await p.queueRestored;
    assert.equal(outcome.status, "dropped");
    assert.equal(outcome.reason, "malformed-record");
    assert.equal(outcome.count, 0, "a single bad record drops the whole queue");
    assert.equal(qc.queueSize(), 0);
    p.stop(); qc.dispose();

    // In-flight exclusion (OR-5): only ENQUEUED records are persisted. A pending
    // non-queued mutation has no record and cannot ride the queueSave payload.
    let durable = null;
    const B = queryClient();
    const pb = persistQueryClient(B, { save: () => {}, load: () => null, version: "v1", throttle: 0,
        queueSave: (env) => { durable = env; }, queueLoad: () => null });
    await pb.queueRestored;
    await enqueueOffline(B, ["todo", 9], { title: "queued" });
    let releasePending;
    const pending = new Promise((res) => { releasePending = res; });
    const nonQueued = mutation(B, { fn: () => pending });    // ordinary mutation, still in flight
    const inflight = nonQueued.mutate({ title: "in-flight" });
    pb.flush();
    assert.equal(durable.queue.length, 1, "only the enqueued record is persisted");
    assert.equal(durable.queue[0].key[1], 9, "the persisted record is the queued one");
    releasePending("done"); await inflight;
    nonQueued.dispose(); pb.stop(); B.dispose();
});
