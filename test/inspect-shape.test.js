// @zakkster/lite-query -- devtools feed record shape + identity contract (Q7).
//
// The feed emits ONE monomorphic record: exactly 10 own keys, always present,
// in one order -> one hidden class per event type. Records are POOLED per type
// and overwritten in place, so the hook MUST copy what it keeps (OR-6). This
// suite pins the 10-key shape, the resolved-once ts clock, the reuse identity,
// and the by-reference key. These subjects exist once C2's status/entry events
// land.

import { test } from "node:test";
import assert from "node:assert/strict";
import { queryClient } from "../Query.js";

const FEED_KEYS = ["type", "ts", "key", "keyHash", "from", "to", "reason", "count", "ok", "value"];

test("shape: every event has exactly the 10 frozen own keys in order", () => {
    const qc = queryClient();
    const shapes = [];
    const stop = qc.inspect((e) => shapes.push(Object.keys(e)));
    qc.setQueryData(["x"], 1);               // create + status
    qc.invalidate(["x"]);                    // stale
    qc.removeQueries(["x"]);                 // remove
    assert.ok(shapes.length >= 3, "several event types observed");
    for (const keys of shapes) {
        assert.equal(keys.length, 10, "exactly 10 own keys");
        assert.deepEqual(keys, FEED_KEYS, "keys present in the frozen order");
    }
    stop();
});

test("shape: ts is a number, resolved once at module load, monotonic non-decreasing", () => {
    const qc = queryClient();
    const stamps = [];
    const stop = qc.inspect((e) => stamps.push(e.ts));
    for (let i = 0; i < 20; i++) qc.setQueryData(["t", i], i);
    assert.ok(stamps.length >= 20);
    for (const ts of stamps) assert.equal(typeof ts, "number");
    for (let i = 1; i < stamps.length; i++) {
        assert.ok(stamps[i] >= stamps[i - 1], "ts never goes backwards");
    }
    stop();
});

test("identity: two events of the SAME type share one pooled object (OR-6)", () => {
    const qc = queryClient();
    const raw = [];
    const stop = qc.inspect((e) => { if (e.type === "entry:status") raw.push(e); });
    qc.setQueryData(["id"], 1);              // status idle->success
    // Copy the first status event's fields BEFORE the second overwrites the pool.
    const snapshot = { ...raw[0] };
    qc.setQueryData(["id"], 2);              // status success->success (same pool slot)
    assert.equal(raw.length, 2);
    assert.ok(Object.is(raw[0], raw[1]), "same type -> same object identity");
    // The manual copy taken after event 1 is unchanged after event 2.
    assert.equal(snapshot.to, "success");
    assert.equal(snapshot.from, "idle");
    stop();
});

test("identity: events of DIFFERENT types are never the same object", () => {
    const qc = queryClient();
    const byType = new Map();
    const stop = qc.inspect((e) => { if (!byType.has(e.type)) byType.set(e.type, e); });
    qc.setQueryData(["d"], 1);               // entry:create + entry:status
    const create = byType.get("entry:create");
    const status = byType.get("entry:status");
    assert.ok(create && status);
    assert.ok(!Object.is(create, status), "distinct types hold distinct pooled objects");
    stop();
});

test("key: the event's key is the entry's array BY REFERENCE, never copied", () => {
    const qc = queryClient();
    const KEY = ["ref", 1];
    let seen = null;
    const stop = qc.inspect((e) => { if (e.type === "entry:create") seen = e.key; });
    qc.setQueryData(KEY, 1);
    assert.ok(Object.is(seen, KEY), "feed key === the caller's key array");
    stop();
});

test("A9: an uninstalled feed emits nothing", () => {
    const qc = queryClient();
    let n = 0;
    const stop = qc.inspect(() => { n++; });
    qc.setQueryData(["u"], 1);
    const n1 = n;
    assert.ok(n1 > 0, "installed hook sees events");
    stop();
    for (let i = 0; i < 50; i++) {
        qc.setQueryData(["u", i], i);
        qc.invalidate(["u", i]);
        qc.removeQueries(["u", i]);
    }
    assert.equal(n, n1, "no events after uninstall");
});
