// @zakkster/lite-query -- devtools feed: two-seam independence, throwing-hook
// containment, and the persist:* events (Q7, C5).
//
// OR-4: qc.inspect and persistQueryClient are two INDEPENDENT single slots.
// Uninstalling one must never disturb the other -- a devtools panel cannot evict
// the persistence adapter, and vice versa. OR-7: a throwing hook is contained
// fail-closed toward the FEED (auto-uninstall + one console.error), never toward
// the cache. The public feed REPORTS persist/hydrate activity; the adapter never
// consumes it.

import { test } from "node:test";
import assert from "node:assert/strict";
import { queryClient, persistQueryClient } from "../Query.js";

const only = (events, type) => events.filter((e) => e.type === type);
const flush = async () => { for (let i = 0; i < 8; i++) await Promise.resolve(); };

test("two seams: uninstalling inspect leaves the persister saving (OR-4)", async () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    let saves = 0;
    const handle = persistQueryClient(qc, {
        save: () => { saves++; }, load: () => null, version: "v1", throttle: 0,
    });
    await handle.restored;                           // write hook armed
    let events = 0;
    const stopInspect = qc.inspect(() => { events++; });
    qc.setQueryData(["a"], 1);
    assert.ok(saves >= 1, "save fires with both seams live");
    assert.ok(events >= 1, "feed fires with both seams live");
    stopInspect();                                   // evict ONLY the feed
    const savesBefore = saves;
    qc.setQueryData(["a"], 2);
    assert.ok(saves > savesBefore, "the persister still saves after inspect uninstall");
    handle.stop();
});

test("two seams: stopping the persister leaves the feed emitting (OR-4)", async () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    const events = [];
    const stopInspect = qc.inspect((e) => events.push(e.type));
    const handle = persistQueryClient(qc, {
        save: () => {}, load: () => null, version: "v1", throttle: 0,
    });
    await handle.restored;
    handle.stop();                                   // evict ONLY the persister
    const before = events.length;
    qc.setQueryData(["b"], 1);
    assert.ok(events.length > before, "the feed still emits after the persister stops");
    stopInspect();
});

test("A8: a throwing hook is contained -- write lands, hook + console.error once, auto-uninstall", () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    const origError = console.error;
    let errs = 0;
    console.error = () => { errs++; };
    try {
        let calls = 0;
        qc.inspect(() => { calls++; throw new Error("panel bug"); });
        qc.setQueryData(["t"], 1);                   // hook throws on its first event
        assert.equal(qc.getQueryData(["t"]), 1, "the cache write completed");
        assert.equal(calls, 1, "the hook was called exactly once");
        assert.equal(errs, 1, "console.error fired exactly once");
        // 20 more ops -- no further hook or console.error calls (feed uninstalled).
        for (let i = 0; i < 20; i++) qc.setQueryData(["t", i], i);
        assert.equal(calls, 1, "no further hook calls after containment");
        assert.equal(errs, 1, "no further console.error calls");
        // The slot is free -- a fresh install returns a function.
        const stop = qc.inspect(() => {});
        assert.equal(typeof stop, "function");
        stop();
    } finally {
        console.error = origError;
    }
});

test("persist:hydrate (ok) reports the seeded record count", () => {
    const qc = queryClient();
    const events = [];
    const stop = qc.inspect((e) => events.push({ ...e }));
    const state = { entries: [
        { key: ["h1"], data: 1, dataUpdatedAt: 0, infinite: false },
        { key: ["h2"], data: 2, dataUpdatedAt: 0, infinite: false },
    ] };
    const res = qc.hydrate(state);
    assert.equal(res.ok, true);
    const hy = only(events, "persist:hydrate");
    assert.equal(hy.length, 1);
    assert.equal(hy[0].ok, true);
    assert.equal(hy[0].count, 2, "count is the number of records seeded");
    assert.equal(hy[0].reason, null);
    stop();
});

test("persist:hydrate (dropped) reports the validation reason with ok=false", () => {
    const qc = queryClient();
    const events = [];
    const stop = qc.inspect((e) => events.push({ ...e }));
    const res = qc.hydrate({ entries: "not-an-array" });
    assert.equal(res.ok, false);
    const hy = only(events, "persist:hydrate");
    assert.equal(hy.length, 1);
    assert.equal(hy[0].ok, false);
    assert.equal(hy[0].count, 0);
    assert.equal(hy[0].reason, "malformed-entries");
    stop();
});

const hasGc = typeof global !== "undefined" && typeof global.gc === "function";

test("QD-4: the status funnel does not pin the last status-written entry after uninstall + removeQueries",
    { skip: !hasGc && "run with --expose-gc to enable" }, async () => {
    // setStatus parks the entry in the module-global _se for the untracked prior-
    // status read. If it is not released, the LAST entry written while a hook was
    // installed -- and its data payload -- stays pinned for the process lifetime,
    // surviving uninstall AND removeQueries. Falsifying probe (reviewer's): WeakRef
    // the payload, write it, uninstall, removeQueries, force GC; a pinned entry
    // fails to collect.
    const qc = queryClient();
    let ref;
    (() => {
        const payload = { big: new Array(64).fill(7), tag: "qd4" };
        ref = new WeakRef(payload);
        const stop = qc.inspect(() => {});
        qc.setQueryData(["big"], payload);   // setStatus success -> parks the entry in _se
        stop();                              // uninstall the feed
        qc.removeQueries(["big"]);           // drop the entry from the cache map
    })();
    for (let i = 0; i < 20; i++) { global.gc(); await new Promise((r) => setTimeout(r, 0)); }
    assert.equal(ref.deref(), undefined, "the payload was collected -- _se released its pin");
    // Sole-retainer control: a LATER install+write on a different key also leaves
    // it collected (the pin is released per-write, not merely overwritten later).
    const stop2 = qc.inspect(() => {});
    qc.setQueryData(["other"], { n: 1 });
    stop2();
    for (let i = 0; i < 5; i++) { global.gc(); await new Promise((r) => setTimeout(r, 0)); }
    assert.equal(ref.deref(), undefined, "still collected after an unrelated later write");
    qc.dispose();
});

test("persist:save is emitted BY the adapter, reporting the envelope entry count", async () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    qc.setQueryData(["s1"], 1);
    qc.setQueryData(["s2"], 2);                       // two success entries to dehydrate
    const events = [];
    const stopInspect = qc.inspect((e) => events.push({ ...e }));
    const handle = persistQueryClient(qc, {
        save: () => {}, load: () => null, version: "v1", throttle: 0,
    });
    await handle.restored;
    qc.setQueryData(["s1"], 3);                       // triggers a throttle-0 save
    await flush();
    const saved = only(events, "persist:save");
    assert.ok(saved.length >= 1, "the adapter emits persist:save");
    assert.ok(saved.every((e) => e.ok === true));
    assert.ok(saved[saved.length - 1].count >= 2, "count is the entries in the envelope");
    assert.ok(saved.every((e) => e.key === null), "persist events are client-scope");
    stopInspect(); handle.stop();
});
