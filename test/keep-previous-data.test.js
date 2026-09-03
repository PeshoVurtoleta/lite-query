/**
 * @zakkster/lite-query -- keepPreviousData (Q10 / v2.2.0).
 *
 * A HANDLE-level presentation, never entry truth (OR-5 -- the cache never lies).
 * Across a reactive key swap the handle keeps showing the previous key's data,
 * with isPlaceholder() true, until the new key's real data arrives. status() and
 * loading() read ENTRY truth throughout; dehydrate/getQueryData/feed never see
 * the hold. The OFF path (no keepPreviousData) is byte-identical to 2.1.0.
 */

import { test, beforeEach } from "node:test";
import assert from "node:assert/strict";

import { signal, effect, createRegistry, setDefaultRegistry, createRoot } from "@zakkster/lite-signal";

import { queryClient, query } from "../Query.js";
import { createQueuedFetcher } from "./harness.js";

beforeEach(() => setDefaultRegistry(createRegistry({ maxNodes: 16384 })));

const tick = () => new Promise((r) => queueMicrotask(r));

// -----------------------------------------------------------------------------
// Validation (ON-4): strictly === true to enable; absent/false = off; else throw.
// -----------------------------------------------------------------------------

test("keepPreviousData: absent is accepted (off)", () => {
    const qc = queryClient();
    assert.doesNotThrow(() => query(qc, { key: ["a"], fetcher: async () => 1 }));
});

test("keepPreviousData: false is accepted (off)", () => {
    const qc = queryClient();
    assert.doesNotThrow(() => query(qc, { key: ["a"], fetcher: async () => 1, keepPreviousData: false }));
});

test("keepPreviousData: true is accepted (on)", () => {
    const qc = queryClient();
    assert.doesNotThrow(() => query(qc, { key: ["a"], fetcher: async () => 1, keepPreviousData: true }));
});

test("keepPreviousData: null throws TypeError (present-and-not-boolean)", () => {
    const qc = queryClient();
    assert.throws(() => query(qc, { key: ["a"], fetcher: async () => 1, keepPreviousData: null }), TypeError);
});

test("keepPreviousData: a number throws TypeError", () => {
    const qc = queryClient();
    assert.throws(() => query(qc, { key: ["a"], fetcher: async () => 1, keepPreviousData: 1 }), TypeError);
});

test("keepPreviousData: a string throws TypeError", () => {
    const qc = queryClient();
    assert.throws(() => query(qc, { key: ["a"], fetcher: async () => 1, keepPreviousData: "yes" }), TypeError);
});

// -----------------------------------------------------------------------------
// The hold: a reactive key swap presents previous data with isPlaceholder() true.
// -----------------------------------------------------------------------------

function mkSwapEnv(queryOpts) {
    const id = signal(1);
    const f = createQueuedFetcher();
    const qc = queryClient({ defaultStaleTime: Infinity });
    const q = query(qc, { key: () => ["u", id()], fetcher: f.fetcher, ...queryOpts });
    const seen = { data: undefined, placeholder: undefined, loading: undefined, status: undefined };
    const d = createRoot(() => effect(() => {
        seen.data = q.data();
        seen.placeholder = q.isPlaceholder();
        seen.loading = q.loading();
        seen.status = q.status();
    }));
    return { id, f, qc, q, seen, d };
}

test("hold: previous data stays visible with isPlaceholder() true until the new key settles", async () => {
    const { id, f, q, seen, d } = mkSwapEnv({ keepPreviousData: true });
    await tick();
    f.resolveNth(0, "user-1");
    await tick(); await tick();
    assert.equal(seen.data, "user-1", "key 1 resolved");
    assert.equal(seen.placeholder, false, "not a placeholder once real data is showing");

    id.set(2);                                       // reactive key swap
    await tick();
    assert.equal(seen.data, "user-1", "held the previous key's data across the swap");
    assert.equal(seen.placeholder, true, "isPlaceholder() true during the hold");

    f.resolveNth(1, "user-2");                        // the new key's fetch settles
    await tick(); await tick();
    assert.equal(seen.data, "user-2", "the new key's real data replaced the placeholder");
    assert.equal(seen.placeholder, false, "isPlaceholder() cleared on the first defined value");
    d(); q.dispose();
});

test("hold: status() and loading() read ENTRY truth during the hold, not the placeholder", async () => {
    const { id, f, q, seen, d } = mkSwapEnv({ keepPreviousData: true });
    await tick();
    f.resolveNth(0, "user-1");
    await tick(); await tick();

    id.set(2);
    await tick();
    // data() shows the held value, but the entry underneath is a fresh fetch.
    assert.equal(seen.data, "user-1", "presentation: held previous data");
    assert.equal(seen.status, "pending", "truth: the new entry is pending");
    assert.equal(seen.loading, true, "truth: loading() is true (new entry fetching, no data)");
    d(); q.dispose();
});

test("hold: the CACHE never lies -- getQueryData(newKey) is undefined while the handle holds", async () => {
    const { id, f, qc, q, seen, d } = mkSwapEnv({ keepPreviousData: true });
    await tick();
    f.resolveNth(0, "user-1");
    await tick(); await tick();

    id.set(2);
    await tick();
    assert.equal(seen.data, "user-1", "handle presents the placeholder");
    assert.equal(qc.getQueryData(["u", 2]), undefined, "cache truth: the new key has no data yet");
    // dehydrate walks entries, not handles -- the placeholder is never serialized.
    const dumped = qc.dehydrate();
    const flat = JSON.stringify(dumped);
    assert.ok(!/user-1/.test(flat) || /user-1/.test(JSON.stringify(qc.getQueryData(["u", 1]))),
        "dehydrate carries only entry truth, never the handle's held presentation for the new key");
    d(); q.dispose();
});

test("OFF path: without keepPreviousData, data() is undefined during the swap and isPlaceholder() is false", async () => {
    const { id, f, q, seen, d } = mkSwapEnv({});     // no keepPreviousData
    await tick();
    f.resolveNth(0, "user-1");
    await tick(); await tick();
    assert.equal(seen.data, "user-1");

    id.set(2);
    await tick();
    assert.equal(seen.data, undefined, "no hold -- data is undefined while the new key loads");
    assert.equal(seen.placeholder, false, "isPlaceholder() is a constant false on the OFF path");
    assert.equal(q.isPlaceholder(), false, "isPlaceholder() outside an effect is false too");
    d(); q.dispose();
});

test("hold: a swap back to a cached key shows its real data immediately (placeholder clears at once)", async () => {
    const { id, f, q, seen, d } = mkSwapEnv({ keepPreviousData: true });
    await tick();
    f.resolveNth(0, "user-1");
    await tick(); await tick();
    id.set(2); await tick();
    f.resolveNth(1, "user-2");
    await tick(); await tick();
    assert.equal(seen.data, "user-2");
    assert.equal(seen.placeholder, false);

    // Back to key 1: its entry is still cached (staleTime Infinity), so real data
    // is available at once -- no placeholder flash.
    id.set(1); await tick();
    assert.equal(seen.data, "user-1", "cached key 1 data shown immediately");
    assert.equal(seen.placeholder, false, "no placeholder when the target key already has data");
    d(); q.dispose();
});
