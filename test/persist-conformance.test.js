/**
 * @zakkster/lite-query -- dehydrated-cache conformance / corruption matrix (T4).
 *
 * A dependency-free mirror of the SHAPE of bake-stream's
 * DehydratedCache.test.js. Imports nothing from bake, ever.
 *
 *   Part A -- a mixed corpus (plain + infinite + zero-entry + a ~1 MB payload)
 *             round-trips byte-exact through JSON, both directions.
 *   Part B -- the corruption matrix: every malformed class DROPS THE WHOLE
 *             payload with its exact reason code; a subsequent hydrate(good) on
 *             the same client proves the cache was left empty (all-or-nothing).
 *             Two legal, non-refusing rows are pinned separately.
 *   Part C -- a fail-open mergeHydrate witness that the SAME refusal driver must
 *             REJECT on 100% of refusal rows, proving the matrix is not vacuous.
 *             MATRIX.length is pinned to a literal.
 *
 * ASCII-only source.
 */

import { test } from "node:test";
import assert from "node:assert/strict";

import { effect } from "@zakkster/lite-signal";

import { queryClient, infiniteQuery } from "../Query.js";
import { createMockClock } from "./harness.js";

const tick = () => new Promise((r) => queueMicrotask(r));

const GOOD = () => ({ key: ["ok"], data: 1, dataUpdatedAt: 0, infinite: false });
const wrap = (entry) => ({ entries: [entry] });

// ============================================================================
// PART A -- round-trip byte-exactness
// ============================================================================

async function buildCorpus() {
    const clock = createMockClock(1000);
    const src = queryClient({ now: clock.now, setTimeout: clock.setTimeout, clearTimeout: clock.clearTimeout, defaultStaleTime: 0 });
    src.setQueryData(["plain", 1], { a: 1, b: [2, 3, "x"] });
    src.setQueryData(["scalar"], "hello");
    const q = infiniteQuery(src, {
        key: ["feed"],
        fetcher: ({ cursor }) => Promise.resolve([cursor == null ? 0 : cursor]),
        getNextCursor: (lastPage, allPages) => (allPages.length < 3 ? allPages.length : null),
    });
    const stop = effect(() => q.data());
    await tick();
    await q.fetchNextPage(); await tick();
    const state = src.dehydrate();
    stop(); q.dispose(); src.dispose();
    return state;
}

test("A: a mixed corpus (plain + infinite) round-trips byte-exact through JSON both directions", async () => {
    const state = await buildCorpus();
    const wire1 = JSON.stringify(state);

    const clock = createMockClock(9000);   // strictly later than the corpus timestamps
    const qc = queryClient({ now: clock.now, setTimeout: clock.setTimeout, clearTimeout: clock.clearTimeout, defaultStaleTime: 0 });
    const res = qc.hydrate(JSON.parse(wire1));
    assert.equal(res.ok, true);
    assert.equal(res.count, 3);

    const wire2 = JSON.stringify(qc.dehydrate());
    assert.equal(wire2, wire1, "hydrate -> dehydrate is byte-exact");
    assert.deepEqual(qc.getQueryData(["plain", 1]), { a: 1, b: [2, 3, "x"] });
    assert.deepEqual(qc.getQueryData(["feed"]), [[0], [1]]);
    qc.dispose();
});

test("A: a zero-entry cache dehydrates to { entries: [] } and hydrates as ok/count 0", () => {
    const src = queryClient({ defaultStaleTime: 0 });
    const state = src.dehydrate();
    assert.deepEqual(state, { entries: [] });
    src.dispose();

    const qc = queryClient({ defaultStaleTime: 0 });
    assert.deepEqual(qc.hydrate(state), { ok: true, count: 0, reason: null });
    qc.dispose();
});

test("A: a ~1 MB single payload round-trips byte-exact", () => {
    const big = { blob: "x".repeat(1_000_000), n: 7 };
    const src = queryClient({ defaultStaleTime: 0 });
    src.setQueryData(["big"], big);
    const wire = JSON.stringify(src.dehydrate());
    assert.ok(wire.length > 1_000_000, "payload is ~1 MB (got " + wire.length + " bytes)");
    src.dispose();

    const qc = queryClient({ defaultStaleTime: 0 });
    const res = qc.hydrate(JSON.parse(wire));
    assert.equal(res.ok, true);
    assert.deepEqual(qc.getQueryData(["big"]), big);
    qc.dispose();
});

// ============================================================================
// PART B -- the corruption matrix
// ============================================================================
//
// MATRIX is the flat list of REFUSAL cases across the conceptual rows (the two
// legal rows -- empty entries, a null DATA value -- do not refuse and are pinned
// separately). Each case maps one malformed payload to the EXACT reason code
// hydrate must produce, with the whole payload dropped.

const MATRIX = [
    { row: 1, name: "state null", reason: "malformed-state", make: () => null },
    { row: 2, name: "state an array", reason: "malformed-state", make: () => [] },
    { row: 3, name: "state a string", reason: "malformed-state", make: () => "nope" },
    { row: 4, name: "entries missing", reason: "malformed-entries", make: () => ({}) },
    { row: 5, name: "entries not an array", reason: "malformed-entries", make: () => ({ entries: {} }) },
    { row: 6, name: "extra top-level key", reason: "malformed-state", make: () => ({ entries: [], extra: 1 }) },
    { row: 7, name: "entry not an object", reason: "malformed-entry", make: () => ({ entries: [42] }) },
    { row: 8, name: "key missing", reason: "malformed-key", make: () => wrap({ data: 1, dataUpdatedAt: 0, infinite: false, extra: 9 }) },
    { row: 8, name: "key not an array", reason: "malformed-key", make: () => wrap({ key: "k", data: 1, dataUpdatedAt: 0, infinite: false }) },
    { row: 9, name: "entry with a 5th key", reason: "malformed-entry", make: () => wrap({ key: ["k"], data: 1, dataUpdatedAt: 0, infinite: false, extra: 5 }) },
    { row: 10, name: "dataUpdatedAt NaN", reason: "malformed-timestamp", make: () => wrap({ key: ["k"], data: 1, dataUpdatedAt: NaN, infinite: false }) },
    { row: 10, name: "dataUpdatedAt Infinity", reason: "malformed-timestamp", make: () => wrap({ key: ["k"], data: 1, dataUpdatedAt: Infinity, infinite: false }) },
    { row: 10, name: "dataUpdatedAt a string", reason: "malformed-timestamp", make: () => wrap({ key: ["k"], data: 1, dataUpdatedAt: "0", infinite: false }) },
    { row: 10, name: "dataUpdatedAt missing", reason: "malformed-timestamp", make: () => wrap({ key: ["k"], data: 1, infinite: false, extra: 7 }) },
    { row: 11, name: "infinite missing", reason: "malformed-entry", make: () => wrap({ key: ["k"], data: 1, dataUpdatedAt: 0, extra: 3 }) },
    { row: 12, name: "infinite non-boolean", reason: "malformed-entry", make: () => wrap({ key: ["k"], data: 1, dataUpdatedAt: 0, infinite: 1 }) },
    { row: 13, name: "infinite true with non-array data", reason: "malformed-pages", make: () => wrap({ key: ["k"], data: 5, dataUpdatedAt: 0, infinite: true }) },
    { row: 14, name: "infinite true with data null", reason: "malformed-pages", make: () => wrap({ key: ["k"], data: null, dataUpdatedAt: 0, infinite: true }) },
    { row: 15, name: "data undefined on a plain entry", reason: "malformed-data", make: () => wrap({ key: ["k"], data: undefined, dataUpdatedAt: 0, infinite: false }) },
    { row: 16, name: "duplicate key hashes", reason: "duplicate-key", make: () => ({ entries: [
        { key: ["d"], data: 1, dataUpdatedAt: 0, infinite: false },
        { key: ["d"], data: 2, dataUpdatedAt: 0, infinite: false },
    ] }) },
    { row: 17, name: "one bad entry among three good ones", reason: "malformed-timestamp", make: () => ({ entries: [
        { key: ["a"], data: 1, dataUpdatedAt: 0, infinite: false },
        { key: ["b"], data: 2, dataUpdatedAt: NaN, infinite: false },   // the one bad entry
        { key: ["c"], data: 3, dataUpdatedAt: 0, infinite: false },
    ] }) },
];

// The refusal driver, reused across the real reader (Part B) and the witness
// (Part C). Returns true iff `fn` DROPPED THE WHOLE payload with the mapped
// reason (ok false, count 0, reason ===).
function refusesWith(fn, c) {
    const r = fn(c.make());
    return r.ok === false && r.count === 0 && r.reason === c.reason;
}

// Real reader: a fresh (empty) client per call, so the OR-2 precondition never
// fires -- we are testing malformed-payload drops, not the non-empty throw.
function realHydrate(state) {
    const qc = queryClient({ defaultStaleTime: 0 });
    const r = qc.hydrate(state);
    qc.dispose();
    return r;
}

for (const c of MATRIX) {
    test("B row " + c.row + ": " + c.name + " -> " + c.reason, () => {
        const qc = queryClient({ defaultStaleTime: 0 });
        const r = qc.hydrate(c.make());
        assert.equal(r.ok, false, c.name + " must drop the whole payload");
        assert.equal(r.count, 0, c.name + " seeds nothing");
        assert.equal(typeof r.reason, "string", c.name + " has a string reason");
        assert.equal(r.reason, c.reason, c.name + " reason code");
        // The whole payload dropped -> the cache is still empty -> a good hydrate
        // on the SAME client now succeeds.
        const good = qc.hydrate({ entries: [GOOD()] });
        assert.equal(good.ok, true, "cache was left empty");
        assert.equal(good.count, 1);
        qc.dispose();
    });
}

test("B legal: an empty entries array hydrates as ok, count 0 (bake row-8 analogue)", () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    assert.deepEqual(qc.hydrate({ entries: [] }), { ok: true, count: 0, reason: null });
    qc.dispose();
});

test("B legal: a null DATA value hydrates fine -- null is a value, not a missing field (bake row-17 analogue)", () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    const r = qc.hydrate({ entries: [{ key: ["n"], data: null, dataUpdatedAt: 0, infinite: false }] });
    assert.equal(r.ok, true);
    assert.equal(r.count, 1);
    assert.equal(qc.getQueryData(["n"]), null, "null seeded as a real value");
    qc.dispose();
});

// ============================================================================
// PART C -- the fail-open witness (matrix-is-not-vacuous proof)
// ============================================================================
//
// mergeHydrate reproduces the fail-open behavior OR-3 rejects: it salvages
// whatever entries look seedable and NEVER drops the whole payload. The SAME
// refusesWith driver must REJECT it on every refusal case -- if the witness
// dropped a case the way the real reader does, that row would prove nothing.
function mergeHydrate(state) {
    const list = state && typeof state === "object" && !Array.isArray(state) && Array.isArray(state.entries)
        ? state.entries : [];
    let count = 0;
    for (const rec of list) {
        if (rec && typeof rec === "object" && !Array.isArray(rec) && Array.isArray(rec.key)) count++;
    }
    return { ok: true, count, reason: null };   // fail-open: salvage, never drop
}

test("C: the matrix is not vacuous -- a fail-open witness is rejected on every refusal case", () => {
    assert.equal(MATRIX.length, 21, "the corruption-matrix row table length changed");
    let realRefused = 0;
    let witnessRejected = 0;
    for (const c of MATRIX) {
        if (refusesWith(realHydrate, c)) realRefused++;
        if (!refusesWith(mergeHydrate, c)) witnessRejected++;
    }
    assert.equal(realRefused, MATRIX.length, "the real reader failed to refuse a mapped case");
    assert.equal(witnessRejected, MATRIX.length, "the witness refused a case it should have salvaged");
});
