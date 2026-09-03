/**
 * @zakkster/lite-query -- refetchInterval (Q10 / v2.2.0).
 *
 * Opt-in polling driven by ONE client-wide scanner (the watchdog precedent):
 * never per-entry timer churn. The interval fires through the NORMAL fetch path
 * (maybeFetch's tail) so dedup / retry / abort / sharedFetch all apply unchanged;
 * the interval-triggered fetch reuses fetch:dispatch with reason "interval" (no
 * new event type). Every timer runs through opts.setTimeout so the mock clock
 * drives them deterministically -- no real sleeps.
 *
 * This file grows across the C1..C5 ladder rungs:
 *   C1 -- option validation (fail closed at the door, ON-4).
 *   C4 -- dispatch through maybeFetch + feed reason "interval".
 *   C5 -- lifecycle (register on attach, unregister on detach/disable/dispose)
 *         and the shared-polling truthfulness contract (G4).
 */

import { test, beforeEach } from "node:test";
import assert from "node:assert/strict";

import { effect, createRegistry, setDefaultRegistry, createRoot } from "@zakkster/lite-signal";

import { queryClient, query } from "../Query.js";
import { createMockClock } from "./harness.js";

beforeEach(() => setDefaultRegistry(createRegistry({ maxNodes: 16384 })));

const tick = () => new Promise((r) => queueMicrotask(r));

// -----------------------------------------------------------------------------
// C1 -- option validation (ON-4): fail closed at query construction.
// -----------------------------------------------------------------------------

test("refetchInterval: absent is accepted (off)", () => {
    const qc = queryClient();
    assert.doesNotThrow(() => query(qc, { key: ["a"], fetcher: async () => 1 }));
});

test("refetchInterval: null is accepted (off)", () => {
    const qc = queryClient();
    assert.doesNotThrow(() => query(qc, { key: ["a"], fetcher: async () => 1, refetchInterval: null }));
});

test("refetchInterval: a finite number > 0 is accepted", () => {
    const qc = queryClient();
    assert.doesNotThrow(() => query(qc, { key: ["a"], fetcher: async () => 1, refetchInterval: 1000 }));
});

test("refetchInterval: 0 throws TypeError (fail closed -- null is not zero)", () => {
    const qc = queryClient();
    assert.throws(() => query(qc, { key: ["a"], fetcher: async () => 1, refetchInterval: 0 }), TypeError);
});

test("refetchInterval: a negative number throws TypeError", () => {
    const qc = queryClient();
    assert.throws(() => query(qc, { key: ["a"], fetcher: async () => 1, refetchInterval: -100 }), TypeError);
});

test("refetchInterval: NaN throws TypeError", () => {
    const qc = queryClient();
    assert.throws(() => query(qc, { key: ["a"], fetcher: async () => 1, refetchInterval: NaN }), TypeError);
});

test("refetchInterval: Infinity throws TypeError", () => {
    const qc = queryClient();
    assert.throws(() => query(qc, { key: ["a"], fetcher: async () => 1, refetchInterval: Infinity }), TypeError);
});

test("refetchInterval: a non-number (string) throws TypeError", () => {
    const qc = queryClient();
    assert.throws(() => query(qc, { key: ["a"], fetcher: async () => 1, refetchInterval: "1000" }), TypeError);
});

test("refetchInterval: a non-number (object) throws TypeError", () => {
    const qc = queryClient();
    assert.throws(() => query(qc, { key: ["a"], fetcher: async () => 1, refetchInterval: {} }), TypeError);
});

// -----------------------------------------------------------------------------
// C3 -- the scanner mechanics: ONE timer, period = min interval, on-or-after due.
// -----------------------------------------------------------------------------

function mkPollEnv(staleTime = 0) {
    const clock = createMockClock();
    const calls = [];
    const qc = queryClient({
        now: clock.now, setTimeout: clock.setTimeout, clearTimeout: clock.clearTimeout,
        defaultStaleTime: staleTime,
    });
    const fetcher = async () => { calls.push(1); return calls.length; };
    return { clock, calls, qc, fetcher };
}

test("scanner: a registered poll fetches on-or-after its due time, never early", async () => {
    const { clock, calls, qc, fetcher } = mkPollEnv();
    const q = query(qc, { key: ["a"], fetcher });
    const stop = createRoot(() => effect(() => q.status()));
    await tick();
    assert.equal(calls.length, 1, "attach fetched once");
    const entry = qc._internal.entries.get(JSON.stringify(["a"]));
    qc._internal.registerPoll(entry, 1000);
    clock.advance(999);
    await tick();
    assert.equal(calls.length, 1, "not fetched before the interval elapses (never early)");
    clock.advance(1);
    await tick();
    assert.equal(calls.length, 2, "fetched once the interval came due");
    stop();
    q.dispose();
});

test("scanner: re-arms across multiple ticks", async () => {
    const { clock, calls, qc, fetcher } = mkPollEnv();
    const q = query(qc, { key: ["a"], fetcher });
    const stop = createRoot(() => effect(() => q.status()));
    await tick();
    const entry = qc._internal.entries.get(JSON.stringify(["a"]));
    qc._internal.registerPoll(entry, 1000);
    for (let i = 0; i < 3; i++) { clock.advance(1000); await tick(); }
    assert.equal(calls.length, 4, "one attach fetch + three interval fetches");
    stop();
    q.dispose();
});

test("scanner: unregister disarms -- no further fetches after the last poll leaves", async () => {
    const { clock, calls, qc, fetcher } = mkPollEnv();
    const q = query(qc, { key: ["a"], fetcher });
    const stop = createRoot(() => effect(() => q.status()));
    await tick();
    const entry = qc._internal.entries.get(JSON.stringify(["a"]));
    qc._internal.registerPoll(entry, 1000);
    clock.advance(1000); await tick();
    assert.equal(calls.length, 2);
    qc._internal.unregisterPoll(entry);
    clock.advance(5000); await tick();
    assert.equal(calls.length, 2, "no ticks fire after the scanner disarms");
    stop();
    q.dispose();
});

test("scanner: refcount -- two registrations, one record, unregister once still polls", async () => {
    const { clock, calls, qc, fetcher } = mkPollEnv();
    const q = query(qc, { key: ["a"], fetcher });
    const stop = createRoot(() => effect(() => q.status()));
    await tick();
    const entry = qc._internal.entries.get(JSON.stringify(["a"]));
    qc._internal.registerPoll(entry, 1000);
    qc._internal.registerPoll(entry, 1000);   // second observer, same key -> refcount 2
    qc._internal.unregisterPoll(entry);        // one leaves -> refcount 1, still armed
    clock.advance(1000); await tick();
    assert.equal(calls.length, 2, "still polling while one registration remains");
    qc._internal.unregisterPoll(entry);        // last leaves -> disarm
    clock.advance(5000); await tick();
    assert.equal(calls.length, 2, "disarmed after the last registration leaves");
    stop();
    q.dispose();
});

test("scanner: period = min registered interval (coarser poll served on-or-after)", async () => {
    const { clock, calls, qc, fetcher } = mkPollEnv();
    const qa = query(qc, { key: ["a"], fetcher });
    const qb = query(qc, { key: ["b"], fetcher });
    const stop = createRoot(() => effect(() => { qa.status(); qb.status(); }));
    await tick();
    assert.equal(calls.length, 2, "two attach fetches");
    const ea = qc._internal.entries.get(JSON.stringify(["a"]));
    const eb = qc._internal.entries.get(JSON.stringify(["b"]));
    qc._internal.registerPoll(ea, 1000);      // fast
    qc._internal.registerPoll(eb, 3000);      // slow
    clock.advance(1000); await tick();
    assert.equal(calls.length, 3, "fast poll fired at 1000, slow not yet due");
    clock.advance(2000); await tick();          // t=3000: both due (fast twice more, slow once)
    assert.ok(calls.length >= 5, "slow poll served on-or-after 3000, fast kept its cadence");
    stop();
    qa.dispose(); qb.dispose();
});
