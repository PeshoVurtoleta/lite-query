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
