// @zakkster/lite-query -- devtools feed (qc.inspect) behavioural suite (Q7).
//
// The feed is a push-mode stream of cache truth a panel renders. This suite
// pins the seam contract (single slot, two throw shapes, idempotent uninstall)
// and, as the emit sites land rung by rung, the event vocabulary itself: entry
// lifecycle, status/staleness, fetch dispatch/settle/abort, cross-tab and
// shared-fetch roles, and mutation lifecycle. Shape/identity live in
// inspect-shape.test.js; stream events in inspect-stream.test.js; the two-seam
// independence + throwing-hook containment in inspect-seams.test.js.

import { test } from "node:test";
import assert from "node:assert/strict";
import { queryClient, query } from "../Query.js";
import { effect, createRoot } from "@zakkster/lite-signal";
import { setupMockEnv, createControlledFetcher } from "./harness.js";

const noop = () => {};

// Collect every event a hook sees into a plain array; return { qc, events, stop }.
function withFeed(opts = {}) {
    const qc = queryClient(opts);
    const events = [];
    const stop = qc.inspect((e) => {
        // The hook MUST copy what it keeps -- events are pooled + overwritten.
        events.push({ ...e });
    });
    return { qc, events, stop };
}

// -- seam contract (C1) ------------------------------------------------------

test("inspect: returns an idempotent uninstall function", () => {
    const qc = queryClient();
    const stop = qc.inspect(noop);
    assert.equal(typeof stop, "function", "inspect returns an uninstall thunk");
    stop();
    stop();                                 // idempotent: a second call is a no-op
    // The slot is free again -- a fresh install must succeed.
    const stop2 = qc.inspect(noop);
    assert.equal(typeof stop2, "function");
    stop2();
});

test("inspect: a second install while one is live throws Error (single slot)", () => {
    const qc = queryClient();
    const stop = qc.inspect(noop);
    assert.throws(() => qc.inspect(noop), (err) =>
        err instanceof Error && !(err instanceof TypeError) &&
        /already installed/.test(err.message));
    stop();
});

test("inspect: uninstall frees the slot for a later install", () => {
    const qc = queryClient();
    const stopA = qc.inspect(noop);
    stopA();
    // A different hook installs cleanly after uninstall (no throw = slot free);
    // and while it holds the slot, a third install throws (slot taken again).
    const stopB = qc.inspect(noop);
    assert.throws(() => qc.inspect(noop), (err) =>
        err instanceof Error && /already installed/.test(err.message));
    stopB();
});

test("inspect: a stale uninstall thunk does not evict a newer hook", () => {
    const qc = queryClient();
    const hookA = () => {};
    const hookB = () => {};
    const stopA = qc.inspect(hookA);
    stopA();
    const stopB = qc.inspect(hookB);
    stopA();                                 // the OLD thunk must be a no-op now
    // If stopA had evicted B, this install would succeed; it must throw because
    // B still holds the single slot.
    assert.throws(() => qc.inspect(noop), (err) =>
        err instanceof Error && /already installed/.test(err.message));
    stopB();
});

for (const bad of [null, undefined, [], {}]) {
    test("inspect: rejects a non-function argument (" +
        (Array.isArray(bad) ? "array" : bad === null ? "null" :
            bad === undefined ? "undefined" : "object") + ") with TypeError", () => {
        const qc = queryClient();
        assert.throws(() => qc.inspect(bad), (err) =>
            err instanceof TypeError && /requires a function/.test(err.message));
    });
}
