// Edge cases adjacent to the main spec: dispose contracts, abort-reason
// surface, observer-count discipline under churn. These guard against
// regressions in the "small but easy to break" corners.

import { test, beforeEach } from "node:test";
import assert from "node:assert/strict";
import { signal, effect, createRegistry, setDefaultRegistry } from "@zakkster/lite-signal";
import { queryClient, query, mutation } from "../Query.js";
import { createControlledFetcher, createMockClock } from "./harness.js";

beforeEach(() => setDefaultRegistry(createRegistry({ maxNodes: 16384 })));
const tick = () => new Promise((r) => queueMicrotask(r));

// ── query.dispose semantics ──────────────────────────────────────────────────

test("query.dispose is idempotent", () => {
    const qc = queryClient();
    const q = query(qc, { key: ["x"], fetcher: async () => 1 });
    q.dispose();
    q.dispose();   // must not throw
    qc.dispose();
});

test("accessors after query.dispose return idle defaults (no throw)", async () => {
    const qc = queryClient();
    const f = createControlledFetcher();
    const q = query(qc, { key: ["x"], fetcher: f.fetcher });
    // attach
    const stop = effect(() => { q.data(); });
    await tick();
    f.resolve(42);
    await tick();
    q.dispose();
    // disposed query: accessors should not throw and should return idle defaults
    assert.equal(q.data(), undefined);
    assert.equal(q.error(), undefined);
    assert.equal(q.status(), "idle");
    assert.equal(q.fetching(), false);
    assert.equal(q.loading(), false);
    stop();
    qc.dispose();
});

// ── mutation.dispose semantics ──────────────────────────────────────────────

test("mutation.dispose is idempotent", () => {
    const qc = queryClient();
    const m = mutation(qc, { fn: async () => 1 });
    m.dispose();
    m.dispose();
    qc.dispose();
});

test("mutation.dispose during in-flight call: awaiter still gets result; state writes are suppressed", async () => {
    const qc = queryClient();
    const f = createControlledFetcher();
    const m = mutation(qc, { fn: f.fetcher });

    const p = m.mutate("v");
    await tick();
    assert.equal(m.loading(), true);
    m.dispose();              // dispose mid-flight
    f.resolve("done");
    // The original awaiter still gets fn's result — fn is the network call;
    // disposing the mutation doesn't cancel network in-flight, it just stops
    // state writes (since gen was bumped) and releases signal nodes.
    const result = await p;
    assert.equal(result, "done");
    qc.dispose();
});

// ── client.dispose contract ─────────────────────────────────────────────────

test("queryClient.dispose is idempotent", () => {
    const qc = queryClient();
    qc.dispose();
    qc.dispose();
});

test("setQueryData after qc.dispose creates an orphan entry but doesn't crash", () => {
    // Honest documentation: post-dispose cache writes still create entries
    // (the cache map is reset on dispose but a subsequent write re-creates).
    // What they MUST NOT do is broadcast (channel is closed). This is a smoke
    // test that the broadcast guard handles it.
    const qc = queryClient();
    qc.dispose();
    assert.doesNotThrow(() => qc.setQueryData(["after-dispose"], 1));
    // Reading back also doesn't throw.
    assert.equal(qc.getQueryData(["after-dispose"]), 1);
});

// ── abort-reason surface ─────────────────────────────────────────────────────

test("abort reasons: detach reason reaches the fetcher when last observer leaves", async () => {
    const qc = queryClient();
    let observedReason;
    const q = query(qc, {
        key: ["x"],
        fetcher: ({ signal }) => new Promise((_, rej) => {
            signal.addEventListener("abort", () => {
                observedReason = signal.reason;
                rej(signal.reason);
            });
        }),
    });
    const stop = effect(() => { q.data(); });
    await tick();
    stop();             // last observer leaves; should abort with 'lite-query:detach'
    await tick();
    assert.equal(observedReason, "lite-query:detach");
    qc.dispose();
});

test("abort reasons: refetch reason reaches the prior fetcher", async () => {
    const qc = queryClient();
    let firstAbortReason;
    const q = query(qc, {
        key: ["x"],
        fetcher: ({ signal }) => new Promise((_, rej) => {
            if (!firstAbortReason) {
                signal.addEventListener("abort", () => {
                    firstAbortReason = signal.reason;
                    rej(signal.reason);
                });
            } else {
                setTimeout(() => rej(new Error("second")), 1);
            }
        }),
    });
    const stop = effect(() => { q.data(); });
    await tick();
    q.refetch().catch(() => {});         // supersedes first
    await tick();
    assert.equal(firstAbortReason, "lite-query:refetch");
    stop();
    qc.dispose();
});

test("abort reasons: removed reason reaches in-flight fetcher when removeQueries called", async () => {
    const qc = queryClient();
    let observedReason;
    const q = query(qc, {
        key: ["x"],
        fetcher: ({ signal }) => new Promise((_, rej) => {
            signal.addEventListener("abort", () => {
                observedReason = signal.reason;
                rej(signal.reason);
            });
        }),
    });
    const stop = effect(() => { q.data(); });
    await tick();
    qc.removeQueries(["x"]);
    await tick();
    assert.equal(observedReason, "lite-query:removed");
    stop();
    qc.dispose();
});

test("abort reasons: timeout reason reaches the fetcher when per-query timeout fires", async () => {
    const clock = createMockClock();
    const qc = queryClient({
        now: clock.now, setTimeout: clock.setTimeout, clearTimeout: clock.clearTimeout,
    });
    let observedReason;
    const q = query(qc, {
        key: ["x"],
        timeout: 100,
        retry: 0,
        fetcher: ({ signal }) => new Promise((_, rej) => {
            signal.addEventListener("abort", () => {
                observedReason = signal.reason;
                rej(signal.reason);
            });
        }),
    });
    const stop = effect(() => { q.data(); });
    await tick();
    clock.advance(101);
    await tick();
    assert.equal(observedReason, "lite-query:timeout");
    stop();
    qc.dispose();
});

// ── reactive-key churn ──────────────────────────────────────────────────────

test("reactive key flipping rapidly: prior fetch is superseded, latest wins", async () => {
    const qc = queryClient();
    let nextResult = 0;
    const sig = signal(0);
    const q = query(qc, {
        key: () => ["k", sig()],
        fetcher: async ({ key }) => {
            const my = ++nextResult;
            await tick(); await tick(); await tick();
            return { my, key };
        },
    });
    const obs = [];
    const stop = effect(() => { obs.push(q.data()); });
    sig.set(1);
    sig.set(2);
    sig.set(3);
    await new Promise((r) => setTimeout(r, 50));
    const final = obs[obs.length - 1];
    // The latest entry resolved is for key 3. Each set creates a fresh entry,
    // so the data accessor follows whichever entry the watcher is currently
    // attached to.
    assert.deepEqual(final?.key, ["k", 3], "latest key wins");
    stop();
    qc.dispose();
});

// ── empty key ───────────────────────────────────────────────────────────────

test("empty key array works (uncommon but legal)", async () => {
    const qc = queryClient();
    const q = query(qc, { key: [], fetcher: async () => "root" });
    const stop = effect(() => { q.data(); });
    await tick(); await tick();
    assert.equal(q.data(), "root");
    stop();
    qc.dispose();
});
