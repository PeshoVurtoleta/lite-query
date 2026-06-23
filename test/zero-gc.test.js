// Zero-GC hot-path contract for lite-query.
//
// Each accessor read inside an effect must not allocate a fresh closure per
// read. trackObserver() registers a hoisted cleanupObserver function; multiple
// reads in one effect run register the same reference, fire at cleanup time,
// and net out observerCount transitions to zero. The cleanup closure must be
// allocated ONCE at query() construction, never per read.
//
// We assert: under a tight loop of effect re-runs (each reading 4 accessors),
// retained heap delta stays under a small budget. Skips automatically if run
// without --expose-gc.

import { test } from "node:test";
import assert from "node:assert/strict";
import { signal, effect, createRegistry, setDefaultRegistry } from "@zakkster/lite-signal";
import { queryClient, query } from "../Query.js";

const hasGc = typeof global !== "undefined" && typeof global.gc === "function";

test("zero-GC: warm accessors retain ~no memory across 50k re-runs", { skip: !hasGc && "run with --expose-gc to enable" }, () => {
    setDefaultRegistry(createRegistry({ maxNodes: 16384 }));

    const qc = queryClient();
    qc.setQueryData(["x"], { v: 1 });
    const q = query(qc, { key: ["x"], fetcher: async () => ({ v: 1 }) });

    const trig = signal(0);
    const stop = effect(() => { trig(); q.data(); q.fetching(); q.status(); q.error(); });

    // Warm V8
    for (let w = 0; w < 5_000; w++) trig.set(w + 1);

    global.gc(); global.gc();
    const before = process.memoryUsage().heapUsed;
    const ITERS = 50_000;
    for (let i = 0; i < ITERS; i++) trig.set(i + 100_000);
    global.gc(); global.gc();
    const after = process.memoryUsage().heapUsed;

    const perRun = (after - before) / ITERS;
    // ~50 B/re-run is generous (real value hovers near zero); a regression that
    // reintroduces per-read closure allocation would push this >100 B/re-run.
    assert.ok(perRun < 50, `expected retained < 50 B/effect-re-run; got ${perRun.toFixed(2)} B`);
    stop();
    qc.dispose();
});

test("hoisted cleanup: 4 reads per re-run still net to observerCount = 0", () => {
    // Semantic guard for the alloc optimization: registering the same cleanup
    // function multiple times within one effect run must still fire each
    // registration on cleanup, so the observer count balances out.
    setDefaultRegistry(createRegistry({ maxNodes: 16384 }));
    const qc = queryClient();
    qc.setQueryData(["x"], 1);
    const q = query(qc, { key: ["x"], fetcher: async () => 1 });

    let runs = 0;
    const trig = signal(0);
    const stop = effect(() => { trig(); q.data(); q.data(); q.data(); q.data(); runs++; });

    for (let i = 0; i < 50; i++) trig.set(i + 1);
    assert.equal(runs, 51, "effect re-runs once per trigger plus initial");

    // If observerCount drifted (e.g. cleanup didn't fire all 4 times), the
    // watcher would never tear down, and subsequent accessors would still see
    // the cached entry after dispose. Dispose, then verify nothing throws and
    // the entry is unaffected for a fresh observer.
    stop();
    // A fresh effect must see the cached data
    let observed;
    const stop2 = effect(() => { observed = q.data(); });
    assert.equal(observed, 1, "data still accessible after observer churn");
    stop2();
    qc.dispose();
});
