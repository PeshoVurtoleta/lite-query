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

import { signal, effect, createRegistry, setDefaultRegistry, createRoot } from "@zakkster/lite-signal";

import { queryClient, query } from "../Query.js";
import { createMockClock, createMockBroadcastChannel, createControlledFetcher, createQueuedFetcher } from "./harness.js";

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

// -----------------------------------------------------------------------------
// C4 -- dispatch through the normal fetch path; fetch:dispatch reason "interval".
// -----------------------------------------------------------------------------

test("dispatch: an interval fetch emits fetch:dispatch with reason 'interval' (no new event type)", async () => {
    const { clock, qc, fetcher } = mkPollEnv();
    const events = [];
    const stop = qc.inspect((e) => { if (e.type === "fetch:dispatch") events.push({ reason: e.reason }); });
    const q = query(qc, { key: ["a"], fetcher });
    const d = createRoot(() => effect(() => q.status()));
    await tick();
    assert.equal(events.length, 1, "attach dispatch");
    assert.equal(events[0].reason, null, "the attach fetch carries no interval reason");
    const entry = qc._internal.entries.get(JSON.stringify(["a"]));
    qc._internal.registerPoll(entry, 1000);
    clock.advance(1000); await tick();
    assert.equal(events.length, 2, "interval dispatch fired");
    assert.equal(events[1].reason, "interval", "the interval fetch is tagged reason 'interval'");
    d(); stop(); q.dispose();
});

test("dispatch: 'interval' is a reason VALUE only -- the 31 event types are unchanged", async () => {
    const { clock, qc, fetcher } = mkPollEnv();
    const types = new Set();
    const stop = qc.inspect((e) => types.add(e.type));
    const q = query(qc, { key: ["a"], fetcher });
    const d = createRoot(() => effect(() => q.status()));
    await tick();
    const entry = qc._internal.entries.get(JSON.stringify(["a"]));
    qc._internal.registerPoll(entry, 1000);
    clock.advance(1000); await tick();
    assert.ok(types.has("fetch:dispatch"), "interval fetch reuses fetch:dispatch");
    assert.ok(!types.has("fetch:interval") && !types.has("poll:tick"), "no new event type minted");
    d(); stop(); q.dispose();
});

test("dispatch: an interval poll dedups against an in-flight fetch (normal path)", async () => {
    // A slow fetcher stays in flight across a tick; maybeFetch's `entry.promise`
    // guard must swallow the interval dispatch rather than starting a second fetch.
    const clock = createMockClock();
    let calls = 0;
    let release;
    const qc = queryClient({ now: clock.now, setTimeout: clock.setTimeout, clearTimeout: clock.clearTimeout, defaultStaleTime: 0 });
    const fetcher = () => { calls++; return new Promise((r) => { release = r; }); };
    const q = query(qc, { key: ["a"], fetcher });
    const d = createRoot(() => effect(() => q.status()));
    await tick();
    assert.equal(calls, 1, "attach fetch in flight");
    const entry = qc._internal.entries.get(JSON.stringify(["a"]));
    qc._internal.registerPoll(entry, 1000);
    clock.advance(1000); await tick();
    assert.equal(calls, 1, "interval dispatch deduped against the in-flight fetch");
    release(42); await tick();
    d(); q.dispose();
});

test("dispatch: a poll does not fetch once the entry has no observers (refcount-gated)", async () => {
    const { clock, calls, qc, fetcher } = mkPollEnv();
    const q = query(qc, { key: ["a"], fetcher });
    const d = createRoot(() => effect(() => q.status()));
    await tick();
    const entry = qc._internal.entries.get(JSON.stringify(["a"]));
    qc._internal.registerPoll(entry, 1000);
    // Drop the observer WITHOUT unregistering the poll (simulate the scan racing
    // teardown): pollDispatch's observerCount>0 guard must keep it from fetching.
    d();
    await tick();
    assert.equal(entry.observerCount, 0, "no observers left");
    clock.advance(1000); await tick();
    assert.equal(calls.length, 1, "an observerless entry is never polled");
    qc._internal.unregisterPoll(entry);
    q.dispose();
});

// -----------------------------------------------------------------------------
// C5 -- watcher-seam lifecycle: register on attach; unregister on detach,
// enabled:false, and dispose. OFF path never touches the scanner.
// -----------------------------------------------------------------------------

test("lifecycle: OFF path -- no refetchInterval leaves pollList null forever (never registered)", async () => {
    const { qc, fetcher } = mkPollEnv();
    assert.equal(qc._internal.pollCount, -1, "pollList is null before any register (null is not zero)");
    const q = query(qc, { key: ["a"], fetcher });
    const d = createRoot(() => effect(() => q.status()));
    await tick();
    assert.equal(qc._internal.pollCount, -1, "an OFF-path query never registers a poll");
    d(); q.dispose();
});

test("lifecycle: register on attach -- a live observer with the option registers one record", async () => {
    const { qc, fetcher } = mkPollEnv();
    const q = query(qc, { key: ["a"], fetcher, refetchInterval: 1000 });
    assert.equal(qc._internal.pollCount, -1, "constructing the query does not register (no observer yet)");
    const d = createRoot(() => effect(() => q.status()));
    await tick();
    assert.equal(qc._internal.pollCount, 1, "attach registered exactly one poll record");
    d(); q.dispose();
});

test("lifecycle: no registration while observerCount is 0 (query alone never polls)", async () => {
    const { clock, calls, qc, fetcher } = mkPollEnv();
    query(qc, { key: ["a"], fetcher, refetchInterval: 1000 });   // never read inside an effect
    clock.advance(5000); await tick();
    assert.equal(qc._internal.pollCount, -1, "no observer -> no registration");
    assert.equal(calls.length, 0, "no observer -> no poll fetch");
});

test("lifecycle: no registration while enabled is false", async () => {
    const { qc, fetcher } = mkPollEnv();
    const q = query(qc, { key: ["a"], fetcher, refetchInterval: 1000, enabled: () => false });
    const d = createRoot(() => effect(() => q.status()));
    await tick();
    assert.equal(qc._internal.pollCount, -1, "enabled:false never registers");
    d(); q.dispose();
});

test("lifecycle: unregister on detach (last observer leaves)", async () => {
    const { qc, fetcher } = mkPollEnv();
    const q = query(qc, { key: ["a"], fetcher, refetchInterval: 1000 });
    const d = createRoot(() => effect(() => q.status()));
    await tick();
    assert.equal(qc._internal.pollCount, 1);
    d();                                    // last observer leaves
    await tick();                           // the deferred stopWatcher microtask runs
    assert.equal(qc._internal.pollCount, 0, "detach unregistered the poll (list empty, not null)");
    q.dispose();
});

test("lifecycle: unregister on enabled flipping false, re-register on flipping true", async () => {
    const { qc, fetcher } = mkPollEnv();
    const en = signal(true);
    const q = query(qc, { key: ["a"], fetcher, refetchInterval: 1000, enabled: () => en() });
    const d = createRoot(() => effect(() => q.status()));
    await tick();
    assert.equal(qc._internal.pollCount, 1, "enabled -> registered");
    en.set(false); await tick();
    assert.equal(qc._internal.pollCount, 0, "disabled -> unregistered");
    en.set(true); await tick();
    assert.equal(qc._internal.pollCount, 1, "re-enabled -> re-registered");
    d(); q.dispose();
});

test("lifecycle: unregister on dispose", async () => {
    const { qc, fetcher } = mkPollEnv();
    const q = query(qc, { key: ["a"], fetcher, refetchInterval: 1000 });
    const d = createRoot(() => effect(() => q.status()));
    await tick();
    assert.equal(qc._internal.pollCount, 1);
    q.dispose();
    assert.equal(qc._internal.pollCount, 0, "dispose unregistered the poll");
    d();
});

test("lifecycle: a reactive key swap moves the poll off the old entry onto the new", async () => {
    const { qc, fetcher } = mkPollEnv();
    const k = signal("a");
    const q = query(qc, { key: () => [k()], fetcher, refetchInterval: 1000 });
    const d = createRoot(() => effect(() => q.status()));
    await tick();
    assert.equal(qc._internal.pollCount, 1, "one record for key a");
    const ea = qc._internal.entries.get(JSON.stringify(["a"]));
    k.set("b"); await tick();
    assert.equal(qc._internal.pollCount, 1, "still one record after the swap (old unregistered, new registered)");
    const eb = qc._internal.entries.get(JSON.stringify(["b"]));
    assert.equal(ea.observerCount, 0, "old entry detached");
    assert.equal(eb.observerCount, 1, "new entry attached");
    d(); q.dispose();
});

// -----------------------------------------------------------------------------
// C5 / G4 -- shared-polling truthfulness. The leader polls; N followers stay
// fresh WITHOUT hitting their own network. A leaderless follower still self-
// fetches within sharedFetchTimeout (the Q8 liveness law, verbatim).
// -----------------------------------------------------------------------------

function setupSharedPoll() {
    const mockBC = createMockBroadcastChannel();
    const clock = createMockClock();
    let leaderAlive = true;                          // the election flag the "kill" flips
    const base = {
        crossTab: true,
        sharedFetch: true,
        broadcastChannel: mockBC.BroadcastChannel,
        crossTabChannel: "poll",
        now: clock.now, setTimeout: clock.setTimeout, clearTimeout: clock.clearTimeout,
        sharedFetchTimeout: 3000,
        defaultStaleTime: 0,
    };
    // The leader's authority is a live election read, not a hard-coded true, so a
    // "kill" can revoke it WITHOUT a clear/remove broadcast (a crashed tab simply
    // vanishes -- it never announces a teardown). demoteLeader() models exactly
    // that: no tab serves fetch-req afterward, and followers must self-heal.
    const qcLeader = queryClient({ ...base, isLeader: () => leaderAlive });
    const followers = [0, 1, 2].map(() => queryClient({ ...base, isLeader: () => false }));
    return { qcLeader, followers, mockBC, clock, demoteLeader() { leaderAlive = false; } };
}

test("G4: leader + 3 followers polling -- upstream fetches === the leader's alone across 20 ticks", async () => {
    const { qcLeader, followers, clock } = setupSharedPoll();
    const leaderF = createQueuedFetcher();
    const followerFs = followers.map(() => createControlledFetcher());

    const qL = query(qcLeader, { key: ["d"], fetcher: leaderF.fetcher, refetchInterval: 1000 });
    const dL = createRoot(() => effect(() => qL.data()));
    const qFs = followers.map((qc, i) => query(qc, { key: ["d"], fetcher: followerFs[i].fetcher, refetchInterval: 1000 }));
    const dFs = qFs.map((q) => createRoot(() => effect(() => q.data())));
    await tick();

    // The attach round: the leader fetched once, followers issued fetch-reqs only.
    assert.equal(leaderF.callCount, 1, "leader fetched on attach");
    for (const f of followerFs) assert.equal(f.callCount, 0, "no follower self-fetched on attach");
    leaderF.resolveNth(0, { tick: 0 });
    await tick(); await tick();

    const before = leaderF.callCount;               // == 1
    // 20 interval ticks. Each tick: leader polls (one upstream fetch, pending);
    // the three followers' fetch-reqs land while it is in flight -> dedup to it;
    // resolve -> leader broadcasts setData -> all followers fresh, zero self-fetch.
    for (let t = 1; t <= 20; t++) {
        clock.advance(1000);
        await tick();                               // deliver fetch-reqs -> dedup onto the in-flight leader fetch
        leaderF.resolveNth(t, { tick: t });
        await tick(); await tick();                 // settle + broadcast + follower receive
    }
    const upstream = leaderF.callCount - before;
    assert.equal(upstream, 20, "exactly one upstream fetch per tick -- the leader's alone (followers deduped)");
    for (const f of followerFs) assert.equal(f.callCount, 0, "no follower ever hit its own network across 20 ticks");

    dL(); dFs.forEach((d) => d());
    qL.dispose(); qFs.forEach((q) => q.dispose());
});

test("G4 liveness: kill the leader -> a follower self-fetches within sharedFetchTimeout", async () => {
    const { qcLeader, followers, clock, demoteLeader } = setupSharedPoll();
    const leaderF = createQueuedFetcher();
    const followerFs = followers.map(() => createControlledFetcher());

    const qL = query(qcLeader, { key: ["d"], fetcher: leaderF.fetcher, refetchInterval: 1000 });
    const dL = createRoot(() => effect(() => qL.data()));
    const qFs = followers.map((qc, i) => query(qc, { key: ["d"], fetcher: followerFs[i].fetcher, refetchInterval: 1000 }));
    const dFs = qFs.map((q) => createRoot(() => effect(() => q.data())));
    await tick();
    leaderF.resolveNth(0, { tick: 0 });
    await tick(); await tick();
    for (const f of followerFs) assert.equal(f.callCount, 0, "followers fresh via the leader, none self-fetched");

    // KILL the leader: revoke its election + stop it observing. No clear/remove
    // is broadcast (a crashed tab just vanishes), so the followers keep their
    // entries and nobody answers their fetch-req.
    demoteLeader();
    dL();
    await tick();

    // A follower's next poll issues a fetch-req nobody serves; its fallback timer
    // must self-fetch within sharedFetchTimeout so the UI never hangs.
    clock.advance(1000);                            // follower poll tick -> requestSharedFetch
    await tick();
    for (const f of followerFs) assert.equal(f.callCount, 0, "no self-fetch before the fallback window elapses");
    clock.advance(3000);                            // sharedFetchTimeout
    await tick();
    const selfFetched = followerFs.reduce((n, f) => n + (f.callCount > 0 ? 1 : 0), 0);
    assert.ok(selfFetched >= 1, "a leaderless follower self-fetched within sharedFetchTimeout (liveness law)");

    dFs.forEach((d) => d());
    qFs.forEach((q) => q.dispose());
    followers.forEach((qc) => qc.dispose());
    qcLeader.dispose();
});
