// Shared-stream (Q8) cross-tab projection + failover tests.
//
// Grows rung by rung: C2 wires the four wire-message cases (inert when off),
// C3-C7 the leader/follower/promotion machinery, C9 the seven failover cells.
// The suite drives real queryClient instances over the mock BroadcastChannel;
// no follower ever holds an iterator (the leader owns it and broadcasts frames).

import { test } from "node:test";
import assert from "node:assert/strict";
import { queryClient } from "../Query.js";
import { streamQuery } from "../StreamQuery.js";
import { createMockClock, createMockBroadcastChannel } from "./harness.js";
import { createRoot, effect, signal } from "@zakkster/lite-signal";
import { pipeToSignal } from "@zakkster/lite-stream";

// -- test rig ---------------------------------------------------------------

// A single clock shared across a tab set so advance() drives every watchdog at
// once. Each tab is a queryClient over one named channel; isLeader is a
// caller-supplied oracle (OR-3: correctness never depends on its answer).
function makeTab(bc, clock, name, isLeader, extra = {}) {
    return queryClient({
        crossTab: true,
        broadcastChannel: bc.BroadcastChannel,
        crossTabChannel: name,
        sharedStream: true,
        isLeader,
        now: clock.now,
        setTimeout: clock.setTimeout,
        clearTimeout: clock.clearTimeout,
        ...extra,
    });
}

// -- C2: wire routing, inert when off ---------------------------------------

test("shared-stream: stream-* messages are inert when sharedStream is off", async () => {
    const clock = createMockClock();
    const bc = createMockBroadcastChannel();
    const qc = queryClient({
        crossTab: true, broadcastChannel: bc.BroadcastChannel, crossTabChannel: "s",
        now: clock.now, setTimeout: clock.setTimeout, clearTimeout: clock.clearTimeout,
    });
    const peer = new bc.BroadcastChannel("s");
    peer.postMessage({ type: "stream-frame", key: ["k"], epochSeq: 1, clientId: "x", seq: 0, value: 42 });
    peer.postMessage({ type: "stream-open", key: ["k"], epochSeq: 1, clientId: "x" });
    peer.postMessage({ type: "stream-end", key: ["k"], epochSeq: 1, clientId: "x", ok: true, error: undefined });
    await clock.flush();
    assert.equal(qc.getQueryData(["k"]), undefined, "no frame projected when off");
    qc.dispose();
    peer.close();
});

test("shared-stream: stream-* to an unknown key is a safe no-op when active", async () => {
    const clock = createMockClock();
    const bc = createMockBroadcastChannel();
    const qc = makeTab(bc, clock, "s", () => false);
    const peer = new bc.BroadcastChannel("s");
    // No local entry has opted into sharing -> every handler early-returns at
    // the streamShared gate; nothing crashes, nothing is cached.
    peer.postMessage({ type: "stream-frame", key: ["ghost"], epochSeq: 3, clientId: "z", seq: 0, value: 1 });
    peer.postMessage({ type: "stream-end", key: ["ghost"], epochSeq: 3, clientId: "z", ok: false, error: "boom" });
    peer.postMessage({ type: "stream-req", key: ["ghost"] });
    await clock.flush();
    assert.equal(qc.getQueryData(["ghost"]), undefined);
    qc.dispose();
    peer.close();
});

// -- C3: leader broadcast path ----------------------------------------------

// Drive an async iterable from an array of values with an explicit end.
async function* fromValues(values) {
    for (const v of values) { yield v; }
}

// A push-driven async iterable: the test steps frames (push/end/fail) and
// flushes microtasks so the leader's pump and the mock channel drain in step.
function makeControllable() {
    let wake = null;
    const buffer = [];
    let done = false;
    let err = null;
    const wakeUp = () => { if (wake) { const r = wake; wake = null; r(); } };
    const iterable = {
        async *[Symbol.asyncIterator]() {
            while (true) {
                if (buffer.length) { yield buffer.shift(); continue; }
                if (err) throw err;
                if (done) return;
                await new Promise((r) => { wake = r; });
            }
        },
    };
    return {
        iterable,
        push(v) { buffer.push(v); wakeUp(); },
        end() { done = true; wakeUp(); },
        fail(e) { err = e; wakeUp(); },
    };
}

// Flush enough microtask rounds that the pump yields and the channel delivers.
async function drain(n = 30) { for (let i = 0; i < n; i++) await Promise.resolve(); }

// Subscribe to a stream handle inside a root so its watcher runs; returns the
// dispose thunk. Reads data/status/count/droppedCount so telemetry updates.
function observe(handle) {
    return createRoot(() => effect(() => {
        handle.data(); handle.status(); handle.count(); handle.droppedCount();
    }));
}

// Capture every message a leader tab emits onto the channel (a raw peer that
// never replies). Returns the captured list + a close thunk.
function sniff(bc, name) {
    const captured = [];
    const peer = new bc.BroadcastChannel(name);
    peer.addEventListener("message", (evt) => captured.push(evt.data));
    return { captured, close: () => peer.close() };
}

test("shared-stream: leader broadcasts stream-open, per-frame stream-frame (monotone seq), stream-end", async () => {
    const clock = createMockClock();
    const bc = createMockBroadcastChannel();
    const leader = makeTab(bc, clock, "L", () => true);
    const spy = sniff(bc, "L");

    const s = streamQuery(leader, { key: ["feed"], stream: () => fromValues([10, 20, 30]) });
    const dispose = createRoot(() => effect(() => { s.data(); }));
    // let the async iterator drain
    for (let i = 0; i < 20; i++) await Promise.resolve();
    await clock.flush();

    const opens = spy.captured.filter((m) => m.type === "stream-open");
    const frames = spy.captured.filter((m) => m.type === "stream-frame");
    const ends = spy.captured.filter((m) => m.type === "stream-end");

    assert.equal(opens.length, 1, "one ownership claim");
    assert.equal(frames.length, 3, "one frame per value");
    assert.deepEqual(frames.map((f) => f.seq), [1, 2, 3], "monotone 1-based seq");
    assert.deepEqual(frames.map((f) => f.value), [10, 20, 30], "values in order");
    assert.equal(frames[0].epochSeq, opens[0].epochSeq, "frames carry the open epoch");
    assert.equal(frames[0].clientId, opens[0].clientId, "frames carry the client id");
    assert.equal(ends.length, 1, "one terminal end");
    assert.equal(ends[0].ok, true, "natural completion is ok:true");
    assert.equal(ends[0].epochSeq, opens[0].epochSeq, "end carries the open epoch");

    dispose();
    s.dispose();
    leader.dispose();
    spy.close();
});

test("shared-stream: a non-shared stream never broadcasts (A7 zero-cost off)", async () => {
    const clock = createMockClock();
    const bc = createMockBroadcastChannel();
    // crossTab on but sharedStream OFF -> a stream owns its iterator locally and
    // emits no stream-* wire traffic.
    const solo = queryClient({
        crossTab: true, broadcastChannel: bc.BroadcastChannel, crossTabChannel: "N",
        now: clock.now, setTimeout: clock.setTimeout, clearTimeout: clock.clearTimeout,
    });
    const spy = sniff(bc, "N");
    const s = streamQuery(solo, { key: ["x"], stream: () => fromValues([1, 2]) });
    const dispose = createRoot(() => effect(() => { s.data(); }));
    for (let i = 0; i < 20; i++) await Promise.resolve();
    await clock.flush();
    assert.equal(spy.captured.filter((m) => String(m.type).startsWith("stream-")).length, 0);
    dispose();
    s.dispose();
    solo.dispose();
    spy.close();
});

test("shared-stream: leader broadcasts stream-end ok:false on iterator error", async () => {
    const clock = createMockClock();
    const bc = createMockBroadcastChannel();
    const leader = makeTab(bc, clock, "E", () => true);
    const spy = sniff(bc, "E");
    const boom = new Error("upstream gone");
    async function* failing() { yield 1; throw boom; }
    const s = streamQuery(leader, { key: ["f"], stream: () => failing() });
    const dispose = createRoot(() => effect(() => { s.data(); s.status(); }));
    for (let i = 0; i < 20; i++) await Promise.resolve();
    await clock.flush();
    const ends = spy.captured.filter((m) => m.type === "stream-end");
    assert.equal(ends.length, 1);
    assert.equal(ends[0].ok, false);
    assert.equal(ends[0].error, boom);
    dispose();
    s.dispose();
    leader.dispose();
    spy.close();
});

// -- C4: follower latest-mode projection ------------------------------------

test("shared-stream: a follower projects the leader's latest frames, holds no iterator", async () => {
    const clock = createMockClock();
    const bc = createMockBroadcastChannel();
    const leader = makeTab(bc, clock, "P", () => true);
    const follower = makeTab(bc, clock, "P", () => false);
    const src = makeControllable();

    const ls = streamQuery(leader, { key: ["t"], stream: () => src.iterable });
    const fs = streamQuery(follower, { key: ["t"], stream: () => { throw new Error("follower must never open an iterator"); } });
    const dl = observe(ls);
    const df = observe(fs);
    await drain();

    src.push(1); await drain();
    src.push(2); await drain();
    src.push(3); await drain();

    assert.equal(leader.getQueryData(["t"]), 3, "leader has latest");
    assert.equal(follower.getQueryData(["t"]), 3, "follower projected latest");
    // follower entry holds no iterator
    const fe = follower._internal.entries.get(JSON.stringify(["t"]));
    assert.equal(fe.streamStop, null, "follower has no pump");
    assert.equal(fe.streamOwner, false, "follower is not the owner");
    assert.equal(fe.streamCount, 3, "follower counted 3 frames");
    assert.equal(fe.streamDropped, 0, "latest mode drops nothing");

    src.end(); await drain();
    dl(); df(); ls.dispose(); fs.dispose(); leader.dispose(); follower.dispose();
});

test("shared-stream: projection gate drops duplicates and stale-epoch frames (OR-4)", async () => {
    const clock = createMockClock();
    const bc = createMockBroadcastChannel();
    const follower = makeTab(bc, clock, "G", () => false);
    const fs = streamQuery(follower, { key: ["k"], stream: () => makeControllable().iterable });
    const df = observe(fs);
    await drain();
    const peer = new bc.BroadcastChannel("G");
    const send = (m) => { peer.postMessage(m); };

    send({ type: "stream-frame", key: ["k"], epochSeq: 1, clientId: "a", seq: 1, value: "v1" });
    await drain();
    assert.equal(follower.getQueryData(["k"]), "v1");
    // duplicate seq -> dropped
    send({ type: "stream-frame", key: ["k"], epochSeq: 1, clientId: "a", seq: 1, value: "DUP" });
    await drain();
    assert.equal(follower.getQueryData(["k"]), "v1", "duplicate dropped");
    // stale lower epoch -> dropped
    send({ type: "stream-frame", key: ["k"], epochSeq: 2, clientId: "b", seq: 1, value: "v2" });
    await drain();
    assert.equal(follower.getQueryData(["k"]), "v2", "higher epoch adopted");
    send({ type: "stream-frame", key: ["k"], epochSeq: 1, clientId: "a", seq: 5, value: "STALE" });
    await drain();
    assert.equal(follower.getQueryData(["k"]), "v2", "stale-epoch frame dropped");

    const fe = follower._internal.entries.get(JSON.stringify(["k"]));
    assert.equal(fe.streamCount, 2, "only two frames applied");
    df(); fs.dispose(); follower.dispose(); peer.close();
});

test("shared-stream: seq gap is accepted and counted, ordering never breaks (OR-4)", async () => {
    const clock = createMockClock();
    const bc = createMockBroadcastChannel();
    const follower = makeTab(bc, clock, "H", () => false);
    const fs = streamQuery(follower, { key: ["k"], stream: () => makeControllable().iterable });
    const df = observe(fs);
    await drain();
    const peer = new bc.BroadcastChannel("H");
    peer.postMessage({ type: "stream-frame", key: ["k"], epochSeq: 1, clientId: "a", seq: 1, value: "a" });
    await drain();
    // seq jumps from 1 to 4 -> two frames missed, accepted and counted as loss
    peer.postMessage({ type: "stream-frame", key: ["k"], epochSeq: 1, clientId: "a", seq: 4, value: "d" });
    await drain();
    assert.equal(follower.getQueryData(["k"]), "d", "gap frame accepted (at-most-once, loss allowed)");
    const fe = follower._internal.entries.get(JSON.stringify(["k"]));
    assert.equal(fe.projSeq, 4, "cursor advanced to the accepted seq");
    df(); fs.dispose(); follower.dispose(); peer.close();
});

test("shared-stream: droppedCount() fallback reads streamDropped for a follower (V4)", async () => {
    const clock = createMockClock();
    const bc = createMockBroadcastChannel();
    const follower = makeTab(bc, clock, "V", () => false);
    const fs = streamQuery(follower, { key: ["k"], stream: () => makeControllable().iterable });
    const df = observe(fs);
    await drain();
    // A follower has no pump (streamStop === null), so droppedCount() must read
    // e.streamDropped -- the public accessor needs no change (V4).
    assert.equal(fs.droppedCount(), 0);
    const fe = follower._internal.entries.get(JSON.stringify(["k"]));
    assert.equal(fe.streamStop, null);
    df(); fs.dispose(); follower.dispose();
});

test("shared-stream: dehydrate excludes a projected follower entry (V7)", async () => {
    const clock = createMockClock();
    const bc = createMockBroadcastChannel();
    const follower = makeTab(bc, clock, "D", () => false);
    const fs = streamQuery(follower, { key: ["k"], stream: () => makeControllable().iterable });
    const df = observe(fs);
    await drain();
    const peer = new bc.BroadcastChannel("D");
    peer.postMessage({ type: "stream-frame", key: ["k"], epochSeq: 1, clientId: "a", seq: 1, value: "x" });
    await drain();
    const snap = follower.dehydrate();
    const hasStream = snap.entries.some((r) => JSON.stringify(r.key) === JSON.stringify(["k"]));
    assert.equal(hasStream, false, "isStream follower entry is not persisted");
    df(); fs.dispose(); follower.dispose(); peer.close();
});

// -- C5: buffer-window projection + differential parity ---------------------

async function* asyncIterableOf(values) { for (const v of values) yield v; }

// The oracle: lite-stream's own pipeToSignal buffer window over V. Returns the
// final snapshot array + its droppedCount. This is the contract projectFrame's
// buffer path must match element-for-element (A6); when LS4 ships
// createSignalWriter the seam swaps and this test does not change.
async function bufferOracle(values, m) {
    const target = signal(undefined);
    const stop = pipeToSignal(asyncIterableOf(values), target, { mode: "buffer", maxBuffer: m });
    for (let i = 0; i < values.length * 3 + 30; i++) await Promise.resolve();
    const out = target();
    return { arr: out, dropped: stop.droppedCount };
}

test("shared-stream: follower buffer window is element-for-element parity with pipeToSignal (A6)", async () => {
    const clock = createMockClock();
    const bc = createMockBroadcastChannel();
    let combos = 0;
    for (const m of [1, 2, 3, 7, 64]) {
        for (const n of [0, 1, m - 1, m, m + 1, 4 * m]) {
            if (n < 0) continue;
            combos++;
            const V = Array.from({ length: n }, (_, i) => ({ id: `${m}:${n}:${i}` }));
            const oracle = await bufferOracle(V, m);

            const name = `PAR-${m}-${n}`;
            const follower = makeTab(bc, clock, name, () => false, {});
            const fs = streamQuery(follower, { key: ["k"], mode: "buffer", maxBuffer: m, stream: () => makeControllable().iterable });
            const df = observe(fs);
            await drain(4);
            const peer = new bc.BroadcastChannel(name);
            for (let i = 0; i < V.length; i++) {
                peer.postMessage({ type: "stream-frame", key: ["k"], epochSeq: 1, clientId: "a", seq: i + 1, value: V[i] });
            }
            await drain(Math.max(12, n * 2 + 8));

            const got = follower.getQueryData(["k"]);
            if (V.length === 0) {
                assert.equal(got, undefined, `${name}: empty -> unwritten`);
            } else {
                assert.equal(Array.isArray(got), true, `${name}: array snapshot`);
                assert.equal(got.length, oracle.arr.length, `${name}: window length parity`);
                for (let i = 0; i < got.length; i++) {
                    assert.equal(got[i], oracle.arr[i], `${name}: element ${i} reference-identical`);
                }
            }
            assert.equal(fs.droppedCount(), oracle.dropped, `${name}: droppedCount parity`);
            df(); fs.dispose(); follower.dispose(); peer.close();
        }
    }
    assert.equal(combos, 30, "30 (m x |V|) combinations exercised");
});

test("shared-stream: buffer follower publishes a FRESH snapshot per frame (no shared reference)", async () => {
    const clock = createMockClock();
    const bc = createMockBroadcastChannel();
    const follower = makeTab(bc, clock, "FR", () => false);
    const fs = streamQuery(follower, { key: ["k"], mode: "buffer", maxBuffer: 3, stream: () => makeControllable().iterable });
    const df = observe(fs);
    await drain(4);
    const peer = new bc.BroadcastChannel("FR");
    peer.postMessage({ type: "stream-frame", key: ["k"], epochSeq: 1, clientId: "a", seq: 1, value: "a" });
    await drain();
    const snap1 = follower.getQueryData(["k"]);
    peer.postMessage({ type: "stream-frame", key: ["k"], epochSeq: 1, clientId: "a", seq: 2, value: "b" });
    await drain();
    const snap2 = follower.getQueryData(["k"]);
    assert.notEqual(snap1, snap2, "distinct array references per frame");
    assert.deepEqual(snap1, ["a"]);
    assert.deepEqual(snap2, ["a", "b"]);
    df(); fs.dispose(); follower.dispose(); peer.close();
});

// -- C6: watchdog / liveness / self-connect / lying oracle (A8) --------------

const ent = (tab, key) => tab._internal.entries.get(JSON.stringify(key));
const owners = (tabs, key) => tabs.filter((t) => { const e = ent(t, key); return e && e.streamOwner; }).length;

test("shared-stream: a follower self-connects when no leader serves within streamIdleTimeout (OR-3)", async () => {
    const clock = createMockClock();
    const bc = createMockBroadcastChannel();
    const src = makeControllable();
    const follower = makeTab(bc, clock, "W", () => false, { streamIdleTimeout: 1000 });
    const fs = streamQuery(follower, { key: ["k"], stream: () => src.iterable });
    const df = observe(fs);
    await drain();
    // no leader answers the stream-req; the follower is not the owner yet
    assert.equal(ent(follower, ["k"]).streamOwner, false);
    // silence past the idle bound -> the watchdog self-connects
    clock.advance(1000);
    await drain();
    assert.equal(ent(follower, ["k"]).streamOwner, true, "follower promoted itself on silence");
    src.push(42); await drain();
    assert.equal(follower.getQueryData(["k"]), 42, "the self-connected iterator drives data");
    df(); fs.dispose(); follower.dispose();
});

test("shared-stream: a healthy leader keeps the follower from promoting (watchdog rearms)", async () => {
    const clock = createMockClock();
    const bc = createMockBroadcastChannel();
    const leaderSrc = makeControllable();
    const leader = makeTab(bc, clock, "WH", () => true, { streamIdleTimeout: 1000 });
    const follower = makeTab(bc, clock, "WH", () => false, { streamIdleTimeout: 1000 });
    const ls = streamQuery(leader, { key: ["k"], stream: () => leaderSrc.iterable });
    const fs = streamQuery(follower, { key: ["k"], stream: () => { throw new Error("follower must not open while leader is healthy"); } });
    const dl = observe(ls); const df = observe(fs);
    await drain();
    // frames arrive well inside the idle bound, keeping lastFrameAt fresh
    for (let round = 0; round < 3; round++) {
        clock.advance(400);
        leaderSrc.push(round); await drain();
    }
    clock.advance(400); await drain();
    assert.equal(ent(follower, ["k"]).streamOwner, false, "follower never promoted under a live leader");
    assert.equal(owners([leader, follower], ["k"]), 1, "exactly one owner");
    leaderSrc.end(); await drain();
    dl(); df(); ls.dispose(); fs.dispose(); leader.dispose(); follower.dispose();
});

test("shared-stream A8: convergence holds with an all-TRUE lying oracle (every tab thinks it leads)", async () => {
    const clock = createMockClock();
    const bc = createMockBroadcastChannel();
    const tabs = [];
    const handles = [];
    const srcs = [];
    for (let i = 0; i < 3; i++) {
        const src = makeControllable();
        const t = makeTab(bc, clock, "AT", () => true, { streamIdleTimeout: 1000 });
        const h = streamQuery(t, { key: ["k"], stream: () => src.iterable });
        observe(h);
        tabs.push(t); handles.push(h); srcs.push(src);
    }
    await drain(20);
    // every tab opened as a leader; the moment any yields a frame the race
    // resolves on (epochSeq, clientId) and the losers abdicate.
    for (let r = 0; r < 3; r++) { srcs.forEach((s) => s.push(r)); await drain(20); }
    assert.equal(owners(tabs, ["k"]), 1, "the epoch/clientId race converges to exactly one connection");
    handles.forEach((h) => h.dispose());
    tabs.forEach((t) => t.dispose());
});

test("shared-stream A8: convergence holds with an all-FALSE lying oracle (every tab thinks it follows)", async () => {
    const clock = createMockClock();
    const bc = createMockBroadcastChannel();
    const tabs = [];
    const handles = [];
    const srcs = [];
    for (let i = 0; i < 3; i++) {
        const src = makeControllable();
        const t = makeTab(bc, clock, "AF", () => false, { streamIdleTimeout: 1000 });
        const h = streamQuery(t, { key: ["k"], stream: () => src.iterable });
        observe(h);
        tabs.push(t); handles.push(h); srcs.push(src);
    }
    await drain(20);
    // nobody claims leadership; every watchdog fires and races to self-connect
    clock.advance(1000); await drain(20);
    for (let r = 0; r < 3; r++) { srcs.forEach((s) => s.push(r)); clock.advance(1000); await drain(20); }
    assert.equal(owners(tabs, ["k"]), 1, "correctness invariant under a lying oracle; only connection count would degrade");
    handles.forEach((h) => h.dispose());
    tabs.forEach((t) => t.dispose());
});

test("shared-stream: streamIdleTimeout defaults to sharedFetchTimeout", () => {
    const bc = createMockBroadcastChannel();
    const qc = queryClient({
        crossTab: true, broadcastChannel: bc.BroadcastChannel, crossTabChannel: "z",
        sharedStream: true, isLeader: () => true, sharedFetchTimeout: 4242,
    });
    assert.equal(qc.options.streamIdleTimeout, 4242);
    qc.dispose();
});

// -- C7: promotion / race tiebreak / abdication / adopt (V1) / V2 -----------

test("shared-stream: a promoted follower adopts its window + counters, never regresses to pending (V1/F4)", async () => {
    const clock = createMockClock();
    const bc = createMockBroadcastChannel();
    const leaderSrc = makeControllable();
    const followerSrc = makeControllable();
    const leader = makeTab(bc, clock, "AD", () => true, { streamIdleTimeout: 1000 });
    const follower = makeTab(bc, clock, "AD", () => false, { streamIdleTimeout: 1000 });
    const ls = streamQuery(leader, { key: ["k"], mode: "buffer", maxBuffer: 5, stream: () => leaderSrc.iterable });
    const fs = streamQuery(follower, { key: ["k"], mode: "buffer", maxBuffer: 5, stream: () => followerSrc.iterable });
    const dl = observe(ls); const df = observe(fs);
    await drain();
    leaderSrc.push("a"); await drain();
    leaderSrc.push("b"); await drain();
    leaderSrc.push("c"); await drain();
    const fe = ent(follower, ["k"]);
    assert.deepEqual(follower.getQueryData(["k"]), ["a", "b", "c"], "projected window");
    assert.equal(fe.streamCount, 3);
    const windowBefore = follower.getQueryData(["k"]);

    // leader leaves gracefully -> follower promotes with adopt:true
    ls.dispose();
    await drain();
    assert.equal(fe.streamOwner, true, "follower promoted itself");
    assert.equal(fe.streamCount, 3, "count continues from k (never reset to 0)");
    assert.notEqual(fe.status(), "pending", "no pending regression");
    assert.equal(follower.getQueryData(["k"]), windowBefore, "window array unchanged across the promotion instant");
    df(); fs.dispose(); leader.dispose(); follower.dispose();
});

test("shared-stream: two followers racing promotion -> exactly one owner (tiebreak)", async () => {
    const clock = createMockClock();
    const bc = createMockBroadcastChannel();
    const a = makeTab(bc, clock, "RC", () => false, { streamIdleTimeout: 1000 });
    const b = makeTab(bc, clock, "RC", () => false, { streamIdleTimeout: 1000 });
    const ha = streamQuery(a, { key: ["k"], stream: () => makeControllable().iterable });
    const hb = streamQuery(b, { key: ["k"], stream: () => makeControllable().iterable });
    observe(ha); observe(hb);
    await drain(10);
    // both followers time out and self-connect in the same drain
    clock.advance(1000); await drain(20);
    assert.equal(owners([a, b], ["k"]), 1, "the (epochSeq, clientId) tiebreak leaves exactly one owner");
    ha.dispose(); hb.dispose(); a.dispose(); b.dispose();
});

test("shared-stream: an owner abdicates on a higher-ranked stream-open and reverts to projecting", async () => {
    const clock = createMockClock();
    const bc = createMockBroadcastChannel();
    const src = makeControllable();
    const tab = makeTab(bc, clock, "AB", () => true, { streamIdleTimeout: 1000 });
    const h = streamQuery(tab, { key: ["k"], stream: () => src.iterable });
    observe(h);
    await drain();
    const e = ent(tab, ["k"]);
    assert.equal(e.streamOwner, true, "opened as owner");
    const myEpoch = e.streamEpoch;
    // a strictly higher epoch claim arrives -> abdicate
    const peer = new bc.BroadcastChannel("AB");
    peer.postMessage({ type: "stream-open", key: ["k"], epochSeq: myEpoch + 5, clientId: "zzzzzzzz" });
    await drain();
    assert.equal(e.streamOwner, false, "abdicated to the higher-ranked leader");
    assert.equal(e.streamStop, null, "own iterator aborted");
    peer.postMessage({ type: "stream-frame", key: ["k"], epochSeq: myEpoch + 5, clientId: "zzzzzzzz", seq: 1, value: "remote" });
    await drain();
    assert.equal(tab.getQueryData(["k"]), "remote", "reverted to projecting the winner's frames");
    h.dispose(); tab.dispose(); peer.close();
});

test("shared-stream: promotion announce is DEFERRED out of the handler, not swallowed (V2)", async () => {
    const clock = createMockClock();
    const bc = createMockBroadcastChannel();
    const follower = makeTab(bc, clock, "V2", () => false, { streamIdleTimeout: 1000 });
    const fs = streamQuery(follower, { key: ["k"], stream: () => makeControllable().iterable });
    observe(fs);
    await drain();
    // establish a projected epoch, then a graceful close arrives from a handler
    const peer = new bc.BroadcastChannel("V2");
    const captured = [];
    peer.addEventListener("message", (evt) => captured.push(evt.data));
    peer.postMessage({ type: "stream-open", key: ["k"], epochSeq: 2, clientId: "leaderA" });
    peer.postMessage({ type: "stream-frame", key: ["k"], epochSeq: 2, clientId: "leaderA", seq: 1, value: "v" });
    await drain();
    captured.length = 0;
    // closing is received INSIDE onRemoteMessage; the promotion's stream-open
    // must still leave (broadcast would swallow it -- broadcastControl/deferral does not).
    peer.postMessage({ type: "stream-end", key: ["k"], epochSeq: 2, clientId: "leaderA", ok: false, error: undefined, reason: "closing" });
    await drain();
    const opens = captured.filter((m) => m.type === "stream-open");
    assert.ok(opens.length >= 1, "the deferred promotion announce reached the channel (V2)");
    assert.equal(ent(follower, ["k"]).streamOwner, true, "follower promoted after the graceful close");
    fs.dispose(); follower.dispose(); peer.close();
});

test("shared-stream: a closing end is not a terminal outcome (no success/error settle)", async () => {
    const clock = createMockClock();
    const bc = createMockBroadcastChannel();
    const follower = makeTab(bc, clock, "CL", () => false, { streamIdleTimeout: 1000 });
    const fs = streamQuery(follower, { key: ["k"], stream: () => makeControllable().iterable });
    observe(fs);
    await drain();
    const peer = new bc.BroadcastChannel("CL");
    peer.postMessage({ type: "stream-open", key: ["k"], epochSeq: 1, clientId: "L" });
    peer.postMessage({ type: "stream-frame", key: ["k"], epochSeq: 1, clientId: "L", seq: 1, value: "x" });
    await drain();
    peer.postMessage({ type: "stream-end", key: ["k"], epochSeq: 1, clientId: "L", ok: false, error: undefined, reason: "closing" });
    await drain();
    const st = ent(follower, ["k"]).status();
    assert.notEqual(st, "error", "closing is not an error settle");
    assert.notEqual(st, "success", "closing is not a success settle");
    fs.dispose(); follower.dispose(); peer.close();
});

test("shared-stream: an owner abdicates on a higher-ranked frame even without a stream-open (frame-driven race)", async () => {
    const clock = createMockClock();
    const bc = createMockBroadcastChannel();
    const src = makeControllable();
    const tab = makeTab(bc, clock, "FD", () => true, { streamIdleTimeout: 1000 });
    const h = streamQuery(tab, { key: ["k"], stream: () => src.iterable });
    observe(h);
    await drain();
    const e = ent(tab, ["k"]);
    const peer = new bc.BroadcastChannel("FD");
    peer.postMessage({ type: "stream-frame", key: ["k"], epochSeq: e.streamEpoch + 9, clientId: "zzzz", seq: 1, value: "win" });
    await drain();
    assert.equal(e.streamOwner, false, "abdicated on the higher-ranked frame");
    assert.equal(tab.getQueryData(["k"]), "win");
    h.dispose(); tab.dispose(); peer.close();
});

// -- C8: teardown releases projection slots (V5) ----------------------------

test("shared-stream: detach releases every follower projection slot (V5)", async () => {
    const clock = createMockClock();
    const bc = createMockBroadcastChannel();
    const follower = makeTab(bc, clock, "T1", () => false, { streamIdleTimeout: 1000 });
    const fs = streamQuery(follower, { key: ["k"], mode: "buffer", maxBuffer: 4, stream: () => makeControllable().iterable });
    const df = observe(fs);
    await drain();
    const peer = new bc.BroadcastChannel("T1");
    peer.postMessage({ type: "stream-frame", key: ["k"], epochSeq: 1, clientId: "L", seq: 1, value: "x" });
    await drain();
    const e = ent(follower, ["k"]);
    assert.notEqual(e.projWindow, null, "window populated while following");
    assert.notEqual(e.streamPromote, null, "promote closure installed");
    // last observer leaves -> detach releases the slots (entry stays cached)
    df();
    await drain();
    assert.equal(e.streamShared, false);
    assert.equal(e.streamOwner, false);
    assert.equal(e.streamPromote, null, "promote closure released");
    assert.equal(e.projWindow, null, "window released");
    assert.equal(e.streamWatchdog, null, "watchdog cleared");
    assert.equal(e.projEpoch, -1);
    assert.equal(e.projSeq, -1);
    fs.dispose(); follower.dispose(); peer.close();
});

test("shared-stream: removeQueries -> disposeEntry releases projection slots (V5)", async () => {
    const clock = createMockClock();
    const bc = createMockBroadcastChannel();
    const follower = makeTab(bc, clock, "T2", () => false, { streamIdleTimeout: 1000 });
    const fs = streamQuery(follower, { key: ["k"], stream: () => makeControllable().iterable });
    const df = observe(fs);
    await drain();
    const peer = new bc.BroadcastChannel("T2");
    peer.postMessage({ type: "stream-frame", key: ["k"], epochSeq: 1, clientId: "L", seq: 1, value: "x" });
    await drain();
    const e = ent(follower, ["k"]);
    follower.removeQueries(["k"]);
    await drain();
    assert.equal(ent(follower, ["k"]), undefined, "entry removed");
    assert.equal(e.streamShared, false);
    assert.equal(e.streamPromote, null);
    assert.equal(e.projWindow, null);
    assert.equal(e.streamWatchdog, null);
    df(); fs.dispose(); follower.dispose(); peer.close();
});

test("shared-stream: clear releases an owner's projection slots + watchdog (V5)", async () => {
    const clock = createMockClock();
    const bc = createMockBroadcastChannel();
    const src = makeControllable();
    const leader = makeTab(bc, clock, "T3", () => true, { streamIdleTimeout: 1000 });
    const ls = streamQuery(leader, { key: ["k"], stream: () => src.iterable });
    const dl = observe(ls);
    await drain();
    src.push(1); await drain();
    const e = ent(leader, ["k"]);
    assert.equal(e.streamOwner, true);
    leader.clear();
    await drain();
    assert.equal(e.streamShared, false);
    assert.equal(e.streamOwner, false);
    assert.equal(e.streamPromote, null);
    assert.equal(e.streamWatchdog, null);
    dl(); ls.dispose(); leader.dispose();
});

// -- C9: the seven failover cells (F1-F7) -----------------------------------

// A mock upstream that counts LIVE connections (A2/G4): every open increments
// `live`, every iterator return/throw/abort decrements it. pushAll/endAll/failAll
// drive whichever connections are currently open (losers abort and discard).
function makeCountedSource() {
    let live = 0, opened = 0;
    const conns = new Set();
    const factory = (ctx) => {
        opened++;
        const conn = { buf: [], wake: null, done: false, err: null, aborted: false, closed: false };
        conn.k = () => { if (conn.wake) { const r = conn.wake; conn.wake = null; r(); } };
        const close = () => { if (!conn.closed) { conn.closed = true; live--; conns.delete(conn); } };
        conns.add(conn); live++;
        if (ctx && ctx.signal) ctx.signal.addEventListener("abort", () => { conn.aborted = true; conn.k(); close(); });
        return {
            async *[Symbol.asyncIterator]() {
                try {
                    while (true) {
                        if (conn.buf.length) { yield conn.buf.shift(); continue; }
                        if (conn.aborted) return;
                        if (conn.err) throw conn.err;
                        if (conn.done) return;
                        await new Promise((r) => { conn.wake = r; });
                    }
                } finally { close(); }
            },
        };
    };
    return {
        factory,
        get live() { return live; },
        get opened() { return opened; },
        pushAll(v) { for (const c of conns) { c.buf.push(v); c.k(); } },
        endAll() { for (const c of conns) { c.done = true; c.k(); } },
        failAll(e) { for (const c of conns) { c.err = e; c.k(); } },
    };
}

// Record every distinct data() a handle projects (OR-4 dup/reorder detector:
// strictly-increasing integer frames make any duplicate or reorder visible).
function observeRec(handle) {
    const values = [];
    const dispose = createRoot(() => effect(() => {
        const v = handle.data();
        if (typeof v === "number") values.push(v);
    }));
    return { dispose, values };
}
function assertStrictlyIncreasing(vals, label) {
    for (let i = 1; i < vals.length; i++) {
        assert.ok(vals[i] > vals[i - 1], `${label}: no dup/reorder (${vals[i - 1]} -> ${vals[i]})`);
    }
}

test("failover: leader closes gracefully -- exactly one follower promotes, connections stay 1", async () => {
    const clock = createMockClock();
    const bc = createMockBroadcastChannel();
    const src = makeCountedSource();
    const leader = makeTab(bc, clock, "F1", () => true, { streamIdleTimeout: 1000 });
    const fa = makeTab(bc, clock, "F1", () => false, { streamIdleTimeout: 1000 });
    const fb = makeTab(bc, clock, "F1", () => false, { streamIdleTimeout: 1000 });
    const hl = streamQuery(leader, { key: ["k"], stream: src.factory });
    const ha = streamQuery(fa, { key: ["k"], stream: src.factory });
    const hb = streamQuery(fb, { key: ["k"], stream: src.factory });
    const rl = observeRec(hl); const ra = observeRec(ha); const rb = observeRec(hb);
    await drain();
    let n = 0;
    for (; n < 5; n++) { src.pushAll(n); await drain(); }
    assert.equal(src.live, 1, "one upstream connection while the leader owns");
    hl.dispose();                                    // graceful close
    await drain(20);
    for (; n < 10; n++) { src.pushAll(n); await drain(); }
    assert.equal(src.live, 1, "exactly one connection survives the handover");
    assert.equal(owners([fa, fb], ["k"]), 1, "exactly one follower promoted");
    assertStrictlyIncreasing(ra.values, "fa");
    assertStrictlyIncreasing(rb.values, "fb");
    rl.dispose(); ra.dispose(); rb.dispose();
    ha.dispose(); hb.dispose(); leader.dispose(); fa.dispose(); fb.dispose();
});

test("failover: leader tab killed -- watchdog promotes within streamIdleTimeout, no dup", async () => {
    const clock = createMockClock();
    const bc = createMockBroadcastChannel();
    const src = makeCountedSource();
    const follower = makeTab(bc, clock, "F2", () => false, { streamIdleTimeout: 1000 });
    const h = streamQuery(follower, { key: ["k"], stream: src.factory });
    const rec = observeRec(h);
    await drain();
    // a raw "leader" peer streams a few frames, then is KILLED (channel closed,
    // NO stream-end). The watchdog is the only detector.
    const peer = new bc.BroadcastChannel("F2");
    peer.postMessage({ type: "stream-open", key: ["k"], epochSeq: 3, clientId: "killedLeader" });
    for (let s = 1; s <= 3; s++) peer.postMessage({ type: "stream-frame", key: ["k"], epochSeq: 3, clientId: "killedLeader", seq: s, value: s });
    await drain();
    assert.equal(follower.getQueryData(["k"]), 3, "projected the killed leader's frames");
    peer.close();                                    // tab killed, no farewell
    assert.equal(ent(follower, ["k"]).streamOwner, false);
    clock.advance(1000); await drain(20);
    assert.equal(ent(follower, ["k"]).streamOwner, true, "watchdog self-connected within streamIdleTimeout");
    src.pushAll(4); await drain();
    assert.equal(follower.getQueryData(["k"]), 4);
    assertStrictlyIncreasing(rec.values, "F2");
    rec.dispose(); h.dispose(); follower.dispose();
});

test("failover: leader hung -- follower self-connects, old leader abdicates on higher epoch", async () => {
    // OR-3 cell: correctness is invariant under a lying oracle -- run it with an
    // all-true and an all-false oracle; only connection count would differ.
    for (const oracle of [() => true, () => false]) {
        const clock = createMockClock();
        const bc = createMockBroadcastChannel();
        const src = makeCountedSource();
        const follower = makeTab(bc, clock, "F3", oracle, { streamIdleTimeout: 1000 });
        const h = streamQuery(follower, { key: ["k"], stream: src.factory });
        const rec = observeRec(h);
        await drain(6);
        // a hung leader: channel alive, frames stopped. If the oracle says the
        // follower leads, it opened its own connection already; either way the
        // watchdog/self-connect must leave it owning after silence.
        const peer = new bc.BroadcastChannel("F3");
        peer.postMessage({ type: "stream-open", key: ["k"], epochSeq: 2, clientId: "hungLeader" });
        peer.postMessage({ type: "stream-frame", key: ["k"], epochSeq: 2, clientId: "hungLeader", seq: 1, value: 1 });
        await drain(6);
        clock.advance(1000); await drain(20);
        src.pushAll(2); await drain(6);
        assert.equal(ent(follower, ["k"]).streamOwner, true, "self-connected on a hung leader (oracle-invariant)");
        assertStrictlyIncreasing(rec.values, "F3");
        rec.dispose(); h.dispose(); follower.dispose(); peer.close();
    }
});

test("failover: follower promoted mid-buffer -- window and counters survive", async () => {
    const clock = createMockClock();
    const bc = createMockBroadcastChannel();
    const leaderSrc = makeControllable();
    const followerSrc = makeControllable();
    const leader = makeTab(bc, clock, "F4", () => true, { streamIdleTimeout: 1000 });
    const follower = makeTab(bc, clock, "F4", () => false, { streamIdleTimeout: 1000 });
    const hl = streamQuery(leader, { key: ["k"], mode: "buffer", maxBuffer: 5, stream: () => leaderSrc.iterable });
    const hf = streamQuery(follower, { key: ["k"], mode: "buffer", maxBuffer: 5, stream: () => followerSrc.iterable });
    const dl = observe(hl); const df = observe(hf);
    await drain();
    leaderSrc.push("a"); await drain();
    leaderSrc.push("b"); await drain();
    const fe = ent(follower, ["k"]);
    const windowAt = follower.getQueryData(["k"]);
    const countAt = fe.streamCount;
    const droppedAt = hf.droppedCount();
    hl.dispose();                                    // promote mid-buffer
    await drain(20);
    assert.equal(fe.streamOwner, true, "promoted");
    assert.equal(follower.getQueryData(["k"]), windowAt, "data() array unchanged across the promotion instant");
    assert.ok(fe.streamCount >= countAt, "count continues from k, never resets to 0");
    assert.ok(hf.droppedCount() >= droppedAt, "droppedCount monotonic");
    assert.notEqual(fe.status(), "pending", "no status regression to pending");
    dl(); df(); hl.dispose(); hf.dispose(); leader.dispose(); follower.dispose();
});

test("failover: two tabs racing promotion -- exactly one connection survives", async () => {
    const clock = createMockClock();
    const bc = createMockBroadcastChannel();
    const src = makeCountedSource();
    const tabs = []; const handles = []; const recs = [];
    for (let i = 0; i < 5; i++) {
        const t = makeTab(bc, clock, "F5", () => false, { streamIdleTimeout: 1000 });
        const h = streamQuery(t, { key: ["k"], stream: src.factory });
        recs.push(observeRec(h));
        tabs.push(t); handles.push(h);
    }
    await drain(20);
    clock.advance(1000); await drain(30);           // all five watchdogs race
    src.pushAll(1); await drain(10);
    src.pushAll(2); await drain(10);
    assert.equal(owners(tabs, ["k"]), 1, "exactly one owner across five tabs");
    assert.equal(src.live, 1, "exactly one upstream connection survives");
    recs.forEach((r, i) => assertStrictlyIncreasing(r.values, `F5-tab${i}`));
    recs.forEach((r) => r.dispose());
    handles.forEach((h) => h.dispose());
    tabs.forEach((t) => t.dispose());
});

test("failover: stream completes during failover -- success in every tab, no dup", async () => {
    const clock = createMockClock();
    const bc = createMockBroadcastChannel();
    const leaderSrc = makeControllable();
    const leader = makeTab(bc, clock, "F6", () => true, { streamIdleTimeout: 1000 });
    const fa = makeTab(bc, clock, "F6", () => false, { streamIdleTimeout: 1000 });
    const fb = makeTab(bc, clock, "F6", () => false, { streamIdleTimeout: 1000 });
    const hl = streamQuery(leader, { key: ["k"], stream: () => leaderSrc.iterable });
    const ha = streamQuery(fa, { key: ["k"], stream: () => makeControllable().iterable });
    const hb = streamQuery(fb, { key: ["k"], stream: () => makeControllable().iterable });
    const rl = observeRec(hl); const ra = observeRec(ha); const rb = observeRec(hb);
    await drain();
    leaderSrc.push(1); await drain();
    leaderSrc.push(2); await drain();
    leaderSrc.end(); await drain(20);                // completes -> stream-end ok:true
    assert.equal(ent(fa, ["k"]).status(), "success", "follower A settled success");
    assert.equal(ent(fb, ["k"]).status(), "success", "follower B settled success");
    assert.equal(ent(leader, ["k"]).status(), "success", "leader settled success");
    assertStrictlyIncreasing(ra.values, "F6-a");
    assertStrictlyIncreasing(rb.values, "F6-b");
    rl.dispose(); ra.dispose(); rb.dispose();
    hl.dispose(); ha.dispose(); hb.dispose(); leader.dispose(); fa.dispose(); fb.dispose();
});

test("failover: stream errors during failover -- error surfaces then recovery, no inherited wedge", async () => {
    const clock = createMockClock();
    const bc = createMockBroadcastChannel();
    const src = makeCountedSource();
    const follower = makeTab(bc, clock, "F7", () => false, { streamIdleTimeout: 1000 });
    const h = streamQuery(follower, { key: ["k"], stream: src.factory });
    const rec = observeRec(h);
    await drain();
    const peer = new bc.BroadcastChannel("F7");
    peer.postMessage({ type: "stream-open", key: ["k"], epochSeq: 2, clientId: "leaderX" });
    peer.postMessage({ type: "stream-frame", key: ["k"], epochSeq: 2, clientId: "leaderX", seq: 1, value: 1 });
    await drain();
    const boom = new Error("upstream failed");
    peer.postMessage({ type: "stream-end", key: ["k"], epochSeq: 2, clientId: "leaderX", ok: false, error: boom });
    await drain();
    const fe = ent(follower, ["k"]);
    assert.equal(fe.status(), "error", "the dead leader's error is observable");
    assert.equal(follower.getQueryData ? untrackErr(follower, ["k"]) : true, boom, "error value surfaced");
    // a follower must never be wedged by a dead leader's failure: the watchdog
    // self-connects and recovery returns it to streaming.
    clock.advance(1000); await drain(20);
    assert.equal(fe.streamOwner, true, "self-connected after the error (no inherited wedge)");
    src.pushAll(5); await drain();
    assert.equal(fe.status(), "streaming", "recovered to streaming");
    assertStrictlyIncreasing(rec.values, "F7");
    rec.dispose(); h.dispose(); follower.dispose(); peer.close();
});

function untrackErr(tab, key) {
    const e = tab._internal.entries.get(JSON.stringify(key));
    return e ? e.error() : undefined;
}

test("shared-stream: sharedStreamActive requires opt-in + a leader oracle + a channel", async () => {
    const clock = createMockClock();
    const bc = createMockBroadcastChannel();
    // opted in but NO oracle -> inactive; a stream-frame stays inert.
    const noOracle = queryClient({
        crossTab: true, broadcastChannel: bc.BroadcastChannel, crossTabChannel: "s2",
        sharedStream: true, now: clock.now, setTimeout: clock.setTimeout, clearTimeout: clock.clearTimeout,
    });
    const peer = new bc.BroadcastChannel("s2");
    peer.postMessage({ type: "stream-frame", key: ["k"], epochSeq: 1, clientId: "x", seq: 0, value: 9 });
    await clock.flush();
    assert.equal(noOracle.getQueryData(["k"]), undefined);
    noOracle.dispose();
    peer.close();
});
