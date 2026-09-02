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
import { createRoot, effect } from "@zakkster/lite-signal";

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
