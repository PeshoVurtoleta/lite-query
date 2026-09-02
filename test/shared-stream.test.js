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
