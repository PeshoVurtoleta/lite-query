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
