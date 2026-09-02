// @zakkster/lite-query -- ON-3 breaking fix: a mutation rejecting with a FALSY
// value settles "error", not "success" (A11).
//
// The retired 1.4.0 quirk branched on `if (resolvedError)`, so a rejection with
// null / 0 / "" / undefined was read as success and the feed reported ok:true.
// 2.0 tracks rejection by control flow: any rejection settles "error", the
// error signal holds the value verbatim, and mutation:settle reports ok:false.
// Migration: branch on status(), never on error() truthiness.

import { test } from "node:test";
import assert from "node:assert/strict";
import { queryClient, mutation } from "../Query.js";

// Each case: a mutation fn that throws the falsy value. `Object.is` compares
// the settled error signal so undefined vs null vs 0 vs "" are distinguished.
const FALSY = [
    { label: "null", value: null },
    { label: "zero", value: 0 },
    { label: "empty-string", value: "" },
    { label: "undefined", value: undefined },
];

for (const { label, value } of FALSY) {
    test(`ON-3: a mutation rejecting with ${label} settles "error" with the value verbatim`, async () => {
        const qc = queryClient();
        const settles = [];
        const stop = qc.inspect((e) => { if (e.type === "mutation:settle") settles.push({ ...e }); });
        const m = mutation(qc, { fn: async () => { throw value; } });

        await assert.rejects(m.mutate({ n: 1 }), (thrown) => {
            // a falsy rejection still rejects the awaited promise with its value
            assert.ok(Object.is(thrown, value) || thrown === value);
            return true;
        });

        assert.equal(m.status(), "error", "status settles error, never success");
        assert.ok(Object.is(m.error(), value), "error signal holds the rejection value verbatim");
        assert.equal(settles.length, 1, "one settle event");
        assert.equal(settles[0].ok, false, "mutation:settle reports ok:false");

        stop();
        m.dispose();
        qc.dispose();
    });
}

test("ON-3: a SUPERSEDED falsy rejection still reports reason 'superseded' with ok:false", async () => {
    const qc = queryClient();
    const settles = [];
    const stop = qc.inspect((e) => { if (e.type === "mutation:settle") settles.push({ ...e }); });

    // A slow mutation that rejects with null, superseded by a fast success.
    let releaseSlow;
    const slow = new Promise((_res, rej) => { releaseSlow = () => rej(null); });
    const m = mutation(qc, {
        fn: (vars) => vars.slow ? slow : Promise.resolve("fast-ok"),
    });

    const pSlow = m.mutate({ slow: true }).catch(() => {});   // gen 1, will reject null
    const pFast = m.mutate({ slow: false });                  // gen 2, resolves first
    await pFast;
    releaseSlow();                                            // gen 1 rejects null AFTER gen 2 settled
    await pSlow;

    const superseded = settles.find((s) => s.reason === "superseded");
    assert.ok(superseded, "the late falsy rejection emits a superseded settle");
    assert.equal(superseded.ok, false, "superseded falsy rejection is ok:false, not ok:true");
    assert.equal(m.status(), "success", "the winning mutation's success state stands");

    stop();
    m.dispose();
    qc.dispose();
});

test("ON-3: a truthy rejection is unchanged (regression guard)", async () => {
    const qc = queryClient();
    const boom = new Error("still an error");
    const m = mutation(qc, { fn: async () => { throw boom; } });
    await assert.rejects(m.mutate({}), /still an error/);
    assert.equal(m.status(), "error");
    assert.equal(m.error(), boom);
    m.dispose();
    qc.dispose();
});
