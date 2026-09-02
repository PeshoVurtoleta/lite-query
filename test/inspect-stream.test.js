// @zakkster/lite-query -- devtools feed: streaming events (Q7, C4).
//
// stream:start / stream:value (the only true per-frame emit) / stream:done, and
// stream:error in BOTH phases -- reason "open" (the stream factory threw
// synchronously) and reason "iterator" (a live iterator failed). The subpath
// emits through _internal.feed + _internal.emitStream, so a stream event is one
// feed.hook !== null test on the pump's hot frame path.

import { test } from "node:test";
import assert from "node:assert/strict";
import { queryClient } from "../Query.js";
import { streamQuery } from "../StreamQuery.js";
import { effect } from "@zakkster/lite-signal";

const drain = async () => { for (let i = 0; i < 12; i++) await Promise.resolve(); };
const only = (events, type) => events.filter((e) => e.type === type);

test("stream:start, stream:value (counts), stream:done over a completing iterator", async () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    const events = [];
    const stop = qc.inspect((e) => events.push({ ...e }));
    async function* src() { yield "a"; yield "b"; }
    const sq = streamQuery(qc, { key: ["s"], stream: src, mode: "latest" });
    const stopE = effect(() => sq.data());
    await drain();
    const start = only(events, "stream:start");
    assert.equal(start.length, 1, "one stream:start");
    assert.deepEqual(start[0].key, ["s"]);
    const vals = only(events, "stream:value");
    assert.equal(vals.length, 2, "one stream:value per frame");
    assert.equal(vals[0].count, 1, "count is streamCount after the first frame");
    assert.equal(vals[1].count, 2);
    const done = only(events, "stream:done");
    assert.equal(done.length, 1, "one stream:done on natural completion");
    assert.equal(done[0].ok, true);
    assert.equal(done[0].count, 2, "done count is the final streamCount");
    stopE(); sq.dispose(); stop(); qc.dispose();
});

test("stream:value carries the frame value by reference", async () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    const events = [];
    const stop = qc.inspect((e) => events.push(e.type === "stream:value" ? e.value : null));
    const FRAME = { n: 1 };
    async function* src() { yield FRAME; }
    const sq = streamQuery(qc, { key: ["sv"], stream: src, mode: "latest" });
    const stopE = effect(() => sq.data());
    await drain();
    assert.ok(events.includes(FRAME), "the emitted frame is the same object the iterator yielded");
    stopE(); sq.dispose(); stop(); qc.dispose();
});

test("stream:error reason 'iterator' when a live iterator throws", async () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    const events = [];
    const stop = qc.inspect((e) => events.push({ ...e }));
    const boom = new Error("iterator failed");
    async function* src() { yield 1; throw boom; }
    const sq = streamQuery(qc, { key: ["se"], stream: src, mode: "latest" });
    const stopE = effect(() => sq.data());
    await drain();
    const err = only(events, "stream:error");
    assert.equal(err.length, 1);
    assert.equal(err[0].reason, "iterator");
    assert.equal(err[0].ok, false);
    assert.equal(err[0].value, boom, "the error value is the thrown error");
    stopE(); sq.dispose(); stop(); qc.dispose();
});

test("stream:error reason 'open' when the stream factory throws synchronously", async () => {
    const qc = queryClient({ defaultStaleTime: 0 });
    const events = [];
    const stop = qc.inspect((e) => events.push({ ...e }));
    const boom = new Error("open failed");
    const sq = streamQuery(qc, { key: ["so"], stream: () => { throw boom; }, mode: "latest" });
    const stopE = effect(() => sq.data());
    await drain();
    const err = only(events, "stream:error");
    assert.equal(err.length, 1);
    assert.equal(err[0].reason, "open");
    assert.equal(err[0].value, boom);
    // A start still preceded the failed open.
    assert.equal(only(events, "stream:start").length, 1);
    stopE(); sq.dispose(); stop(); qc.dispose();
});
