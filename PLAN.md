# PLAN -- Q7 -- lite-query v1.5.0 -- the devtools feed (ratified)

Operator ratification, 2026-09-02. The planner spec below is verbatim; this
header resolves its three STOP items and binds the coder. BRIEF.md OR-1..OR-10
remain in force; where this header speaks, it is the ruling.

- ON-1 (resolves STOP-1): APPROVED -- the per-client feed cell
  `const feed = { hook: null, pool: null }`, shared through `_internal.feed`,
  tested as `feed.hook !== null` at every emit site in both files. The
  charter's `qc._hook` spelling was the intent (one predictable null-test
  branch), not a storage mandate: there is no `qc` self-binding inside the
  client closure (planner V2), a closure `let` cannot cross the subpath
  boundary, and a public own-property would let user code bypass install
  validation. One cold allocation per client; no per-entry slots (OR-5
  holds). BRIEF T1 is read accordingly.
- ON-2 (resolves STOP-2): option (a). The phase-H profiled window and its
  GATE line stay BYTE-FROZEN exactly as shipped at 1.4.0. The hook
  double-run (absent / installed-pooled / installed-fresh) plus the
  20000-write sub-loop run as a separate provenance section of phase H
  AFTER the frozen gate evaluation, printing their own lines, gated by
  checkNoGc { maxMajor: 0, maxPauseMs: 4 } -- they never alter the GATE
  line. G3's "zero added allocation" is proven by the absent run's
  heapDelta/op === 0.00 in that section, not by re-freezing the GATE.
- ON-3 (resolves STOP-3): confirmed. The gating Q7 control addition is
  C-feed (QUERY_TORTURE_BREAK=feed -> detach-without-attach; controls go
  4 -> 5, tripped=5/5 under BREAK=1). Attempt D is recorded pass-or-fail
  verbatim in INCONCLUSIVE.md and is NOT a gate clause -- a control that
  cannot trip is decorative (OR-10); BRIEF G2 is read accordingly.
- ON-4 (recorded items, binding notes):
  - The OR-7 throwing-hook proposal (contain + auto-uninstall + exactly one
    console.error, the in-progress cache operation completes) is APPROVED
    as specified, including the guarded console.error.
  - The T3 recommendation (per-type pooled records built at install,
    dropped at uninstall) is APPROVED as the implementation target; phase H
    still measures both candidates per OR-6 and both land in the CHANGELOG.
  - V8/C6.0 pre-check (stub-client `mutation(` calls in test/) is binding
    on the coder before C5 lands.
  - V10: the stale "Suite >= 200" ROADMAP body line is corrected in the
    closeout docs commit, never mid-pipeline.
  - V11: OR-4's install/uninstall semantics win over the lite-signal
    variant, as the planner ruled.
  - Section 8's heading "268 -> 300" is a typo for the body's computed
    310 (268 + 42); the binding numbers are: plan +42, QA-asserted floor
    >= 298, BRIEF floor 278 (G1).

Pipeline: coder implements C1..C8 in order, one commit per rung, suite and
guards green at every rung; reviewer audits the full diff before qa; QA
count-freeze applies (docs print the frozen final count in C8).

---

# Q7 PLANNER SPEC -- @zakkster/lite-query v1.5.0 -- the devtools feed (`qc.inspect`)

Read: BRIEF.md, ROADMAP.md:854-925, Query.js (1820 lines, whole),
StreamQuery.js (whole), test/harness.js, test/torture.mjs,
test/torture/controls.mjs, bench/torture/cache-fuzzer.mjs, llms.txt,
CHANGELOG.md [1.4.0] head, LiteDevtools/llms.txt, LiteSignal/llms.txt.

---

## 1. V-FINDINGS (verified, file:line)

**V1 -- the persist seam is exactly as OR-4 describes.** `let persistHook =
null` Query.js:258; `notifyWrite()` 259-261; `installPersistHook` 266-277
(TypeError 267-269 non-function; Error 270-272 double-install;
`persistHook === fn`-guarded idempotent uninstall 274-276). Six
`notifyWrite()` sites: **439** (gc expiry), **668** (commitPage-throw
branch), **697** (settle, success+error uniformly), **756** (setQueryData,
covers every remote apply), **819** (removeQueries, `removed > 0`), **832**
(clear, `had`). Q7 touches none of these lines' conditions; every co-located
feed emit goes **after** the `notifyWrite()` call.

**V2 -- there is no `qc` binding inside the client closure.** `queryClient()`
returns an anonymous object literal (1069-1083); nothing inside can write
`qc._hook`. Worse, `StreamQuery.js:87` destructures `qc._internal` **once**
(`const { ensureEntry, attach, detach, opts }`), so a core `let inspectHook`
is invisible to the subpath forever. The charter's literal
`if (qc._hook !== null)` is unimplementable across the two files. ->
**STOP-1** + the feed-cell design (S6 below).

**V3 -- entry status is observable only as a signal write.** Status
transitions occur at 16 `status.set` sites, all fire-and-forget with no
prior-value read: Query.js **495** (detach pending->idle), **505** (detach
stream pending|streaming->idle), **548** (requestSharedFetch->pending),
**593** (runFetch->pending), **662** (commit-throw->error), **678**
(->success), **689** (->error), **749** (setQueryData->success), **977**
(seedEntry->success), **1001** (seedInfinite->success), **1353** (configure
adoption reset->idle); StreamQuery.js **108** (->pending), **115** (factory
throw->error), **133** (->streaming), **141** (->error), **156** (->success).
No entry slot mirrors the previous status (createEntry 323-371 has no such
field), so a `from` field requires reading `entry.status()` -- which must be
untracked, because `setQueryData`/`invalidate` are legal inside a user effect
and an added tracked read would subscribe (a behavior change, OR-9). See A-2
for the allocation-free reader.

**V4 -- staleness is one boolean.** `invalidatedSinceCompletion` set true at
**764** (invalidate loop, per matched entry); cleared at **664**, **693**,
**980**, **1003**, StreamQuery.js:**106**. Read only by `shouldFetch` 516-525
and `shouldStartStream` 175-181. It is the entire staleness surface.

**V5 -- abort reasons.** `ABORT_REASON` frozen at Query.js:**119-124**
(`lite-query:detach|refetch|removed|timeout`). Six `.abort(...)` call sites:
**436** (gc, REMOVED), **491** (detach, DETACH), **580** (runFetch supersede,
REFETCH), **602** (timeout timer, TIMEOUT), **812** (removeQueries, REMOVED),
**826** (clear, REMOVED). The reason string is available literally at each
site -- no lookup, no construction.

**V6 -- the fuzzer today asserts four things and no event semantics.**
bench/torture/cache-fuzzer.mjs: zero thrown errors (272-275), exact echo
ledger `broadcastsDuringFuzz === localMutations` (276-280), cross-tab
convergence on 24 sentinel keys (281-284), full entry-map drain + zero
dangling timers + pool at baseline (285-300). Break hook at **53** + **249**
injects one phantom broadcast (the C-fuzz control, controls.mjs:141-151).
`sharedFetch` is OFF (header line 33) -- so no `shared:*` event is
fuzz-reachable, and the G4 coverage set must exclude them or it lies.

**V7 -- phase H's warm loop performs zero status writes and zero stream frame
writes.** torture.mjs:272-279 reads only (`warm.data()`, `buf.count()`,
`buf.droppedCount()`, `buf.data()`, `inf.pages()/data()/hasNextPage()`); the
20 stream pushes happen at **268**, before `new GcProfiler().start()` at
**271**. The byte-frozen GATE line is produced by `evaluateGate` 136-146.
Consequence: the frozen GATE **cannot** prove zero-cost-uninstalled on the
stream frame path. -> **STOP-2**.

**V8 -- `mutation()` never touches `qc`.** Query.js:1552-1655 uses only
`mutOpts`; the parameter `qc` is unread and unvalidated. Any `qc._internal`
read added at construction changes the failure mode for a stub client.
Mandatory coder pre-check (task C6.0).

**V9 -- `disposeEntry` (401-427) is a shared helper on three removal paths**
(gc 438, removeQueries 816, clear 829). Emitting there loses the cause; emit
at the three call sites instead.

**V10 -- ROADMAP body line 915 ("Suite >= 200") is stale**; ROADMAP
frontmatter line 862 and BRIEF `tests_min` both say 278 against a 268 base
(llms.txt:3). 278 governs; recorded, not a STOP.

**V11 -- OR-3 vs OR-4 uninstall semantics diverge and OR-4 wins.**
lite-signal's `onGraphMutation` unsubscribe "restores prior listener"
(LiteSignal/llms.txt:519-521) and accepts `null` to replace;
`installPersistHook` throws on double-install and has no prior listener to
restore. OR-4's "SAME install/uninstall semantics (mirror the in-repo
precedent, including the two throw shapes)" is explicit -> mirror
installPersistHook; mirror lite-signal only for shape/casing/`ts`/idempotence.
Documented divergence, not a STOP.

**V12 -- `ts` convention verified at source.** lite-devtools LifecycleEvent:
`{ type, id, observed, ts }`, `ts: performance.now() if available, else
Date.now()` (LiteDevtools/llms.txt:254-259). Flat object, lower-case string
`type`, idempotent unsubscribe (line 62). We mirror all four.

### 1a. Emit-site enumeration (36 sites, 23 types)

| # | type | site(s) | note |
|---|---|---|---|
| 1 | `entry:create` | **386-389** (ensureEntryByHash, after `scheduleGc(e)`) | the one creation path (`ensureEntry` 373-375 delegates) |
| 2 | `entry:attach` | **474** (after `observerCount++`) | before `cancelGc` |
| 3 | `entry:detach` | **486** (after `observerCount--`) | before the ==0 teardown block |
| 4 | `entry:gc` | **439** (after `notifyWrite()`) | inside the `observerCount === 0` branch |
| 5 | `entry:remove` | **817** (removeQueries, reason `remove`), **829** (clear loop, reason `clear`), **1046** (hydrate rollback, reason `hydrate-rollback`) | 1046 closes the create-without-remove hole left by seeding at 1038 |
| 6 | `entry:status` | ONE funnel inside new `setStatus(entry, to)`; 16 call sites converted: Query.js 495, 505, 548, 593, 662, 678, 689, 749, 977, 1001, 1353; StreamQuery.js 108, 115, 133, 141, 156 | fires even when `from === to` (a `setQueryData` re-write is a real cache write) |
| 7 | `entry:stale` | **764** (invalidate loop, per matched entry) | |
| 8 | `fetch:dispatch` | **591** (after `entry.abortController = ac`) | `count = gen`, `value = startCursor` on infinite |
| 9 | `fetch:settle` | **697** (after notifyWrite site 2), **668** (commit-throw, after notifyWrite site 3, reason `commit-throw`) | |
| 10 | `fetch:abort` | **436**, **491**, **580**, **602**, **812**, **826** -- immediately after each `.abort(...)` | reason = the ABORT_REASON literal at that site |
| 11 | `tab:send` | **293** (inside `broadcast`, after the postMessage try/catch) | one funnel covers all 5 callers (550, 757, 804, 820, 833); `ok=false` when postMessage threw |
| 12 | `tab:receive` | **299-300** (after `const m = evt.data`, before the switch) | reason = `m && m.type` |
| 13 | `shared:request` | **550** (after the fetch-req broadcast) | reason `follower` |
| 14 | `shared:fallback` | **557** (inside the timer, when the self-fetch guard holds) | reason `follower-timeout` |
| 15 | `shared:serve` | **312** (leader branch, when `e && e.fetcher`) | reason `leader` |
| 16 | `stream:start` | StreamQuery.js **109** | after counter reset + pending |
| 17 | `stream:value` | StreamQuery.js **134** (in `onValue`, after the increment) | the only true per-frame emit |
| 18 | `stream:done` | StreamQuery.js **156** (in `onDone`) | |
| 19 | `stream:error` | StreamQuery.js **141** (reason `iterator`), **115** (factory throw, reason `open`) | |
| 20 | `mutation:start` | **1572** (after `error.set(undefined)`) | `count = gen` |
| 21 | `mutation:settle` | **1600** (immediately after the gen-guard block) | emitted unconditionally, `reason="superseded"` when `gen !== mutationGen` -- keeps the state machine total |
| 22 | `persist:hydrate` | **1025** (validation drop), **1049** (rollback drop), **1051** (ok) | |
| 23 | `persist:save` | **1733** (after `save(envelope)`), **1730** (dehydrate threw, reason `dehydrate-threw`) | emission BY the adapter, never consumption (OR-4 holds) |

### 1b. Non-sites (recorded with reasons)

| non-site | line(s) | reason |
|---|---|---|
| `notifyWrite()` body | 259-261 | the private seam stays private and untouched (OR-4); the feed reports causes, not the persist trigger |
| `disposeEntry` | 401-427 | shared by 3 paths; emitting here loses the cause (V9) |
| generation-guard returns | 628, 637, 647, 648, 650 | a superseded resolution changes no state; the matching `fetch:abort` already told the truth |
| retry backoff | 635-636 | charter lists dispatch/settle/abort only; no consumer for `fetch:retry` in 1.5.0 |
| runFetch early returns | 567, 568, 573-575 | dedup / no-fetcher / exhausted-infinite: no fetch was dispatched |
| echo-suppressed broadcast | 292 | nothing was sent; emitting would make the send/receive ledger lie |
| leader result broadcast | 683-686 | already reported by `tab:send` at 293 -- one action, one event |
| non-leader fetch-req break | 310 | nothing happened, and every tab would emit on every request |
| staleness clears | 664, 693, 980, 1003, SQ:106 | implied by `fetch:settle` / `persist:hydrate` / `stream:start` |
| stream `onAbort` | SQ:145-153 | intentional teardown; covered by `entry:detach` + the following `stream:start` |
| pre-abort inside startStream | SQ:99-102 | same |
| mutation `reset` / `dispose` | 1637-1641, 1648-1653 | no cache state; the mutation accessors are the panel's source |
| mutation callbacks | 1607-1622 | user callbacks, contained by design; their `qc` calls already emit |
| `hydrate` empty-cache throw | 1018-1022 | the call did nothing and throws out to the caller |
| `prefetch` | 845-868 | every effect it has routes through ensureEntry / runFetch sites |
| `dispose()` | 1059-1067 | `clear()` at 1060 emits N `entry:remove` + `tab:send` |

---

## 2. FROZEN EVENT VOCABULARY

One **uniform, monomorphic record**: exactly **10 own keys, always present,
in this order**. A field that does not apply is `null` (`count` is `0`, `ok`
is `false`). One hidden class for every event type -> the panel's property
accesses stay monomorphic. This is the table d.ts, llms.txt and lite-studio
consume.

| field | type | meaning / when valid |
|---|---|---|
| `type` | string | the discriminant, lower-case, `domain:verb` (table below). Always. |
| `ts` | number | `performance.now()` if available else `Date.now()`, resolved ONCE at module load. Always. NOT `opts.now` (that is the staleness clock; a mock clock must not stamp the feed). Monotonic non-decreasing within a process. |
| `key` | any[] \| null | the entry's key array **by reference, never copied**. null for client-scope (`tab:*`, `persist:*`) and `mutation:*`. |
| `keyHash` | string \| null | `entry.keyHash`; the panel's identity. null where `key` is null. |
| `from` | string \| null | prior status, `entry:status` only. |
| `to` | string \| null | new status, `entry:status` only. |
| `reason` | string \| null | abort reason (the ABORT_REASON literal), remove cause, hydrate reason code, shared role, stream phase, `tab:receive` message type, `mutation:settle` superseded. |
| `count` | number | numeric payload; `0` when N/A. observerCount, fetchGen, streamCount, hydrate record count, mutationGen. |
| `ok` | boolean | outcome flag; `false` when N/A. |
| `value` | unknown \| null | by-reference payload: fetched data, error, stream frame, cursor. Never serialized (non-goal). |

**Type table (23, frozen).** OR-8: our domain words win over lite-devtools'
connect/disconnect.

| type | key/keyHash | from/to | reason | count | ok | value |
|---|---|---|---|---|---|---|
| `entry:create` | yes | - | - | 0 | false | - |
| `entry:attach` | yes | - | - | observerCount after | false | - |
| `entry:detach` | yes | - | - | observerCount after | false | - |
| `entry:gc` | yes | - | - | 0 | false | - |
| `entry:remove` | yes | - | `remove` \| `clear` \| `hydrate-rollback` | 0 | false | - |
| `entry:status` | yes | yes/yes | - | 0 | false | - |
| `entry:stale` | yes | - | `invalidate` | 0 | false | - |
| `fetch:dispatch` | yes | - | `force` \| null | fetchGen | false | cursor \| null |
| `fetch:settle` | yes | - | null \| `commit-throw` | fetchGen | ok | data \| error |
| `fetch:abort` | yes | - | `lite-query:detach\|refetch\|removed\|timeout` | fetchGen | false | - |
| `tab:send` | - | - | msg type | 0 | postMessage ok | - |
| `tab:receive` | - | - | msg type | 0 | false | - |
| `shared:request` | yes | - | `follower` | 0 | false | - |
| `shared:fallback` | yes | - | `follower-timeout` | 0 | false | - |
| `shared:serve` | yes | - | `leader` | 0 | false | - |
| `stream:start` | yes | - | - | 0 | false | - |
| `stream:value` | yes | - | - | streamCount after | false | frame value |
| `stream:done` | yes | - | - | streamCount final | true | - |
| `stream:error` | yes | - | `open` \| `iterator` | streamCount | false | error |
| `mutation:start` | - | - | - | mutationGen | false | vars |
| `mutation:settle` | - | - | null \| `superseded` | mutationGen | !error | data \| error |
| `persist:hydrate` | - | - | null \| hydrate reason code | records seeded | ok | - |
| `persist:save` | - | - | null \| `dehydrate-threw` | entries in envelope | ok | - |

---

## 3. T3 DECISION -- recommendation

**Recommend (c): the measured hybrid -- ONE preallocated record PER TYPE,
built at install, dropped at uninstall.** `inspect(hook)` builds a 23-slot
table of pooled records in one cold loop; every emit overwrites the 10 fields
of its type's record and passes it; `uninstall()` nulls the table. Rationale:

- Beats (b) allocate-when-installed on steady state: an installed panel under
  a stream at 60 Hz allocates 0 B/frame instead of one 10-field object per
  frame; the lite-signal bar (allocation-free dispatch when registered,
  LiteSignal/llms.txt:386-387) is met, not merely approached.
- Beats plain (a) preallocated-at-construction on the off switch: an app that
  never calls `inspect()` retains **zero** feed objects; nothing is built
  until install (OR-5's "the uninstalled branch is the product" extends to
  retained bytes, not just cycles).
- Per-type (not one global record) keeps re-entrancy honest: a hook that
  calls `qc.setQueryData` inside `stream:value` cannot clobber the record it
  is still reading unless the nested event is the same type (documented).
- Cost: the hook **must copy what it keeps**. Documented loudly in README +
  llms.txt + d.ts + Cookbook 20, and pinned by a counted identity test
  (OR-6).

**Phase H measures BOTH candidates, same 200000-iteration warm body, three
runs:**
1. `absent` -- no hook. Records: GATE line (byte-compare),
   `gc.major/minor/maxMs`, heapUsed delta over the profiled window, B/op.
2. `installed-pooled` -- candidate (c), no-op copying hook
   (`(e) => { sink = e.type.length + e.count; }`). Records: the same four +
   hook invocation count.
3. `installed-fresh` -- candidate (b) simulated by a hook-side allocation
   twin (`(e) => keep = { type: e.type, ts: e.ts, keyHash: e.keyHash }`
   discarded per call), same iteration count. Records: the same four.

Recorded in bench provenance and in the CHANGELOG `[1.5.0]` head as a 3-row
table: `run | major | minor | maxMs | heapDelta bytes | B/op`. Because the
warm loop performs no writes (V7), runs 2 and 3 must additionally drive a
**write sub-loop** of 20000 `setQueryData` ops outside the frozen GATE window
(see STOP-2) to produce non-degenerate emit numbers.

---

## 4. OR-7 THROWING-HOOK RULING (proposal)

**Contain + auto-uninstall + loud, synchronous report. Fail closed toward the
FEED, never toward the cache.**

The hook is the untrusted party ("observe-only, never throw, never mutate").
Propagating would let a devtools bug abort a cache write mid-commit (e.g. a
throw at 817 between `entries.delete` and the loop's next iteration). Silent
swallowing is forbidden. Auto-uninstall alone would be silent.

Ruling: the single dispatch funnel wraps the call in `try/catch`. On a throw
it (1) nulls `feed.hook` and `feed.pool` -- the app instantly returns to the
zero-cost uninstalled path; (2) calls `console.error("lite-query: inspect
hook threw; feed uninstalled", err)` exactly once; (3) returns normally so
the in-progress cache operation completes. The previously-returned uninstall
thunk stays safe (its `=== hook` guard already fails). A later `inspect(fn)`
succeeds because the slot is free.

Rejected alternative, recorded: re-throwing out-of-band via
`queueMicrotask(() => { throw err; })` -- it converts a panel bug into an
`uncaughtException` that can kill the host process and the test runner;
louder is not fail-closed.

**Pinning test** (A8): stub `console.error`; install a hook that throws on
its first call; run `qc.setQueryData(["t"], 1)`; assert the write landed
(`getQueryData === 1`), the hook was called exactly 1 time, `console.error`
exactly 1 time, no further calls after 20 more cache ops, and a fresh
`inspect(noop)` returns a function.

---

## 5. ALGORITHMS

**A-1 The feed cell + install/uninstall (mirrors installPersistHook 266-277
exactly).**

```
// client closure, beside persistHook (Query.js ~258). ONE cell so the core and
// the /stream subpath read the same live slot (V2); a `let` cannot cross files.
const feed = { hook: null, pool: null };

function inspect(hook) {
    if (typeof hook !== "function") {
        throw new TypeError("lite-query: inspect requires a function");     // arrays land here
    }
    if (feed.hook !== null) {
        throw new Error("lite-query: an inspect hook is already installed on this client (single-slot seam)");
    }
    feed.pool = buildEventPool();      // cold: 23 records, once per install
    feed.hook = hook;
    return function uninstallInspect() {
        if (feed.hook === hook) { feed.hook = null; feed.pool = null; }     // idempotent
    };
}
```
`inspect` is added to the returned object literal (1069-1083) and `feed` to
`_internal` (1082) for the subpath. Installing/uninstalling `inspect` never
reads or writes `persistHook`, and vice versa (OR-4).

**A-2 The emit-site pattern (the null-test discipline).** Nothing is
constructed before the test -- no object, no array, no template string, no
`ts` read, no `arguments`:

```
if (feed.hook !== null) emitEntry("entry:attach", entry, entry.observerCount);
```
`emitEntry` / `emitFetch` / `emitTab` / `emitStream` / `emitClient` are five
cold closures defined once per client; each grabs its pooled record,
overwrites all 10 fields (never a partial write -- stale fields are lies),
and calls `fire()`. `fire()` is the single try/catch funnel of A-4. Status
funnel:

```
function setStatus(entry, to) {
    if (feed.hook !== null) {
        _se = entry; const from = untrack(READ_STATUS);   // hoisted module-level reader
        entry.status.set(to);
        emitStatus(entry, from, to);
        return;
    }
    entry.status.set(to);
}
```
`const READ_STATUS = () => _se.status();` is module-level and hoisted -- zero
allocation per call, the same trick as `cleanupObserver` (1199-1206);
`untrack` is already imported (31). Uninstalled cost per status write: one
call frame + one null test. Uninstalled cost on the warm read path: **zero**
(no status write occurs there, V7).

**A-3 Ordering law.** At any site co-located with `notifyWrite()`, the feed
emits **after** it. Every emit is placed **after** the state commit it
reports, except `fetch:dispatch` (which reports the start) -- so a hook
re-entering the cache always observes committed state.

**A-4 Dispatch funnel (synchronous, OR-7).**
```
function fire(ev) {
    const h = feed.hook;
    try { h(ev); }
    catch (err) { feed.hook = null; feed.pool = null;
        try { console.error("lite-query: inspect hook threw; feed uninstalled", err); } catch {} }
}
```
No queue, no microtask, no reordering.

**A-5 Fuzzer assertion mode (cache-fuzzer.mjs, env `TORTURE_FEED=1`, ON by
default under phase P).** Per-`keyHash` state record (plain object in a Map,
cold path -- the fuzzer is not a hot body):
`{ live, observers, inFlight, status, stream }` with
`stream in { none, open, done }`.

Legal transitions and violations:
- `entry:create` -> `live=true, observers=0, status="idle", stream="none"`.
  On an already-live hash: violation `create-over-live`.
- `entry:attach` -> `observers++`. `entry:detach` -> `observers--`; if it
  would go negative: **`detach-without-attach`**.
- `entry:gc` / `entry:remove` -> `live=false`. Any subsequent event for that
  hash other than `entry:create`: **`event-after-remove`**.
- `fetch:dispatch` -> if `inFlight` already true: **`dispatch-without-clear`**
  (a supersede must emit `fetch:abort` at 580 first). Else `inFlight=true`.
- `fetch:settle` / `fetch:abort` -> if `!inFlight`:
  **`settle-without-dispatch`**. Else `inFlight=false`.
- `entry:status` -> if `from !== state.status`: **`status-from-mismatch`**;
  else `state.status = to`.
- `stream:start` -> `stream="open"` (legal from any stream state: restart).
  `stream:value` when `stream !== "open"`: **`value-after-done`** /
  `value-without-start`. `stream:done`/`stream:error` -> `stream="done"`; a
  second one: `terminal-twice`.
- `tab:send` vs `tab:receive`: counted, and asserted
  `receives <= sends * (tabs-1)` -- an echo-loop shows as a strict violation
  `receive-overflow`.

Coverage gate (G4): an expected-set of the **14 fuzz-reachable** types
(`entry:create|attach|detach|gc|remove|status|stale`,
`fetch:dispatch|settle|abort`, `tab:send|receive`, `stream:start|value`;
`stream:done/error` counted but not required, since a random abort may
pre-empt them). Any expected type with count 0 -> `uncovered-event-class`,
exit 1. `shared:*`, `mutation:*`, `persist:*` are explicitly excluded because
the fuzzer sets `sharedFetch` off (V6) and issues neither -- an inclusive set
would be a lie.

Reporting: `violations[]` of `{ rule, keyHash, detail }`; print the first 10
to stderr, print `FAIL: feed state machine -- N violations`, `exit 1`. Break
hook: `QUERY_TORTURE_BREAK=feed` skips exactly one `entry:attach` bookkeeping
update, producing a guaranteed `detach-without-attach` -> the new **C-feed**
control.

---

## 6. C-LADDER

- **C1 -- seam.** `feed` cell, `inspect()`, `buildEventPool()`, `fire()`, the
  5 emit helpers, `_internal.feed`. Zero emit sites.
  `Query.js:queryClient/inspect`, `Query.js:queryClient/fire`.
- **C2 -- entry lifecycle + status funnel.** Sites 1-7 incl. the 11 core
  `setStatus` conversions. `Query.js:ensureEntryByHash`, `attach`, `detach`,
  `scheduleGc`, `removeQueries`, `clear`, `hydrate`, `invalidate`,
  `setStatus`.
- **C3 -- fetch + cross-tab + shared.** Sites 8-15. `Query.js:runFetch`,
  `broadcast`, `onRemoteMessage`, `requestSharedFetch`.
- **C4 -- stream.** Sites 16-19 + the 5 stream `setStatus` conversions,
  through `_internal.feed` / `_internal.setStatus`.
  `StreamQuery.js:streamQuery/startStream`.
- **C5 -- mutation + persist.** Sites 20-23. `Query.js:mutation`,
  `Query.js:persistQueryClient/doSave`. Prereq **C6.0**: grep `test/` for any
  `mutation(` call whose first arg is not a real client (V8); if one exists,
  the `_internal` read is guarded `qc && qc._internal ? ... : null`.
- **C6 -- fuzzer assertion mode + C-feed control.**
  `bench/torture/cache-fuzzer.mjs`, `test/torture/controls.mjs:ctlFeed`,
  `runControls` id list.
- **C7 -- torture phase H double-run + bench provenance. Carries OR-10
  attempt D.** `test/torture.mjs:runPhaseH`. Attempt D: 4096 cycles of
  `inspect(hookClosure)` / uninstall where `hookClosure` is
  `tracker.track`ed with `{ audit: true }` **outside any owner** and carried
  across the install/uninstall boundary while the client's owner tree is torn
  down; honest pass-or-fail, recorded verbatim in INCONCLUSIVE.md as Attempt
  D beside A/B/C. If it does not fire, it is re-recorded and carried -- never
  faked, never added as a ctl that cannot fire.
- **C8 -- docs, last.** README "Devtools feed" section + lite-studio pointer
  (no panel promises), llms.txt (scope line flip + the full vocabulary table
  + the copy contract), Query.d.ts (`inspect` signature + `QueryFeedEvent` +
  the type union), Cookbook recipe 20 (console logger in 10 lines),
  CHANGELOG `[1.5.0]` head with every OR decision, the T3 3-row measured
  table, and the four **Fixed** lines for 033e670 (OR-2). No version stamp
  (OR-1).

---

## 7. FALSIFIABLE ASSERTIONS (qa verifies verbatim)

1. `node --expose-gc test/torture.mjs` prints exactly `GATE leak=size 0/0
   findings=0 warnings=0 | gc major=0 minor=0 maxMs=0.00 | ok` then `ok`,
   exit 0 -- byte-identical to the 1.4.0 line.
2. **GC budget:** phase H run 2 (hook installed, no-op copying hook, 200000
   warm iterations + 20000-write sub-loop) reports `checkNoGc` verdict `pass`
   under `{ maxMajor: 0, maxPauseMs: 4 }`, `gc.major === 0`, and
   `heapDelta / 220000 <= 1.0` bytes-per-op. Run 1 (absent) reports
   `heapDelta / 220000 === 0.0` to two decimals. Both numbers printed in
   bench provenance.
3. **Retention:** over 4096 install/uninstall cycles (attempt D),
   `tracker.size()` returns to **0** and the 256-sample WeakRef census is not
   256/256 live.
4. Fuzzer assertion mode: **0** violations over 2 runs (`TORTURE_SECONDS=5`,
   seeds `0x1234567` and `0x7654321`), and all **14** expected event types
   have count `>= 1`.
5. `QUERY_TORTURE_BREAK=feed` trips: child exit 1 containing `FAIL: feed
   state machine` and `detach-without-attach`. The four existing controls
   (`alloc`, `detach`, `fuzz`, `pages`) still print `CONTROL <id> tripped`,
   summary `tripped=5/5` under `QUERY_TORTURE_BREAK=1`.
6. Suite: `npm test` reports `>= 298` pass, **0** fail, **0** skip.
   `git diff --stat` over the 268 pre-existing test files is empty (OR-9).
7. Identity (OR-6): two sequential `entry:status` events satisfy
   `Object.is(a, b) === true`; `Object.keys(a).length === 10` on both; a
   manual copy taken after event 1 is unchanged after event 2; two events of
   **different** types are never identical.
8. Throwing hook: cache write completes, hook called exactly **1** time,
   stubbed `console.error` called exactly **1** time, **0** further hook
   calls over 20 subsequent ops, re-install returns a function.
9. Zero emission when uninstalled: install a counting hook, run 100 ops
   (`n1 > 0`), uninstall, run 1000 mixed ops -> counter still `=== n1`.
10. Two seams (OR-4): with both a persister and an inspect hook installed,
    `uninstallInspect()` leaves `save` firing (`>= 1` call after a write);
    `handle.stop()` leaves the feed firing (`>= 1` event after stop);
    `inspect()` twice throws `Error`, and `null` / `undefined` / `[]` / `{}`
    each throw `TypeError` (4 counted cases).
11. All 23 frozen types fire with the documented 10-key shape under counted
    tests (G5).

---

## 8. TEST PLAN (268 -> 310)

New files only (OR-9 forbids editing existing tests).

- **`test/inspect.test.js` -- 26 tests.** Install/uninstall/idempotence/
  double-install/4 rejections (8); entry lifecycle create/attach/detach/gc/
  remove x3 causes (7); status `from`/`to` + `entry:stale` (3); fetch
  dispatch/settle/abort x4 reasons (4); tab send/receive + shared
  request/serve/fallback (2 -- shared covered in the cross-tab section, 2
  tests); mutation start/settle+superseded (2).
- **`test/inspect-shape.test.js` -- 6 tests.** 10-key monomorphic shape; `ts`
  type + monotonicity; reuse identity (OR-6); different-type non-identity;
  `key` is by-reference; uninstalled emits nothing (A9).
- **`test/inspect-stream.test.js` -- 4 tests.** `stream:start`/`value`/
  `done`, `stream:error` both phases (open + iterator).
- **`test/inspect-seams.test.js` -- 6 tests.** Two-seam independence x2
  (A10), throwing hook (A8), persist:hydrate ok + dropped, persist:save.

Total **+42 -> 310**; a conservative floor of **>= 298** is asserted so a
merged/split test does not miss the gate. `tests_min 278` cleared with
margin; `skip_max 0` (no `{ skip: true }`, no `test:gc`-only tests).
llms.txt's count line is updated to the QA-frozen final number in C8.

---

## 9. STOP ITEMS (operator ruling required; do not design around)

- **STOP-1 -- `qc._hook` is unimplementable as written (V2).** BRIEF T1 /
  charter line 896 specify `if (qc._hook !== null)`. There is no `qc` binding
  inside `queryClient` (1069-1083), and `StreamQuery.js:87` destructures
  `_internal` once, so a core `let` can never be read live by the subpath.
  Proposal: a per-client cell `const feed = { hook: null, pool: null }` (one
  cold allocation per client, no per-entry slot), tested as
  `feed.hook !== null` at every site in both files. This is a literal
  deviation from the charter's spelled form and needs the operator's word
  before C1.
- **STOP-2 -- the byte-frozen GATE cannot prove the stream frame path (V7).**
  OR-5/G3 demand a byte-identical GATE **and** "zero added allocation" proof;
  but phase H's profiled window (torture.mjs:271-279) performs zero status
  writes and zero stream frame writes, so the hardest emit site
  (`stream:value`, StreamQuery.js:134, on a path llms.txt:105 documents as
  "one signal write per frame, ZERO allocation") is measured by nothing.
  Adding pushes/writes **inside** the profiled window can move `maxMs` and
  break byte-identity. Ruling needed: (a) keep the GATE window frozen and
  prove the write/frame paths in a **separate** provenance loop outside it
  (planner's preference), or (b) extend the window and re-freeze a new GATE
  line (contradicts "byte-frozen"). Everything in C7 depends on this answer.
- **STOP-3 -- G2's "plus any honest Q7 addition per OR-10" is ambiguous
  against three failed attempts.** controls.mjs:22-27 records that the
  findings clause has no control after A/B/C. If attempt D again does not
  fire, G2 must not be read as requiring a fifth *tripping* control.
  Proposal: the honest Q7 addition that gates is **C-feed** (the fuzzer
  state-machine control, guaranteed to trip); attempt D is recorded
  pass-or-fail in INCONCLUSIVE.md and is explicitly **not** a gate clause.
  Confirm before C7, or the ladder stalls on an unfirable control.

**Recorded, not STOP:** ROADMAP line 915's "Suite >= 200" is stale against
278 (V10); OR-3 vs OR-4 uninstall semantics resolved in favour of OR-4
(V11); `mutation()`'s unvalidated `qc` is a coder pre-check, not a
contradiction (V8, task C6.0).
