# PLAN -- Q8 -- lite-query v2.0.0 -- shared streams (ratified)

Operator ratification, 2026-09-02. The planner spec below is verbatim; this
header resolves STOP-DECISION-1 and STOP-1..STOP-6 and binds the coder.
BRIEF.md OR-1..OR-11 remain in force; where this header speaks, it rules.

- ON-1 (STOP-DECISION-1): SPLIT, ratified. 2.0.0 = shared streams alone;
  the offline mutation queue defers to 2.1.0 as a named ROADMAP brief with
  its full OR-6 semantics carried intact and G6 moving with it -- recorded
  explicitly, never implied. The charter's lean held: the queue does not
  ride an unfinished failover story. tests_min 354 stands (G1); the plan's
  44 new tests plus ON-3's rung target ~362.
- ON-2 (spike verdicts + STOP-1): all four rulings RATIFIED -- push-writer
  -> lite-stream (LS4 shrinking to createSignalWriter alone is the right
  1.4.0; the law-4 falsification of their hand-made-queue escape hatch is
  the recorded reason); idleTimeout -> in-package watchdog here (their
  design note implemented in the domain that can see the failure;
  standalone lite-stream version stays parked their side); share/refcount
  -> struck; sequencing -> Q8 proceeds in PARALLEL behind the single
  projectFrame seam, semantics pinned by the differential parity test
  whose oracle is lite-stream's live pipeToSignal -- the test is the
  contract and outlives both bodies; LS4's writer swaps in at a later
  minor with the test unchanged. STOP-1's corrected transport wording is
  RATIFIED and is the ONLY wording either roadmap may carry: "followers
  receive frames as BroadcastChannel messages over the existing crossTab
  channel; lite-channel supplies only the caller-wired isLeader oracle."
  The parity assertion + 12-case corpus go to lite-stream via the live
  peer session BEFORE LS4 codes (G9; operator relays).
- ON-3 (STOP-2): FIX IT AT THE MAJOR. The falsy-rejection quirk
  (Query.js ~1817 `if (resolvedError)`: a mutation rejecting with
  null/0/""/undefined settles "success", feed ok true) violates suite law
  ("fail closed on every unverified state; null is not zero") and 2.0 is
  the only major on the ladder. mutation() now tracks rejection by
  CONTROL FLOW, not truthiness: any rejection settles status "error", the
  error signal holds the rejection verbatim (falsy values included), feed
  `ok` = not-rejected. BREAKING entry + migration note ("branch on
  status(), never on error() truthiness"); the llms.txt/d.ts quirk
  clauses are replaced by the fixed contract. New rung C10b with >= 4
  counted tests (null / 0 / "" / undefined rejections; superseded
  interplay). If any of the 314 pins the quirk, it is retired BY NAME per
  OR-9 (expected: none -- report either way).
- ON-4 (STOP-3/4/5/6): llms.txt's stale count lines (207, 219) reconciled
  in C13; the coder verifies package.json files[] against the 13-file
  expectation before C1 (STOP-4); the charter's stale "Suite >= 230" is
  corrected in the closeout docs commit only (STOP-5); STOP-6 binds --
  stream-frame carries ONE key per message, and any cross-key batching
  proposal is a spec decision requiring operator ratification, not an
  optimization.
- ON-5 (design ratifications): V1's `startStream(entry, { adopt })`
  preservation path; V2's deferred promotion announce (one queueMicrotask
  per promotion, cold -- a cross-tab control-message scheduling decision;
  the feed's synchronous-dispatch law is untouched and the processingRemote
  echo guard is NOT weakened); the deliberate no-`stream:frame-send` trim
  (tab:send + stream:value already tell the truth; no 60Hz double work);
  the additive 23 -> 26 feed vocabulary on the frozen 10-key record; G5 as
  PROHIBITION (the leader keeps zero per-follower state -- no replay
  buffer, no acks, no cursors, ever); the epoch/seq/clientId law exactly
  as specified (monotonic integers + one cold ASCII clientId; never
  derived from opts.now -- a mock clock must not decide ownership).

Pipeline: coder implements C1..C13 (with C10b inserted) in order, one
commit per rung, suite + guards green at every rung, the frozen GATE
byte-identical throughout; reviewer audits the full diff before qa; QA
count-freeze applies (docs print the frozen final count in C13).

---

# Q8 PLANNER SPEC (verbatim)

# (a) SPIKE VERDICT -- T0

## Facts 1-3 against the real code

**Fact 1 -- CONFIRMED IN SUBSTANCE, FALSIFIED IN NAMING.** Followers receive
push messages and hold no iterator: the only iterator site in the package is
`StreamQuery.js:131` (`pipeToSignal`), reached only from `startStream`; the
follower fetch analogue never calls a fetcher (`Query.js:729-750
requestSharedFetch` broadcasts + arms a timer). But the transport is **not
lite-channel** -- it is `opts.broadcastChannel` (`Query.js:111-112`,
constructed `Query.js:447-450`). lite-channel appears only as prose in the
`isLeader` comment (`Query.js:103-106`). Recording "lite-channel messages" in
both roadmaps invites a coder to import it and break zero-runtime-deps.
**Corrected wording: "followers receive frames as BroadcastChannel messages
over the existing crossTab channel; lite-channel supplies only the
caller-wired `isLeader` oracle."**

**Fact 2 -- CONFIRMED.** `startStream` (`StreamQuery.js:97`) unconditionally
builds a fresh `AbortController` (`:114`) and calls `streamOpts.stream(...)`
fresh (`:117`); `entry.streamRestart = () => startStream(entry)` (`:177`).
There is no code path that can adopt a foreign iterator; adoption is not
merely forbidden, it is unexpressible in the current shape.

**Fact 3 -- CONFIRMED.** `shouldStartStream` (`StreamQuery.js:183-189`), line
`:184` `if (entry.streamStop !== null) return false;` with the comment
"already streaming (shared observer)". In-process sharing exists; cross-tab
sharing is the channel broadcast.

## Candidate rulings

**push-writer -> lite-stream. SURVIVES. Verdict upheld and its evidence
UPGRADED (not a matrix-cell overturn).** LS4's own menu offers an escape
hatch -- "possibly just documenting the enriched pipeToSignal over a
hand-made async queue" (`LiteStream/ROADMAP.md:566-568`). That escape hatch
is **falsified here on law 4, not on taste**: a hand-made async queue feeding
`pipeToSignal` allocates one promise plus one resolver **per frame** on the
follower's warm path. At 60Hz across N tabs that is exactly the young-gen
pressure `Query.js:1410-1417` was written to eliminate. So the push-writer is
not a convenience for avoiding a third copy; it is the only zero-alloc
expression of follower projection. `createSignalWriter` is therefore
**required in lite-stream**, and LS4 shrinking to it alone is the right
1.4.0.

**idleTimeout -> lite-query (NOT structurally required in lite-stream).
Verdict upheld.** The cell that could have resurrected it is **F3 leader
hung (frames stop, channel alive)**, and it does not: the detector must run
on **channel** messages, and by hypothesis the stalled party is the leader's
own iterator -- the party least able to act. lite-stream cannot observe
channel traffic (fact 1), so an `idleTimeout` on `pipeToSignal` would arm on
the wrong side of the failure. Our watchdog adopts their recorded design
note verbatim, in-package: **never per-value `clearTimeout`/`setTimeout`;
`lastFrameAt` plus one periodic check-and-rearm timer, amortized O(1)**
(`ROADMAP.md:1048-1050`). Stays parked on their side pending a standalone
SSE consumer.

**share/refcount -> NOWHERE. STRUCK.** No cell in F1-F7 produces
one-iterator -> N-local-consumers: every cell is cross-tab, and fact 2
forbids adoption, so a shared in-process iterator never exists. In-process
sharing is already `shouldStartStream`. N-keys-over-one-socket is struck by
OR-11.

## SEQUENCING RULING

**Q8 proceeds in PARALLEL with the parity pinned on our side, structured so
no third copy is created.** Blocking on LS4 strands the last session of the
ladder; a naive hand-roll earns the anti-third-copy objection. The
resolution: the follower window lands behind exactly one internal seam,
`projectFrame(entry, v)` (~15 lines, C5 below), and its semantics are pinned
by a **differential test whose oracle is lite-stream's own `pipeToSignal`
running live in our suite** -- not a re-description of the window in prose or
in a second implementation. When LS4 ships `createSignalWriter`, the seam
body is swapped for the writer in a 2.1 minor and **the parity test does not
change**. The test is the contract and outlives both bodies. Admissible
under OR-5's stranding clause; the same corpus is handed to LS4 before it
codes so their s7 pins it verbatim.

Version floors, when they exist, get cited from lite-stream's own llms.txt
consumer lines (`llms.txt:191` today reads `^1.3.0`), never from session
numbers.

## Parity assertion handed to lite-stream (EXACT text for LS4 s7)

> For any finite value sequence `V = [v0..vn-1]` and any integer `m >= 1`: a
> target signal driven by `createSignalWriter(target, { mode: "buffer",
> maxBuffer: m })` with `writer.push(v)` called once per element of `V` in
> order must leave `target()` element-for-element reference-identical to the
> array left by `pipeToSignal(asyncIterableOf(V), target2, { mode: "buffer",
> maxBuffer: m })` after its `onDone`, and `writer.droppedCount` must `===`
> that pipe's stop-fn `droppedCount`. Required for every `m` in
> `{1, 2, 3, 7, 64}` crossed with every `|V|` in `{0, 1, m-1, m, m+1, 4m}`.
> The window is drop-oldest, newest-last; each `push` publishes a FRESH
> snapshot array (`target()` after push i is never the same reference as
> after push i+1). In `mode: "latest"` a push is one signal write and
> allocates zero bytes. Element identity is by reference -- values are never
> copied, compared, or serialized.

## Frame-shape corpus outline (12 cases, hand to LS4 with the above)

1. empty sequence then `end()` -> target unwritten, `droppedCount` 0.
2. `|V| < m` -> no drop, full array.
3. `|V| === m` -> no drop, exact window.
4. `|V| === m + 1` -> `droppedCount` 1, oldest element absent.
5. `|V| === 4m` -> `droppedCount === 3m`.
6. `m === 1` -> `droppedCount === |V| - 1`.
7. `push` after `end()` -> ignored, `droppedCount` frozen (fail closed).
8. `error(e)` then `push` -> ignored, error state terminal.
9. `undefined` and `null` are legal frame values (a frame value is opaque).
10. same-reference value pushed twice -> two distinct window slots (no
    `Object.is` dedup in buffer mode).
11. 100k pushes, `mode: "latest"` -> `maxMajor 0`, `< 1.0 B/op`.
12. the returned handle stays call-compatible with LS3's stop-fn shape
    (`LiteStream/ROADMAP.md:499-501` compat constraint) -- additive
    properties only.

---

# (b) STOP-DECISION-1

**RECOMMENDATION: split. 2.0.0 = shared streams. 2.1.0 = offline mutation
queue, deferral recorded explicitly (not implied).**

Rationale grounded in the matrix's real size against observable session
history:

- **Q6 consumed a full session to ship ONE seam** (persist:
  `dehydrate`/`hydrate`/adapter, `Query.js:1087-1264` + `1917-2061`).
  **Q7 consumed a full session to ship ONE seam** (feed: one hook, one pool,
  36 emit sites, `Query.js:126-445`). Shared streams is strictly larger than
  either: it is a **new wire protocol** (4 message types), a **new state
  machine** (project / promote / abdicate), a **watchdog**, a **window
  implementation**, plus **7 named failover cells**.
- The test substrate does not exist yet and must be built inside the
  session. `harness.js` has no leader oracle, no connection counter, and no
  N-tab helper. `bench/torture/cache-fuzzer.mjs` is **hard-wired to exactly
  two tabs** -- `:44`, `:180 const tabs = [tabA, tabB]`, `:244
  tabs[randInt(2)]`, `:398 tabs.length - 1`. G4's 5-tab soak requires
  generalizing it to N, which is a rung of its own.
- The queue is a **third independent seam**: durability over the Q6 persist
  substrate, per-key replay ordering, per-item result surfacing,
  drop-on-missing-entry, plus G6's airplane-mode script. It has no
  dependency on the failover matrix, so deferring it costs nothing
  architecturally.
- The charter's own lean is decisive and I concur without qualification:
  **"Do not let the queue ride an unfinished failover story."** A queue that
  replays through a leader whose promotion semantics are still being
  debugged is exactly the fail-open surface OR-6 exists to prevent.

`tests_min: 354` and `skip_max: 0` stay unchanged -- shared streams alone
reaches +40 (see the test plan). The `[2.0.0]` BREAKING section states
truthfully that the major rides the new cross-tab stream semantics; the
queue's deferral is recorded in ROADMAP section 5 as a named Q9/2.1 brief,
with G6 moved with it.

---

# (c) FULL SPEC -- 2.0.0 shared streams

## V-findings (numbered, file:line)

**V1 (BLOCKING, the highest-value spike output).** `startStream` is
destructive on entry and therefore **cannot serve promotion**.
`StreamQuery.js:107-111` resets `streamCount = 0`, `streamDropped = 0`,
clears `error`, and calls `setStatus(entry, "pending")`. A follower promoted
mid-buffer (cell F4) that routes through `startStream` **wipes its
already-projected window and flips a `streaming` entry back to `pending`** --
visible frame loss beyond what OR-4 permits, plus a status regression. Fix:
`startStream(entry, { adopt })`; when `adopt` is true, preserve
`streamCount`/`streamDropped`/`data`, skip the `pending` transition, and keep
`error` untouched. Every counter reset stays on the non-adopt path so the
existing 24 stream tests are unaffected.

**V2 (BLOCKING).** `broadcast` returns early while `processingRemote` is
true (`Query.js:459-460`). Any Q8 control message emitted from **inside**
`onRemoteMessage` (`:466-493`) is silently swallowed. The promotion announce
in cells F2/F3/F5 is triggered by a received message in at least one path.
Fix: promotion announces are deferred out of the handler (one
`queueMicrotask`, cold, once per promotion -- never per frame), or a named
`broadcastControl` that bypasses the echo guard with its own loop-safety
argument. Do **not** weaken the `processingRemote` guard itself; it is what
stops the echo storm the fuzzer asserts at `cache-fuzzer.mjs:397-398`.

**V3.** `sharedFetchActive` is computed once (`Query.js:454-455`) and
exported through `_internal` as a **value** (`Query.js:1301`);
`StreamQuery.js:90` destructures only `{ ensureEntry, attach, detach, opts,
feed, setStatus, emitStream }`. The `/stream` subpath currently cannot see
leader state at all. `_internal` must grow `sharedStreamActive`, `broadcast`,
and the frame-projection entry points. Additive, no existing key changes.

**V4 (favourable).** `droppedCount()` already has exactly the two-branch
fallback a follower needs: `StreamQuery.js:302-310` reads
`s.raw.droppedCount` while a pump is live and falls through to
`e.streamDropped` otherwise. A follower has no pump (`streamStop === null`),
so it reads `e.streamDropped` -- **the public accessor needs no change**, the
projection path just maintains that field. Pin this as a test so a future
refactor cannot quietly break it.

**V5.** `detach` (`Query.js:684-690`) and `disposeEntry` (`Query.js:579-583`)
tear down via `entry.streamStop`. A follower has none, so **projection state
would survive detach and GC-schedule** with its window and epoch cursors
intact. New per-entry projection slots must be released at both sites, or
the OR-10 attempt-E retention class is created by construction rather than
discovered.

**V6.** `disposeEntry` releases stream slots but the entry-shape comment at
`Query.js:513-517` promises uniform monomorphic slots. All new projection
state must be declared in `createEntry` (`Query.js:497-544`) at null/false/0
defaults, exactly as the stream and infinite slots were, or plain query
entries get a second hidden class at the hot attach/detach/GC sites.

**V7.** `dehydrate` skips stream entries (`Query.js:1091 if (e.isStream)
continue;`). A projected follower entry is `isStream` (set at
`StreamQuery.js:224`), so it is correctly excluded from persistence with no
change. Confirm by test; do not "improve" it.

## The failover matrix -- 7 named cells, per-cell mechanism

| # | Named test | Mechanism against real machinery |
|---|---|---|
| **F1** | `failover: leader closes gracefully -- exactly one follower promotes, connections stay 1` | Leader `qc.dispose()` (`Query.js:1271-1279`) -> `clear()` -> `disposeEntry` -> `streamStop` (`:579-583`). Add a `stream-end{reason:"closing"}` broadcast **before** `streamStop`, emitted from the dispose path (not from a remote handler, so V2 does not bite). Followers run the promotion race on receipt. |
| **F2** | `failover: leader tab killed -- watchdog promotes within streamIdleTimeout, no dup` | Simulate via `MockBC.close()` (`harness.js:181-187`) with no `stream-end`. No message ever arrives; the **only** detector is the watchdog. Followers' `lastFrameAt` goes stale, the periodic rearm timer fires, promotion race runs. Frame gap counted, zero duplicates. |
| **F3** | `failover: leader hung -- follower self-connects, old leader abdicates on higher epoch` | Leader stops yielding, channel alive. Same watchdog path as F2, but two iterators would briefly exist. The promoter broadcasts `stream-open{epochSeq}` strictly greater than the one it last saw; a tab that owns the key at a **lower** epochSeq calls `streamStop` and reverts to projecting. If the hung leader never processes the message, correctness is untouched -- it just wastes one connection, and the connection-count assertion is scoped to "after messages drain". **This is the cell that proves OR-3.** |
| **F4** | `failover: follower promoted mid-buffer -- window and counters survive` | Buffer mode, `k < maxBuffer` projected values, then promote. Requires **V1's adopt path**. Asserts `data()` array unchanged across the promotion instant, `count()` continues from k (never resets to 0), `droppedCount()` monotonic, status never returns to `pending`. |
| **F5** | `failover: two tabs racing promotion -- exactly one connection survives` | Both watchdogs fire in one drain. Tiebreak is **election-state-independent**: highest `(epochSeq, clientId)` lexicographic pair wins; a loser that already opened aborts via `streamStop` and reverts to projecting. Followers gate on `(epochSeq, clientId)`, not on arrival order, so the loser's in-flight frames are discarded rather than interleaved. Asserts connection count `=== 1`, zero duplicated frames in all 5 tabs. |
| **F6** | `failover: stream completes during failover -- success in every tab, no dup` | Leader `onDone` (`StreamQuery.js:161-168`) races a promotion. `stream-end{ok:true, epochSeq}` is terminal for that key at that epoch **or higher**; a promoter receiving it after opening calls `streamStop` and settles `success`. A `stream-end` for a strictly lower epochSeq is ignored. Asserts final status `success` in all 5 tabs, `<= 1` connection opened after promotion. |
| **F7** | `failover: stream errors during failover -- error surfaces then recovery, no inherited wedge` | Leader `onError` (`StreamQuery.js:142-151`) broadcasts `stream-end{ok:false, error}`. Followers project the error into `entry.error` and status `error` **and simultaneously arm the watchdog** -- a follower must never be wedged by a dead leader's failure, because correctness cannot depend on election state. If the promoter's fresh connect succeeds, status returns to `streaming`. Asserts the error is observable, then recovery, with no duplicated frames across the transition. |

**OR-4 invariant asserted in every cell and in the soak:** duplication zero,
reordering zero, loss permitted and *counted*.

## Frame wire shape (additive; 4 new message types)

The `onRemoteMessage` switch (`Query.js:471-489`) grows four cases beside
the existing five. All are inert unless `sharedStreamActive`.

- `{ type: "stream-open", key, epochSeq, clientId }` -- ownership claim.
- `{ type: "stream-frame", key, epochSeq, clientId, seq, value }` -- one per
  frame; the hot message.
- `{ type: "stream-end", key, epochSeq, clientId, ok, error }` -- done or
  error, terminal for `<= epochSeq`.
- `{ type: "stream-req", key }` -- follower asks for an owner; mirrors
  `fetch-req` (`Query.js:476-488`) including the `isLeader()` gate.

Ordering and dedup law (OR-4), enforced in `projectFrame` before any signal
write:
- `epochSeq < entry.projEpoch` -> **drop** (a dead leader's late frame).
- `epochSeq === entry.projEpoch && seq <= entry.projSeq` -> **drop**
  (duplicate).
- `seq > entry.projSeq + 1` -> **accept and count the gap** (at-most-once
  loss, documented).
- `epochSeq > entry.projEpoch` -> adopt the new epoch, reset `projSeq`,
  count the boundary as a gap.

`epochSeq` is a plain monotonic integer carried forward as `max(seen) + 1`
by whoever opens; `clientId` is one ASCII string built **once per client** at
construction (cold). Neither is derived from `opts.now` -- a mock clock must
never decide ownership.

## New feed event types (23 -> 26; the 10-key record is frozen and unchanged)

Appended to `FEED_TYPES` (`Query.js:143-152`) in vocabulary order;
`buildEventPool` (`Query.js:158-168`) picks them up with no shape change.

| type | key/keyHash | from/to | reason | count | ok | value |
|---|---|---|---|---|---|---|
| `stream:project` | yes | - | - | seq applied | false | frame value |
| `stream:promote` | yes | prior epoch / new epoch | `leader-closed` \| `leader-hung` \| `leader-killed` \| `race-won` \| `race-lost` \| `abdicate` | epochSeq | won | - |
| `stream:gap` | yes | - | `gap` \| `epoch-change` | frames missed | false | - |

**No `stream:frame-send` type.** The leader's broadcast is already covered
honestly by `tab:send` (`Query.js:463`, `reason = msg.type =
"stream-frame"`) and its local set already emits `stream:value`
(`StreamQuery.js:140`). Adding a fourth type would double per-frame feed
work at 60Hz for zero new truth. This is a deliberate trim, recorded.

## Leader-election reuse design (OR-3 extended verbatim)

- New options in `resolveOptions` (`Query.js:91-114`): `sharedStream`
  (default `false`), `streamIdleTimeout` (default `opts.sharedFetchTimeout`,
  i.e. 3000). No new oracle -- **the same `opts.isLeader`**.
- `sharedStreamActive = opts.sharedStream && typeof opts.isLeader ===
  "function" && !!channel`, computed once beside `sharedFetchActive`
  (`Query.js:454-455`). Inert otherwise: every tab owns its connection,
  which is exactly 1.1.0's shipped behaviour.
- **Watchdog:** `entry.lastFrameAt` is a number write per frame (zero alloc,
  no timer churn) plus **one periodic check-and-rearm timer per projected
  entry**, unref-guarded like `Query.js:626-628` and `:747-749`. This is
  lite-stream's own recorded `idleTimeout` design note
  (`ROADMAP.md:1048-1050`) implemented in the domain that can actually see
  the failure.
- **OR-3's sentence must remain true verbatim.** `llms.txt:180` verified
  today, byte-for-byte: *"Liveness: if no leader serves within
  sharedFetchTimeout, the follower self-fetches. Correctness never depends
  on election state."* Extended sentence to land in the docs rung:
  *"Liveness: if no frame arrives within streamIdleTimeout, the follower
  self-connects. Correctness never depends on election state."* Both
  sentences ship; both are asserted by a test (F3 and the existing
  shared-fetch tests respectively).
- A design whose correctness depends on election state is rejected by
  definition. Concretely: **no cell may assert "the leader is unique"** --
  every cell asserts only "connection count converges to 1 after messages
  drain", which is true even when the oracle lies.

## Slow-follower bound design (G5)

The strong form, and it falls out of the existing machinery rather than
being added: **the leader keeps exactly zero per-follower state.**
`broadcast` is fire-and-forget `channel.postMessage` (`Query.js:462`) with
no queue, no ack, no retry. The ruling is therefore a **prohibition, not a
mechanism**: no replay buffer, no ack protocol, no per-follower cursor may
be introduced. A slow follower bounds *itself* through its own `maxBuffer`
window and counts its own drops -- which is precisely why follower and local
windows must be parity-identical.

G5 asserts: leader retained bytes with **1 stalled follower** vs **4 stalled
followers** differ by `< 1 KB` after 50k frames, and the leader's `entries`
map size is identical in both runs.

## Offline mutation queue -- EXPLICIT DEFERRAL RECORD (OR-6)

Deferred to **2.1.0**, not implied. Recorded verbatim in ROADMAP section 5
as its own brief and in the `[2.0.0]` CHANGELOG under a "Deferred" heading.
Carried forward intact: explicit opt-in **per mutation** (a silent default
is fail-open and forbidden); durability over the Q6 persist seam with the
same version-stamp discipline; replay order-per-key, per-item observable,
and **drops** items whose entries no longer exist (a drop is a surfaced
result, never a silent retry); replay observable via the feed. **G6 moves
with it.** `mutation()` (`Query.js:1771-1890`) is untouched in 2.0
[operator note: except ON-3's falsy-rejection breaking fix].

## C-ladder (atomic committable rungs; docs last)

1. **C1** -- `Query.js:resolveOptions` + `sharedStreamActive` +
   `createEntry` projection slots (null/false/0, V6) + `_internal` growth
   (V3). No behaviour change; 314 green.
2. **C2** -- `Query.js:onRemoteMessage` four new cases, inert behind
   `sharedStreamActive`; the deferred-announce fix for **V2**.
3. **C3** -- `StreamQuery.js:startStream` leader path: seq counter, frame
   broadcast from the existing `onValue` tap (`:137-141`), `stream-open` /
   `stream-end`.
4. **C4** -- `Query.js:projectFrame` follower path, **latest mode only**:
   epoch/seq gate, `entry.streamDropped` maintenance (V4), watchdog stamp.
5. **C5** -- `Query.js:projectFrame` buffer window + the differential parity
   test against a live `pipeToSignal` oracle.
6. **C6** -- watchdog timer + self-connect. OR-3 extended to streams.
7. **C7** -- promotion, race tiebreak, abdication, and
   `startStream(entry, { adopt })` (**V1**).
8. **C8** -- teardown release of projection slots at `detach` and
   `disposeEntry` (**V5**).
9. **C9** -- the 7 matrix cells as named tests (F1-F7).
10. **C10** -- 3 feed types + `inspect` tests + zero-cost-off proof.
11. **C10b (ON-3)** -- the falsy-rejection breaking fix in `mutation()`:
    rejection tracked by control flow; >= 4 counted tests; BREAKING +
    migration text staged for C13.
12. **C11** -- torture: generalize `cache-fuzzer.mjs` from 2 tabs to N, add
    the 5-tab soak and the leader-failover soak, **printing strictly after
    the frozen GATE line** (Q7 ON-2 precedent, `torture.mjs:307-309`,
    `:506-508`).
13. **C12 -- the OR-10 attempt-E rung: `leader teardown with live follower
    projection buffers`.** The honest control is a projection slot
    deliberately retained past `disposeEntry`; if it cannot be made to trip,
    it is recorded as inconclusive in `INCONCLUSIVE.md` in the
    verbatim-attempt style. A control that cannot trip is decorative -- it
    is never faked.
14. **C13 (docs last)** -- `llms.txt` (extended liveness sentence, 26-type
    table, new options, new semantics, stale-count reconciliation per
    STOP-3), `StreamQuery.d.ts` + `Query.d.ts`, README, Cookbook,
    `CHANGELOG [2.0.0]` with BREAKING + migration, LS4 verdict recorded in
    **both** roadmaps, and the STOP-4 pack verification.

## Numbered falsifiable assertions

1. **A1 (OR-4 duplication).** Across the 5-tab soak, 50,000 frames, leader
   killed every 500 ops: duplicated frames observed in any follower `=== 0`,
   and reordered frames `=== 0`. Loss is permitted and reported as a number;
   a run reporting loss `> 0` passes, a run reporting duplication `>= 1`
   fails.
2. **A2 (G4 connection count).** The mock source's live-connection counter
   reads exactly `1` at every drain boundary and never exceeds `2`
   transiently (the F3/F5 overlap window), across at least 100 leader kills.
3. **A3 (GC budget, hot body).** Follower projection in latest mode over
   100,000 frames: `maxMajor === 0`, `maxPauseMs <= 4`, and `< 1.00 B/op` --
   the same profile the frozen GATE line reports (`torture.mjs:398-422`
   provenance discipline). Buffer mode gets its own named budget of one
   snapshot array per frame; the global `maxMajor: 0` rule never widens.
4. **A4 (retention).** After 200 promote/abdicate cycles across 5 tabs, in
   **every** tab: `lite-leak tracker.size() === 0`, `entries.size === 0`,
   `clock.pendingCount === 0`, and the lite-signal pool is at baseline. Run
   twice; both runs must return to 0, so a single-cycle-late release cannot
   pass.
5. **A5 (G5 slow-follower bound).** Leader retained bytes after 50,000
   frames differ by `< 1024` between a 1-stalled-follower run and a
   4-stalled-follower run; leader `entries.size` identical. Falsified by any
   per-follower allocation.
6. **A6 (parity).** For all `m` in `{1,2,3,7,64}` and all `|V|` in
   `{0,1,m-1,m,m+1,4m}`, follower `data()` is element-for-element
   reference-identical to a local `pipeToSignal` buffer stream's, and
   follower `droppedCount() ===` the local stream's. 30 combinations, zero
   tolerance.
7. **A7 (zero-cost off).** With `sharedStream: false`, the byte-frozen GATE
   line at `torture.mjs:140` is **byte-identical** to 1.5.0's, and the
   existing 24 stream tests pass unmodified.
8. **A8 (OR-3).** F3 passes with an `isLeader` oracle that returns `true` in
   **every** tab, and again with one that returns `false` in every tab.
   Correctness is invariant under a lying oracle; only connection count
   degrades.
9. **A9 (OR-1).** `package.json` and `Query.js:41 VERSION` read `"1.5.0"`
   at pipeline close; `version-sync.test.js` green; the `[2.0.0]` CHANGELOG
   head present.
10. **A10 (feed shape).** All 26 pooled records carry exactly 10 own keys in
    the frozen order; `inspect-shape.test.js` extended, not edited.
11. **A11 (ON-3, operator-added).** A mutation rejecting with `null`, `0`,
    `""`, or `undefined` settles status `"error"`, `error()` returns the
    rejection reference-equal (or value-equal for primitives), the feed's
    `mutation:settle` reports `ok: false`; a superseded falsy rejection
    still reports `reason: "superseded"` with `ok: false`.

## Test plan -- 314 -> 354 (floor exactly met; plan 44 for margin)

| Group | New tests |
|---|---|
| F1-F7 failover cells (one named test each) | 7 |
| Wire shape: 4 message types, epoch gate, seq dedup, gap counting, inert-when-off | 8 |
| Projection: latest + buffer, parity, `droppedCount` fallback (V4), `dehydrate` exclusion (V7) | 7 |
| Watchdog / liveness / self-connect / lying-oracle invariance (A8) | 6 |
| Promotion: adopt path (V1), race tiebreak, abdication, deferred announce (V2) | 7 |
| Feed: 3 new types x shape + zero-cost off + no-4th-type regression | 6 |
| Teardown: projection slots released at detach and disposeEntry (V5) | 3 |
| ON-3 falsy-rejection breaking fix (operator-added) | 4 |
| **Total** | **48 -> suite 362, `skip_max: 0`** |

Every addition is additive. **Zero of the 314 existing tests are retired**
unless ON-3's sweep finds a quirk-pinning test, which is then retired BY
NAME per OR-9 -- and the `[2.0.0]` BREAKING section carries ON-3's fix as
its one true break, never an invented one (OR-7).

## STOP items (falsified against the code -- operator rulings in the header)

- **STOP-1.** "Followers receive frames as lite-channel MESSAGES" (BRIEF T0
  / charter `ROADMAP.md:1016-1018`) is **wrong on the transport**. There is
  no lite-channel dependency in `Query.js`; the channel is
  `opts.broadcastChannel` (`:111-112`, `:447-450`), and lite-channel appears
  only in a prose comment about wiring `isLeader` (`:103-106`). Ratify the
  corrected wording before it is copied into both roadmaps and tempts an
  import that breaks zero-runtime-deps. [ON-2: ratified.]
- **STOP-2.** OR-7 asserts the ledger holds **no** accumulated
  deferred-breaking nits. `llms.txt:66` records one: `mutation:settle`
  reports `ok: true` for a **falsy** rejection reason -- "inherited 1.4.0
  quirk, documented not changed". That is a deferred breaking nit sitting in
  the shipped surface, and 2.0 is the only major on the ladder. **Ruling
  needed: fix it in the BREAKING section, or record explicitly that it is
  deliberately carried past the only major that could have fixed it.**
  Silence here is the failure mode OR-7 exists to prevent. [ON-3: fixed at
  the major.]
- **STOP-3.** `llms.txt` contradicts itself on test counts. Line 3 says
  **314** ("253 core"); line 207 says "**142** core deterministic tests";
  line 219 says "test/query.test.js -- **106** deterministic tests". Lines
  207 and 219 are stale and the surface guard does not catch prose.
  Reconcile in C13, not mid-pipeline. [ON-4: ratified.]
- **STOP-4.** G8 pins "13 today" files in `npm pack --dry-run`. I did not
  read `package.json` (outside my read scope), so I cannot confirm 13. **The
  coder must verify the count against the actual `files[]` before treating
  13 as the baseline**, and any change is a named spec decision, not drift.
  [ON-4: ratified.]
- **STOP-5.** OR-8 is confirmed: the charter's `ASSERTIONS` line "Suite >=
  230" at `ROADMAP.md:1074` is stale. 354 governs. Correct it in the
  closeout docs commit only. [ON-4: ratified.]
- **STOP-6 (scope guard).** OR-11 holds throughout: `stream-frame` carries
  **one key per message**. No cell in F1-F7 requires multiplexing N keys
  over one socket, so the NON-GOAL is not resurrected. If a coder proposes
  batching frames across keys for throughput, that is a new spec decision
  requiring operator ratification -- it is not an optimization. [ON-4:
  binding.]
