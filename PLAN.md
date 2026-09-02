# PLAN -- Q9 -- lite-query v2.1.0 -- ratified 2026-09-03

Planner spec ratified by the operator with the amendments ON-1..ON-5 below.
Where an ON note conflicts with the spec text underneath, the ON note wins.
BRIEF.md (c4ca7c5) holds the operator rulings OR-1..OR-10; this file is the
coder's source of truth for WHAT to build.

## OPERATOR NOTES (amendments to the spec below)

- ON-1 -- STOP-DECISION-2 RATIFIED: outcome (a), the swap proceeds via
  writer-slot injection. The deciding evidence was re-verified by the
  operator at source before ratification (2026-09-03): the ONLY non-null
  `streamMode` assignment in the package is StreamQuery.js:255
  (`entry.streamMode = mode` in registerFollower); core Query.js writes
  only `null` (releaseProjection :585, entry literal); projectBuffer has
  exactly one caller, Query.js:649, guarded by
  `entry.streamMode === "buffer"`; the projWindow contracts live at
  test/shared-stream.test.js:627/:635/:658; the frozen envelope/state
  length gates exist at Query.js:2412/:1524. Buffer-mode projection is
  unreachable without the /stream subpath loaded -- no second window body
  can survive the swap.
- ON-2 -- AT-LEAST-ONCE RATIFIED, sentence corrected for truthfulness.
  Per-item RESULTS are not persisted; the REMOVAL is. The required
  verbatim llms.txt sentence is therefore (replacing the spec's version):
  "Replay is at-least-once across a crash: an item leaves the durable
  queue only after its terminal per-item result exists and the removal
  write has been handed to queueSave, so a crash between dispatch and
  that write replays the item on the next replayQueue() call. Every
  queue record carries a stable id -- use it as your idempotency key on
  the server. In-run, within a single replayQueue() call, every item is
  dispatched exactly once."
- ON-3 -- opt-in shape validation moves to mutation() CONSTRUCTION (the
  package's validate-at-the-door norm), and malformed opt-in fails
  closed instead of silently not queueing: `queue` present and not a
  boolean -> TypeError at mutation() (a caller who wrote `queue: 1`
  believes they opted in; silently taking the normal path is fail-open).
  `queue: true` requires at construction: `offline` a function, `name` a
  string, `queueKey` an array -- each violation a TypeError, nothing
  constructed. mutate() then handles ONLY the runtime oracle outcomes
  (throw / non-boolean return -> rejection with code LQ_OFFLINE_ORACLE,
  status "error", nothing dispatched, nothing queued). The enqueue
  receipt mutate() resolves is `{ queued: true, id }` -- exactly 2 keys,
  one build site. C3's assertions adjust to construction-time throws.
- ON-4 -- poison-item lever (scope addition, rung C4): an offline queue
  with no removal surface traps a permanently-rejected mutation forever.
  Add `qc.dropQueued(id) -> boolean`: true = found, removed, persisted
  through the same save path, one `queue:drop` feed event with reason
  "caller-dropped"; false = not present (including queueStore === null)
  -- an answer, not an error, no throw. Rejected-forever items remain
  visible via per-item results + `tries`; this is the caller's explicit
  exit. d.ts entry + 2 tests; the floor moves 387 -> 389.
- ON-5 -- everything else ratified as returned: tasks C1..C9 in ladder
  order (C3/C4 adjusted per ON-3/ON-4), the test floors (+27 total ->
  389 >= 380), the torture additions (airplane-mode soak + attempt F
  after the frozen GATE), the d.ts/llms.txt deltas, rejections (f)1-6
  with their forcing clauses, and the RISK escalation rule: if the
  shipped surface-drift guard hard-codes a client-method list and fails
  on replayQueue/queueSize/dropQueued, the coder STOPS and escalates to
  the operator -- editing the guard test is forbidden (OR-2/G5).

---

## PLANNER SPEC (verbatim, 2026-09-03)

STOPPED-EARLY: no. Full ruling below.

### (a) STOP-DECISION-2 (OR-8): OUTCOME (a) -- writer-slot injection. Swap APPROVED.

Deciding evidence (caller analysis): projectBuffer has exactly ONE caller,
Query.js:649, guarded by `entry.streamMode === "buffer"`. `streamMode` is
assigned "buffer" at exactly ONE site in the package: StreamQuery.js:255
(registerFollower). Core Query.js assigns it only null (:882 entry literal,
:585 releaseProjection). Therefore buffer-mode projection is unreachable in
a tab that never loaded the /stream subpath -- moving the body behind a
/stream-installed slot cannot leave a second reachable body. The "two live
window bodies by load-state luck" hazard does not exist here.

Slot shape (on the entry, declared next to projWindow at Query.js:894 so
every entry keeps one hidden class):
- `projWriter: null` -- holds lite-stream's 4-name handle
  (push/end/error/droppedCount), or null.

Install -- StreamQuery.js:252 registerFollower, ONLY when mode === "buffer",
after `entry.projWindow = null`:
`entry.projWriter = createSignalWriter(shim, { mode: "buffer", maxBuffer })`
where `shim` is a per-entry, per-registration object
`{ set(v) { entry.projWindow = v; entry.data.set(v); } }`. One cold
allocation per follower registration; ZERO per frame.

Why the shim is required, not cosmetic: shipped test
test/shared-stream.test.js:627 asserts `e.projWindow != null` "window
populated while following" (buffer mode, maxBuffer: 4), and :635/:658
assert it returns to null. The writer holds its ring privately, so a
direct entry.data target would silently break a frozen contract (OR-2/G5
forbid editing it). The shim is a legal target under lite-stream's own
validation -- Stream.js:721 requires only `typeof target.set ===
"function"`, and llms.txt states the rule as "invalid target (null /
non-object / no .set) -> TypeError". Document the adaptation in llms.txt
design notes.

Hot body after the swap (projectFrame, replacing the streamMode ===
"buffer" string compare):

    const w = entry.projWriter;
    if (w !== null) { w.push(value); entry.streamDropped = w.droppedCount; }
    else entry.data.set(value);

One null check replaces one string compare; droppedCount is a getter read
(zero alloc, monomorphic). projectBuffer (Query.js:658-682) is DELETED --
the third copy of the drop-oldest ring is gone, which is T4's whole point.
Latest-mode projection stays `entry.data.set(value)`, core-only,
stream-import-free: a plain-query or latest-mode follower tab is
byte-identical to 2.0.0.

Release -- Query.js:579 releaseProjection, immediately BEFORE
`entry.projWindow = null`:
`if (entry.projWriter !== null) { try { entry.projWriter.end(); } catch {}
entry.projWriter = null; }`
end() is idempotent, never writes to target, and nulls the writer's
internal win/tgt (Stream.js:764-765) -- which is what releases the shim
closure's capture of the entry. streamDropped was already mirrored, so
droppedCount() post-release is unaffected by the terminal freeze.

Peer floor: optional peerDependency @zakkster/lite-stream ^1.3.0 -> ^1.4.0,
cited ERA-BOUND from lite-stream's own llms.txt line 118:
`### createSignalWriter(target, opts?) -> { push, end, error, droppedCount }
(since 1.4.0)`. Never from a session number. devDependency ^1.4.0 per
OR-10 (lockfile rung, not a version site).

G8 unaffected: Query.js still imports lite-signal only; createSignalWriter
is imported unconditionally by StreamQuery.js alone.

### (b) OR-6 crash boundary: AT-LEAST-ONCE.

Removal from the durable queue happens AFTER the item's terminal per-item
result is recorded AND the queue has been handed to queueSave. A crash
between dispatch and that persisted removal replays the item on the next
replayQueue. Rationale: at-most-once destroys durable user intent silently
and unsurfaced -- the exact fail-open shape OR-3 forbids; a double-fire is
caller-addressable via the record's stable id.

Required verbatim llms.txt sentence: SUPERSEDED BY ON-2 (use the ON-2
text).

Named crash-window test (G9): test/queue-crash.test.js ->
"queue: a crash between dispatch and the persisted removal replays the
item (at-least-once, G9)" -- a queueSave thunk that throws on the removal
write, followed by a fresh client + queueLoad of the last durable payload,
asserting the item is present and dispatches a second time with the SAME
id.

### (c) RECOMMENDED SCOPE -- full spec

1. Opt-in flag + dispatch semantics (T1, OR-3, OR-4).
Two mutOpts fields, both required for any queueing to occur:
- `queue: true` -- strict identity check. [Validation timing and
  malformed-value handling per ON-3.]
- `offline: () => boolean` -- the caller's oracle (the isLeader
  precedent). The library never reads navigator.onLine, never listens,
  never polls (OR-4).
- `queueKey: <query key array>` -- REQUIRED when queue: true (it is what
  T3's "entry no longer exists" drop is resolved against). [TypeError at
  construction per ON-3.]
- `name` -- REQUIRED string when queue: true (what a reloaded tab
  resolves the handler by). [Construction-validated per ON-3.]

Dispatch ladder in mutate(vars), evaluated once, before onMutate and
before fn:
1. queue not opted in -> unchanged 2.0.0 path, byte-identical.
2. [construction-time validation per ON-3 -- no per-dispatch shape
   checks survive here]
3. Call offline() inside a try. A THROW or a NON-BOOLEAN return ->
   mutate() rejects with a tagged error (err.code = "LQ_OFFLINE_ORACLE"),
   status settles "error", nothing dispatched, nothing queued. An
   unverified connectivity state must not silently pick either branch.
4. offline() === false -> normal fetch path, unchanged.
5. offline() === true -> enqueue. status settles the new value "queued";
   mutate() RESOLVES the receipt `{ queued: true, id }` (ON-3; never a
   silent limbo, never a throw); onMutate/fn/onSuccess/onError do NOT
   run; onSettled DOES run with (undefined, undefined, vars, undefined)
   (Phase-4 law: onSettled always runs).
6. Enqueue against a full queue -> mutate() REJECTS err.code =
   "LQ_QUEUE_FULL" (OR-3: surfaced rejection, never a silent drop).

status() gains one additional value "queued", reachable only under
opt-in; loading() stays `status() === "pending"` (a queued mutation is
not loading). reset() clears it to "idle"; it does NOT dequeue the
durable item (the record outlives the handle by design -- documented).

REJECTED from scope: queueOnFailure. See (f).

2. Monomorphic queue record (T2) -- exactly 7 own keys, one order, built
at one site:

    { id: string, name: string, key: array, keyHash: string,
      vars: any, at: number, tries: number }

id = `clientId + ":" + (++queueSeq)` (stable across persist; the
at-least-once idempotency key). name = mutOpts.name. key/keyHash from
queueKey. tries starts 0, increments per dispatch attempt.

3. Store. qc holds queueStore = null until the first enqueue (null is
not zero: no queue is not an empty queue). Backing array + a queueSeq
counter. Client option maxQueue (default 100; validated positive
integer, else TypeError at client construction). Read-only
introspection: `qc.queueSize() -> number` (0 when queueStore === null)
-- used by the retention assertions.

4. Replay surface (T3, OR-4).

    qc.replayQueue(resolve) -> Promise<QueueReplayResult>

- resolve(record) -> fn | null -- caller-supplied handler resolver keyed
  off record.name. Not a function -> synchronous TypeError.
- Strictly sequential FIFO (await each) -- order preserved globally and
  therefore per key.
- Single-flight: a re-entrant call while one is running REJECTS
  err.code = "LQ_REPLAY_BUSY" (G6 in-run exactly-once by construction,
  not by test).
- No queue (queueStore === null) -> resolves
  { status: "empty", total: 0, replayed: 0, failed: 0, dropped: 0,
    items: [] }.

Per-item result, 5 keys, one order:
`{ id, key, status: "ok" | "error" | "dropped", value, reason }`.
- Entry for record.keyHash absent from the cache -> DROPPED, never
  dispatched, never silently retried: status "dropped", reason
  "entry-missing".
- resolve() returns null/non-function -> status "dropped", reason
  "handler-unresolved".
- resolve() itself throws -> status "dropped", reason "resolver-threw".
- Handler rejects -> status "error", value = the rejection VERBATIM,
  tracked by CONTROL FLOW (rejected boolean), never truthiness -- a
  falsy rejection settles "error" (ON-3-of-Q8 governs replay, T1). An
  errored item STAYS in the queue for the next call; tries increments.
- Handler resolves -> status "ok", value verbatim; removal per the
  at-least-once ordering in (b).

Return shape:
`{ status: "done" | "empty", total, replayed, failed, dropped, items }`
(6 keys).

[ON-4 addition: `qc.dropQueued(id) -> boolean` -- the poison-item exit;
found -> removed + persisted + one queue:drop feed event reason
"caller-dropped", returns true; absent -> returns false, no throw.]

5. Persist integration (T2, OR-5).
Blocking fact from the real code: persistQueryClient enforces
`Object.keys(envelope).length !== 2` (Query.js:2412) and
validateHydrateState enforces `Object.keys(state).length !== 1`
(Query.js:1524). Both are shipped contracts with tests. The queue
therefore CANNOT ride inside the existing envelope or state without
editing frozen tests (OR-2/G5 forbid it).

Ruling: the queue rides the SAME adapter, same version stamp, SIBLING
thunks -- additive options on persistQueryClient:
- queueSave(envelope), queueLoad() -- both optional; supplying one
  without the other -> TypeError at install (fail closed, no
  half-durable queue).
- Envelope: `{ version, queue: [...] }` -- exactly 2 keys, the same
  version value the cache envelope stamps, validated by a new
  validateQueueEnvelope mirroring the frozen ladder verbatim in
  discipline.
- Written on every enqueue and on every replay removal, through the
  SAME throttle window as doSave (a queue write arms the same
  trailing-edge timer); stop() flushes it; flush() forces it.
- Restore, on install, before the cache restore arms its hook:
  handle.queueRestored -- a promise that ALWAYS resolves, never
  rejects: `{ status: "restored" | "empty" | "dropped", count, reason }`.
  - queueLoad() returns null/undefined ->
    { status: "empty", count: 0, reason: null } -- an absent queue
    section is "no queue", NEVER an error (OR-3).
  - load-threw | malformed-envelope | version-mismatch (strict ===, no
    coercion) | malformed-queue | malformed-record ->
    { status: "dropped", count: 0, reason }. The WHOLE queue drops --
    never a partial restore -- and the drop is surfaced twice: on
    queueRestored AND as a queue:drop feed event carrying the reason.
- Record validation: all 7 keys present, correct primitive types, key an
  array, no own symbols, one contained pass -- a single bad record drops
  the whole queue (fail closed).
- OR-5: only ENQUEUED records are ever persisted. A pending in-flight
  mutation has no record and cannot be captured.

6. Feed vocabulary (T3, OR-7). Five additive domain:verb types on the
frozen 10-key record; 26 -> 31:
`"queue:enqueue"`, `"queue:restore"`, `"queue:replay"`, `"queue:settle"`,
`"queue:drop"`.
Field mapping: key/keyHash from the record; count = queue length after
the transition (queue:restore: records restored); ok = !rejected
(control flow, never truthiness); value = vars on enqueue, data-or-error
verbatim on settle; reason = the drop/settle reason code. All emits
behind the existing `feed.hook !== null` test. No second hook, no
record-shape change.

### (d) TASKS -- ladder order

C1 -- oracle upgrade (before the swap). package.json:devDependencies ->
@zakkster/lite-stream "^1.4.0"; lockfile refreshed; lite-signal override
pin untouched.
Assertions: node_modules/@zakkster/lite-stream/package.json version
1.4.0; test/shared-stream.test.js:325 (A6) passes UNEDITED against live
1.4.0, 30 combinations; full suite 362 pass / 0 fail / 0 skip; Query.js
unchanged in this commit (git diff --stat shows no Query.js).

C2 -- queue core. Query.js:createQueryClient (queueStore/queueSeq/
enqueue/queueSize), Query.js:FEED_TYPES (5 additive types),
Query.js:buildEventPool (unchanged code, 31 pooled records).
Assertions: FEED_TYPES.length === 31; every new type is domain:verb; a
pooled queue record has exactly the 10 frozen own keys in the frozen
order; qc.queueSize() === 0 on a fresh client and queueStore === null
(not []); maxQueue non-integer/0/-1/NaN each throw TypeError at
construction.

C3 -- mutation opt-in. Query.js:mutation (construction validation per
ON-3 + the dispatch ladder), Query.js:mutation.mutate.
Assertions: queue absent -> the 2.0.0 body executes with ZERO added
allocation (bench delta 0 B/op); `queue: 1` -> TypeError at mutation()
construction (ON-3); queue: true without offline -> TypeError at
construction; queue: true without a string name -> TypeError at
construction; queue: true without an array queueKey -> TypeError at
construction; a throwing offline() rejects mutate() with code ===
"LQ_OFFLINE_ORACLE" and queueSize() stays 0; offline() returning 1
(truthy non-boolean) rejects identically; offline() === true resolves
`{ queued: true, id }`, status() === "queued", fn call count 0,
onSettled call count 1; a full queue rejects code === "LQ_QUEUE_FULL"
and queueSize() is unchanged.

C4 -- replay surface + drop lever. Query.js:createQueryClient.replayQueue
+ dropQueued (ON-4).
Assertions: 5 items enqueued across 2 keys replay in enqueue order
(recorded dispatch order deep-equals enqueue order); each fn is called
exactly once per replayQueue call; a second concurrent call rejects
LQ_REPLAY_BUSY while the first is in flight; a handler rejecting with
null yields status "error" with value === null and the item REMAINS
queued with tries === 1; a removed cache entry yields status "dropped" /
reason "entry-missing" with the handler call count 0; every per-item
result object has exactly 5 own keys in the fixed order;
dropQueued(knownId) === true, queueSize() decremented, one queue:drop
event reason "caller-dropped"; dropQueued("nope") === false, no event.

C5 -- persist seam. Query.js:persistQueryClient (queueSave/queueLoad/
queueRestored), Query.js:validateQueueEnvelope.
Assertions: queueSave without queueLoad throws TypeError at install;
queueLoad() -> null resolves { status: "empty", count: 0, reason: null };
a 3-key envelope resolves reason "malformed-envelope"; version "2"
against version 2 resolves "version-mismatch" (strict), queueSize() ===
0, and exactly one queue:drop event fires; one corrupt record among 4
good ones drops all 4 (count 0); a pending non-queued mutation is absent
from every queueSave payload; the existing 362 tests pass unedited (the
cache envelope path is byte-identical).

C6 -- crash boundary. Query.js:createQueryClient.replayQueue
(removal-after-persist ordering).
Assertions: dispatch order is fn -> record terminal -> queueSave ->
splice (assert via a save-thunk call log); a queueSave that throws on
the removal write leaves the item durable and it re-dispatches with the
identical id on the next call; the ON-2 at-least-once sentence is
present verbatim in llms.txt (surface guard); no code path removes a
record before its terminal result exists.

C7 -- the swap. Query.js:projectFrame, delete Query.js:projectBuffer,
Query.js:releaseProjection, Query.js entry literal (projWriter),
StreamQuery.js:registerFollower, package.json:peerDependencies.
Assertions: `grep -c "function projectBuffer" Query.js` is 0;
`grep -n "from ['\"]" Query.js` lists lite-signal only (G8); A6 passes
UNEDITED, 30 combinations, element references identical and
droppedCount parity exact; test/shared-stream.test.js:616 and :642
(projWindow populated then null) pass unedited; after releaseProjection,
entry.projWriter === null; latest-mode follower projection allocates
0 B/frame over 10k frames; peerDependency reads ^1.4.0; the N-tab soak
reports zero dup, zero reorder.

C8 -- torture. test/torture.mjs.
Assertions: the GATE window is byte-identical (git diff shows only
additions strictly AFTER the frozen gate evaluation); all 5 existing
controls still trip; the airplane-mode soak and attempt F print after
the gate; INCONCLUSIVE.md records attempt F verbatim with its honest
outcome.

C9 -- docs (last, one commit). README.md, Cookbook.md, llms.txt,
Query.d.ts, CHANGELOG.md.
Assertions: every feed-count site reads 31 and all move in this single
commit (git show --stat lists README, Cookbook, llms.txt together --
OR-7/QD-4); the ON-2 at-least-once sentence appears verbatim in
llms.txt; CHANGELOG.md head is [2.1.0] with Added/Changed only, no
Breaking section; package.json version and Query.js:VERSION both still
read 2.0.0 (OR-1); ascii guard green; surface guard green;
`npm pack --dry-run` lists 13 files with test/, bench/, INCONCLUSIVE.md,
BRIEF.md, PLAN.md absent.

### (e) Test floors, torture, surface deltas

Base 362, additions +27 (ON-4 adjusted), floor 389 (>= 380).
- test/queue.test.js -- enqueue/opt-in/dispatch ladder/caps/receipt/
  status: +8
- test/queue-replay.test.js -- order, per-item shape, drops,
  single-flight, falsy-rejection: +7
- test/queue-persist.test.js -- stamp, mismatch, malformed, whole-drop,
  empty-is-not-error, in-flight exclusion: +5
- test/queue-crash.test.js -- G9 at-least-once + removal ordering: +2
- test/queue-feed.test.js -- 31-type count guard + 10-key record on the
  5 new types: +2
- test/shared-stream.test.js additive block -- projWriter installed in
  buffer mode only, null after release, absent in latest mode: +1
- dropQueued (ON-4): +2
All additive; zero edits to the 362.

Torture additions (strictly after the frozen GATE evaluation):
airplane-mode soak -- 200 cycles of {enqueue N=25 offline, persist, drop
client, restore, replay}, asserting qc.queueSize() returns to 0 every
cycle and pool baseline unchanged, maxMajor budget held; attempt F
(OR-9) -- replay teardown with in-flight replay handles (client disposed
mid-replayQueue), recorded verbatim in INCONCLUSIVE.md whatever the
outcome; a control that cannot trip is reported as decorative, never
faked.

Query.d.ts deltas (additive only): MutationOptions.queue?: true,
.offline?: () => boolean, .queueKey?: QueryKey, .name?: string;
MutationStatus gains "queued"; QueuedReceipt; QueueRecord;
QueueItemResult; QueueReplayResult; QueryClient.replayQueue,
.queueSize, .dropQueued (ON-4); QueryClientOptions.maxQueue?: number;
PersistOptions.queueSave?/.queueLoad?; PersistHandle.queueRestored?.

llms.txt deltas: the queue section (opt-in ladder, record shape, replay
signature + result shapes, the verbatim ON-2 at-least-once sentence, the
whole-drop rule); the 31-type feed table; the createSignalWriter seam
note (writer slot + shim target rationale, "(since 1.4.0)" peer floor).
Every liveness-adjacent sentence must be true the way OR-3/Q8 made the
stream sentence true.

### (f) REJECTED

1. queueOnFailure (auto-enqueue on a rejected fetch). Rejected by OR-3
   (fail-closed queueing) -- a rejection cannot be distinguished from a
   semantic 4xx, so auto-queueing it is a silent retry of a mutation the
   server refused. Parked with the reason recorded in ROADMAP; only the
   explicit offline() oracle ships in 2.1.0.
2. Queue inside the persist envelope or state. Rejected by OR-2/G5 --
   Query.js:2412 (length !== 2) and Query.js:1524 (length !== 1) are
   enforced by shipped tests; a third key would require editing them.
   Sibling queueSave/queueLoad thunks on the same adapter, same version
   stamp, instead.
3. A queue:frame-style per-frame or per-tick emit, and any second hook.
   Rejected by OR-7 (record-shape frozen, no second hook) and the
   recorded Q8 trim precedent.
4. Any connectivity detection, reconnect listener, or retry timer.
   Rejected by OR-4.
5. In-session version stamp. Rejected by OR-1 -- package.json and
   Query.js:VERSION stay 2.0.0.
6. Direct entry.data as the writer target (no shim). Rejected by OR-2 --
   it silently breaks test/shared-stream.test.js:627.

RISK: if the shipped surface-drift guard (G6) enumerates client-method
names in a hard-coded list, adding replayQueue/queueSize/dropQueued
fails it and the fix would be a forbidden test edit -- the coder must
STOP and escalate to the operator, not edit; mitigation already built in
by keeping all three surfaces as client methods rather than new module
exports.
