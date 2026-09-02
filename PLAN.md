# PLAN -- Q6 -- lite-query v1.4.0 -- persistence (RATIFIED)

Operator ratification of the Q6 planner spec, 2026-09-02. The planner's
deliverables A-E follow VERBATIM below the rulings. BRIEF.md remains the
contract; this file is the build order. Where the two disagree, the rulings
here win (they are the newer operator word).

## OPERATOR RULINGS ON THE PLANNER'S STOP ITEMS

- ON-1 (resolves STOP-1, unblocks C2): RATIFIED. The configure() adoption
  branch plus the recomputeCursor extraction from rebuildInfinite are approved
  edits to the Q5-pinned surface. Conditions: the adoption trigger is exactly
  `entry.isInfinite && entry.getNextCursor === null` (the planner proved the
  state unambiguous -- only disposeEntry also nulls it, and that entry is
  already out of the map); there is ONE cursor-recompute site in the file
  after the extraction, not two; D1/D2/D3 regressions in
  test/infinite-query.test.js stay green at every rung; the reviewer audits
  the adoption branch and its throw containment against the Q5 pinned
  contracts explicitly. No cursors stored, no slot added -- confirmed as the
  ruling intended.
- ON-2 (resolves STOP-2): RATIFIED. The "macrotask" wording in OR-2 was
  wrong for the mock (queueMicrotask, test/harness.js:166); the conclusion
  survives (synchronous boot safe under both; the mock is stricter, which is
  the safe direction). BRIEF.md OR-2 has been amended by the operator this
  session; the [1.4.0] CHANGELOG head records the correction; plan tests 11
  and 12 are PINNED deliberate contracts (same-tick boot cannot observe a
  queued remote setData; an awaited boot that raced a remote setData drops as
  cache-not-empty), never flake.
- ON-3 (resolves STOP-3): RATIFIED, all three shape decisions, recorded in
  the CHANGELOG head:
  (a) hydrate THROWS only on the OR-2 empty-cache precondition (programming
      error) and RETURNS { ok, count, reason } for malformed stored payloads
      (external data) -- the asymmetry mirrors setQueryData's local-throw /
      remote-drop law.
  (b) a FINITE future dataUpdatedAt is clamped to opts.now() (bounds
      freshness at exactly one staleTime from boot -- the fail-open direction
      is the unclamped one); a NON-FINITE dataUpdatedAt (NaN, Infinity,
      string, missing) is malformed-timestamp and drops the WHOLE payload
      per OR-3. The boundary is finite-vs-not, stated in the doc comment.
  (c) stop() FLUSHES the pending save, no opt-out; `qc.clear()` then
      `stop()` is the documented logout path (clear is hook site 5, so
      emptiness persists). save() rejections stay contained.
- ON-4 (floors): tests_min 218 stands as the FLOOR; the plan projects >= 233.
  The shipped docs print the real recount, never the floor (Q5 discipline).
- ON-5 (reviewer checklist): the V4 six-site hook list is the reviewer's
  checklist verbatim. Adding or removing a site is a DECLARED decision in the
  commit message with a one-line rationale, never silent. The five recorded
  non-sites (invalidate, ensureEntry, attach/detach, prefetch-passthrough,
  hydrate seeding) are part of the checklist too -- a hook appearing at one
  of them is a defect unless declared and justified.

Pipeline note (unchanged from Q5): coder implements C1..C8 in order, one
commit per rung, suite green and GATE byte-identical at every rung; then
reviewer (REJECTED goes back to coder); then qa. No version stamp (OR-1).

---

# THE PLANNER SPEC (verbatim)

Read: BRIEF.md, ROADMAP.md (Q6 block L733-845; parking ledger L1115-1149),
Query.js (all 1386 lines), llms.txt, Query.d.ts, test/harness.js,
test/infinite-query.test.js (D1/D2/D3 at L334/370/385/409), test/torture.mjs,
test/torture/controls.mjs, test/ascii-guard.test.js, test/surface-guard.test.js,
package.json, CHANGELOG head, INCONCLUSIVE.md, and cross-repo read-only
../LiteBakeStream/llms.txt + ../LiteBakeStream/test/DehydratedCache.test.js.

## A. SPEC

**Hot body vs cold path (decided before anything else).** The hot body is:
the accessor reads in query()/infiniteQuery() (Query.js:985-1012, 1176-1197),
entry.data/error/status/fetching signal reads, and the entry slot table
(createEntry, Query.js:281-327). Persistence adds ZERO bytes there: no new
entry slot, no new signal, no read-path branch. The cold path is: dehydrate
(walks the map), hydrate (boot-only), persistQueryClient, and ONE
client-scope null-test (persistHook !== null) on six settle/commit sites --
none of which is in the 200000-iteration warm loop that phase H measures.
G8 therefore holds by construction.

### Findings (verification duties)

**V1 (OR-2 precondition) -- CONFIRMED, with a correction to the ruling's
stated mechanism.**
Real path: channel.addEventListener("message", onRemoteMessage)
(Query.js:238); the only remote message that can populate an empty cache is
`case "setData": setQueryData(...)` (Query.js:258) which calls ensureEntry
(Query.js:671). invalidate/remove/clear/fetch-req never create entries
(Query.js:259-270, 706, 753).
Mock path: test/harness.js:166 delivers via queueMicrotask, i.e. a MICROTASK,
not a macrotask -- the comment on that line ("spec: async delivery") is
accurate, OR-2's word "macrotask-scheduled" is not, for the mock.
Consequence: (a) a strictly SYNCHRONOUS create qc -> hydrate is safe under
both, so OR-2's precondition stands; (b) the mock is STRICTER than the real
channel (it fires after one await, the real one only after a task), so a test
that passes under the mock passes against a real channel -- the safe
direction; (c) any adapter boot with an await between construction and
hydrate (i.e. every async load()) CAN observe a remote entry under the mock --
that is exactly OR-6's "resolves the outcome channel as a drop" case, and
T4/persist.test.js must pin it deliberately rather than discover it as flake.

**V2 (OR-4 recompute-at-attach) -- SOUND ONLY WITH A 4-LINE ADOPTION BRANCH;
the literal mechanism named in OR-4 is not available pre-attach.** Trace:
setQueryData(key, pagesArray) rebuilds pages+flat+cursor ONLY if
e.isInfinite is already true (Query.js:682-692 -> rebuildInfinite, 188-203),
and isInfinite is set exclusively by infiniteQuery's configure()
(Query.js:1087-1099). On a hydrate-seeded plain entry, configure() runs
disposeNode(entry.data), allocates a fresh signal(undefined,
{equals: NEVER_EQUAL}) and calls resetPages(entry) (134-139) -- it DESTROYS
the restored pages. Conversely, if hydrate seeds the entry as
already-infinite, configure() early-returns at Query.js:1088 and never
installs infOpts.getNextCursor, leaving entry.getNextCursor === null;
rebuildInfinite then skips the cursor recompute (197) and commitPage would
call null(...) (170) and route a TypeError into the error ladder. So:
hydrate seeds the infinite shape itself, and configure() gains an adoption
branch keyed on the already-unambiguous state
`entry.isInfinite && entry.getNextCursor === null` (only disposeEntry also
nulls it, Query.js:363, and that entry is already out of the map at
758/386/771). No new slot. [Ratified: ON-1.]

**V3 (OR-8 timestamp reuse) -- CONFIRMED, no new slot.** The staleness-math
source is `entry.lastCompletedAt` -- declared Query.js:291 (-Infinity),
written at 610, 638, 695, read by shouldFetch at Query.js:469
(`(opts.now() - entry.lastCompletedAt) >= entry.staleTime`). The ROADMAP's
"dataUpdatedAt" is a WIRE name, not a runtime slot; the plan keeps
dataUpdatedAt as the JSON field and lastCompletedAt as the slot. Entry
monomorphism unchanged.

**V4 (hook placement) -- the exact list the reviewer holds against the
diff.** notifyWrite() (`if (persistHook !== null) persistHook();`) at exactly
six sites in Query.js:
1. setQueryData -- after clearSharedTimer(e), beside broadcast (~701).
   Covers local writes AND every remote apply (remote routes through
   here, 258).
2. runFetch settle -- after entry.abortController = null (~642), covering ok
   AND error uniformly (an entry that flips to error must leave the
   snapshot).
3. runFetch commitPage-throw branch -- before return
   Promise.reject(commitErr) (~615), because that branch returns before
   site 2.
4. removeQueries -- after the loop, only if >= 1 entry matched (~760).
5. clear -- after entries.clear(), only if the map was non-empty (~771).
6. the cacheTime GC timer callback -- after entries.delete(entry.keyHash)
   (~386), so an expired entry stops being persisted.
NOT hook sites (recorded decisions): invalidate (no content change;
invalidatedSinceCompletion is not serialized), ensureEntry, attach/detach,
prefetch (its writes land via site 2), and hydrate seeding (would re-save
what we just loaded).

**V5 (no-broadcast seeding) -- CONFIRMED.** broadcast() (Query.js:248-251)
is reached only from setQueryData:701, invalidate:748, removeQueries:761,
clear:772, requestSharedFetch:497, runFetch:630. Hydrate must NOT call
setQueryData; it writes e.data/error/status, e.lastCompletedAt,
e.invalidatedSinceCompletion directly through a private seedEntry, so it
never reaches a broadcast site and never fires notifyWrite. It must NOT
borrow processingRemote = true as a suppression trick: that flag also flips
the infinite non-array path from throw to silent-drop (Query.js:683) and
would lie about provenance.

### Dehydrated shape (pinned)

Primitive emits STATE only; adapter stamps version (OR-6 confirmed):

- `qc.dehydrate() -> { entries: [ EntryRecord, ... ] }` -- exactly one own
  key, `entries`.
- EntryRecord -- exactly FOUR own keys, uniform on every record (monomorphic
  wire shape, mirrors the entry-slot law):
  - `key` -- the entry's key array, by reference.
  - `data` -- for a plain entry, entry.data() by reference; for an infinite
    entry, a SHALLOW COPY of entry.pages (pages.slice(), cold path) because
    pages is the one structure the cache grows in place.
  - `dataUpdatedAt` -- entry.lastCompletedAt, must be finite.
  - `infinite` -- boolean, ALWAYS present (an absent flag is malformed, not
    "false").
- Included iff: untrack(status()) === "success" AND !entry.isStream AND
  Number.isFinite(entry.lastCompletedAt) AND (infinite ?
  Array.isArray(entry.pages) : entry.data() !== undefined). Pending/error/
  stream entries are excluded by construction, both exclusions documented in
  the doc comment and tested.
- Aliasing contract (documented, tested): key, data, and page CONTENTS are
  references, not deep copies. Serialize the payload before further cache
  writes.
- dehydrate's doc comment states it walks the whole map and is cold by
  definition (OR-8).

### Hydrate algorithm

`qc.hydrate(state) -> { ok: boolean, count: number, reason: string | null }`

1. OR-2 precondition, FIRST, before anything else: if the entries map is
   non-empty, THROW Error("lite-query: hydrate requires an empty cache
   (N entries present) -- hydrate at boot, before any observer attaches").
   Throw is reserved for this programming error; a malformed STORED payload
   returns { ok: false } (asymmetry mirrors setQueryData's local-throw /
   remote-drop law, Query.js:682-687). [Ratified: ON-3a.]
2. OR-3 validation pass over the WHOLE payload BEFORE any mutation. Reason
   codes (stable, ASCII, pinned): malformed-state, malformed-entries,
   malformed-entry, malformed-key, malformed-data, malformed-timestamp,
   malformed-pages, duplicate-key. Nothing is written until the pass
   completes.
3. Seeding (only after a clean pass): per record ensureEntry(key) (arms the
   normal cacheTime GC -- a restored, unobserved entry expires like a
   prefetched one), then plain entries get data.set(data),
   error.set(undefined), status.set("success"),
   lastCompletedAt = Math.min(dataUpdatedAt, opts.now()) (a FINITE future
   timestamp is clamped to our clock -- bounds freshness, never invents
   data [ON-3b]), invalidatedSinceCompletion = false. Infinite records go
   through seedInfinite: isInfinite = true, dispose the plain data node and
   recreate it with NEVER_EQUAL, pages = record.data, flat rebuilt by the
   same loop rebuildInfinite uses, nextCursor = null, hasNext = FALSE (fail
   closed: an unattached restored infinite entry can never auto-fetch at a
   wrong cursor -- runFetch bails at Query.js:520), pageGen = 0,
   getNextCursor = null (functions never serialize).
4. No broadcast, no notifyWrite (V5).

### Infinite restore (OR-4 mechanics)

configure() (Query.js:1087) gains one adoption branch: when
`entry.isInfinite && entry.getNextCursor === null`, install
infOpts.getNextCursor and recompute via recomputeCursor(entry) -- a helper
EXTRACTED FROM rebuildInfinite's tail (Query.js:197-201) and called by both,
so there is one cursor-recompute in the file, not two. A getNextCursor that
THROWS during adoption is contained fail-closed: catch, resetPages(entry),
data.set(undefined), status.set("idle"), lastCompletedAt = -Infinity -- the
maybeFetch two lines later (Query.js:1138) then does a clean page-one fetch.
Post-restore fetchNextPage() therefore starts from the recomputed cursor and
appends page N+1 (G6). [Ratified: ON-1.]

### Adapter

`persistQueryClient(qc, { save, load, version, throttle? })
-> { restored, flush, stop }` -- a CORE export of Query.js (OR-5), zero new
deps, storage-agnostic thunks.

- Install-time validation (fail closed, throws): save/load must be
  functions; version is REQUIRED with no default (string/number only; strict
  === at compare, no coercion); throttle ?? 1000 must be a finite number
  >= 0; a second install on the same client throws (single-slot seam).
- Restore outcome surface: handle.restored -- a promise that always RESOLVES
  (never rejects) with { status: "restored" | "empty" | "dropped", count,
  reason }. Reasons: null | "load-threw" | "malformed-envelope" |
  "version-mismatch" | "cache-not-empty" | a hydrate reason code. load()
  returning null/undefined is "empty" (normal boot, OR-3) and never reaches
  hydrate. A late async load() whose entries already exist hits OR-2: the
  adapter CATCHES hydrate's throw and resolves "dropped" /
  "cache-not-empty" -- never a merge, never an unhandled rejection, never a
  silent nothing.
- Envelope: exactly { version, state }, two own keys. Anything else ->
  "malformed-envelope".
- Hook arming order: the write hook is installed AFTER the restore outcome
  settles, in all three branches -- so seeding can never trigger a save and
  a boot-window write can never persist a half cache.
- Throttle (OR-7): trailing-edge coalescing on
  qc.options.setTimeout/clearTimeout (the resolved opts object is public,
  Query.js:825), .unref()-guarded like every other timer in the file. First
  notify arms the window; the timer is nulled BEFORE the flush builds state,
  so a write during save() opens a fresh window. save() rejections are
  contained (never unhandled); the adapter keeps running.
- flush() forces the pending save now; stop() is idempotent, uninstalls the
  hook and clears the timer.
- OR-7 decision: FLUSH-ON-STOP, with no option [Ratified: ON-3c]. A pending
  timer MEANS there is a committed cache write not yet on disk; stop() fires
  at page teardown / logout / client dispose -- precisely when the last
  write matters most, and dropping it loses user-visible state for zero
  benefit. The logout case does not need a second mode: qc.clear() then
  stop() persists EMPTINESS (clear is hook site 5), which is the correct
  logout semantic. Recorded in the CHANGELOG head.

### Docs facts (T6/OR-9)

The bake recipe cites bake-stream's line VERBATIM (../LiteBakeStream/
llms.txt:98):

  "Consumer floors: validated preserve reads need `>= 1.3.1` (the six S1
  read doors all closed there; the preserve doors themselves shipped in
  `1.3.0`, but the full validated read set is `1.3.1`); CRC-verified
  persistence needs `>= 1.6.0` (`{ crc: true }` on write,
  `{ verifyCrc: true }` on open); abortable range reads need `>= 1.7.0`."

Plus, from the same line: below the crossover, "use plain JSON", in those
words; and the getJSON rule -- a native SyntaxError (its .code is undefined)
means the stored record is not JSON: discard the cache and re-fetch, never
retry the same bytes. No numbers restated from our ROADMAP; session numbers
are never semvers.

## B. TASK LADDER

Every rung leaves npm test green, 0 skip, and the GATE line byte-identical.
No version stamp anywhere (OR-1: package.json:3 and Query.js:41 stay
"1.3.0").

**C1 -- primitive: dehydrate + hydrate (plain entries).**
Scope: qc.dehydrate/qc.hydrate, seedEntry, the validation pass, reason
codes; exported on the client object beside prefetch. Files: Query.js,
test/persist.test.js (new).
Commit: `feat(query): qc.dehydrate/qc.hydrate -- boot-only, all-or-nothing
seeding (T1, OR-2/OR-3/OR-8)`
Proof: npm test (>= 203 + new, 0 fail 0 skip).

**C2 -- infinite round-trip.**
Scope: seedInfinite, recomputeCursor extracted from rebuildInfinite, the
configure() adoption branch + its throw containment. Files: Query.js,
test/persist.test.js.
Commit: `feat(query): infinite entries round-trip -- pages restore,
cursor/hasNext recompute at first attach (T3, OR-4/ON-1)`
Proof: npm test; D1/D2/D3 in test/infinite-query.test.js still green.

**C3 -- the private write seam.**
Scope: client-scope persistHook single slot, notifyWrite(), the six V4 call
sites, _internal.installPersistHook(fn) -> uninstall. Files: Query.js,
test/persist.test.js.
Commit: `feat(query): private single-slot cache-write hook -- one null test
per commit site (T2, OR-5/OR-8)`
Proof: npm test + node --expose-gc test/torture.mjs (GATE byte-identical,
first check of G8).

**C4 -- the adapter.**
Scope: persistQueryClient export, install validation, restore ladder,
throttle, flush/stop (flush-on-stop). Files: Query.js, test/persist.test.js.
Commit: `feat(query): persistQueryClient(qc, { save, load, version,
throttle }) -- storage-agnostic, fail-closed restore (T2, OR-5/OR-6/OR-7)`
Proof: npm test.

**C5 -- conformance / corruption matrix.**
Scope: test/persist-conformance.test.js mirroring the SHAPE of
../LiteBakeStream/test/DehydratedCache.test.js (Part A round-trip / Part B
matrix table + single driver / Part C fail-open witness + pinned
MATRIX.length). Imports nothing from bake, ever. Files: test only.
Commit: `test(query): dehydrated-cache corruption matrix -- every malformed
class drops the whole payload (T4, OR-3)`
Proof: npm test.

**C6 -- torture phase + OR-11 attempt.**
Scope: phase H gains the 4096-cycle dehydrate/hydrate/teardown loop
(inserted after the existing phase-1 loop, before globalThis.gc?.() at
test/torture.mjs:194, so live/findings cover it); module-scope
release/fetcher discipline preserved; the OR-11 findings-clause attempt
through adapter stop() with a pending throttled save and a tracked payload
handle, recorded verbatim in INCONCLUSIVE.md as Attempt C in the Q5 style --
a ctlPersist control is added to test/torture/controls.mjs IFF it actually
trips; otherwise the four live controls stay alloc/detach/fuzz/pages and the
non-firing is recorded, never faked. Files: test/torture.mjs,
INCONCLUSIVE.md, (conditionally) test/torture/controls.mjs.
Commit: `test(torture): phase H dehydrate/hydrate cycle loop + OR-11
findings-clause attempt C (T5, OR-11)`
Proof: node --expose-gc test/torture.mjs (GATE byte-identical + ok) and
npm run torture:control (>= 4 tripped: lines).

**C7 -- docs pass.**
Scope: llms.txt (new surface + the two scope lines flip: "No SSR hydration"
and "No persistence of infinite pages"), Query.d.ts (dehydrate/hydrate on
QueryClient, DehydratedState, DehydratedEntry, HydrateResult, PersistOptions,
RestoreOutcome, PersistHandle, persistQueryClient), README.md (facts-table
persistence row -- TanStack: plugin; SWR: manual -- plus the
allocation-table row), Cookbook.md recipes (a) localStorage + IndexedDB
thunks, (b) the bake-backed variant with the verbatim consumer-floor quote
(OR-9), (c) streamQuery + RangeReader (OR-10). If the ingest-progress recipe
does not fit, EXPLICITLY re-park it in the ROADMAP ledger -- no silent drop.
Files: llms.txt, Query.d.ts, README.md, Cookbook.md, ROADMAP.md.
Commit: `docs(query): persistence surface -- llms.txt, d.ts, README facts
row, three Cookbook recipes (T6, OR-9/OR-10)`
Proof: npm test (surface-guard + ascii-guard are the gate).

**C8 -- CHANGELOG head + pack.**
Scope: `## [1.4.0] -- unreleased` head recording every OR decision (incl.
flush-on-stop, the future-timestamp clamp, the throw/return asymmetry, the
invalidate non-hook decision, and V1's correction to OR-2's "macrotask"
wording). Files: CHANGELOG.md.
Commit: `docs(changelog): 1.4.0 head -- persistence primitive, adapter, OR
decisions (OR-1: no version stamp)`
Proof: npm test && npm run torture && npm pack --dry-run.

## C. ASSERTIONS (qa runs these verbatim)

1. [G1] npm test -> pass >= 218, fail 0, skip 0, todo 0. Base is 203; C1-C5
   add >= 30.
2. [G2] node --expose-gc test/torture.mjs prints, byte for byte,
   `GATE leak=size 0/0 findings=0 warnings=0 | gc major=0 minor=0
   maxMs=0.00 | ok` followed by `ok`, exit 0.
   QUERY_TORTURE_BREAK=1 node --expose-gc test/torture.mjs prints
   `CONTROL SUMMARY tripped=4/4` (or 5/5 iff C6's attempt tripped).
3. [G5 / GC budget] Phase H's checkNoGc(summary, { maxMajor: 0,
   maxPauseMs: 4 }) returns verdict === 'pass' with summary.gc.major === 0
   and summary.gc.maxMs <= 4.00 across the 200000-iteration warm loop WITH
   the new cycle loop present.
4. [G5 / retention] After 4096 dehydrate/hydrate/teardown cycles,
   tracker.size() === 0 and tracker.audit().length === 0; the WeakRef census
   over the last 256 sampled payload handles is not 256/256 live.
5. [G3] For a 5-key success cache: dehydrate ->
   JSON.parse(JSON.stringify(...)) -> fresh client -> hydrate yields
   deepStrictEqual getQueryData for all 5 keys, result.ok === true,
   result.count === 5; and with staleTime: 30_000 and a mock clock advanced
   29_999 ms the restored client issues 0 fetches, at 30_000 ms exactly 1 --
   identical to a client that never restarted.
6. [G4] Every row of the MATRIX table drops the WHOLE payload: hydrate
   returns ok === false, count === 0, typeof reason === "string", and a
   subsequent hydrate(goodState) on the same client returns ok === true
   (proving the cache was still empty). MATRIX.length equals its pinned
   literal. The fail-open witness is rejected on 100% of rows.
7. [G4] version: "v2" against a { version: "v1", state } payload resolves
   handle.restored with { status: "dropped", reason: "version-mismatch",
   count: 0 } and getQueryData is undefined for ALL stored keys (0 of N
   hydrated).
8. [G6] Restored infinite entry: pages() deep-equals the 3 dehydrated pages
   and data() the flat view; after first attach hasNextPage() === true; one
   fetchNextPage() produces exactly 1 fetcher call whose cursor equals the
   recomputed cursor (not null), and pages().length === 4 with page 1
   unchanged at index 0.
9. [G8] With no adapter installed, a 4096-cycle no-adapter run leaves the
   GATE line identical to the 1.3.0 line asserted in (2), and
   bench/torture/query-soak.mjs, cache-fuzzer.mjs, shared-fetch-soak.mjs all
   exit 0 under phase P.
10. [G7] npm test surface-guard passes with persistQueryClient present in
    both llms.txt and Query.d.ts; ascii-guard walks every new file
    (test/persist.test.js, test/persist-conformance.test.js, any fixture
    .mjs) with 0 non-ASCII bytes.
11. [G9] npm pack --dry-run reports exactly 13 files; test/, bench/,
    INCONCLUSIVE.md, BRIEF.md, PLAN.md, ROADMAP.md absent; llms.txt +
    CHANGELOG.md present. (13 = the 12 package.json:29-42 entries +
    package.json; Q6 ships no new file.)
12. [G1/OR-1] Query.js:41 is `export const VERSION = "1.3.0"` and
    package.json:3 is "1.3.0"; test/version-sync.test.js green;
    CHANGELOG.md has a [1.4.0] head.

## D. TEST PLAN (>= 30 new; suite 203 -> >= 233, floor 218)

**test/persist.test.js** (new):
1. dehydrate: a success cache round-trips through JSON with identical
   getQueryData for every key
2. dehydrate: pending entries are never serialized (a promise is not data)
3. dehydrate: error entries are never serialized
4. dehydrate: stream entries are never serialized (a connection is not data)
5. dehydrate: an infinite entry is marked and carries a COPY of its pages
   array
6. dehydrate: the emitted state has no version field (the adapter stamps
   it, OR-6)
7. hydrate: seeded entries are stale-aware -- 29999ms no refetch, 30000ms
   exactly one (mock clock)
8. hydrate: THROWS on a non-empty cache, and the message names the
   empty-cache precondition (OR-2)
9. hydrate: throws even when the existing entry has zero observers (entry
   count, not observer count)
10. hydrate: never broadcasts -- a peer tab sees zero messages after seeding
    (mock BroadcastChannel)
11. hydrate: a same-tick boot (create qc -> hydrate) cannot observe a queued
    remote setData (V1)
12. hydrate: an awaited boot on a crossTab client that received a remote
    setData drops as cache-not-empty (V1c)
13. hydrate: a future dataUpdatedAt is clamped to now() -- restored entry is
    not immortally fresh
14. hydrate: seeded entries GC at cacheTime like any unobserved entry
15. infinite: a restored list continues paginating -- fetchNextPage appends
    page N+1 exactly once (G6)
16. infinite: hasNextPage is false before attach and correct after the first
    attach (fail closed)
17. infinite: a getNextCursor that throws at adoption resets to a clean
    page-one fetch, never wedges
18. write hook: fires on setData / fetch settle / commitPage throw /
    removeQueries / clear / GC expiry (6 sites)
19. write hook: does NOT fire on invalidate, attach, detach, or hydrate
    seeding
20. write hook: zero calls with no adapter installed; a second install
    throws
21. adapter: version is REQUIRED -- no default, install throws without it
    (OR-6)
22. adapter: load() -> null is an EMPTY store, status "empty", not an error
    (OR-3)
23. adapter: a throwing/rejecting load() resolves { status: "dropped",
    reason: "load-threw" } -- never unhandled
24. adapter: version mismatch drops 100% of entries and fetches fresh
25. adapter: throttle coalesces N writes in a window into exactly ONE save
    (mock clock, trailing edge)
26. adapter: stop() uninstalls the hook and clears the pending timer;
    further writes save nothing
27. adapter: stop() FLUSHES a pending save (OR-7/ON-3c decision); clear() +
    stop() persists emptiness
28. adapter: a rejecting save() is contained -- the adapter keeps running,
    no unhandled rejection
29. adapter: the restore outcome is observable before the first observer
    attaches

**test/persist-conformance.test.js** (new): Part A -- byte-shape round-trip
of a mixed corpus (plain + infinite + 0-entry cache + a ~1 MB payload),
3 tests. Part B -- the driver dropsWholePayload(state) over MATRIX rows (one
test() per row, name `B row N: <case> -> <reason>`): (1) state null;
(2) state an array; (3) state a string; (4) entries missing; (5) entries not
an array; (6) extra top-level key; (7) entry not an object; (8) key missing /
not an array; (9) entry with a 5th key; (10) dataUpdatedAt NaN / Infinity /
string / missing; (11) infinite missing; (12) infinite non-boolean;
(13) infinite: true with non-array data; (14) infinite: true with data:
null; (15) data: undefined on a plain entry; (16) duplicate key hashes;
(17) one bad entry among three good ones -> nothing hydrates. Plus two
legal, non-refusing rows pinned separately (bake's row-8/17 analogue):
{ entries: [] } hydrates as ok: true, count: 0; a legal null DATA VALUE
hydrates fine (null is a value, not a missing field). Part C -- the
fail-open mergeHydrate witness that seeds what it can, rejected on 100% of
refusal rows, with MATRIX.length pinned to a literal.

## E. STOP -- resolved by the operator rulings above

- STOP-1 -> ON-1 (ratified; C2 unblocked).
- STOP-2 -> ON-2 (ratified; BRIEF.md amended, CHANGELOG records it).
- STOP-3 -> ON-3 (ratified: throw/return asymmetry, finite-future clamp with
  non-finite = whole-payload drop, flush-on-stop with clear()+stop() as the
  logout path).
