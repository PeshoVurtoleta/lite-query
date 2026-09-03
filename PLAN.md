# PLAN -- Q10 -- lite-query v2.2.0 -- ratified 2026-09-03

Planner spike + spec ratified with the amendments ON-1..ON-5 below. Where
an ON note conflicts with the spec text underneath, the ON note wins.
BRIEF.md (1e04b88) holds OR-1..OR-10; this file is the coder's source of
truth for WHAT to build.

## OPERATOR NOTES (amendments)

- ON-1 -- rulings (a) and (b) RATIFIED. Operator re-verified every
  deciding anchor at source (2026-09-03): the maybeFetch tail at
  Query.js:1365-1369 (shouldFetch gate, then requestSharedFetch for a
  follower / runFetch for leader-or-single-tab); the sharedFallbackTimer
  self-fetch with the observerCount guard at :1385-1394 (the liveness
  law's mechanism -- a leaderless follower still fetches); the watcher
  entry-swap seam at :2042-2050 (`entry !== attachedEntry` detach/attach
  block -- the pre-detach capture point for the hold). The interval
  dispatching through maybeFetch's tail inherits dedup/retry/abort/
  sharedFetch/liveness BY CONSTRUCTION -- that is the G4 mechanism.
- ON-2 -- ruling (c) maxPages DEFER RATIFIED, evidence verified and
  SHARPENED: refetch() replays the whole list from page one
  (Query.js:2378-2380 resets nextCursor=null/hasNext=true; commitPage
  stages fresh arrays on a page-one replace, :236-242) and exactly ONE
  cursor exists, derived from the LAST page (recomputeCursor,
  :268-276). Under a drop-oldest bound, a staleness refetch would
  repopulate from page one -- the retained window silently jumps from
  the TAIL of the list to the HEAD, and the flat append (:241) goes
  O(n) per page besides. The bound corrupts refetch; OR-6 defers it.
  ROADMAP parking-ledger sentence (C9 lands it verbatim):
  "maxPages page bound: DEFERRED (Q10 spike) -- forward-only refetch
  stores a single tail-derived cursor and replays from page one, so a
  drop-oldest window jumps from tail to head on every refetch;
  revisit only with bidirectional cursors (themselves still
  consumer-gated per the Q5 ruling)."
- ON-3 -- ladder completions the compressed spec left implicit:
  a TORTURE rung (interval-churn soak + placeholder-swap churn +
  attempt G, all printing strictly AFTER the frozen GATE; attempt G
  recorded verbatim in INCONCLUSIVE.md whatever the outcome), and the
  DOCS rung carries: BOTH Cookbook items (recipe 1 gains the
  refetchInterval option alongside the manual pattern; a NEW
  keepPreviousData paginated-table recipe), README claim/count sites,
  the llms.txt:245 replacement, the CHANGELOG [2.2.0] head, the ON-2
  ledger sentence into ROADMAP, and -- if llms.txt enumerates feed
  `reason` values anywhere -- the "interval" value added there
  same-commit (OR-10/QD-4).
- ON-4 -- option validation sharpened (the Q9 ON-3 lesson, fail closed
  at the door): `refetchInterval` absent or null = off; otherwise it
  MUST be a finite number > 0 -- 0, negative, NaN, Infinity,
  non-number each throw TypeError at resolveOptions/query construction.
  `keepPreviousData` strictly `=== true` to enable; absent/false = off;
  present-and-not-boolean throws TypeError. Lifecycle (pinned by named
  tests): no poll registration while enabled is false or observerCount
  is 0; register on attach when the option is set; unregister on
  detach, on enabled flipping false, and on dispose.
- ON-5 -- floors per area (base 391, +19 -> 410; QA freezes the final):
  interval option/lifecycle/dispatch +10, keepPreviousData +6, feed
  reason-value +1, teardown/retention +2. The bench rung adds no tests;
  its assertions are the green run + same-commit headline regeneration.

## PLANNER SPEC (verbatim, 2026-09-03; compressed form)

SPEC: Hot body unchanged -- no new branch in data()/status()/loading(),
commitPage, or any emit site; the keepPreviousData hold is two locals in
query() read only after `e.data() === undefined`, so equals/dehydrate/
feed never see it (dehydrate walks entries, not handles).
Cold path: resolveOptions gains refetchInterval; ONE client-wide array
scanner (watchdog precedent, ROADMAP:1059-1062) dispatching maybeFetch's
tail; maxPages DEFERRED.

RULINGS:
(a) scanner, not per-entry timers; dispatch = runFetch |
    requestSharedFetch (Query.js:1365-1369) so dedup/retry/abort/
    sharedFetch and the leaderless self-fetch (sharedFallbackTimer,
    :1385-1394) apply unchanged.
(b) hold at handle, isPlaceholder(), status()/loading() read entry
    truth; query() only this session.
(c) DEFER -- refetch sets nextCursor=null; hasNext=true (:2378-2380)
    and commitPage replaces from page one (:236); only ONE cursor
    exists (:257, :269-275), so a dropped head is unrecoverable and the
    flat append (:241) would go O(n). [ON-2 sharpens and ratifies.]

TASKS (ladder order; one commit per rung):
- C1 `Query.js:resolveOptions` -- refetchInterval (finite > 0 or null),
  fail-closed throw. [ON-4 shapes the validation.]
- C2 `Query.js:queryClient` -- pollList (null until first register --
  null is not zero), registerPoll/unregisterPoll refcount, swap-remove.
- C3 `Query.js:pollTick` -- ONE opts.setTimeout, period = min registered
  interval, nextPollAt scan, unref'd, re-armed only if list non-empty.
- C4 `Query.js:pollDispatch` -- maybeFetch tail; feed reason "interval"
  on fetch:dispatch (OR-10, no new event type).
- C5 `Query.js:query` watcher -- register at the entry !== attachedEntry
  seam (:2042-2050); stopWatcher/enabled:false/dispose unregister.
  G4 shared-polling truthfulness test lives here: leader + 3 followers
  under sharedFetch with intervals -> upstream fetches === the
  leader's alone across 20 ticks; kill the leader -> a follower
  self-fetches within sharedFetchTimeout (the liveness law, asserted).
- C6 `Query.js:query` -- heldData/isPlaceholder(), captured pre-detach,
  cleared on first defined e.data(); OFF path byte-identical.
- C7 `bench/bench.mjs` -- scenarios F prefetch vs prefetchQuery,
  G dehydrate -> hydrate cycle, H feed-installed vs QueryCache.subscribe,
  I fetchNextPage vs InfiniteQueryObserver, J queue enqueue+replayQueue
  vs paused mutations + resumePausedMutations (philosophy-differing,
  labeled in output per OR-7); fairness asserts preserved; EVERY
  headline number in llms.txt/README regenerated from THIS rung's
  measured run IN THIS COMMIT (OR-7).
- C8 torture -- interval-churn soak (10k arm/disarm cycles under
  observer churn; zero dangling mock-clock timers at drain) +
  placeholder-swap churn + attempt G (teardown with live armed
  recurring timers), all strictly after the frozen GATE; INCONCLUSIVE.md
  verbatim. [ON-3]
- C9 docs (last, one commit) -- Query.d.ts deltas; llms.txt (interval +
  placeholder sections, the :245 replacement, reason-value enumeration
  if present); README; Cookbook (recipe 1 option + the new
  keepPreviousData recipe); CHANGELOG [2.2.0] head (Added/Changed only);
  the ON-2 maxPages ledger sentence into ROADMAP. No version stamp
  (OR-1: package.json and VERSION stay 2.1.0). [ON-3]

ASSERTIONS (floor):
1. Suite >= 410 pass / 0 fail / 0 skip; GATE line byte-identical.
2. OFF path: refetchInterval unset -> pollList === null, 0 timers
   armed, bytes-per-op delta 0 vs 2.1.0 on the zero-gc harness.
3. Interval soak: 10k arm/disarm cycles, maxMajor 0 on the warm path,
   mock clock only (no real sleeps), < 8 B/op.
4. Retention: after dispose with live armed timers,
   harness timers.size === 0 and entries.size === 0 over 500 cycles.
5. G4: leader + 3 followers, 20 ticks -> upstream fetches === 20;
   leader killed -> follower fetches within sharedFetchTimeout.

RISK (documented, asserted): scanner granularity = smallest registered
interval; polls fire on-or-after their due time, never early.

REJECTED (with forcing clause):
- maxPages this session -- OR-6 via ruling (c)/ON-2.
- Per-entry interval timers -- OR-3 (timer law; watchdog precedent).
- A new feed event type for interval fetches -- OR-10 (reason value is
  data, not vocabulary).
- keepPreviousData on infiniteQuery this session -- ruling (b): the
  live flat array + NEVER_EQUAL discipline make a handle-level hold a
  different design; deferred with the reason recorded, not implied.
