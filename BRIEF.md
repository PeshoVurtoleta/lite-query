# BRIEF -- Q6 -- lite-query v1.4.0 -- persistence: dehydrate / hydrate / adapter

Operator contract for the Q6 pipeline (planner -> coder -> reviewer -> qa).
Source charter: ROADMAP.md section 5, Q6. This file wins over memory; the
ROADMAP wins over this file only where this file is silent.

---
package: "@zakkster/lite-query"
session: Q6
status: pipeline-complete (2026-09-02; C1-C8 + QD-1..QD-5 fixes; reviewer APPROVED x2, QA PASS; suite 268, GATE byte-identical; awaiting /release 1.4.0 + operator publish per OR-1)
version_target: 1.4.0        # stamped by the /release drill, NOT in-session (OR-1)
tests_min: 218               # floor; suite is 203 at session start
skip_max: 0
torture: "law harness ok + 4096-cycle dehydrate/hydrate/teardown phase"
depends_on: [Q5 (shipped 1.3.0, operator publish 855214e)]
blocks: [Q7]
downstream: "bake-stream MX1/MX2/MX3 charter -- MX1's tripwire is 'lite-query Q6 ships'"
carry_from_q5: "findings-clause torture control still uncontrolled (INCONCLUSIVE.md holds the two Q5 attempts); re-attempt via persistence teardown paths (OR-11)"
---

## PURPOSE

Instant cold start, fail-closed. Serialize resolved entries, hydrate on boot
before the first observer attaches, render instantly, revalidate per normal
staleness rules. One primitive (dehydrate/hydrate) and one adapter on top
(persistQueryClient) -- built in that order. Highest UX leverage per line in
the plan, and the first Q session with a named downstream consumer: bake-stream
is fully developed at 1.7.1 and its next charter (MX1/MX2/MX3) arms on this
session shipping.

## TASKS

- T1 -- Primitive. `qc.dehydrate() -> state`: plain-JSON-able snapshot of
  SUCCESS entries only -- per entry: key, data, updated-at timestamp (reuse the
  entry's existing staleness-math timestamp slot; planner names it). Never
  pending/error entries (a promise is not data), never stream entries (a
  connection is not data) -- both exclusions documented AND tested.
  `qc.hydrate(state)`: pre-observer seeding under OR-2/OR-3; seeded entries
  arrive stale-aware so staleTime math is honest across reloads. Hydrate never
  broadcasts (no cross-tab echo); the documented boot order is: create qc ->
  hydrate -> attach observers -> crossTab settles via normal invalidation.
- T2 -- Adapter. `persistQueryClient(qc, { save, load, version, throttle? })`
  per OR-5/OR-6/OR-7: subscribes to cache writes through a PRIVATE single-slot
  hook (one null-test branch on write paths; the public feed is Q7's),
  throttles serialization, stamps `{ version, state }`, restores on install.
- T3 -- Infinite entries round-trip per OR-4: dehydrate marks them and carries
  pages; hydrate rebuilds pages + flat through the existing rebuild discipline;
  cursor/hasNext recompute at first infiniteQuery attach. A restored list must
  continue paginating correctly (fetchNextPage picks up from the restored
  pages).
- T4 -- Conformance/corruption suite: our dependency-free copy of the
  cache-shaped corruption matrix (mirror the SHAPE of bake-stream's
  `../LiteBakeStream/test/DehydratedCache.test.js`; import nothing from bake).
  Every corrupted/malformed payload class refuses to hydrate (OR-3) with the
  whole payload dropped and the outcome observable.
- T5 -- Torture. Phase H gains the 4096-cycle dehydrate/hydrate/teardown loop
  (serialization is the classic accidental-retention factory); pool baseline +
  law harness green; GATE line byte-frozen. OR-11 findings-clause attempt.
- T6 -- Docs pass. README (facts table gains its persistence row -- TanStack:
  plugin; SWR: manual), llms.txt (the "No SSR hydration" and "No persistence
  of infinite pages" scope lines flip), d.ts for every new surface, CHANGELOG
  `[1.4.0]` head (records every OR decision), Cookbook: (a) persistQueryClient
  recipe with localStorage AND IndexedDB thunks; (b) the bake-backed variant
  per OR-9; (c) the streamQuery + RangeReader recipe per OR-10.

## OPERATOR RULINGS

- OR-1 (standing PR-1). No in-session version stamp. package.json and
  Query.js VERSION stay 1.3.0 (the sync guard couples them); the CHANGELOG
  `[1.4.0]` head lands in-session; the /release 1.4.0 drill performs the stamp
  across its sites after the pipeline closes.
- OR-2 (the charter's recorded DECISION, resolved: reject). hydrate is
  boot-only and fail-closed: `qc.hydrate(state)` THROWS if the cache holds ANY
  entry -- observed or not. Empty cache is the precondition and the error
  message names it. Rationale: the strictest checkable form of reject; a late
  hydrate overwriting live data is the ABA bug of this domain, and an entry
  count of zero is cheaper and less ambiguous than any observer-based rule.
  BroadcastChannel delivery is asynchronous -- the real channel a task, the
  mock in test/harness.js a MICROTASK (planner V1 verified this and corrected
  the original "macrotask" wording here) -- so a strictly synchronous boot
  (create qc -> hydrate, no await between) cannot observe a remote entry under
  either, and the mock is the stricter of the two. An awaited boot (any async
  load()) legitimately CAN hit the precondition and resolves as a drop per
  OR-6 -- a pinned contract, not flake. Incremental / multi-shot hydration is a deferred
  consumer-driven variant (NON-GOALS), not a softer default.
- OR-3 all-or-nothing hydration. One malformed entry -- wrong shape,
  non-array pages on an infinite-marked entry, non-finite timestamp, anything
  unverified -- drops the WHOLE payload: nothing hydrates, the outcome is
  observable, no partial state. Matches the version-mismatch discipline and
  bake's "never hydrated wrong" law. `load()` returning null/undefined is an
  EMPTY store (normal boot, not an error); any other malformed payload is a
  drop. null is not zero.
- OR-4 infinite serialization. Snapshot marks infinite entries and carries
  the raw pages array; cursor and hasNext are NOT serialized -- getNextCursor
  lives in code, functions never serialize, so both recompute from the restored
  pages at first infiniteQuery attach (the same rebuild discipline
  setQueryData(key, pagesArray) already uses). If the planner finds
  recompute-at-attach unsound, that is a STOP item for the operator, not a
  license to store cursors.
- OR-5 the adapter is a CORE export from Query.js. Storage-agnostic
  save/load thunks only; zero new deps of ANY kind; bake wiring lives entirely
  in Cookbook thunks (no subpath, no optional peer -- the Q5 prefetch
  precedent). The write seam it subscribes to stays PRIVATE.
- OR-6 adapter contract. `version` is REQUIRED, no default. On restore:
  version mismatch or malformed payload -> drop everything, fetch fresh. The
  restore outcome is observable to the caller (planner specs the shape -- a
  settled promise or status accessor); an async `load()` that resolves after
  entries exist hits OR-2's precondition and resolves the outcome channel as a
  drop -- never a merge, never an unhandled rejection, never a silent nothing.
- OR-7 throttle. Uses the client's injectable setTimeout/clearTimeout/now
  (mock-clock testable); trailing-edge coalescing; `stop()` uninstalls the
  hook and clears any pending timer. Flush-on-stop vs drop-on-stop: planner
  recommends with rationale; the choice is recorded in the CHANGELOG head.
- OR-8 zero-cost-when-unused is a GATE, not a hope. With no adapter
  installed the warm path allocates exactly what it does at 1.3.0 and phase H
  holds the byte-frozen GATE line. Entry monomorphism unchanged: no new
  per-entry slots unless proven necessary -- the staleness timestamp already
  exists; REUSE it. Dehydrate walks the cache and is cold by definition; its
  doc comment says so.
- OR-9 bake floors. The Cookbook bake variant cites bake-stream's llms.txt
  "Consumer floors:" sentence VERBATIM (their llms.txt, the line beside the
  persistQueryClient recipe sketch, ~line 98) -- never restate version numbers
  from our ROADMAP; session numbers are never semvers. The crossover guidance
  is quoted per their line ("below the crossover, use plain JSON" in those
  words) and the recipe carries their getJSON-SyntaxError rule: a native
  SyntaxError means the stored record is not JSON -- discard the cache and
  re-fetch, never retry the same bytes.
- OR-10 (Q5's OR-5, honored). The streamQuery + RangeReader recipe rides
  THIS docs pass -- it was deferred here on the record. The ingest-progress
  recipe (parking ledger, bake recipe 2) MAY ride if the docs pass has room;
  if it does not, re-park it explicitly in the ROADMAP ledger -- no silent
  drops.
- OR-11 (carry_from_q5). One honest findings-clause control attempt through
  the NEW teardown surface (candidate: adapter stop with a pending throttled
  save and a tracked payload handle). Outcome recorded in INCONCLUSIVE.md in
  the same verbatim-attempt style as Q5's two attempts; a control that cannot
  trip is decorative -- never fake one, never add a ctl that cannot fire.

## GATES

- G1 suite >= 218 pass, 0 fail, 0 skip, under the default `npm test`.
- G2 law harness: `node --expose-gc test/torture.mjs` prints the byte-frozen
  GATE line (`GATE leak=size 0/0 findings=0 warnings=0 | gc major=0 minor=0
  maxMs=0.00 | ok`) then `ok`; every live control still trips
  (alloc/detach/fuzz/pages + any Q6 addition).
- G3 round-trip: dehydrate -> NEW client -> hydrate -> getQueryData identical
  for every success key, and staleness decisions identical to a client that
  never restarted (mock clock proves both).
- G4 a version bump drops 100% of stale-schema payloads; every row of the T4
  corruption matrix refuses with the whole payload dropped.
- G5 phase H: the 4096-cycle dehydrate/hydrate/teardown loop shows zero
  retained growth; pool baseline holds (activeLinks === 0, entry map drained).
- G6 infinite round-trip: pages + flat identical after restore; hasNextPage
  correct after first attach; a post-restore fetchNextPage appends page N+1
  (not page one) exactly once.
- G7 drift guards green: ascii guard over every new/edited file; surface guard
  sees every new export in llms.txt + d.ts.
- G8 no-adapter baseline: warm-path allocation and the GATE line are
  unchanged from 1.3.0 with persistence unused.
- G9 `npm pack --dry-run`: 13 files, test/ + bench/ + INCONCLUSIVE.md + this
  file absent, llms.txt + CHANGELOG.md present.

## NON-GOALS

- No storage implementations shipped (thunks only; recipes show
  localStorage / IndexedDB / bake).
- No partial, selective, incremental, or multi-shot hydration (OR-2 is
  single-shot into an empty cache; softer variants wait for a named consumer).
- No offline mutation queue (Q8 -- replay semantics, not persistence).
- No focus/reconnect triggers (still parked; still no consumer).
- No bake-stream subpath, peer, or import anywhere in shipped or test code.
- No public inspect/feed API (the write seam stays private until Q7).
- No dehydration of pending, error, or stream entries -- ever, in any mode.

## DONE WHEN

A reload renders from cache instantly and revalidates honestly; a schema bump
can never resurrect stale shapes; an infinite list restores and keeps
paginating from where it stopped; suite >= 218 with the GATE byte-identical;
CHANGELOG head landed. Then: awaiting /release 1.4.0 + operator publish per
OR-1 -- and on publish, bake-stream's MX1 tripwire arms.
