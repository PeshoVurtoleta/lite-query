# PLAN.md -- Q5 -- @zakkster/lite-query v1.3.0 -- infiniteQuery + prefetch

OPERATOR: ratified 2026-09-02 as delivered by the planner, with four notes.

ON-1 (commit ladder imposed -- the planner delivered ordered tasks; the
coder groups them into THESE rungs, each green, no red commits):
- C1 runtime core: tasks 1-4 + 6 + 7 (entry slots, commitPage/resetPages,
  runFetch branch, infiniteQuery, invalidate arm, setQueryData rebuild)
  landing WITH the ~13 unit tests that prove them (from task 8's list,
  minus the prefetch/cross-tab ones). Prove: npm test green (>=181+those),
  law harness ok.
- C2 prefetch: task 5 + its tests (prefetch-adoption-zero-refetch,
  prefetch-fresh-is-noop, prefetch-entry-GCs-at-cacheTime) + the
  cross-tab-page-sync test. Prove: full 17 landed, npm test >= 198/0/0.
- C3 zero-gc contract: task 9. Prove: test green under --expose-gc.
- C4 torture: tasks 10-13 (phase H extension, soak churn + pages oracle,
  ctlPages, OR-6 attempts) + ON-2 below. Prove: GATE byte-identical,
  4 controls individually tripped, OR-6 outcome recorded.
- C5 docs: task 14. Prove: drift guards run green (G9), ON-3 satisfied.
- C6 CHANGELOG: task 15, placeholders filled from C1-C5 measured runs.
Post-ladder: G7 fresh-clone drill (scratchpad), G8 tree/scope check.

ON-2 (gap fixed -- MANDATORY in C4): the BRIEF T3/ASSERTIONS fuzzer proof
is missing from the task list. Extend bench/torture/shared-fetch-soak.mjs
(the sharedFetch oracle lives there): two tabs, one fetchNextPage storm on
a shared infinite key -- assert followers converge to identical pages
arrays AND the leader fetched each page exactly once (per-page dedup).
The oracle must be able to fail; if it gains a break value, Q4 conventions
apply.

ON-3 (the RISK is a doc MUST): data() returns a live, growing array
(in-place flat append + NEVER_EQUAL notify). The aliasing contract --
"copy if you retain across pages" -- MUST land in Query.d.ts JSDoc AND
llms.txt in C5, not as an afterthought.

ON-4 (floors): the unit floor is >= 198 / 0 fail / 0 skip (181 + 17);
BRIEF's 195 is the minimum, 198 is the plan's own claim -- G1 asserts 198.

Standing law (BRIEF OR-1..OR-8 all bind; restated hot items):
- OR-1: NO version/VERSION token change anywhere in the diff; CHANGELOG
  [1.3.0] head lands in C6; the /release 1.3.0 drill stamps.
- OR-2: GATE line byte-compatible; existing controls untouched and still
  tripping; additions follow Q4 conventions (honest unknown-value errors,
  DID-NOT-TRIP reserved).
- OR-3: entry monomorphism -- the 7 new slots are null/false/0 on every
  plain entry, one hidden class, zero new signal nodes.
- PLAN/BRIEF/ROADMAP in no coder commit. ASCII-only. node:test only.

---

## PLANNER SPEC (ratified verbatim)

Hot body unchanged -- `Query.js:query` accessors, `attach`, `detach`,
`runFetch`'s success block gain no new branch on plain entries;
`createEntry` gains 7 uniform slots (`isInfinite:false, pages:null,
flat:null, nextCursor:null, hasNext:false, pageGen:0, getNextCursor:null`),
all null/false/0, one hidden class, zero new signal nodes (pages ride
`entry.data`, flat is non-reactive like `streamCount`). Cold path takes
everything else: `commitPage` (in-place `pages.push` + loop-append into
`flat`, no O(n) re-copy; `entry.equals = NEVER_EQUAL` so same-ref
`data.set` notifies), one `if (entry.isInfinite)` in `runFetch`'s
resolution, `pageGen` guard blocking two-generation mixing, `invalidate`
reset-to-page-one, `prefetch` via `ensureEntry`+`maybeFetch`.

ARCHITECTURE (OR-8): core `Query.js` export, not a subpath -- `/stream`
exists only because lite-stream is an optional peer; infiniteQuery adds no
peer, `prefetch` is unavoidably core, named ESM + `sideEffects:false`
already tree-shakes it, and the slots live in `createEntry` regardless.
files[] unchanged; G5 asserts pack = 13.

## TASKS (grouped per ON-1)

1. `Query.js:createEntry` -- 7 slots; `Query.js:disposeEntry`/`detach`
   reset them (no new signal to dispose).
2. `Query.js:commitPage` + `Query.js:resetPages` (cold, module-level).
3. `Query.js:runFetch` -- single `isInfinite` branch to `commitPage`;
   reuse `fetchGen`, `ABORT_REASON.DETACH/REFETCH/REMOVED/TIMEOUT`, retry,
   timeout, `pendingRefetchAfterCurrent`, sharedFetch broadcast.
4. `Query.js:infiniteQuery` -- watcher/`trackObserver`/`cleanupObserver`
   cloned from `query()`; `pages() data() hasNextPage() fetchNextPage()
   status() error() refetch() dispose()`; `fetchNextPage` dedups on
   `entry.promise`.
5. `Query.js:queryClient.prefetch` + `_internal` unchanged surface; OR-4
   no-op = no fetch and no GC re-arm.
6. `Query.js:invalidate` -- `isInfinite` arm: `++pageGen`, refetch page
   one, replace on success.
7. `Query.js:setQueryData` -- rebuild `flat` on remote/manual pages write
   (cross-tab sync of the single entry).
8. `test/query.test.js` -- 17 tests: accumulation-order, flat-order,
   hasNextPage-true-before-first, hasNextPage-false-on-null-cursor,
   concurrent-fetchNextPage-dedup, fetchNextPage-after-exhaustion-noop,
   invalidate-replaces-array, late-page-from-dead-pageGen-swallowed,
   detach-aborts-mid-page, key-change-aborts-mid-page,
   error-ladder-preserves-pages, refetch-forces-page-one, enabled-gate,
   prefetch-adoption-zero-refetch, prefetch-fresh-is-noop,
   prefetch-entry-GCs-at-cacheTime, cross-tab-page-sync.
9. `test/zero-gc.test.js` -- warm `pages()/data()/hasNextPage()` re-run
   budget.
10. `test/torture.mjs:runPhaseH` -- infinite handle prefilled 4 pages,
    read inside the 200000 HOT loop; GATE line byte-identical.
11. `bench/torture/query-soak.mjs` -- 32 infinite handles, fetchNextPage
    storms, invalidate races + gen/idx oracle; `QUERY_TORTURE_BREAK=pages`
    splices a foreign-gen page.
12. `test/torture/controls.mjs:ctlPages` -- spawns the soak with `pages`;
    ids `['alloc','detach','fuzz','pages']`; unknown-value text updated;
    `DID-NOT-TRIP` reserved.
13. OR-6: `test/torture/controls.mjs` attempt -- (A) dispose owner
    mid-page-fetch with handle still tracked
    (`createOwnerCascadeOrphanKernel`), (B) track the fetchNextPage
    promise, `removeQueries` mid-flight (`createAsyncRetentionKernel`).
    Fires -> `ctlFindings` under Q4 conventions + `INCONCLUSIVE.md`
    bullet resolved. Does not fire -> re-record both attempts verbatim,
    carry to Q6. Either way it lands in the QA report.
14. Docs: `README.md:60-67` pagination row + trailing-sentence flip and
    `:64` test count; `llms.txt:3,24,55,128,131,143-158` (delete both
    "no pagination" bullets, add OR-4 + no-fetchPreviousPage +
    no-page-persistence deferrals); `Query.d.ts:109,187`
    (`InfiniteQueryOptions/InfiniteQuery/infiniteQuery`,
    `QueryClient.prefetch`); `Cookbook.md:93-122` recipe 4 flipped,
    `:124-151` recipe 5 gains `qc.prefetch`. Plus ON-2's soak edit is C4,
    and ON-3's aliasing contract lands here.
15. `CHANGELOG.md` [1.3.0] head, facts skeleton, `<MEASURED>`
    placeholders filled from real runs. No version/VERSION token (OR-1).

## ASSERTIONS

- `npm test` -> >= 198 pass / 0 fail / 0 skip (181 base + 17);
  `npm pack --dry-run` -> exactly 13 files, no test/, bench/,
  INCONCLUSIVE.md.
- GC budget: `node --expose-gc test/torture.mjs` phase H holds
  `{maxMajor: 0, maxPauseMs: 4}` across 200000 warm reads that include
  `pages()/data()`; `test/zero-gc.test.js` < 50 B retained per effect
  re-run over 50000 re-runs.
- Retention: `tracker.size()` returns to 0 over 4096 build/teardown
  cycles with infinite handles in the churn; WeakRef census not 256/256;
  query-soak `entries.size === 0`, `activeLinks === 0`,
  `pendingCount === 0`.
- Controls: 4/4 print `tripped:` individually (`alloc`, `detach`, `fuzz`,
  `pages`); `QUERY_TORTURE_BREAK=pages` alone exits 1 with
  `FAIL: page mixing`.
- ON-2: shared-fetch soak proves two-tab page-storm convergence + leader
  fetched each page exactly once, and the oracle is proven able to fail.
- Every commit green (no red rungs), tree clean, no PLAN/BRIEF/ROADMAP in
  any coder commit, ASCII guard + surface guard green after the
  facts-table flip, fresh-clone drill in the scratchpad passes suite +
  law gate.

## RISK (doc MUST per ON-3)

`flat` is appended in place, so `data()` returns a live array -- aliasing
must be documented in `Query.d.ts` and llms.txt or a consumer holding an
old reference sees it grow.

## STOP

None -- `Query.js:196` `createEntry` is one object literal and the stream
slots at `:212-221` are positional-independent null/false/0 defaults, so
7 more slots keep the shape monomorphic without touching them.
