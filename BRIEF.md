# BRIEF -- Q5 -- lite-query v1.3.0 -- infiniteQuery + prefetch

Session Q5 of ROADMAP.md (section 5). Q4 is SHIPPED (1.2.1 published
e7f69bd, artifact verified; the suite-law torture harness is live).
Depends_on Q3+Q4: both satisfied. Environment verified 2026-09-02: all
five peers installed = registry (signal 1.5.0, await 1.3.0, stream 1.3.0,
leak 1.10.0, gc-profiler 1.16.0). Base suite: 181/0/0.

---
package: "@zakkster/lite-query"
version_target: 1.3.0
status: planned
tests_min: 195        # 181 base + ~15 new (brief's old ">= 175" line is
                      # pre-rebase and superseded by this number)
skip_max: 0
gc_maxMajor: 0
gc_maxPauseMs: 4
torture: "law harness ok + pagination churn; EVERY control still trips"
findings: []          # anchored to the README facts table's own honesty row
carry_from_q4: findings-clause control (see OR-6)
depends_on: [Q3, Q4]
blocks: [Q6]
---

## PURPOSE

Delete the last honest "trails TanStack" row. The facts table says it
itself: "pagination is a Cookbook recipe rather than a built-in API".
Recipe 4 hand-rolls page accumulation on every user. Promote it: cursor
pagination first-class, plus qc.prefetch for route-loader warm-up (pairs
with lite-router). This was the old roadmap's 1.2; it is 1.3 here because
the /await surface bump took 1.2.

## TASKS

T1. `infiniteQuery(qc, { key, fetcher, getNextCursor, enabled?,
    cacheTime?, staleTime?, retry? })` -> handle with `pages()`, `data()`
    (flat), `fetchNextPage()`, `hasNextPage()`, `status()`, `error()`,
    `refetch()`, plus the standard observer lifecycle. Non-negotiable
    design constraints:
      * One cache entry per infinite query (the pages array IS the data),
        living in the same queryClient cache: getQueryData / invalidate /
        removeQueries / cross-tab behave uniformly, exactly like
        streamQuery did it (entry monomorphism precedent: uniform slots,
        no second hidden class).
      * fetchNextPage while a page is in flight: dedup, not double-fetch
        -- reuse the entry.promise guard discipline.
      * invalidate refetches from page one and replaces the array on
        success (stale-while-revalidate over the whole list; no partial
        page mixing from two generations -- generation guard extends to
        the pages array).
      * Abort vocabulary: unmount/key-change abort mid-page fetch exactly
        as query() does; a late page from a dead generation is swallowed.
T2. `qc.prefetch(key, fetcher, opts?)`: fetch into cache with zero
    observers, standard cacheTime GC afterwards; a later query() adopts
    the entry (no refetch if fresh). Freshness decision PINNED by OR-4.
T3. Cross-tab: pages sync via the existing entry broadcast (it is one
    entry); sharedFetch leader dedup applies per page fetch. Assert in
    the fuzzer.
T4. Docs: README facts-table row flips; API section; llms.txt; d.ts.
    Cookbook recipe 4 becomes "use infiniteQuery; here is the manual
    version if you need custom accumulation". Recipe 5 gains the
    qc.prefetch form. CHANGELOG [1.3.0] head (facts + measured numbers;
    the version stamp itself is the drill's -- OR-1).
T5. Tests ~15 (counted suite): accumulation order, hasNextPage false on
    null cursor, dedup on concurrent fetchNextPage, invalidate-refetch
    generation safety, abort on detach mid-page, prefetch adoption,
    cross-tab page sync.
T6. Torture (additive per OR-2): pagination churn joins
    bench/torture/query-soak.mjs (mount/unmount infinite handles,
    fetchNextPage storms, invalidate races); phase H warm-read loop
    extended to cover pages()/data() reads at zero allocation; law
    harness green end-to-end, every existing control still trips.

## HOT PATH

pages() and data() reads on an attached observer allocate nothing (return
the stored array/flat view; flatten on write, not on read). fetchNextPage
is a cold path and may allocate its promise; the accumulation append must
not re-copy the whole pages array per page beyond the one structural
append the cache write requires.

## OPERATOR RULINGS

- OR-1 (version stamp -- the PR-1 precedent, now standing law): no coder
  commit touches `"version"` in package.json or `VERSION` in Query.js.
  Both stay 1.2.1 all session; version-sync.test.js stays green; the
  CHANGELOG gains its [1.3.0] head in-session and the /release 1.3.0
  drill performs the stamp across its sites.
- OR-2 (torture is additive): the Q4 law harness is frozen surface --
  GATE line format byte-compatible, no seed/Q4 assertion weakened or
  renumbered, all three controls still trip individually. Q5 extends:
  query-soak.mjs gains the pagination churn (it is a phase P child; its
  oracle must be able to fail -- if the churn adds a gate, it follows the
  Q4 control conventions: a QUERY_TORTURE_BREAK value, honest unknown-
  value errors, "DID-NOT-TRIP" reserved for genuine non-trips). The
  phase H extension covers warm pages() reads under the same
  { maxMajor: 0, maxPauseMs: 4 } rules.
- OR-3 (entry monomorphism is law): infinite entries use uniform slots
  exactly as streamQuery's stream slots did -- no second hidden class on
  cache entries, and a plain query() entry pays zero new allocation for
  the feature's existence.
- OR-4 (prefetch freshness PINNED): prefetch of an already-fresh entry is
  a NO-OP. A `force` variant is deferred until a consumer exists; the
  no-op decision and the deferral are recorded in the docs (llms.txt
  decision note), not just implied.
- OR-5 (scope): the bake-stream RangeReader Cookbook recipe does NOT ride
  Q5 -- it is deferred to Q6's docs pass (parking ledger updated then).
  Q5's diff stays surface-focused.
- OR-6 (carry from Q4): the findings-clause control. The kernel audit()
  path could not fire through the 1.2.x public surface under OR-zero-
  source rules. Q5 CHANGES the surface: the planner includes an explicit
  task to attempt a legal trigger via infiniteQuery/prefetch teardown
  paths; if it fires, control it (Q4 conventions); if not, re-record in
  INCONCLUSIVE.md with the attempt described and carry to Q6. Either
  outcome appears in the final QA-verified report.
- OR-7 (environment): peers verified current this session; no floors
  work, no npm install unless a new devDep is chartered (none is).
- OR-8 (architecture decision is the planner's, with reasons): where
  infiniteQuery lives -- a core Query.js export (like mutation) or a new
  subpath pair (like ./stream) -- is the planner's call, argued against
  the law (single PascalCase main file, sideEffects:false, tree-shaking,
  entry monomorphism) and the precedent that qc.prefetch is unavoidably
  core (it hangs off the client). If a new shipped file pair results,
  files[] grows and the pack-count gate asserts the NEW exact number.

## GATES

G1 `npm test` -> >=195 pass / 0 fail / 0 skipped. G2 law harness:
`node --expose-gc test/torture.mjs` -> GATE line + "ok", exit 0. G3 every
QUERY_TORTURE_BREAK control (plus any Q5 addition) individually non-zero
with `tripped:`. G4 `npm run torture` green; prepublishOnly runs it.
G5 `npm pack --dry-run` -> exact expected file count (13, or 13+2 per
OR-8), test/ + bench/ + INCONCLUSIVE.md absent, llms.txt + CHANGELOG
present. G6 ASCII guard green over the walked set. G7 fresh-clone drill
(scratchpad) incl. law gate + controls. G8 tree clean; no
PLAN/BRIEF/ROADMAP and no version/VERSION token in any coder commit.
G9 drift guards green after the facts-table flip (the guards exist to
catch exactly this class of edit -- run them, do not reason about them).

## NON-GOALS

No bidirectional pagination (fetchPreviousPage) until a consumer exists
-- record the deferral. No focus/reconnect triggers (parked). No
persistence of pages (Q6 decides how infinite entries serialize). No
bake-stream recipe (OR-5, rides Q6). No demo showcase work (the drill
bumps the footer only).

## DONE WHEN

cursor pagination is first-class, cache-uniform, cross-tab-correct, and
the comparison table needs no apology row; 1.3.0 drill-ready (publish is
the operator's).
