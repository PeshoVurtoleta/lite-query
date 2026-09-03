# BRIEF -- Q10 -- lite-query v2.2.0 -- freshness + polish (the gap session)

Operator contract for the Q10 pipeline (planner -> coder -> reviewer -> qa).
Source charter: the 2026-09-03 gap assessment vs @tanstack/query-core
5.102.8 (installed) -- the three highest-value absences that fit the house
law, plus the comparative bench refresh the assessment proved stale (bench/
bench.mjs untouched since the initial commit; 8 of 11 post-1.2 surfaces
absent from bench/). This file wins over memory; the ROADMAP parking ledger
(~:1225-1240) wins over this file only where this file is silent.

---
package: "@zakkster/lite-query"
session: Q10
status: in-pipeline (2026-09-03)
version_target: 2.2.0       # stamped by the /release drill, NOT in-session (OR-1)
tests_min: 410              # floor; suite is 391 (shipped 2.1.0) at session start
skip_max: 0
torture: "law harness ok + interval-churn and placeholder-swap soaks +
  attempt G on interval teardown with live timers"
depends_on: [Q9 (shipped 2.1.0, operator publish 051b9ed)]
downstream: none gated (the last session)
carry_from_q9: "findings-clause torture control STILL uncontrolled after
  SIX attempts (Q5 A/B, Q6 C, Q7 D, Q8 E, Q9 F; INCONCLUSIVE.md holds all
  six verbatim); attempt G rides the interval surface -- recurring armed
  timers + refcount-gated teardown is a genuinely NEW retention shape
  (OR-9)"
---

## PURPOSE

Close the three competitive gaps that are real, valuable, and compatible
with the law -- polling, previous-data hold, a page bound -- and bring the
comparative benchmark up to the 2.1.0 surface so every headline claim is
re-measured, not inherited. Everything opt-in, everything additive; the
OFF path is byte-identical to 2.1.0 on all three features.

## TASKS

- T0 -- THE SPIKE, before any spec (OR-2). Three design rulings against
  the REAL code, then STOP-DECISION-1 (ship all three, or defer any whose
  ruling is unsafe -- a named ruling in the ledger, never a doc warning):
  (a) interval timer discipline -- the ratified watchdog design note
      governs (ROADMAP ~:1059-1062: never per-value clearTimeout/
      setTimeout churn; lastX + one periodic check-and-rearm timer,
      amortized O(1)). Rule the lifecycle: refcount-gated (zero observers
      = zero polling), enabled:false = no polling, dispose leaves no
      dangling timer (mock-clock-provable), and the interval fires
      through the NORMAL fetch path so dedup/retry/abort/sharedFetch all
      apply unchanged.
  (b) placeholder semantics -- where held data lives. The CACHE never
      lies (OR-5): previous data held across a reactive key swap is a
      HANDLE-level presentation with an explicit isPlaceholder() flag;
      entry state, dehydrate, persist, and the feed reflect truth. Rule
      what status()/loading() read during the hold, the equals
      interaction, and whether infiniteQuery is in scope this session.
  (c) maxPages -- the refetch hazard decides it. Our generation-guarded
      refetch replays the cursor chain; rule whether a drop-oldest page
      window retains provably-correct refetch (what exactly does refetch
      replay, from which page, with which cursor). If the bound can
      corrupt refetch, DEFER maxPages by named ruling (OR-6).
- T1 -- refetchInterval per ruling (a). Opt-in per query (client-default
  admissible if the planner rules it); OFF path byte-identical, zero
  added allocation.
- T2 -- keepPreviousData per ruling (b). Opt-in flag; isPlaceholder() on
  the handle; scope (query only vs + infinite) per spike.
- T3 -- maxPages per ruling (c), IF it survives the spike.
- T4 -- bench refresh. New comparative scenarios vs @tanstack/query-core
  where a true counterpart exists: prefetch vs prefetchQuery; full
  dehydrate -> hydrate cycle; persister save/load; feed-installed
  overhead vs QueryCache.subscribe listener; infiniteQuery fetchNextPage;
  queue enqueue + replayQueue vs paused mutations + resumePausedMutations
  (philosophy-differing -- labeled as such in the output, OR-7). Keep the
  existing fairness asserts (same fetcher/keys/observer pattern, both
  libraries run full paths, runtime version print). Solo-only surfaces
  (sharedStream, projectFrame) stay in torture -- no fake comparisons.
- T5 -- Torture. Interval-churn soak (arm/disarm across observer
  refcount churn; teardown with live armed timers; zero dangling timers
  at drain) + placeholder-swap churn; both strictly AFTER the frozen
  GATE evaluation (ON-2 precedent). Attempt G per OR-9.
- T6 -- Docs. README/llms.txt/Query.d.ts/Cookbook (polling recipe gains
  the in-library option; a keepPreviousData paginated-table recipe);
  CHANGELOG `[2.2.0]` head in-session, Added/Changed only; fix the
  llms.txt:245 staleness ("deferred to 1.x" while shipped is 2.1.0);
  every headline bench number in llms.txt/README regenerated from the T4
  run's measured output IN THE SAME COMMIT as the bench change (OR-7).

## OPERATOR RULINGS

- OR-1 (standing PR-1). No in-session version stamp. package.json and
  the Query.js VERSION const stay 2.1.0; the CHANGELOG `[2.2.0]` head
  lands in-session; the /release 2.2.0 drill performs the stamp.
- OR-2 spike-first. The planner returns, in ONE message: the three T0
  rulings with named code evidence, STOP-DECISION-1, and the FULL spec
  for the recommended scope (any deferral recorded explicitly).
- OR-3 timer law. No per-value or per-tick timer churn on any recurring
  path; one periodic check-and-rearm timer amortized O(1) (the watchdog
  precedent); every timer refcount-gated and provably torn down (the
  soak asserts zero dangling mock-clock timers at drain).
- OR-4 the shared-polling story must be TRUE. "The leader polls once and
  N tabs stay fresh" ships as documentation ONLY if the mechanism
  guarantees it under sharedFetch -- and the Q8 liveness law still
  governs verbatim: a follower with no live leader MUST still self-fetch
  within the bound. Correctness never depends on election state.
- OR-5 placeholder truthfulness. The cache never lies: held previous
  data is handle-level presentation with an explicit isPlaceholder()
  flag; dehydrate/persist never capture placeholder state; feed events
  reflect entry truth, not the hold.
- OR-6 maxPages fail-closed. A page bound that can corrupt refetch is
  REJECTED by named ruling and deferred -- never shipped behind a doc
  warning. If shipped, the spike's hazard is pinned as a named test.
- OR-7 bench honesty. Comparative numbers only where a true counterpart
  exists; philosophy-differing pairs labeled in the printed output; no
  cherry-picked N; every published headline number regenerated from
  measured output in the same commit that changes the bench (the QD-4
  same-commit law extended to bench claims). bench/ stays out of the
  pack (13 files unchanged).
- OR-8 additive minor. The shipped 391 tests are frozen contracts --
  none edited, none retired; zero breaking budget; all three features
  strictly opt-in with OFF paths byte-identical in behavior AND
  allocation to 2.1.0.
- OR-9 (carry_from_q9). One honest findings-clause attempt G through the
  interval surface: teardown with live armed recurring timers under
  observer churn. Verbatim outcome in INCONCLUSIVE.md; a control that
  cannot trip is decorative -- never fake one.
- OR-10 feed discipline. No new event types unless the spec forces them
  (an interval-triggered fetch SHOULD reuse the existing fetch:*
  vocabulary -- a new `reason` VALUE is additive data, not vocabulary);
  the 10-key record stays frozen; any enumerated-values doc site moves
  same-commit (QD-4 law).

## GATES

- G1 suite >= 410 pass, 0 fail, 0 skip, under the default `npm test`.
- G2 law harness: the byte-frozen GATE line then `ok`; every control
  still trips (5 + any honest attempt-G addition).
- G3 interval contract, named tests: zero observers -> no timer armed;
  enabled:false -> none; dispose -> zero dangling mock-clock timers;
  the interval dispatch runs the normal fetch path (dedup/retry/abort
  provably apply).
- G4 shared-polling truthfulness: leader + followers under sharedFetch
  with intervals -- upstream fetch count === the leader's alone while
  the leader serves within the bound; a follower with no leader
  self-fetches (the liveness law, asserted).
- G5 placeholder: a reactive key swap holds previous data with
  isPlaceholder() true until settle; entry/dehydrate/feed reflect truth
  throughout; OFF path byte-identical.
- G6 maxPages (if shipped): pages provably bounded, drops counted and
  documented, refetch of a bounded list pinned correct by the spike's
  hazard test.
- G7 bench: the refreshed suite runs green against the installed
  query-core; fairness asserts present in output; the new scenarios
  exist; llms.txt/README headline numbers match THIS run's output in
  the same commit.
- G8 drift guards green; `npm pack --dry-run` = 13 files; core import
  discipline unchanged (Query.js imports lite-signal only).
- G9 docs: llms.txt:245 fixed; every count/claim site reconciled per
  QD-4; ascii law holds.

## NON-GOALS

- Focus/reconnect revalidation -- stays PARKED per the ledger (consumer-
  driven rider, not this session).
- Bidirectional fetchPreviousPage -- only the bound (if safe), never the
  API without a consumer (the Q5 ruling stands).
- No select/data transforms (lite-signal computed() is the answer), no
  networkMode, no connectivity ownership (Q9 OR-4 carries), no new
  subpath, no feed record-shape change, no version stamp in-session.

## DONE WHEN

The spike-ratified feature set is green with OFF paths byte-identical;
the shared-polling story is mechanically true and asserted; the bench
compares the 2.1.0 surface honestly with regenerated headline numbers;
suite >= 410 with the GATE byte-identical; `[2.2.0]` head landed;
llms.txt:245 truthful. Then: awaiting /release 2.2.0 + operator publish
per OR-1 -- and the ladder rests.
