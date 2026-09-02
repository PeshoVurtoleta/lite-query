# BRIEF -- Q8 -- lite-query v2.0.0 -- shared streams + offline mutations (the major)

Operator contract for the Q8 pipeline (planner -> coder -> reviewer -> qa).
Source charter: ROADMAP.md section 5, Q8 (lines ~940-1085, incl. the SPIKE
INPUTS recorded 2026-09-02). This file wins over memory; the ROADMAP wins over
this file only where this file is silent.

---
package: "@zakkster/lite-query"
session: Q8
status: shipped (2026-09-02; SPLIT ratified -- streams ship, queue -> Q9/2.1; C1-C13 + C10b BREAKING fix + QD-1..QD-4; reviewer DELTA APPROVED after one REJECTED (epoch-collision window), QA PASS after one docs FAIL(3); suite 362/0/0, GATE byte-identical, controls 5/5; drill sites=3/47 tests=362/0 GATE ok pack 13 files 108.0 kB; operator publish da8b377; registry artifact verified: 13 files, [2.0.0] head, VERSION sync, latest=2.0.0)
version_target: 2.0.0       # stamped by the /release drill, NOT in-session (OR-1)
tests_min: 354              # floor; suite is 314 at session start
skip_max: 0
torture: "law harness ok + leader-failover stream soak + 5-tab N-tab soak"
depends_on: [Q7 (shipped 1.5.0, operator publish 8b6743c)]
downstream: "lite-stream LS4 (v1.4.0, status: gated) -- THEIR gate IS our
  session-start spike ruling; recorded on both sides (their ROADMAP ~120-126:
  'Their spike is our tripwire'). The verdict lands in BOTH roadmaps before
  any follower-window code is written."
carry_from_q7: "findings-clause torture control STILL uncontrolled after four
  attempts (Q5 A/B, Q6 C, Q7 D; INCONCLUSIVE.md holds all four verbatim + the
  QD-4 postscript); attempt E rides the offline-queue teardown or the
  leader-failover surface (OR-10)"
---

## PURPOSE

The strategic sequel the 1.1.0 streaming work was built to enable, and the
LAST session of the eight. Shared fetch collapsed N tabs to one request; a
shared stream collapses N tabs to one SSE/websocket connection: the leader
owns the iterator and broadcasts frames, followers project them into their
local caches. Plus the offline mutation queue with replay-on-reconnect. Both
are semantics-heavy -- which is exactly why they ride a major, alone, AFTER
the Q7 feed exists to make their failure modes observable.

## TASKS

- T0 -- THE SPIKE, before any spec (OR-2). Paper-spike the failover matrix
  against the REAL sharedFetch/crossTab machinery in Query.js; confirm SPIKE
  INPUT facts 1-3 (charter lines ~1015-1023) against the actual design
  sketch; rule each LS4 candidate (push-writer / idleTimeout / share) as
  lite-stream vs lite-query vs nowhere, overturning a provisional verdict
  only with a named matrix cell; rule the SEQUENCING (OR-5); then
  STOP-DECISION-1: ship as one 2.0, or split (2.0 shared streams, 2.1
  offline mutations). The charter's lean is on the record: "Do not let the
  queue ride an unfinished failover story."
- T1 -- Shared streams. Leader owns the one iterator and broadcasts frames;
  followers project frames into their local entry (no follower ever holds an
  iterator). Reuse the sharedFetch machinery: same isLeader oracle, same
  fallback-timer liveness discipline (OR-3).
- T2 -- Failover matrix, each cell a NAMED test: leader closes gracefully /
  leader tab killed / leader hung (frames stop, channel alive) / follower
  promoted mid-buffer / two tabs racing promotion / stream completes during
  failover / stream errors during failover. A promoted follower starts a
  FRESH iterator; nothing adopts a dead leader's iterator mid-flight.
- T3 -- Buffer-mode semantics across tabs: followers receive the same
  windowed view discipline (maxBuffer, droppedCount) as a local stream; a
  slow follower cannot grow the leader's memory -- bound it, count drops,
  assert the bound in torture (G5).
- T4 -- Offline mutation queue, IF in the ratified scope (OR-6): explicit
  opt-in per mutation; durable via the Q6 persistence seam, version-stamped;
  replay preserves order per key, surfaces per-item results, and DROPS
  (never silently retries) items whose entries no longer exist; replay is
  observable via the Q7 feed.
- T5 -- Torture. Leader-failover stream soak + the N-tab soak: 5 simulated
  tabs, ONE upstream connection at all times (assert the connection count at
  the mock source), kill the leader every k ops, frames neither duplicated
  nor reordered in any follower, pool baseline in every tab. The frozen GATE
  window stays byte-identical (Q7 ON-2 precedent: new soaks print after the
  frozen gate evaluation). OR-10 attempt E.
- T6 -- Docs pass. README (shared streams + queue sections), llms.txt (the
  liveness sentence extended to streams and still TRUE, full new surface),
  d.ts, Cookbook recipes, CHANGELOG `[2.0.0]` head with the BREAKING section
  + migration notes per suite drill (OR-7). The LS4 verdict recorded in both
  roadmaps; if the push-writer survives, the parity assertion + frame-shape
  corpus are handed to lite-stream BEFORE LS4 codes (OR-5).

## OPERATOR RULINGS

- OR-1 (standing PR-1). No in-session version stamp. package.json and the
  Query.js VERSION const stay 1.5.0; the CHANGELOG `[2.0.0]` head lands
  in-session; the /release 2.0.0 drill performs the stamp after the pipeline
  closes.
- OR-2 spike-first process. The planner runs T0 and returns, in ONE
  message: (a) the spike verdict (facts confirmed/falsified, LS4 candidate
  rulings, sequencing ruling), (b) STOP-DECISION-1 -- its scope
  recommendation with rationale, and (c) the FULL spec for the RECOMMENDED
  scope (the deferred half's deferral recorded explicitly, never implied).
  The operator ratifies scope in the PLAN header before any code.
- OR-3 the liveness law, cited verbatim from llms.txt:180 (verified
  2026-09-02): "Liveness: if no leader serves within sharedFetchTimeout,
  the follower self-fetches. Correctness never depends on election state."
  That sentence is load-bearing for fetch and MUST stay true for streams: a
  follower that stops receiving frames within a bound self-connects. Any
  design whose correctness depends on election state is rejected by
  definition, not by review.
- OR-4 at-most-once projection. Frame LOSS on failover is permitted and
  documented; frame DUPLICATION and frame REORDERING are not -- both
  asserted in the matrix and the N-tab soak. No cell may weaken this to
  at-least-once to make a test pass.
- OR-5 the LS4 two-way gate. The spike's candidate verdicts land in BOTH
  roadmaps (ours here; lite-stream's via the live litestream peer session --
  the operator relays the verdict; direct edit of ../LiteStream/ROADMAP.md
  only as fallback). The SEQUENCING ruling is spike output, one of: Q8
  blocks on LS4 shipping / Q8 proceeds in parallel consuming LS4's pinned
  parity tests / Q8 hand-rolls the follower window with the parity
  assertion pinned on our side. The anti-third-copy lean is on record (the
  charter: same-windowed-discipline is only GUARANTEED by same-code), but a
  hand-roll with pinned parity is admissible if blocking would strand the
  session. Version floors, when they exist, are cited from lite-stream's
  own llms.txt consumer lines -- never session numbers (era-bound law).
- OR-6 queue semantics (scope-dependent), fail-closed: queueing is explicit
  opt-in PER MUTATION -- a default that queues silently is fail-open and
  forbidden; durability rides the Q6 persistence seam with the same
  version-stamp discipline; replay is order-per-key, per-item observable,
  and DROPS items whose entries no longer exist (a drop is a surfaced
  result, never a silent retry).
- OR-7 breaking budget. The ledger holds NO accumulated deferred-breaking
  nits (grep-verified 2026-09-02; the planner re-sweeps and confirms). If
  that holds, the `[2.0.0]` BREAKING section states truthfully that the
  major rides the new cross-tab semantics and any operator-ratified
  contract retirements (OR-9), with migration notes regardless. Never
  invent a break to justify the number.
- OR-8 the charter's ASSERTIONS line "Suite >= 230" is stale (V10-class);
  354 governs (this file's tests_min). Corrected in the closeout docs
  commit, never mid-pipeline.
- OR-9 the 314 existing tests are contracts. A 2.0 MAY retire one ONLY
  under an explicit operator-ratified breaking change, each retirement
  named in the CHANGELOG BREAKING section with its replacement contract;
  silent edits to existing tests remain forbidden. Everything not
  explicitly broken stays additive (the Q5-Q7 discipline).
- OR-10 (carry_from_q7). One honest findings-clause attempt E through the
  NEW surface (candidates: queue teardown with in-flight replay handles;
  leader teardown with follower projection buffers). Outcome recorded in
  INCONCLUSIVE.md in the verbatim-attempt style; a control that cannot trip
  is decorative -- never fake one.
- OR-11 transport non-goal (1.1.0 discipline, charter): lite-query
  coordinates iterators and frames; sockets belong to the caller. No
  transport ownership, no N-keys-over-one-socket multiplexing (struck by
  the SPIKE INPUTS unless a named cell resurrects it).

## GATES

- G1 suite >= 354 pass, 0 fail, 0 skip, under the default `npm test`.
- G2 law harness: the byte-frozen GATE line then `ok`; every live control
  still trips (5 + any honest Q8 addition per OR-10).
- G3 failover matrix green, all seven cells named tests.
- G4 N-tab soak: 5 tabs, connection count === 1 at the mock source through
  leader churn (kill every k ops), zero duplicated and zero reordered
  frames in every follower, pool baseline in every tab.
- G5 the slow-follower bound: leader memory bounded under a stalled
  follower, drops counted, asserted in torture.
- G6 (scope-dependent) airplane-mode script: dispatch offline -> reload ->
  reconnect replays in order, exactly once each, observable via the feed.
- G7 drift guards green (ascii, surface guard over llms.txt + d.ts).
- G8 `npm pack --dry-run`: file list per the ratified spec (13 today; any
  change is a named spec decision, not drift), test/ bench/ INCONCLUSIVE.md
  BRIEF.md PLAN.md absent, llms.txt + CHANGELOG.md + Cookbook.md present.
- G9 the LS4 verdict is recorded in both roadmaps BEFORE the coder writes
  follower-window code.

## NON-GOALS

- Whatever STOP-DECISION-1 defers -- recorded in the ROADMAP explicitly,
  never implied.
- No transport ownership; no socket multiplexing (OR-11).
- No silent queueing default (OR-6).
- No second feed hook, no feed changes beyond new event coverage for the
  new semantics (vocabulary grows additively if the spec needs it; the
  10-key record shape is frozen).
- No version stamp in-session (OR-1).

## DONE WHEN

Five open tabs hold one upstream connection through leader churn without
frame duplication or reordering; queued mutations (if in scope) survive a
reload and replay exactly once, observably; suite >= 354 with the GATE
byte-identical; `[2.0.0]` head + migration notes landed; the LS4 gate is
flipped on both sides. Then: awaiting /release 2.0.0 + operator publish per
OR-1 -- the eighth and final rung of the ladder.
