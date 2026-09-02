# BRIEF -- Q9 -- lite-query v2.1.0 -- offline mutation queue + LS4 seam swap

Operator contract for the Q9 pipeline (planner -> coder -> reviewer -> qa).
Source charter: ROADMAP.md Q9 stub (lines ~1131-1155) + the Q8 charter's
offline-queue bullet (lines ~1013-1018), both carried forward INTACT by
STOP-DECISION-1. This file wins over memory; the ROADMAP wins over this
file only where this file is silent.

---
package: "@zakkster/lite-query"
session: Q9
status: in-pipeline (2026-09-03)
version_target: 2.1.0       # stamped by the /release drill, NOT in-session (OR-1)
tests_min: 380              # floor; suite is 362 (shipped 2.0.0) at session start
skip_max: 0
torture: "law harness ok + airplane-mode replay soak + attempt F on queue teardown"
depends_on: [Q8 (shipped 2.0.0, operator publish da8b377),
  lite-stream 1.4.0 (registry-verified 2026-09-03: latest=1.4.0, 7-file
  tarball, createSignalWriter at Stream.js:717, CHANGELOG head
  "1.4.0 -- 2026-09-03")]
downstream: none gated (the ladder's final rung)
carry_from_q8: "findings-clause torture control STILL uncontrolled after
  FIVE attempts (Q5 A/B, Q6 C, Q7 D, Q8 E; INCONCLUSIVE.md holds all five
  verbatim); attempt F rides the queue's replay teardown with in-flight
  handles (OR-9)"
---

## PURPOSE

The deferred half of the 2.0 major, riding on a FINISHED failover story --
exactly as the split's own rationale demanded. Two bodies of work, one
minor: (1) the offline mutation queue with durable replay-on-reconnect,
carried intact from the Q8 charter; (2) the projectFrame seam swap to
lite-stream 1.4.0's createSignalWriter, deleting the hand-rolled third
copy of the windowed-view discipline -- the swap the parity test was built
to make mechanical.

## TASKS

- T1 -- Queue surface. Explicit opt-in PER MUTATION (a default that queues
  silently is fail-open and forbidden -- OR-3); the planner names the flag
  and the dispatch semantics. When the fetcher cannot run (caller says
  offline / queue-on-failure per spec), the mutation lands in the durable
  queue with a surfaced "queued" outcome -- never a silent limbo. The ON-3
  status law governs replay results too: control-flow tracking, falsy
  rejections settle "error".
- T2 -- Durability. The queue rides the Q6 persistence seam
  (persistQueryClient's save/load thunks) with the SAME version-stamp
  discipline: the adapter stamps, mismatch or corruption drops the WHOLE
  queue fail-closed -- and the drop is surfaced/observable, never silent
  (OR-5). Monomorphic queue records; the planner specs the shape.
- T3 -- Replay. Explicitly triggered by the caller (OR-4 -- no
  connectivity ownership). Order preserved per key; per-item results
  surfaced; items whose entries no longer exist are DROPPED with a
  surfaced result (never silently retried); everything observable via the
  Q7 feed (vocabulary grows additively, 10-key record frozen -- OR-7).
  The crash-boundary semantics are RULED, not implied (OR-6).
- T4 -- LS4 seam swap. projectFrame/projectBuffer's hand-rolled follower
  window is replaced by lite-stream 1.4.0's createSignalWriter UNDER the
  core-import constraint (OR-8): Query.js imports lite-signal ONLY. Their
  llms.txt consumer paragraph is the fit contract: "The follower frames
  arrive by callback and feed writer.push(frame) -- the writer needs ZERO
  channel / epoch awareness." The differential parity test
  (shared-stream.test.js C5 block, A6) is the CONTRACT and does not
  change: it must pass against the live 1.4.0 oracle BEFORE the swap
  (oracle upgrade first, as its own rung) and AFTER (the swap body).
  STOP-DECISION-2 rules the mechanism (see OR-8).
- T5 -- Torture. Airplane-mode replay soak (queue churn + reload cycles,
  pool baseline); attempt F per OR-9; the frozen GATE window stays
  byte-identical -- new prints strictly AFTER the frozen gate evaluation
  (ON-2 precedent).
- T6 -- Docs. README queue section; llms.txt full new surface (any new
  liveness-adjacent sentence must be TRUE the way OR-3/Q8 made the stream
  sentence true); d.ts; Cookbook airplane-mode recipe; CHANGELOG [2.1.0]
  head in-session (Added/Changed only -- zero breaking budget, OR-2);
  every feed-count site (README/Cookbook/llms.txt) reconciled in the SAME
  commit that grows the vocabulary (OR-7, the QD-4 lesson).

## OPERATOR RULINGS

- OR-1 (standing PR-1). No in-session version stamp. package.json and the
  Query.js VERSION const stay 2.0.0; the CHANGELOG `[2.1.0]` head lands
  in-session; the /release 2.1.0 drill performs the stamp after the
  pipeline closes.
- OR-2 minor discipline. Everything additive; zero breaking budget; the
  shipped 362 tests are contracts -- none edited, none retired. The
  no-opt-in path is byte-identical in behavior to 2.0.0 (the existing
  suite proves it by passing unedited); OFF-path allocation stays zero.
- OR-3 fail-closed queueing. Queueing is explicit opt-in per mutation;
  queue-full and storage-failure are surfaced rejections, never silent
  drops or silent retries; a restored queue that fails validation drops
  whole and surfaces the drop. null is not zero: an absent queue section
  in persisted state is "no queue", never an error.
- OR-4 transport non-goal (1.1.0/OR-11 carry). No navigator.onLine, no
  reconnect listeners, no connectivity polling, no timers watching the
  network. Replay fires when the CALLER calls the replay surface. The
  library coordinates mutations and frames; sockets and connectivity
  belong to the caller.
- OR-5 durability discipline. Version REQUIRED on the seam (Q6 law);
  the adapter stamps, the primitive validates; a version mismatch, a
  corrupt record, or a record for an unknown shape drops the whole queue
  fail-closed with a surfaced/observable reason. Queue persistence never
  captures non-queued in-flight mutations.
- OR-6 crash-boundary ruling. In-run replay is exactly-once per item
  (G6). Across a crash mid-replay the planner RULES one of: at-most-once
  (item removed from the durable queue before dispatch; a crash may lose
  it) or at-least-once (removed after its terminal per-item result; a
  crash may double-fire, caller idempotency documented). Either is
  admissible ONLY with the choice stated in llms.txt and pinned by a
  named test simulating the crash window. The undocumented middle is
  forbidden.
- OR-7 feed growth. New queue events are additive `domain:verb` types on
  the frozen 10-key record; the planner names them and the final count;
  README/Cookbook/llms.txt count sites move in the same commit. No second
  hook, no record-shape change.
- OR-8 the seam swap, STOP-DECISION-2. Constraint (grep-gated, G8): core
  Query.js imports lite-signal ONLY -- no lite-stream import, static or
  dynamic, ever. Admissible outcomes, ruled by the planner against the
  real code: (a) the windowed projection body moves behind an
  entry-carried writer slot installed from StreamQuery.js (which MAY
  import createSignalWriter unconditionally -- the /stream optional peer
  floor then bumps ^1.3.0 -> ^1.4.0, cited era-bound from lite-stream's
  own llms.txt "(since 1.4.0)" line, never from session numbers), with
  core's latest-mode projection remaining stream-import-free so a
  plain-query follower tab is unaffected; or (b) the swap is REJECTED by
  a named ruling (e.g. the window provably serves followers with no
  /stream module loaded in buffer mode) -- the hand-roll stays, the
  parity oracle still upgrades to live 1.4.0, and the deletion is parked
  with the reason recorded in the ROADMAP. TWO live window bodies
  selected by load-state luck is fail-open and forbidden -- whichever
  body ships, it is the ONLY body on that path.
- OR-9 (carry_from_q8). One honest findings-clause attempt F through the
  queue's replay teardown with in-flight replay handles. Outcome recorded
  in INCONCLUSIVE.md in the verbatim-attempt style; a control that cannot
  trip is decorative -- never fake one.
- OR-10 devDependency bump. @zakkster/lite-stream devDep moves to ^1.4.0
  (the parity oracle needs the live writer); the lite-signal override pin
  stays. This is a lockfile rung, not a version site.

## GATES

- G1 suite >= 380 pass, 0 fail, 0 skip, under the default `npm test`.
- G2 law harness: the byte-frozen GATE line then `ok`; every live control
  still trips (5 + any honest attempt-F addition per OR-9).
- G3 airplane-mode script (the stub's G6, verbatim): dispatch offline ->
  reload -> reconnect -> replays in order, exactly once each,
  feed-observable, per-item results surfaced.
- G4 parity across the swap: the A6 differential test green against live
  lite-stream 1.4.0 BEFORE the swap rung and AFTER it, unchanged; the
  N-tab soak's zero-dup/zero-reorder unchanged.
- G5 OFF-path: no queue opt-in anywhere -> behavior byte-identical to
  2.0.0; the 362 shipped tests pass unedited (OR-2).
- G6 drift guards green (ascii, surface guard over llms.txt + d.ts).
- G7 `npm pack --dry-run`: 13 files (any change is a named spec decision,
  not drift); test/ bench/ INCONCLUSIVE.md BRIEF.md PLAN.md absent.
- G8 core import discipline: `grep -n "from ['\"]" Query.js` shows
  lite-signal only (OR-8's constraint, held as a gate).
- G9 crash-boundary test named and green, matching the OR-6 ruling as
  documented in llms.txt.

## NON-GOALS

- No silent queueing default (OR-3). No connectivity ownership (OR-4).
- No breaking changes, no test retirements (OR-2).
- No feed record-shape change, no second hook (OR-7).
- No version stamp in-session (OR-1).
- No new subpath, no new entry point -- the queue rides the existing
  mutation + persist surfaces.

## DONE WHEN

The airplane-mode script replays in order exactly once, observably; a
version-mismatched queue drops whole and says so; the A6 parity test is
green against live 1.4.0 on both sides of the swap (or the swap is
rejected by named ruling with the oracle still upgraded); suite >= 380
with the GATE byte-identical; `[2.1.0]` head landed. Then: awaiting
/release 2.1.0 + operator publish per OR-1 -- the queue ships on a
finished failover story, and the ladder is done.
