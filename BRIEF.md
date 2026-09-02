# BRIEF -- Q7 -- lite-query v1.5.0 -- the devtools feed: qc.inspect

Operator contract for the Q7 pipeline (planner -> coder -> reviewer -> qa).
Source charter: ROADMAP.md section 5, Q7. This file wins over memory; the
ROADMAP wins over this file only where this file is silent.

---
package: "@zakkster/lite-query"
session: Q7
status: planned
version_target: 1.5.0       # stamped by the /release drill, NOT in-session (OR-1)
tests_min: 278              # floor; suite is 268 at session start
skip_max: 0
torture: "law harness ok; phase H gated TWICE -- hook absent and hook installed"
depends_on: [Q6 (shipped 1.4.0, operator publish e2f07ba)]
blocks: [Q8]
downstream: "lite-studio's cache panel (their repo, their session) -- consumes the
  feed's vocabulary; no import in either direction, ever"
carry_from_q6: "findings-clause torture control STILL uncontrolled after three
  attempts (Q5 A/B, Q6 C; INCONCLUSIVE.md holds all three verbatim); attempt D
  rides the inspect install/uninstall lifecycle (OR-10)"
---

## PURPOSE

Observability with a zero-cost off switch. SPEC.md promised devtools; the
panel belongs to lite-studio -- what lite-query owes the ecosystem is the
FEED: a push-mode stream of cache truth a panel can render. The entire design
tension is the off switch: law 4 says the hot path buys nothing it does not
use, so the uninstalled branch is the product. This release also carries the
Cookbook cross-review fix already committed at 033e670 (OR-2).

## TASKS

- T1 -- Feed core. `qc.inspect(hook)` installs ONE hook (single slot, not a
  list -- a panel multiplexes; the hook-array rejection is recorded and
  tested), returns an idempotent uninstall function. Every emit site is one
  predictable `!== null` branch; nothing -- no object, no array, no string,
  no arguments building -- is constructed before that test passes.
- T2 -- Event classes, from the charter verbatim: entry lifecycle
  (create/attach/detach/gc/remove), status + staleness transitions, fetch
  dispatch/settle/abort (with reason), cross-tab send/receive, sharedFetch
  leader/follower events, stream start/value/done/error, mutation lifecycle,
  hydrate / persist save. The planner enumerates the exact emit-site list
  against Query.js the way Q6's V4 pinned the six write sites -- every site
  named, every non-site recorded with a reason.
- T3 -- The event-object DECISION. Charter offers two designs: (a)
  preallocated + reused per event type (the hook must copy what it keeps --
  the zero-GC feed contract, documented loudly); (b) allocated only when a
  hook is installed. The lite-signal integer-opcode precedent (OR-3) is on
  the record as a third data point and the allocation bar. Planner recommends
  with rationale; phase H measures BOTH candidate costs; the decision and the
  measured numbers land in the CHANGELOG head (OR-6).
- T4 -- Fuzzer assertion mode. The existing cache fuzzer gains an
  events-consistency mode: the observed stream must form a consistent state
  machine (no detach without attach, no settle without dispatch, no
  stream value after done, no event after its entry's remove/gc except
  create). Feed coverage of every lifecycle the fuzzer can provoke is a gate
  (G4), not a hope.
- T5 -- Torture. Phase H runs the full warm loop TWICE -- hook absent, and
  hook installed with a no-op copying hook. Both hold maxMajor 0; the absent
  run shows zero added allocation vs the Q6 baseline and the byte-frozen GATE
  line; both numbers recorded in bench provenance (G3). OR-10 findings-clause
  attempt D through the install/uninstall lifecycle.
- T6 -- Docs pass. README (short "Devtools feed" section + pointer to
  lite-studio, no panel promises), llms.txt (scope line flips; full event
  vocabulary table), d.ts (event union + inspect signature), Cookbook recipe
  20: a console logger in 10 lines. CHANGELOG `[1.5.0]` head records every OR
  decision, the T3 measured numbers, AND the Fixed lines for the four
  bake-facing Cookbook sample defects fixed at 033e670 (OR-2).

## OPERATOR RULINGS

- OR-1 (standing PR-1). No in-session version stamp. package.json and the
  Query.js VERSION const stay 1.4.0 (the sync guard couples them); the
  CHANGELOG `[1.5.0]` head lands in-session; the /release 1.5.0 drill
  performs the stamp across its sites after the pipeline closes.
- OR-2 (user ruling, 2026-09-02: "cookbook will fold here"). No 1.4.1 docs
  patch. The four bake-facing Cookbook sample fixes (commit 033e670 --
  kebab-case subpath + root import, RangeReader.open over HTTPRangeAdapter,
  columnar prefetchRange/syncRange rewrite, unpooled-Buffer slice) ship with
  1.5.0 and are recorded as Fixed lines in the `[1.5.0]` head. The verbatim
  bake floors quote (Cookbook recipe 18) remains untouchable.
- OR-3 vocabulary provenance (verified at source today; do not invent from
  memory). The charter's pointer resolves one repo deeper than it says:
  lite-studio consumes lite-devtools -- `watchGraph`, `track`, `leakWatch`
  are lite-devtools' API over lite-signal's engine hook. The conventions Q7
  MIRRORS, read from lite-devtools/llms.txt and lite-signal/llms.txt
  2026-09-02: flat event objects with a lower-case string `type`
  discriminant and a `ts` stamp (performance.now() if available, else
  Date.now()); push surfaces return an IDEMPOTENT stop/uninstall; a single
  nullable listener behind one branch-predicted null test (lite-signal
  `onGraphMutation(fn)`, opcodes as three integers, allocation-free dispatch
  when registered -- the suite's allocation bar for engine-side feeds);
  observe-only hook contract ("never throw, never mutate"). Mirror the
  conventions; import NOTHING from either package; restate no version floors
  of theirs anywhere. The planner re-reads both llms.txt files before
  freezing event names.
- OR-4 two seams, never one. Q6's PRIVATE persistHook stays untouched and
  private: single slot, TypeError on non-function, Error on double-install,
  `=== fn`-guarded idempotent uninstall (Query.js installPersistHook,
  ~253-277), notifyWrite() at the six V4 sites. `qc.inspect` is a NEW,
  INDEPENDENT single slot with the SAME install/uninstall semantics
  (mirror the in-repo precedent, including the two throw shapes). Installing
  or uninstalling either hook never affects the other -- a devtools panel
  must not be able to evict the persistence adapter, or vice versa. The
  public feed REPORTS persist/hydrate activity as events; the adapter never
  consumes the public feed.
- OR-5 zero-cost-uninstalled is a GATE, not a hope (Q6's OR-8 discipline).
  With no hook installed the warm path allocates exactly what 1.4.0
  allocates, the GATE line is byte-frozen, and no new per-entry slots appear
  unless the planner proves one necessary (STOP item, not a license).
- OR-6 the T3 DECISION belongs to the planner's spec and phase H's numbers.
  If reused-singleton events win: the hook-must-copy contract is documented
  in README + llms.txt + d.ts, and a counted test proves two sequential
  events of one type share object identity with fields overwritten. If
  allocate-when-installed wins: the installed-run phase H numbers are the
  documented cost. Either way both measured candidates land in the CHANGELOG
  head. Hybrids (opcode + reused payload) are admissible if measured.
- OR-7 dispatch is SYNCHRONOUS at the emit site (the onGraphMutation
  discipline): no queue, no microtask, no reordering -- the G4 state-machine
  assertion depends on emission order being the truth. A hook that blocks
  blocks the app; that is the panel's problem and the docs say so. What a
  THROWING hook does is the planner's to pin fail-closed (contain vs
  propagate vs auto-uninstall), with the chosen behavior tested and the
  rationale recorded -- silent swallowing is not an option.
- OR-8 events speak lite-query's OWN domain vocabulary, lower-case: entry /
  fetch / tab / shared / stream / mutation / persist terms we already ship
  in docs (attach/detach, dispatch/settle/abort, leader/follower). Where a
  concept collides with lite-devtools' naming (their connect/disconnect vs
  our attach/detach), our shipped domain vocabulary WINS -- mirroring means
  conventions (shape, casing, ts, idempotent uninstall), not renaming our
  domain. If the planner believes true mirroring demands a rename, that is
  a STOP item for the operator.
- OR-9 additive minor. No removal, rename, or behavior change of any 1.4.0
  surface; the 268 existing tests are contracts and pass UNMODIFIED (any
  edit to an existing test is a STOP item). Emit-site insertion into
  functions that also carry notifyWrite() must leave the persist seam's
  trigger conditions untouched.
- OR-10 (carry_from_q6). One honest findings-clause control attempt through
  the NEW surface (candidates: an uninstalled hook closure pinned across
  install/uninstall cycles; a reused event object retained by a misbehaving
  hook -- whichever can HONESTLY trip). Outcome recorded in INCONCLUSIVE.md
  in the same verbatim-attempt style as A/B/C; a control that cannot trip is
  decorative -- never fake one, never add a ctl that cannot fire.

## GATES

- G1 suite >= 278 pass, 0 fail, 0 skip, under the default `npm test`.
- G2 law harness: `node --expose-gc test/torture.mjs` prints the byte-frozen
  GATE line (`GATE leak=size 0/0 findings=0 warnings=0 | gc major=0 minor=0
  maxMs=0.00 | ok`) then `ok`; all four live controls still trip, plus any
  honest Q7 addition per OR-10.
- G3 phase H double-run: hook-absent run byte-identical GATE + zero added
  allocation vs the Q6 baseline; hook-installed (no-op copying hook) run
  maxMajor 0; both numbers recorded in bench provenance.
- G4 fuzzer assertion mode green: the event stream is a consistent state
  machine across every lifecycle the fuzzer can provoke.
- G5 every T2 event class fires with its documented shape (counted tests);
  uninstall stops emission mid-run; double-install throws; hook-array and
  non-function rejection throw TypeError -- all counted.
- G6 existing 268 tests pass unmodified (OR-9).
- G7 drift guards green: ascii guard over every new/edited file; surface
  guard sees `qc.inspect` + the event union in llms.txt and d.ts.
- G8 `npm pack --dry-run`: 13 files, test/ + bench/ + INCONCLUSIVE.md + this
  file absent, llms.txt + CHANGELOG.md + Cookbook.md present.

## NON-GOALS

- No UI, no formatting, no serialization of the feed (the panel's job).
- No ring buffer, no history, no replay (panel's job).
- No second hook, no hook arrays, no per-event subscriptions (a panel
  multiplexes; rejection recorded and tested).
- The persistence adapter never rides the public feed (OR-4).
- No async dispatch, no event queue (OR-7).
- No new deps of any kind; no imports from lite-devtools / lite-studio.
- No focus/reconnect triggers (still parked; still no consumer).
- No version stamp in-session (OR-1).

## DONE WHEN

A lite-studio cache panel could be built against the feed without touching
lite-query internals; an app that never calls `inspect()` pays nothing
measurable (numbers on the record); suite >= 278 with the GATE
byte-identical; `[1.5.0]` head landed including the 033e670 Cookbook Fixed
lines. Then: awaiting /release 1.5.0 + operator publish per OR-1.
