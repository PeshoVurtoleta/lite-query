# INCONCLUSIVE -- triage for lite-query's torture gates

Repo doc, not shipped (absent from package.json `files[]`). It exists so that a
third-verdict result from the phase-H gate is triaged, never gated away.

## Scope

Node source `'gc'`, the `checkNoGc` lane, lite-gc-profiler 1.16.0 /
lite-leak 1.10.0. `test/torture.mjs` phase H calls
`checkNoGc(summary, { maxMajor: 0, maxPauseMs: 4 })` and folds
`report.verdict === 'pass'` into the gate. A `pass` is green; a `fail` is a
budget breach; an `inconclusive` is neither and is triaged here.

## The rule

Never resolve an inconclusive with `allowInconclusive`. That is the escape
hatch, not the fix (profiler llms.txt, Key semantics). `allowInconclusive` is
set nowhere in `test/torture.mjs` or `test/torture/controls.mjs`.

## Reason codes we can actually see on this lane

- `not_observed`   -- profiler never `start()`ed, or `gc.observed === false`
                      (engaged only via `sampleHeap`/phase, so the GC-event
                      channel was never watched). Fix the harness, not the
                      threshold. Phase H always `start()`s, so the observer is
                      attached before the warm loop.
- `no_rules`       -- empty / all-`undefined` rules object. Vacuity is not a
                      pass. `RULES` is frozen with two live thresholds.
- `partial_report` -- a hard exit mid-measurement. Rerun.
- `noise_floor`    -- (v1.16.0) the machine is too noisy to resolve the signal;
                      see environment hygiene.
- non-finite metric -> inconclusive, never pass (GC-10).

NOT applicable here: `uasm_below_granularity`, `fingerprint_mismatch`,
`source_mismatch`, `mixed_sources`, `invalid_baseline`, `bracket_inverted`
(the browser / baseline / differential lanes this harness does not use).

## Rerun guidance

Three consecutive runs, same seed. Two-of-three inconclusive means it is real;
resolve the cause, do not widen the budget.

## Environment hygiene

- `--expose-gc` present (the npm scripts pass it).
- No other node process competing on the box.
- No profiler / debugger attached.
- Check `summary.gc.foreignForced`: non-zero means something else collected
  inside our window and the numbers are polluted.
- Import the profiler exactly once. Phase H and the controls share one
  module-cached copy; a second copy keeps its own in-flight guard and cannot
  see ours.

## When to suspect the harness

- Two profilers in flight (break mode skips phases P and H precisely to keep
  exactly one profiler live).
- A control that stopped tripping (`npm run torture:control` no longer prints
  three `tripped:` lines).
- The WeakRef census reads all-live on a plain run (`sampledLive === 256`) --
  either a real retention regression or a broken settle loop.

## Known uncontrolled clauses

- The `findings.length === 0` clause has NO control. Firing
  `createOwnerCascadeOrphanKernel().audit()` needs a lite-signal owner tree that
  breaks under a still-tracked handle. lite-query's public surface never leaves
  that state: breaking a query entry (`removeQueries` / `clear`) does not touch
  the lite-signal owner tree the kernel watches, and clean owner disposal
  auto-untracks (`size()` returns to 0). Confirmed empirically in session Q4.
  Staging it would require a runtime edit to Query.js / StreamQuery.js, which
  OR-1 forbids in a harness session. Recorded, not faked; carried to Q5.

  Q5 (OR-6) added NEW public surface (`infiniteQuery` / `qc.prefetch`) and
  re-attempted a legal trigger through its teardown paths. Both attempts were
  run this session against the real entry points:

    Attempt A -- dispose the owner mid-page-fetch with the handle still tracked.
      `createRoot(() => effect(() => { const iq = infiniteQuery(qc, { fetcher:
      never-resolves, ... }); iq.data(); track(iq, ..., { audit:true }); }))`,
      then dispose the root WHILE the page-one fetch is in flight and the handle
      is NOT disposed. Result: `audit().length === 0`, `size() === 0`. The owner
      cascade auto-untracks exactly as query()/streamQuery() do; the infinite
      watcher rides the same `createRoot(effect)` discipline, so it cannot leave
      the kernel's trip state.

    Attempt B -- track the in-flight `fetchNextPage()` promise, then
      `removeQueries` mid-flight (aborts with `lite-query:removed`). Result:
      `audit().length === 0`. (`size()` shows the tracked promise only while the
      test itself still holds the local reference -- an artifact of the probe,
      not a lite-query retention; the audit finding, which is the clause under
      test, never fires.)

  OUTCOME: does NOT fire. Both attempts re-recorded verbatim above; the clause
  stays uncontrolled and is carried to Q6. No `ctlFindings` control was added
  (a control that cannot trip is decorative). The four live controls remain
  `alloc` / `detach` / `fuzz` / `pages`.

  Q6 (OR-11) added NEW public surface (`persistQueryClient` / `qc.dehydrate` /
  `qc.hydrate`) and re-attempted a legal trigger through the adapter teardown
  path. The attempt was run this session against the real entry points:

    Attempt C -- track a dehydrated payload with `{ audit: true }` and carry it
      across `persistQueryClient.stop()` with a PENDING throttled save.
      `persistQueryClient(dst, { save, load: () => payload, version: 'v1',
      throttle: 1000 })`; then `await handle.restored` (arms the write hook),
      `dst.setQueryData(['w'], i)` (opens a pending save window on the injected
      mock clock), and `handle.stop()` (FLUSH-ON-STOP fires the pending save and
      uninstalls the hook) -- the payload tracked and audited the whole way
      through. Result: `tracker.audit().length === 0` observed while the payload
      was tracked immediately after `stop()`. The adapter's teardown touches no
      lite-signal owner tree (persistQueryClient uses no `createRoot`/`effect`;
      the write hook is a plain single-slot thunk), so it cannot leave the
      owner-cascade kernel's trip state. `size()` reflects the tracked payload
      only while the probe itself holds the reference -- an artifact of the
      probe, not a lite-query retention (and a payload has no reactive owner to
      auto-untrack, so lite-leak 1.10.0 `untrack` does not drop `size()`
      synchronously; the shipped phase-1b loop therefore proves payload
      reachability with a WeakRef census, not a registration).

  OUTCOME: does NOT fire. Attempt C re-recorded verbatim above; the clause stays
  uncontrolled and is carried forward. No `ctlPersist` control was added (a
  control that cannot trip is decorative). The four live controls remain
  `alloc` / `detach` / `fuzz` / `pages`.

  Q7 (OR-10) added NEW public surface (`qc.inspect` -- the devtools feed) and
  re-attempted a legal trigger through the inspect install/uninstall lifecycle.
  The attempt was run this session against the real entry points, as a section of
  phase H that executes AFTER the frozen gate evaluation (ON-2), so it can never
  move the byte-frozen GATE line:

    Attempt D -- carry a tracked inspect-hook closure across the install/uninstall
      boundary while the client owner tree is torn down. 4096 cycles of:
      `const hookClosure = makeInspectHook()` (closes over a module-level sink
      only, never the client -- held-value contract preserved), `tracker.track(
      hookClosure, NOOP_RELEASE, 'inspect-hook', { audit: true })` OUTSIDE any
      lite-signal owner, `qc.inspect(hookClosure)`, drive events through the hook
      (`qc.setQueryData`), then `uninstall()` + `qc.clear()` + `qc.dispose()` --
      the hook carried the whole way through. Result (this session):
      `tracker.size()` returned to its pre-attempt value (live-delta `0`) and
      `tracker.audit().length === 0` after `gc()` + a settle tick. The inspect
      seam holds a plain single-slot cell (`feed = { hook, pool }`); uninstall
      nulls both, and the feed uses no `createRoot`/`effect`, so tearing it down
      touches no lite-signal owner tree the owner-cascade kernel watches -- it
      cannot leave the kernel's trip state, exactly as query()/streamQuery()/
      persistQueryClient do not.

  OUTCOME: does NOT fire. Attempt D re-recorded verbatim above; the
  findings-clause stays uncontrolled and is carried forward. Per ON-3, Attempt D
  is explicitly NOT a gate clause. The honest Q7 addition that DOES gate is the
  fuzzer feed state-machine control C-feed (`QUERY_TORTURE_BREAK=feed` -> a
  guaranteed `detach-without-attach` via a dropped attach bookkeeping update);
  the live controls are now `alloc` / `detach` / `fuzz` / `pages` / `feed`
  (`tripped=5/5` under `QUERY_TORTURE_BREAK=1`). No `ctlFindings` control was
  added (a control that cannot trip is decorative).

  POSTSCRIPT (reviewer, QD-4). Attempt D found no retention through the inspect
  install/uninstall lifecycle -- but review then found a real one it was blind to:
  the `setStatus` funnel parked each status-written entry in the module-global
  `_se` for the untracked prior-status read and never cleared it, so the LAST
  entry written while a hook was installed (with its data payload) stayed pinned
  for the process lifetime, surviving uninstall and `removeQueries`. Attempt D
  churned fresh clients per cycle and never re-read the specific window where the
  parked entry stays live, so its `tracker.size()`/`audit()` probe stepped right
  past the pin. It was caught by measurement in review (WeakRef the payload,
  write, uninstall, removeQueries, force GC -> not collected; overwriting `_se`
  released it, proving `_se` the sole retainer) and fixed (`_se = null` right
  after the read). The lesson is the same one A/B/C/D keep teaching: this
  findings/retention clause is honestly UNCONTROLLED -- a hand-run attempt that
  does not fire is not proof of absence, and a keyed retention like this is found
  by an adversarial probe or a reviewer, not by the churn loop. Recorded, not
  spun; the clause stays carried.

  Q8 (OR-10) added NEW public surface (`sharedStream` -- cross-tab shared
  streams: a follower projects a leader's frames into its local entry, holding
  no iterator) and re-attempted a legal trigger through the follower-projection
  teardown path. The attempt was run this session against the real entry points,
  as a section of phase H that executes AFTER the frozen gate evaluation (ON-2),
  so it can never move the byte-frozen GATE line:

    Attempt E -- carry a live follower's projection slots past disposeEntry while
      the leader is torn down. 2048 cycles of: a follower `queryClient({
      sharedStream: true, isLeader: () => false })` + `streamQuery(qc, { mode:
      "buffer", maxBuffer: 4, ... })` subscribed inside `createRoot(effect)`, a
      raw peer broadcasts two `stream-frame`s so the entry builds a `projWindow`
      array AND installs a `streamPromote` closure (which captures the /stream
      body's scope -- the strongest retention candidate); then
      `tracker.track(entry.streamPromote, NOOP_RELEASE, 'stream-promote', {
      audit: true })` OUTSIDE any lite-signal owner (held-value contract
      preserved -- neither tag nor release closes over the target), followed by
      `stop()` + `h.dispose()` + `qc.removeQueries(['e'])` + `qc.dispose()` -- the
      promote closure carried the whole way through the teardown. Result (this
      session): `tracker.size()` returned to its pre-attempt value (live-delta
      `0`) and `tracker.audit().length === 0` after `gc()` + a settle tick. C8's
      `releaseProjection` nulls every projection slot -- the window array, the
      epoch/seq cursors, the watchdog, and the streamPromote closure -- at BOTH
      detach and disposeEntry (V5), and the follower uses the same
      `createRoot(effect)` watcher discipline as query()/streamQuery(), so
      tearing it down touches no lite-signal owner tree the owner-cascade kernel
      watches and leaves nothing for the async-retention kernel. It cannot leave
      either kernel's trip state.

  OUTCOME: does NOT fire. Attempt E re-recorded verbatim above; the
  findings-clause stays uncontrolled and is carried forward. Per ON-3, Attempt E
  is explicitly NOT a gate clause (a control that cannot trip is decorative). The
  live controls remain `alloc` / `detach` / `fuzz` / `pages` / `feed`
  (`tripped=5/5` under `QUERY_TORTURE_BREAK=1`). The lesson A/B/C/D/E keep
  teaching holds: a hand-run attempt that does not fire is not proof of absence
  -- a keyed retention like the QD-4 `_se` pin is found by an adversarial probe
  or a reviewer, not by the churn loop. The honest Q8 gate additions that DO
  gate are the N-tab fuzzer generalization (echo/ledger laws over N) and the
  5-tab shared-stream soak (dup/reorder=0, one connection under churn, G5).

The census clause (`censusOk`) WAS uncontrolled between commit e56af54 and its
fix: C-detach hardcoded `censusOk: true` (an undeclared drift from PLAN
Assertion 3). It is now controlled -- C-detach builds a WeakRef census over its
128 strongly-held handles and the whole sample comes back live (128/128),
tripping the census clause alongside the leak clause. No longer an open item.

## Escalation

Any inconclusive that survives triage becomes a finding ID in ROADMAP.md
section 2. It is never gated away.
