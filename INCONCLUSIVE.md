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

The census clause (`censusOk`) WAS uncontrolled between commit e56af54 and its
fix: C-detach hardcoded `censusOk: true` (an undeclared drift from PLAN
Assertion 3). It is now controlled -- C-detach builds a WeakRef census over its
128 strongly-held handles and the whole sample comes back live (128/128),
tripping the census clause alongside the leak clause. No longer an open item.

## Escalation

Any inconclusive that survives triage becomes a finding ID in ROADMAP.md
section 2. It is never gated away.
