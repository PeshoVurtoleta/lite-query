# Changelog

All notable changes to `@zakkster/lite-await` are documented in this file. The
format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] -- 2026-06-22

Initial release. Zero-GC bridge between `@zakkster/lite-signal` and Promise /
async-await. Single-file ESM, ~765 lines, zero runtime dependencies, peer dep
on `@zakkster/lite-signal ^1.2.0`.

### Added

**Core primitives** -- every awaiter accepts `{ timeout?: number, signal?: AbortSignal }`:

- `whenSignal(source, predicate, opts?)` -- resolve when a reactive source first
  satisfies a predicate. Supports synchronous initial match (resolves on next
  microtask), timeout, and AbortSignal. Settlement always cleans the effect.
- `allOf(specs, opts?)` -- resolve when every `[source, predicate]` spec
  satisfies. Returns values in input order. Any failure aborts the remaining
  in-flight specs.
- `anyOf(specs, opts?)` -- first spec to resolve wins; siblings aborted.
  Returns `{ index, value }`. Rejects with `AggregateError` only if every
  spec rejects independently.
- `raceOf(specs, opts?)` -- first spec to settle wins (success OR failure).
  Returns `{ index, value }` on first success; cascades first rejection.

**Promise wrappers** -- for arbitrary, non-signal-aware promises:

- `withTimeout(promise, ms)` -- wrap with deadline.
- `withAbort(promise, signal)` -- wrap with AbortSignal.

**Convenience shorthands**:

- `whenTruthy(source, opts?)` -- equivalent to `whenSignal(source, Boolean, opts)`.
- `whenEquals(source, target, opts?)` -- equivalent to `whenSignal(source, (v) => Object.is(v, target), opts)`.

**Bidirectional bridge**:

- `fromPromise(promise, initialData?)` -- project a Promise lifecycle into a
  `Signal<AsyncState<T>>`. The signal transitions exactly once
  (`pending` -> `resolved` | `rejected`); dispose via lite-signal's `dispose(sig)`
  when done.

**lite-statechart specialization**:

- `whenStatechart(machine, stateName, opts?)` -- duck-typed; resolves when
  `machine` enters `stateName`. Uses `onTransition` directly (one observer
  slot) instead of tracking the state signal (one effect node).

**Errors**:

- `TimeoutError extends Error` -- `{ name: "TimeoutError", timeout: number }`.
  The numeric `timeout` field is the ms value that elapsed.

### Design invariants

- **Cleanup is structural.** Every settlement path -- resolve, reject,
  timeout, abort -- runs the same `fullCleanup` that disposes the effect,
  clears the timeout, and removes the abort listener. There is no settlement
  path where one of these is skipped. Verified by 4096-cycle leak probe in
  `test/09-cleanup-leak.test.mjs`.
- **A throwing predicate or source settles by rejection.** If a predicate --
  or the `source()` getter -- throws, on the synchronous first read or on a
  later change-driven fire, the promise rejects with the thrown value and the
  effect tears down via the same late-binding stop. Without this the throw
  would unwind through `flushEffects` and escape at the signal-writer's
  `.set()` call site, leaving the promise pending with a leaked effect node.
  Verified in `test/11-predicate-throw.test.mjs`.
- **Pre-aborted signal short-circuits.** Every primitive checks
  `signal.aborted` first and returns a rejected Promise synchronously,
  without constructing internal machinery just to tear it down.
- **Hoisted untrack body.** `whenSignal`'s predicate-check closure is
  allocated once at registration and reused per fire -- the
  ZERO-GC HOT PATH discipline borrowed from `WatchEx.js`.
- **Late-binding `stopFn`.** Handles the race where `effect()` fires its body
  synchronously on first registration and the predicate is already true,
  mirroring `WatchEx.js`'s `watchUntil`.
- **Internal AbortController per combinator.** Each of `allOf` / `anyOf` /
  `raceOf` owns one to coordinate sibling cleanup; `linkUserSignal` forwards
  the user's signal in and removes the forward listener on settlement.

### Benchmarks (Node 22, M1-class)

```
whenSignal resolve              213K ops/s   ~4 B/op retained
whenSignal pre-aborted           87K ops/s   ~3 B/op
allOf 4-spec resolve            103K ops/s   ~5 B/op
anyOf 4-spec resolve             55K ops/s   ~3 B/op
raceOf 4-spec resolve            60K ops/s   ~3 B/op
fromPromise lifecycle           228K ops/s   ~5 B/op
```

Final lite-signal pool returns to baseline after every scenario.

### Testing

85 tests total. 81 functional (run with `npm test`), plus 4 GC heap-budget
assertions that run under `--expose-gc` (`npm run test:gc`):

- `01-when-signal` (13 tests): predicate, sync match, timeout, abort, reason
  propagation, validation, cleanup.
- `02-all-of` (9 tests): in-order, out-of-order, sync+async mix, empty,
  timeout, abort, validation, cleanup.
- `03-any-of` (7 tests): `{index, value}` shape, sync winner, sibling
  cleanup, `AggregateError` on total fail, timeout, empty.
- `04-race-of` (6 tests): first-to-settle, success-vs-error patterns, cleanup.
- `05-with-timeout` (11 tests): both Promise wrappers, including
  identity-on-Infinity and null-signal cases.
- `06-from-promise` (7 tests): pending / resolved / rejected transitions,
  initialData preservation, effect integration, disposable.
- `07-abort-signal` (5 tests): pre-aborted short-circuit matrix across every
  primitive, mid-flight cleanup matrix, timeout-vs-abort race, 1K-cycle
  leak probe.
- `08-statechart` (8 tests): duck-typed mock + best-effort real
  `lite-statechart` integration.
- `09-cleanup-leak` (7 tests): 4K / 2K / 2K / 1K / 1K / 1K / 500 high-volume
  cycles across every settlement path, mixed-mode aggregate. Asserts pool
  returns to baseline.
- `10-gc` (4 budgets, `--expose-gc`): 10K whenSignal / 5K aborts / 2K allOf /
  2K anyOf each retain < 1 MB.
- `11-predicate-throw` (8 tests): a throwing predicate or `source()` getter is
  routed to rejection on the sync first read and on change-driven fires (never
  escaping at the writer's `.set()`); combinator propagation; 1K-cycle leak
  probe.

[1.0.0]: https://github.com/PeshoVurtoleta/lite-await/releases/tag/v1.0.0
