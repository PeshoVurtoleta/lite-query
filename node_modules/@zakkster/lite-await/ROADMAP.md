# Roadmap

Everything below is **gated on real consumer pull**. The 1.0.0 surface is
deliberately tight: `whenSignal`, the three combinators, two Promise
wrappers, two shorthands, `fromPromise`, `whenStatechart`, `TimeoutError`.
Adding primitives is cheap; deprecating them is not. Each item below has a
concrete consumer in mind, and lands only when that consumer is ready to
exercise it.

## Considered for 1.x (additive, non-breaking)

### `withRetry(fn, opts)` -- exponential backoff
**Trigger:** lite-twitch-helix / lite-twitch-ebs need this for transient
network failures. Shape sketch:

```js
const data = await withRetry(
    (signal) => fetch(url, { signal }),
    { attempts: 3, baseMs: 100, factor: 2, jitter: 0.2, signal: ctrl.signal }
);
```

Each attempt receives an AbortSignal so the inner work IS cancellable -- the
factory-style API is the difference from `withTimeout`. Should retry on
network errors and 5xx responses; not on 4xx (caller's bug). Predicate
parameter for "is this retryable?" considered.

**Status:** sketched, deferred to first real consumer.

### `mapLimit(items, fn, limit, opts?)` -- concurrency-limited fan-out
**Trigger:** lite-twitch-helix batch endpoints (`/users?login=...`,
`/games?id=...`). Need to issue N requests with concurrency cap K so we
don't trip Twitch rate limits. Shape sketch:

```js
const users = await mapLimit(
    userIds,
    (id, signal) => fetch(`/users/${id}`, { signal }),
    8,   // max 8 concurrent
    { signal: ctrl.signal, timeout: 30000 }
);
```

Returns results in input order. Any rejection cancels in-flight and pending.
Equivalent to `Promise.all` with a semaphore.

**Status:** sketched, deferred.

### `fromAsyncIterable(iter, opts?)` -- bridge async generators -> signal
**Trigger:** lite-rollback's incoming frame buffer, lite-twitch-ebs's SSE
stream. Each yielded value becomes the new signal value; the signal also
tracks `{ done, error }`. Shape sketch:

```js
const frameSig = fromAsyncIterable(networkFrames(), { initial: null });
effect(() => {
    const frame = frameSig().value;
    if (frame !== null) applyFrame(frame);
});
```

Backpressure question: drop-on-overflow vs. ring-buffer-with-latest vs.
strict-consume. The right default depends on the consumer. Likely the answer
is "configurable, default to latest-wins."

**Status:** sketched, deferred -- needs the consumer decision on backpressure.

### `whenObservable(observable, opts?)` -- bridge from RxJS et al.
**Trigger:** users mixing lite-signal with existing RxJS code in
brownfield codebases. Subscribe-then-await-first-value pattern. Implementable
in ~20 lines if needed.

**Status:** sketched, deferred -- low priority because consumers in the
lite-* ecosystem don't typically have RxJS.

### `signal-from-event(target, eventName)` and reverse
**Trigger:** lite-twitch SDK needs to bridge `window.Twitch.ext.onAuthorized`
etc. into signals. May land in lite-twitch instead of here -- TBD where the
right home is.

**Status:** discussed, undecided on package boundary.

## Considered and rejected

### Signal-pooling for `fromPromise` result
Tempting to pool the `{status, data, error}` object since exactly two
transitions happen. Empirically the pool overhead (set-then-clear of three
fields, plus pool lookup) is larger than the GC cost of two object literals
per Promise lifecycle. **Rejected** as candidate #1 (~2024-06-22, sketched
in this branch, never landed).

### `withDeadline(promise, atUnixMs)` -- absolute deadline variant
The mental model of "deadline at this clock time" is foreign to JS code;
relative `timeout: ms` is what every API in the platform uses. **Rejected**
as added confusion without a clear win.

### Configurable cleanup policies
"What if the user wants to keep the effect alive after first match?" --
that's not what `whenSignal` is. They want `watch` or `watchChanged` from
WatchEx.js. **Rejected** to keep the primitive single-purpose.

### Wrapping `lite-signal`'s `whenAsync`
Considered making `whenSignal(source, predicate, opts)` wrap
`whenAsync(() => predicate(source()))` and adding the timeout/abort layer
externally. The composition has subtle timing: `whenAsync` resolves on the
next predicate-truthy state-change, not on the satisfying VALUE, so wrapping
loses access to the value. Building directly on `effect` + `untrack` gives
us value-return and structural cleanup in one piece. **Rejected** as
architecturally awkward.

## Non-goals (will never land here)

- Cache management. That's `@zakkster/lite-query`.
- Schedulers / priorities. That's `@zakkster/lite-clock`.
- A full observable / stream abstraction. Departs from the "lite"
  philosophy; the right home is a separate package with a clearly different
  scope.
- DOM-specific helpers (debounce, throttle, requestIdleCallback wrappers).
  Those are platform concerns and don't belong in an async-coordination
  primitive.
- Network helpers (fetch wrappers, parse-and-validate). Out of scope; the
  consumer should compose with `withTimeout` / `withAbort` themselves.

## Versioning policy

- **Patch (1.0.x):** documentation, internal optimizations, bug fixes that
  don't change observable behavior. No API additions.
- **Minor (1.x.0):** additive only. New exports, new options on existing
  primitives. Existing call sites must continue to work unchanged.
- **Major (x.0.0):** only for API-level changes. The library is small enough
  that the bar for a major version bump is very high.
