# lite-query 1.1.0 — Integration Roadmap

Folding `@zakkster/lite-await` and `@zakkster/lite-stream` into lite-query to add **streaming queries** and **async coordination**, without touching the core's dependency footprint.

---

## TL;DR — the decision

1. **Re-export, don't reimplement.** lite-await and lite-stream are published and tested. lite-query consumes them; it does not copy their code. Single source of truth.
2. **The headline is streaming queries, not `fromPromise`.** `fromPromise` is the async-state *shape* that `query()` already supersedes with a cache. `lite-stream` gives lite-query something it genuinely lacks: multi-shot (iterator-backed) queries.
3. **Subpath exports keep the core lean.** `@zakkster/lite-query` keeps today's exact three peer deps. New capabilities live behind `/stream` and `/await`, with lite-stream and lite-await as *optional* peer deps. Core-only users install nothing new.
4. **Vocabulary is reconciled deliberately.** A new `"streaming"` status extends lite-query's `idle | pending | success | error`. `fromPromise`'s native `pending/resolved/rejected` is documented, not force-wrapped.

---

## Why these two, and why now

lite-query 1.0 covered the *single-value, cached, refetchable* case completely: keys, observers, staleness, retry, invalidation, cross-tab, and leader-election fetch dedup. The gap it leaves is the *stream*: data that arrives as N values over time rather than one promise. Today a developer who wants a live Helix cursor, a PubSub topic, or an SSE feed in the signal world has to drop out of lite-query entirely and hand-roll an iterator pump.

`lite-stream` closes exactly that gap, and its design targets — "paginated APIs, SSE streams, network frame queues, pubsub topics" — line up with the `lite-twitch-*` SDK on the horizon. A streaming query is the primitive a `lite-twitch-pubsub` or `lite-twitch-helix` consumer wants: subscribe by key, get reactive state, abort on unmount, bound the buffer so a chatty topic can't OOM the tab.

`lite-await` is the smaller, complementary win. Its combinators (`whenSignal`, `allOf`, `anyOf`, `raceOf`) are the right tool for *coordinating* query state imperatively — "await all three queries to reach success," "race the cache against a 200ms timeout," "block this route guard until the user query settles." lite-query 1.0's Cookbook reaches for `lite-watch-ex`'s `watchOnce` for this; `lite-await` does it better (first-class timeout + AbortSignal, structural cleanup).

---

## Architecture decision: re-export + adapt

### Why not reimplement `fromPromise` in lite-query

The temptation is to absorb `fromPromise` into Query.js so there's "one import." Reject it:

- **Drift.** Two implementations of the same promise-to-signal projection will diverge on edge cases (settle-once guards, abort semantics, initial-data handling). The published one is already tested.
- **It contradicts the platform thesis.** The entire strategic argument for the `@zakkster/*` ecosystem is that the libraries *compose*. lite-query re-exporting lite-await is that thesis made concrete. lite-query copy-pasting lite-await is the thesis refuted.
- **`query()` already does promises better.** For the cached case, `query({ key, fetcher })` beats `fromPromise` on every axis. The only thing `fromPromise` adds is the *no-cache* path, which is a thin escape hatch, not a feature worth owning twice.

### What "adapt" means

Re-export the primitives verbatim, and add a **thin bridge layer** where lite-query's own vocabulary needs to win:

- `streamQuery` (new) — wraps `lite-stream`'s `fromAsyncIterable` but exposes lite-query-shaped accessors (`data()/error()/status()/done()`), normalized status, and the cache/observer/GC lifecycle.
- `whenQuery` / `whenAllQueries` (new) — wrap `lite-await`'s `whenSignal` / `allOf` but speak in queries (`whenQuery(q)` resolves with `q.data()` on success, rejects with `q.error()` on error).
- `fromPromise`, `whenSignal`, `allOf`, etc. — re-exported as-is for power users.

---

## The vocabulary problem

Three status vocabularies are in play:

| Source | Vocabulary |
|---|---|
| lite-query `query()` | `idle` · `pending` · `success` · `error` |
| lite-await `fromPromise` | `pending` · `resolved` · `rejected` |
| lite-stream state | no status string; `{ done: bool, error: any, count, ... }` |

Reconciliation rules for 1.1.0:

- **`streamQuery` normalizes into lite-query's vocabulary, extended by one value.** New status set: `idle | pending | streaming | success | error`, where `pending` = subscribed but no value yet, `streaming` = at least one value and not done, `success` = iterator completed naturally, `error` = iterator threw / aborted. Stream-specific facts (`done`, `count`, `droppedCount`) are separate accessors, not statuses.
- **`fromPromise` is re-exported unchanged.** Its `pending/resolved/rejected` is documented with an explicit mapping table (`resolved → success`, `rejected → error`). We do **not** wrap it to renormalize — that would reintroduce drift. If the mismatch proves to be a real ergonomic tax in practice, the fix belongs upstream (a vocabulary option in lite-await 1.1), not in a lite-query wrapper.

---

## Package shape

### `package.json` exports (target)

```jsonc
{
  "version": "1.1.0",
  "exports": {
    ".":        { "import": "./Query.js" },
    "./stream": { "import": "./StreamQuery.js" },
    "./await":  { "import": "./Awaitable.js" }
  },
  "peerDependencies": {
    "@zakkster/lite-signal":  "^1.1.3",
    "@zakkster/lite-store":   "^1.0.0",
    "@zakkster/lite-channel": "^1.0.0",
    "@zakkster/lite-stream":  "^1.0.0",
    "@zakkster/lite-await":   "^1.0.0"
  },
  "peerDependenciesMeta": {
    "@zakkster/lite-stream": { "optional": true },
    "@zakkster/lite-await":  { "optional": true }
  },
  "files": [
    "Query.js", "StreamQuery.js", "Awaitable.js",
    "README.md", "QuickStart.md", "Cookbook.md", "CHANGELOG.md", "LICENSE", "llms.txt"
  ]
}
```

The two new deps are **optional** peers: npm won't warn a core-only consumer, and the `/stream` and `/await` entry points fail loudly (clear import error) only if used without their dep installed.

### File layout

- `Query.js` — core. **Minimal, additive changes only** (see Phase 2: stream-aware teardown in shared lifecycle paths).
- `StreamQuery.js` — new. `streamQuery()` built on lite-stream. Imports the core client's internals via the existing `_internal` seam.
- `Awaitable.js` — new. Re-exports lite-await + the two query bridges. (Named `Awaitable.js`, not `Await.js`, to avoid colliding with the upstream filename in mental models and to dodge `await` as a reserved word in tooling that string-matches filenames.)

---

## New public surface

### `@zakkster/lite-query/stream`

```ts
streamQuery(qc, {
  key: any[] | (() => any[]),
  stream: ({ key, signal }) => AsyncIterable<T> | AsyncIterator<T>,
  mode?: "latest" | "buffer",          // default "latest"
  maxBuffer?: number,                   // REQUIRED iff mode === "buffer"
  enabled?: boolean | (() => boolean),
  cacheTime?: number,
  restartOnKeyChange?: boolean,         // default true
}): {
  data:         () => T | T[] | undefined,  // latest value, or buffer array
  error:        () => unknown,
  status:       () => "idle" | "pending" | "streaming" | "success" | "error",
  done:         () => boolean,
  count:        () => number,
  droppedCount: () => number,               // 0 outside buffer mode
  restart:      () => void,
  dispose:      () => void,
}
```

Semantics:

- **Lazy**, exactly like `query()`: no observers reading an accessor inside an effect → no subscription, no iterator pulled.
- On first observer, calls `stream({ key, signal })` and pipes it through `fromAsyncIterable` into the cache entry's signals. `signal` aborts on detach / key change / `restart()` / `removeQueries`.
- On last observer leaving, the stream is aborted (`iterator.return()` via the abort signal — lite-stream's structural cleanup), and the entry is GC-scheduled after `cacheTime`.
- **Reactive key** restarts the stream on a real key change (abort old, start new) when `restartOnKeyChange` is true.
- Lives in the **same `queryClient` cache** as `query()`: `qc.getQueryData(key)` returns the current stream state, `qc.invalidate(key)` / `qc.removeQueries(key)` work uniformly.
- **Streaming data does not cross-tab broadcast.** Each tab owns its own connection (an SSE/websocket is per-document). Invalidation/removal still broadcast. (Cross-tab stream *sharing* is the 1.2 headline — see Future.)

### `@zakkster/lite-query/await`

Re-exported from lite-await, verbatim:

```
whenSignal · whenTruthy · whenEquals
allOf · anyOf · raceOf
withTimeout · withAbort
fromPromise
TimeoutError
```

Added bridges (lite-query-native):

```ts
// Resolve when a query reaches a state; reject on error.
// Default predicate: status === "success". Resolves with q.data().
whenQuery<T>(
  q: QueryHandle<T>,
  predicate?: (status: string) => boolean,
  opts?: { timeout?: number, signal?: AbortSignal }
): Promise<T>

// Resolve when ALL queries satisfy success (or a shared predicate).
// Resolves with the data array in input order. Rejects on first error/timeout.
whenAllQueries<T>(
  queries: QueryHandle<T>[],
  opts?: { timeout?: number, signal?: AbortSignal }
): Promise<T[]>
```

`whenQuery` is a thin `whenSignal(() => q.status(), ...)` with an error-status escape that rejects using `q.error()`. `whenAllQueries` is `allOf` over `[() => q.status(), s => s === "success"]` specs, mapping results back to `q.data()`.

### `fromPromise` positioning

Re-exported via `/await`. Documented as the **queryless query**: one-shot promise to reactive `{status, data, error}` with no cache, no key, no refetch. Use it when caching is genuinely unwanted (a fire-once mutation result, a config blob fetched once at boot). Reach for `query()` the moment you want any of keys, dedup, refetch, invalidation, or cross-tab. Vocabulary mapping documented inline.

---

## Signal disposal discipline

Zero-GC means every allocated signal node is accounted for and returned to the pool. The integration's disposal contract:

1. **`entry.streamState` (the `fromAsyncIterable` signal) is the one that leaks if ignored.** lite-stream does not auto-dispose it — its docs are explicit that the caller calls `dispose()`. `streamQuery` disposes `entry.streamState` on: stream **restart** (key change / invalidate re-establish — dispose old before new), `removeQueries`, `clear`, and cacheTime **GC eviction**.
2. **`entry.streamStop()` is resource cleanup, distinct from signal disposal.** It triggers `iterator.return()`, closing the underlying SSE/websocket, and fires on every teardown path: last-observer detach, restart, removeQueries, clear, GC.
3. **General hardening (query and stream entries alike):** on GC eviction and `clear()` — provably zero observers — dispose the entry's owned signals (`data`/`error`/`status`/`fetching`, plus `streamState` if present). On `removeQueries` with *live* observers, abort/stop but defer signal disposal to the subsequent GC, so the watcher cannot read a disposed signal in the gap before it re-attaches.
4. **Re-exported `fromPromise`:** the returned signal is the consumer's to own; lite-await's `dispose()` contract is surfaced in the docs, not silently dropped.
5. **The `/await` bridges allocate nothing to dispose.** `whenQuery` / `whenAllQueries` create effects only via `whenSignal` / `allOf`, which lite-await tears down structurally on resolve/reject/timeout/abort. No lite-query-side cleanup.

---



### Phase 0 — Plumbing (half a day) -- DONE

- Bump to `1.1.0`.
- Add optional peer deps + `peerDependenciesMeta`.
- Add subpath exports; create empty `StreamQuery.js` / `Awaitable.js` stubs that throw a clear "not yet implemented" if imported.
- **Exit gate:** core 106 tests unchanged and green; `import "@zakkster/lite-query"` byte-identical behavior.
- **Result:** `package.json` shipped with the three subpath exports and optional peers; `StreamQuery.js` is a throwing Phase-2 stub; both `./await` and `./stream` resolve by package name. Verified against published `@zakkster/lite-signal@1.2.2`, `@zakkster/lite-await@1.0.0`, `@zakkster/lite-stream@1.0.0`.

### Phase 1 — Async coordination `/await` (1 day) -- DONE

- `Awaitable.js`: re-export lite-await primitives; implement `whenQuery`, `whenAllQueries`.
- Tests (new file `test/awaitable.test.js`): `whenQuery` resolves on success / rejects on error / honors timeout / honors abort; `whenAllQueries` resolves in order / rejects on first failure; re-export smoke tests.
- Cookbook: migrate recipe #11 (`watchOnce`) to `whenQuery`; add "await all queries", "race cache vs timeout", "route guard with whenQuery".
- **Exit gate:** new tests green; core untouched; Cookbook recipes runnable.
- **Risk:** none to core (purely additive new file).
- **Result:** `Awaitable.js` shipped; `whenQuery` uses the throwing-predicate path into `whenSignal` (rejects with `q.error()`, resolves with `q.data()`), with a `whenQuery(q, opts)` overload; `whenAllQueries` is fail-fast over `allOf`, data array in input order. **18/18 tests pass** against the published packages. Cookbook migration deferred to Phase 3 (docs pass) since the core/Cookbook files live in the 1.0 tree.

### Phase 2 — Streaming queries `/stream` -- DONE

- Add **three uniform slots** to the cache entry shape in `Query.js` — `isStream` (false), `streamStop` (null), `streamState` (null) — on *every* entry, initialized in `createEntry`. Uniform slots keep the entry monomorphic (no second hidden class at the hot `attach`/`detach`/GC sites); a `streamState` *signal node* is allocated only when a stream actually runs. `promise` is left alone (the fetch-dedup guard reads it).
- Add stream-stop + signal-disposal to the shared teardown paths (see "Signal disposal discipline"), each guarded by `if (entry.streamStop)` / `if (entry.streamState)`.
- `StreamQuery.js`: `streamQuery()` with its own watcher that, on attach, starts the `fromAsyncIterable` pump and points `entry.streamState` at its signal; on key change / detach, aborts via `streamStop`. Reuses `_internal.{ensureEntry, attach, detach}`.
- `getQueryData` / `invalidate` become stream-aware via `entry.isStream`: `getQueryData` returns the current value/values from `streamState`; `invalidate` aborts and re-establishes the stream (dispose old `streamState`, start new).
- Status normalization (`pending → streaming → success/error`).
- Tests (`test/stream-query.test.js`): latest mode value progression; buffer mode ring + `droppedCount`; natural `done` → `success`; iterator throw → `error`; abort-on-detach calls `iterator.return()`; reactive key restart; `enabled` gate; `getQueryData`/`removeQueries` interop; **signal-disposal assertions** (restart and removal dispose `streamState`).
- **Exit gate:** new tests green; **core 106 tests still green**; a `streamQuery` and a `query` coexist in one client in an integration test.
- **Risk:** entry-shape + shared-teardown edits touch code the 106 tests exercise. Mitigation: uniform guarded slots; full suite run after every edit; documented fallback to a separate stream registry if a clean seam proves elusive.
- **Result:** shipped. `streamQuery` pumps via lite-stream `pipeToSignal` into the entry's existing `data` signal -- **zero extra signals allocated** (so nothing extra to dispose; the whole entry is released by `disposeEntry` on GC/remove/clear, which also calls `streamStop`). Latest-mode hot path is one signal write per frame, zero alloc; buffer mode allocates a windowed snapshot per element. Uniform stream slots (`isStream`/`streamStop`/`streamRestart`/`streamCount`/`streamDropped`) keep the entry monomorphic. `getQueryData`/`removeQueries` work unchanged; `invalidate` restarts the stream. **15 stream tests pass; full suite 152 pass / 0 fail** (core 120 + await 18 + stream 15, one pre-existing core skip).
- **Dependency consequence:** the watcher fix imports `createRoot`, so lite-query 1.1.0 now requires **lite-signal >= 1.5.0** (where `createRoot` lands). This also resolves the 1.2.0-introduced owner-tree regression that broke reactive keys on the published 1.2.x line.

### Phase 3 — Docs, demo, release (1-2 days)

- README: "Streaming queries" section; "/await" section; facts-table row (streaming: lite-query built-in vs TanStack experimental `streamedQuery` vs SWR none); exports table; updated install notes for optional deps.
- Cookbook: SSE feed, paginated-Helix accumulation (buffer mode), PubSub-topic latest, websocket presence.
- llms.txt + SPEC update; test count and recipe count bumps.
- Demo: a separate `stream-query-demo.html` — chunked ingest into zero-GC buffers piped through the cache to a 60fps reactive UI, latest/buffer toggle, live `droppedCount` and heap readout under load. Cross-tab claim limited to `invalidate`-and-re-establish (per the 1.1.0 scope note).
- Publish: `npm test` → `npm publish` → `git tag v1.1.0`.

**Total: ~5-7 working days**, front-loadable (Phase 1 ships value on day 1; Phase 2 is the meat).

---

## Test plan (additions)

- `test/awaitable.test.js` — ~10 tests: bridges + re-export smoke.
- `test/stream-query.test.js` — ~14 tests: both modes, all three termination paths, lazy/abort/restart/enabled, cache interop.
- Reuse the existing harness (`createMockClock`, mock `BroadcastChannel`); add a `createControlledAsyncIterable()` helper (manually push / done / throw) mirroring `createControlledFetcher`.
- **Hard gate every phase: the existing core suite stays at its current count and 0 failures.**

---

## Risks and mitigations

- **Core regression from Phase 2 entry edits.** Mitigation: optional, guarded stream fields; full-suite run per edit; documented fallback to a separate registry.
- **`fromPromise` vocabulary tax.** Mitigation: document the mapping for 1.1.0; push true alignment upstream into lite-await 1.1 rather than wrapping.
- **Bundle creep.** Mitigation: subpath exports + `sideEffects: false`; core import unchanged; streaming/await code only enters a bundle when imported.
- **Optional-peer friction.** Mitigation: `peerDependenciesMeta.optional`; entry points throw an actionable error naming the missing package.
- **Stream backpressure surprises.** Mitigation: buffer mode requires an explicit `maxBuffer` (inherited from lite-stream's refusal to buffer unbounded); document `droppedCount` as the observability hook.

---

## Future — 1.2 preview: shared streams

The natural sequel, and the reason the streaming work matters strategically. lite-query 1.0 shipped **shared fetch**: the leader tab issues a request, followers receive the result over `BroadcastChannel`. A **shared stream** is its multi-shot dual — the leader holds *one* SSE/websocket connection and broadcasts each frame to followers, so five open tabs mean one connection to your EBS, not five.

For Twitch Extensions this is the headline feature: a viewer with the panel open in multiple tabs, or an extension that would otherwise multiply PubSub connections per tab, collapses to a single upstream connection per browser. It reuses the exact `isLeader` oracle and fallback-timer machinery already built for shared fetch. Scoped out of 1.1.0 to keep the streaming primitive landing first and clean, but it is the obvious 1.2 anchor.

---

## Locked decisions

1. **`whenQuery` rejects with `q.error()`.** No settle-always variant. Implemented via a *throwing predicate* into `whenSignal` (`if (s === "error") throw q.error()`), which lite-await routes to `doReject` — so it mirrors `whenSignal`'s settlement semantics exactly instead of bolting on a parallel reject path. Non-throwing await is a consumer's `try/catch` or `Promise.allSettled`.
2. **New status `"streaming"`.** Reusing `"pending"` would make `loading()` (keyed off `status() === "pending"`, only entered with empty data) lie about a stream that has hot partial data; reusing `"success"` would make `done()` the only live/finished discriminator. Machine: `idle → pending → streaming → success | error`. `loading()` stays `pending`.
3. **Unified cache, monomorphic entry.** `streamQuery` entries live in the same `queryClient` cache so `getQueryData` / `invalidate` / `removeQueries` work uniformly. The entry gains three **uniform** slots on *every* entry (query and stream) to stay monomorphic: `isStream` (false), `streamStop` (null), `streamState` (null). A stream signal node is allocated only when a stream actually runs. **`promise` is NOT reused for the pump** — the fetch-dedup guard is `if (entry.promise) return`, and aliasing it with a stream handle would misfire that guard and confuse the mid-flight-invalidation logic. `streamStop` is a dedicated slot at the same memory cost.
4. **Separate `stream-query-demo.html`.** Keeps the fetch-dedup demo focused; gives the streaming marquee (chunked ingest → zero-GC buffers → cache → 60fps, no hot-path concatenation) its own stage. **Scope note:** in 1.1.0 streaming data does not cross tabs (each tab owns its connection); the demo's only cross-tab claim is `invalidate` propagation (invalidate a key → every tab aborts and re-establishes its own stream). One-connection-shared-across-tabs is 1.2 (shared streams).
