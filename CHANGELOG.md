# Changelog

All notable changes to `@zakkster/lite-query` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] — Unreleased

Integration release: **streaming queries** (via `@zakkster/lite-stream`) and
**async coordination** (via `@zakkster/lite-await`). The core `@zakkster/lite-query`
entry point and its three peer dependencies are unchanged — all new capability
ships behind opt-in subpath exports with optional peer deps. See `ROADMAP.md`
for the full design and rationale.

### Added

- **Streaming queries** — `@zakkster/lite-query/stream` → `streamQuery(qc, opts)`.
  A multi-shot, iterator-backed query: subscribe by key to an async iterable
  (SSE, websocket frames, paginated cursors, pubsub topics) and read its state
  as signals. Built on `lite-stream`'s `fromAsyncIterable`.
  - Modes: `"latest"` (signal holds the most recent value) and `"buffer"`
    (signal holds a bounded ring of recent values; `maxBuffer` required —
    unbounded buffering is rejected by design).
  - Accessors: `data()` / `error()` / `status()` / `done()` / `count()` /
    `droppedCount()`, plus `restart()` and `dispose()`.
  - Lazy (no observers → no iterator pulled), structural abort-on-detach
    (`iterator.return()` via the abort signal), reactive-key restart, and an
    `enabled` gate — the same lifecycle guarantees as `query()`.
  - Lives in the same `queryClient` cache: `getQueryData` / `invalidate` /
    `removeQueries` operate on stream entries uniformly.
- **New status value `"streaming"`** for stream entries, extending the query
  vocabulary to `idle | pending | streaming | success | error`. `pending` =
  subscribed, no value yet; `streaming` = at least one value, not done;
  `success` = iterator completed naturally; `error` = iterator threw or aborted.
- **Async coordination** — `@zakkster/lite-query/await`. Re-exports the
  `lite-await` primitives verbatim (`whenSignal`, `whenTruthy`, `whenEquals`,
  `allOf`, `anyOf`, `raceOf`, `withTimeout`, `withAbort`, `fromPromise`,
  `TimeoutError`) plus two query-native bridges:
  - `whenQuery(q, predicate?, opts?)` — resolves with `q.data()` when the query
    reaches `success` (or a custom predicate over `status()`); rejects with
    `q.error()` on `error`. Honors `timeout` and `signal`.
  - `whenAllQueries(queries, opts?)` — resolves with the data array in input
    order when every query reaches `success`; rejects on the first error or
    timeout. Built on `allOf`.
- **`fromPromise` re-export** — positioned as the "queryless query": one-shot
  promise → reactive `{ status, data, error }` with no cache, for the cases
  where caching is genuinely unwanted. Its native `pending/resolved/rejected`
  vocabulary is documented with an explicit mapping to lite-query's
  `success/error` (`resolved → success`, `rejected → error`).

### Changed

- **Minimum `@zakkster/lite-signal` is now `>=1.5.0`.** The query/stream watcher
  is created inside `createRoot` (new in lite-signal 1.5.0) so it is not adopted
  by the consumer effect that triggers the first read. lite-signal's owner tree
  (introduced in 1.2.0) otherwise cascade-disposes the watcher on the consumer's
  next re-run, which silently broke reactive keys and refetch on lite-signal
  1.2.0-1.4.x. This is the ecosystem's first use of the `createRoot` escape
  hatch. lite-query owns the watcher's lifecycle explicitly, so detaching it from
  the owner tree costs no auto-cleanup.
- `package.json` gains subpath exports (`.`, `./stream`, `./await`) and two
  **optional** peer dependencies (`@zakkster/lite-stream`, `@zakkster/lite-await`
  via `peerDependenciesMeta.optional`). Core-only consumers see no new install
  requirement and no peer-dep warnings.
- Cache-entry teardown paths (`detach` on last observer, `removeQueries`,
  `clear`, GC eviction) now also stop an active stream pump when present
  (`streamStop` -> `iterator.return()`, closing the connection). The stream slots
  on an entry are uniform (`isStream`/`streamStop`/`streamRestart`/`streamCount`/
  `streamDropped`, all null/false/0 by default) so the entry stays monomorphic;
  a plain `query()` entry allocates no stream signal node.
- `invalidate` on a stream entry aborts and re-establishes the stream (rather
  than refetching), via the `streamRestart` hook the active stream installs.

### Documentation

- README: "Streaming queries" and "/await" sections; facts-table row for
  streaming (lite-query built-in vs TanStack experimental `streamedQuery` vs
  SWR none); exports table; optional-dependency install notes.
- Cookbook: streaming recipes (SSE feed, paginated-Helix accumulation in buffer
  mode, pubsub-topic latest, websocket presence); coordination recipes
  (await-all-queries, race-cache-vs-timeout, route guard via `whenQuery`).
  Recipe #11 migrated from `lite-watch-ex`'s `watchOnce` to `whenQuery`.
- New `ROADMAP.md` documenting the integration architecture, the
  re-export-don't-reimplement decision, the status-vocabulary reconciliation,
  and the 1.2 "shared streams" direction.

### Tested

- `test/awaitable.test.js` (18 tests): `whenQuery` / `whenAllQueries`
  success / error / timeout / abort paths, the `whenQuery(q, opts)` overload,
  plus re-export smoke including a `fromPromise` projection.
- `test/stream-query.test.js` (15 tests): latest- and buffer-mode value
  progression, all three termination paths (natural done, iterator throw,
  abort-on-detach calling `iterator.return()`), lazy subscription, shared
  observers sharing one pump, reactive-key restart, `enabled` gate,
  `getQueryData`/`invalidate`/`removeQueries` interop, imperative `restart()`,
  and coexistence of a `streamQuery` and a `query` in one client.
- Full suite green on lite-signal 1.5.0-alpha: **152 pass / 0 fail** across the
  core 120 (incl. one pre-existing skip), the 18 await tests, and the 15 stream
  tests. The core suite's reactive-key tests -- which regressed on lite-signal
  1.2.0-1.4.x -- pass again with the `createRoot` watcher fix.

### Notes / known limitations

- **Signal disposal.** `streamQuery` disposes the `fromAsyncIterable` stream-state
  signal on restart, `removeQueries`, `clear`, and GC eviction (lite-stream does
  not auto-dispose it). The pump's stop fn (`iterator.return()`, closing the
  underlying connection) fires on every teardown path. GC eviction and `clear()`
  also dispose the entry's `data`/`error`/`status`/`fetching` signals; the
  `/await` bridges allocate nothing requiring lite-query-side cleanup. Re-exported
  `fromPromise` returns a consumer-owned signal — call `dispose()` on it per
  lite-await's contract.
- **Entry shape stays monomorphic.** The three stream slots (`isStream`,
  `streamStop`, `streamState`) are added uniformly to every entry as
  `false`/`null`; a stream signal node is allocated only when a stream runs. The
  `promise` slot is not reused for the pump (it backs the fetch-dedup guard).
- **Streaming data does not cross-tab broadcast.** Each tab owns its own
  connection (SSE/websocket is per-document). `invalidate` and `removeQueries`
  still propagate cross-tab (every tab aborts and re-establishes its own stream).
  Leader-election **shared streams** — one upstream connection per browser, frames
  broadcast to follower tabs, the multi-shot dual of 1.0's shared fetch — are the
  planned 1.2 headline.

## [1.0.0] — 2026-05-28

Initial release.

### Added

- `queryClient()` — cache and lifecycle owner; supports `defaultStaleTime`,
  `defaultCacheTime`, `defaultTimeout`, `retry`, `retryDelay`, `crossTab`,
  injectable `now` / `setTimeout` / `clearTimeout` / `broadcastChannel`.
- `query(qc, opts)` — reactive query factory with lazy observer tracking, same-
  entry detection on watcher re-runs, generation guard, retry-with-backoff,
  abort-on-detach, stale-while-revalidate.
- `mutation(qc, opts)` — `onMutate` / `onSuccess` / `onError` / `onSettled`
  callback chain with `mutationGen` race protection and callback-error
  containment (`onSettled` always fires).
- Cache operations: `getQueryData` / `setQueryData` (value or updater fn) /
  `invalidate` (prefix-match by default, `{exact:true}` for precise) /
  `removeQueries` / `clear` / `dispose`.
- **Cross-tab cache coherence** via `BroadcastChannel` for explicit cache
  mutations (`setQueryData`, `invalidate`, `removeQueries`, `clear`). Echo
  suppression via `processingRemote` flag. Opt-in (`crossTab: true`).
- **Cross-tab fetch deduplication** via leader election (`sharedFetch: true` +
  injectable `isLeader`). Follower tabs broadcast a `fetch-req` instead of
  fetching; the leader fulfills it once and broadcasts the result. Each
  follower arms a fallback timer (`sharedFetchTimeout`, default 3000ms) and
  self-fetches if no leader serves — a liveness guarantee independent of the
  election state. Composes with `@zakkster/lite-channel`'s leader signal with
  no hard dependency. The feature TanStack Query and SWR don't ship.
- Per-query and client-default `timeout` option. Aborts the fetch with
  `ABORT_REASON.TIMEOUT` reason on the `AbortSignal`.
- Abort reason vocabulary: `ABORT_REASON.DETACH | REFETCH | REMOVED | TIMEOUT`
  exposed via `signal.reason` for retry/logging decisions in user fetchers.
- Mid-flight invalidation semantics: option (b) — let the in-flight finish,
  then refetch immediately.
- Opt-in `equals` per query for structural sharing without re-firing effects
  on referentially-different-but-structurally-equal data.

### Tested

- 106 deterministic tests across 22 sections.
- Adversarial cases: mutation race (slow first + fast second), `onSuccess`
  throw with `onSettled` still firing, shared-observer mid-fetch dispose,
  cross-tab race conditions, reactive `enabled` → false abort, sparse-key
  cache hits, three forms of abort reasons.
- Shared-fetch coverage: follower defers to leader broadcast; leader fulfils a
  follower request for a non-observed-but-cached query; follower fallback
  self-fetch on absent leader; leader's own observed fetch broadcasts to
  followers; `sharedFetch` inert without `isLeader`; follower `refetch()`
  defers to the leader.

### Architecture decisions documented in code

- Observer tracking: explicit refcount + microtask-deferred watcher disposal.
- Same-entry watcher re-runs don't churn detach/re-attach.
- Invalidation tracked via a boolean flag, not a timestamp (avoids same-tick
  precision bug).
- Cross-tab data broadcast is kept (the differentiator from TanStack/SWR).
- Shared-fetch fallback guarantees liveness without depending on election
  timing; the leader's result-broadcast is async so it isn't suppressed by the
  `processingRemote` guard.
- GC + timeout + shared-fallback timers `unref()`'d in Node for clean exit.
