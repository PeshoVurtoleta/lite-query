# @zakkster/lite-query

Reactive async cache with cross-tab coherence. Built on `@zakkster/lite-signal`. ~6KB minified+gzipped, framework-agnostic, 106 tests, zero runtime dependencies outside the lite ecosystem.

[![npm version](https://img.shields.io/npm/v/@zakkster/lite-query.svg?style=for-the-badge&color=latest)](https://www.npmjs.com/package/@zakkster/lite-query)
![Zero-GC](https://img.shields.io/badge/Zero--GC-Hot%20path-00C853?style=for-the-badge&logo=leaf&logoColor=white)
[![sponsor](https://img.shields.io/badge/sponsor-PeshoVurtoleta-ea4aaa.svg?logo=github)](https://github.com/sponsors/PeshoVurtoleta)
[![npm bundle size](https://img.shields.io/bundlephobia/minzip/@zakkster/lite-query?style=for-the-badge)](https://bundlephobia.com/result?p=@zakkster/lite-query)
[![npm downloads](https://img.shields.io/npm/dm/@zakkster/lite-query?style=for-the-badge&color=blue)](https://www.npmjs.com/package/@zakkster/lite-query)
[![npm total downloads](https://img.shields.io/npm/dt/@zakkster/lite-query?style=for-the-badge&color=blue)](https://www.npmjs.com/package/@zakkster/lite-query)
![TypeScript](https://img.shields.io/badge/TypeScript-Types-informational?style=flat-square)
[![lite-signal peer](https://img.shields.io/npm/dependency-version/@zakkster/lite-query/peer/@zakkster/lite-signal?style=for-the-badge&color=blue)](https://github.com/PeshoVurtoleta/lite-signal)
![Dependencies](https://img.shields.io/badge/runtime%20deps-0-brightgreen?style=flat-square)
[![license](https://img.shields.io/badge/license-MIT-blue?style=flat-square)](LICENSE.txt)

**~3.9× faster on invalidate, ~3.7× faster on mutations, ~2.5× faster at 1000-concurrent-query scaling, with up to 10× less transient memory** vs `@tanstack/query-core` ([see Performance](#performance)). Cross-tab cache coherence and cross-tab fetch dedup built in.

```js
const todos = query(qc, {
  key: () => ['todos', filter()],
  fetcher: async ({ key, signal }) => fetch(`/api/${key.join('/')}`, { signal }).then(r => r.json()),
});

effect(() => {
  if (todos.loading()) render('Loading...');
  else if (todos.error()) render(`Error: ${todos.error().message}`);
  else render(todos.data());
});
```

That's the entire surface. Two functions and a client. The library is small because the *idea* is small.

## Why does this exist?

Three reasons.

**Cross-tab cache coherence as a first-class feature.** Open your app in two tabs. Update a todo in one. The other tab's UI updates instantly — same cache state, no extra fetch, no polling, no service worker. Just `BroadcastChannel`, opt-in (`crossTab: true`), and the library handles the propagation. TanStack Query and SWR don't ship this — you can wire it manually with their plugins, but it's not in the box.

**Cross-tab fetch deduplication — five tabs, one request.** This is the feature no other query library ships. With `sharedFetch: true` and a leader oracle wired from `@zakkster/lite-channel`, only the *leader* tab issues network requests; every other tab receives the result over `BroadcastChannel`. Five tabs polling the same dashboard stop hammering your API five times per cycle — the leader fetches once and shares. Followers fall back to self-fetching if no leader can serve them, so correctness never depends on the election state. (See [Cross-tab fetch deduplication](#cross-tab-fetch-deduplication) below.)

**Framework-agnostic.** No `useQuery` hook. No React, no Solid, no Vue dependency. The return value of `query()` is a plain object with signal accessors (`data()`, `error()`, `loading()`, `status()`, `fetching()`). Read them inside an `effect` from `@zakkster/lite-signal` and you get reactivity. Wrap them in 20 lines if you want a React/Vue integration. The core stays lean.

**Honest size.** The whole library is 710 lines of JS, ~5–6 KB minified+gzipped (plus its peer deps in the lite ecosystem). For comparison: TanStack Query's core is ~13 KB, SWR is ~5 KB. Lite-query lands in the same weight class as SWR while shipping a richer feature set (retry, mutations, cross-tab) and shedding the React lock-in.

## Facts table

The honest comparison. Numbers are min+gzip, current as of writing.

| | lite-query | TanStack Query | SWR |
|---|---|---|---|
| Bundle (core, min+gzip) | ~6 KB | ~13 KB | ~5 KB |
| Framework requirement | None | React / Vue / Solid / Svelte adapter | React |
| Cross-tab cache sync | **Built-in (opt-in)** | Manual (plugin) | Manual |
| Cross-tab fetch dedup (leader election) | **Built-in (opt-in)** | No | No |
| Stale-while-revalidate | Yes | Yes | Yes |
| Retry with backoff | Yes | Yes | No (manual) |
| Optimistic updates + rollback | Yes (`onMutate` ctx) | Yes (`onMutate` ctx) | Yes (`mutate` API) |
| Mutation race protection (`mutationGen`) | Yes | Yes | Partial |
| Reactive keys | Native (signals) | Via framework state | Via framework state |
| Multi-shot / streaming queries | **`streamQuery` (SSE/ws/cursor)** | Experimental (`streamedQuery`) | No |
| Abort reason vocabulary | Yes (`signal.reason`) | No | No |
| Per-query timeout | Yes | No (manual via fetcher) | No (manual via fetcher) |
| Devtools UI | Roadmap | Yes (mature) | Yes |
| Tests | 152 | ~hundreds | ~hundreds |
| Foundation | Signals (lite-signal) | Observer pattern | SWR algo + hooks |

Where lite-query trails: pagination is a Cookbook recipe rather than a built-in API, and the devtools panel is on the roadmap. Where it leads: cross-tab and the signal-native composition story.

## Performance

Measured against `@tanstack/query-core` 5.101 on Node 22 (SWR is React-coupled — no framework-agnostic core to compare against, excluded for honest apples-to-apples). Same fetcher, same keys, same observer pattern. Both libraries run their full lookup, observer, and cleanup paths — nothing stubbed.

| Scenario | lite-query | TanStack query-core | Speedup |
|---|---:|---:|---:|
| Cold attach → resolve → dispose | ~25,000 ops/sec | ~19,000 ops/sec | ~1.25× |
| Warm cache hit (already resolved) | ~400,000 ops/sec | ~320,000 ops/sec | ~1.2× |
| **Invalidate 50 observed queries** | **~10,000 ops/sec** | **~2,800 ops/sec** | **~3.9×** |
| **Mutation w/ optimistic + rollback** | **~68,000 ops/sec** | **~19,000 ops/sec** | **~3.7×** |
| **1000 parallel queries / cycle** | **~110 ops/sec** | **~45 ops/sec** | **~2.5×** |

Where lite-query wins decisively: **invalidation** (~3.9× — the signal-graph propagates without TanStack's observer-notification fan-out and per-query Promise allocation), **mutations with optimistic updates** (~3.7× — the `setQueryData` → `onMutate` → `fn` → rollback path is a direct signal write chain, not a queued observer cycle), and **high-concurrency scaling** (~2.5× on 1000 concurrent queries, with ~70% less transient memory).

Memory: lite-query allocates 4×–10× **less transient memory per operation** on the high-allocation scenarios (invalidate, mutation, 1000-parallel) thanks to lite-signal's zero-GC primitives and proper entry-signal disposal on cache removal. The 1000-parallel scenario goes from ~750 KB/cycle (query-core) to ~280 KB/cycle (lite-query). Warm reads are within noise of each other — both libraries have a tight warm path.

Reproduce: `npm install && npm run bench` (Node 18+, includes warmup, transient + retained byte tracking). Numbers vary ~10–15% run-to-run; the ratios are stable.

## Install

```sh
npm install @zakkster/lite-query @zakkster/lite-signal @zakkster/lite-store @zakkster/lite-channel
```

Three peer dependencies. They're all in the same family and play together by design. See [Ecosystem](#ecosystem) below for what each one does and why.

Two subpath entry points are **optional** and pull one extra peer each, only if you use them:

```sh
npm install @zakkster/lite-stream   # for @zakkster/lite-query/stream
npm install @zakkster/lite-await    # for @zakkster/lite-query/await
```

The core (`@zakkster/lite-query`) does not import either, so core-only installs see no extra requirement and no peer warnings. lite-query 1.1.0 requires `@zakkster/lite-signal >= 1.5.0` (the query/stream watcher uses `createRoot`).

## Quick taste

```js
import { signal, effect } from '@zakkster/lite-signal';
import { queryClient, query, mutation } from '@zakkster/lite-query';

const qc = queryClient({
  defaultStaleTime: 5_000,
  defaultCacheTime: 5 * 60_000,
  crossTab: true,                      // ← cross-tab sync
});

// A reactive query
const userId = signal(1);
const user = query(qc, {
  key: () => ['user', userId()],
  fetcher: async ({ key, signal }) =>
    fetch(`/api/users/${key[1]}`, { signal }).then(r => r.json()),
});

// Drive UI from signals
effect(() => {
  if (user.loading()) renderSpinner();
  else if (user.error()) renderError(user.error());
  else renderUser(user.data());
});

// Mutate with optimistic rollback
const updateUser = mutation(qc, {
  fn: (patch) => fetch(`/api/users/${patch.id}`, {
    method: 'PUT', body: JSON.stringify(patch),
  }).then(r => r.json()),

  onMutate: (patch) => {
    const prev = qc.getQueryData(['user', patch.id]);
    qc.setQueryData(['user', patch.id], { ...prev, ...patch });
    return { prev };                   // context → onError
  },
  onError: (err, patch, ctx) => qc.setQueryData(['user', patch.id], ctx.prev),
  onSuccess: () => qc.invalidate(['user']),
});

await updateUser.mutate({ id: 1, name: 'Zahary' });
```

For more, see [QuickStart.md](./QuickStart.md) and [Cookbook.md](./Cookbook.md).

## Core concepts

**`queryClient(options)` — the cache.** Owns a `Map` of cache entries keyed by hashed query keys. Owns the `BroadcastChannel` if `crossTab: true`. Exposes cache operations: `getQueryData`, `setQueryData`, `invalidate`, `removeQueries`, `clear`, `dispose`.

**`query(qc, { key, fetcher, ... })` — a reactive query.** The `key` can be a value (`['todos']`) or a function (`() => ['todo', id()]`). A function key subscribes to the signals it reads — changing them triggers a refetch with the new key. The query has no observers until something reads one of its accessors (`data()` / `error()` / `loading()` / `status()` / `fetching()`) inside an effect. No observers → no fetch. This is the lazy property.

**`mutation(qc, { fn, onMutate, ... })` — an async action with optimistic support.** Calling `mutation.mutate(vars)` returns a Promise. The optional callbacks run in a strict phase order: `onMutate(vars)` returns a context object → `fn(vars)` runs → `onSuccess(data, vars, ctx)` or `onError(err, vars, ctx)` → `onSettled(data, err, vars, ctx)`. `onSettled` *always* fires, even if earlier callbacks threw. Concurrent mutations are gen-guarded: a slow first cannot overwrite a fast second's state.

**Cross-tab coherence.** When `crossTab: true`, the client wires a `BroadcastChannel`. Explicit cache mutations (`setQueryData`, `invalidate`, `removeQueries`, `clear`) propagate to other tabs. Background fetch results do *not* propagate (otherwise tabs would broadcast-storm). Receiving tabs apply changes locally without re-broadcasting (echo suppression via a `processingRemote` flag).

## API surface

The complete public surface. Everything not listed here is internal.

### `queryClient(options?)`

Returns `{ options, getQueryData, setQueryData, invalidate, removeQueries, clear, dispose }`.

```ts
queryClient({
  defaultStaleTime?: number,       // ms; 0 (default) = stale on every attach
  defaultCacheTime?: number,       // ms; default 5 * 60_000 (5 min)
  defaultTimeout?: number,         // ms; default Infinity
  retry?: number | ((attempt, err) => boolean),  // default 3
  retryDelay?: (attempt) => number,  // default: min(2^(n-1) * 1000, 30s)
  crossTab?: boolean,              // default false
  crossTabChannel?: string,        // default 'lite-query'
  sharedFetch?: boolean,           // default false; dedup fetches across tabs (needs isLeader)
  isLeader?: () => boolean,        // leader oracle, e.g. lite-channel's sync.isLeader
  sharedFetchTimeout?: number,     // ms; default 3000; follower fallback self-fetch delay
  now?: () => number,              // injectable for tests
  setTimeout?: typeof setTimeout,
  clearTimeout?: typeof clearTimeout,
  broadcastChannel?: typeof BroadcastChannel,  // injectable for tests
})
```

### `query(qc, opts)`

Returns `{ data, error, loading, fetching, status, refetch, dispose }` — all functions.

```ts
query(qc, {
  key: any[] | (() => any[]),       // value or reactive function
  fetcher: async ({ key, signal }) => any,
  staleTime?: number,
  cacheTime?: number,
  timeout?: number,
  retry?: number | function,
  retryDelay?: function,
  enabled?: boolean | (() => boolean),  // gate on a reactive condition
  equals?: (a, b) => boolean,           // default Object.is
})
```

### `mutation(qc, opts)`

Returns `{ data, error, loading, status, mutate, reset }`.

```ts
mutation(qc, {
  fn: async (vars) => any,
  onMutate?:  async (vars) => any,                       // returns context
  onSuccess?: async (data, vars, ctx) => void,
  onError?:   async (err, vars, ctx) => void,
  onSettled?: async (data, err, vars, ctx) => void,      // ALWAYS fires
})
```

### `streamQuery(qc, opts)` — subpath `@zakkster/lite-query/stream`

The multi-shot sibling of `query()`: subscribe a cache key to an async iterable — SSE frames, websocket messages, a paginated cursor, a pubsub topic — instead of a one-shot fetch. Values are pumped through [`@zakkster/lite-stream`](https://www.npmjs.com/package/@zakkster/lite-stream) into the **same cache entry** a query would use, so `getQueryData`, `invalidate`, and `removeQueries` operate on a stream uniformly. Requires `@zakkster/lite-stream`.

```ts
import { streamQuery } from "@zakkster/lite-query/stream";

const ticks = streamQuery(qc, {
  key: ["prices", symbol],                    // static or reactive, like query()
  stream: ({ key, signal }) => sseIterable(`/prices/${key[1]}`, signal),
  mode: "latest",                             // "latest" (default) | "buffer"
});

effect(() => {
  if (ticks.loading()) return;                // status: pending (no value yet)
  render(ticks.data());                       // updates on every frame
});
```

Returns `{ data, error, status, loading, done, count, droppedCount, restart, dispose }`. Status runs `idle → pending → streaming → success | error`; `loading()` is `pending`, `done()` is `success`. In `"latest"` mode `data()` is the most recent value (one signal write per frame, zero allocation); in `"buffer"` mode (`maxBuffer` required) `data()` is a sliding window array and `droppedCount()` counts values that fell off the back. `count()`/`droppedCount()` are non-reactive snapshots — read them next to `data()` in the same effect to see them advance.

Same lifecycle guarantees as `query()`: lazy (no connection until observed), abort-on-detach (last observer leaving calls `iterator.return()`), reactive-key restart, and an `enabled` gate. `invalidate(key)` aborts and re-establishes the stream. Streaming data does **not** cross tabs in 1.1.0 (each tab owns its connection); only `invalidate` propagates, so every tab reconnects. See `demo/stream-query-demo.html` for a live latest/buffer/lifecycle walkthrough.

### Async coordination — subpath `@zakkster/lite-query/await`

Bridges a reactive query to a one-shot promise — for imperative flows, route loaders, or tests. Re-exports the [`@zakkster/lite-await`](https://www.npmjs.com/package/@zakkster/lite-await) primitives verbatim and adds two query-native helpers. Requires `@zakkster/lite-await`.

```ts
import { whenQuery, whenAllQueries } from "@zakkster/lite-query/await";

const user = await whenQuery(userQ, { timeout: 5000 });   // resolves with data(); rejects with error()
const [a, b] = await whenAllQueries([aQ, bQ]);            // fail-fast, data in input order

// any predicate over status — e.g. wait for a streamQuery's first frame:
await whenQuery(ticks, (status) => status === "streaming");
```

## Honest behaviour notes

A few things to know that aren't obvious from the API:

- **`loading()` vs `fetching()`** — `loading()` is true only on the initial fetch (no data yet). `fetching()` is true on any fetch, including background revalidation. Use `loading` for spinners, `fetching` for subtle indicators.

A few things to know that aren't obvious from the API:

- **`loading()` vs `fetching()`** — `loading()` is true only on the initial fetch (no data yet). `fetching()` is true on any fetch, including background revalidation. Use `loading` for spinners, `fetching` for subtle indicators.
- **`staleTime: 0`** (the default) means every fresh observer attach triggers a refetch. If you want "cache hits don't refetch", set `staleTime: Infinity` or some large value.
- **Cross-tab broadcasts only explicit mutations.** Background fetch results stay local. If Tab A and Tab B both mount a query for the same key, they each fetch independently and store independently. Only `setQueryData` / `invalidate` / `removeQueries` propagate.
- **Same-entry watcher re-runs don't refetch.** If your reactive `key` function reads multiple signals but produces the same key on re-run, the watcher detects that and doesn't tear down / re-create the attachment. Refetch only happens on a *real* key change.
- **`mutate(vars)` returns a promise reflecting *that call's* outcome.** Two rapid `mutate()` calls each get their own promise, but the mutation's *state signals* (`data`, `status`, `error`) reflect only the latest one. Gen-guarded.
- **`onSettled` always fires.** Even if `onMutate` / `fn` / `onSuccess` / `onError` threw. Callback errors are contained so a buggy callback can't lock the user's UI.
- **`AbortSignal.reason`** carries one of `'lite-query:detach' | 'lite-query:refetch' | 'lite-query:removed' | 'lite-query:timeout'` so your fetcher can make smart retry decisions.

## Cross-tab fetch deduplication

The feature no other query library ships. Five tabs open to the same dashboard normally means five identical API calls every poll cycle. With leader-election shared fetch, the leader tab fetches once and broadcasts the result; the others get it free.

```js
import { createTabSync } from '@zakkster/lite-channel';
import { queryClient } from '@zakkster/lite-query';

const sync = createTabSync();                  // lite-channel: leader election as a signal

const qc = queryClient({
  crossTab: true,
  sharedFetch: true,
  isLeader: () => sync.isLeader(),             // wire the leader oracle
  sharedFetchTimeout: 3000,                    // fallback self-fetch if no leader serves in time
});
```

How it works:

1. A follower tab needs data. Instead of fetching, it broadcasts a `fetch-req` and shows a loading state.
2. The leader receives the request and fetches (deduping if it's already fetching that key), then broadcasts the result. Every tab observing that key updates.
3. **Liveness guarantee:** each follower arms a fallback timer. If no leader responds within `sharedFetchTimeout` — election in progress, leader absent, or the leader doesn't have that query defined — the follower self-fetches. The UI never hangs.

Honest constraints:

- The leader can only fulfill a request for a query it currently has alive (observed, or within `cacheTime`). If the leader navigated away and its entry was GC'd, the follower falls back to self-fetch. **Correctness is always preserved; the dedup *benefit* is best-effort during those transitions.**
- `sharedFetch` requires both `crossTab: true` and a valid `isLeader` function. Without `isLeader`, it's inert and every tab fetches independently — a safe default with no breakage.
- This composes directly with `@zakkster/lite-channel`, but lite-query has no hard dependency on it. You supply `isLeader` from any source; lite-channel just happens to expose it as a ready-made signal.

## Why an ecosystem, not just a library

lite-query isn't a standalone package — it's one piece of a reactive platform where everything shares a single substrate (`@zakkster/lite-signal`) and one design discipline (zero-GC, fine-grained, framework-agnostic). That coherence is the point:

- Its **reactive keys** read signals from `lite-router`, so route changes drive refetches with no glue code.
- Its **cross-tab layer** is the same `BroadcastChannel` playbook as `lite-channel`, and its **fetch dedup** is powered by `lite-channel`'s leader election.
- Its **cache** can persist through `lite-persist` for instant cold-starts.
- Its **mutations** pair naturally with `lite-form` for validated submissions.
- Everything renders through `lite-element`, `lite-scene`, or `lite-virtual` without a framework in sight.

TanStack Query is excellent — and welded to React's render model. lite-query is for teams who've committed to signals end to end and want their data layer to compose with the rest of their stack rather than fight it. Adopt one `@zakkster/*` package and the others click in, because they were built to.

## Ecosystem

Lite-query is one library in a signals-native platform. Each package is single-purpose, zero-GC, framework-agnostic, and built on the same `lite-signal` core:

- **[`@zakkster/lite-signal`](https://github.com/PeshoVurtoleta/lite-signal)** — the reactive core. `signal` / `computed` / `effect` / `batch` / `untrack` / `onCleanup` / `isTracking`. Everything reactive here is one of these.
- **[`@zakkster/lite-store`](https://github.com/PeshoVurtoleta/lite-store)** — fine-grained reactivity for objects and arrays via Proxy, with lazy per-key signal allocation.
- **[`@zakkster/lite-channel`](https://github.com/PeshoVurtoleta/lite-channel)** — cross-tab sync over `BroadcastChannel`: Lamport-clock LWW, reactive presence, and **leader election** — the engine behind lite-query's fetch dedup.
- **[`@zakkster/lite-router`](https://github.com/PeshoVurtoleta/lite-router)** — sub-2KB SPA router exposing pathname, query params, and route matches as signals. Pair with reactive keys for route-driven queries.
- **[`@zakkster/lite-persist`](https://github.com/PeshoVurtoleta/lite-persist)** — debounced, coalesced `localStorage`/`sessionStorage` sync. Persist the cache for instant cold-starts.
- **[`@zakkster/lite-form`](https://github.com/PeshoVurtoleta/lite-form)** — headless reactive forms with hoisted schema validation. Pairs with mutations for validated submissions.
- **[`@zakkster/lite-resource`](https://github.com/PeshoVurtoleta/lite-resource)** — async state as a single signal. The minimal sibling to lite-query when you don't need a cache.
- **[`@zakkster/lite-element`](https://github.com/PeshoVurtoleta/lite-element)** · **[`lite-virtual`](https://github.com/PeshoVurtoleta/lite-virtual)** · **[`lite-scene`](https://github.com/PeshoVurtoleta/lite-scene)** — rendering: custom elements, list/grid windowing, Canvas2D scene graph.
- **[`@zakkster/lite-raf`](https://github.com/PeshoVurtoleta/lite-raf)** · **[`lite-time`](https://github.com/PeshoVurtoleta/lite-time)** — scheduling: frame-rate loop and drift-corrected wall-clock cadence, both as signals.

If you're new to the family, start with lite-signal — every other library here is layered on top.

## Tests

```sh
npm test
```

152 deterministic tests. Run output:

```
# tests 153
# pass 152
# fail 0
```

The core suite (120, incl. one pre-existing skip) uses a controlled fetcher, mock clock, and mock `BroadcastChannel` so every test is deterministic — no real timers, no real network. The optional entry points add 18 (`/await`) and 15 (`/stream`) tests, the latter driving a manually-pumped async iterator through every termination path. See `test/harness.js` for the mocks.

## Browser support

Modern browsers with `BroadcastChannel` and `AbortController` — anything from 2020 forward. Falls back gracefully when `BroadcastChannel` is unavailable (cross-tab is just a no-op).

## License

MIT © Zahary Shinikchiev. See [LICENSE](./LICENSE).
