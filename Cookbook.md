# Cookbook

Real-world patterns that don't fit in the API reference but come up constantly. Each recipe is self-contained and tested against real code.

If you're new to lite-query, read [QuickStart.md](./QuickStart.md) first. This document assumes you know what a query and a mutation are.

---

## 1. Polling -- refetch every N seconds while observed

For a dashboard, a build status indicator, a notification badge. Want: refetch on a schedule, but only while someone is watching.

```js
import { effect, onCleanup } from '@zakkster/lite-signal';
import { query } from '@zakkster/lite-query';

const buildStatus = query(qc, {
  key: ['build', 'status'],
  fetcher: async ({ signal }) =>
    fetch('/api/build/status', { signal }).then(r => r.json()),
  staleTime: 0,                                      // always stale on attach
});

// Polling effect: only runs while subscribed
effect(() => {
  buildStatus.data();                                // subscribe
  const id = setInterval(() => buildStatus.refetch(), 5_000);
  onCleanup(() => clearInterval(id));
});
```

When the effect is disposed (component unmounts), `clearInterval` fires and the polling stops. The query's `onCleanup` integration with lite-signal does the right thing automatically.

---

## 2. Debounced reactive keys -- search-as-you-type without spam

A search input that changes the query key on every keystroke would fire a fetch per keystroke. Wrap the key signal in a debounce.

```js
import { signal, computed } from '@zakkster/lite-signal';
import { watchEffect } from '@zakkster/lite-watch-ex';   // if available
import { query } from '@zakkster/lite-query';

const queryText = signal('');
const debouncedText = signal('');

// 300ms debounce
let timer = null;
effect(() => {
  const text = queryText();
  clearTimeout(timer);
  timer = setTimeout(() => debouncedText.set(text), 300);
});

const search = query(qc, {
  key: () => ['search', debouncedText()],
  fetcher: async ({ key, signal }) =>
    fetch(`/api/search?q=${encodeURIComponent(key[1])}`, { signal }).then(r => r.json()),
  enabled: () => debouncedText().length > 1,         // skip empty / single-char
});
```

The `enabled` gate prevents fetches for trivial queries. The debounce ensures only the final keystroke after a pause triggers a network call.

---

## 3. Dependent queries -- fetch B after A resolves

User has to be loaded before we can fetch their posts. Use `enabled` to gate.

```js
const userId = signal(1);

const user = query(qc, {
  key: () => ['user', userId()],
  fetcher: async ({ key, signal }) =>
    fetch(`/api/users/${key[1]}`, { signal }).then(r => r.json()),
});

const userPosts = query(qc, {
  key: () => ['posts', 'by-user', user.data()?.id],
  fetcher: async ({ key, signal }) =>
    fetch(`/api/users/${key[2]}/posts`, { signal }).then(r => r.json()),
  enabled: () => !!user.data()?.id,                  // gate on user being loaded
});
```

`userPosts` stays idle until `user.data()` resolves. The moment `user.data()?.id` becomes truthy, `enabled` flips, and `userPosts` fetches with the resolved id.

---

## 4. Pagination -- cursor-accumulate pages with `infiniteQuery`

Cursor pagination is first-class. `infiniteQuery` holds the whole list in one cache entry: `pages()` is the array of raw page results, `data()` is the flattened accumulation, `fetchNextPage()` appends the next page, and `hasNextPage()` reads false once `getNextCursor` returns null.

```js
import { infiniteQuery } from '@zakkster/lite-query';

const feed = infiniteQuery(qc, {
  key: ['todos'],
  fetcher: async ({ cursor, signal }) =>
    fetch(`/api/todos?after=${cursor ?? ''}&size=20`, { signal }).then(r => r.json()),
  getNextCursor: (lastPage) => lastPage.nextCursor ?? null,   // null => exhausted
});

effect(() => render(feed.data()));                   // attach + fetch page one

function loadMore() {
  if (feed.hasNextPage()) feed.fetchNextPage();      // dedups while in flight
}
```

`invalidate(['todos'])` (or `feed.refetch()`) refetches from page one and replaces the whole list on success -- stale-while-revalidate over the accumulation, with a generation guard that swallows a late page from a superseded fetch, so two generations never mix.

Aliasing contract: `data()` returns a LIVE array that grows in place as pages arrive. If you need a point-in-time snapshot of the list (to diff it, or hold it across a `fetchNextPage`), copy it: `const snapshot = [...feed.data()]`.

If you need custom accumulation the library can't express (dedupe by id, interleave two sources, reset on a filter change), hand-roll it with a page signal instead:

```js
const page = signal(1);
const accumulated = signal([]);
const todosPage = query(qc, {
  key: () => ['todos', page()],
  fetcher: async ({ key, signal }) =>
    fetch(`/api/todos?page=${key[1]}&size=20`, { signal }).then(r => r.json()),
});
effect(() => {
  const data = todosPage.data();
  if (!data) return;
  if (page() === 1) accumulated.set(data);           // reset on first page
  else accumulated.set([...accumulated.peek(), ...data]);
});
```

---

## 5. Prefetch on hover -- speculate before navigation

The user hovers a link. Start fetching the destination's data so it's cached when they click. `qc.prefetch(key, fetcher, opts?)` is the built-in for exactly this -- it warms an entry with zero observers, no throwaway `query()` handle to dispose.

```js
function prefetchUserOnHover(userId) {
  qc.prefetch(
    ['user', userId],
    async ({ key, signal }) =>
      fetch(`/api/users/${key[1]}`, { signal }).then(r => r.json()),
  );
  // Fresh entry already cached? Prefetch is a no-op (no refetch, no GC re-arm).
}

<a href="/users/42" onmouseover={() => prefetchUserOnHover(42)}>View user 42</a>
```

When the user clicks and the destination mounts `query(qc, { key: ['user', 42] })`, the cache hit serves the prefetched data instantly (no refetch if still fresh). Pairs with a route loader: `await qc.prefetch(routeKey, routeFetcher)` before the transition, and the mounted query adopts the warmed entry.

The manual form (spawn a temporary `query()`, touch it, dispose) still works if you need the observer lifecycle, but `qc.prefetch` is the direct path:

```js
const q = query(qc, { key: ['user', 42], fetcher });
effect(() => q.data())();                            // immediate dispose -- fetch lands in cache
```

---

## 6. Optimistic updates with rollback -- the canonical pattern

Used in the QuickStart but worth a deeper recipe. Three pieces: `onMutate` snapshots + writes optimistic data, `onError` restores from snapshot, `onSuccess` invalidates for the server's truth.

```js
const toggleTodo = mutation(qc, {
  fn: (id) =>
    fetch(`/api/todos/${id}/toggle`, { method: 'POST' }).then(r => r.json()),

  onMutate: (id) => {
    const prev = qc.getQueryData(['todos']);
    qc.setQueryData(['todos'], (todos) =>
      todos.map(t => t.id === id ? { ...t, done: !t.done } : t)
    );
    return { prev };
  },

  onError: (err, id, ctx) => {
    qc.setQueryData(['todos'], ctx.prev);            // roll back
    showToast('Could not toggle todo');
  },

  onSuccess: () => qc.invalidate(['todos']),         // refetch source of truth
});
```

The UI flips immediately when you call `toggleTodo.mutate(id)`. If the server says no, it flips back and the user sees a toast. If yes, a background refetch syncs to the server's representation.

---

## 7. Cross-tab optimistic updates -- make the magic visible

This is the unique feature. With `crossTab: true`, the optimistic update in tab A is broadcast to tab B and lands there instantly too. Demo for clients:

```js
const qc = queryClient({ crossTab: true });

const todos = query(qc, { /* ... */ });

const addTodo = mutation(qc, {
  fn: (text) => fetch('/api/todos', { method: 'POST', body: text }).then(r => r.json()),
  onMutate: (text) => {
    const prev = qc.getQueryData(['todos']);
    qc.setQueryData(['todos'], (old) => [...old, { id: 'temp', text, _optimistic: true }]);
    return { prev };
  },
  onError: (err, vars, ctx) => qc.setQueryData(['todos'], ctx.prev),
  onSuccess: () => qc.invalidate(['todos']),
});
```

Open the app in two windows side-by-side. Click "Add Todo" in window A. Window B's list updates instantly -- the `setQueryData` from `onMutate` propagated via `BroadcastChannel`. The subsequent `invalidate` on success also propagates, so both tabs refetch in sync.

Caveat: large payloads (>1MB) in `setQueryData` will be `structuredClone`'d on the main thread of every tab. For batch operations like importing a CSV, use `invalidate` instead of `setQueryData` -- that triggers a refetch in other tabs without copying the payload.

---

## 8. Conditional fetching with `enabled` reactive -- abort on view change

A modal opens; the modal fetches its data. The modal closes; we want to cancel the fetch immediately. Use a reactive `enabled`.

```js
const modalOpen = signal(false);

const modalData = query(qc, {
  key: ['modal', 'data'],
  fetcher: async ({ signal }) =>
    fetch('/api/heavy-data', { signal }).then(r => r.json()),
  enabled: () => modalOpen(),
});

// Open modal -- fetch starts
modalOpen.set(true);

// Close modal before fetch resolves -- fetch is aborted via AbortSignal,
// status reverts to 'idle', no work wasted.
modalOpen.set(false);
```

The user's fetcher receives `signal.reason === 'lite-query:detach'`, so they can log differently from a timeout abort.

---

## 9. Smart retry -- bail out on 4xx, keep retrying 5xx

The function-form `retry` lets you decide per-error.

```js
const data = query(qc, {
  key: ['protected', 'resource'],
  fetcher: async ({ signal }) => {
    const res = await fetch('/api/protected', { signal });
    if (!res.ok) throw Object.assign(new Error(res.statusText), { status: res.status });
    return res.json();
  },
  retry: (attempt, err) => {
    // Don't retry 4xx -- they won't succeed.
    if (err.status >= 400 && err.status < 500) return false;
    // Retry up to 3 times for everything else (5xx, network).
    return attempt < 3;
  },
});
```

Adopted directly from TanStack Query's convention because it's the right shape.

---

## 10. Distinguish abort reasons in your fetcher

Useful for logging or alternative recovery paths.

```js
const data = query(qc, {
  key: ['something'],
  fetcher: async ({ signal }) => {
    try {
      const res = await fetch('/api/something', { signal });
      return res.json();
    } catch (err) {
      if (signal.aborted) {
        switch (signal.reason) {
          case 'lite-query:timeout':
            console.warn('Fetch timed out; consider increasing the timeout option');
            break;
          case 'lite-query:detach':
            // User left the page -- no log needed
            break;
          case 'lite-query:refetch':
            // A newer refetch superseded us -- also no log
            break;
          case 'lite-query:removed':
            console.warn('Query was removed mid-flight');
            break;
        }
      }
      throw err;
    }
  },
  timeout: 10_000,
});
```

---

## 11. Wait for a query with `whenQuery`

Sometimes you need a one-shot read that *awaits* the first success -- a route guard, a setup script, a test. The `@zakkster/lite-query/await` entry point ships `whenQuery` for exactly this (and re-exports the whole `@zakkster/lite-await` toolkit).

```js
import { whenQuery, whenAllQueries } from '@zakkster/lite-query/await';

const user = query(qc, { /* ... */ });

// Imperative: resolves with data() on success, rejects with error() on failure.
const userData = await whenQuery(user, { timeout: 5000 });

// Gate a screen on several queries at once -- fail-fast, data in input order:
const [profile, prefs, flags] = await whenAllQueries([profileQ, prefsQ, flagsQ]);

// Any predicate over status -- e.g. await a streamQuery's first frame:
await whenQuery(ticks, (status) => status === 'streaming');
```

`whenQuery` forwards `timeout` / `signal` to the underlying `whenSignal`, so a slow query rejects with `TimeoutError` and an aborted one rejects with the abort reason. Requires `@zakkster/lite-await`. lite-query keeps this in an optional subpath rather than the core -- composability over feature-creep.

---

## 12. Cross-tab fetch deduplication -- five tabs, one request

The headline feature. With a leader oracle wired from `@zakkster/lite-channel`, only the leader tab fetches; followers receive the result over `BroadcastChannel`. For a dashboard users keep open in many tabs, this collapses N polling requests into one.

```js
import { createTabSync } from '@zakkster/lite-channel';
import { queryClient, query } from '@zakkster/lite-query';

const sync = createTabSync();

const qc = queryClient({
  crossTab: true,
  sharedFetch: true,
  isLeader: () => sync.isLeader(),
  sharedFetchTimeout: 3000,
});

// Every tab defines the same query. Only the leader actually hits the network.
const metrics = query(qc, {
  key: ['metrics', 'live'],
  fetcher: async ({ signal }) =>
    fetch('/api/metrics/live', { signal }).then(r => r.json()),
  staleTime: 0,
});

// Poll every 10s -- in the leader only; followers get the broadcast.
effect(() => {
  metrics.data();
  const id = setInterval(() => metrics.refetch(), 10_000);
  onCleanup(() => clearInterval(id));
});
```

Open the app in five tabs. Watch your server logs: one request per cycle, not five. Close the leader tab -- `lite-channel` elects a new leader, and within `sharedFetchTimeout` the followers either get served by the new leader or self-fetch. No request is ever lost.

The trade-off to know: the leader can only serve a query it currently has alive. If the leader is on a different route where that query isn't mounted, the follower's fallback timer fires and it self-fetches. Correctness holds; you just lose the dedup benefit for that key until a tab with the query becomes leader.

---

## 13. Testing your queries -- the harness pattern

For unit tests, inject a mock clock + controlled fetcher into the client. The harness from lite-query's own test suite is reusable.

```js
import { createControlledFetcher, createMockClock } from '@zakkster/lite-query/test/harness';
import { queryClient, query } from '@zakkster/lite-query';

test('user query loads and shows data', async () => {
  const clock = createMockClock();
  const fetcher = createControlledFetcher();

  const qc = queryClient({
    now: clock.now,
    setTimeout: clock.setTimeout,
    clearTimeout: clock.clearTimeout,
  });

  const user = query(qc, {
    key: ['user', 1],
    fetcher: fetcher.fetcher,
  });

  effect(() => user.data());

  await tick();
  assert.equal(user.loading(), true);

  fetcher.resolve({ id: 1, name: 'Zahary' });
  await tick();

  assert.equal(user.loading(), false);
  assert.deepEqual(user.data(), { id: 1, name: 'Zahary' });
});
```

No real network, no real timers, deterministic outcomes. The full harness with `createMockBroadcastChannel` and `setupMockEnv` is at `test/harness.js`.

---

## 14. Streaming -- a live SSE feed in latest mode

`streamQuery` (from `@zakkster/lite-query/stream`, peer `@zakkster/lite-stream`) subscribes a key to an async iterable. In `latest` mode `data()` is the most recent value -- one signal write per frame, zero allocation.

```js
import { streamQuery } from '@zakkster/lite-query/stream';

async function* sse(url, signal) {
  const res = await fetch(url, { signal });
  const reader = res.body.getReader();
  const dec = new TextDecoder();
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) return;                              // iterator completes -> status "success"
      yield JSON.parse(dec.decode(value));
    }
  } finally {
    reader.cancel();                                 // runs on abort-on-detach
  }
}

const prices = streamQuery(qc, {
  key: ['prices', symbol],
  stream: ({ key, signal }) => sse(`/sse/prices/${key[1]}`, signal),
});

effect(() => {
  if (prices.loading()) return;                      // pending: subscribed, no frame yet
  renderTicker(prices.data(), prices.count());       // count() read alongside data() stays fresh
});
```

When the effect disposes, the last observer is gone: the stream is aborted (`reader.cancel()` via the generator's `finally`) and the entry is GC-scheduled. A reactive `key` (different `symbol`) aborts the old feed and opens a new one.

---

## 15. Streaming -- paginated cursor in buffer mode

`buffer` mode keeps a sliding window of the last `maxBuffer` values; `data()` is the window array and `droppedCount()` counts what fell off. Good for "last N events" or accumulating a bounded feed.

```js
async function* pages({ key, signal }) {
  let cursor = null;
  do {
    const page = await fetchPage(key[1], cursor, signal);
    for (const row of page.rows) yield row;          // yield each row; the window holds the latest maxBuffer
    cursor = page.nextCursor;
  } while (cursor && !signal.aborted);
}

const feed = streamQuery(qc, {
  key: ['activity', userId],
  stream: pages,
  mode: 'buffer',
  maxBuffer: 50,
});

effect(() => {
  const rows = feed.data() ?? [];                    // T[] in buffer mode
  renderList(rows);
  if (feed.droppedCount() > 0) showTrimNotice(feed.droppedCount());
});
```

Memory is bounded by the window, not the stream length -- the 10,000th row costs the same as the 51st.

---

## 16. Streaming -- websocket with invalidate-driven reconnect

A long-lived socket as a stream, plus cross-tab reconnect. `invalidate(key)` aborts the live connection and re-establishes a fresh one; with `crossTab: true` the invalidation reaches every tab, so they all reconnect (in 1.1.0 each tab owns its own socket -- only the invalidate signal crosses tabs).

```js
async function* socket({ key, signal }) {
  const ws = new WebSocket(`wss://api.example.com/room/${key[1]}`);
  const queue = [];
  let wake;
  ws.onmessage = (e) => { queue.push(JSON.parse(e.data)); wake && wake(); };
  signal.addEventListener('abort', () => ws.close(), { once: true });
  try {
    while (!signal.aborted) {
      if (queue.length === 0) await new Promise((r) => (wake = r));
      while (queue.length) yield queue.shift();
    }
  } finally {
    ws.close();
  }
}

const room = streamQuery(qc, { key: ['room', roomId], stream: socket });

// Force a reconnect everywhere (e.g. after an auth refresh):
qc.invalidate(['room', roomId]);
```

---

## 17. Cross-tab shared streams -- five tabs, one socket

Shared fetch (recipe 12) collapses N tabs to one *request*; `sharedStream: true` collapses N tabs to one *connection*. The leader tab owns the single iterator and broadcasts each frame; every other tab projects the frames and holds no iterator. The `streamQuery` call is identical in every tab -- the sharing is transparent, and failover is handled for you.

```js
import { queryClient } from '@zakkster/lite-query';
import { streamQuery } from '@zakkster/lite-query/stream';
import { createChannel } from '@zakkster/lite-channel';

const channel = createChannel('prices');

const qc = queryClient({
  crossTab: true,
  broadcastChannel: BroadcastChannel,
  sharedStream: true,
  isLeader: () => channel.sync.isLeader(),   // the same oracle sharedFetch uses
  streamIdleTimeout: 3000,                    // self-connect if no frame arrives in time
});

// Same call in every tab. One tab opens the SSE connection; the rest project.
const prices = streamQuery(qc, {
  key: () => ['prices', symbol()],
  stream: ({ key, signal }) => sseSource(`/stream/${key[1]}`, signal),
  mode: 'buffer',
  maxBuffer: 200,
});

effect(() => renderChart(prices.data()));      // followers see the same window, live
effect(() => showDropped(prices.droppedCount()));
```

What you get without wiring it:

- **One connection.** Open the app in five tabs -- your server sees one SSE stream, not five. `data()`, `count()`, and `droppedCount()` read the projected window exactly as a local stream reads its own.
- **At-most-once frames (OR-4).** No duplicated or reordered frames, ever. Frame *loss* on failover is permitted and counted (`droppedCount()` is your own window's drops; `stream:gap` in the feed reports frames missed in transit).
- **Failover you don't write.** Close the leader tab, kill it, or let it hang -- a follower promotes and opens a **fresh** connection within `streamIdleTimeout`. Nothing adopts a dead leader's iterator, and the `(epochSeq, clientId)` tiebreak guarantees exactly one owner survives.
- **Watch the handover in the feed:** install `qc.inspect(hook)` and observe `stream:promote` (with `from`/`to` epochs and a reason like `leader-killed`), `stream:project` per applied frame, and `stream:gap` when a boundary loses frames.

`sharedStream` needs `crossTab: true`, a channel, and `isLeader`; without them it's inert and every tab owns its own connection -- a safe default. Frames ride the existing crossTab `BroadcastChannel`; there is no transport ownership and no hard `lite-channel` dependency (it only supplies `isLeader`).

---

## 18. Persist the cache for instant cold-starts -- localStorage and IndexedDB thunks

`persistQueryClient(qc, { save, load, version, throttle? })` is storage-agnostic: you supply `save` and `load` thunks. It restores once on install (before any observer attaches), then throttles a dehydrated snapshot to `save`. `version` is REQUIRED -- bump it whenever your cached shapes change and the old cache is dropped instead of resurrected. `handle.restored` is a promise that always resolves (never rejects) with `{ status, count, reason }`.

Boot order matters: create the client, install the persister, `await handle.restored`, THEN attach observers. Hydrate is boot-only and fail-closed -- it throws if the cache already holds an entry.

```js
import { queryClient, persistQueryClient } from '@zakkster/lite-query';

// --- localStorage (synchronous thunks) ---
const qc = queryClient({ defaultStaleTime: 30_000 });
const persister = persistQueryClient(qc, {
  version: 'v1',                                  // bump to drop the old cache on a schema change
  throttle: 1000,                                 // coalesce bursts of writes into one save/second
  save: (envelope) => localStorage.setItem('lq-cache', JSON.stringify(envelope)),
  load: () => {
    const raw = localStorage.getItem('lq-cache');
    if (raw == null) return null;                 // null/undefined => empty store, a normal boot
    try {
      return JSON.parse(raw);
    } catch (err) {
      // A native SyntaxError (its .code is undefined) means the stored record is
      // not JSON: discard the cache and re-fetch, never retry the same bytes.
      if (err instanceof SyntaxError) { localStorage.removeItem('lq-cache'); return null; }
      throw err;                                  // anything else resolves as { status: 'dropped', reason: 'load-threw' }
    }
  },
});

const outcome = await persister.restored;         // { status: 'restored' | 'empty' | 'dropped', count, reason }
// ...now attach observers: query()/infiniteQuery() read from cache instantly and revalidate per staleTime.

// On logout, persist emptiness then stop (clear is a write hook site):
// qc.clear(); persister.stop();

// --- IndexedDB (async thunks -- same adapter, promises instead of sync) ---
function idbThunks(dbName = 'lq', store = 'cache', key = 'state') {
  const open = () => new Promise((res, rej) => {
    const r = indexedDB.open(dbName, 1);
    r.onupgradeneeded = () => r.result.createObjectStore(store);
    r.onsuccess = () => res(r.result);
    r.onerror = () => rej(r.error);
  });
  const tx = async (mode, fn) => {
    const db = await open();
    return new Promise((res, rej) => {
      const t = db.transaction(store, mode);
      const req = fn(t.objectStore(store));
      t.oncomplete = () => res(req && req.result);
      t.onerror = () => rej(t.error);
    });
  };
  return {
    save: (envelope) => tx('readwrite', (s) => s.put(envelope, key)),
    load: () => tx('readonly', (s) => s.get(key)),   // undefined when absent => empty store
  };
}

const qc2 = queryClient({ defaultStaleTime: 30_000 });
const persister2 = persistQueryClient(qc2, { version: 'v1', throttle: 1000, ...idbThunks() });
await persister2.restored;
```

An async `load()` that resolves AFTER a cross-tab `setData` already populated this client hits the empty-cache precondition; the adapter catches it and resolves `{ status: 'dropped', reason: 'cache-not-empty' }` -- never a merge, never an unhandled rejection.

## 19. Back the cache with a baked container -- `@zakkster/lite-bake-stream`

For a large cache, bake each dehydrated entry as one JSON record in a preserve-mode container and read it back lazily. The bake wiring lives entirely in your thunks -- lite-query ships no bake subpath, peer, or import. Pick your floor from bake-stream's own `llms.txt` "Consumer floors:" line, quoted verbatim (never restate version numbers from another package's roadmap):

> Consumer floors: validated preserve reads need `>= 1.3.1` (the six S1 read doors all closed there; the preserve doors themselves shipped in `1.3.0`, but the full validated read set is `1.3.1`); CRC-verified persistence needs `>= 1.6.0` (`{ crc: true }` on write, `{ verifyCrc: true }` on open); abortable range reads need `>= 1.7.0`.

Below the crossover, use plain JSON (whole-cache `JSON.parse` wins full hydration); the bake container wins above it, and for lazy first-entry boot the O(1) preserve open always wins. And carry bake's read rule: a `getJSON(i)` that throws a native `SyntaxError` (its `.code` is undefined) means the stored record is not JSON -- discard the cache and re-fetch, never retry the same bytes.

```js
import { serialize, PreserveReader } from '@zakkster/lite-bake-stream';
import { persistQueryClient } from '@zakkster/lite-query';

const persister = persistQueryClient(qc, {
  version: 'v1',
  save: (envelope) => {
    // One NDJSON record per dehydrated entry -> one preserve record per query.
    const ndjson = envelope.state.entries.map((e) => JSON.stringify(e)).join('\n') + '\n';
    const container = serialize(new TextEncoder().encode(ndjson), { preserve: true, writer: { crc: true } });
    writeToDisk('lq.lbk', container, envelope.version);   // your storage
  },
  load: () => {
    const bytes = readFromDisk('lq.lbk');
    if (bytes == null) return null;
    // A Node Buffer is a VIEW into a shared pool: bytes.buffer is the pool, not
    // the container, and fails closed at open -- slice the unpooled copy out.
    const ab = bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
    const reader = new PreserveReader(ab, { verifyCrc: true });   // fails closed on CRC mismatch/absence
    const entries = [];
    for (let i = 0; i < reader.totalRows; i++) {
      try {
        entries.push(reader.getJSON(i));
      } catch (err) {
        if (err instanceof SyntaxError) return null;   // not JSON -> discard the whole cache, re-fetch
        throw err;
      }
    }
    return { version: readVersion('lq.lbk'), state: { entries } };
  },
});
```

Corrupted storage is refused at open or first read with a named `R_` code, never hydrated wrong -- bake-stream's consumer-shaped corruption matrix is its `test/DehydratedCache.test.js`.

## 20. A reactive window over a remote multi-GB container -- `streamQuery` + `RangeReader`

Bake-stream's `RangeReader` reads a remote LBK1 container over HTTP Range with zone-map pruning -- no full download -- and (from `>= 1.7.0`) is abortable: reader-level `{ signal }`, an adapter contract `fetch(byteOffset, byteLength, signal?)`, an `R_ABORTED` refusal, and no partial cache on abort. That abortability is exactly what lets it compose with `streamQuery`, whose abort-on-detach law needs a cancellable source: when the last observer leaves, `streamQuery` aborts the iterator, which aborts the in-flight range read.

```js
import { streamQuery } from '@zakkster/lite-query/stream';
import { RangeReader, HTTPRangeAdapter } from '@zakkster/lite-bake-stream/range-reader';

// The stream factory yields rows in prefetched shard windows; `signal` is the
// streamQuery abort signal (fires on detach / removeQueries / key change).
const feed = streamQuery(qc, {
  key: ['remote-feed', filter()],
  mode: 'buffer',
  maxBuffer: 200,
  stream: async function* ({ signal }) {
    const adapter = await HTTPRangeAdapter.open(FEED_URL, { signal });   // probes size; every Range fetch rides it
    const reader = await RangeReader.open(adapter, { signal });          // abort-on-detach -> R_ABORTED, no partial cache
    for (let first = 0; first < reader.totalRows; first += 200) {
      const last = Math.min(first + 200, reader.totalRows);
      await reader.prefetchRange(first, last);       // one ranged fetch per uncached shard in the window
      const win = reader.syncRange(first, last);     // await-free field reads inside the cached window
      for (let i = first; i < last; i++) yield { ts: win.get(i, 'ts'), msg: win.get(i, 'msg') };
    }
  },
});
effect(() => render(feed.data()));                   // detach -> abort -> no partial cache
```

Cite only what bake-stream's `llms.txt` states about `RangeReader` and its adapter; version floors come from its consumer-floor line above, never from session numbers.

---

## 21. A console logger for the devtools feed -- ten lines

`qc.inspect(hook)` installs one observe-only hook and returns an idempotent
uninstall. The record is **pooled per type and overwritten in place**, so copy
what you keep. Zero cost when uninstalled; a throwing hook is contained (the feed
auto-uninstalls, the cache write completes) -- so a logger bug never wedges the
cache.

```ts
const stop = qc.inspect((e) => {
  const id = e.keyHash ?? '-';                         // null for tab:*/mutation:*/persist:*
  if (e.type === 'entry:status') console.log(`[lq] ${id} ${e.from} -> ${e.to}`);
  else if (e.type === 'fetch:settle') console.log(`[lq] ${id} fetch ${e.ok ? 'ok' : 'error'} gen=${e.count}`);
  else if (e.type === 'fetch:abort') console.log(`[lq] ${id} abort ${e.reason}`);
  else if (e.type === 'stream:value') console.log(`[lq] ${id} frame #${e.count}`);
  else console.log(`[lq] ${e.type} ${id}`);            // every other type (incl. queue:*)
});
// ... later, on teardown:
stop();
```

The full 31-type vocabulary and the 10-key record shape are in `llms.txt`. The
rendering **panel** is lite-studio's job -- this feed is what it renders; nothing
is imported in either direction.

## 22. Airplane mode -- queue mutations offline, replay on reconnect

Opt a mutation into the durable queue with `queue: true` and your own `offline`
oracle. While offline `mutate()` enqueues (status `"queued"`, a `{ queued, id }`
receipt) instead of dispatching; a fresh boot restores the queue from persistence
and the caller replays it on reconnect. Replay is **at-least-once across a crash**
-- every record carries a stable `id`, so key your server writes on it.

```ts
// 1. Persist the queue alongside the cache (sibling thunks, both or neither).
const persist = persistQueryClient(qc, {
  version: 'v1',
  save:      (env) => localStorage.setItem('lq:cache', JSON.stringify(env)),
  load:      ()    => JSON.parse(localStorage.getItem('lq:cache') || 'null'),
  queueSave: (env) => localStorage.setItem('lq:queue', JSON.stringify(env)),
  queueLoad: ()    => JSON.parse(localStorage.getItem('lq:queue') || 'null'),
});
// Await BOTH restores before any replay: replay resolves each record against a
// LIVE cache entry, so if the cache has not re-seeded its entries yet every
// restored record drops as "entry-missing" (a drop is a removal -- the mutation
// is lost). The cache restore must complete first.
await Promise.all([persist.restored, persist.queueRestored]);   // { status, count, reason } each

// 2. A mutation that queues while offline.
const saveTodo = mutation(qc, {
  fn: (todo) => fetch('/api/todos', { method: 'POST', body: JSON.stringify(todo) }),
  queue: true,
  offline: () => !navigator.onLine,   // your oracle -- the library never polls
  name: 'saveTodo',
  queueKey: ['todos'],
});
await saveTodo.mutate({ id: 1, title: 'buy milk' });   // offline -> { queued: true, id }

// 3. On reconnect, YOU trigger replay (FIFO, single-flight, per-item results).
window.addEventListener('online', async () => {
  const result = await qc.replayQueue((record) =>
    record.name === 'saveTodo'
      ? ((vars) => fetch('/api/todos', { method: 'POST', body: JSON.stringify(vars) }))
      : null
  );
  // A rejected item stays queued (tries++) for the next replay; a resolved item
  // is removed; an undispatchable one is dropped with a surfaced reason.
  console.log(`replayed ${result.replayed}, failed ${result.failed}, dropped ${result.dropped}`);
});
```

A record whose cache entry no longer exists drops (`reason: "entry-missing"`),
never a silent retry; `qc.dropQueued(id)` is the explicit exit for a
permanently-rejected item; `qc.queueSize()` reports the durable count.

---

That's the cookbook. Recipes for SSR hydration and shared (one-connection-per-cluster) streams will land as those features ship. The bake-backed ingest-progress recipe (`ingestStream` `onProgress` as a `streamQuery` source) is parked in ROADMAP.md until a consumer asks for it.

If you have a pattern that should live here, open an issue or a PR.
