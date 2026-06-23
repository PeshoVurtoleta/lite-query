# Cookbook

Real-world patterns that don't fit in the API reference but come up constantly. Each recipe is self-contained and tested against real code.

If you're new to lite-query, read [QuickStart.md](./QuickStart.md) first. This document assumes you know what a query and a mutation are.

---

## 1. Polling — refetch every N seconds while observed

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

## 2. Debounced reactive keys — search-as-you-type without spam

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

## 3. Dependent queries — fetch B after A resolves

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

## 4. Pagination — accumulate pages into a single list

lite-query doesn't ship a built-in `useInfiniteQuery` because the pattern is simple enough to compose. Hold a page signal, query per page, accumulate manually.

```js
const page = signal(1);
const accumulated = signal([]);

const todosPage = query(qc, {
  key: () => ['todos', page()],
  fetcher: async ({ key, signal }) =>
    fetch(`/api/todos?page=${key[1]}&size=20`, { signal }).then(r => r.json()),
});

// Append fresh pages to the accumulator
effect(() => {
  const data = todosPage.data();
  if (!data) return;
  if (page() === 1) accumulated.set(data);           // reset on first page
  else accumulated.set([...accumulated.peek(), ...data]);
});

function loadMore() {
  if (!todosPage.fetching()) page.set(page.peek() + 1);
}
```

This pattern keeps you in control of the accumulator (reset on filter change, dedupe by id, etc.) without the library imposing assumptions about how your pagination works.

---

## 5. Prefetch on hover — speculate before navigation

The user hovers a link. Start fetching the destination's data so it's cached when they click.

```js
function prefetchUserOnHover(userId) {
  // Spawn a temporary query; dispose right away so it doesn't keep observers.
  // The fetch still runs; the cache entry survives until cacheTime.
  const q = query(qc, {
    key: ['user', userId],
    fetcher: async ({ key, signal }) =>
      fetch(`/api/users/${key[1]}`, { signal }).then(r => r.json()),
  });

  // Touch the accessor to start the fetch, then dispose.
  // The fetch is in-flight; result lands in the cache.
  effect(() => q.data())();                          // immediate dispose

  // Alternative: don't bother with effect, call refetch directly
  // q.refetch().catch(() => {});
}

<a href="/users/42" onmouseover={() => prefetchUserOnHover(42)}>View user 42</a>
```

When the user actually clicks and the destination mounts a query with key `['user', 42]`, the cache hit serves the prefetched data instantly.

---

## 6. Optimistic updates with rollback — the canonical pattern

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

## 7. Cross-tab optimistic updates — make the magic visible

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

Open the app in two windows side-by-side. Click "Add Todo" in window A. Window B's list updates instantly — the `setQueryData` from `onMutate` propagated via `BroadcastChannel`. The subsequent `invalidate` on success also propagates, so both tabs refetch in sync.

Caveat: large payloads (>1MB) in `setQueryData` will be `structuredClone`'d on the main thread of every tab. For batch operations like importing a CSV, use `invalidate` instead of `setQueryData` — that triggers a refetch in other tabs without copying the payload.

---

## 8. Conditional fetching with `enabled` reactive — abort on view change

A modal opens; the modal fetches its data. The modal closes; we want to cancel the fetch immediately. Use a reactive `enabled`.

```js
const modalOpen = signal(false);

const modalData = query(qc, {
  key: ['modal', 'data'],
  fetcher: async ({ signal }) =>
    fetch('/api/heavy-data', { signal }).then(r => r.json()),
  enabled: () => modalOpen(),
});

// Open modal — fetch starts
modalOpen.set(true);

// Close modal before fetch resolves — fetch is aborted via AbortSignal,
// status reverts to 'idle', no work wasted.
modalOpen.set(false);
```

The user's fetcher receives `signal.reason === 'lite-query:detach'`, so they can log differently from a timeout abort.

---

## 9. Smart retry — bail out on 4xx, keep retrying 5xx

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
    // Don't retry 4xx — they won't succeed.
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
            // User left the page — no log needed
            break;
          case 'lite-query:refetch':
            // A newer refetch superseded us — also no log
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

Sometimes you need a one-shot read that *awaits* the first success — a route guard, a setup script, a test. The `@zakkster/lite-query/await` entry point ships `whenQuery` for exactly this (and re-exports the whole `@zakkster/lite-await` toolkit).

```js
import { whenQuery, whenAllQueries } from '@zakkster/lite-query/await';

const user = query(qc, { /* ... */ });

// Imperative: resolves with data() on success, rejects with error() on failure.
const userData = await whenQuery(user, { timeout: 5000 });

// Gate a screen on several queries at once — fail-fast, data in input order:
const [profile, prefs, flags] = await whenAllQueries([profileQ, prefsQ, flagsQ]);

// Any predicate over status — e.g. await a streamQuery's first frame:
await whenQuery(ticks, (status) => status === 'streaming');
```

`whenQuery` forwards `timeout` / `signal` to the underlying `whenSignal`, so a slow query rejects with `TimeoutError` and an aborted one rejects with the abort reason. Requires `@zakkster/lite-await`. lite-query keeps this in an optional subpath rather than the core — composability over feature-creep.

---

## 12. Cross-tab fetch deduplication — five tabs, one request

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

// Poll every 10s — in the leader only; followers get the broadcast.
effect(() => {
  metrics.data();
  const id = setInterval(() => metrics.refetch(), 10_000);
  onCleanup(() => clearInterval(id));
});
```

Open the app in five tabs. Watch your server logs: one request per cycle, not five. Close the leader tab — `lite-channel` elects a new leader, and within `sharedFetchTimeout` the followers either get served by the new leader or self-fetch. No request is ever lost.

The trade-off to know: the leader can only serve a query it currently has alive. If the leader is on a different route where that query isn't mounted, the follower's fallback timer fires and it self-fetches. Correctness holds; you just lose the dedup benefit for that key until a tab with the query becomes leader.

---

## 13. Testing your queries — the harness pattern

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

## 14. Streaming — a live SSE feed in latest mode

`streamQuery` (from `@zakkster/lite-query/stream`, peer `@zakkster/lite-stream`) subscribes a key to an async iterable. In `latest` mode `data()` is the most recent value — one signal write per frame, zero allocation.

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

## 15. Streaming — paginated cursor in buffer mode

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

Memory is bounded by the window, not the stream length — the 10,000th row costs the same as the 51st.

---

## 16. Streaming — websocket with invalidate-driven reconnect

A long-lived socket as a stream, plus cross-tab reconnect. `invalidate(key)` aborts the live connection and re-establishes a fresh one; with `crossTab: true` the invalidation reaches every tab, so they all reconnect (in 1.1.0 each tab owns its own socket — only the invalidate signal crosses tabs).

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

That's the cookbook for v1.1. Recipes for devtools integration, SSR hydration, and shared (one-connection-per-cluster) streams will land as those features ship.

If you have a pattern that should live here, open an issue or a PR.
