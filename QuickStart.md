# QuickStart

You have lite-query installed. You want code on screen in five minutes. This is that document.

```sh
npm install @zakkster/lite-query @zakkster/lite-signal
```

One required peer: `@zakkster/lite-signal` (the reactive primitive). The
streaming and await tastes below add one optional peer each, pulled only by
their subpath -- noted where they appear.

## Step 1 -- the client

The client owns the cache. Make one per app (or one per logical scope -- e.g., per logged-in user session).

```js
import { queryClient } from '@zakkster/lite-query';

export const qc = queryClient({
  defaultStaleTime: 30_000,       // ms -- refetch if observer attaches to data older than this
  defaultCacheTime: 5 * 60_000,   // ms -- entry GC'd this long after the last observer leaves
});
```

## Step 2 -- a query

A `query()` defines *what* to fetch and *when*. It doesn't fetch on creation -- only when something reads its accessors inside an effect.

```js
import { effect } from '@zakkster/lite-signal';
import { query } from '@zakkster/lite-query';

const todos = query(qc, {
  key: ['todos'],                                    // static key for now
  fetcher: async ({ signal }) =>
    fetch('/api/todos', { signal }).then(r => r.json()),
});

effect(() => {
  if (todos.loading()) console.log('Loading...');
  else if (todos.error()) console.log('Error:', todos.error().message);
  else console.log('Data:', todos.data());
});
```

The fetcher receives `{ key, signal }`. The `signal` is an `AbortSignal` that fires when the query is disposed, the key changes, a refetch supersedes it, or a per-query timeout expires. Pass it to `fetch()` so the request cancels properly.

## Step 3 -- a reactive key

The `key` can be a function. Reading signals inside that function subscribes the query -- when those signals change, the query refetches with the new key.

```js
import { signal } from '@zakkster/lite-signal';

const userId = signal(1);

const user = query(qc, {
  key: () => ['user', userId()],                     // function, not value
  fetcher: async ({ key, signal }) =>
    fetch(`/api/users/${key[1]}`, { signal }).then(r => r.json()),
});

effect(() => console.log(user.data()));

userId.set(2);                                       // -> triggers a new fetch with key ['user', 2]
```

The old fetch is aborted automatically. If you swap back to `userId.set(1)` and the cache entry hasn't been GC'd yet, you get the cached `user 1` value instantly.

## Step 4 -- a mutation

Mutations are async actions that change server state. Define an `fn` that calls the network, and optional callbacks for optimistic updates and side effects.

```js
import { mutation } from '@zakkster/lite-query';

const addTodo = mutation(qc, {
  fn: (newTodo) =>
    fetch('/api/todos', {
      method: 'POST',
      body: JSON.stringify(newTodo),
    }).then(r => r.json()),

  // Optimistic UI: update cache BEFORE the network responds.
  onMutate: (newTodo) => {
    const prev = qc.getQueryData(['todos']);
    qc.setQueryData(['todos'], (old) => [...(old || []), { ...newTodo, optimistic: true }]);
    return { prev };                                 // context for rollback
  },

  onError: (err, vars, ctx) => {
    qc.setQueryData(['todos'], ctx.prev);            // roll back on failure
  },

  onSuccess: () => qc.invalidate(['todos']),         // refetch fresh server state
});

const result = await addTodo.mutate({ text: 'Buy milk' });
```

The mutation has its own reactive accessors:

```js
effect(() => {
  if (addTodo.loading()) showSpinner();
  else if (addTodo.error()) showError(addTodo.error());
});
```

## Step 5 -- cross-tab sync (the unique feature)

Two lines. Add `crossTab: true` to your client. Done.

```js
const qc = queryClient({
  defaultStaleTime: 30_000,
  defaultCacheTime: 5 * 60_000,
  crossTab: true,                                    // <- that's it
});
```

Now `qc.setQueryData()`, `qc.invalidate()`, `qc.removeQueries()`, and `qc.clear()` propagate to all tabs of your app on the same origin. Optimistic updates land in every tab instantly. Invalidations trigger refetches in every tab simultaneously.

Open your app in two windows side by side. Update something in one. Watch the other refresh.

## Step 6 -- streaming (latest mode)

Some sources push instead of resolving once -- an SSE feed, a websocket, a live
metric. `streamQuery` (subpath `@zakkster/lite-query/stream`, optional peer
`@zakkster/lite-stream`) subscribes a key to an async iterable. In `latest` mode
`data()` is the most recent frame -- one signal write per frame, zero allocation.

```js
import { streamQuery } from '@zakkster/lite-query/stream';

async function* sse(url, signal) {
  const res = await fetch(url, { signal });
  const reader = res.body.getReader();
  const dec = new TextDecoder();
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) return;                                // completes -> status 'success'
      yield JSON.parse(dec.decode(value));
    }
  } finally {
    reader.cancel();                                   // runs on abort-on-detach
  }
}

const prices = streamQuery(qc, {
  key: ['prices', 'ACME'],
  stream: ({ key, signal }) => sse(`/sse/prices/${key[1]}`, signal),
});

effect(() => {
  if (prices.loading()) return;                        // pending: subscribed, no frame yet
  console.log(prices.data(), prices.count());          // latest value + frames seen
});
```

When the last observer disposes, the stream is aborted (the generator's
`finally` runs) and the entry is GC-scheduled -- exactly like a `query()`.

## Step 7 -- awaiting a query (route guard)

Sometimes you need a one-shot read that *awaits* the first success rather than
reacting to it -- a route guard, a setup script, a test. The
`@zakkster/lite-query/await` entry point (optional peer `@zakkster/lite-await`)
ships `whenQuery` for exactly that.

```js
import { whenQuery } from '@zakkster/lite-query/await';

// In a route guard: block navigation until the session query resolves.
async function requireSession() {
  const session = query(qc, {
    key: ['session'],
    fetcher: ({ signal }) => fetch('/api/session', { signal }).then(r => r.json()),
  });
  return whenQuery(session, { timeout: 5000 });        // resolves data(), rejects error()/TimeoutError
}
```

`whenQuery` forwards `timeout` / `signal` to the underlying wait, so a slow
query rejects with `TimeoutError` and an aborted one rejects with the abort
reason. It stays in an optional subpath, not the core -- composability over
feature-creep.

## Step 8 -- clean up

When you're done with a query (component unmount, route change, etc.):

```js
todos.dispose();
```

This decrements the observer count on the cache entry. If it was the last observer, the in-flight fetch is aborted and the entry is scheduled for garbage collection after `cacheTime`. If a new observer arrives before then, the cached data is served instantly and the GC timer is cancelled.

When tearing down the entire client (e.g., in a test, micro-frontend unmount, or hot-reload):

```js
qc.dispose();
```

This clears the cache and closes the `BroadcastChannel`. Without this, the channel listener keeps the client alive indefinitely.

---

That's the full surface in eight steps -- queries, mutations, cross-tab sync, streaming, and awaiting. For patterns beyond this (pagination, polling, dependent queries, debounced reactive keys, integration with `lite-watch-ex`), see [Cookbook.md](./Cookbook.md). For the complete API reference, see [README.md](./README.md).
