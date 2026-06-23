# QuickStart

You have lite-query installed. You want code on screen in five minutes. This is that document.

```sh
npm install @zakkster/lite-query @zakkster/lite-signal @zakkster/lite-store @zakkster/lite-channel
```

## Step 1 — the client

The client owns the cache. Make one per app (or one per logical scope — e.g., per logged-in user session).

```js
import { queryClient } from '@zakkster/lite-query';

export const qc = queryClient({
  defaultStaleTime: 30_000,       // ms — refetch if observer attaches to data older than this
  defaultCacheTime: 5 * 60_000,   // ms — entry GC'd this long after the last observer leaves
});
```

## Step 2 — a query

A `query()` defines *what* to fetch and *when*. It doesn't fetch on creation — only when something reads its accessors inside an effect.

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

## Step 3 — a reactive key

The `key` can be a function. Reading signals inside that function subscribes the query — when those signals change, the query refetches with the new key.

```js
import { signal } from '@zakkster/lite-signal';

const userId = signal(1);

const user = query(qc, {
  key: () => ['user', userId()],                     // function, not value
  fetcher: async ({ key, signal }) =>
    fetch(`/api/users/${key[1]}`, { signal }).then(r => r.json()),
});

effect(() => console.log(user.data()));

userId.set(2);                                       // → triggers a new fetch with key ['user', 2]
```

The old fetch is aborted automatically. If you swap back to `userId.set(1)` and the cache entry hasn't been GC'd yet, you get the cached `user 1` value instantly.

## Step 4 — a mutation

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

## Step 5 — cross-tab sync (the unique feature)

Two lines. Add `crossTab: true` to your client. Done.

```js
const qc = queryClient({
  defaultStaleTime: 30_000,
  defaultCacheTime: 5 * 60_000,
  crossTab: true,                                    // ← that's it
});
```

Now `qc.setQueryData()`, `qc.invalidate()`, `qc.removeQueries()`, and `qc.clear()` propagate to all tabs of your app on the same origin. Optimistic updates land in every tab instantly. Invalidations trigger refetches in every tab simultaneously.

Open your app in two windows side by side. Update something in one. Watch the other refresh.

## Step 6 — clean up

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

That's the full surface in five steps. For patterns beyond this (pagination, polling, dependent queries, debounced reactive keys, integration with `lite-watch-ex`), see [Cookbook.md](./Cookbook.md). For the complete API reference, see [README.md](./README.md).
