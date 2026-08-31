# `bench/torture/` -- lite-query soak / fuzz suite

These are **not benchmarks**. Each file hammers a lite-query design pillar under
high-volume churn, reports an ops/sec for context only, and then asserts a set
of hard invariants. Exit code is `0` on a clean run, `1` on any thrown error or
failed invariant -- so they drop straight into CI.

Every file drives the deterministic mock clock and mock BroadcastChannel from
[`test/harness.js`](../../test/harness.js), so GC timers, deferred fetches,
retries, and cross-tab delivery are reproducible rather than wall-clock racy.
Each installs an explicit lite-signal registry via `setDefaultRegistry`
(`onCapacityExceeded:"grow"`, so a churn spike can't trip the default cap) and
compares `registry.stats()` before and after the run -- that pool snapshot is
the leak oracle, so **no `--expose-gc` is required**.

## Run

```sh
npm run torture          # all three, in sequence
npm run torture:soak     # query-soak.mjs
npm run torture:fuzz     # cache-fuzzer.mjs
npm run torture:shared   # shared-fetch-soak.mjs
```

Knobs (env): `TORTURE_SECONDS` (default `5`), `TORTURE_SEED` (reproducible op
stream), and `FOLLOWERS` (shared-fetch-soak only, default `4`).

```sh
TORTURE_SECONDS=30 TORTURE_SEED=7 npm run torture:soak
```

## What each file tortures

### `query-soak.mjs` -- single-client cache-lifecycle churn
A fixed keyspace with static and reactive-keyed `query()` handles and a pool of
observer effects. Continuously mounts/unmounts observers, flips reactive keys,
and issues `setQueryData` / `invalidate` / `removeQueries` / `refetch` / batched
writes. Fetchers resolve on a microtask, resolve on a deferred mock-clock timer
(so a detach or removal mid-flight has to abort a live fetch and the generation
guard has to swallow the late resolution), or reject.

Asserts: zero errors * the entry map fully drains after teardown * no timers
dangle * the signal-node/link pool returns to baseline (`activeLinks === 0`).
This is the refcount -> GC -> `disposeEntry` chain proving it leaks nothing under
churn -- a dropped detach or a forgotten signal in `disposeEntry` shows up as a
non-zero pool.

### `cache-fuzzer.mjs` -- two-tab cross-tab coherence
Two clients share one instrumented BroadcastChannel with `crossTab:true` and
fuzz the same cache concurrently, including `streamQuery` handles that mount,
`restart()`, and tear down under load.

Asserts: zero errors * **no echo storm** -- every locally-issued mutation
broadcasts exactly once and every remotely-applied mutation broadcasts zero
times, so total `postMessage`s equals the mutations we issued (an echo-loop
regression makes this strictly greater, and runs away) * **convergence** -- after
the churn, a sentinel written on tab A is read back on both tabs * both entry
maps drain * pool returns to baseline. `sharedFetch` is intentionally off here
(its `fetch-req` traffic is a separate protocol) so the echo count stays exact.

### `shared-fetch-soak.mjs` -- leader/follower fetch dedup
One leader tab and N followers share a channel with `sharedFetch:true`. The
leader keeps a permanent observer on every shared key; followers churn observers
while the leader invalidates and writes. `sharedFetchTimeout` is set far beyond
any mid-run clock advance, so a follower fallback self-fetch is structurally
impossible until teardown.

Asserts: zero errors * **the dedup invariant** -- follower tabs issue *zero*
local fetches for shared keys (each follower fetcher is a spy; the sum must be
`0`) * **liveness** -- the leader actually fetched, and a follower ends up
holding the leader's fetched value for a probe key it never fetched itself
(proving the result travelled leader -> follower over the channel) * every entry
map drains * pool returns to baseline.

---

Like the `bench/` benchmarks and the lite-signal `bench/torture/` soaks these
mirror, this directory is dev-only: it is not in `package.json` `files[]` and
never ships to npm.
