# Changelog

All notable changes to `@zakkster/lite-query` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.5.0] -- unreleased

The devtools FEED (`qc.inspect`) -- observability with a zero-cost off switch.
SPEC promised devtools; the PANEL belongs to lite-studio (its repo), so what
lite-query owes the ecosystem is the feed a panel renders: a synchronous,
push-mode stream of cache truth. The whole design tension is the off switch --
law 4 says the hot path buys nothing it does not use, so the uninstalled branch
is the product. Per OR-1 the version stamp lands only with the `/release 1.5.0`
drill after the pipeline closes; this head lands in-session with `package.json`
and the `VERSION` const held at `1.4.0`.

### Added

- **`qc.inspect(hook) -> uninstall`** -- install ONE observe-only feed hook (a
  panel multiplexes; hook arrays are rejected). Mirrors `persistQueryClient`'s
  install semantics exactly (TypeError on a non-function, Error on double-install,
  `=== hook`-guarded idempotent uninstall). Two INDEPENDENT single slots (OR-4):
  uninstalling the feed never disturbs the persister and vice versa. Emits from
  36 sites across 23 frozen `domain:verb` event types (entry lifecycle
  create/attach/detach/gc/remove, status + staleness, fetch dispatch/settle/abort
  with reason, cross-tab send/receive, sharedFetch request/fallback/serve, stream
  start/value/done/error, mutation start/settle, persist hydrate/save).
- **One monomorphic feed record** -- exactly 10 own keys, always present, in one
  order (`type, ts, key, keyHash, from, to, reason, count, ok, value`); a field
  that does not apply is `null` (`count` 0, `ok` false). One hidden class per
  type keeps a panel's reads monomorphic. `ts` is `performance.now()` (else
  `Date.now()`) resolved once at module load -- NOT `opts.now` -- monotonic
  non-decreasing. `key`/`value` are by reference, never copied, never serialized.
- **`QueryFeedEvent` + `FeedEventType` + `FeedHook`** typed in `Query.d.ts`; the
  full field table + 23-type vocabulary in `llms.txt`; README "Devtools feed"
  section + lite-studio pointer; Cookbook recipe 20 (a console logger in ten
  lines). The hook-must-copy contract is documented in all four.

### Decisions

- **OR-1** -- no in-session version stamp; the `[1.5.0]` head lands now, the
  `/release 1.5.0` drill stamps `package.json` / `VERSION` / `llms.txt` after the
  pipeline closes.
- **ON-1 (resolves STOP-1)** -- the charter's `qc._hook` spelling is
  unimplementable (no `qc` self-binding inside the client closure; `StreamQuery`
  destructures `_internal` once, so a core `let` can never be read live by the
  subpath). Shipped as a per-client cell `const feed = { hook: null, pool: null }`
  shared through `_internal.feed`, tested `feed.hook !== null` at every emit site
  in both files. One cold allocation per client; no per-entry slot (OR-5 holds).
- **OR-4 / V11** -- two seams, never one; OR-4's install/uninstall semantics win
  over the lite-signal `onGraphMutation` variant (no prior-listener restore, two
  throw shapes). The public feed REPORTS persist/hydrate activity; the adapter
  never consumes it.
- **ON-4 / OR-7** -- a throwing hook is contained fail-closed TOWARD THE FEED:
  the single synchronous dispatch funnel nulls the slot (instant return to the
  zero-cost path), logs exactly one `console.error`, and returns so the
  in-progress cache write completes. Re-throwing out-of-band via a microtask was
  rejected (it can kill the host process -- louder is not fail-closed).
- **OR-8** -- events speak lite-query's own lower-case domain vocabulary
  (attach/detach, dispatch/settle/abort, leader/follower); our shipped words win
  over lite-devtools' connect/disconnect. Mirroring means conventions (flat
  shape, lower-case string `type`, `ts`, idempotent uninstall), not renaming.
- **T3 / ON-4 (ratified)** -- the event object is ONE preallocated record PER
  TYPE, built at install, dropped at uninstall: an installed panel under a 60Hz
  stream allocates zero bytes per frame, and an app that never calls `inspect()`
  retains zero feed objects. The cost is the hook-must-copy contract, pinned by a
  counted identity test (two sequential same-type events share object identity).
- **ON-2 (resolves STOP-2)** -- the phase-H profiled window and its GATE line
  stay BYTE-FROZEN. The feed double-run (absent / installed-pooled /
  installed-fresh) plus a 20000-write sub-loop run as a separate provenance
  section of phase H AFTER the frozen gate evaluation, printing their own lines.
- **ON-3 / OR-10** -- the gating Q7 control is C-feed
  (`QUERY_TORTURE_BREAK=feed` -> a guaranteed `detach-without-attach` in the
  fuzzer feed state machine; controls now trip 5/5). Attempt D (a tracked
  inspect-hook closure carried across install/uninstall while the owner tree is
  torn down) DID NOT FIRE -- recorded verbatim in `INCONCLUSIVE.md` beside A/B/C;
  it is explicitly NOT a gate clause (a control that cannot trip is decorative).
- **QD-2 (operator ruling)** -- abort-emit honesty: every `fetch:abort` emit
  site captures `const was = signal.aborted` BEFORE the (unchanged, unconditional)
  `.abort(...)` call and emits only when `was === false`, so a controller a later
  site re-aborts (a timeout racing a remove/clear/supersede) emits ONE
  `fetch:abort` per dispatched generation -- zero behavior change, the feed is now
  one-abort-per-dispatch honest. The fuzzer assertion mode gates on this: after
  the teardown drain, `fetch:dispatch` count == `fetch:settle + fetch:abort`
  count, per-hash and global, strict equality. (The dropped per-keyHash SEQUENCE
  rules stay dropped -- keyHash aliasing under the fuzzer's remove-observed +
  restart-stranded-handle chaos is unresolvable without an entry-identity
  discriminator the frozen 10-key vocabulary cannot carry; rationale recorded in
  the fuzzer header.)
- **QD-3 (operator ruling)** -- `mutation:settle.count` is the settling
  mutation's OWN generation (pairs with `mutation:start` by value), NOT the
  current latest gen -- so a `superseded` settle still carries the id of the
  mutation that finished. Pinned in `llms.txt` and `Query.d.ts`.

### Phase H provenance -- T3 candidates measured (OR-6)

Both candidate designs measured under the same warm body (200000 reads) with a
20000-write emit sub-loop for the installed runs, outside the frozen GATE window.
Representative run:

| run | major | minor | maxMs | heapDelta bytes | B/op |
|---|---|---|---|---|---|
| absent (no hook) | 0 | 0 | 0.00 | ~31900 | ~0.14 |
| installed-pooled (candidate c) | 0 | 1 | 0.08 | ~61000 | ~0.28 |
| installed-fresh (candidate b) | 0 | 1 | 0.07 | ~41600 | ~0.19 |

The absent `B/op` is V8 heap bookkeeping noise (nonzero even at minor 0); the GATE
is the absent run's GC-PROFILE IDENTITY to the frozen GATE line (major 0 / minor 0
/ maxMs 0.00) -- the true zero-added-allocation signal -- with `B/op` held under a
hard < 1.0 bound so a real per-op retention regression still trips (QD-1).
Candidate (c) -- one pooled record per type -- ships: it holds `maxMajor 0` under
the emit load and allocates zero per event (the pooled record is overwritten in
place); candidate (b), a fresh per-event object, is the same order but pays the
allocation the hook-must-copy contract lets callers opt into only when they keep.

### Fixed

Four bake-facing Cookbook sample defects (commit 033e670, folded here per OR-2 --
no 1.4.1 docs patch; the verbatim bake-floors quote in recipe 18 is untouched):

- Recipes 18/19 imported PascalCase subpaths absent from lite-bake-stream's
  exports map (kebab-case only): `PreserveReader` is now root-imported
  (`RangeReader` is not a root export at 1.7.1 -- `/range-reader` subpath).
- Recipe 19 constructed `new RangeReader({ fetch, signal })`; the shipped API is
  `await RangeReader.open(adapter, { signal })` with `HTTPRangeAdapter.open`
  replacing the hand-rolled fetch (which also returned `ArrayBuffer` where the
  adapter contract requires `Uint8Array`).
- Recipe 19 called `reader.getJSON(i)` (a `PreserveReader` API); `RangeReader` is
  columnar -- rewritten to `prefetchRange`/`syncRange` windowed reads.
- Recipe 18 passed pooled `bytes.buffer`; now slices the unpooled copy (the bake
  suite idiom) so the cache actually restores on Node.

## [1.4.0] -- 2026-09-02

Persistence: a dehydrate/hydrate primitive and a `persistQueryClient` adapter on
top, built in that order. Instant cold start, fail-closed. Per OR-1 the version
stamp landed only with the `/release 1.4.0` drill after the pipeline closed (no
in-session stamp; the pipeline ran with `package.json` and the `VERSION` const
at `1.3.0`).

### Added

- `qc.dehydrate() -> { entries }` -- a plain-JSON snapshot of the SUCCESS entries
  only (cold path; walks the whole map, OR-8). Pending, error, and stream entries
  are never serialized (a promise, a failure, a live connection are not data --
  both exclusions tested). The state has exactly one own key `entries`; each
  record is monomorphic with exactly four own keys `{ key, data, dataUpdatedAt,
  infinite }`. `dataUpdatedAt` reuses the entry's existing staleness timestamp
  slot (`lastCompletedAt`) -- no new per-entry slot (OR-8). An infinite record
  carries a shallow copy of its pages array; `key` / `data` / page contents are
  references, not deep copies (aliasing contract, tested). No `version` field --
  the adapter stamps `{ version, state }` (OR-6).
- `qc.hydrate(state) -> { ok, count, reason }` -- boot-only, all-or-nothing cache
  restore (OR-2/OR-3). Validation runs over the whole payload before any
  mutation. Reason codes (stable, ASCII): `malformed-state`, `malformed-entries`,
  `malformed-entry`, `malformed-key`, `malformed-data`, `malformed-timestamp`,
  `malformed-pages`, `duplicate-key`. Seeded entries are stale-aware and arm the
  normal cacheTime GC; hydrate never broadcasts and never fires the write hook.
- `persistQueryClient(qc, { save, load, version, throttle? }) -> { restored,
  flush, stop }` -- a CORE export (OR-5), storage-agnostic thunks, zero new deps.
  Install validation is fail-closed: `save`/`load` must be functions; `version`
  is REQUIRED with no default (string/number, strict `===` at compare);
  `throttle ?? 1000` must be finite `>= 0`; a second install on the same client
  throws (single-slot seam). `handle.restored` always RESOLVES (never rejects)
  with `{ status: "restored" | "empty" | "dropped", count, reason }`; reasons are
  `null | "load-threw" | "malformed-envelope" | "version-mismatch" |
  "cache-not-empty" | <hydrate reason code>`. Restore reads once on install,
  before any observer attaches; the write hook is armed only AFTER the outcome
  settles, in all three branches, so seeding can never trigger a save. The
  envelope is exactly `{ version, state }` (two own keys).
- Infinite round-trip (OR-4/ON-1): dehydrate marks the entry and carries a copy
  of its pages; hydrate rebuilds pages + flat with `hasNext` false and
  `getNextCursor` null (fail closed -- an unattached restored list cannot
  auto-fetch at a wrong cursor). The first `infiniteQuery` attach adopts it: a
  new branch in `configure()`, keyed on the unambiguous state `entry.isInfinite
  && entry.getNextCursor === null`, installs `getNextCursor` and recomputes the
  cursor/hasNext from the restored pages via a `recomputeCursor` helper extracted
  from `rebuildInfinite` (one cursor-recompute site in the file, not two). A
  `getNextCursor` that throws at adoption is contained fail-closed: reset to a
  clean page-one fetch, never a wedge.

### Contracts (fail-closed)

- Throw/return asymmetry (ON-3a): `hydrate` THROWS only on the empty-cache
  precondition (a programming error -- the ABA bug of this domain; the error
  message names it and counts entries, not observers) and RETURNS `{ ok: false,
  count: 0, reason }` for a malformed stored payload (external data). This
  mirrors `setQueryData`'s local-throw / remote-drop law.
- Timestamp clamp (ON-3b): a seeded `lastCompletedAt = Math.min(dataUpdatedAt,
  now())` -- a FINITE future timestamp is clamped to our clock (bounds freshness
  at one staleTime from boot, never invents data); a NON-finite `dataUpdatedAt`
  (NaN, Infinity, string, missing) is `malformed-timestamp` and drops the WHOLE
  payload (the boundary is finite-vs-not).
- Flush-on-stop (OR-7/ON-3c), no opt-out: a pending throttle timer means a
  committed cache write is not yet on disk, and `stop()` fires at teardown /
  logout / dispose -- so it flushes the pending save, then uninstalls the hook
  and clears the timer. The documented logout path is `qc.clear()` then `stop()`,
  which persists emptiness (clear is a write hook site). `save()` rejections are
  contained; the timer is nulled before the flush builds state, so a write during
  `save()` opens a fresh window.
- Private write seam (OR-5): a single-slot `persistHook` on the client, fired by
  `notifyWrite()` at exactly SIX commit/settle sites -- `setQueryData` (covers
  every remote apply), `runFetch` settle (success and error uniformly), the
  `runFetch` commitPage-throw branch (returns before the settle site),
  `removeQueries` (only when `>= 1` entry matched), `clear` (only when the map
  was non-empty), and the cacheTime GC timer callback (an expired entry leaves
  the snapshot). `invalidate` is deliberately NOT a hook site -- it changes no
  cached content and `invalidatedSinceCompletion` is not serialized; nor are
  `ensureEntry`, attach/detach, prefetch (its writes land via the settle site),
  or hydrate seeding (which would re-save what was just loaded). With no adapter
  installed the warm path is unchanged (the hook is one `persistHook !== null`
  test at each cold site, none on the warm-read loop -- OR-8/G8).
- Boot-window / cross-tab safety (OR-2, corrected by ON-2): the earlier OR-2
  wording said the BroadcastChannel delivers on a "macrotask"; verified against
  the mock (`test/harness.js`, `queueMicrotask`) it is a MICROTASK, and the real
  channel a task -- so a strictly synchronous `create qc -> hydrate` cannot
  observe a queued remote `setData` under either, and the mock is the stricter of
  the two. An awaited boot that raced a remote `setData` legitimately hits the
  empty-cache precondition and drops as `cache-not-empty` (a pinned contract, not
  flake).

### Tests + torture

- 65 new core tests (`test/persist.test.js` 36, `test/persist-conformance.test.js`
  29); the suite is 268 (was 203), 0 fail, 0 skip. The conformance file mirrors
  the SHAPE of bake-stream's `test/DehydratedCache.test.js` (Part A round-trip,
  Part B corruption matrix with a pinned `MATRIX.length` of 23, Part C fail-open
  witness) and imports nothing from bake.
- Torture phase H gains a 4096-cycle dehydrate/hydrate/teardown loop through the
  adapter (install -> restored -> pending throttled save -> flush-on-stop),
  before the gc settle so live/findings cover it. GATE line unchanged:
  `GATE leak=size 0/0 findings=0 warnings=0 | gc major=0 minor=0 maxMs=0.00 | ok`.
  Break-mode controls trip 4/4 (`alloc` / `detach` / `fuzz` / `pages`).
- OR-11 findings-clause attempt C (INCONCLUSIVE.md): a payload tracked with
  `{ audit: true }` carried across `persistQueryClient.stop()` with a pending
  throttled save. It does NOT fire -- the adapter teardown touches no lite-signal
  owner tree, so it cannot leave the owner-cascade kernel's trip state. No
  `ctlPersist` control was added (a control that cannot trip is decorative); the
  clause stays uncontrolled and the non-firing is recorded, not faked.

### Fixed (QA QD-1..QD-4, fail-closed hardening)

- QD-1: `persistQueryClient` `flush()` is now a strict no-op once `stop()` has
  run (it was missing the `stopped` guard `onWrite()` already had, so a
  flush-after-stop double-saved). `stop()` stays idempotent; an explicit
  `flush()`/`flush()` while running still double-saves by design ("force now").
- QD-2: `hydrate` validation now MATERIALIZES a snapshot -- each of the four
  fields is read EXACTLY ONCE per record into internal flat storage, and seeding
  consumes only that snapshot, never the caller's objects again. A TOCTOU index
  getter (clean to validation, evil to seeding) can no longer diverge validated
  data from seeded data; first-read wins by construction.
- QD-3: symbol own-keys are malformed at both levels -- the own-key guards on the
  state object and each record now also require
  `Object.getOwnPropertySymbols(x).length === 0` (a symbol-keyed extra property
  slipped past the `Object.keys(...).length !== 4` guard). JSON carries no
  symbols, so nothing legitimate is rejected.
- QD-4: the entire validation traversal (including the QD-2 snapshot reads) is
  exception-contained -- a throwing getter during payload access returns the
  malformed reason for that level instead of escaping, so `hydrate`'s ONLY throw
  is the OR-2 empty-cache precondition (now tagged `err.code =
  "LQ_HYDRATE_NOT_EMPTY"`). The adapter's restore catch distinguishes: the
  precondition resolves `"cache-not-empty"`; any OTHER throw out of `hydrate`
  resolves a new sanctioned reason `"hydrate-threw"` (symmetric with
  `"load-threw"`, belt-and-braces so `restored` ALWAYS resolves even if a future
  defect reintroduces a throw). `"hydrate-threw"` is added to the RestoreOutcome
  reason union (Query.d.ts) and the llms.txt reason list. The conformance matrix
  gains the two adversarial classes (symbol-keyed record, throwing getter),
  moving `MATRIX.length` from 21 to 23.
- QD-5 (reviewer delta): the hydrate seed path now consumes the VALIDATED key
  hash. Validation materializes `record.keyHash`; seeding threads it through a new
  internal `ensureEntryByHash(key, keyHash)` (and `createEntry` takes the hash),
  so seeding re-walks NO caller object -- closing the last single-read gap (a
  stateful getter nested in a key ELEMENT could otherwise be walked twice, one
  walk escaping the contained validation pass, and a counting getter could
  diverge the storage hash from the validated/dup-checked hash). The validated
  hash is the storage hash and the map key by construction. The seed loop is also
  wrapped belt-and-braces: on ANY throw mid-seeding it rolls back every entry the
  call created (all fresh + unobserved: cancelGc + map delete + disposeEntry) and
  returns `{ ok: false, count: 0, reason: "malformed-entry" }` -- a future
  regression degrades to a clean all-or-nothing drop, never a throw or a partial
  cache (ON-3a + OR-3 held structurally).

## [1.3.0] -- 2026-09-02

Cursor pagination and route-loader prefetch, both first-class. The last honest
"trails TanStack" row -- pagination as a Cookbook recipe -- is gone.

### Added

- `infiniteQuery(qc, { key, fetcher, getNextCursor, enabled?, cacheTime?,
  staleTime?, retry? })` -- the cursor-paginated sibling of `query()`. One cache
  entry holds the whole list: `pages()` is the array of raw page results,
  `data()` is the flattened accumulation, `fetchNextPage()` appends the next page
  (dedups on the in-flight promise; no-op once exhausted), `hasNextPage()` reads
  the cursor state, plus `status()/error()/fetching()/refetch()/dispose()`. The
  fetcher receives `{ key, cursor, signal }` (cursor is `null` for page one).
  Lives in the SAME cache as `query()` -- getQueryData / setQueryData /
  invalidate / removeQueries / cross-tab all operate on it uniformly.
  `invalidate` / `refetch` rebuild from page one and replace the array on success
  (stale-while-revalidate over the whole list); a `pageGen` generation guard
  swallows a late page from a superseded fetch, so two generations never mix.
- `qc.prefetch(key, fetcher, opts?)` -- warm an entry with zero observers (route
  loaders, hover speculation). Fetches unless already fresh; a later `query()`
  adopts it without refetching; the entry GCs at cacheTime like any unobserved
  one. Per OR-4, prefetch of a fresh entry is a NO-OP -- no fetch, no cacheTime
  GC re-arm; a `force` variant is deferred until a consumer needs it.
- Entry monomorphism preserved: seven uniform infinite slots (`isInfinite`,
  `pages`, `flat`, `nextCursor`, `hasNext`, `pageGen`, `getNextCursor`) sit at
  their null/false/0 defaults on every plain entry -- one hidden class, zero new
  signal nodes for a plain query. Pages ride the entry's `data` signal (recreated
  once with a never-equal comparator so a same-ref set after an in-place push
  still notifies); the flat accumulation appends in place with no O(n) re-copy.
- Aliasing contract (documented in Query.d.ts + llms.txt): `data()` returns a
  LIVE array that grows in place as pages arrive -- copy it (`[...q.data()]`) to
  retain a snapshot across a `fetchNextPage`.

### Contracts (fail-closed)

- `getNextCursor` throw containment: a throw from user `getNextCursor` while
  resolving a page routes into the error ladder exactly as a rejected fetcher
  would (status `"error"`, `error()` set, `fetching` false, promise cleared);
  the staged page is rolled back so committed pages are preserved, and a later
  `fetchNextPage()`/`refetch()` re-attempts the same cursor cleanly. It never
  wedges the entry.
- `setQueryData` on an infinite entry requires a pages ARRAY. Non-array is
  asymmetric: a LOCAL call throws `TypeError`; a REMOTE/cross-tab non-array
  payload is dropped silently (cannot throw across tabs), leaving the entry
  untouched -- coherence preserved.
- `qc.prefetch` is a strict NO-OP on an infinite entry: it carries no page
  cursor and must never advance live pagination. Returns the in-flight promise
  or current data without fetching. Paginated prefetch is a deliberate future
  API, not this path.

### Tests + torture

- 203 deterministic tests (142 core + 31 await + 24 stream + 6 drift guards),
  up from 181: `test/infinite-query.test.js` (21 -- accumulation/flat order,
  cursor exhaustion, concurrent-fetchNextPage dedup, invalidate/refetch page-one
  replace, late-page-from-dead-generation swallow, detach + key-change mid-page
  abort, error ladder, enabled gate, cross-tab page sync, prefetch adoption /
  fresh no-op / cacheTime GC, plus the three contract regressions above) and a
  zero-GC warm-read contract for infinite handles.
- The /await VERSION provenance guard now asserts SOURCE identity (Awaitable.js
  re-exports `VERSION` from `./Query.js`; its lite-await re-export block carries
  none) instead of value inequality with lite-await's VERSION -- the 1.3.0/1.3.0
  version collision between the two packages falsified the original `notEqual`
  mechanism during this release drill. Test count unchanged.
- `test/torture.mjs` phase H reads a prefilled 4-page infinite handle inside the
  200000 warm loop at zero allocation; the GATE line is byte-identical:
  `GATE leak=size 0/0 findings=0 warnings=0 | gc major=0 minor=0 maxMs=0.00 | ok`.
- `bench/torture/query-soak.mjs` gains 32 infinite handles, fetchNextPage storms
  and invalidate races with a gen/idx page-accumulation oracle;
  `QUERY_TORTURE_BREAK=pages` splices a foreign-generation page and the oracle
  trips (`FAIL: page mixing`, exit 1). New phase-C control `pages` joins
  `alloc`/`detach`/`fuzz`; `QUERY_TORTURE_BREAK=1` trips 4/4; the unknown-value
  message reads `1|alloc|detach|fuzz|pages`.
- `bench/torture/shared-fetch-soak.mjs` gains a two-tab infinite page-storm
  oracle: a follower storms `fetchNextPage` on a shared key and converges to the
  leader's exact pages array while the leader fetches each page exactly once
  (`per-page [[0,1],[1,1],[2,1],[3,1],[4,1]]`, follower self-fetches 0). Proven
  able to fail via `QUERY_TORTURE_BREAK=storm` (`FAIL: page-storm divergence`).
- OR-6: the findings-clause audit path was re-attempted through the new
  infinite/prefetch teardown paths (dispose-owner-mid-page-fetch and
  tracked-fetchNextPage-promise + removeQueries-mid-flight). Neither fired;
  re-recorded in INCONCLUSIVE.md and carried to Q6.

## [1.2.1] -- 2026-09-02

Harness only. No runtime change: Query.js, StreamQuery.js and Awaitable.js are
byte-identical to 1.2.0.

### Added

- `test/torture.mjs` is now the single suite-law entry (`npm run torture:law`).
  Phase P execs the three `bench/torture/` soaks as children and asserts exit 0
  on each; phase H keeps the 1.2.0 lifecycle-churn and warm-read gates unchanged;
  phase C adds controls. Prints the same `GATE leak=... | gc ... | ok` line, then
  `ok`. Exit 0/1. Measured this session:
  `GATE leak=size 0/0 findings=0 warnings=0 | gc major=0 minor=0 maxMs=0.00 | ok`.
- Phase C controls behind `QUERY_TORTURE_BREAK` (`1` = all, or `alloc`/`detach`/
  `fuzz`): an alloc-per-iteration twin of the warm loop, a skipped-detach
  teardown, and a one-phantom-broadcast corruption of the fuzzer's echo oracle.
  Each verified to exit non-zero individually (alloc 1, detach 1, fuzz 1);
  `QUERY_TORTURE_BREAK=1` trips 3/3. `npm run torture:control`. An unknown
  value (including `"0"`, which is truthy) exits 1 naming the valid set
  (`1|alloc|detach|fuzz`); an empty value runs the plain gate ("set" means
  non-empty). Closes Q-09: the 1.2.0 leak/gc gate had no control and could
  not fail.
- WeakRef census beside `tracker.size()` (lite-leak 1.10.0: size() counts
  registrations, not reachability). One-sided gate: fails only when nothing in
  the sample is collected across 8 settle cycles (plain run 0/256). The detach
  control exercises this clause too: its 128 pinned handles come back all-live
  (128/128), so it trips the census clause alongside the leak clause.
- `test/torture.mjs` refuses to run without `--expose-gc`: it exits 1 with
  `missing --expose-gc; run: node --expose-gc test/torture.mjs` before any phase,
  instead of failing incidentally with a leak-shaped census message.
- Explicit `verdict === 'pass'` requirement; an inconclusive verdict fails and
  points at INCONCLUSIVE.md. `allowInconclusive` is never set.
- `INCONCLUSIVE.md` -- triage for the third verdict. Repo doc, not shipped. It
  records the one uncontrolled clause: `findings.length === 0` has no control
  (owner-cascade audit() cannot fire through the public surface without a
  runtime edit OR-1 forbids); carried to Q5.
- `bench/torture/README.md`: measured numbers now carry date / machine / node.

### Changed

- `npm run torture` is the union entry (phase P runs the three soaks once).
  `prepublishOnly` runs the full law. `torture:leak` retained as an alias.
- ascii-guard walk extended to `test/**/*.mjs` and `INCONCLUSIVE.md` (no new
  tests; the existing walk test covers more files -- suite stays 181).
- llms.txt / README test counts corrected: 177 -> 181. The 177 figure predated
  the four QA boundary tests (764c8eb) that split stream 20 -> 24.

### Gates

- `npm test` -> 181/0/0. `npm run torture` -> ok, exit 0.
  Controls: alloc 1, detach 1, fuzz 1. `npm pack --dry-run` -> 13 files.

## [1.2.0] -- 2026-09-02

The sibling refresh: catch up to both peers in one minor. `npm test` -> 181
pass, 0 fail, 0 skipped; `npm run torture` -> 4/4 PASS, exit 0, ending
`GATE leak=size 0/0 findings=0 warnings=0 | gc major=0 minor=0`.

### Added

- **18-name parity with `@zakkster/lite-await` 1.3.0** (Q-07). The `/await`
  subpath now re-exports the eight primitives that were `undefined` from it
  while sitting in the installed dependency: `allSettledOf`, `withResolvers`,
  `tryFn`, `delay`, `withRetry`, `mapLimit`, `whenStatechart`,
  `createAwaitScope`. Verbatim, zero wrapping -- single source of truth.
  Boundary docs keep them and `query()` apart: `withRetry` is not the cache's
  refetch; `delay` advances wall clock, not a mock clock; `createAwaitScope`
  binds N signal-aware awaiters to one `AbortController` (upstream
  `decisions/0007` owns the contract).
- **`VERSION` const** (Q-16). Defined in `Query.js`, re-exported by `/stream`
  and `/await`, so every entry reports one version string -- the single
  runtime version source. `test/version-sync.test.js` asserts
  `VERSION === package.json` (string compare).
- **Suite-law torture gate seed.** `test/torture.mjs` wired to the real
  entry points: 4096 query + streamQuery lifecycle cycles with the
  lite-leak tracker returning to 0 (0 findings), then 200000 warm accessor
  reads under lite-gc-profiler at `major=0 minor=0 maxMs=0.00`. Appended to
  the `npm run torture` chain (now 4 phases). `@zakkster/lite-leak` and
  `@zakkster/lite-gc-profiler` join as devDependencies; nothing ships
  (`test/` is never in `files[]`).
- **Tests 158 -> 181.** Five streamQuery parity tests recording the observable
  semantics across the pipeToSignal collapse (status ladder, droppedCount
  ladder, restart reset, throwing iterator, abort-is-not-an-error); eight
  re-export identity tests plus `withRetry` / `mapLimit` / `createAwaitScope`
  integrations and the subpath surface set-compare; the version-sync guard;
  four QA boundary tests (`droppedCount()` after dispose, `maxBuffer: 1`
  window, restart before the first frame, latest-mode droppedCount stays 0).

### Changed

- **StreamQuery internals: the hand-rolled buffer ring collapses into
  lite-stream 1.3.0's own `pipeToSignal`.** `mode`/`maxBuffer` replace the
  ring; `onValue` drives the status ladder + count; `onAbort` absorbs
  intentional aborts (detach / restart / removeQueries), which with it present
  never reach `onError` (lite-stream `decisions/0001`); `droppedCount` reads
  the stop fn's own overflow getter, snapshotted on every terminal path.
  Zero user-visible API change -- the parity suite passes untouched on both
  sides. Buffer-mode bench (node v26.3.1, 2026-09-02; N=200000, maxBuffer=8,
  median of 3): 31.9 ms before / 31.8 ms after -- within run-to-run noise, no
  measurable delta; both drop exactly 204992.
- **Sibling floors -> `^1.3.0`** for both `@zakkster/lite-await` and
  `@zakkster/lite-stream` (peer + devDep). Installed-surface verified before
  any dependent code: await exposes 18 named + `VERSION` incl.
  `createAwaitScope`; stream's `pipeToSignal` accepts `mode`/`maxBuffer`/
  `onValue`/`onAbort` and its stop fn carries `droppedCount`/`overflowCount`.
- **Vendored demo copies -> 1.3.0** (`demo/vendor/lite-await.js`,
  `demo/vendor/lite-stream.js`), byte-copies of the published registry
  tarballs; `demo/VENDOR.md` rows + per-copy grep checks updated. Not shipped
  (`demo/` is never in `files[]`).

### Fixed

- Nothing. No defect surfaced; the collapse is behavior-preserving by
  construction and proven so by the frozen parity suite.

### Notes

- **`fromPromise` vocabulary is final.** lite-await `decisions/0009` verdict =
  REJECT (accepted 2026-09-01): the name and its `{status, data, error}`
  signal-state projection are locked, and the docs mapping table is permanent.

## [1.1.2] -- 2026-08-31

A docs-and-guards patch. Zero logic: no runtime token in `Query.js`,
`StreamQuery.js`, or `Awaitable.js` changed -- every `.js`/`.d.ts` hunk is
comment bytes only. `npm test` -> 158 pass, 0 fail, 0 skipped;
`npm run torture` -> 3x PASS, exit 0. Version unchanged (the release drill
owns the bump).

### Added

- **Two drift guards join the default `npm test`** (153 -> 158 tests):
  `test/ascii-guard.test.js` walks the package.json `files[]` scope (parsed at
  runtime, so future `files[]` changes are covered) plus `test/*.js` and
  `bench/**/*.mjs`, asserting every byte is printable ASCII + LF. It carries an
  allowlist mechanism for the source-law U+00D7/U+00B5 exception (empty today)
  and two failing controls (an inline U+2014 fixture; the allowlist escape
  hatch). `test/surface-guard.test.js` asserts every runtime export is
  documented in `llms.txt` AND typed in its `.d.ts`, and every peer named in
  `llms.txt` resolves to `package.json` peerDependencies, with a failing
  control. Both are pure `node:test` + `node:fs`, zero new dependencies.
- **Guides shipped.** `QuickStart.md` and `Cookbook.md` added to `files[]`;
  `npm pack` -> 13 files (was 11).
- **`demo/VENDOR.md`** documenting the pinned `demo/vendor/` copies
  (`Signal.js`/`Watch.js` 1.5.0, `lite-await.js` 1.0.0, `lite-stream.js`
  1.0.0) and the pre-bump `grep -c createRoot demo/vendor/Signal.js` check.
  Not shipped (`demo/` is never in `files[]`).

### Changed

- **QuickStart 1.1 refresh.** Install line reduced to the one required peer
  (`@zakkster/lite-signal`); added a `streamQuery` latest-mode taste and a
  `whenQuery` route-guard taste (consistent with Cookbook 14 and 11); the
  stale "five steps" closing claim corrected.
- **README doc links flipped** from absolute GitHub URLs back to relative
  `./QuickStart.md` / `./Cookbook.md`.
- **SPEC.md reconciled with ROADMAP.md.** Marked as the historical 1.1.0
  release-candidate document; its "Post-publish roadmap" body replaced by a
  two-line pointer to ROADMAP.md.
- **Wording riders.** README "peer deps in the lite ecosystem" -> the one
  required peer; README lite-channel "powered by" / "engine behind" softened
  to "the convenient `isLeader` source" (matching the no-hard-dependency note
  already in the doc); README stream-demo path qualified "in the repo".

### Fixed

- **ASCII-law sweep** (Q-08) -- comment/prose punctuation only, no logic.
  Per-file non-ASCII codepoints swept to 0: Query.js 837, Query.d.ts 443,
  StreamQuery.js 63, StreamQuery.d.ts 2, Awaitable.d.ts 2; test/query.test.js
  3449, test/edge-cases.test.js 328, test/harness.js 184; bench/bench.mjs 308,
  bench/torture/cache-fuzzer.mjs 440, bench/torture/query-soak.mjs 476,
  bench/torture/shared-fetch-soak.mjs 476, bench/torture/README.md 25;
  README.md 75, llms.txt 57, CHANGELOG.md 23, Cookbook.md 30, QuickStart.md
  13, SPEC.md 11. Mapping: U+2500 run -> `-` x N; em/en dash -> `--`/`-`;
  arrows -> `->`/`<-`; U+2265/U+2264 -> `>=`/`<=`; middle dot and bullet ->
  `*`; (c)-sign -> `(c)`; i-diaeresis -> `i`; U+00D7 -> `x`; U+25B6 -> `>`.
- **Stale sequencing claim.** `llms.txt` said one-connection-shared streaming
  lands in "1.2"; the roadmap moved that work to 2.0 -- corrected to "2.0
  (see ROADMAP.md)".
- **Doc test counts aligned to the runtime.** The guards raise `npm test` to
  158, so the README tagline, facts-table row, and Tests section, and the
  llms.txt headline, now say 158 (breakdown: 120 core + 18 await + 15 stream
  + 5 repo drift guards). Per-section counts (120 core, 106 in
  query.test.js) were verified still correct and left as-is.

## [1.1.1] -- 2026-08-31

A gates-and-truth patch. No runtime code changed -- no `.js` source file is
touched by this release. The diff is scripts, metadata, docs, and the torture
suite that was written but never committed.

### Added

- **Torture suite landed** under `bench/torture/`: `query-soak.mjs`
  (cache-lifecycle churn), `cache-fuzzer.mjs` (two-tab coherence including
  streamQuery handles), `shared-fetch-soak.mjs` (leader/follower dedup under
  churn), plus the `torture`, `torture:soak`, `torture:fuzz` and
  `torture:shared` scripts. Seeded PRNG (`TORTURE_SEED`), deterministic mock
  clock and mock `BroadcastChannel`, and hard invariants: zero errors, entry
  map drains, no dangling timers, signal registry back to baseline
  (`activeLinks === 0`), exit 1 on any violation. `npm run torture` ->
  3x PASS, exit 0.
- **`prepublishOnly`: `npm test && npm run torture`** -- the gate is now
  structurally in front of every publish, not a step someone remembers.
- **`.gitignore`** (`node_modules/`, `*.tgz`, `package-lock.json` --
  re-affirming the repo's deliberate no-lockfile stance). Its absence let a
  partial `node_modules` survive in the tree; every test file failed with
  `ERR_MODULE_NOT_FOUND` until `npm install`.

### Changed

- **Peer floor stabilized.** `@zakkster/lite-signal` moves from
  `>=1.5.0-alpha` to `^1.5.0` in both `peerDependencies` and
  `devDependencies`. 1.5.0 stable is published; README and llms.txt already
  said `>= 1.5.0`.

### Fixed

- **The default test gate now runs the zero-GC identity test.** `npm test`
  was `node --test test/*.test.js`, while `test/zero-gc.test.js` gates itself
  on `--expose-gc` -- so every run printed `skipped 1` and the package's
  headline claim went unexercised. The script is now
  `node --expose-gc --test test/*.test.js`: 153 pass, 0 fail, 0 skipped. The
  README Tests section is updated to match.
- **The README install section now matches the manifest.** It listed
  `@zakkster/lite-store` and `@zakkster/lite-channel` as two of "three peer
  dependencies". Neither is a peer: the manifest declares
  `@zakkster/lite-signal` (required) plus `@zakkster/lite-stream` and
  `@zakkster/lite-await` (optional, reached only through the `/stream` and
  `/await` subpaths), and `Query.js` imports only `lite-signal`. The
  Ecosystem section keeps the cross-reference -- those packages compose with
  lite-query, they are not required by it.
- **llms.txt's peer block told a consumer to install a signal version on
  which `/stream` cannot run.** It said `@zakkster/lite-signal ^1.1.3` (the
  real floor is `^1.5.0`; the query/stream watchers use `createRoot`, which
  does not exist in 1.1.x) and repeated the lite-store / lite-channel peer
  claim. The block now mirrors `package.json` name-for-name.
- **llms.txt's headline test count disagreed with its own breakdown** ("152
  deterministic tests" vs a parenthetical summing to 153). It counted passes
  under the gate that skipped one test; both now say 153.
- **Dead documentation links in the published tarball.** The README pointed
  at `./QuickStart.md` and `./Cookbook.md`; `files[]` ships neither, so both
  links were broken on the npm page. Both now point at absolute GitHub blob
  URLs on `main`. Decision: link out now rather than ship the files, because
  QuickStart.md still documents the 1.0 surface; the ship-in-tarball decision
  is deferred until it is refreshed. Rejected alternative -- adding
  Cookbook.md to `files[]` immediately: it would leave the two links
  inconsistent for a release with no reader benefit. The license badge also
  pointed at `LICENSE.txt`; the file is `LICENSE`.
- **The `[1.1.0]` heading carried no release date** in the tarball npm serves
  as `latest` -- it still claimed the shipped version was pending. It is now
  stamped with its actual publish date (2026-06-23).

## [1.1.0] -- 2026-06-23

Integration release: **streaming queries** (via `@zakkster/lite-stream`) and
**async coordination** (via `@zakkster/lite-await`). The core `@zakkster/lite-query`
entry point and its three peer dependencies are unchanged -- all new capability
ships behind opt-in subpath exports with optional peer deps. See `ROADMAP.md`
for the full design and rationale.

### Added

- **Streaming queries** -- `@zakkster/lite-query/stream` -> `streamQuery(qc, opts)`.
  A multi-shot, iterator-backed query: subscribe by key to an async iterable
  (SSE, websocket frames, paginated cursors, pubsub topics) and read its state
  as signals. Built on `lite-stream`'s `fromAsyncIterable`.
  - Modes: `"latest"` (signal holds the most recent value) and `"buffer"`
    (signal holds a bounded ring of recent values; `maxBuffer` required --
    unbounded buffering is rejected by design).
  - Accessors: `data()` / `error()` / `status()` / `done()` / `count()` /
    `droppedCount()`, plus `restart()` and `dispose()`.
  - Lazy (no observers -> no iterator pulled), structural abort-on-detach
    (`iterator.return()` via the abort signal), reactive-key restart, and an
    `enabled` gate -- the same lifecycle guarantees as `query()`.
  - Lives in the same `queryClient` cache: `getQueryData` / `invalidate` /
    `removeQueries` operate on stream entries uniformly.
- **New status value `"streaming"`** for stream entries, extending the query
  vocabulary to `idle | pending | streaming | success | error`. `pending` =
  subscribed, no value yet; `streaming` = at least one value, not done;
  `success` = iterator completed naturally; `error` = iterator threw or aborted.
- **Async coordination** -- `@zakkster/lite-query/await`. Re-exports the
  `lite-await` primitives verbatim (`whenSignal`, `whenTruthy`, `whenEquals`,
  `allOf`, `anyOf`, `raceOf`, `withTimeout`, `withAbort`, `fromPromise`,
  `TimeoutError`) plus two query-native bridges:
  - `whenQuery(q, predicate?, opts?)` -- resolves with `q.data()` when the query
    reaches `success` (or a custom predicate over `status()`); rejects with
    `q.error()` on `error`. Honors `timeout` and `signal`.
  - `whenAllQueries(queries, opts?)` -- resolves with the data array in input
    order when every query reaches `success`; rejects on the first error or
    timeout. Built on `allOf`.
- **`fromPromise` re-export** -- positioned as the "queryless query": one-shot
  promise -> reactive `{ status, data, error }` with no cache, for the cases
  where caching is genuinely unwanted. Its native `pending/resolved/rejected`
  vocabulary is documented with an explicit mapping to lite-query's
  `success/error` (`resolved -> success`, `rejected -> error`).

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
  `fromPromise` returns a consumer-owned signal -- call `dispose()` on it per
  lite-await's contract.
- **Entry shape stays monomorphic.** The three stream slots (`isStream`,
  `streamStop`, `streamState`) are added uniformly to every entry as
  `false`/`null`; a stream signal node is allocated only when a stream runs. The
  `promise` slot is not reused for the pump (it backs the fetch-dedup guard).
- **Streaming data does not cross-tab broadcast.** Each tab owns its own
  connection (SSE/websocket is per-document). `invalidate` and `removeQueries`
  still propagate cross-tab (every tab aborts and re-establishes its own stream).
  Leader-election **shared streams** -- one upstream connection per browser, frames
  broadcast to follower tabs, the multi-shot dual of 1.0's shared fetch -- are the
  planned 1.2 headline.

## [1.0.0] -- 2026-05-28

Initial release.

### Added

- `queryClient()` -- cache and lifecycle owner; supports `defaultStaleTime`,
  `defaultCacheTime`, `defaultTimeout`, `retry`, `retryDelay`, `crossTab`,
  injectable `now` / `setTimeout` / `clearTimeout` / `broadcastChannel`.
- `query(qc, opts)` -- reactive query factory with lazy observer tracking, same-
  entry detection on watcher re-runs, generation guard, retry-with-backoff,
  abort-on-detach, stale-while-revalidate.
- `mutation(qc, opts)` -- `onMutate` / `onSuccess` / `onError` / `onSettled`
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
  self-fetches if no leader serves -- a liveness guarantee independent of the
  election state. Composes with `@zakkster/lite-channel`'s leader signal with
  no hard dependency. The feature TanStack Query and SWR don't ship.
- Per-query and client-default `timeout` option. Aborts the fetch with
  `ABORT_REASON.TIMEOUT` reason on the `AbortSignal`.
- Abort reason vocabulary: `ABORT_REASON.DETACH | REFETCH | REMOVED | TIMEOUT`
  exposed via `signal.reason` for retry/logging decisions in user fetchers.
- Mid-flight invalidation semantics: option (b) -- let the in-flight finish,
  then refetch immediately.
- Opt-in `equals` per query for structural sharing without re-firing effects
  on referentially-different-but-structurally-equal data.

### Tested

- 106 deterministic tests across 22 sections.
- Adversarial cases: mutation race (slow first + fast second), `onSuccess`
  throw with `onSettled` still firing, shared-observer mid-fetch dispose,
  cross-tab race conditions, reactive `enabled` -> false abort, sparse-key
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
