# @zakkster/lite-query — release candidate

**Status:** Publication-ready. 120/120 tests green under `npm run test:gc`
(119 green under plain `npm test`; the zero-GC contract test auto-skips
without `--expose-gc`). Full docs + interactive demo + ecosystem positioning
complete.

## Deliverables

| File | What |
|---|---|
| `Query.js` | Core library. Watcher now wraps `effect` in `createRoot` (survives the lite-signal >=1.2 owner tree); entries carry uniform stream slots |
| `Query.d.ts` | TypeScript declarations (core surface) |
| `StreamQuery.js` | `/stream` entry — `streamQuery`, pumps lite-stream into the cache entry's own signals (zero extra signals) |
| `StreamQuery.d.ts` | Declarations for `/stream` (latest -> `T`, buffer -> `T[]` overloads) |
| `Awaitable.js` | `/await` entry — re-exports lite-await + `whenQuery` / `whenAllQueries` bridges |
| `Awaitable.d.ts` | Declarations for `/await` |
| `package.json` | npm publish config — three subpath exports w/ types, optional peers, MIT |
| `LICENSE` | MIT |
| `README.md` | Public docs + facts table (incl. streaming row) + bench + streaming/await API |
| `QuickStart.md` | 5-step intro |
| `Cookbook.md` | 16 recipes (13 core + 3 streaming); recipe 11 migrated to `whenQuery` |
| `CHANGELOG.md` | 1.1.0 + 1.0.0 notes, Keep-a-Changelog format |
| `llms.txt` | AI-friendly reference (incl. `/stream` + `/await`) |
| `demo/index.html` | Interactive 4-scene core demo with live cache inspector |
| `demo/stream-query-demo.html` | 3-scene streaming demo (latest 60fps / buffer / lifecycle) + cache inspector + status diagram |
| `demo/vendor/` | One shared lite-signal 1.5.0-alpha (+ Watch.js), lite-stream, lite-await — until 1.5.0 hits esm.sh |
| `bench/bench.mjs` | Comparative bench vs `@tanstack/query-core` 5.101 |
| `test/query.test.js` | Core specification suite |
| `test/edge-cases.test.js` | Dispose contracts, abort-reason surface, reactive-key churn |
| `test/zero-gc.test.js` | Zero-allocation hot-path contract |
| `test/awaitable.test.js` | 18 tests for `/await` |
| `test/stream-query.test.js` | 15 tests for `/stream` (all termination paths, lazy, restart, interop) |
| `test/harness.js` | Mock fetcher, clock, BroadcastChannel |

## Pre-publish checklist

- [x] 152/153 tests green under `npm run test:gc` (core 120 incl. 1 pre-existing skip + await 18 + stream 15)
- [x] Bench compiles and runs against `@tanstack/query-core` 5.101
- [x] TypeScript types shipped for every entry point (`Query.d.ts`, `StreamQuery.d.ts`, `Awaitable.d.ts`); typecheck clean under `tsc --strict --module nodenext`
- [x] Core peer deps declared (`lite-signal >=1.5.0`, `lite-store ^1.0.0`, `lite-channel ^1.0.0`)
- [x] Optional peers declared via `peerDependenciesMeta.optional` (`lite-stream`, `lite-await`) — core-only installs see no extra requirement / no warnings
- [x] `exports` map: `.`, `./stream`, `./await`, each with `node`/`import`/`types`/`default` (author convention) + top-level `types`
- [x] LICENSE present
- [x] Comparison docs avoid attacking; pure facts
- [x] Cross-tab feature prominently showcased in demo + README
- [x] `sideEffects: false` for tree-shaking
- [x] Engines field set (Node 18+)
- [x] Hot-path zero-allocation contract test in suite
- [x] CHANGELOG / llms.txt / README refreshed (streaming + await, test counts, lite-signal >=1.5.0)
- [x] Demos vendor a single lite-signal 1.5.0-alpha under `demo/vendor/` (esm.sh has no 1.5.0 yet); one shared instance, no dual-package split
- [ ] **Blocker: publish `@zakkster/lite-signal` 1.5.0 first.** The watcher imports `createRoot` (1.5.0); lite-query 1.1.0 will not resolve against an older lite-signal.
- [ ] After 1.5.0 is on npm + esm.sh: revert both demo import maps from `./vendor/Signal.js` to `https://esm.sh/@zakkster/lite-signal@1.5.0`
- [ ] Verify repository URL in `package.json` matches your actual remote
- [ ] `npm publish --access public` when ready

## Publish command sequence

```sh
# 0. Publish @zakkster/lite-signal 1.5.0 FIRST (createRoot dependency).
cd lite-query
npm run test:gc       # verify 152 pass including the zero-GC contract
npm run bench         # sanity-check bench numbers vs current TanStack
npm publish --access public
git tag v1.1.0 && git push --tags
```

## Demo serving

Both demos need a static HTTP server (file:// works for most things, but
cross-tab BroadcastChannel needs an origin):

```sh
npx serve .                                # from repo root
# or
python3 -m http.server 8080
```

- Core demo: `http://localhost:8080/demo/index.html` — open in two tabs for the cross-tab scene.
- Streaming demo: `http://localhost:8080/demo/stream-query-demo.html` — latest-mode 60fps oscilloscope, buffer-mode event feed, and a lifecycle scene (subscribe/unsubscribe shows abort-on-detach; Invalidate restarts and, across two tabs, reconnects both).

Both import maps currently point `@zakkster/lite-signal` at the vendored
`./demo/vendor/Signal.js` (1.5.0-alpha) so every module — lite-query,
lite-stream, lite-await, lite-store, lite-channel — shares ONE lite-signal
instance. Without a single instance you silently get two signal graphs and
reactivity won't bridge. Revert to `esm.sh@1.5.0` once 1.5.0 is published.

## Post-publish roadmap (1.x)

- Devtools panel (browser extension) — query inspector, message log, time-travel
- Focus / reconnect refetch triggers (injectable browser-event listeners)
- SSR hydration via `qc.hydrate(state)` / `qc.dehydrate()`
- Built-in `useInfiniteQuery`-style helper (currently a Cookbook recipe)
