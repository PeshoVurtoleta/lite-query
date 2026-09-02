# Vendored demo dependencies

The demos pin one shared copy of each peer under `demo/vendor/` so every module
resolves the SAME lite-signal instance (a split graph silently breaks
reactivity). Not shipped: `demo/` is never in package.json `files[]`.

| File | Upstream | Version |
| --- | --- | --- |
| `Signal.js` | `@zakkster/lite-signal` | 1.5.0 |
| `Watch.js` | `@zakkster/lite-signal` (watch companion) | 1.5.0 (inferred -- no version stamp of its own; copied alongside Signal.js) |
| `lite-await.js` | `@zakkster/lite-await` | 1.3.0 |
| `lite-stream.js` | `@zakkster/lite-stream` | 1.3.0 |

The `lite-await.js` / `lite-stream.js` copies are byte-copies of the published
1.3.0 registry tarballs (`npm pack @zakkster/lite-await@1.3.0` /
`@zakkster/lite-stream@1.3.0`, extracted `Await.js` / `Stream.js`) -- never a
sibling working tree, which is how drift ships.

Before any peer-bump release, confirm the pinned copies still carry the API the
demos and the core depend on:

```sh
grep -c createRoot demo/vendor/Signal.js            # must be > 0 (the watcher needs createRoot)
grep -c createAwaitScope demo/vendor/lite-await.js  # must be > 0 (the 1.3.0 scope factory)
grep -c pipeToSignal demo/vendor/lite-stream.js     # must be > 0 (streamQuery's pump)
```
