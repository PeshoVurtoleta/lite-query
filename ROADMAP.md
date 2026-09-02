# lite-query -- enriched roadmap (post-1.1.0)

Eight sessions for one package, anchored to a verified-findings ledger.
Supersedes the 1.1.0 integration roadmap (this file's previous content; in git
history at HEAD). Written 2026-08-31 against the tree as checked out and the
registry as published, in the structure of `../BLUEPRINT_ROADMAP.md`.

**Why it was rewritten.** The old roadmap was a design document for the 1.1.0
integration, and 1.1.0 shipped. Its status header described its own tail
incorrectly ("README/Cookbook/demo tail open" -- all three exist and are
committed), its "road ahead" section disagreed with `SPEC.md`'s post-publish
list, and the working tree it described could not run its own test suite. Every
claim below was **reproduced by running a command**, not inferred from reading.
The reproduction is quoted with each finding.

The package's state, honestly:

| Axis | State |
| --- | --- |
| Published | `@zakkster/lite-query@1.1.1` is npm latest (Q1 shipped 2026-08-31; artifact gates G16-G19 verified against the registry tarball) |
| Tests | 153 tests; 152 pass + **1 silently skipped** under `npm test`; 153/153 with `--expose-gc` (verified 2026-08-31) |
| Torture | 3 soak/fuzz scripts, all PASS exit 0 -- but **staged, uncommitted**, and not the suite-law harness |
| Docs | README/Cookbook/demo shipped and current for 1.1.0; QuickStart stale; install section factually wrong; npm tarball has dead links |
| Correctness | **No S1 findings.** Nothing verified corrupts user data. The ledger is gate-integrity and doc-truth debt |
| Dev env | Was broken at session start: corrupt partial `node_modules` (no `.gitignore`), `npm test` failed 5/5 files |

No S1 means this roadmap's shape differs from the blueprint's: there is no
emergency-fix session. The first session turns the gates back on and ships the
accumulated hygiene; features build on top of gates that provably run.

---

## 0. Ground truth (verified 2026-08-31; sibling rows re-verified 2026-09-02)

Registry state, checked with `npm view <pkg> version`:

| Package | Latest | Relevant fact |
| --- | --- | --- |
| `@zakkster/lite-query` | 1.1.2 | Q2 shipped (operator publish; registry `latest` = 1.1.2). Artifact verified 2026-09-02 from the registry tarball: 13 files, `[1.1.2] -- 2026-08-31` CHANGELOG head, manifest version sync |
| `@zakkster/lite-await` | 1.3.0 | fully upgraded (V1 -> 1.2.1, C1 -> 1.3.0): **18 named exports** + VERSION. `createAwaitScope` shipped -- triggering consumer was lite-room 1.1.0 (`decisions/0007`), NOT our Q5; that gate resolved externally. `decisions/0009` verdict: **REJECT** (locked decision #4 closes). Q3's re-export target is now EIGHT (Q-07) |
| `@zakkster/lite-stream` | 1.1.0 | published 2026-07-10; additive `toAsyncIterable` enrichments; `^1.0.0` peer admits it; suite green against it. Roadmap enriched 2026-09-01 (`../LiteStream/ROADMAP.md`, LS1..LS4): its LS-01 (dispose-mid-pump pulls forever; llms claim false since 1.0.0) verified NOT to bite us -- `streamStop` aborts before any teardown (`StreamQuery.js:156`); its LS3 v1.3.0 targets our hand-rolled ring (see parking ledger). UPDATE 2026-09-02: lite-stream is now **1.3.0** -- LS1..LS3 all shipped (their harness caught and fixed a real pump bug, LS-13, on the way); `pipeToSignal` gained `mode`/`maxBuffer`, `droppedCount`/`overflowCount` getters on the stop fn, an `onValue` tap, and `onAbort` (abort vocabulary pinned in their `decisions/0001`); their s7 conformance tier freezes OUR exact 1.1.x call shape and proves a rewrite-parity case. The collapse rider is folded into Q3. LS4 (1.4.0) stays GATED on Q8's spike; SPIKE INPUTS recorded in the Q8 brief |
| `@zakkster/lite-signal` | 1.5.0 | stable; the `>=1.5.0-alpha` peer floor is now pointing below a released stable (Q-06) |
| `@zakkster/lite-store` | 1.2.0 | NOT a peer of lite-query 1.1.0, despite what the README says (Q-04) |
| `@zakkster/lite-channel` | 1.0.1 | same (Q-04) |
| `@zakkster/lite-leak` | 1.10.0 | required by suite law for the torture gate; not a devDep here (Q-09) |
| `@zakkster/lite-gc-profiler` | 1.16.0 | same (Q-09) |

Full suite verified green against the **newest** published peers (await 1.2.0,
stream 1.1.0, signal 1.5.0): 153 pass / 0 fail with `--expose-gc`. No
compatibility break from any peer bump since 1.1.0 shipped.

**The dev-env corruption, so it never recurs.** There is no `.gitignore`. The
initial commit included `node_modules/` and `package-lock.json`; commits
`b56ae8b` and `7dadc32` deleted them from tracking, and `76c4f79` removed one
last stray tracked file -- `node_modules/@zakkster/lite-await/CHANGELOG.md`.
That exact file was still on disk today as the *only* content of
`node_modules/@zakkster/`, shadowing nothing and satisfying no import:
`npm test` failed all 5 test files with `ERR_MODULE_NOT_FOUND:
@zakkster/lite-signal`. One `npm install` restored the environment. Without a
`.gitignore` the whole sequence is one `git add .` away from happening again.

Metadata checked and **clean** (the blueprint's cross-wired-repo lesson):
`homepage`/`repository`/`bugs`/`funding` all point at
`PeshoVurtoleta/lite-query`, matching `git remote -v`. Author/license law
satisfied (MIT, Zahary Shinikchiev).

---

## 1. Shared law (holds in every session)

1. **The gate must actually run.** A skipped test is not a passing test. Every
   identity claim (zero-GC warm path, pool-clean teardown) is exercised by the
   default `npm test` command, not by a flag someone must remember. Skip count
   in the default gate is 0, asserted per session.
2. **Every gate must be provably able to fail.** Each torture assertion ships
   with a deliberately-broken control variant that exits non-zero. A gate
   without a failing control is decorative.
3. **Re-export, don't reimplement.** lite-await and lite-stream are the single
   source of truth for their primitives. lite-query adds query-native bridges
   (`whenQuery`, `streamQuery`) and never forks upstream semantics. When the
   re-export list drifts behind upstream, that is scheduled work (Q-07), never
   a reason to copy code in.
4. **Zero allocation on the warm path.** Warm accessor reads
   (`data()/error()/status()` under an attached observer) and the latest-mode
   stream pump allocate nothing. Cold paths (fetch dispatch, entry creation)
   allocate by nature; the discipline is that every allocation is returned:
   after teardown the signal registry pool returns to baseline
   (`activeLinks === 0`) and the entry map drains. Bytes in a hot body, not
   instructions.
5. **Fail closed on every unverified state.** A persistence payload with the
   wrong schema version is dropped, not hydrated. A follower that cannot verify
   a leader self-fetches. null is not zero.
6. **Docs are part of the surface.** README, llms.txt, d.ts and CHANGELOG move
   in the same session as the code they describe. A published file that
   contradicts `package.json` (Q-04) is a bug with a severity, not a chore.
7. **ASCII-only shipped files** per suite law (`->`, `<=`, `x`; U+00D7 and
   U+00B5 are the only sanctioned exceptions, in source). The current tree
   violates this ~1,400 times (Q-08).
8. **Decisions are recorded before coding.** Any brief below marked with a
   DECISION block gets its choice written into the CHANGELOG entry (or a
   `decisions/` file once one exists) before the diff, with the rejected
   alternative and why.

---

## 2. Verified findings

Reproduced against this tree on 2026-08-31. Severity: **S1** = silent data
loss or corruption, **S2** = broken documented guarantee, **S3** =
hygiene/contract gap. There are no S1 findings; that is a measured statement,
not an assumption -- the suite, the three torture scripts, and the probes below
all ran clean.

| ID | Sev | Finding | Reproduction |
| --- | --- | --- | --- |
| **Q-01** | S2 | **The checked-out tree could not run its own gates.** `node_modules/` contained exactly one file (`@zakkster/lite-await/CHANGELOG.md`, a leftover of the tracked-then-deleted history in section 0); every test file failed with `ERR_MODULE_NOT_FOUND: @zakkster/lite-signal`. Every quality claim in the old roadmap ("152 passing") was unverifiable in this tree until `npm install`. | `npm test` -> 5/5 files fail; `ls node_modules/@zakkster/lite-await` -> `CHANGELOG.md` only. After `npm install`: 152 pass / 1 skip |
| **Q-02** | S2 | **The zero-GC identity test is silently skipped by the default gate.** `test/zero-gc.test.js:20` gates on `--expose-gc`; the `test` script is `node --test test/*.test.js` without it. Every `npm test` run prints `skipped 1` and the package's headline claim goes unexercised. Same failure class as the blueprint's AR-02: a green run over an untested hazard. Fix verified: `node --expose-gc --test test/*.test.js` -> 153 pass / 0 skip. | `npm test` -> `skipped 1`, the skip reason names `--expose-gc` |
| **Q-03** | S2 | **The published CHANGELOG says 1.1.0 is unreleased.** Head reads `## [1.1.0] -- Unreleased` in the tarball that npm serves as `latest`. Verified by downloading `dist.tarball` from the registry, not by reading the local file. | `curl <dist.tarball> \| tar -xzO package/CHANGELOG.md \| head` -> `Unreleased` |
| **Q-04** | S2 | **The README's install section documents a dependency set the package does not have.** It says "Three peer dependencies" and tells users to `npm install ... @zakkster/lite-store @zakkster/lite-channel`. `package.json` peers are `lite-signal` (required) + `lite-stream`/`lite-await` (optional); `Query.js` imports only `lite-signal`. Published README contradicts published manifest. | README `## Install` vs `package.json` `peerDependencies`; `grep "^import" Query.js` -> lite-signal only |
| **Q-05** | S3 | **Dead links in the npm tarball.** README links `./QuickStart.md` and `./Cookbook.md`; `files[]` ships neither (tarball = 11 files, verified from the registry tarball listing). The facts table also leans on "pagination is a Cookbook recipe" -- a claim whose evidence npm consumers cannot open. | `tar -tzf <tarball>` -> no QuickStart.md, no Cookbook.md; README line ~149 |
| **Q-06** | S3 | **Alpha peer floor in a stable release.** `"@zakkster/lite-signal": ">=1.5.0-alpha"` in `peerDependencies` and `devDependencies`, while 1.5.0 stable is published and README/llms.txt both say `>= 1.5.0`. Three documents, two vocabularies. | `package.json` lines 51, 65 |
| **Q-07** | S3 | **/await re-export drift: SEVEN of 17 upstream named exports missing** (corrected 2026-08-31 from six -- the original count was diffed against a truncated read of the upstream export block). `Awaitable.js` re-exports 10 (`whenSignal, whenTruthy, whenEquals, allOf, anyOf, raceOf, withTimeout, withAbort, fromPromise, TimeoutError`). lite-await 1.2.0 also ships `allSettledOf, withResolvers, tryFn, delay, withRetry, mapLimit` AND the 1.0.0-era `whenStatechart` -- none re-exported, none in any lite-query doc. Upstream additionally exports `VERSION`, which consumers deliberately do NOT re-export (it names lite-await's version; the convention is now stated in lite-await's llms.txt). The `^1.0.0` peer range already resolves to 1.2.0, so the gap is invisible until a user imports from the subpath and gets `undefined`. UPDATE 2026-09-02: upstream is now 1.3.0 with **18** named exports (`createAwaitScope` joined via its C1); the gap is EIGHT and Q3 is amended to it. | full export block `sed -n '/^export {/,/^};/p' ../LiteAwait/Await.js` vs `Awaitable.js` export block |
| **Q-08** | S3 | **~1,400 ASCII-law violations across shipped files.** Counted per file: `Query.js` 790x U+2500 box-drawing + 37 em-dashes + 8 arrows + 1 U+2265 + 1 `i`-diaeresis; `Query.d.ts` 433x U+2500 + 10 em; `StreamQuery.js` 63x U+2500; `llms.txt` 38 em + 13 arrows; `README.md` 41 em + 13 arrows + misc (`(c)` sign, middle dots, en-dashes, left arrow); `CHANGELOG.md` 18 em + 6 arrows; both other d.ts 2 em each. The 21 U+00D7 (`x`) occurrences in README/llms.txt sit inside the law's source exception but the docs law prescribes literal `x`. | `grep -oP '[^\x00-\x7F]' <file> \| sort \| uniq -c` per file |
| **Q-09** | S3 | **The torture harness deviates from suite law and is not landed.** Law: every change proven by `node --expose-gc test/torture.mjs` with `@zakkster/lite-leak` + `@zakkster/lite-gc-profiler`. Reality: three scripts under `bench/torture/` using a signal-registry pool snapshot as the leak oracle (deliberately no `--expose-gc`), neither law package in devDeps, no `test/torture.mjs`, and the whole suite plus its `package.json` script wiring is **staged but uncommitted**. The pool oracle is good -- and blind to JS-heap retention outside pooled nodes (closures, arrays, Maps held past teardown), which is exactly the blind-spot class the profiler exists to close. All three scripts PASS exit 0 today. | `git status` -> staged `bench/torture/*`, modified `package.json`; `npm run torture` -> 3x PASS, exit 0 |
| **Q-10** | S3 | **No `.gitignore`, no lockfile.** Root cause of Q-01 (section 0 tells the story). `package-lock.json` was deleted from tracking and never ignored; `node_modules` shows as `??`. | `ls -a \| grep gitignore` -> nothing |
| **Q-11** | S3 | **QuickStart.md predates 1.1.0.** One incidental match for `stream\|await` in the whole file; no streaming or coordination coverage. README points to it as the on-ramp. | `grep -cin "stream\|await" QuickStart.md` -> 1 |
| **Q-12** | S3 | **Three planning documents disagree.** Old ROADMAP header: "README/Cookbook/demo tail open" -- while README has both new API sections, Cookbook has recipes 11 + 14-16 (watchOnce fully migrated to `whenQuery`, 0 mentions remain), and `demo/stream-query-demo.html` exists, importmap-wired with vendored peers (vendored `Signal.js` verified to contain `createRoot`). `SPEC.md`'s post-publish list (devtools panel, focus/reconnect refetch, SSR hydrate/dehydrate, infinite helper) was never reconciled with the old roadmap's road-ahead (infiniteQuery, persistence, devtools feed, shared streams). This rewrite is the fix; Q2 keeps it fixed. | compare old ROADMAP status header vs `ls demo/`, `grep -n "^## " Cookbook.md`, `sed -n '88,93p' SPEC.md` |
| **Q-13** | S2 | **llms.txt tells an LLM consumer to install a signal version on which /stream cannot run.** Its `## Peer dependencies` block (lines ~107-112) says `@zakkster/lite-signal ^1.1.3` -- the real floor is `^1.5.0` (`createRoot` does not exist in 1.1.x; StreamQuery imports it) -- and lists `lite-store` / `lite-channel` as peers, which is Q-04's contradiction reproduced in the one file the pipeline reads to learn a sibling's API. Found by the Q1 planner pass, verified by reading the lines. | `sed -n '107,112p' llms.txt` vs `package.json` peers; `grep createRoot StreamQuery.js` |
| **Q-14** | S3 | **llms.txt's headline test count disagrees with its own parenthetical.** Line 3: "152 deterministic tests (120 core ... + 18 await + 15 stream)" -- the breakdown sums to 153; the headline counted passes under the gate that skips one (Q-02). | `sed -n '3p' llms.txt`; 120+18+15 = 153 |
| **Q-15** | S3 | **README license badge links `LICENSE.txt`; the file is `LICENSE`.** Dead in repo and tarball both. | `sed -n '14p' README.md`; `ls LICENSE.txt` -> no such file |
| **Q-16** | S3 | **No `VERSION` export -- the /release drill's expected sync site does not exist.** Never shipped one; version truth lives in two places (package.json + CHANGELOG head) plus informal header stamps in StreamQuery.js/Awaitable.js line 1 (and none in Query.js). Surfaced by the 1.1.1 release drill. Scheduled: Q3 ships `VERSION` from all three entry points (public surface -> minor). | `grep -n VERSION Query.js StreamQuery.js Awaitable.js` -> nothing |
Positive results worth pinning (they bound the ledger): the full suite is green
against the newest published peers; all three torture scripts pass; a missing
optional peer fails at import with Node's `ERR_MODULE_NOT_FOUND` naming the
exact package (probed by hiding `lite-stream` and importing `./StreamQuery.js`)
-- raw but actionable, acceptable as-is; the demo's vendored `Signal.js` is new
enough for `createRoot`, so the demo is not broken by the 1.5.0 requirement.

---

## 3. The torture suite

### What exists (staged, passing, uncommitted)

```
bench/torture/
  README.md              # design notes; pool-snapshot oracle, no --expose-gc needed
  query-soak.mjs         # single-client cache-lifecycle churn; pool -> baseline
  cache-fuzzer.mjs       # two-tab cross-tab coherence incl. streamQuery handles
  shared-fetch-soak.mjs  # leader/follower dedup under churn; liveness probe
```

All three: seeded PRNG (`TORTURE_SEED`), deterministic mock clock + mock
BroadcastChannel from `test/harness.js`, hard invariants (zero errors, entry
map drains, no dangling timers, `registry.stats()` back to baseline,
`activeLinks === 0`), exit 1 on any violation. `TORTURE_SECONDS` scales
duration (default 5). Measured 2026-08-31 on Apple Silicon / darwin 25.5, ops/s
for context only: soak ~51k, fuzzer ~131k, shared ~31k.

These land in Q1 exactly as staged. They are necessary, not sufficient.

### What the law adds (Q4)

`test/torture.mjs` -- the suite-law entry point, run as
`node --expose-gc test/torture.mjs`, printing exactly `ok` on success:

- **Phase P (pool)**: drive the three existing soaks as child phases (spawn or
  import), asserting their exit codes. The pool oracle stays the primary leak
  detector.
- **Phase H (heap)**: the blind-spot closer. `@zakkster/lite-gc-profiler`
  around a mixed warm-path loop (attached accessor reads + latest-mode stream
  pump + batched writes): gate `maxMajor: 0` and the profiler's
  retained-growth rule over `leak_cycles: 4096` build/teardown cycles.
  `@zakkster/lite-leak` on the queryClient: build up, tear down, assert no
  retained client/entry graph. This catches a closure or Map held past
  `disposeEntry` that the pool snapshot cannot see.
- **Phase C (controls)**: every gate above with a deliberately-broken twin
  that must exit non-zero -- an op loop that allocates per iteration; a
  teardown that skips one `detach`; a fuzzer oracle corrupted on purpose. If a
  control passes, the gate is decorative (law 2).

Harness rules carried from the blueprint: scratch allocated once outside
loops; failure messages built only on failure; one profiler measurement in
flight at a time; never resolve an unexpected `inconclusive` with an allow
flag -- triage it.

`test/` and `bench/` never enter `files[]`. `npm pack --dry-run` proves it
each release.

---

## 4. Session order

```
Q1 --> Q2 --> Q3 --+
 |                 +--> Q5 --> Q6 --> Q7 --> Q8
 +--> Q4 ----------+
```

- **Q1 blocks everything.** No session runs on a tree whose gates are dark.
- **Q2 (docs truth) before Q3** so the drift guard exists before the surface
  grows; Q3's six new re-exports are the guard's first real test.
- **Q4 (law torture) needs only Q1** (the staged suite must be committed
  first) and runs parallel to Q2/Q3 if wanted.
- **Feature sessions (Q5-Q8) wait for Q3 and Q4**: surface settled, gates at
  full strength. Q5/Q6/Q7 are mutually independent; the order below is
  leverage order (delete the last trailing comparison-table row, then the
  biggest UX win, then the ecosystem play). Q8 is the major and goes last.

Version ladder: Q1 = 1.1.1, Q2 = 1.1.2, Q3 = 1.2.0, Q4 = 1.2.1,
Q5 = 1.3.0, Q6 = 1.4.0, Q7 = 1.5.0, Q8 = 2.0.0. The old road-ahead numbered
infiniteQuery as 1.2; renumbered here because the /await refresh adds public
surface and semver makes that call, not sentiment.

---

## 5. The briefs

===============================================================================
# Q1 -- lite-query v1.1.1 -- turn the gates back on (ship first)
===============================================================================

```markdown
---
package: "@zakkster/lite-query"
version_target: 1.1.1
status: shipped (2026-08-31; commits e052fc0/ada4cc0/f5908e5/42b3bcc/646542d)
tests_min: 153
skip_max: 0
torture: "npm run torture -> 3x PASS, exit 0"
pool_baseline: "activeLinks === 0 after teardown"
findings: [Q-01, Q-02, Q-03, Q-04, Q-05, Q-06, Q-10, Q-09(land), Q-13, Q-14, Q-15]
blocks: [Q2, Q3, Q4]
---

# lite-query -- the gate that proves everything must itself run

PURPOSE
  At session start this tree could not run its own tests (Q-01), and when it
  can, the default command silently skips the one test that checks the
  package's identity claim (Q-02). Meanwhile npm serves a CHANGELOG that says
  the current release never happened (Q-03) and a README that tells users to
  install two packages the core does not depend on (Q-04). Every fix is a
  one-to-five-line diff plus one commit of already-staged work. Small enough
  to ship today; nothing else may ship before it.

TASKS
  - Add `.gitignore`: `node_modules/`, `*.tgz`. (Q-10; root cause of Q-01.)
  - Test script gains the flag its own suite checks for:
    `"test": "node --expose-gc --test test/*.test.js"`. Pre-verified:
    153 pass / 0 fail / 0 skipped. (Q-02)
  - Commit the staged torture suite exactly as staged: `bench/torture/*` and
    the four `torture*` script entries in package.json. Run it once more
    post-commit. (Q-09, landing half)
  - CHANGELOG: stamp `## [1.1.0] -- 2026-08-31` (the date it is being made
    true on the registry is gone; use the actual publish date if recoverable
    from `npm view @zakkster/lite-query time`, else today's). Add `[1.1.1]`
    with this session's entries. (Q-03)
  - Peer floor: `">=1.5.0-alpha"` -> `"^1.5.0"` in peerDependencies AND
    devDependencies. Aligns package.json with what README and llms.txt
    already say. (Q-06)
  - README install section: delete the store/channel install line and the
    "Three peer dependencies" sentence; state the truth -- one required peer
    (lite-signal ^1.5.0), two optional peers behind subpaths. Keep the
    Ecosystem cross-sell, just stop calling them peers. (Q-04)
  - DECISION (record in CHANGELOG): dead tarball links (Q-05).
      A. Point both README links at absolute GitHub URLs now; decide
         ship-in-files[] in Q2 after QuickStart is refreshed.  [recommended:
         2-line diff, nothing stale ships]
      B. Add Cookbook.md to files[] now (it is current), GitHub URL for
         QuickStart until Q2.
    Either way: no stale file enters the tarball.
  - Add `"prepublishOnly": "npm test && npm run torture"` so the gate is
    structurally in front of every future publish.
  - Ride-alongs surfaced by the Q1 planner pass (same "published artifact
    lies" class, each a line-scale fix in a file already in this diff's
    blast radius; recorded as findings first):
      * llms.txt peer block (Q-13): rewrite to mirror package.json --
        signal `^1.5.0` required, stream/await `^1.0.0` optional via their
        subpaths; store/channel are not peers. ASCII in rewritten lines.
      * llms.txt headline count (Q-14): `152` -> `153 deterministic tests`;
        the parenthetical already sums to 153.
      * README license badge (Q-15): `LICENSE.txt` -> `LICENSE`.
  - `/release 1.1.1`.

HOT PATH
  Untouched. This session contains zero runtime-code changes -- package.json,
  CHANGELOG, README, .gitignore, and one git commit of staged files. The diff
  proves it: no `.js` source file changes.

ASSERTIONS
  - `npm test` -> 153 pass, 0 fail, **0 skipped** -- the skip line is gone
    from the default gate's output.
  - `npm run torture` -> 3x PASS, exit 0, from a fresh `npm install` on a
    clean clone (proves Q-01 cannot recur silently: CI-shaped command works
    from scratch).
  - `git status` clean after commit; `node_modules/` ignored.
  - `npm pack --dry-run`: 11 files (or 12 if decision B), no `test/`, no
    `bench/`; every relative link in the packed README resolves.
  - Registry after publish: `npm view @zakkster/lite-query version` -> 1.1.1;
    fetched tarball CHANGELOG head no longer says Unreleased.
  - README **and llms.txt** name exactly the peers package.json declares;
    `grep -n "lite-store\|lite-channel" llms.txt README.md` shows no line
    calling either a peer, and llms.txt states the `^1.5.0` signal floor.
  - llms.txt says 153 tests; no README/llms link (badge included) points at
    a file that does not exist in its context.

NON-GOALS
  No ASCII sweep (Q2 -- it is mechanical and belongs in a diff that contains
  nothing else). No QuickStart rewrite (Q2). No new exports (Q3). No
  test/torture.mjs law harness (Q4). No source-file edits at all.

DONE WHEN
  fresh-clone npm install && npm test && npm run torture is green with zero
  skips; 1.1.1 published; the registry tarball tells the truth
```

===============================================================================
# Q2 -- lite-query v1.1.2 -- reconcile every doc with the code
===============================================================================

```markdown
---
package: "@zakkster/lite-query"
version_target: 1.1.2
status: shipped
tests_min: 153
skip_max: 0
torture: "3x PASS + controls fail"
findings: [Q-08, Q-11, Q-12, Q-05(close)]
depends_on: [Q1]
blocks: [Q3]
---

# lite-query -- a docs release whose diff contains no logic

PURPOSE
  The blueprint's R4 lesson: rewrite docs only after the surface stops moving
  -- and the 1.1.0 surface has stopped. QuickStart still teaches 1.0 (Q-11),
  ~1,400 characters violate the ASCII law (Q-08), and until this session
  there is no guard that stops llms.txt from drifting behind the real export
  surface again. Everything here must be provably behavior-neutral.

TASKS
  - ASCII sweep (Q-08), mechanical and complete: U+2500 dividers -> `---`
    runs; em/en dashes -> `--`/`-`; arrows -> `->`/`<-`; middle dots -> `*`
    or commas; `(c)` sign -> `(c)`; the stray i-diaeresis (likely "naive") ->
    ASCII. In README/llms.txt prose, U+00D7 -> literal `x` per docs law.
    Scope: every file in `files[]` plus test/bench sources. After the sweep,
    add the guard below so it stays swept.
  - QuickStart.md refreshed to 1.1: a streaming taste (`streamQuery` latest
    mode) and an await taste (`whenQuery` route guard), each a runnable
    snippet consistent with Cookbook recipes 11/14. (Q-11)
  - Close Q-05 per Q1's decision: ship QuickStart.md + Cookbook.md in
    `files[]` now that both are current, and flip README links back to
    relative. `npm pack --dry-run` re-verified.
  - Reconcile the planning docs (Q-12): SPEC.md's "Post-publish roadmap"
    section is replaced by a pointer to this ROADMAP (single source of
    truth). Its four items are accounted for here: infinite helper -> Q5,
    SSR hydrate/dehydrate -> Q6, devtools -> Q7 (feed) + lite-studio (panel,
    out of scope here), focus/reconnect refetch triggers -> parked (see
    section 6, Deferred).
  - Drift guards, added to the default test run (they are tests, not docs):
      * ASCII guard: walk `files[]` + `test/` + `bench/`, assert no byte
        outside the sanctioned set. This is the control that makes the sweep
        permanent.
      * Surface guard: `import * as core / stream / await` from the three
        entry points; assert every exported name appears in llms.txt and in
        the matching `.d.ts`. Two-directional where cheap (names in llms.txt
        that no entry point exports -> fail). The blueprint's R4 guard,
        generalized.
  - Verify demo vendor freshness while in the neighborhood: vendored
    `Signal.js` carries `createRoot` (verified today); leave a one-line
    VENDOR note in demo/ stating the pinned upstream versions and the check
    command, so the next peer bump has a checklist item instead of a
    surprise.

HOT PATH
  Untouched by definition: the only .js changes are comment characters and
  two new test files. `git diff --stat` on source files shows comment-only
  hunks; reviewer verifies no token outside comments/strings changed in
  Query.js / StreamQuery.js / Awaitable.js.

ASSERTIONS
  - ASCII guard green over every shipped + test/bench file; then flip one
    byte to U+2014 in a scratch copy and prove the guard fails (control).
  - Surface guard green; then add a fake name to llms.txt and prove it fails
    (control).
  - `npm test` 155+ pass (153 + guards), 0 skip; torture 3x PASS.
  - `npm pack --dry-run` -> 13 files including QuickStart.md + Cookbook.md;
    every relative README link resolves inside the tarball listing.
  - SPEC.md contains no roadmap claims that this file does not.
  - Published README/llms.txt byte-identical to repo copies at tag.

NON-GOALS
  No new exports (Q3). No torture-law harness (Q4). No behavior change of
  any kind -- if a fix wants logic, it gets a finding ID and its own session.

DONE WHEN
  docs, d.ts, llms.txt, README, SPEC and this ROADMAP agree with the code
  and with each other; both drift guards are in the default gate with proven
  failing controls; 1.1.2 published
```

===============================================================================
# Q3 -- lite-query v1.2.0 -- the sibling refresh (/await -> 1.3.0, /stream collapse -> 1.3.0)
===============================================================================

Amended 2026-09-02: both siblings shipped past the original brief while Q2
was in flight. lite-await is 1.3.0 (18 exports; 0009 verdict REJECT on
disk; createAwaitScope shipped via lite-room, not our Q5). lite-stream is
1.3.0 (the LS3 consumer-contract session built FOR this package). The
original NON-GOAL "no streamQuery changes" is deliberately flipped by
operator ruling: adopting the contract upstream shipped for us IS the
session; deferring it to Q5 would leave three hand-rolled mechanisms alive
for exactly one release cycle for no benefit. Scope stays pinned: the
streamQuery diff touches `startStream` + the droppedCount plumbing, nothing
else.

```markdown
---
package: "@zakkster/lite-query"
version_target: 1.2.0
status: planned
tests_min: 170
skip_max: 0
torture: "3x PASS + controls fail"
findings: [Q-07, Q-16]
depends_on: [Q2]
blocks: [Q5]
---

# lite-query -- the re-export thesis only holds if the re-export is current

PURPOSE
  Awaitable.js froze the lite-await surface at 1.0; upstream is at 1.3.0
  with 18 named exports, so today `import { withRetry } from
  "@zakkster/lite-query/await"` is `undefined` while `withRetry` sits in
  the installed dependency. And StreamQuery still hand-rolls the buffer
  window, status tap, and abort filter that lite-stream 1.3.0 now offers
  natively -- options designed off OUR code sites (their LS3, their
  decisions/0001). One minor catches this package up to both siblings:
  re-export what exists, delete what upstream absorbed.

TASKS
  - Re-export the EIGHT: `allSettledOf`, `withResolvers`, `tryFn`, `delay`,
    `withRetry`, `mapLimit`, `whenStatechart`, `createAwaitScope` --
    verbatim, alongside the existing ten. Full named-export parity with
    upstream (18), which the surface guard then holds automatically.
  - DECISION (record): `VERSION` is NOT re-exported -- upstream's VERSION
    names lite-await's version and would read as lite-query's from this
    subpath. Convention agreed with upstream (its llms.txt states it);
    the surface guard's parity check excludes VERSION by name with a
    comment citing this decision.
  - Peer + devDep floors, matching the surfaces actually used (the old
    roadmap's own rule): `"@zakkster/lite-await": "^1.3.0"` (createAwaitScope
    is 1.3.0); optional peer `"@zakkster/lite-stream": "^1.3.0"` (StreamQuery
    now calls mode/onValue/onAbort/droppedCount -- all 1.3.0).
  - **The StreamQuery collapse** (from the parking ledger; upstream made it
    a floor-bumped adoption, not a gamble -- their s7 tier freezes our
    1.1.x call shape AND proves a rewrite-parity case, N=12/maxBuffer=4 ->
    droppedCount 8, newest-last):
      * hand ring (`StreamQuery.js:119-137`) -> `mode: "buffer"` +
        `maxBuffer` on pipeToSignal; the O(n) `shift()` and per-frame
        `slice()` leave this package.
      * first-frame status tap smuggled through `transform` -> `onValue`
        (fires before each set, after transform); `transform` disappears
        from the call entirely (it carried no mapping of its own).
      * abort filter (`StreamQuery.js:139-142`) -> `onAbort` (per upstream
        decisions/0001, aborts route there and never reach onError).
      * `droppedCount()` accessor semantics BYTE-IDENTICAL to today,
        sourced from the stop fn's getter: snapshot into the entry counter
        on every terminal path (onDone/onError/onAbort) and on restart
        reset, so reads after completion and across restarts are unchanged.
      * Parity proof discipline: write the semantics-recording tests FIRST
        (status transitions, droppedCount ladder incl. overflow, restart
        reset, error paths, abort-is-not-an-error), watch them pass against
        the CURRENT code, then collapse, then watch the same tests pass
        unchanged. The test diff is the review artifact.
  - Vendored demo copies (VENDOR.md's own pre-peer-bump law): refresh
    `demo/vendor/lite-await.js` 1.0.0 -> 1.3.0 and
    `demo/vendor/lite-stream.js` 1.0.0 -> 1.3.0; update VENDOR.md rows +
    its API check commands; re-run the demo smoke.
  - Documentation, README /await section and llms.txt:
      * the two boundary lines: `withRetry` vs `query()` retry (query()
        owns cached-fetch retry -- per-entry, backoff, abort-aware;
        withRetry is for imperative one-shot flows outside the cache);
        `delay` vs the test-harness mock clock (delay is wall-clock).
      * `createAwaitScope` one-liner: scoped awaiters, one controller, one
        teardown -- semantics owned upstream (their decisions/0007); we
        re-export, we do not re-document the contract.
      * fromPromise vocabulary CLOSED: upstream verdict REJECT
        (../LiteAwait/decisions/0009, read 2026-09-02). Locked decision #4
        closes permanently; the mapping table (`resolved -> success`,
        `rejected -> error`) is the final answer -- say so in the /await
        docs and stop tracking it.
      * StreamQuery internals note: the window/tap/abort handling now live
        upstream; zero user-visible API change (the options surface of
        streamQuery() is untouched).
  - Tests (~15 new): re-export identity for all eight
    (`assert.equal(reexported, upstream)` against the installed package),
    VERSION-exclusion by name, one integration each for withRetry
    (rejecting fetcher, succeeds on 3rd try), mapLimit (concurrency
    ceiling observed via in-flight counter), createAwaitScope
    (scope abort settles a pre-bound whenSignal; teardown observed);
    whenStatechart identity only (its integration home is upstream). Plus
    the collapse parity suite (~5, written first, per above).
  - Awaitable.d.ts: the eight types re-exported. The Q2 surface guard
    enforces llms.txt + d.ts completeness automatically -- this session is
    its first real test.
  - `VERSION` const (Q-16): export from Query.js, re-export from
    StreamQuery.js and Awaitable.js; three-place sync check (package.json ==
    CHANGELOG head == VERSION) joins the default test run, and the /release
    drill's step-4 comparison finally has its site. Retire the informal
    line-1 header stamps in favor of the const (one version, one source).

HOT PATH
  Re-exports are module-scope bindings; zero runtime code added to any
  query path. The collapse REMOVES per-frame work from the stream path
  (O(n) shift + slice -> upstream's O(1) ring with one snapshot per set);
  bench before/after on the buffer-mode stream scenario and record the
  delta in the CHANGELOG line. The two bridges (whenQuery/whenAllQueries)
  are untouched.

ASSERTIONS
  - All 18 upstream named exports importable from
    `@zakkster/lite-query/await`; identity-equal to the upstream bindings;
    VERSION deliberately absent and the exclusion tested by name.
  - Collapse parity suite green BEFORE and AFTER the collapse with zero
    edits to the tests themselves; upstream's s7 rewrite-parity numbers
    reproduced locally (droppedCount 8 on N=12/maxBuffer=4).
  - Surface guard (Q2) green with the eight added -- and record that it
    FIRED mid-development before llms.txt caught up (the guard's control
    in the wild).
  - `npm ls` resolves lite-await >= 1.3.0 AND lite-stream >= 1.3.0;
    installing lite-await 1.2.x fails the peer range loudly.
  - `npm test` >= 170 pass, 0 skip; torture 3x PASS; controls fail.
  - CHANGELOG `[1.2.0]`: the eight, both floors, the collapse (with the
    bench delta), the 0009 closure.

NON-GOALS
  No new bridges (whenAnyQuery etc. wait for a real consumer). No wrapping
  of withRetry into query(). No streamQuery changes BEYOND the collapse
  (the diff is startStream + droppedCount plumbing; anything else is a
  reviewer REJECT). No idleTimeout / push-writer / share adoption -- that
  is Q8/LS4 territory and stays behind the spike.

DONE WHEN
  the /await subpath surface equals the installed lite-await surface plus
  the two bridges minus VERSION; StreamQuery's window/tap/filter live
  upstream with parity proven; vendored copies match the floors; 1.2.0
  published
```

===============================================================================
# Q4 -- lite-query v1.2.1 -- the suite-law torture harness
===============================================================================

Amended 2026-09-02, post-Q3 pipeline: Q3's coder landed a SEED of this
session's deliverable as a ratified deviation (`8fa6ad4`): test/torture.mjs
with lite-leak + lite-gc-profiler devDeps, phase 1 = 4096 query+streamQuery
lifecycle churn with `tracker.size() -> 0`, phase 2 = 200K warm accessor
reads at `major=0 minor=0`, appended to the `npm run torture` chain
(reviewer-audited: harness rules honored). Q4 therefore EXTENDS the seed
rather than starting from zero -- and inherits one verified gap as its
first task: QA confirmed 2026-09-02 that the leak/gc gate has NO
break-switch control (the ascii/surface guards carry in-suite controls;
test/torture.mjs does not). A gate that cannot fail is decorative; Q4's
"controls provably fail" assertion now names this gate explicitly.

```markdown
---
package: "@zakkster/lite-query"
version_target: 1.2.1
status: planned
gc_maxMajor: 0
gc_maxPauseMs: 4
alloc_bytes_per_op: 0    # warm accessor reads + latest-mode pump
leak_cycles: 4096
peers_dev: ["@zakkster/lite-leak", "@zakkster/lite-gc-profiler"]
findings: [Q-09]
depends_on: [Q1]
blocks: [Q5]
---

# lite-query -- close the oracle's blind spot, satisfy the law

PURPOSE
  The pool-snapshot oracle proves every pooled signal node and link comes
  back -- and cannot see a closure, array, or Map retained past disposeEntry,
  because those never touch the pool. Suite law names the tools for exactly
  that blind spot (lite-leak + lite-gc-profiler under --expose-gc) and this
  package does not use them. Build the law harness ON TOP of the existing
  scripts, not instead of them (section 3 spec).

TASKS
  - devDeps: `@zakkster/lite-leak` (1.10.0+), `@zakkster/lite-gc-profiler`
    (1.16.0+). Read BOTH packages' llms.txt for the current surface before
    writing a line -- do not write gate calls from memory (the blueprint's
    anti-hallucination rule; rule keys have changed across profiler
    versions and unknown keys throw).
  - `test/torture.mjs` printing exactly "ok" / exit 0, phases per section 3:
      P: run the three bench/torture scripts, assert exit 0 each.
      H: profiler-gated warm-path loop (maxMajor: 0, pause <= 4ms) +
         lite-leak build/teardown over 4096 cycles asserting no retained
         queryClient graph and no monotonic heap growth.
      C: controls -- an alloc-per-iteration twin loop, a skipped-detach
         teardown, a corrupted fuzzer oracle; each must exit non-zero.
  - `"torture:law": "node --expose-gc test/torture.mjs"`; `torture` becomes
    the union; prepublishOnly (Q1) now runs the whole thing.
  - INCONCLUSIVE.md triage note per harness rules (what to do when the
    profiler returns inconclusive; never allowInconclusive).
  - CHANGELOG [1.2.1]: harness only, no runtime change.

HOT PATH
  Zero source changes to Query.js / StreamQuery.js / Awaitable.js. If phase
  H finds retention, THAT fix is its own finding ID + session (or rides
  here only if it is provably a test-harness artifact) -- do not silently
  fold a runtime fix into a harness patch.

ASSERTIONS
  - `node --expose-gc test/torture.mjs` -> "ok", exit 0.
  - Every phase-C control exits non-zero, each proven in one run log.
  - Phase H numbers recorded in bench/torture/README.md with provenance
    (date, machine, node version) next to the existing ops/s numbers.
  - `npm pack --dry-run`: test/ + bench/ still excluded; tarball unchanged.
  - Suite + torture green from a fresh clone (the Q1 CI-shape assertion,
    now including the law gate).

NON-GOALS
  No runtime changes. No gate numbers invented beyond the frontmatter
  budget. No rewrite of the three existing scripts -- they are the base.

DONE WHEN
  the law command exists and passes; its controls provably fail; retention
  outside the pool is finally observable; 1.2.1 published
```

===============================================================================
# Q5 -- lite-query v1.3.0 -- infiniteQuery + prefetch
===============================================================================

```markdown
---
package: "@zakkster/lite-query"
version_target: 1.3.0
status: planned
tests_min: 195   # rebased 2026-09-02: written against a 158 base, suite is 181 post-Q3
skip_max: 0
torture: "law harness ok + pagination churn phase"
findings: []   # anchored to the README facts table's own honesty row
depends_on: [Q3, Q4]
blocks: [Q6]
---

# lite-query -- delete the last honest "trails TanStack" row

PURPOSE
  The facts table says it itself: "pagination is a Cookbook recipe rather
  than a built-in API". Recipe 4 hand-rolls page accumulation on every user.
  Promote it: cursor pagination first-class, plus qc.prefetch for
  route-loader warm-up (pairs with lite-router). This was the old roadmap's
  1.2; it is 1.3 here because the /await surface bump took 1.2.

TASKS
  - `infiniteQuery(qc, { key, fetcher, getNextCursor, enabled?, cacheTime?,
    staleTime?, retry? })` -> handle with `pages()`, `data()` (flat),
    `fetchNextPage()`, `hasNextPage()`, `status()`, `error()`, `refetch()`,
    plus the standard observer lifecycle. Design constraints, non-negotiable:
      * One cache entry per infinite query (the pages array IS the data),
        living in the same queryClient cache: getQueryData / invalidate /
        removeQueries / cross-tab behave uniformly, exactly like streamQuery
        did it (entry monomorphism precedent: uniform slots, no second
        hidden class).
      * fetchNextPage while a page is in flight: dedup, not double-fetch --
        reuse the entry.promise guard discipline.
      * invalidate refetches from page one and replaces the array on
        success (stale-while-revalidate over the whole list; no partial
        page mixing from two generations -- generation guard extends to
        the pages array).
      * Abort vocabulary: unmount/key-change abort mid-page fetch exactly
        as query() does; a late page from a dead generation is swallowed.
  - `qc.prefetch(key, fetcher, opts?)`: fetch into cache with zero
    observers, standard cacheTime GC afterwards; a later query() adopts the
    entry (no refetch if fresh). One decision to record: prefetch of an
    already-fresh entry is a no-op (recommended) vs forced.
  - Cross-tab: pages sync via the existing entry broadcast (it is one
    entry); sharedFetch leader dedup applies per page fetch. Assert in the
    fuzzer.
  - README: facts-table row flips; API section; llms.txt; d.ts. Cookbook
    recipe 4 becomes "use infiniteQuery; here is the manual version if you
    need custom accumulation". Recipe 5 gains the qc.prefetch form.
  - Tests ~15: accumulation order, hasNextPage false on null cursor, dedup
    on concurrent fetchNextPage, invalidate-refetch generation safety,
    abort on detach mid-page, prefetch adoption, cross-tab page sync.
  - Torture: pagination churn joins query-soak (mount/unmount infinite
    handles, fetchNextPage storms, invalidate races); pool baseline + law
    harness green.

HOT PATH
  pages() and data() reads on an attached observer allocate nothing (return
  the stored array/flat view; flatten on write, not on read). fetchNextPage
  is a cold path and may allocate its promise; the accumulation append must
  not re-copy the whole pages array per page beyond the one structural
  append the cache write requires.

ASSERTIONS
  - All new tests green; suite >= 175, 0 skip; law torture ok; controls fail.
  - Facts table no longer contains the trailing row; drift guards green.
  - Fuzzer proves: two tabs, one fetchNextPage storm, followers receive
    identical pages arrays, leader fetched each page once.
  - assertOps-style check: warm pages() read allocates 0 (phase H loop
    extended to cover it).

NON-GOALS
  No bidirectional (fetchPreviousPage) until a consumer exists -- record
  the deferral. No focus/reconnect triggers (parked, section 6). No
  persistence of pages (Q6 decides how infinite entries serialize).

DONE WHEN
  cursor pagination is first-class, cache-uniform, cross-tab-correct, and
  the comparison table needs no apology row; 1.3.0 published
```

===============================================================================
# Q6 -- lite-query v1.4.0 -- persistence: dehydrate / hydrate / adapter
===============================================================================

```markdown
---
package: "@zakkster/lite-query"
version_target: 1.4.0
status: planned
tests_min: 210   # rebased 2026-09-02 (181 post-Q3 base)
skip_max: 0
torture: "law harness ok + hydrate/dehydrate cycle phase"
findings: []   # SPEC.md post-publish item, reconciled by Q2
depends_on: [Q5]
blocks: [Q7]
---

# lite-query -- instant cold start, fail-closed

PURPOSE
  Highest UX leverage per line of code in the plan: serialize resolved
  entries, hydrate on boot before the first observer attaches, render
  instantly, revalidate per staleness rules. SPEC.md wanted "SSR hydration
  via qc.hydrate/dehydrate"; the old road-ahead wanted a storage adapter.
  They are one primitive and one adapter on top of it -- build it in that
  order.

TASKS
  - Primitive: `qc.dehydrate() -> state` (plain-JSON-able snapshot of
    success entries: key, data, dataUpdatedAt; never pending/error entries,
    never stream entries -- a connection is not data; record both
    exclusions) and `qc.hydrate(state)` (pre-observer seeding; entries
    arrive already-stale-aware via dataUpdatedAt so staleTime math is
    honest across reloads).
  - DECISION to record: hydrate after observers exist -- reject (fail
    closed, throw) vs merge. Recommended: reject; hydration is a boot-time
    operation and a late hydrate overwriting live data is the ABA bug of
    this domain.
  - Adapter: `persistQueryClient(qc, { save, load, version, throttle? })`
    -- subscribes to cache writes (the Q7 feed seam, kept internal for
    now), throttles serialization, stamps `{ version, state }`. On load:
    version mismatch -> drop everything, fetch fresh (fail closed; null is
    not zero). Storage-agnostic save/load thunks; lite-persist recipe in
    the Cookbook shows localStorage and IndexedDB.
  - Bake-backed persistence recipe (added 2026-08-31): a second Cookbook
    variant wires the thunks to `@zakkster/lite-bake-stream` preserve
    mode -- one cache entry per record, `PreserveReader.getJSON(i)` for
    lazy per-entry materialization, so boot becomes a sync zero-alloc
    container open and each entry parses on first observer attach instead
    of one monolithic JSON.parse over the whole dehydrated cache. Store is
    IndexedDB/CacheStorage Blobs (LBK1 is bytes; localStorage would pay a
    base64 tax). Floors (cross-repo; version citations refreshed
    2026-09-02 -- bake-stream's shipped versions DRIFTED above its
    chartered targets, so never cite its session numbers as semvers):
    the recipe requires the validated-preserve-reads floor and, if it
    touches RangeReader, the abort floor (MQ1, shipped content = 1.7.0).
    Both floors are published as ONE consumer-floor line in bake-stream's
    llms.txt by its MQ2 session -- cite THAT line verbatim at Q6 coding
    time; do not restate numbers from this file. Rationale unchanged:
    BS-06 means an unpatched PreserveReader can hand back short bytes
    from corrupted storage instead of refusing at open, and a persistence
    adapter must fail closed to fresh-fetch. The JSON-vs-bake crossover
    number is measured ONCE, by bake-stream's MQ2 session (targets 1.7.1;
    see ../LiteBakeStream/ROADMAP.md progress log), and this recipe cites
    it -- below the crossover the recipe says "use plain JSON", in those
    words. MQ2 also ships the cache-shaped conformance suite runnable
    FROM this repo (dependency-free both directions); Q6 lands our copy.
    Discipline unchanged: lite-query keeps zero runtime deps;
    bake lives entirely in the caller-wired adapter thunks (an
    optional-peer subpath only if a real consumer demands it, per the
    /stream and /await precedent).
  - infiniteQuery entries: pages arrays serialize like any success entry
    (decided here, tested here).
  - Cross-tab note: persistence + crossTab both restoring on boot must not
    fight -- hydrate seeds silently (no broadcast echo); document the
    boot order (hydrate, then attach, then crossTab settles via normal
    invalidation).
  - README + llms.txt + d.ts + Cookbook recipe; facts table gains its row
    (TanStack: plugin; SWR: manual).
  - Tests ~15: round-trip fidelity, staleness math across reload, version
    mismatch drops, pending/error/stream exclusion, late-hydrate rejection,
    throttle coalescing, infinite pages round-trip.
  - Torture: 4096-cycle dehydrate/hydrate/teardown loop under phase H --
    serialization is the classic accidental-retention factory.

HOT PATH
  Zero cost when unused: no persistence code on read/write paths beyond the
  adapter's subscription check (one null test, same discipline the Q7 feed
  will formalize). Dehydrate walks the cache -- cold by definition, says so
  in its doc comment.

ASSERTIONS
  - Round-trip: dehydrate -> new client -> hydrate -> identical
    getQueryData for every success key; staleness decisions identical to a
    client that never restarted (mock clock proves it).
  - Version bump drops 100% of stale-schema payloads; nothing hydrates.
  - Phase H: the cycle loop shows zero retained growth over 4096 cycles.
  - Suite >= 190, 0 skip; law torture ok; drift guards green.

NON-GOALS
  No storage implementations shipped (thunks only). No partial/selective
  hydration until a consumer asks. No offline mutation queue -- that is
  Q8's other half and it is not persistence, it is replay semantics.

DONE WHEN
  a reload renders from cache instantly, revalidates honestly, and a schema
  bump can never resurrect stale shapes; 1.4.0 published
```

===============================================================================
# Q7 -- lite-query v1.5.0 -- the devtools feed
===============================================================================

```markdown
---
package: "@zakkster/lite-query"
version_target: 1.5.0
status: planned
tests_min: 220   # rebased 2026-09-02 (181 post-Q3 base)
skip_max: 0
torture: "law harness ok; hook-installed vs not, both gated"
findings: []   # SPEC.md devtools item, feed half; panel is lite-studio's
depends_on: [Q6]
blocks: [Q8]
---

# lite-query -- observability with a zero-cost off switch

PURPOSE
  SPEC.md promised a devtools panel; the panel belongs to lite-studio. What
  lite-query owes the ecosystem is the feed: a push-mode stream of cache
  truth a panel can render. The entire design tension is the off switch --
  law 4 says the hot path buys nothing it does not use.

TASKS
  - `qc.inspect(hook)`: installs a single hook (one, not a list -- a panel
    multiplexes; record the rejection of hook arrays), returns an uninstall
    function. Events: entry lifecycle (create/attach/detach/gc/remove),
    status + staleness transitions, fetch dispatch/settle/abort (with
    reason), cross-tab send/receive, sharedFetch leader/follower events,
    stream start/value/done/error, mutation lifecycle, hydrate/persist
    save. Shape mirrors lite-studio's watchGraph vocabulary (read
    lite-studio's llms.txt before freezing names -- do not invent from
    memory).
  - Event objects: preallocated + reused per event type (document loudly
    that the hook must copy what it keeps -- the zero-GC feed contract), or
    allocated-only-when-hook-installed. DECISION recorded with the measured
    cost of each under phase H.
  - Every emit site is `if (qc._hook !== null)` -- one predictable branch,
    no closure, no arguments building unless installed.
  - README (short section + pointer to lite-studio), llms.txt, d.ts, one
    Cookbook recipe (console logger in 10 lines).
  - Tests ~12: every event class fires with the documented shape; uninstall
    stops emission; reused-object contract (two events, same identity,
    fields overwritten); no emission and no allocation when uninstalled.

HOT PATH
  The uninstalled branch is the product. Phase H runs the full warm loop
  twice -- hook absent and hook installed with a no-op copy -- and both
  must hold maxMajor: 0; the absent run must show zero added allocation
  vs the Q6 baseline (record both numbers in bench provenance).

ASSERTIONS
  - Feed covers every lifecycle the fuzzer can provoke (fuzzer gains an
    assertion mode: events observed form a consistent state machine --
    no detach without attach, no settle without dispatch).
  - Uninstalled overhead: unmeasurable vs baseline (numbers recorded).
  - Suite >= 200, 0 skip; law torture ok; controls fail; guards green.

NON-GOALS
  No UI, no formatting, no serialization of the feed (the panel's job). No
  ring buffer / history (panel's job). No second hook.

DONE WHEN
  a lite-studio cache panel can be built against the feed without touching
  lite-query internals, and an app that never calls inspect() pays nothing
  measurable; 1.5.0 published
```

===============================================================================
# Q8 -- lite-query v2.0.0 -- shared streams + offline mutations (the major)
===============================================================================

```markdown
---
package: "@zakkster/lite-query"
version_target: 2.0.0
status: planned
tests_min: 250   # rebased 2026-09-02 (181 post-Q3 base)
skip_max: 0
torture: "law harness ok + leader-failover stream soak"
findings: []   # the old roadmap's Future section, sequencing preserved
depends_on: [Q7]
---

# lite-query -- one connection per browser, and mutations that survive it

PURPOSE
  The strategic sequel the 1.1.0 streaming work was built to enable. Shared
  fetch collapsed N tabs to one request; a shared stream collapses N tabs
  to one SSE/websocket connection: the leader owns the iterator and
  broadcasts frames, followers project them into their local cache. Plus
  the offline mutation queue with replay-on-reconnect. Both are
  semantics-heavy -- which is exactly why they ride a major, alone, after
  the feed (Q7) exists to make their failure modes observable.

SCOPE DECISION (record before design)
  Ship as one 2.0 or split (2.0 shared streams, 2.1 offline mutations)?
  Recommended: decide at session start from a one-day spike on the
  leader-failover matrix below; if the matrix needs more than a session on
  its own, split. Do not let the queue ride an unfinished failover story.

DESIGN OBLIGATIONS (the brief's floor, not its ceiling)
  - Shared streams reuse the sharedFetch machinery: same isLeader oracle,
    same fallback-timer liveness discipline (a follower that stops
    receiving frames within a bound self-connects -- correctness never
    depends on election state; that sentence is already load-bearing in
    llms.txt for fetch and MUST stay true for streams).
  - Failover matrix, each cell a named test: leader closes gracefully /
    leader tab killed / leader hung (frames stop, channel alive) /
    follower promoted mid-buffer / two tabs racing promotion / stream
    completes during failover / stream errors during failover. Frame loss
    on failover is permitted and documented (at-most-once projection);
    frame duplication and frame reordering are NOT -- assert both.
  - Buffer-mode semantics across tabs: followers receive the same windowed
    view discipline (maxBuffer, droppedCount) as a local stream; a slow
    follower cannot grow the leader's memory (law 4 -- bound it, count
    drops, assert the bound in torture).
  - The spike also rules the lite-stream split: ../LiteStream/ROADMAP.md
    LS4 (v1.4.0, status: gated) holds a candidate menu -- per-value
    idleTimeout (the "leader hung, frames stop" detector), a push-writer
    surface for follower frame projection, share/refcount primitives.
    Decide which land in lite-stream vs in here vs nowhere; LS4's gate IS
    this ruling (their gate is our spike -- record the verdict in both
    roadmaps before designing).
  - Offline mutation queue: explicit opt-in per mutation (a default that
    queues silently is fail-open); durable via the Q6 persistence seam
    (version-stamped like everything else); replay preserves order per
    key, surfaces per-item results, and drops (never silently retries)
    items whose entries no longer exist. Replay is observable via the Q7
    feed.
  - Breaking-change budget: this is THE 2.0 -- collect every deferred
    breaking nit (if any accumulated in the ledger by then) into it.
    CHANGELOG breaking section + migration notes, per suite drill.

SPIKE INPUTS -- the lite-stream split (recorded 2026-09-02; the spike
DECIDES, this is its evidence file, gathered while both packages' 1.3.0
surfaces were fresh)

  Topology facts that decide most of it:
  1. Followers receive frames as lite-channel MESSAGES (push); no follower
     ever holds an iterator. lite-stream's pull-based surfaces cannot see
     follower traffic at all.
  2. A promoted follower STARTS A FRESH iterator (failover matrix above);
     nothing adopts a dead leader's iterator mid-flight.
  3. In-process observer sharing already exists here: entry dedup
     (`shouldStartStream` -- "don't double-pump a shared entry").
     Cross-tab sharing IS the channel broadcast.

  Candidate verdicts (provisional -- overturn only with a named matrix cell):
  - **push-writer -> lite-stream** (the LS4 core, strongest case). Without
    it, follower-side buffer mode re-hand-rolls the exact window Q3 just
    deleted -- a third copy of the semantics, and the obligation above
    ("followers receive the SAME windowed view discipline") is only
    guaranteed by same-code, which the upstream already factored for the
    enriched pipeToSignal. Shape sketch: `createSignalWriter(target,
    { mode, maxBuffer }) -> { push(v), end(), error(e), droppedCount }` --
    the enqueue half of pipeToSignal without the pull pump. Side profit:
    retires their `fromEventTarget` parking entry into a recipe.
    Pre-integration requirement: hand lite-stream the parity assertion
    (follower droppedCount == local-stream droppedCount for identical
    sequences) BEFORE LS4 codes, so their s7 pins it and Q8 consumes the
    pinned tests verbatim.
  - **idleTimeout -> not structurally required by Q8.** The follower
    watchdog ("frames stop, channel alive -> self-connect within a bound")
    runs on channel messages: `lastFrameAt` + one timer in THIS package,
    domain-correct, invisible to lite-stream (fact 1). Leader self-health
    is an optimization, not a correctness cell -- followers self-connecting
    around a hung leader IS the liveness law above. As a STANDALONE
    lite-stream feature (SSE stall detection; their overall-deadline
    `timeout` is the wrong tool users will reach for) it still needs its
    own named consumer -- parked on their side until one ships. Design note
    recorded for whenever it lands: never per-value clearTimeout/setTimeout
    (timer churn per frame breaks their zero-churn law); `lastValueAt` +
    one periodic check-and-rearm timer, amortized O(1).
  - **share/refcount -> nowhere visible.** In-process sharing = entry dedup
    (fact 3, exists); cross-tab = channel domain (fact 2 -- no shared
    in-process iterator ever exists); N keys over one socket = transport
    multiplexing, rejected by this brief's own NON-GOALS. Strike unless
    the spike produces a named cell that needs one-iterator ->
    N-local-consumers.

  Spike checklist (answer these, record the verdict in BOTH roadmaps, flip
  LS4 `status: gated` -> planned-or-closed):
  1. Confirm facts 1-3 against the actual Q8 design sketch.
  2. Rule each candidate: lite-stream / lite-query / nowhere.
  3. If push-writer survives: deliver the parity assertion + a frame-shape
     corpus to LS4 before it codes.
  4. LS4 shrinking to the push-writer alone is a perfectly good 1.4.0.

ASSERTIONS (floor)
  - Failover matrix green, every cell named.
  - N-tab soak (fuzzer extended): 5 simulated tabs, one upstream
    connection at all times (assert connection count at the mock source),
    kill the leader every k ops, frames neither duplicated nor reordered
    in any follower, pool baseline in every tab.
  - Offline queue: airplane-mode script (dispatch offline -> reload ->
    reconnect) replays in order, exactly once each, observable in feed.
  - Suite >= 230, 0 skip; law torture ok; controls fail; guards green.

NON-GOALS
  Whatever the scope decision defers (recorded, not implied). No transport
  ownership -- lite-query coordinates iterators and frames; sockets belong
  to the caller, exactly as in 1.1.0.

DONE WHEN
  five open tabs hold one upstream connection through leader churn without
  frame duplication; queued mutations survive a reload and replay exactly
  once; 2.0.0 published with the migration note
```

---

## 6. How to run it

In order: Q1, then Q2/Q4 in either order (Q2 first recommended), Q3, then
Q5 -> Q6 -> Q7 -> Q8. `status: planned -> shipped` in the brief's frontmatter
after each `/release`.

Per session, the suite pipeline: author `BRIEF.md` in the package from the
brief above, then planner -> coder -> reviewer -> qa (reviewer REJECTED goes
back to coder, not forward), then `/release <version>`. Every module change is
proven by the torture gate available at that session (Q1-Q3: the three
scripts; Q4 onward: the law harness). No gate output is a FAIL.

### If you only do a subset

1. **Q1 today.** The registry is serving an "Unreleased" changelog and an
   install command that names the wrong dependencies, and the default test
   gate skips the identity test. Every fix is trivial, pre-verified, and the
   session unblocks all others. Nothing else in this file has that ratio.
2. **Q2 before any surface work.** The drift guards are what keep Q3-Q8 from
   regenerating this ledger; the ASCII sweep is only cheap while no other
   diff is in flight.
3. **Q4 before any feature.** The pool oracle's blind spot is precisely
   where feature work (pages arrays, serialization, event objects) would
   leak.
4. **Q5 is the leverage pick** if only one feature ships this quarter: it
   deletes the last honest "trails" row in the public comparison table.
5. **Q8 never jumps the queue.** It is the only session whose failure modes
   are distributed-systems shaped; it goes last on purpose, after the feed
   exists to debug it.

### Deferred, with reasons (the parking ledger)

- **Focus / reconnect refetch triggers** (SPEC item): wants injectable
  browser-event listeners to keep the zero-DOM-dependency law; small API,
  real design decision. Candidate rider on Q5 or Q6 if a consumer shows up;
  parked until then, so it is not padding.
- **Devtools PANEL**: lite-studio's, consuming Q7's feed. Out of this
  package's scope by design.
- **whenAnyQuery / further bridges**: no consumer yet (Q3 NON-GOALS).
- **createAwaitScope re-export**: RESOLVED 2026-09-02. C1 shipped in
  lite-await 1.3.0 without waiting for our Q5 -- the triggering consumer
  was lite-room 1.1.0 (`awaitRoomSignal`, per their decisions/0007).
  Re-export moved into Q3's task list; this entry retires.
- **fetchPreviousPage / bidirectional infinite**: no consumer yet (Q5).
- **StreamQuery hand-ring collapse** (added 2026-09-01): MOVED into Q3
  2026-09-02 -- lite-stream 1.3.0 shipped the enriched pipeToSignal
  (mode/maxBuffer, droppedCount getters, onValue, onAbort per their
  decisions/0001) plus an s7 rewrite-parity proof of a StreamQuery-shaped
  rewrite. The collapse and the vendored-copy refresh are Q3 tasks now;
  see the Q3 brief for the parity discipline. Retired from parking.
- **fromPromise vocabulary normalization**: RESOLVED 2026-09-02 -- verdict
  REJECT on the record (../LiteAwait/decisions/0009, dated 2026-09-01).
  Locked decision #4 closed; Q3 states the closure in the /await docs.
  Retired.
- **lite-bake-stream composition recipes** (added 2026-08-31, surface read
  from its llms.txt at 1.0.0): (1) `streamQuery` + `RangeReader` -- a
  reactive, cached, abortable window over a remote multi-GB LBK1 container
  via HTTP Range + zone-map pruning, no full download; (2) `ingestStream`'s
  `onProgress` as a `streamQuery` source -- reactive ingest-progress UI in
  ~10 lines. Recipe-grade, zero core change; candidates to ride Q5 or Q6's
  docs pass. Bake-stream's side is scheduled (2026-08-31) as sessions MQ1
  and MQ2 in ../LiteBakeStream/ROADMAP.md. VERSION DRIFT NOTE 2026-09-02:
  bake-stream shipped past its chartered numbers (M5 -> 1.5.0,
  M6 -> 1.6.0, registry latest 1.6.1); MQ1 is content-complete as a
  frozen 1.7.0 diff awaiting operator publish, MQ2 targets 1.7.1. Recipe
  (1) requires MQ1 (abortable range reads: reader-level `{ signal }`,
  adapter `fetch(byteOffset, byteLength, signal?)`, `R_ABORTED`,
  no-partial-cache -- originally probed here: streamQuery's
  abort-on-detach law cannot compose with an uncancellable reader); the
  persistence recipe's conformance suite and crossover number are MQ2's.
  Cite floors from bake-stream's llms.txt consumer-floor line (an MQ2
  deliverable), never from session numbers. Explicitly NOT
  for the hot cache path: entries hold live
  values read by signals, and a serialize/deserialize toll between a
  signal read and its data would break law 4 -- bake belongs at
  boundaries (disk, network, boot).
- **Baked torture fixtures** (optional, Q4+): a deterministic LBK1 fixture
  for large-payload churn would keep JSON.parse allocation noise out of
  the profiler-gated phase H measurement, borrowing lite-bake-stream's
  8 GB-soak methodology. Only if phase H ever needs large payloads;
  otherwise padding.

### Locked decisions carried forward from the 1.1.0 roadmap

Still binding; the full rationale lives in this file's git history.

1. **Streaming status vocabulary**: `idle | pending | streaming | success |
   error`; `loading()` keys off `pending` only. `done/count/droppedCount`
   are accessors, not statuses.
2. **whenQuery rejects with `q.error()`** via the throwing-predicate path
   into `whenSignal` -- settlement semantics mirror upstream exactly.
3. **Unified cache, monomorphic entry**: stream slots are uniform on every
   entry; `entry.promise` is never aliased for the stream pump (the
   fetch-dedup guard reads it). The same discipline binds infiniteQuery
   (Q5) and any future entry kind.
4. **fromPromise re-exported unchanged**; vocabulary mapping is a docs
   table, not a wrapper. CLOSED 2026-09-02: upstream verdict REJECT on the
   record (../LiteAwait/decisions/0009) -- the table is the permanent
   answer; this decision is no longer tracked, only inherited.
5. **Streaming data does not cross tabs in 1.x** -- each tab owns its
   connection; only invalidation/removal broadcast. 2.0 (Q8) is where that
   changes, by design not by drift.
6. **Re-export, don't reimplement** -- the ecosystem thesis, now law 3.

### The habit this roadmap is built around

Every finding in section 2 has a command next to it because it was found by
running that command. The three sharpest lessons this tree teaches, kept in
front of the reviewer subagent:

- **Q-01**: a quality claim is a claim about a tree someone can check out.
  The suite was "152 passing" in a tree where `npm test` could not load a
  single file. From Q1 on, the fresh-clone assertion makes "passing" mean
  passing.
- **Q-02** is AR-02 wearing a nicer costume: nothing was miswritten, the
  test is good -- it just never runs where anyone looks. Coverage is not
  the same as exercise, and a skip counter nobody reads is a green light
  over a hole.
- **Q-04/Q-03**: the registry is a publication, not a mirror of intent.
  What npm serves today says "Unreleased" and names the wrong dependencies,
  and no local gate can see that -- only pulling the published artifact
  can. The release drill checks the artifact, not the working tree.

MIT (c) Zahary Shinikchiev
