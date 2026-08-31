# PLAN.md -- Q1 -- lite-query v1.1.1 -- coder implementation plan

Produced by the planner subagent 2026-08-31 from BRIEF.md + ROADMAP.md;
operator rulings appended at the end (they amend the tasks -- read both).

---

## SPEC

v1.1.1 is a gates-and-truth patch: it changes **no runtime code**. The diff is
exactly six paths -- `.gitignore` (new), `package.json`, `CHANGELOG.md`,
`README.md`, `llms.txt` (per operator ruling OR-1), and the already-staged
`bench/torture/*` -- and its whole job is to make the default gate actually
exercise the package's identity claim (`--expose-gc` in `npm test`, Q-02), to
land the torture suite that was written but never committed (Q-09 landing
half), to stop the manifest from pointing below a released stable
(`>=1.5.0-alpha` -> `^1.5.0`, Q-06), and to make the published artifacts stop
lying: a CHANGELOG whose head says the shipped version is Unreleased (Q-03), a
README install section that names two non-peers as peers (Q-04), an llms.txt
peer block that repeats the same lie with a signal floor on which /stream
cannot run (Q-13), dead links (Q-05, Q-15), and a self-inconsistent test count
(Q-14). The root cause of the dead tree (Q-01) is closed structurally by
`.gitignore` (Q-10) plus `prepublishOnly`. If any task appears to require an
edit to `Query.js`, `StreamQuery.js`, `Awaitable.js` or any `.d.ts`, **STOP**
-- it is a new ledger finding, not a Q1 task.

**Hot path: untouched.** `git diff --name-only` at session end must contain
zero `.js` files outside `bench/torture/`.

---

## ATOMIC TASKS

Execute in order. Each task is independently revertible.

### T1 -- create `.gitignore` (Q-10, A4)

New file, exact contents (trailing newline):

```
node_modules/
*.tgz
```

Plus one DECISION the dry run surfaced: `npm install` regenerates
`package-lock.json`, which commit `b56eb` deliberately deleted -- so every
install dirties the tree and fails gate G11 (A4). Options:
  (a) add `package-lock.json` to `.gitignore` -- preserves the repo's
      historical no-lockfile stance (recommended default);
  (b) commit the lockfile -- makes G15's fresh-clone resolve reproducible,
      reversing the b56eb decision (operator call, say so in the CHANGELOG).
Record the choice in C1's commit body either way.

### T2 -- `package.json:scripts.test` (Q-02, A1)

Line 42, one-line replacement:

before: `    "test": "node --test test/*.test.js",`
after:  `    "test": "node --expose-gc --test test/*.test.js",`

`--expose-gc` must sit **before** `--test` (see RISKS R1).

### T3 -- `package.json:scripts.prepublishOnly` (A1, A2)

Insert directly after the `"test"` line:

```json
    "prepublishOnly": "npm test && npm run torture",
```

The four `torture*` entries already present in the working tree stay
byte-identical; do not rewrite or reorder them.

### T4 -- `package.json` peer + dev floor (Q-06, A7)

Two identical one-line replacements -- line 51 (peerDependencies) and line 65
(devDependencies):

before: `    "@zakkster/lite-signal": ">=1.5.0-alpha",`
after:  `    "@zakkster/lite-signal": "^1.5.0",`

Do not touch lite-await/lite-stream ranges (`^1.0.0`; await's bump is Q3). Do
not touch `"version": "1.1.0"` (the /release drill owns the bump). Run
`npm install` after this edit; confirm lite-signal resolves to 1.5.0; re-run
gates.

### T5 -- keep `bench/torture/*` exactly as staged (Q-09 land, A2)

Four files: README.md, query-soak.mjs, cache-fuzzer.mjs,
shared-fetch-soak.mjs. **No edits of any kind.** They ride this session's
commit as-is; tuning and the law harness are Q4.

### T6 -- `CHANGELOG.md` -- date-stamp 1.1.0 (Q-03, A8)

First: `npm view @zakkster/lite-query time --json`; take the `"1.1.0"` value,
truncate to `YYYY-MM-DD` = `<PUBDATE>`. If unavailable, use `2026-08-31` and
say which path was taken in the commit body. Never infer the date from git
log.

Line 8: `## [1.1.0] — Unreleased` -> `## [1.1.0] -- <PUBDATE>`
(the rewritten line uses ASCII `--`; the em dashes elsewhere in the 1.1.0 body
are Q-08 / Q2's sweep -- leave them).

### T7 -- `CHANGELOG.md` -- insert the `[1.1.1]` section (Q-03, A8)

Insert between the preamble and the stamped `## [1.1.0]` heading, ASCII
punctuation only. Exact text -- as amended by operator ruling OR-2 (two extra
Fixed bullets vs the planner draft):

```markdown
## [1.1.1] -- 2026-08-31

A gates-and-truth patch. No runtime code changed -- no `.js` source file is
touched by this release. The diff is scripts, metadata, docs, and the torture
suite that was written but never committed.

### Fixed

- **The default test gate now runs the zero-GC identity test.** `npm test`
  was `node --test test/*.test.js`, while `test/zero-gc.test.js` gates itself
  on `--expose-gc` -- so every run printed `skipped 1` and the package's
  headline claim went unexercised. The script is now
  `node --expose-gc --test test/*.test.js`: 153 pass, 0 fail, 0 skipped.
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
- **The `[1.1.0]` heading said "Unreleased"** in the tarball npm serves as
  `latest`. It is now stamped with its actual publish date.

### Changed

- **Peer floor stabilized.** `@zakkster/lite-signal` moves from
  `>=1.5.0-alpha` to `^1.5.0` in both `peerDependencies` and
  `devDependencies`. 1.5.0 stable is published; README and llms.txt already
  said `>= 1.5.0`.

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
- **`.gitignore`** (`node_modules/`, `*.tgz`). Its absence let a partial
  `node_modules` survive in the tree; every test file failed with
  `ERR_MODULE_NOT_FOUND` until `npm install`.
```

### T8 -- `README.md` lines 87-102 -- the Install section (Q-04, A6)

Replace the block from `## Install` through the paragraph ending
"...uses `createRoot`)." with:

```markdown
## Install

```sh
npm install @zakkster/lite-query @zakkster/lite-signal
```

One required peer dependency: `@zakkster/lite-signal` (`^1.5.0` -- the query and stream watchers use `createRoot`).

Two subpath entry points are **optional** and pull one extra peer each, only if you use them:

```sh
npm install @zakkster/lite-stream   # for @zakkster/lite-query/stream
npm install @zakkster/lite-await    # for @zakkster/lite-query/await
```

The core (`@zakkster/lite-query`) imports neither, so core-only installs see no extra requirement and no peer warnings. The rest of the `@zakkster/*` family composes with lite-query -- see [Ecosystem](#ecosystem) below -- but nothing else is required.
```

### T9 -- `README.md` line 149 -- absolute doc links (Q-05, A5)

before:
`For more, see [QuickStart.md](./QuickStart.md) and [Cookbook.md](./Cookbook.md).`
after:
`For more, see [QuickStart.md](https://github.com/PeshoVurtoleta/lite-query/blob/main/QuickStart.md) and [Cookbook.md](https://github.com/PeshoVurtoleta/lite-query/blob/main/Cookbook.md).`

`files[]` unchanged (NON-GOAL).

### T10 -- `README.md` line 14 -- license badge (Q-15, A11) [OR-3: KEEP]

before: `...](LICENSE.txt)`  after: `...](LICENSE)`

### T11 -- `llms.txt` peer block (Q-13, A6) [added by OR-1]

Replace the `## Peer dependencies` block (currently):

```
## Peer dependencies

@zakkster/lite-signal ^1.1.3 (the reactive primitive)
@zakkster/lite-store ^1.0.0 (used internally for benches/demo, not in public API)
@zakkster/lite-channel ^1.0.0 (cross-tab BC patterns)

Optional companion:
@zakkster/lite-watch-ex (watchOnce / watchUntil / watchEffect) — for composing query state with imperative flows
```

with (ASCII only):

```
## Peer dependencies

@zakkster/lite-signal ^1.5.0 (required -- the reactive primitive; the query/stream watchers use createRoot)

Optional, pulled only by their subpath:
@zakkster/lite-stream ^1.0.0 (./stream -> streamQuery)
@zakkster/lite-await ^1.0.0 (./await -> coordination primitives + query bridges)

Optional companion:
@zakkster/lite-watch-ex (watchOnce / watchUntil / watchEffect) -- for composing query state with imperative flows
```

### T12 -- `llms.txt` line 3 test count (Q-14, A10) [added by OR-1]

In line 3: `152 deterministic tests` -> `153 deterministic tests`. The
parenthetical `(120 core ... + 18 await + 15 stream)` stays -- it already
sums to 153.

### T13 -- review-only pass: every other "peer" claim in README (Q-04, A6)

Read; edit nothing unless a line literally calls lite-store/lite-channel a
peer or requirement. Planner verdicts, to be confirmed:

- line 12 (lite-signal peer badge): correct, leave.
- line 39 (leader oracle "wired from lite-channel"): wiring option, leave.
- line 43 ("plus its peer deps in the lite ecosystem"): vague, leave; Q2
  wording pass.
- line 241 (`demo/stream-query-demo.html` code span): not a link; leave;
  Q2 ledger candidate.
- lines 300-301 ("no hard dependency on it"): correct and load-bearing,
  leave.
- lines 308, 321 (lite-channel "powers" fetch dedup): overstated coupling,
  not a peer claim; leave; Q2 wording pass.

Post-condition: `grep -in "peer" README.md llms.txt` shows no line naming
store/channel.

---

## COMMIT PLAN

Three commits, in order. **Do not push. Do not publish. Do not bump
`"version"`** -- `/release 1.1.1` is the operator's final step and owns the
version sync.

**C1 -- gates and hygiene** (T1-T5): `.gitignore`, `package.json`,
`bench/torture/*`.
Message: `chore: turn the default gate on and land the torture suite` + body
per planner draft (findings Q-01, Q-02, Q-06, Q-09 landing, Q-10; note "No
runtime code changed").

**C2 -- doc truth** (T6-T12): `CHANGELOG.md`, `README.md`, `llms.txt`.
Message: `docs: make the published artifacts tell the truth` + body naming
Q-03, Q-04, Q-05, Q-13, Q-14, Q-15.

**C3 -- planning docs**: `ROADMAP.md`, `BRIEF.md`, `PLAN.md`.
Message: `docs: Q1 planning set -- enriched roadmap, session brief,
implementation plan`.

After C3, `git status --porcelain` prints nothing.

---

## GATES

From the package root.

Pre-commit (after T1-T13):

- G0 env: `npm install` exits 0; `npm ls @zakkster/lite-signal` -> 1.5.0.
- G1 (A1): `npm test` -> `# pass 153`, `# fail 0`, `# skipped 0`; and
  `npm test 2>&1 | grep -c "skipped 1"` -> 0. Do not accept `pass 153`
  alone (R1).
- G2 (A2): `npm run torture; echo exit=$?` -> 3x PASS, exit=0.
- G3 (A7): `grep -n -- "-alpha" package.json` -> nothing;
  `grep -c '"@zakkster/lite-signal": "\^1.5.0"' package.json` -> 2.
- G4/G5 (A6): `grep -in "peer" README.md llms.txt` -> no line names
  store/channel; llms.txt block matches package.json name-for-name.
- G6/G7 (A8): CHANGELOG head `## [1.1.1] -- 2026-08-31`;
  `grep -c Unreleased CHANGELOG.md` -> 0; 1.1.0 stamped with `<PUBDATE>`.
- G8 (A5): `npm pack --dry-run` -> 11 files, no test/, no bench/.
- G9 (A5/A11): `grep -o "](\./[^)]*)" README.md` -> nothing;
  `grep -c "LICENSE.txt" README.md` -> 0.
- G9b (A10): `grep -c "152 deterministic" llms.txt` -> 0.
- G10 hot path: `git diff --name-only; git diff --cached --name-only` -> no
  `.js` outside bench/torture/.

Post-commit:

- G11 (A4): `git status --porcelain` -> empty.
- G12 (A4): `git check-ignore -v node_modules` -> `.gitignore:1`.
- G13 (A4): `git ls-files | grep -c node_modules` -> 0.
- G14 (A2): `npm run torture` re-run -> 3x PASS, exit 0.
- G15 (A3, headline): clone the repo into the scratchpad, `npm install &&
  npm test && npm run torture` -> green end-to-end, 0 skipped. Q-01 exists
  because "152 passing" was a claim about a tree nobody could check out.

Post-publish (operator, after `/release 1.1.1`):

- G16 (A9): `npm view @zakkster/lite-query version` -> 1.1.1.
- G17 (A9): fetched tarball CHANGELOG head is `[1.1.1]`; no "Unreleased".
- G18 (A5): tarball = 11 entries, no test/, no bench/.
- G19 (A6): tarball README install block + llms.txt peer block clean of
  store/channel.

Verify the artifact, not the working tree (Q-03/Q-04's lesson).

---

## RISKS

- **R1 -- `--expose-gc` position.** The runner forwards execArgv to per-file
  child processes only for flags before `--test`; after it, they are runner
  args and the skip silently returns as a green run. G1's `grep -c
  "skipped 1" -> 0` is the control.
- **R2 -- `npm view ... time` needs network.** Offline fallback 2026-08-31,
  stated in the commit body. Never use the git tag date -- a tag is not a
  publish event.
- **R3 -- /release vs the inserted heading.** The coder leaves `"version":
  "1.1.0"`; if the release drill inserts its own `[1.1.1]` heading rather
  than validating T7's, the heading must be byte-identical
  (`## [1.1.1] -- 2026-08-31`) or two headings result. Check the drill
  before running.
- **R4 -- prepublishOnly timing.** Adds ~20-30s to publish (test + three 5s
  soaks). Does not run on `npm pack --dry-run` (no prepack defined).
- **R5 -- resolved by OR-1** (was: llms.txt peer block out of README-scoped
  brief). Now in scope as T11/T12.
- **R6 -- `test/*.test.js` glob** breaks on Windows cmd; pre-existing, out
  of scope, noted for any future Windows CI.
- **R7 -- A4 cleanliness.** ROADMAP.md/BRIEF.md/PLAN.md are dirty until C3;
  run G8 with `--dry-run` only (a real `.tgz` before T1 lands would show
  untracked).

---

## OPERATOR RULINGS (2026-08-31)

- **OR-1 (planner R5 + T12):** option (b), widened. The llms.txt peer block
  is Q-04's lie in the file the pipeline reads, and its `^1.1.3` floor is an
  actively broken instruction (no `createRoot` before 1.5.0) -- verified by
  reading the block and StreamQuery.js's import. Fix in Q1 as T11/T12.
  Ledger updated: findings **Q-13** (S2, llms.txt peer block), **Q-14** (S3,
  test count), recorded in ROADMAP.md section 2; BRIEF.md tasks 9-11 and
  assertions A6/A10/A11 amended to match.
- **OR-2 (CHANGELOG):** T7's text extended with the two llms.txt bullets and
  the LICENSE badge sentence, as shown above.
- **OR-3 (planner T10):** keep the LICENSE badge fix in Q1; recorded as
  ledger finding **Q-15** (S3). A5/A11 cover it.
- **Execution boundary:** planning is complete; coder -> reviewer -> qa ->
  /release run as their own steps on operator go-ahead. Publishing (step
  T-release, gates G16-G19) is operator-only.
- **OR-4 (post-coder amendments, 2026-08-31):** two plan bugs surfaced by the
  coder's literal gate run, both resolved in favor of keeping the gates
  strict:
    (1) T7's prescribed `[1.1.0]`-heading bullet itself quoted the word
        "Unreleased", making G7 (`grep -c Unreleased -> 0`) unsatisfiable.
        The bullet is reworded ("carried no release date ... claimed the
        shipped version was pending") so the strict grep stays meaningful:
        any future Unreleased heading fails loudly. A8/G7 now pass
        literally.
    (2) G9's `grep -o "](\./[^)]*)"` is broader than assertion A5: it also
        catches README line ~353's `[LICENSE](./LICENSE)`, which is valid
        (LICENSE ships in files[]). A5 is the assertion of record ("no
        relative link points at a file absent from the tarball"); G9 is
        read as: every relative-link target must appear in `npm pack
        --dry-run` output. The `./LICENSE` link stays.
    (3) The coder's report of the generic torture-harness mandate vs the
        binding Q4 NON-GOAL is correct as handled: this session's committed
        gate is `npm run torture` (bench/torture), and `test/torture.mjs`
        remains Q4's deliverable.
- **OR-5 (post-reviewer amendments, 2026-08-31):** reviewer APPROVED with two
  nits; both acted on before commit because each would ship a
  self-contradicting artifact in a truth release:
    (1) README Tests section said "152 deterministic tests" / `# pass 152` /
        "incl. one pre-existing skip" -- stale the moment T2 lands. Updated
        to 153 / `# skipped 0`; the touched line also goes ASCII per law.
    (2) CHANGELOG's `.gitignore` bullet parenthetical now lists all three
        ignored patterns including `package-lock.json` (the b56eb
        no-lockfile stance), instead of reading as complete at two.
- **OR-6 (post-qa amendment, 2026-08-31):** QA passed all 25 checks and
  observed two further stale test counts outside every assertion's scope:
  README line 3 tagline said "106 tests" (a 1.0-era number) and the facts
  table said "Tests | 152". Both -> 153 in a follow-up commit before
  /release, so 1.1.1 does not ship three different counts in one file.
  Ledger: recorded as part of Q-14's closure (same finding class, same
  fix).
