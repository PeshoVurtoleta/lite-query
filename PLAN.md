# PLAN.md -- Q4 -- @zakkster/lite-query v1.2.1 -- suite-law torture harness

OPERATOR: ratified 2026-09-02 as delivered by the planner, with three notes.
ON-1: PR-1 is RATIFIED -- no coder commit touches `version` in package.json or
`VERSION` in Query.js; the CHANGELOG `[1.2.1]` head lands at C7 while the
package stays 1.2.0 (Q3 precedent: 560ac74 landed `[1.2.0]` pre-drill; the
/release drill owns the stamp and its sites). ON-2: the C-detach
findings-clause variant outcome MUST appear in the coder's final report either
way -- "fires, controlled" or "cannot fire without a runtime edit, recorded in
INCONCLUSIVE.md + C3 message, carried to Q5". ON-3: G7's fresh clone goes
under the session scratchpad directory, not /tmp.

## RULINGS CARRIED (restated for the coder; every one binds)

- **OR-1** ZERO edits to `Query.js` / `StreamQuery.js` / `Awaitable.js`. Real retention found by phase H or a control = STOP + new finding ID, never an in-session fix.
- **OR-2** Seed assertions are not renumbered or rewritten. The gate expression and the `GATE leak=... | gc ... | ok` line move verbatim into one evaluator; format stays byte-compatible with the 1.2.0 CHANGELOG citation.
- **OR-3** Every gate call is written from the installed llms.txt read this session: lite-leak 1.10.0, lite-gc-profiler 1.16.0. `checkNoGc` accepts ONLY `maxMajor|maxMinor|maxPauseMs|maxTotalMs|maxAllocRate|maxArrayBuffersGrowth`; any other key throws `TypeError` naming the owning lane. Seed's `{maxMajor:0,maxPauseMs:4}` is valid as-is. Do not add `maxBytesPerOp` (checkOps lane) or `maxBytesPerCall` (checkAllocs lane).
- **No red commits.** Every "prove the gap first" run is a pre-implementation run recorded in the commit message, never a committed failing state.
- Unit count stays **181**; control fixtures live in `test/torture/`, outside `test/*.test.js`.
- `PLAN.md` / `BRIEF.md` / `ROADMAP.md` appear in NO coder commit.
- ASCII-only. Zero runtime deps. `node:test` only.
- **PR-1 (version stamp).** `test/version-sync.test.js` couples `package.json.version` to `Query.js`'s `VERSION` literal. Bumping to 1.2.1 in-session would require editing Query.js, which OR-1 forbids. Per OR-6 precedent (the CHANGELOG-head leg belongs to the /release drill), **Q4 commits no version bump**: package.json stays `1.2.0`, the CHANGELOG gains its `[1.2.1]` head (T6), and the operator's `/release 1.2.1` performs the two-token bump. Coder: do not touch `version` or `VERSION`.

---

## SPEC -- target architecture of `test/torture.mjs`

`test/torture.mjs` becomes THE single entry (`node --expose-gc test/torture.mjs`). One module-level tracker, one frozen rules object, one gate evaluator, three call sites.

```
module scope (unchanged from seed):
  RULES = { maxMajor: 0, maxPauseMs: 4 }        // frozen; checkNoGc lane only (OR-3)
  tracker = createLeakTracker(...)              // exactly ONE per process
    + createOwnerCascadeOrphanKernel()
    + createObserverOrphanKernel()
    + createAsyncRetentionKernel()
  // KEEP the seed's in-file comment verbatim: global timer/listener kernels are
  // deliberately omitted -- lite-query patches no global timer/listener surface
  // (its GC timers are injectable); register only patched surfaces.

evaluateGate({ live, findings, leaks, warns, summary, report }) -> { ok, line }
  // The seed's boolean and the seed's GATE string, moved not rewritten (OR-2).
  // ADDITIVE clause, fail-closed per profiler 1.16.0 three-state law:
  //   ok = report.ok && report.verdict === 'pass' && live === 0
  //        && leaks.length === 0 && findings.length === 0 && censusOk
  // verdict !== 'pass' prints, on stderr, the reason code + "see INCONCLUSIVE.md".
  // allowInconclusive is NEVER set anywhere in this file.
```

**Phase P (pool).** `spawnSync(process.execPath, [script], { stdio: 'pipe' })` for `bench/torture/query-soak.mjs`, `cache-fuzzer.mjs`, `shared-fetch-soak.mjs`, in that order. No extra execArgv (the bench README's contract: no `--expose-gc` needed). Child env = `process.env` with `QUERY_TORTURE_BREAK` **deleted** (env-bleed guard). Assert `status === 0` each. Child stdout is buffered and printed ONLY on failure (to stderr) or under `QUERY_TORTURE_VERBOSE=1` -- harness rule: failure messages built only on failure. `QUERY_TORTURE_VERBOSE=1` is how the coder harvests T5's ops/s numbers.

**Phase H (heap).** The seed's two loops, bodies unchanged: 4096-cycle lifecycle churn (`live`, `findings`, `warns`) and 200000 warm reads under `GcProfiler` + `checkNoGc(s, RULES)`. One profiler in flight at a time (v1.16.0 law).

**Phase H census (new, additive, non-flaky).** lite-leak 1.10.0 states `size()` is "live REGISTRATIONS -- not a reachability census; torture gates pair it with a WeakRef census". The seed's `live === 0` is satisfied largely by auto-untrack on owner cleanup, so it is weaker than it reads. Add: `WeakRef`s to the last **256** churn handles, settled with **8** cycles of `globalThis.gc()` + macrotask yield. Gate ONE-SIDED: fail iff `sampledLive === 256` (nothing collected at all -- a real retention signature). Partial survival is noise and is never a failure. Census result prints only under VERBOSE or on failure; it folds into `ok`.

**Phase C (controls).** Cold path. Runs iff `QUERY_TORTURE_BREAK` is set; values `1` (all) or `alloc` | `detach` | `fuzz` (one -- G3 requires each individually verified). In break mode phases P and H are **skipped**, so exactly one profiler is ever in flight and no P child can fail for the wrong reason. Bodies live in `test/torture/controls.mjs`, reached by `await import('./torture/controls.mjs')` inside the break branch only -- zero cost, zero bytes on the plain run. Controls reuse the module tracker and `RULES` and call the **same** `evaluateGate`; a control that trips a lookalike gate proves nothing.

**Phase print/fail discipline.** Phases run in order and all run (P failure does not skip H -- one run yields the complete picture); verdicts aggregate. On success stdout is exactly:

```
GATE leak=size 0/0 findings=0 warnings=0 | gc major=0 minor=<n> maxMs=<n.nn> | ok
ok
```

exit 0. On any failure: no `ok` line, one `FAIL <phase> <detail>` line per failed phase on **stderr**, `process.exitCode = 1`. In break mode stdout is `CONTROL <id> tripped: <clause>` per control plus `CONTROL SUMMARY tripped=<k>/<n>`, exit non-zero always.

---

## CONTROL DESIGNS (T2) -- one per gate clause minimum

### C-alloc -- twin of the H warm loop; must trip `checkNoGc(RULES)`
**Mechanism.** Same 200000-iteration shape, same `RULES`, same `evaluateGate`, one difference: a module-level `const keep = []` outside the loop and `keep.push({ i, pad: new Array(16).fill(i) })` per iteration. ~200k retained objects (~40 MB) forces old-space growth and major collections; the seed's loop retains nothing.
**Trips:** `report.ok === false`, violation `metric: 'maxMajor'`, `limit: 0`, `actual >= 1` (and typically `maxPauseMs` too).
**Expected:** exit non-zero; stdout `CONTROL alloc tripped: gc`; stderr carries the seed's existing `violation maxMajor limit=0 actual=<n>` line.
**If it does NOT trip:** stdout `CONTROL alloc DID-NOT-TRIP -- gate is decorative`, exit non-zero with that distinct text. G3 greps for `tripped:`, never for the exit code alone.

### C-detach -- skipped teardown; must trip lite-leak
**Mechanism.** 64 cycles of the H churn body with two deliberate omissions: `track()` is called **outside** any owner (so lite-leak's `onCleanup(untrack)` auto-untrack never registers) and the handles are pushed into a module-level `RETAIN` array instead of being `dispose()`d, with no `qc.clear()`. Held-value contract preserved: `NOOP_RELEASE` and the tag still close over nothing.
**Trips:** the `live === 0` clause -- `tracker.size() === 128` (64 query + 64 streamQuery) -- and, because the refs are strong, the census clause at `256/256`-equivalent for its sample.
**Expected:** exit non-zero; stdout `CONTROL detach tripped: leak`; stderr `FAIL H leak=size 128/0`.
**Coder verification step (not an assumption).** Attempt a second variant that trips the `findings.length === 0` clause via `createOwnerCascadeOrphanKernel`'s `audit()` (`reason: 'stale'|'diverged'|'truncated'`) using `{ audit: true }` handles whose owner tree breaks after track-time. Run it; if it cannot be made to fire without touching `Query.js`/`StreamQuery.js`, **do not fake it** -- record "the `findings.length === 0` clause has no control" as an open item in `INCONCLUSIVE.md` and in the C3 commit message, and carry it to Q5. Recording an uncontrolled clause is the honest outcome; inventing a finding is not.

### C-fuzz -- corrupted oracle for phase P's fuzzer
**Mechanism.** One env-gated site in `bench/torture/cache-fuzzer.mjs` (OR-1 protects only the three runtime files). At the knobs block add `const BREAK = process.env.QUERY_TORTURE_BREAK === "1" || process.env.QUERY_TORTURE_BREAK === "fuzz";` and change the snapshot line to `const broadcastsDuringFuzz = broadcasts + (BREAK ? 1 : 0);` -- one phantom re-broadcast, exactly the echo regression the oracle exists to catch. Phase C spawns the fuzzer as a child **with** the var set.
**Trips:** the echo-storm invariant, `broadcastsDuringFuzz !== localMutations`.
**Expected:** child exit 1, child stderr contains `FAIL: echo storm -- expected` and `(re-broadcast leak)`; harness stdout `CONTROL fuzz tripped: fuzz-oracle`.
**Plain run proven untouched:** `grep -n QUERY_TORTURE_BREAK bench/torture/cache-fuzzer.mjs` returns exactly **1** line; `node bench/torture/cache-fuzzer.mjs` with no env exits **0**; `node bench/torture/cache-fuzzer.mjs` output is byte-identical modulo timing to a pre-C3 run of the same `TORTURE_SEED`. Record both runs in the C3 commit message.

**Plain-run untouched proof for all three:** `QUERY_TORTURE_BREAK` appears in `test/torture.mjs` in exactly one `if` guard, in `test/torture/controls.mjs`, and in the one fuzzer line. `env | grep QUERY_TORTURE` empty, then `npm run torture` green.

---

## SCRIPTS DIFF (T3) -- exact `scripts` block after the change

```json
  "scripts": {
    "test": "node --expose-gc --test test/*.test.js",
    "prepublishOnly": "npm test && npm run torture",
    "bench": "node --expose-gc bench/bench.mjs",
    "torture": "npm run torture:law",
    "torture:law": "node --expose-gc test/torture.mjs",
    "torture:control": "QUERY_TORTURE_BREAK=1 node --expose-gc test/torture.mjs || echo control-failed-as-expected",
    "torture:soak": "node bench/torture/query-soak.mjs",
    "torture:fuzz": "node bench/torture/cache-fuzzer.mjs",
    "torture:shared": "node bench/torture/shared-fetch-soak.mjs",
    "torture:leak": "node --expose-gc test/torture.mjs"
  },
```

`torture` is the union entry because `torture:law` phase P already execs all three soaks -- chaining them again would double a ~15 s run. `torture:soak|fuzz|shared` stay as documented per-script escape hatches (bench README lines 19-23 keep telling the truth). `torture:leak` is retained as a back-compat alias for the Q3 CHANGELOG's cited command. `prepublishOnly` now runs the whole law via `torture`.

---

## T4 -- `INCONCLUSIVE.md` outline (repo doc; NOT in `files[]`)

```
# INCONCLUSIVE -- triage for lite-query's torture gates
## Scope           node source 'gc', checkNoGc lane, profiler 1.16.0 / leak 1.10.0
## The rule        Never resolve an inconclusive with allowInconclusive. That is
                   the escape hatch, not the fix (profiler llms.txt, Key semantics).
## Reason codes we can actually see on this lane
   not_observed    profiler never start()ed, or gc.observed===false (engaged via
                   sampleHeap/phase only). Fix the harness, not the threshold.
   no_rules        empty/all-undefined rules object. Vacuity is not a pass.
   partial_report  hard exit mid-measurement. Rerun.
   noise_floor     v1.16.0. Machine too noisy; see environment hygiene.
   non-finite metric -> inconclusive, never pass (GC-10).
   NOT applicable here: uasm_below_granularity, fingerprint_mismatch,
   source_mismatch, mixed_sources, invalid_baseline, bracket_inverted
   (browser / baseline / differential lanes we do not use).
## Rerun guidance      3 consecutive runs, same seed; 2-of-3 inconclusive = real.
## Environment hygiene --expose-gc present; no other node process on the box; no
                       profiler/debugger attached; check summary.gc.foreignForced
                       (non-zero = someone else collected inside our window, the
                       numbers are polluted); import the profiler exactly once.
## When to suspect the harness  two profilers in flight; a control that stopped
                       tripping; census all-live on a plain run.
## Known uncontrolled clauses   <the findings-clause outcome from C-detach>
## Escalation      any inconclusive that survives triage becomes a finding ID in
                   ROADMAP.md section 2. It is never gated away.
```

---

## T5 -- provenance format in `bench/torture/README.md`

Append, immediately after the existing "Measured 2026-08-31 ... ops/s" prose, one block. Every number from a real run the coder performs; no number is carried from this plan.

```
## Measured

| run | date | machine | node | figure |
|-----|------|---------|------|--------|
| query-soak.mjs      | 2026-08-31 | Apple Silicon / darwin 25.5 | <recorded> | ~51k ops/s |
| cache-fuzzer.mjs    | 2026-08-31 | Apple Silicon / darwin 25.5 | <recorded> | ~131k ops/s |
| shared-fetch-soak.mjs| 2026-08-31 | Apple Silicon / darwin 25.5 | <recorded> | ~31k ops/s |
| phase P (all three)  | <today>   | <uname -m> / darwin 25.6    | <node -v> | soak <n>k / fuzz <n>k / shared <n>k ops/s |
| phase H leak         | <today>   | <uname -m> / darwin 25.6    | <node -v> | 4096 cycles, size 0/0, findings 0, warnings 0, census <n>/256 live |
| phase H gc           | <today>   | <uname -m> / darwin 25.6    | <node -v> | 200000 reads, major 0, minor <n>, maxMs <n.nn> (rules maxMajor 0 / maxPauseMs 4) |

ops/s is context, not a gate. The gate is the invariant list; phase H's gate is
`GATE ... | ok` from `npm run torture:law`.
```
The 2026-08-31 rows keep their original date and machine string -- do not restamp history. If the node version of those rows is unknown, write `unrecorded`, never a guess.

---

## T6 -- CHANGELOG `[1.2.1]` skeleton (coder fills `<...>` from real runs)

```
## [1.2.1] -- <YYYY-MM-DD>

Harness only. No runtime change: Query.js, StreamQuery.js and Awaitable.js are
byte-identical to 1.2.0.

### Added
- `test/torture.mjs` is now the single suite-law entry (`npm run torture:law`).
  Phase P execs the three `bench/torture/` soaks as children and asserts exit 0
  on each; phase H keeps the 1.2.0 lifecycle-churn and warm-read gates unchanged;
  phase C adds controls. Prints the same `GATE leak=... | gc ... | ok` line, then
  `ok`. Exit 0/1.
- Phase C controls behind `QUERY_TORTURE_BREAK` (`1` = all, or `alloc`/`detach`/
  `fuzz`): an alloc-per-iteration twin of the warm loop, a skipped-detach
  teardown, and a one-phantom-broadcast corruption of the fuzzer's echo oracle.
  Each verified to exit non-zero individually. `npm run torture:control`.
  Closes Q-09: the 1.2.0 leak/gc gate had no control and could not fail.
- WeakRef census beside `tracker.size()` (lite-leak 1.10.0: size() counts
  registrations, not reachability). One-sided gate: fails only when nothing in
  the 256-handle sample is collected across 8 settle cycles.
- Explicit `verdict === 'pass'` requirement; an inconclusive verdict fails and
  points at INCONCLUSIVE.md. `allowInconclusive` is never set.
- `INCONCLUSIVE.md` -- triage for the third verdict. Repo doc, not shipped.
- `bench/torture/README.md`: measured numbers now carry date / machine / node.

### Changed
- `npm run torture` is the union entry (phase P runs the three soaks once).
  `prepublishOnly` runs the full law. `torture:leak` retained as an alias.
- ascii-guard walk extended to `test/**/*.mjs` and `INCONCLUSIVE.md` (no new
  tests; the existing walk test covers more files).
- llms.txt / README test counts corrected: <old> -> <recounted>. The <old>
  figure predated the four QA boundary tests (764c8eb).

### Gates
- `npm test` -> <n>/0/0. `npm run torture` -> ok, exit 0.
  Controls: alloc <exit>, detach <exit>, fuzz <exit>. `npm pack --dry-run` -> 13 files.
```

---

## T7 -- llms.txt / README recount step

1. `npm test 2>&1 | tail -20` on a clean tree; record `# tests` / `# pass` / `# fail` / `# skipped` verbatim.
2. Per-file counts: `node --expose-gc --test test/query.test.js` etc., one run each; record each number. Never derive a number by arithmetic from this plan.
3. Rewrite `llms.txt:3` so the headline **equals the sum of its own parenthetical** (Q-14's exact failure mode -- do not reproduce it). Also true up `llms.txt:135` ("120 deterministic tests using:") and `llms.txt:147` ("test/query.test.js -- 106 deterministic tests") from step 2.
4. Same fact appears in `README.md:346` and the pasted run output at `README.md:349-350` (`# tests 177` / `# pass 177`). Same commit, same measured numbers -- the README block must be a real paste from step 1, not a hand edit.
5. `grep -rn "177" --exclude-dir=node_modules .` returns no stale doc claim (ROADMAP/CHANGELOG history entries are historical record and stay).

---

## COMMIT LADDER

**C1 -- `test/torture.mjs`: phase P + H, gate evaluator extracted.**
Pre-run to record in the message: current `node --expose-gc test/torture.mjs` output (the 1.2.0 GATE line), so the restructure's byte-compatibility is provable against a captured baseline.
Prove: `node --expose-gc test/torture.mjs` -> exit 0, last stdout line exactly `ok`, preceding line matches `^GATE leak=size 0/0 findings=0 warnings=0 \| gc major=0 minor=[0-9]+ maxMs=[0-9]+\.[0-9]{2} \| ok$`; `npm test` -> 181/0/0.

**C2 -- ascii-guard walk covers `test/**/*.mjs`.**
Prove: `npm test` -> 181/0/0 (three ascii-guard tests, count unchanged -- the walk grows inside an existing test body); `node --test test/ascii-guard.test.js` green with `test/torture.mjs` in the walk list.

**C3 -- phase C controls.** `test/torture/controls.mjs` + the break branch + the one fuzzer line.
Prove: `QUERY_TORTURE_BREAK=alloc`, `=detach`, `=fuzz`, `=1` -- four runs, each exit non-zero, each stdout containing `tripped:` and none containing `DID-NOT-TRIP`; `node bench/torture/cache-fuzzer.mjs` (no env) exit 0; `npm test` -> 181/0/0; `node --expose-gc test/torture.mjs` (no env) -> `ok` exit 0.

**C4 -- `package.json` scripts block** (exact block above).
Prove: `npm run torture` exit 0 ending `ok`; `npm run torture:control` prints `control-failed-as-expected`; `npm run torture:soak|fuzz|shared` each exit 0; `npm pack --dry-run 2>&1 | grep "total files"` -> `13`.

**C5 -- `INCONCLUSIVE.md` + ascii-guard ROOT_DOCS.** Both in one commit (the guard `statSync`s its list and fails closed on a missing file -- splitting them would commit a red state).
Prove: `npm test` -> 181/0/0; `grep -c INCONCLUSIVE.md package.json` -> `0`; `npm pack --dry-run` -> 13 files, no `INCONCLUSIVE.md`; `LC_ALL=C grep -n '[^ -~]' INCONCLUSIVE.md` -> empty.

**C6 -- `bench/torture/README.md` provenance.** Numbers from `QUERY_TORTURE_VERBOSE=1 npm run torture` plus `node -v` / `uname -m`, pasted in the C6 message.
Prove: `npm test` -> 181/0/0 (README.md under bench/ is not ascii-walked -- `.mjs` only -- so verify manually with the `grep '[^ -~]'` above).

**C7 -- `CHANGELOG.md` `[1.2.1]` head**, placeholders replaced by C1-C6 recorded numbers.
Prove: `head -1 CHANGELOG.md` region shows `## [1.2.1]`; `grep -n '<' CHANGELOG.md` finds no unfilled placeholder; `npm test` -> 181/0/0 (version-sync stays green: package.json remains 1.2.0, CHANGELOG head is the /release drill's leg per PR-1).

**C8 -- llms.txt + README recount** (T7 steps 1-5).
Prove: `npm test` -> 181/0/0; headline equals its parenthetical sum; `node --test test/surface-guard.test.js` green.

**Post-ladder (operator):** `/release 1.2.1` performs the package.json + `Query.js VERSION` bump. Not a coder commit.

### Gates mapped
- **G1** `npm test` 181/0/0 -- asserted at C1,C2,C3,C4,C5,C6,C7,C8.
- **G2** `node --expose-gc test/torture.mjs` -> `ok` exit 0 -- C1, re-asserted C3, C4.
- **G3** every control non-zero, individually -- C3 (four runs, text-checked).
- **G4** `npm run torture` green; `prepublishOnly` runs it -- C4.
- **G5** `npm pack --dry-run` -> 13 files, `test/`+`bench/`+`INCONCLUSIVE.md` excluded -- C4, re-asserted C5.
- **G6** ASCII guard green over the widened walk -- C2, C5.
- **G7** fresh-clone drill incl. the law gate -- after C8: clone into the session scratchpad, `npm i && npm test && npm run torture && npm run torture:control`.
- **G8** `git status` clean after C8; `git log --stat` shows no `PLAN.md`/`BRIEF.md`/`ROADMAP.md` and no `Query.js`/`StreamQuery.js`/`Awaitable.js` in any of C1-C8 (`git diff 1.2.0..HEAD --name-only`).

---

## ASSERTIONS (falsifiable, with numbers)

1. **GC budget.** Phase H: `checkNoGc(summary, { maxMajor: 0, maxPauseMs: 4 })` -> `verdict === 'pass'`, `summary.gc.major === 0`, `summary.gc.maxMs <= 4.00` over 200000 warm reads. C-alloc, same rules, same evaluator -> `actual >= 1` on `maxMajor`.
2. **Retention.** `tracker.size()` returns to **0** over **4096** build/teardown cycles, `findings.length === 0`, `warns.length === 0`. C-detach -> `size() === 128` over 64 cycles.
3. **Census.** 256-handle WeakRef sample, 8 settle cycles: plain run `sampledLive < 256`; C-detach `sampledLive === 256`.
4. **Controls.** 4 break runs, each exit non-zero, `tripped:` present, `DID-NOT-TRIP` absent, `3/3` in the `=1` summary.
5. **Scope.** `npm test` = 181/0/0 at every ladder step; `npm pack --dry-run` = 13 files; `git diff 1.2.0..HEAD --name-only` contains none of the three runtime files.

## STOP

None blocking. Two items ruled rather than escalated: **PR-1** (the 1.2.1 stamp is deferred to `/release`, so OR-1 and `version-sync.test.js` do not collide) and the **C-detach findings-clause variant** (if it cannot fire without a runtime edit, it is recorded as an uncontrolled clause in INCONCLUSIVE.md and carried to Q5 -- never faked, never fixed in-session). If phase H or any control exposes real retention in `Query.js`/`StreamQuery.js`/`Awaitable.js`: halt the ladder, open a finding ID, report.
