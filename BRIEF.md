# BRIEF -- Q1 -- lite-query v1.1.1 -- turn the gates back on

Session Q1 of ROADMAP.md (section 5). Read ROADMAP.md sections 0-4 for the
verified findings ledger and ground truth; every fact referenced here carries
a reproduction there. Findings in scope: Q-01, Q-02, Q-03, Q-04, Q-05, Q-06,
Q-10, the landing half of Q-09, and the planner-pass ride-alongs Q-13, Q-14,
Q-15 (operator ruling 2026-08-31: fix in Q1, option (b) of the planner's R5 --
the llms.txt peer block is the same lie as Q-04 in the file the pipeline
reads, and its `^1.1.3` signal floor would hand a consumer a broken /stream).

---
package: "@zakkster/lite-query"
version_target: 1.1.1
status: planned
tests_min: 153
skip_max: 0
torture: "npm run torture -> 3x PASS, exit 0"
pool_baseline: "activeLinks === 0 after teardown"
findings: [Q-01, Q-02, Q-03, Q-04, Q-05, Q-06, Q-10, Q-09(land), Q-13, Q-14, Q-15]
blocks: [Q2, Q3, Q4]
---

## PURPOSE

At session start this tree could not run its own tests (Q-01: corrupt partial
node_modules, no .gitignore), and when it can, the default command silently
skips the zero-GC identity test (Q-02). npm currently serves a CHANGELOG whose
head says `## [1.1.0] -- Unreleased` for the version it lists as latest
(Q-03), and a README install section that says "Three peer dependencies" and
tells users to install @zakkster/lite-store and @zakkster/lite-channel --
neither of which is a peer of 1.1.0; package.json declares lite-signal
(required) plus lite-stream/lite-await (optional), and Query.js imports only
lite-signal (Q-04). Every fix is a one-to-five-line diff plus one commit of
already-staged work. Nothing else ships before this.

## PRE-VERIFIED FACTS (2026-08-31, this machine)

- `npm install` restores the env: lite-await 1.2.0, lite-stream 1.1.0,
  lite-signal 1.5.0. Suite green against all three.
- `node --test test/*.test.js` -> 153 tests, 152 pass, 1 skipped
  (test/zero-gc.test.js:20, gated on --expose-gc).
- `node --expose-gc --test test/*.test.js` -> 153 pass, 0 fail, 0 skipped.
- `npm run torture` (staged wiring) -> 3x PASS, exit 0.
- Registry tarball CHANGELOG head verified to say Unreleased.
- npm tarball = 11 files; README links ./QuickStart.md and ./Cookbook.md,
  neither shipped (Q-05).
- git status: bench/torture/* staged; package.json modified (adds the four
  torture script entries); ROADMAP.md rewritten this session; BRIEF.md new.

## TASKS

1. Add `.gitignore`: `node_modules/`, `*.tgz`. (Q-10)
2. package.json `"test"`: `node --test test/*.test.js` ->
   `node --expose-gc --test test/*.test.js`. (Q-02)
3. package.json peer + dev floor: `"@zakkster/lite-signal": ">=1.5.0-alpha"`
   -> `"^1.5.0"` in BOTH peerDependencies and devDependencies. (Q-06)
4. package.json: add `"prepublishOnly": "npm test && npm run torture"`.
5. Keep the staged torture suite exactly as staged (bench/torture/* + the
   four torture* scripts); it is part of this session's commit. (Q-09 land)
6. CHANGELOG.md: stamp the 1.1.0 heading with its real publish date -- take
   it from `npm view @zakkster/lite-query time` (fall back to 2026-08-31
   only if unavailable) -- and add a `[1.1.1]` section listing: gate fix
   (--expose-gc in default test), peer floor stabilization, torture suite
   landed, README install-section correction, link fix, .gitignore,
   prepublishOnly. ASCII punctuation in new lines (`--`, `->`); do not
   re-sweep existing lines (that is Q2). (Q-03)
7. README `## Install`: remove the store/channel install line and the
   "Three peer dependencies" sentence. State: one required peer
   (@zakkster/lite-signal ^1.5.0), two optional peers pulled only by the
   /stream and /await subpaths. Keep the Ecosystem section cross-reference;
   stop calling store/channel peers anywhere. (Q-04)
8. README doc links (Q-05), decision A (recommended, decide and record in
   CHANGELOG): point ./QuickStart.md and ./Cookbook.md at absolute GitHub
   blob URLs (repo: PeshoVurtoleta/lite-query, branch main) so the npm page
   has no dead links; files[] unchanged; ship-in-tarball decision moves to
   Q2 after QuickStart is refreshed.
9. llms.txt `## Peer dependencies` block (lines ~107-112): rewrite to mirror
   package.json exactly -- `@zakkster/lite-signal ^1.5.0` required (the
   query/stream watchers use createRoot; the current `^1.1.3` line is a
   broken instruction), `@zakkster/lite-stream ^1.0.0` and
   `@zakkster/lite-await ^1.0.0` optional via their subpaths; lite-store /
   lite-channel removed from the block (they are ecosystem companions, not
   peers -- the companion line may stay, ASCII-punctuated). (Q-13)
10. llms.txt line 3: `152 deterministic tests` -> `153 deterministic tests`;
    the parenthetical breakdown (120 core + 18 await + 15 stream) already
    sums to 153 and stays. (Q-14)
11. README line 14: license badge target `LICENSE.txt` -> `LICENSE`. (Q-15)
12. Release 1.1.1 via the /release drill (version sync package.json +
    CHANGELOG, prepublish gate runs test + torture).

## HOT PATH

Untouched. Zero .js source changes -- the diff is package.json, CHANGELOG.md,
README.md, .gitignore, plus the staged bench/torture files. If any task
appears to need a source edit, STOP: it is a new finding for the ledger, not
a Q1 task.

## ASSERTIONS (falsifiable, each maps to a gate)

- A1: `npm test` -> 153 pass, 0 fail, 0 skipped. The literal string
  `skipped 1` no longer appears in default gate output.
- A2: `npm run torture` -> three PASS lines, exit 0.
- A3: Fresh-clone shape: from a clean checkout (or after `rm -rf
  node_modules`), `npm install && npm test && npm run torture` is green
  end-to-end.
- A4: `git status` clean after commit; `node_modules/` listed by
  `git check-ignore node_modules` .
- A5: `npm pack --dry-run` -> same 11 files, no test/, no bench/; no
  relative link in README points at a file absent from the tarball.
- A6: neither README nor llms.txt contains a line calling lite-store or
  lite-channel a peer; llms.txt's peer block states the ^1.5.0 signal floor
  and matches package.json name-for-name.
- A7: package.json contains no "-alpha" anywhere; peer floor "^1.5.0".
- A8: CHANGELOG head is `[1.1.1]` with today's date; the 1.1.0 entry is
  date-stamped; the word "Unreleased" appears nowhere.
- A9: post-publish (release step): `npm view @zakkster/lite-query version`
  -> 1.1.1, and the fetched tarball's CHANGELOG head is not Unreleased.
- A10: llms.txt line 3 says 153; `grep -c "152 deterministic" llms.txt` -> 0.
- A11: README line 14 badge targets `LICENSE`; `grep -c "LICENSE.txt"
  README.md` -> 0.

## NON-GOALS

No ASCII sweep of existing lines (Q2). No QuickStart rewrite (Q2). No new
exports (Q3). No test/torture.mjs law harness (Q4). No source-file edits.
No files[] changes.

## DONE WHEN

fresh-clone npm install && npm test && npm run torture is green with zero
skips; 1.1.1 published; the registry tarball tells the truth
