# BRIEF -- Q2 -- lite-query v1.1.2 -- reconcile every doc with the code

Session Q2 of ROADMAP.md (section 5). Q1 shipped as 1.1.1 (status: shipped,
artifact gates G16-G19 verified against the registry tarball). Findings in
scope: Q-08 (ASCII sweep), Q-11 (QuickStart refresh), Q-12 (planning-doc
reconcile), and the close of Q-05 (ship the guides). Plus the carryover
riders accumulated by Q1's pipeline, listed below -- each was verified during
Q1, none is invented.

---
package: "@zakkster/lite-query"
version_target: 1.1.2
status: planned
tests_min: 155        # 153 + the two guard suites; planner pins the exact number
skip_max: 0
torture: "npm run torture -> 3x PASS, exit 0"
logic_changes: 0      # the defining constraint of this session
findings: [Q-08, Q-11, Q-12, Q-05(close)]
depends_on: [Q1]
blocks: [Q3]
---

## PURPOSE

A docs release whose diff contains no logic. The 1.1.x surface is stable, so
this is the one cheap moment to: sweep ~1,400 ASCII-law violations out of the
shipped files (Q-08), bring QuickStart from 1.0 to 1.1 (Q-11), make SPEC.md
stop competing with ROADMAP.md (Q-12), ship the two guides the README points
at (closing Q-05 the way Q1's decision A deferred), and -- the part that
outlives the session -- add two drift guards to the default test run so this
ledger class can never silently regrow.

## TASKS

1. **ASCII sweep (Q-08).** Scope: every file in `files[]` (after task 4:
   Query.js, Query.d.ts, StreamQuery.js, StreamQuery.d.ts, Awaitable.js,
   Awaitable.d.ts, README.md, CHANGELOG.md, llms.txt, QuickStart.md,
   Cookbook.md) plus `test/*.js` and `bench/**/*.mjs`. NOT demo/ (demo-audit
   law governs it; vendored files are upstream copies).
   Known inventory from the Q-08 ledger row (planner re-verifies at session
   start): Query.js 790x U+2500 + 37 em-dash + 8 arrows + 1 U+2265 + 1
   i-diaeresis; Query.d.ts 433x U+2500 + 10 em; StreamQuery.js 63x U+2500;
   StreamQuery.d.ts / Awaitable.d.ts 2 em each; llms.txt 38 em + 13 arrows +
   7 U+00D7; README.md 41 em + 13 arrows + 14 U+00D7 + 1 (c)-sign + 3
   middle-dots + 3 en-dash + 1 left-arrow; CHANGELOG.md ~23 (1.1.0-era body).
   Mapping: U+2500 runs -> `-` runs of the same visual length or a plain
   `// ---` divider; em/en dash -> `--`/`-`; arrows -> `->`/`<-`; middle dot
   -> `*` or comma per context; (c)-sign -> `(c)`; i-diaeresis -> plain i
   ("naive"); U+2265 -> `>=`. In README/llms/docs prose, U+00D7 -> literal
   `x` (docs law); in .js source U+00D7/U+00B5 MAY stay (source-law
   exception) but none are known to exist there.
   Constraint: comment/prose punctuation only. In .js files every hunk is
   inside a comment; no code token, no string literal that code reads, no
   test expectation string changes meaning. CHANGELOG history is swept
   (punctuation only -- released-entry WORDING stays byte-identical
   otherwise).

2. **Drift guards, joining the default `npm test` (they are what makes the
   sweep permanent):**
   a. `test/ascii-guard.test.js` -- walks the task-1 scope list, asserts no
      byte outside printable ASCII + LF (with an explicit, commented
      allowlist mechanism for the source-law U+00D7/U+00B5 exception, empty
      today). The guard function takes a buffer so the test can prove the
      failure path with an inline fixture (control: a fixture containing one
      U+2014 must fail) without mutating repo files.
   b. `test/surface-guard.test.js` -- `import * as` each of the three entry
      points; assert every exported name appears in llms.txt AND in the
      matching .d.ts; reverse direction: every `@zakkster/lite-*` package
      name mentioned in llms.txt's peer block exists in package.json
      peerDependencies. Control: a fixture llms body missing one name must
      fail.
   Both suites are pure node:test + node:fs -- zero new dependencies.

3. **QuickStart.md refresh (Q-11).** Add a streaming taste (`streamQuery`
   latest mode, ~15 lines) and an await taste (`whenQuery` route-guard,
   ~10 lines), each runnable and consistent with Cookbook recipes 14 and 11
   respectively. Fix anything in the existing 1.0 content that 1.1 made
   false. Keep the file's short on-ramp voice; it is not a second README.

4. **Close Q-05: ship the guides.** `files[]` += `QuickStart.md`,
   `Cookbook.md` (both current after task 3). README doc links (currently
   absolute GitHub URLs from Q1's decision A) flip back to relative
   `./QuickStart.md` / `./Cookbook.md`. `npm pack --dry-run` -> 13 files.

5. **Planning-doc reconcile (Q-12).** SPEC.md: replace the "Post-publish
   roadmap (1.x)" section body with a two-line pointer to ROADMAP.md; add
   one header line marking SPEC.md as the historical 1.1.0
   release-candidate document. No other SPEC rewrites.

6. **Carryover riders from Q1's pipeline** (each line-scale, all verified):
   a. llms.txt line ~72: "One-connection-shared is 1.2." -- stale
      sequencing; this roadmap moved shared streams to 2.0 (Q8). Say 2.0.
      Grep README for any matching "1.2" claim and align if found.
   b. README line ~43: "(plus its peer deps in the lite ecosystem)" --
      post-Q1 there is ONE required peer; reword to match.
   c. README lines ~308/~321: lite-channel "powers" / "the engine behind"
      fetch dedup -- overstated coupling. Truth: sharedFetch needs only an
      `isLeader` oracle; lite-channel is the convenient source (lines
      ~300-301 already say so). Soften both to match.
   d. README line ~241: `demo/stream-query-demo.html` code-span points at a
      repo-only path; qualify "in the repo" or link the GitHub blob URL.
   e. `demo/VENDOR.md` (new, ~6 lines): the pinned upstream copies in
      demo/vendor/ (Signal.js, Watch.js, lite-await.js, lite-stream.js),
      what versions they correspond to, and the one-line check command
      (e.g. `grep -c createRoot demo/vendor/Signal.js`) to run before any
      peer-bump release. Not in files[].

7. **CHANGELOG [1.1.2]** entry: Added (guards, guides shipped, VENDOR note)
   / Changed (QuickStart 1.1 refresh, links relative, SPEC pointer, wording
   riders) / Fixed (ASCII sweep with the per-file counts, stale 1.2
   sequencing claim). Facts and counts only.

## HOT PATH

Zero logic changes anywhere -- the .js diffs are comment-bytes only, plus two
new test files. The reviewer's primary job this session: prove every .js hunk
in Query.js / StreamQuery.js / Awaitable.js touches only comments (no
token-stream change; e.g. compare `node --check` + a token-level diff or
careful hunk review), and that test/bench sweeps did not alter any string a
test asserts on.

## ASSERTIONS

- A1: ASCII guard green over the whole scope; its inline-fixture control
  fails (proven in the suite itself).
- A2: Surface guard green; its control fails likewise.
- A3: `npm test` -> tests_min met (planner pins exact), 0 fail, 0 skipped.
- A4: `npm run torture` -> 3x PASS, exit 0 (sweep touched bench comments
  only; behavior identical).
- A5: `npm pack --dry-run` -> 13 files; QuickStart.md + Cookbook.md present;
  no test/, bench/, demo/; every relative README link target present in the
  listing.
- A6: `grep -rP '[^\x00-\x7F]' <files[] scope> test/ bench/` -> zero hits
  (or only allowlisted U+00D7/U+00B5 in source, currently none expected).
- A7: `git diff <Q1-tag>..HEAD -- Query.js StreamQuery.js Awaitable.js`
  contains comment-only hunks; `node --check` passes on all three; the
  reviewer states this explicitly.
- A8: QuickStart contains a runnable streamQuery snippet and a runnable
  whenQuery snippet consistent with Cookbook 14/11.
- A9: SPEC.md's roadmap section is a pointer; no roadmap claim in SPEC
  contradicts ROADMAP.md.
- A10: llms.txt and README contain no claim that shared streams land in
  1.2.
- A11: CHANGELOG [1.1.2] head present; version stays 1.1.1 in package.json
  until the /release drill.

## NON-GOALS

No new exports and no VERSION const (Q3, findings Q-07/Q-16). No
test/torture.mjs law harness (Q4). No behavior change of any kind -- if a
sweep hunk wants to touch a code token, STOP and report. No demo HTML
content changes beyond the new VENDOR.md file. No README restructuring
beyond the named lines.

## DONE WHEN

docs, d.ts, llms.txt, README, SPEC, QuickStart and this ROADMAP agree with
the code and each other; both drift guards run in the default gate with
proven failing controls; the shipped-file scope is ASCII-clean; 1.1.2 ready
for the /release drill
