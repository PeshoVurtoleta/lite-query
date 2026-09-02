# BRIEF -- Q3 -- lite-query v1.2.0 -- the sibling refresh

Session Q3 of ROADMAP.md (section 5, amended 2026-09-02). Q2 shipped as
1.1.2 (status: shipped; artifact verified from the registry tarball: 13
files, `[1.1.2]` CHANGELOG head, version sync). Both upstream siblings
moved while Q2 was in flight and this brief is amended to their ACTUAL
published surfaces -- every fact below was re-verified by running on
2026-09-02, none is carried from memory.

---
package: "@zakkster/lite-query"
version_target: 1.2.0
status: planned
tests_min: 170        # 158 + ~15 new; planner pins the exact number
skip_max: 0
torture: "npm run torture -> 3x PASS, exit 0; controls fail"
findings: [Q-07, Q-16]
depends_on: [Q2]
blocks: [Q5]
---

## PURPOSE

Catch this package up to both siblings in one minor. lite-await 1.3.0
ships 18 named exports; `/await` re-exports 10 -- eight are `undefined`
from the subpath while sitting in the installed dependency (Q-07).
lite-stream 1.3.0 shipped the LS3 consumer-contract session, whose three
enrichments were designed off OUR code sites; StreamQuery still hand-rolls
all three. Re-export what exists, delete what upstream absorbed, and give
VERSION its site (Q-16).

## VERIFIED UPSTREAM FACTS (the session's inputs)

- lite-await 1.3.0 export block (`sed -n '/^export {/,/^};/p'
  ../LiteAwait/Await.js`): 18 named + VERSION. The eight missing here:
  `allSettledOf, withResolvers, tryFn, delay, withRetry, mapLimit,
  whenStatechart, createAwaitScope`.
- `../LiteAwait/decisions/0009`: fromPromise vocabulary verdict = REJECT
  (accepted 2026-09-01). Locked decision #4 closes; the docs mapping table
  is permanent.
- `../LiteAwait/decisions/0007`: createAwaitScope contract (owns-or-borrows
  controller, per-call link teardown). We re-export; upstream owns the
  documented contract.
- lite-stream 1.3.0 (`../LiteStream/CHANGELOG.md` head): pipeToSignal
  gained `mode: "buffer"` + required `maxBuffer` (fresh newest-last
  snapshot per set, drop-oldest), `droppedCount`/`overflowCount` getters
  ON THE STOP FN, `onValue(v)` tap (before each set, after transform),
  `onAbort(reason)` (when present, aborts route there and NEVER reach
  onError -- their decisions/0001). Un-optioned call shapes byte-identical
  to 1.1.0. Their s7 conformance tier freezes our exact current call shape
  and proves a rewrite-parity case (N=12/maxBuffer=4 -> droppedCount 8,
  newest-last).

## TASKS (full detail in the ROADMAP Q3 brief; this is the contract)

T1. Awaitable.js: re-export the eight, verbatim; 18-name parity. VERSION
    deliberately excluded (convention recorded both sides; surface guard
    excludes it BY NAME with a comment citing the decision).
T2. Floors matching surfaces used: peer + devDep lite-await `^1.3.0`;
    optional peer lite-stream `^1.3.0`.
T3. StreamQuery collapse, parity-first: write the semantics-recording
    tests FIRST (status transitions incl. first-frame "streaming",
    droppedCount ladder with overflow, restart reset, error paths,
    abort-is-not-an-error), watch them pass against CURRENT code, then:
    hand ring (119-137) -> mode/maxBuffer; status tap -> onValue (transform
    leaves the call); abort filter (139-142) -> onAbort; droppedCount()
    accessor BYTE-IDENTICAL semantics via terminal-path snapshots from the
    stop fn's getter. Same tests pass unchanged after. The diff touches
    startStream + droppedCount plumbing ONLY.
T4. Vendored demo copies -> 1.3.0 (lite-await.js, lite-stream.js);
    VENDOR.md rows + check commands; demo smoke re-run.
T5. Docs (README /await + llms.txt): the eight; withRetry-vs-query() and
    delay-vs-mock-clock boundary lines; createAwaitScope one-liner citing
    upstream 0007; fromPromise closure sentence (verdict REJECT, table is
    final); StreamQuery internals note (zero user-visible API change).
T6. VERSION const (Q-16): export from Query.js, re-export from
    StreamQuery.js + Awaitable.js; three-place sync test in the default
    run; retire the line-1 header stamps.
T7. Tests: 8 identity + VERSION-exclusion + withRetry/mapLimit/
    createAwaitScope integrations + the T3 parity suite. >= 170 total,
    0 skip.
T8. CHANGELOG `[1.2.0]`: the eight, both floors, the collapse with a
    before/after bench delta on the buffer-mode stream scenario, the 0009
    closure. Version stays 1.1.2 in package.json -- the /release drill
    owns the bump.

## OPERATOR RULINGS (pre-made, so the pipeline does not stall)

- **OR-1 (NON-GOAL flip).** Q3's original "no streamQuery changes" is
  deliberately flipped FOR THE COLLAPSE ONLY. Any streamQuery diff beyond
  startStream + droppedCount plumbing is a reviewer REJECT.
- **OR-2 (parity is the review artifact).** The T3 test suite must exist
  and pass in a commit BEFORE the collapse commit. Reviewer verifies the
  suite is untouched across the collapse diff.
- **OR-3 (vendored copies are copies).** demo/vendor files are byte-copies
  of the published 1.3.0 artifacts (fetch from the registry tarball, not
  from sibling working trees) -- vendoring a working tree is how drift
  ships.
- **OR-4 (no new surface).** createAwaitScope/withRetry/etc. are
  re-exports; zero wrapping, zero option-massaging. Boundary docs exist
  precisely to keep query() and withRetry apart.

## GATES (planner expands; these are the floor)

G1 `npm test` >= 170 pass, 0 fail, 0 skip. G2 torture 3x PASS + controls
fail. G3 surface guard green (and its mid-dev firing recorded). G4
identity: all 18 importable from the subpath, `assert.equal` to upstream
bindings; VERSION absent. G5 T3 parity suite green pre- AND post-collapse,
zero test edits between. G6 `npm ls` floors >= 1.3.0 both siblings. G7
`npm pack --dry-run` -> 13 files, unchanged set. G8 ASCII guard green.
G9 `git status` clean post-commit; fresh-clone install + test + torture
green.
