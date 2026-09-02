# BRIEF -- Q4 -- lite-query v1.2.1 -- the suite-law torture harness (extend the seed)

Session Q4 of ROADMAP.md (section 5, incl. the 2026-09-02 preamble
amendment). Q3 is SHIPPED: 1.2.0 published 2026-09-02 (operator commit
c339c12) and artifact-verified from the registry tarball (13 files,
`[1.2.0] -- 2026-09-02` head, `VERSION = "1.2.0"` string-equal to the
manifest). The start condition is satisfied; Q4 is live.

---
package: "@zakkster/lite-query"
version_target: 1.2.1
status: planned
tests_min: 181        # unit count unchanged; this session's floor is gates, not tests
skip_max: 0
gc_maxMajor: 0
gc_maxPauseMs: 4
leak_cycles: 4096
torture: "test/torture.mjs -> ok exit 0; EVERY phase-C control exits non-zero"
findings: [Q-09(close)]
depends_on: [Q3]
blocks: [Q5]
---

## PURPOSE

Close Q-09 for real. Q3's ratified deviation (`8fa6ad4`) seeded
test/torture.mjs -- lite-leak + lite-gc-profiler devDeps, 4096-cycle
lifecycle churn with the tracker returning to 0, 200K warm reads at
major=0 minor=0 -- but QA proved the seed's one honest gap: the leak/gc
gate has NO control. A gate that cannot fail is decorative. Q4 turns the
seed into the full law harness: phase structure, controls, triage doc,
provenance record.

## WHAT THE SEED ALREADY DELIVERS (do not rebuild)

- devDeps present: @zakkster/lite-leak, @zakkster/lite-gc-profiler
  (verify floors against their llms.txt -- read BOTH before writing any
  gate call; rule keys have changed across profiler versions and unknown
  keys throw).
- test/torture.mjs: lifecycle churn (~= brief phase H's leak half) +
  profiler-gated warm reads (~= phase H's gc half), kernels chosen and
  documented (global timer/listener kernels deliberately omitted --
  the seed's commit message records why; keep that reasoning in-file).
- `torture:leak` wired into the `npm run torture` chain (4 phases total
  with the three bench/torture scripts).

## TASKS (the delta)

T1. Restructure test/torture.mjs into the law shape without discarding
    seed bodies: phase P (exec the three bench/torture scripts, assert
    exit 0 each -- torture.mjs becomes THE single entry), phase H (the
    seed's two gates, kept byte-equivalent in behavior), phase C
    (controls). Prints exactly "ok", exit 0/1.
T2. Phase C controls behind a break switch (QUERY_TORTURE_BREAK=1), one
    per gate minimum: an alloc-per-iteration twin of the H loop; a
    skipped-detach teardown that must trip lite-leak; a corrupted fuzzer
    oracle for phase P's fuzz script. Each control proven to exit
    non-zero in a recorded run log. The plain run stays green.
T3. Scripts: `"torture:law": "node --expose-gc test/torture.mjs"`;
    `torture` becomes the union entry (law included); `prepublishOnly`
    (from Q1) now runs the whole thing. `torture:control` mirrors the
    lite-stream convention (`QUERY_TORTURE_BREAK=1 ... || echo
    control-failed-as-expected`).
T4. INCONCLUSIVE.md: the triage note per harness rules -- what to do
    when the profiler returns inconclusive (rerun guidance, environment
    hygiene, when to suspect the harness); never allowInconclusive.
T5. Phase H numbers recorded in bench/torture/README.md with provenance
    (date, machine class, node version) next to the existing ops/s
    numbers.
T6. CHANGELOG [1.2.1]: harness only, no runtime change. Facts + measured
    numbers.
T7. llms.txt true-up (docs only, found at Q3 artifact verification):
    line 3 still says "177 deterministic tests" but the shipped suite is
    181 -- the four QA boundary tests (764c8eb) landed after that line
    was written (stream split 20 -> 24). Recount every number in that
    line from a fresh `npm test` run, never from memory; the drift
    guards do not currently lock the test count, so this line lies
    silently.

## OPERATOR RULINGS

- OR-1: ZERO source changes to Query.js / StreamQuery.js / Awaitable.js.
  If phase H or a control finds real retention, that is a new finding ID
  and its own patch -- STOP and report; never fold a runtime fix into a
  harness session (the bake-stream M0 discipline).
- OR-2: do not renumber or rewrite the seed's assertions to make phases
  fit -- restructure around them; the seed's GATE line format
  (`GATE leak=... | gc ... | ok`) is already cited in Q3's CHANGELOG and
  must keep printing.
- OR-3: gate calls are written from the CURRENT llms.txt of lite-leak
  and lite-gc-profiler, read in-session -- never from memory.

## GATES

G1 `npm test` -> 181/0/0 (unchanged -- this session adds no unit tests
unless a control needs a fixture, which lives under test/torture/, not
in the counted suite). G2 `node --expose-gc test/torture.mjs` -> "ok"
exit 0. G3 QUERY_TORTURE_BREAK=1 -> non-zero, EVERY control individually
verified. G4 `npm run torture` green end-to-end; `prepublishOnly` runs
it. G5 `npm pack --dry-run` -> 13 files, test/ + bench/ + INCONCLUSIVE.md
excluded (add INCONCLUSIVE.md to no-ship scope; it is repo doc, not
tarball doc). G6 ASCII guard green. G7 fresh-clone drill incl. the law
gate. G8 `git status` clean post-commits.
