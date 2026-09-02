# PLAN.md -- Q3 -- lite-query v1.2.0 -- coder implementation plan

From the Q3 planner pass (2026-09-02) + operator rulings resolving its four
STOP items. Contract: BRIEF.md (Q3). Package root:
/Users/zakkster/Work/Portfolio/LiteLibrariesSuite/LiteQuery.

## STOP-ITEM RESOLUTIONS (operator rulings, continuing BRIEF's OR numbering)

- **OR-5 (resolves STOP 1 + STOP 2 -- stale node_modules, not stale
  registry).** The planner correctly read node_modules (lite-await 1.2.0,
  lite-stream 1.1.0) as what is INSTALLED; the registry has 1.3.0 of both
  (operator-verified 2026-09-02: `npm view` latest = 1.3.0 / 1.3.0). The
  fix is sequencing, not descoping: the floors commit (C2) bumps peer +
  devDep for BOTH siblings to `^1.3.0` and runs `npm install` (no
  lockfile), then VERIFIES the installed surfaces before any dependent
  task: lite-await exposes 18 named + VERSION incl. `createAwaitScope`;
  lite-stream `pipeToSignal` accepts mode/maxBuffer/onValue/onAbort and
  its stop fn carries droppedCount/overflowCount getters. If either
  verification fails, STOP -- do not code against the BRIEF's claims.
  The planner's "do not bump lite-stream's floor while the code uses the
  1.1.0 surface" concern is honored by C-ordering: the floor bump and the
  collapse ship in the same session, and no commit between C2 and C4
  publishes anything (publish is the drill's, later).
- **OR-6 (resolves STOP 3 -- version-sync is two-place; the CHANGELOG leg
  belongs to the drill).** `VERSION` const = "1.1.2" this session, equal to
  package.json. The default-run sync test asserts EXACTLY
  `VERSION === require("./package.json").version` (string compare). The
  CHANGELOG-head leg is asserted by the /release drill (its steps 3-4
  already own it); Q2 precedent: the `[1.1.2]` entry landed while
  package.json said 1.1.1. A permanent test that is red between
  session-end and publish would be a gate that MUST fail -- as broken as
  one that cannot.
- **OR-7 (no red commits).** Every commit leaves `npm test` green. "The
  new identity tests prove the gap" is demonstrated by running them
  against pre-implementation code DURING development and recording the
  failure count in the implementing commit's message -- never by
  committing a red suite. Same for the G3 surface-guard firing: run the
  guard after the export edit and before the docs edit, capture the
  failure output, cite it in the commit message; the committed state is
  green.
- **OR-8 (resolves STOP 4 -- ADR attestation).** The planner could not
  read outside the package. Operator attests, verified 2026-09-02 by
  reading the files: `../LiteAwait/decisions/0009` verdict REJECT
  (accepted, dated 2026-09-01); `../LiteAwait/decisions/0007` accepted
  (createAwaitScope, consumer lite-room 1.1.0); `../LiteStream/decisions/
  0001` accepted (abort vocabulary: from-side abort -> onAbort when
  present, NEVER reaching onError). The coder may cite these as facts.

## SPEC

Hot body: nothing. T1/T2/T5/T8 are module-scope bindings, manifests, and
text; `query()`, `mutation()`, `whenQuery`, `whenAllQueries` keep
byte-identical bodies. The ONLY behavioral diff in the session is the
StreamQuery collapse (C4), licensed by OR-1 to touch `startStream` + the
droppedCount plumbing and NOTHING else -- and it must be observably
behavior-preserving, proven by the C1 parity suite passing untouched on
both sides of it.

Planner-verified current facts the edits are anchored to:
- `Awaitable.js:22-33` import list; `:180-195` export block (10 upstream
  names + 2 bridges); line-1 header stamp.
- `StreamQuery.js` startStream ring at `:119-137`, abort filter at
  `:139-142`, export at `:302`, line-1 stamp.
- `Query.js` has NO export block -- inline exports at `:137`, `:645`,
  `:852`; VERSION const is a new inline `export const`.
- `package.json` floors at `:53` (peer lite-await), `:55` (peer
  lite-stream), `:67`/`:69` (devDeps).
- `demo/VENDOR.md:11-12` -- lite-await.js and lite-stream.js both pinned
  1.0.0.
- Test suite today: 158 = query 106, awaitable 18, stream-query 15,
  edge-cases 12, ascii-guard 3, zero-gc 2, surface-guard 2.

## COMMIT PLAN (each commit green; no version bump; no publish)

C1 `test: record streamQuery semantics before the collapse` (OR-2)
   test/stream-query.test.js +5, green against CURRENT code with the
   CURRENTLY INSTALLED lite-stream 1.1.0:
     - "parity: status ladder idle -> pending -> streaming -> success"
     - "parity: droppedCount ladder N=12/maxBuffer=4 -> 8, newest-last"
     - "parity: restart resets count and droppedCount"
     - "parity: throwing iterator -> status error + error() surfaced"
     - "parity: abort is not an error (detach/restart/removeQueries)"
   These five are THE parity artifact; after C1 they are frozen (any later
   edit to them is a reviewer REJECT per OR-2).

C2 `chore: sibling floors to ^1.3.0 + install verification`
   package.json: lite-await peer+devDep `^1.3.0`; lite-stream peer+devDep
   `^1.3.0`. `npm install` (NO lockfile -- it is gitignored; do not commit
   node_modules). Then verify and record in the commit message:
     - `npm ls @zakkster/lite-await @zakkster/lite-stream` -> 1.3.0 both.
     - lite-await surface: 18 named + VERSION, `createAwaitScope` a
       function (node -e dynamic import probe).
     - lite-stream: `grep -n "onValue\|onAbort\|droppedCount" node_modules/
       @zakkster/lite-stream/Stream.js` shows the enriched pipeToSignal.
   FULL suite green (163 incl. C1's five) -- this re-proves the parity
   suite against 1.3.0 and IS the upstream byte-compat claim tested in
   our house. STOP if anything above fails.

C3 `feat(await): 18-name parity with lite-await 1.3.0`
   - Awaitable.js: import + export the EIGHT (`allSettledOf`,
     `withResolvers`, `tryFn`, `delay`, `withRetry`, `mapLimit`,
     `whenStatechart`, `createAwaitScope`), verbatim, no wrapping (OR-4).
     VERSION excluded BY NAME with a comment citing the convention.
   - Awaitable.d.ts: the eight type re-exports.
   - llms.txt + README /await sections: the eight; withRetry-vs-query()
     and delay-vs-mock-clock boundary lines; createAwaitScope one-liner
     citing upstream 0007; fromPromise 0009-REJECT closure sentence.
   - Tests +13 in test/awaitable.test.js: 8 identity
     (`assert.equal(reexported, upstream)` per name), "VERSION is not
     exported from /await", "subpath named surface == upstream minus
     VERSION plus whenQuery/whenAllQueries" (Object.keys set compare),
     withRetry succeeds-on-3rd-attempt, mapLimit concurrency ceiling
     (in-flight counter), createAwaitScope abort settles a pre-bound
     whenSignal + teardown observed. whenStatechart identity only.
   - OR-7 evidence in the message: identity tests run against HEAD~
     (8 failures), surface guard run before the docs edit (fired -- G3
     recorded), both green at commit.

C4 `refactor(stream): collapse the hand ring into pipeToSignal 1.3.0`
   startStream only (OR-1):
   - delete the ring block (`:119-137` today; re-locate by content after
     C1-C3 drift): `mode` + `maxBuffer` pass through to pipeToSignal
     (streamQuery's own validation at `:75-85` stays -- it is OUR
     option-surface contract and its error messages are pinned by tests).
   - `transform` leaves the call; first-frame status transition moves to
     `onValue` (fires before each set, after transform -- planner-verified
     ordering); count tracking moves there too.
   - abort filter deleted; `onAbort` absorbs intentional aborts (upstream
     0001: with onAbort present they never reach onError). onError body
     loses the `ac.signal.aborted` guard.
   - droppedCount: read the stop fn's getter; snapshot into
     `entry.streamDropped` on EVERY terminal path (onDone/onError/onAbort)
     and on restart reset, so `droppedCount()` reads byte-identical
     semantics after completion and across restarts.
   - The C1 parity suite passes UNTOUCHED (G5). Bench the buffer-mode
     stream scenario before/after (scratchpad harness if bench/ lacks
     one); record the measured delta with provenance in the commit
     message, or state "not measured" -- never estimate (law: measured
     numbers only).

C5 `chore(demo): vendored copies -> 1.3.0 from registry tarballs` (OR-3)
   `npm pack @zakkster/lite-await@1.3.0` and `@zakkster/lite-stream@1.3.0`
   in the scratchpad; extract; byte-copy `Await.js` -> demo/vendor/
   lite-await.js and `Stream.js` -> demo/vendor/lite-stream.js. Update
   VENDOR.md rows (versions + the check commands; add a `grep -c
   createAwaitScope demo/vendor/lite-await.js` line). `node --check` both
   copies; verify the demo importmap wiring still resolves (grep the demo
   HTML for the vendor paths).

C6 `feat: VERSION const + two-place sync (Q-16, OR-6)`
   Query.js: `export const VERSION = "1.1.2";` (inline, near the header).
   StreamQuery.js + Awaitable.js: `export { VERSION } from "./Query.js";`
   New test/version-sync.test.js (1 test): imported VERSION ===
   package.json version, string compare. Retire the line-1 header version
   stamps in all three files (comment text only -- token streams of
   shipped logic unchanged; the reviewer checks). llms.txt/README: note
   VERSION as the one runtime version source.

C7 `docs: CHANGELOG [1.2.0]`
   Sections Added (the eight, VERSION const) / Changed (collapse with the
   C4 measured delta or "not measured", floors ^1.3.0 both) / Fixed (none
   unless found). The 0009 closure sentence. Facts only, ASCII only.
   package.json stays 1.1.2 (the /release drill owns the bump).

## GATES (mapped to BRIEF G1-G9)

Per commit: `npm test` green (G1 floor rises C1 163 -> C3 176 -> C6 177),
`node --check` on touched .js, ascii-guard green (G8, in the suite).
Final: G2 `npm run torture` 3x PASS + controls fail; G4 = the C3 identity
block (18 importable, VERSION absent); G5 = C1 suite untouched across C4
(prove with `git diff C1..HEAD -- test/stream-query.test.js` limited to
additions BELOW the parity block, ideally empty); G6 `npm ls` -> 1.3.0
both; G7 `npm pack --dry-run` -> 13 files, set unchanged (VENDOR.md and
demo/ stay unshipped); G9 `git status` clean + fresh-clone drill
(scratchpad clone, install, test, torture, delete).

Projected final count: 158 + 5 (C1) + 13 (C3) + 1 (C6) = **177** (BRIEF
floor 170, frontmatter tests_min honored).

## RISKS

R1 Upstream surfaces differ from the BRIEF's claims -> C2's verification
   step STOPs before any dependent code (OR-5).
R2 The collapse changes droppedCount timing observably (getter vs per-push
   counter) -> the C1 ladder test is the tripwire; if it fails, STOP and
   report -- do not adjust the test (OR-2).
R3 onValue/onAbort semantics differ from the planner's reading ->
   re-verify against installed 1.3.0 source in C2 (grep the actual
   Stream.js), not the CHANGELOG prose.
R4 Vendored 1.3.0 copies break the demos (importmap or API drift) ->
   node --check + importmap grep in C5; a demo-visible break is a STOP,
   not a silent fix.
R5 createAwaitScope integration test flakes (timer/abort timing) -> use
   deterministic settlement (pre-resolved signals, manual aborts), no
   wall-clock waits.
