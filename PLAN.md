# PLAN.md -- Q2 -- lite-query v1.1.2 -- coder implementation plan

From the Q2 planner pass (2026-08-31) + operator rulings. Contract: BRIEF.md
(Q2). Package root: /Users/zakkster/Work/Portfolio/LiteLibrariesSuite/LiteQuery.

## SPEC

Docs release, zero logic. The planner verified: all 57 non-ASCII occurrences
in the three shipped .js sources sit inside comments (line or block) -- the
token streams of Query.js / StreamQuery.js / Awaitable.js must be identical
before and after. Total sweep scope: 589 non-ASCII-bearing lines across 23
files. The durable deliverable is the two drift guards joining `npm test`
(153 -> 158 tests). The one complication the planner caught: test/ and bench/
carry non-ASCII inside STRING LITERALS (test titles, printed dividers like
`"─".repeat(125)`, `<=`/`->` glyphs in test names) -- resolved by OR-1
below.

## OPERATOR RULINGS

- **OR-1 (STOP items -- display strings in test/bench).** The law is
  ASCII-only SOURCE, regardless of syntactic position, and the new ascii
  guard covers test/ + bench/ -- so these strings cannot stay. They MAY be
  swept because they are display-only, under two conditions the coder must
  prove per literal: (i) grep shows no assertion, regex, or gate matches the
  literal's content anywhere in the repo (test titles are not asserted on;
  the torture PASS-line greps are ASCII already); (ii) after the sweep,
  `npm test` and `npm run torture` outputs are behaviorally identical (same
  counts, same exits). Any literal that IS matched somewhere: STOP, report,
  leave for a ruling. The guard allowlist stays empty.
- **OR-2 (U+2500 divider convention).** A run of N U+2500 becomes N ASCII
  `-` characters (preserves visual width and alignment in every file
  uniformly; no judgment calls). Applies to comments and to the bench
  display literals under OR-1.
- **OR-3 (sweep mechanics).** Scripted, not hand-edited: a throwaway
  script in the scratchpad
  (/private/tmp/claude-502/-Users-zakkster-Work-Portfolio-LiteLibrariesSuite-LiteQuery/ccc5a41f-26f2-468a-8a86-2b584cfcf7dc/scratchpad)
  applying the mapping table below file-by-file, followed per file class by:
  `node --check` (for .js/.mjs), `npm test`, and a `grep -P '[^\x00-\x7F]'`
  audit. The script is never committed. Review the diff per file before
  moving on.

## MAPPING TABLE (from BRIEF task 1)

| From | To |
| --- | --- |
| U+2500 run (length N) | `-` x N (OR-2) |
| em dash U+2014 | `--` |
| en dash U+2013 | `-` |
| right arrow U+2192 | `->` |
| left arrow U+2190 | `<-` |
| U+2265 | `>=` |
| U+2264 | `<=` |
| middle dot U+00B7 | `*` or comma per context |
| copyright U+00A9 | `(c)` |
| i-diaeresis U+00EF | `i` ("naive") |
| U+00D7 in docs/prose | `x` |
| curly quotes, if any found | `'` / `"` |

Anything encountered outside this table: STOP and report before mapping.

## ATOMIC TASKS

T1. `test/ascii-guard.test.js` (3 tests): walk list = `files[]` parsed from
    package.json at runtime + `test/*.js` + `bench/**/*.mjs` globs (tracks
    future files[] changes automatically); predicate over a Buffer allowing
    printable ASCII + LF (+ optional per-file allowlist arg, empty today);
    tests: (1) every walked file passes, (2) inline fixture containing one
    U+2014 fails the predicate (control), (3) allowlist mechanism admits a
    listed codepoint (proves the escape hatch works without using it).
T2. `test/surface-guard.test.js` (2 tests): relative dynamic imports of
    ../Query.js, ../StreamQuery.js, ../Awaitable.js (relative -- works under
    `node --test` from the repo root with zero resolution tricks); (1) every
    export name appears in llms.txt AND its entry's .d.ts, and every
    `@zakkster/lite-*` name in llms.txt's peer block exists in package.json
    peerDependencies; (2) fixture llms body missing one name fails
    (control).
T3. Sweep the three shipped .js + three .d.ts (comment bytes only; 57 + 447
    occurrences). Gate per file: `node --check`, then full `npm test`.
T4. Sweep test/*.js + bench/**/*.mjs under OR-1 (prove each display literal
    unmatched first). Gate: `npm test` counts identical; `npm run torture`
    3x PASS exit 0, output lines behaviorally identical.
T5. Sweep README.md, llms.txt, CHANGELOG.md (history punctuation only,
    wording byte-identical otherwise), Cookbook.md, QuickStart.md, SPEC.md.
T6. QuickStart.md 1.1 refresh: fix the install line (ONE required peer);
    fix the stale "five steps" claim (planner: line ~143); add a
    streamQuery latest-mode taste consistent with Cookbook 14 and a
    whenQuery route-guard taste consistent with Cookbook 11. Short on-ramp
    voice; runnable snippets.
T7. Riders (quote-verify each line in the file before editing; line numbers
    are approximate after sweeps): llms.txt "One-connection-shared is 1.2."
    -> "... is 2.0 (see ROADMAP.md)."; README ~43 "(plus its peer deps in
    the lite ecosystem)" -> singular required peer wording; README
    ~308/~321 lite-channel "powers"/"engine behind" -> "the convenient
    isLeader source" framing consistent with lines ~300-301; README ~241
    demo path -> qualify "in the repo"; README doc links -> relative
    ./QuickStart.md ./Cookbook.md; package.json files[] += QuickStart.md,
    Cookbook.md; SPEC.md header line marking it the historical 1.1.0 RC
    doc + "Post-publish roadmap" body replaced by a two-line pointer to
    ROADMAP.md; new demo/VENDOR.md (~6 lines, pinned copies + check
    command), not in files[].
T8. CHANGELOG `[1.1.2]` entry, sections Added / Changed / Fixed, ASCII
    only, per-file sweep counts included, facts only. Version stays 1.1.1
    in package.json (the /release drill owns the bump).

## COMMIT PLAN (no push, no publish, no version bump)

C1 guards + source/test/bench sweep: test/ascii-guard.test.js,
   test/surface-guard.test.js, Query.js, Query.d.ts, StreamQuery.js,
   StreamQuery.d.ts, Awaitable.js, Awaitable.d.ts, test/*.js,
   bench/**/*.mjs.
C2 docs content: README.md, llms.txt, CHANGELOG.md, QuickStart.md,
   Cookbook.md, SPEC.md, package.json (files[] only), demo/VENDOR.md.
C3 planning docs: ROADMAP.md, BRIEF.md, PLAN.md.
Messages follow the Q1 pattern: what/why/findings trailer +
Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>.

## GATES

Pre-commit: G1 `npm test` -> tests 158, pass 158, fail 0, skipped 0.
G2 `npm run torture` -> 3x PASS, exit 0. G3 `grep -rP '[^\x00-\x7F]'` over
files[] scope + test/ + bench/ -> zero hits. G4 `node --check` green on the
three shipped .js. G5 `npm pack --dry-run` -> 13 files, QuickStart + Cookbook
present, no test/bench/demo. G6 every relative README link target in the
pack listing. G7 riders verified by grep (no "is 1.2" sequencing claim; no
"peer deps" plural; SPEC pointer present). G8 `git diff -- '*.js'` for the
three shipped sources shows comment-only hunks.
Post-commit: G9 `git status --porcelain` empty. G10 fresh-clone install +
test + torture green (scratchpad clone, then delete).

## RISKS

R1 sweep corrupting a matched string -- OR-1's per-literal grep is the
control; reviewer re-verifies. R2 the U+2500 volume (1,200+) makes hand
edits unviable -- scripted per OR-3 with per-file diff review. R3 files[]
runtime parse in the ascii guard must not choke if files[] gains a
directory entry later -- treat entries as files, skip non-existent with a
loud failure (fail closed: a files[] entry that does not exist IS a bug).
R4 Cookbook.md is large and newly in files[] -- it is in the sweep scope
and the guard scope; its counts go in the CHANGELOG line. R5 test-title
sweeps change `npm test` display lines -- acceptable per OR-1; counts are
the gate, not titles. R6 CHANGELOG history sweep must not change wording --
diff review word-by-word on that file (punctuation-only hunks).

## POST-CODER RULINGS (2026-08-31)

- **OR-4 (off-table glyphs in bench/bench.mjs):** the coder found U+2022
  bullets (x4) and one U+25B6 in display-only console strings, outside the
  MAPPING TABLE, and mapped by extension (bullet -> `*`, U+25B6 -> `>`)
  with OR-1 proofs. Ratified; the mappings are recorded in the CHANGELOG's
  sweep bullet.
- **OR-5 (test-count drift):** the guards raise the runtime to 158; the
  coder correctly left the four doc TOTALS (README tagline, facts row,
  Tests section, llms.txt headline) at 153 as out-of-scope. Fixed by the
  operator before review -- Q-14's lesson is precisely that totals and
  runtime must not diverge in a shipped artifact. Per-section counts
  (llms.txt "120 core", "106 in query.test.js") verified correct and
  unchanged. CHANGELOG gains the alignment bullet.
