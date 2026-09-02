// @zakkster/lite-await 1.3.0
// Zero-GC bridge between @zakkster/lite-signal and Promise/async-await.
//
// Brings the four Promise combinators (race / any / all / allSettled) into the
// signal world, paired with first-class AbortSignal + timeout. Cleanup is
// structural -- every settlement path (resolve, reject, timeout, abort) tears
// the underlying effect down and returns its node to the lite-signal pool.
//
// Complements rather than duplicates lite-signal:
//   lite-signal's whenAsync(predicate) is bare and predicate-only.
//   lite-await's whenSignal(sig, pred, { timeout, signal }) is the rich form.
//   The combinators (allOf / anyOf / raceOf) coordinate multi-source waits
//   with shared AbortSignal cleanup -- a primitive that lite-signal does not
//   ship and that every async consumer ends up reinventing badly.
//
// Copyright (c) 2026 Zahary Shinikchiev <shinikchiev@yahoo.com>
// MIT License

import { effect, untrack, signal as _signal, dispose } from "@zakkster/lite-signal";

// Package version. Kept in lockstep with package.json and llms.txt (three-place
// version law); asserted by test/12-version-sync.test.mjs.
const VERSION = "1.3.0";

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/**
 * Thrown when a `timeout` deadline elapses before settlement.
 */
class TimeoutError extends Error {
    constructor(timeoutMs) {
        super("lite-await: timed out after " + timeoutMs + "ms");
        this.name = "TimeoutError";
        this.timeout = timeoutMs;
    }
}

/**
 * Build an error that matches the platform's AbortError shape. Prefers the
 * abort signal's `reason` (DOM spec); falls back to a DOMException-shaped
 * AbortError, finally to a plain Error.
 * @private
 */
function makeAbortError(signal) {
    if (signal !== undefined && signal !== null && signal.reason !== undefined) {
        return signal.reason;
    }
    if (typeof DOMException !== "undefined") {
        return new DOMException("Aborted", "AbortError");
    }
    const err = new Error("Aborted");
    err.name = "AbortError";
    return err;
}

// ---------------------------------------------------------------------------
// Internal helper: link a user-provided AbortSignal to an internal controller
// ---------------------------------------------------------------------------
// The combinators each create an internal AbortController to cancel their
// child whenSignal() promises on settlement. If the user also passes an
// AbortSignal, we forward its abort to the internal controller and clean up
// the listener once the bundle settles. Returns a `cleanup` function that
// removes the forwarded listener (idempotent).
function linkUserSignal(internalCtrl, userSignal) {
    if (userSignal === undefined || userSignal === null) {
        return noopCleanup;
    }
    if (userSignal.aborted) {
        internalCtrl.abort(userSignal.reason !== undefined ? userSignal.reason : makeAbortError(userSignal));
        return noopCleanup;
    }
    const listener = () => internalCtrl.abort(
        userSignal.reason !== undefined ? userSignal.reason : makeAbortError(userSignal)
    );
    userSignal.addEventListener("abort", listener);
    let removed = false;
    return function cleanupForward() {
        if (removed) return;
        removed = true;
        userSignal.removeEventListener("abort", listener);
    };
}
function noopCleanup() {}

// ---------------------------------------------------------------------------
// Cold-path option reader (registration-time; never per-fire)
// ---------------------------------------------------------------------------
// The single door for { timeout, signal }, shared by whenSignal, allOf, anyOf,
// raceOf and whenStatechart (see decisions/0002-timeout-law.md). Enforces the
// uniform timeout law A (undefined / Infinity == no deadline; anything else
// must be a finite non-negative number, else RangeError -- null is NOT zero),
// shape-checks the signal (null/undefined ok, else it must look like an
// AbortSignal: boolean .aborted plus add/removeEventListener functions), and
// rejects unknown own-enumerable keys with a did-you-mean hint. Runs once per
// call; the null/undefined-opts path allocates nothing. Results are returned
// through two module-scoped out-params, read by the caller BEFORE any
// re-entrant readOpts call (the combinators call it again per child spec). Each
// opts property is read exactly once, so a getter-bearing opts is not
// re-invoked.
let _optsTimeout;
let _optsSignal;

function readOpts(opts, name) {
    if (opts === undefined || opts === null) {
        _optsTimeout = undefined;
        _optsSignal = undefined;
        return;
    }
    // Fail-closed shape door: a non-object opts (number / boolean / bigint /
    // symbol / string) or an array has no own-enumerable timeout/signal key, so
    // Object.keys would return [] and the malformed opts would be silently
    // accepted as "no opts". A non-null, non-undefined opts that cannot be a
    // plain options object is a caller bug -- reject it before the key scan.
    if (typeof opts !== "object" || Array.isArray(opts)) {
        throw new TypeError(
            name + ": opts must be a plain { timeout, signal } object -- got " +
            (Array.isArray(opts) ? "array" : typeof opts));
    }
    // Unknown-key rejection first: a typo'd key ("timeot") is a loud error, not
    // a silent "no deadline". One Object.keys per call, cold path.
    const keys = Object.keys(opts);
    for (let i = 0; i < keys.length; i++) {
        const k = keys[i];
        if (k !== "timeout" && k !== "signal") {
            const near = nearestOptKey(k);
            throw new TypeError(
                name + ": unknown option '" + k + "'" +
                (near !== null ? " (did you mean '" + near + "'?)" : "") +
                " -- known options: timeout, signal");
        }
    }
    const t = opts.timeout;
    if (t !== undefined && t !== Infinity && (!Number.isFinite(t) || t < 0)) {
        throw new RangeError(
            name + ": timeout must be a finite non-negative number, Infinity, or undefined");
    }
    const sig = opts.signal;
    if (sig !== undefined && sig !== null &&
        (typeof sig.aborted !== "boolean" ||
         typeof sig.addEventListener !== "function" ||
         typeof sig.removeEventListener !== "function")) {
        throw new TypeError(
            name + ": signal must be an AbortSignal " +
            "(boolean .aborted plus addEventListener/removeEventListener)");
    }
    _optsTimeout = t;
    _optsSignal = sig;
}

// Levenshtein-based did-you-mean for the two known option keys. Cold path.
function nearestOptKey(key) {
    const dt = editDistance(key, "timeout");
    const ds = editDistance(key, "signal");
    if (dt <= 2 && dt <= ds) return "timeout";
    if (ds <= 2) return "signal";
    return null;
}

function editDistance(a, b) {
    const al = a.length;
    const bl = b.length;
    if (al === 0) return bl;
    if (bl === 0) return al;
    let prev = new Array(bl + 1);
    let curr = new Array(bl + 1);
    for (let j = 0; j <= bl; j++) prev[j] = j;
    for (let i = 1; i <= al; i++) {
        curr[0] = i;
        const ai = a.charCodeAt(i - 1);
        for (let j = 1; j <= bl; j++) {
            const cost = ai === b.charCodeAt(j - 1) ? 0 : 1;
            let m = prev[j] + 1;
            const del = curr[j - 1] + 1;
            if (del < m) m = del;
            const sub = prev[j - 1] + cost;
            if (sub < m) m = sub;
            curr[j] = m;
        }
        const tmp = prev; prev = curr; curr = tmp;
    }
    return prev[bl];
}

// ---------------------------------------------------------------------------
// Cold-path option reader for delay() -- DEDICATED, not readOpts
// ---------------------------------------------------------------------------
// delay() takes { signal } ONLY (ms IS the timeout; there is no nested timeout
// key). It gets its own reader rather than sharing readOpts / the module-scoped
// _optsTimeout/_optsSignal out-param channel, for two reasons: (1) delay has a
// different legal key set (signal only -- a "timeout" key here is an error, not
// a second deadline), and (2) reusing the shared channel would risk re-entrancy
// with a combinator that called readOpts and has not yet read its out-params.
// This reader RETURNS the validated signal as a plain local value -- no shared
// state. Cold path; the null/undefined-opts path allocates nothing.
function readDelayOpts(opts, name) {
    if (opts === undefined || opts === null) {
        return undefined;
    }
    // Fail-closed shape door (same seam as readOpts): a scalar/array opts has no
    // own-enumerable signal key, so Object.keys would return [] and the
    // malformed opts would be silently accepted as "no opts". Reject it first.
    if (typeof opts !== "object" || Array.isArray(opts)) {
        throw new TypeError(
            name + ": opts must be a plain { signal } object -- got " +
            (Array.isArray(opts) ? "array" : typeof opts));
    }
    const keys = Object.keys(opts);
    for (let i = 0; i < keys.length; i++) {
        const k = keys[i];
        if (k !== "signal") {
            const ds = editDistance(k, "signal");
            throw new TypeError(
                name + ": unknown option '" + k + "'" +
                (ds <= 2 ? " (did you mean 'signal'?)" : "") +
                " -- known options: signal (ms is the timeout)");
        }
    }
    const sig = opts.signal;
    if (sig !== undefined && sig !== null &&
        (typeof sig.aborted !== "boolean" ||
         typeof sig.addEventListener !== "function" ||
         typeof sig.removeEventListener !== "function")) {
        throw new TypeError(
            name + ": signal must be an AbortSignal " +
            "(boolean .aborted plus addEventListener/removeEventListener)");
    }
    return sig;
}

// ---------------------------------------------------------------------------
// Cold-path option reader for withRetry() -- DEDICATED, not readOpts
// ---------------------------------------------------------------------------
// withRetry takes { attempts, baseMs, factor, jitter, signal?, timeout?,
// retryable? }. It gets its own reader because the shared readOpts rejects any
// key that is not timeout / signal, so the retry-policy keys cannot route
// through it. Validation runs entirely at the door, BEFORE the first attempt is
// launched: every numeric key per the W1 numeric law, then retryable, then the
// signal shape and timeout law reused verbatim from readOpts. attempts is
// REQUIRED (a positive integer >= 1); baseMs / factor / jitter default when
// omitted; retryable / signal / timeout are optional. Results are returned
// through module-scoped out-params (no allocation on this cold path), read by
// withRetry BEFORE any re-entrant reader call. See decisions/0005.
let _retryAttempts;
let _retryBaseMs;
let _retryFactor;
let _retryJitter;
let _retryRetryable;
let _retrySignal;
let _retryTimeout;

function readRetryOpts(opts, name) {
    // Fail-closed shape door (same seam as readOpts / readDelayOpts): opts is
    // REQUIRED here because attempts is required, so undefined / null / a scalar
    // / an array all reject before any key is read.
    if (opts === undefined || opts === null ||
        typeof opts !== "object" || Array.isArray(opts)) {
        throw new TypeError(
            name + ": opts must be a plain { attempts, baseMs, factor, jitter, " +
            "signal, timeout, retryable } object -- got " +
            (Array.isArray(opts) ? "array" : opts === null ? "null" : typeof opts));
    }
    // Unknown-key rejection first: a typo'd key is a loud error, not a silent
    // default. One Object.keys per call, cold path.
    const keys = Object.keys(opts);
    for (let i = 0; i < keys.length; i++) {
        const k = keys[i];
        if (k !== "attempts" && k !== "baseMs" && k !== "factor" && k !== "jitter" &&
            k !== "signal" && k !== "timeout" && k !== "retryable") {
            const near = nearestRetryKey(k);
            throw new TypeError(
                name + ": unknown option '" + k + "'" +
                (near !== null ? " (did you mean '" + near + "'?)" : "") +
                " -- known options: attempts, baseMs, factor, jitter, signal, timeout, retryable");
        }
    }
    // attempts: required positive integer (>= 1). undefined / NaN / 1.5 / 0 /
    // negative / Infinity / non-number all fail Number.isInteger or the bound.
    const attempts = opts.attempts;
    if (!Number.isInteger(attempts) || attempts < 1) {
        throw new RangeError(name + ": attempts must be an integer >= 1");
    }
    // baseMs: finite non-negative; defaults to 0 (retry with no backoff).
    const baseMs = opts.baseMs === undefined ? 0 : opts.baseMs;
    if (!Number.isFinite(baseMs) || baseMs < 0) {
        throw new RangeError(name + ": baseMs must be a finite non-negative number");
    }
    // factor: finite >= 1; defaults to 2 (classic exponential doubling).
    const factor = opts.factor === undefined ? 2 : opts.factor;
    if (!Number.isFinite(factor) || factor < 1) {
        throw new RangeError(name + ": factor must be a finite number >= 1");
    }
    // jitter: finite in [0, 1]; defaults to 0 (deterministic backoff).
    const jitter = opts.jitter === undefined ? 0 : opts.jitter;
    if (!Number.isFinite(jitter) || jitter < 0 || jitter > 1) {
        throw new RangeError(name + ": jitter must be a finite number in [0, 1]");
    }
    // retryable: if present, must be a function (error, attempt) => boolean.
    const retryable = opts.retryable;
    if (retryable !== undefined && typeof retryable !== "function") {
        throw new TypeError(name + ": retryable must be a function (error, attempt) => boolean");
    }
    // signal / timeout: reuse readOpts' shape + law verbatim.
    const sig = opts.signal;
    if (sig !== undefined && sig !== null &&
        (typeof sig.aborted !== "boolean" ||
         typeof sig.addEventListener !== "function" ||
         typeof sig.removeEventListener !== "function")) {
        throw new TypeError(
            name + ": signal must be an AbortSignal " +
            "(boolean .aborted plus addEventListener/removeEventListener)");
    }
    const t = opts.timeout;
    if (t !== undefined && t !== Infinity && (!Number.isFinite(t) || t < 0)) {
        throw new RangeError(
            name + ": timeout must be a finite non-negative number, Infinity, or undefined");
    }
    _retryAttempts = attempts;
    _retryBaseMs = baseMs;
    _retryFactor = factor;
    _retryJitter = jitter;
    _retryRetryable = retryable;
    _retrySignal = sig;
    _retryTimeout = t;
}

// Levenshtein-based did-you-mean for the seven known withRetry option keys.
// Cold path (only on a rejected unknown key).
function nearestRetryKey(key) {
    const known = ["attempts", "baseMs", "factor", "jitter", "signal", "timeout", "retryable"];
    let best = null;
    let bestD = 3; // accept edit-distance <= 2
    for (let i = 0; i < known.length; i++) {
        const d = editDistance(key, known[i]);
        if (d < bestD) { bestD = d; best = known[i]; }
    }
    return best;
}

// Thenable per the Promises/A+ duck test: a non-null object-or-function whose
// `.then` is callable. Promise.resolve adopts any such value; the wrappers
// reject everything else at the door.
function isThenable(v) {
    return v !== null &&
        (typeof v === "object" || typeof v === "function") &&
        typeof v.then === "function";
}

// ---------------------------------------------------------------------------
// whenSignal -- the foundational primitive
// ---------------------------------------------------------------------------

/**
 * Wait until a reactive source first satisfies `predicate`, then resolve with
 * the satisfying value. Supports timeout and AbortSignal. Settlement -- by
 * resolve, abort, or timeout -- always cleans the underlying effect, returning
 * its node to the lite-signal pool.
 *
 * If the source ALREADY satisfies the predicate on first read, resolves
 * synchronously on the next microtask. The underlying effect is created and
 * torn down within a single tick; no source-change notification is required.
 *
 * @template T
 * @param {() => T} source             Reactive read function (signal/computed/getter).
 * @param {(value: T) => unknown} predicate
 *                                     Returns truthy to resolve.
 * @param {{ timeout?: number, signal?: AbortSignal }} [opts]
 * @returns {Promise<T>}               Resolves with the satisfying value.
 *
 * @example
 *   const status = signal("loading");
 *   const v = await whenSignal(status, (s) => s === "ready", { timeout: 5000 });
 */
function whenSignal(source, predicate, opts) {
    if (typeof source !== "function") {
        return Promise.reject(new TypeError("whenSignal: source must be a function"));
    }
    if (typeof predicate !== "function") {
        return Promise.reject(new TypeError("whenSignal: predicate must be a function"));
    }
    try { readOpts(opts, "whenSignal"); }
    catch (e) { return Promise.reject(e); }
    const userSignal = _optsSignal;
    const timeoutMs  = _optsTimeout;

    if (userSignal !== undefined && userSignal !== null && userSignal.aborted) {
        return Promise.reject(makeAbortError(userSignal));
    }

    return new Promise(function (resolve, reject) {
        let settled = false;
        let stopEffect = null;
        let stopPending = false;
        let timeoutId = null;
        let abortListener = null;

        // Late-binding stop: callable before `stopEffect` is assigned (e.g.,
        // synchronously inside the predicate fire on first effect run). The
        // pattern is borrowed verbatim from WatchEx.js's watchUntil.
        const stop = function () {
            if (stopEffect !== null) {
                stopEffect();
                stopEffect = null;
            } else {
                stopPending = true;
            }
        };

        const fullCleanup = function () {
            stop();
            if (timeoutId !== null) {
                clearTimeout(timeoutId);
                timeoutId = null;
            }
            if (abortListener !== null && userSignal !== undefined && userSignal !== null) {
                userSignal.removeEventListener("abort", abortListener);
                abortListener = null;
            }
        };

        const doResolve = function (value) {
            if (settled) return;
            settled = true;
            fullCleanup();
            resolve(value);
        };

        const doReject = function (err) {
            if (settled) return;
            settled = true;
            fullCleanup();
            reject(err);
        };

        // timeout was validated by readOpts (Law A); undefined / Infinity arm
        // no timer (no deadline). Only a finite non-negative value arms one.
        if (timeoutMs !== undefined && timeoutMs !== Infinity) {
            timeoutId = setTimeout(function () {
                doReject(new TimeoutError(timeoutMs));
            }, timeoutMs);
        }

        if (userSignal !== undefined && userSignal !== null) {
            abortListener = function () { doReject(makeAbortError(userSignal)); };
            userSignal.addEventListener("abort", abortListener);
        }

        // Hoisted untrack body: declared ONCE, reused per fire. Reads
        // `currentValue` from the shared closure; effect body fills it before
        // calling. Matches WatchEx.js's ZERO-GC HOT PATH pattern.
        //
        // A throwing predicate -- or a throwing `source()` getter -- is routed
        // to doReject so the promise settles and the effect tears down. Without
        // this, a throw on a change-driven fire escapes at the signal-WRITER's
        // `.set()` call site (it unwinds through flushEffects) and leaves this
        // promise pending forever with a live, leaked effect node. doReject
        // disposes the effect from within its own run -- the same late-binding
        // stop path doResolve already relies on. The happy path (predicate
        // returns without throwing) allocates nothing: a try block with no
        // throw is free, and `ok` is a stack local, not a heap allocation.
        let currentValue;
        const checkPredicate = function () {
            if (settled) return;
            let ok;
            try {
                ok = predicate(currentValue);
            } catch (e) {
                doReject(e);
                return;
            }
            if (ok) doResolve(currentValue);
        };

        stopEffect = effect(function () {
            if (settled) return;
            try {
                currentValue = source();
            } catch (e) {
                doReject(e);
                return;
            }
            untrack(checkPredicate);
        });

        // If `stop` was called from inside the synchronous first effect run
        // (predicate satisfied on initial read), honor the deferred request.
        if (stopPending) {
            stopEffect();
            stopEffect = null;
        }
    });
}

// ---------------------------------------------------------------------------
// Combinators
// ---------------------------------------------------------------------------

/**
 * Wait until EVERY spec's signal satisfies its predicate. Resolves with the
 * values in input order. Rejects on first rejection (timeout, abort, or
 * spec-specific error). On any failure path the remaining in-flight specs are
 * aborted so their effects clean up.
 *
 * @template T
 * @param {Array<[() => T, (value: T) => unknown]>} specs   [source, predicate] pairs.
 * @param {{ timeout?: number, signal?: AbortSignal }} [opts]
 * @returns {Promise<T[]>}
 */
function allOf(specs, opts) {
    if (!Array.isArray(specs)) {
        return Promise.reject(new TypeError("allOf: specs must be an array"));
    }
    // Validate opts BEFORE the empty-specs shortcut: an invalid opts on [] still
    // rejects with the validation error, never the empty-case result.
    try { readOpts(opts, "allOf"); }
    catch (e) { return Promise.reject(e); }
    const userSignal = _optsSignal;
    const timeoutMs  = _optsTimeout;

    const n = specs.length;
    if (n === 0) return Promise.resolve([]);

    if (userSignal !== undefined && userSignal !== null && userSignal.aborted) {
        return Promise.reject(makeAbortError(userSignal));
    }

    const internalCtrl = new AbortController();
    const unlink = linkUserSignal(internalCtrl, userSignal);

    return new Promise(function (resolve, reject) {
        let settled = false;
        let resolvedCount = 0;
        const values = new Array(n);
        let timeoutId = null;

        const settleResolve = function (v) {
            if (settled) return;
            settled = true;
            unlink();
            if (timeoutId !== null) clearTimeout(timeoutId);
            resolve(v);
        };

        const settleReject = function (e) {
            if (settled) return;
            settled = true;
            internalCtrl.abort(e);                       // cancel in-flight siblings
            unlink();
            if (timeoutId !== null) clearTimeout(timeoutId);
            reject(e);
        };

        if (timeoutMs !== undefined && timeoutMs !== Infinity) {
            timeoutId = setTimeout(function () {
                settleReject(new TimeoutError(timeoutMs));
            }, timeoutMs);
        }

        for (let i = 0; i < n; i++) {
            const spec = specs[i];
            if (!Array.isArray(spec) || spec.length < 2) {
                settleReject(new TypeError("allOf: each spec must be [source, predicate]"));
                return;
            }
            whenSignal(spec[0], spec[1], { signal: internalCtrl.signal }).then(
                function (v) {
                    if (settled) return;
                    values[i] = v;
                    resolvedCount = (resolvedCount + 1) | 0;
                    if (resolvedCount === n) settleResolve(values);
                },
                function (e) { settleReject(e); }
            );
        }
    });
}

/**
 * Wait until ANY spec resolves. Resolves with `{ index, value }` of the
 * winner. Other in-flight specs are aborted on win. Rejects only if every
 * spec rejects -- with an `AggregateError` carrying the per-spec rejections.
 *
 * @template T
 * @param {Array<[() => T, (value: T) => unknown]>} specs
 * @param {{ timeout?: number, signal?: AbortSignal }} [opts]
 * @returns {Promise<{ index: number, value: T }>}
 */
function anyOf(specs, opts) {
    if (!Array.isArray(specs)) {
        return Promise.reject(new TypeError("anyOf: specs must be an array"));
    }
    // Validate opts BEFORE the empty-specs shortcut: an invalid opts on [] still
    // rejects with the validation error, never the empty-case AggregateError.
    try { readOpts(opts, "anyOf"); }
    catch (e) { return Promise.reject(e); }
    const userSignal = _optsSignal;
    const timeoutMs  = _optsTimeout;

    const n = specs.length;
    if (n === 0) {
        return Promise.reject(new AggregateError([], "anyOf: empty specs"));
    }

    if (userSignal !== undefined && userSignal !== null && userSignal.aborted) {
        return Promise.reject(makeAbortError(userSignal));
    }

    const internalCtrl = new AbortController();
    const unlink = linkUserSignal(internalCtrl, userSignal);

    return new Promise(function (resolve, reject) {
        let settled = false;
        let rejectedCount = 0;
        const errors = new Array(n);
        let timeoutId = null;
        let abortListener = null;

        // Remove BOTH user-signal listeners on settle: linkUserSignal's forward
        // (into internalCtrl) and the direct one armed below (AW-03). Called from
        // every settlement path so nothing is left on the caller's (often shared)
        // signal.
        const unlinkAll = function () {
            unlink();
            if (abortListener !== null && userSignal !== undefined && userSignal !== null) {
                userSignal.removeEventListener("abort", abortListener);
                abortListener = null;
            }
        };

        const settleResolve = function (winner) {
            if (settled) return;
            settled = true;
            internalCtrl.abort();                        // cancel siblings
            unlinkAll();
            if (timeoutId !== null) clearTimeout(timeoutId);
            resolve(winner);
        };

        const settleReject = function (e) {
            if (settled) return;
            settled = true;
            internalCtrl.abort(e);
            unlinkAll();
            if (timeoutId !== null) clearTimeout(timeoutId);
            reject(e);
        };

        if (timeoutMs !== undefined && timeoutMs !== Infinity) {
            timeoutId = setTimeout(function () {
                settleReject(new TimeoutError(timeoutMs));
            }, timeoutMs);
        }

        // AW-03: a mid-flight USER abort must reject the platform AbortError
        // directly -- the same shape the pre-abort short-circuit above already
        // returns -- not the AggregateError the per-child counter would produce
        // once the aborted children reject. This direct listener wins that race:
        // settleReject sets `settled` FIRST, so the children's rejections fall
        // into their own `settled` guard and never reach errors[]/rejectedCount.
        // Mirrors whenSignal's direct abort listener verbatim.
        if (userSignal !== undefined && userSignal !== null) {
            abortListener = function () { settleReject(makeAbortError(userSignal)); };
            userSignal.addEventListener("abort", abortListener);
        }

        for (let i = 0; i < n; i++) {
            const spec = specs[i];
            if (!Array.isArray(spec) || spec.length < 2) {
                settleReject(new TypeError("anyOf: each spec must be [source, predicate]"));
                return;
            }
            whenSignal(spec[0], spec[1], { signal: internalCtrl.signal }).then(
                function (v) { settleResolve({ index: i, value: v }); },
                function (e) {
                    if (settled) return;
                    errors[i] = e;
                    rejectedCount = (rejectedCount + 1) | 0;
                    if (rejectedCount === n) {
                        settleReject(new AggregateError(errors, "anyOf: all specs rejected"));
                    }
                }
            );
        }
    });
}

/**
 * Settle on the FIRST spec to settle -- success OR failure. Resolves with
 * `{ index, value }` of the first to satisfy; rejects with the first rejection.
 * Unlike `anyOf`, individual aborts/timeouts of one spec cascade to the bundle.
 *
 * @template T
 * @param {Array<[() => T, (value: T) => unknown]>} specs
 * @param {{ timeout?: number, signal?: AbortSignal }} [opts]
 * @returns {Promise<{ index: number, value: T }>}
 */
function raceOf(specs, opts) {
    if (!Array.isArray(specs)) {
        return Promise.reject(new TypeError("raceOf: specs must be an array"));
    }
    // Validate opts BEFORE the empty-specs shortcut (AW-02): raceOf([], opts)
    // must honor its bundle opts, not silently discard them.
    try { readOpts(opts, "raceOf"); }
    catch (e) { return Promise.reject(e); }
    const userSignal = _optsSignal;
    const timeoutMs  = _optsTimeout;

    if (userSignal !== undefined && userSignal !== null && userSignal.aborted) {
        return Promise.reject(makeAbortError(userSignal));
    }

    const n = specs.length;
    if (n === 0) {
        // Empty race honors bundle opts: a deadline rejects TimeoutError at the
        // deadline; a live signal rejects the abort reason on abort; with
        // neither it stays forever-pending (Promise.race([]) parity). No child
        // exists to propagate a mid-flight abort, so the signal is listened to
        // directly here (the pre-abort case is already short-circuited above).
        if ((timeoutMs === undefined || timeoutMs === Infinity) &&
            (userSignal === undefined || userSignal === null)) {
            return new Promise(function () {});     // matches Promise.race semantics
        }
        return new Promise(function (resolve, reject) {
            let settled = false;
            let timeoutId = null;
            let abortListener = null;
            const finish = function (err) {
                if (settled) return;
                settled = true;
                if (timeoutId !== null) clearTimeout(timeoutId);
                if (abortListener !== null && userSignal !== undefined && userSignal !== null) {
                    userSignal.removeEventListener("abort", abortListener);
                }
                reject(err);
            };
            if (timeoutMs !== undefined && timeoutMs !== Infinity) {
                timeoutId = setTimeout(function () {
                    finish(new TimeoutError(timeoutMs));
                }, timeoutMs);
            }
            if (userSignal !== undefined && userSignal !== null) {
                abortListener = function () { finish(makeAbortError(userSignal)); };
                userSignal.addEventListener("abort", abortListener);
            }
        });
    }

    const internalCtrl = new AbortController();
    const unlink = linkUserSignal(internalCtrl, userSignal);

    return new Promise(function (resolve, reject) {
        let settled = false;
        let timeoutId = null;

        const cleanupShared = function () {
            internalCtrl.abort();
            unlink();
            if (timeoutId !== null) clearTimeout(timeoutId);
        };

        const settleResolve = function (winner) {
            if (settled) return;
            settled = true;
            cleanupShared();
            resolve(winner);
        };

        const settleReject = function (e) {
            if (settled) return;
            settled = true;
            cleanupShared();
            reject(e);
        };

        if (timeoutMs !== undefined && timeoutMs !== Infinity) {
            timeoutId = setTimeout(function () {
                settleReject(new TimeoutError(timeoutMs));
            }, timeoutMs);
        }

        for (let i = 0; i < n; i++) {
            const spec = specs[i];
            if (!Array.isArray(spec) || spec.length < 2) {
                settleReject(new TypeError("raceOf: each spec must be [source, predicate]"));
                return;
            }
            whenSignal(spec[0], spec[1], { signal: internalCtrl.signal }).then(
                function (v) { settleResolve({ index: i, value: v }); },
                function (e) { settleReject(e); }
            );
        }
    });
}

// ---------------------------------------------------------------------------
// Promise wrappers (non-signal-aware; for fetch and other arbitrary promises)
// ---------------------------------------------------------------------------

/**
 * Wrap an arbitrary Promise with a deadline. Rejects with `TimeoutError`
 * after `ms` if the inner promise has not settled. The inner promise is NOT
 * cancelled (it can't be, without an AbortSignal-aware producer) -- it is
 * simply detached from the result.
 *
 * For signal-based work, prefer the `timeout` option on `whenSignal` /
 * `allOf` / `anyOf` / `raceOf`: those primitives' cleanup IS structural
 * because they own their effects.
 *
 * @template T
 * @param {Promise<T>} promise
 * @param {number} ms
 * @returns {Promise<T>}
 */
function withTimeout(promise, ms) {
    // Validate the input that can be misused BEFORE arming any timer (AW-06):
    // a non-thenable rejects at the door with zero timers scheduled. Order:
    // thenability -> identity passthrough (undefined/Infinity) -> ms law.
    if (!isThenable(promise)) {
        return Promise.reject(new TypeError("withTimeout: promise must be a thenable"));
    }
    if (ms === undefined || ms === Infinity) return promise;
    if (!Number.isFinite(ms) || ms < 0) {
        return Promise.reject(new RangeError("withTimeout: ms must be a finite non-negative number"));
    }
    return new Promise(function (resolve, reject) {
        let settled = false;
        const id = setTimeout(function () {
            if (settled) return;
            settled = true;
            reject(new TimeoutError(ms));
        }, ms);
        // Registration-time guard (cold path, AW-06): a hostile thenable whose
        // .then() throws synchronously must not leave the timer armed. Tear it
        // down and route the throw to the outer rejection.
        try {
            promise.then(
                function (v) {
                    if (settled) return;
                    settled = true;
                    clearTimeout(id);
                    resolve(v);
                },
                function (e) {
                    if (settled) return;
                    settled = true;
                    clearTimeout(id);
                    reject(e);
                }
            );
        } catch (err) {
            if (!settled) {
                settled = true;
                clearTimeout(id);
                reject(err);
            }
        }
    });
}

/**
 * Wrap an arbitrary Promise with an AbortSignal. Rejects with an AbortError
 * if the signal aborts before the inner promise settles. Like `withTimeout`,
 * the inner work cannot be cancelled here -- the result is merely detached.
 *
 * For signal-based work, pass the AbortSignal as the `signal` option on the
 * primitive itself; those primitives DO cancel their underlying effects.
 *
 * @template T
 * @param {Promise<T>} promise
 * @param {AbortSignal} abortSignal
 * @returns {Promise<T>}
 */
function withAbort(promise, abortSignal) {
    // Validate the input that can be misused BEFORE adding any listener (AW-05):
    // a non-thenable rejects at the door with zero listeners attached to the
    // caller's (often long-lived, shared) AbortSignal.
    if (!isThenable(promise)) {
        return Promise.reject(new TypeError("withAbort: promise must be a thenable"));
    }
    if (abortSignal === undefined || abortSignal === null) return promise;
    if (abortSignal.aborted) {
        return Promise.reject(makeAbortError(abortSignal));
    }
    return new Promise(function (resolve, reject) {
        let settled = false;
        const listener = function () {
            if (settled) return;
            settled = true;
            abortSignal.removeEventListener("abort", listener);
            reject(makeAbortError(abortSignal));
        };
        abortSignal.addEventListener("abort", listener);
        // Registration-time guard (cold path, AW-05): a hostile thenable whose
        // .then() throws synchronously must not orphan the abort listener on the
        // caller's (often shared) signal. Detach and route the throw to reject.
        try {
            promise.then(
                function (v) {
                    if (settled) return;
                    settled = true;
                    abortSignal.removeEventListener("abort", listener);
                    resolve(v);
                },
                function (e) {
                    if (settled) return;
                    settled = true;
                    abortSignal.removeEventListener("abort", listener);
                    reject(e);
                }
            );
        } catch (err) {
            if (!settled) {
                settled = true;
                abortSignal.removeEventListener("abort", listener);
                reject(err);
            }
        }
    });
}

// ---------------------------------------------------------------------------
// Convenience shorthands
// ---------------------------------------------------------------------------

/**
 * Wait until `source()` becomes truthy. Equivalent to
 * `whenSignal(source, Boolean, opts)`.
 *
 * @template T
 * @param {() => T} source
 * @param {{ timeout?: number, signal?: AbortSignal }} [opts]
 * @returns {Promise<T>}
 */
function whenTruthy(source, opts) {
    return whenSignal(source, Boolean, opts);
}

/**
 * Wait until `source()` equals `target` (via `Object.is`). Equivalent to
 * `whenSignal(source, (v) => Object.is(v, target), opts)`.
 *
 * @template T
 * @param {() => T} source
 * @param {T} target
 * @param {{ timeout?: number, signal?: AbortSignal }} [opts]
 * @returns {Promise<T>}
 */
function whenEquals(source, target, opts) {
    return whenSignal(source, function (v) { return Object.is(v, target); }, opts);
}

// ---------------------------------------------------------------------------
// fromPromise -- signal-shaped async state
// ---------------------------------------------------------------------------

/**
 * Drive a single reactive signal from a Promise's lifecycle. The signal holds
 * one of three shapes:
 *
 *   { status: "pending",  data: initialData, error: undefined }
 *   { status: "resolved", data: <value>,     error: undefined }
 *   { status: "rejected", data: initialData, error: <reason>  }
 *
 * The signal updates EXACTLY ONCE (on settlement). Consumers can read its
 * current shape via `state()` / `state.peek()` / `state.subscribe(fn)`. To
 * release the signal's node back to the lite-signal pool, call the standard
 * `dispose(signal)` from lite-signal.
 *
 * Use this to project async work into the signal-reactive UI loop:
 *
 *   const userQuery = fromPromise(fetchUser(id));
 *   effect(() => {
 *       const s = userQuery();
 *       if (s.status === "pending") renderSpinner();
 *       else if (s.status === "resolved") renderUser(s.data);
 *       else renderError(s.error);
 *   });
 *
 * @template T
 * @param {Promise<T>} promise
 * @param {T} [initialData] Optional placeholder data while pending.
 * @returns {import("@zakkster/lite-signal").Signal<{
 *     status: "pending" | "resolved" | "rejected",
 *     data: T | undefined,
 *     error: unknown
 * }>}
 */
function fromPromise(promise, initialData) {
    // Validate thenability BEFORE acquiring the signal node (AW-04): a
    // non-thenable throws synchronously with zero nodes drawn from lite-signal's
    // bounded pool. A sync throw (not a born-dead signal) keeps misuse loud and
    // the return type honest: "a signal that will transition", never one that is
    // born rejected.
    if (!isThenable(promise)) {
        throw new TypeError("fromPromise: promise must be a thenable");
    }
    const sig = _signal({
        status: "pending",
        data: initialData,
        error: undefined
    });
    let settled = false;
    // Registration-time guard (cold path, AW-04): a hostile thenable whose
    // .then() throws synchronously must not orphan the acquired signal node in
    // lite-signal's bounded pool. Dispose it, then rethrow to preserve the
    // documented sync-loud misuse contract.
    try {
        promise.then(
            function (data) {
                if (settled) return;
                settled = true;
                sig.set({ status: "resolved", data: data, error: undefined });
            },
            function (error) {
                if (settled) return;
                settled = true;
                sig.set({ status: "rejected", data: initialData, error: error });
            }
        );
    } catch (err) {
        dispose(sig);
        throw err;
    }
    return sig;
}

// ---------------------------------------------------------------------------
// whenStatechart -- duck-typed lite-statechart specialization
// ---------------------------------------------------------------------------

/**
 * Resolve when the statechart `machine` enters `stateName`. Uses
 * `machine.onTransition` directly (one observer slot in the listener pool)
 * rather than going through `machine.state` as a reactive read (which would
 * allocate a tracking effect node). One fewer reactive-graph node per wait.
 *
 * Duck-typed: any object exposing `state.peek(): string` and
 * `onTransition(fn): () => void` works -- so this also accepts custom FSMs
 * shaped like lite-statechart.
 *
 * If the machine is ALREADY in `stateName`, resolves on the next microtask.
 *
 * @param {{ state: { peek: () => string }, onTransition: (fn: (from: string, to: string, event: string, payload: unknown) => void) => () => void }} machine
 * @param {string} stateName
 * @param {{ timeout?: number, signal?: AbortSignal }} [opts]
 * @returns {Promise<void>}
 */
function whenStatechart(machine, stateName, opts) {
    if (machine === null || typeof machine !== "object") {
        return Promise.reject(new TypeError("whenStatechart: machine must be an object"));
    }
    if (typeof stateName !== "string") {
        return Promise.reject(new TypeError("whenStatechart: stateName must be a string"));
    }
    if (typeof machine.onTransition !== "function" ||
        machine.state === undefined ||
        typeof machine.state.peek !== "function") {
        return Promise.reject(new TypeError(
            "whenStatechart: machine must expose state.peek() and onTransition(fn)"
        ));
    }

    // Hoist opts validation ABOVE the already-in-state fast path (AW-07): the
    // same invalid call gets the same answer in any machine state.
    try { readOpts(opts, "whenStatechart"); }
    catch (e) { return Promise.reject(e); }
    const userSignal = _optsSignal;
    const timeoutMs  = _optsTimeout;

    if (userSignal !== undefined && userSignal !== null && userSignal.aborted) {
        return Promise.reject(makeAbortError(userSignal));
    }

    if (machine.state.peek() === stateName) {
        return Promise.resolve();
    }

    return new Promise(function (resolve, reject) {
        let settled = false;
        let unsubscribe = null;
        let timeoutId = null;
        let abortListener = null;

        const fullCleanup = function () {
            if (unsubscribe !== null) {
                unsubscribe();
                unsubscribe = null;
            }
            if (timeoutId !== null) {
                clearTimeout(timeoutId);
                timeoutId = null;
            }
            if (abortListener !== null && userSignal !== undefined && userSignal !== null) {
                userSignal.removeEventListener("abort", abortListener);
                abortListener = null;
            }
        };

        const doResolve = function () {
            if (settled) return;
            settled = true;
            fullCleanup();
            resolve();
        };

        const doReject = function (err) {
            if (settled) return;
            settled = true;
            fullCleanup();
            reject(err);
        };

        // timeout was validated by readOpts (Law A); undefined / Infinity arm
        // no timer. Only a finite non-negative value arms one.
        if (timeoutMs !== undefined && timeoutMs !== Infinity) {
            timeoutId = setTimeout(function () {
                doReject(new TimeoutError(timeoutMs));
            }, timeoutMs);
        }

        if (userSignal !== undefined && userSignal !== null) {
            abortListener = function () { doReject(makeAbortError(userSignal)); };
            userSignal.addEventListener("abort", abortListener);
        }

        unsubscribe = machine.onTransition(function (_from, to) {
            if (to === stateName) doResolve();
        });
    });
}

// ---------------------------------------------------------------------------
// Platform-parity primitives (withResolvers / tryFn / allSettledOf / delay)
// ---------------------------------------------------------------------------
// See decisions/0004-parity-surface.md. Own implementations, no native
// feature-detection: identical behavior on every supported node, parity proven
// against the native (where present) and a hand oracle by the T8 tier.

/**
 * The classic executor-capture, as a first-class primitive. Returns a Promise
 * and its two settlement functions, so the resolve/reject can be handed to code
 * that settles later without nesting an executor. Mirrors the ES2024
 * `Promise.withResolvers()`; implemented directly so it behaves identically on
 * nodes older than 22 (where the native is absent).
 *
 * One monomorphic object literal + one Promise per call -- the settlement cost,
 * dies young. No opts, no pooling (Promises are one-shot; see decision 0004).
 *
 * @template T
 * @returns {{ promise: Promise<T>, resolve: (value: T | PromiseLike<T>) => void, reject: (reason?: unknown) => void }}
 */
function withResolvers() {
    let res, rej;
    const promise = new Promise(function (a, b) { res = a; rej = b; });
    return { promise: promise, resolve: res, reject: rej };
}

/**
 * Run `fn` inside a try and adopt its outcome as a Promise: a synchronous
 * return resolves, a synchronous throw REJECTS (never escapes as a sync throw
 * -- the caller always gets a Promise), a returned thenable is adopted. Mirrors
 * the ES2025 `Promise.try()`; implemented directly so it behaves identically on
 * nodes older than 24. A non-function `fn` rejects TypeError at the door.
 *
 * The zero-arg fast path (`arguments.length === 1`, i.e. only `fn`) calls
 * `fn()` with no rest-array allocation. The args path costs exactly one array
 * (the forwarded arguments); that array is the price of arg forwarding and dies
 * young with the call.
 *
 * @template T
 * @param {(...args: any[]) => T | PromiseLike<T>} fn
 * @param {...any} args
 * @returns {Promise<T>}
 */
function tryFn(fn, ...args) {
    if (typeof fn !== "function") {
        return Promise.reject(new TypeError("tryFn: fn must be a function"));
    }
    if (arguments.length === 1) {
        // Zero-arg fast path: no rest array is consumed.
        try { return Promise.resolve(fn()); }
        catch (e) { return Promise.reject(e); }
    }
    // Args path: `args` is the one array allocated here, forwarded to fn.
    try { return Promise.resolve(fn.apply(undefined, args)); }
    catch (e) { return Promise.reject(e); }
}

/**
 * Wait for EVERY spec to settle -- success OR failure -- and resolve with a
 * result array in SPEC order, key-for-key the platform `Promise.allSettled`
 * shape: `{ status: "fulfilled", value }` or `{ status: "rejected", reason }`.
 * A spec rejecting NEVER rejects the bundle (that is the point of allSettled).
 *
 * The bundle rejects ONLY on a bundle-level timeout (W1) or a bundle-level
 * abort (W2 identity: the platform AbortError directly). Empty specs resolve
 * `[]` after opts validation. See decisions/0004-parity-surface.md.
 *
 * @param {Array<[() => unknown, (value: unknown) => unknown]>} specs
 * @param {{ timeout?: number, signal?: AbortSignal }} [opts]
 * @returns {Promise<Array<{ status: "fulfilled", value: unknown } | { status: "rejected", reason: unknown }>>}
 */
function allSettledOf(specs, opts) {
    if (!Array.isArray(specs)) {
        return Promise.reject(new TypeError("allSettledOf: specs must be an array"));
    }
    // Validate opts BEFORE the empty-specs shortcut (same law as allOf): an
    // invalid opts on [] still rejects with the validation error.
    try { readOpts(opts, "allSettledOf"); }
    catch (e) { return Promise.reject(e); }
    const userSignal = _optsSignal;
    const timeoutMs  = _optsTimeout;

    const n = specs.length;
    if (n === 0) return Promise.resolve([]);

    if (userSignal !== undefined && userSignal !== null && userSignal.aborted) {
        return Promise.reject(makeAbortError(userSignal));
    }

    const internalCtrl = new AbortController();
    const unlink = linkUserSignal(internalCtrl, userSignal);

    return new Promise(function (resolve, reject) {
        let settled = false;
        let settledCount = 0;
        const results = new Array(n);
        let timeoutId = null;
        let abortListener = null;

        // Remove BOTH user-signal listeners on settle (linkUserSignal's forward
        // and the direct one armed below), mirroring anyOf's unlinkAll.
        const unlinkAll = function () {
            unlink();
            if (abortListener !== null && userSignal !== undefined && userSignal !== null) {
                userSignal.removeEventListener("abort", abortListener);
                abortListener = null;
            }
        };

        const settleResolve = function (v) {
            if (settled) return;
            settled = true;
            internalCtrl.abort();                        // cancel any in-flight siblings
            unlinkAll();
            if (timeoutId !== null) clearTimeout(timeoutId);
            resolve(v);
        };

        const settleReject = function (e) {
            if (settled) return;
            settled = true;
            internalCtrl.abort(e);                       // cancel in-flight siblings
            unlinkAll();
            if (timeoutId !== null) clearTimeout(timeoutId);
            reject(e);
        };

        if (timeoutMs !== undefined && timeoutMs !== Infinity) {
            timeoutId = setTimeout(function () {
                settleReject(new TimeoutError(timeoutMs));
            }, timeoutMs);
        }

        // AW-03-style direct user-signal listener: a mid-flight bundle abort
        // rejects the platform AbortError directly (settled is set FIRST, so the
        // aborted children's rejections fall into their own handler and land
        // harmlessly / are ignored under the settled guard -- never unhandled).
        if (userSignal !== undefined && userSignal !== null) {
            abortListener = function () { settleReject(makeAbortError(userSignal)); };
            userSignal.addEventListener("abort", abortListener);
        }

        for (let i = 0; i < n; i++) {
            const spec = specs[i];
            if (!Array.isArray(spec) || spec.length < 2) {
                settleReject(new TypeError("allSettledOf: each spec must be [source, predicate]"));
                return;
            }
            // Both handlers are attached at child creation -- the rejection
            // handler is NEVER absent, so an aborted sibling's rejection always
            // lands here and never escapes as an unhandledRejection. On the
            // bundle-reject path `settled` is already true, so the write is
            // skipped and the counter is not advanced (harmless no-op).
            whenSignal(spec[0], spec[1], { signal: internalCtrl.signal }).then(
                function (v) {
                    if (settled) return;
                    results[i] = { status: "fulfilled", value: v };
                    settledCount = (settledCount + 1) | 0;
                    if (settledCount === n) settleResolve(results);
                },
                function (reason) {
                    if (settled) return;
                    results[i] = { status: "rejected", reason: reason };
                    settledCount = (settledCount + 1) | 0;
                    if (settledCount === n) settleResolve(results);
                }
            );
        }
    });
}

/**
 * Resolve after `ms` milliseconds -- a one-shot timer as a cancellable Promise.
 * `opts.signal` aborts the wait with the signal's reason AND clears the timer
 * (structural cleanup). Obeys the W1 timeout law for `ms`: `undefined` or
 * `Infinity` never settles (no timer armed); `NaN` / negative / non-number
 * reject RangeError at the door. A pre-aborted signal rejects immediately.
 *
 * `opts` accepts ONLY `signal` -- `ms` IS the timeout, so a nested `timeout`
 * key is an unknown-key error (readDelayOpts). See decisions/0004.
 *
 * @param {number} ms
 * @param {{ signal?: AbortSignal }} [opts]
 * @returns {Promise<void>}
 */
function delay(ms, opts) {
    if (ms !== undefined && ms !== Infinity && (!Number.isFinite(ms) || ms < 0)) {
        return Promise.reject(new RangeError(
            "delay: ms must be a finite non-negative number, Infinity, or undefined"));
    }
    let signal;
    try { signal = readDelayOpts(opts, "delay"); }
    catch (e) { return Promise.reject(e); }

    if (signal !== undefined && signal !== null && signal.aborted) {
        return Promise.reject(makeAbortError(signal));
    }

    // Infinity / undefined == never settles. With no signal there is nothing to
    // clean up, so return a bare forever-pending Promise (zero machinery). With
    // a signal, fall through: the abort listener below must still be able to
    // reject an abortable forever-wait, and no timer is armed for Infinity.
    const noTimer = ms === undefined || ms === Infinity;
    if (noTimer && (signal === undefined || signal === null)) {
        return new Promise(function () {});
    }

    return new Promise(function (resolve, reject) {
        let settled = false;
        let timeoutId = null;
        let abortListener = null;

        const finish = function () {
            if (timeoutId !== null) {
                clearTimeout(timeoutId);
                timeoutId = null;
            }
            if (abortListener !== null && signal !== undefined && signal !== null) {
                signal.removeEventListener("abort", abortListener);
                abortListener = null;
            }
        };

        if (!noTimer) {
            timeoutId = setTimeout(function () {
                if (settled) return;
                settled = true;
                finish();
                resolve();
            }, ms);
        }

        if (signal !== undefined && signal !== null) {
            abortListener = function () {
                if (settled) return;
                settled = true;
                finish();
                reject(makeAbortError(signal));
            };
            signal.addEventListener("abort", abortListener);
        }
    });
}

// ---------------------------------------------------------------------------
// Consumer-pull helpers (withRetry / mapLimit)
// ---------------------------------------------------------------------------
// See decisions/0005-consumer-helpers.md. Both build ON the shipped foundations
// (tryFn + delay + the allSettledOf settlement discipline) and inherit the W1
// timeout law + the W2 abort identity. Registration + settlement primitives:
// per attempt / item they allocate one AbortController + one tryFn args array
// (dies young); nothing is retained after settlement, and the whenSignal
// per-fire hot body is untouched.

// setTimeout's safe ceiling: a delay past 2**31 - 1 ms overflows the platform
// timer and emits a TimeoutOverflowWarning (then fires almost immediately).
const MAX_BACKOFF_MS = 2147483647;

// Compute the backoff before attempt k+1: baseMs * factor**(k-1), jittered by
// +/- `jitter` fraction. Cold path (runs between attempts, never per-fire), so
// Math.random is fine (D-jitter: deterministic-seed injection rejected as
// over-engineering a cold path). jitter <= 1 keeps the result non-negative.
function retryBackoff(baseMs, factor, jitter, k) {
    let ms = baseMs * Math.pow(factor, k - 1);
    if (jitter > 0 && ms > 0) {
        const spread = ms * jitter;
        ms = ms - spread + Math.random() * 2 * spread;
    }
    // Fail closed on the cold backoff path: a pathological factor**(k-1) can
    // overflow ms to Infinity (delay(Infinity) hangs forever) or, with baseMs 0,
    // to 0*Infinity = NaN (delay(NaN) rejects RangeError). Floor a non-finite
    // result to 0 and clamp to setTimeout's safe max so the backoff is always a
    // legal finite delay that never trips a TimeoutOverflowWarning.
    if (!(ms >= 0)) return 0;
    return ms > MAX_BACKOFF_MS ? MAX_BACKOFF_MS : ms;
}

// An AbortError from ANY source is terminal and is never routed through the
// retryable predicate (D2). A cancellation is not a transient failure.
function isAbortError(err) {
    return err !== null && err !== undefined && err.name === "AbortError";
}

/**
 * Retry a factory with exponential backoff until it resolves, the attempts are
 * exhausted, or the bundle is cancelled. Each attempt gets a FRESH per-attempt
 * `AbortController` linked to the bundle `signal` (a new controller each attempt
 * so aborting attempt k never bleeds into k+1), and runs through `tryFn` so a
 * sync-throwing factory becomes a failed attempt on the uniform error channel.
 *
 * Laws (see decisions/0005):
 *   - Exhaustion rejects the LAST attempt's error (D1).
 *   - A bundle abort (signal or the `timeout` budget) short-circuits and rejects
 *     the abort reason / TimeoutError DIRECTLY (D2, W2 identity); `retryable` is
 *     never consulted for an AbortError.
 *   - `timeout` is the TOTAL budget across all attempts + backoffs (D3), not
 *     per-attempt -- per-attempt cancellation is the factory's job via its
 *     per-attempt signal.
 *   - `retryable` defaults to retrying every non-abort error (D5); a throwing
 *     predicate rejects the bundle with the thrown value.
 *
 * @template T
 * @param {(signal: AbortSignal) => T | PromiseLike<T>} fn   Per-attempt factory.
 * @param {{ attempts: number, baseMs?: number, factor?: number, jitter?: number,
 *           signal?: AbortSignal, timeout?: number,
 *           retryable?: (error: unknown, attempt: number) => boolean }} opts
 * @returns {Promise<T>}
 */
function withRetry(fn, opts) {
    if (typeof fn !== "function") {
        return Promise.reject(new TypeError("withRetry: fn must be a function"));
    }
    try { readRetryOpts(opts, "withRetry"); }
    catch (e) { return Promise.reject(e); }
    const attempts   = _retryAttempts;
    const baseMs     = _retryBaseMs;
    const factor     = _retryFactor;
    const jitter     = _retryJitter;
    const retryable  = _retryRetryable;
    const bundleSig  = _retrySignal;
    const timeoutMs  = _retryTimeout;

    if (bundleSig !== undefined && bundleSig !== null && bundleSig.aborted) {
        return Promise.reject(makeAbortError(bundleSig));
    }

    // A bundle-INTERNAL controller is the single cancellation root, exactly like
    // allOf / raceOf / allSettledOf. linkUserSignal forwards a user abort into
    // it; the bundle timer and every reject path abort it. The per-attempt
    // controller AND the backoff delay both hang off internalCtrl.signal, so a
    // bundle timeout that fires DURING a non-zero backoff tears that sleep down
    // (its timer + listener) -- honoring the TOTAL-budget contract (D3) and
    // leaving zero orphaned timer/listener on the caller's signal.
    const internalCtrl = new AbortController();
    const unlink = linkUserSignal(internalCtrl, bundleSig);

    return new Promise(function (resolve, reject) {
        let settled = false;
        let timeoutId = null;
        let abortListener = null;

        // Tear down every bundle-level resource: the linkUserSignal forward, the
        // bundle timer, and the direct user-abort listener. Mirrors anyOf /
        // allSettledOf's unlinkAll + the combinator timer clear.
        const removeBundle = function () {
            unlink();
            if (timeoutId !== null) { clearTimeout(timeoutId); timeoutId = null; }
            if (abortListener !== null && bundleSig !== undefined && bundleSig !== null) {
                bundleSig.removeEventListener("abort", abortListener);
                abortListener = null;
            }
        };

        const settleResolve = function (v) {
            if (settled) return;
            settled = true;
            // No internalCtrl.abort() here: the resolving attempt has already
            // settled (its link listener was removed by the success handler) and
            // no backoff is in flight, so there is nothing to cancel -- skipping
            // the abort keeps the happy path free of the captured-stack
            // DOMException garbage (same discipline as allOf's settleResolve).
            removeBundle();
            resolve(v);
        };

        const settleReject = function (e) {
            if (settled) return;
            settled = true;
            internalCtrl.abort(e);   // cancels the in-flight attempt AND any backoff delay
            removeBundle();
            reject(e);
        };

        // D3: one bundle-level timer as the TOTAL budget across attempts + backoffs.
        if (timeoutMs !== undefined && timeoutMs !== Infinity) {
            timeoutId = setTimeout(function () {
                settleReject(new TimeoutError(timeoutMs));
            }, timeoutMs);
        }

        // D2: direct bundle-abort listener -- sets `settled` FIRST so an
        // in-flight attempt's rejection lands under the guard (never retried,
        // never unhandled). Mirrors anyOf / allSettledOf.
        if (bundleSig !== undefined && bundleSig !== null) {
            abortListener = function () { settleReject(makeAbortError(bundleSig)); };
            bundleSig.addEventListener("abort", abortListener);
        }

        const runAttempt = function (k) {
            if (settled) return;
            // A fresh controller per attempt, chained off the INTERNAL signal so
            // aborting internalCtrl (any reject path: timeout, user abort,
            // non-retryable failure) cancels the in-flight attempt. A new
            // controller each attempt so aborting attempt k never bleeds into k+1.
            const attemptCtrl = new AbortController();
            const unlinkAttempt = linkUserSignal(attemptCtrl, internalCtrl.signal);
            tryFn(fn, attemptCtrl.signal).then(
                function (v) {
                    unlinkAttempt();
                    settleResolve(v);
                },
                function (err) {
                    unlinkAttempt();
                    if (settled) return;                 // bundle already aborted / timed out
                    if (isAbortError(err)) {             // D2: abort is never a retry
                        settleReject(err);
                        return;
                    }
                    if (k >= attempts) {                 // D1: exhaustion -> last error
                        settleReject(err);
                        return;
                    }
                    let again;
                    if (retryable === undefined) {
                        again = true;                    // D5: default retries non-abort
                    } else {
                        try { again = !!retryable(err, k); }
                        catch (pe) { settleReject(pe); return; }  // D5: broken predicate fails closed
                    }
                    if (!again) {
                        settleReject(err);
                        return;
                    }
                    // Backoff on the INTERNAL signal (abortable, cleanup-proven),
                    // then the next attempt. A bundle timeout OR user abort during
                    // the sleep aborts internalCtrl -> the delay clears its timer +
                    // removes its listener and rejects, observed here and ignored
                    // since the bundle is already settled.
                    delay(retryBackoff(baseMs, factor, jitter, k), { signal: internalCtrl.signal }).then(
                        function () { if (!settled) runAttempt(k + 1); },
                        function () { /* delay aborted: bundle already settled */ }
                    );
                }
            );
        };

        runAttempt(1);
    });
}

/**
 * Map `items` through an async factory with a concurrency cap of `limit`,
 * resolving with the results in INPUT order. Launches up to `limit` at once; as
 * each settles, the next pending is launched until `items` is exhausted. Each
 * item runs through `tryFn` off a shared internal `AbortController` (linked to
 * the bundle signal), with BOTH handlers attached at creation (the allSettledOf
 * discipline) so an aborted pending / in-flight item never escapes as an
 * unhandledRejection.
 *
 * The FIRST rejection aborts the shared controller (cancelling in-flight items),
 * stops launching pending items, and rejects the bundle with that error -- or
 * the abort reason directly on a bundle abort (W2), or `TimeoutError` on the
 * bundle deadline (W1). Empty `items` resolve `[]` after validation. The factory
 * is called `(item, index, signal)` (D4, major-locked).
 *
 * @template I, T
 * @param {ReadonlyArray<I>} items
 * @param {(item: I, index: number, signal: AbortSignal) => T | PromiseLike<T>} fn
 * @param {number} limit                Max concurrent, an integer >= 1.
 * @param {{ signal?: AbortSignal, timeout?: number }} [opts]
 * @returns {Promise<T[]>}
 */
function mapLimit(items, fn, limit, opts) {
    if (!Array.isArray(items)) {
        return Promise.reject(new TypeError("mapLimit: items must be an array"));
    }
    if (typeof fn !== "function") {
        return Promise.reject(new TypeError("mapLimit: fn must be a function"));
    }
    // limit at the door: integer >= 1. 0 / negative / 1.5 / NaN / "8" / true all
    // fail Number.isInteger or the bound -> RangeError before any acquisition.
    if (!Number.isInteger(limit) || limit < 1) {
        return Promise.reject(new RangeError("mapLimit: limit must be an integer >= 1"));
    }
    try { readOpts(opts, "mapLimit"); }
    catch (e) { return Promise.reject(e); }
    const userSignal = _optsSignal;
    const timeoutMs  = _optsTimeout;

    const n = items.length;
    if (n === 0) return Promise.resolve([]);

    if (userSignal !== undefined && userSignal !== null && userSignal.aborted) {
        return Promise.reject(makeAbortError(userSignal));
    }

    const internalCtrl = new AbortController();
    const unlink = linkUserSignal(internalCtrl, userSignal);

    return new Promise(function (resolve, reject) {
        let settled = false;
        let timeoutId = null;
        let abortListener = null;
        let completed = 0;              // stack int -- results landed
        let next = 0;                   // stack int -- index of the next item to launch
        const results = new Array(n);

        const unlinkAll = function () {
            unlink();
            if (abortListener !== null && userSignal !== undefined && userSignal !== null) {
                userSignal.removeEventListener("abort", abortListener);
                abortListener = null;
            }
        };

        const settleResolve = function (v) {
            if (settled) return;
            settled = true;
            // No internalCtrl.abort() here: resolve only fires when completed === n
            // (every item already settled), so there is nothing in flight to
            // cancel. Skipping the abort keeps the happy path free of the
            // AbortController's captured-stack DOMException garbage.
            unlinkAll();
            if (timeoutId !== null) clearTimeout(timeoutId);
            resolve(v);
        };

        const settleReject = function (e) {
            if (settled) return;
            settled = true;
            internalCtrl.abort(e);      // cancel in-flight items
            unlinkAll();
            if (timeoutId !== null) clearTimeout(timeoutId);
            reject(e);
        };

        if (timeoutMs !== undefined && timeoutMs !== Infinity) {
            timeoutId = setTimeout(function () {
                settleReject(new TimeoutError(timeoutMs));
            }, timeoutMs);
        }

        // Direct bundle-abort listener (W2 identity), same shape as the
        // combinators: settled FIRST so an aborted item's rejection is observed.
        if (userSignal !== undefined && userSignal !== null) {
            abortListener = function () { settleReject(makeAbortError(userSignal)); };
            userSignal.addEventListener("abort", abortListener);
        }

        // Launch exactly one pending item (if any and not settled). Each item's
        // success handler launches the next, so concurrency never exceeds the
        // initial `limit` fill below. results[i] preserves INPUT order.
        const startOne = function () {
            if (settled || next >= n) return;
            const i = next;
            next = (next + 1) | 0;
            tryFn(fn, items[i], i, internalCtrl.signal).then(
                function (v) {
                    if (settled) return;
                    results[i] = v;
                    completed = (completed + 1) | 0;
                    if (completed === n) { settleResolve(results); return; }
                    startOne();
                },
                function (e) { settleReject(e); }   // first rejection sinks the bundle
            );
        };

        for (let slot = 0; slot < limit && slot < n; slot++) startOne();
    });
}

// ---------------------------------------------------------------------------
// createAwaitScope -- one controller, N pre-bound awaiters, one teardown
// ---------------------------------------------------------------------------
// Cold-path factory (decisions/0007). Builds ONE bound surface object over a
// single AbortController; every signal-aware primitive is pre-bound to
// scope.signal so a consumer (lite-room's Room.js:5047 awaitRoomSignal) stops
// hand-rolling the per-call controller + close-hook + cleanup dance.
//
// Two per-call paths (D2):
//   - NO per-call signal: scope.signal flows straight through (zero link). The
//     primitive's own abort listener surfaces the scope reason unflattened via
//     makeAbortError, and removes itself on settle -- no orphan surface.
//   - BOTH scope.signal AND a per-call signal: an internal controller is fed by
//     BOTH through the EXISTING linkUserSignal (reason-preserving), and BOTH
//     links are released on EVERY settlement path via the p.then(release,
//     release) wrapper -- the exact Room.js:5047 cleanup shape.
//
// Module-scoped out-param for the prepared link, read by the caller BEFORE any
// re-entrant call (the underlying primitive never calls prepScopeOpts, so there
// is no re-entrancy; the value is read synchronously right after prep).

/**
 * Build a scope binding N signal-aware awaiters to one AbortController.
 *
 * @param {AbortController} [ctrl]  Omitted => the scope OWNS a fresh controller
 *   (scope.abort works). Passed => the scope BORROWS it (scope.abort throws).
 * @returns {object} the bound surface (see decisions/0007 D4).
 */
function createAwaitScope(ctrl) {
    // D1: own vs borrow. undefined => OWN a fresh controller. Anything else must
    // be AbortController-shaped => BORROW. null is not "omitted" (null is not
    // zero): a non-undefined, non-controller ctrl is a fail-closed TypeError.
    let owned;
    let controller;
    if (ctrl === undefined) {
        controller = new AbortController();
        owned = true;
    } else if (ctrl !== null && typeof ctrl === "object" &&
        typeof ctrl.abort === "function" &&
        ctrl.signal !== undefined && ctrl.signal !== null &&
        typeof ctrl.signal.aborted === "boolean" &&
        typeof ctrl.signal.addEventListener === "function" &&
        typeof ctrl.signal.removeEventListener === "function") {
        controller = ctrl;
        owned = false;
    } else {
        throw new TypeError(
            "createAwaitScope: ctrl must be an AbortController " +
            "(abort() plus an AbortSignal-shaped .signal), or omitted to own a fresh one");
    }

    const scopeSignal = controller.signal;

    // D3: ONE reused fast-path opts object. readOpts reads its two keys
    // synchronously and never retains it, so sharing it across every
    // no-per-call-signal call -- even concurrent in-flight waits -- is safe.
    const scopeOpts = { signal: scopeSignal };

    // Prepare the effective opts + (optional) link for one scoped call. Sets
    // _scopeLink to a release() when the BOTH path armed an internal link, else
    // null. Returns the opts object to hand the underlying primitive.
    const prepScopeOpts = function (opts) {
        if (opts === undefined || opts === null) {
            _scopeLink = null;
            return scopeOpts;
        }
        // Snapshot every own-enumerable key ONCE (honors the single-read law and
        // preserves any typo'd key so the underlying reader still rejects it).
        const merged = Object.assign({}, opts);
        const perSignal = merged.signal;
        if (perSignal === undefined || perSignal === null) {
            merged.signal = scopeSignal;
            _scopeLink = null;
            return merged;
        }
        // Shape-check the per-call signal BEFORE linking anything (mirrors
        // readOpts' door, Await.js:140-147). A malformed per-call signal must
        // behave EXACTLY like the bare primitive: a rejected promise from the
        // underlying reader, never a sync throw here AND never a forwarded
        // listener orphaned on the long-lived scope.signal. So arm no link, leave
        // merged.signal untouched, and let the primitive's readOpts reject it.
        if (typeof perSignal.aborted !== "boolean" ||
            typeof perSignal.addEventListener !== "function" ||
            typeof perSignal.removeEventListener !== "function") {
            _scopeLink = null;
            return merged;
        }
        // BOTH path: link scope.signal AND the per-call signal into one internal
        // controller (reason-preserving via linkUserSignal). Release BOTH on
        // every settlement path (D2).
        const internalCtrl = new AbortController();
        const unlinkScope = linkUserSignal(internalCtrl, scopeSignal);
        const unlinkUser = linkUserSignal(internalCtrl, perSignal);
        merged.signal = internalCtrl.signal;
        _scopeLink = function release() { unlinkScope(); unlinkUser(); };
        return merged;
    };

    return {
        get signal() { return scopeSignal; },
        get aborted() { return scopeSignal.aborted; },
        abort: function (reason) {
            // D1: only an owned controller may be aborted through the scope.
            if (!owned) {
                throw new Error(
                    "createAwaitScope: this scope BORROWS its controller -- abort it " +
                    "through the controller you passed, not through scope.abort()");
            }
            controller.abort(reason);
        },
        whenSignal: function (source, predicate, opts) {
            const eff = prepScopeOpts(opts); const link = _scopeLink;
            const p = whenSignal(source, predicate, eff);
            return link === null ? p : wrapScopeRelease(p, link);
        },
        allOf: function (specs, opts) {
            const eff = prepScopeOpts(opts); const link = _scopeLink;
            const p = allOf(specs, eff);
            return link === null ? p : wrapScopeRelease(p, link);
        },
        anyOf: function (specs, opts) {
            const eff = prepScopeOpts(opts); const link = _scopeLink;
            const p = anyOf(specs, eff);
            return link === null ? p : wrapScopeRelease(p, link);
        },
        raceOf: function (specs, opts) {
            const eff = prepScopeOpts(opts); const link = _scopeLink;
            const p = raceOf(specs, eff);
            return link === null ? p : wrapScopeRelease(p, link);
        },
        allSettledOf: function (specs, opts) {
            const eff = prepScopeOpts(opts); const link = _scopeLink;
            const p = allSettledOf(specs, eff);
            return link === null ? p : wrapScopeRelease(p, link);
        },
        withRetry: function (fn, opts) {
            const eff = prepScopeOpts(opts); const link = _scopeLink;
            const p = withRetry(fn, eff);
            return link === null ? p : wrapScopeRelease(p, link);
        },
        mapLimit: function (items, fn, limit, opts) {
            const eff = prepScopeOpts(opts); const link = _scopeLink;
            const p = mapLimit(items, fn, limit, eff);
            return link === null ? p : wrapScopeRelease(p, link);
        },
        delay: function (ms, opts) {
            const eff = prepScopeOpts(opts); const link = _scopeLink;
            const p = delay(ms, eff);
            return link === null ? p : wrapScopeRelease(p, link);
        },
        whenStatechart: function (machine, stateName, opts) {
            const eff = prepScopeOpts(opts); const link = _scopeLink;
            const p = whenStatechart(machine, stateName, eff);
            return link === null ? p : wrapScopeRelease(p, link);
        },
        whenTruthy: function (source, opts) {
            const eff = prepScopeOpts(opts); const link = _scopeLink;
            const p = whenTruthy(source, eff);
            return link === null ? p : wrapScopeRelease(p, link);
        },
        whenEquals: function (source, target, opts) {
            const eff = prepScopeOpts(opts); const link = _scopeLink;
            const p = whenEquals(source, target, eff);
            return link === null ? p : wrapScopeRelease(p, link);
        }
    };
}

// Module-scoped out-param: the link release prepared by prepScopeOpts, read
// synchronously by the scoped method before it hands off to the primitive.
let _scopeLink = null;

// Wrap a scoped promise so BOTH links release on every settlement path (D2),
// preserving the underlying value/rejection (the Room.js:5047 cleanup shape).
// Only the BOTH path (per-call signal present) reaches here -- never the hot
// straight-through path.
function wrapScopeRelease(p, release) {
    return p.then(
        function (v) { release(); return v; },
        function (e) { release(); throw e; }
    );
}

// ---------------------------------------------------------------------------
// Exports
// ---------------------------------------------------------------------------

export {
    // Core primitives
    whenSignal,
    allOf,
    anyOf,
    raceOf,
    allSettledOf,
    // Platform-parity primitives
    withResolvers,
    tryFn,
    delay,
    // Consumer-pull helpers
    withRetry,
    mapLimit,
    // Promise wrappers (non-signal-aware)
    withTimeout,
    withAbort,
    // Convenience shorthands
    whenTruthy,
    whenEquals,
    // Bidirectional bridge
    fromPromise,
    // Specializations
    whenStatechart,
    // Scopes
    createAwaitScope,
    // Errors
    TimeoutError,
    // Metadata
    VERSION
};
