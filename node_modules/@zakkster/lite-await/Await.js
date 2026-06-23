// @zakkster/lite-await 1.0.0
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

import { effect, untrack, signal as _signal } from "@zakkster/lite-signal";

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
    const userSignal = (opts !== undefined && opts !== null) ? opts.signal : undefined;
    const timeoutMs  = (opts !== undefined && opts !== null) ? opts.timeout : undefined;

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

        if (timeoutMs !== undefined) {
            if (!Number.isFinite(timeoutMs) || timeoutMs < 0) {
                reject(new RangeError("whenSignal: timeout must be a finite non-negative number"));
                return;
            }
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
    const n = specs.length;
    if (n === 0) return Promise.resolve([]);

    const userSignal = (opts !== undefined && opts !== null) ? opts.signal : undefined;
    if (userSignal !== undefined && userSignal !== null && userSignal.aborted) {
        return Promise.reject(makeAbortError(userSignal));
    }

    const internalCtrl = new AbortController();
    const timeoutMs  = (opts !== undefined && opts !== null) ? opts.timeout : undefined;
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

        if (timeoutMs !== undefined) {
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
    const n = specs.length;
    if (n === 0) {
        return Promise.reject(new AggregateError([], "anyOf: empty specs"));
    }

    const userSignal = (opts !== undefined && opts !== null) ? opts.signal : undefined;
    if (userSignal !== undefined && userSignal !== null && userSignal.aborted) {
        return Promise.reject(makeAbortError(userSignal));
    }

    const internalCtrl = new AbortController();
    const timeoutMs  = (opts !== undefined && opts !== null) ? opts.timeout : undefined;
    const unlink = linkUserSignal(internalCtrl, userSignal);

    return new Promise(function (resolve, reject) {
        let settled = false;
        let rejectedCount = 0;
        const errors = new Array(n);
        let timeoutId = null;

        const settleResolve = function (winner) {
            if (settled) return;
            settled = true;
            internalCtrl.abort();                        // cancel siblings
            unlink();
            if (timeoutId !== null) clearTimeout(timeoutId);
            resolve(winner);
        };

        const settleReject = function (e) {
            if (settled) return;
            settled = true;
            internalCtrl.abort(e);
            unlink();
            if (timeoutId !== null) clearTimeout(timeoutId);
            reject(e);
        };

        if (timeoutMs !== undefined) {
            timeoutId = setTimeout(function () {
                settleReject(new TimeoutError(timeoutMs));
            }, timeoutMs);
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
    const n = specs.length;
    if (n === 0) {
        return new Promise(function () {});         // matches Promise.race semantics
    }

    const userSignal = (opts !== undefined && opts !== null) ? opts.signal : undefined;
    if (userSignal !== undefined && userSignal !== null && userSignal.aborted) {
        return Promise.reject(makeAbortError(userSignal));
    }

    const internalCtrl = new AbortController();
    const timeoutMs  = (opts !== undefined && opts !== null) ? opts.timeout : undefined;
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

        if (timeoutMs !== undefined) {
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
    const sig = _signal({
        status: "pending",
        data: initialData,
        error: undefined
    });
    let settled = false;
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

    const userSignal = (opts !== undefined && opts !== null) ? opts.signal : undefined;
    const timeoutMs  = (opts !== undefined && opts !== null) ? opts.timeout : undefined;

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

        if (timeoutMs !== undefined) {
            if (!Number.isFinite(timeoutMs) || timeoutMs < 0) {
                reject(new RangeError("whenStatechart: timeout must be a finite non-negative number"));
                return;
            }
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
// Exports
// ---------------------------------------------------------------------------

export {
    // Core primitives
    whenSignal,
    allOf,
    anyOf,
    raceOf,
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
    // Errors
    TimeoutError
};
