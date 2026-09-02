// @zakkster/lite-stream 1.3.0
//
// Zero-GC bridge between async iterators and @zakkster/lite-signal. The
// multi-shot dual of lite-await's fromPromise: project an async source of N
// values (paginated APIs, SSE streams, network frame queues, pubsub topics)
// into a signal-shaped reactive surface.
//
// Three cleanup paths, all structural:
//   1. Iterator natural completion ({done: true})  -> signal state goes done
//   2. Iterator throws                              -> signal state goes error
//   3. Caller-provided AbortSignal aborts           -> iterator.return() called
//
// Two modes:
//   - "latest" (default): signal value = most recent yielded value. Cheap;
//     consumer reads sig().value. Best for "show the current frame / latest
//     pubsub message / live cursor position".
//   - "buffer":           signal value = bounded ring of recent values, newest
//     last. REQUIRES maxBuffer (no unbounded buffering -- that is a memory
//     bug pretending to be a feature). Overflow drops oldest and increments
//     droppedCount. Best for "process every helix page in order, don't miss
//     events".
//
// 1.1 added `mode: "latest"`, `filter`, `timeout`, `Symbol.asyncDispose`,
// multi-waiter FIFO queue, and `overflowCount` on `toAsyncIterable`. See
// CHANGELOG.md [1.1.0] and llms.txt (toAsyncIterable section) for the full
// 1.1 story.
//
// Copyright (c) 2026 Zahary Shinikchiev <shinikchiev@yahoo.com>
// MIT License

import { signal as _signal } from "@zakkster/lite-signal";

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/**
 * Thrown when a `timeout` deadline elapses before iterator settlement.
 * Introduced in 1.1 alongside `toAsyncIterable`'s `timeout` option.
 * Structurally identical to lite-await's `TimeoutError` so users can
 * duck-check via `e.name === "TimeoutError"` across both packages -- but
 * NOT imported from lite-await, to preserve lite-stream's zero-dep story.
 *
 * A structural twin of lite-await's TimeoutError, deliberately not imported
 * (zero-dep by design), carrying `name === "TimeoutError"` and a `.timeout`
 * field (the elapsed deadline in ms). The duck-check is suite-wide vocabulary;
 * renaming it would be a breaking change everywhere it is consumed.
 */
class TimeoutError extends Error {
    constructor(timeoutMs) {
        super("lite-stream: timed out after " + timeoutMs + "ms");
        this.name = "TimeoutError";
        this.timeout = timeoutMs;
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

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

// Obtain an async iterator from an async iterable. Accepts both forms
// (iterator-already-iterator, or iterable-needing-Symbol.asyncIterator).
function toIterator(source) {
    if (source === null || source === undefined) {
        throw new TypeError("lite-stream: source must be an async iterable or iterator");
    }
    // Already an iterator (has next())
    if (typeof source.next === "function") return source;
    // Async iterable (has Symbol.asyncIterator)
    if (typeof source[Symbol.asyncIterator] === "function") {
        return source[Symbol.asyncIterator]();
    }
    // Sync iterable (has Symbol.iterator) -- accepted as a convenience.
    if (typeof source[Symbol.iterator] === "function") {
        return source[Symbol.iterator]();
    }
    throw new TypeError(
        "lite-stream: source must be an async iterable, async iterator, or iterable"
    );
}

// Best-effort iterator close. Async iterators may expose `return()` to signal
// "no more values will be pulled"; calling it releases resources held by the
// generator. We swallow any error -- the consumer already moved on.
function closeIterator(iter) {
    if (iter !== null && typeof iter.return === "function") {
        try {
            const ret = iter.return();
            // `return()` returns a Promise. We don't await it; we just need
            // to give the generator a chance to clean up. Attach a noop catch
            // so an unhandled rejection doesn't blow up the process.
            if (ret !== undefined && typeof ret.then === "function") {
                ret.then(undefined, () => {});
            }
        } catch (_e) { /* swallowed: cleanup is best-effort */ }
    }
}

// ---------------------------------------------------------------------------
// State shapes (per-mode)
// ---------------------------------------------------------------------------
//
// "latest" mode signal value:
//   { value, count, done, error }
//
// "buffer" mode signal value:
//   { values, count, droppedCount, done, error }
//
// We allocate ONE state object per signal lifecycle and mutate-in-place via
// `signal.set(state)`. But lite-signal compares with Object.is; mutating the
// same object would cause subsequent set()s to be no-ops. So we shallow-clone
// on every update. That clone is the only per-yield JS-heap allocation in the
// hot path (besides the iterator's own intrinsic cost).
//
// For "buffer" mode, the underlying `values` array is the user-facing snapshot;
// we allocate a fresh array on overflow and on each set to avoid in-place
// mutation that would be visible to past observers. The ring buffer beneath
// the snapshot is a fixed-size Array that we cycle through.

// ---------------------------------------------------------------------------
// fromAsyncIterable
// ---------------------------------------------------------------------------

/**
 * Drive a signal from an async iterator. Returns a Signal whose value is a
 * tagged state object reflecting the iterator's lifecycle.
 *
 * In "latest" mode (default), the signal value is the most recent yielded
 * value. In "buffer" mode, the signal value is a bounded array of recent
 * values (newest last); `maxBuffer` is REQUIRED -- unbounded buffering is
 * rejected.
 *
 * Termination on any of:
 *   - iterator natural completion ({done: true})
 *   - iterator throws
 *   - opts.signal aborts (the iterator's return() is called best-effort)
 *
 * Disposing the result signal does NOT stop the pump: the iterator keeps
 * pulling. AbortSignal or natural completion are the only stop mechanisms.
 * A disposed signal's set() is a silent no-op, so the pump never learns of
 * the disposal; an infinite source becomes an unbounded background loop.
 * Pass opts.signal and abort it to stop early. See ROADMAP LS-01.
 *
 * @template T
 * @param {AsyncIterable<T> | AsyncIterator<T> | Iterable<T>} source
 * @param {{
 *     mode?: "latest" | "buffer",
 *     maxBuffer?: number,
 *     initial?: T,
 *     signal?: AbortSignal,
 *     onError?: (err: unknown) => void,
 *     onDone?:  () => void
 * }} [opts]
 * @returns {import("@zakkster/lite-signal").Signal<unknown>}
 */
function fromAsyncIterable(source, opts) {
    const mode      = (opts !== undefined && opts !== null) ? (opts.mode || "latest") : "latest";
    const maxBuffer = (opts !== undefined && opts !== null) ? opts.maxBuffer : undefined;
    const initial   = (opts !== undefined && opts !== null) ? opts.initial   : undefined;
    const abortSig  = (opts !== undefined && opts !== null) ? opts.signal    : undefined;
    const onError   = (opts !== undefined && opts !== null) ? opts.onError   : undefined;
    const onDone    = (opts !== undefined && opts !== null) ? opts.onDone    : undefined;

    if (mode !== "latest" && mode !== "buffer") {
        throw new TypeError(
            "lite-stream: opts.mode must be \"latest\" or \"buffer\" (got " + JSON.stringify(mode) + ")"
        );
    }
    if (mode === "buffer") {
        if (typeof maxBuffer !== "number" || !Number.isFinite(maxBuffer) || maxBuffer < 1 || (maxBuffer | 0) !== maxBuffer) {
            throw new RangeError(
                "lite-stream: \"buffer\" mode requires opts.maxBuffer to be a positive integer "
                    + "(got " + JSON.stringify(maxBuffer) + "). Unbounded buffering is a memory "
                    + "bug pretending to be a feature; pick a deliberate ceiling."
            );
        }
    }

    // Initial state.
    let state;
    if (mode === "latest") {
        state = { value: initial, count: 0, done: false, error: undefined };
    } else {
        state = { values: [], count: 0, droppedCount: 0, done: false, error: undefined };
    }
    const sig = _signal(state);
    // Track current state locally so we never depend on sig.peek() in the
    // pump's resolve handler. If the consumer disposes the signal between
    // construction and the next pull, sig.peek() returns undefined; reading
    // .count or .droppedCount off undefined would crash the pump and surface
    // as an unhandledRejection. We mutate lastState in lockstep with sig.set()
    // and use a guarded write helper to short-circuit when the signal has
    // been disposed externally.
    let lastState = state;

    // Pre-aborted: settle synchronously, never start the iterator.
    if (abortSig !== undefined && abortSig !== null && abortSig.aborted) {
        const err = makeAbortError(abortSig);
        const errState = makeErrorState(lastState, err, mode);
        lastState = errState;
        try { sig.set(errState); } catch (_e) {}
        if (onError !== undefined) {
            try { onError(err); } catch (_e) {}
        }
        return sig;
    }

    // Invalid source -- throw synchronously. Construction-time input errors
    // are a programmer bug; surfacing them via signal state would just defer
    // a crash to a later read.
    const iter = toIterator(source);

    let stopped = false;
    let abortListener = null;

    const cleanup = function () {
        if (abortListener !== null && abortSig !== undefined && abortSig !== null) {
            abortSig.removeEventListener("abort", abortListener);
            abortListener = null;
        }
    };

    const stop = function (cause) {
        if (stopped) return;
        stopped = true;
        cleanup();
        closeIterator(iter);
        // `cause` is null on natural done; an Error otherwise.
        let terminalState;
        if (cause === null) {
            terminalState = makeDoneState(lastState, mode);
        } else {
            terminalState = makeErrorState(lastState, cause, mode);
        }
        lastState = terminalState;
        // sig.set may throw or no-op if the signal was disposed externally;
        // we've already recorded the terminal state in lastState.
        try { sig.set(terminalState); } catch (_e) {}
        if (cause === null) {
            if (onDone !== undefined) {
                try { onDone(); } catch (_e) {}
            }
        } else {
            if (onError !== undefined) {
                try { onError(cause); } catch (_e) {}
            }
        }
    };

    if (abortSig !== undefined && abortSig !== null) {
        abortListener = function () { stop(makeAbortError(abortSig)); };
        abortSig.addEventListener("abort", abortListener);
    }

    // Ring buffer for "buffer" mode. Fixed-size, indexed via a head pointer
    // that wraps. On overflow, head advances over the oldest slot. The
    // user-facing `values` snapshot is rebuilt on each yield by reading the
    // ring in order; cheap because maxBuffer is a chosen ceiling.
    let ring     = null;
    let ringHead = 0;
    let ringLen  = 0;
    if (mode === "buffer") {
        ring = new Array(maxBuffer);
    }

    // The pump. Pull values until done/error/abort. We use a recursive .then
    // chain rather than for-await so that we can hand a stable handle to the
    // iterator and bail synchronously on stop without leaving a dangling
    // for-await waiter.
    function pump() {
        if (stopped) return;
        let nextPromise;
        try {
            nextPromise = iter.next();
        } catch (err) {
            // Synchronous throw from .next() (rare; mostly sync iterators
            // adapted into the async path).
            stop(err);
            return;
        }
        Promise.resolve(nextPromise).then(
            function (result) {
                if (stopped) return;
                if (result === null || typeof result !== "object") {
                    stop(new TypeError("lite-stream: iterator.next() returned non-object"));
                    return;
                }
                // LS-13: a throwing done/value getter on the result routes to
                // the "iterator throws" leg (stop(err)). Wrap ONLY the two
                // property reads -- widening this try over sig.set() below would
                // swallow the LS-01 peer-defense catch semantics. Each property
                // is read exactly once into a local.
                let rDone, v;
                try {
                    rDone = result.done;
                    v = result.value;
                } catch (err) {
                    stop(err);
                    return;
                }
                if (rDone === true) {
                    stop(null);
                    return;
                }
                // Build the new state from lastState (never sig.peek), then
                // commit to both lastState and sig. The try/catch around set()
                // is defensive against a peer whose set() throws; on a throw we
                // bail and tear down the iterator rather than pull into the
                // void. Disposal is NOT the trigger -- set() after dispose() is
                // a silent no-op in lite-signal 1.2.2 and 1.5.0 (see LS-01).
                let newState;
                if (mode === "latest") {
                    newState = {
                        value: v,
                        count: (lastState.count + 1) | 0,
                        done: false,
                        error: undefined
                    };
                } else {
                    // Buffer mode: push into ring; build snapshot array.
                    let dropped = lastState.droppedCount;
                    if (ringLen < maxBuffer) {
                        ring[(ringHead + ringLen) % maxBuffer] = v;
                        ringLen = (ringLen + 1) | 0;
                    } else {
                        // Overwrite at ringHead (oldest); advance head.
                        ring[ringHead] = v;
                        ringHead = (ringHead + 1) % maxBuffer;
                        dropped = (dropped + 1) | 0;
                    }
                    // Snapshot: a fresh array in newest-last order.
                    const values = new Array(ringLen);
                    for (let i = 0; i < ringLen; i = (i + 1) | 0) {
                        values[i] = ring[(ringHead + i) % maxBuffer];
                    }
                    newState = {
                        values: values,
                        count: (lastState.count + 1) | 0,
                        droppedCount: dropped,
                        done: false,
                        error: undefined
                    };
                }
                lastState = newState;
                try {
                    sig.set(newState);
                } catch (_e) {
                    // Defensive against a peer signal whose set() throws. This
                    // is NOT disposal detection: set() after dispose() is a
                    // silent no-op in lite-signal 1.2.2 and 1.5.0 (probe P0),
                    // so this branch has never fired against any published
                    // peer. If it ever does, tear down without firing onError.
                    stopped = true;
                    cleanup();
                    closeIterator(iter);
                    return;
                }
                // pump() is called DIRECTLY here -- no explicit deferral at
                // this site. The stack is already unwound because this
                // continuation runs from a .then() handler, i.e. a fresh
                // microtask. Proven safe: 1,000,000 synchronous values drained
                // in ~80ms with no stack overflow (probe P4, node v26.3.1,
                // 2026-09-01).
                pump();
            },
            function (err) {
                stop(err);
            }
        );
    }

    // Kick off. queueMicrotask to defer the first pull until after the caller
    // can subscribe; we don't want a synchronous yield to fire before the
    // caller's effect() is wired.
    queueMicrotask(pump);

    return sig;
}

// Build a terminal state object preserving the most recent value/values.
function makeDoneState(prevState, mode) {
    if (mode === "latest") {
        return {
            value: prevState.value,
            count: prevState.count,
            done:  true,
            error: undefined
        };
    }
    return {
        values: prevState.values,
        count: prevState.count,
        droppedCount: prevState.droppedCount,
        done:  true,
        error: undefined
    };
}

function makeErrorState(prevState, err, mode) {
    if (mode === "latest") {
        return {
            value: prevState.value,
            count: prevState.count,
            done:  true,
            error: err
        };
    }
    return {
        values: prevState.values,
        count: prevState.count,
        droppedCount: prevState.droppedCount,
        done:  true,
        error: err
    };
}

// ---------------------------------------------------------------------------
// pipeToSignal
// ---------------------------------------------------------------------------

/**
 * Lower-level companion to `fromAsyncIterable`: pump an existing signal from
 * an async iterator. The signal's value is replaced directly with each yielded
 * value (no wrapper state). Returns a `stop` function that ends the pump,
 * calls iterator.return(), and removes the abort listener.
 *
 * Use this when:
 *   - You already have a signal you want to drive
 *   - You don't want the {value, count, done, error} wrapper
 *   - You want a stop fn instead of an AbortController for cleanup
 *
 * NOTE: this does NOT dispose the target signal. The caller owns its lifetime.
 * Disposing the target signal does NOT stop the pump: call the returned
 * stop fn or abort opts.signal. See ROADMAP LS-01.
 *
 * Enrichment (1.3.0, all ADDITIVE -- the un-optioned path is byte-identical to
 * 1.1.0): `mode: "buffer"` (with REQUIRED `maxBuffer`) drives the target with a
 * bounded newest-last snapshot window instead of the raw value, dropping oldest
 * on overflow and counting the drops; `onValue(v)` is a per-value tap that fires
 * once BEFORE each set (a throw routes through the same leg as a throwing
 * transform -- stop + onError); `onAbort(reason)` receives aborts INSTEAD of
 * onError when present (so consumers stop filtering intentional aborts out of
 * their error handler). The returned stop fn carries `droppedCount` and
 * `overflowCount` getters (aliases of the same overflow counter); it stays
 * callable and idempotent exactly as 1.1.0.
 *
 * @template T
 * @param {AsyncIterable<T> | AsyncIterator<T>} source
 * @param {import("@zakkster/lite-signal").Signal<T>} target
 * @param {{
 *     signal?:  AbortSignal,
 *     onError?: (err: unknown) => void,
 *     onDone?:  () => void,
 *     transform?: (value: T) => T,
 *     mode?: "latest" | "buffer",
 *     maxBuffer?: number,
 *     onValue?: (value: T) => void,
 *     onAbort?: (reason: unknown) => void
 * }} [opts]
 * @returns {(() => void) & { readonly droppedCount: number, readonly overflowCount: number }} stop fn (idempotent)
 */
function pipeToSignal(source, target, opts) {
    if (target === null || target === undefined || typeof target.set !== "function") {
        throw new TypeError("lite-stream: pipeToSignal target must be a writable signal");
    }
    const abortSig  = (opts !== undefined && opts !== null) ? opts.signal    : undefined;
    const onError   = (opts !== undefined && opts !== null) ? opts.onError   : undefined;
    const onDone    = (opts !== undefined && opts !== null) ? opts.onDone    : undefined;
    const transform = (opts !== undefined && opts !== null) ? opts.transform : undefined;
    const mode      = (opts !== undefined && opts !== null) ? (opts.mode || "latest") : "latest";
    const maxBuffer = (opts !== undefined && opts !== null) ? opts.maxBuffer : undefined;
    const onValue   = (opts !== undefined && opts !== null) ? opts.onValue   : undefined;
    const onAbort   = (opts !== undefined && opts !== null) ? opts.onAbort   : undefined;

    // Validate ONCE at construction. Unknown mode / latest+maxBuffer are
    // programmer bugs surfaced synchronously; buffer mode requires a deliberate
    // ceiling (unbounded buffering is a memory bug pretending to be a feature).
    if (mode !== "latest" && mode !== "buffer") {
        throw new TypeError(
            "lite-stream: opts.mode must be \"latest\" or \"buffer\" (got " + JSON.stringify(mode) + ")"
        );
    }
    if (mode === "latest" && maxBuffer !== undefined) {
        throw new TypeError(
            "lite-stream: opts.maxBuffer is not allowed with mode: \"latest\" -- latest-wins sets the raw value"
        );
    }
    if (mode === "buffer") {
        if (typeof maxBuffer !== "number" || !Number.isFinite(maxBuffer) || maxBuffer < 1 || (maxBuffer | 0) !== maxBuffer) {
            throw new RangeError(
                "lite-stream: \"buffer\" mode requires opts.maxBuffer to be a positive integer "
                    + "(got " + JSON.stringify(maxBuffer) + "). Unbounded buffering is a memory "
                    + "bug pretending to be a feature; pick a deliberate ceiling."
            );
        }
    }
    const isBuffer = mode === "buffer";

    // Overflow counter. droppedCount and overflowCount are aliases (a drop is
    // an overflow), matching toAsyncIterable's vocabulary. Both getters on the
    // returned stop fn read this one variable; no per-value allocation.
    let dropped = 0;
    // Attach the observability getters to whatever stop fn we return. Closes
    // over `dropped`; defined once at construction, never on the hot path.
    const attachCounters = function (fn) {
        Object.defineProperty(fn, "droppedCount", { get() { return dropped; }, enumerable: true, configurable: true });
        Object.defineProperty(fn, "overflowCount", { get() { return dropped; }, enumerable: true, configurable: true });
        return fn;
    };

    if (abortSig !== undefined && abortSig !== null && abortSig.aborted) {
        const err = makeAbortError(abortSig);
        if (onAbort !== undefined) {
            try { onAbort(err); } catch (_e) {}
        } else if (onError !== undefined) {
            try { onError(err); } catch (_e) {}
        }
        return attachCounters(function noopStop() {});
    }

    const iter = toIterator(source);
    let stopped = false;
    let abortListener = null;

    // Buffer-mode ring: fixed-size, head-pointer wrap, oldest dropped on
    // overflow. Snapshot rebuilt newest-last per set (fromAsyncIterable pattern).
    let ring     = null;
    let ringHead = 0;
    let ringLen  = 0;
    if (isBuffer) ring = new Array(maxBuffer);

    const stop = function () {
        if (stopped) return;
        stopped = true;
        if (abortListener !== null && abortSig !== undefined && abortSig !== null) {
            abortSig.removeEventListener("abort", abortListener);
            abortListener = null;
        }
        closeIterator(iter);
    };

    if (abortSig !== undefined && abortSig !== null) {
        abortListener = function () {
            const err = makeAbortError(abortSig);
            stop();
            // onAbort, when present, receives aborts INSTEAD of onError.
            if (onAbort !== undefined) {
                try { onAbort(err); } catch (_e) {}
            } else if (onError !== undefined) {
                try { onError(err); } catch (_e) {}
            }
        };
        abortSig.addEventListener("abort", abortListener);
    }

    function pump() {
        if (stopped) return;
        let nextPromise;
        try { nextPromise = iter.next(); }
        catch (err) {
            stop();
            if (onError !== undefined) {
                try { onError(err); } catch (_e) {}
            }
            return;
        }
        Promise.resolve(nextPromise).then(
            function (result) {
                if (stopped) return;
                if (result === null || typeof result !== "object") {
                    stop();
                    if (onError !== undefined) {
                        try { onError(new TypeError("lite-stream: iterator.next() returned non-object")); } catch (_e) {}
                    }
                    return;
                }
                // LS-13: a throwing done/value getter routes to the "iterator
                // throws" leg (stop + onError). Wrap ONLY the two property reads
                // -- widening this try over transform()/target.set() below would
                // fold the transform-throw leg into this one. Each property is
                // read exactly once into a local.
                let rDone, rValue;
                try {
                    rDone = result.done;
                    rValue = result.value;
                } catch (err) {
                    stop();
                    if (onError !== undefined) {
                        try { onError(err); } catch (_e) {}
                    }
                    return;
                }
                if (rDone === true) {
                    stop();
                    if (onDone !== undefined) {
                        try { onDone(); } catch (_e) {}
                    }
                    return;
                }
                // transform -> onValue tap -> set. A throw anywhere here routes
                // to the same stop + onError leg (the 1.1.0 transform-throw
                // precedent; see test/04 "transform throwing stops the pump").
                try {
                    const v = transform === undefined ? rValue : transform(rValue);
                    if (onValue !== undefined) onValue(v);
                    if (isBuffer) {
                        if (ringLen < maxBuffer) {
                            ring[(ringHead + ringLen) % maxBuffer] = v;
                            ringLen = (ringLen + 1) | 0;
                        } else {
                            ring[ringHead] = v;
                            ringHead = (ringHead + 1) % maxBuffer;
                            dropped = (dropped + 1) | 0;
                        }
                        const snapshot = new Array(ringLen);
                        for (let i = 0; i < ringLen; i = (i + 1) | 0) {
                            snapshot[i] = ring[(ringHead + i) % maxBuffer];
                        }
                        target.set(snapshot);
                    } else {
                        target.set(v);
                    }
                } catch (err) {
                    stop();
                    if (onError !== undefined) {
                        try { onError(err); } catch (_e) {}
                    }
                    return;
                }
                pump();
            },
            function (err) {
                stop();
                if (onError !== undefined) {
                    try { onError(err); } catch (_e) {}
                }
            }
        );
    }

    queueMicrotask(pump);
    return attachCounters(stop);
}

// ---------------------------------------------------------------------------
// Symbol.asyncDispose feature detection (Node 20+)
// ---------------------------------------------------------------------------
// Cached once at module load. When the runtime lacks Symbol.asyncDispose
// (Node 18/19), the iterable's disposer property is simply absent;
// iter.return() is the portable path.

const ASYNC_DISPOSE = (typeof Symbol !== "undefined" && Symbol.asyncDispose)
    ? Symbol.asyncDispose
    : null;

// ---------------------------------------------------------------------------
// toAsyncIterable -- the reverse direction
// ---------------------------------------------------------------------------

/**
 * Yield signal changes as an async iterable. Each change resolves a pending
 * `next()` call. Two backpressure modes (1.1):
 *
 *   - "buffer" (default; matches 1.0.0): FIFO ring buffer of maxBuffer size;
 *     on overflow the OLDEST is dropped and both droppedCount and
 *     overflowCount are incremented.
 *   - "latest" (1.1): single mutable slot; producer overwrites; every
 *     overwrite bumps overflowCount (and droppedCount for compat).
 *
 * 1.1 also adds `filter` (per-value gate; throwing filter rejects the
 * current next() and terminates without a writer surface), `timeout`
 * (overall deadline; rejects with TimeoutError), Symbol.asyncDispose
 * on Node 20+, and a multi-waiter queue that fixes the 1.0.0 latent bug
 * where Promise.all([iter.next(), iter.next()]) silently lost the first
 * resolver.
 *
 * See CHANGELOG.md [1.1.0] and llms.txt (toAsyncIterable section) for the
 * full locked semantics.
 *
 * @template T
 * @param {import("@zakkster/lite-signal").Signal<T> | import("@zakkster/lite-signal").Computed<T>} sig
 * @param {{
 *     signal?:      AbortSignal,
 *     emitInitial?: boolean,
 *     maxBuffer?:   number,
 *     mode?:        "latest" | "buffer",
 *     filter?:      (v: T) => unknown,
 *     timeout?:     number
 * }} [opts]
 * @returns {AsyncIterable<T> & { readonly droppedCount: number, readonly overflowCount: number }}
 */
function toAsyncIterable(sig, opts) {
    // --- Validation ---
    if (sig === null || sig === undefined || typeof sig.subscribe !== "function" || typeof sig.peek !== "function") {
        throw new TypeError(
            "lite-stream: toAsyncIterable expects a readable lite-signal (Signal/Computed with .peek and .subscribe)"
        );
    }
    const abortSig    = (opts !== undefined && opts !== null) ? opts.signal      : undefined;
    const emitInitial = (opts !== undefined && opts !== null && opts.emitInitial === false) ? false : true;
    const filter      = (opts !== undefined && opts !== null) ? opts.filter      : undefined;
    const timeoutMs   = (opts !== undefined && opts !== null) ? opts.timeout     : undefined;
    const modeOpt     = (opts !== undefined && opts !== null) ? opts.mode        : undefined;
    const maxBufferOpt = (opts !== undefined && opts !== null) ? opts.maxBuffer  : undefined;

    // Mode. Default "buffer" preserves 1.0.0 behavior.
    let mode;
    if (modeOpt === undefined || modeOpt === "buffer") {
        mode = "buffer";
    } else if (modeOpt === "latest") {
        mode = "latest";
    } else {
        throw new TypeError(
            "lite-stream: opts.mode must be \"latest\" or \"buffer\" (got " + JSON.stringify(modeOpt) + ")"
        );
    }
    const isLatestWins = mode === "latest";

    // maxBuffer only meaningful in buffer mode. Combining latest + maxBuffer
    // is user error; silently ignoring one would hide the mistake.
    if (isLatestWins && maxBufferOpt !== undefined) {
        throw new TypeError(
            "lite-stream: opts.maxBuffer is not allowed with mode: \"latest\" -- latest-wins uses a single slot"
        );
    }
    const maxBuffer = maxBufferOpt !== undefined ? maxBufferOpt : 1024;
    if (!isLatestWins) {
        if (typeof maxBuffer !== "number" || !Number.isFinite(maxBuffer) || maxBuffer < 1 || (maxBuffer | 0) !== maxBuffer) {
            throw new RangeError(
                "lite-stream: opts.maxBuffer must be a positive integer (got " + JSON.stringify(maxBuffer) + ")"
            );
        }
    }

    if (filter !== undefined && typeof filter !== "function") {
        throw new TypeError("lite-stream: opts.filter must be a function");
    }
    if (timeoutMs !== undefined) {
        if (typeof timeoutMs !== "number" || !Number.isFinite(timeoutMs) || timeoutMs < 0) {
            throw new RangeError(
                "lite-stream: opts.timeout must be a finite non-negative number (got " + JSON.stringify(timeoutMs) + ")"
            );
        }
    }

    // --- State ---
    // Buffer-mode ring (unchanged from 1.0.0 layout); null in latest mode.
    const queue = isLatestWins ? null : new Array(maxBuffer);
    let qHead   = 0;
    let qLen    = 0;
    // Latest-mode slot.
    let pendingValue;
    let hasPending = false;
    // Overflow. `droppedCount` and `overflowCount` are aliases for the same
    // counter; 1.0.0 exposed `droppedCount`, 1.1 adds `overflowCount` as the
    // mode-neutral vocabulary. Both getters read this variable.
    let overflowCount = 0;
    // Multi-waiter FIFO queue (fixes the 1.0.0 single-slot bug where
    // concurrent .next() calls silently lost the first resolver).
    const waiters = [];
    // Termination
    let done = false;
    let firstNextErr = null;      // stashed for the next .next() when no waiter was pending
    // Cleanup handles
    let unsubscribe = null;
    let unsubscribePending = false;
    let abortListener = null;
    let timeoutId = null;

    // Late-binding unsubscribe: needed when a throwing filter (or throwing
    // subscription callback body) fires during the synchronous initial
    // subscribe call, at which point `unsubscribe` is still null. Mirror of
    // lite-await's stop/stopPending pattern.
    function doUnsubscribe() {
        if (unsubscribe !== null) {
            unsubscribe();
            unsubscribe = null;
        } else {
            unsubscribePending = true;
        }
    }

    function fullCleanup() {
        doUnsubscribe();
        if (timeoutId !== null) {
            clearTimeout(timeoutId);
            timeoutId = null;
        }
        if (abortListener !== null && abortSig !== undefined && abortSig !== null) {
            abortSig.removeEventListener("abort", abortListener);
            abortListener = null;
        }
        // Release unread values so they can be GC'd before the iterable
        // object itself is. One-time cost per iterable lifetime.
        pendingValue = undefined;
        hasPending = false;
        if (queue !== null) {
            for (let i = 0; i < maxBuffer; i = (i + 1) | 0) queue[i] = undefined;
            qHead = 0;
            qLen  = 0;
        }
    }

    // Termination via error: reject all pending waiters, or stash for the
    // next .next() call. Route for timeout and throwing-filter paths.
    function terminateWithError(err) {
        if (done) return;
        done = true;
        fullCleanup();
        if (waiters.length > 0) {
            while (waiters.length > 0) {
                const w = waiters.shift();
                w.reject(err);
            }
        } else {
            firstNextErr = err;
        }
    }

    // Termination via done (abort, natural end via consumer return()): resolve
    // pending waiters with {done: true}. Preserves 1.0.0 abort-is-graceful
    // behavior (mid-iteration abort ends for-await without throw).
    function terminateAsDone() {
        if (done) return;
        done = true;
        fullCleanup();
        while (waiters.length > 0) {
            const w = waiters.shift();
            w.resolve({ value: undefined, done: true });
        }
    }

    function enqueue(v) {
        if (done) return;
        // Deliver directly to the oldest waiter if any.
        if (waiters.length > 0) {
            const w = waiters.shift();
            w.resolve({ value: v, done: false });
            return;
        }
        // Otherwise stash in the mode-appropriate structure.
        if (isLatestWins) {
            if (hasPending) overflowCount = (overflowCount + 1) | 0;
            pendingValue = v;
            hasPending = true;
        } else {
            if (qLen < maxBuffer) {
                queue[(qHead + qLen) % maxBuffer] = v;
                qLen = (qLen + 1) | 0;
            } else {
                // Drop oldest.
                queue[qHead] = v;
                qHead = (qHead + 1) % maxBuffer;
                overflowCount = (overflowCount + 1) | 0;
            }
        }
    }

    // --- Iterable object ---
    const iterable = {
        [Symbol.asyncIterator]() { return this; },
        next() {
            if (done) {
                if (firstNextErr !== null) {
                    const err = firstNextErr;
                    firstNextErr = null;
                    return Promise.reject(err);
                }
                return Promise.resolve({ value: undefined, done: true });
            }
            // Value available in slot / ring?
            if (isLatestWins && hasPending) {
                const v = pendingValue;
                pendingValue = undefined;
                hasPending = false;
                return Promise.resolve({ value: v, done: false });
            }
            if (!isLatestWins && qLen > 0) {
                const v = queue[qHead];
                queue[qHead] = undefined;
                qHead = (qHead + 1) % maxBuffer;
                qLen = (qLen - 1) | 0;
                return Promise.resolve({ value: v, done: false });
            }
            // Nothing available -- queue a waiter.
            return new Promise(function (resolve, reject) {
                waiters.push({ resolve: resolve, reject: reject });
            });
        },
        return(value) {
            if (done) return Promise.resolve({ value: undefined, done: true });
            terminateAsDone();
            return Promise.resolve({ value: value, done: true });
        },
        throw(err) {
            if (done) return Promise.reject(err);
            // Consumer-driven throw: pending waiters reject with the error,
            // subsequent next() returns done. Same shape as terminateWithError
            // but without stashing (throw() call site gets its own rejection).
            done = true;
            fullCleanup();
            while (waiters.length > 0) {
                const w = waiters.shift();
                w.reject(err);
            }
            return Promise.reject(err);
        },
        get droppedCount() { return overflowCount; },
        get overflowCount() { return overflowCount; }
    };

    if (ASYNC_DISPOSE !== null) {
        iterable[ASYNC_DISPOSE] = function () { return iterable.return(); };
    }

    // --- Pre-aborted signal: mark done, skip all subscription/timer setup.
    // Matches 1.0.0 behavior: first next() resolves with {done: true} rather
    // than rejecting. ---
    if (abortSig !== undefined && abortSig !== null && abortSig.aborted) {
        done = true;
        return iterable;
    }

    // --- Timeout setup ---
    if (timeoutMs !== undefined) {
        timeoutId = setTimeout(function () {
            terminateWithError(new TimeoutError(timeoutMs));
        }, timeoutMs);
    }

    // --- AbortSignal listener ---
    if (abortSig !== undefined && abortSig !== null) {
        abortListener = function () { terminateAsDone(); };
        abortSig.addEventListener("abort", abortListener);
    }

    // --- Subscribe. lite-signal's subscribe fires synchronously with the
    // current value on registration; use a flag to suppress that initial
    // fire if emitInitial is false. Filter (if provided) applies to the
    // initial fire when emitInitial is true. A throwing filter is routed
    // via terminateWithError so the throw never surfaces at the signal
    // writer's .set() call site. ---
    let suppressedInitial = !emitInitial;
    unsubscribe = sig.subscribe(function (v) {
        if (done) return;
        if (suppressedInitial) { suppressedInitial = false; return; }
        if (filter !== undefined) {
            let ok;
            try {
                ok = filter(v);
            } catch (e) {
                terminateWithError(e);
                return;
            }
            if (!ok) return;
        }
        enqueue(v);
    });

    // If terminateWithError was called from inside the synchronous initial
    // subscribe fire (throwing filter on the initial value), honor the
    // deferred unsubscribe now that we have the handle.
    if (unsubscribePending && unsubscribe !== null) {
        unsubscribe();
        unsubscribe = null;
    }

    return iterable;
}

// ---------------------------------------------------------------------------
// Exports
// ---------------------------------------------------------------------------

export {
    fromAsyncIterable,
    pipeToSignal,
    toAsyncIterable,
    TimeoutError
};

// Always equals the installed package.json version (single source of truth is
// package.json; the /release drill bumps both sites in the same commit).
export const VERSION = "1.3.0";
