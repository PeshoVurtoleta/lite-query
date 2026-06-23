// @zakkster/lite-stream 1.0.0
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
// Copyright (c) 2026 Zahary Shinikchiev <shinikchiev@yahoo.com>
// MIT License

import { signal as _signal } from "@zakkster/lite-signal";

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
 * To stop the stream without an AbortSignal, you can either:
 *   - dispose the returned signal via lite-signal's dispose(sig); pending
 *     iterator pulls will see the signal as disposed on next set() (no-op),
 *     but the iterator continues until natural completion. PREFER AbortSignal.
 *   - pass opts.signal and abort it externally.
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
                if (result.done === true) {
                    stop(null);
                    return;
                }
                const v = result.value;
                // Build the new state from lastState (never sig.peek), then
                // commit to both lastState and sig. If sig.set throws (consumer
                // disposed the signal externally), bail and tear down the
                // iterator so we don't keep pulling into the void.
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
                    // Signal disposed externally. Tear down quietly without
                    // firing onError -- this is consumer-initiated cleanup,
                    // not a stream error.
                    stopped = true;
                    cleanup();
                    closeIterator(iter);
                    return;
                }
                // Defer the next pull through the microtask queue so back-to-back
                // synchronous yields don't blow the stack.
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
 *
 * @template T
 * @param {AsyncIterable<T> | AsyncIterator<T>} source
 * @param {import("@zakkster/lite-signal").Signal<T>} target
 * @param {{
 *     signal?:  AbortSignal,
 *     onError?: (err: unknown) => void,
 *     onDone?:  () => void,
 *     transform?: (value: T) => T
 * }} [opts]
 * @returns {() => void} stop fn (idempotent)
 */
function pipeToSignal(source, target, opts) {
    if (target === null || target === undefined || typeof target.set !== "function") {
        throw new TypeError("lite-stream: pipeToSignal target must be a writable signal");
    }
    const abortSig  = (opts !== undefined && opts !== null) ? opts.signal    : undefined;
    const onError   = (opts !== undefined && opts !== null) ? opts.onError   : undefined;
    const onDone    = (opts !== undefined && opts !== null) ? opts.onDone    : undefined;
    const transform = (opts !== undefined && opts !== null) ? opts.transform : undefined;

    if (abortSig !== undefined && abortSig !== null && abortSig.aborted) {
        if (onError !== undefined) {
            try { onError(makeAbortError(abortSig)); } catch (_e) {}
        }
        return function noopStop() {};
    }

    const iter = toIterator(source);
    let stopped = false;
    let abortListener = null;

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
            if (onError !== undefined) {
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
                if (result.done === true) {
                    stop();
                    if (onDone !== undefined) {
                        try { onDone(); } catch (_e) {}
                    }
                    return;
                }
                try {
                    target.set(transform === undefined ? result.value : transform(result.value));
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
    return stop;
}

// ---------------------------------------------------------------------------
// toAsyncIterable -- the reverse direction
// ---------------------------------------------------------------------------

/**
 * Yield signal changes as an async iterable. Each change to the signal
 * resolves a pending `next()` call. If the consumer is slower than the
 * producer, values queue in an internal bounded buffer; on overflow, the
 * OLDEST queued value is dropped and `droppedCount` is incremented. The
 * dropped count is observable via the iterable's `.droppedCount` property
 * (read after consuming).
 *
 * The iterator naturally completes when `opts.signal` aborts; iterator.return()
 * resolves cleanly.
 *
 * @template T
 * @param {import("@zakkster/lite-signal").Signal<T> | import("@zakkster/lite-signal").Computed<T>} sig
 * @param {{
 *     signal?:     AbortSignal,
 *     emitInitial?: boolean,    // default true: yield the current value first
 *     maxBuffer?:   number       // default 1024
 * }} [opts]
 * @returns {AsyncIterable<T> & { readonly droppedCount: number }}
 */
function toAsyncIterable(sig, opts) {
    if (sig === null || sig === undefined || typeof sig.subscribe !== "function" || typeof sig.peek !== "function") {
        throw new TypeError(
            "lite-stream: toAsyncIterable expects a readable lite-signal (Signal/Computed with .peek and .subscribe)"
        );
    }
    const abortSig    = (opts !== undefined && opts !== null) ? opts.signal      : undefined;
    const emitInitial = (opts !== undefined && opts !== null && opts.emitInitial === false) ? false : true;
    const maxBuffer   = (opts !== undefined && opts !== null && opts.maxBuffer !== undefined) ? opts.maxBuffer : 1024;

    if (typeof maxBuffer !== "number" || !Number.isFinite(maxBuffer) || maxBuffer < 1 || (maxBuffer | 0) !== maxBuffer) {
        throw new RangeError(
            "lite-stream: opts.maxBuffer must be a positive integer (got " + JSON.stringify(maxBuffer) + ")"
        );
    }

    const queue   = new Array(maxBuffer);
    let qHead     = 0;
    let qLen      = 0;
    let droppedCount = 0;
    let pendingResolve = null;
    let done = false;
    let unsubscribe = null;
    let abortListener = null;

    function enqueue(v) {
        if (done) return;
        if (pendingResolve !== null) {
            const r = pendingResolve;
            pendingResolve = null;
            r({ value: v, done: false });
            return;
        }
        if (qLen < maxBuffer) {
            queue[(qHead + qLen) % maxBuffer] = v;
            qLen = (qLen + 1) | 0;
        } else {
            // Drop oldest.
            queue[qHead] = v;
            qHead = (qHead + 1) % maxBuffer;
            droppedCount = (droppedCount + 1) | 0;
        }
    }

    function teardown() {
        if (done) return;
        done = true;
        if (unsubscribe !== null) { unsubscribe(); unsubscribe = null; }
        if (abortListener !== null && abortSig !== undefined && abortSig !== null) {
            abortSig.removeEventListener("abort", abortListener);
            abortListener = null;
        }
        if (pendingResolve !== null) {
            const r = pendingResolve;
            pendingResolve = null;
            r({ value: undefined, done: true });
        }
    }

    if (abortSig !== undefined && abortSig !== null) {
        if (abortSig.aborted) {
            done = true;
        } else {
            abortListener = function () { teardown(); };
            abortSig.addEventListener("abort", abortListener);
        }
    }

    // Subscribe to signal changes. lite-signal's subscribe fires once
    // synchronously with the current value when called; we use a flag to
    // suppress that initial fire if emitInitial is false.
    let suppressedInitial = !emitInitial;
    if (!done) {
        unsubscribe = sig.subscribe(function (v) {
            if (suppressedInitial) { suppressedInitial = false; return; }
            enqueue(v);
        });
    }

    const iterable = {
        [Symbol.asyncIterator]() { return this; },
        next() {
            if (qLen > 0) {
                const v = queue[qHead];
                queue[qHead] = undefined;
                qHead = (qHead + 1) % maxBuffer;
                qLen = (qLen - 1) | 0;
                return Promise.resolve({ value: v, done: false });
            }
            if (done) return Promise.resolve({ value: undefined, done: true });
            return new Promise(function (resolve) { pendingResolve = resolve; });
        },
        return(value) {
            teardown();
            return Promise.resolve({ value: value, done: true });
        },
        throw(err) {
            teardown();
            return Promise.reject(err);
        },
        get droppedCount() { return droppedCount; }
    };

    return iterable;
}

// ---------------------------------------------------------------------------
// Exports
// ---------------------------------------------------------------------------

export {
    fromAsyncIterable,
    pipeToSignal,
    toAsyncIterable
};
