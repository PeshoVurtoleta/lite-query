// @zakkster/lite-query/stream
//
// Streaming queries -- a multi-shot, iterator-backed query built on
// @zakkster/lite-stream. Subscribe by key to an async iterable (SSE, websocket
// frames, paginated cursors, pubsub topics); the values are pumped into the
// SAME queryClient cache entry a query() would use, so getQueryData /
// invalidate / removeQueries all operate on a stream uniformly.
//
// Design (locked in ROADMAP.md):
//   - Unified cache. A stream entry lives in qc's entry map alongside query
//     entries; the entry shape carries uniform stream slots (isStream,
//     streamStop, streamRestart, streamCount, streamDropped) so it stays
//     monomorphic. No extra signal node is allocated -- the pump writes into
//     the entry's existing data/error/status signals via lite-stream's
//     pipeToSignal. That means ZERO extra signals to dispose (the whole entry
//     is released by the client's disposeEntry on GC / removeQueries / clear),
//     and the latest-mode hot path is one signal write per frame, zero alloc.
//   - Status: idle -> pending (subscribed, 0 values) -> streaming (>=1 value,
//     not done) -> success (iterator done) | error (threw). loading() ==
//     pending; done() == success.
//   - Lazy + abort-on-detach + reactive-key restart + enabled gate, same
//     lifecycle guarantees as query(). The watcher is created inside
//     createRoot so lite-signal's owner tree (>=1.2) doesn't cascade-dispose it
//     when the consumer effect re-runs.
//
// Requires @zakkster/lite-stream as a peer dependency. Importing this entry
// without it installed throws a clear module-resolution error.
//
// Copyright (c) 2026 Zahary Shinikchiev <shinikchiev@yahoo.com>
// MIT License

import {
    signal, effect, onCleanup, untrack, isTracking, createRoot,
    dispose as disposeNode
} from "@zakkster/lite-signal";
import { pipeToSignal } from "@zakkster/lite-stream";

/**
 * Define a streaming query. Lazy: no iterator is pulled until something reads
 * an accessor inside an effect. When the last reader disposes, the iterator is
 * closed (iterator.return()) and the entry is GC-scheduled after cacheTime.
 *
 * @template T, K
 * @param {import("./Query.js").QueryClient} qc
 * @param {{
 *   key: K[] | (() => K[]),
 *   stream: (ctx: { key: K[], signal: AbortSignal }) => AsyncIterable<T> | AsyncIterator<T>,
 *   mode?: "latest" | "buffer",
 *   maxBuffer?: number,
 *   enabled?: boolean | (() => boolean),
 *   cacheTime?: number,
 * }} streamOpts
 * @returns {{
 *   data: () => T | T[] | undefined,
 *   error: () => unknown,
 *   status: () => "idle" | "pending" | "streaming" | "success" | "error",
 *   done: () => boolean,
 *   count: () => number,
 *   droppedCount: () => number,
 *   loading: () => boolean,
 *   restart: () => void,
 *   dispose: () => void,
 * }}
 */
function streamQuery(qc, streamOpts) {
    if (streamOpts === null || typeof streamOpts !== "object") {
        throw new TypeError("streamQuery: options object is required");
    }
    if (streamOpts.key === undefined) {
        throw new TypeError("streamQuery: `key` is required");
    }
    if (typeof streamOpts.stream !== "function") {
        throw new TypeError("streamQuery: `stream` must be a function ({ key, signal }) => AsyncIterable");
    }
    const mode = streamOpts.mode === undefined ? "latest" : streamOpts.mode;
    if (mode !== "latest" && mode !== "buffer") {
        throw new TypeError('streamQuery: `mode` must be "latest" or "buffer"');
    }
    let maxBuffer = 0;
    if (mode === "buffer") {
        maxBuffer = streamOpts.maxBuffer | 0;
        if (maxBuffer <= 0) {
            throw new TypeError("streamQuery: buffer mode requires a positive integer `maxBuffer`");
        }
    }

    // feed is the live per-client devtools cell (V2: a `let` in Query.js cannot
    // cross this subpath boundary, so we read it -- and setStatus -- off the same
    // _internal object). All stream emit sites are one `feed.hook !== null` test.
    const {
        ensureEntry, attach, detach, opts, feed, setStatus, emitStream,
        sharedStreamActive, broadcast, clientId, claimStreamEpoch,
        armWatchdog, disarmWatchdog,
    } = qc._internal;

    // -- stream pump ---------------------------------------------------------
    // Pumps the iterator into entry.data via pipeToSignal. Aborts and restarts
    // any existing pump first (used by restart() / invalidate). Writes status
    // transitions into the entry's existing status/error signals; tracks count
    // and dropped as non-reactive entry counters.
    //
    // Under sharedStream (Q8) a call to startStream means THIS tab is opening
    // (or has been promoted to) the one iterator: it claims an ownership epoch,
    // announces it, and broadcasts every frame (and the terminal end) so
    // follower tabs can project them. Followers never call startStream -- they
    // registerFollower and project incoming frames. `adopt` (V1, C7) preserves
    // the already-projected window/counters across a mid-buffer promotion.
    function startStream(entry, { adopt = false } = {}) {
        // Abort an existing pump first (restart / invalidate). The old pump's
        // abort routes to its onAbort (never onError, per lite-stream's abort
        // vocabulary), which snapshots its own counters and nulls its own
        // slots -- so it won't stomp the new pump's.
        if (entry.streamStop) {
            try { entry.streamStop(); } catch {}
            entry.streamStop = null;
        }

        const shared = sharedStreamActive;
        if (!adopt) {
            entry.streamCount = 0;
            entry.streamDropped = 0;
            entry.invalidatedSinceCompletion = false;
            entry.error.set(undefined);
            setStatus(entry, "pending");
        } else {
            // V1 adopt: a promoted follower keeps its projected window, counters,
            // data, and error -- only the invalidation flag is cleared. No
            // pending regression, no visible frame loss beyond OR-4's bound.
            entry.invalidatedSinceCompletion = false;
        }
        if (feed.hook !== null) emitStream("stream:start", entry, entry.streamCount, false, null, null);   // site 16

        if (shared) {
            entry.streamShared = true;
            entry.streamOwner = true;
            disarmWatchdog(entry);                       // an owner needs no liveness timer
            const epochSeq = claimStreamEpoch(entry);    // sets streamEpoch, streamSeq = 0
            entry.lastFrameAt = opts.now();
            broadcast({ type: "stream-open", key: entry.key, epochSeq, clientId });
        }

        const ac = new AbortController();
        let source;
        try {
            source = streamOpts.stream({ key: entry.key, signal: ac.signal });
        } catch (err) {
            entry.error.set(err);
            setStatus(entry, "error");
            if (feed.hook !== null) emitStream("stream:error", entry, entry.streamCount, false, err, "open");   // site 19 (factory throw)
            if (shared && entry.streamOwner) {
                entry.streamOwner = false;
                broadcast({ type: "stream-end", key: entry.key, epochSeq: entry.streamEpoch, clientId, ok: false, error: err });
            }
            return;
        }

        // The buffer window (bounded, drop-oldest, fresh newest-last snapshot
        // per set) is lite-stream 1.3.0's own `mode`/`maxBuffer` now -- the hand
        // ring is gone. `onValue` taps each value AFTER transform and BEFORE the
        // set to drive the status ladder + count; the drop count is read from
        // the stop fn's own getter (never a hand counter). Intentional aborts
        // route to `onAbort` and NEVER reach `onError` (lite-stream 0001).
        const stop = pipeToSignal(source, entry.data, {
            signal: ac.signal,
            mode: mode,
            // maxBuffer is meaningful only in buffer mode; lite-stream fails
            // closed if it is passed with mode "latest".
            maxBuffer: mode === "buffer" ? maxBuffer : undefined,
            onValue: (v) => {
                if (entry.streamCount === 0) setStatus(entry, "streaming");
                entry.streamCount = (entry.streamCount + 1) | 0;
                // Leader send path (cold relative to local reads): stamp a
                // monotone seq and broadcast the frame. Built ONLY here and on
                // the remote-receive path; a non-shared stream skips it entirely
                // so the frozen GATE never sees a per-frame allocation.
                if (shared && entry.streamOwner) {
                    entry.streamSeq = (entry.streamSeq + 1) | 0;
                    broadcast({ type: "stream-frame", key: entry.key, epochSeq: entry.streamEpoch, clientId, seq: entry.streamSeq, value: v });
                }
                if (feed.hook !== null) emitStream("stream:value", entry, entry.streamCount, false, v, null);   // site 17
            },
            onError: (err) => {
                // A genuine iterator failure. Snapshot the final drop count so
                // droppedCount() stays stable after teardown.
                entry.streamDropped = stop.droppedCount;
                entry.error.set(err);
                setStatus(entry, "error");
                if (feed.hook !== null) emitStream("stream:error", entry, entry.streamCount, false, err, "iterator");   // site 19 (iterator)
                if (shared && entry.streamOwner) {
                    entry.streamOwner = false;
                    broadcast({ type: "stream-end", key: entry.key, epochSeq: entry.streamEpoch, clientId, ok: false, error: err });
                }
                entry.streamStop = null;
                entry.streamRestart = null;
            },
            onAbort: () => {
                // Intentional abort (detach / restart / removeQueries / abdicate)
                // is not a failure: snapshot the final drop count so
                // droppedCount() reads byte-identical after teardown, then
                // release the handles. Status is reset by the caller (detach ->
                // idle; restart -> pending). No stream-end is broadcast -- an
                // abort is a local relinquish, not a terminal outcome for the key.
                entry.streamDropped = stop.droppedCount;
                entry.streamStop = null;
                entry.streamRestart = null;
            },
            onDone: () => {
                entry.streamDropped = stop.droppedCount;
                setStatus(entry, "success");
                entry.lastCompletedAt = opts.now();
                if (feed.hook !== null) emitStream("stream:done", entry, entry.streamCount, true, null, null);   // site 18
                if (shared && entry.streamOwner) {
                    entry.streamOwner = false;
                    broadcast({ type: "stream-end", key: entry.key, epochSeq: entry.streamEpoch, clientId, ok: true, error: undefined });
                }
                entry.streamStop = null;
                entry.streamRestart = null;
            },
        });

        // streamStop aborts the iterator (signalling the user's stream to clean
        // up) then stops the pump. Its `.raw` is the pipeToSignal stop fn, whose
        // live droppedCount getter the accessor reads while the pump is active.
        const streamStop = () => { ac.abort(); stop(); };
        streamStop.raw = stop;
        entry.streamStop = streamStop;
        entry.streamRestart = () => startStream(entry);
        // A promoted follower re-enters here with adopt:true (V1) -- the entry
        // carries the thunk so Query.js's watchdog/race can start THIS tab's
        // iterator without importing the /stream body.
        entry.streamPromote = (reason) => startStream(entry, { adopt: true });
    }

    // Register this tab as a FOLLOWER of a shared stream: no iterator, project
    // the leader's frames. Marks the entry shared, records the projection
    // discipline (mode/maxBuffer) so Query.js's projectFrame can window without
    // seeing streamOpts, resets the epoch/seq cursors, shows loading, asks the
    // channel for an owner, and arms the liveness watchdog (OR-3: if no frame
    // arrives within streamIdleTimeout this tab self-connects). Also installs
    // streamPromote so the watchdog/race can promote it in place.
    function registerFollower(entry) {
        entry.streamShared = true;
        entry.streamOwner = false;
        entry.streamMode = mode;
        entry.streamMaxBuffer = maxBuffer;
        entry.projEpoch = -1;
        entry.projSeq = -1;
        entry.streamCount = 0;
        entry.streamDropped = 0;
        entry.invalidatedSinceCompletion = false;
        entry.error.set(undefined);
        setStatus(entry, "pending");
        if (feed.hook !== null) emitStream("stream:start", entry, 0, false, null, null);   // site 16
        entry.streamPromote = (reason) => startStream(entry, { adopt: true });
        entry.lastFrameAt = opts.now();
        broadcast({ type: "stream-req", key: entry.key });
        armWatchdog(entry);
    }

    // Should the watcher start a stream on this (fresh) attach? Mirrors
    // shouldFetch: don't double-pump a shared entry, don't restart a terminal
    // stream unless it was invalidated.
    function shouldStartStream(entry) {
        if (entry.streamStop !== null) return false;     // owner already streaming (shared observer)
        if (entry.streamShared && !entry.streamOwner) return false;  // already following in this tab
        const s = entry.status();
        if (s === "idle") return true;                    // never started / reset on detach
        if (s === "success" || s === "error") return entry.invalidatedSinceCompletion;
        return false;
    }

    // -- observer / watcher (mirrors query(); see Query.js for the rationale) --
    const currentEntry = signal(null);
    let watcher = null;
    let attachedEntry = null;
    let disposed = false;
    let stopScheduled = false;
    let observerCount = 0;

    function startWatcher() {
        if (watcher !== null || disposed) return;
        // createRoot detaches the watcher from the consumer effect that triggers
        // the first read (lite-signal >=1.2 owner tree would otherwise cascade-
        // dispose it on the consumer's re-run). We dispose it ourselves.
        watcher = createRoot(() => effect(() => {
            const keyVal = typeof streamOpts.key === "function"
                ? streamOpts.key()
                : streamOpts.key;
            const isEnabled = streamOpts.enabled === undefined
                ? true
                : typeof streamOpts.enabled === "function"
                    ? streamOpts.enabled()
                    : !!streamOpts.enabled;

            if (!isEnabled) {
                if (attachedEntry) {
                    untrack(() => detach(attachedEntry));
                    attachedEntry = null;
                }
                currentEntry.set(null);
                return;
            }

            const entry = untrack(() => ensureEntry(keyVal));
            entry.isStream = true;

            if (entry !== attachedEntry) {
                if (attachedEntry) untrack(() => detach(attachedEntry));
                untrack(() => attach(entry, streamOpts));
                attachedEntry = entry;
                currentEntry.set(entry);
                untrack(() => {
                    if (!shouldStartStream(entry)) return;
                    if (sharedStreamActive) {
                        // The oracle only HINTS the initial role (OR-3); the
                        // epoch race + watchdog converge to one connection even
                        // if it lies. A hinted leader owns the iterator and
                        // broadcasts; a hinted follower projects and self-connects
                        // on silence.
                        if (opts.isLeader()) startStream(entry);
                        else registerFollower(entry);
                    } else {
                        startStream(entry);
                    }
                });
            }
        }));
    }

    function stopWatcher() {
        if (watcher === null) return;
        watcher();
        watcher = null;
        if (attachedEntry) {
            detach(attachedEntry);
            attachedEntry = null;
        }
        currentEntry.set(null);
    }

    const cleanupObserver = () => {
        if (disposed) return;
        observerCount--;
        if (observerCount === 0 && !stopScheduled) {
            stopScheduled = true;
            queueMicrotask(maybeStopWatcher);
        }
    };
    const maybeStopWatcher = () => {
        stopScheduled = false;
        if (observerCount === 0 && watcher !== null && !disposed) stopWatcher();
    };

    function trackObserver() {
        if (disposed || !isTracking()) return;
        observerCount++;
        if (watcher === null) startWatcher();
        onCleanup(cleanupObserver);
    }

    return {
        data() {
            trackObserver();
            const e = currentEntry();
            return e ? e.data() : undefined;
        },
        error() {
            trackObserver();
            const e = currentEntry();
            return e ? e.error() : undefined;
        },
        status() {
            trackObserver();
            const e = currentEntry();
            return e ? e.status() : "idle";
        },
        // loading: subscribed but no value yet (status pending).
        loading() {
            trackObserver();
            const e = currentEntry();
            return e ? e.status() === "pending" : false;
        },
        // done: the iterator completed naturally (status success).
        done() {
            trackObserver();
            const e = currentEntry();
            return e ? e.status() === "success" : false;
        },
        // count / droppedCount are non-reactive telemetry snapshots -- reading
        // them alongside data() in one effect updates them as values arrive,
        // without a per-frame signal write of their own. (See ROADMAP.md.)
        count() {
            const e = untrack(() => currentEntry());
            return e ? e.streamCount : 0;
        },
        droppedCount() {
            const e = untrack(() => currentEntry());
            if (!e) return 0;
            // While a pump is live, read its own overflow getter (byte-identical
            // to lite-stream's droppedCount); after any terminal path it is
            // snapshotted into entry.streamDropped, so fall back to that.
            const s = e.streamStop;
            return (s !== null && s.raw !== undefined) ? s.raw.droppedCount : e.streamDropped;
        },
        // Imperative restart: abort the current stream and re-establish it.
        restart() {
            const e = untrack(() => currentEntry());
            if (e) startStream(e);
        },
        dispose() {
            disposed = true;
            stopWatcher();
            // Return currentEntry's signal node to lite-signal's pool -- without
            // this, creating + disposing many streamQuery handles leaks one
            // signal per call.
            try { disposeNode(currentEntry); } catch {}
        },
    };
}

// One runtime version source for every subpath (see Query.js VERSION).
export { VERSION } from "./Query.js";

export { streamQuery };
