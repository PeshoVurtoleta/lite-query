/**
 * @zakkster/lite-query — async cache + reactive queries built on lite-signal.
 *
 * Design pillars:
 *   - Explicit observer reference counting per cache entry. attach() ++count,
 *     detach() --count; count → 0 aborts in-flight fetch + schedules cacheTime
 *     GC; count: 0 → ≥1 cancels the pending GC timer. Shared-observer correctness
 *     (one dispose doesn't starve the others) falls out of this.
 *   - Generation guard on every fetch. Each runFetch increments entry.fetchGen;
 *     resolutions check gen before mutating state. Aborted-but-still-resolving
 *     fetches are silently ignored. No race window.
 *   - Cross-tab: BroadcastChannel-backed, opt-in. Cache writes / invalidations /
 *     removals propagate; background fetch results do NOT (otherwise tabs cross-
 *     talk forever). A `processingRemote` flag suppresses echo loops.
 *   - Mid-flight invalidation: option (b) — let the in-flight finish, then
 *     immediately refetch. We set `pendingRefetchAfterCurrent` and the
 *     resolution path drains it.
 *   - Reactive `enabled`: when it flips to false, the watcher effect re-runs,
 *     its onCleanup fires detach (which aborts the fetch if we were the last
 *     observer), and the body returns without re-attaching. Status reads back
 *     as 'idle' via the null-entry fallback in the accessors.
 *
 * What this module does NOT do (deferred to 1.x):
 *   - Focus / reconnect refetch triggers (will be injectable callbacks).
 *   - Suspense, framework hooks. Use the signal accessors directly.
 *   - Infinite queries. Build on top of this.
 *   - Structural-deep equality by default. Opt in via per-query `equals`.
 */

import {
    signal, effect, onCleanup, untrack, isTracking, createRoot,
    dispose as disposeNode,                          // local dispose() = qc.dispose() (client teardown);
                                                     // alias the lite-signal one to avoid the shadow
} from "@zakkster/lite-signal";

const noop = () => {};

// ─────────────────────────────────────────────────────────────────────────────
// Key hashing & matching
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Stable hash for query keys. Objects are serialized with sorted property
 * order so `{a:1, b:2}` and `{b:2, a:1}` hash identically. Arrays preserve
 * order — array indices are meaningful in query keys.
 */
function hashKey(key) {
    return JSON.stringify(key, (_, v) => {
        if (v && typeof v === "object" && !Array.isArray(v)) {
            const sorted = {};
            for (const k of Object.keys(v).sort()) sorted[k] = v[k];
            return sorted;
        }
        return v;
    });
}

/**
 * Match an entry's key against a pattern. With `exact:true`, the keys must
 * be element-equivalent. Otherwise, prefix match: every element of pattern
 * must equal the corresponding element of entryKey.
 */
function keyMatches(entryKey, pattern, exact) {
    if (exact) return hashKey(entryKey) === hashKey(pattern);
    if (!Array.isArray(entryKey) || !Array.isArray(pattern)) {
        return hashKey(entryKey) === hashKey(pattern);
    }
    if (pattern.length > entryKey.length) return false;
    for (let i = 0; i < pattern.length; i++) {
        if (hashKey(entryKey[i]) !== hashKey(pattern[i])) return false;
    }
    return true;
}

// ─────────────────────────────────────────────────────────────────────────────
// Option resolution
// ─────────────────────────────────────────────────────────────────────────────

function resolveOptions(o) {
    return {
        defaultStaleTime: o.defaultStaleTime ?? 0,
        defaultCacheTime: o.defaultCacheTime ?? 5 * 60_000,
        defaultTimeout: o.defaultTimeout ?? Infinity,
        retry: o.retry ?? 3,
        retryDelay: o.retryDelay ?? ((attempt) =>
            Math.min(1000 * 2 ** (attempt - 1), 30_000)),
        crossTab: o.crossTab ?? false,
        crossTabChannel: o.crossTabChannel ?? "lite-query",
        // Cross-tab fetch deduplication. When sharedFetch is on AND a valid
        // isLeader function is supplied, follower tabs don't fetch — they ask
        // the leader (via fetch-req broadcast) and receive the result. Wire
        // isLeader from @zakkster/lite-channel's leader signal.
        sharedFetch: o.sharedFetch ?? false,
        isLeader: o.isLeader ?? null,
        sharedFetchTimeout: o.sharedFetchTimeout ?? 3000,
        now: o.now ?? (() => Date.now()),
        setTimeout: o.setTimeout ?? ((fn, ms) => globalThis.setTimeout(fn, ms)),
        clearTimeout: o.clearTimeout ?? ((id) => globalThis.clearTimeout(id)),
        broadcastChannel: o.broadcastChannel ??
            (typeof BroadcastChannel !== "undefined" ? BroadcastChannel : null),
    };
}

// Abort reasons exposed on AbortSignal.reason. Users' fetchers can inspect
// these to decide whether to retry — e.g., a user-initiated detach (component
// unmounting) is non-retryable, but a timeout might be.
const ABORT_REASON = Object.freeze({
    DETACH:   "lite-query:detach",       // last observer left (or reactive key changed)
    REFETCH:  "lite-query:refetch",      // forced via .refetch()
    REMOVED:  "lite-query:removed",      // qc.removeQueries / qc.clear
    TIMEOUT:  "lite-query:timeout",      // per-query timeout exceeded
});

// ─────────────────────────────────────────────────────────────────────────────
// queryClient
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Create a query client — the cache + lifecycle owner. Make one per app (or
 * one per logical scope, e.g. per logged-in user session).
 *
 * The returned object exposes both cache operations (`getQueryData`,
 * `setQueryData`, `invalidate`, `removeQueries`, `clear`) and lifecycle
 * (`dispose`). Cross-tab coherence (opt-in via `crossTab: true`) propagates
 * cache mutations to peer tabs via BroadcastChannel.
 *
 * @param {import("./Query.js").QueryClientOptions} [options]
 * @returns {import("./Query.js").QueryClient}
 *
 * @example
 *   const qc = queryClient({
 *       defaultStaleTime: 30_000,
 *       defaultCacheTime: 5 * 60_000,
 *       crossTab: true,           // propagate setQueryData/invalidate across tabs
 *   });
 */
export function queryClient(options = {}) {
    const opts = resolveOptions(options);
    /** @type {Map<string, QueryEntry>} */
    const entries = new Map();

    let channel = null;
    let processingRemote = false;

    if (opts.crossTab && opts.broadcastChannel) {
        channel = new opts.broadcastChannel(opts.crossTabChannel);
        channel.addEventListener("message", onRemoteMessage);
    }

    // Shared-fetch is active only when opted in, a leader oracle is supplied,
    // and a channel exists to coordinate over.
    const sharedFetchActive =
        opts.sharedFetch && typeof opts.isLeader === "function" && !!channel;

    // ── cross-tab ──

    function broadcast(msg) {
        if (!channel || processingRemote) return;
        try { channel.postMessage(msg); } catch { /* serialization or closed */ }
    }

    function onRemoteMessage(evt) {
        processingRemote = true;
        try {
            const m = evt.data;
            switch (m && m.type) {
                case "setData":    setQueryData(m.key, m.value);    break;
                case "invalidate": invalidate(m.key, m.opts || {}); break;
                case "remove":     removeQueries(m.key, m.opts || {}); break;
                case "clear":      clear();                          break;
                case "fetch-req": {
                    // A follower is asking for data. Only the leader fulfills,
                    // and only for a query it currently has alive with a fetcher.
                    // runFetch resolves async (after this handler returns), so
                    // its result-broadcast isn't suppressed by processingRemote.
                    if (!sharedFetchActive || !opts.isLeader()) break;
                    const e = entries.get(hashKey(m.key));
                    if (e && e.fetcher) runFetch(e).catch(noop);
                    break;
                }
            }
        } finally {
            processingRemote = false;
        }
    }

    // ── entries ──

    function createEntry(key) {
        return {
            key,
            keyHash: hashKey(key),
            data: signal(undefined),
            error: signal(undefined),
            status: signal("idle"),
            fetching: signal(false),
            observerCount: 0,
            promise: null,
            abortController: null,
            lastCompletedAt: -Infinity,
            invalidatedSinceCompletion: false,
            gcTimerId: null,
            pendingRefetchAfterCurrent: false,
            sharedFallbackTimer: null,
            // Stream slots — uniform on every entry to keep the shape
            // monomorphic (no second hidden class at the hot attach/detach/GC
            // sites). A query entry leaves these at their null/false/0 defaults;
            // only a streamQuery entry populates them. No stream signal node is
            // allocated for a plain query.
            isStream: false,
            streamStop: null,            // () => void  — abort the iterator / close the connection
            streamRestart: null,         // () => void  — abort + re-establish (invalidate)
            streamCount: 0,              // non-reactive: values seen this session
            streamDropped: 0,            // non-reactive: values dropped (buffer mode)
            fetcher: null,
            equals: Object.is,
            staleTime: opts.defaultStaleTime,
            cacheTime: opts.defaultCacheTime,
            timeout: opts.defaultTimeout,
            retry: opts.retry,
            retryDelay: opts.retryDelay,
            fetchGen: 0,
        };
    }

    function ensureEntry(key) {
        const h = hashKey(key);
        let e = entries.get(h);
        if (!e) {
            e = createEntry(key);
            entries.set(h, e);
            // Entry has no observers yet — schedule GC immediately. attach()
            // will cancel this if an observer arrives before the timer fires.
            scheduleGc(e);
        }
        return e;
    }

    // Releases the per-entry signal nodes back to lite-signal's pool. Without
    // this, an app that creates + removes many query keys over its lifetime
    // (route-mounted queries in an SPA, long-running diagnostic dashboards)
    // accumulates entries' signal handles in the registry until the next GC
    // run — eventually tripping the registry capacity cap. Called from the
    // three entry-removal sites: GC timer, removeQueries, clear.
    function disposeEntry(entry) {
        // Stop any live stream first (abort the iterator -> iterator.return(),
        // closing the underlying SSE/websocket) before releasing signal nodes.
        if (entry.streamStop) {
            try { entry.streamStop(); } catch {}
            entry.streamStop = null;
            entry.streamRestart = null;
        }
        // disposeNode is lite-signal's `dispose`, aliased at import time so it
        // doesn't shadow the local qc.dispose() function. Calling the wrong
        // one would clear the entire cache map (see commit message for the
        // ~600-line stack-trace anecdote).
        try { disposeNode(entry.data); }     catch {}
        try { disposeNode(entry.error); }    catch {}
        try { disposeNode(entry.status); }   catch {}
        try { disposeNode(entry.fetching); } catch {}
    }

    function scheduleGc(entry) {
        if (entry.gcTimerId !== null) opts.clearTimeout(entry.gcTimerId);
        entry.gcTimerId = null;
        if (entry.cacheTime === Infinity) return;
        entry.gcTimerId = opts.setTimeout(() => {
            entry.gcTimerId = null;
            if (entry.observerCount === 0) {
                if (entry.abortController) entry.abortController.abort(ABORT_REASON.REMOVED);
                entries.delete(entry.keyHash);
                disposeEntry(entry);
            }
        }, entry.cacheTime);
        // In Node, unref'd timers don't prevent process exit. This means a
        // test that creates entries but never calls qc.clear() / removeQueries
        // doesn't hang the runner. In browsers, gcTimerId is a number and
        // .unref doesn't exist — the guard makes this a no-op there.
        if (entry.gcTimerId && typeof entry.gcTimerId.unref === "function") {
            entry.gcTimerId.unref();
        }
    }

    function cancelGc(entry) {
        if (entry.gcTimerId !== null) {
            opts.clearTimeout(entry.gcTimerId);
            entry.gcTimerId = null;
        }
    }

    // ── attach / detach ──

    function attach(entry, queryOpts) {
        // First observer to attach configures the entry's per-query options.
        // Subsequent attaches don't override — first wins. (A common gotcha
        // in TanStack too: two queries with the same key + different
        // staleTimes — first one wins, document it.)
        if (entry.observerCount === 0) {
            if (queryOpts.fetcher)                       entry.fetcher    = queryOpts.fetcher;
            if (queryOpts.staleTime  !== undefined)      entry.staleTime  = queryOpts.staleTime;
            if (queryOpts.cacheTime  !== undefined)      entry.cacheTime  = queryOpts.cacheTime;
            if (queryOpts.timeout    !== undefined)      entry.timeout    = queryOpts.timeout;
            if (queryOpts.retry      !== undefined)      entry.retry      = queryOpts.retry;
            if (queryOpts.retryDelay !== undefined)      entry.retryDelay = queryOpts.retryDelay;
            if (queryOpts.equals     !== undefined)      entry.equals     = queryOpts.equals;
        }
        entry.observerCount++;
        cancelGc(entry);
    }

    function clearSharedTimer(entry) {
        if (entry.sharedFallbackTimer !== null) {
            opts.clearTimeout(entry.sharedFallbackTimer);
            entry.sharedFallbackTimer = null;
        }
    }

    function detach(entry) {
        entry.observerCount--;
        if (entry.observerCount === 0) {
            // Last observer gone — abort in-flight if any. Resolution paths
            // gate on the generation guard, so a late resolution is harmless.
            if (entry.abortController) {
                entry.abortController.abort(ABORT_REASON.DETACH);
                entry.abortController = null;
                entry.promise = null;
                entry.fetching.set(false);
                if (entry.status() === "pending") entry.status.set("idle");
            }
            // Last observer gone on a stream — close the connection. The entry
            // stays cached (scheduleGc); a re-attach before GC re-establishes a
            // fresh stream via the watcher. Cached data() survives until GC.
            if (entry.streamStop) {
                try { entry.streamStop(); } catch {}
                entry.streamStop = null;
                entry.streamRestart = null;
                const s = entry.status();
                if (s === "pending" || s === "streaming") entry.status.set("idle");
            }
            clearSharedTimer(entry);
            scheduleGc(entry);
        }
    }

    // ── fetch lifecycle ──

    // Pure predicate: does this entry need a (re)fetch right now? Used by both
    // the leader path (runFetch) and the follower path (requestSharedFetch).
    function shouldFetch(entry) {
        const status = entry.status();
        if (status === "idle") return true;
        if (status === "error") return entry.invalidatedSinceCompletion;
        if (status === "success") {
            if (entry.invalidatedSinceCompletion) return true;
            return (opts.now() - entry.lastCompletedAt) >= entry.staleTime;
        }
        return false;
    }

    function maybeFetch(entry) {
        if (!entry.fetcher) return;
        if (entry.promise) return;                       // already fetching locally
        if (entry.sharedFallbackTimer !== null) return;  // already awaiting a shared fetch
        if (!shouldFetch(entry)) return;

        if (sharedFetchActive && !opts.isLeader()) {
            requestSharedFetch(entry);                   // follower: ask the leader
        } else {
            runFetch(entry).catch(noop);                 // leader or single-tab: fetch
        }
    }

    // Follower path under sharedFetch: broadcast a request, show loading, and
    // arm a fallback timer. If the leader broadcasts a result before the timer
    // fires, setQueryData clears the timer. Otherwise we self-fetch so the UI
    // never hangs — liveness guarantee during leader elections or when the
    // leader doesn't have the query defined.
    function requestSharedFetch(entry) {
        entry.fetching.set(true);
        if (entry.data() === undefined && entry.status() !== "error") {
            entry.status.set("pending");
        }
        broadcast({ type: "fetch-req", key: entry.key });
        clearSharedTimer(entry);
        entry.sharedFallbackTimer = opts.setTimeout(() => {
            entry.sharedFallbackTimer = null;
            // The timer firing means no leader fulfilled the request — the
            // arrival path (setQueryData) would have cleared it otherwise.
            // Self-fetch so the UI never hangs.
            if (entry.observerCount > 0 && entry.promise === null) {
                runFetch(entry).catch(noop);
            }
        }, opts.sharedFetchTimeout);
        if (entry.sharedFallbackTimer && typeof entry.sharedFallbackTimer.unref === "function") {
            entry.sharedFallbackTimer.unref();
        }
    }

    function runFetch(entry, { force = false } = {}) {
        if (!force && entry.promise) return entry.promise;
        if (!entry.fetcher) return Promise.resolve(undefined);

        // Abort any prior in-flight fetch. The old promise's resolution will
        // be filtered by the generation guard below.
        if (entry.abortController) {
            entry.abortController.abort(ABORT_REASON.REFETCH);
        }

        const gen = ++entry.fetchGen;
        const ac = new AbortController();
        entry.abortController = ac;
        entry.fetching.set(true);
        if (entry.data() === undefined && entry.status() !== "error") {
            entry.status.set("pending");
        }

        // Per-query timeout. If specified and finite, set up a timer that
        // aborts the AbortController with ABORT_REASON.TIMEOUT. Cleared on
        // resolution (success or error path).
        let timeoutId = null;
        if (entry.timeout != null && isFinite(entry.timeout)) {
            timeoutId = opts.setTimeout(() => {
                ac.abort(ABORT_REASON.TIMEOUT);
            }, entry.timeout);
            if (timeoutId && typeof timeoutId.unref === "function") {
                timeoutId.unref();
            }
        }

        const clearTimeoutTimer = () => {
            if (timeoutId !== null) {
                opts.clearTimeout(timeoutId);
                timeoutId = null;
            }
        };

        const promise = (async () => {
            let attempt = 0;
            // Retry loop. We catch within the loop so that the promise
            // returned to the caller reflects the final outcome.
            // eslint-disable-next-line no-constant-condition
            while (true) {
                try {
                    const data = await entry.fetcher({
                        key: entry.key,
                        signal: ac.signal,
                    });
                    return { ok: true, data };
                } catch (err) {
                    if (gen !== entry.fetchGen || ac.signal.aborted) {
                        return { ok: false, err, superseded: true };
                    }
                    const shouldRetry = typeof entry.retry === "function"
                        ? entry.retry(attempt, err)
                        : attempt < entry.retry;
                    if (!shouldRetry) return { ok: false, err };
                    const delay = entry.retryDelay(attempt + 1);
                    await new Promise((res) => opts.setTimeout(res, delay));
                    if (gen !== entry.fetchGen || ac.signal.aborted) {
                        return { ok: false, err, superseded: true };
                    }
                    attempt++;
                }
            }
        })().then((outcome) => {
            clearTimeoutTimer();

            // Generation guard: a newer fetch invalidates this resolution.
            if (gen !== entry.fetchGen) return;
            if (ac.signal.aborted) return;

            if (outcome.superseded) return;

            if (outcome.ok) {
                if (!entry.equals(entry.data(), outcome.data)) {
                    entry.data.set(outcome.data);
                }
                entry.error.set(undefined);
                entry.status.set("success");
                // Shared-fetch: the leader broadcasts its results so follower
                // tabs receive them without issuing their own network calls.
                if (sharedFetchActive && opts.isLeader()) {
                    broadcast({ type: "setData", key: entry.key, value: outcome.data });
                }
            } else {
                entry.error.set(outcome.err);
                entry.status.set("error");
            }

            entry.lastCompletedAt = opts.now();
            entry.invalidatedSinceCompletion = false;
            entry.fetching.set(false);
            entry.promise = null;
            entry.abortController = null;

            // Mid-flight invalidation follow-up (option b: let-finish + refetch).
            if (
                outcome.ok &&
                entry.pendingRefetchAfterCurrent &&
                entry.observerCount > 0
            ) {
                entry.pendingRefetchAfterCurrent = false;
                runFetch(entry).catch(noop);
            }

            return outcome.ok ? outcome.data : Promise.reject(outcome.err);
        });

        entry.promise = promise;
        return promise;
    }

    // ── public cache API ──

    function getQueryData(key) {
        const e = entries.get(hashKey(key));
        if (!e) return undefined;
        // Peek without subscribing — getQueryData is an imperative read.
        return untrack(() => e.data());
    }

    function setQueryData(key, valueOrFn) {
        const e = ensureEntry(key);
        const newVal = typeof valueOrFn === "function"
            ? valueOrFn(untrack(() => e.data()))
            : valueOrFn;
        e.data.set(newVal);
        e.error.set(undefined);
        e.status.set("success");
        e.lastCompletedAt = opts.now();
        // If a shared-fetch follower was awaiting the leader's result, this IS
        // that result — stop the loading state and cancel the fallback timer.
        // The promise===null guard avoids clobbering a genuine in-flight fetch.
        if (e.promise === null) e.fetching.set(false);
        clearSharedTimer(e);
        broadcast({ type: "setData", key, value: newVal });
    }

    function invalidate(key, invOpts = {}) {
        const exact = invOpts.exact ?? false;
        for (const e of entries.values()) {
            if (!keyMatches(e.key, key, exact)) continue;
            e.invalidatedSinceCompletion = true;
            if (e.isStream) {
                // A stream is invalidated by aborting and re-establishing it.
                // streamRestart is installed by streamQuery while observed; if
                // unobserved there's nothing live to restart (the next attach
                // starts fresh and will see invalidatedSinceCompletion).
                if (e.observerCount > 0 && e.streamRestart) e.streamRestart();
            } else if (e.observerCount > 0 && e.fetcher) {
                if (e.promise) {
                    e.pendingRefetchAfterCurrent = true;
                } else if (sharedFetchActive && !opts.isLeader()) {
                    // Follower: defer to the leader rather than fetching locally.
                    // (If this invalidate arrived as a broadcast, the fetch-req
                    // is suppressed during remote processing — but the leader
                    // invalidated too and will broadcast its result, which
                    // clears our fallback timer. Liveness still holds.)
                    requestSharedFetch(e);
                } else {
                    runFetch(e).catch(noop);
                }
            }
        }
        broadcast({ type: "invalidate", key, opts: invOpts });
    }

    function removeQueries(key, rmOpts = {}) {
        const exact = rmOpts.exact ?? false;
        for (const [h, e] of [...entries]) {
            if (!keyMatches(e.key, key, exact)) continue;
            if (e.abortController) e.abortController.abort(ABORT_REASON.REMOVED);
            cancelGc(e);
            clearSharedTimer(e);
            entries.delete(h);
            disposeEntry(e);
        }
        broadcast({ type: "remove", key, opts: rmOpts });
    }

    function clear() {
        for (const e of entries.values()) {
            if (e.abortController) e.abortController.abort(ABORT_REASON.REMOVED);
            cancelGc(e);
            clearSharedTimer(e);
            disposeEntry(e);
        }
        entries.clear();
        broadcast({ type: "clear" });
    }

    // Dispose the entire client. Releases the BroadcastChannel listener which
    // would otherwise keep the client + its entire cache map alive in
    // scenarios where clients are created and discarded — testing,
    // micro-frontends, dev hot-reload. After dispose(), further mutations are
    // no-ops (cache is cleared, channel is closed).
    function dispose() {
        clear();
        if (channel) {
            try { channel.removeEventListener("message", onRemoteMessage); }
            catch { /* some mock channels don't implement removeEventListener */ }
            try { channel.close(); } catch { /* same */ }
            channel = null;
        }
    }

    return {
        options: opts,
        getQueryData,
        setQueryData,
        invalidate,
        removeQueries,
        clear,
        dispose,
        // Internal API consumed by query()/mutation(). Not part of the public
        // surface; documented as such in llms.txt.
        _internal: { entries, ensureEntry, attach, detach, maybeFetch, runFetch, requestSharedFetch, sharedFetchActive, opts },
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// query()
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Define a reactive query — the read-side primitive. Lazy: no fetch fires
 * until something reads one of the returned accessors inside an effect. When
 * the last effect that reads them disposes, the in-flight fetch (if any) is
 * aborted with `lite-query:detach` and the entry is scheduled for GC after
 * `cacheTime`.
 *
 * @template T, K
 * @param {import("./Query.js").QueryClient} qc
 * @param {import("./Query.js").QueryOptions<T, K>} queryOpts
 * @returns {import("./Query.js").Query<T>}
 *
 * @example
 *   const userId = signal(1);
 *   const user = query(qc, {
 *       key: () => ["user", userId()],            // reactive key
 *       fetcher: ({ key, signal }) => fetch(`/api/users/${key[1]}`, { signal }).then(r => r.json()),
 *   });
 *   effect(() => console.log(user.data()));       // attach + fetch
 */
export function query(qc, queryOpts) {
    const { ensureEntry, attach, detach, maybeFetch, runFetch, requestSharedFetch, sharedFetchActive, opts } = qc._internal;

    // currentEntry is itself a signal — accessors subscribe to it so they
    // refire when the key changes (reactive key) or attach/detach flips.
    const currentEntry = signal(null);

    // Lazy watcher: only running when outer subscribers exist. This is the
    // "no observers → no fetch" property — query() alone doesn't fetch; the
    // first read of an accessor inside an effect starts the engine.
    let watcher = null;
    let observerCount = 0;
    let stopScheduled = false;
    let disposed = false;

    // The entry we're currently attached to. Tracked explicitly so the
    // watcher re-running with an identical key (a common case when the key
    // function reads multiple signals but the resulting key is the same)
    // doesn't churn detach/attach + spurious refetches.
    let attachedEntry = null;

    function startWatcher() {
        if (watcher !== null || disposed) return;
        // createRoot detaches the watcher from whatever consumer effect happens
        // to trigger the first accessor read. Without it, lite-signal's owner
        // tree (>=1.2) adopts the watcher as that effect's child and cascade-
        // disposes it on the consumer's next re-run -- which silently breaks
        // reactive keys / refetch (the watcher never re-runs again). We own the
        // watcher's lifecycle explicitly (stopWatcher / dispose), so detaching
        // it from the owner tree loses nothing.
        watcher = createRoot(() => effect(() => {
            // ONLY key and enabled are tracked here — internal entry state
            // (status, data, fetching) is read inside untrack so it doesn't
            // cause the watcher to re-run and call attach() again.
            const keyVal = typeof queryOpts.key === "function"
                ? queryOpts.key()
                : queryOpts.key;

            const isEnabled = queryOpts.enabled === undefined
                ? true
                : typeof queryOpts.enabled === "function"
                    ? queryOpts.enabled()
                    : !!queryOpts.enabled;

            if (!isEnabled) {
                if (attachedEntry) {
                    untrack(() => detach(attachedEntry));
                    attachedEntry = null;
                }
                currentEntry.set(null);
                return;
            }

            const entry = untrack(() => ensureEntry(keyVal));

            if (entry !== attachedEntry) {
                // Different entry: detach the old, attach the new, decide on
                // fetch via maybeFetch on this fresh attachment.
                if (attachedEntry) untrack(() => detach(attachedEntry));
                untrack(() => attach(entry, queryOpts));
                attachedEntry = entry;
                currentEntry.set(entry);
                untrack(() => maybeFetch(entry));
            }
            // Same entry: leave attach state alone, no maybeFetch. The user's
            // key didn't actually change — refetch only happens on explicit
            // .refetch(), invalidate(), or a true key change.
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

    // Hoisted once per query() instance to keep the accessor hot path zero-
    // allocation. lite-signal's onCleanup accepts the same function reference
    // registered multiple times within one effect run; each registration fires
    // independently at cleanup, decrementing observerCount once per read. Net
    // semantics are identical to allocating a fresh closure per read; what we
    // save is roughly one closure (~33 B) per accessor read. For a UI calling
    // q.data()/.fetching()/.status()/.error() in an effect at 60Hz across N
    // queries, this eliminates 4*60*N closures/second of young-gen pressure.
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

    /**
     * Called from each accessor. If we're inside a reactive context, register
     * an observer reference (++count, start watcher if first). The matching
     * decrement happens via onCleanup of the calling effect (the shared
     * `cleanupObserver` closure above — no per-read allocation).
     *
     * Critical: between an effect's re-run cleanup and body, observerCount
     * transits N → 0 → N. Naïvely stopping the watcher when count hits zero
     * would tear down the entry between re-runs. We defer the stop to a
     * microtask; if a re-attach happens first (the usual case), the deferred
     * stop becomes a no-op.
     */
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
        // loading: pending AND no data yet (initial load only)
        loading() {
            trackObserver();
            const e = currentEntry();
            if (!e) return false;
            return e.fetching() && e.data() === undefined;
        },
        // fetching: any fetch in progress including background revalidation
        fetching() {
            trackObserver();
            const e = currentEntry();
            return e ? e.fetching() : false;
        },
        refetch() {
            const e = untrack(() => currentEntry());
            if (!e) return Promise.resolve(undefined);
            // Under shared-fetch, a follower asks the leader rather than
            // hitting the network itself. The leader's broadcast result
            // updates this tab and clears the fallback timer.
            if (sharedFetchActive && !opts.isLeader()) {
                requestSharedFetch(e);
                return e.promise || Promise.resolve(untrack(() => e.data()));
            }
            return runFetch(e, { force: true });
        },
        dispose() {
            disposed = true;
            stopWatcher();
            // Return currentEntry's signal node to lite-signal's pool — without
            // this, an app that creates + disposes many queries (e.g. many
            // routes over an SPA lifetime) leaks one signal per query() call.
            try { disposeNode(currentEntry); } catch {}
        },
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// mutation()
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Define a mutation — the write-side primitive. Composes the canonical
 * `onMutate` → `fn` → (`onSuccess` | `onError`) → `onSettled` chain with
 * per-call generation tracking so concurrent `mutate(varsB)` after slow
 * `mutate(varsA)` doesn't corrupt A's awaited result.
 *
 * Callback errors in `onSuccess` / `onError` / `onSettled` are CONTAINED —
 * they're logged but don't propagate to `mutate()`'s awaited promise. The
 * caller's `await mutate(vars)` always reflects `fn`'s outcome.
 *
 * `onSettled` is guaranteed to fire — success path, error path, even if
 * `onSuccess` or `onError` throws.
 *
 * @template TData, TVars, TCtx
 * @param {import("./Query.js").QueryClient} qc
 * @param {import("./Query.js").MutationOptions<TData, TVars, TCtx>} mutOpts
 * @returns {import("./Query.js").Mutation<TData, TVars>}
 *
 * @example
 *   const addTodo = mutation(qc, {
 *       fn: (text) => fetch("/api/todos", { method: "POST", body: text }).then(r => r.json()),
 *       onMutate: (text) => {
 *           const prev = qc.getQueryData(["todos"]);
 *           qc.setQueryData(["todos"], (old) => [...old, { text, _optimistic: true }]);
 *           return { prev };
 *       },
 *       onError:   (err, vars, ctx) => qc.setQueryData(["todos"], ctx.prev),
 *       onSuccess: () => qc.invalidate(["todos"]),
 *   });
 */
export function mutation(qc, mutOpts) {
    const data = signal(undefined);
    const error = signal(undefined);
    const status = signal("idle");

    // mutationGen mirrors the fetchGen pattern used in queries. Two rapid
    // mutate() calls — slow first, fast second — must not let the first one
    // overwrite the second's settled state. Gen-guarding the SIGNAL writes
    // (not the promises or callbacks) preserves the right semantics:
    //   - each mutate() promise resolves/rejects with its own outcome
    //   - state signals reflect the LATEST mutation only
    //   - callbacks always fire (the user fired the mutation; they get the
    //     hook regardless of who finished first)
    let mutationGen = 0;

    async function mutate(vars) {
        const gen = ++mutationGen;
        // Mark pending immediately on the new mutation. This is the LATEST
        // generation by definition, so no gen check needed here.
        status.set("pending");
        error.set(undefined);

        let ctx;
        let resolvedData;
        let resolvedError;

        // Phase 1: run the mutation (onMutate + fn).
        // We collect outcome into local variables and DON'T let onSuccess /
        // onError be inside this try — those are callbacks, and a throw in a
        // callback shouldn't flip mutation state from success to error or
        // vice-versa.
        try {
            if (mutOpts.onMutate) ctx = await mutOpts.onMutate(vars);
            resolvedData = await mutOpts.fn(vars);
        } catch (err) {
            resolvedError = err;
        }

        // Phase 2: update signals under the gen guard. Only the latest
        // mutation gets to set state.
        if (gen === mutationGen) {
            if (resolvedError) {
                error.set(resolvedError);
                status.set("error");
            } else {
                data.set(resolvedData);
                status.set("success");
            }
        }

        // Phase 3: side-effect callbacks. Errors are contained — a buggy
        // onSuccess should not abort the rest of the chain or flip state.
        // (This is the deliberate deviation from the reviewer's pattern,
        // which has onSuccess throws cascading to the catch block and firing
        // onError with the callback's error. That's worse than the original.)
        try {
            if (resolvedError) {
                if (mutOpts.onError) await mutOpts.onError(resolvedError, vars, ctx);
            } else {
                if (mutOpts.onSuccess) await mutOpts.onSuccess(resolvedData, vars, ctx);
            }
        } catch { /* callback errors don't propagate or alter state */ }

        // Phase 4: onSettled ALWAYS runs. This is what the user relies on for
        // UI cleanup (hide spinner, re-enable submit button). A bug in any
        // earlier callback cannot lock their UI.
        if (mutOpts.onSettled) {
            try {
                await mutOpts.onSettled(resolvedData, resolvedError, vars, ctx);
            } catch { /* same containment */ }
        }

        // Phase 5: the user's awaited promise reflects the fetch outcome,
        // unaffected by callback errors. mutate(varsA) returns A's outcome
        // even if a later mutate(varsB) is running concurrently.
        if (resolvedError) throw resolvedError;
        return resolvedData;
    }

    return {
        data:    () => data(),
        error:   () => error(),
        status:  () => status(),
        loading: () => status() === "pending",
        mutate,
        reset() {
            mutationGen++;                               // invalidate any in-flight
            data.set(undefined);
            error.set(undefined);
            status.set("idle");
        },
        // Releases data/error/status signal nodes back to lite-signal's pool.
        // Mutations are usually long-lived (one per logical action), so most
        // apps never call this — but for ephemeral mutations or tests that
        // build + tear down many in a row, calling dispose() prevents pool
        // pressure on the default registry.
        dispose() {
            mutationGen++;                               // cancel any in-flight
            try { disposeNode(data); }    catch {}
            try { disposeNode(error); }   catch {}
            try { disposeNode(status); }  catch {}
        },
    };
}
