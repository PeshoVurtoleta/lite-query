/**
 * @zakkster/lite-query -- async cache + reactive queries built on lite-signal.
 *
 * Design pillars:
 *   - Explicit observer reference counting per cache entry. attach() ++count,
 *     detach() --count; count -> 0 aborts in-flight fetch + schedules cacheTime
 *     GC; count: 0 -> >=1 cancels the pending GC timer. Shared-observer correctness
 *     (one dispose doesn't starve the others) falls out of this.
 *   - Generation guard on every fetch. Each runFetch increments entry.fetchGen;
 *     resolutions check gen before mutating state. Aborted-but-still-resolving
 *     fetches are silently ignored. No race window.
 *   - Cross-tab: BroadcastChannel-backed, opt-in. Cache writes / invalidations /
 *     removals propagate; background fetch results do NOT (otherwise tabs cross-
 *     talk forever). A `processingRemote` flag suppresses echo loops.
 *   - Mid-flight invalidation: option (b) -- let the in-flight finish, then
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

// The single runtime source of this package's version. Re-exported by the
// /stream and /await entries so all three subpaths report one string, and
// asserted equal to package.json by test/version-sync.test.js. The /release
// drill bumps this in lockstep with package.json + llms.txt (packaging law:
// same commit or not at all).
export const VERSION = "1.3.0";

const noop = () => {};

// -----------------------------------------------------------------------------
// Key hashing & matching
// -----------------------------------------------------------------------------

/**
 * Stable hash for query keys. Objects are serialized with sorted property
 * order so `{a:1, b:2}` and `{b:2, a:1}` hash identically. Arrays preserve
 * order -- array indices are meaningful in query keys.
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

// -----------------------------------------------------------------------------
// Option resolution
// -----------------------------------------------------------------------------

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
        // isLeader function is supplied, follower tabs don't fetch -- they ask
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
// these to decide whether to retry -- e.g., a user-initiated detach (component
// unmounting) is non-retryable, but a timeout might be.
const ABORT_REASON = Object.freeze({
    DETACH:   "lite-query:detach",       // last observer left (or reactive key changed)
    REFETCH:  "lite-query:refetch",      // forced via .refetch()
    REMOVED:  "lite-query:removed",      // qc.removeQueries / qc.clear
    TIMEOUT:  "lite-query:timeout",      // per-query timeout exceeded
});

// -----------------------------------------------------------------------------
// Infinite-query page accumulation (cold path)
// -----------------------------------------------------------------------------

// An equals that never reports equal. An infinite entry's `data` signal is
// created with this so a same-ref `data.set(entry.pages)` still notifies after
// an in-place page push -- lite-signal otherwise skips the notification when
// the reference is unchanged (Object.is dedup). Plain entries keep Object.is.
const NEVER_EQUAL = () => false;

// Initialize the seven infinite slots to their "page one is due" state: empty
// accumulation, no cursor yet, hasNext true (so hasNextPage() reads true before
// the first fetch). isInfinite / pageGen / getNextCursor are set by the caller.
function resetPages(entry) {
    entry.pages = [];
    entry.flat = [];
    entry.nextCursor = null;
    entry.hasNext = true;
}

// Append a freshly-resolved page. `startCursor === null` marks a page-one fetch
// (initial load or a post-invalidate refetch): the accumulation is rebuilt from
// scratch (replace-on-success -- old pages stayed visible until now), otherwise
// the page appends in place with a flat loop-append (no O(n) re-copy of prior
// pages). `startGen` guards against a page resolving into a generation a later
// invalidate already superseded (the mid-flight let-finish path does not bump
// fetchGen, so this pageGen check is what swallows the stale page).
//
// ATOMIC: user `getNextCursor` runs AFTER the page is staged but BEFORE the
// commit is published, and a throw rolls the staged mutation back to exactly the
// pre-commit state (fresh arrays discarded on replace; page + its flat items
// popped on append) then re-throws. The resolution path routes that throw into
// the error ladder, so a getNextCursor that throws cannot wedge the entry and a
// later fetchNextPage()/refetch() re-attempts the same cursor cleanly.
function commitPage(entry, pageData, startGen, startCursor) {
    if (startGen !== entry.pageGen) return;         // dead generation -- swallow
    // Stage into fresh arrays on a page-one replace (so a throw leaves the old
    // pages intact), or into the live arrays on an append (rolled back on throw).
    const pagesArr = startCursor === null ? [] : entry.pages;
    const flatArr  = startCursor === null ? [] : entry.flat;
    const flatMark = flatArr.length;
    pagesArr.push(pageData);
    if (Array.isArray(pageData)) {
        for (let i = 0; i < pageData.length; i++) flatArr.push(pageData[i]);
    } else {
        flatArr.push(pageData);
    }
    let cursor;
    try {
        cursor = entry.getNextCursor(pageData, pagesArr);
    } catch (err) {
        if (startCursor !== null) {                 // append -> undo the staging
            pagesArr.pop();
            flatArr.length = flatMark;
        }                                           // replace -> fresh arrays discarded
        throw err;
    }
    entry.pages = pagesArr;
    entry.flat = flatArr;
    entry.nextCursor = cursor === undefined ? null : cursor;
    entry.hasNext = entry.nextCursor !== null;
    entry.data.set(entry.pages);
}

// Recompute an infinite entry's cursor + hasNext from its current pages, by
// calling the installed getNextCursor on the last page. The SINGLE cursor-
// recompute site in the file: both rebuildInfinite (whole-list writes) and the
// configure() adoption branch (hydrate restore, first attach) route through
// here (ON-1). A no-op when getNextCursor is null or there are no pages -- so a
// seeded entry whose cursor function is not yet installed leaves hasNext alone.
// May THROW if the user getNextCursor throws; callers contain it as they must.
function recomputeCursor(entry) {
    const pages = entry.pages;
    if (entry.getNextCursor && pages.length > 0) {
        const c = entry.getNextCursor(pages[pages.length - 1], pages);
        entry.nextCursor = c === undefined ? null : c;
        entry.hasNext = entry.nextCursor !== null;
    }
}

// Rebuild an infinite entry from an externally-supplied pages array (a manual
// qc.setQueryData or a cross-tab / shared-fetch broadcast of the whole list).
// Recomputes the flat view + cursor so a follower can request the next page.
function rebuildInfinite(entry, pages) {
    const flat = [];
    for (let i = 0; i < pages.length; i++) {
        const p = pages[i];
        if (Array.isArray(p)) for (let j = 0; j < p.length; j++) flat.push(p[j]);
        else flat.push(p);
    }
    entry.pages = pages;
    entry.flat = flat;
    recomputeCursor(entry);
    entry.data.set(pages);
}

// -----------------------------------------------------------------------------
// queryClient
// -----------------------------------------------------------------------------

/**
 * Create a query client -- the cache + lifecycle owner. Make one per app (or
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

    // -- cross-tab --

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

    // -- entries --

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
            // Stream slots -- uniform on every entry to keep the shape
            // monomorphic (no second hidden class at the hot attach/detach/GC
            // sites). A query entry leaves these at their null/false/0 defaults;
            // only a streamQuery entry populates them. No stream signal node is
            // allocated for a plain query.
            isStream: false,
            streamStop: null,            // () => void  -- abort the iterator / close the connection
            streamRestart: null,         // () => void  -- abort + re-establish (invalidate)
            streamCount: 0,              // non-reactive: values seen this session
            streamDropped: 0,            // non-reactive: values dropped (buffer mode)
            // Infinite-query slots -- uniform on every entry (same monomorphism
            // rule as the stream slots above). A plain query() entry leaves all
            // seven at their null/false/0 defaults and allocates no extra node;
            // only an infiniteQuery() entry populates them. `pages` rides the
            // entry's `data` signal (set(entry.pages) after each in-place push);
            // `flat` is a non-reactive flattened view like streamCount.
            isInfinite: false,
            pages: null,                 // array of raw page results (== data() value)
            flat: null,                  // flattened accumulation (live, grows in place)
            nextCursor: null,            // cursor for the NEXT page fetch
            hasNext: false,              // is there a next page to fetch?
            pageGen: 0,                  // accumulation generation (invalidate bumps it)
            getNextCursor: null,         // (lastPage, allPages) => cursor | null
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
            // Entry has no observers yet -- schedule GC immediately. attach()
            // will cancel this if an observer arrives before the timer fires.
            scheduleGc(e);
        }
        return e;
    }

    // Releases the per-entry signal nodes back to lite-signal's pool. Without
    // this, an app that creates + removes many query keys over its lifetime
    // (route-mounted queries in an SPA, long-running diagnostic dashboards)
    // accumulates entries' signal handles in the registry until the next GC
    // run -- eventually tripping the registry capacity cap. Called from the
    // three entry-removal sites: GC timer, removeQueries, clear.
    function disposeEntry(entry) {
        // Stop any live stream first (abort the iterator -> iterator.return(),
        // closing the underlying SSE/websocket) before releasing signal nodes.
        if (entry.streamStop) {
            try { entry.streamStop(); } catch {}
            entry.streamStop = null;
            entry.streamRestart = null;
        }
        // Release the infinite-query accumulation so a destroyed entry does not
        // pin its pages/flat arrays until the next major GC. These are plain
        // arrays, not signal nodes -- nulling is the whole release.
        if (entry.isInfinite) {
            entry.pages = null;
            entry.flat = null;
            entry.getNextCursor = null;
            entry.nextCursor = null;
            entry.hasNext = false;
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
        // .unref doesn't exist -- the guard makes this a no-op there.
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

    // -- attach / detach --

    function attach(entry, queryOpts) {
        // First observer to attach configures the entry's per-query options.
        // Subsequent attaches don't override -- first wins. (A common gotcha
        // in TanStack too: two queries with the same key + different
        // staleTimes -- first one wins, document it.)
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
            // Last observer gone -- abort in-flight if any. Resolution paths
            // gate on the generation guard, so a late resolution is harmless.
            if (entry.abortController) {
                entry.abortController.abort(ABORT_REASON.DETACH);
                entry.abortController = null;
                entry.promise = null;
                entry.fetching.set(false);
                if (entry.status() === "pending") entry.status.set("idle");
            }
            // Last observer gone on a stream -- close the connection. The entry
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

    // -- fetch lifecycle --

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
    // never hangs -- liveness guarantee during leader elections or when the
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
            // The timer firing means no leader fulfilled the request -- the
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
        // Infinite entries only auto-fetch when a next page is due: hasNext is
        // true before the first page (resetPages) and after each non-terminal
        // page, false once the cursor runs out. A forced page-one refetch
        // (invalidate / refetch) re-arms hasNext before calling in.
        if (entry.isInfinite && !entry.hasNext) {
            return entry.promise || Promise.resolve(undefined);
        }

        // Abort any prior in-flight fetch. The old promise's resolution will
        // be filtered by the generation guard below.
        if (entry.abortController) {
            entry.abortController.abort(ABORT_REASON.REFETCH);
        }

        const gen = ++entry.fetchGen;
        // Snapshot the accumulation generation + cursor at fetch start (infinite
        // only). commitPage uses them: startCursor === null => page one (replace),
        // startGen mismatch => a page that a later invalidate superseded.
        const startPageGen = entry.pageGen;
        const startCursor = entry.isInfinite ? entry.nextCursor : undefined;
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
                    const data = await entry.fetcher(entry.isInfinite
                        ? { key: entry.key, cursor: startCursor, signal: ac.signal }
                        : { key: entry.key, signal: ac.signal });
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

            if (outcome.ok && entry.isInfinite) {
                // A throw from user getNextCursor (via commitPage) is contained
                // into the error ladder exactly as a rejected fetcher would be:
                // committed pages are preserved (commitPage rolled the staged
                // page back), status/error/fetching/promise are settled, and a
                // later fetchNextPage()/refetch() re-attempts cleanly.
                try {
                    commitPage(entry, outcome.data, startPageGen, startCursor);
                } catch (commitErr) {
                    entry.error.set(commitErr);
                    entry.status.set("error");
                    entry.lastCompletedAt = opts.now();
                    entry.invalidatedSinceCompletion = false;
                    entry.fetching.set(false);
                    entry.promise = null;
                    entry.abortController = null;
                    return Promise.reject(commitErr);
                }
            }

            if (outcome.ok) {
                if (!entry.isInfinite && !entry.equals(entry.data(), outcome.data)) {
                    entry.data.set(outcome.data);
                }
                entry.error.set(undefined);
                entry.status.set("success");
                // Shared-fetch: the leader broadcasts its results so follower
                // tabs receive them without issuing their own network calls. An
                // infinite entry broadcasts the WHOLE pages array (one entry,
                // one list); followers rebuild flat + cursor from it.
                if (sharedFetchActive && opts.isLeader()) {
                    broadcast({ type: "setData", key: entry.key,
                        value: entry.isInfinite ? entry.pages : outcome.data });
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

    // -- public cache API --

    function getQueryData(key) {
        const e = entries.get(hashKey(key));
        if (!e) return undefined;
        // Peek without subscribing -- getQueryData is an imperative read.
        return untrack(() => e.data());
    }

    function setQueryData(key, valueOrFn) {
        const e = ensureEntry(key);
        const newVal = typeof valueOrFn === "function"
            ? valueOrFn(untrack(() => e.data()))
            : valueOrFn;
        // Infinite entries store the pages ARRAY in `data`; a manual or
        // cross-tab write replaces the whole list and rebuilds the flat view +
        // cursor so hasNextPage()/fetchNextPage() stay coherent. A non-array on
        // an infinite entry is a contract violation, handled fail-closed but
        // asymmetric: a LOCAL caller gets a TypeError (a programming error);
        // a REMOTE/cross-tab apply cannot throw across tabs, so it drops the
        // payload and leaves the entry untouched (coherence preserved).
        if (e.isInfinite && !Array.isArray(newVal)) {
            if (processingRemote) return;
            throw new TypeError(
                "setQueryData: infinite entries accept an array of pages, got " +
                (newVal === null ? "null" : typeof newVal));
        }
        if (e.isInfinite) {
            rebuildInfinite(e, newVal);
        } else {
            e.data.set(newVal);
        }
        e.error.set(undefined);
        e.status.set("success");
        e.lastCompletedAt = opts.now();
        // If a shared-fetch follower was awaiting the leader's result, this IS
        // that result -- stop the loading state and cancel the fallback timer.
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
            } else if (e.isInfinite) {
                // Refetch the whole list from page one, replacing on success
                // (stale-while-revalidate: the old pages stay visible until the
                // page-one refetch lands). Bumping pageGen + resetting the cursor
                // makes any in-flight page from the old generation a no-op in
                // commitPage; hasNext re-arms so the page-one fetch can run.
                e.pageGen++;
                e.nextCursor = null;
                e.hasNext = true;
                if (e.observerCount > 0 && e.fetcher) {
                    if (e.promise) {
                        e.pendingRefetchAfterCurrent = true;
                    } else if (sharedFetchActive && !opts.isLeader()) {
                        requestSharedFetch(e);
                    } else {
                        runFetch(e).catch(noop);
                    }
                }
            } else if (e.observerCount > 0 && e.fetcher) {
                if (e.promise) {
                    e.pendingRefetchAfterCurrent = true;
                } else if (sharedFetchActive && !opts.isLeader()) {
                    // Follower: defer to the leader rather than fetching locally.
                    // (If this invalidate arrived as a broadcast, the fetch-req
                    // is suppressed during remote processing -- but the leader
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

    // Warm an entry into the cache with zero observers -- a route-loader /
    // hover speculation primitive. The entry fetches (unless already fresh) and
    // then GCs after cacheTime exactly like an observed one whose observers all
    // left; a later query() with the same key adopts it (no refetch if fresh).
    //
    // OR-4: prefetch of an already-fresh entry is a NO-OP -- no fetch is issued
    // and the pending cacheTime GC is NOT re-armed (ensureEntry scheduled it
    // once at creation; a fresh hit must not extend the entry's lease). A
    // `force` variant is deferred until a consumer needs it.
    function prefetch(key, fetcher, prefetchOpts = {}) {
        const e = ensureEntry(key);
        // Infinite entries are never prefetched: prefetch carries no page cursor
        // and must not advance live pagination (with staleTime 0 the entry is
        // always stale, so an unguarded runFetch would silently fetch the NEXT
        // page). Strict no-op -- return the in-flight promise or current data,
        // never runFetch, never touch pagination or GC arming.
        if (e.isInfinite) {
            return e.promise || Promise.resolve(untrack(() => e.data()));
        }
        if (fetcher && !e.fetcher) e.fetcher = fetcher;
        if (prefetchOpts.staleTime  !== undefined) e.staleTime  = prefetchOpts.staleTime;
        if (prefetchOpts.cacheTime  !== undefined) e.cacheTime  = prefetchOpts.cacheTime;
        if (prefetchOpts.timeout    !== undefined) e.timeout    = prefetchOpts.timeout;
        if (prefetchOpts.retry      !== undefined) e.retry      = prefetchOpts.retry;
        if (prefetchOpts.retryDelay !== undefined) e.retryDelay = prefetchOpts.retryDelay;
        if (!e.fetcher) return Promise.resolve(undefined);
        if (!shouldFetch(e)) {
            // Fresh -> no-op. Return the in-flight promise if one exists, else
            // the current data; never re-arm GC, never issue a fetch.
            return e.promise || Promise.resolve(untrack(() => e.data()));
        }
        return runFetch(e).catch(noop);
    }

    // -- persistence (cold path) --

    // Serialize the SUCCESS entries into a plain-JSON snapshot. Cold by
    // definition: it walks the whole entries map (OR-8). Emits STATE only --
    // no version field (the adapter stamps { version, state }, OR-6). Each
    // record is monomorphic: exactly four own keys { key, data, dataUpdatedAt,
    // infinite }. Pending / error / stream entries are excluded (a promise, a
    // failure, a live connection are not data); an infinite entry carries a
    // SHALLOW COPY of its pages array (pages is the one structure the cache
    // grows in place). key, data, and page CONTENTS are references, not deep
    // copies -- serialize the payload before any further cache write.
    function dehydrate() {
        const out = [];
        for (const e of entries.values()) {
            if (untrack(() => e.status()) !== "success") continue;
            if (e.isStream) continue;
            if (!Number.isFinite(e.lastCompletedAt)) continue;
            if (e.isInfinite) {
                if (!Array.isArray(e.pages)) continue;
                out.push({ key: e.key, data: e.pages.slice(),
                    dataUpdatedAt: e.lastCompletedAt, infinite: true });
            } else {
                const d = untrack(() => e.data());
                if (d === undefined) continue;
                out.push({ key: e.key, data: d,
                    dataUpdatedAt: e.lastCompletedAt, infinite: false });
            }
        }
        return { entries: out };
    }

    // Validate a hydrate payload in ONE pass over the whole state BEFORE any
    // mutation (OR-3 all-or-nothing). Returns a stable ASCII reason code on the
    // first defect, or null when the whole payload is clean. A malformed STORED
    // payload is external data -> a reason, never a throw (the throw is reserved
    // for the OR-2 empty-cache precondition, a programming error).
    function validateHydrateState(state) {
        if (state === null || typeof state !== "object" || Array.isArray(state)) {
            return "malformed-state";
        }
        if (!Array.isArray(state.entries)) return "malformed-entries";
        if (Object.keys(state).length !== 1) return "malformed-state";
        const seen = new Set();
        for (const rec of state.entries) {
            if (rec === null || typeof rec !== "object" || Array.isArray(rec)) {
                return "malformed-entry";
            }
            if (Object.keys(rec).length !== 4) return "malformed-entry";
            if (!Array.isArray(rec.key)) return "malformed-key";
            if (typeof rec.infinite !== "boolean") return "malformed-entry";
            if (!Number.isFinite(rec.dataUpdatedAt)) return "malformed-timestamp";
            if (rec.infinite) {
                if (!Array.isArray(rec.data)) return "malformed-pages";
            } else if (rec.data === undefined) {
                return "malformed-data";
            }
            const h = hashKey(rec.key);
            if (seen.has(h)) return "duplicate-key";
            seen.add(h);
        }
        return null;
    }

    // Seed a plain entry from a validated record. Writes the signals directly
    // (never setQueryData -> never a broadcast, V5) so a boot seeding cannot
    // echo across tabs. lastCompletedAt is CLAMPED to our clock: a finite future
    // timestamp bounds freshness at one staleTime from boot, never invents data
    // (ON-3b). status is "success" -- the whole point of hydrate.
    function seedEntry(e, rec) {
        e.data.set(rec.data);
        e.error.set(undefined);
        e.status.set("success");
        e.lastCompletedAt = Math.min(rec.dataUpdatedAt, opts.now());
        e.invalidatedSinceCompletion = false;
    }

    // Seed an infinite entry from a validated record. Restores the shape itself
    // (V2): mark isInfinite, recreate the data node with NEVER_EQUAL so a later
    // in-place page push still notifies, rebuild pages + flat through the same
    // rebuildInfinite loop. getNextCursor is NULL (functions never serialize) --
    // it is installed and the cursor recomputed at the first infiniteQuery
    // attach (configure() adoption). hasNext is FALSE (fail closed: an
    // unattached restored list can never auto-fetch at a wrong cursor -- runFetch
    // bails on !hasNext).
    function seedInfinite(e, rec) {
        e.isInfinite = true;
        e.getNextCursor = null;
        e.pageGen = 0;
        try { disposeNode(e.data); } catch {}
        e.data = signal(undefined, { equals: NEVER_EQUAL });
        rebuildInfinite(e, rec.data);      // pages + flat (recompute is a no-op: getNextCursor null)
        e.nextCursor = null;
        e.hasNext = false;
        e.error.set(undefined);
        e.status.set("success");
        e.lastCompletedAt = Math.min(rec.dataUpdatedAt, opts.now());
        e.invalidatedSinceCompletion = false;
    }

    // Boot-only cache restore (OR-2). Fail-closed:
    //   1. THROWS if the cache is non-empty (a late hydrate over live data is
    //      the ABA bug of this domain) -- reserved for that programming error.
    //   2. Validates the WHOLE payload before any mutation; a malformed stored
    //      payload RETURNS { ok:false, count:0, reason } (external data drops,
    //      never throws -- the asymmetry mirrors setQueryData's local-throw /
    //      remote-drop law).
    //   3. Seeds only after a clean pass; never broadcasts, never notifies (V5).
    function hydrate(state) {
        if (entries.size > 0) {
            throw new Error("lite-query: hydrate requires an empty cache (" +
                entries.size + " entries present) -- hydrate at boot, before " +
                "any observer attaches");
        }
        const reason = validateHydrateState(state);
        if (reason !== null) return { ok: false, count: 0, reason };
        for (const rec of state.entries) {
            const e = ensureEntry(rec.key);
            if (rec.infinite) seedInfinite(e, rec);
            else seedEntry(e, rec);
        }
        return { ok: true, count: state.entries.length, reason: null };
    }

    // Dispose the entire client. Releases the BroadcastChannel listener which
    // would otherwise keep the client + its entire cache map alive in
    // scenarios where clients are created and discarded -- testing,
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
        prefetch,
        dehydrate,
        hydrate,
        dispose,
        // Internal API consumed by query()/mutation(). Not part of the public
        // surface; documented as such in llms.txt.
        _internal: { entries, ensureEntry, attach, detach, maybeFetch, runFetch, requestSharedFetch, sharedFetchActive, opts },
    };
}

// -----------------------------------------------------------------------------
// query()
// -----------------------------------------------------------------------------

/**
 * Define a reactive query -- the read-side primitive. Lazy: no fetch fires
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

    // currentEntry is itself a signal -- accessors subscribe to it so they
    // refire when the key changes (reactive key) or attach/detach flips.
    const currentEntry = signal(null);

    // Lazy watcher: only running when outer subscribers exist. This is the
    // "no observers -> no fetch" property -- query() alone doesn't fetch; the
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
            // ONLY key and enabled are tracked here -- internal entry state
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
            // key didn't actually change -- refetch only happens on explicit
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
     * `cleanupObserver` closure above -- no per-read allocation).
     *
     * Critical: between an effect's re-run cleanup and body, observerCount
     * transits N -> 0 -> N. Naively stopping the watcher when count hits zero
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
            // Return currentEntry's signal node to lite-signal's pool -- without
            // this, an app that creates + disposes many queries (e.g. many
            // routes over an SPA lifetime) leaks one signal per query() call.
            try { disposeNode(currentEntry); } catch {}
        },
    };
}

// -----------------------------------------------------------------------------
// infiniteQuery()
// -----------------------------------------------------------------------------

/**
 * Define a cursor-paginated query -- the accumulating sibling of query(). One
 * cache entry holds the whole list: `pages()` is the array of raw page results,
 * `data()` is the flattened accumulation. `fetchNextPage()` fetches the page at
 * the current cursor and appends it in place; `getNextCursor(lastPage, allPages)`
 * returns the cursor for the next fetch, or `null`/`undefined` when exhausted
 * (then `hasNextPage()` reads false). Same lazy + abort-on-detach + reactive-key
 * + enabled lifecycle as query(); lives in the SAME cache, so getQueryData /
 * setQueryData / invalidate / removeQueries / cross-tab operate on it uniformly.
 *
 * The fetcher receives `{ key, cursor, signal }` (cursor is `null` for page one).
 *
 * @template T, C, K
 * @param {import("./Query.js").QueryClient} qc
 * @param {import("./Query.js").InfiniteQueryOptions<T, C, K>} infOpts
 * @returns {import("./Query.js").InfiniteQuery<T>}
 *
 * @example
 *   const feed = infiniteQuery(qc, {
 *       key: ["feed"],
 *       fetcher: ({ cursor, signal }) =>
 *           fetch(`/api/feed?after=${cursor ?? ""}`, { signal }).then(r => r.json()),
 *       getNextCursor: (last) => last.nextCursor ?? null,
 *   });
 *   effect(() => console.log(feed.data()));            // attach + fetch page one
 *   button.onclick = () => feed.fetchNextPage();
 */
export function infiniteQuery(qc, infOpts) {
    if (infOpts === null || typeof infOpts !== "object") {
        throw new TypeError("infiniteQuery: options object is required");
    }
    if (infOpts.key === undefined) {
        throw new TypeError("infiniteQuery: `key` is required");
    }
    if (typeof infOpts.fetcher !== "function") {
        throw new TypeError("infiniteQuery: `fetcher` must be a function ({ key, cursor, signal }) => Promise");
    }
    if (typeof infOpts.getNextCursor !== "function") {
        throw new TypeError("infiniteQuery: `getNextCursor` must be a function (lastPage, allPages) => cursor | null");
    }

    const { ensureEntry, attach, detach, maybeFetch, runFetch, requestSharedFetch, sharedFetchActive, opts } = qc._internal;

    // Configure a fresh entry as infinite exactly once. The data signal is
    // recreated with NEVER_EQUAL so an in-place page push (same array ref)
    // still notifies pages()/data(); plain entries keep Object.is. A shared or
    // GC-recreated entry re-configures only when isInfinite is still false.
    function configure(entry) {
        if (entry.isInfinite) {
            // Adoption (OR-4): a hydrate-seeded infinite entry has its pages
            // restored but getNextCursor === null (functions never serialize).
            // This first attach installs the cursor function and recomputes the
            // cursor + hasNext from the restored pages -- the only state that
            // reaches here with isInfinite true and getNextCursor null (a live
            // configure sets it; disposeEntry nulls it only after removing the
            // entry from the map, ON-1). A getNextCursor that THROWS during
            // adoption is contained fail-closed: reset to a clean page-one fetch
            // (the maybeFetch two lines downstream then loads page one), never a
            // wedge.
            if (entry.getNextCursor === null) {
                entry.getNextCursor = infOpts.getNextCursor;
                try {
                    recomputeCursor(entry);
                } catch {
                    resetPages(entry);
                    entry.data.set(undefined);
                    entry.status.set("idle");
                    entry.lastCompletedAt = -Infinity;
                }
            }
            return;
        }
        entry.isInfinite = true;
        entry.getNextCursor = infOpts.getNextCursor;
        entry.pageGen = 0;
        try { disposeNode(entry.data); } catch {}
        entry.data = signal(undefined, { equals: NEVER_EQUAL });
        resetPages(entry);
        // data() stays undefined until the first page commits, so the status
        // ladder reports "pending" on the initial load (the pending guard keys
        // off data() === undefined). pages()/data()/hasNextPage() subscribe to
        // this signal and re-run when commitPage notifies.
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
        watcher = createRoot(() => effect(() => {
            const keyVal = typeof infOpts.key === "function"
                ? infOpts.key()
                : infOpts.key;
            const isEnabled = infOpts.enabled === undefined
                ? true
                : typeof infOpts.enabled === "function"
                    ? infOpts.enabled()
                    : !!infOpts.enabled;

            if (!isEnabled) {
                if (attachedEntry) {
                    untrack(() => detach(attachedEntry));
                    attachedEntry = null;
                }
                currentEntry.set(null);
                return;
            }

            const entry = untrack(() => ensureEntry(keyVal));
            untrack(() => configure(entry));

            if (entry !== attachedEntry) {
                if (attachedEntry) untrack(() => detach(attachedEntry));
                untrack(() => attach(entry, infOpts));
                attachedEntry = entry;
                currentEntry.set(entry);
                untrack(() => maybeFetch(entry));
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
        // The array of raw page results (== the entry's reactive data value).
        pages() {
            trackObserver();
            const e = currentEntry();
            return e ? e.data() : undefined;
        },
        // The flattened accumulation. LIVE and growing: the returned array is
        // appended to in place as pages arrive (subscribe via e.data()). Copy it
        // (`[...feed.data()]`) if you need to retain a page-N snapshot.
        data() {
            trackObserver();
            const e = currentEntry();
            if (!e) return undefined;
            e.data();                        // subscribe: re-run when a page commits
            return e.flat;
        },
        hasNextPage() {
            trackObserver();
            const e = currentEntry();
            if (!e) return false;
            e.data();                        // subscribe: hasNext updates per commit
            return e.hasNext;
        },
        // Fetch the page at the current cursor and append it. Dedups on the
        // entry promise (a concurrent call returns the in-flight one); a no-op
        // once exhausted (hasNext false).
        fetchNextPage() {
            const e = untrack(() => currentEntry());
            if (!e) return Promise.resolve(undefined);
            if (e.promise) return e.promise;
            if (!e.hasNext) return Promise.resolve(undefined);
            if (sharedFetchActive && !opts.isLeader()) {
                requestSharedFetch(e);
                return e.promise || Promise.resolve(untrack(() => e.flat));
            }
            return runFetch(e).catch(noop);
        },
        status() {
            trackObserver();
            const e = currentEntry();
            return e ? e.status() : "idle";
        },
        error() {
            trackObserver();
            const e = currentEntry();
            return e ? e.error() : undefined;
        },
        fetching() {
            trackObserver();
            const e = currentEntry();
            return e ? e.fetching() : false;
        },
        // Refetch the whole list from page one (force), replacing on success.
        refetch() {
            const e = untrack(() => currentEntry());
            if (!e) return Promise.resolve(undefined);
            e.pageGen++;
            e.nextCursor = null;
            e.hasNext = true;
            if (sharedFetchActive && !opts.isLeader()) {
                requestSharedFetch(e);
                return e.promise || Promise.resolve(untrack(() => e.flat));
            }
            return runFetch(e, { force: true });
        },
        dispose() {
            disposed = true;
            stopWatcher();
            try { disposeNode(currentEntry); } catch {}
        },
    };
}

// -----------------------------------------------------------------------------
// mutation()
// -----------------------------------------------------------------------------

/**
 * Define a mutation -- the write-side primitive. Composes the canonical
 * `onMutate` -> `fn` -> (`onSuccess` | `onError`) -> `onSettled` chain with
 * per-call generation tracking so concurrent `mutate(varsB)` after slow
 * `mutate(varsA)` doesn't corrupt A's awaited result.
 *
 * Callback errors in `onSuccess` / `onError` / `onSettled` are CONTAINED --
 * they're logged but don't propagate to `mutate()`'s awaited promise. The
 * caller's `await mutate(vars)` always reflects `fn`'s outcome.
 *
 * `onSettled` is guaranteed to fire -- success path, error path, even if
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
    // mutate() calls -- slow first, fast second -- must not let the first one
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
        // onError be inside this try -- those are callbacks, and a throw in a
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

        // Phase 3: side-effect callbacks. Errors are contained -- a buggy
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
        // apps never call this -- but for ephemeral mutations or tests that
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
