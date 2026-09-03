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
export const VERSION = "2.1.0";

const noop = () => {};

// One persister per client (OR-5 single-slot seam). A second install on the
// same client throws synchronously; stop() clears the reservation so a later
// re-install succeeds. Weak so a discarded client never pins its entry.
const persistInstalled = new WeakSet();

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
        // Cross-tab stream sharing (Q8). When on AND a leader oracle is
        // supplied, the leader tab owns the one iterator and broadcasts frames;
        // followers project them into their local entry (no follower holds an
        // iterator). Same isLeader oracle as sharedFetch. streamIdleTimeout is
        // the watchdog bound: if no frame arrives within it a follower
        // self-connects (OR-3 liveness, extended to streams). Defaults to the
        // shared-fetch timeout so a single knob tunes both liveness windows.
        sharedStream: o.sharedStream ?? false,
        streamIdleTimeout: o.streamIdleTimeout ?? o.sharedFetchTimeout ?? 3000,
        now: o.now ?? (() => Date.now()),
        setTimeout: o.setTimeout ?? ((fn, ms) => globalThis.setTimeout(fn, ms)),
        clearTimeout: o.clearTimeout ?? ((id) => globalThis.clearTimeout(id)),
        broadcastChannel: o.broadcastChannel ??
            (typeof BroadcastChannel !== "undefined" ? BroadcastChannel : null),
    };
}

// refetchInterval validation (ON-4, fail closed at the door). Absent or null =
// off (returns null -- null is not zero, so the scanner can tell "never asked to
// poll" from "poll every 0ms"). Otherwise it MUST be a finite number > 0 (ms);
// 0, negative, NaN, Infinity, or a non-number each throw synchronously at query
// construction. The interval scanner (registerPoll) therefore never sees an
// invalid period -- the door rejects it, not the hot path.
function validateRefetchInterval(v) {
    if (v == null) return null;
    if (typeof v !== "number" || !Number.isFinite(v) || v <= 0) {
        throw new TypeError(
            "lite-query: refetchInterval must be a finite number > 0 (ms), or null/omitted to disable");
    }
    return v;
}

// keepPreviousData validation (ON-4, fail closed at the door). Absent (undefined)
// or false = off; strictly === true to enable; anything else present (null, a
// number, a string) throws TypeError at query construction -- an ambiguous truthy
// value is never silently coerced. Returns the boolean the hold reads.
function validateKeepPreviousData(v) {
    if (v === undefined) return false;
    if (typeof v !== "boolean") {
        throw new TypeError(
            "lite-query: keepPreviousData must be a boolean (keepPreviousData: true to opt in)");
    }
    return v;
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
// Devtools feed (qc.inspect) -- module-level cold scaffolding (Q7)
// -----------------------------------------------------------------------------

// The feed timestamp clock, resolved ONCE at module load: performance.now() when
// available (monotonic, non-decreasing within a process), else Date.now(). NOT
// opts.now -- a mock staleness clock must never stamp the feed. Read per emit,
// AFTER the feed.hook null test (never before, A-2).
const FEED_NOW = (typeof performance === "object" && performance !== null &&
    typeof performance.now === "function")
    ? () => performance.now()
    : () => Date.now();

// The 31 frozen event types, in vocabulary order. One preallocated record per
// type is built at inspect() install and dropped at uninstall (T3 ratified): an
// installed panel under a 60Hz stream allocates zero bytes per frame, and an app
// that never calls inspect() retains zero feed objects.
const FEED_TYPES = [
    "entry:create", "entry:attach", "entry:detach", "entry:gc", "entry:remove",
    "entry:status", "entry:stale",
    "fetch:dispatch", "fetch:settle", "fetch:abort",
    "tab:send", "tab:receive",
    "shared:request", "shared:fallback", "shared:serve",
    "stream:start", "stream:value", "stream:done", "stream:error",
    "mutation:start", "mutation:settle",
    "persist:hydrate", "persist:save",
    // Shared-stream vocabulary (Q8, 23 -> 26; the 10-key record is unchanged).
    // There is deliberately NO stream:frame-send type -- the leader's broadcast
    // is already told by tab:send + stream:value, so a fourth per-frame emit
    // would double 60Hz work for zero new truth (recorded trim).
    "stream:project", "stream:promote", "stream:gap",
    // Offline-queue vocabulary (Q9, 26 -> 31; the 10-key record is unchanged).
    // There is deliberately NO per-frame queue emit -- the queue lifecycle is
    // enqueue/restore/replay/settle/drop, all cold events (OR-7 trim carried).
    "queue:enqueue", "queue:restore", "queue:replay", "queue:settle", "queue:drop",
];

// Build the per-type pooled record table (cold, once per install). Every record
// carries exactly the 10 frozen own keys in one order -> one hidden class per
// type, so a panel's property reads stay monomorphic. The hook MUST copy what it
// keeps: the next event of the same type overwrites these fields in place.
function buildEventPool() {
    const pool = {};
    for (let i = 0; i < FEED_TYPES.length; i++) {
        const t = FEED_TYPES[i];
        pool[t] = {
            type: t, ts: 0, key: null, keyHash: null, from: null, to: null,
            reason: null, count: 0, ok: false, value: null,
        };
    }
    return pool;
}

// The status-funnel reader. Hoisted module-level so setStatus reads an entry's
// prior status with ZERO per-call allocation; the entry is parked in _se
// immediately before the untracked read and NULLED immediately after (QD-4 --
// otherwise the last status-written entry, with its data payload, stays pinned
// module-globally for the process lifetime, surviving uninstall/removeQueries).
// Dispatch is synchronous, so no interleave can occur between the park and the
// release, and the read stays correct.
let _se = null;
const READ_STATUS = () => _se.status();

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

    // -- refetchInterval scanner state (Q10) --
    // ONE client-wide poll scanner (the watchdog precedent, ROADMAP ~:1059-1062):
    // never a timer per entry. pollList is null until the first registerPoll --
    // null is not zero, so an app that never polls pays nothing and the OFF-path
    // assertion `pollList === null` distinguishes "never registered" from "empty".
    // Each record is { entry, interval, nextPollAt, count }; count is the observer
    // refcount so N watchers polling one entry share one record (first interval
    // wins, mirroring attach()'s first-observer-configures rule). pollTimerId holds
    // the single armed check-and-rearm timer (C3).
    let pollList = null;
    let pollTimerId = null;

    // Private single-slot cache-write hook (OR-5). The persistence adapter
    // installs one thunk here; notifyWrite() calls it after every committed
    // cache write at the six V4 sites. null when no adapter is installed, so an
    // unused client pays exactly one `!== null` test at each cold write site and
    // nothing on the warm read path (OR-8/G8). The feed stays PRIVATE until Q7.
    let persistHook = null;
    function notifyWrite() {
        if (persistHook !== null) persistHook();
    }

    // Install the single write hook. Returns an uninstall thunk that nulls the
    // slot only if it still holds this fn. A second install while one is live
    // throws -- the single-slot seam the adapter relies on.
    function installPersistHook(fn) {
        if (typeof fn !== "function") {
            throw new TypeError("lite-query: installPersistHook requires a function");
        }
        if (persistHook !== null) {
            throw new Error("lite-query: a persist hook is already installed on this client (single-slot seam)");
        }
        persistHook = fn;
        return function uninstallPersistHook() {
            if (persistHook === fn) persistHook = null;
        };
    }

    // Public single-slot devtools feed (Q7, OR-4). Independent of persistHook:
    // installing/uninstalling either never touches the other. `hook` is null when
    // no panel is attached -- the uninstalled branch is the product (OR-5), so
    // every emit site is one `feed.hook !== null` test and nothing (no object, no
    // array, no string, no ts read) is constructed before it passes (A-2). `pool`
    // holds the 26 per-type records only while installed.
    const feed = { hook: null, pool: null };

    // Synchronous dispatch funnel (OR-7). A throwing hook is contained fail-closed
    // TOWARD THE FEED: null the slot (an instant return to the zero-cost path),
    // report once, and return so the in-progress cache write completes. Never
    // re-thrown, never queued -- G4's state machine depends on emission order
    // being the truth.
    function fire(ev) {
        const h = feed.hook;
        try {
            h(ev);
        } catch (err) {
            feed.hook = null;
            feed.pool = null;
            try {
                console.error("lite-query: inspect hook threw; feed uninstalled", err);
            } catch { /* a throwing console must not re-enter the funnel */ }
        }
    }

    // Five cold emit closures (one per domain), each grabbing its pooled record,
    // overwriting all 10 fields (never a partial write -- a stale field is a lie),
    // and firing. Reached only from behind a feed.hook !== null test.
    function emitEntry(type, entry, count, reason) {
        const ev = feed.pool[type];
        ev.type = type; ev.ts = FEED_NOW();
        ev.key = entry.key; ev.keyHash = entry.keyHash;
        ev.from = null; ev.to = null;
        ev.reason = reason; ev.count = count; ev.ok = false; ev.value = null;
        fire(ev);
    }
    function emitFetch(type, entry, count, ok, value, reason) {
        const ev = feed.pool[type];
        ev.type = type; ev.ts = FEED_NOW();
        ev.key = entry.key; ev.keyHash = entry.keyHash;
        ev.from = null; ev.to = null;
        ev.reason = reason; ev.count = count; ev.ok = ok; ev.value = value;
        fire(ev);
    }
    function emitStream(type, entry, count, ok, value, reason) {
        const ev = feed.pool[type];
        ev.type = type; ev.ts = FEED_NOW();
        ev.key = entry.key; ev.keyHash = entry.keyHash;
        ev.from = null; ev.to = null;
        ev.reason = reason; ev.count = count; ev.ok = ok; ev.value = value;
        fire(ev);
    }
    // stream:promote carries the epoch transition in from/to (prior epoch ->
    // new epoch); count is the new epochSeq, ok is "won" (true when THIS tab
    // took ownership, false on abdication). Cold -- a promotion is rare.
    function emitStreamPromote(entry, fromEpoch, toEpoch, won, reason) {
        const ev = feed.pool["stream:promote"];
        ev.type = "stream:promote"; ev.ts = FEED_NOW();
        ev.key = entry.key; ev.keyHash = entry.keyHash;
        ev.from = fromEpoch; ev.to = toEpoch;
        ev.reason = reason; ev.count = toEpoch; ev.ok = won; ev.value = null;
        fire(ev);
    }
    function emitTab(type, reason, ok) {
        const ev = feed.pool[type];
        ev.type = type; ev.ts = FEED_NOW();
        ev.key = null; ev.keyHash = null;
        ev.from = null; ev.to = null;
        ev.reason = reason; ev.count = 0; ev.ok = ok; ev.value = null;
        fire(ev);
    }
    function emitClient(type, count, ok, value, reason) {
        const ev = feed.pool[type];
        ev.type = type; ev.ts = FEED_NOW();
        ev.key = null; ev.keyHash = null;
        ev.from = null; ev.to = null;
        ev.reason = reason; ev.count = count; ev.ok = ok; ev.value = value;
        fire(ev);
    }
    function emitStatus(entry, from, to) {
        const ev = feed.pool["entry:status"];
        ev.type = "entry:status"; ev.ts = FEED_NOW();
        ev.key = entry.key; ev.keyHash = entry.keyHash;
        ev.from = from; ev.to = to;
        ev.reason = null; ev.count = 0; ev.ok = false; ev.value = null;
        fire(ev);
    }
    // The queue emit closure (Q9). key/keyHash come from the queue record (never
    // an entry -- a queued mutation may target a key with no live entry); count is
    // the queue length after the transition; ok is control-flow (!rejected), never
    // a truthiness read; value is vars on enqueue / data-or-error verbatim on
    // settle; reason is the drop/settle reason code. All 10 fields overwritten --
    // never a partial write. Reached only from behind a feed.hook !== null test.
    function emitQueue(type, key, keyHash, count, ok, value, reason) {
        const ev = feed.pool[type];
        ev.type = type; ev.ts = FEED_NOW();
        ev.key = key; ev.keyHash = keyHash;
        ev.from = null; ev.to = null;
        ev.reason = reason; ev.count = count; ev.ok = ok; ev.value = value;
        fire(ev);
    }

    // The status write funnel (site 6). Emits entry:status AFTER the signal
    // commit so a re-entering hook observes committed state; fires even when
    // from === to (a setQueryData re-write is a real cache write). The prior
    // status is read UNTRACKED (a tracked read would subscribe -- setQueryData /
    // invalidate are legal inside a user effect, V3). Uninstalled cost: one call
    // frame + one null test; no status write occurs on the warm read path (V7).
    function setStatus(entry, to) {
        if (feed.hook !== null) {
            _se = entry;
            const from = untrack(READ_STATUS);
            _se = null;                      // QD-4: release the pin immediately -- synchronous
            entry.status.set(to);            // dispatch guarantees no interleave before this line,
            emitStatus(entry, from, to);     // so a live entry (+ its data payload) is never retained
            return;
        }
        entry.status.set(to);
    }

    // Install the single devtools feed hook (Q7). Mirrors installPersistHook's
    // two throw shapes exactly (TypeError on a non-function -- a hook array lands
    // here -- Error on double-install) and its === guarded idempotent uninstall.
    // Installing/uninstalling inspect never reads or writes persistHook (OR-4).
    function inspect(hook) {
        if (typeof hook !== "function") {
            throw new TypeError("lite-query: inspect requires a function");
        }
        if (feed.hook !== null) {
            throw new Error("lite-query: an inspect hook is already installed on this client (single-slot seam)");
        }
        feed.pool = buildEventPool();      // cold: 26 records, once per install
        feed.hook = hook;
        return function uninstallInspect() {
            if (feed.hook === hook) { feed.hook = null; feed.pool = null; }
        };
    }

    if (opts.crossTab && opts.broadcastChannel) {
        channel = new opts.broadcastChannel(opts.crossTabChannel);
        channel.addEventListener("message", onRemoteMessage);
    }

    // Shared-fetch is active only when opted in, a leader oracle is supplied,
    // and a channel exists to coordinate over.
    const sharedFetchActive =
        opts.sharedFetch && typeof opts.isLeader === "function" && !!channel;

    // Shared-stream (Q8) uses the same gate: opt-in + a leader oracle + a
    // channel. Inert otherwise -- every tab owns its connection, which is
    // exactly 1.1.0's shipped behaviour, so an app that never opts in pays
    // nothing on any warm path.
    const sharedStreamActive =
        opts.sharedStream && typeof opts.isLeader === "function" && !!channel;

    // One ASCII client identity, built ONCE at construction (cold). Used as the
    // lexicographic tiebreak in the promotion race; NEVER derived from opts.now
    // (a mock clock must not decide ownership). Randomness only breaks ties, so
    // Math.random is acceptable here -- it never touches a warm path.
    const clientId = "c" + Math.random().toString(36).slice(2, 10) +
        Math.random().toString(36).slice(2, 6);

    // -- offline mutation queue (Q9) --
    //
    // Durable per-client store of mutations enqueued while the caller's offline()
    // oracle reported the transport down (T1). null until the first enqueue --
    // null is not zero: no queue is not an empty queue (OR-3). queueSeq is the
    // monotone per-client counter that stamps every record's stable id
    // (clientId + ":" + (++queueSeq)); it survives a persist round trip so ids
    // never collide within a client's lifetime.
    const maxQueue = options.maxQueue === undefined ? 100 : options.maxQueue;
    if (!Number.isInteger(maxQueue) || maxQueue < 1) {
        throw new TypeError("lite-query: maxQueue must be a positive integer");
    }
    let queueStore = null;
    let queueSeq = 0;
    // Single-flight guard for replayQueue (G6 in-run exactly-once BY
    // CONSTRUCTION): a re-entrant call while one is running rejects LQ_REPLAY_BUSY
    // rather than interleaving two dispatch loops over the same durable store.
    let replaying = false;
    // Private single-slot queue-persist seam (Q9, OR-5). The persistence adapter
    // installs { arm, saveRemoval } here when queueSave/queueLoad are supplied.
    // `arm` rides the SAME trailing-edge throttle as the cache doSave (an enqueue
    // or drop coalesces into the next cache save); `saveRemoval(id)` is the
    // synchronous at-least-once removal write. null when no adapter is installed
    // -- the queue is then in-memory only and every write site pays one !== null
    // test and nothing else.
    let queueHooks = null;

    // Number of durably-enqueued mutations (0 when the queue was never touched).
    // Read-only introspection for retention assertions and caller UI.
    function queueSize() {
        return queueStore === null ? 0 : queueStore.length;
    }

    // Remove the record with this id from the durable store (cold). A linear scan
    // -- the queue is small (bounded by maxQueue) and this is off the warm path.
    function removeQueueRecord(id) {
        if (queueStore === null) return;
        for (let i = 0; i < queueStore.length; i++) {
            if (queueStore[i].id === id) { queueStore.splice(i, 1); return; }
        }
    }

    // Build the per-item replay result -- exactly 5 own keys in the frozen order,
    // at THIS one site (monomorphic; a panel's reads stay one hidden class).
    function queueItemResult(rec, itemStatus, value, reason) {
        return { id: rec.id, key: rec.key, status: itemStatus, value, reason };
    }

    // Enqueue a mutation record (cold, one per offline dispatch). Builds the
    // monomorphic 7-key record at THIS one site, in the frozen order, and pushes
    // it onto the durable store (lazily created -- null is not an empty array).
    // Returns the record on success, or null when the queue is full (the caller
    // -- mutate() -- surfaces LQ_QUEUE_FULL; queueSeq is NOT advanced on a full
    // queue, so a rejected enqueue leaves both length and the id counter intact).
    function enqueue(name, key, keyHash, vars) {
        if (queueStore === null) queueStore = [];
        if (queueStore.length >= maxQueue) return null;
        const rec = {
            id: clientId + ":" + (++queueSeq),
            name,
            key,
            keyHash,
            vars,
            at: opts.now(),
            tries: 0,
        };
        queueStore.push(rec);
        if (queueHooks !== null) queueHooks.arm();      // OR-5: persist the enqueue
        if (feed.hook !== null) emitQueue("queue:enqueue", key, keyHash, queueStore.length, true, vars, null);
        return rec;
    }

    // Serialize the queue for persistence (cold). Returns a fresh array of
    // shallow-copied records (each the 7 frozen keys); `excludeId` omits one
    // record -- the removal-write payload that reflects an item's absence WHILE it
    // is still in the live store, so the durable removal precedes the in-memory
    // splice (at-least-once, OR-6). key/vars are references, never deep copies
    // (mirrors dehydrate()); tries is copied by value so a later in-place
    // increment cannot mutate an already-handed-off snapshot.
    function dehydrateQueue(excludeId) {
        if (queueStore === null) return [];
        const out = [];
        for (let i = 0; i < queueStore.length; i++) {
            const r = queueStore[i];
            if (r.id === excludeId) continue;
            out.push({ id: r.id, name: r.name, key: r.key, keyHash: r.keyHash,
                vars: r.vars, at: r.at, tries: r.tries });
        }
        return out;
    }

    // Restore a validated queue on adapter install (before any enqueue). The
    // records keep their ORIGINAL ids (the at-least-once idempotency key survives
    // the round trip); new enqueues in this fresh client stamp a fresh clientId,
    // so restored and new ids can never collide. An empty restored queue is [] --
    // a real (if empty) restored queue, distinct from the untouched null store.
    function restoreQueue(records) {
        queueStore = records;
    }

    // Single-slot install for the queue-persist seam. Mirrors installPersistHook:
    // throws on double-install, returns an idempotent === guarded uninstall.
    function installQueueHook(hooks) {
        if (queueHooks !== null) {
            throw new Error("lite-query: a queue-persist hook is already installed on this client (single-slot seam)");
        }
        queueHooks = hooks;
        return function uninstallQueueHook() {
            if (queueHooks === hooks) queueHooks = null;
        };
    }

    // Replay the durable queue (T3, OR-4). The CALLER triggers it (the library
    // never watches connectivity); `resolve(record) -> fn | null` maps a record
    // back to the handler that re-runs it (keyed off record.name -- what a
    // reloaded tab resolves the handler by). Strictly sequential FIFO (await each)
    // so order is preserved globally and therefore per key. Single-flight (G6):
    // a re-entrant call rejects LQ_REPLAY_BUSY. Per-item results are surfaced; a
    // record whose entry no longer exists, or whose handler cannot be resolved, is
    // DROPPED (never silently retried); a handler REJECTION keeps the item queued
    // (tries++) for the next call; a handler RESOLUTION removes it (at-least-once
    // ordering in doReplayItem). Rejection is tracked by CONTROL FLOW, never by
    // truthiness -- a falsy rejection settles "error" (ON-3-of-Q8 governs replay).
    function replayQueue(resolve) {
        if (typeof resolve !== "function") {
            throw new TypeError("lite-query: replayQueue(resolve) requires a resolver function (record) => fn | null");
        }
        if (replaying) {
            const err = new Error("lite-query: replayQueue is already running (single-flight)");
            err.code = "LQ_REPLAY_BUSY";
            return Promise.reject(err);
        }
        if (queueStore === null || queueStore.length === 0) {
            return Promise.resolve({ status: "empty", total: 0, replayed: 0, failed: 0, dropped: 0, items: [] });
        }
        replaying = true;
        return doReplay(resolve).finally(() => { replaying = false; });
    }

    async function doReplay(resolve) {
        // Snapshot the records present NOW (FIFO order): an enqueue that lands
        // during replay rides the NEXT call, and removal walks the LIVE store by
        // id so the snapshot index never goes stale under a splice.
        const snapshot = queueStore.slice();
        const items = [];
        let replayed = 0, failed = 0, dropped = 0;
        for (let i = 0; i < snapshot.length; i++) {
            const rec = snapshot[i];
            // A record evicted from the LIVE store mid-run -- dropQueued() fired
            // from an earlier item's handler -- must NOT dispatch: the poison-item
            // exit must exit (ON-4). Re-check presence by reference identity in the
            // live store (which the snapshot cannot see). dropQueued already emitted
            // the single queue:drop when it removed the record, so record the
            // dropped result WITHOUT a second feed emit and WITHOUT re-removing.
            if (queueStore === null || queueStore.indexOf(rec) === -1) {
                items.push(queueItemResult(rec, "dropped", undefined, "caller-dropped"));
                dropped++;
                continue;
            }
            // An entry that no longer exists -> DROPPED, never dispatched, never
            // silently retried (OR-3). Resolver failures drop identically.
            if (entries.get(rec.keyHash) === undefined) {
                items.push(queueItemResult(rec, "dropped", undefined, "entry-missing"));
                removeQueueRecord(rec.id); dropped++;
                if (feed.hook !== null) emitQueue("queue:settle", rec.key, rec.keyHash, queueStore.length, false, null, "entry-missing");
                continue;
            }
            let handler;
            let resolverThrew = false;
            try { handler = resolve(rec); } catch { resolverThrew = true; }
            if (resolverThrew) {
                items.push(queueItemResult(rec, "dropped", undefined, "resolver-threw"));
                removeQueueRecord(rec.id); dropped++;
                if (feed.hook !== null) emitQueue("queue:settle", rec.key, rec.keyHash, queueStore.length, false, null, "resolver-threw");
                continue;
            }
            if (typeof handler !== "function") {
                items.push(queueItemResult(rec, "dropped", undefined, "handler-unresolved"));
                removeQueueRecord(rec.id); dropped++;
                if (feed.hook !== null) emitQueue("queue:settle", rec.key, rec.keyHash, queueStore.length, false, null, "handler-unresolved");
                continue;
            }
            // Dispatch: tries increments per attempt. The item stays durable until
            // its terminal result exists (at-least-once, OR-6 -- see doReplayItem).
            rec.tries = (rec.tries + 1) | 0;
            if (feed.hook !== null) emitQueue("queue:replay", rec.key, rec.keyHash, queueSize(), false, rec.vars, null);
            let value;
            let rejected = false;
            try { value = await handler(rec.vars); } catch (err) { rejected = true; value = err; }
            const r = doReplayItem(rec, value, rejected);
            items.push(r);
            if (rejected) failed++; else replayed++;
            if (feed.hook !== null) emitQueue("queue:settle", rec.key, rec.keyHash, queueSize(), !rejected, value, r.reason);
        }
        return { status: "done", total: snapshot.length, replayed, failed, dropped, items };
    }

    // Terminal disposition of one replayed item (OR-6 at-least-once ordering, made
    // explicit in C6): the record's terminal result is recorded FIRST; ONLY THEN
    // is it removed from the durable store. A rejection (tracked by control flow,
    // falsy included) keeps the item queued for the next call. A resolution builds
    // the "ok" result, then removes the record.
    function doReplayItem(rec, value, rejected) {
        if (rejected) {
            return queueItemResult(rec, "error", value, null);
        }
        const r = queueItemResult(rec, "ok", value, null);
        // At-least-once (OR-6): the terminal result now exists (r). Hand the
        // REMOVAL to the durable store FIRST -- the envelope excludes rec.id while
        // rec is still in the live store, so the persisted queue reflects the
        // item's absence before the in-memory splice. Only if that write is
        // accepted (or there is no adapter) do we splice. A save that THROWS on
        // the removal write leaves the record durable -- both here and on disk --
        // and it re-dispatches with the identical id on the next call (G9).
        let persisted = true;
        if (queueHooks !== null) {
            try { queueHooks.saveRemoval(rec.id); }
            catch { persisted = false; }
        }
        if (persisted) removeQueueRecord(rec.id);
        return r;
    }

    // The poison-item exit (ON-4). An offline queue with no removal surface traps
    // a permanently-rejected mutation forever; dropQueued is the caller's explicit
    // eviction. Found -> removed, persisted through the same save path, one
    // queue:drop feed event (reason "caller-dropped"), returns true. Absent
    // (including queueStore === null) -> returns false: an ANSWER, not an error,
    // never a throw. Rejected-forever items stay visible via per-item results +
    // tries until the caller chooses this exit.
    function dropQueued(id) {
        if (queueStore === null) return false;
        for (let i = 0; i < queueStore.length; i++) {
            if (queueStore[i].id === id) {
                const rec = queueStore[i];
                queueStore.splice(i, 1);
                if (queueHooks !== null) queueHooks.arm();   // persist the drop
                if (feed.hook !== null) emitQueue("queue:drop", rec.key, rec.keyHash, queueStore.length, false, null, "caller-dropped");
                return true;
            }
        }
        return false;
    }

    // Highest epochSeq this client has ever seen on any shared-stream key.
    // Whoever opens a stream stamps it with max(seen) + 1, so epochs are
    // globally monotone across the tab set without a shared counter.
    let maxEpochSeen = 0;

    // -- cross-tab --

    function broadcast(msg) {
        if (!channel || processingRemote) return;
        let ok = true;
        try { channel.postMessage(msg); } catch { ok = false; /* serialization or closed */ }
        if (feed.hook !== null) emitTab("tab:send", msg.type, ok);   // site 11
    }

    function onRemoteMessage(evt) {
        processingRemote = true;
        try {
            const m = evt.data;
            if (feed.hook !== null) emitTab("tab:receive", (m && m.type) || null, false);   // site 12
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
                    if (e && e.fetcher) {
                        if (feed.hook !== null) emitEntry("shared:serve", e, 0, "leader");   // site 15
                        runFetch(e).catch(noop);
                    }
                    break;
                }
                // -- shared streams (Q8). All four are inert unless
                // sharedStreamActive AND a local entry has opted into sharing;
                // a tab that never opts in early-returns at the gate. --
                case "stream-open":  if (sharedStreamActive) onStreamOpen(m);  break;
                case "stream-frame": if (sharedStreamActive) onStreamFrame(m); break;
                case "stream-end":   if (sharedStreamActive) onStreamEnd(m);   break;
                case "stream-req":   if (sharedStreamActive) onStreamReq(m);   break;
            }
        } finally {
            processingRemote = false;
        }
    }

    // -- shared streams (Q8) --------------------------------------------------

    // Control-message send that survives the echo guard (V2). A control message
    // (a promotion announce, a stream-req reply) emitted from INSIDE
    // onRemoteMessage would be swallowed by broadcast()'s `processingRemote`
    // early-return; defer it onto a microtask so it leaves once the guard is
    // down -- cold, once per announce, NEVER per frame. The guard itself is not
    // weakened, so the per-frame echo storm the fuzzer pins still cannot loop.
    function broadcastControl(msg) {
        if (!channel) return;
        queueMicrotask(() => {
            if (!channel) return;
            let ok = true;
            try { channel.postMessage(msg); } catch { ok = false; }
            if (feed.hook !== null) emitTab("tab:send", msg.type, ok);
        });
    }

    // Ownership rank: is (ae, ac) strictly below (be, bc) in the (epochSeq,
    // clientId) lexicographic order? The tiebreak is election-state-independent
    // (OR-3): a higher epoch wins; equal epochs break on clientId. opts.now
    // never enters -- a mock clock must not decide ownership.
    function rankBelow(ae, ac, be, bc) {
        if (ae !== be) return ae < be;
        return ac < bc;
    }

    // Release EVERY shared-stream projection slot back to its default (V5). A
    // follower holds no iterator, so detach/disposeEntry's streamStop teardown
    // never touches its window, epoch cursors, watchdog, or the streamPromote
    // closure -- without this they (and the closure's captured /stream scope)
    // survive detach and GC-schedule, manufacturing the OR-10 retention class by
    // construction. Idempotent; safe on a plain query entry (all defaults).
    function releaseProjection(entry) {
        disarmWatchdog(entry);
        entry.streamShared = false;
        entry.streamOwner = false;
        entry.streamPromoting = false;
        entry.streamPromote = null;
        entry.streamMode = null;
        entry.streamMaxBuffer = 0;
        entry.streamEpoch = 0;
        entry.streamSeq = 0;
        entry.projEpoch = -1;
        entry.projClientId = null;
        entry.projSeq = -1;
        // LS4 seam teardown (Q9): end the buffer writer before dropping the window.
        // end() is idempotent, never writes to the target, and nulls the writer's
        // internal win/tgt -- which releases the shim closure's capture of this
        // entry (the retention the release ladder exists to prevent). streamDropped
        // was already mirrored each frame, so droppedCount() post-release is
        // unaffected by the terminal freeze.
        if (entry.projWriter !== null) {
            try { entry.projWriter.end(); } catch { /* terminal freeze must never throw out of release */ }
            entry.projWriter = null;
        }
        entry.projWindow = null;
        entry.lastFrameAt = 0;
    }

    // Relinquish this tab's ownership of a shared stream's iterator (abdication
    // in F3/F5, or teardown). Aborts the live iterator via the streamStop the
    // /stream pump installed, then reverts to a follower (streamOwner false).
    // The entry stays in the map projecting the winner's frames.
    function closeStream(entry) {
        if (entry.streamStop) {
            try { entry.streamStop(); } catch {}
            entry.streamStop = null;
            entry.streamRestart = null;
        }
        entry.streamOwner = false;
    }

    // Follower frame projection. The OR-4 dedup/ordering gate runs BEFORE any
    // signal write, so duplication and reordering are STRUCTURALLY impossible
    // (not merely untested); frame loss is permitted and counted. Latest mode is
    // a single signal write + field stores -- zero allocation per frame. Buffer
    // mode routes through the entry's lite-stream writer (the LS4 seam, Q9): the
    // hand-rolled drop-oldest ring is gone -- lite-stream 1.4.0 owns that discipline.
    function projectFrame(entry, epochSeq, frameClientId, seq, value) {
        // The projection cursor is the TRIPLE (projEpoch, projClientId, projSeq):
        // an epoch is not a unique owner. Two tabs promoting from the same
        // observed epoch both claim max(seen)+1 -- identical epochSeq, distinct
        // clientIds, independent seq counters. Resolving ownership by the SAME
        // (epochSeq, clientId) rank the tiebreak uses makes every follower
        // converge on the winner INDEPENDENT of arrival order (QD-1); seq dedup
        // then applies only WITHIN one (epochSeq, clientId) pair.
        if (epochSeq < entry.projEpoch) return;              // stale epoch -> drop
        let gap = 0;
        let epochChange = false;
        if (epochSeq > entry.projEpoch) {                    // strictly higher epoch: owner boundary
            epochChange = entry.projEpoch !== -1;            // a real boundary, not the first-ever frame
            entry.projEpoch = epochSeq;
            entry.projClientId = frameClientId;
            entry.projSeq = 0;                               // expect seq 1 next (fresh iterator)
        } else if (frameClientId !== entry.projClientId) {   // SAME epoch, different owner (collision)
            // Resolve by rank: a frame from a lower-ranked (epochSeq, clientId)
            // pair is a loser about to abdicate -- DROP it (F5's "discarded, not
            // interleaved", now by construction). A higher-ranked pair is the new
            // owner: adopt it as a boundary, exactly like a higher epoch.
            if (entry.projClientId !== null &&
                !rankBelow(entry.projEpoch, entry.projClientId, epochSeq, frameClientId)) {
                return;                                      // incoming ranks below -> drop
            }
            epochChange = entry.projClientId !== null;
            entry.projClientId = frameClientId;
            entry.projSeq = 0;
        }
        if (seq <= entry.projSeq) return;                    // duplicate within this owner -> drop
        if (seq > entry.projSeq + 1) gap = (seq - entry.projSeq - 1) | 0;   // missed frames (at-most-once loss)
        entry.projSeq = seq;
        if (entry.streamCount === 0) setStatus(entry, "streaming");
        entry.streamCount = (entry.streamCount + 1) | 0;
        // LS4 seam (Q9, OR-8): a buffer-mode follower carries a lite-stream writer
        // installed from /stream; latest mode (and any plain entry) leaves it null
        // and writes the value straight through. One null check replaces the prior
        // streamMode string compare; droppedCount is a getter read (zero alloc).
        const w = entry.projWriter;
        if (w !== null) { w.push(value); entry.streamDropped = w.droppedCount; }
        else entry.data.set(value);                          // latest: one signal write, zero alloc
        if (feed.hook !== null) {                            // cold: only with a panel installed
            emitStream("stream:project", entry, seq, false, value, null);
            if (epochChange) emitStreamGap(entry, gap, "epoch-change");
            else if (gap > 0) emitStreamGap(entry, gap, "gap");
        }
    }

    // stream:gap carries the count of frames missed (reason "gap" within an
    // epoch, "epoch-change" at a failover boundary). Reached only behind a
    // feed.hook null test, so an app with no panel pays nothing.
    function emitStreamGap(entry, count, reason) {
        emitStream("stream:gap", entry, count, false, null, reason);
    }

    // Watchdog liveness (OR-3 extended to streams). lastFrameAt is a plain
    // number stamped per frame (zero alloc, no per-value timer churn); ONE
    // periodic check-and-rearm timer per projected entry decides liveness --
    // lite-stream's own idleTimeout design note, implemented in the domain that
    // can see the failure. If no frame has arrived within streamIdleTimeout the
    // follower self-connects (openStream). A healthy leader's frames keep
    // lastFrameAt fresh, so the check just rearms. Never per-value
    // setTimeout/clearTimeout -- amortized O(1).
    function armWatchdog(entry) {
        if (entry.streamWatchdog !== null) return;           // already armed
        const period = opts.streamIdleTimeout;
        entry.streamWatchdog = opts.setTimeout(function watchdogCheck() {
            entry.streamWatchdog = null;
            // Only a still-following, observed entry needs liveness. A success
            // terminal is done; an owner drives its own connection; an
            // error terminal KEEPS the watchdog so it can recover (F7).
            if (!entry.streamShared || entry.streamOwner || entry.observerCount === 0) return;
            if (entry.status() === "success") return;
            if ((opts.now() - entry.lastFrameAt) >= period) {
                openStream(entry, "leader-hung");            // OR-3: self-connect on silence
            } else {
                armWatchdog(entry);                          // fresh: rearm
            }
        }, period);
        if (entry.streamWatchdog && typeof entry.streamWatchdog.unref === "function") {
            entry.streamWatchdog.unref();
        }
    }
    function disarmWatchdog(entry) {
        if (entry.streamWatchdog !== null) {
            opts.clearTimeout(entry.streamWatchdog);
            entry.streamWatchdog = null;
        }
    }

    // Promotion / self-connect: THIS tab opens a fresh iterator and claims
    // ownership. The announce is DEFERRED one queueMicrotask (V2, cold, once per
    // promotion) so a promotion triggered from inside onRemoteMessage leaves
    // after the processingRemote echo guard is down -- the guard itself is
    // untouched. streamPromoting guards a double-promote in the microtask gap.
    // startStream (the /stream-installed streamPromote thunk) claims a strictly
    // higher epoch, sets streamOwner, announces stream-open, and adopts the
    // projected window (V1) so no frame is lost beyond OR-4's bound.
    function openStream(entry, reason) {
        if (!entry.streamShared || entry.streamOwner || entry.streamPromoting) return;
        if (entry.observerCount === 0) return;
        disarmWatchdog(entry);
        entry.streamPromoting = true;
        queueMicrotask(() => {
            entry.streamPromoting = false;
            if (!entry.streamShared || entry.streamOwner || entry.observerCount === 0) return;
            if (typeof entry.streamPromote === "function") entry.streamPromote(reason);
        });
    }

    // Allocate the next ownership epoch for a tab that is opening a shared
    // stream: max(seen) + 1, carried on the entry. Globally monotone across the
    // tab set without a shared counter -- every tab tracks the max epoch it has
    // observed and claims strictly above it. NEVER derived from opts.now.
    function claimStreamEpoch(entry) {
        maxEpochSeen = maxEpochSeen + 1;
        entry.streamEpoch = maxEpochSeen;
        entry.streamSeq = 0;
        return maxEpochSeen;
    }

    // stream-open: an ownership claim. Track the epoch (so max(seen)+1 stays
    // globally monotone) and abdicate if we own this key at a lower rank.
    function onStreamOpen(m) {
        if (m.epochSeq > maxEpochSeen) maxEpochSeen = m.epochSeq;
        const e = entries.get(hashKey(m.key));
        if (!e || !e.streamShared) return;
        e.lastFrameAt = opts.now();                  // a claim is liveness evidence
        if (e.streamOwner &&
            rankBelow(e.streamEpoch, clientId, m.epochSeq, m.clientId)) {
            if (feed.hook !== null) emitStreamPromote(e, e.streamEpoch, m.epochSeq, false, "abdicate");
            closeStream(e);                          // F3/F5: loser reverts to projecting
            e.projEpoch = m.epochSeq;                // adopt the winner's (epoch, owner) as the cursor
            e.projClientId = m.clientId;
            e.projSeq = 0;                           // expect the winner's seq 1 next (no spurious gap)
            armWatchdog(e);                          // and stay live if the winner then hangs (F3)
        }
    }

    // stream-frame: the hot receive path. Track the epoch, stamp liveness, and
    // hand the frame to projectFrame's epoch/seq gate (C4/C5). If THIS tab is an
    // owner and the frame outranks it, a higher-ranked leader exists: abdicate
    // and project (the frame-driven half of the race convergence -- two leaders
    // that never traded a stream-open still converge the instant either yields a
    // frame; pure rank, never election state, OR-3).
    function onStreamFrame(m) {
        if (m.epochSeq > maxEpochSeen) maxEpochSeen = m.epochSeq;
        const e = entries.get(hashKey(m.key));
        if (!e || !e.streamShared) return;
        e.lastFrameAt = opts.now();
        if (e.streamOwner &&
            rankBelow(e.streamEpoch, clientId, m.epochSeq, m.clientId)) {
            if (feed.hook !== null) emitStreamPromote(e, e.streamEpoch, m.epochSeq, false, "abdicate");
            closeStream(e);                          // a higher-ranked owner won
            armWatchdog(e);                          // stay live if it then hangs (F3/F7)
        }
        // An OWNER never projects (QD-2): its own iterator holds the authoritative
        // data, and a straggler frame from a lower-ranked owner must not overwrite
        // it or drag projEpoch forward. Once the owner has abdicated above
        // (streamOwner false), subsequent frames project normally.
        if (!e.streamOwner) projectFrame(e, m.epochSeq, m.clientId, m.seq, m.value);
    }

    // stream-end: terminal for this key at <= epochSeq. A strictly older
    // epoch's late end is ignored; an end at our epoch-or-higher supersedes a
    // local owner. Success settles success; error surfaces the failure AND arms
    // the watchdog (F7) -- a follower is never wedged by a dead leader.
    function onStreamEnd(m) {
        if (m.epochSeq > maxEpochSeen) maxEpochSeen = m.epochSeq;
        const e = entries.get(hashKey(m.key));
        if (!e || !e.streamShared) return;
        if (m.epochSeq < e.projEpoch) return;
        // A graceful close (F1) is NOT a terminal outcome for the key: the
        // leader is leaving but the stream lives on. Followers run the promotion
        // race on receipt; the winner's higher epoch makes the losers abdicate.
        if (m.reason === "closing") {
            if (e.streamOwner &&
                !rankBelow(m.epochSeq, m.clientId, e.streamEpoch, clientId)) {
                closeStream(e);
            }
            openStream(e, "leader-closed");
            armWatchdog(e);                              // and stay live if the race stalls
            return;
        }
        if (e.streamOwner &&
            !rankBelow(m.epochSeq, m.clientId, e.streamEpoch, clientId)) {
            if (feed.hook !== null) emitStreamPromote(e, e.streamEpoch, m.epochSeq, false, "abdicate");
            closeStream(e);
        }
        e.projEpoch = m.epochSeq;
        if (m.ok) {
            setStatus(e, "success");
            e.lastCompletedAt = opts.now();
            disarmWatchdog(e);
        } else {
            e.error.set(m.error);
            setStatus(e, "error");
            armWatchdog(e);
        }
    }

    // stream-req: a follower asks for an owner. Mirrors fetch-req -- only a
    // leader that currently owns this key answers, and it must defer the
    // re-announce out of the handler (V2) or the echo guard swallows it.
    function onStreamReq(m) {
        if (!opts.isLeader()) return;
        const e = entries.get(hashKey(m.key));
        if (e && e.streamOwner) {
            broadcastControl({ type: "stream-open", key: e.key, epochSeq: e.streamEpoch, clientId });
        }
    }

    // -- entries --

    function createEntry(key, keyHash) {
        return {
            key,
            keyHash,
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
            // Shared-stream projection slots (Q8, V6). Uniform on every entry at
            // null/false/0 defaults so a plain query keeps ONE hidden class; only
            // a shared streamQuery entry populates them. A follower carries the
            // projection window here (no iterator); the leader carries the seq
            // counter it stamps outgoing frames with.
            streamShared: false,         // participates in cross-tab stream sharing
            streamMode: null,            // "latest" | "buffer" -- projection discipline
            streamMaxBuffer: 0,          // buffer window size (buffer mode only)
            streamOwner: false,          // this tab currently owns the iterator (leader)
            streamEpoch: 0,              // the epoch this tab stamps when it owns
            streamSeq: 0,                // per-frame seq counter on the owned iterator
            projEpoch: -1,               // highest epoch projected (dedup cursor, -1 = none)
            projClientId: null,          // owner clientId of the projected epoch (QD-1 triple cursor)
            projSeq: -1,                 // highest seq projected within (projEpoch, projClientId)
            lastFrameAt: 0,              // watchdog stamp: opts.now() of the last frame
            streamWatchdog: null,        // periodic check-and-rearm liveness timer
            streamPromote: null,         // (reason) => open THIS tab's iterator (adopt path, V1)
            streamPromoting: false,      // a promotion microtask is in flight (double-promote guard)
            projWindow: null,            // buffer-mode follower window (last snapshot array)
            projWriter: null,            // lite-stream 1.4.0 buffer writer (push/end/error/droppedCount), or null (LS4 seam, Q9)
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
        return ensureEntryByHash(key, hashKey(key));
    }

    // As ensureEntry, but with the key hash SUPPLIED. The hydrate seed path uses
    // this with the hash the validation pass already computed (record.keyHash),
    // so seeding performs ZERO reads of the caller's key array -- no second
    // hashKey walk (QD-5). The validated hash is threaded into createEntry as the
    // storage hash and the map key, so the dup-check, the entry, and the map all
    // agree; a getter nested in a key element can never diverge them.
    function ensureEntryByHash(key, keyHash) {
        let e = entries.get(keyHash);
        if (!e) {
            e = createEntry(key, keyHash);      // hash SUPPLIED -- no re-walk of key
            entries.set(keyHash, e);
            // Entry has no observers yet -- schedule GC immediately. attach()
            // will cancel this if an observer arrives before the timer fires.
            scheduleGc(e);
            if (feed.hook !== null) emitEntry("entry:create", e, 0, null);   // site 1
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
        // A shared-stream owner leaving gracefully tells the tab set BEFORE it
        // stops its iterator (F1): a stream-end with reason "closing" is not a
        // terminal outcome for the key -- followers run the promotion race on
        // receipt so the stream continues on another tab. Emitted from the
        // dispose path (never from a remote handler), so V2 does not bite. A
        // KILLED tab (F2) never reaches here, so no close is sent and the
        // watchdog is the only detector -- exactly as the matrix requires.
        if (sharedStreamActive && entry.streamOwner && channel) {
            broadcast({ type: "stream-end", key: entry.key, epochSeq: entry.streamEpoch, clientId, ok: false, error: undefined, reason: "closing" });
        }
        // Stop any live stream first (abort the iterator -> iterator.return(),
        // closing the underlying SSE/websocket) before releasing signal nodes.
        if (entry.streamStop) {
            try { entry.streamStop(); } catch {}
            entry.streamStop = null;
            entry.streamRestart = null;
        }
        releaseProjection(entry);           // V5: null the window / cursors / promote closure
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
                if (entry.abortController) {
                    const was = entry.abortController.signal.aborted;   // QD-2: emit at most once per controller
                    entry.abortController.abort(ABORT_REASON.REMOVED);
                    if (was === false && feed.hook !== null) emitFetch("fetch:abort", entry, entry.fetchGen, false, null, ABORT_REASON.REMOVED);   // site 10 (gc)
                }
                entries.delete(entry.keyHash);
                disposeEntry(entry);
                notifyWrite();                           // hook site 6: an expired entry leaves the snapshot
                if (feed.hook !== null) emitEntry("entry:gc", entry, 0, null);   // site 4
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

    // -- refetchInterval scanner: register / unregister (C2) --

    // Register a poll for `entry` at `interval` ms. Refcount-gated: if the entry
    // already has a record, bump its count (first interval wins) rather than
    // adding a second record, so N observers of one key share one scanner slot.
    // The first registration lazily creates pollList (null -> []) and arms the
    // scanner (C3). nextPollAt is stamped from opts.now() so the first tick is
    // due one full interval out, never early.
    function registerPoll(entry, interval) {
        if (pollList === null) pollList = [];
        for (let i = 0; i < pollList.length; i++) {
            if (pollList[i].entry === entry) { pollList[i].count++; return; }
        }
        pollList.push({ entry, interval, nextPollAt: opts.now() + interval, count: 1 });
        armPoll();
    }

    // Unregister a poll for `entry`. Refcount-gated: decrement, and only when the
    // count reaches zero swap-remove the record (O(1), order-agnostic) and, if the
    // list is now empty, disarm the scanner timer. A missing entry is a silent
    // no-op (idempotent teardown -- detach may fire after a disable already pulled
    // the record).
    function unregisterPoll(entry) {
        if (pollList === null) return;
        for (let i = 0; i < pollList.length; i++) {
            if (pollList[i].entry === entry) {
                if (--pollList[i].count <= 0) {
                    const last = pollList.length - 1;
                    pollList[i] = pollList[last];
                    pollList.pop();
                    if (pollList.length === 0 && pollTimerId !== null) {
                        opts.clearTimeout(pollTimerId);
                        pollTimerId = null;
                    }
                }
                return;
            }
        }
    }

    // -- refetchInterval scanner: the single check-and-rearm timer (C3) --

    // The scanner granularity: the SMALLEST registered interval. Firing at the
    // min means a coarser poll is served on-or-after its due time (a nextPollAt
    // gate below), never early -- the documented RISK. O(list) and cold.
    function pollPeriod() {
        let m = Infinity;
        for (let i = 0; i < pollList.length; i++) {
            if (pollList[i].interval < m) m = pollList[i].interval;
        }
        return m;
    }

    // Arm the ONE scanner timer if it is not already armed and the list is
    // non-empty. All timers go through opts.setTimeout (the injectable door) so
    // the mock clock drives them; unref'd so a polling app never pins the Node
    // event loop open.
    function armPoll() {
        if (pollTimerId !== null) return;
        if (pollList === null || pollList.length === 0) return;
        pollTimerId = opts.setTimeout(pollTick, pollPeriod());
        if (pollTimerId && typeof pollTimerId.unref === "function") pollTimerId.unref();
    }

    // One tick of the scanner: scan every record, dispatch the ones whose
    // nextPollAt has come due (>= now, never early), advance their nextPollAt by
    // one interval, then re-arm -- but ONLY if the list is still non-empty (a
    // dispatch that drained the last observer could unregister mid-scan). The
    // timer id is nulled first so armPoll() re-arms exactly one successor.
    function pollTick() {
        pollTimerId = null;
        if (pollList === null || pollList.length === 0) return;
        const now = opts.now();
        for (let i = 0; i < pollList.length; i++) {
            const rec = pollList[i];
            if (now >= rec.nextPollAt) {
                rec.nextPollAt = now + rec.interval;
                pollDispatch(rec.entry);
            }
        }
        armPoll();
    }

    // Dispatch one due poll through the NORMAL fetch path (the maybeFetch tail),
    // so dedup / retry / abort / sharedFetch and the leaderless self-fetch all
    // apply BY CONSTRUCTION. The "interval" reason rides fetch:dispatch (OR-10, no
    // new event type). Refcount-gated: a record whose observers have all left
    // (before its unregister runs) never fetches.
    function pollDispatch(entry) {
        if (entry.observerCount > 0) maybeFetch(entry, "interval");
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
        if (feed.hook !== null) emitEntry("entry:attach", entry, entry.observerCount, null);   // site 2
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
        if (feed.hook !== null) emitEntry("entry:detach", entry, entry.observerCount, null);   // site 3
        if (entry.observerCount === 0) {
            // Last observer gone -- abort in-flight if any. Resolution paths
            // gate on the generation guard, so a late resolution is harmless.
            if (entry.abortController) {
                const was = entry.abortController.signal.aborted;   // QD-2: emit at most once per controller
                entry.abortController.abort(ABORT_REASON.DETACH);
                if (was === false && feed.hook !== null) emitFetch("fetch:abort", entry, entry.fetchGen, false, null, ABORT_REASON.DETACH);   // site 10 (detach)
                entry.abortController = null;
                entry.promise = null;
                entry.fetching.set(false);
                if (entry.status() === "pending") setStatus(entry, "idle");
            }
            // Last observer gone on a stream -- close the connection. The entry
            // stays cached (scheduleGc); a re-attach before GC re-establishes a
            // fresh stream via the watcher. Cached data() survives until GC.
            if (entry.streamStop) {
                // A shared owner leaving gracefully hands the stream on (F1).
                if (sharedStreamActive && entry.streamOwner && channel) {
                    broadcast({ type: "stream-end", key: entry.key, epochSeq: entry.streamEpoch, clientId, ok: false, error: undefined, reason: "closing" });
                }
                try { entry.streamStop(); } catch {}
                entry.streamStop = null;
                entry.streamRestart = null;
                const s = entry.status();
                if (s === "pending" || s === "streaming") setStatus(entry, "idle");
            }
            // Shared-stream teardown (owner or follower): stop the watchdog and
            // release every projection slot so a re-attach before GC re-registers
            // cleanly and nothing (window, cursors, promote closure) is retained
            // past the last observer (V5).
            if (entry.streamShared) {
                const s = entry.status();
                if (s === "pending" || s === "streaming") setStatus(entry, "idle");
                releaseProjection(entry);
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

    function maybeFetch(entry, reason) {
        if (!entry.fetcher) return;
        if (entry.promise) return;                       // already fetching locally
        if (entry.sharedFallbackTimer !== null) return;  // already awaiting a shared fetch
        if (!shouldFetch(entry)) return;

        if (sharedFetchActive && !opts.isLeader()) {
            requestSharedFetch(entry);                   // follower: ask the leader
        } else {
            // reason undefined (every non-interval caller) -> runFetch(entry,
            // undefined), byte-identical to the 2.1.0 runFetch(entry) call. The
            // interval scanner passes "interval" so fetch:dispatch carries it
            // (OR-10: a reason VALUE, not a new event type).
            runFetch(entry, reason === undefined ? undefined : { reason }).catch(noop);
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
            setStatus(entry, "pending");
        }
        broadcast({ type: "fetch-req", key: entry.key });
        if (feed.hook !== null) emitEntry("shared:request", entry, 0, "follower");   // site 13
        clearSharedTimer(entry);
        entry.sharedFallbackTimer = opts.setTimeout(() => {
            entry.sharedFallbackTimer = null;
            // The timer firing means no leader fulfilled the request -- the
            // arrival path (setQueryData) would have cleared it otherwise.
            // Self-fetch so the UI never hangs.
            if (entry.observerCount > 0 && entry.promise === null) {
                if (feed.hook !== null) emitEntry("shared:fallback", entry, 0, "follower-timeout");   // site 14
                runFetch(entry).catch(noop);
            }
        }, opts.sharedFetchTimeout);
        if (entry.sharedFallbackTimer && typeof entry.sharedFallbackTimer.unref === "function") {
            entry.sharedFallbackTimer.unref();
        }
    }

    function runFetch(entry, { force = false, reason = null } = {}) {
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
            const was = entry.abortController.signal.aborted;   // QD-2: emit at most once per controller
            entry.abortController.abort(ABORT_REASON.REFETCH);
            if (was === false && feed.hook !== null) emitFetch("fetch:abort", entry, entry.fetchGen, false, null, ABORT_REASON.REFETCH);   // site 10 (supersede)
        }

        const gen = ++entry.fetchGen;
        // Snapshot the accumulation generation + cursor at fetch start (infinite
        // only). commitPage uses them: startCursor === null => page one (replace),
        // startGen mismatch => a page that a later invalidate superseded.
        const startPageGen = entry.pageGen;
        const startCursor = entry.isInfinite ? entry.nextCursor : undefined;
        const ac = new AbortController();
        entry.abortController = ac;
        if (feed.hook !== null) emitFetch("fetch:dispatch", entry, gen, false,   // site 8
            entry.isInfinite ? startCursor : null, reason ?? (force ? "force" : null));
        entry.fetching.set(true);
        if (entry.data() === undefined && entry.status() !== "error") {
            setStatus(entry, "pending");
        }

        // Per-query timeout. If specified and finite, set up a timer that
        // aborts the AbortController with ABORT_REASON.TIMEOUT. Cleared on
        // resolution (success or error path).
        let timeoutId = null;
        if (entry.timeout != null && isFinite(entry.timeout)) {
            timeoutId = opts.setTimeout(() => {
                const was = ac.signal.aborted;   // QD-2: emit at most once per controller
                ac.abort(ABORT_REASON.TIMEOUT);
                if (was === false && feed.hook !== null) emitFetch("fetch:abort", entry, gen, false, null, ABORT_REASON.TIMEOUT);   // site 10 (timeout)
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
                    setStatus(entry, "error");
                    entry.lastCompletedAt = opts.now();
                    entry.invalidatedSinceCompletion = false;
                    entry.fetching.set(false);
                    entry.promise = null;
                    entry.abortController = null;
                    notifyWrite();                       // hook site 3: commitPage-throw returns before site 2
                    if (feed.hook !== null) emitFetch("fetch:settle", entry, gen, false, commitErr, "commit-throw");   // site 9b
                    return Promise.reject(commitErr);
                }
            }

            if (outcome.ok) {
                if (!entry.isInfinite && !entry.equals(entry.data(), outcome.data)) {
                    entry.data.set(outcome.data);
                }
                entry.error.set(undefined);
                setStatus(entry, "success");
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
                setStatus(entry, "error");
            }

            entry.lastCompletedAt = opts.now();
            entry.invalidatedSinceCompletion = false;
            entry.fetching.set(false);
            entry.promise = null;
            entry.abortController = null;
            notifyWrite();                               // hook site 2: settle -- success AND error uniformly
            if (feed.hook !== null) emitFetch("fetch:settle", entry, gen, outcome.ok,   // site 9a
                outcome.ok ? outcome.data : outcome.err, null);

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
        setStatus(e, "success");
        e.lastCompletedAt = opts.now();
        // If a shared-fetch follower was awaiting the leader's result, this IS
        // that result -- stop the loading state and cancel the fallback timer.
        // The promise===null guard avoids clobbering a genuine in-flight fetch.
        if (e.promise === null) e.fetching.set(false);
        clearSharedTimer(e);
        notifyWrite();                                   // hook site 1: local writes AND every remote apply
        broadcast({ type: "setData", key, value: newVal });
    }

    function invalidate(key, invOpts = {}) {
        const exact = invOpts.exact ?? false;
        for (const e of entries.values()) {
            if (!keyMatches(e.key, key, exact)) continue;
            e.invalidatedSinceCompletion = true;
            if (feed.hook !== null) emitEntry("entry:stale", e, 0, "invalidate");   // site 7
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
        let removed = 0;
        for (const [h, e] of [...entries]) {
            if (!keyMatches(e.key, key, exact)) continue;
            if (e.abortController) {
                const was = e.abortController.signal.aborted;   // QD-2: emit at most once per controller
                e.abortController.abort(ABORT_REASON.REMOVED);
                if (was === false && feed.hook !== null) emitFetch("fetch:abort", e, e.fetchGen, false, null, ABORT_REASON.REMOVED);   // site 10 (removeQueries)
            }
            cancelGc(e);
            clearSharedTimer(e);
            entries.delete(h);
            disposeEntry(e);
            if (feed.hook !== null) emitEntry("entry:remove", e, 0, "remove");   // site 5a
            removed++;
        }
        if (removed > 0) notifyWrite();                  // hook site 4: only when >= 1 entry matched
        broadcast({ type: "remove", key, opts: rmOpts });
    }

    function clear() {
        const had = entries.size > 0;
        for (const e of entries.values()) {
            if (e.abortController) {
                const was = e.abortController.signal.aborted;   // QD-2: emit at most once per controller
                e.abortController.abort(ABORT_REASON.REMOVED);
                if (was === false && feed.hook !== null) emitFetch("fetch:abort", e, e.fetchGen, false, null, ABORT_REASON.REMOVED);   // site 10 (clear)
            }
            cancelGc(e);
            clearSharedTimer(e);
            disposeEntry(e);
            if (feed.hook !== null) emitEntry("entry:remove", e, 0, "clear");   // site 5b
        }
        entries.clear();
        if (had) notifyWrite();                          // hook site 5: map was non-empty (persists emptiness)
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

    // Validate a hydrate payload in ONE contained pass over the whole state
    // BEFORE any mutation (OR-3 all-or-nothing), MATERIALIZING a snapshot as it
    // goes: each field (key, data, dataUpdatedAt, infinite) is read EXACTLY ONCE
    // per record into flat internal storage, and seeding consumes ONLY that
    // snapshot -- so a TOCTOU getter that returns clean data to validation and
    // evil data to seeding cannot exist (QD-2, first-read wins). The traversal is
    // exception-contained: any throw during payload access (a throwing getter,
    // QD-4a) returns the malformed reason for that level, never escapes. Symbol
    // own-keys are malformed at both levels (QD-3: JSON carries no symbols, so
    // nothing legitimate is rejected). Returns { reason } on the first defect, or
    // { reason: null, records } (materialized) when the whole payload is clean.
    function validateHydrateState(state) {
        try {
            if (state === null || typeof state !== "object" || Array.isArray(state)) {
                return { reason: "malformed-state" };
            }
            if (Object.getOwnPropertySymbols(state).length !== 0) {
                return { reason: "malformed-state" };
            }
            const list = state.entries;                 // single read of the field
            if (!Array.isArray(list)) return { reason: "malformed-entries" };
            if (Object.keys(state).length !== 1) return { reason: "malformed-state" };
            const seen = new Set();
            const records = [];
            for (let i = 0; i < list.length; i++) {
                const r = validateRecord(list, i, seen);
                if (r.reason !== null) return { reason: r.reason };
                records.push(r.record);
            }
            return { reason: null, records };
        } catch {
            return { reason: "malformed-state" };
        }
    }

    // Validate + materialize ONE record, exception-contained (any throw returns a
    // record-level malformed reason). Reads the array slot and each of the four
    // fields exactly once; the returned snapshot is what seeds.
    function validateRecord(list, i, seen) {
        try {
            const rec = list[i];                        // single read of the slot
            if (rec === null || typeof rec !== "object" || Array.isArray(rec)) {
                return { reason: "malformed-entry" };
            }
            if (Object.getOwnPropertySymbols(rec).length !== 0) {
                return { reason: "malformed-entry" };
            }
            if (Object.keys(rec).length !== 4) return { reason: "malformed-entry" };
            const key = rec.key;                        // one read per field
            const data = rec.data;
            const dataUpdatedAt = rec.dataUpdatedAt;
            const infinite = rec.infinite;
            if (!Array.isArray(key)) return { reason: "malformed-key" };
            if (typeof infinite !== "boolean") return { reason: "malformed-entry" };
            if (!Number.isFinite(dataUpdatedAt)) return { reason: "malformed-timestamp" };
            if (infinite) {
                if (!Array.isArray(data)) return { reason: "malformed-pages" };
            } else if (data === undefined) {
                return { reason: "malformed-data" };
            }
            const keyHash = hashKey(key);
            if (seen.has(keyHash)) return { reason: "duplicate-key" };
            seen.add(keyHash);
            return { reason: null, record: { key, keyHash, data, dataUpdatedAt, infinite } };
        } catch {
            return { reason: "malformed-entry" };
        }
    }

    // Seed a plain entry from a validated record. Writes the signals directly
    // (never setQueryData -> never a broadcast, V5) so a boot seeding cannot
    // echo across tabs. lastCompletedAt is CLAMPED to our clock: a finite future
    // timestamp bounds freshness at one staleTime from boot, never invents data
    // (ON-3b). status is "success" -- the whole point of hydrate.
    function seedEntry(e, rec) {
        e.data.set(rec.data);
        e.error.set(undefined);
        setStatus(e, "success");
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
        setStatus(e, "success");
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
            // The ONLY throw hydrate makes -- the empty-cache precondition. Tagged
            // so the adapter can distinguish it from any other (contained) defect.
            const err = new Error("lite-query: hydrate requires an empty cache (" +
                entries.size + " entries present) -- hydrate at boot, before " +
                "any observer attaches");
            err.code = "LQ_HYDRATE_NOT_EMPTY";
            throw err;
        }
        const v = validateHydrateState(state);
        if (v.reason !== null) {
            if (feed.hook !== null) emitClient("persist:hydrate", 0, false, null, v.reason);   // site 22 (validation drop)
            return { ok: false, count: 0, reason: v.reason };
        }
        // Seed ONLY from the materialized snapshot -- never the caller's objects
        // again (QD-2/QD-5): the entry is keyed by the VALIDATED hash
        // (record.keyHash), so seeding reads no caller object and cannot re-walk
        // a key element. The loop is wrapped belt-and-braces (QD-5): after fix
        // above it reads no payload data, so this is unreachable from a payload
        // getter, but a future regression must degrade to a clean all-or-nothing
        // drop -- never a throw, never a partially seeded cache. On any throw,
        // roll back every entry THIS call created (all fresh + unobserved) with
        // the existing teardown discipline and return a malformed drop.
        const seeded = [];
        try {
            for (const record of v.records) {
                const e = ensureEntryByHash(record.key, record.keyHash);
                seeded.push(e);
                if (record.infinite) seedInfinite(e, record);
                else seedEntry(e, record);
            }
        } catch {
            for (const e of seeded) {
                cancelGc(e);
                entries.delete(e.keyHash);
                disposeEntry(e);
                if (feed.hook !== null) emitEntry("entry:remove", e, 0, "hydrate-rollback");   // site 5c
            }
            if (feed.hook !== null) emitClient("persist:hydrate", 0, false, null, "malformed-entry");   // site 22 (rollback drop)
            return { ok: false, count: 0, reason: "malformed-entry" };
        }
        if (feed.hook !== null) emitClient("persist:hydrate", v.records.length, true, null, null);   // site 22 (ok)
        return { ok: true, count: v.records.length, reason: null };
    }

    // Dispose the entire client. Releases the BroadcastChannel listener which
    // would otherwise keep the client + its entire cache map alive in
    // scenarios where clients are created and discarded -- testing,
    // micro-frontends, dev hot-reload. After dispose(), further mutations are
    // no-ops (cache is cleared, channel is closed).
    function dispose() {
        clear();
        // Definitive teardown of the interval scanner: disarm the single timer
        // and drop the record list so a disposed client leaves NO dangling poll
        // timer (G3) and pins nothing through the timer's closure (attempt G).
        if (pollTimerId !== null) {
            opts.clearTimeout(pollTimerId);
            pollTimerId = null;
        }
        pollList = null;
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
        // Install a devtools feed hook (Q7). One slot per client; observe-only,
        // never throw, never mutate. The hook MUST copy what it keeps -- events
        // are pooled per type and overwritten in place. Returns an idempotent
        // uninstall thunk.
        inspect,
        // Read-only count of durably-enqueued offline mutations (Q9). 0 when the
        // queue was never touched (queueStore === null); null is not zero.
        queueSize,
        // Replay the durable offline queue (caller-triggered, OR-4). Sequential
        // FIFO, single-flight, per-item results surfaced; at-least-once across a
        // crash. See llms.txt for the resolver contract and result shapes.
        replayQueue,
        // The poison-item exit (ON-4): evict one permanently-rejected record by
        // id. Returns true if found+removed, false if absent -- never throws.
        dropQueued,
        // Internal API consumed by query()/mutation() and the /stream subpath.
        // Not part of the public surface; documented as such in llms.txt. `feed`
        // is the live per-client cell (V2: a `let` cannot cross the subpath
        // boundary, so the subpath reads the same object through _internal).
        _internal: { entries, ensureEntry, attach, detach, maybeFetch, runFetch, requestSharedFetch, sharedFetchActive, sharedStreamActive, broadcast, clientId, claimStreamEpoch, armWatchdog, disarmWatchdog, closeStream, opts, installPersistHook, feed, setStatus, emitStream, emitStreamPromote, emitClient, enqueue, emitQueue, installQueueHook, dehydrateQueue, restoreQueue, registerPoll, unregisterPoll, get pollCount() { return pollList === null ? -1 : pollList.length; } },
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
    const { ensureEntry, attach, detach, maybeFetch, runFetch, requestSharedFetch, sharedFetchActive, opts, registerPoll, unregisterPoll } = qc._internal;

    // Fail closed at the door (ON-4): an invalid refetchInterval throws here, at
    // construction, so the scanner and every hot path downstream only ever see a
    // finite period > 0 or null (off). null is not zero.
    const refetchInterval = validateRefetchInterval(queryOpts.refetchInterval);

    // keepPreviousData hold (C6): a HANDLE-level presentation, never entry truth
    // (OR-5 -- the cache never lies). heldData carries the previous entry's data
    // across a reactive key swap; placeholderSig is the reactive isPlaceholder()
    // flag. Both are allocated ONLY when the option is on, so an OFF-path query
    // constructs exactly what 2.1.0 did (placeholderSig === null is the OFF path).
    // status()/loading() read entry truth and are untouched; only data() consults
    // the hold, and only after e.data() is undefined (dehydrate walks entries, not
    // handles, so persist/feed never see the placeholder).
    const keepPrev = validateKeepPreviousData(queryOpts.keepPreviousData);
    let heldData = undefined;
    const placeholderSig = keepPrev ? signal(false) : null;

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
                    // Unregister the poll BEFORE detach (ON-4: no polling while
                    // enabled is false). refetchInterval === null is the OFF path
                    // -- this guard is a single comparison on the cold watcher.
                    if (refetchInterval !== null) unregisterPoll(attachedEntry);
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
                if (attachedEntry) {
                    // Capture the outgoing entry's data as the placeholder BEFORE
                    // detaching it (the pre-detach seam). Only when it is defined:
                    // a swap away from a still-loading entry holds nothing.
                    if (placeholderSig !== null) {
                        const prev = untrack(() => attachedEntry.data());
                        if (prev !== undefined) { heldData = prev; placeholderSig.set(true); }
                    }
                    if (refetchInterval !== null) unregisterPoll(attachedEntry);
                    untrack(() => detach(attachedEntry));
                }
                untrack(() => attach(entry, queryOpts));
                attachedEntry = entry;
                currentEntry.set(entry);
                // Register the poll on THIS fresh attachment (ON-4: register on
                // attach when the option is set; observerCount is >= 1 here).
                if (refetchInterval !== null) registerPoll(entry, refetchInterval);
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
            // Unregister the poll on teardown (stopWatcher covers detach-to-zero
            // AND dispose). Idempotent under refcount if it already left.
            if (refetchInterval !== null) unregisterPoll(attachedEntry);
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
            const d = e ? e.data() : undefined;
            // OFF path (placeholderSig === null): a single comparison, then the
            // 2.1.0 return -- zero added allocation. ON path: hold heldData while
            // the new entry has no data yet, and clear on the first defined value.
            if (placeholderSig === null) return d;
            if (d !== undefined) {
                if (placeholderSig.peek()) { heldData = undefined; placeholderSig.set(false); }
                return d;
            }
            return placeholderSig.peek() ? heldData : d;
        },
        // isPlaceholder(): reactive true while the handle is showing held previous
        // data across a key swap, false once the new entry's real data arrives.
        // OFF path returns a constant false with no subscription.
        isPlaceholder() {
            if (placeholderSig === null) return false;
            trackObserver();
            return placeholderSig();
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
            // Release the placeholder hold (ON path only): drop the retained data
            // reference and return the flag's signal node to the pool.
            if (placeholderSig !== null) {
                heldData = undefined;
                try { disposeNode(placeholderSig); } catch {}
            }
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

    const { ensureEntry, attach, detach, maybeFetch, runFetch, requestSharedFetch, sharedFetchActive, opts, setStatus } = qc._internal;

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
                    setStatus(entry, "idle");
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

    // Devtools feed access, read fail-closed at construction (V8: mutation()
    // never otherwise touches qc, and a stub-client first arg must not throw --
    // C6.0 confirmed no test passes a non-client, but the guard costs nothing and
    // keeps a fake qc from wedging the mutation). null feed -> no emit, no throw.
    const _mi = qc && qc._internal;
    const feed = _mi ? _mi.feed : null;
    const emitClient = _mi ? _mi.emitClient : null;

    // Offline-queue opt-in (Q9, T1/ON-3). Validate the opt-in SHAPE at the door
    // -- the package's validate-at-construction norm -- so a malformed opt-in
    // fails closed with nothing constructed, never silently taking the normal
    // path (fail-open). `queue` present and not a boolean is a caller who thinks
    // they opted in; that must throw. queue:true then requires an offline oracle
    // (a function), a name (a string, what a reloaded tab resolves the handler
    // by), and a queueKey (an array, what a dropped entry is resolved against).
    if (mutOpts.queue !== undefined && typeof mutOpts.queue !== "boolean") {
        throw new TypeError("lite-query: mutation `queue` must be a boolean (queue: true to opt in)");
    }
    const queueEnabled = mutOpts.queue === true;
    if (queueEnabled) {
        if (typeof mutOpts.offline !== "function") {
            throw new TypeError("lite-query: mutation queue:true requires `offline` to be a function () => boolean");
        }
        if (typeof mutOpts.name !== "string") {
            throw new TypeError("lite-query: mutation queue:true requires `name` to be a string");
        }
        if (!Array.isArray(mutOpts.queueKey)) {
            throw new TypeError("lite-query: mutation queue:true requires `queueKey` to be a query-key array");
        }
        if (!_mi || typeof _mi.enqueue !== "function") {
            throw new TypeError("lite-query: mutation queue:true requires a real query client");
        }
    }
    // Bound once at construction (cold). null on the no-opt-in path, so mutate()'s
    // ladder is gated by a single boolean read and the 2.0.0 body is unchanged.
    const enqueue = queueEnabled ? _mi.enqueue : null;

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
        // Offline dispatch ladder (Q9, T1/OR-3) -- evaluated ONCE, before onMutate
        // and before fn, and ONLY under the per-mutation opt-in. The whole branch
        // sits behind a single boolean read, so a mutation that never opts in pays
        // one test and ZERO allocation: the 2.0.0 body below is byte-identical.
        // The oracle read + enqueue are synchronous (no await before the terminal
        // decision), so no second mutate() can interleave and steal `gen` here.
        if (queueEnabled) {
            // Call the caller's connectivity oracle inside a try. An unverified
            // connectivity state must not silently pick a branch (null is not
            // zero): a THROW or a NON-BOOLEAN return fails closed -- nothing
            // dispatched, nothing queued, status settles "error", the awaited
            // promise rejects with a tagged LQ_OFFLINE_ORACLE.
            let off;
            let oracleThrew = false;
            try { off = mutOpts.offline(); } catch { oracleThrew = true; }
            if (oracleThrew || (off !== true && off !== false)) {
                const err = new Error("lite-query: offline() must return a boolean and must not throw");
                err.code = "LQ_OFFLINE_ORACLE";
                if (gen === mutationGen) { error.set(err); status.set("error"); }
                throw err;
            }
            if (off === true) {
                // Transport down -> durable enqueue. onMutate/fn/onSuccess/onError
                // do NOT run; onSettled DOES (Phase-4 law). A full queue is a
                // surfaced rejection (LQ_QUEUE_FULL), never a silent drop (OR-3).
                const rec = enqueue(mutOpts.name, mutOpts.queueKey, hashKey(mutOpts.queueKey), vars);
                if (rec === null) {
                    const err = new Error("lite-query: mutation queue is full");
                    err.code = "LQ_QUEUE_FULL";
                    if (gen === mutationGen) { error.set(err); status.set("error"); }
                    throw err;
                }
                if (gen === mutationGen) { error.set(undefined); status.set("queued"); }
                if (mutOpts.onSettled) {
                    try { await mutOpts.onSettled(undefined, undefined, vars, undefined); }
                    catch { /* callback errors don't propagate or alter state */ }
                }
                return { queued: true, id: rec.id };   // the one receipt build site (ON-3)
            }
            // off === false -> the transport is up: fall through to the normal
            // 2.0.0 path with the gen already advanced above.
        }
        // Mark pending immediately on the new mutation. This is the LATEST
        // generation by definition, so no gen check needed here.
        status.set("pending");
        error.set(undefined);
        if (feed !== null && feed.hook !== null) emitClient("mutation:start", gen, false, vars, null);   // site 20

        let ctx;
        let resolvedData;
        let resolvedError;
        // Rejection is tracked by CONTROL FLOW, never by truthiness (ON-3). A
        // mutation that rejects with null / 0 / "" / undefined is STILL a
        // rejection -- "fail closed on every unverified state; null is not
        // zero". Branching on `if (resolvedError)` would settle those falsy
        // rejections as success (the retired 1.4.0 quirk). `rejected` is the
        // one source of truth for every downstream branch.
        let rejected = false;

        // Phase 1: run the mutation (onMutate + fn).
        // We collect outcome into local variables and DON'T let onSuccess /
        // onError be inside this try -- those are callbacks, and a throw in a
        // callback shouldn't flip mutation state from success to error or
        // vice-versa.
        try {
            if (mutOpts.onMutate) ctx = await mutOpts.onMutate(vars);
            resolvedData = await mutOpts.fn(vars);
        } catch (err) {
            rejected = true;
            resolvedError = err;
        }

        // Phase 2: update signals under the gen guard. Only the latest
        // mutation gets to set state. Any rejection settles "error" and the
        // error signal holds the rejection value VERBATIM (falsy included).
        if (gen === mutationGen) {
            if (rejected) {
                error.set(resolvedError);
                status.set("error");
            } else {
                data.set(resolvedData);
                status.set("success");
            }
        }
        // Site 21: emitted UNCONDITIONALLY so the state machine stays total;
        // ok = not-rejected (never a truthiness read); reason "superseded" when
        // a later mutate() has already advanced the gen.
        if (feed !== null && feed.hook !== null) {
            emitClient("mutation:settle", gen, !rejected,
                rejected ? resolvedError : resolvedData,
                gen !== mutationGen ? "superseded" : null);
        }

        // Phase 3: side-effect callbacks. Errors are contained -- a buggy
        // onSuccess should not abort the rest of the chain or flip state.
        // (This is the deliberate deviation from the reviewer's pattern,
        // which has onSuccess throws cascading to the catch block and firing
        // onError with the callback's error. That's worse than the original.)
        try {
            if (rejected) {
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
        // even if a later mutate(varsB) is running concurrently. A rejection
        // re-throws its value verbatim (falsy included) -- control flow, not
        // truthiness.
        if (rejected) throw resolvedError;
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

// -----------------------------------------------------------------------------
// Offline-queue envelope validation (Q9, OR-5) -- module-level, cold
// -----------------------------------------------------------------------------

// Validate + materialize ONE queue record, exception-contained. All 7 keys
// present, correct primitive types, key an array, no own symbols -- a single
// defect drops the WHOLE queue (fail closed), so this returns the first reason
// found. vars is unconstrained (any JSON value the caller stored).
function validateQueueRecord(list, i) {
    try {
        const rec = list[i];
        if (rec === null || typeof rec !== "object" || Array.isArray(rec)) {
            return { reason: "malformed-record" };
        }
        if (Object.getOwnPropertySymbols(rec).length !== 0) {
            return { reason: "malformed-record" };
        }
        if (Object.keys(rec).length !== 7) return { reason: "malformed-record" };
        const id = rec.id, name = rec.name, key = rec.key, keyHash = rec.keyHash;
        const vars = rec.vars, at = rec.at, tries = rec.tries;
        if (typeof id !== "string") return { reason: "malformed-record" };
        if (typeof name !== "string") return { reason: "malformed-record" };
        if (!Array.isArray(key)) return { reason: "malformed-record" };
        if (typeof keyHash !== "string") return { reason: "malformed-record" };
        if (typeof at !== "number" || !Number.isFinite(at)) return { reason: "malformed-record" };
        if (typeof tries !== "number" || !Number.isInteger(tries) || tries < 0) {
            return { reason: "malformed-record" };
        }
        return { reason: null, record: { id, name, key, keyHash, vars, at, tries } };
    } catch {
        return { reason: "malformed-record" };
    }
}

// Validate a restored queue envelope in ONE contained pass BEFORE any mutation
// (OR-3 all-or-nothing), mirroring the frozen cache-envelope ladder verbatim in
// discipline: exactly 2 keys { version, queue }, strict === version match (no
// coercion), queue an array, every record clean. Returns { reason } on the first
// defect (the WHOLE queue drops -- never a partial restore) or { reason: null,
// records } (materialized) when clean.
function validateQueueEnvelope(envelope, version) {
    try {
        if (envelope === null || typeof envelope !== "object" || Array.isArray(envelope)) {
            return { reason: "malformed-envelope" };
        }
        if (Object.getOwnPropertySymbols(envelope).length !== 0) {
            return { reason: "malformed-envelope" };
        }
        if (Object.keys(envelope).length !== 2 ||
            !("version" in envelope) || !("queue" in envelope)) {
            return { reason: "malformed-envelope" };
        }
        if (envelope.version !== version) {               // strict ===, no coercion
            return { reason: "version-mismatch" };
        }
        const list = envelope.queue;
        if (!Array.isArray(list)) return { reason: "malformed-queue" };
        const records = [];
        for (let i = 0; i < list.length; i++) {
            const r = validateQueueRecord(list, i);
            if (r.reason !== null) return { reason: r.reason };
            records.push(r.record);
        }
        return { reason: null, records };
    } catch {
        return { reason: "malformed-envelope" };
    }
}

// -----------------------------------------------------------------------------
// persistQueryClient() -- the persistence adapter
// -----------------------------------------------------------------------------

/**
 * Wire a query client to a storage backend (OR-5). Storage-agnostic: `save` and
 * `load` are thunks the caller supplies (localStorage, IndexedDB, a bake-stream
 * cache -- see Cookbook); the adapter never touches a specific backend and adds
 * zero deps.
 *
 * On install it restores once from `load()` (before any observer attaches) and
 * then subscribes to the client's PRIVATE cache-write hook, throttling the
 * dehydrated snapshot to `save()`. The write hook is armed only AFTER the
 * restore outcome settles, in all three branches, so a boot-window write can
 * never persist a half-restored cache.
 *
 * Fail-closed everywhere: a schema `version` is REQUIRED (a bump drops the old
 * cache); a malformed / mismatched / unreadable payload restores NOTHING and the
 * outcome is observable via `handle.restored` (a promise that always RESOLVES,
 * never rejects). save() rejections are contained.
 *
 * @param {import("./Query.js").QueryClient} qc
 * @param {import("./Query.js").PersistOptions} persistOpts
 * @returns {import("./Query.js").PersistHandle}
 */
export function persistQueryClient(qc, persistOpts) {
    if (persistOpts === null || typeof persistOpts !== "object") {
        throw new TypeError("persistQueryClient: an options object is required");
    }
    const { save, load, version, throttle, queueSave, queueLoad } = persistOpts;
    if (typeof save !== "function") {
        throw new TypeError("persistQueryClient: `save` must be a function (envelope) => void | Promise");
    }
    if (typeof load !== "function") {
        throw new TypeError("persistQueryClient: `load` must be a function () => envelope | Promise");
    }
    if (version === undefined) {
        throw new TypeError("persistQueryClient: `version` is required (no default) -- a schema bump must drop the old cache");
    }
    if (typeof version !== "string" && typeof version !== "number") {
        throw new TypeError("persistQueryClient: `version` must be a string or number");
    }
    const throttleMs = throttle ?? 1000;
    if (typeof throttleMs !== "number" || !Number.isFinite(throttleMs) || throttleMs < 0) {
        throw new TypeError("persistQueryClient: `throttle` must be a finite number >= 0");
    }
    // Offline-queue seam (Q9, OR-5). queueSave/queueLoad are optional SIBLING
    // thunks on the same adapter (the frozen envelope/state length gates forbid
    // riding the queue inside them). Supplying one without the other is a
    // half-durable queue -- fail closed at install.
    const hasQueue = queueSave !== undefined || queueLoad !== undefined;
    if (hasQueue) {
        if (typeof queueSave !== "function") {
            throw new TypeError("persistQueryClient: `queueSave` must be a function (envelope) => void | Promise when `queueLoad` is supplied");
        }
        if (typeof queueLoad !== "function") {
            throw new TypeError("persistQueryClient: `queueLoad` must be a function () => envelope | Promise when `queueSave` is supplied");
        }
    }
    if (persistInstalled.has(qc)) {
        throw new Error("persistQueryClient: a persister is already installed on this client (single-slot seam)");
    }
    persistInstalled.add(qc);

    const opts = qc.options;      // resolved, public -- carries setTimeout/clearTimeout
    // Devtools feed access (read fail-closed). The adapter REPORTS its saves as
    // persist:save events; it never CONSUMES the public feed (OR-4).
    const _pi = qc._internal;
    const feed = _pi ? _pi.feed : null;
    const emitClient = _pi ? _pi.emitClient : null;
    const emitQueue = _pi ? _pi.emitQueue : null;
    let timer = null;
    let uninstall = null;
    let queueUninstall = null;
    let stopped = false;
    // Set by a queue WRITE (enqueue/drop) so the shared doSave persists the queue
    // ONLY when it actually changed -- a pure cache write must not re-save (and
    // re-throw for) an unchanged queue. The synchronous at-least-once removal
    // write does NOT use this path (it is saveQueueRemoval, direct + ordered).
    let queueDirty = false;

    // Arm the shared throttle for a queue change (enqueue/drop). Marks the queue
    // dirty and rides the same trailing-edge timer as the cache doSave.
    function armQueue() {
        queueDirty = true;
        onWrite();
    }

    // Persist the full current queue (cold). Called by doSave only when the queue
    // is dirty. queueSave throws are contained here (the trailing-edge coalesce is
    // not the at-least-once removal write -- that path is saveQueueRemoval).
    function saveQueue() {
        try {
            const r = queueSave({ version, queue: _pi.dehydrateQueue(undefined) });
            if (r && typeof r.then === "function") r.then(noop, noop);
        } catch { /* coalesced queue save throw contained */ }
    }

    // The at-least-once removal write (OR-6). Builds the envelope EXCLUDING the
    // resolved record while it is still in the live store, so the persisted queue
    // reflects the removal BEFORE the caller (doReplayItem) splices in memory.
    // Throws are propagated to doReplayItem, which then leaves the record durable.
    function saveQueueRemoval(id) {
        const r = queueSave({ version, queue: _pi.dehydrateQueue(id) });
        if (r && typeof r.then === "function") r.then(noop, noop);
    }

    function clearTimer() {
        if (timer !== null) {
            opts.clearTimeout(timer);
            timer = null;
        }
    }

    // Build the snapshot and hand it to save(). The timer is nulled BEFORE the
    // state is built (via clearTimer) so a cache write DURING save() opens a
    // fresh window rather than being swallowed. save() rejections are contained
    // -- never unhandled; the persister keeps running.
    function doSave() {
        clearTimer();
        let envelope;
        try {
            envelope = { version, state: qc.dehydrate() };
        } catch {
            if (feed !== null && feed.hook !== null) emitClient("persist:save", 0, false, null, "dehydrate-threw");   // site 23 (dehydrate threw)
            return;                // a snapshot build must never wedge the persister
        }
        try {
            const r = save(envelope);
            if (r && typeof r.then === "function") r.then(noop, noop);
        } catch { /* synchronous save throw contained */ }
        if (feed !== null && feed.hook !== null) emitClient("persist:save", envelope.state.entries.length, true, null, null);   // site 23 (saved)
        if (hasQueue && queueDirty) { queueDirty = false; saveQueue(); }   // OR-5: only when the queue actually changed
    }

    // Trailing-edge coalescing (OR-7): the first write in a window arms the
    // timer; subsequent writes ride the same window. throttle 0 saves eagerly.
    function onWrite() {
        if (stopped) return;
        if (throttleMs === 0) { doSave(); return; }
        if (timer !== null) return;
        timer = opts.setTimeout(doSave, throttleMs);
        if (timer && typeof timer.unref === "function") timer.unref();
    }

    // Restore ladder (OR-6). Always resolves; the outcome is observable before
    // any observer attaches. Reasons: null | load-threw | malformed-envelope |
    // version-mismatch | cache-not-empty | a hydrate reason code.
    const restored = (async () => {
        let envelope;
        try {
            envelope = await load();
        } catch {
            return { status: "dropped", count: 0, reason: "load-threw" };
        }
        if (envelope === null || envelope === undefined) {
            return { status: "empty", count: 0, reason: null };   // normal boot (OR-3)
        }
        if (typeof envelope !== "object" || Array.isArray(envelope) ||
            Object.keys(envelope).length !== 2 ||
            !("version" in envelope) || !("state" in envelope)) {
            return { status: "dropped", count: 0, reason: "malformed-envelope" };
        }
        if (envelope.version !== version) {               // strict ===, no coercion
            return { status: "dropped", count: 0, reason: "version-mismatch" };
        }
        let result;
        try {
            result = qc.hydrate(envelope.state);
        } catch (err) {
            // The empty-cache precondition (a late load() whose entries already
            // exist) resolves cache-not-empty. Any OTHER throw out of hydrate
            // resolves the sanctioned "hydrate-threw" (belt-and-braces so
            // restored ALWAYS resolves even if a future defect reintroduces a
            // throw). QD-4b: honest labeling, never mislabel as cache-not-empty.
            if (err && err.code === "LQ_HYDRATE_NOT_EMPTY") {
                return { status: "dropped", count: 0, reason: "cache-not-empty" };
            }
            return { status: "dropped", count: 0, reason: "hydrate-threw" };
        }
        if (!result.ok) {
            return { status: "dropped", count: 0, reason: result.reason };
        }
        return { status: "restored", count: result.count, reason: null };
    })().then((outcome) => {
        // Arm the write hook AFTER the restore settles -- in ALL three branches.
        if (!stopped) {
            try { uninstall = qc._internal.installPersistHook(onWrite); }
            catch { /* client disposed, or a manual hook already holds the slot */ }
        }
        return outcome;
    });

    // Queue restore ladder (Q9, OR-3/OR-5). ALWAYS resolves, never rejects. An
    // absent queue section is "no queue" (empty), never an error; any defect --
    // load throw, malformed envelope, version mismatch, malformed queue/record --
    // drops the WHOLE queue (never a partial restore) and surfaces the drop TWICE:
    // on this promise AND as a queue:drop feed event carrying the reason. Only
    // installed when the queueSave/queueLoad seam is wired.
    const queueRestored = !hasQueue ? undefined : (async () => {
        let envelope;
        try {
            envelope = await queueLoad();
        } catch {
            return dropRestoredQueue("load-threw");
        }
        if (envelope === null || envelope === undefined) {
            return { status: "empty", count: 0, reason: null };   // no queue (OR-3)
        }
        const v = validateQueueEnvelope(envelope, version);
        if (v.reason !== null) {
            return dropRestoredQueue(v.reason);
        }
        qc._internal.restoreQueue(v.records);
        if (feed !== null && feed.hook !== null && emitQueue !== null) {
            emitQueue("queue:restore", null, null, v.records.length, true, null, null);
        }
        return { status: "restored", count: v.records.length, reason: null };
    })().then((outcome) => {
        // Arm the queue seam AFTER the restore settles, in every branch (mirrors
        // the cache hook arming). arm rides the cache throttle; saveRemoval is the
        // synchronous at-least-once removal write.
        if (!stopped) {
            try { queueUninstall = qc._internal.installQueueHook({ arm: armQueue, saveRemoval: saveQueueRemoval }); }
            catch { /* client disposed, or a hook already holds the slot */ }
        }
        return outcome;
    });

    // Surface a whole-queue drop twice (OR-5): the queueRestored outcome AND a
    // queue:drop feed event with the reason. The dropped queue never seeds.
    function dropRestoredQueue(reason) {
        if (feed !== null && feed.hook !== null && emitQueue !== null) {
            emitQueue("queue:drop", null, null, 0, false, null, reason);
        }
        return { status: "dropped", count: 0, reason };
    }

    // Force the pending save now. QD-1: a strict no-op once stopped (stop()
    // already flushed and uninstalled) -- flush-after-stop must not re-save.
    // While running, an explicit flush()/flush() double-saves by design ("force
    // now" semantics).
    function flush() {
        if (stopped) return;
        doSave();
    }

    // Idempotent. FLUSH-ON-STOP (OR-7/ON-3c): a pending timer means a committed
    // cache write is not yet on disk; stop() fires at teardown / logout / client
    // dispose -- exactly when the last write matters most. Uninstalls the hook
    // and clears the timer; clears the single-slot reservation.
    function stop() {
        if (stopped) return;
        stopped = true;
        if (timer !== null) doSave();     // flush the pending write to disk (cache + queue)
        clearTimer();
        if (uninstall) { uninstall(); uninstall = null; }
        if (queueUninstall) { queueUninstall(); queueUninstall = null; }
        persistInstalled.delete(qc);
    }

    // queueRestored is present only when the queueSave/queueLoad seam is wired
    // (an undefined property when the adapter carries no queue -- OR-5).
    const handle = { restored, flush, stop };
    if (hasQueue) handle.queueRestored = queueRestored;
    return handle;
}
