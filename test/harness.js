/**
 * Test harness for lite-query.
 *
 * Three primitives every async-cache test suite needs to be deterministic:
 *
 *   1. createControlledFetcher / createQueuedFetcher
 *      - The fetcher is a Promise we resolve/reject from the test, not from
 *        a real network call. Tests assert that *N* fetches happened, that
 *        fetch N+1 didn't happen, etc.
 *
 *   2. createMockClock
 *      - now(), setTimeout(), clearTimeout() that advance only when the test
 *        calls advance(). Stale-time / cache-time / retry-delay logic becomes
 *        trivially testable.
 *
 *   3. createMockBroadcastChannel
 *      - Cross-tab messaging without spawning a worker. Multiple "tabs" each
 *        instantiate the channel; postMessage propagates to others via
 *        queueMicrotask (matching the real spec's async delivery).
 *
 * Used together: a test can spin up two queryClients sharing one mock channel,
 * advance time to fire a stale-time refetch, resolve the fetcher with a value,
 * and assert that the other "tab" got the invalidation broadcast.
 */

// ── Controlled fetcher ──────────────────────────────────────────────────────

/**
 * One-shot fetcher: every call returns a fresh pending promise that the test
 * resolves manually. Useful when a test only triggers one fetch.
 */
export function createControlledFetcher() {
    const calls = [];
    let resolver = null;
    let rejecter = null;

    const fetcher = (ctx) => {
        calls.push(ctx);
        return new Promise((res, rej) => {
            resolver = res;
            rejecter = rej;
        });
    };

    return {
        fetcher,
        calls,
        resolve(value) { if (!resolver) throw new Error("no pending fetch"); resolver(value); resolver = rejecter = null; },
        reject(err)    { if (!rejecter) throw new Error("no pending fetch"); rejecter(err);   resolver = rejecter = null; },
        get callCount() { return calls.length; },
        get lastCall()  { return calls[calls.length - 1]; },
    };
}

/**
 * Queue-based fetcher: each call enqueues a pending resolver. Tests resolve
 * the Nth call independently. Useful for testing dedup, race conditions,
 * back-to-back refetches.
 */
export function createQueuedFetcher() {
    const calls = [];
    const queue = [];

    const fetcher = (ctx) => {
        calls.push(ctx);
        return new Promise((res, rej) => { queue.push({ res, rej }); });
    };

    return {
        fetcher,
        calls,
        resolveNth(n, value) {
            if (!queue[n]) throw new Error(`no pending fetch at index ${n}`);
            queue[n].res(value);
            queue[n] = null;
        },
        rejectNth(n, err) {
            if (!queue[n]) throw new Error(`no pending fetch at index ${n}`);
            queue[n].rej(err);
            queue[n] = null;
        },
        get callCount()    { return calls.length; },
        get pendingCount() { return queue.filter(Boolean).length; },
    };
}

// ── Mock clock ──────────────────────────────────────────────────────────────

/**
 * A deterministic clock. `advance(ms)` fires every scheduled callback whose
 * scheduled time falls within the window, in chronological order. Used to
 * drive stale-time, cache-time, retry-delay logic in tests without `await`s
 * on real timers.
 *
 * @param {number} [initial=0]
 */
export function createMockClock(initial = 0) {
    let current = initial;
    let nextId = 1;
    const timers = new Map();  // id → { at, fn }

    return {
        now: () => current,

        setTimeout(fn, ms) {
            const id = nextId++;
            timers.set(id, { at: current + ms, fn });
            return id;
        },

        clearTimeout(id) {
            timers.delete(id);
        },

        /** Advance the clock by `ms`, firing any timers that come due. */
        advance(ms) {
            const target = current + ms;
            while (true) {
                let nextId = null;
                let nextAt = Infinity;
                for (const [id, t] of timers) {
                    if (t.at <= target && t.at < nextAt) { nextId = id; nextAt = t.at; }
                }
                if (nextId === null) break;
                const t = timers.get(nextId);
                timers.delete(nextId);
                current = t.at;
                t.fn();
            }
            current = target;
        },

        /** Flush microtask queue so resolved promises chain out. */
        async flush() {
            // Drain microtasks; each `await` yields a microtask round-trip.
            for (let i = 0; i < 8; i++) await Promise.resolve();
        },

        get pendingCount() { return timers.size; },
    };
}

// ── Mock BroadcastChannel ───────────────────────────────────────────────────

/**
 * Returns a fresh BroadcastChannel constructor + a `reset()` that wipes all
 * channel state between tests. Channels with the same name share messages.
 */
export function createMockBroadcastChannel() {
    const channelsByName = new Map();    // name → Set<MockBC>

    class MockBC {
        constructor(name) {
            this.name = name;
            this._listeners = new Set();
            this._onmessage = null;
            this._closed = false;
            if (!channelsByName.has(name)) channelsByName.set(name, new Set());
            channelsByName.get(name).add(this);
        }
        postMessage(data) {
            if (this._closed) return;
            const peers = channelsByName.get(this.name);
            for (const peer of peers) {
                if (peer === this || peer._closed) continue;
                queueMicrotask(() => {                       // spec: async delivery
                    const evt = { data };
                    for (const listener of peer._listeners) listener(evt);
                    if (peer._onmessage) peer._onmessage(evt);
                });
            }
        }
        addEventListener(type, fn) {
            if (type === "message") this._listeners.add(fn);
        }
        removeEventListener(type, fn) {
            if (type === "message") this._listeners.delete(fn);
        }
        set onmessage(fn) { this._onmessage = fn; }
        get onmessage()   { return this._onmessage; }
        close() {
            this._closed = true;
            this._listeners.clear();
            this._onmessage = null;
            channelsByName.get(this.name)?.delete(this);
        }
    }

    return {
        BroadcastChannel: MockBC,
        reset() { channelsByName.clear(); },
        peerCount(name) { return (channelsByName.get(name) || new Set()).size; },
    };
}

// ── Convenience: a queryClient configured against a mock environment ───────

/**
 * Build a queryClient pre-wired to a mock clock and (optionally) a mock
 * BroadcastChannel. Returns `{ qc, clock, mockBC }` so individual tests can
 * advance the clock and inspect cross-tab state.
 */
export function setupMockEnv(queryClient, opts = {}) {
    const clock = createMockClock();
    const mockBC = opts.crossTab ? createMockBroadcastChannel() : null;
    const qc = queryClient({
        now: clock.now,
        setTimeout: clock.setTimeout,
        clearTimeout: clock.clearTimeout,
        broadcastChannel: mockBC ? mockBC.BroadcastChannel : undefined,
        crossTab: !!opts.crossTab,
        crossTabChannel: opts.crossTabChannel ?? "test",
        defaultStaleTime: opts.defaultStaleTime ?? 0,
        defaultCacheTime: opts.defaultCacheTime ?? 5 * 60_000,
        defaultTimeout: opts.defaultTimeout ?? Infinity,
        retry: opts.retry ?? 0,                                // tests opt in to retry
        retryDelay: opts.retryDelay ?? (() => 1000),
    });
    return { qc, clock, mockBC };
}
