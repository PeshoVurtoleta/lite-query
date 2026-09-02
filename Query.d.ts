/**
 * @zakkster/lite-query -- type declarations
 *
 * Reactive async cache with cross-tab coherence, built on @zakkster/lite-signal.
 * Three exports: `queryClient` (cache + lifecycle owner), `query` (reactive
 * read-side primitive), `mutation` (write-side primitive with optimistic
 * update + rollback flow).
 *
 * The unique differentiator vs TanStack Query / SWR: cross-tab cache coherence
 * (via BroadcastChannel) and cross-tab fetch deduplication (leader-elected,
 * follower fallback). See `QueryClientOptions.crossTab` and `sharedFetch`.
 */

// --- Abort reasons (string literals) -----------------------------------------

/**
 * Reasons attached to `AbortSignal.reason` when lite-query aborts a fetch.
 * Surface these to the user's fetcher for logging / alternative recovery.
 */
export type AbortReason =
    | "lite-query:detach"   // last observer left -- fetch aborted as wasted work
    | "lite-query:refetch"  // a newer fetch is starting on the same entry
    | "lite-query:removed"  // entry removed via removeQueries / clear
    | "lite-query:timeout"; // per-query or default `timeout` elapsed

// --- Status + lifecycle ------------------------------------------------------

export type QueryStatus = "idle" | "pending" | "success" | "error";
export type MutationStatus = "idle" | "pending" | "success" | "error";

// --- Reactive accessor (lite-signal shape -- function call, no `.value`) ------

/** A read-only reactive accessor -- call as `fn()` to read; tracks in effects. */
export type ReadAccessor<T> = () => T;

// --- Fetcher contract --------------------------------------------------------

/** Argument passed to every `query` fetcher. */
export interface FetcherContext<K extends readonly unknown[] = readonly unknown[]> {
    /** The cache key for this fetch. */
    key: K;
    /**
     * Abort signal. Fires on detach, refetch supersede, query removal, or
     * timeout. `signal.reason` is one of `AbortReason`. Pass to `fetch()`.
     */
    signal: AbortSignal;
}

export type Fetcher<T, K extends readonly unknown[] = readonly unknown[]>
    = (ctx: FetcherContext<K>) => Promise<T>;

// --- Retry policy ------------------------------------------------------------

/** Constant retry count, or a per-attempt decision function. */
export type RetryPolicy = number | ((attempt: number, error: unknown) => boolean);

/** Delay between retries -- receives the attempt number (1-indexed). */
export type RetryDelay = (attempt: number) => number;

// --- QueryClient -------------------------------------------------------------

/** Options accepted by `queryClient(...)`. */
export interface QueryClientOptions {
    /** Default ms before cache entries are considered stale on (re-)attach. Default 0. */
    defaultStaleTime?: number;
    /** Default ms a cache entry survives after its last observer leaves. Default 5 * 60_000. */
    defaultCacheTime?: number;
    /** Default ms before a fetch aborts with `lite-query:timeout`. Default `Infinity`. */
    defaultTimeout?: number;
    /** Default retry policy. Default 3. */
    retry?: RetryPolicy;
    /** Default retry delay function (1-indexed attempt). Default exponential, capped at 30s. */
    retryDelay?: RetryDelay;

    /**
     * Enable cross-tab cache coherence via BroadcastChannel. When true,
     * `setQueryData` / `invalidate` / `removeQueries` / `clear` propagate to
     * peer tabs of the same origin. Default false.
     */
    crossTab?: boolean;
    /** BroadcastChannel name. Default `"lite-query"`. */
    crossTabChannel?: string;

    /**
     * Enable cross-tab fetch deduplication. Requires `crossTab: true` and
     * `isLeader` to be useful (otherwise inert -- each tab fetches itself,
     * which is the safe default). When active, follower tabs broadcast
     * `{ type: "fetch-req", key }` instead of hitting the network; the leader
     * answers from its own observed/cached entry and broadcasts the result.
     */
    sharedFetch?: boolean;
    /** Leader-election oracle. Wire from `@zakkster/lite-channel`'s `sync.isLeader`. */
    isLeader?: () => boolean;
    /** Ms a follower waits before falling back to self-fetch. Default 3000. */
    sharedFetchTimeout?: number;

    // -- Injectables for tests --
    /** Inject a deterministic clock. Default `Date.now`. */
    now?: () => number;
    /** Inject a setTimeout (e.g., a mock clock's). */
    setTimeout?: (fn: () => void, ms: number) => unknown;
    /** Inject a clearTimeout matching the injected setTimeout. */
    clearTimeout?: (handle: unknown) => void;
    /** Inject a BroadcastChannel constructor (e.g., a mock). */
    broadcastChannel?: new (name: string) => BroadcastChannel;
}

/** Returned by `queryClient(...)`. The cache + lifecycle owner. */
// --- Devtools feed (qc.inspect) ----------------------------------------------

/** The 23 frozen feed event types -- `domain:verb`, lower-case. */
export type FeedEventType =
    | "entry:create" | "entry:attach" | "entry:detach" | "entry:gc"
    | "entry:remove" | "entry:status" | "entry:stale"
    | "fetch:dispatch" | "fetch:settle" | "fetch:abort"
    | "tab:send" | "tab:receive"
    | "shared:request" | "shared:fallback" | "shared:serve"
    | "stream:start" | "stream:value" | "stream:done" | "stream:error"
    | "mutation:start" | "mutation:settle"
    | "persist:hydrate" | "persist:save";

/**
 * One monomorphic feed record: exactly 10 own keys, always present, in this
 * order (one hidden class per type -- a panel's property reads stay monomorphic).
 * A field that does not apply is null (`count` is 0, `ok` is false).
 *
 * POOLED per type and OVERWRITTEN in place: the SAME object is handed to the hook
 * for every event of a given type. The hook MUST COPY what it keeps -- reading a
 * retained reference after the next event of that type reads stale fields. This
 * is the zero-GC feed contract (an installed hook allocates nothing per event).
 *
 * `ts` is performance.now() (else Date.now()), resolved once at module load --
 * NOT opts.now (a mock staleness clock never stamps the feed); monotonic
 * non-decreasing. `key` / `value` are BY REFERENCE, never copied, never
 * serialized (that is the panel's job).
 */
export interface QueryFeedEvent {
    /** The discriminant. */
    type: FeedEventType;
    /** performance.now()/Date.now() timestamp; monotonic non-decreasing. */
    ts: number;
    /** The entry's key array by reference; null for client-scope events. */
    key: readonly unknown[] | null;
    /** entry.keyHash -- the panel's identity. null where key is null. */
    keyHash: string | null;
    /** Prior status (entry:status only), else null. */
    from: string | null;
    /** New status (entry:status only), else null. */
    to: string | null;
    /** Abort reason / remove cause / hydrate code / role / stream phase / msg type. */
    reason: string | null;
    /**
     * Numeric payload (observerCount / fetchGen / streamCount / records); 0 if
     * N/A. For `mutation:settle` this is the settling mutation's OWN generation
     * (pairs with `mutation:start` by value) -- not the current latest gen, so a
     * `superseded` settle still carries the id of the mutation that finished.
     */
    count: number;
    /**
     * Outcome flag; false if N/A. For `mutation:settle` this mirrors the mutation
     * state machine's truthiness (`!error`) -- a FALSY thrown rejection reason
     * reports `ok: true`. This is an inherited 1.4.0 quirk, documented rather than
     * changed.
     */
    ok: boolean;
    /** By-reference payload (data / error / frame / cursor / vars); null if N/A. */
    value: unknown;
}

/** A hook installed via qc.inspect. Observe-only: never throw, never mutate. */
export type FeedHook = (event: QueryFeedEvent) => void;

export interface QueryClient {
    /** Frozen reference to the resolved options. */
    readonly options: Readonly<QueryClientOptions>;

    /** Untracked read of an entry's current data (or `undefined`). */
    getQueryData<T = unknown>(key: readonly unknown[]): T | undefined;

    /**
     * Imperatively set an entry's data. Accepts a value OR an updater
     * `(prev) => next`. When `crossTab: true`, propagates to peer tabs.
     *
     * On an infinite entry the value must be a pages ARRAY (it rebuilds the flat
     * view + cursor). A non-array is fail-closed asymmetric: a LOCAL call throws
     * `TypeError`; a REMOTE/cross-tab non-array payload is dropped silently
     * (cannot throw across tabs) and leaves the entry untouched.
     */
    setQueryData<T = unknown>(
        key: readonly unknown[],
        value: T | ((prev: T | undefined) => T),
    ): void;

    /**
     * Mark matching entries as stale, triggering refetch where observed.
     * By default a prefix match; `{ exact: true }` for precise match.
     */
    invalidate(key: readonly unknown[], opts?: { exact?: boolean }): void;

    /**
     * Remove matching entries from the cache (aborts in-flight with
     * `lite-query:removed`). Prefix match by default; `{ exact: true }` for
     * precise match. Cross-tab propagating when enabled.
     */
    removeQueries(key: readonly unknown[], opts?: { exact?: boolean }): void;

    /** Empty the entire cache. Cross-tab propagating when enabled. */
    clear(): void;

    /**
     * Warm an entry into the cache with zero observers (route loaders, hover
     * speculation). Fetches unless the entry is already fresh; a later query()
     * with the same key adopts it without refetching. Prefetch of a fresh
     * entry is a NO-OP -- no fetch, no cacheTime GC re-arm.
     *
     * Prefetch is ALSO a strict no-op on an infinite entry (one with a live
     * infiniteQuery): it carries no page cursor and must never advance live
     * pagination, so it returns without fetching or touching the pages.
     */
    prefetch<T = unknown, K extends readonly unknown[] = readonly unknown[]>(
        key: K,
        fetcher: Fetcher<T, K>,
        opts?: {
            staleTime?: number;
            cacheTime?: number;
            timeout?: number;
            retry?: RetryPolicy;
            retryDelay?: RetryDelay;
        },
    ): Promise<T | undefined>;

    /**
     * Snapshot the SUCCESS entries into a plain-JSON dehydrated state (cold
     * path; walks the whole cache). Pending / error / stream entries are never
     * serialized. Infinite entries are marked and carry a shallow copy of their
     * pages array. The emitted state has no `version` field -- the adapter
     * stamps `{ version, state }`. key, data, and page CONTENTS are references,
     * not deep copies: serialize before any further cache write.
     */
    dehydrate(): DehydratedState;

    /**
     * Boot-only cache restore (fail-closed). THROWS if the cache is non-empty
     * (a programming error -- hydrate before any observer attaches); a malformed
     * STORED payload RETURNS `{ ok: false, count: 0, reason }`. Validation runs
     * over the WHOLE payload before any mutation (all-or-nothing). Seeded
     * entries are stale-aware (a future timestamp is clamped to now()) and never
     * broadcast.
     */
    hydrate(state: DehydratedState): HydrateResult;

    /**
     * Install a devtools feed hook -- a push-mode stream of cache truth a panel
     * (e.g. lite-studio) renders. ONE hook per client (a panel multiplexes): a
     * second install while one is live throws Error; a non-function (including an
     * array) throws TypeError. Returns an IDEMPOTENT uninstall thunk. Dispatch is
     * SYNCHRONOUS at the emit site (emission order is the truth); a throwing hook
     * is contained fail-closed -- the feed auto-uninstalls and logs once via
     * console.error, and the in-progress cache write completes. The hook MUST
     * COPY what it keeps (events are pooled per type -- see QueryFeedEvent).
     * Independent of persistQueryClient's private write seam (two seams, never
     * one): uninstalling either never disturbs the other. Zero cost when
     * uninstalled -- every emit site is a single branch-predicted null test.
     */
    inspect(hook: FeedHook): () => void;

    /** Clear the cache and close the BroadcastChannel listener. */
    dispose(): void;
}

export function queryClient(options?: QueryClientOptions): QueryClient;

// --- Persistence (dehydrate / hydrate / persistQueryClient) ------------------

/** A single dehydrated cache entry -- monomorphic wire shape, four own keys. */
export interface DehydratedEntry {
    /** The entry's key array (by reference). */
    key: readonly unknown[];
    /** Plain entry: the data value. Infinite entry: a shallow copy of pages. */
    data: unknown;
    /** The entry's staleness timestamp (must be finite). */
    dataUpdatedAt: number;
    /** True iff this record is an infinite entry (always present). */
    infinite: boolean;
}

/** The dehydrated state emitted by `qc.dehydrate()` -- exactly one own key. */
export interface DehydratedState {
    entries: DehydratedEntry[];
}

/** Reason codes returned by `qc.hydrate` on a malformed payload (fail-closed). */
export type HydrateReason =
    | "malformed-state"
    | "malformed-entries"
    | "malformed-entry"
    | "malformed-key"
    | "malformed-data"
    | "malformed-timestamp"
    | "malformed-pages"
    | "duplicate-key";

/** The observable result of `qc.hydrate(state)`. */
export interface HydrateResult {
    /** True iff the whole payload validated and seeded. */
    ok: boolean;
    /** Number of entries seeded (0 on any drop). */
    count: number;
    /** null on success, else a stable reason code. */
    reason: HydrateReason | null;
}

/** Options for `persistQueryClient(qc, opts)`. */
export interface PersistOptions {
    /** Persist the dehydrated envelope. May return a promise; rejections are contained. */
    save: (envelope: { version: string | number; state: DehydratedState }) => void | Promise<void>;
    /** Load the stored envelope (or null/undefined for an empty store). May be async. */
    load: () => unknown | Promise<unknown>;
    /** REQUIRED schema version (string or number). A mismatch drops the whole cache. */
    version: string | number;
    /** Trailing-edge coalescing window in ms. Default 1000. Must be finite and >= 0. */
    throttle?: number;
}

/** The status of a restore attempt. */
export type RestoreStatus = "restored" | "empty" | "dropped";

/** The reason a restore resolved as it did. */
export type RestoreReason =
    | null
    | "load-threw"
    | "malformed-envelope"
    | "version-mismatch"
    | "cache-not-empty"
    | "hydrate-threw"
    | HydrateReason;

/** The settled restore outcome (the `restored` promise always RESOLVES). */
export interface RestoreOutcome {
    status: RestoreStatus;
    count: number;
    reason: RestoreReason;
}

/** The handle returned by `persistQueryClient`. */
export interface PersistHandle {
    /** Resolves (never rejects) with the restore outcome, observable before any observer attaches. */
    restored: Promise<RestoreOutcome>;
    /** Force the pending save now. */
    flush: () => void;
    /** Idempotent. Flushes any pending save, uninstalls the write hook, clears the timer. */
    stop: () => void;
}

/**
 * Wire a query client to a storage backend. Storage-agnostic thunks only; zero
 * new deps. Restores once on install (before any observer attaches), then
 * throttles the dehydrated snapshot to `save()` through the client's private
 * write hook. Fail-closed: `version` is required, and a malformed / mismatched
 * / unreadable payload restores nothing with the outcome observable via
 * `handle.restored`.
 */
export function persistQueryClient(qc: QueryClient, opts: PersistOptions): PersistHandle;

// --- query() -----------------------------------------------------------------

/** Options for a single `query(qc, opts)`. */
export interface QueryOptions<
    T = unknown,
    K extends readonly unknown[] = readonly unknown[],
> {
    /**
     * Cache key -- static array OR a function reading reactive signals. When
     * the function form returns a different key, the previous fetch is
     * aborted with `lite-query:refetch` and a new one is issued.
     */
    key: K | (() => K);
    /** Async fetcher. Must propagate `signal` for proper cancellation. */
    fetcher: Fetcher<T, K>;
    /** Override `defaultStaleTime` for this query. */
    staleTime?: number;
    /** Override `defaultCacheTime` for this query. */
    cacheTime?: number;
    /** Override `defaultTimeout` for this query. */
    timeout?: number;
    /** Override `retry` policy for this query. */
    retry?: RetryPolicy;
    /** Override `retryDelay` for this query. */
    retryDelay?: RetryDelay;
    /**
     * Gate. Reactive (function form) or static. When false, the query stays
     * idle; flipping to true triggers a fetch (or serves cache).
     */
    enabled?: boolean | (() => boolean);
    /**
     * Equality function for structural sharing. Default `Object.is`. Return
     * `true` if the new data is "the same as before" so observers don't
     * needlessly re-fire on referentially-different-but-structurally-equal
     * data.
     */
    equals?: (a: T | undefined, b: T) => boolean;
}

/** Returned by `query(...)`. All accessors are functions -- call to read. */
export interface Query<T = unknown> {
    /** Latest data, or `undefined` if never resolved. */
    data: ReadAccessor<T | undefined>;
    /** Latest error, or `undefined`. */
    error: ReadAccessor<unknown>;
    /** True when there is NO cached data and a fetch is in flight. */
    loading: ReadAccessor<boolean>;
    /** True when ANY fetch is in flight (including background revalidation). */
    fetching: ReadAccessor<boolean>;
    /** Coarse status. */
    status: ReadAccessor<QueryStatus>;
    /** Force a refetch. Resolves to the new data (or rejects on error). */
    refetch: () => Promise<T | undefined>;
    /** Drop the observer this Query holds. Idempotent. */
    dispose: () => void;
}

export function query<
    T = unknown,
    K extends readonly unknown[] = readonly unknown[],
>(qc: QueryClient, opts: QueryOptions<T, K>): Query<T>;

// --- infiniteQuery() ---------------------------------------------------------

/** Context passed to an `infiniteQuery` fetcher. `cursor` is `null` for page one. */
export interface InfiniteFetcherContext<
    C = unknown,
    K extends readonly unknown[] = readonly unknown[],
> {
    key: K;
    /** Cursor for this page fetch. `null` on the initial (page-one) load. */
    cursor: C | null;
    signal: AbortSignal;
}

/** Options for `infiniteQuery(qc, opts)`. */
export interface InfiniteQueryOptions<
    T = unknown,
    C = unknown,
    K extends readonly unknown[] = readonly unknown[],
> {
    /** Cache key -- static array OR a reactive function, same as query(). */
    key: K | (() => K);
    /** Async page fetcher. Receives `{ key, cursor, signal }`. */
    fetcher: (ctx: InfiniteFetcherContext<C, K>) => Promise<T>;
    /**
     * Derive the next page's cursor from the last page (and all pages so far).
     * Return `null`/`undefined` to signal exhaustion -- `hasNextPage()` reads
     * false and `fetchNextPage()` becomes a no-op.
     *
     * A throw here is CONTAINED into the error ladder (status `"error"`,
     * `error()` = the thrown value, `fetching` false, promise cleared); the
     * already-committed pages are preserved and a later `fetchNextPage()` /
     * `refetch()` re-attempts the same cursor cleanly. It never wedges the entry.
     */
    getNextCursor: (lastPage: T, allPages: T[]) => C | null | undefined;
    /** Override `defaultStaleTime` for this query. */
    staleTime?: number;
    /** Override `defaultCacheTime` for this query. */
    cacheTime?: number;
    /** Override `defaultTimeout` for this query. */
    timeout?: number;
    /** Override `retry` policy for this query. */
    retry?: RetryPolicy;
    /** Override `retryDelay` for this query. */
    retryDelay?: RetryDelay;
    /** Gate. Reactive (function form) or static. */
    enabled?: boolean | (() => boolean);
}

/**
 * Returned by `infiniteQuery(...)`. All accessors are functions -- call to read.
 *
 * `data()` returns a LIVE array: it is appended to in place as pages arrive, so
 * a reference captured across a `fetchNextPage()` will be seen to grow. Copy it
 * (`[...q.data()]`) if you need to retain a point-in-time snapshot of the list.
 *
 * One accessor kind per key: do not point a `query()` and an `infiniteQuery()`
 * at the same cache key. There is one entry per key, and configuring it as
 * infinite gives its `data` signal never-equal notification semantics that a
 * plain `query()` observer on that key would inherit -- outside the supported
 * contract.
 */
export interface InfiniteQuery<T = unknown> {
    /** The array of raw page results (one entry per fetched page). */
    pages: ReadAccessor<T[] | undefined>;
    /** The flattened accumulation across all pages. Live + growing (see above). */
    data: ReadAccessor<unknown[] | undefined>;
    /** True while there is a next page to fetch (cursor not yet exhausted). */
    hasNextPage: ReadAccessor<boolean>;
    /** Fetch + append the next page. Dedups on the in-flight fetch; no-op when exhausted. */
    fetchNextPage: () => Promise<unknown>;
    /** Coarse status of the current page fetch. */
    status: ReadAccessor<QueryStatus>;
    /** Latest page-fetch error, or `undefined`. */
    error: ReadAccessor<unknown>;
    /** True when any page fetch is in flight (including a background refetch). */
    fetching: ReadAccessor<boolean>;
    /** Refetch the whole list from page one, replacing on success. */
    refetch: () => Promise<unknown>;
    /** Drop the observer this handle holds. Idempotent. */
    dispose: () => void;
}

export function infiniteQuery<
    T = unknown,
    C = unknown,
    K extends readonly unknown[] = readonly unknown[],
>(qc: QueryClient, opts: InfiniteQueryOptions<T, C, K>): InfiniteQuery<T>;

// --- mutation() --------------------------------------------------------------

export interface MutationOptions<TData = unknown, TVars = unknown, TCtx = unknown> {
    /** The mutation itself. Should call the network and return server data. */
    fn: (vars: TVars) => Promise<TData>;
    /**
     * Runs FIRST. Snapshot cache + write optimistic updates here. Its return
     * value is passed as `ctx` to the later callbacks.
     */
    onMutate?: (vars: TVars) => TCtx | Promise<TCtx>;
    /** Runs on fn success. Errors here are CONTAINED -- they do NOT propagate. */
    onSuccess?: (data: TData, vars: TVars, ctx: TCtx) => void | Promise<void>;
    /** Runs on fn error. Roll back from `ctx` here. Errors here CONTAINED. */
    onError?: (error: unknown, vars: TVars, ctx: TCtx) => void | Promise<void>;
    /** Runs LAST -- success or error. ALWAYS fires, even if onSuccess threw. */
    onSettled?: (
        data: TData | undefined,
        error: unknown,
        vars: TVars,
        ctx: TCtx,
    ) => void | Promise<void>;
}

export interface Mutation<TData = unknown, TVars = unknown> {
    /** Last success data. */
    data: ReadAccessor<TData | undefined>;
    /** Last error. */
    error: ReadAccessor<unknown>;
    /** Coarse status. */
    status: ReadAccessor<MutationStatus>;
    /** True when `status === "pending"`. */
    loading: ReadAccessor<boolean>;
    /**
     * Run the mutation. Resolves with `fn`'s result (UNAFFECTED by callback
     * errors). Concurrent `mutate(varsB)` after a slow `mutate(varsA)` does
     * NOT corrupt A's awaited result.
     */
    mutate: (vars: TVars) => Promise<TData>;
    /** Cancel any in-flight + zero out data/error/status. */
    reset: () => void;
    /**
     * Release the mutation's signal nodes back to lite-signal's pool.
     * Idempotent. Most apps never need this (mutations are usually long-
     * lived); useful for tests + ephemeral mutations to prevent pool
     * pressure on the default registry.
     */
    dispose: () => void;
}

export function mutation<TData = unknown, TVars = unknown, TCtx = unknown>(
    qc: QueryClient,
    opts: MutationOptions<TData, TVars, TCtx>,
): Mutation<TData, TVars>;

/** The package version string. The single runtime source (Query.js);
 * re-exported by the /stream and /await entries and asserted equal to
 * package.json by test/version-sync.test.js. */
export const VERSION: string;
