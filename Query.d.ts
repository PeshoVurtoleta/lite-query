/**
 * @zakkster/lite-query — type declarations
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

// ─── Abort reasons (string literals) ─────────────────────────────────────────

/**
 * Reasons attached to `AbortSignal.reason` when lite-query aborts a fetch.
 * Surface these to the user's fetcher for logging / alternative recovery.
 */
export type AbortReason =
    | "lite-query:detach"   // last observer left — fetch aborted as wasted work
    | "lite-query:refetch"  // a newer fetch is starting on the same entry
    | "lite-query:removed"  // entry removed via removeQueries / clear
    | "lite-query:timeout"; // per-query or default `timeout` elapsed

// ─── Status + lifecycle ──────────────────────────────────────────────────────

export type QueryStatus = "idle" | "pending" | "success" | "error";
export type MutationStatus = "idle" | "pending" | "success" | "error";

// ─── Reactive accessor (lite-signal shape — function call, no `.value`) ──────

/** A read-only reactive accessor — call as `fn()` to read; tracks in effects. */
export type ReadAccessor<T> = () => T;

// ─── Fetcher contract ────────────────────────────────────────────────────────

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

// ─── Retry policy ────────────────────────────────────────────────────────────

/** Constant retry count, or a per-attempt decision function. */
export type RetryPolicy = number | ((attempt: number, error: unknown) => boolean);

/** Delay between retries — receives the attempt number (1-indexed). */
export type RetryDelay = (attempt: number) => number;

// ─── QueryClient ─────────────────────────────────────────────────────────────

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
     * `isLeader` to be useful (otherwise inert — each tab fetches itself,
     * which is the safe default). When active, follower tabs broadcast
     * `{ type: "fetch-req", key }` instead of hitting the network; the leader
     * answers from its own observed/cached entry and broadcasts the result.
     */
    sharedFetch?: boolean;
    /** Leader-election oracle. Wire from `@zakkster/lite-channel`'s `sync.isLeader`. */
    isLeader?: () => boolean;
    /** Ms a follower waits before falling back to self-fetch. Default 3000. */
    sharedFetchTimeout?: number;

    // ── Injectables for tests ──
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
export interface QueryClient {
    /** Frozen reference to the resolved options. */
    readonly options: Readonly<QueryClientOptions>;

    /** Untracked read of an entry's current data (or `undefined`). */
    getQueryData<T = unknown>(key: readonly unknown[]): T | undefined;

    /**
     * Imperatively set an entry's data. Accepts a value OR an updater
     * `(prev) => next`. When `crossTab: true`, propagates to peer tabs.
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

    /** Clear the cache and close the BroadcastChannel listener. */
    dispose(): void;
}

export function queryClient(options?: QueryClientOptions): QueryClient;

// ─── query() ─────────────────────────────────────────────────────────────────

/** Options for a single `query(qc, opts)`. */
export interface QueryOptions<
    T = unknown,
    K extends readonly unknown[] = readonly unknown[],
> {
    /**
     * Cache key — static array OR a function reading reactive signals. When
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

/** Returned by `query(...)`. All accessors are functions — call to read. */
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

// ─── mutation() ──────────────────────────────────────────────────────────────

export interface MutationOptions<TData = unknown, TVars = unknown, TCtx = unknown> {
    /** The mutation itself. Should call the network and return server data. */
    fn: (vars: TVars) => Promise<TData>;
    /**
     * Runs FIRST. Snapshot cache + write optimistic updates here. Its return
     * value is passed as `ctx` to the later callbacks.
     */
    onMutate?: (vars: TVars) => TCtx | Promise<TCtx>;
    /** Runs on fn success. Errors here are CONTAINED — they do NOT propagate. */
    onSuccess?: (data: TData, vars: TVars, ctx: TCtx) => void | Promise<void>;
    /** Runs on fn error. Roll back from `ctx` here. Errors here CONTAINED. */
    onError?: (error: unknown, vars: TVars, ctx: TCtx) => void | Promise<void>;
    /** Runs LAST — success or error. ALWAYS fires, even if onSuccess threw. */
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
