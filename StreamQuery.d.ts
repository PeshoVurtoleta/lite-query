/**
 * @zakkster/lite-query/stream — type declarations
 *
 * One export: `streamQuery` (the read-side primitive for an iterator-backed,
 * multi-shot query). Values are pumped through @zakkster/lite-stream into the
 * same queryClient cache a `query()` uses. Requires @zakkster/lite-stream.
 */

import type { QueryClient, ReadAccessor } from "./Query.js";

/** Stream status. `streaming` is the multi-shot state a fetch query never has. */
export type StreamStatus = "idle" | "pending" | "streaming" | "success" | "error";

/** Delivery mode. `latest` keeps the most recent value; `buffer` keeps a window. */
export type StreamMode = "latest" | "buffer";

export interface StreamContext<K extends readonly unknown[] = readonly unknown[]> {
    /** The cache key for this stream. */
    key: K;
    /**
     * Abort signal. Fires on detach (last observer leaves), reactive-key
     * change, `restart()`, `invalidate`, and `removeQueries` / `clear`. The
     * source should stop yielding when it fires (and is also `iterator.return()`-ed).
     */
    signal: AbortSignal;
}

/** A factory producing the async iterable/iterator to consume for a given key. */
export type StreamSource<T, K extends readonly unknown[] = readonly unknown[]>
    = (ctx: StreamContext<K>) => AsyncIterable<T> | AsyncIterator<T>;

export interface StreamQueryOptions<
    T = unknown,
    K extends readonly unknown[] = readonly unknown[],
> {
    /**
     * Cache key — static array OR a function reading reactive signals. A new
     * key aborts the current stream and establishes one for the new key.
     */
    key: K | (() => K);
    /** Factory for the source iterable. Must stop when `ctx.signal` aborts. */
    stream: StreamSource<T, K>;
    /** `"latest"` (default) or `"buffer"`. */
    mode?: StreamMode;
    /** Window size for buffer mode. Required (positive integer) when `mode` is `"buffer"`. */
    maxBuffer?: number;
    /**
     * Gate. Reactive (function form) or static. When false the stream is not
     * established; flipping to true starts it; flipping to false aborts it.
     */
    enabled?: boolean | (() => boolean);
    /** Override `defaultCacheTime` for this entry. */
    cacheTime?: number;
}

/**
 * Returned by `streamQuery(...)`. Accessors marked reactive are functions to
 * call inside an effect; `count` / `droppedCount` are non-reactive snapshots
 * (read them alongside `data()` in the same effect to see them advance).
 *
 * `TData` is what `data()` yields: the value type in latest mode, the window
 * array in buffer mode (see the overloads on `streamQuery`).
 */
export interface StreamQuery<TData = unknown> {
    /** Latest value (latest mode) or current window (buffer mode); `undefined` before the first value. */
    data: ReadAccessor<TData | undefined>;
    /** Latest error, or `undefined`. Set only on a genuine failure (not on intentional abort). */
    error: ReadAccessor<unknown>;
    /** Coarse status. */
    status: ReadAccessor<StreamStatus>;
    /** True before the first value arrives (status `pending`). */
    loading: ReadAccessor<boolean>;
    /** True once the iterator completes naturally (status `success`). */
    done: ReadAccessor<boolean>;
    /** Non-reactive: number of values seen this session. */
    count: () => number;
    /** Non-reactive: number of values dropped from the window (buffer mode). */
    droppedCount: () => number;
    /** Abort the current stream and re-establish it. */
    restart: () => void;
    /** Drop the observer this handle holds and release its signal node. Idempotent. */
    dispose: () => void;
}

// buffer mode: data() yields the window array
export function streamQuery<
    T = unknown,
    K extends readonly unknown[] = readonly unknown[],
>(qc: QueryClient, opts: StreamQueryOptions<T, K> & { mode: "buffer" }): StreamQuery<T[]>;

// latest mode (default): data() yields the value
export function streamQuery<
    T = unknown,
    K extends readonly unknown[] = readonly unknown[],
>(qc: QueryClient, opts: StreamQueryOptions<T, K>): StreamQuery<T>;
