/**
 * @zakkster/lite-query/await — type declarations
 *
 * Async coordination for lite-query. Re-exports the @zakkster/lite-await
 * primitives verbatim (single source of truth) and adds two query-native
 * bridges: `whenQuery` and `whenAllQueries`. Requires @zakkster/lite-await.
 */

// Re-exported lite-await primitives (types come from lite-await's own .d.ts).
export {
    whenSignal,
    whenTruthy,
    whenEquals,
    allOf,
    anyOf,
    raceOf,
    withTimeout,
    withAbort,
    fromPromise,
    TimeoutError,
} from "@zakkster/lite-await";

/** Options accepted by the query bridges. */
export interface WhenQueryOptions {
    /** Reject with `TimeoutError` if not settled within this many ms. */
    timeout?: number;
    /** Reject with the signal's abort reason when it fires. */
    signal?: AbortSignal;
}

/** Minimal shape both `query()` and `streamQuery()` handles satisfy. */
export interface QueryHandleLike<T = unknown> {
    status: () => string;
    data: () => T;
    error: () => unknown;
}

/**
 * Resolve when a query's status satisfies `predicate` (default: `"success"`),
 * with the query's `data()`. Rejects with the query's `error()` if it reaches
 * `"error"` first, or with `TimeoutError` / the abort reason per `opts`.
 *
 * Works with a `streamQuery` handle too — e.g. await the `"streaming"` status:
 * `whenQuery(s, (st) => st === "streaming")`.
 */
export function whenQuery<T = unknown>(
    q: QueryHandleLike<T>,
    opts?: WhenQueryOptions,
): Promise<T>;
export function whenQuery<T = unknown>(
    q: QueryHandleLike<T>,
    predicate: (status: string) => unknown,
    opts?: WhenQueryOptions,
): Promise<T>;

/**
 * Resolve when EVERY query reaches `"success"`, with the data values in input
 * order. Fail-fast: rejects on the first query to reach `"error"` (with that
 * query's error), or on timeout / abort.
 */
export function whenAllQueries<T = unknown>(
    queries: ReadonlyArray<QueryHandleLike<T>>,
    opts?: WhenQueryOptions,
): Promise<T[]>;
