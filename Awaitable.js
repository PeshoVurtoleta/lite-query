// @zakkster/lite-query/await 1.1.0
//
// Async coordination for lite-query. Re-exports the @zakkster/lite-await
// primitives verbatim (single source of truth -- no reimplementation) and adds
// two query-native bridges that speak in query handles instead of raw signals.
//
//   whenQuery(q, predicate?, opts?)  -- await a single query reaching a state.
//   whenAllQueries(queries, opts?)   -- await every query reaching success.
//
// Both bridges reject with the query's own error() on an error status, mirroring
// lite-await's whenSignal settlement semantics: the bridge predicate THROWS the
// error, and whenSignal/allOf route a throwing predicate to rejection. No
// parallel reject path, no settle-always variant -- a consumer that wants
// non-throwing await uses try/catch or Promise.allSettled.
//
// Requires @zakkster/lite-await as a peer dependency. Importing this entry
// without it installed throws a clear module-resolution error.
//
// Copyright (c) 2026 Zahary Shinikchiev <shinikchiev@yahoo.com>
// MIT License

import {
    whenSignal,
    whenTruthy,
    whenEquals,
    allOf,
    anyOf,
    raceOf,
    withTimeout,
    withAbort,
    fromPromise,
    TimeoutError
} from "@zakkster/lite-await";

// ---------------------------------------------------------------------------
// Internal: validate a query handle (duck-typed)
// ---------------------------------------------------------------------------
// A query handle is anything exposing status(), data(), error() as functions --
// so this accepts both query() and streamQuery() handles, and any custom shape
// that matches. We never reach into lite-query internals here.

function isQueryHandle(q) {
    return q !== null
        && typeof q === "object"
        && typeof q.status === "function"
        && typeof q.data === "function"
        && typeof q.error === "function";
}

// ---------------------------------------------------------------------------
// whenQuery
// ---------------------------------------------------------------------------

/**
 * Resolve when a query reaches a target state; reject with the query's error()
 * if it reaches "error" first.
 *
 * The default predicate waits for status === "success" and resolves with the
 * query's data(). A custom predicate over the status string overrides that
 * (e.g. wait for "streaming" on a streamQuery). Honors `timeout` and `signal`
 * by forwarding them to the underlying whenSignal.
 *
 * Overload: whenQuery(q, opts) is accepted when the second argument is an
 * options object (not a function) and no third argument is given.
 *
 * @template T
 * @param {{ status: () => string, data: () => T, error: () => unknown }} q
 * @param {((status: string) => unknown) | { timeout?: number, signal?: AbortSignal }} [predicate]
 * @param {{ timeout?: number, signal?: AbortSignal }} [opts]
 * @returns {Promise<T>} Resolves with q.data(); rejects with q.error() on error
 *                       status, or TimeoutError / AbortError per opts.
 *
 * @example
 *   const user = query(qc, { key: ["user", id], fetcher });
 *   const data = await whenQuery(user, { timeout: 5000 });
 */
function whenQuery(q, predicate, opts) {
    // Accept whenQuery(q, opts): second arg is an options object, no third arg.
    if (opts === undefined
        && predicate !== null
        && typeof predicate === "object") {
        opts = predicate;
        predicate = undefined;
    }

    if (!isQueryHandle(q)) {
        return Promise.reject(new TypeError(
            "whenQuery: q must be a query handle with status(), data(), error()"
        ));
    }
    if (predicate !== undefined && predicate !== null && typeof predicate !== "function") {
        return Promise.reject(new TypeError(
            "whenQuery: predicate must be a function (status) => boolean"
        ));
    }

    const pred = (typeof predicate === "function")
        ? predicate
        : function (status) { return status === "success"; };

    return whenSignal(
        function () { return q.status(); },
        function (status) {
            // Reaching "error" rejects with the query's own error. whenSignal
            // routes this throw to its rejection path -- identical settlement
            // to a fetch promise that rejected.
            if (status === "error") {
                throw q.error();
            }
            return pred(status);
        },
        opts
    ).then(function () {
        return q.data();
    });
}

// ---------------------------------------------------------------------------
// whenAllQueries
// ---------------------------------------------------------------------------

/**
 * Resolve when EVERY query reaches "success", with the data values in input
 * order. Rejects on the first query to reach "error" (with that query's
 * error()), or on timeout / abort. Fail-fast, matching Promise.all and allOf:
 * one failed dependency fails the aggregate, and the remaining waits are
 * aborted so their effects clean up.
 *
 * @template T
 * @param {Array<{ status: () => string, data: () => T, error: () => unknown }>} queries
 * @param {{ timeout?: number, signal?: AbortSignal }} [opts]
 * @returns {Promise<T[]>} Resolves with the data array in input order.
 *
 * @example
 *   const [u, prefs, flags] = await whenAllQueries([userQ, prefsQ, flagsQ], { timeout: 8000 });
 */
function whenAllQueries(queries, opts) {
    if (!Array.isArray(queries)) {
        return Promise.reject(new TypeError("whenAllQueries: queries must be an array"));
    }
    if (queries.length === 0) {
        return Promise.resolve([]);
    }
    for (let i = 0; i < queries.length; i = (i + 1) | 0) {
        if (!isQueryHandle(queries[i])) {
            return Promise.reject(new TypeError(
                "whenAllQueries: queries[" + i + "] must be a query handle with "
                    + "status(), data(), error()"
            ));
        }
    }

    const specs = new Array(queries.length);
    for (let i = 0; i < queries.length; i = (i + 1) | 0) {
        const q = queries[i];
        specs[i] = [
            function () { return q.status(); },
            function (status) {
                if (status === "error") {
                    throw q.error();
                }
                return status === "success";
            }
        ];
    }

    return allOf(specs, opts).then(function () {
        const out = new Array(queries.length);
        for (let i = 0; i < queries.length; i = (i + 1) | 0) {
            out[i] = queries[i].data();
        }
        return out;
    });
}

// ---------------------------------------------------------------------------
// Exports
// ---------------------------------------------------------------------------

export {
    // Re-exported lite-await primitives (single source of truth).
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
    // lite-query-native bridges.
    whenQuery,
    whenAllQueries
};
