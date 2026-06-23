/**
 * @zakkster/lite-await -- zero-GC bridge between lite-signal and Promise.
 *
 * Public type surface for the JavaScript implementation in `Await.js`.
 */

import { Signal } from "@zakkster/lite-signal";

// ---------------------------------------------------------------------------
// Shared options
// ---------------------------------------------------------------------------

/**
 * Options accepted by every awaiter. AbortSignal-first: any awaiter can be
 * cancelled by aborting the signal you pass in, structurally cleaning up its
 * underlying effect.
 */
export interface AwaitOptions {
    /** Reject with `TimeoutError` if no settlement within this many ms. */
    timeout?: number;
    /** Reject if this AbortSignal aborts before settlement. */
    signal?: AbortSignal;
}

/** A spec for the multi-source combinators: `[source, predicate]`. */
export type AwaitSpec<T> = readonly [() => T, (value: T) => unknown];

// ---------------------------------------------------------------------------
// Async state (fromPromise)
// ---------------------------------------------------------------------------

export type AsyncState<T> =
    | { readonly status: "pending";  readonly data: T | undefined; readonly error: undefined }
    | { readonly status: "resolved"; readonly data: T;             readonly error: undefined }
    | { readonly status: "rejected"; readonly data: T | undefined; readonly error: unknown };

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/**
 * Thrown when a `timeout` deadline elapses before settlement.
 */
export class TimeoutError extends Error {
    readonly name: "TimeoutError";
    readonly timeout: number;
    constructor(timeoutMs: number);
}

// ---------------------------------------------------------------------------
// Core primitives
// ---------------------------------------------------------------------------

/**
 * Wait until `source()` first satisfies `predicate`, then resolve with the
 * satisfying value. Supports `timeout` and `signal` options. Settlement -- by
 * resolve, abort, or timeout -- always cleans the underlying effect.
 *
 * If the source ALREADY satisfies the predicate on first read, resolves
 * synchronously on the next microtask.
 */
export function whenSignal<T>(
    source: () => T,
    predicate: (value: T) => unknown,
    opts?: AwaitOptions
): Promise<T>;

/**
 * Wait until EVERY spec resolves. Resolves with the values in input order.
 * Rejects on first rejection (timeout, abort, or per-spec error); on any
 * failure path, the remaining in-flight specs are aborted.
 */
export function allOf<const S extends ReadonlyArray<AwaitSpec<any>>>(
    specs: S,
    opts?: AwaitOptions
): Promise<{ -readonly [K in keyof S]: S[K] extends AwaitSpec<infer T> ? T : never }>;

/**
 * Wait until ANY spec resolves. Resolves with `{ index, value }` of the
 * winner. Rejects with `AggregateError` only if EVERY spec rejects.
 */
export function anyOf<T>(
    specs: ReadonlyArray<AwaitSpec<T>>,
    opts?: AwaitOptions
): Promise<{ index: number; value: T }>;

/**
 * Settle on the FIRST spec to settle -- success OR failure. Resolves with
 * `{ index, value }` on first success; rejects on first rejection (including
 * a single spec's abort/timeout).
 */
export function raceOf<T>(
    specs: ReadonlyArray<AwaitSpec<T>>,
    opts?: AwaitOptions
): Promise<{ index: number; value: T }>;

// ---------------------------------------------------------------------------
// Promise wrappers (non-signal-aware)
// ---------------------------------------------------------------------------

/**
 * Wrap an arbitrary Promise with a deadline. Rejects with `TimeoutError`
 * after `ms`. Inner work is NOT cancelled -- use the `timeout` option on the
 * primitives for signal-aware work.
 */
export function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T>;

/**
 * Wrap an arbitrary Promise with an AbortSignal. Rejects with AbortError if
 * the signal aborts before settlement. Inner work is NOT cancelled.
 */
export function withAbort<T>(promise: Promise<T>, signal: AbortSignal): Promise<T>;

// ---------------------------------------------------------------------------
// Convenience shorthands
// ---------------------------------------------------------------------------

/** `whenSignal(source, Boolean, opts)`. */
export function whenTruthy<T>(source: () => T, opts?: AwaitOptions): Promise<T>;

/** `whenSignal(source, (v) => Object.is(v, target), opts)`. */
export function whenEquals<T>(source: () => T, target: T, opts?: AwaitOptions): Promise<T>;

// ---------------------------------------------------------------------------
// Bidirectional bridge: Promise -> Signal
// ---------------------------------------------------------------------------

/**
 * Drive a single lite-signal from a Promise's lifecycle. The signal holds
 * one of three shapes: `pending`, `resolved`, or `rejected`. Updates EXACTLY
 * ONCE on settlement. Use `dispose(sig)` from lite-signal to release the
 * signal's node back to the pool when no longer needed.
 */
export function fromPromise<T>(
    promise: Promise<T>,
    initialData?: T
): Signal<AsyncState<T>>;

// ---------------------------------------------------------------------------
// Specialization: lite-statechart
// ---------------------------------------------------------------------------

/** Minimal duck-typed interface accepted by `whenStatechart`. */
export interface StatechartLike {
    state: { peek(): string };
    onTransition(
        fn: (from: string, to: string, event: string, payload: unknown) => void
    ): () => void;
}

/**
 * Resolve when the statechart enters `stateName`. Uses `onTransition`
 * directly (one listener slot) instead of going through `state` as a tracked
 * read (which would allocate a tracking effect node). Duck-typed so any FSM
 * shaped like lite-statechart works.
 */
export function whenStatechart(
    machine: StatechartLike,
    stateName: string,
    opts?: AwaitOptions
): Promise<void>;
