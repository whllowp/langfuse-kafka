import { LRUCache } from "lru-cache";
import { recordIncrement } from "../instrumentation";

// Upper bound on cached query results. The cache only ever holds ClickHouse
// row payloads for UI table queries, so this caps memory regardless of key
// cardinality.
const MAX_RESULT_CACHE_ENTRIES = 1000;

const inflightRequests = new Map<string, Promise<unknown[]>>();
const resultCache = new LRUCache<string, unknown[]>({
  max: MAX_RESULT_CACHE_ENTRIES,
  ttlAutopurge: false,
  allowStale: false,
  updateAgeOnGet: false,
  updateAgeOnHas: false,
});

/**
 * Coalesces identical concurrent read queries into a single execution and,
 * for `ttlMs > 0`, serves successful results from a short-lived TTL cache.
 *
 * The key MUST be built from the caller's structured request inputs, never
 * from the rendered query text: filter rendering generates random parameter
 * names per call, so the rendered SQL of two identical logical requests never
 * matches and a query-text key would not coalesce anything.
 *
 * The in-flight placeholder is registered synchronously before the first
 * await, so callers that invoke the same key in the same synchronous block
 * (e.g. a `Promise.all` over identical requests) share one execution.
 */
export const coalescedQueryClickhouse = async <T>({
  key,
  ttlMs,
  loader,
}: {
  key: string;
  ttlMs: number;
  loader: () => Promise<T[]>;
}): Promise<T[]> => {
  if (ttlMs > 0) {
    const cached = resultCache.get(key);
    if (cached !== undefined) {
      recordIncrement("langfuse.query.coalesced", 1, { outcome: "ttl_hit" });
      return cached as T[];
    }
  }

  const inflight = inflightRequests.get(key) as Promise<T[]> | undefined;
  if (inflight !== undefined) {
    recordIncrement("langfuse.query.coalesced", 1, { outcome: "shared" });
    return inflight;
  }

  const promise = loader().then(
    (result) => {
      // Only successful results are cached; ttlMs of 0 disables the result
      // cache and keeps single-flight behavior only.
      if (ttlMs > 0) {
        resultCache.set(key, result, { ttl: ttlMs });
      }
      inflightRequests.delete(key);
      return result;
    },
    (error) => {
      // Errors are never cached. Every subscriber of the in-flight promise
      // receives the same rejection and the next call re-executes the loader.
      inflightRequests.delete(key);
      throw error;
    },
  );
  inflightRequests.set(key, promise);

  return promise;
};

/**
 * Deterministic JSON serialization for coalescing/cache keys.
 *
 * Value semantics MUST come from JSON.stringify itself: Date instances are
 * serialized through `toJSON` to ISO strings, function properties are
 * dropped, and `undefined` follows JSON.stringify rules (omitted from
 * objects, `null` inside arrays, top-level `undefined` yields the literal
 * string "undefined"). Only plain objects (prototype `Object.prototype` or
 * `null`) get their keys sorted, recursively; arrays keep their order.
 *
 * Do not replace the JSON.stringify roundtrip below with a hand-written
 * recursion over own enumerable properties: without `toJSON` handling, Date
 * instances serialize as "{}" and distinct time windows collide into a
 * single key, making single-flight and the TTL cache return data for the
 * wrong time window.
 */
export const stableStringify = (value: unknown): string => {
  const json = JSON.stringify(value);
  if (json === undefined) {
    return "undefined";
  }
  return stringifySorted(JSON.parse(json));
};

/**
 * Normalizes structured request inputs for key construction: Date instances
 * become ISO strings, arrays are traversed element-wise, plain objects
 * property-wise; every other value passes through unchanged. Applying this
 * before `stableStringify` guarantees the key input contains no Date
 * instances regardless of how the serializer treats them.
 */
export const normalizeFilterForKey = (value: unknown): unknown => {
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (Array.isArray(value)) {
    return value.map((entry) => normalizeFilterForKey(entry));
  }
  if (isPlainObject(value)) {
    const normalized: Record<string, unknown> = {};
    for (const [entryKey, entryValue] of Object.entries(value)) {
      normalized[entryKey] = normalizeFilterForKey(entryValue);
    }
    return normalized;
  }
  return value;
};

const isPlainObject = (value: unknown): value is Record<string, unknown> => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return false;
  }
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
};

// Operates on values that already went through JSON.stringify semantics, so
// no Date instances (or other toJSON-bearing values) can occur here.
const stringifySorted = (value: unknown): string => {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value) as string;
  }
  if (Array.isArray(value)) {
    return `[${value.map((entry) => stringifySorted(entry)).join(",")}]`;
  }
  const source = value as Record<string, unknown>;
  const body = Object.keys(source)
    .sort()
    .map(
      (entryKey) =>
        `${JSON.stringify(entryKey)}:${stringifySorted(source[entryKey])}`,
    )
    .join(",");
  return `{${body}}`;
};

export const __resetForTests = (): void => {
  inflightRequests.clear();
  resultCache.clear();
};
