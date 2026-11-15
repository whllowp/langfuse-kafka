import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  __resetForTests,
  coalescedQueryClickhouse,
  normalizeFilterForKey,
  stableStringify,
} from "./queryRequestCoalescer";

describe("coalescedQueryClickhouse", () => {
  beforeEach(() => {
    __resetForTests();
  });

  it("executes the loader once for concurrent calls with the same key", async () => {
    const loader = vi.fn(async () => {
      await new Promise((resolve) => setTimeout(resolve, 10));
      return [{ value: 1 }];
    });

    const results = await Promise.all(
      Array.from({ length: 5 }, () =>
        coalescedQueryClickhouse({ key: "same", ttlMs: 0, loader }),
      ),
    );

    expect(loader).toHaveBeenCalledTimes(1);
    expect(results).toHaveLength(5);
    for (const result of results) {
      expect(result).toEqual([{ value: 1 }]);
    }
  });

  it("serves cached results within the TTL and misses when the key changes", async () => {
    const loaderA = vi.fn(async () => ["a"]);
    await coalescedQueryClickhouse({
      key: "a",
      ttlMs: 60_000,
      loader: loaderA,
    });
    await coalescedQueryClickhouse({
      key: "a",
      ttlMs: 60_000,
      loader: loaderA,
    });
    expect(loaderA).toHaveBeenCalledTimes(1);

    const loaderB = vi.fn(async () => ["b"]);
    await coalescedQueryClickhouse({
      key: "b",
      ttlMs: 60_000,
      loader: loaderB,
    });
    expect(loaderB).toHaveBeenCalledTimes(1);
  });

  it("propagates one rejection to all subscribers and does not cache errors", async () => {
    const error = new Error("boom");
    const loader = vi
      .fn<() => Promise<string[]>>()
      .mockRejectedValueOnce(error)
      .mockResolvedValue(["ok"]);

    const results = await Promise.allSettled([
      coalescedQueryClickhouse({ key: "failing", ttlMs: 60_000, loader }),
      coalescedQueryClickhouse({ key: "failing", ttlMs: 60_000, loader }),
    ]);

    expect(results.map((result) => result.status)).toEqual([
      "rejected",
      "rejected",
    ]);
    for (const result of results) {
      if (result.status === "rejected") {
        expect(result.reason).toBe(error);
      }
    }
    expect(loader).toHaveBeenCalledTimes(1);

    // The failure was not cached: the next call re-executes the loader.
    const recovered = await coalescedQueryClickhouse({
      key: "failing",
      ttlMs: 60_000,
      loader,
    });
    expect(recovered).toEqual(["ok"]);
    expect(loader).toHaveBeenCalledTimes(2);
  });

  it("re-executes after __resetForTests clears all state", async () => {
    const loader = vi.fn(async () => ["value"]);

    await coalescedQueryClickhouse({ key: "reset", ttlMs: 60_000, loader });
    __resetForTests();
    await coalescedQueryClickhouse({ key: "reset", ttlMs: 60_000, loader });

    expect(loader).toHaveBeenCalledTimes(2);
  });

  it("single-flights but does not cache results when ttlMs is 0", async () => {
    const loader = vi.fn(async () => ["value"]);

    await Promise.all([
      coalescedQueryClickhouse({ key: "uncached", ttlMs: 0, loader }),
      coalescedQueryClickhouse({ key: "uncached", ttlMs: 0, loader }),
    ]);
    expect(loader).toHaveBeenCalledTimes(1);

    // The in-flight execution has settled: without a result cache the next
    // sequential call runs the loader again.
    await coalescedQueryClickhouse({ key: "uncached", ttlMs: 0, loader });
    expect(loader).toHaveBeenCalledTimes(2);
  });
});

describe("stableStringify Date semantics", () => {
  // Filter time windows carry Date instances, so the serialization must
  // distinguish Dates (via toJSON -> ISO string), never collapse them to "{}".
  const filterShape = (value: unknown) => ({
    column: "timestamp",
    operator: ">",
    value,
    type: "datetime",
  });

  it("produces different keys for different Date values", () => {
    expect(
      stableStringify(filterShape(new Date("2026-01-01T00:00:00.000Z"))),
    ).not.toBe(
      stableStringify(filterShape(new Date("2026-06-01T00:00:00.000Z"))),
    );
  });

  it("produces the same key for equal Dates (same millisecond)", () => {
    expect(
      stableStringify(filterShape(new Date("2026-01-01T00:00:00.000Z"))),
    ).toBe(stableStringify(filterShape(new Date("2026-01-01T00:00:00.000Z"))));
  });

  it("serializes a Date identically to its ISO string form", () => {
    expect(
      stableStringify(filterShape(new Date("2026-01-01T00:00:00.000Z"))),
    ).toBe(stableStringify(filterShape("2026-01-01T00:00:00.000Z")));
  });
});

describe("normalizeFilterForKey", () => {
  const filterShape = (value: unknown) => ({
    column: "timestamp",
    operator: ">",
    value,
    type: "datetime",
  });

  it("converts Date instances to ISO strings, deeply", () => {
    expect(
      normalizeFilterForKey({
        outer: [filterShape(new Date("2026-01-01T00:00:00.000Z"))],
      }),
    ).toEqual({
      outer: [filterShape("2026-01-01T00:00:00.000Z")],
    });
  });

  it("normalizes a Date so that its key matches the equivalent string object", () => {
    const dateFilter = normalizeFilterForKey(
      filterShape(new Date("2026-01-01T00:00:00.000Z")),
    );
    const stringFilter = filterShape("2026-01-01T00:00:00.000Z");
    expect(stableStringify(dateFilter)).toBe(stableStringify(stringFilter));
  });
});
