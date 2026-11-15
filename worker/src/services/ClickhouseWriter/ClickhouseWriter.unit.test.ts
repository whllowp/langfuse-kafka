import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import * as serverExports from "@langfuse/shared/src/server";

import { env } from "../../env";
import { logger } from "@langfuse/shared/src/server";
import { ClickhouseWriter, TableName } from "../ClickhouseWriter";

// Mock recordHistogram, recordDistribution, recordCount, recordGauge
vi.mock("@langfuse/shared/src/server", async (importOriginal) => {
  const original = (await importOriginal()) as {};
  return {
    ...original,
    recordHistogram: vi.fn(),
    recordDistribution: vi.fn(),
    recordIncrement: vi.fn(),
    recordCount: vi.fn(),
    recordGauge: vi.fn(),
    logger: {
      info: vi.fn(),
      debug: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
    },
  };
});

vi.mock("../../env", async (importOriginal) => {
  const original = (await importOriginal()) as {};
  return {
    ...original,
    env: {
      LANGFUSE_INGESTION_CLICKHOUSE_WRITE_BATCH_SIZE: 100,
      LANGFUSE_INGESTION_CLICKHOUSE_WRITE_MAX_BATCH_SIZE: 1000,
      LANGFUSE_INGESTION_CLICKHOUSE_WRITE_INTERVAL_MS: 5000,
      LANGFUSE_INGESTION_CLICKHOUSE_MAX_ATTEMPTS: 3,
    },
  };
});

describe("ClickhouseWriter", () => {
  let clickhouseClientMock: {
    insert: ReturnType<typeof vi.fn>;
  };
  let writer: ClickhouseWriter;

  beforeEach(() => {
    vi.clearAllMocks();
    clickhouseClientMock = {
      insert: vi.fn(),
    };
    vi.useFakeTimers();
    writer = ClickhouseWriter.getInstance(clickhouseClientMock);
  });

  afterEach(async () => {
    vi.useRealTimers();

    // Reset singleton instance
    await writer.shutdown();

    ClickhouseWriter.instance = null;
    vi.restoreAllMocks();
    vi.clearAllMocks();
  });

  it("should be a singleton", () => {
    const instance1 = ClickhouseWriter.getInstance();
    const instance2 = ClickhouseWriter.getInstance();

    expect(instance1).toBe(instance2);
  });

  it("should initialize with correct values", () => {
    expect(writer.batchSize).toBe(
      env.LANGFUSE_INGESTION_CLICKHOUSE_WRITE_BATCH_SIZE,
    );
    expect(writer.writeInterval).toBe(
      env.LANGFUSE_INGESTION_CLICKHOUSE_WRITE_INTERVAL_MS,
    );
    expect(writer.maxAttempts).toBe(
      env.LANGFUSE_INGESTION_CLICKHOUSE_MAX_ATTEMPTS,
    );
  });

  it("should add items to the queue", () => {
    const traceData = { id: "1", name: "test" };
    writer.addToQueue(TableName.Traces, traceData as any);

    expect(writer["queue"][TableName.Traces]).toHaveLength(1);
    expect(writer["queue"][TableName.Traces][0].data).toEqual(traceData);
  });

  it("should flush when queue reaches batch size", async () => {
    const mockInsert = vi
      .spyOn(clickhouseClientMock, "insert")
      .mockResolvedValue();

    for (let i = 0; i < writer.batchSize; i++) {
      writer.addToQueue(TableName.Traces, { id: `${i}`, name: "test" } as any);
    }

    await vi.advanceTimersByTimeAsync(writer.writeInterval);

    expect(mockInsert).toHaveBeenCalledTimes(1);
    expect(writer["queue"][TableName.Traces]).toHaveLength(0);
  });

  it("should flush at regular intervals", async () => {
    const mockInsert = vi
      .spyOn(clickhouseClientMock, "insert")
      .mockResolvedValue();
    writer.addToQueue(TableName.Traces, { id: "1", name: "test" });

    await vi.advanceTimersByTimeAsync(writer.writeInterval);

    expect(mockInsert).toHaveBeenCalledTimes(1);
  });

  it("should mark writer insert log comments as multi-project", async () => {
    const mockInsert = vi
      .spyOn(clickhouseClientMock, "insert")
      .mockResolvedValue();

    writer.addToQueue(TableName.Traces, {
      id: "1",
      name: "test",
      project_id: "project-1",
    } as any);
    writer.addToQueue(TableName.Traces, {
      id: "2",
      name: "test",
      project_id: "project-1",
    } as any);

    await vi.advanceTimersByTimeAsync(writer.writeInterval);

    const logComment = JSON.parse(
      mockInsert.mock.calls[0][0].clickhouse_settings.log_comment,
    );
    expect(logComment).toMatchObject({
      surface: "worker",
      route: "clickhouse-writer",
      projectId: "MULTI_PROJECT",
    });
  });

  it("should handle errors and retry", async () => {
    const mockInsert = vi
      .spyOn(clickhouseClientMock, "insert")
      .mockRejectedValueOnce(new Error("DB Error"))
      .mockResolvedValueOnce();

    writer.addToQueue(TableName.Traces, { id: "1", name: "test" });

    await vi.advanceTimersByTimeAsync(writer.writeInterval);

    expect(mockInsert).toHaveBeenCalledTimes(1);
    expect(logger.error).toHaveBeenCalled();
    expect(writer["queue"][TableName.Traces]).toHaveLength(1);
    expect(writer["queue"][TableName.Traces][0].attempts).toBe(2);

    await vi.advanceTimersByTimeAsync(writer.writeInterval);
    expect(mockInsert).toHaveBeenCalledTimes(2);
    expect(writer["queue"][TableName.Traces]).toHaveLength(0);
  });

  it("should drop records after max attempts", async () => {
    const mockInsert = vi
      .spyOn(clickhouseClientMock, "insert")
      .mockRejectedValue(new Error("DB Error"));

    writer.addToQueue(TableName.Traces, { id: "1", name: "test" });

    for (let i = 0; i < writer.maxAttempts; i++) {
      await vi.advanceTimersByTimeAsync(writer.writeInterval);
    }

    await vi.advanceTimersByTimeAsync(writer.writeInterval);

    expect(mockInsert).toHaveBeenCalledTimes(writer.maxAttempts);
    expect(
      logger.error.mock.calls.some((call) =>
        call[0].includes("Max attempts reached"),
      ),
    ).toBe(true);
    expect(writer["queue"][TableName.Traces]).toHaveLength(0);
    expect(serverExports.recordIncrement).toHaveBeenCalledWith(
      "langfuse.queue.clickhouse_writer.rows_dropped",
      1,
      { entity_type: TableName.Traces },
    );
  });

  it("should retry client request timeouts within the same flush", async () => {
    const mockInsert = vi
      .spyOn(clickhouseClientMock, "insert")
      .mockRejectedValueOnce(new Error("Timeout error."))
      .mockResolvedValueOnce();

    writer.addToQueue(TableName.Traces, { id: "1", name: "test" });

    await vi.advanceTimersByTimeAsync(writer.writeInterval);
    // let the backOff retry delay elapse
    await vi.advanceTimersByTimeAsync(200);

    expect(mockInsert).toHaveBeenCalledTimes(2);
    expect(writer["queue"][TableName.Traces]).toHaveLength(0);
    expect(serverExports.recordIncrement).not.toHaveBeenCalledWith(
      "langfuse.queue.clickhouse_writer.rows_dropped",
      expect.anything(),
      expect.anything(),
    );
  });

  it("should shutdown gracefully", async () => {
    writer.addToQueue(TableName.Traces, { id: "1", name: "test" });
    const mockInsert = vi
      .spyOn(clickhouseClientMock, "insert")
      .mockResolvedValue();

    await writer.shutdown();

    expect(mockInsert).toHaveBeenCalledTimes(1);
    expect(writer["intervalId"]).toBeNull();
    expect(logger.info).toHaveBeenCalledWith(
      "ClickhouseWriter shutdown complete.",
    );
  });

  it("should handle multiple table types", async () => {
    const mockInsert = vi
      .spyOn(clickhouseClientMock, "insert")
      .mockResolvedValue();

    writer.addToQueue(TableName.Traces, { id: "1", name: "trace" });
    writer.addToQueue(TableName.Scores, { id: "2", name: "score" });
    writer.addToQueue(TableName.Observations, { id: "3", name: "observation" });

    await vi.advanceTimersByTimeAsync(writer.writeInterval);

    expect(mockInsert).toHaveBeenCalledTimes(3);
    expect(writer["queue"][TableName.Traces]).toHaveLength(0);
    expect(writer["queue"][TableName.Scores]).toHaveLength(0);
    expect(writer["queue"][TableName.Observations]).toHaveLength(0);
  });

  it("should flush other tables on interval ticks while one table's flush is in flight", async () => {
    let resolveFirstInsert!: () => void;
    const firstInsert = new Promise<void>((resolve) => {
      resolveFirstInsert = resolve;
    });
    const mockInsert = vi
      .spyOn(clickhouseClientMock, "insert")
      .mockReturnValueOnce(firstInsert)
      .mockResolvedValue();

    // Traces has a flush in flight that hangs on its INSERT (backOff defers
    // even the first attempt by a microtask).
    for (let i = 0; i < writer.batchSize; i++) {
      writer.addToQueue(TableName.Traces, { id: `a${i}`, name: "test" } as any);
    }
    await vi.advanceTimersByTimeAsync(0);

    // Scores is a low-traffic table whose rows must not wait for Traces.
    writer.addToQueue(TableName.Scores, { id: "s1", name: "score" } as any);
    writer.addToQueue(TableName.Scores, { id: "s2", name: "score" } as any);

    await vi.advanceTimersByTimeAsync(writer.writeInterval);

    const insertCallsFor = (table: TableName) =>
      mockInsert.mock.calls.filter((call) => call[0].table === table);

    // Per-table single flight: the tick starts no second Traces INSERT...
    expect(insertCallsFor(TableName.Traces)).toHaveLength(1);
    // ...while the same tick flushes Scores.
    expect(insertCallsFor(TableName.Scores)).toHaveLength(1);
    expect(insertCallsFor(TableName.Scores)[0][0].values).toHaveLength(2);
    expect(writer["queue"][TableName.Scores]).toHaveLength(0);

    resolveFirstInsert();
    await vi.advanceTimersByTimeAsync(writer.writeInterval);

    expect(mockInsert).toHaveBeenCalledTimes(2);
    expect(writer["queue"][TableName.Traces]).toHaveLength(0);
  });

  it("should set up interval correctly in start method", () => {
    const setIntervalSpy = vi.spyOn(global, "setInterval");
    writer["start"]();

    expect(setIntervalSpy).toHaveBeenCalledWith(
      expect.any(Function),
      writer.writeInterval,
    );
  });

  it("should flush all queues when flushAll is called directly", async () => {
    const mockInsert = vi
      .spyOn(clickhouseClientMock, "insert")
      .mockResolvedValue();
    writer.addToQueue(TableName.Traces, { id: "1", name: "trace" });
    writer.addToQueue(TableName.Scores, { id: "2", name: "score" });

    await writer["flushAll"](true);

    expect(mockInsert).toHaveBeenCalledTimes(2);
    expect(writer["queue"][TableName.Traces]).toHaveLength(0);
    expect(writer["queue"][TableName.Scores]).toHaveLength(0);
  });

  it("should handle adding items to queue while flush is in progress", async () => {
    const mockInsert = vi
      .spyOn(clickhouseClientMock, "insert")
      .mockImplementation(() => {
        writer.addToQueue(TableName.Traces, { id: "2", name: "test2" });
        return Promise.resolve();
      });

    writer.addToQueue(TableName.Traces, { id: "1", name: "test1" });

    await vi.advanceTimersByTimeAsync(writer.writeInterval);

    expect(mockInsert).toHaveBeenCalledTimes(1);
    expect(writer["queue"][TableName.Traces]).toHaveLength(1);
    expect(writer["queue"][TableName.Traces][0].data.id).toBe("2");
  });

  it("should handle concurrent writes during high load", async () => {
    const mockInsert = vi
      .spyOn(clickhouseClientMock, "insert")
      .mockResolvedValue();
    const concurrentWrites = 1000;

    const writes = Array.from({ length: concurrentWrites }, (_, i) =>
      writer.addToQueue(TableName.Traces, { id: `${i}`, name: `test${i}` }),
    );

    await Promise.all(writes);
    await vi.advanceTimersByTimeAsync(writer.writeInterval);

    // The add that crosses batchSize starts the only flush. Every later
    // trigger is skipped while it is in flight, so its drain loop writes the
    // remaining 900 rows in a single maxBatchSize-capped INSERT. Stable only
    // because flush() assigns the in-flight placeholder synchronously before
    // its first await, while the adds above are still running.
    expect(mockInsert).toHaveBeenCalledTimes(2);
    expect(mockInsert.mock.calls[0][0].values).toHaveLength(100);
    expect(mockInsert.mock.calls[1][0].values).toHaveLength(900);
    expect(writer["queue"][TableName.Traces].length).toBeLessThan(
      writer.batchSize,
    );
  });

  it("should not trigger a second INSERT for a table with a flush in flight", async () => {
    let resolveFirstInsert!: () => void;
    const firstInsert = new Promise<void>((resolve) => {
      resolveFirstInsert = resolve;
    });
    const mockInsert = vi
      .spyOn(clickhouseClientMock, "insert")
      .mockReturnValueOnce(firstInsert)
      .mockResolvedValueOnce();

    for (let i = 0; i < writer.batchSize; i++) {
      writer.addToQueue(TableName.Traces, { id: `a${i}`, name: "test" } as any);
    }

    // Let the triggered flush reach its INSERT: backOff defers even the
    // first attempt by a microtask, so the call needs an await to happen
    // and hang on the pending promise.
    await vi.advanceTimersByTimeAsync(0);

    // Cross the batchSize threshold again while the first INSERT is pending.
    for (let i = 0; i < writer.batchSize; i++) {
      writer.addToQueue(TableName.Traces, { id: `b${i}`, name: "test" } as any);
    }

    expect(mockInsert).toHaveBeenCalledTimes(1);
    expect(writer["queue"][TableName.Traces]).toHaveLength(100);

    resolveFirstInsert();
    await writer.shutdown();

    expect(mockInsert).toHaveBeenCalledTimes(2);
    expect(writer["queue"][TableName.Traces]).toHaveLength(0);
  });

  it("should drain a backlog in maxBatchSize-capped INSERTs", async () => {
    const mockInsert = vi
      .spyOn(clickhouseClientMock, "insert")
      .mockResolvedValue();
    writer["maxBatchSize"] = 250;

    // Seed the queue directly so the full backlog is visible to the first
    // splice instead of being consumed by addToQueue-triggered flushes.
    for (let i = 0; i < 600; i++) {
      writer["queue"][TableName.Traces].push({
        createdAt: Date.now(),
        attempts: 1,
        data: { id: `${i}`, name: "test" },
      });
    }

    await writer["flush"](TableName.Traces);

    expect(mockInsert).toHaveBeenCalledTimes(3);
    expect(mockInsert.mock.calls.map((call) => call[0].values.length)).toEqual([
      250, 250, 100,
    ]);
    expect(writer["queue"][TableName.Traces]).toHaveLength(0);
  });

  it("should not lose rows below batchSize that arrive during a flush on shutdown", async () => {
    let resolveFirstInsert!: () => void;
    const firstInsert = new Promise<void>((resolve) => {
      resolveFirstInsert = resolve;
    });
    const mockInsert = vi
      .spyOn(clickhouseClientMock, "insert")
      .mockReturnValueOnce(firstInsert)
      .mockResolvedValueOnce();

    // The first batchSize rows start a flush whose INSERT stays pending
    // (backOff defers even the first attempt by a microtask).
    for (let i = 0; i < writer.batchSize; i++) {
      writer.addToQueue(TableName.Traces, { id: `a${i}`, name: "test" } as any);
    }
    await vi.advanceTimersByTimeAsync(0);

    // Rows below batchSize arriving while that flush is in flight trigger no
    // new flush; shutdown must still write them out.
    for (let i = 0; i < 80; i++) {
      writer.addToQueue(TableName.Traces, { id: `b${i}`, name: "test" } as any);
    }

    resolveFirstInsert();
    await writer.shutdown();

    expect(mockInsert).toHaveBeenCalledTimes(2);
    const writtenRows = mockInsert.mock.calls.reduce(
      (total, call) => total + call[0].values.length,
      0,
    );
    expect(writtenRows).toBe(180);
    expect(writer["queue"][TableName.Traces]).toHaveLength(0);
  });

  it("should keep maxBatchSize at or above batchSize", () => {
    expect(writer["maxBatchSize"]).toBeGreaterThanOrEqual(writer.batchSize);
  });

  it("should raise maxBatchSize to batchSize when the configured max is lower", async () => {
    const envMock = vi.mocked(env);
    const originalMax =
      envMock.LANGFUSE_INGESTION_CLICKHOUSE_WRITE_MAX_BATCH_SIZE;
    envMock.LANGFUSE_INGESTION_CLICKHOUSE_WRITE_MAX_BATCH_SIZE = 50;

    ClickhouseWriter.instance = null;
    try {
      const constrainedWriter =
        ClickhouseWriter.getInstance(clickhouseClientMock);

      expect(constrainedWriter.batchSize).toBe(
        env.LANGFUSE_INGESTION_CLICKHOUSE_WRITE_BATCH_SIZE,
      );
      expect(constrainedWriter["maxBatchSize"]).toBe(
        constrainedWriter.batchSize,
      );

      await constrainedWriter.shutdown();
    } finally {
      envMock.LANGFUSE_INGESTION_CLICKHOUSE_WRITE_MAX_BATCH_SIZE = originalMax;
      ClickhouseWriter.instance = writer;
    }
  });

  it("should report wait time and processing time metrics correctly", async () => {
    const histogramSpy = vi.spyOn(serverExports, "recordHistogram");
    const distributionSpy = vi.spyOn(serverExports, "recordDistribution");
    const mockInsert = vi
      .spyOn(clickhouseClientMock, "insert")
      .mockResolvedValue();

    writer.addToQueue(TableName.Traces, { id: "1", name: "test" });

    await vi.advanceTimersByTimeAsync(writer.writeInterval);

    expect(histogramSpy).toHaveBeenCalledWith(
      "langfuse.queue.clickhouse_writer.wait_time",
      expect.any(Number),
      { unit: "milliseconds" },
    );

    expect(histogramSpy).toHaveBeenCalledWith(
      "langfuse.queue.clickhouse_writer.processing_time",
      expect.any(Number),
      { unit: "milliseconds" },
    );

    expect(distributionSpy).toHaveBeenCalledWith(
      "langfuse.queue.clickhouse_writer.time_distribution",
      expect.any(Number),
      {
        entity_type: TableName.Traces,
        type: "wait",
        unit: "milliseconds",
      },
    );

    expect(distributionSpy).toHaveBeenCalledWith(
      "langfuse.queue.clickhouse_writer.time_distribution",
      expect.any(Number),
      {
        entity_type: TableName.Traces,
        type: "processing",
        unit: "milliseconds",
      },
    );
  });

  it("should handle different types of Clickhouse client errors", async () => {
    const mockInsert = vi
      .spyOn(clickhouseClientMock, "insert")
      .mockRejectedValueOnce(new Error("Network error"))
      .mockRejectedValueOnce(new Error("Timeout"))
      .mockResolvedValueOnce();

    writer.addToQueue(TableName.Traces, { id: "1", name: "test" });

    await vi.advanceTimersByTimeAsync(writer.writeInterval);
    expect(logger.error).toHaveBeenCalledWith(
      expect.stringContaining("Network error"),
    );

    await vi.advanceTimersByTimeAsync(writer.writeInterval);
    expect(logger.error).toHaveBeenCalledWith(
      expect.stringContaining("Timeout"),
    );

    await vi.advanceTimersByTimeAsync(writer.writeInterval);
    expect(writer["queue"][TableName.Traces]).toHaveLength(0);
  });

  it("should handle partial queue flush correctly", async () => {
    const mockInsert = vi
      .spyOn(clickhouseClientMock, "insert")
      .mockResolvedValue();
    const partialQueueSize = Math.floor(writer.batchSize / 2);

    for (let i = 0; i < partialQueueSize; i++) {
      writer.addToQueue(TableName.Traces, { id: `${i}`, name: "test" } as any);
    }

    await vi.advanceTimersByTimeAsync(writer.writeInterval);

    expect(mockInsert).toHaveBeenCalledTimes(1);
    expect(mockInsert).toHaveBeenCalledWith(
      expect.objectContaining({
        values: expect.arrayContaining(
          new Array(partialQueueSize).fill(expect.any(Object)),
        ),
      }),
    );
    expect(writer["queue"][TableName.Traces]).toHaveLength(0);
  });

  it("should continue functioning after encountering an error", async () => {
    const mockInsert = vi
      .spyOn(clickhouseClientMock, "insert")
      .mockRejectedValueOnce(new Error("DB Error"))
      .mockResolvedValue();

    writer.addToQueue(TableName.Traces, { id: "1", name: "test1" });
    await vi.advanceTimersByTimeAsync(writer.writeInterval);

    writer.addToQueue(TableName.Traces, { id: "2", name: "test2" });
    await vi.advanceTimersByTimeAsync(writer.writeInterval);

    expect(mockInsert).toHaveBeenCalledTimes(2);
    expect(writer["queue"][TableName.Traces]).toHaveLength(0);
  });

  describe("Decimal64(12) clamping", () => {
    describe("clampDecimal64Value", () => {
      it.each([
        { input: 0, expected: [0, false], name: "zero" },
        { input: 0.001, expected: [0.001, false], name: "small positive" },
        { input: -42.5, expected: [-42.5, false], name: "small negative" },
        {
          input: 999_999,
          expected: [999_999, false],
          name: "just under limit",
        },
        {
          input: 999_999.999_999,
          expected: [999_999.999_999, false],
          name: "max representable",
        },
        {
          input: 1_000_000,
          expected: [999_999.999_999, true],
          name: "exact limit",
        },
        {
          input: 8_859_794,
          expected: [999_999.999_999, true],
          name: "positive overflow",
        },
        {
          input: -1_000_000,
          expected: [-999_999.999_999, true],
          name: "exact negative limit",
        },
        {
          input: -8_859_794,
          expected: [-999_999.999_999, true],
          name: "negative overflow",
        },
        { input: NaN, expected: [0, true], name: "NaN" },
        { input: Infinity, expected: [0, true], name: "positive Infinity" },
        { input: -Infinity, expected: [0, true], name: "negative Infinity" },
      ])("$name ($input)", ({ input, expected }) => {
        expect(ClickhouseWriter["clampDecimal64Value"](input)).toEqual(
          expected,
        );
      });
    });

    describe("clampDecimal64Map", () => {
      it("returns undefined for undefined input", () => {
        const result = writer["clampDecimal64Map"](undefined, {
          recordId: "r1",
          projectId: "p1",
          fieldName: "cost_details",
        });
        expect(result).toBeUndefined();
      });

      it("returns original map when no values need clamping", () => {
        const map = { input: 0.001, output: 42.5 };
        const result = writer["clampDecimal64Map"](map, {
          recordId: "r1",
          projectId: "p1",
          fieldName: "cost_details",
        });
        expect(result).toBe(map); // same reference, no allocation
        expect(logger.warn).not.toHaveBeenCalled();
      });

      it("clamps multiple overflowing entries correctly", () => {
        const result = writer["clampDecimal64Map"](
          { input: 2_000_000, output: -5_000_000, total: NaN },
          { recordId: "r1", projectId: "p1", fieldName: "cost_details" },
        );
        expect(result).toEqual({
          input: 999_999.999_999,
          output: -999_999.999_999,
          total: 0,
        });
      });

      it("clamps only overflowing entries and logs once", () => {
        const result = writer["clampDecimal64Map"](
          { input: 0.001, output: 8_859_794 },
          { recordId: "r1", projectId: "p1", fieldName: "cost_details" },
        );
        expect(result).toEqual({ input: 0.001, output: 999_999.999_999 });
        expect(logger.warn).toHaveBeenCalledWith(
          "Clamped Decimal64(12) overflow in cost map",
          expect.objectContaining({
            projectId: "p1",
            recordId: "r1",
            fieldName: "cost_details",
          }),
        );
      });
    });

    describe("flush integration", () => {
      it("clamps observation cost fields before inserting", async () => {
        const mockInsert = vi
          .spyOn(clickhouseClientMock, "insert")
          .mockResolvedValue();

        writer.addToQueue(TableName.Observations, {
          id: "obs-1",
          project_id: "proj-1",
          cost_details: { total: 8_859_794 },
          provided_cost_details: { total: 1_234_567 },
          total_cost: 9_999_999,
        } as any);
        await vi.advanceTimersByTimeAsync(writer.writeInterval);

        const inserted = mockInsert.mock.calls[0][0].values[0];
        expect(inserted.cost_details.total).toBe(999_999.999_999);
        expect(inserted.provided_cost_details.total).toBe(999_999.999_999);
        expect(inserted.total_cost).toBe(999_999.999_999);
      });
    });
  });

  describe("truncation logic", () => {
    it("should truncate oversized input field", () => {
      const largeInput = "a".repeat(2 * 1024 * 1024); // 2MB string
      const record = {
        id: "1",
        input: largeInput,
        output: "normal output",
        metadata: { key: "value" },
      } as any;

      const truncatedRecord = writer["truncateOversizedRecord"](
        TableName.Traces,
        record,
      );

      expect(truncatedRecord.id).toBe("1");
      expect((truncatedRecord as any).output).toBe("normal output");
      expect((truncatedRecord as any).metadata).toEqual({ key: "value" });
      expect((truncatedRecord as any).input).toContain(
        "[TRUNCATED: Field exceeded size limit]",
      );
      expect((truncatedRecord as any).input.length).toBeLessThan(
        largeInput.length,
      );
      expect((truncatedRecord as any).input).toMatch(
        /^a+\[TRUNCATED: Field exceeded size limit]$/,
      );
    });

    it("should truncate oversized output field", () => {
      const largeOutput = "b".repeat(2 * 1024 * 1024); // 2MB string
      const record = {
        id: "1",
        input: "normal input",
        output: largeOutput,
        metadata: { key: "value" },
      };

      const truncatedRecord = writer["truncateOversizedRecord"](
        TableName.Traces,
        record,
      );

      expect(truncatedRecord.id).toBe("1");
      expect(truncatedRecord.input).toBe("normal input");
      expect(truncatedRecord.metadata).toEqual({ key: "value" });
      expect(truncatedRecord.output).toContain(
        "[TRUNCATED: Field exceeded size limit]",
      );
      expect(truncatedRecord.output.length).toBeLessThan(largeOutput.length);
      expect(truncatedRecord.output).toMatch(
        /^b+\[TRUNCATED: Field exceeded size limit\]$/,
      );
    });

    it("should truncate oversized metadata values", () => {
      const largeMetadataValue = "c".repeat(2 * 1024 * 1024); // 2MB string
      const record = {
        id: "1",
        input: "normal input",
        output: "normal output",
        metadata: {
          normalKey: "normal value",
          largeKey: largeMetadataValue,
          anotherNormalKey: "another normal value",
        },
      };

      const truncatedRecord = writer["truncateOversizedRecord"](
        TableName.Traces,
        record,
      );

      expect(truncatedRecord.id).toBe("1");
      expect(truncatedRecord.input).toBe("normal input");
      expect(truncatedRecord.output).toBe("normal output");
      expect(truncatedRecord.metadata.normalKey).toBe("normal value");
      expect(truncatedRecord.metadata.anotherNormalKey).toBe(
        "another normal value",
      );
      expect(truncatedRecord.metadata.largeKey).toContain(
        "[TRUNCATED: Field exceeded size limit]",
      );
      expect(truncatedRecord.metadata.largeKey.length).toBeLessThan(
        largeMetadataValue.length,
      );
      expect(truncatedRecord.metadata.largeKey).toMatch(
        /^c+\[TRUNCATED: Field exceeded size limit\]$/,
      );
    });

    it("should not truncate normal-sized fields", () => {
      const normalRecord = {
        id: "1",
        input: "normal input",
        output: "normal output",
        metadata: { key: "value" },
      };

      const truncatedRecord = writer["truncateOversizedRecord"](
        TableName.Traces,
        normalRecord,
      );

      expect(truncatedRecord).toEqual(normalRecord);
    });

    it("should handle size errors with truncation in retry logic", async () => {
      const largeInput = "a".repeat(2 * 1024 * 1024); // 2MB string
      const record = {
        id: "1",
        input: largeInput,
        output: "normal output",
      } as any;

      const mockInsert = vi
        .spyOn(clickhouseClientMock, "insert")
        .mockRejectedValueOnce(
          new Error(
            "size of json object is extremely large and expected not greater than 1MB",
          ),
        )
        .mockResolvedValueOnce();

      writer.addToQueue(TableName.Traces, record);

      await vi.advanceTimersByTimeAsync(writer.writeInterval);

      expect(mockInsert).toHaveBeenCalledTimes(1);
      expect(logger.error).toHaveBeenCalledWith(
        expect.stringContaining("size of json object is extremely large"),
      );

      // Second attempt with truncated data
      await vi.advanceTimersByTimeAsync(writer.writeInterval);

      expect(mockInsert).toHaveBeenCalledTimes(2);
      expect(logger.warn).toHaveBeenCalledWith(
        expect.stringContaining("Truncating oversized records"),
        expect.objectContaining({
          attemptNumber: 1,
          error:
            "size of json object is extremely large and expected not greater than 1MB",
        }),
      );
      expect(writer["queue"][TableName.Traces]).toHaveLength(0);

      // Verify that the second call used truncated data
      const secondCallArgs = mockInsert.mock.calls[1][0];
      expect(secondCallArgs.values[0].input).toContain(
        "[TRUNCATED: Field exceeded size limit]",
      );
    });

    it("should handle string length errors with batch splitting", async () => {
      const mockInsert = vi
        .spyOn(clickhouseClientMock, "insert")
        .mockRejectedValueOnce(new Error("invalid string length"))
        .mockResolvedValue();

      // Add 4 records to test splitting
      const records = Array.from({ length: 4 }, (_, i) => ({
        id: `${i}`,
        name: `test${i}`,
      }));

      records.forEach((record) => {
        writer.addToQueue(TableName.Traces, record as any);
      });

      await vi.advanceTimersByTimeAsync(writer.writeInterval);

      // After first interval: should have done initial call + retry with first half
      expect(mockInsert).toHaveBeenCalled();
      expect(logger.warn).toHaveBeenCalledWith(
        expect.stringContaining("Splitting batch and retrying"),
        expect.objectContaining({
          error: "invalid string length",
          batchSize: 4,
        }),
      );

      // Check that queue now has the second half (2 records) at the front
      expect(writer["queue"][TableName.Traces]).toHaveLength(2);
      expect(writer["queue"][TableName.Traces][0].data.id).toBe("2");
      expect(writer["queue"][TableName.Traces][1].data.id).toBe("3");

      // Advance timer again to process the requeued items
      await vi.advanceTimersByTimeAsync(writer.writeInterval);

      expect(writer["queue"][TableName.Traces]).toHaveLength(0);
    });
  });
});
