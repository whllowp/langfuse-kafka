import { beforeEach, describe, expect, it, vi } from "vitest";

const mockQueryClickhouse = vi.hoisted(() => vi.fn());
const mockShouldSkipObservationsFinal = vi.hoisted(() => vi.fn());
const mockClickhouseCompliantRandomCharacters = vi.hoisted(() => vi.fn());

vi.mock("../repositories", () => ({
  OBSERVATIONS_TO_TRACE_INTERVAL: "INTERVAL 2 DAY",
  SCORE_TO_TRACE_OBSERVATIONS_INTERVAL: "INTERVAL 1 HOUR",
  clickhouseCompliantRandomCharacters: mockClickhouseCompliantRandomCharacters,
  parseClickhouseUTCDateTimeFormat: (date: string) => new Date(date),
  queryClickhouse: mockQueryClickhouse,
  reduceUsageOrCostDetails: (details: Record<string, number>) => details,
}));

vi.mock("../queries/clickhouse-sql/query-options", () => ({
  shouldSkipObservationsFinal: mockShouldSkipObservationsFinal,
}));

import {
  getTraceDeleteCursorPageFromTraces,
  getTracesTable,
  getTracesTableCount,
  getTracesTableMetrics,
} from "./traces-ui-table-service";
import { __resetForTests } from "../cache/queryRequestCoalescer";
import type { FilterState } from "../../types";
import type { TracingSearchType } from "../../interfaces/search";

const scoreFilter: FilterState = [
  {
    column: "scores_avg",
    type: "numberObject",
    key: "quality",
    operator: ">",
    value: 0.5,
  },
];

const environmentFilter: FilterState = [
  {
    column: "environment",
    type: "stringOptions",
    operator: "any of",
    value: ["production"],
  },
];

const datetimeFilter = (value: Date): FilterState => [
  {
    column: "timestamp",
    type: "datetime",
    operator: ">",
    value,
  },
];

const getCapturedQuery = () => {
  const { query, params } = mockQueryClickhouse.mock.calls.at(-1)![0] as {
    query: string;
    params: Record<string, unknown>;
  };

  return {
    query: query.replace(/\s+/g, " "),
    params,
  };
};

describe("getTraceDeleteCursorPageFromTraces", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    __resetForTests();
    let counter = 0;
    mockClickhouseCompliantRandomCharacters.mockImplementation(
      () => `x${++counter}`,
    );
    mockQueryClickhouse.mockResolvedValue([]);
    mockShouldSkipObservationsFinal.mockResolvedValue(false);
  });

  it("uses canonical cursor ordering without FINAL", async () => {
    await getTraceDeleteCursorPageFromTraces({
      projectId: "project-1",
      filter: [],
      cutoffCreatedAt: new Date("2026-01-01T00:00:00.000Z"),
      limit: 100,
    });

    expect(mockQueryClickhouse).toHaveBeenCalledOnce();
    const { query } = mockQueryClickhouse.mock.calls[0]![0] as {
      query: string;
    };
    const normalizedQuery = query.replace(/\s+/g, " ");

    expect(normalizedQuery).toMatch(/\bFROM traces t\b/);
    expect(normalizedQuery).not.toMatch(/\bFROM traces t FINAL\b/);
    expect(normalizedQuery).toContain(
      "ORDER BY t.timestamp DESC, t.id DESC, t.event_ts DESC",
    );
    expect(normalizedQuery).toContain("LIMIT 1 BY id, project_id");
    expect(normalizedQuery).toContain("LIMIT {limit: Int32}");
  });

  it("pushes referenced score names into filter-only score aggregation", async () => {
    await getTracesTable({
      projectId: "project-1",
      filter: scoreFilter,
      limit: 50,
      page: 0,
    });

    const { query, params } = getCapturedQuery();
    expect(query).toContain("name IN ({");
    expect(Object.values(params)).toContainEqual(["quality"]);
  });

  it("keeps complete score aggregation for metrics", async () => {
    await getTracesTableMetrics({
      projectId: "project-1",
      filter: scoreFilter,
      limit: 50,
      page: 0,
    });

    expect(getCapturedQuery().query).not.toContain("name IN ({");
  });

  it("keeps complete score aggregation when ordering by scores", async () => {
    await getTracesTable({
      projectId: "project-1",
      filter: scoreFilter,
      orderBy: { column: "scores_avg", order: "ASC" },
      limit: 50,
      page: 0,
    });

    expect(getCapturedQuery().query).not.toContain("name IN ({");
  });

  it("coalesces identical count requests into one queryClickhouse call despite random filter parameter names", async () => {
    const props = {
      projectId: "project-coalesce",
      filter: environmentFilter,
      searchType: [] as TracingSearchType[],
    };

    // Identical logical requests render different query texts because every
    // filter apply() draws fresh random parameter names. Coalescing must key
    // on the structured request, so both sequential calls share one query.
    await getTracesTableCount(props);
    await getTracesTableCount(props);
    expect(mockQueryClickhouse).toHaveBeenCalledTimes(1);

    // A fresh project keeps this pair out of the TTL result cache, so the
    // concurrent calls can only merge via in-flight single-flight. That
    // requires the coalescer to register its in-flight placeholder
    // synchronously before its first await: otherwise the second call of the
    // synchronous Promise.all pair starts before the placeholder exists and
    // the loader runs twice.
    const concurrentProps = { ...props, projectId: "project-coalesce-2" };
    await Promise.all([
      getTracesTableCount(concurrentProps),
      getTracesTableCount(concurrentProps),
    ]);
    expect(mockQueryClickhouse).toHaveBeenCalledTimes(2);
  });

  it("does not coalesce count requests across different datetime filter values", async () => {
    await getTracesTableCount({
      projectId: "project-datetime",
      filter: datetimeFilter(new Date("2026-01-01T00:00:00.000Z")),
      searchType: [],
    });
    await getTracesTableCount({
      projectId: "project-datetime",
      filter: datetimeFilter(new Date("2026-06-01T00:00:00.000Z")),
      searchType: [],
    });

    // Distinct time windows must produce distinct keys. A key serializer
    // that collapses Date instances would collide here, run one query, and
    // serve one time window's count for both requests.
    expect(mockQueryClickhouse).toHaveBeenCalledTimes(2);
  });

  it("coalesces count requests with the same datetime filter value", async () => {
    const props = {
      projectId: "project-datetime-same",
      filter: datetimeFilter(new Date("2026-01-01T00:00:00.000Z")),
      searchType: [] as TracingSearchType[],
    };

    await getTracesTableCount(props);
    await getTracesTableCount(props);
    expect(mockQueryClickhouse).toHaveBeenCalledTimes(1);
  });
});
