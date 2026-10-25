import { resolve } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { MetricHistoryWork, PublicMetricHourResponse } from "../../../../../frontend/src/features/status/metric-history";

const clientPath = resolve(__dirname, "../../../../../frontend/src/features/status/metric-history-client.ts");
const apiPath = resolve(__dirname, "../../../../../frontend/src/api/status.ts");
const transportPath = resolve(__dirname, "../../../../../frontend/src/api/generated/v2-client.ts");
const HOUR = 1_800_000_000;

function chunk(agentId = 1, bucketStart = HOUR): PublicMetricHourResponse {
  return { success: true, agent_id: agentId, bucket_start: bucketStart, chunk: {
    v: 1, r: 1, p: { "0": { c: 12, m: [1000, 250, 750, 25], l: [1, 2, 3],
      s: [null, null], p: null, t: null, u: null, v4: null, v6: null, d: [], n: [] } },
  } };
}

const work: MetricHistoryWork = { agentId: 1, from: HOUR, to: HOUR + 3600, hours: [chunk()] };

beforeEach(() => vi.resetModules());
afterEach(() => {
  vi.unstubAllGlobals();
  vi.doUnmock(transportPath);
});

describe("browser public metric history", () => {
  it("falls back to local expansion when a Web Worker cannot start", async () => {
    vi.stubGlobal("Worker", class { constructor() { throw new Error("Worker blocked"); } });
    const module = await import(clientPath);
    expect(await module.materializeMetricHours(work)).toMatchObject([{ cpu_usage: 12, memory_used: 250 }]);
  });

  it("falls back when a started Web Worker fails, while preserving cancellation", async () => {
    const terminate = vi.fn();
    vi.stubGlobal("Worker", class {
      onerror?: () => void;
      terminate = terminate;
      postMessage() { queueMicrotask(() => this.onerror?.()); }
    });
    const module = await import(clientPath);
    expect(await module.materializeMetricHours(work)).toMatchObject([{ cpu_usage: 12 }]);
    expect(terminate).toHaveBeenCalledOnce();
    const abort = new AbortController();
    abort.abort();
    await expect(module.materializeMetricHours(work, abort.signal)).rejects.toMatchObject({ name: "AbortError" });
  });

  it("downloads at most four hours concurrently and reuses all unchanged revisions", async () => {
    let active = 0;
    let maximum = 0;
    let downloads = 0;
    let headRevision = "1";
    const GET = vi.fn(async (path: string, options: {
      params: { path: { agentId: number; bucketStart?: number }; query?: { buckets: string } };
      cache?: string;
    }) => {
      const { agentId, bucketStart } = options.params.path;
      if (path.endsWith("/manifest")) {
        return { response: new Response(), data: { success: true, agent_id: agentId,
          from: HOUR, to: HOUR + 25 * 3600,
          hours: Array.from({ length: 25 }, (_, index) => ({
            bucket_start: HOUR + index * 3600, revision: index === 24 ? headRevision : "1",
          })) } };
      }
      expect(options.cache).toBe("no-cache");
      if (path.endsWith("/hours")) {
        return { response: new Response(), data: { success: true, agent_id: agentId,
          hours: [], missing: options.params.query!.buckets.split(",").map(Number) } };
      }
      downloads++;
      active++;
      maximum = Math.max(maximum, active);
      await new Promise((resolve) => setTimeout(resolve, 2));
      active--;
      return { response: new Response(), data: chunk(agentId, bucketStart) };
    });
    vi.doMock(transportPath, () => ({ v2Client: { GET },
      unwrapOpenApi: (result: { data: unknown }) => result.data }));
    const api = await import(apiPath);
    await api.getPublicAgentMetrics(1);
    expect(downloads).toBe(25);
    expect(maximum).toBe(4);
    await api.getPublicAgentMetrics(1);
    expect(downloads).toBe(25);
    headRevision = "2";
    await api.getPublicAgentMetrics(1);
    expect(downloads).toBe(26);
    // Six agents exceed the 128-hour cache budget. Reopening the first requires
    // its evicted hours to be loaded again instead of retaining an unbounded map.
    for (let agent = 2; agent <= 6; agent++) await api.getPublicAgentMetrics(agent);
    const before = downloads;
    await api.getPublicAgentMetrics(1);
    expect(downloads).toBeGreaterThan(before);
    expect(maximum).toBe(4);
  });

  it("loads a cold v6 day in one bulk request and then fetches only a changed hour", async () => {
    let headRevision = "1";
    const bulkBuckets: number[][] = [];
    let singles = 0;
    const GET = vi.fn(async (path: string, options: {
      params: { path: { agentId: number }; query?: { buckets: string } };
      cache?: string;
    }) => {
      const { agentId } = options.params.path;
      if (path.endsWith("/manifest")) {
        return { response: new Response(), data: { success: true, agent_id: agentId,
          from: HOUR, to: HOUR + 25 * 3600,
          hours: Array.from({ length: 25 }, (_, index) => ({
            bucket_start: HOUR + index * 3600, revision: index === 24 ? headRevision : "1",
          })) } };
      }
      if (path.endsWith("/hours")) {
        expect(options.cache).toBe("no-cache");
        const buckets = options.params.query!.buckets.split(",").map(Number);
        bulkBuckets.push(buckets);
        return { response: new Response(), data: { success: true, agent_id: agentId,
          hours: buckets.map((bucket_start) => ({ bucket_start, chunk: chunk(agentId, bucket_start).chunk })),
          missing: [] } };
      }
      singles++;
      throw new Error("Ready public hours must not use single-hour requests");
    });
    vi.doMock(transportPath, () => ({ v2Client: { GET },
      unwrapOpenApi: (result: { data: unknown }) => result.data }));
    const api = await import(apiPath);
    expect((await api.getPublicAgentMetrics(1)).agent).toHaveLength(25);
    expect(bulkBuckets).toHaveLength(1);
    expect(bulkBuckets[0]).toHaveLength(25);
    await api.getPublicAgentMetrics(1);
    expect(bulkBuckets).toHaveLength(1);
    headRevision = "2";
    await api.getPublicAgentMetrics(1);
    expect(bulkBuckets[1]).toEqual([HOUR + 24 * 3600]);
    expect(singles).toBe(0);
  });
});
