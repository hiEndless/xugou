import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { Bindings } from "../../../models/db";
import { createMigratedSqliteD1 } from "../../../test-utils/migrated-d1";
import type { SqliteD1 } from "../../../test-utils/sqlite-d1";
import { blockSamples } from "../metricblock/materialize";
import { decodeBlock, inspectBlock } from "../metricblock/decode";
import { METRIC_HISTORY_CONTENT_TYPE, type AgentMetricHistoryMetadata } from "../metricblock/history-wire";
import { queryLegacyAgentMetrics } from "../persistence/D1LegacyAgentFacade";
import { createMetricHistoryResponse } from "./history-response";
import { agentsV2 } from "./routes";

const fixture = (JSON.parse(readFileSync(resolve(process.cwd(),
  "src/modules/agents/metricblock/__fixtures__/blocks.json"), "utf8")) as {
    cases: Array<{ name: string; data: string; pointCount: number }>;
  }).cases.find(({ name }) => name === "rollup-60s")!;
const raw = new Uint8Array(Buffer.from(fixture.data, "base64"));
const hour = inspectBlock(raw).bucketStart;
let sqlite: SqliteD1;
let env: Bindings;

beforeEach(() => {
  sqlite = createMigratedSqliteD1();
  sqlite.raw.prepare("INSERT INTO agents(id,name,token,created_at,updated_at) VALUES(1,?,?,?,?)")
    .run("fixture", "unused", "2026-01-01", "2026-01-01");
  sqlite.raw.prepare(
    "INSERT INTO agent_nodes(id,name,collect_interval_ms,report_interval_ms,created_at_ms,updated_at_ms) VALUES(1,?,1000,60000,0,0)"
  ).run("fixture");
  env = { DB: sqlite.DB } as unknown as Bindings;
  vi.spyOn(Date, "now").mockReturnValue((hour + 7200) * 1000);
});

afterEach(() => { sqlite.close(); vi.restoreAllMocks(); });

async function seedHistory() {
  const sample = blockSamples(await decodeBlock(raw))[0];
  const legacy = { ...sample, timestampMs: (hour + 1800) * 1000, cpuUsage: 99 };
  sqlite.raw.prepare(
    `INSERT INTO agent_metric_blocks
     (agent_id,resolution,bucket_start,point_count,codec,byte_size,data,summary_json)
     VALUES(1,60,?,?,1,?,?,?)`
  ).run(hour, fixture.pointCount, raw.length, raw,
    JSON.stringify({ sealed: true, legacy_samples: [legacy] }));
  sqlite.raw.prepare(
    `INSERT INTO agent_metric_blocks
     (agent_id,resolution,bucket_start,point_count,codec,byte_size,data,public_json)
     VALUES(1,60,?,0,1,0,?,?)`
  ).run(hour + 3600, new Uint8Array(), JSON.stringify({ v: 1, r: 1, p: {} }));
  const pending = { collected_at: new Date((hour + 3660) * 1000).toISOString(),
    cpu: { usage: 77 }, memory: { total: 100, used: 70, free: 30, usage_rate: 70 } };
  sqlite.raw.prepare(
    `INSERT INTO agent_metric_blocks
     (agent_id,resolution,bucket_start,point_count,codec,byte_size,data,summary_json)
     VALUES(1,1,?,60,1,0,?,?)`
  ).run(hour + 3660, new Uint8Array(), JSON.stringify(pending));
  return { legacy, pending };
}

describe("管理端原始历史传输", () => {
  it("Worker 不解压，原样流式发送 Go 归档、兼容样本和未封存摘要", async () => {
    const expected = await seedHistory();
    const inflate = vi.spyOn(globalThis, "DecompressionStream");

    const response = await createMetricHistoryResponse(env, 1, 24);

    expect(response?.headers.get("Content-Type")).toBe(METRIC_HISTORY_CONTENT_TYPE);
    expect(response?.headers.get("Cache-Control")).toBe("private, no-store");
    const bytes = new Uint8Array(await response!.arrayBuffer());
    expect(inflate).not.toHaveBeenCalled();
    expect(String.fromCharCode(...bytes.subarray(0, 4))).toBe("XHR1");
    const metadataLength = new DataView(bytes.buffer).getUint32(4, true);
    const metadata = JSON.parse(new TextDecoder().decode(bytes.subarray(8, 8 + metadataLength))) as AgentMetricHistoryMetadata;
    expect(metadata).toMatchObject({ agent_id: 1, resolution: 60, max_points: 160 });
    expect(metadata.blocks).toEqual([{ byte_length: raw.length, legacy_samples: [expected.legacy] }]);
    expect(metadata.pending).toEqual([expected.pending]);
    expect(bytes.subarray(8 + metadataLength)).toEqual(raw);
    expect(Number(response?.headers.get("Content-Length"))).toBe(bytes.byteLength);
  });

  it("同一真实归档经浏览器解码得到与旧 JSON API 完全相同的字段", async () => {
    await seedHistory();
    const response = await createMetricHistoryResponse(env, 1, 24);
    // 动态边界避免让 backend 的 CommonJS 类型项目引入前端 import.meta/React 构建配置。
    const decoderPath = resolve(process.cwd(),
      "../frontend/src/features/agents/history/decodeAgentMetricHistory.ts");
    const browser = await import(decoderPath) as {
      decodeAgentMetricHistory(buffer: ArrayBuffer, agentId: number): Promise<unknown>;
    };
    const actual = await browser.decodeAgentMetricHistory(await response!.arrayBuffer(), 1);
    const legacy = await queryLegacyAgentMetrics(env, 1, 24);
    expect(actual).toEqual(JSON.parse(JSON.stringify(legacy)));
  });

  it("路由拒绝非法窗口，并对未知 Agent 返回 404", async () => {
    const invalid = await agentsV2.request("/1/metrics/blocks?hours=2", undefined, env);
    expect(invalid.status).toBe(400);
    const unknown = await agentsV2.request("/999/metrics/blocks?hours=24", undefined, env);
    expect(unknown.status).toBe(404);
    expect(await createMetricHistoryResponse(env, 999, 24)).toBeNull();
  });
});
